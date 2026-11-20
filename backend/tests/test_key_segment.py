"""代理键键段批次端点与事实外键批次归属测试。

覆盖：
- 键段批次端点计算：闭区间恰好容纳标称键数，按实际步长核对首键/末键/总数
- 分配批次登记、历史重叠编号的归属判定
- 事实外键按批次校验、重叠编号结合原批次与自然键重关联
- 有效编号不指向多个成员，不同成员的键不能共享
"""
from __future__ import annotations

import pytest

from app.store import Store
from app.store import store as global_store
from app.services.pipeline import fact_partition_write
from app.services.surrogate_key import (
    KEY_SEGMENT_SIZE,
    SurrogateKeyService,
    _allocate_key_range,
)


@pytest.fixture
def store() -> Store:
    """每个测试使用独立 store 实例。"""
    return Store()


def _register_overlap(registry, dim_code="DIM_PRODUCT"):
    """登记两个历史批次：批次1记录末键多出一个编号（历史缺陷），
    批次2首键与批次1记录末键相同，形成边界编号重叠。"""
    batch1 = registry.register(
        key_id=1, key_name="sk_product", dim_code=dim_code,
        start=1000000, end=1010000, step=1, count=10000,
    )
    batch2 = registry.register(
        key_id=1, key_name="sk_product", dim_code=dim_code,
        start=1010000, end=1019999, step=1, count=10000,
    )
    return batch1, batch2


# ======================================================================
# Section A: 键段批次端点计算
# ======================================================================


class TestKeySegmentEndpoint:
    """键段批次端点计算：首键、末键、可分配总数按实际步长核对。"""

    def test_closed_interval_exact_count(self) -> None:
        """标称一万个键的闭区间不多出一个编号：末键 = 首键 + (总数-1)*步长。"""
        seg = _allocate_key_range({"初始值": "1000000", "步长": "1"})
        assert seg["start"] == 1000000
        assert seg["end"] == 1009999
        assert seg["count"] == KEY_SEGMENT_SIZE == 10000
        # 闭区间实际键数核对
        assert (seg["end"] - seg["start"]) // seg["step"] + 1 == seg["count"]

    def test_step_scales_last_key(self) -> None:
        """步长为 5 时末键 = 首键 + 9999*5，可分配总数仍为标称值。"""
        seg = _allocate_key_range({"初始值": "100", "步长": "5"})
        assert seg["start"] == 100
        assert seg["end"] == 100 + 5 * (KEY_SEGMENT_SIZE - 1)
        assert (seg["end"] - seg["start"]) % seg["step"] == 0
        assert (seg["end"] - seg["start"]) // seg["step"] + 1 == KEY_SEGMENT_SIZE

    def test_next_batch_starts_after_valid_end(self, store: Store) -> None:
        """下一批次首键紧接上一批次修正后的末键，两批次有效键域不重叠。"""
        registry = store.key_batches
        # 历史缺陷批次：记录末键 1010000 比修正末键 1009999 多一个编号
        batch1 = registry.register(
            key_id=1, key_name="sk_a", dim_code="DIM_A",
            start=1000000, end=1010000, step=1, count=10000,
        )
        seg2 = _allocate_key_range(
            {"初始值": "1000000", "步长": "1"},
            registry.batches_for(key_id=1),
        )
        assert seg2["start"] == registry.valid_end(batch1) + 1 == 1010000
        batch2 = registry.register(
            key_id=1, key_name="sk_a", dim_code="DIM_A",
            start=seg2["start"], end=seg2["end"], step=seg2["step"], count=seg2["count"],
        )
        # 边界编号只落在批次2的有效键域内
        assert not registry.owns(batch1, seg2["start"])
        assert registry.owns(batch2, seg2["start"])


# ======================================================================
# Section B: 分配批次登记与重叠编号归属
# ======================================================================


class TestBatchAttribution:
    """同一边界编号被两个分配批次引用时的归属判定。"""

    def test_boundary_key_owned_by_single_batch(self, store: Store) -> None:
        registry = store.key_batches
        batch1, batch2 = _register_overlap(registry)
        # 两个批次的原始记录都引用了边界编号
        refs = registry.referencing_batches("DIM_PRODUCT", 1010000)
        assert [b["batch_id"] for b in refs] == [batch1["batch_id"], batch2["batch_id"]]
        # 修正后归属唯一批次：批次1有效末键为1009999，边界编号归批次2
        assert registry.valid_end(batch1) == 1009999
        owner = registry.attribute("DIM_PRODUCT", 1010000)
        assert owner is not None
        assert owner["batch_id"] == batch2["batch_id"]

    def test_first_last_and_total_per_batch(self, store: Store) -> None:
        registry = store.key_batches
        batch1, batch2 = _register_overlap(registry)
        for batch, first, last in ((batch1, 1000000, 1009999), (batch2, 1010000, 1019999)):
            start, end, step = registry.valid_range(batch)
            assert (start, end, step) == (first, last, 1)
            assert (end - start) // step + 1 == 10000

    def test_overlap_keys_reported_with_owner(self, store: Store) -> None:
        registry = store.key_batches
        batch1, batch2 = _register_overlap(registry)
        overlaps = registry.overlap_keys("DIM_PRODUCT")
        assert [o["value"] for o in overlaps] == [1010000]
        assert overlaps[0]["owner_batch"] == batch2["batch_id"]
        assert set(overlaps[0]["referenced_by"]) == {batch1["batch_id"], batch2["batch_id"]}

    def test_out_of_range_key_has_no_owner(self, store: Store) -> None:
        registry = store.key_batches
        _register_overlap(registry)
        assert registry.attribute("DIM_PRODUCT", 1020000) is None
        assert registry.attribute("DIM_PRODUCT", 999999) is None


# ======================================================================
# Section C: 批次内成员归属
# ======================================================================


class TestMemberAttribution:
    """批次内编号确认给成员：不同成员的键不能共享。"""

    def test_assign_and_holder(self, store: Store) -> None:
        registry = store.key_batches
        batch = registry.register(
            key_id=1, key_name="sk_a", dim_code="DIM_A",
            start=100, end=199, step=1, count=100,
        )
        ok, _ = registry.assign(batch["batch_id"], 100, "SKU-1")
        assert ok
        assert registry.holder(batch["batch_id"], 100) == "SKU-1"
        # 同一成员重复确认幂等
        ok, _ = registry.assign(batch["batch_id"], 100, "SKU-1")
        assert ok

    def test_key_not_shared_between_members(self, store: Store) -> None:
        registry = store.key_batches
        batch = registry.register(
            key_id=1, key_name="sk_a", dim_code="DIM_A",
            start=100, end=199, step=1, count=100,
        )
        registry.assign(batch["batch_id"], 100, "SKU-1")
        ok, reason = registry.assign(batch["batch_id"], 100, "SKU-2")
        assert not ok
        assert "不能共享" in reason

    def test_assign_out_of_valid_range_rejected(self, store: Store) -> None:
        registry = store.key_batches
        # 记录末键 200 比修正末键 198 多一个步长
        batch = registry.register(
            key_id=1, key_name="sk_a", dim_code="DIM_A",
            start=100, end=200, step=2, count=50,
        )
        assert registry.valid_end(batch) == 198
        ok, _ = registry.assign(batch["batch_id"], 200, "SKU-1")
        assert not ok
        # 未按步长对齐的编号同样拒绝
        ok, _ = registry.assign(batch["batch_id"], 101, "SKU-1")
        assert not ok


# ======================================================================
# Section D: 事实外键按批次校验与重关联
# ======================================================================


class TestFactFkBatchValidation:
    """事实设计用修正后的归属校验分区外键。"""

    def _write(self, store: Store, records: list[dict]) -> dict:
        return fact_partition_write(
            fact_key="销售明细事实",
            records=records,
            partition_field="dt",
            dim_fk_fields={},
            store=store,
            sk_fk_fields={"产品键": "DIM_PRODUCT"},
            natural_key_field="产品自然键",
            batch_field="_批次",
        )

    def test_valid_key_attributed_to_batch(self, store: Store) -> None:
        batch1, _ = _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "1000001", "产品自然键": "SKU-1",
             "_批次": batch1["batch_id"], "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 1
        assert result["violations"] == []
        attr = result["key_attributions"][0]
        assert attr["batch_id"] == batch1["batch_id"]
        assert attr["key"] == 1000001

    def test_boundary_key_row_from_owner_batch_keeps_key(self, store: Store) -> None:
        batch1, batch2 = _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "1010000", "产品自然键": "SKU-2",
             "_批次": batch2["batch_id"], "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 1
        attr = result["key_attributions"][0]
        assert attr["batch_id"] == batch2["batch_id"]
        # 标明边界编号同时被批次1的原始记录引用
        assert attr["shared_refs"] == [batch1["batch_id"]]

    def test_overlapped_row_reassociated_by_batch_and_natural_key(self, store: Store) -> None:
        """原批次失去边界编号的行：结合原批次与自然键换发新键，可按原批次核对旧键和新键。"""
        batch1, _ = _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "1000001", "产品自然键": "SKU-1",
             "_批次": batch1["batch_id"], "dt": "2026-11-01"},
            {"产品键": "1010000", "产品自然键": "SKU-3",
             "_批次": batch1["batch_id"], "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 2
        assert len(result["reassociated"]) == 1
        reass = result["reassociated"][0]
        assert reass["row"] == "SKU-3"
        assert reass["batch_id"] == batch1["batch_id"]
        assert reass["old_key"] == 1010000
        # 旧键不在原批次修正后的有效键域内，新键在其中
        assert not store.key_batches.owns(batch1, reass["old_key"])
        assert store.key_batches.owns(batch1, reass["new_key"])
        # 批次1内 1000001 已确认给 SKU-1，换发第一个未确认编号 1000000
        assert reass["new_key"] == 1000000
        # 事实行写入的是新键
        rows = [r for r in store.rows("fact_design") if r.get("产品自然键") == "SKU-3"]
        assert len(rows) == 1
        assert rows[0]["产品键"] == "1000000"

    def test_other_batch_rows_untouched_by_number(self, store: Store) -> None:
        """只按编号会改错另一批已写入的事实：归属批次的行必须原样保留。"""
        batch1, batch2 = _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "1010000", "产品自然键": "SKU-2",
             "_批次": batch2["batch_id"], "dt": "2026-11-01"},
            {"产品键": "1010000", "产品自然键": "SKU-3",
             "_批次": batch1["batch_id"], "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 2
        # 批次2（归属批次）的行保持原编号
        rows_b2 = [r for r in store.rows("fact_design") if r.get("产品自然键") == "SKU-2"]
        assert rows_b2[0]["产品键"] == "1010000"
        # 批次1的行被换发新键
        rows_b1 = [r for r in store.rows("fact_design") if r.get("产品自然键") == "SKU-3"]
        assert rows_b1[0]["产品键"] != "1010000"
        # 边界编号只确认给批次2的成员，不指向多个成员
        assert store.key_batches.holder(batch2["batch_id"], 1010000) == "SKU-2"
        assert store.key_batches.holder(batch1["batch_id"], 1010000) is None

    def test_valid_key_not_shared_between_members(self, store: Store) -> None:
        """有效编号指向多个成员时拒绝写入。"""
        batch1, _ = _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "1000001", "产品自然键": "SKU-1",
             "_批次": batch1["batch_id"], "dt": "2026-11-01"},
            {"产品键": "1000001", "产品自然键": "SKU-9",
             "_批次": batch1["batch_id"], "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 1
        assert len(result["violations"]) == 1
        assert "归属冲突" in result["violations"][0]["reason"]

    def test_key_out_of_any_batch_rejected(self, store: Store) -> None:
        _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "9999999", "产品自然键": "SKU-X", "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 0
        assert "不在任何分配批次" in result["violations"][0]["reason"]

    def test_row_batch_mismatch_rejected(self, store: Store) -> None:
        """行记录的原批次从未引用该编号时拒绝。"""
        _, batch2 = _register_overlap(store.key_batches)
        result = self._write(store, [
            {"产品键": "1000001", "产品自然键": "SKU-1",
             "_批次": batch2["batch_id"], "dt": "2026-11-01"},
        ])
        assert result["wrote"] == 0
        assert "原分配批次" in result["violations"][0]["reason"]


# ======================================================================
# Section E: 代理键服务动作与批次登记
# ======================================================================


class TestSurrogateKeyServiceAllocation:
    """代理键服务：分配键段动作登记批次，重置键值清除批次。"""

    @pytest.fixture
    def service(self):
        service = SurrogateKeyService()
        created: list[int] = []
        yield service, created
        for entry_id in created:
            global_store.key_batches.clear(entry_id)
        rows = global_store.rows("surrogate_key")
        rows[:] = [r for r in rows if r.get("id") not in created]

    def _create(self, service, created, **values):
        entry, missing = service.create_entry(values)
        assert not missing
        created.append(entry["id"])
        return entry

    def test_allocate_registers_batch(self, service) -> None:
        svc, created = service
        entry = self._create(
            svc, created, 键值名称="sk_t1", 目标维度="DIM_T1",
            生成策略="自增序列", 初始值="5000000", 步长="1",
        )
        entry, message = svc.run_action(entry["id"], "分配键段")
        assert entry is not None
        batches = global_store.key_batches.batches_for(key_id=entry["id"])
        assert len(batches) == 1
        batch = batches[0]
        assert (batch["start"], batch["end"], batch["count"], batch["step"]) == (
            5000000, 5009999, 10000, 1,
        )
        assert "首键5000000" in message
        assert "末键5009999" in message
        assert "可分配10000个" in message
        assert entry["_当前批次"] == batch["batch_id"]

    def test_second_batch_has_no_overlap(self, service) -> None:
        svc, created = service
        entry = self._create(
            svc, created, 键值名称="sk_t2", 目标维度="DIM_T2",
            生成策略="自增序列", 初始值="1000000", 步长="1",
        )
        svc.run_action(entry["id"], "分配键段")
        svc.run_action(entry["id"], "分配键段")
        batches = global_store.key_batches.batches_for(key_id=entry["id"])
        assert len(batches) == 2
        first, second = batches
        assert second["start"] == first["end"] + 1
        # 两批次有效键域无交集，同一维度下无重叠编号
        assert global_store.key_batches.overlap_keys("DIM_T2") == []

    def test_reset_clears_batches(self, service) -> None:
        svc, created = service
        entry = self._create(
            svc, created, 键值名称="sk_t3", 目标维度="DIM_T3",
            生成策略="自增序列", 初始值="100", 步长="1",
        )
        svc.run_action(entry["id"], "分配键段")
        _, message = svc.run_action(entry["id"], "重置键值")
        assert global_store.key_batches.batches_for(key_id=entry["id"]) == []
        assert "已清除1个历史分配批次" in message
