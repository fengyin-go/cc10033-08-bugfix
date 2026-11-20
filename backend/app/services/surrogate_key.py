"""代理键服务：管理维度表的代理键生成策略。

增强能力：
- 键段分配策略：自增序列/UUID_HASH/HASH_MOD
- 步长和初始值管理
- 键段批次端点计算：闭区间末键 = 首键 + (可分配总数 - 1) * 步长
- 按分配批次登记键段，供事实外键按批次校验归属
- 键耗尽检测和自动重置
- 冲突处理：跳过重试/重新分配/异常告警
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import KeyBatchRegistry, store

KEY_STRATEGIES = {"自增序列", "UUID_HASH", "HASH_MOD", "雪花算法", "时间戳序列"}

#: 每个键段批次的标称键数
KEY_SEGMENT_SIZE = 10000


class SurrogateKeyService(BaseModuleService):
    MODULE = "surrogate_key"
    ENTITY = "键值定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["键值名称", "目标维度", "生成策略", "初始值", "步长", "唯一范围", "冲突处理"]
    REQUIRED_FIELDS = ["键值名称", "目标维度", "生成策略"]
    STATUS_ORDER = ["草稿", "已分配", "已耗尽", "已重置"]
    ACTION_RULES = {"分配键段": "草稿", "重置键值": "已耗尽"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        strategy = str(values.get("生成策略", "")).strip()
        if strategy not in KEY_STRATEGIES:
            values["生成策略"] = "自增序列"

        if not values.get("初始值"):
            values["初始值"] = "1000000"
        if not values.get("步长"):
            values["步长"] = "1"
        if not values.get("唯一范围"):
            values["唯一范围"] = "全局"
        if not values.get("冲突处理"):
            values["冲突处理"] = "跳过重试"

        # 消费维度定义
        store.channel.consume("dim.defined", "surrogate_key")

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("surrogate_key.defined", {
            "key_name": entry["键值名称"],
            "dim_code": entry.get("目标维度"),
            "strategy": entry.get("生成策略"),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "surrogate_key", "surrogate_key_def",
            f"sk_{entry['id']}",
            {"key_name": entry["键值名称"], "dim_code": entry.get("目标维度")},
            ["dim_definition", "surrogate_key"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        if action == "分配键段":
            prior = store.key_batches.batches_for(key_id=entry_id)
            allocated = _allocate_key_range(entry, prior)
            batch = store.key_batches.register(
                key_id=entry_id,
                key_name=str(entry["键值名称"]),
                dim_code=str(entry.get("目标维度", "")),
                start=allocated["start"],
                end=allocated["end"],
                step=allocated["step"],
                count=allocated["count"],
            )
            entry["_当前批次"] = batch["batch_id"]
            entry["_键段首键"] = str(allocated["start"])
            entry["_键段末键"] = str(allocated["end"])
            store.channel.publish("surrogate_key.allocated", {
                "key_name": entry["键值名称"],
                "range": allocated,
                "batch": batch,
                "entry_id": entry_id,
            })
            overlaps = store.key_batches.overlap_keys(dim_code=batch["dim_code"])
            message = (
                f"{message}：批次{batch['batch_id']} 首键{allocated['start']} "
                f"末键{allocated['end']} 可分配{allocated['count']}个"
                f"（步长{allocated['step']}）"
            )
            if overlaps:
                message += f"；检测到{len(overlaps)}个历史重叠编号，归属以修正后键段为准"
        elif action == "重置键值":
            cleared = store.key_batches.clear(entry_id)
            for field in ("_当前批次", "_键段首键", "_键段末键"):
                entry.pop(field, None)
            store.channel.publish("surrogate_key.reset", {
                "key_name": entry["键值名称"],
                "cleared_batches": cleared,
                "entry_id": entry_id,
            })
            if cleared:
                message = f"{message}：已清除{cleared}个历史分配批次"
        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        exhausted = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已耗尽")
        base["exhausted_keys"] = exhausted
        # 分配批次记录：与事实设计外键校验共用同一份登记册
        base["key_batches"] = [
            {
                "batch_id": batch["batch_id"],
                "key_name": batch["key_name"],
                "dim_code": batch["dim_code"],
                "start": batch["start"],
                "end": batch["end"],
                "valid_end": store.key_batches.valid_end(batch),
                "step": batch["step"],
                "count": batch["count"],
            }
            for batch in store.key_batches.batches_for()
        ]
        base["overlap_keys"] = store.key_batches.overlap_keys()
        return base


def _allocate_key_range(
    entry: dict[str, Any],
    prior_batches: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """计算下一分配批次的键段端点。

    闭区间 [首键, 末键] 恰好容纳 count 个键：末键 = 首键 + (count - 1) * 步长。
    同一键值定义下已有批次时，新批次首键紧接上一批次修正后的末键之后，
    避免历史缺陷批次多出的边界编号与下一批次首尾相接。
    """
    start = int(entry.get("初始值", "1000000"))
    step = int(entry.get("步长", "1"))
    if prior_batches:
        last = max(prior_batches, key=lambda b: b["batch_no"])
        seg_start = KeyBatchRegistry.valid_end(last) + step
    else:
        seg_start = start
    seg_end = seg_start + step * (KEY_SEGMENT_SIZE - 1)
    return {"start": seg_start, "end": seg_end, "count": KEY_SEGMENT_SIZE, "step": step}
