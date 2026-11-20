"""内存数据仓库：跨模块数据通道、失败恢复、快照引擎和产物注册。

各业务模块产生的数据产物通过 DataChannel 发布，下游模块消费；
FailureRecovery 管理断点恢复；SnapshotEngine 提供版本快照能力。
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS


class DataChannel:
    """跨模块数据通道：上游模块发布数据产物，下游模块通过 topic 消费。"""

    def __init__(self) -> None:
        self._channels: dict[str, list[dict[str, Any]]] = {}
        self._consumers: dict[str, list[str]] = {}

    def publish(self, topic: str, payload: dict[str, Any]) -> str:
        """将数据产物发布到指定 topic，返回消息ID。"""
        msg_id = str(uuid.uuid4())[:12]
        entry = {
            "msg_id": msg_id,
            "topic": topic,
            "payload": payload,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
            "expired": False,
        }
        self._channels.setdefault(topic, []).append(entry)
        return msg_id

    def consume(self, topic: str, consumer: str) -> list[dict[str, Any]]:
        """消费指定 topic 上未被当前消费者消费过的消息，标记已消费。"""
        channel = self._channels.get(topic, [])
        results: list[dict[str, Any]] = []
        for entry in channel:
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                results.append(entry)
        return results

    def consume_latest(self, topic: str, consumer: str) -> dict[str, Any] | None:
        """消费指定 topic 上最新一条未被消费的消息。"""
        channel = self._channels.get(topic, [])
        for entry in reversed(channel):
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                return entry
        return None

    def topic_snapshot(self, topic: str) -> list[dict[str, Any]]:
        """获取 topic 上所有消息的快照。"""
        return list(self._channels.get(topic, []))

    def expire_topic(self, topic: str) -> int:
        """过期指定 topic 上所有未消费消息，返回过期数量。"""
        count = 0
        for entry in self._channels.get(topic, []):
            if not entry["expired"]:
                entry["expired"] = True
                count += 1
        return count

    def list_topics(self) -> list[str]:
        return sorted(self._channels)

    def topic_stats(self) -> dict[str, dict[str, int]]:
        stats: dict[str, dict[str, int]] = {}
        for topic, entries in self._channels.items():
            consumed = sum(1 for e in entries if len(e["consumed_by"]) > 0)
            pending = len(entries) - consumed
            expired = sum(1 for e in entries if e["expired"])
            stats[topic] = {"total": len(entries), "consumed": consumed, "pending": pending, "expired": expired}
        return stats


class FailureRecovery:
    """失败恢复管理器：检查点管理、重试计数和失败链路追踪。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._retry_logs: dict[str, list[dict[str, Any]]] = {}
        self._failure_chains: dict[str, list[dict[str, Any]]] = {}

    def create_checkpoint(self, module: str, task_id: str, state: dict[str, Any]) -> str:
        """创建处理检查点，返回检查点ID。"""
        cp_id = f"{module}-{task_id}-{uuid.uuid4().hex[:8]}"
        self._checkpoints[cp_id] = {
            "cp_id": cp_id,
            "module": module,
            "task_id": task_id,
            "state": dict(state),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "restored_at": None,
            "restore_count": 0,
            "completed": False,
        }
        return cp_id

    def get_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        return self._checkpoints.get(cp_id)

    def update_checkpoint(self, cp_id: str, state: dict[str, Any]) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["state"].update(state)
        return True

    def mark_completed(self, cp_id: str) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["completed"] = True
        return True

    def restore_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return None
        cp["restored_at"] = datetime.now(timezone.utc).isoformat()
        cp["restore_count"] += 1
        return dict(cp)

    def log_retry(self, module: str, task_id: str, attempt: int, reason: str, success: bool) -> None:
        self._retry_logs.setdefault(module, [])
        self._retry_logs[module].append({
            "task_id": task_id,
            "attempt": attempt,
            "reason": reason,
            "success": success,
            "logged_at": datetime.now(timezone.utc).isoformat(),
        })

    def retry_history(self, module: str) -> list[dict[str, Any]]:
        return self._retry_logs.get(module, [])

    def log_failure_chain(self, chain_id: str, step: str, module: str, error: str, upstream: str | None = None) -> None:
        self._failure_chains.setdefault(chain_id, [])
        self._failure_chains[chain_id].append({
            "step": step,
            "module": module,
            "error": error,
            "upstream": upstream,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def chain_summary(self, chain_id: str) -> list[dict[str, Any]]:
        return self._failure_chains.get(chain_id, [])

    def recovery_summary(self) -> dict[str, Any]:
        completed = sum(1 for cp in self._checkpoints.values() if cp["completed"])
        pending = len(self._checkpoints) - completed
        total_retries = sum(len(logs) for logs in self._retry_logs.values())
        total_chains = len(self._failure_chains)
        return {
            "checkpoints": {"total": len(self._checkpoints), "completed": completed, "pending": pending},
            "retries": total_retries,
            "failure_chains": total_chains,
        }


class SnapshotEngine:
    """版本快照引擎：创建、比较和管理数据快照。"""

    def __init__(self) -> None:
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._diff_logs: dict[str, dict[str, Any]] = {}

    def create_snapshot(self, module: str, entry_id: int, data: dict[str, Any], version_label: str = "") -> str:
        """创建数据快照，返回快照ID。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-v{version_label}-{uuid.uuid4().hex[:8]}"
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": None,
            "retention_policy": "keep_forever",
        }
        return snap_id

    def create_version_snapshot(self, module: str, entry_id: int, data: dict[str, Any],
                                version_label: str, parent_snap_id: str | None = None) -> str:
        """创建带父子关系的版本快照，用于模型发布版本管理。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-{version_label}-{uuid.uuid4().hex[:8]}"
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": parent_snap_id,
            "retention_policy": "keep_forever",
        }
        return snap_id

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshots.get(snap_id)

    def list_snapshots(self, module: str, entry_id: int) -> list[dict[str, Any]]:
        return [
            s for s in self._snapshots.values()
            if s["module"] == module and s["entry_id"] == entry_id
        ]

    def diff_snapshots(self, snap_id_a: str, snap_id_b: str) -> dict[str, Any] | None:
        snap_a = self._snapshots.get(snap_id_a)
        snap_b = self._snapshots.get(snap_id_b)
        if not snap_a or not snap_b:
            return None
        keys_a = set(snap_a["data"])
        keys_b = set(snap_b["data"])
        added = {k: snap_b["data"][k] for k in keys_b - keys_a}
        removed = {k: snap_a["data"][k] for k in keys_a - keys_b}
        modified = {}
        for k in keys_a & keys_b:
            if snap_a["data"][k] != snap_b["data"][k]:
                modified[k] = {"old": snap_a["data"][k], "new": snap_b["data"][k]}
        diff_id = f"diff-{uuid.uuid4().hex[:8]}"
        result = {
            "diff_id": diff_id,
            "snap_a": snap_id_a,
            "snap_b": snap_id_b,
            "added_fields": added,
            "removed_fields": removed,
            "modified_fields": modified,
            "total_changes": len(added) + len(removed) + len(modified),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._diff_logs[diff_id] = result
        return result

    def rollback_to_snapshot(self, module: str, entry_id: int, snap_id: str, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        """将数据回滚到指定快照版本。"""
        snap = self._snapshots.get(snap_id)
        if not snap:
            return None
        for row in rows:
            if row.get("id") == entry_id:
                for k, v in snap["data"].items():
                    row[k] = v
                row["_rollback_to"] = snap_id
                row["_rollback_at"] = datetime.now(timezone.utc).isoformat()
                return row
        return None

    def snapshot_summary(self) -> dict[str, Any]:
        by_module: dict[str, int] = {}
        for s in self._snapshots.values():
            by_module[s["module"]] = by_module.get(s["module"], 0) + 1
        return {"total_snapshots": len(self._snapshots), "by_module": by_module, "total_diffs": len(self._diff_logs)}


class ArtifactRegistry:
    """数据产物注册表：追踪各模块产出的数据工件。"""

    def __init__(self) -> None:
        self._artifacts: dict[str, dict[str, Any]] = {}

    def register(self, module: str, artifact_type: str, artifact_id: str,
                 metadata: dict[str, Any], source_modules: list[str] | None = None) -> str:
        """注册数据产物，返回注册ID。"""
        reg_id = f"{module}-{artifact_type}-{artifact_id}"
        self._artifacts[reg_id] = {
            "reg_id": reg_id,
            "module": module,
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "metadata": dict(metadata),
            "source_modules": source_modules or [module],
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
        }
        return reg_id

    def get_artifact(self, reg_id: str) -> dict[str, Any] | None:
        return self._artifacts.get(reg_id)

    def list_by_module(self, module: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["module"] == module]

    def mark_consumed(self, reg_id: str, consumer: str) -> bool:
        a = self._artifacts.get(reg_id)
        if a is None:
            return False
        a["consumed_by"].append(consumer)
        return True

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._artifacts.values())

    def by_type(self, artifact_type: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["artifact_type"] == artifact_type]

    def artifact_summary(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_module: dict[str, int] = {}
        for a in self._artifacts.values():
            by_type[a["artifact_type"]] = by_type.get(a["artifact_type"], 0) + 1
            by_module[a["module"]] = by_module.get(a["module"], 0) + 1
        return {"total": len(self._artifacts), "by_type": by_type, "by_module": by_module}


def _ap_values_in(batch: dict[str, Any], lo: int, hi: int):
    """枚举批次原始记录键道在闭区间 [lo, hi] 内的编号。"""
    start, end, step = int(batch["start"]), int(batch["end"]), int(batch["step"])
    lo = max(lo, start)
    hi = min(hi, end)
    if lo > hi or step <= 0:
        return
    offset = (lo - start + step - 1) // step
    value = start + offset * step
    while value <= hi:
        yield value
        value += step


def _range_intersection(a: dict[str, Any], b: dict[str, Any]) -> list[int]:
    """两个批次原始记录区间共同引用的编号（按各自步长对齐）。"""
    lo = max(int(a["start"]), int(b["start"]))
    hi = min(int(a["end"]), int(b["end"]))
    if lo > hi:
        return []
    return sorted(set(_ap_values_in(a, lo, hi)) & set(_ap_values_in(b, lo, hi)))


class KeyBatchRegistry:
    """代理键键段批次登记册：记录每次键段分配批次，判定编号归属并处置历史重叠。

    - 每个批次的有效键域为闭区间 [首键, 首键 + (可分配总数 - 1) * 步长]，
      编号按步长对齐；
    - 历史缺陷批次的记录末键可能多出一个步长，归属判定以修正后的有效键域为准；
    - 同一边界编号被两个批次引用时，只有有效键域覆盖它的批次拥有该编号，
      另一批次已写入的事实需结合原分配批次与自然键重关联；
    - 批次内编号确认给唯一成员（自然键），不同成员的键不能共享。
    """

    def __init__(self) -> None:
        self._batches: list[dict[str, Any]] = []
        # batch_id -> {编号: 自然键}，批次内编号已确认给哪个成员
        self._assignments: dict[str, dict[int, str]] = {}

    # ---------- 批次登记 ----------

    def register(self, *, key_id: int, key_name: str, dim_code: str,
                 start: int, end: int, step: int, count: int) -> dict[str, Any]:
        """登记一个分配批次，批次号在同一键值定义下递增。"""
        batch_no = 1 + sum(1 for b in self._batches if b["key_id"] == key_id)
        batch = {
            "batch_id": f"sk{key_id}-b{batch_no}",
            "key_id": key_id,
            "key_name": key_name,
            "dim_code": dim_code,
            "batch_no": batch_no,
            "start": int(start),
            "end": int(end),
            "step": int(step),
            "count": int(count),
            "allocated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._batches.append(batch)
        return batch

    def clear(self, key_id: int) -> int:
        """重置键值时清除该键值定义的全部批次与成员归属记录，返回清除批次数。"""
        removed = [b for b in self._batches if b["key_id"] == key_id]
        if removed:
            self._batches = [b for b in self._batches if b["key_id"] != key_id]
            for batch in removed:
                self._assignments.pop(batch["batch_id"], None)
        return len(removed)

    def get(self, batch_id: str) -> dict[str, Any] | None:
        for batch in self._batches:
            if batch["batch_id"] == batch_id:
                return batch
        return None

    def batches_for(self, key_id: int | None = None,
                    dim_code: str | None = None) -> list[dict[str, Any]]:
        """按键值定义或维度编码查询批次，按批次号排序。"""
        rows = list(self._batches)
        if key_id is not None:
            rows = [b for b in rows if b["key_id"] == key_id]
        if dim_code is not None:
            rows = [b for b in rows if b["dim_code"] == dim_code]
        return sorted(rows, key=lambda b: (b["key_id"], b["batch_no"]))

    # ---------- 端点与归属判定 ----------

    @staticmethod
    def valid_end(batch: dict[str, Any]) -> int:
        """修正后的末键：首键 + (可分配总数 - 1) * 步长。"""
        return int(batch["start"]) + (int(batch["count"]) - 1) * int(batch["step"])

    def valid_range(self, batch: dict[str, Any]) -> tuple[int, int, int]:
        """批次的 (首键, 修正末键, 步长)。"""
        return int(batch["start"]), self.valid_end(batch), int(batch["step"])

    def owns(self, batch: dict[str, Any], value: int) -> bool:
        """编号是否落在批次修正后的有效键域内（闭区间且按步长对齐）。"""
        start, end, step = self.valid_range(batch)
        return step > 0 and start <= value <= end and (value - start) % step == 0

    def references(self, batch: dict[str, Any], value: int) -> bool:
        """编号是否落在批次原始记录区间内（含历史缺陷多出的边界编号）。"""
        start, end, step = int(batch["start"]), int(batch["end"]), int(batch["step"])
        return step > 0 and start <= value <= end and (value - start) % step == 0

    def referencing_batches(self, dim_code: str, value: int) -> list[dict[str, Any]]:
        """原始记录引用了该编号的全部批次，按批次号排序。"""
        return [b for b in self.batches_for(dim_code=dim_code) if self.references(b, value)]

    def attribute(self, dim_code: str, value: int) -> dict[str, Any] | None:
        """编号归属：修正后有效键域内持有该编号的唯一批次。

        同一边界编号被两个批次引用时，仅有效键域覆盖它的批次拥有该编号，
        保证有效编号不会同时指向多个批次。
        """
        owners = [b for b in self.batches_for(dim_code=dim_code) if self.owns(b, value)]
        if not owners:
            return None
        return owners[0]

    def overlap_keys(self, dim_code: str | None = None) -> list[dict[str, Any]]:
        """检测被多个批次原始记录同时引用的编号（历史重叠边界）。

        返回每个重叠编号、引用它的批次列表以及修正后的归属批次。
        """
        batches = self.batches_for(dim_code=dim_code)
        result: list[dict[str, Any]] = []
        for dim in sorted({b["dim_code"] for b in batches}):
            dim_batches = [b for b in batches if b["dim_code"] == dim]
            hits: dict[int, set[str]] = {}
            for index, batch in enumerate(dim_batches):
                for other in dim_batches[index + 1:]:
                    for value in _range_intersection(batch, other):
                        hits.setdefault(value, set()).update(
                            (batch["batch_id"], other["batch_id"]))
            for value in sorted(hits):
                owner = self.attribute(dim, value)
                result.append({
                    "dim_code": dim,
                    "value": value,
                    "referenced_by": sorted(hits[value]),
                    "owner_batch": owner["batch_id"] if owner else None,
                })
        return result

    # ---------- 成员归属 ----------

    def assign(self, batch_id: str, value: int, natural_key: str) -> tuple[bool, str]:
        """在批次内把编号确认给指定自然键成员。

        编号必须落在该批次修正后的有效键域内；同一批次内编号已确认给
        其他成员时拒绝——不同成员的键不能共享。
        """
        batch = self.get(batch_id)
        if batch is None:
            return False, f"分配批次 {batch_id} 不存在"
        if not self.owns(batch, value):
            return False, f"编号 {value} 不在批次 {batch_id} 的有效键域内"
        members = self._assignments.setdefault(batch_id, {})
        holder = members.get(value)
        if holder is not None and holder != natural_key:
            return False, f"编号 {value} 已确认给成员 {holder}，不能共享"
        members[value] = natural_key
        return True, ""

    def holder(self, batch_id: str, value: int) -> str | None:
        """编号在该批次内确认给的成员自然键。"""
        return self._assignments.get(batch_id, {}).get(value)

    def release(self, batch_id: str, value: int, natural_key: str) -> bool:
        """解除批次内编号与成员的确认关系（重关联时回收旧编号）。"""
        members = self._assignments.get(batch_id, {})
        if members.get(value) == natural_key:
            del members[value]
            return True
        return False

    def next_free_key(self, batch_id: str) -> int | None:
        """批次有效键域内第一个未确认的编号，耗尽时返回 None。"""
        batch = self.get(batch_id)
        if batch is None:
            return None
        start, end, step = self.valid_range(batch)
        used = self._assignments.get(batch_id, {})
        value = start
        while value <= end:
            if value not in used:
                return value
            value += step
        return None

    def assignments(self, batch_id: str) -> dict[int, str]:
        """批次内已确认的 编号 -> 自然键 映射。"""
        return dict(self._assignments.get(batch_id, {}))


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self.channel = DataChannel()
        self.recovery = FailureRecovery()
        self.snapshot = SnapshotEngine()
        self.artifacts = ArtifactRegistry()
        self.key_batches = KeyBatchRegistry()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def insert(self, module: str, entry: dict[str, Any]) -> None:
        self.rows(module).append(entry)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        channel_stats = self.channel.topic_stats()
        recovery = self.recovery.recovery_summary()
        snapshot_info = self.snapshot.snapshot_summary()
        artifact_info = self.artifacts.artifact_summary()
        return {
            "cards": cards,
            "modules": modules,
            "data_channels": {"topics": len(channel_stats), "stats": channel_stats},
            "recovery": recovery,
            "snapshots": snapshot_info,
            "artifacts": artifact_info,
        }


store = Store()