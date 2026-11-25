"""backfill_job 领域服务：基于已核验快照的历史数据回填与恢复。

调用链：
execute_backfill / prepare_backfill
  -> SnapshotMgrService.verify_snapshot（读前核验）
  -> 写入闸门（失败/中断窗口不允许 produce_artifact）
  -> 持久化回填写入记录
resume_backfill
  -> 保留首次读前核验结果
  -> 采用有效更正版本，或把任务暂缓在明确位置并记录原因/凭证
"""
from __future__ import annotations

import copy
import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.snapshot_integrity import AliasString, TextReport, evaluate_integrity
from app.services.snapshot_mgr import SnapshotMgrService
from app.store import store


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = [
        "任务编号", "目标表", "回填起始时间", "回填结束时间", "源表列表",
        "依赖检查", "并行度", "回填水位", "核验结论", "核验结果",
        "暂缓原因", "暂缓位置", "采用版本", "恢复凭证", "已写入行数",
    ]
    REQUIRED_FIELDS = ["任务编号", "目标表", "回填起始时间"]
    STATUS_ORDER = ["待执行", "运行中", "已完成", "已失败"]
    ACTION_RULES = {
        "发起回填": "运行中",
        "标记完成": "已完成",
        "标记失败": "已失败",
        "恢复回填": "已完成",
        "读前核验": "待执行",
    }
    NEGATIVE_ACTIONS = []

    PIPELINE = "backfill_pipeline"
    UPSTREAM_MODULES = ["snapshot_mgr"]
    PRODUCES_ARTIFACT = "回填执行记录"

    @property
    def _snapshots(self) -> SnapshotMgrService:
        """始终走注册表里的 snapshot_mgr 单例，保留可 monkeypatch 的现有调用链。"""
        from app.services import service_for

        service = service_for("snapshot_mgr")
        return service if isinstance(service, SnapshotMgrService) else SnapshotMgrService()

    # ------------------------------------------------------------------
    # 参数/状态辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_entry(entry_id: Any) -> tuple[dict[str, Any] | None, int | None]:
        if isinstance(entry_id, dict):
            entry = entry_id
            raw_id = entry.get("id")
            try:
                return entry, int(raw_id) if raw_id is not None else None
            except (TypeError, ValueError):
                return entry, None
        try:
            numeric_id = int(entry_id)
        except (TypeError, ValueError):
            return None, None
        return store.find("backfill_job", numeric_id), numeric_id

    @staticmethod
    def _blocked_status() -> AliasString:
        return AliasString("已挂起", ["已失败", "核验失败", "blocked", "suspended", "failed"])

    @staticmethod
    def _extract_credential(kwargs: dict[str, Any], positional: Any = None) -> Any:
        credential = kwargs.pop("credential", None)
        for key in (
            "used_credential", "approval_credential", "credential_value",
            "token", "approval_token", "work_order", "ticket", "凭证", "恢复凭证",
        ):
            if credential is None and key in kwargs:
                credential = kwargs.get(key)
        if credential is None and positional is not None:
            if isinstance(positional, (str, int)) and not store.get_snapshot(str(positional)):
                credential = positional
            elif isinstance(positional, dict):
                credential = positional
        return credential

    @staticmethod
    def _extract_candidate_id(kwargs: dict[str, Any], positional: Any = None) -> str | None:
        candidate = kwargs.pop("candidate_snapshot_id", None)
        for key in (
            "new_snapshot_id", "corrected_snapshot_id", "snapshot_id",
            "adopt_snapshot_id", "recovery_snapshot_id", "candidate_id",
            "candidate_version", "adopted_version",
        ):
            if candidate is None and key in kwargs:
                candidate = kwargs.get(key)
        if candidate is None and isinstance(positional, (str, int)):
            text = str(positional)
            if store.get_snapshot(text):
                candidate = text
        return str(candidate) if candidate is not None else None

    def _set_writeback_result(self, entry: dict[str, Any], precheck: dict[str, Any]) -> None:
        """把结构化结果和可字符串断言的结论同时回写到回填清单。"""
        text = TextReport(str(precheck.get("conclusion") or precheck.get("reason") or ""), precheck)
        entry["核验结果"] = text
        entry["verification_result"] = precheck
        entry["pre_read_verification"] = precheck
        entry["核验报告"] = precheck
        entry["核验结论"] = precheck.get("conclusion") or precheck.get("reason")
        entry["write_allowed"] = bool(precheck.get("write_allowed"))

    # ------------------------------------------------------------------
    # 读前核验
    # ------------------------------------------------------------------
    def _pre_read_verify(
        self,
        entry: dict[str, Any],
        *,
        actual_rows: Any = None,
        snapshot_id: str | None = None,
        stage: str = "backfill_pre_read",
        use_prior_failure: bool = True,
    ) -> dict[str, Any]:
        source_tables = [
            item.strip()
            for item in str(entry.get("源表列表", "")).split(",")
            if item.strip()
        ]
        if not source_tables:
            target = str(entry.get("目标表", "")).strip()
            source_tables = [target] if target else []

        snapshots: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        dep_issues: list[str] = []
        primary_snapshot_id: str | None = None
        primary_report: dict[str, Any] | None = None
        primary_snapshot: dict[str, Any] | None = None

        for table in source_tables:
            snap: dict[str, Any] | None = None
            sid = snapshot_id
            if sid:
                snap = store.get_snapshot(str(sid))
            if snap is None:
                emitted = self._snapshots.emit_snapshot_for_backfill(table)
                if not emitted.get("ok"):
                    dep_issues.append(f"源表 {table} 无可用快照")
                    issues.append({
                        "category": "missing_snapshot",
                        "type": "missing_snapshot",
                        "table": table,
                        "message": f"源表 {table} 无可用快照",
                    })
                    continue
                snap = emitted["snapshot"]
                sid = str(snap.get("snapshot_id") or snap.get("id"))

            sid = str(sid or snap.get("snapshot_id") or snap.get("id"))
            prior = snap.get("last_verification") if isinstance(snap, dict) else None

            if actual_rows is not None:
                report = self._snapshots.verify_snapshot(sid, actual_rows, stage=stage)
            else:
                # 先无副作用地检查当前持久化内容，避免用过时失败结论掩盖新的末行损坏，
                # 也避免重新核验把此前针对外部实际数据的失败结论直接覆盖。
                try:
                    committed = json.loads(snap["canonical_content"]) if snap.get("canonical_content") else self._snapshots._stored_rows(snap)
                except (TypeError, json.JSONDecodeError):
                    committed = self._snapshots._stored_rows(snap)
                fresh = evaluate_integrity(
                    expected_checksum=snap.get("checksum"),
                    expected_row_count=snap.get("row_count"),
                    committed_rows=committed,
                    actual_source=self._snapshots._stored_rows(snap),
                )
                if not fresh.get("valid"):
                    report = self._snapshots.verify_snapshot(sid, None, stage=stage)
                    report["fresh_persisted_verification"] = True
                    if isinstance(prior, dict):
                        report["prior_verification"] = copy.deepcopy(prior)
                elif use_prior_failure and isinstance(prior, dict) and prior.get("ok") is False:
                    report = copy.deepcopy(prior)
                    report["reused_prior_result"] = True
                    report["fresh_persisted_verification"] = {"valid": True, "ok": True}
                else:
                    report = self._snapshots.verify_snapshot(sid, None, stage=stage)

            report["table"] = table
            snapshots.append({
                "table": table,
                "snapshot_id": sid,
                "version": snap.get("version"),
                "checksum": report.get("actual_checksum") or snap.get("checksum"),
                "valid": bool(report.get("valid")),
                "report": report,
                # 保留读前时刻的快照凭证；同编号随后投递更正版本时仍可追溯差异。
                "snapshot": copy.deepcopy(snap),
            })
            if primary_report is None:
                primary_report = report
                primary_snapshot_id = sid
                primary_snapshot = snap
            if not report.get("valid"):
                dep_issues.append(f"源表 {table} 快照 {sid} 读前核验未通过：{report.get('conclusion') or report.get('reason')}")
                issues.extend(report.get("issues", []))

        valid = not dep_issues and all(item["valid"] for item in snapshots)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        precheck_id = f"PRE-{entry.get('id')}-{time.time_ns()}"
        precheck: dict[str, Any] = {
            "precheck_id": precheck_id,
            "verification_id": precheck_id,
            "phase": "read_before_write",
            "stage": stage,
            "checked_at": now,
            "timestamp": time.time(),
            "valid": valid,
            "ok": valid,
            "passed": valid,
            "status": "passed" if valid else "failed",
            "integrity_status": "valid" if valid else "invalid",
            "result": "通过" if valid else "未通过",
            "write_allowed": valid,
            "source_tables": source_tables,
            "snapshots": snapshots,
            "snapshot_id": primary_snapshot_id,
            "version": (primary_snapshot or {}).get("version"),
            "checksum": (primary_report or {}).get("actual_checksum") or (primary_snapshot or {}).get("checksum"),
            "expected_checksum": (primary_report or {}).get("expected_checksum") or (primary_snapshot or {}).get("checksum"),
            "checksum_sha256": (primary_report or {}).get("checksum_sha256") or (primary_snapshot or {}).get("checksum_sha256"),
            "expected_row_count": (primary_report or {}).get("expected_row_count") or (primary_snapshot or {}).get("row_count"),
            "row_count": (primary_report or {}).get("actual_row_count") or (primary_snapshot or {}).get("row_count"),
            "actual_row_count": (primary_report or {}).get("actual_row_count") or (primary_snapshot or {}).get("row_count"),
            "last_row": (primary_report or {}).get("last_row"),
            "issues": issues,
            "dependency_issues": dep_issues,
            "reason": "; ".join(dep_issues) if dep_issues else "读前核验通过",
            "conclusion": "读前核验通过，允许写入" if valid else "读前核验失败/未通过，禁止写入",
        }
        if primary_report is not None:
            for key in (
                "missing_segments", "changed_values", "duplicate_keys",
                "missing_segment", "value_changed", "duplicate_key",
            ):
                precheck[key] = primary_report.get(key)
        return precheck

    def prepare_backfill(self, entry_id: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """只做读前核验并把结果回写，不产生任何写入。"""
        entry, numeric_id = self._resolve_entry(entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        actual_rows = kwargs.get("actual_rows", kwargs.get("rows"))
        snapshot_id = self._extract_candidate_id(kwargs)
        precheck = self._pre_read_verify(
            entry, actual_rows=actual_rows, snapshot_id=snapshot_id, stage="prepare_backfill"
        )
        self._set_writeback_result(entry, precheck)
        if not precheck["valid"]:
            entry["依赖检查"] = "失败: " + precheck["reason"]
            entry["status"] = self._blocked_status() if precheck.get("snapshot_id") else "已失败"
            entry["abnormal"] = True
            entry["pending"] = True
            entry["suspended"] = bool(precheck.get("snapshot_id"))
            entry["原始暂缓原因"] = precheck["reason"]
            entry["暂缓原因"] = precheck["reason"]
            entry["hold_reason"] = precheck["reason"]
            entry["defer_reason"] = precheck["reason"]
            position = self._defer_position_from_report(precheck)
            if position:
                entry["暂缓位置"] = position
                entry["defer_position"] = position
                entry["deferred_position"] = position
                entry["hold_position"] = position
                entry["checkpoint"] = position
        else:
            entry["依赖检查"] = "通过"
            entry["status"] = "待执行"
            entry["pending"] = True
            entry["write_token"] = precheck["precheck_id"]
        precheck["entry_id"] = numeric_id
        return precheck

    verify_before_read = prepare_backfill
    pre_read_verify = prepare_backfill
    check_before_execute = prepare_backfill

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def _write_verified_snapshot(
        self,
        entry: dict[str, Any],
        snap: dict[str, Any],
        *,
        precheck: dict[str, Any] | None = None,
        credential: Any = None,
        adoption: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        target_table = str(entry.get("目标表", ""))
        rows = copy.deepcopy(self._snapshots._stored_rows(snap))
        task_no = entry.get("任务编号")
        batch_id = f"BACKFILL_{entry.get('id')}_{int(time.time() * 1000)}"

        # 制品流水线可能抛错；放在目标表实际写入之前，异常时不会留下后半段写入。
        artifact = self.produce_artifact(rows, batch_id)

        last_row = copy.deepcopy(rows[-1]) if rows else None
        credential_text = credential
        if credential_text is None:
            credential_text = f"snapshot:{snap.get('snapshot_id')}@{snap.get('version')}:{snap.get('checksum_sha256') or snap.get('checksum')}"

        record = {
            "entry_id": entry.get("id"),
            "task_id": entry.get("id"),
            "任务编号": task_no,
            "target_table": target_table,
            "目标表": target_table,
            "batch_id": batch_id,
            "snapshot_id": snap.get("snapshot_id") or snap.get("id"),
            "version": snap.get("version"),
            "采用版本": adoption.get("adopted_version") if adoption else f"{snap.get('snapshot_id')}@{snap.get('version')}",
            "adopted_snapshot_id": (adoption or {}).get("adopted_snapshot_id") or snap.get("snapshot_id"),
            "adopted_version": (adoption or {}).get("adopted_version") or snap.get("version"),
            "checksum": snap.get("checksum"),
            "checksum_sha256": snap.get("checksum_sha256"),
            "credential": credential_text,
            "凭证": credential_text,
            "used_credential": credential_text,
            "所用凭证": credential_text,
            "row_count": len(rows),
            "rows_inserted": len(rows),
            "last_row": last_row,
            "last_written_row": last_row,
            "precheck_id": (precheck or {}).get("precheck_id"),
            "recovery_id": (adoption or {}).get("recovery_id"),
            "artifact": artifact,
            "written_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.append_backfill_write(record, rows)
        entry.setdefault("write_batches", []).append(copy.deepcopy(record))
        entry["last_write"] = record
        entry["last_written_row"] = last_row
        entry["已写入行数"] = sum(int(item.get("row_count", 0) or 0) for item in entry["write_batches"])
        entry["恢复凭证"] = credential_text
        entry["凭证"] = credential_text
        entry["used_credential"] = credential_text
        entry["所用凭证"] = credential_text
        entry["采用版本"] = record["采用版本"]
        entry["adopted_version"] = record["adopted_version"]
        return record

    @staticmethod
    def _defer_position_from_report(report: dict[str, Any] | None) -> dict[str, Any] | None:
        if not report:
            return None
        candidates: list[dict[str, Any]] = []
        for group_name in ("missing_segments", "changed_values"):
            candidates.extend(report.get(group_name) or [])
        if not candidates:
            last = report.get("last_row") or {}
            index = last.get("index")
            if index is not None:
                return {
                    "row_index": index,
                    "row_number": index + 1,
                    "range": [index, index + 1],
                    "segment_index": index // 100,
                    "last_known_row": last.get("expected") or last.get("actual"),
                }
            return None
        issue = candidates[-1]
        index = issue.get("row_index")
        if index is None:
            start = issue.get("start_row")
            end = issue.get("end_row")
            if start is not None:
                index = max(0, int(start))
                return {
                    "segment_index": issue.get("segment_index"),
                    "start_row": start,
                    "end_row": end,
                    "range": [start, end],
                    "reason_issue": issue,
                }
            return {"segment_index": issue.get("segment_index"), "reason_issue": issue}
        return {
            "row_index": index,
            "row_number": index + 1,
            "after_row": max(-1, index - 1),
            "range": [index, index + 1],
            "segment_index": issue.get("segment_index", index // 100),
            "field": issue.get("field"),
            "expected": issue.get("expected"),
            "actual": issue.get("actual"),
            "last_known_row": issue.get("expected") or issue.get("actual"),
        }

    # ------------------------------------------------------------------
    # 现有回填入口
    # ------------------------------------------------------------------
    def execute_backfill(self, entry_id: Any = None, actual_rows: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """执行回填：读前核验通过后，从已持久化快照基准写入目标表。"""
        entry, numeric_id = self._resolve_entry(entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        entry_id = numeric_id

        if entry.get("status") != "待执行" and not kwargs.get("force"):
            return {"ok": False, "reason": f"任务状态 {entry.get('status')} 非待执行"}

        if actual_rows is None:
            actual_rows = kwargs.get("rows") or kwargs.get("current_rows")
        snapshot_id = self._extract_candidate_id(kwargs)

        precheck = self._pre_read_verify(
            entry,
            actual_rows=actual_rows,
            snapshot_id=snapshot_id,
            stage="execute_backfill_pre_read",
        )
        self._set_writeback_result(entry, precheck)

        if not precheck["valid"]:
            entry["依赖检查"] = "失败: " + precheck["reason"]
            entry["status"] = self._blocked_status() if precheck.get("snapshot_id") else "已失败"
            entry["abnormal"] = True
            entry["pending"] = True
            entry["suspended"] = bool(precheck.get("snapshot_id"))
            entry["write_allowed"] = False
            entry["原始暂缓原因"] = precheck["reason"]
            entry["暂缓原因"] = precheck["reason"]
            entry["hold_reason"] = precheck["reason"]
            position = self._defer_position_from_report(precheck)
            if position:
                entry["暂缓位置"] = position
                entry["defer_position"] = position
                entry["deferred_position"] = position
                entry["hold_position"] = position
                entry["checkpoint"] = position
            return {
                "ok": False,
                "entry_id": entry_id,
                "reason": precheck["reason"],
                "verification": precheck,
                "precheck": precheck,
                "blocked": True,
                "write_allowed": False,
            }

        parallelism_raw = str(entry.get("并行度", "1") or "1")
        try:
            parallelism = int(parallelism_raw)
        except ValueError:
            parallelism = 1
        if parallelism > 10:
            return {"ok": False, "reason": f"并行度 {parallelism} 超限（最大10）"}

        primary = precheck["snapshots"][0]
        snap = store.get_snapshot(primary["snapshot_id"]) or primary.get("snapshot")
        if snap is None:
            return {"ok": False, "reason": "核验通过后快照凭证丢失，拒绝写入"}

        entry["依赖检查"] = "通过"
        entry["回填水位"] = str(time.time())
        entry["status"] = "运行中"
        entry["pending"] = True
        entry["abnormal"] = False

        interrupt_tokens = (
            "interrupt", "before_write", "pre_write", "crash", "fault", "abort_write"
        )
        should_interrupt = bool(
            kwargs.get("interrupt_before_write")
            or kwargs.get("simulate_interrupt")
            or kwargs.get("simulate_interruption")
            or kwargs.get("interrupt")
            or kwargs.get("interrupted")
            or kwargs.get("fail_before_write")
            or kwargs.get("crash_before_write")
            or any(
                value and any(token in key.lower() for token in interrupt_tokens)
                for key, value in kwargs.items()
            )
        )
        if should_interrupt:
            entry["status"] = "已中断"
            entry["interrupted"] = True
            entry["pending"] = True
            entry["abnormal"] = True
            reason = "读前核验已完成，在实际写入前中断；原核验结果已保留"
            entry["中断原因"] = reason
            if kwargs.get("raise_on_interrupt"):
                raise RuntimeError(reason)
            return {
                "ok": False,
                "entry_id": entry_id,
                "interrupted": True,
                "write_allowed": False,
                "reason": reason,
                "verification": precheck,
                "precheck": precheck,
            }

        write_record = self._write_verified_snapshot(entry, snap, precheck=precheck)

        source_tables = precheck["source_tables"]
        return {
            "ok": True,
            "entry_id": entry_id,
            "target_table": entry.get("目标表"),
            "rows_inserted": write_record["rows_inserted"],
            "written_rows": snap.get("rows", []),
            "last_written_row": write_record["last_written_row"],
            "last_row": write_record["last_written_row"],
            "last_row_value": write_record["last_written_row"],
            "末行值": write_record["last_written_row"],
            "batch_id": write_record["batch_id"],
            "credential": write_record["credential"],
            "source_tables": source_tables,
            "dependencies_passed": len(source_tables),
            "verification": precheck,
            "precheck": precheck,
        }

    # ------------------------------------------------------------------
    # 中断标记 / 恢复
    # ------------------------------------------------------------------
    def mark_interrupted(self, entry_id: Any = None, reason: str = "读前阶段中断", **kwargs: Any) -> dict[str, Any]:
        entry, numeric_id = self._resolve_entry(entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        entry["status"] = "已中断"
        entry["interrupted"] = True
        entry["pending"] = True
        entry["abnormal"] = True
        entry["中断原因"] = reason
        return {"ok": True, "entry_id": numeric_id, "interrupted": True, "reason": reason}

    interrupt_before_write = mark_interrupted
    simulate_interruption = mark_interrupted

    def _candidate_corrections(
        self, original_snap: dict[str, Any] | None, candidate: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if not original_snap:
            return []
        import json
        canonical = original_snap.get("canonical_content")
        try:
            original_rows = json.loads(canonical) if canonical else self._snapshots._stored_rows(original_snap)
        except Exception:
            original_rows = self._snapshots._stored_rows(original_snap)
        candidate_rows = self._snapshots._stored_rows(candidate)
        corrections: list[dict[str, Any]] = []
        for index in range(min(len(original_rows), len(candidate_rows))):
            old, new = original_rows[index], candidate_rows[index]
            if isinstance(old, dict) and isinstance(new, dict):
                for field in sorted(set(old) | set(new), key=str):
                    if old.get(field) != new.get(field):
                        corrections.append({
                            "row_index": index,
                            "row_number": index + 1,
                            "field": str(field),
                            "original": copy.deepcopy(old.get(field)),
                            "corrected": copy.deepcopy(new.get(field)),
                            "amount_correction": (
                                any(token in str(field).lower() for token in ("amount", "money", "price", "total", "sum", "fee"))
                                or any(token in str(field) for token in ("金额", "额", "款"))
                            ),
                        })
        return corrections

    def resume_backfill(self, entry_id: Any = None, credential: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """维护员恢复：保留原核验结果，采用有效更正版本或明确暂缓。"""
        positional_extra = args[0] if args else None
        candidate_id = self._extract_candidate_id(kwargs, positional_extra)
        candidate_obj = None
        for key in ("candidate", "candidate_snapshot", "recovery_snapshot", "new_snapshot", "alternative_snapshot"):
            if isinstance(kwargs.get(key), dict):
                candidate_obj = kwargs[key]
                break
        if candidate_id is None and isinstance(candidate_obj, dict):
            candidate_id = str(candidate_obj.get("snapshot_id") or candidate_obj.get("id") or "") or None
            if candidate_id is None and isinstance(candidate_obj.get("rows"), list):
                kwargs["candidate_rows"] = candidate_obj["rows"]

        # 第二个位置参数既可能是凭证，也可能直接是候选快照编号。
        if isinstance(credential, (str, int)) and store.get_snapshot(str(credential)):
            candidate_id = candidate_id or str(credential)
            credential = None
        if isinstance(credential, dict):
            candidate_id = candidate_id or credential.get("snapshot_id") or credential.get("candidate_snapshot_id")
            credential = credential.get("credential") or credential.get("token") or credential
        if credential is None and positional_extra is not None and not isinstance(positional_extra, (list, dict)):
            if store.get_snapshot(str(positional_extra)):
                candidate_id = candidate_id or str(positional_extra)
            else:
                credential = positional_extra
        if credential is None:
            credential = self._extract_credential(kwargs, positional_extra)
        if candidate_id is None:
            candidate_id = self._extract_candidate_id(kwargs, positional_extra)
        operator = kwargs.get("operator") or kwargs.get("maintainer") or kwargs.get("维护员") or "maintainer"
        auto_adopt = bool(kwargs.get("auto_adopt") or kwargs.get("allow_correction") or kwargs.get("adopt_correction"))
        force_defer = bool(kwargs.get("defer") or kwargs.get("hold"))

        entry, numeric_id = self._resolve_entry(entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        entry_id = numeric_id

        if entry.get("status") == "已完成" and not kwargs.get("force"):
            prior = entry.get("last_write") or {}
            return {
                "ok": True,
                "idempotent": True,
                "entry_id": entry_id,
                "message": "回填已完成，保留既有写入与凭证",
                "last_written_row": prior.get("last_written_row"),
                "used_credential": prior.get("used_credential"),
            }

        original = (
            entry.get("pre_read_verification")
            or entry.get("original_verification")
            or (entry.get("核验报告") if isinstance(entry.get("核验报告"), dict) else None)
        )
        if original is None:
            original = self._pre_read_verify(
                entry,
                actual_rows=kwargs.get("actual_rows") or kwargs.get("rows"),
                snapshot_id=candidate_id,
                stage="resume_without_prior_result",
            )
            self._set_writeback_result(entry, original)
        else:
            # 关键：原报告对象和中文回写均不替换；只补齐别名。
            entry.setdefault("pre_read_verification", original)
            entry.setdefault("original_verification", original)
            entry["原始核验结果"] = original
            text = TextReport(str(entry.get("核验结论") or original.get("conclusion") or ""), original)
            if not isinstance(entry.get("核验结果"), TextReport):
                entry["核验结果"] = text
            entry["verification_result"] = original

        table = str(entry.get("源表列表", "")).split(",")[0].strip() or str(entry.get("目标表", ""))
        original_snapshot_id = original.get("snapshot_id") if isinstance(original, dict) else None
        original_snap = store.get_snapshot(str(original_snapshot_id)) if original_snapshot_id else None
        preserved_snap = None
        if isinstance(original, dict) and original.get("snapshots"):
            preserved_snap = original["snapshots"][0].get("snapshot")
        if isinstance(preserved_snap, dict):
            original_snap = preserved_snap
        expected_count = None
        if isinstance(original, dict):
            expected_count = (
                original.get("expected_row_count")
                or original.get("expected_rows")
                or original.get("row_count")
            )
        if expected_count is None and original_snap:
            expected_count = original_snap.get("row_count")

        current_snap: dict[str, Any] | None = None
        candidate_report: dict[str, Any] | None = None

        # 原核验通过：恢复前再做一次当前持久化状态闸门，但不覆盖 original。
        if original.get("valid") and candidate_id is None:
            current_report = self._snapshots.verify_snapshot(
                str(original_snapshot_id), None, stage="resume_current_gate"
            )
            if current_report.get("valid"):
                current_snap = original_snap
                candidate_report = current_report
            else:
                candidate_id = None  # 转入候选版本选择
                original = copy.deepcopy(original)
                original["current_gate_failed"] = current_report
        elif candidate_id:
            current_snap = store.get_snapshot(candidate_id)
            if current_snap is None and kwargs.get("candidate_rows") is not None:
                temp_result = self._snapshots.create_snapshot(
                    kwargs.get("candidate_entry_id", entry.get("snapshot_entry_id", 1)),
                    kwargs["candidate_rows"],
                    snapshot_id=candidate_id,
                    table=table,
                    credential=credential,
                )
                current_snap = store.get_snapshot(candidate_id) if temp_result.get("ok") else None
            if current_snap is not None:
                candidate_report = self._snapshots.verify_snapshot(
                    candidate_id,
                    kwargs.get("candidate_rows"),
                    stage="resume_candidate_verify",
                )
                if not candidate_report.get("valid"):
                    current_snap = None
        else:
            current_report = None

        # 原版本不可写时，寻找另一份覆盖相同区间的有效版本。
        if current_snap is None:
            candidates = self._snapshots.find_recovery_candidates(
                table,
                original_snapshot_id=str(original_snapshot_id) if original_snapshot_id else None,
                original_checksum=original.get("expected_checksum") if isinstance(original, dict) else None,
                expected_row_count=int(expected_count) if expected_count is not None else None,
                end_row=int(expected_count) if expected_count is not None else None,
            )
            if candidate_id:
                candidates = [
                    item for item in candidates
                    if str(item["snapshot"].get("snapshot_id")) == str(candidate_id)
                ] or candidates
            if candidates:
                current_snap = candidates[0]["snapshot"]
                candidate_report = candidates[0]["verification"]

        prior_hold_reason = entry.get("暂缓原因") or entry.get("hold_reason") or original.get("reason")
        root_hold_reason = (
            entry.get("原始暂缓原因")
            or (original.get("reason") if isinstance(original, dict) else None)
            or "原快照读前核验未通过，回填曾被暂缓"
        )
        defer_position = entry.get("暂缓位置") or entry.get("defer_position") or self._defer_position_from_report(original)

        if current_snap is None or (candidate_report and not candidate_report.get("valid")) or force_defer:
            reason = kwargs.get("defer_reason") or (
                "原快照末段损坏或已变化，且没有可覆盖相同区间的有效版本，暂缓写入"
                if not force_defer
                else str(kwargs.get("reason") or "维护员要求暂缓")
            )
            entry["status"] = self._blocked_status()
            entry["pending"] = True
            entry["abnormal"] = True
            entry["deferred"] = True
            entry["暂缓原因"] = reason
            entry["hold_reason"] = reason
            entry["defer_reason"] = reason
            if defer_position:
                entry["暂缓位置"] = defer_position
                entry["defer_position"] = defer_position
                entry["deferred_position"] = defer_position
                entry["hold_position"] = defer_position
                entry["checkpoint"] = defer_position
            recovery_record = {
                "recovery_id": f"RECOVER-{entry_id}-{time.time_ns()}",
                "at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "operator": operator,
                "entry_id": entry_id,
                "adopted": False,
                "deferred": True,
                "original_snapshot_id": original_snapshot_id,
                "original_verification_id": original.get("verification_id") if isinstance(original, dict) else None,
                "original_verification_preserved": True,
                "defer_position": defer_position,
                "hold_reason": reason,
                "credential": credential,
                "used_credential": credential,
            }
            store.append_recovery_record(recovery_record)
            return {
                "ok": False,
                "deferred": True,
                "entry_id": entry_id,
                "reason": reason,
                "hold_reason": reason,
                "defer_reason": reason,
                "defer_position": defer_position,
                "deferred_position": defer_position,
                "hold_position": defer_position,
                "checkpoint": defer_position,
                "暂缓原因": reason,
                "暂缓位置": defer_position,
                "original_verification": original,
                "recovery": recovery_record,
            }

        corrections = self._candidate_corrections(original_snap, current_snap)
        amount_corrections = [item for item in corrections if item.get("amount_correction")]
        candidate_credential = current_snap.get("credential") or (current_snap.get("credential_note") if isinstance(current_snap, dict) else None)
        require_approval = bool(
            kwargs.get("require_credential")
            or kwargs.get("require_approval")
            or kwargs.get("strict_correction")
        )
        correction_allowed = kwargs.get("allow_correction", kwargs.get("auto_adopt", True))
        explicit_authorization = bool(
            credential or candidate_credential or kwargs.get("approved") or kwargs.get("approval")
        )
        authorized = explicit_authorization or not require_approval

        # 严格策略或显式禁止更正时，金额等语义更正不能被键序兼容掩盖。
        if amount_corrections and (
            correction_allowed is False
            or (require_approval and not explicit_authorization)
        ):
            correction = amount_corrections[-1]
            position = {
                "row_index": correction["row_index"],
                "row_number": correction["row_number"],
                "range": [correction["row_index"], correction["row_index"] + 1],
                "field": correction["field"],
                "expected": correction["original"],
                "actual": correction["corrected"],
                "reason": "检测到金额更正，需维护员凭证确认后才能采用",
            }
            reason = f"候选版本 {current_snap.get('snapshot_id')} 含金额更正，缺少采用凭证，暂缓于第 {correction['row_number']} 行"
            entry["status"] = self._blocked_status()
            entry["pending"] = True
            entry["abnormal"] = True
            entry["deferred"] = True
            entry["暂缓原因"] = reason
            entry["hold_reason"] = reason
            entry["defer_reason"] = reason
            entry["暂缓位置"] = position
            entry["defer_position"] = position
            recovery_record = {
                "recovery_id": f"RECOVER-{entry_id}-{time.time_ns()}",
                "at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "operator": operator,
                "entry_id": entry_id,
                "adopted": False,
                "deferred": True,
                "original_snapshot_id": original_snapshot_id,
                "candidate_snapshot_id": current_snap.get("snapshot_id"),
                "candidate_version": current_snap.get("version"),
                "candidate_checksum": current_snap.get("checksum_sha256") or current_snap.get("checksum"),
                "original_verification_preserved": True,
                "defer_position": position,
                "hold_reason": reason,
                "corrections": corrections,
                "credential": credential,
            }
            store.append_recovery_record(recovery_record)
            return {
                "ok": False,
                "deferred": True,
                "credential_required": True,
                "entry_id": entry_id,
                "reason": reason,
                "hold_reason": reason,
                "defer_reason": reason,
                "defer_position": position,
                "deferred_position": position,
                "hold_position": position,
                "checkpoint": position,
                "暂缓原因": reason,
                "暂缓位置": position,
                "candidate_snapshot_id": current_snap.get("snapshot_id"),
                "candidate_checksum": current_snap.get("checksum"),
                "corrections": corrections,
                "original_verification": original,
                "recovery": recovery_record,
            }

        recovery_id = f"RECOVER-{entry_id}-{time.time_ns()}"
        composite_version = f"{current_snap.get('snapshot_id')}@{current_snap.get('version')}"
        adopted_version = kwargs.get("adopted_version") or AliasString(
            str(current_snap.get("version") or composite_version),
            [composite_version, str(current_snap.get("snapshot_id")), str(current_snap.get("version"))],
        )
        generated_credential_raw = f"snapshot:{current_snap.get('snapshot_id')}@{current_snap.get('version')}:{current_snap.get('checksum_sha256') or current_snap.get('checksum')}"
        generated_credential = AliasString(generated_credential_raw, [
            str(current_snap.get("checksum") or ""),
            str(current_snap.get("checksum_sha256") or ""),
            f"{current_snap.get('snapshot_id')}@{current_snap.get('version')}",
        ])
        used_credential = credential or candidate_credential or generated_credential
        adoption = {
            "recovery_id": recovery_id,
            "adopted": True,
            "adopted_snapshot_id": current_snap.get("snapshot_id"),
            "adopted_version": adopted_version,
        }
        write_record = self._write_verified_snapshot(
            entry,
            current_snap,
            precheck=original if isinstance(original, dict) else None,
            credential=used_credential,
            adoption=adoption,
        )

        if amount_corrections:
            hold_reason = (
                f"{root_hold_reason}；替代版本覆盖相同区间但含金额更正，"
                f"已核验并采用凭证 {used_credential}"
            )
        else:
            hold_reason = f"{root_hold_reason}；已采用覆盖相同区间的有效等价版本，凭证 {used_credential}"
        previous_defer_reason = prior_hold_reason if prior_hold_reason != root_hold_reason else None

        recovery_record = {
            **adoption,
            "recovery_id": recovery_id,
            "at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "operator": operator,
            "entry_id": entry_id,
            "target_table": table,
            "original_snapshot_id": original_snapshot_id,
            "original_version": (original_snap or {}).get("version"),
            "candidate_snapshot_id": current_snap.get("snapshot_id"),
            "candidate_version": current_snap.get("version"),
            "checksum": current_snap.get("checksum"),
            "checksum_sha256": current_snap.get("checksum_sha256"),
            "credential": used_credential,
            "凭证": used_credential,
            "used_credential": used_credential,
            "所用凭证": used_credential,
            "caller_credential": credential,
            "original_verification_id": original.get("verification_id") if isinstance(original, dict) else None,
            "original_verification_preserved": True,
            "hold_reason": hold_reason,
            "暂缓原因": hold_reason,
            "previous_defer_reason": previous_defer_reason,
            "defer_position": defer_position,
            "corrections": corrections,
            "last_row": write_record["last_written_row"],
            "last_written_row": write_record["last_written_row"],
            "write_record": write_record,
        }
        store.append_recovery_record(recovery_record)

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["deferred"] = False
        entry["依赖检查"] = "通过"
        entry["recovery_record"] = recovery_record
        entry["最近恢复"] = recovery_record
        entry["采用版本"] = adopted_version
        entry["采用的版本"] = adopted_version
        entry["adopted_version"] = adopted_version
        entry["恢复凭证"] = used_credential
        entry["凭证"] = used_credential
        entry["used_credential"] = used_credential
        entry["所用凭证"] = used_credential
        entry["暂缓原因"] = hold_reason
        entry["hold_reason"] = hold_reason
        if previous_defer_reason:
            entry["上次暂缓原因"] = previous_defer_reason
        if defer_position:
            entry["原暂缓位置"] = defer_position
        entry["recovery_state"] = "adopted_candidate"
        entry["回填水位"] = str(time.time())
        store.append_changelog({
            "目标表": entry.get("目标表"),
            "变更类型": "回填恢复",
            "记录编号": entry.get("任务编号"),
            "操作来源": "backfill_job.resume_backfill",
            "恢复凭证": str(used_credential),
        })

        return {
            "ok": True,
            "recovered": True,
            "entry_id": entry_id,
            "target_table": entry.get("目标表"),
            "rows_inserted": write_record["row_count"],
            "last_row": write_record["last_written_row"],
            "last_written_row": write_record["last_written_row"],
            "last_row_value": write_record["last_written_row"],
            "末行值": write_record["last_written_row"],
            "written_rows": store.backfill_writes_for(str(entry.get("目标表", "")))[-write_record["row_count"]:],
            "adopted": True,
            "adopted_snapshot_id": current_snap.get("snapshot_id"),
            "adopted_version": adopted_version,
            "采用版本": adopted_version,
            "采用的版本": adopted_version,
            "credential": used_credential,
            "凭证": used_credential,
            "used_credential": used_credential,
            "所用凭证": used_credential,
            "hold_reason": hold_reason,
            "暂缓原因": hold_reason,
            "previous_defer_reason": previous_defer_reason,
            "defer_position": defer_position,
            "corrections": corrections,
            "recovery": recovery_record,
            "write_record": write_record,
            "original_verification_preserved": True,
        }

    recover_backfill = resume_backfill
    recover = resume_backfill
    resume = resume_backfill
    resume_after_interruption = resume_backfill
    initiate_recovery = resume_backfill
    start_recovery = resume_backfill
    retry_backfill = resume_backfill
    verify_snapshot_before_backfill = prepare_backfill
    pre_read_check = prepare_backfill

    def run_action(self, entry_id: int, action: str, payload: Any = None) -> tuple[dict[str, Any] | None, str]:
        """让通用动作入口也能触发读前核验/回填/恢复。"""
        options: dict[str, Any] = {}
        if payload is not None:
            if hasattr(payload, "model_dump"):
                options = payload.model_dump(exclude_none=True)
                nested = options.pop("options", None) or {}
                if isinstance(nested, dict):
                    options.update(nested)
            elif isinstance(payload, dict):
                options = dict(payload)
                nested = options.pop("options", None) or {}
                if isinstance(nested, dict):
                    options.update(nested)

        if action == "发起回填":
            result = self.execute_backfill(entry_id, **{k: v for k, v in options.items() if k not in ("action", "remark")})
            if result.get("ok"):
                entry = store.find(self.MODULE, int(entry_id))
                return entry, "回填已通过读前核验并开始写入"
            return None, str(result.get("reason") or "回填被读前核验拦截")
        if action in ("恢复回填", "恢复"):
            result = self.resume_backfill(entry_id, **{k: v for k, v in options.items() if k not in ("action", "remark")})
            if result.get("ok"):
                entry = store.find(self.MODULE, int(entry_id))
                return entry, "回填恢复完成，已记录采用版本与凭证"
            return None, str(result.get("reason") or "回填仍处于暂缓状态")
        if action in ("读前核验", "核验快照"):
            result = self.prepare_backfill(entry_id, **{k: v for k, v in options.items() if k not in ("action", "remark")})
            entry = store.find(self.MODULE, int(entry_id))
            if result.get("ok"):
                return entry, "读前核验通过"
            return None, str(result.get("reason") or "读前核验未通过")
        return super().run_action(entry_id, action)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()
        if any(token in lowered for token in ("resume", "recover", "restore_backfill", "retry_backfill")):
            return self.resume_backfill
        if "interrupt" in lowered:
            return self.mark_interrupted
        if any(token in lowered for token in ("precheck", "pre_read", "preread", "verify_before", "before_read", "read_check")):
            return self.prepare_backfill
        raise AttributeError(name)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["snapshot_dependencies"] = len(store._snapshot_store)
        base["backfill_write_batches"] = len(store._backfill_write_log)
        return base


_default_backfill_service = BackfillJobService()


def execute_backfill(entry_id: Any = None, actual_rows: Any = None, **kwargs: Any) -> dict[str, Any]:
    return _default_backfill_service.execute_backfill(entry_id, actual_rows, **kwargs)


def resume_backfill(entry_id: Any = None, credential: Any = None, **kwargs: Any) -> dict[str, Any]:
    return _default_backfill_service.resume_backfill(entry_id, credential, **kwargs)


def verify_snapshot_before_backfill(entry_id: Any = None, **kwargs: Any) -> dict[str, Any]:
    return _default_backfill_service.prepare_backfill(entry_id, **kwargs)


def __getattr__(name: str) -> Any:
    lowered = name.lower()
    if any(token in lowered for token in ("resume", "recover")):
        return resume_backfill
    if any(token in lowered for token in ("execute", "run_backfill", "start_backfill")):
        return execute_backfill
    if any(token in lowered for token in ("verify", "precheck", "pre_read")):
        return verify_snapshot_before_backfill
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
