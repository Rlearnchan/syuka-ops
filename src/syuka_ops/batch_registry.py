from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .analysis_pipeline import (
    AnalysisConfig,
    apply_openai_ad_batch_output,
    apply_openai_batch_output,
    fetch_openai_batch,
)
from .config import AppPaths


REGISTRY_FILENAME = "openai_analysis_batches.json"


def registry_path(paths: AppPaths) -> Path:
    return paths.batches_dir / REGISTRY_FILENAME


def load_registry(paths: AppPaths) -> list[dict[str, Any]]:
    target = registry_path(paths)
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def save_registry(paths: AppPaths, rows: list[dict[str, Any]]) -> Path:
    target = registry_path(paths)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def register_submitted_batch(
    paths: AppPaths,
    submission: dict[str, Any],
    *,
    kind: str = "analysis",
    analysis_source: str = "generated_openai_batch",
) -> dict[str, Any]:
    batch_id = str(submission.get("batch_id") or "").strip()
    if not batch_id:
        raise RuntimeError("batch_id 없이 배치 제출 결과를 등록할 수 없습니다.")

    rows = load_registry(paths)
    now = datetime.now().isoformat(timespec="seconds")
    entry = next((row for row in rows if row.get("batch_id") == batch_id), None)
    if entry is None:
        entry = {
            "batch_id": batch_id,
            "kind": kind,
            "analysis_source": analysis_source,
            "submitted_at": now,
            "applied_at": None,
            "applied_rows": 0,
            "failed_rows": [],
        }
        rows.append(entry)

    entry.update(
        {
            "input_path": submission.get("input_path"),
            "input_file_id": submission.get("input_file_id"),
            "status": submission.get("status"),
            "updated_at": now,
        }
    )
    save_registry(paths, rows)
    return entry


def sync_registered_batches(
    conn,
    *,
    paths: AppPaths,
    config: AnalysisConfig,
    limit: int = 0,
    kinds: tuple[str, ...] = ("analysis",),
) -> dict[str, Any]:
    rows = load_registry(paths)
    checked_batches = 0
    pending_batches = 0
    failed_batches = 0
    applied_batches = 0
    applied_rows = 0
    now = datetime.now().isoformat(timespec="seconds")

    for row in rows:
        if row.get("kind") not in kinds:
            continue
        if row.get("applied_at"):
            continue
        batch_id = str(row.get("batch_id") or "").strip()
        if not batch_id:
            continue
        if limit and checked_batches >= limit:
            pending_batches += 1
            continue

        checked_batches += 1
        try:
            batch = fetch_openai_batch(config, batch_id)
        except Exception as exc:
            row["last_error"] = str(exc)
            row["updated_at"] = now
            continue

        status = str(batch.get("status") or "")
        row["status"] = status
        row["updated_at"] = now
        row["output_file_id"] = batch.get("output_file_id")
        if status == "completed" and row.get("output_file_id"):
            row["completed_at"] = datetime.now().isoformat(timespec="seconds")
            try:
                if row.get("kind") == "ad_analysis":
                    result = apply_openai_ad_batch_output(
                        conn,
                        config=config,
                        file_id=str(row["output_file_id"]),
                        analysis_source=str(row.get("analysis_source") or "generated_openai_ad_batch"),
                    )
                else:
                    result = apply_openai_batch_output(
                        conn,
                        config=config,
                        file_id=str(row["output_file_id"]),
                        analysis_source=str(row.get("analysis_source") or "generated_openai_batch"),
                    )
            except Exception as exc:
                row["last_error"] = str(exc)
                continue
            row["applied_at"] = datetime.now().isoformat(timespec="seconds")
            row["applied_rows"] = int(result.get("processed_rows") or 0)
            row["failed_rows"] = list(result.get("failed_rows") or [])
            row["last_error"] = None
            applied_batches += 1
            applied_rows += row["applied_rows"]
        elif status in {"failed", "expired", "cancelled"}:
            failed_batches += 1
        else:
            pending_batches += 1

    save_registry(paths, rows)
    return {
        "registry_path": str(registry_path(paths)),
        "tracked_batches": len(rows),
        "checked_batches": checked_batches,
        "pending_batches": pending_batches,
        "failed_batches": failed_batches,
        "applied_batches": applied_batches,
        "applied_rows": applied_rows,
    }


def sync_registered_analysis_batches(
    conn,
    *,
    paths: AppPaths,
    config: AnalysisConfig,
    limit: int = 0,
) -> dict[str, Any]:
    return sync_registered_batches(
        conn,
        paths=paths,
        config=config,
        limit=limit,
        kinds=("analysis",),
    )
