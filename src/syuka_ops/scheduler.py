from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import AppPaths


DEFAULT_INCREMENTAL_TIMES = "00:00,09:00,18:00"
DEFAULT_RETRY_TIMES = "00:20"
DEFAULT_TIMEZONE = "Asia/Seoul"
DEFAULT_BATCH_SYNC_MINUTES = 30
DEFAULT_POLL_SECONDS = 30
STATE_FILENAME = "collector_scheduler_state.json"


def parse_hhmm_list(raw: str) -> list[str]:
    values: list[str] = []
    for item in (raw or "").split(","):
        text = item.strip()
        if not text:
            continue
        hour, minute = text.split(":", 1)
        values.append(f"{int(hour):02d}:{int(minute):02d}")
    return sorted(dict.fromkeys(values))


def state_path(paths: AppPaths) -> Path:
    return paths.reports_dir / STATE_FILENAME


def load_state(paths: AppPaths) -> dict[str, object]:
    target = state_path(paths)
    if not target.exists():
        return {"jobs": {}, "last_batch_sync_at": None}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"jobs": {}, "last_batch_sync_at": None}
    if not isinstance(payload, dict):
        return {"jobs": {}, "last_batch_sync_at": None}
    payload.setdefault("jobs", {})
    payload.setdefault("last_batch_sync_at", None)
    return payload


def save_state(paths: AppPaths, state: dict[str, object]) -> None:
    target = state_path(paths)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def append_log(paths: AppPaths, message: str, *, timezone_name: str = DEFAULT_TIMEZONE) -> None:
    log_dir = paths.base_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    timezone = ZoneInfo(timezone_name)
    line = f"{datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')} {message}"
    with (log_dir / "collector_scheduler.log").open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    print(line, flush=True)


def run_subcommand(args: list[str]) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, "-m", "syuka_ops.collector", *args]
    return subprocess.run(
        command,
        check=False,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
    )


def log_subcommand_result(
    paths: AppPaths,
    *,
    label: str,
    result: subprocess.CompletedProcess[str],
    timezone_name: str,
    max_output_lines: int = 20,
) -> None:
    command_text = shlex.join(str(part) for part in result.args)
    append_log(
        paths,
        f"[scheduler] {label} returncode={result.returncode} command={command_text}",
        timezone_name=timezone_name,
    )
    for stream_name, content in (("stdout", result.stdout), ("stderr", result.stderr)):
        if not content:
            continue
        lines = [line for line in content.splitlines() if line.strip()]
        if not lines:
            continue
        if len(lines) > max_output_lines:
            append_log(
                paths,
                f"[scheduler] {label} {stream_name} truncated showing last {max_output_lines} of {len(lines)} lines",
                timezone_name=timezone_name,
            )
            lines = lines[-max_output_lines:]
        for line in lines:
            append_log(paths, f"[scheduler] {label} {stream_name}: {line}", timezone_name=timezone_name)


def maybe_run_job(paths: AppPaths, state: dict[str, object], *, job_name: str, hhmm: str, today: str) -> bool:
    jobs = state.get("jobs")
    if not isinstance(jobs, dict):
        jobs = {}
        state["jobs"] = jobs
    key = f"{job_name}@{hhmm}"
    if jobs.get(key) == today:
        return False
    jobs[key] = today
    save_state(paths, state)
    return True


def run_incremental_cycle(paths: AppPaths, *, analysis_limit: int, timezone_name: str) -> None:
    append_log(paths, "[scheduler] incremental start", timezone_name=timezone_name)
    result = run_subcommand(["--mode", "incremental", "--base-dir", str(paths.base_dir)])
    log_subcommand_result(paths, label="incremental.collect", result=result, timezone_name=timezone_name)
    if os.environ.get("SYUKA_ANALYSIS_API_KEY") or os.environ.get("OPENAI_API_KEY"):
        append_log(paths, "[scheduler] analysis start", timezone_name=timezone_name)
        result = run_subcommand(
            [
                "--mode",
                "generate-analysis",
                "--base-dir",
                str(paths.base_dir),
                "--analysis-provider",
                "openai",
                "--analysis-limit",
                str(analysis_limit),
            ]
        )
        log_subcommand_result(paths, label="incremental.analysis", result=result, timezone_name=timezone_name)
    result = run_subcommand(["--mode", "sync-analysis-batches", "--base-dir", str(paths.base_dir)])
    log_subcommand_result(paths, label="incremental.batch-sync", result=result, timezone_name=timezone_name)
    append_log(paths, "[scheduler] incremental finish", timezone_name=timezone_name)


def run_retry_cycle(paths: AppPaths, *, timezone_name: str) -> None:
    append_log(paths, "[scheduler] retry-failed start", timezone_name=timezone_name)
    result = run_subcommand(["--mode", "retry-failed", "--base-dir", str(paths.base_dir)])
    log_subcommand_result(paths, label="retry-failed.collect", result=result, timezone_name=timezone_name)
    result = run_subcommand(["--mode", "sync-analysis-batches", "--base-dir", str(paths.base_dir)])
    log_subcommand_result(paths, label="retry-failed.batch-sync", result=result, timezone_name=timezone_name)
    append_log(paths, "[scheduler] retry-failed finish", timezone_name=timezone_name)


def should_sync_batches(state: dict[str, object], *, now_ts: float, interval_minutes: int) -> bool:
    raw = state.get("last_batch_sync_at")
    if raw is None:
        return True
    try:
        last_sync = float(raw)
    except (TypeError, ValueError):
        return True
    return now_ts - last_sync >= interval_minutes * 60


def run_batch_sync(paths: AppPaths, state: dict[str, object], *, timezone_name: str) -> None:
    append_log(paths, "[scheduler] batch-sync start", timezone_name=timezone_name)
    result = run_subcommand(["--mode", "sync-analysis-batches", "--base-dir", str(paths.base_dir)])
    log_subcommand_result(paths, label="batch-sync.collect", result=result, timezone_name=timezone_name)
    state["last_batch_sync_at"] = time.time()
    save_state(paths, state)
    append_log(paths, "[scheduler] batch-sync finish", timezone_name=timezone_name)


def prune_state_jobs(state: dict[str, object], *, incremental_times: list[str], retry_times: list[str]) -> None:
    jobs = state.get("jobs")
    if not isinstance(jobs, dict):
        state["jobs"] = {}
        return
    allowed_keys = {f"incremental@{hhmm}" for hhmm in incremental_times} | {f"retry-failed@{hhmm}" for hhmm in retry_times}
    stale_keys = [key for key in jobs if key not in allowed_keys]
    for key in stale_keys:
        jobs.pop(key, None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run syuka-ops collector on a daily schedule")
    parser.add_argument("--base-dir", default=os.environ.get("SYUKA_DATA_DIR", "./data"))
    parser.add_argument("--timezone", default=os.environ.get("SYUKA_SCHEDULER_TIMEZONE", DEFAULT_TIMEZONE))
    parser.add_argument("--poll-seconds", type=int, default=int(os.environ.get("SYUKA_SCHEDULER_POLL_SECONDS", str(DEFAULT_POLL_SECONDS))))
    parser.add_argument("--incremental-times", default=os.environ.get("SYUKA_SCHEDULER_INCREMENTAL_TIMES", DEFAULT_INCREMENTAL_TIMES))
    parser.add_argument("--retry-times", default=os.environ.get("SYUKA_SCHEDULER_RETRY_TIMES", DEFAULT_RETRY_TIMES))
    parser.add_argument("--analysis-limit", type=int, default=int(os.environ.get("SYUKA_SCHEDULER_ANALYSIS_LIMIT", "25")))
    parser.add_argument(
        "--batch-sync-minutes",
        type=int,
        default=int(os.environ.get("SYUKA_SCHEDULER_BATCH_SYNC_MINUTES", str(DEFAULT_BATCH_SYNC_MINUTES))),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    paths = AppPaths.from_base_dir(Path(args.base_dir))
    paths.ensure()
    state = load_state(paths)
    timezone = ZoneInfo(args.timezone)
    incremental_times = parse_hhmm_list(args.incremental_times)
    retry_times = parse_hhmm_list(args.retry_times)
    prune_state_jobs(state, incremental_times=incremental_times, retry_times=retry_times)
    save_state(paths, state)

    append_log(
        paths,
        (
            "[scheduler] started "
            f"(tz={args.timezone}, incremental={','.join(incremental_times)}, "
            f"retry={','.join(retry_times)}, analysis_limit={args.analysis_limit})"
        ),
        timezone_name=args.timezone,
    )

    while True:
        now = datetime.now(timezone)
        today = now.strftime("%Y-%m-%d")
        hhmm = now.strftime("%H:%M")

        if hhmm in incremental_times and maybe_run_job(paths, state, job_name="incremental", hhmm=hhmm, today=today):
            run_incremental_cycle(paths, analysis_limit=args.analysis_limit, timezone_name=args.timezone)

        if hhmm in retry_times and maybe_run_job(paths, state, job_name="retry-failed", hhmm=hhmm, today=today):
            run_retry_cycle(paths, timezone_name=args.timezone)

        if should_sync_batches(state, now_ts=time.time(), interval_minutes=args.batch_sync_minutes):
            run_batch_sync(paths, state, timezone_name=args.timezone)

        time.sleep(max(10, args.poll_seconds))


if __name__ == "__main__":
    main()
