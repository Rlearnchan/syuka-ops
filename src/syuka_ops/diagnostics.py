from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import AppPaths
from .db import connect, init_db


TIMESTAMP_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def parse_timestamp(line: str) -> datetime | None:
    match = TIMESTAMP_RE.match(line)
    if not match:
        return None
    return datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")


def read_recent_lines(path: Path, since: datetime) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    selected: list[str] = []
    for line in lines:
        ts = parse_timestamp(line)
        if ts is None or ts >= since:
            selected.append(line)
    return selected


def count_contains(lines: list[str], pattern: str) -> int:
    return sum(1 for line in lines if pattern in line)


def collect_log_metrics(paths: AppPaths, days: int) -> dict[str, Any]:
    since = datetime.now() - timedelta(days=days)
    log_dir = paths.base_dir / "logs"

    slack_lines: list[str] = []
    collector_lines: list[str] = []
    stale_mac_lines: list[str] = []

    for path in sorted(log_dir.glob("slack_bot_*.log")):
        slack_lines.extend(read_recent_lines(path, since))
    for name in (
        "workspace_slack_bot.launchd.err.log",
        "workspace_slack_bot.launchd.out.log",
        "company_slack_bot.launchd.err.log",
        "company_slack_bot.launchd.out.log",
        "launchd.err.log",
        "launchd.out.log",
    ):
        path = log_dir / name
        lines = read_recent_lines(path, since)
        slack_lines.extend(lines)
        stale_mac_lines.extend([line for line in lines if "/syuka-gpt/syuka-ops" in line])
    for path in sorted(log_dir.glob("daily_update_*.log")):
        collector_lines.extend(read_recent_lines(path, since))
    for path in sorted(log_dir.glob("generate_analysis_*.log")):
        collector_lines.extend(read_recent_lines(path, since))

    return {
        "days": days,
        "slack_request_count": count_contains(slack_lines, "Incoming Slack request"),
        "dm_count": count_contains(slack_lines, "Handled direct message"),
        "button_action_count": count_contains(slack_lines, "Handled button action"),
        "reconnect_count": count_contains(slack_lines, "A new session has been established"),
        "broken_pipe_count": sum(1 for line in slack_lines if "BrokenPipeError" in line or "Broken pipe" in line),
        "collector_started_count": count_contains(collector_lines, "Daily update started"),
        "collector_finished_count": count_contains(collector_lines, "Daily update finished"),
        "collector_analysis_runs": count_contains(collector_lines, "[analysis] 시작"),
        "stale_mac_path_errors": len(stale_mac_lines),
    }


def collect_db_metrics(conn, days: int) -> dict[str, Any]:
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    latest_subtitle_attempts = conn.execute(
        """
        SELECT status, COUNT(*) AS c
        FROM (
            SELECT
                video_id,
                status,
                ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY id DESC) AS rn
            FROM download_attempts
            WHERE stage = 'subtitle'
        )
        WHERE rn = 1
        GROUP BY status
        """
    ).fetchall()
    subtitle_status_counts = {row["status"]: row["c"] for row in latest_subtitle_attempts}

    return {
        "new_videos_last_days": conn.execute(
            "SELECT COUNT(*) FROM videos WHERE created_at >= ?",
            (since,),
        ).fetchone()[0],
        "new_transcripts_last_days": conn.execute(
            "SELECT COUNT(*) FROM transcripts WHERE collected_at >= ?",
            (since,),
        ).fetchone()[0],
        "new_analysis_last_days": conn.execute(
            "SELECT COUNT(*) FROM video_analysis WHERE updated_at >= ?",
            (since,),
        ).fetchone()[0],
        "current_info_json_missing": conn.execute(
            "SELECT COUNT(*) FROM videos WHERE info_json_path IS NULL OR TRIM(info_json_path) = ''"
        ).fetchone()[0],
        "current_transcript_pending": conn.execute(
            """
            SELECT COUNT(*)
            FROM videos v
            LEFT JOIN transcripts t ON t.video_id = v.video_id
            WHERE (v.has_ko_sub = 1 OR v.has_auto_ko_sub = 1)
              AND t.video_id IS NULL
            """
        ).fetchone()[0],
        "current_analysis_missing": conn.execute(
            """
            SELECT COUNT(*)
            FROM videos v
            LEFT JOIN video_analysis a ON a.video_id = v.video_id
            WHERE a.video_id IS NULL
            """
        ).fetchone()[0],
        "current_subtitle_failed_latest": subtitle_status_counts.get("failed", 0),
        "current_subtitle_skipped_latest": subtitle_status_counts.get("skipped", 0),
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Syuka Ops Diagnostics",
        "",
        f"- Generated at: {report['generated_at']}",
        f"- Window: last {report['days']} days",
        "",
        "## Log metrics",
    ]
    for key, value in report["logs"].items():
        if key == "days":
            continue
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## DB metrics"])
    for key, value in report["db"].items():
        lines.append(f"- `{key}`: {value}")
    return "\n".join(lines)


def save_report(paths: AppPaths, report: dict[str, Any], output_format: str) -> Path:
    report_dir = paths.base_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "md" if output_format == "markdown" else "json"
    output_path = report_dir / f"syuka_ops_diagnostics_{timestamp}.{suffix}"
    content = render_markdown(report) if output_format == "markdown" else json.dumps(report, ensure_ascii=False, indent=2)
    output_path.write_text(content, encoding="utf-8")
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize recent logs and DB health for syuka-ops")
    parser.add_argument("--base-dir", default=os.environ.get("SYUKA_DATA_DIR", "./data"))
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    paths = AppPaths.from_base_dir(Path(args.base_dir))
    paths.ensure()
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        report = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "days": args.days,
            "logs": collect_log_metrics(paths, args.days),
            "db": collect_db_metrics(conn, args.days),
        }
        output_path = save_report(paths, report, args.format)
        if args.format == "json":
            print(json.dumps({**report, "saved_to": str(output_path)}, ensure_ascii=False, indent=2))
        else:
            print(render_markdown(report))
            print()
            print(f"Saved to: {output_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
