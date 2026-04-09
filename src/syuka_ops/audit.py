from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
from typing import Any

from .config import AppPaths, resolve_stored_path
from .collector import all_info_json_paths
from .db import connect, init_db


def normalize_reason(stderr: str) -> str:
    text = (stderr or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "sign in to confirm your age" in lowered:
        return "Age restricted"
    if "requested format is not available" in lowered:
        return "Requested format unavailable"
    if "offline." in lowered:
        return "Offline stream/video"
    if "this live event will begin in a few moments" in lowered:
        return "Upcoming live stream"
    if "premieres in" in lowered:
        return "Upcoming premiere"
    if "ffmpeg not found" in lowered:
        return "ffmpeg missing"
    return text.splitlines()[0][:120]


def latest_attempt_map(conn, stage: str) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT video_id, status, attempts, returncode, stderr, created_at
        FROM (
            SELECT
                video_id,
                status,
                attempts,
                returncode,
                stderr,
                created_at,
                ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY id DESC) AS rn
            FROM download_attempts
            WHERE stage = ?
        )
        WHERE rn = 1
        """,
        (stage,),
    ).fetchall()
    return {
        row["video_id"]: {
            "status": row["status"],
            "attempts": row["attempts"],
            "returncode": row["returncode"],
            "stderr": row["stderr"] or "",
            "created_at": row["created_at"],
        }
        for row in rows
    }


def parse_keywords_json(keywords_json: str | None) -> list[str]:
    if not keywords_json:
        return []
    try:
        parsed = json.loads(keywords_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def sentence_count(text: str | None) -> int:
    cleaned = (text or "").strip()
    if not cleaned:
        return 0
    parts = re.split(r"[.!?\n]+", cleaned)
    return len([part for part in parts if part.strip()])


def collect_summary(conn, paths: AppPaths, expected_total: int | None) -> dict[str, Any]:
    stats = conn.execute(
        """
        SELECT
            COUNT(*) AS video_count,
            SUM(CASE WHEN has_ko_sub = 1 THEN 1 ELSE 0 END) AS ko_sub_count,
            SUM(CASE WHEN has_auto_ko_sub = 1 THEN 1 ELSE 0 END) AS auto_ko_sub_count,
            SUM(CASE WHEN has_ko_sub = 1 OR has_auto_ko_sub = 1 THEN 1 ELSE 0 END) AS any_ko_sub_count,
            SUM(CASE WHEN info_json_path IS NOT NULL AND TRIM(info_json_path) != '' THEN 1 ELSE 0 END) AS info_json_count,
            SUM(CASE WHEN source_url IS NOT NULL AND TRIM(source_url) != '' THEN 1 ELSE 0 END) AS source_url_count,
            SUM(CASE WHEN thumbnail_url IS NOT NULL AND TRIM(thumbnail_url) != '' THEN 1 ELSE 0 END) AS thumbnail_url_count,
            MAX(upload_date) AS latest_upload_date
        FROM videos
        """
    ).fetchone()

    transcript_count = conn.execute("SELECT COUNT(*) FROM transcripts").fetchone()[0]
    analysis_count = conn.execute("SELECT COUNT(*) FROM video_analysis").fetchone()[0]
    transcript_path_missing = conn.execute(
        "SELECT COUNT(*) FROM transcripts WHERE subtitle_path IS NULL OR TRIM(subtitle_path) = ''"
    ).fetchone()[0]
    transcript_empty = conn.execute(
        "SELECT COUNT(*) FROM transcripts WHERE dialogue IS NULL OR TRIM(dialogue) = ''"
    ).fetchone()[0]
    transcript_file_missing = 0
    for row in conn.execute(
        """
        SELECT subtitle_path
        FROM transcripts
        WHERE subtitle_path IS NOT NULL AND TRIM(subtitle_path) != ''
        """
    ).fetchall():
        resolved_path = resolve_stored_path(
            row["subtitle_path"],
            base_dir=paths.base_dir,
            search_dirs=[paths.raw_dir],
        )
        if resolved_path is None:
            transcript_file_missing += 1

    missing_info_json = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE info_json_path IS NULL OR TRIM(info_json_path) = ''"
    ).fetchone()[0]
    missing_source_url = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE source_url IS NULL OR TRIM(source_url) = ''"
    ).fetchone()[0]
    missing_thumbnail_url = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE thumbnail_url IS NULL OR TRIM(thumbnail_url) = ''"
    ).fetchone()[0]
    missing_analysis = conn.execute(
        """
        SELECT COUNT(*)
        FROM videos v
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE a.video_id IS NULL
        """
    ).fetchone()[0]
    missing_transcripts_any_sub = conn.execute(
        """
        SELECT COUNT(*)
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        WHERE (v.has_ko_sub = 1 OR v.has_auto_ko_sub = 1)
          AND t.video_id IS NULL
        """
    ).fetchone()[0]
    missing_transcripts_manual_sub = conn.execute(
        """
        SELECT COUNT(*)
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        WHERE v.has_ko_sub = 1
          AND t.video_id IS NULL
        """
    ).fetchone()[0]
    transcript_without_sub_flags = conn.execute(
        """
        SELECT COUNT(*)
        FROM videos v
        JOIN transcripts t ON t.video_id = v.video_id
        WHERE v.has_ko_sub = 0
          AND v.has_auto_ko_sub = 0
        """
    ).fetchone()[0]
    transcript_source_mismatch = conn.execute(
        """
        SELECT COUNT(*)
        FROM videos v
        JOIN transcripts t ON t.video_id = v.video_id
        WHERE (t.subtitle_source = 'manual' AND v.has_ko_sub = 0)
           OR (t.subtitle_source = 'auto' AND v.has_auto_ko_sub = 0)
        """
    ).fetchone()[0]
    analysis_summary_missing = conn.execute(
        "SELECT COUNT(*) FROM video_analysis WHERE summary IS NULL OR TRIM(summary) = ''"
    ).fetchone()[0]
    analysis_keywords_missing = conn.execute(
        "SELECT COUNT(*) FROM video_analysis WHERE keywords_json IS NULL OR TRIM(keywords_json) = ''"
    ).fetchone()[0]
    analysis_source_risk_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM video_analysis
        WHERE analysis_source IN ('legacy_script', 'generated_ollama')
        """
    ).fetchone()[0]
    auto_subtitle_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM transcripts
        WHERE subtitle_source = 'auto'
        """
    ).fetchone()[0]
    suspicious_analysis_quality = len(collect_analysis_quality_rows(conn, limit=10_000))
    suspicious_transcript_quality = len(collect_transcript_quality_rows(conn, limit=10_000))

    info_attempts = latest_attempt_map(conn, "info_json")
    subtitle_attempts = latest_attempt_map(conn, "subtitle")
    info_json_skipped = sum(1 for attempt in info_attempts.values() if attempt["status"] == "skipped")
    subtitle_failed = sum(1 for attempt in subtitle_attempts.values() if attempt["status"] == "failed")
    subtitle_skipped = sum(1 for attempt in subtitle_attempts.values() if attempt["status"] == "skipped")

    info_skip_reasons: dict[str, int] = {}
    subtitle_failure_reasons: dict[str, int] = {}
    for attempt in info_attempts.values():
        if attempt["status"] == "skipped":
            reason = normalize_reason(attempt["stderr"]) or "unknown"
            info_skip_reasons[reason] = info_skip_reasons.get(reason, 0) + 1
    for attempt in subtitle_attempts.values():
        if attempt["status"] == "failed":
            reason = normalize_reason(attempt["stderr"]) or "unknown"
            subtitle_failure_reasons[reason] = subtitle_failure_reasons.get(reason, 0) + 1

    raw_info_json_count = len(all_info_json_paths(paths.raw_dir))
    raw_srt_count = len(list(paths.raw_dir.glob("*.ko.srt")))
    raw_vtt_count = len(list(paths.raw_dir.glob("*.ko.vtt")))
    thumbnail_file_count = len([p for p in paths.thumbnails_dir.glob("*.jpg") if p.is_file()])

    summary = {
        "video_count": int(stats["video_count"] or 0),
        "expected_total": expected_total,
        "missing_videos_vs_expected": max(expected_total - int(stats["video_count"] or 0), 0)
        if expected_total is not None
        else None,
        "ko_sub_count": int(stats["ko_sub_count"] or 0),
        "auto_ko_sub_count": int(stats["auto_ko_sub_count"] or 0),
        "any_ko_sub_count": int(stats["any_ko_sub_count"] or 0),
        "transcript_count": transcript_count,
        "analysis_count": analysis_count,
        "db_info_json_linked": int(stats["info_json_count"] or 0),
        "db_source_url_present": int(stats["source_url_count"] or 0),
        "db_thumbnail_url_present": int(stats["thumbnail_url_count"] or 0),
        "raw_info_json_files": raw_info_json_count,
        "raw_subtitle_srt_files": raw_srt_count,
        "raw_subtitle_vtt_files": raw_vtt_count,
        "thumbnail_files": thumbnail_file_count,
        "missing_info_json": missing_info_json,
        "missing_source_url": missing_source_url,
        "missing_thumbnail_url": missing_thumbnail_url,
        "missing_transcripts_any_sub": missing_transcripts_any_sub,
        "missing_transcripts_manual_sub": missing_transcripts_manual_sub,
        "missing_analysis": missing_analysis,
        "transcript_path_missing": transcript_path_missing,
        "transcript_file_missing": transcript_file_missing,
        "transcript_empty": transcript_empty,
        "transcript_without_sub_flags": transcript_without_sub_flags,
        "transcript_source_mismatch": transcript_source_mismatch,
        "analysis_summary_missing": analysis_summary_missing,
        "analysis_keywords_missing": analysis_keywords_missing,
        "analysis_source_risk_count": analysis_source_risk_count,
        "auto_subtitle_count": auto_subtitle_count,
        "suspicious_analysis_quality": suspicious_analysis_quality,
        "suspicious_transcript_quality": suspicious_transcript_quality,
        "info_json_skipped": info_json_skipped,
        "subtitle_failed": subtitle_failed,
        "subtitle_skipped": subtitle_skipped,
        "latest_upload_date": stats["latest_upload_date"] or "N/A",
        "info_skip_reasons": info_skip_reasons,
        "subtitle_failure_reasons": subtitle_failure_reasons,
    }
    return summary


def collect_missing_info_json_rows(conn, limit: int) -> list[dict[str, Any]]:
    info_attempts = latest_attempt_map(conn, "info_json")
    rows = conn.execute(
        """
        SELECT video_id, upload_date, title, has_ko_sub, has_auto_ko_sub, source_url
        FROM videos
        WHERE info_json_path IS NULL OR TRIM(info_json_path) = ''
        ORDER BY upload_date DESC, video_id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "upload_date": row["upload_date"],
            "video_id": row["video_id"],
            "title": row["title"],
            "has_ko_sub": int(row["has_ko_sub"] or 0),
            "has_auto_ko_sub": int(row["has_auto_ko_sub"] or 0),
            "source_url": row["source_url"] or "",
            "latest_status": info_attempts.get(row["video_id"], {}).get("status", ""),
            "latest_reason": normalize_reason(info_attempts.get(row["video_id"], {}).get("stderr", "")),
        }
        for row in rows
    ]


def collect_missing_transcript_rows(conn, limit: int) -> list[dict[str, Any]]:
    subtitle_attempts = latest_attempt_map(conn, "subtitle")
    rows = conn.execute(
        """
        SELECT v.video_id, v.upload_date, v.title, v.has_ko_sub, v.has_auto_ko_sub
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        WHERE (v.has_ko_sub = 1 OR v.has_auto_ko_sub = 1)
          AND t.video_id IS NULL
        ORDER BY v.upload_date DESC, v.video_id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "upload_date": row["upload_date"],
            "video_id": row["video_id"],
            "title": row["title"],
            "has_ko_sub": int(row["has_ko_sub"] or 0),
            "has_auto_ko_sub": int(row["has_auto_ko_sub"] or 0),
            "latest_status": subtitle_attempts.get(row["video_id"], {}).get("status", ""),
            "latest_reason": normalize_reason(subtitle_attempts.get(row["video_id"], {}).get("stderr", "")),
        }
        for row in rows
    ]


def collect_analysis_gap_rows(conn, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            v.upload_date,
            v.video_id,
            v.title,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE a.video_id IS NULL
        ORDER BY v.upload_date DESC, v.video_id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def collect_analysis_quality_rows(conn, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            v.upload_date,
            v.video_id,
            v.title,
            a.summary,
            a.keywords_json,
            a.analysis_source
        FROM video_analysis a
        JOIN videos v ON v.video_id = a.video_id
        ORDER BY v.upload_date DESC, v.video_id DESC
        """
    ).fetchall()

    suspicious: list[dict[str, Any]] = []
    for row in rows:
        summary = (row["summary"] or "").strip()
        keywords = parse_keywords_json(row["keywords_json"])
        issues: list[str] = []
        if len(summary) < 80:
            issues.append("summary_too_short")
        if sentence_count(summary) < 2:
            issues.append("summary_too_few_sentences")
        if summary.startswith(("요약:", "#", "-", "*")):
            issues.append("summary_formatting_artifact")
        if "```" in summary:
            issues.append("summary_code_fence")
        if len(keywords) < 5:
            issues.append("keywords_too_few")
        if len(set(keywords)) != len(keywords):
            issues.append("keywords_duplicated")
        source_risk = row["analysis_source"] in {"legacy_script", "generated_ollama"}
        if source_risk and issues:
            issues.append(f"source_{row['analysis_source']}")
        if not issues:
            continue
        suspicious.append(
            {
                "upload_date": row["upload_date"],
                "video_id": row["video_id"],
                "title": row["title"],
                "analysis_source": row["analysis_source"],
                "issues": issues,
            }
        )
        if len(suspicious) >= limit:
            break
    return suspicious


def collect_transcript_quality_rows(conn, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            v.upload_date,
            v.video_id,
            v.title,
            t.dialogue,
            t.segment_count,
            t.subtitle_source
        FROM transcripts t
        JOIN videos v ON v.video_id = t.video_id
        ORDER BY v.upload_date DESC, v.video_id DESC
        """
    ).fetchall()

    suspicious: list[dict[str, Any]] = []
    for row in rows:
        dialogue = (row["dialogue"] or "").strip()
        issues: list[str] = []
        if len(dialogue) < 500:
            issues.append("dialogue_too_short")
        if int(row["segment_count"] or 0) < 10:
            issues.append("segment_count_low")
        if row["subtitle_source"] == "auto" and issues:
            issues.append("auto_subtitle")
        if not issues:
            continue
        suspicious.append(
            {
                "upload_date": row["upload_date"],
                "video_id": row["video_id"],
                "title": row["title"],
                "subtitle_source": row["subtitle_source"],
                "issues": issues,
            }
        )
        if len(suspicious) >= limit:
            break
    return suspicious


def collect_subtitle_target_rows(conn, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            v.upload_date,
            v.video_id,
            v.title,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        WHERE v.has_ko_sub = 1
        ORDER BY v.upload_date DESC, v.video_id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def collect_integrity_rows(conn, paths: AppPaths, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            v.upload_date,
            v.video_id,
            v.title,
            v.has_ko_sub,
            v.has_auto_ko_sub,
            CASE WHEN t.video_id IS NULL THEN 0 ELSE 1 END AS has_transcript,
            t.subtitle_path,
            t.subtitle_source,
            CASE WHEN a.video_id IS NULL THEN 0 ELSE 1 END AS has_analysis,
            v.info_json_path,
            v.source_url,
            v.thumbnail_url
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.video_id
        LEFT JOIN video_analysis a ON a.video_id = v.video_id
        WHERE (
                (v.has_ko_sub = 1 OR v.has_auto_ko_sub = 1) AND t.video_id IS NULL
            )
           OR (
                t.video_id IS NOT NULL
                AND (t.subtitle_path IS NULL OR TRIM(t.subtitle_path) = '' OR t.dialogue IS NULL OR TRIM(t.dialogue) = '')
            )
           OR (
                t.video_id IS NOT NULL
                AND v.has_ko_sub = 0
                AND v.has_auto_ko_sub = 0
            )
           OR (
                t.subtitle_source = 'manual'
                AND v.has_ko_sub = 0
            )
           OR (
                t.subtitle_source = 'auto'
                AND v.has_auto_ko_sub = 0
            )
           OR (v.info_json_path IS NULL OR TRIM(v.info_json_path) = '')
           OR (v.source_url IS NULL OR TRIM(v.source_url) = '')
           OR (v.thumbnail_url IS NULL OR TRIM(v.thumbnail_url) = '')
           OR (a.video_id IS NULL)
        ORDER BY v.upload_date DESC, v.video_id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        issues: list[str] = []
        subtitle_path = row["subtitle_path"]
        if (row["has_ko_sub"] or row["has_auto_ko_sub"]) and not row["has_transcript"]:
            issues.append("transcript_missing_for_subtitle_flag")
        if row["has_transcript"] and (not subtitle_path or not str(subtitle_path).strip()):
            issues.append("transcript_path_missing")
        elif row["has_transcript"] and resolve_stored_path(
            subtitle_path,
            base_dir=paths.base_dir,
            search_dirs=[paths.raw_dir],
        ) is None:
            issues.append("transcript_file_missing")
        if row["has_transcript"] and not row["has_ko_sub"] and not row["has_auto_ko_sub"]:
            issues.append("transcript_without_sub_flags")
        if row["subtitle_source"] == "manual" and not row["has_ko_sub"]:
            issues.append("manual_transcript_without_manual_flag")
        if row["subtitle_source"] == "auto" and not row["has_auto_ko_sub"]:
            issues.append("auto_transcript_without_auto_flag")
        if not row["has_analysis"]:
            issues.append("analysis_missing")
        if not row["info_json_path"]:
            issues.append("info_json_missing")
        if not row["source_url"]:
            issues.append("source_url_missing")
        if not row["thumbnail_url"]:
            issues.append("thumbnail_url_missing")
        results.append(
            {
                "upload_date": row["upload_date"],
                "video_id": row["video_id"],
                "title": row["title"],
                "issues": issues,
            }
        )
    return results


def render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    summary = report.get("summary")
    if summary:
        lines.append("== Audit Summary ==")
        for key, value in summary.items():
            if isinstance(value, dict):
                lines.append(f"{key}:")
                if not value:
                    lines.append("  none")
                else:
                    for reason, count in sorted(value.items(), key=lambda item: (-item[1], item[0])):
                        lines.append(f"  - {reason}: {count}")
            elif value is not None:
                lines.append(f"{key}: {value}")

    for section_name in (
        "missing_info_json",
        "missing_transcripts",
        "subtitle_targets",
        "analysis_gaps",
        "analysis_quality",
        "transcript_quality",
        "integrity",
    ):
        rows = report.get(section_name)
        if not rows:
            continue
        lines.append("")
        lines.append(f"== {section_name} ==")
        for row in rows:
            if section_name == "integrity":
                lines.append(
                    f"- {row['upload_date']} | `{row['video_id']}` | {', '.join(row['issues'])} | {row['title']}"
                )
            elif section_name == "analysis_quality":
                lines.append(
                    f"- {row['upload_date']} | `{row['video_id']}` | {row['analysis_source']} | {', '.join(row['issues'])} | {row['title']}"
                )
            elif section_name == "transcript_quality":
                lines.append(
                    f"- {row['upload_date']} | `{row['video_id']}` | {row['subtitle_source']} | {', '.join(row['issues'])} | {row['title']}"
                )
            elif section_name == "subtitle_targets":
                lines.append(
                    f"- {row['upload_date']} | `{row['video_id']}` | has_transcript={row['has_transcript']} | {row['title']}"
                )
            elif section_name == "analysis_gaps":
                lines.append(
                    f"- {row['upload_date']} | `{row['video_id']}` | has_transcript={row['has_transcript']} | {row['title']}"
                )
            else:
                latest = row.get("latest_status") or "none"
                reason = row.get("latest_reason") or "none"
                lines.append(
                    f"- {row['upload_date']} | `{row['video_id']}` | latest={latest} | reason={reason} | {row['title']}"
                )
    return "\n".join(lines)


def render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = ["# Syuka Audit", "", "## Summary"]
    for key, value in summary.items():
        if isinstance(value, dict):
            lines.append(f"- `{key}`")
            if not value:
                lines.append("  none")
            else:
                for reason, count in sorted(value.items(), key=lambda item: (-item[1], item[0])):
                    lines.append(f"  - {reason}: {count}")
        elif value is not None:
            lines.append(f"- `{key}`: {value}")

    for section_name in (
        "missing_info_json",
        "missing_transcripts",
        "subtitle_targets",
        "analysis_gaps",
        "analysis_quality",
        "transcript_quality",
        "integrity",
    ):
        rows = report.get(section_name)
        if not rows:
            continue
        lines.extend(["", f"## {section_name}"])
        for row in rows:
            if section_name == "integrity":
                lines.append(
                    f"- `{row['video_id']}` ({row['upload_date']}) - {', '.join(row['issues'])} - {row['title']}"
                )
            elif section_name == "analysis_quality":
                lines.append(
                    f"- `{row['video_id']}` ({row['upload_date']}) - {row['analysis_source']} - {', '.join(row['issues'])} - {row['title']}"
                )
            elif section_name == "transcript_quality":
                lines.append(
                    f"- `{row['video_id']}` ({row['upload_date']}) - {row['subtitle_source']} - {', '.join(row['issues'])} - {row['title']}"
                )
            elif section_name == "subtitle_targets":
                lines.append(
                    f"- `{row['video_id']}` ({row['upload_date']}) - has_transcript={row['has_transcript']} - {row['title']}"
                )
            elif section_name == "analysis_gaps":
                lines.append(
                    f"- `{row['video_id']}` ({row['upload_date']}) - has_transcript={row['has_transcript']} - {row['title']}"
                )
            else:
                latest = row.get("latest_status") or "none"
                reason = row.get("latest_reason") or "none"
                lines.append(
                    f"- `{row['video_id']}` ({row['upload_date']}) - latest={latest} - reason={reason} - {row['title']}"
                )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit syuka-ops DB and local assets")
    parser.add_argument("--base-dir", default=os.environ.get("SYUKA_DATA_DIR", "./data"))
    parser.add_argument(
        "--command",
        choices=[
            "summary",
            "missing-info-json",
            "missing-transcripts",
            "subtitle-targets",
            "analysis-gaps",
            "analysis-quality",
            "transcript-quality",
            "integrity",
            "all",
        ],
        default="summary",
    )
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--expected-total", type=int, default=None)
    parser.add_argument("--format", choices=["text", "json", "markdown"], default="text")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    paths = AppPaths.from_base_dir(Path(args.base_dir))
    paths.ensure()
    conn = connect(paths.db_path)
    init_db(conn)

    try:
        report: dict[str, Any] = {}
        if args.command in {"summary", "all"}:
            report["summary"] = collect_summary(conn, paths, args.expected_total)
        if args.command in {"missing-info-json", "all"}:
            report["missing_info_json"] = collect_missing_info_json_rows(conn, args.limit)
        if args.command in {"missing-transcripts", "all"}:
            report["missing_transcripts"] = collect_missing_transcript_rows(conn, args.limit)
        if args.command in {"subtitle-targets", "all"}:
            report["subtitle_targets"] = collect_subtitle_target_rows(conn, args.limit)
        if args.command in {"analysis-gaps", "all"}:
            report["analysis_gaps"] = collect_analysis_gap_rows(conn, args.limit)
        if args.command in {"analysis-quality", "all"}:
            report["analysis_quality"] = collect_analysis_quality_rows(conn, args.limit)
        if args.command in {"transcript-quality", "all"}:
            report["transcript_quality"] = collect_transcript_quality_rows(conn, args.limit)
        if args.command in {"integrity", "all"}:
            report["integrity"] = collect_integrity_rows(conn, paths, args.limit)

        if args.format == "json":
            print(json.dumps(report, ensure_ascii=False, indent=2))
        elif args.format == "markdown":
            print(render_markdown(report))
        else:
            print(render_text(report))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
