from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from syuka_ops.subtitle_utils import load_subtitle_segments
from syuka_ops.text_utils import normalize_dialogue


@dataclass
class RepairStats:
    scanned: int = 0
    updated: int = 0
    missing_segments: int = 0
    unchanged: int = 0
    total_before_chars: int = 0
    total_after_chars: int = 0


def repaired_dialogue(subtitle_path: str) -> tuple[str, int]:
    segments = load_subtitle_segments(subtitle_path)
    dialogue = normalize_dialogue(" ".join(segment.text for segment in segments if segment.text.strip()))
    return dialogue, len(segments)


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair duplicated auto subtitle transcripts in-place.")
    parser.add_argument("--db-path", default="./data/db/syuka_ops.db")
    parser.add_argument("--base-dir", default="./data")
    parser.add_argument("--channel-key", default=None)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    os.environ["SYUKA_DATA_DIR"] = args.base_dir
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    params: list[object] = []
    sql = """
        SELECT
            t.video_id,
            t.dialogue,
            t.subtitle_path,
            t.segment_count,
            v.channel_key,
            v.title
        FROM transcripts t
        JOIN videos v ON v.video_id = t.video_id
        WHERE t.subtitle_source = 'auto'
    """
    if args.channel_key:
        sql += " AND v.channel_key = ?"
        params.append(args.channel_key)
    sql += " ORDER BY v.channel_key, v.upload_date DESC, t.video_id ASC"
    if args.limit and args.limit > 0:
        sql += " LIMIT ?"
        params.append(args.limit)

    rows = conn.execute(sql, params).fetchall()
    stats = RepairStats()

    for row in rows:
        stats.scanned += 1
        before_dialogue = row["dialogue"] or ""
        stats.total_before_chars += len(before_dialogue)
        subtitle_path = row["subtitle_path"] or ""
        after_dialogue, after_segments = repaired_dialogue(subtitle_path)
        stats.total_after_chars += len(after_dialogue)

        if not after_dialogue:
            stats.missing_segments += 1
            continue

        if before_dialogue == after_dialogue and int(row["segment_count"] or 0) == after_segments:
            stats.unchanged += 1
            continue

        if not args.dry_run:
            conn.execute(
                """
                UPDATE transcripts
                SET dialogue = ?, segment_count = ?, collected_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
                """,
                (after_dialogue, after_segments, row["video_id"]),
            )
        stats.updated += 1

    if not args.dry_run:
        conn.commit()

    print(
        {
            "scanned": stats.scanned,
            "updated": stats.updated,
            "unchanged": stats.unchanged,
            "missing_segments": stats.missing_segments,
            "total_before_chars": stats.total_before_chars,
            "total_after_chars": stats.total_after_chars,
        }
    )


if __name__ == "__main__":
    main()
