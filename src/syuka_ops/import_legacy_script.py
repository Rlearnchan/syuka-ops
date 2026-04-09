from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from .config import AppPaths
from .db import connect, init_db, upsert_video_analysis


def parse_keywords_json(raw: str) -> str:
    if not raw or not raw.strip():
        return "[]"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return "[]"
    if not isinstance(parsed, list):
        return "[]"
    normalized = [str(item).strip() for item in parsed if str(item).strip()]
    return json.dumps(normalized, ensure_ascii=False)


def import_script_analysis(
    *,
    base_dir: Path,
    script_csv: Path,
    analysis_source: str = "legacy_script",
) -> dict[str, int | str]:
    paths = AppPaths.from_base_dir(base_dir)
    paths.ensure()

    conn = connect(paths.db_path)
    init_db(conn)

    imported = 0
    skipped = 0

    with script_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            video_id = (row.get("video_id") or "").strip()
            if not video_id:
                skipped += 1
                continue

            upsert_video_analysis(
                conn,
                {
                    "video_id": video_id,
                    "summary": (row.get("summary") or "").strip() or None,
                    "keywords_json": parse_keywords_json(row.get("keyword") or ""),
                    "analysis_source": analysis_source,
                },
            )
            imported += 1

    conn.commit()
    conn.close()

    return {
        "imported_analysis_rows": imported,
        "skipped_rows": skipped,
        "db_path": str(paths.db_path),
        "script_csv": str(script_csv),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import legacy script.csv summaries and keywords into syuka-ops SQLite DB")
    parser.add_argument("--base-dir", default="./data", help="Target syuka-ops data directory")
    parser.add_argument("--script-csv", required=True, help="Legacy script.csv path")
    parser.add_argument("--analysis-source", default="legacy_script", help="Source label stored in DB")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = import_script_analysis(
        base_dir=Path(args.base_dir),
        script_csv=Path(args.script_csv),
        analysis_source=args.analysis_source,
    )
    print(result)


if __name__ == "__main__":
    main()
