from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path

from .config import AppPaths
from .db import connect, init_db, upsert_video


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def find_info_json(raw_dir: Path | None, video_id: str) -> Path | None:
    if not raw_dir or not raw_dir.exists():
        return None
    matches = sorted(raw_dir.glob(f"*__{video_id}__*.info.json"))
    return matches[0] if matches else None


def copy_tree_contents(source_dir: Path | None, target_dir: Path, pattern: str = "*") -> int:
    if not source_dir or not source_dir.exists():
        return 0

    target_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for source_path in source_dir.glob(pattern):
        if source_path.is_dir():
            continue
        if source_path.name == ".DS_Store":
            continue
        target_path = target_dir / source_path.name
        if not target_path.exists():
            shutil.copy2(source_path, target_path)
            copied += 1
    return copied


def import_meta(
    *,
    base_dir: Path,
    meta_csv: Path,
    legacy_raw_dir: Path | None,
    legacy_thumbnails_dir: Path | None,
    copy_info_json: bool,
    copy_thumbnails: bool,
) -> dict[str, int]:
    paths = AppPaths.from_base_dir(base_dir)
    paths.ensure()

    conn = connect(paths.db_path)
    init_db(conn)

    imported = 0
    linked_info_json = 0

    with meta_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            video_id = (row.get("video_id") or "").strip()
            if not video_id:
                continue

            info_json_path = None
            legacy_info_json = find_info_json(legacy_raw_dir, video_id)
            if legacy_info_json:
                linked_info_json += 1
                if copy_info_json:
                    paths.raw_dir.mkdir(parents=True, exist_ok=True)
                    target_info_json = paths.raw_dir / legacy_info_json.name
                    if not target_info_json.exists():
                        shutil.copy2(legacy_info_json, target_info_json)
                    info_json_path = str(target_info_json)
                else:
                    info_json_path = str(legacy_info_json)

            upsert_video(
                conn,
                {
                    "video_id": video_id,
                    "title": (row.get("title") or "").strip(),
                    "upload_date": (row.get("date") or "").strip(),
                    "view_count": int(float(row.get("view") or 0)),
                    "like_count": int(float(row.get("like") or 0)),
                    "has_ko_sub": parse_bool(row.get("ko_sub", "")),
                    "has_auto_ko_sub": False,
                    "thumbnail_url": (row.get("thumbnail_url") or "").strip() or None,
                    "source_url": f"https://www.youtube.com/watch?v={video_id}",
                    "info_json_path": info_json_path,
                },
            )
            imported += 1

    conn.commit()
    conn.close()

    copied_info_json = 0
    copied_thumbnails = 0
    if copy_info_json:
        copied_info_json = copy_tree_contents(legacy_raw_dir, paths.raw_dir, "*.info.json")
    if copy_thumbnails:
        copied_thumbnails = copy_tree_contents(legacy_thumbnails_dir, paths.thumbnails_dir, "*.jpg")

    return {
        "imported_videos": imported,
        "linked_info_json": linked_info_json,
        "copied_info_json": copied_info_json,
        "copied_thumbnails": copied_thumbnails,
        "db_path": str(paths.db_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import legacy meta.csv into syuka-ops SQLite DB")
    parser.add_argument("--base-dir", default="./data", help="Target syuka-ops data directory")
    parser.add_argument("--meta-csv", required=True, help="Legacy meta.csv path")
    parser.add_argument("--legacy-raw-dir", help="Legacy scripts/raw directory")
    parser.add_argument("--legacy-thumbnails-dir", help="Legacy thumbnails directory")
    parser.add_argument("--copy-info-json", action="store_true", help="Copy legacy info.json files into target raw dir")
    parser.add_argument("--copy-thumbnails", action="store_true", help="Copy legacy thumbnails into target thumbnails dir")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = import_meta(
        base_dir=Path(args.base_dir),
        meta_csv=Path(args.meta_csv),
        legacy_raw_dir=Path(args.legacy_raw_dir) if args.legacy_raw_dir else None,
        legacy_thumbnails_dir=Path(args.legacy_thumbnails_dir) if args.legacy_thumbnails_dir else None,
        copy_info_json=args.copy_info_json,
        copy_thumbnails=args.copy_thumbnails,
    )
    print(result)


if __name__ == "__main__":
    main()
