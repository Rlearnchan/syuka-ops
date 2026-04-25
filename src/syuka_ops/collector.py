from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import requests
import srt
import yt_dlp
from tqdm import tqdm

from .analysis_pipeline import (
    AnalysisConfig,
    apply_openai_ad_batch_output,
    apply_openai_batch_output,
    fetch_openai_batch,
    prepare_openai_batch_ad_analysis,
    prepare_openai_batch_analysis,
    submit_openai_batch_analysis,
    sync_generated_ad_analysis,
    sync_generated_analysis,
)
from .batch_registry import register_submitted_batch, sync_registered_analysis_batches, sync_registered_batches
from .config import (
    DEFAULT_CHANNEL_KEY,
    AppPaths,
    channel_configs,
    get_channel_by_url,
    get_channel_config,
    resolve_stored_path,
)
from .db import (
    connect,
    failed_subtitle_video_ids,
    init_db,
    latest_video_date,
    record_attempt,
    transcript_video_ids,
    upsert_transcript,
    upsert_video,
)
from .import_legacy_script import import_script_analysis
from .script_catalog import default_script_csv_path
from .subtitle_utils import parse_vtt_to_subs
from .text_utils import normalize_dialogue, strip_caption_markup


def norm_date(x: str) -> str:
    if isinstance(x, str) and re.fullmatch(r"\d{8}", x):
        return f"{x[:4]}-{x[4:6]}-{x[6:]}"
    return x


def clean_text(text: str) -> str:
    return normalize_dialogue(strip_caption_markup(text))


def is_asr_subtitle_entry(entry: dict) -> bool:
    url = str(entry.get("url") or "").lower()
    return "kind=asr" in url


def subtitle_availability(meta: dict) -> tuple[bool, bool]:
    subtitles = meta.get("subtitles", {}) or {}
    automatic_captions = meta.get("automatic_captions", {}) or {}
    ko_subtitles = subtitles.get("ko", []) or []
    has_manual_ko = any(not is_asr_subtitle_entry(entry) for entry in ko_subtitles)
    has_auto_ko = "ko" in automatic_captions or any(is_asr_subtitle_entry(entry) for entry in ko_subtitles)
    return has_manual_ko, has_auto_ko


def safe_title_for_path(title: str, max_length: int = 80) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", title or "").strip()[:max_length]


def subtitle_paths(raw_dir: Path, video_id: str) -> tuple[list[str], list[str]]:
    srt_paths = glob.glob(str(raw_dir / f"*__{video_id}__*.ko.srt"))
    vtt_paths = glob.glob(str(raw_dir / f"*__{video_id}__*.ko.vtt"))
    return sorted(srt_paths), sorted(vtt_paths)


def has_subtitle_file(raw_dir: Path, video_id: str) -> bool:
    srt_paths, vtt_paths = subtitle_paths(raw_dir, video_id)
    return bool(srt_paths or vtt_paths)


def stored_subtitle_looks_auto(path_value: str | Path | None, paths: AppPaths) -> bool:
    path = paths.resolve_raw_path(path_value)
    if path is None or not path.exists():
        return False
    try:
        sample = path.read_text(encoding="utf-8", errors="replace")[:20_000]
    except OSError:
        return False
    return "<c>" in sample or "</c>" in sample or re.search(r"<\d\d:\d\d:\d\d\.\d+>", sample) is not None


def info_json_paths(raw_dir: Path, video_id: str) -> list[str]:
    candidates = []
    for path in glob.glob(str(raw_dir / f"*__{video_id}__*.info.json")):
        if path not in candidates:
            candidates.append(path)
    # Some older backfill runs wrote truncated `*.json` filenames instead of
    # `*.info.json`. Keep recognizing them so the DB can link existing files.
    for path in glob.glob(str(raw_dir / f"*__{video_id}__*.json")):
        if path.endswith(".info.json"):
            continue
        if any(path.endswith(suffix) for suffix in (".ko.srt", ".ko.vtt", ".json3")):
            continue
        if path not in candidates:
            candidates.append(path)
    return sorted(candidates)


def all_info_json_paths(raw_dir: Path) -> list[Path]:
    candidates: dict[str, Path] = {}
    for path in raw_dir.glob("*.info.json"):
        candidates[str(path)] = path
    for path in raw_dir.glob("*.json"):
        if path.name.endswith(".info.json"):
            continue
        if any(path.name.endswith(suffix) for suffix in (".ko.srt", ".ko.vtt", ".json3")):
            continue
        candidates[str(path)] = path
    return sorted(candidates.values())


def write_info_json(paths: AppPaths, meta: dict, existing_path: str | None = None) -> Path:
    if existing_path:
        resolved_existing = resolve_stored_path(
            existing_path,
            base_dir=paths.base_dir,
            search_dirs=[paths.raw_dir],
        )
        if resolved_existing is not None:
            info_path = resolved_existing
        else:
            existing_text = str(existing_path).replace("\\", "/")
            if existing_text.startswith(("scripts/", "thumbnails/", "db/")):
                info_path = paths.base_dir / Path(existing_text)
            else:
                info_path = paths.raw_dir / existing_text.rsplit("/", 1)[-1]
    else:
        upload_date = norm_date(meta.get("upload_date", "")) or "unknown-date"
        title = safe_title_for_path(meta.get("title", "untitled"))
        info_path = paths.raw_dir / f"{upload_date}__{meta['id']}__{title}.info.json"
    info_path.parent.mkdir(parents=True, exist_ok=True)
    info_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    return info_path


def parse_json_stdout(stdout: str) -> dict | None:
    if not stdout:
        return None
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return None


def is_transient_error(stderr: str) -> bool:
    if not stderr:
        return False
    stderr_lower = stderr.lower()
    patterns = [
        "http error 429",
        "too many requests",
        "confirm you're not a bot",
        "please sign in",
        "timed out",
        "remote end closed connection",
        "temporarily unavailable",
    ]
    return any(pattern in stderr_lower for pattern in patterns)


def should_skip_video(
    stderr: str,
    *,
    used_auth: bool = False,
    include_format_unavailable: bool = False,
) -> bool:
    if not stderr:
        return False
    stderr_lower = stderr.lower()
    patterns = [
        "sign in to confirm your age",
        "offline.",
        "this live event will begin in a few moments",
        "premieres in",
    ]
    if any(pattern in stderr_lower for pattern in patterns):
        return True
    if include_format_unavailable and used_auth and "requested format is not available" in stderr_lower:
        return True
    return False


@dataclass
class CollectOptions:
    mode: str
    channel_url: str | None = None
    channel_key: str | None = None
    base_dir: str = os.environ.get("SYUKA_DATA_DIR", "./data")
    sleep_requests: float = 0.4
    retries: int = 3
    max_attempts: int = 4
    retry_backoff: float = 8.0
    video_batch_size: int = 0
    video_batch_index: int = 1
    skip_thumbnails: bool = False
    skip_transcripts: bool = False
    cookies_from_browser: Optional[str] = None
    cookies_file: Optional[str] = None
    video_ids: Optional[list[str]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    oldest_first: bool = False
    recent_days: int = 90
    script_csv: Optional[str] = None
    analysis_provider: str = os.environ.get("SYUKA_ANALYSIS_PROVIDER", "openai")
    analysis_model: str = os.environ.get("SYUKA_ANALYSIS_MODEL", "gpt-5-mini")
    analysis_base_url: str = os.environ.get("SYUKA_ANALYSIS_BASE_URL", "https://api.openai.com/v1")
    analysis_api_key: Optional[str] = os.environ.get("SYUKA_ANALYSIS_API_KEY") or os.environ.get("OPENAI_API_KEY")
    analysis_limit: int = 0
    analysis_overwrite: bool = False
    analysis_batch_path: Optional[str] = None
    analysis_batch_id: Optional[str] = None
    analysis_batch_output_file_id: Optional[str] = None


def yt_dlp_command() -> str:
    executable = shutil.which("yt-dlp")
    if executable:
        return executable

    venv_bin = Path(sys.prefix) / "bin" / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)

    sibling = Path(sys.executable).resolve().parent / "yt-dlp"
    if sibling.exists():
        return str(sibling)

    raise FileNotFoundError("yt-dlp executable not found in PATH or current Python environment")


def fetch_video_infos(
    paths: AppPaths,
    channel_url: str,
    *,
    channel_key: str | None = None,
    channel_name: str | None = None,
    is_short: bool = False,
    only_last_n: Optional[int] = None,
    date_after: Optional[str] = None,
    use_date_ranges: bool = False,
    sleep_requests: float = 0.4,
    retries: int = 3,
) -> list[dict]:
    resolved_channel = get_channel_by_url(channel_url)
    effective_channel_key = channel_key or (resolved_channel.key if resolved_channel else DEFAULT_CHANNEL_KEY)
    effective_channel_name = channel_name or (resolved_channel.display_name if resolved_channel else "?덉뭅?붾뱶")
    raw_dir = paths.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_tmpl = str(raw_dir / "%(upload_date>%Y-%m-%d)s__%(id)s__%(title)s")
    target_video_ids: set[str] | None = None

    rows = []
    seen_ids = set()

    if use_date_ranges:
        current_year = datetime.now().year
        ranges = []
        for year in range(current_year - 4, current_year + 1):
            ranges.append((f"{year-1}1231", f"{year+1}0101"))
        ranges.append((f"{current_year - 5}1231", None))
    else:
        ranges = [(date_after, None)]
        if only_last_n:
            target_video_ids = set(channel_video_ids(channel_url, limit=only_last_n))

    for range_after, range_before in ranges:
        cmd = [
            yt_dlp_command(),
            channel_url,
            "-o",
            out_tmpl,
            "--skip-download",
            "--yes-playlist",
            "--write-info-json",
            "--ignore-errors",
            "--retries",
            str(retries),
            "--sleep-requests",
            str(sleep_requests),
            "--no-warnings",
            "--extractor-args",
            "youtube:player_client=default",
        ]
        if only_last_n:
            cmd += ["--playlist-end", str(only_last_n)]
        if range_after:
            cmd += ["--dateafter", range_after]
        if range_before:
            cmd += ["--datebefore", range_before]

        subprocess.run(cmd, check=False, text=True, capture_output=True)

    for info_path in sorted(raw_dir.glob("*info.json")):
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            continue
        video_id = meta.get("id")
        if not video_id or video_id in seen_ids:
            continue
        if target_video_ids is not None and video_id not in target_video_ids:
            continue
        seen_ids.add(video_id)
        rows.append(
            {
                "video_id": video_id,
                "channel_key": effective_channel_key,
                "channel_name": effective_channel_name,
                "title": meta.get("title", ""),
                "upload_date": norm_date(meta.get("upload_date", "")),
                "duration_seconds": int(meta.get("duration") or 0) if meta.get("duration") is not None else None,
                "is_short": is_short,
                "view_count": meta.get("view_count", 0),
                "like_count": meta.get("like_count", 0),
                "has_ko_sub": subtitle_availability(meta)[0],
                "has_auto_ko_sub": subtitle_availability(meta)[1],
                "thumbnail_url": meta.get("thumbnail"),
                "source_url": f"https://www.youtube.com/watch?v={video_id}",
                "info_json_path": str(info_path),
            }
        )
    return rows


def channel_video_ids(channel_url: str, limit: int = 2500) -> list[str]:
    ydl = yt_dlp.YoutubeDL(
        {
            "quiet": True,
            "extract_flat": True,
            "skip_download": True,
            "playlistend": limit,
        }
    )
    info = ydl.extract_info(channel_url, download=False)
    entries = info.get("entries") or []
    ids = []
    seen = set()
    for entry in entries:
        video_id = entry.get("id")
        if video_id and video_id not in seen:
            seen.add(video_id)
            ids.append(video_id)
    return ids


def upsert_videos(conn, rows: Iterable[dict]) -> None:
    count = 0
    for row in rows:
        upsert_video(conn, merge_with_existing_video(conn, row))
        count += 1
    conn.commit()
    print(f"videos upsert complete: {count}")


def merge_with_existing_video(conn, row: dict) -> dict:
    existing = conn.execute(
        """
        SELECT channel_key, channel_name, duration_seconds, is_short, has_ko_sub, has_auto_ko_sub, thumbnail_url, source_url, info_json_path
        FROM videos
        WHERE video_id = ?
        """,
        (row["video_id"],),
    ).fetchone()
    if not existing:
        return row

    merged = dict(row)
    merged["channel_key"] = row.get("channel_key") or existing["channel_key"] or DEFAULT_CHANNEL_KEY
    merged["channel_name"] = row.get("channel_name") or existing["channel_name"] or "?덉뭅?붾뱶"
    merged["duration_seconds"] = row.get("duration_seconds") or existing["duration_seconds"]
    merged["is_short"] = bool(row.get("is_short")) or bool(existing["is_short"])
    merged["has_ko_sub"] = bool(row.get("has_ko_sub")) or bool(existing["has_ko_sub"])
    merged["has_auto_ko_sub"] = bool(row.get("has_auto_ko_sub")) or bool(existing["has_auto_ko_sub"])
    merged["thumbnail_url"] = row.get("thumbnail_url") or existing["thumbnail_url"]
    merged["source_url"] = row.get("source_url") or existing["source_url"]
    merged["info_json_path"] = row.get("info_json_path") or existing["info_json_path"]
    return merged


def refresh_videos_from_local_info_json(
    conn, paths: AppPaths, video_ids: Optional[set[str]] = None
) -> int:
    count = 0
    for info_path in all_info_json_paths(paths.raw_dir):
        row = video_row_from_info_json(str(info_path), paths)
        if not row:
            continue
        if video_ids is not None and row["video_id"] not in video_ids:
            continue
        upsert_video(conn, merge_with_existing_video(conn, row))
        count += 1
    conn.commit()
    print(f"local info.json refresh complete: {count}")
    return count


def compute_incremental_last_n(conn, *, channel_key: str | None = None, is_short: bool | None = None) -> int | None:
    latest_date = latest_video_date(conn, channel_key=channel_key, is_short=is_short)
    if not latest_date:
        return None
    latest = datetime.strptime(latest_date, "%Y-%m-%d").date()
    today = datetime.now().date()
    if latest >= today:
        return 0
    diff = (today - latest).days
    return max(10, int((diff + 2) * 1.5))


def video_row_from_info_json(info_path: str, paths: AppPaths | None = None) -> dict | None:
    try:
        with open(info_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        return None

    video_id = meta.get("id")
    if not video_id:
        return None

    has_manual_ko, has_auto_ko = subtitle_availability(meta)
    uploader_id = meta.get("uploader_id") or meta.get("channel_id")
    uploader_name = meta.get("uploader") or meta.get("channel")
    channel = None
    if uploader_id:
        for candidate in channel_configs():
            uploader_id_text = str(uploader_id).lower()
            if candidate.key in uploader_id_text:
                channel = candidate
                break
    if channel is None:
        webpage_url = str(meta.get("channel_url") or meta.get("uploader_url") or "")
        channel = get_channel_by_url(webpage_url)
    if channel is None:
        channel_key = str(uploader_id or DEFAULT_CHANNEL_KEY)
        channel_name = str(uploader_name or "?덉뭅?붾뱶")
    else:
        channel_key = channel.key
        channel_name = channel.display_name
    return {
        "video_id": video_id,
        "channel_key": channel_key,
        "channel_name": channel_name,
        "title": meta.get("title", ""),
        "upload_date": norm_date(meta.get("upload_date", "")),
        "duration_seconds": int(meta.get("duration") or 0) if meta.get("duration") is not None else None,
        "is_short": bool(meta.get("webpage_url_basename") == "shorts" or "/shorts/" in str(meta.get("webpage_url") or "").lower()),
        "view_count": meta.get("view_count", 0),
        "like_count": meta.get("like_count", 0),
        "has_ko_sub": has_manual_ko,
        "has_auto_ko_sub": has_auto_ko,
        "thumbnail_url": meta.get("thumbnail"),
        "source_url": f"https://www.youtube.com/watch?v={video_id}",
        "info_json_path": paths.to_portable_path(info_path) if paths else info_path,
    }


def select_target_video_ids(conn, options: CollectOptions) -> list[str]:
    if options.video_ids is not None:
        requested_ids = list(dict.fromkeys(options.video_ids))
        if not requested_ids:
            return []
        placeholders = ",".join("?" for _ in requested_ids)
        existing_transcripts = transcript_video_ids(conn)
        rows = conn.execute(
            f"""
            SELECT video_id, has_ko_sub, has_auto_ko_sub, COALESCE(is_short, 0) AS is_short
            FROM videos
            WHERE video_id IN ({placeholders})
            """,
            requested_ids,
        ).fetchall()
        video_rows = {row["video_id"]: row for row in rows}
        target_list: list[str] = []
        for video_id in requested_ids:
            row = video_rows.get(video_id)
            if not row:
                continue
            if row["is_short"]:
                continue
            if not row["has_ko_sub"] and not row["has_auto_ko_sub"]:
                continue
            transcript = conn.execute(
                "SELECT subtitle_source FROM transcripts WHERE video_id = ?",
                (video_id,),
            ).fetchone()
            needs_initial_download = video_id not in existing_transcripts
            needs_manual_upgrade = bool(row["has_ko_sub"] and transcript and transcript["subtitle_source"] == "auto")
            if needs_initial_download or needs_manual_upgrade:
                target_list.append(video_id)
        return target_list

    existing_transcripts = transcript_video_ids(conn)
    if options.mode == "retry-failed":
        targets = failed_subtitle_video_ids(conn)
    else:
        conditions = ["COALESCE(is_short, 0) = 0", "(has_ko_sub = 1 OR has_auto_ko_sub = 1)"]
        params: list[str] = []
        if options.date_from:
            conditions.append("upload_date >= ?")
            params.append(options.date_from)
        if options.date_to:
            conditions.append("upload_date <= ?")
            params.append(options.date_to)
        query = f"""
            SELECT video_id
            FROM videos
            WHERE {' AND '.join(conditions)}
              AND NOT EXISTS (
                    SELECT 1
                    FROM download_attempts da
                    WHERE da.video_id = videos.video_id
                      AND da.stage = 'subtitle'
                      AND da.id = (
                          SELECT MAX(da2.id)
                          FROM download_attempts da2
                          WHERE da2.video_id = videos.video_id
                            AND da2.stage = 'subtitle'
                      )
                      AND da.status = 'skipped'
              )
            ORDER BY upload_date DESC
        """
        rows = conn.execute(query, params).fetchall()
        targets = {row["video_id"] for row in rows if row["video_id"] not in existing_transcripts}
        upgrade_conditions = [
            "v.has_ko_sub = 1",
            "t.subtitle_source = 'auto'",
        ]
        upgrade_params: list[str] = []
        if options.date_from:
            upgrade_conditions.append("v.upload_date >= ?")
            upgrade_params.append(options.date_from)
        elif options.recent_days > 0:
            cutoff = datetime.now().date() - timedelta(days=options.recent_days)
            upgrade_conditions.append("v.upload_date >= ?")
            upgrade_params.append(cutoff.isoformat())
        if options.date_to:
            upgrade_conditions.append("v.upload_date <= ?")
            upgrade_params.append(options.date_to)
        upgrade_query = f"""
            SELECT v.video_id
            FROM videos v
            JOIN transcripts t ON t.video_id = v.video_id
            WHERE {' AND '.join(upgrade_conditions)}
            ORDER BY v.upload_date DESC
        """
        targets.update(row["video_id"] for row in conn.execute(upgrade_query, upgrade_params).fetchall())

    target_list = list(targets)
    if options.video_batch_size > 0:
        start = (options.video_batch_index - 1) * options.video_batch_size
        end = start + options.video_batch_size
        target_list = target_list[start:end]
    return target_list


def select_missing_info_json_video_ids(conn, options: CollectOptions) -> list[str]:
    if options.video_ids is not None:
        return list(dict.fromkeys(options.video_ids))

    conditions = ["(info_json_path IS NULL OR info_json_path = '')"]
    params: list[str] = []
    if options.date_from:
        conditions.append("upload_date >= ?")
        params.append(options.date_from)
    if options.date_to:
        conditions.append("upload_date <= ?")
        params.append(options.date_to)
    order_by = "upload_date ASC, video_id ASC" if options.oldest_first else "upload_date DESC, video_id DESC"
    query = f"""
        SELECT video_id
        FROM videos
        WHERE {' AND '.join(conditions)}
          AND NOT EXISTS (
                SELECT 1
                FROM download_attempts da
                WHERE da.video_id = videos.video_id
                  AND da.stage = 'info_json'
                  AND da.id = (
                      SELECT MAX(da2.id)
                      FROM download_attempts da2
                      WHERE da2.video_id = videos.video_id
                        AND da2.stage = 'info_json'
                  )
                  AND da.status = 'skipped'
          )
        ORDER BY {order_by}
    """
    rows = conn.execute(query, params).fetchall()
    target_list = [row["video_id"] for row in rows]
    if options.video_batch_size > 0:
        start = (options.video_batch_index - 1) * options.video_batch_size
        end = start + options.video_batch_size
        target_list = target_list[start:end]
    return target_list


def apply_cookie_options(cmd: list[str], options: CollectOptions) -> list[str]:
    if options.cookies_from_browser:
        cmd += ["--cookies-from-browser", options.cookies_from_browser]
    if options.cookies_file:
        cmd += ["--cookies", options.cookies_file]
    return cmd


def youtube_extractor_args(options: CollectOptions) -> str:
    # Preserve yt-dlp's upstream default client strategy. When auth is available,
    # keep age-restricted fallbacks like tv_embedded/web_creator in play.
    if options.cookies_from_browser or options.cookies_file:
        return "youtube:player_client=default,tv_embedded,web_creator"
    return "youtube:player_client=default"


def fetch_video_infos_for_ids(paths: AppPaths, video_ids: list[str], options: CollectOptions) -> list[dict]:
    if not video_ids:
        return []

    rows: list[dict] = []
    for video_id in tqdm(video_ids, desc="video meta backfill"):
        cmd = [
            yt_dlp_command(),
            f"https://www.youtube.com/watch?v={video_id}",
            "--skip-download",
            "--dump-single-json",
            "--ignore-no-formats-error",
            "--ignore-errors",
            "--retries",
            str(options.retries),
            "--sleep-requests",
            str(options.sleep_requests),
            "--no-warnings",
            "--extractor-args",
            youtube_extractor_args(options),
        ]
        cmd = apply_cookie_options(cmd, options)
        completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
        meta = parse_json_stdout(completed.stdout or "")
        if not meta or not meta.get("id"):
            continue
        existing = info_json_paths(paths.raw_dir, video_id)
        info_path = write_info_json(paths, meta, existing[0] if existing else None)
        row = video_row_from_info_json(str(info_path), paths)
        if row:
            rows.append(row)
    return rows


def download_info_json_for_video(paths: AppPaths, video_id: str, options: CollectOptions) -> dict:
    existing_paths = set(info_json_paths(paths.raw_dir, video_id))
    cmd = [
        yt_dlp_command(),
        f"https://www.youtube.com/watch?v={video_id}",
        "--skip-download",
        "--dump-single-json",
        "--ignore-no-formats-error",
        "--ignore-errors",
        "--retries",
        str(options.retries),
        "--sleep-requests",
        str(options.sleep_requests),
        "--no-warnings",
        "--extractor-args",
        youtube_extractor_args(options),
    ]
    cmd = apply_cookie_options(cmd, options)
    completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
    meta = parse_json_stdout(completed.stdout or "")
    if meta and meta.get("id"):
        existing = sorted(existing_paths)
        info_path = write_info_json(paths, meta, existing[0] if existing else None)
        updated_paths = {str(info_path)}
    else:
        updated_paths = set(info_json_paths(paths.raw_dir, video_id))

    new_paths = updated_paths - existing_paths
    if new_paths:
        status = "downloaded"
    elif updated_paths:
        status = "already_present"
    elif should_skip_video(
        completed.stderr or "",
        used_auth=bool(options.cookies_from_browser or options.cookies_file),
        include_format_unavailable=True,
    ):
        status = "skipped"
    elif completed.returncode != 0:
        status = "failed"
    else:
        status = "no_info_json_written"

    return {
        "video_id": video_id,
        "stage": "info_json",
        "status": status,
        "attempts": 1,
        "returncode": completed.returncode,
        "stderr": completed.stderr or "",
        "stdout": completed.stdout or "",
    }


def sync_channel_meta(conn, paths: AppPaths, options: CollectOptions) -> None:
    channel_url = options.channel_url or get_channel_config(options.channel_key or DEFAULT_CHANNEL_KEY).url
    channel = get_channel_by_url(channel_url)
    channel_ids = channel_video_ids(channel_url)
    db_rows = conn.execute("SELECT video_id FROM videos").fetchall()
    db_ids = {row["video_id"] for row in db_rows}
    missing_ids = [video_id for video_id in channel_ids if video_id not in db_ids]

    print(f"channel video ids: {len(channel_ids)}")
    print(f"db video ids: {len(db_ids)}")
    print(f"missing channel videos in db: {len(missing_ids)}")

    if options.video_batch_size > 0:
        start = (options.video_batch_index - 1) * options.video_batch_size
        end = start + options.video_batch_size
        missing_ids = missing_ids[start:end]
        print(f"batch-scoped missing videos: {len(missing_ids)}")

    rows = fetch_video_infos_for_ids(paths, missing_ids, options)
    for row in rows:
        if channel:
            row["channel_key"] = channel.key
            row["channel_name"] = channel.display_name
    upsert_videos(conn, rows)


def select_missing_subtitle_metadata_video_ids(conn, options: CollectOptions) -> list[str]:
    if options.video_ids is not None:
        return list(dict.fromkeys(options.video_ids))

    conditions = ["has_ko_sub = 0", "has_auto_ko_sub = 0"]
    params: list[str] = []
    if options.date_from:
        conditions.append("upload_date >= ?")
        params.append(options.date_from)
    if options.date_to:
        conditions.append("upload_date <= ?")
        params.append(options.date_to)
    order_by = "upload_date ASC, video_id ASC" if options.oldest_first else "upload_date DESC, video_id DESC"
    query = f"""
        SELECT video_id
        FROM videos
        WHERE {' AND '.join(conditions)}
        ORDER BY {order_by}
    """
    rows = conn.execute(query, params).fetchall()
    target_list = [row["video_id"] for row in rows]
    if options.video_batch_size > 0:
        start = (options.video_batch_index - 1) * options.video_batch_size
        end = start + options.video_batch_size
        target_list = target_list[start:end]
    return target_list


def select_metric_refresh_video_ids(conn, options: CollectOptions) -> list[str]:
    if options.video_ids is not None:
        return list(dict.fromkeys(options.video_ids))

    conditions: list[str] = []
    params: list[str] = []
    if options.date_from:
        conditions.append("upload_date >= ?")
        params.append(options.date_from)
    if options.date_to:
        conditions.append("upload_date <= ?")
        params.append(options.date_to)

    if not conditions and options.recent_days > 0:
        cutoff = datetime.now().date() - timedelta(days=options.recent_days)
        conditions.append("upload_date >= ?")
        params.append(cutoff.isoformat())

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    order_by = "upload_date ASC, video_id ASC" if options.oldest_first else "upload_date DESC, video_id DESC"
    query = f"""
        SELECT video_id
        FROM videos
        {where_clause}
        ORDER BY {order_by}
    """
    rows = conn.execute(query, params).fetchall()
    target_list = [row["video_id"] for row in rows]
    if options.video_batch_size > 0:
        start = (options.video_batch_index - 1) * options.video_batch_size
        end = start + options.video_batch_size
        target_list = target_list[start:end]
    return target_list


def refresh_subtitle_metadata(conn, paths: AppPaths, options: CollectOptions) -> None:
    target_video_ids = select_missing_subtitle_metadata_video_ids(conn, options)
    if not target_video_ids:
        print("No subtitle metadata refresh targets.")
        return

    print(f"subtitle metadata refresh targets: {len(target_video_ids)}")
    rows = fetch_video_infos_for_ids(paths, target_video_ids, options)
    if not rows:
        print("No subtitle metadata rows were fetched.")
        return
    upsert_videos(conn, rows)


def refresh_metrics(conn, paths: AppPaths, options: CollectOptions) -> None:
    target_video_ids = select_metric_refresh_video_ids(conn, options)
    if not target_video_ids:
        print("No metric refresh targets.")
        return

    print(f"metric refresh targets: {len(target_video_ids)}")
    rows = fetch_video_infos_for_ids(paths, target_video_ids, options)
    if not rows:
        print("No metric refresh rows were fetched.")
        return
    upsert_videos(conn, rows)


def backfill_info_json(conn, paths: AppPaths, options: CollectOptions) -> None:
    target_video_ids = select_missing_info_json_video_ids(conn, options)
    if not target_video_ids:
        print("No info.json backfill targets.")
        return

    print(f"info.json backfill targets: {len(target_video_ids)}")
    for video_id in tqdm(target_video_ids, desc="info.json backfill"):
        result = download_info_json_for_video(paths, video_id, options)
        record_attempt(conn, result)

    conn.commit()

    refresh_videos_from_local_info_json(conn, paths, set(target_video_ids))


def download_subtitle_for_video(
    paths: AppPaths,
    video_id: str,
    options: CollectOptions,
    *,
    allowed_sources: set[str] | None = None,
    force_overwrite: bool = False,
) -> dict:
    out_tmpl = str(paths.raw_dir / f"%(upload_date>%Y-%m-%d)s__{video_id}__%(title).80s")
    last_stdout = ""
    last_stderr = ""
    last_returncode = 0
    last_source = "manual"

    subtitle_modes = [
        ("manual", ["--write-sub"]),
        ("auto", ["--write-auto-sub"]),
    ]
    if allowed_sources:
        subtitle_modes = [(source, flags) for source, flags in subtitle_modes if source in allowed_sources]

    for source, subtitle_flags in subtitle_modes:
        existing_srt_paths, existing_vtt_paths = subtitle_paths(paths.raw_dir, video_id)
        existing_paths = set(existing_srt_paths + existing_vtt_paths)

        for attempt in range(1, options.max_attempts + 1):
            cmd = [
                yt_dlp_command(),
                f"https://www.youtube.com/watch?v={video_id}",
                "-o",
                out_tmpl,
                "--skip-download",
                # Some videos expose subtitle tracks even when yt-dlp reports no downloadable formats.
                "--ignore-no-formats-error",
                *subtitle_flags,
                "--sub-langs",
                "ko",
                "--sub-format",
                "vtt/best",
                "--write-info-json",
                "--ignore-errors",
                "--retries",
                str(options.retries),
                "--sleep-requests",
                str(options.sleep_requests),
                "--sleep-subtitles",
                str(max(1.0, options.sleep_requests)),
                "--no-warnings",
                "--extractor-args",
                youtube_extractor_args(options),
            ]
            if force_overwrite and source == "manual":
                cmd.append("--force-overwrites")
            cmd = apply_cookie_options(cmd, options)

            completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
            last_stdout = completed.stdout or ""
            last_stderr = completed.stderr or ""
            last_returncode = completed.returncode
            last_source = source

            updated_srt_paths, updated_vtt_paths = subtitle_paths(paths.raw_dir, video_id)
            updated_paths = set(updated_srt_paths + updated_vtt_paths)
            if updated_paths - existing_paths or updated_paths:
                return {
                    "video_id": video_id,
                    "stage": "subtitle",
                    "status": "downloaded",
                    "attempts": attempt,
                    "returncode": last_returncode,
                    "stderr": last_stderr,
                    "stdout": last_stdout,
                    "subtitle_source": source,
                }

            if should_skip_video(
                last_stderr,
                used_auth=bool(options.cookies_from_browser or options.cookies_file),
            ):
                return {
                    "video_id": video_id,
                    "stage": "subtitle",
                    "status": "skipped",
                    "attempts": attempt,
                    "returncode": last_returncode,
                    "stderr": last_stderr,
                    "stdout": last_stdout,
                    "subtitle_source": source,
                }

            if attempt < options.max_attempts and is_transient_error(last_stderr):
                time.sleep(options.retry_backoff * attempt)
                continue
            break

        if "requested format is not available" in last_stderr.lower():
            continue

    return {
        "video_id": video_id,
        "stage": "subtitle",
        "status": "failed",
        "attempts": options.max_attempts,
        "returncode": last_returncode,
        "stderr": last_stderr,
        "stdout": last_stdout,
        "subtitle_source": last_source,
    }


def collect_transcripts(conn, paths: AppPaths, options: CollectOptions) -> None:
    target_video_ids = select_target_video_ids(conn, options)
    if not target_video_ids:
        print("No subtitle collection targets.")
        return

    print(f"subtitle collection targets: {len(target_video_ids)}")
    for index, video_id in enumerate(tqdm(target_video_ids, desc="subtitle download"), start=1):
        video_row = conn.execute(
            """
            SELECT v.has_ko_sub, v.has_auto_ko_sub, t.subtitle_source, t.subtitle_path
            FROM videos v
            LEFT JOIN transcripts t ON t.video_id = v.video_id
            WHERE v.video_id = ?
            """,
            (video_id,),
        ).fetchone()
        allowed_sources = None
        force_overwrite = False
        if video_row:
            allowed_sources = set()
            if video_row["has_ko_sub"]:
                allowed_sources.add("manual")
            if video_row["has_auto_ko_sub"]:
                allowed_sources.add("auto")
            force_overwrite = bool(
                video_row["has_ko_sub"]
                and (
                    video_row["subtitle_source"] == "auto"
                    or stored_subtitle_looks_auto(video_row["subtitle_path"], paths)
                )
            )
            if not allowed_sources:
                allowed_sources = None
        result = download_subtitle_for_video(
            paths,
            video_id,
            options,
            allowed_sources=allowed_sources,
            force_overwrite=force_overwrite,
        )
        record_attempt(conn, result)
        conn.commit()

        if result["status"] != "downloaded":
            continue

        info_paths = info_json_paths(paths.raw_dir, video_id)
        if info_paths:
            video_row = video_row_from_info_json(info_paths[0], paths)
            if video_row:
                upsert_video(conn, merge_with_existing_video(conn, video_row))
                conn.commit()

        srt_paths, vtt_paths = subtitle_paths(paths.raw_dir, video_id)
        subs = []
        subtitle_path = None
        if srt_paths:
            subtitle_path = srt_paths[0]
            try:
                with open(subtitle_path, "r", encoding="utf-8") as f:
                    subs = list(srt.parse(f.read()))
            except Exception:
                subs = []
        elif vtt_paths:
            subtitle_path = vtt_paths[0]
            subs = parse_vtt_to_subs(subtitle_path)

        if not subs:
            continue

        dialogue = clean_text(" ".join(sub.content.strip() for sub in subs if sub.content.strip()))
        upsert_transcript(
            conn,
                {
                    "video_id": video_id,
                    "dialogue": dialogue,
                    "subtitle_path": paths.to_portable_path(subtitle_path),
                    "subtitle_source": result.get("subtitle_source", "manual"),
                    "segment_count": len(subs),
                },
            )
        conn.commit()

        if options.video_batch_size > 0 and index % min(options.video_batch_size, 20) == 0 and index < len(target_video_ids):
            time.sleep(20.0)


def thumbnail_candidate_urls(video_id: str, preferred_url: Optional[str]) -> list[str]:
    candidates = []
    base_items = [
        preferred_url,
        f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/maxresdefault_live.jpg",
        f"https://i.ytimg.com/vi/{video_id}/sddefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        f"https://i.ytimg.com/vi_webp/{video_id}/maxresdefault.webp",
        f"https://i.ytimg.com/vi_webp/{video_id}/sddefault.webp",
        f"https://i.ytimg.com/vi_webp/{video_id}/hqdefault.webp",
        f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg",
        f"https://i.ytimg.com/vi_webp/{video_id}/mqdefault.webp",
    ]
    if preferred_url:
        base_items.extend(
            [
                preferred_url.replace("/vi_webp/", "/vi/").replace(".webp", ".jpg"),
                preferred_url.replace("/vi/", "/vi_webp/").replace(".jpg", ".webp"),
                preferred_url.replace("maxresdefault_live.jpg", "maxresdefault.jpg"),
                preferred_url.replace("maxresdefault.jpg", "sddefault.jpg"),
                preferred_url.replace("maxresdefault.jpg", "hqdefault.jpg"),
                preferred_url.replace("maxresdefault.webp", "sddefault.webp"),
                preferred_url.replace("maxresdefault.webp", "hqdefault.webp"),
            ]
        )

    for item in base_items:
        if item and item not in candidates:
            candidates.append(item)
    return candidates


def thumbnail_exists_for_video(paths: AppPaths, video_id: str) -> bool:
    return any(paths.thumbnails_dir.glob(f"*_{video_id}_*"))


def download_thumbnails(conn, paths: AppPaths, overwrite: bool = False) -> None:
    rows = conn.execute("SELECT video_id, title, upload_date, thumbnail_url FROM videos ORDER BY upload_date DESC").fetchall()
    if not rows:
        return
    session = requests.Session()
    downloaded = 0
    for row in tqdm(rows, desc="thumbnail download"):
        safe_title = safe_title_for_path(row["title"], max_length=120)
        output_path = paths.thumbnails_dir / f'{row["upload_date"]}_{row["video_id"]}_{safe_title}.jpg'
        if not overwrite and thumbnail_exists_for_video(paths, row["video_id"]):
            continue
        for url in thumbnail_candidate_urls(row["video_id"], row["thumbnail_url"]):
            try:
                response = session.get(url, timeout=20)
                content_type = response.headers.get("content-type", "").lower()
                if response.status_code == 200 and response.content and content_type.startswith("image/"):
                    output_path.write_bytes(response.content)
                    downloaded += 1
                    break
            except requests.RequestException:
                continue
    print(f"thumbnail download complete: {downloaded}")


def download_thumbnails_for_video_ids(
    conn,
    paths: AppPaths,
    video_ids: list[str],
    *,
    overwrite: bool = False,
) -> None:
    if not video_ids:
        return
    placeholders = ",".join("?" for _ in video_ids)
    rows = conn.execute(
        f"""
        SELECT video_id, title, upload_date, thumbnail_url
        FROM videos
        WHERE video_id IN ({placeholders})
        ORDER BY upload_date DESC
        """,
        video_ids,
    ).fetchall()
    if not rows:
        return
    session = requests.Session()
    downloaded = 0
    for row in tqdm(rows, desc="thumbnail download"):
        safe_title = safe_title_for_path(row["title"], max_length=120)
        output_path = paths.thumbnails_dir / f'{row["upload_date"]}_{row["video_id"]}_{safe_title}.jpg'
        if not overwrite and thumbnail_exists_for_video(paths, row["video_id"]):
            continue
        for url in thumbnail_candidate_urls(row["video_id"], row["thumbnail_url"]):
            try:
                response = session.get(url, timeout=20)
                content_type = response.headers.get("content-type", "").lower()
                if response.status_code == 200 and response.content and content_type.startswith("image/"):
                    output_path.write_bytes(response.content)
                    downloaded += 1
                    break
            except requests.RequestException:
                continue
    print(f"thumbnail download complete: {downloaded}")


def selected_channels(options: CollectOptions) -> list[tuple[str, str, str]]:
    if options.channel_key:
        channel = get_channel_config(options.channel_key)
        return [(channel.key, channel.display_name, channel.url)]
    if options.channel_url:
        channel = get_channel_by_url(options.channel_url)
        if channel:
            return [(channel.key, channel.display_name, channel.url)]
        return [(DEFAULT_CHANNEL_KEY, "?덉뭅?붾뱶", options.channel_url)]
    return [(channel.key, channel.display_name, channel.url) for channel in channel_configs()]


def selected_short_channels(options: CollectOptions) -> list[tuple[str, str, str]]:
    if options.channel_key:
        channel = get_channel_config(options.channel_key)
        return [(channel.key, channel.display_name, channel.shorts_url)]
    if options.channel_url:
        channel = get_channel_by_url(options.channel_url)
        if channel:
            return [(channel.key, channel.display_name, channel.shorts_url)]
        fallback = str(options.channel_url).rstrip("/")
        if fallback.endswith("/videos"):
            fallback = fallback[:-7] + "/shorts"
        elif not fallback.endswith("/shorts"):
            fallback += "/shorts"
        return [(DEFAULT_CHANNEL_KEY, "슈카월드", fallback)]
    return [(channel.key, channel.display_name, channel.shorts_url) for channel in channel_configs()]


def run_collect(options: CollectOptions) -> None:
    paths = AppPaths.from_base_dir(options.base_dir)
    paths.ensure()
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        channels = selected_short_channels(options) if options.mode in {"full-shorts", "incremental-shorts"} else selected_channels(options)
        if options.mode == "full":
            videos: list[dict] = []
            for channel_key, channel_name, channel_url in channels:
                videos.extend(
                    fetch_video_infos(
                        paths,
                        channel_url=channel_url,
                        channel_key=channel_key,
                        channel_name=channel_name,
                        use_date_ranges=True,
                        sleep_requests=options.sleep_requests,
                        retries=options.retries,
                    )
                )
            upsert_videos(conn, videos)
        elif options.mode == "incremental":
            videos: list[dict] = []
            for channel_key, channel_name, channel_url in channels:
                last_n = compute_incremental_last_n(conn, channel_key=channel_key, is_short=False)
                if last_n == 0:
                    continue
                videos.extend(
                    fetch_video_infos(
                        paths,
                        channel_url=channel_url,
                        channel_key=channel_key,
                        channel_name=channel_name,
                        only_last_n=last_n,
                        sleep_requests=options.sleep_requests,
                        retries=options.retries,
                    )
                )
            if videos:
                upsert_videos(conn, videos)
                options = replace(options, video_ids=[row["video_id"] for row in videos])
            else:
                print("Latest metadata is already up to date; skipping incremental metadata refresh.")
        elif options.mode == "full-shorts":
            shorts: list[dict] = []
            for channel_key, channel_name, channel_url in channels:
                shorts.extend(
                    fetch_video_infos(
                        paths,
                        channel_url=channel_url,
                        channel_key=channel_key,
                        channel_name=channel_name,
                        is_short=True,
                        use_date_ranges=True,
                        sleep_requests=options.sleep_requests,
                        retries=options.retries,
                    )
                )
            upsert_videos(conn, shorts)
            options = replace(options, skip_transcripts=True)
        elif options.mode == "incremental-shorts":
            shorts: list[dict] = []
            for channel_key, channel_name, channel_url in channels:
                last_n = compute_incremental_last_n(conn, channel_key=channel_key, is_short=True)
                if last_n == 0:
                    continue
                shorts.extend(
                    fetch_video_infos(
                        paths,
                        channel_url=channel_url,
                        channel_key=channel_key,
                        channel_name=channel_name,
                        is_short=True,
                        only_last_n=last_n,
                        sleep_requests=options.sleep_requests,
                        retries=options.retries,
                    )
                )
            if shorts:
                upsert_videos(conn, shorts)
                options = replace(options, video_ids=[row["video_id"] for row in shorts], skip_transcripts=True)
            else:
                print("Latest shorts metadata is already up to date; skipping shorts metadata refresh.")
        elif options.mode == "sync-channel-meta":
            for channel_key, _, channel_url in channels:
                sync_channel_meta(conn, paths, replace(options, channel_key=channel_key, channel_url=channel_url))
        elif options.mode == "backfill-info-json":
            backfill_info_json(conn, paths, options)
        elif options.mode == "refresh-metrics":
            refresh_metrics(conn, paths, options)
        elif options.mode == "refresh-subtitle-meta":
            refresh_subtitle_metadata(conn, paths, options)
        elif options.mode == "refresh-local-info":
            print("Refreshing DB rows from local info.json files.")
        elif options.mode == "retry-failed":
            print("Retrying failed subtitle downloads.")
            options = replace(options, video_ids=select_target_video_ids(conn, options))
        elif options.mode == "sync-legacy-analysis":
            script_csv = Path(options.script_csv) if options.script_csv else default_script_csv_path()
            result = import_script_analysis(
                base_dir=Path(options.base_dir),
                script_csv=script_csv,
                analysis_source="legacy_script",
            )
            print(result)
            return
        elif options.mode == "generate-analysis":
            result = sync_generated_analysis(
                conn,
                config=AnalysisConfig(
                    provider=options.analysis_provider,
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                limit=options.analysis_limit,
                overwrite=options.analysis_overwrite,
                video_ids=options.video_ids,
                date_from=options.date_from,
                date_to=options.date_to,
                oldest_first=options.oldest_first,
            )
            print(result)
            return
        elif options.mode == "generate-ad-analysis":
            result = sync_generated_ad_analysis(
                conn,
                config=AnalysisConfig(
                    provider=options.analysis_provider,
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                limit=options.analysis_limit,
                overwrite=options.analysis_overwrite,
                video_ids=options.video_ids,
                date_from=options.date_from,
                date_to=options.date_to,
                oldest_first=options.oldest_first,
            )
            print(result)
            return
        elif options.mode == "prepare-analysis-batch":
            batch_path = options.analysis_batch_path or str(
                paths.batches_dir / f"analysis_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            )
            result = prepare_openai_batch_analysis(
                conn,
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                output_path=batch_path,
                limit=options.analysis_limit,
                overwrite=options.analysis_overwrite,
                video_ids=options.video_ids,
                date_from=options.date_from,
                date_to=options.date_to,
                oldest_first=options.oldest_first,
            )
            print(result)
            return
        elif options.mode == "prepare-ad-batch":
            batch_path = options.analysis_batch_path or str(
                paths.batches_dir / f"ad_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            )
            result = prepare_openai_batch_ad_analysis(
                conn,
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                output_path=batch_path,
                limit=options.analysis_limit,
                overwrite=options.analysis_overwrite,
                video_ids=options.video_ids,
                date_from=options.date_from,
                date_to=options.date_to,
                oldest_first=options.oldest_first,
            )
            print(result)
            return
        elif options.mode == "submit-analysis-batch":
            batch_path = options.analysis_batch_path
            if not batch_path:
                raise RuntimeError("--analysis-batch-path is required.")
            result = submit_openai_batch_analysis(
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                input_path=batch_path,
            )
            register_submitted_batch(paths, result)
            print(result)
            return
        elif options.mode == "submit-ad-batch":
            batch_path = options.analysis_batch_path
            if not batch_path:
                raise RuntimeError("--analysis-batch-path is required.")
            result = submit_openai_batch_analysis(
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                input_path=batch_path,
                metadata={
                    "source": "syuka_ops_ad_analysis",
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
            register_submitted_batch(
                paths,
                result,
                kind="ad_analysis",
                analysis_source="generated_openai_ad_batch",
            )
            print(result)
            return
        elif options.mode == "check-analysis-batch":
            if not options.analysis_batch_id:
                raise RuntimeError("--analysis-batch-id is required.")
            result = fetch_openai_batch(
                AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                options.analysis_batch_id,
            )
            print(result)
            return
        elif options.mode == "sync-analysis-batches":
            result = sync_registered_batches(
                conn,
                paths=paths,
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                kinds=("analysis", "ad_analysis"),
            )
            print(result)
            return
        elif options.mode == "apply-analysis-batch":
            result = apply_openai_batch_output(
                conn,
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                output_path=options.analysis_batch_path,
                batch_id=options.analysis_batch_id,
                file_id=options.analysis_batch_output_file_id,
            )
            print(result)
            return
        elif options.mode == "apply-ad-batch":
            result = apply_openai_ad_batch_output(
                conn,
                config=AnalysisConfig(
                    provider="openai",
                    model=options.analysis_model,
                    base_url=options.analysis_base_url,
                    api_key=options.analysis_api_key,
                ),
                output_path=options.analysis_batch_path,
                batch_id=options.analysis_batch_id,
                file_id=options.analysis_batch_output_file_id,
            )
            print(result)
            return

        refresh_targets = set(options.video_ids) if options.video_ids is not None else None
        refresh_videos_from_local_info_json(conn, paths, refresh_targets)
        if not options.skip_transcripts:
            collect_transcripts(conn, paths, options)
        if not options.skip_thumbnails:
            if options.video_ids is not None:
                download_thumbnails_for_video_ids(conn, paths, options.video_ids)
            else:
                download_thumbnails(conn, paths)
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Syuka Ops collector")
    parser.add_argument(
        "--mode",
        choices=[
            "full",
            "incremental",
            "full-shorts",
            "incremental-shorts",
            "sync-channel-meta",
            "backfill-info-json",
            "refresh-metrics",
            "refresh-subtitle-meta",
            "refresh-local-info",
            "retry-failed",
            "sync-legacy-analysis",
            "generate-analysis",
            "generate-ad-analysis",
            "prepare-analysis-batch",
            "prepare-ad-batch",
            "submit-analysis-batch",
            "submit-ad-batch",
            "check-analysis-batch",
            "sync-analysis-batches",
            "apply-analysis-batch",
            "apply-ad-batch",
        ],
        default="incremental",
    )
    parser.add_argument("--base-dir", default=os.environ.get("SYUKA_DATA_DIR", "./data"))
    parser.add_argument("--channel-url", default=None)
    parser.add_argument("--channel-key", choices=[channel.key for channel in channel_configs()], default=None)
    parser.add_argument("--sleep-requests", type=float, default=0.4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--retry-backoff", type=float, default=8.0)
    parser.add_argument("--video-batch-size", type=int, default=0)
    parser.add_argument("--video-batch-index", type=int, default=1)
    parser.add_argument("--skip-thumbnails", action="store_true")
    parser.add_argument("--skip-transcripts", action="store_true")
    parser.add_argument("--cookies-from-browser", default=os.environ.get("YT_DLP_COOKIES_FROM_BROWSER"))
    parser.add_argument("--cookies-file", default=os.environ.get("YT_DLP_COOKIES_FILE"))
    parser.add_argument("--video-id", dest="video_ids", action="append", default=None)
    parser.add_argument("--date-from", default=None, help="Filter target videos from this upload date (YYYY-MM-DD)")
    parser.add_argument("--date-to", default=None, help="Filter target videos through this upload date (YYYY-MM-DD)")
    parser.add_argument("--oldest-first", action="store_true", help="Process older upload dates first")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=90,
        help="For refresh-metrics, refresh videos uploaded within the last N days when no explicit date range is given",
    )
    parser.add_argument("--script-csv", default=None, help="Legacy script.csv path used for summary/keyword DB sync")
    parser.add_argument("--analysis-provider", choices=["ollama", "openai"], default=os.environ.get("SYUKA_ANALYSIS_PROVIDER", "openai"))
    parser.add_argument("--analysis-model", default=os.environ.get("SYUKA_ANALYSIS_MODEL", "gpt-5-mini"))
    parser.add_argument("--analysis-base-url", default=os.environ.get("SYUKA_ANALYSIS_BASE_URL", "https://api.openai.com/v1"))
    parser.add_argument("--analysis-api-key", default=os.environ.get("SYUKA_ANALYSIS_API_KEY") or os.environ.get("OPENAI_API_KEY"))
    parser.add_argument("--analysis-limit", type=int, default=0, help="Max transcript rows to analyze; 0 means no limit")
    parser.add_argument("--analysis-overwrite", action="store_true", help="Re-generate analysis even when summary/keywords already exist")
    parser.add_argument("--analysis-batch-path", default=None, help="Path to batch input/output jsonl")
    parser.add_argument("--analysis-batch-id", default=None, help="OpenAI batch id")
    parser.add_argument("--analysis-batch-output-file-id", default=None, help="OpenAI output file id")
    return parser


def options_from_args(args: argparse.Namespace) -> CollectOptions:
    return CollectOptions(
        mode=args.mode,
        base_dir=args.base_dir,
        channel_url=args.channel_url,
        channel_key=args.channel_key,
        sleep_requests=args.sleep_requests,
        retries=args.retries,
        max_attempts=args.max_attempts,
        retry_backoff=args.retry_backoff,
        video_batch_size=args.video_batch_size,
        video_batch_index=args.video_batch_index,
        skip_thumbnails=args.skip_thumbnails,
        skip_transcripts=args.skip_transcripts,
        cookies_from_browser=args.cookies_from_browser,
        cookies_file=args.cookies_file,
        video_ids=args.video_ids,
        date_from=args.date_from,
        date_to=args.date_to,
        oldest_first=args.oldest_first,
        recent_days=args.recent_days,
        script_csv=args.script_csv,
        analysis_provider=args.analysis_provider,
        analysis_model=args.analysis_model,
        analysis_base_url=args.analysis_base_url,
        analysis_api_key=args.analysis_api_key,
        analysis_limit=args.analysis_limit,
        analysis_overwrite=args.analysis_overwrite,
        analysis_batch_path=args.analysis_batch_path,
        analysis_batch_id=args.analysis_batch_id,
        analysis_batch_output_file_id=args.analysis_batch_output_file_id,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    options = options_from_args(args)
    run_collect(options)


if __name__ == "__main__":
    main()

