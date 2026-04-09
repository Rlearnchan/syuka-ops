from __future__ import annotations

import csv
import json
import os
import re
from functools import lru_cache
from pathlib import Path


SUMMARY_PREFIX_RE = re.compile(
    r"^(?:다음은 제공된 스크립트의 5~10줄 요약입니다\.?|##\s*스크립트 요약\s*\(5~10줄\))\s*",
    re.MULTILINE,
)
WHITESPACE_RE = re.compile(r"\s+")


def default_script_csv_path() -> Path:
    return Path(__file__).resolve().parents[3] / "scripts" / "script.csv"


def script_csv_path() -> Path:
    configured = os.environ.get("SYUKA_SCRIPT_CSV")
    return Path(configured).expanduser().resolve() if configured else default_script_csv_path()


def load_script_index() -> dict[str, dict[str, object]]:
    path = script_csv_path()
    if not path.exists():
        return {}
    stat = path.stat()
    return _load_script_index_cached(str(path), stat.st_mtime_ns)


def clear_script_index_cache() -> None:
    _load_script_index_cached.cache_clear()


def default_legacy_script_csv_exists() -> bool:
    return default_script_csv_path().exists()


@lru_cache(maxsize=4)
def _load_script_index_cached(path_str: str, _mtime_ns: int) -> dict[str, dict[str, object]]:
    path = Path(path_str)
    records: dict[str, dict[str, object]] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            video_id = (row.get("video_id") or "").strip()
            if not video_id:
                continue
            records[video_id] = {
                "date": (row.get("date") or "").strip(),
                "title": (row.get("title") or "").strip(),
                "summary": clean_summary_text(row.get("summary") or ""),
                "keywords": parse_keywords(row.get("keyword") or ""),
                "dialogue": row.get("dialogue") or "",
            }
    return records


def get_script_record(video_id: str) -> dict[str, object] | None:
    return load_script_index().get(video_id)


def parse_keywords(raw: str) -> list[str]:
    if not raw or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    keywords: list[str] = []
    for item in parsed:
        keyword = str(item).strip()
        if keyword:
            keywords.append(keyword)
    return keywords


def clean_summary_text(summary: str) -> str:
    cleaned = (summary or "").replace("\r", "\n").strip()
    cleaned = SUMMARY_PREFIX_RE.sub("", cleaned).strip()
    lines = [line.strip(" -*") for line in cleaned.splitlines() if line.strip()]
    cleaned = "\n".join(lines).strip()
    cleaned = cleaned.replace("\n\n", "\n")
    return cleaned


def summary_preview(summary: str, *, max_lines: int = 3, max_chars: int = 360) -> str:
    cleaned = clean_summary_text(summary)
    if not cleaned:
        return ""
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    selected: list[str] = []
    total = 0
    for line in lines:
        line = WHITESPACE_RE.sub(" ", line)
        extra = len(line) + (1 if selected else 0)
        if selected and (len(selected) >= max_lines or total + extra > max_chars):
            break
        selected.append(line)
        total += extra
    if not selected:
        return ""
    preview = "\n".join(f"- {line}" for line in selected)
    if len(cleaned) > len(" ".join(selected)):
        preview += "\n- ..."
    return preview
