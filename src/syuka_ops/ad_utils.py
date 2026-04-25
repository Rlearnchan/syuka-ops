from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from .config import AppPaths, resolve_stored_path


PAID_PATTERNS = [
    re.compile(r"(?P<advertiser>[^.\n]{1,80}?)(?:의|의\s+)?유료\s*광고가\s*포함", re.IGNORECASE),
    re.compile(r"(?:본\s*)?(?:영상|콘텐츠)[^\n.]{0,40}?(?P<advertiser>[^.\n]{1,80}?)(?:의|의\s+)?유료\s*광고", re.IGNORECASE),
    re.compile(r"(?:본\s*)?(?:영상|콘텐츠)[^\n.]{0,40}?(?P<advertiser>[^.\n]{1,80}?)(?:의|의\s+)?지원을\s*받아", re.IGNORECASE),
    re.compile(r"(?:제작\s*지원|협찬)\s*[:：]?\s*(?P<advertiser>[^,\n.]{1,80})", re.IGNORECASE),
]

AD_SIGNAL_KEYWORDS = [
    "유료광고",
    "유료 광고",
    "광고",
    "협찬",
    "제작지원",
    "제작 지원",
    "지원을 받아",
    "브랜디드",
    "프로모션",
    "이벤트",
    "혜택",
    "가입",
    "다운로드",
    "앱 설치",
    "바로가기",
    "링크",
]


def load_info_json(info_json_path: str | None) -> dict[str, Any]:
    if not info_json_path:
        return {}
    candidate_base_dirs: list[str] = []
    env_base_dir = os.environ.get("SYUKA_DATA_DIR")
    if env_base_dir:
        candidate_base_dirs.append(env_base_dir)
    if "./data" not in candidate_base_dirs:
        candidate_base_dirs.append("./data")

    for base_dir in candidate_base_dirs:
        app_paths = AppPaths.from_base_dir(base_dir)
        path = resolve_stored_path(info_json_path, base_dir=app_paths.base_dir, search_dirs=[app_paths.raw_dir])
        if path is None:
            path = Path(info_json_path)
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
    return {}


def normalize_advertiser_name(name: str) -> str:
    cleaned = (name or "").strip()
    cleaned = cleaned.strip(" .,:;/[](){}<>")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def description_snippet(description: str, *, max_chars: int = 220) -> str:
    text = (description or "").replace("\r", "\n")
    text = re.sub(r"\n+", " / ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def query_snippet(text: str, query: str, *, max_chars: int = 220) -> str:
    flat = re.sub(r"\s+", " ", (text or "").replace("\r", "\n")).strip()
    if not query:
        return description_snippet(flat, max_chars=max_chars)
    index = flat.lower().find(query.lower())
    if index < 0:
        return description_snippet(flat, max_chars=max_chars)
    start = max(0, index - max_chars // 3)
    end = min(len(flat), start + max_chars)
    if end - start < max_chars:
        start = max(0, end - max_chars)
    snippet = flat[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(flat):
        snippet += "..."
    return snippet


def has_ad_signal(text: str) -> bool:
    lower = (text or "").lower()
    return any(keyword.lower() in lower for keyword in AD_SIGNAL_KEYWORDS)


def detect_paid_promotion(description: str) -> dict[str, str] | None:
    text = (description or "").strip()
    if not text:
        return None

    for pattern in PAID_PATTERNS:
        match = pattern.search(text)
        if match:
            advertiser = normalize_advertiser_name(match.groupdict().get("advertiser", ""))
            return {
                "advertiser": advertiser,
                "matched_text": match.group(0).strip(),
                "snippet": description_snippet(text),
            }

    if has_ad_signal(text):
        return {
            "advertiser": "",
            "matched_text": "설명에 광고/지원 힌트 포함",
            "snippet": description_snippet(text),
        }
    return None
