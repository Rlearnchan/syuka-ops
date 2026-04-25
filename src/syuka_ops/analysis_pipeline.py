from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .ad_utils import load_info_json, normalize_advertiser_name, should_analyze_ad_description
from .db import (
    pending_video_ad_analysis_rows,
    pending_video_analysis_rows,
    upsert_video_ad_analysis,
    upsert_video_analysis,
)

KEYWORD_EXTRACTION_SYSTEM_PROMPT = """?뱀떊? ?덉뭅?붾뱶 諛⑹넚 ?ㅽ겕由쏀듃?먯꽌 ?듭떖 ?ㅼ썙?쒕? 異붿텧?섎뒗 ?꾩슦誘몄엯?덈떎.
諛섎뱶???꾨옒 JSON ?뺤떇?쇰줈留??듯븯?몄슂. ?ㅻⅨ ?ㅻ챸?대굹 肄붾뱶 釉붾줉? 湲덉??낅땲??

?묐떟 ?뺤떇:
{
  "keywords": ["?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??"]
}

洹쒖튃:
- keywords 諛곗뿴? 8~12媛?- 以묐났 ?녿뒗 ?듭떖 紐낆궗 ?꾩＜
- ?쒕ぉ蹂대떎 ?ㅼ젣 ?ㅽ겕由쏀듃?먯꽌 鍮꾩쨷????媛쒕뀗???곗꽑
- 諛섎뱶???좏슚??JSON留?異쒕젰
"""

KEYWORD_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "keywords": {
            "type": "array",
            "items": {"type": "string"},
        }
    },
    "required": ["keywords"],
}

SUMMARY_EXTRACTION_SYSTEM_PROMPT = """?뱀떊? ?덉뭅?붾뱶 諛⑹넚 ?ㅽ겕由쏀듃瑜??붿빟?섎뒗 ?꾩슦誘몄엯?덈떎.
?ㅻ챸 ?놁씠 ?붿빟 蹂몃Ц留?異쒕젰?섏꽭??

洹쒖튃:
- 4~6臾몄옣
- ?ъ떎 以묒떖
- ?먯뿰?ㅻ윭???쒓뎅??臾몃떒
- 留덊겕?ㅼ슫 ?쒕ぉ, 遺덈┸, `?붿빟:` 媛숈? 癒몃━留?湲덉?
"""

COMBINED_ANALYSIS_SYSTEM_PROMPT = """?뱀떊? ?덉뭅?붾뱶 諛⑹넚 ?ㅽ겕由쏀듃瑜?遺꾩꽍?섎뒗 ?꾩슦誘몄엯?덈떎.
諛섎뱶???꾨옒 JSON ?뺤떇?쇰줈留??듯븯?몄슂. ?ㅻⅨ ?ㅻ챸, 肄붾뱶釉붾줉, 二쇱꽍? 湲덉??낅땲??

?묐떟 ?뺤떇:
{
  "summary": "4~6臾몄옣 ?붿빟",
  "keywords": ["?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??", "?ㅼ썙??"]
}

洹쒖튃:
- summary??4~6臾몄옣, ?ъ떎 以묒떖, ?먯뿰?ㅻ윭???쒓뎅??臾몃떒
- keywords??8~12媛? 以묐났 ?녿뒗 ?듭떖 紐낆궗 ?꾩＜
- 留덊겕?ㅼ슫, 踰덊샇, 遺덈┸, `?붿빟:` 媛숈? 癒몃━留?湲덉?
- 諛섎뱶???좏슚??JSON留?異쒕젰
"""

ANALYSIS_RESULT_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {"type": "string"},
        "keywords": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["summary", "keywords"],
}

AD_ANALYSIS_SYSTEM_PROMPT = """당신은 유튜브 영상 설명란에서 광고 또는 협찬 여부를 추출하는 도우미입니다.
반드시 아래 JSON 형식으로만 응답하세요. 다른 설명, 코드블록, 주석은 금지합니다.

응답 형식:
{
  "ad_detected": true,
  "advertiser": "가장 가능성이 높은 광고주 이름",
  "advertiser_candidates": ["후보1", "후보2", "후보3"],
  "evidence_text": "설명란에서 광고/협찬으로 판단한 핵심 문장",
  "description_excerpt": "설명란에서 관련 부분만 짧게 발췌",
  "confidence": 0.0
}

규칙:
- 광고/협찬이 없다고 판단하면 ad_detected는 false
- advertiser_candidates는 가능성 높은 순서대로 최대 3개
- advertiser는 advertiser_candidates의 첫 번째 값과 같게 맞추기
- 광고주를 특정할 수 없으면 advertiser는 빈 문자열, advertiser_candidates는 빈 배열
- evidence_text와 description_excerpt는 1~2문장 이내
- confidence는 0.0~1.0
- 반드시 유효한 JSON만 출력
"""

AD_ANALYSIS_RESULT_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "ad_detected": {"type": "boolean"},
        "advertiser": {"type": "string"},
        "advertiser_candidates": {
            "type": "array",
            "items": {"type": "string"},
        },
        "evidence_text": {"type": "string"},
        "description_excerpt": {"type": "string"},
        "confidence": {"type": "number"},
    },
    "required": ["ad_detected", "advertiser", "advertiser_candidates", "evidence_text", "description_excerpt", "confidence"],
}


@dataclass(frozen=True)
class AnalysisConfig:
    provider: str = "openai"
    model: str = "gpt-5-mini"
    base_url: str = "https://api.openai.com/v1"
    api_key: str | None = None
    temperature: float = 0.2
    timeout: int = 120
    max_retries: int = 3
    max_chars: int = 15000


def _trim_dialogue(dialogue: str, max_chars: int) -> str:
    doc = (dialogue or "").strip()
    if len(doc) <= max_chars:
        return doc
    half = max_chars // 2
    return doc[:half] + "\n... [以묐왂] ...\n" + doc[-half:]


def build_keyword_prompt(title: str, date: str, dialogue: str, max_chars: int = 15000) -> str:
    doc = _trim_dialogue(dialogue, max_chars)
    return f"[?쒕ぉ] {title}\n[寃뚯떆?? {date}\n[?ㅽ겕由쏀듃]\n{doc}"


def build_summary_prompt(title: str, date: str, dialogue: str, max_chars: int = 15000) -> str:
    doc = _trim_dialogue(dialogue, max_chars)
    return f"[?쒕ぉ] {title}\n[寃뚯떆?? {date}\n[?ㅽ겕由쏀듃]\n{doc}\n\n???ㅽ겕由쏀듃瑜?4~6臾몄옣?쇰줈 ?붿빟?댁＜?몄슂."


def build_combined_analysis_prompt(title: str, date: str, dialogue: str, max_chars: int = 15000) -> str:
    doc = _trim_dialogue(dialogue, max_chars)
    return f"[?쒕ぉ] {title}\n[寃뚯떆?? {date}\n[?ㅽ겕由쏀듃]\n{doc}"


def build_ad_analysis_prompt(title: str, date: str, description: str, max_chars: int = 12000) -> str:
    doc = _trim_dialogue(description, max_chars)
    return f"[제목] {title}\n[게시일] {date}\n[설명란]\n{doc}"


def _normalize_advertiser_candidates(raw_candidates: list[Any], advertiser: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add_candidate(value: Any) -> None:
        normalized = normalize_advertiser_name(str(value or ""))
        if not normalized:
            return
        key = normalized.casefold()
        if key in seen:
            return
        seen.add(key)
        candidates.append(normalized)

    add_candidate(advertiser)
    for candidate in raw_candidates:
        add_candidate(candidate)
    return candidates[:3]


def _prepare_ad_analysis_targets(rows: list[Any]) -> dict[str, Any]:
    targets: list[dict[str, Any]] = []
    filtered_rows = 0
    missing_description_rows = 0

    for row in rows:
        info = load_info_json(row["info_json_path"])
        description = str(info.get("description") or "").strip()
        if not description:
            missing_description_rows += 1
            continue
        if not should_analyze_ad_description(row["title"] or "", description):
            filtered_rows += 1
            continue
        target = dict(row)
        target["description"] = description
        targets.append(target)

    return {
        "targets": targets,
        "filtered_rows": filtered_rows,
        "missing_description_rows": missing_description_rows,
    }

def check_ollama(config: AnalysisConfig) -> dict[str, Any]:
    response = requests.get(f"{config.base_url}/api/tags", timeout=10)
    response.raise_for_status()
    payload = response.json()
    models = [item.get("name", "") for item in payload.get("models", [])]
    if config.model not in models:
        raise RuntimeError(
            f"Ollama model '{config.model}' is not installed. Available models: {models}"
        )
    return payload


def check_openai(config: AnalysisConfig) -> None:
    if not config.api_key:
        raise RuntimeError("OpenAI provider瑜??ъ슜?섎젮硫?OPENAI_API_KEY ?먮뒗 SYUKA_ANALYSIS_API_KEY媛 ?꾩슂?⑸땲??")


def _openai_headers(config: AnalysisConfig) -> dict[str, str]:
    if not config.api_key:
        raise RuntimeError("OpenAI API key媛 ?놁뒿?덈떎.")
    return {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }


def _post_ollama_chat(
    config: AnalysisConfig,
    *,
    system_prompt: str,
    user_prompt: str,
    json_mode: bool = False,
) -> str:
    payload: dict[str, Any] = {
        "model": config.model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": config.temperature,
        "stream": False,
    }
    if json_mode:
        payload["format"] = KEYWORD_JSON_SCHEMA

    for attempt in range(1, config.max_retries + 1):
        try:
            response = requests.post(
                f"{config.base_url}/api/chat",
                json=payload,
                timeout=config.timeout,
            )
            response.raise_for_status()
            content = response.json().get("message", {}).get("content", "").strip()
            if not content:
                raise ValueError("鍮??묐떟??諛쏆븯?듬땲??")
            return content
        except (requests.RequestException, ValueError):
            if attempt >= config.max_retries:
                raise
            time.sleep(2 ** attempt)
    return ""


def _extract_openai_output_text(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    outputs = payload.get("output") or []
    for item in outputs:
        if not isinstance(item, dict):
            continue
        for content_item in item.get("content") or []:
            if not isinstance(content_item, dict):
                continue
            text = content_item.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    raise ValueError("OpenAI ?묐떟?먯꽌 ?띿뒪?몃? 李얠? 紐삵뻽?듬땲??")


def _post_openai_response(
    config: AnalysisConfig,
    *,
    system_prompt: str,
    user_prompt: str,
    json_mode: bool = False,
) -> str:
    payload: dict[str, Any] = {
        "model": config.model,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    if json_mode:
        payload["text"] = {
            "format": {
                "type": "json_schema",
                "name": "analysis_keywords",
                "schema": KEYWORD_JSON_SCHEMA,
                "strict": True,
            }
        }

    for attempt in range(1, config.max_retries + 1):
        try:
            response = requests.post(
                f"{config.base_url.rstrip('/')}/responses",
                headers=_openai_headers(config),
                json=payload,
                timeout=config.timeout,
            )
            response.raise_for_status()
            return _extract_openai_output_text(response.json())
        except (requests.RequestException, ValueError):
            if attempt >= config.max_retries:
                raise
            time.sleep(2 ** attempt)
    return ""


def _post_generation(
    config: AnalysisConfig,
    *,
    system_prompt: str,
    user_prompt: str,
    json_mode: bool = False,
) -> str:
    if config.provider == "openai":
        return _post_openai_response(
            config,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            json_mode=json_mode,
        )
    return _post_ollama_chat(
        config,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        json_mode=json_mode,
    )


def _normalize_keywords(raw_keywords: list[Any]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in raw_keywords:
        keyword = str(item or "").strip()
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        deduped.append(keyword)
    return deduped[:12]


def _parse_keyword_response(content: str) -> list[str]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        cleaned = "\n".join(lines[1:-1]) if len(lines) > 2 else cleaned
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(cleaned)
        raw_keywords = parsed.get("keywords", [])
        if isinstance(raw_keywords, list):
            return _normalize_keywords(raw_keywords)
    except json.JSONDecodeError:
        pass

    match = re.search(r'"keywords"\s*:\s*\[(.*?)\]', cleaned, re.DOTALL)
    if not match:
        return []
    raw_keywords = re.findall(r'"([^"]+)"', match.group(1))
    return _normalize_keywords(raw_keywords)


def _clean_generated_summary(text: str) -> str:
    summary = (text or "").strip()
    prefixes = ["?붿빟:", "?ㅼ쓬? ?붿빟?낅땲??", "?ㅼ쓬? ?ㅽ겕由쏀듃 ?붿빟?낅땲??"]
    for prefix in prefixes:
        if summary.startswith(prefix):
            summary = summary[len(prefix) :].strip()
    summary = re.sub(r"^#+\s*", "", summary)
    summary = re.sub(r"(?m)^\s*[-*]\s+", "", summary)
    summary = re.sub(r"\n{3,}", "\n\n", summary).strip()
    return summary


def _parse_analysis_result_response(content: str) -> dict[str, Any]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        cleaned = "\n".join(lines[1:-1]) if len(lines) > 2 else cleaned
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    parsed = json.loads(cleaned)
    return {
        "summary": _clean_generated_summary(str(parsed.get("summary") or "")),
        "keywords": _normalize_keywords(parsed.get("keywords") or []),
    }


def generate_video_analysis(
    config: AnalysisConfig,
    *,
    title: str,
    upload_date: str,
    dialogue: str,
) -> dict[str, Any]:
    keyword_prompt = build_keyword_prompt(title, upload_date, dialogue, config.max_chars)
    summary_prompt = build_summary_prompt(title, upload_date, dialogue, config.max_chars)

    keywords_content = _post_generation(
        config,
        system_prompt=KEYWORD_EXTRACTION_SYSTEM_PROMPT,
        user_prompt=keyword_prompt,
        json_mode=True,
    )
    summary_content = _post_generation(
        config,
        system_prompt=SUMMARY_EXTRACTION_SYSTEM_PROMPT,
        user_prompt=summary_prompt,
        json_mode=False,
    )

    keywords = _parse_keyword_response(keywords_content)
    if len(keywords) < 5:
        retry_prompt = keyword_prompt + "\n\n以묒슂: ?덈Т ?볤쾶 ?≪? 留먭퀬 ?ㅼ젣濡?以묒슂???듭떖 ?ㅼ썙?쒕? 8媛??댁긽 二쇱꽭??"
        retry_content = _post_generation(
            config,
            system_prompt=KEYWORD_EXTRACTION_SYSTEM_PROMPT,
            user_prompt=retry_prompt,
            json_mode=True,
        )
        retry_keywords = _parse_keyword_response(retry_content)
        if len(retry_keywords) > len(keywords):
            keywords = retry_keywords

    return {
        "summary": _clean_generated_summary(summary_content),
        "keywords": keywords,
    }


def _parse_ad_analysis_result_response(content: str) -> dict[str, Any]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        cleaned = "\n".join(lines[1:-1]) if len(lines) > 2 else cleaned
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    parsed = json.loads(cleaned)
    advertiser = normalize_advertiser_name(str(parsed.get("advertiser") or ""))
    advertiser_candidates = _normalize_advertiser_candidates(parsed.get("advertiser_candidates") or [], advertiser)
    if advertiser_candidates:
        advertiser = advertiser_candidates[0]
    evidence_text = str(parsed.get("evidence_text") or "").strip()
    description_excerpt = str(parsed.get("description_excerpt") or "").strip()
    try:
        confidence = float(parsed.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(confidence, 1.0))
    return {
        "ad_detected": bool(parsed.get("ad_detected")),
        "advertiser": advertiser,
        "advertiser_candidates": advertiser_candidates,
        "evidence_text": evidence_text,
        "description_excerpt": description_excerpt,
        "confidence": confidence,
    }


def build_openai_ad_batch_requests(rows: list[Any], *, config: AnalysisConfig) -> list[dict[str, Any]]:
    payload_rows: list[dict[str, Any]] = []
    for row in rows:
        description = str(row.get("description") or "").strip()
        if not description:
            continue
        payload_rows.append(
            {
                "custom_id": f"ad-analysis::{row['video_id']}",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": config.model,
                    "input": [
                        {"role": "system", "content": AD_ANALYSIS_SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": build_ad_analysis_prompt(
                                row["title"] or "",
                                row["upload_date"] or "",
                                description,
                                config.max_chars,
                            ),
                        },
                    ],
                    "text": {
                        "format": {
                            "type": "json_schema",
                            "name": "ad_analysis_payload",
                            "schema": AD_ANALYSIS_RESULT_JSON_SCHEMA,
                            "strict": True,
                        }
                    },
                },
            }
        )
    return payload_rows


def write_openai_ad_batch_input(
    rows: list[Any],
    *,
    config: AnalysisConfig,
    output_path: str | Path,
) -> Path:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for item in build_openai_ad_batch_requests(rows, config=config):
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")
    return target

def build_openai_batch_requests(rows: list[Any], *, config: AnalysisConfig) -> list[dict[str, Any]]:
    payload_rows: list[dict[str, Any]] = []
    for row in rows:
        payload_rows.append(
            {
                "custom_id": f"analysis::{row['video_id']}",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": config.model,
                    "input": [
                        {"role": "system", "content": COMBINED_ANALYSIS_SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": build_combined_analysis_prompt(
                                row["title"] or "",
                                row["upload_date"] or "",
                                row["dialogue"] or "",
                                config.max_chars,
                            ),
                        },
                    ],
                    "text": {
                        "format": {
                            "type": "json_schema",
                            "name": "analysis_payload",
                            "schema": ANALYSIS_RESULT_JSON_SCHEMA,
                            "strict": True,
                        }
                    },
                },
            }
        )
    return payload_rows


def write_openai_batch_input(
    rows: list[Any],
    *,
    config: AnalysisConfig,
    output_path: str | Path,
) -> Path:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for item in build_openai_batch_requests(rows, config=config):
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")
    return target


def _openai_file_upload(config: AnalysisConfig, input_path: Path) -> dict[str, Any]:
    with input_path.open("rb") as handle:
        response = requests.post(
            f"{config.base_url.rstrip('/')}/files",
            headers={"Authorization": f"Bearer {config.api_key}"},
            data={"purpose": "batch"},
            files={"file": (input_path.name, handle, "application/jsonl")},
            timeout=config.timeout,
        )
    response.raise_for_status()
    return response.json()


def _openai_create_batch(
    config: AnalysisConfig,
    *,
    input_file_id: str,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "input_file_id": input_file_id,
        "endpoint": "/v1/responses",
        "completion_window": "24h",
    }
    if metadata:
        payload["metadata"] = metadata
    response = requests.post(
        f"{config.base_url.rstrip('/')}/batches",
        headers=_openai_headers(config),
        json=payload,
        timeout=config.timeout,
    )
    response.raise_for_status()
    return response.json()


def fetch_openai_batch(config: AnalysisConfig, batch_id: str) -> dict[str, Any]:
    response = requests.get(
        f"{config.base_url.rstrip('/')}/batches/{batch_id}",
        headers=_openai_headers(config),
        timeout=config.timeout,
    )
    response.raise_for_status()
    return response.json()


def fetch_openai_file_content(config: AnalysisConfig, file_id: str) -> str:
    response = requests.get(
        f"{config.base_url.rstrip('/')}/files/{file_id}/content",
        headers=_openai_headers(config),
        timeout=config.timeout,
    )
    response.raise_for_status()
    return response.text


def prepare_openai_batch_analysis(
    conn,
    *,
    config: AnalysisConfig,
    output_path: str | Path,
    limit: int = 0,
    overwrite: bool = False,
    video_ids: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    oldest_first: bool = False,
) -> dict[str, Any]:
    check_openai(config)
    rows = pending_video_analysis_rows(
        conn,
        limit=limit,
        overwrite=overwrite,
        video_ids=video_ids,
        date_from=date_from,
        date_to=date_to,
        oldest_first=oldest_first,
    )
    target = write_openai_batch_input(rows, config=config, output_path=output_path)
    return {
        "prepared_rows": len(rows),
        "output_path": str(target),
        "model": config.model,
    }


def submit_openai_batch_analysis(
    *,
    config: AnalysisConfig,
    input_path: str | Path,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    check_openai(config)
    target = Path(input_path)
    upload = _openai_file_upload(config, target)
    batch = _openai_create_batch(
        config,
        input_file_id=upload["id"],
        metadata=metadata or {
            "source": "syuka_ops",
            "created_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    return {
        "input_path": str(target),
        "input_file_id": upload["id"],
        "batch_id": batch["id"],
        "status": batch.get("status", ""),
    }


def apply_openai_batch_output(
    conn,
    *,
    config: AnalysisConfig,
    output_path: str | Path | None = None,
    batch_id: str | None = None,
    file_id: str | None = None,
    analysis_source: str = "generated_openai_batch",
) -> dict[str, Any]:
    check_openai(config)

    resolved_file_id = file_id
    payload_text: str
    if batch_id:
        batch = fetch_openai_batch(config, batch_id)
        resolved_file_id = batch.get("output_file_id")
        if not resolved_file_id:
            raise RuntimeError(
                f"Batch {batch_id} has no output_file_id yet. Current status: {batch.get('status')}"
            )
    if resolved_file_id:
        payload_text = fetch_openai_file_content(config, resolved_file_id)
        if output_path:
            target = Path(output_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(payload_text, encoding="utf-8")
    elif output_path:
        payload_text = Path(output_path).read_text(encoding="utf-8")
    else:
        raise RuntimeError("output_path ?먮뒗 batch_id/file_id 以??섎굹媛 ?꾩슂?⑸땲??")

    processed = 0
    failed_rows: list[str] = []
    for line in payload_text.splitlines():
        text = line.strip()
        if not text:
            continue
        item = json.loads(text)
        custom_id = str(item.get("custom_id") or "")
        if not custom_id.startswith("analysis::"):
            continue
        video_id = custom_id.split("::", 1)[1]
        body = item.get("response", {}).get("body", {})
        try:
            parsed = _parse_analysis_result_response(_extract_openai_output_text(body))
        except Exception:
            failed_rows.append(video_id)
            continue

        upsert_video_analysis(
            conn,
            {
                "video_id": video_id,
                "summary": parsed["summary"],
                "keywords_json": json.dumps(parsed["keywords"], ensure_ascii=False),
                "analysis_source": analysis_source,
            },
        )
        processed += 1

    conn.commit()
    return {
        "processed_rows": processed,
        "failed_rows": failed_rows,
        "analysis_source": analysis_source,
        "output_file_id": resolved_file_id,
    }


def sync_generated_analysis(
    conn,
    *,
    config: AnalysisConfig,
    limit: int = 0,
    overwrite: bool = False,
    video_ids: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    oldest_first: bool = False,
    analysis_source: str | None = None,
) -> dict[str, Any]:
    if config.provider == "openai":
        check_openai(config)
    else:
        check_ollama(config)

    if analysis_source is None:
        analysis_source = f"generated_{config.provider}"
    rows = pending_video_analysis_rows(
        conn,
        limit=limit,
        overwrite=overwrite,
        video_ids=video_ids,
        date_from=date_from,
        date_to=date_to,
        oldest_first=oldest_first,
    )

    total = len(rows)
    if total == 0:
        print("[analysis] 泥섎━??遺꾩꽍 ??곸씠 ?놁뒿?덈떎.", flush=True)
        return {
            "processed_rows": 0,
            "requested_limit": limit,
            "overwrite": overwrite,
            "analysis_source": analysis_source,
            "model": config.model,
        }

    print(
        f"[analysis] ?쒖옉: 珥?{total}嫄?| model={config.model} | source={analysis_source}",
        flush=True,
    )
    processed = 0
    for index, row in enumerate(rows, start=1):
        title = (row["title"] or "").strip()
        title_preview = title[:60] + ("..." if len(title) > 60 else "")
        print(f"[analysis] [{index}/{total}] {row['video_id']} | {title_preview}", flush=True)
        generated = generate_video_analysis(
            config,
            title=row["title"] or "",
            upload_date=row["upload_date"] or "",
            dialogue=row["dialogue"] or "",
        )
        upsert_video_analysis(
            conn,
            {
                "video_id": row["video_id"],
                "summary": generated["summary"],
                "keywords_json": json.dumps(generated["keywords"], ensure_ascii=False),
                "analysis_source": analysis_source,
            },
        )
        processed += 1
        print(
            f"[analysis] ?꾨즺 [{index}/{total}] {row['video_id']} | keywords={len(generated['keywords'])}",
            flush=True,
        )
        sys.stdout.flush()

    conn.commit()
    print(f"[analysis] 醫낅즺: {processed}嫄?泥섎━ ?꾨즺", flush=True)
    return {
        "processed_rows": processed,
        "requested_limit": limit,
        "overwrite": overwrite,
        "analysis_source": analysis_source,
        "model": config.model,
    }

def prepare_openai_batch_ad_analysis(
    conn,
    *,
    config: AnalysisConfig,
    output_path: str | Path,
    limit: int = 0,
    overwrite: bool = False,
    video_ids: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    oldest_first: bool = False,
) -> dict[str, Any]:
    check_openai(config)
    rows = pending_video_ad_analysis_rows(
        conn,
        limit=limit,
        overwrite=overwrite,
        video_ids=video_ids,
        date_from=date_from,
        date_to=date_to,
        oldest_first=oldest_first,
    )
    screening = _prepare_ad_analysis_targets(rows)
    targets = screening["targets"]
    target = write_openai_ad_batch_input(targets, config=config, output_path=output_path)
    prepared_rows = len(targets)
    return {
        "prepared_rows": prepared_rows,
        "candidate_rows": len(rows),
        "filtered_rows": screening["filtered_rows"],
        "missing_description_rows": screening["missing_description_rows"],
        "output_path": str(target),
        "model": config.model,
    }


def apply_openai_ad_batch_output(
    conn,
    *,
    config: AnalysisConfig,
    output_path: str | Path | None = None,
    batch_id: str | None = None,
    file_id: str | None = None,
    analysis_source: str = "generated_openai_ad_batch",
) -> dict[str, Any]:
    check_openai(config)

    resolved_file_id = file_id
    payload_text: str
    if batch_id:
        batch = fetch_openai_batch(config, batch_id)
        resolved_file_id = batch.get("output_file_id")
        if not resolved_file_id:
            raise RuntimeError(
                f"Batch {batch_id} has no output_file_id yet. Current status: {batch.get('status')}"
            )
    if resolved_file_id:
        payload_text = fetch_openai_file_content(config, resolved_file_id)
        if output_path:
            target = Path(output_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(payload_text, encoding="utf-8")
    elif output_path:
        payload_text = Path(output_path).read_text(encoding="utf-8")
    else:
        raise RuntimeError("output_path 또는 batch_id/file_id 중 하나가 필요합니다.")

    processed = 0
    failed_rows: list[str] = []
    for line in payload_text.splitlines():
        text = line.strip()
        if not text:
            continue
        item = json.loads(text)
        custom_id = str(item.get("custom_id") or "")
        if not custom_id.startswith("ad-analysis::"):
            continue
        video_id = custom_id.split("::", 1)[1]
        body = item.get("response", {}).get("body", {})
        try:
            parsed = _parse_ad_analysis_result_response(_extract_openai_output_text(body))
        except Exception:
            failed_rows.append(video_id)
            continue

        upsert_video_ad_analysis(
            conn,
            {
                "video_id": video_id,
                "ad_detected": parsed["ad_detected"],
                "advertiser": parsed["advertiser"],
                "advertiser_candidates_json": json.dumps(parsed["advertiser_candidates"], ensure_ascii=False),
                "evidence_text": parsed["evidence_text"],
                "description_excerpt": parsed["description_excerpt"],
                "confidence": parsed["confidence"],
                "raw_json": json.dumps(parsed, ensure_ascii=False),
                "analysis_source": analysis_source,
            },
        )
        processed += 1

    conn.commit()
    return {
        "processed_rows": processed,
        "failed_rows": failed_rows,
        "analysis_source": analysis_source,
        "output_file_id": resolved_file_id,
    }


def generate_video_ad_analysis(
    config: AnalysisConfig,
    *,
    title: str,
    upload_date: str,
    description: str,
) -> dict[str, Any]:
    content = _post_generation(
        config,
        system_prompt=AD_ANALYSIS_SYSTEM_PROMPT,
        user_prompt=build_ad_analysis_prompt(title, upload_date, description, config.max_chars),
        json_mode=False,
    )
    return _parse_ad_analysis_result_response(content)


def sync_generated_ad_analysis(
    conn,
    *,
    config: AnalysisConfig,
    limit: int = 0,
    overwrite: bool = False,
    video_ids: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    oldest_first: bool = False,
    analysis_source: str | None = None,
) -> dict[str, Any]:
    if config.provider == "openai":
        check_openai(config)
    else:
        check_ollama(config)

    if analysis_source is None:
        analysis_source = f"generated_ad_{config.provider}"
    rows = pending_video_ad_analysis_rows(
        conn,
        limit=limit,
        overwrite=overwrite,
        video_ids=video_ids,
        date_from=date_from,
        date_to=date_to,
        oldest_first=oldest_first,
    )

    processed = 0
    failed_rows: list[str] = []
    screening = _prepare_ad_analysis_targets(rows)
    skipped_rows = screening["filtered_rows"] + screening["missing_description_rows"]
    for row in screening["targets"]:
        try:
            parsed = generate_video_ad_analysis(
                config,
                title=row["title"] or "",
                upload_date=row["upload_date"] or "",
                description=row["description"] or "",
            )
        except Exception:
            failed_rows.append(row["video_id"])
            continue

        upsert_video_ad_analysis(
            conn,
            {
                "video_id": row["video_id"],
                "ad_detected": parsed["ad_detected"],
                "advertiser": parsed["advertiser"],
                "advertiser_candidates_json": json.dumps(parsed["advertiser_candidates"], ensure_ascii=False),
                "evidence_text": parsed["evidence_text"],
                "description_excerpt": parsed["description_excerpt"],
                "confidence": parsed["confidence"],
                "raw_json": json.dumps(parsed, ensure_ascii=False),
                "analysis_source": analysis_source,
            },
        )
        processed += 1

    conn.commit()
    return {
        "processed_rows": processed,
        "failed_rows": failed_rows,
        "skipped_rows": skipped_rows,
        "analysis_source": analysis_source,
        "model": config.model,
    }
