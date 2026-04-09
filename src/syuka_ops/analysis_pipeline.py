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

from .db import pending_video_analysis_rows, upsert_video_analysis

KEYWORD_EXTRACTION_SYSTEM_PROMPT = """당신은 슈카월드 방송 스크립트에서 핵심 키워드를 추출하는 도우미입니다.
반드시 아래 JSON 형식으로만 답하세요. 다른 설명이나 코드 블록은 금지입니다.

응답 형식:
{
  "keywords": ["키워드1", "키워드2", "키워드3", "키워드4", "키워드5", "키워드6", "키워드7", "키워드8"]
}

규칙:
- keywords 배열은 8~12개
- 중복 없는 핵심 명사 위주
- 제목보다 실제 스크립트에서 비중이 큰 개념을 우선
- 반드시 유효한 JSON만 출력
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

SUMMARY_EXTRACTION_SYSTEM_PROMPT = """당신은 슈카월드 방송 스크립트를 요약하는 도우미입니다.
설명 없이 요약 본문만 출력하세요.

규칙:
- 4~6문장
- 사실 중심
- 자연스러운 한국어 문단
- 마크다운 제목, 불릿, `요약:` 같은 머리말 금지
"""

COMBINED_ANALYSIS_SYSTEM_PROMPT = """당신은 슈카월드 방송 스크립트를 분석하는 도우미입니다.
반드시 아래 JSON 형식으로만 답하세요. 다른 설명, 코드블록, 주석은 금지입니다.

응답 형식:
{
  "summary": "4~6문장 요약",
  "keywords": ["키워드1", "키워드2", "키워드3", "키워드4", "키워드5", "키워드6", "키워드7", "키워드8"]
}

규칙:
- summary는 4~6문장, 사실 중심, 자연스러운 한국어 문단
- keywords는 8~12개, 중복 없는 핵심 명사 위주
- 마크다운, 번호, 불릿, `요약:` 같은 머리말 금지
- 반드시 유효한 JSON만 출력
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
    return doc[:half] + "\n... [중략] ...\n" + doc[-half:]


def build_keyword_prompt(title: str, date: str, dialogue: str, max_chars: int = 15000) -> str:
    doc = _trim_dialogue(dialogue, max_chars)
    return f"[제목] {title}\n[게시일] {date}\n[스크립트]\n{doc}"


def build_summary_prompt(title: str, date: str, dialogue: str, max_chars: int = 15000) -> str:
    doc = _trim_dialogue(dialogue, max_chars)
    return f"[제목] {title}\n[게시일] {date}\n[스크립트]\n{doc}\n\n위 스크립트를 4~6문장으로 요약해주세요."


def build_combined_analysis_prompt(title: str, date: str, dialogue: str, max_chars: int = 15000) -> str:
    doc = _trim_dialogue(dialogue, max_chars)
    return f"[제목] {title}\n[게시일] {date}\n[스크립트]\n{doc}"


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
        raise RuntimeError("OpenAI provider를 사용하려면 OPENAI_API_KEY 또는 SYUKA_ANALYSIS_API_KEY가 필요합니다.")


def _openai_headers(config: AnalysisConfig) -> dict[str, str]:
    if not config.api_key:
        raise RuntimeError("OpenAI API key가 없습니다.")
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
                raise ValueError("빈 응답을 받았습니다.")
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
    raise ValueError("OpenAI 응답에서 텍스트를 찾지 못했습니다.")


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
    prefixes = ["요약:", "다음은 요약입니다.", "다음은 스크립트 요약입니다."]
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
        retry_prompt = keyword_prompt + "\n\n중요: 너무 넓게 잡지 말고 실제로 중요한 핵심 키워드를 8개 이상 주세요."
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
        raise RuntimeError("output_path 또는 batch_id/file_id 중 하나가 필요합니다.")

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
        print("[analysis] 처리할 분석 대상이 없습니다.", flush=True)
        return {
            "processed_rows": 0,
            "requested_limit": limit,
            "overwrite": overwrite,
            "analysis_source": analysis_source,
            "model": config.model,
        }

    print(
        f"[analysis] 시작: 총 {total}건 | model={config.model} | source={analysis_source}",
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
            f"[analysis] 완료 [{index}/{total}] {row['video_id']} | keywords={len(generated['keywords'])}",
            flush=True,
        )
        sys.stdout.flush()

    conn.commit()
    print(f"[analysis] 종료: {processed}건 처리 완료", flush=True)
    return {
        "processed_rows": processed,
        "requested_limit": limit,
        "overwrite": overwrite,
        "analysis_source": analysis_source,
        "model": config.model,
    }
