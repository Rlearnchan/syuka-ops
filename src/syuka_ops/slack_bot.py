from __future__ import annotations

from datetime import datetime
import logging
import os
import json
import re
import shlex
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .config import AppPaths, get_channel_config
from .ad_utils import detect_paid_promotion, has_ad_signal, load_info_json, query_snippet
from .db import (
    browse_video_count,
    browse_videos,
    browse_short_video_count,
    browse_short_videos,
    collection_stats,
    connect,
    get_video,
    init_db,
    recent_attempts,
    recent_videos,
    search_video_ad_rows,
    search_video_ad_rows_count,
    search_videos,
    search_videos_count,
    transcript_snippets,
    transcript_snippets_count,
    video_rows_with_info_json,
)
from .subtitle_utils import (
    chapter_highlights,
    format_timestamp,
    load_subtitle_segments,
    match_seconds_for_excerpt,
    match_seconds_for_text,
    match_timestamp_for_text,
    sampled_subtitle_segments,
)
from .text_utils import (
    keyword_context_snippets,
    representative_points,
)


load_dotenv()


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    if not any(isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler) for handler in root_logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    try:
        paths = AppPaths.from_base_dir(base_dir())
        paths.ensure()
        log_dir = paths.base_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"slack_bot_{datetime.now():%Y%m%d}.log"
        if not any(isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", "") == str(log_path) for handler in root_logger.handlers):
            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
    except Exception:
        pass

logger = logging.getLogger(__name__)
HOME_BANNER_URL = "https://raw.githubusercontent.com/Rlearnchan/syuka-ops/main/%E1%84%87%E1%85%A2%E1%84%82%E1%85%A5.png"
USER_NAME_CACHE: dict[str, str] = {}
CHANNEL_BROWSE_COMMANDS = {
    "world": ("syukaworld", "슈카월드"),
    "슈카월드": ("syukaworld", "슈카월드"),
    "월드": ("syukaworld", "슈카월드"),
    "moneycomics": ("moneymoneycomics", "머니코믹스"),
    "moneymoneycomics": ("moneymoneycomics", "머니코믹스"),
    "머니코믹스": ("moneymoneycomics", "머니코믹스"),
    "머코": ("moneymoneycomics", "머니코믹스"),
}
SHORTS_BROWSE_COMMANDS = {
    "월드쇼츠": ("syukaworld", "슈카월드"),
    "머코쇼츠": ("moneymoneycomics", "머니코믹스"),
}
PREFIXED_CHANNEL_COMMANDS = {
    "월드주제": ("search", "syukaworld"),
    "월드언급": ("transcript", "syukaworld"),
    "월드광고": ("ads", "syukaworld"),
    "월드썸넬": ("thumbnail", "syukaworld"),
    "월드썸네일": ("thumbnail", "syukaworld"),
    "머코주제": ("search", "moneymoneycomics"),
    "머코언급": ("transcript", "moneymoneycomics"),
    "머코광고": ("ads", "moneymoneycomics"),
    "머코썸넬": ("thumbnail", "moneymoneycomics"),
    "머코썸네일": ("thumbnail", "moneymoneycomics"),
}


@dataclass
class SlackResponse:
    text: str
    blocks: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class SlackRuntimeConfig:
    data_dir: str
    allowed_channel_ids: frozenset[str]
    allowed_user_ids: frozenset[str]


def base_dir() -> str:
    return os.environ.get("SYUKA_DATA_DIR", "./data")


configure_logging()


def parse_csv_env(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(part.strip() for part in value.split(",") if part.strip())


def extract_request_user_id(body: dict[str, Any]) -> str | None:
    user_id = body.get("user_id")
    if isinstance(user_id, str) and user_id:
        return user_id
    user = body.get("user")
    if isinstance(user, dict):
        nested_user_id = user.get("id")
        if isinstance(nested_user_id, str) and nested_user_id:
            return nested_user_id
    event = body.get("event")
    if isinstance(event, dict):
        event_user_id = event.get("user")
        if isinstance(event_user_id, str) and event_user_id:
            return event_user_id
    return None


def extract_request_channel_id(body: dict[str, Any]) -> str | None:
    channel_id = body.get("channel_id")
    if isinstance(channel_id, str) and channel_id:
        return channel_id
    channel = body.get("channel")
    if isinstance(channel, dict):
        nested_channel_id = channel.get("id")
        if isinstance(nested_channel_id, str) and nested_channel_id:
            return nested_channel_id
    event = body.get("event")
    if isinstance(event, dict):
        event_channel_id = event.get("channel")
        if isinstance(event_channel_id, str) and event_channel_id:
            return event_channel_id
    return None


def slack_user_label(client, user_id: str | None) -> str:
    if not user_id:
        return "unknown"
    cached = USER_NAME_CACHE.get(user_id)
    if cached:
        return f"{cached}({user_id})"
    try:
        user_info = client.users_info(user=user_id)
        user = user_info.get("user") or {}
        profile = user.get("profile") or {}
        display_name = (
            profile.get("display_name")
            or profile.get("real_name")
            or user.get("real_name")
            or user.get("name")
            or user_id
        )
    except Exception:
        display_name = user_id
    USER_NAME_CACHE[user_id] = str(display_name)
    return f"{display_name}({user_id})"


@lru_cache(maxsize=1)
def runtime_config() -> SlackRuntimeConfig:
    return SlackRuntimeConfig(
        data_dir=base_dir(),
        allowed_channel_ids=parse_csv_env(os.environ.get("SLACK_ALLOWED_CHANNEL_IDS")),
        allowed_user_ids=parse_csv_env(os.environ.get("SLACK_ALLOWED_USER_IDS")),
    )


def with_db(data_dir: str | None = None):
    paths = AppPaths.from_base_dir(data_dir or runtime_config().data_dir)
    paths.ensure()
    conn = connect(paths.db_path)
    init_db(conn)
    return conn


def row_number(row, key: str) -> int:
    try:
        value = row[key]
    except (KeyError, IndexError):
        return 0
    return int(value or 0)


def row_text(row, key: str) -> str:
    try:
        value = row[key]
    except (KeyError, IndexError):
        return ""
    return str(value or "")


def subtitle_status_label(row) -> str:
    transcript_source = row_text(row, "subtitle_source")
    if transcript_source == "manual":
        return "수동 자막"
    if transcript_source == "auto":
        return "자동 자막"
    if row_number(row, "has_ko_sub"):
        return "수동 자막"
    if row_number(row, "has_auto_ko_sub"):
        return "자동 자막"
    return "자막 없음"


def video_meta_line(row) -> str:
    channel_name = row_text(row, "channel_name")
    channel_part = f"{channel_name} | " if channel_name else ""
    kind_part = "쇼츠 | " if row_number(row, "is_short") else ""
    return f"`{row['video_id']}` | {channel_part}{row['upload_date']} | {kind_part}{subtitle_status_label(row)}"


def video_stats_line(row) -> str:
    return f"조회수 {row_number(row, 'view_count'):,} | 좋아요 {row_number(row, 'like_count'):,}"


def format_video_row(row) -> str:
    return (
        f"- {row['title']} | {video_meta_line(row)} | {video_stats_line(row)}"
    )


def keyword_badges(keywords: list[str], *, limit: int = 5) -> str:
    selected = [keyword for keyword in keywords[:limit] if keyword]
    if not selected:
        return ""
    return " ".join(f"`{keyword}`" for keyword in selected)


def search_match_reasons(row) -> list[str]:
    reasons: list[str] = []
    if row["title_match"]:
        reasons.append("제목")
    if row["keyword_match"]:
        reasons.append("키워드")
    if row["summary_match"]:
        reasons.append("요약")
    if row["transcript_match"]:
        reasons.append("전문")
    if row["video_id_match"]:
        reasons.append("video_id")
    return reasons


def parse_keywords_json(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


LOW_SIGNAL_KEYWORDS = {
    "것",
    "이거",
    "그거",
    "저거",
    "무엇",
    "뭐",
    "얘기",
    "이야기",
    "표현",
    "방향",
    "대응",
    "전략",
    "개념",
    "소재",
    "아이디어",
    "시점",
    "부분",
    "문제",
    "상황",
    "내용",
    "관련",
}


def display_keywords(keywords: list[str], *, min_count: int = 3) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_keyword in keywords:
        keyword = str(raw_keyword or "").strip()
        if not keyword:
            continue
        if keyword.lower() in LOW_SIGNAL_KEYWORDS or keyword in LOW_SIGNAL_KEYWORDS:
            continue
        if len(keyword) <= 1:
            continue
        if re.fullmatch(r"[0-9\s\-_/]+", keyword):
            continue
        normalized_key = re.sub(r"\s+", " ", keyword)
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        cleaned.append(normalized_key)
    if len(cleaned) >= min_count:
        return cleaned
    return []


def clean_summary_text(summary: str | None) -> str:
    if not summary:
        return ""
    text = str(summary).replace("\r", "\n").strip()
    text = re.sub(r"^#+\s*", "", text)
    text = re.sub(r"^\*+\s*요약\s*:?\*+\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(?:다음은\s+제공된\s+스크립트의\s+\d+~\d+줄\s+요약입니다\.?)\s*", "", text)
    text = re.sub(r"^(?:다음은\s+제공된\s+스크립트의\s+요약입니다\.?)\s*", "", text)
    text = re.sub(r"^(?:다음은\s+위\s+스크립트를\s+\d+~\d+줄로\s+요약한\s+내용입니다\.?)\s*", "", text)
    text = re.sub(r"^(?:요약하자면,?\s*)", "", text)
    text = re.sub(r"^(?:핵심\s+내용은\s+다음과\s+같습니다\.?\s*)", "", text)
    text = re.sub(r"^(?:주요\s+내용은\s+다음과\s+같습니다\.?\s*)", "", text)
    text = re.sub(r"\b핵심\s+내용은\s+다음과\s+같습니다\.?", "", text)
    text = re.sub(r"\b주요\s+내용은\s+다음과\s+같습니다\.?", "", text)
    text = text.replace("**", "")
    lines = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^#+\s*", "", line)
        line = re.sub(r"^\*+\s*", "", line)
        line = re.sub(r"^[-*]\s+", "", line)
        line = re.sub(r"^\d+\.\s+", "", line)
        line = re.sub(r"^(?:요약하자면,?\s*)", "", line)
        line = re.sub(r"^(?:핵심\s+내용은\s+다음과\s+같습니다\.?\s*)", "", line)
        line = re.sub(r"^(?:주요\s+내용은\s+다음과\s+같습니다\.?\s*)", "", line)
        if not line:
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def summary_preview(summary: str | None, *, max_lines: int = 3, max_chars: int = 360) -> str:
    cleaned = clean_summary_text(summary)
    if not cleaned:
        return ""
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    selected: list[str] = []
    total = 0
    for line in lines:
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


def concise_summary_preview(summary: str | None, *, max_sentences: int = 2, max_chars: int = 220) -> str:
    cleaned = clean_summary_text(summary)
    if not cleaned:
        return ""
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+|(?<=[다요죠네까]\.)\s+", cleaned) if part.strip()]
    if not sentences:
        compact = cleaned[:max_chars].strip()
        return compact

    selected: list[str] = []
    total = 0
    for sentence in sentences:
        extra = len(sentence) + (1 if selected else 0)
        if selected and (len(selected) >= max_sentences or total + extra > max_chars):
            break
        selected.append(sentence)
        total += extra
    text = " ".join(selected).strip()
    return text


def analysis_pending_text(*, has_transcript: bool) -> str:
    if has_transcript:
        return "요약과 키워드는 아직 준비 중입니다."
    return "아직 분석용 전문이 준비되지 않았습니다. 썸네일이나 YouTube 원본으로 먼저 확인해 주세요."


def highlight_query(text: str, query: str) -> str:
    if not text or not query:
        return text
    safe_query = query.replace("`", "")
    if not safe_query:
        return text
    pattern = re.compile(re.escape(safe_query), re.IGNORECASE)
    return pattern.sub(lambda match: f"`{match.group(0)}`", text)


def help_text() -> str:
    return "\n".join(
        [
            "슈카창고 사용 안내",
            "",
            "호출 방법:",
            "- 슬래시 커맨드: `/syuka help`",
            "- 멘션: `@슈카창고 help`",
            "- 멘션으로 부르면 보통 해당 대화에 댓글처럼 답변합니다.",
            "",
            "바로 써보기:",
            "`/syuka 슈카월드`",
            "`/syuka 머니코믹스`",
            "`/syuka 월드주제 AI`",
            '`/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`',
            "`/syuka 머코주제 트럼프`",
            '`/syuka 머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"`',
            "`/syuka 머코광고 시킹알파`",
            "",
            "자주 쓰는 방법:",
            "- 최근 영상 훑기: `/syuka 슈카월드`",
            "- 머니코믹스 최근 보기: `/syuka 머니코믹스`",
            "- 슈카월드 주제 찾기: `/syuka 월드주제 AI`",
            '- 자막에서 실제 발언 찾기: `/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`',
            '- 머니코믹스 발언 찾기: `/syuka 머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"`',
            "- 광고 사례 찾기: `/syuka 머코광고 시킹알파`",
            "- 썸네일 크게 보기: `/syuka 머코썸넬 트럼프` 또는 `/syuka 썸네일 abc123`",
            "- 수집 상태 보기: `/syuka collect-status`",
            "",
            "명령어 목록:",
            "- `help`: 이 안내 보기",
            "- `슈카월드` / `머니코믹스`: 채널별 최근 업로드 브라우징",
            "- `월드주제` / `머코주제`: 제목과 분석 키워드 기준 검색",
            "- `월드언급` / `머코언급`: 자막 문장과 대목 검색",
            "- `월드광고` / `머코광고`: 설명란 기준 광고 사례 검색",
            "- `월드주제/월드언급/월드광고/월드썸넬`: 슈카월드 전용 바로가기",
            "- `머코주제/머코언급/머코광고/머코썸넬`: 머니코믹스 전용 바로가기",
            "- `video <video_id>`: 특정 영상 상세 보기",
            "- `full <video_id>` / `전문 <video_id>`: 전문 대목 더 보기",
            "- `thumbnail <video_id 또는 키워드>` / `썸네일 <video_id 또는 키워드>`: 썸네일 보기",
            "- `collect-status`: 수집 현황 보기",
            "- `추천질문`: 바로 써볼 수 있는 질문 예시 보기",
            "",
            "예시:",
            "`/syuka 슈카월드`",
            "`/syuka 머니코믹스`",
            "`/syuka 월드주제 AI`",
            '`/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`',
            "`/syuka 머코주제 트럼프`",
            '`/syuka 머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"`',
            "`/syuka 머코광고 시킹알파`",
            "`/syuka video abc123`",
            "`/syuka 전문 abc123`",
            '`/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`',
            "`/syuka thumbnail abc123`",
            "`/syuka collect-status`",
        ]
    )


def access_denied_response() -> SlackResponse:
    return SlackResponse(text="이 채널 또는 사용자에게는 아직 봇 사용 권한이 열려 있지 않습니다.")


def friendly_error_response() -> SlackResponse:
    return SlackResponse(
        text="요청을 처리하는 중 오류가 났습니다. 잠시 후 다시 시도해 주세요. 문제가 계속되면 `/syuka help`로 기본 동작부터 확인해 주세요."
    )


def no_results_response(query: str, *, kind: str, channel_label: str | None = None) -> SlackResponse:
    browse_hint = "/syuka 슈카월드"
    search_hint = "/syuka 월드주제 <키워드>"
    transcript_hint = "/syuka 월드언급 <문장 또는 표현>"
    thumbnail_hint = "/syuka 월드썸넬 <video_id 또는 키워드>"
    if channel_label == "머코":
        browse_hint = "/syuka 머니코믹스"
        search_hint = "/syuka 머코주제 <키워드>"
        transcript_hint = "/syuka 머코언급 <문장 또는 표현>"
        thumbnail_hint = "/syuka 머코썸넬 <video_id 또는 키워드>"
    elif channel_label == "월드":
        browse_hint = "/syuka 슈카월드"
        search_hint = "/syuka 월드주제 <키워드>"
        transcript_hint = "/syuka 월드언급 <문장 또는 표현>"
        thumbnail_hint = "/syuka 월드썸넬 <video_id 또는 키워드>"
    hints = {
        "search": (
            f"`{query}` 관련 영상을 찾지 못했습니다.\n"
            "다음처럼 다시 시도해 보세요:\n"
            "- 더 짧은 키워드로 검색\n"
            f"- `{transcript_hint}` 로 자막 본문 검색\n"
            f"- `{browse_hint}` 로 전체 흐름부터 훑어보기"
        ),
        "transcript": (
            f"`{query}` 관련 자막 스니펫이 없습니다.\n"
            "다음처럼 다시 시도해 보세요:\n"
            "- 문장을 조금 더 짧게 자르거나 핵심 표현만 남기기\n"
            f"- `{search_hint}` 로 제목+자막 통합 검색\n"
            f"- `{browse_hint}` 로 먼저 관련 영상을 훑어보기"
        ),
        "thumbnail": (
            f"`{query}` 관련 썸네일 후보를 찾지 못했습니다.\n"
            "다음처럼 다시 시도해 보세요:\n"
            "- 더 짧은 키워드로 검색\n"
            f"- `{search_hint}` 로 먼저 후보 영상 찾기\n"
            f"- 정확한 `video_id`가 있으면 `{thumbnail_hint}` 사용"
        ),
    }
    return SlackResponse(text=hints[kind])


def unknown_command_response(command: str) -> SlackResponse:
    return SlackResponse(
        text=(
            f"`{command}` 명령을 이해하지 못했습니다.\n"
            "이렇게 시작해 보세요:\n"
            "- `/syuka help`\n"
            "- `/syuka 슈카월드`\n"
            "- `/syuka 월드주제 AI`\n"
            '- `/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`'
        )
    )


def block_header(text: str) -> dict[str, Any]:
    return {"type": "header", "text": {"type": "plain_text", "text": text[:150]}}


def block_section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text[:3000]}}


def block_divider() -> dict[str, Any]:
    return {"type": "divider"}


def block_actions(*elements: dict[str, Any]) -> dict[str, Any]:
    return {"type": "actions", "elements": list(elements)}


def button_link(text: str, url: str, *, action_id: str = "open_link") -> dict[str, Any]:
    return {
        "type": "button",
        "action_id": action_id[:255],
        "text": {"type": "plain_text", "text": text[:75]},
        "url": url,
    }


def button_command(text: str, command: str, *, action_id: str = "run_command") -> dict[str, Any]:
    return {
        "type": "button",
        "action_id": action_id[:255],
        "text": {"type": "plain_text", "text": text[:75]},
        "value": command[:2000],
    }


def unique_action_id(prefix: str, *parts: object) -> str:
    raw = "_".join(str(part) for part in parts if part is not None and str(part).strip())
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", raw).strip("_")
    if not normalized:
        normalized = "action"
    return f"{prefix}_{normalized}"[:255]


def image_block(image_url: str, alt_text: str) -> dict[str, Any]:
    return {"type": "image", "image_url": image_url, "alt_text": alt_text[:2000]}


def transcript_chunks(
    segments,
    *,
    source_label: str,
    max_chars: int = 2600,
) -> tuple[list[str], int]:
    rendered_lines = [f"- ({format_timestamp(segment.start_seconds)}) {segment.text}" for segment in segments]
    chunks: list[str] = []
    current = ""

    for line in rendered_lines:
        addition = f"{line}\n"
        if current and len(current) + len(addition) > max_chars:
            chunks.append(current.rstrip())
            current = addition
        else:
            current += addition

    if current.strip():
        chunks.append(current.rstrip())

    return chunks, len(rendered_lines)


def youtube_url(row) -> str:
    return row["source_url"] or f"https://www.youtube.com/watch?v={row['video_id']}"


def youtube_timestamp_url(row, seconds: int | None) -> str:
    base = youtube_url(row)
    if seconds is None:
        return base
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}t={max(0, int(seconds))}"


QUOTE_TRANSLATION = str.maketrans({
    "“": '"',
    "”": '"',
    "„": '"',
    "‟": '"',
    "‘": "'",
    "’": "'",
    "‚": "'",
    "`": "'",
})

COMMAND_HINTS = (
    "광고찾기",
    "주제찾기",
    "언급찾기",
    "썸네일",
    "슈카월드",
    "머니코믹스",
    "월드",
    "머코",
    "월드주제",
    "월드언급",
    "월드광고",
    "월드썸넬",
    "월드썸네일",
    "머코주제",
    "머코언급",
    "머코광고",
    "머코썸넬",
    "머코썸네일",
    "help",
    "도움말",
)


def normalize_query_text(text: str) -> str:
    normalized = text.translate(QUOTE_TRANSLATION)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    while len(normalized) >= 2:
        if (normalized[0], normalized[-1]) in {('"', '"'), ("'", "'")}:
            normalized = normalized[1:-1].strip()
            continue
        break

    return normalized


def reorder_embedded_command(text: str) -> str:
    for command in COMMAND_HINTS:
        if text == command or text.startswith(f"{command} "):
            return text
        if text.endswith(f" {command}"):
            prefix = text[: -len(command)].strip()
            if prefix:
                return f"{command} {prefix}"
    return text


def prepare_query_text(text: str) -> str:
    return reorder_embedded_command(normalize_query_text(text))


def strip_trailing_request_phrase(text: str) -> str:
    patterns = [
        r"(?:영상)?\s*찾아(?:줘|주세요|줘요)$",
        r"(?:영상)?\s*보여(?:줘|주세요|줘요)$",
        r"\s*알려(?:줘|주세요|줘요)$",
        r"\s*말해(?:줘|주세요|줘요)$",
        r"\s*궁금해$",
        r"\s*뭐였지\??$",
        r"\s*어디서\s*말했지\??$",
        r"\s*어디서\s*나왔지\??$",
        r"\s*어디서\s*언급했지\??$",
        r"\s*먼저\s*보고\s*싶어$",
        r"\s*사례\s*보여(?:줘|주세요|줘요)$",
        r"\s*사례\s*찾아(?:줘|주세요|줘요)$",
        r"\s*했던\s*영상\s*보여(?:줘|주세요|줘요)$",
        r"\s*했던\s*거\s*보여(?:줘|주세요|줘요)$",
        r"\s*했던$",
    ]
    cleaned = text
    for pattern in patterns:
        cleaned = re.sub(pattern, "", cleaned).strip()
    return re.sub(r"[?!.]+$", "", cleaned).strip()


def route_natural_query(text: str) -> str:
    if not text:
        return text

    if any(text == hint or text.startswith(f"{hint} ") for hint in COMMAND_HINTS):
        return text

    lowered = text.lower()
    if "help" == lowered:
        return "help"

    if "썸네일" in text:
        query = strip_trailing_request_phrase(text.replace("썸네일", " ").strip())
        query = re.sub(r"^(?:이|그|저)\s*영상\s*", "", query).strip()
        query = re.sub(r"^(?:이|그|저)\s*", "", query).strip()
        return f"썸네일 {query}".strip()

    if "광고" in text or "협찬" in text:
        query = text
        query = query.replace("광고", " ").replace("협찬", " ")
        query = strip_trailing_request_phrase(query)
        if query:
            return f"광고찾기 {query}"

    mention_cues = ("어디서", "언급", "나온 대목", "실제 발언", "자막", "말했지", "나왔지")
    if any(cue in text for cue in mention_cues):
        query = strip_trailing_request_phrase(text)
        for cue in mention_cues:
            query = query.replace(cue, " ")
        query = re.sub(r"\s+", " ", query).strip()
        if query:
            return f"언급찾기 {query}"

    topic_cues = ("찾아줘", "찾아주세요", "보여줘", "보여주세요", "관련 영상", "영상")
    if any(cue in text for cue in topic_cues):
        query = text
        for cue in ("관련 영상", "영상"):
            query = query.replace(cue, " ")
        query = strip_trailing_request_phrase(query)
        if query:
            return f"주제찾기 {query}"

    return text


def parse_argument_options(argument: str) -> tuple[str, int, int]:
    if not argument:
        return "", 8, 1

    try:
        tokens = shlex.split(argument)
    except ValueError:
        tokens = argument.split()

    query_tokens: list[str] = []
    limit = None
    page = 1
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "--limit" and i + 1 < len(tokens):
            if tokens[i + 1].isdigit():
                limit = int(tokens[i + 1])
            i += 2
            continue
        if token == "--page" and i + 1 < len(tokens):
            if tokens[i + 1].isdigit():
                page = int(tokens[i + 1])
            i += 2
            continue
        query_tokens.append(token)
        i += 1

    safe_limit = max(1, min(limit or 8, 20))
    safe_page = max(1, page)
    return " ".join(query_tokens).strip(), safe_limit, safe_page


def parse_recent_options(argument: str) -> tuple[int, int]:
    if not argument:
        return 8, 1

    query, limit, page = parse_argument_options(argument)
    if not query:
        return limit, page

    parts = query.split()
    if len(parts) >= 1 and parts[0].isdigit():
        limit = max(1, min(int(parts[0]), 20))
    if len(parts) >= 2 and parts[1].isdigit():
        page = max(1, int(parts[1]))
    return limit, page


def parse_world_options(argument: str) -> tuple[str | None, str, int, int]:
    if not argument:
        return None, "latest", 8, 1

    query, limit, page = parse_argument_options(argument)
    tokens = query.split()
    year: str | None = None
    sort = "latest"

    if tokens and tokens[0] == "최신":
        tokens = tokens[1:]

    remaining: list[str] = []
    for token in tokens:
        if re.fullmatch(r"20\d{2}년?", token):
            year = token.replace("년", "")
            continue
        if token in {"좋아요", "좋아요순"}:
            sort = "likes"
            continue
        if token in {"조회수", "조회수순"}:
            sort = "views"
            continue
        remaining.append(token)

    if remaining and remaining[0].isdigit():
        limit = max(1, min(int(remaining[0]), 20))
    if len(remaining) >= 2 and remaining[1].isdigit():
        page = max(1, int(remaining[1]))

    return year, sort, limit, page


def pagination_text(*, page: int, shown_count: int, total_count: int) -> str:
    return f"페이지 {page} | 결과 {shown_count}/{total_count}건"


def pagination_command(command: str, *, query: str = "", limit: int, page: int) -> str:
    if command == "recent":
        return f"recent {limit} {page}"
    if command == "world":
        query_part = f"{query} " if query else ""
        return f"world {query_part}--limit {limit} --page {page}".strip()
    query_part = f"{query} " if query else ""
    return f"{command} {query_part}--limit {limit} --page {page}".strip()


def pagination_actions(*, command: str, query: str = "", limit: int, page: int, row_count: int) -> dict[str, Any] | None:
    elements: list[dict[str, Any]] = []
    if page > 1:
        elements.append(
            button_command(
                "이전",
                pagination_command(command, query=query, limit=limit, page=page - 1),
                action_id=unique_action_id("run_command", command, query, limit, page - 1, "prev"),
            )
        )
    if row_count >= limit:
        elements.append(
            button_command(
                "다음",
                pagination_command(command, query=query, limit=limit, page=page + 1),
                action_id=unique_action_id("run_command", command, query, limit, page + 1, "next"),
            )
        )
    if not elements:
        return None
    return block_actions(*elements)


def examples_response() -> SlackResponse:
    text = "\n".join(
        [
            "추천질문",
            "- 최근 영상 보기: `/syuka 슈카월드`",
            "- 머코 최근 보기: `/syuka 머니코믹스`",
            "- 월드 주제 찾기: `/syuka 월드주제 AI`",
            '- 월드 실제 발언 찾기: `/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`',
            "- 머코 주제 찾기: `/syuka 머코주제 트럼프`",
            '- 머코 실제 발언 찾기: `/syuka 머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"`',
            "- 머코 광고 사례 찾기: `/syuka 머코광고 시킹알파`",
            "- 썸네일 보기: `/syuka 머코썸넬 트럼프`",
            "- 영상 상세 보기: `/syuka video wIuEqwmuORU`",
        ]
    )
    blocks = [
        block_header("추천질문"),
        block_section(
            "*이렇게 물어보면 바로 써보기 좋습니다*\n"
            "`/syuka 슈카월드`\n"
            "`/syuka 머니코믹스`\n"
            "`/syuka 월드주제 AI`\n"
            '`/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`' "\n"
            "`/syuka video NwNvW0lLVtc`\n"
            "`/syuka 머코주제 트럼프`\n"
            '`/syuka 머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"`' "\n"
            "`/syuka 머코광고 시킹알파`\n"
            "`/syuka video wIuEqwmuORU`\n"
            "`/syuka 머코썸넬 트럼프`\n"
            "`/syuka collect-status`"
        ),
        block_actions(
            button_command("슈카월드", "슈카월드", action_id="run_command_world"),
            button_command("머니코믹스", "머니코믹스", action_id="run_command_moneycomics"),
            button_command("월드주제", "월드주제 AI", action_id="run_command_topic"),
            button_command("월드언급", '월드언급 "자, 오늘의 주제 AI 빅뱅입니다"', action_id="run_command_world_mention"),
            button_command("월드광고", "월드광고 구글", action_id="run_command_ads"),
        ),
        block_actions(
            button_command("머코주제", "머코주제 트럼프", action_id="run_command_money_topic"),
            button_command("머코언급", '머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"', action_id="run_command_mention"),
            button_command("머코광고", "머코광고 시킹알파", action_id="run_command_money_ads"),
            button_command("머코썸넬", "머코썸넬 트럼프", action_id="run_command_money_thumbnail"),
            button_command("도움말", "help", action_id="run_command_help"),
        ),
    ]
    return SlackResponse(text=text, blocks=blocks)


def help_response() -> SlackResponse:
    text = help_text()
    blocks = [
        block_header("슈카창고 도움말"),
        block_section(
            "*호출 방법*\n"
            "`/syuka help`\n"
            "`@슈카창고 help`\n"
            "채널과 DM에서 같은 명령어를 그대로 쓸 수 있고, 멘션으로 부르면 보통 해당 대화에 댓글처럼 이어서 답변합니다."
        ),
        block_section(
            "*바로 써보기*\n"
            "1. 최근 흐름을 보고 싶으면 `슈카월드`\n"
            "2. 머니코믹스 흐름을 보려면 `머니코믹스`\n"
            "3. 특정 주제가 궁금하면 `월드주제 AI`\n"
            '4. 실제 발언을 찾고 싶으면 `월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`' "\n"
            "5. 영상 ID를 알고 있으면 `video <video_id>` 로 바로 상세를 여세요"
        ),
        block_actions(
            button_command("슈카월드", "슈카월드", action_id="run_command_world"),
            button_command("머니코믹스", "머니코믹스", action_id="run_command_moneycomics"),
            button_command("월드주제", "월드주제 AI", action_id="run_command_topic"),
            button_command("월드언급", '월드언급 "자, 오늘의 주제 AI 빅뱅입니다"', action_id="run_command_mention"),
            button_command("월드광고", "월드광고 구글", action_id="run_command_ads"),
        ),
        block_section(
            "*어떤 데이터를 읽나요?*\n"
            "`주제`는 제목과 분석 키워드를 읽습니다.\n"
            "`언급`은 자막에서 실제로 나온 문장과 대목을 읽습니다.\n"
            "`광고`는 설명란에서 광고·협찬 문맥을 읽고, 추출 결과가 있으면 그 결과를 우선 보여줍니다."
        ),
        block_actions(
            button_command("월드언급", '월드언급 "자, 오늘의 주제 AI 빅뱅입니다"', action_id="run_command_world_mention"),
            button_command("월드썸넬", "월드썸넬 AI", action_id="run_command_world_thumbnail"),
            button_command("머코주제", "머코주제 트럼프", action_id="run_command_money_topic"),
            button_command("머코언급", '머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"', action_id="run_command_mention"),
            button_command("추천질문", "추천질문", action_id="run_command_examples"),
        ),
        block_divider(),
        block_section(
            "*무엇을 할 수 있나요?*\n"
            "`슈카월드` `머니코믹스` 채널별 최근 업로드 영상 훑기\n"
            "`주제찾기 <키워드>` 제목과 키워드 기준으로 관련 영상 찾기\n"
            "`언급찾기 <문장 또는 표현>` 자막 속 실제 발언 찾기\n"
            "`광고찾기 [업체명 또는 키워드]` 설명란 기준 광고/협찬 문맥 찾기\n"
            "`월드주제/월드언급/월드광고/월드썸넬` 슈카월드 전용 바로가기\n"
            "`머코주제/머코언급/머코광고/머코썸넬` 머니코믹스 전용 바로가기\n"
            "`video <video_id>` 영상 하나 자세히 보기\n"
            "`전문 <video_id>` 전문 대목 더 보기\n"
            "`썸네일 <키워드 또는 video_id>` 썸네일 보기\n"
            "`collect-status` 수집 상태 보기\n"
            "`추천질문` 바로 써볼 질문 보기"
        ),
        block_divider(),
        block_section(
            "*자주 쓰는 예시*\n"
            "`/syuka 슈카월드`\n"
            "`/syuka 머니코믹스`\n"
            "`/syuka 월드주제 AI`\n"
            '`/syuka 월드언급 "자, 오늘의 주제 AI 빅뱅입니다"`' "\n"
            '`/syuka 머코언급 "본인이 버리지 못하는 물건들이 좀 있으세요"`' "\n"
            "`/syuka 머코광고 시킹알파`\n"
            "`/syuka 머코썸넬 트럼프`\n"
            "`/syuka video wIuEqwmuORU`\n"
            "`/syuka 전문 wIuEqwmuORU`\n"
            "`/syuka collect-status`"
        ),
        block_divider(),
        block_section(
            "*결과 보기*\n"
            "검색 결과가 나오면 버튼으로 `상세 보기`, `썸네일 보기`, `YouTube 열기`를 바로 누를 수 있습니다.\n"
            "명령어를 다시 입력하지 않아도 다음 화면으로 이어집니다."
        ),
    ]
    return SlackResponse(text=text, blocks=blocks)


def browse_response(
    rows,
    *,
    header: str,
    text_title: str,
    command_name: str,
    command_query: str = "",
    limit: int,
    page: int,
    total_count: int,
) -> SlackResponse:
    text = f"{text_title}:\n" + "\n".join(format_video_row(row) for row in rows)
    blocks = [block_header(header), block_section(pagination_text(page=page, shown_count=len(rows), total_count=total_count))]
    for row in rows:
        section = block_section(
            f"*{row['title']}*\n\n"
            f"{video_meta_line(row)}\n"
            f"{video_stats_line(row)}"
        )
        if row["thumbnail_url"]:
            section["accessory"] = {
                "type": "image",
                "image_url": row["thumbnail_url"],
                "alt_text": row["title"][:2000],
            }
        blocks.append(section)
        blocks.append(
            block_actions(
                button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "recent", row["video_id"], "detail")),
                button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "recent", row["video_id"], "thumb")),
                button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
            )
        )
    pager = pagination_actions(command=command_name, query=command_query, limit=limit, page=page, row_count=len(rows))
    if pager:
        blocks.extend([block_divider(), pager])
    return SlackResponse(text=text, blocks=blocks)


def search_response(
    query: str,
    rows,
    *,
    limit: int,
    page: int,
    total_count: int,
    command_name: str = "search",
) -> SlackResponse:
    text = f"`{query}` 검색 결과:\n" + "\n".join(format_video_row(row) for row in rows)
    blocks = [
        block_header(f"검색 결과: {query}"),
        block_section(pagination_text(page=page, shown_count=len(rows), total_count=total_count)),
    ]
    for row in rows:
        keywords = display_keywords(parse_keywords_json(row["keywords_json"]))
        summary_text = concise_summary_preview(row["summary"], max_sentences=2, max_chars=180)
        body_lines = [
            f"*{row['title']}*\n"
            f"\n{video_meta_line(row)}\n"
            f"{video_stats_line(row)}"
        ]
        keyword_line = keyword_badges(keywords)
        if keyword_line:
            body_lines.append(f"*키워드* {keyword_line}")
        if summary_text:
            body_lines.append(f"*한줄 요약*\n{summary_text}")
        elif row["has_transcript"]:
            body_lines.append("*요약 상태* 요약 준비 중")
        section = block_section("\n\n".join(body_lines))
        if row["thumbnail_url"]:
            section["accessory"] = {
                "type": "image",
                "image_url": row["thumbnail_url"],
                "alt_text": row["title"][:2000],
            }
        blocks.append(section)
        blocks.append(
            block_actions(
                button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "search", row["video_id"], "detail")),
                button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "search", row["video_id"], "thumb")),
                button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
            )
        )
    pager = pagination_actions(command=command_name, query=query, limit=limit, page=page, row_count=len(rows))
    if pager:
        blocks.extend([block_divider(), pager])
    return SlackResponse(text=text, blocks=blocks)


def video_response(row) -> SlackResponse:
    keywords = display_keywords(parse_keywords_json(row["keywords_json"]))
    summary_text = concise_summary_preview(row["summary"], max_sentences=3, max_chars=280)
    text_lines = [
        f"제목: {row['title']}",
        video_meta_line(row),
        video_stats_line(row),
    ]
    text = "\n".join(text_lines)
    blocks = [
        block_header("영상 상세"),
        block_section(
            f"*{row['title']}*\n"
            f"{video_meta_line(row)}\n"
            f"{video_stats_line(row)}"
        ),
    ]
    keyword_line = keyword_badges(keywords, limit=6)
    if summary_text:
        blocks.extend([block_divider(), block_section(f"*핵심 요약*\n{summary_text}")])
        text += "\n요약:\n" + summary_text
    else:
        fallback_text = analysis_pending_text(has_transcript=bool(row["has_transcript"]))
        blocks.extend([block_divider(), block_section(f"*요약 상태*\n{fallback_text}")])
        text += "\n요약 상태:\n" + fallback_text
    if keyword_line:
        blocks.append(block_section(f"*핵심 키워드*\n{keyword_line}"))
        text += "\n키워드: " + ", ".join(keywords[:6])
    if row["thumbnail_url"]:
        blocks.extend([block_divider(), image_block(row["thumbnail_url"], row["title"])])
    first_point_offset = None
    if row["dialogue"]:
        chapter_items = chapter_highlights(row["info_json_path"], row["subtitle_path"], limit=1)
        if chapter_items:
            first_point_offset = chapter_items[0].start_seconds
        else:
            sampled_segments = sampled_subtitle_segments(row["subtitle_path"], limit=1)
            if sampled_segments:
                first_point_offset = sampled_segments[0].start_seconds
            else:
                preview_points = representative_points(row["dialogue"], limit=1)
                if preview_points:
                    first_point_offset = match_seconds_for_text(row["subtitle_path"], preview_points[0])
    blocks.extend(
        [
            block_divider(),
            block_actions(
                button_command("전문", f"full {row['video_id']}", action_id=unique_action_id("run_command", "video", row["video_id"], "full")),
                button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "video", row["video_id"], "thumb")),
                button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
            ),
        ]
    )
    if row["dialogue"]:
        source_label = "수동 자막 기반" if row["subtitle_source"] == "manual" else "자동 자막 기반"
        quick_view_lines = [f"*핵심만 보기* ({source_label})"]
        rendered_points = []
        chapter_items = chapter_highlights(row["info_json_path"], row["subtitle_path"], limit=5)
        if chapter_items:
            for item in chapter_items:
                timestamp = format_timestamp(item.start_seconds)
                quick_view_lines.append(f"- ({timestamp}) *{item.title}*\n  {item.excerpt}")
                rendered_points.append(f"- ({timestamp}) {item.title}\n  {item.excerpt}")
        else:
            sampled_segments = sampled_subtitle_segments(row["subtitle_path"], limit=5)
            if sampled_segments:
                for segment in sampled_segments:
                    timestamp = format_timestamp(segment.start_seconds)
                    quick_view_lines.append(f"- ({timestamp}) {segment.text}")
                    rendered_points.append(f"- ({timestamp}) {segment.text}")
            else:
                points = representative_points(row["dialogue"], limit=5)
                if points:
                    for point in points:
                        seconds = match_seconds_for_text(row["subtitle_path"], point)
                        timestamp = format_timestamp(seconds) if seconds is not None else None
                        prefix = f"- ({timestamp}) " if timestamp else "- "
                        quick_view_lines.append(f"{prefix}{point}")
                        rendered_points.append(f"{prefix}{point}")
                else:
                    quick_view_lines.append("- 전문은 있지만 아직 읽기 좋게 정리된 문장을 만들지 못했습니다.")
                    rendered_points.append("- 전문은 있지만 아직 읽기 좋게 정리된 문장을 만들지 못했습니다.")
        blocks.extend(
            [
                block_divider(),
                block_section("\n".join(quick_view_lines)),
            ]
        )
        text += "\n핵심만 보기:\n" + "\n".join(rendered_points)
    return SlackResponse(text=text, blocks=blocks)


def full_transcript_response(row, *, page: int = 1, chunks_per_page: int = 8) -> SlackResponse:
    source_label = "수동 자막 기반" if row["subtitle_source"] == "manual" else "자동 자막 기반"
    subtitle_path = row["subtitle_path"]
    segments = load_subtitle_segments(subtitle_path) if subtitle_path else []
    blocks = [
        block_header("전문 보기"),
        block_section(
            f"*{row['title']}*\n\n"
            f"{video_meta_line(row)}\n"
            f"{video_stats_line(row)}"
        ),
    ]
    text_lines = [
        f"전문 보기: {row['title']}",
        f"video_id: `{row['video_id']}`",
    ]
    page_count = 1
    safe_page = 1
    if segments:
        chunks, total_lines = transcript_chunks(segments, source_label=source_label)
        page_count = max(1, (len(chunks) + chunks_per_page - 1) // chunks_per_page)
        safe_page = min(max(1, page), page_count)
        start = (safe_page - 1) * chunks_per_page
        page_chunks = chunks[start : start + chunks_per_page]
        text_lines.append(f"전문 줄 수: {total_lines:,}")
        text_lines.append(f"전문 페이지: {safe_page}/{page_count}")
        text_lines.extend(page_chunks)
        blocks.extend(
            [
                block_divider(),
                block_section(f"*전문* ({source_label})\n{safe_page}/{page_count}쪽 · 전체 {total_lines:,}줄"),
            ]
        )
        for chunk in page_chunks:
            blocks.extend([block_divider(), block_section(chunk)])
        if page_count > 1:
            page_note = "이전/다음 버튼으로 전문을 Slack 안에서 이어서 볼 수 있습니다."
            blocks.extend([block_divider(), block_section(page_note)])
            text_lines.append(page_note)
    else:
        if row["has_transcript"] and not subtitle_path:
            fallback = "전문 데이터는 등록돼 있지만 `subtitle_path`가 비어 있어 실제 자막 파일을 읽지 못했습니다."
        elif row["has_transcript"] and subtitle_path:
            fallback = (
                "전문 데이터는 등록돼 있지만 실제 자막 파일을 읽지 못했습니다. "
                "DB 감사에서 `transcript_path_missing` 또는 `transcript_file_missing` 항목을 확인해 주세요."
            )
        else:
            fallback = analysis_pending_text(has_transcript=bool(row["has_transcript"]))
        blocks.extend([block_divider(), block_section(f"*전문 상태*\n{fallback}")])
        text_lines.append(fallback)

    action_elements: list[dict[str, Any]] = []
    if segments and page_count > 1 and safe_page > 1:
        action_elements.append(
            button_command(
                "이전 전문",
                f"full {row['video_id']} --page {safe_page - 1}",
                action_id=unique_action_id("run_command", "full", row["video_id"], "page", safe_page - 1),
            )
        )
    if segments and page_count > 1 and safe_page < page_count:
        action_elements.append(
            button_command(
                "다음 전문",
                f"full {row['video_id']} --page {safe_page + 1}",
                action_id=unique_action_id("run_command", "full", row["video_id"], "page", safe_page + 1),
            )
        )
    action_elements.extend(
        [
            button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "full", row["video_id"], "detail")),
            button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "full", row["video_id"], "thumb")),
            button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
        ]
    )
    blocks.extend(
        [
            block_divider(),
            block_actions(*action_elements[:5]),
        ]
    )
    return SlackResponse(text="\n".join(text_lines), blocks=blocks)


def transcript_response(
    query: str,
    rows,
    *,
    limit: int,
    page: int,
    total_count: int,
    command_name: str = "transcript",
) -> SlackResponse:
    lines = []
    blocks = [
        block_header(f"자막 스니펫: {query}"),
        block_section(pagination_text(page=page, shown_count=len(rows), total_count=total_count)),
    ]
    for row in rows:
        keywords = display_keywords(parse_keywords_json(row["keywords_json"]))
        context_summary = concise_summary_preview(row["summary"], max_sentences=1, max_chars=120)
        snippets = keyword_context_snippets(row["dialogue"], query, max_snippets=2, max_chars=220)
        offsets = [match_seconds_for_excerpt(row["subtitle_path"], snippet) for snippet in snippets]
        rendered_snippets: list[str] = []
        for index, snippet in enumerate(snippets):
            highlighted_snippet = highlight_query(snippet, query)
            offset = offsets[index] if index < len(offsets) else None
            timestamp = format_timestamp(offset) if offset is not None else None
            rendered_snippets.append(f"> ({timestamp}) {highlighted_snippet}" if timestamp else f"> {highlighted_snippet}")
        snippet_text = "\n".join(rendered_snippets) if rendered_snippets else "- 관련 발췌를 만들지 못했습니다."
        plain_context = context_summary or analysis_pending_text(has_transcript=bool(row["dialogue"]))
        lines.append(f"- {row['title']} | {video_meta_line(row)}\n맥락: {plain_context}\n{snippet_text}")
        keyword_line = keyword_badges(keywords, limit=4)
        linked_snippets: list[str] = []
        for index, snippet in enumerate(snippets):
            offset = offsets[index] if index < len(offsets) else None
            highlighted_snippet = highlight_query(snippet, query)
            timestamp = format_timestamp(offset) if offset is not None else None
            linked_snippets.append(f"> ({timestamp}) {highlighted_snippet}" if timestamp else f"> {highlighted_snippet}")
        block_snippet_text = "\n".join(linked_snippets) if linked_snippets else "- 관련 발췌를 만들지 못했습니다."
        body_lines = [
            f"*{row['title']}*\n\n"
            f"{video_meta_line(row)}\n"
            f"{video_stats_line(row)}"
        ]
        if context_summary:
            body_lines.append(f"*맥락*\n{context_summary}")
        else:
            body_lines.append(f"*분석 상태*\n{analysis_pending_text(has_transcript=bool(row['dialogue']))}")
        body_lines.append(f"*이 표현이 나온 대목*\n{block_snippet_text}")
        if keyword_line:
            body_lines.append(f"*키워드* {keyword_line}")
        section = block_section(
            "\n\n".join(body_lines)
        )
        if row["thumbnail_url"]:
            section["accessory"] = {
                "type": "image",
                "image_url": row["thumbnail_url"],
                "alt_text": row["title"][:2000],
            }
        blocks.append(section)
        primary_offset = offsets[0] if offsets else None
        blocks.append(
            block_actions(
                button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "transcript", row["video_id"], "detail")),
                button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "transcript", row["video_id"], "thumb")),
                button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
            )
        )
    pager = pagination_actions(command=command_name, query=query, limit=limit, page=page, row_count=len(rows))
    if pager:
        blocks.extend([block_divider(), pager])
    return SlackResponse(text="\n".join(lines), blocks=blocks)


def ad_search_rows(
    conn,
    *,
    query: str,
    limit: int,
    page: int,
    channel_key: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    def parse_advertiser_candidates(raw_value: Any) -> list[str]:
        if not raw_value:
            return []
        try:
            parsed = json.loads(raw_value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        if not isinstance(parsed, list):
            return []
        candidates: list[str] = []
        seen: set[str] = set()
        for item in parsed:
            name = str(item or "").strip()
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(name)
        return candidates[:3]

    offset = (page - 1) * limit
    extracted_rows = search_video_ad_rows(conn, query, limit=limit, offset=offset, channel_key=channel_key)
    extracted_total_count = search_video_ad_rows_count(conn, query, channel_key=channel_key)
    if extracted_rows:
        return [
            {
                "video_id": row["video_id"],
                "title": row["title"],
                "upload_date": row["upload_date"],
                "view_count": row["view_count"],
                "like_count": row["like_count"],
                "thumbnail_url": row["thumbnail_url"],
                "source_url": row["source_url"],
                "advertiser": row["advertiser"],
                "advertiser_candidates": parse_advertiser_candidates(row["advertiser_candidates_json"]),
                "matched_text": row["evidence_text"] or "",
                "snippet": row["description_excerpt"] or row["evidence_text"] or "",
                "match_type": "추출",
                "score": int(round(float(row["confidence"] or 0) * 100)),
                "analysis_source": row["analysis_source"],
            }
            for row in extracted_rows
        ], extracted_total_count

    matched: list[dict[str, Any]] = []
    lower_query = query.lower().strip()

    for row in video_rows_with_info_json(conn, channel_key=channel_key):
        info = load_info_json(row["info_json_path"])
        description = info.get("description") or ""
        detected = detect_paid_promotion(description)
        title = row["title"] or ""
        haystacks = [
            title,
            detected.get("advertiser", "") if detected else "",
            detected.get("matched_text", "") if detected else "",
            description,
        ]
        query_matched = bool(lower_query and any(lower_query in value.lower() for value in haystacks if value))
        if not detected and not query_matched:
            continue
        if lower_query and not query_matched:
            continue
        if detected:
            match_type = "확정"
            score = 100
            advertiser = detected.get("advertiser", "") or query
            matched_text = detected.get("matched_text", "")
            snippet = detected.get("snippet", "")
        else:
            has_signal = has_ad_signal(description) or has_ad_signal(title)
            match_type = "후보"
            score = 70 if has_signal else 40
            advertiser = query
            matched_text = "업체명이 제목/설명에 직접 언급됨"
            snippet = query_snippet(description or title, query)
        matched.append(
            {
                "video_id": row["video_id"],
                "title": title,
                "upload_date": row["upload_date"],
                "view_count": row["view_count"],
                "like_count": row["like_count"],
                "thumbnail_url": row["thumbnail_url"],
                "source_url": row["source_url"],
                "advertiser": advertiser,
                "advertiser_candidates": [advertiser] if advertiser else [],
                "matched_text": matched_text,
                "snippet": snippet,
                "match_type": match_type,
                "score": score,
            }
        )

    matched.sort(key=lambda item: (item["score"], item["upload_date"] or ""), reverse=True)
    total_count = len(matched)
    return matched[offset : offset + limit], total_count


def ad_search_response(
    query: str,
    rows: list[dict[str, Any]],
    *,
    limit: int,
    page: int,
    total_count: int,
    command_name: str = "ads",
) -> SlackResponse:
    text_lines = [f"`{query}` 광고 사례 검색 결과:"]
    blocks = [
        block_header(f"광고 사례: {query}"),
        block_section(pagination_text(page=page, shown_count=len(rows), total_count=total_count)),
    ]
    for row in rows:
        advertiser = row["advertiser"] or "광고주 미상"
        advertiser_candidates = [candidate for candidate in row.get("advertiser_candidates", []) if candidate]
        text_lines.append(f"- {row['title']} | {video_meta_line(row)} | 광고주 {advertiser}")
        body_lines = [
            f"*{row['title']}*\n\n"
            f"{video_meta_line(row)}\n"
            f"{video_stats_line(row)}",
            f"*광고주*\n{advertiser}",
        ]
        if len(advertiser_candidates) > 1:
            body_lines.append(
                "*광고주 후보*\n"
                + "\n".join(f"{index}. {candidate}" for index, candidate in enumerate(advertiser_candidates, start=1))
            )
        if row.get("matched_text"):
            body_lines.append(f"*근거*\n{row['matched_text']}")
        body_lines.extend(
            [
            f"*설명 발췌*\n{row['snippet']}",
            ]
        )
        section = block_section("\n\n".join(body_lines))
        if row["thumbnail_url"]:
            section["accessory"] = {
                "type": "image",
                "image_url": row["thumbnail_url"],
                "alt_text": row["title"][:2000],
            }
        blocks.append(section)
        blocks.append(
            block_actions(
                button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "ad", row["video_id"], "detail")),
                button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "ad", row["video_id"], "thumb")),
                button_link("유튜브", row["source_url"] or f"https://www.youtube.com/watch?v={row['video_id']}", action_id="open_youtube_link"),
            )
        )
    pager = pagination_actions(command=command_name, query=query, limit=limit, page=page, row_count=len(rows))
    if pager:
        blocks.extend([block_divider(), pager])
    return SlackResponse(text="\n".join(text_lines), blocks=blocks)


def thumbnail_response(row) -> SlackResponse:
    if not row["thumbnail_url"]:
        return SlackResponse(text=f"`{row['video_id']}` 영상에는 썸네일 URL이 없습니다.")
    text = (
        f"썸네일 보기\n"
        f"- 제목: {row['title']}\n"
        f"- 메타: {video_meta_line(row)}\n"
        f"- 썸네일: {row['thumbnail_url']}"
    )
    blocks = [
        block_header("영상 썸네일"),
        block_section(
            f"*{row['title']}*\n"
            f"{video_meta_line(row)}"
        ),
        block_divider(),
        image_block(row["thumbnail_url"], row["title"]),
        block_divider(),
        block_actions(
            button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "thumbnail", row["video_id"], "detail")),
            button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
        ),
    ]
    return SlackResponse(text=text, blocks=blocks)


def thumbnail_candidates_response(
    query: str,
    rows,
    *,
    limit: int,
    page: int,
    total_count: int,
    command_name: str = "thumbnail",
) -> SlackResponse:
    text = f"`{query}` 관련 썸네일 후보:\n" + "\n".join(format_video_row(row) for row in rows)
    blocks = [
        block_header(f"썸네일 후보: {query}"),
        block_section(pagination_text(page=page, shown_count=len(rows), total_count=total_count)),
    ]
    for row in rows:
        section = block_section(
            f"*{row['title']}*\n\n"
            f"{video_meta_line(row)}\n"
            f"{video_stats_line(row)}"
        )
        if row["thumbnail_url"]:
            section["accessory"] = {
                "type": "image",
                "image_url": row["thumbnail_url"],
                "alt_text": row["title"][:2000],
            }
        blocks.append(section)
        action_items = [
            button_command("상세", f"video {row['video_id']}", action_id=unique_action_id("run_command", "thumbnail_candidates", row["video_id"], "detail")),
            button_command("썸네일", f"thumbnail {row['video_id']}", action_id=unique_action_id("run_command", "thumbnail_candidates", row["video_id"], "thumb")),
            button_link("유튜브", youtube_url(row), action_id="open_youtube_link"),
        ]
        blocks.append(block_actions(*action_items))
    pager = pagination_actions(command=command_name, query=query, limit=limit, page=page, row_count=len(rows))
    if pager:
        blocks.extend([block_divider(), pager])
    return SlackResponse(text=text, blocks=blocks)


def app_home_view(*, user_name: str | None = None) -> dict[str, Any]:
    welcome_name = user_name or "사용자"
    return {
        "type": "home",
        "blocks": [
            block_header("슈카창고"),
            block_section(
                f"*{welcome_name}님, DM 탭에서 바로 시작해보세요.*\n"
                "슈카월드와 머니코믹스 영상을 채널별로 찾고, 자막 대목과 설명란 문맥까지 바로 확인하는 내부 검색 도구입니다."
            ),
            block_divider(),
            block_section(
                "*활용 가능 명령어*"
            ),
            block_actions(
            button_command("슈카월드", "슈카월드", action_id="run_command_home_world"),
            button_command("월드쇼츠", "월드쇼츠 잉어", action_id="run_command_home_world_shorts"),
            button_command("월드주제", "월드주제 투자", action_id="run_command_home_topic"),
            button_command("월드언급", "월드언급 효율적으로 대응", action_id="run_command_home_world_mention"),
            button_command("월드광고", "월드광고 카카오페이증권", action_id="run_command_home_world_ads"),
            button_command("월드썸넬", "월드썸넬 염소", action_id="run_command_home_world_thumbnail"),
        ),
            block_actions(
                button_command("머니코믹스", "머니코믹스", action_id="run_command_home_moneycomics"),
                button_command("머코쇼츠", "머코쇼츠 최신 5", action_id="run_command_home_money_shorts"),
                button_command("머코주제", "머코주제 트럼프", action_id="run_command_home_money_topic"),
                button_command("머코언급", '머코언급 "돈은 안 내도 되는데 쿠팡도"', action_id="run_command_home_mention"),
                button_command("머코광고", "머코광고 시킹알파", action_id="run_command_home_ads"),
                button_command("머코썸넬", "머코썸넬 트럼프", action_id="run_command_home_money_thumbnail"),
            ),
            block_divider(),
            image_block(HOME_BANNER_URL, "슈카창고 배너"),
            block_section(
                "*검색 방법*\n"
                "• `슈카월드`, `월드쇼츠` : 최신 업로드 흐름을 먼저 볼 때\n"
                "• `월드주제 투자` : 관련 영상을 넓게 모아볼 때\n"
                "• `월드언급 효율적으로 대응` : 실제 발언 대목을 확인할 때\n"
                "• `월드광고 카카오페이증권` : 설명란 광고 사례를 바로 볼 때\n"
                "• `월드썸넬 염소` : 기억나는 장면으로 썸네일 후보를 찾을 때"
            ),
            block_divider(),
            block_section(
                "*다른 호출 방법*\n"
                "소개된 명령어는 모두 채널에서도 그대로 사용할 수 있습니다.\n"
                "상세 보기는 `/syuka video <video_id>` 형태로 영상 아이디를 넣어 바로 열면 됩니다."
            ),
            block_divider(),
            block_section(
                "*패치노트*\n"
                "`2026-03-25` 슈카창고 베타 버전을 Slack에 처음 오픈했습니다.\n"
                "`2026-04-01` 슈카창고를 상시 구동 환경에서 사용할 수 있게 정비했습니다.\n"
                "`2026-04-08` 영상 정보 수집을 자동화했습니다.\n"
                "`2026-04-25` 머니코믹스 채널을 추가하고, 광고 검색을 보강했습니다.\n"
                "`2026-04-26` 쇼츠 지원을 추가했습니다."
            ),
        ],
    }

def app_home_result_view(response: SlackResponse, *, command: str) -> dict[str, Any]:
    blocks = response.blocks or [block_section(response.text)]
    return {"type": "home", "blocks": blocks[:100]}


def collect_status_response(stats, attempts) -> SlackResponse:
    latest_upload = stats["latest_upload_date"] or "없음"
    latest_attempt = stats["latest_attempt_at"] or "없음"
    summary = (
        f"*영상 수*: {stats['video_count'] or 0:,}\n"
        f"*한글 수동 자막 가능*: {stats['ko_sub_count'] or 0:,}\n"
        f"*한글 자동 자막 가능*: {stats['auto_ko_sub_count'] or 0:,}\n"
        f"*한글 자막 가능(합계)*: {stats['any_ko_sub_count'] or 0:,}\n"
        f"*전문 수집 완료*: {stats['transcript_count'] or 0:,}\n"
        f"*자막 성공 로그*: {stats['subtitle_successes'] or 0:,}\n"
        f"*자막 실패 로그*: {stats['subtitle_failures'] or 0:,}\n"
        f"*최신 업로드일*: {latest_upload}\n"
        f"*마지막 시도 시각*: {latest_attempt}"
    )
    text_lines = [
        "수집 상태",
        f"- 영상 수: {stats['video_count'] or 0:,}",
        f"- 한글 수동 자막 가능: {stats['ko_sub_count'] or 0:,}",
        f"- 한글 자동 자막 가능: {stats['auto_ko_sub_count'] or 0:,}",
        f"- 한글 자막 가능(합계): {stats['any_ko_sub_count'] or 0:,}",
        f"- 전문 수집 완료: {stats['transcript_count'] or 0:,}",
        f"- 자막 실패 로그: {stats['subtitle_failures'] or 0:,}",
        f"- 최신 업로드일: {latest_upload}",
        f"- 마지막 시도 시각: {latest_attempt}",
    ]
    blocks = [block_header("수집 상태"), block_section(summary)]

    if attempts:
        lines = []
        for row in attempts:
            status_icon = "성공" if row["status"] == "downloaded" else "실패"
            line = (
                f"`{row['video_id']}` | {row['stage']} | {status_icon} | "
                f"시도 {row['attempts']} | {row['created_at']}"
            )
            lines.append(line)
        blocks.extend([block_divider(), block_section("*최근 시도*\n" + "\n".join(lines))])
        text_lines.append("최근 시도:")
        text_lines.extend(lines)

    return SlackResponse(text="\n".join(text_lines), blocks=blocks)


def response_to_kwargs(response: SlackResponse) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"text": response.text}
    if response.blocks:
        kwargs["blocks"] = response.blocks
    return kwargs


def is_request_allowed(*, channel_id: str | None, user_id: str | None) -> bool:
    config = runtime_config()
    if config.allowed_channel_ids and channel_id is not None and channel_id not in config.allowed_channel_ids:
        return False
    if config.allowed_user_ids and user_id not in config.allowed_user_ids:
        return False
    return True


def strip_bot_mention(text: str, bot_user_id: str) -> str:
    return text.replace(f"<@{bot_user_id}>", "").strip()


def thread_ts_from_body(body: dict[str, Any]) -> str | None:
    message = body.get("message") or {}
    return message.get("thread_ts") or message.get("ts")


def handle_query(text: str, *, data_dir: str | None = None) -> SlackResponse:
    text = prepare_query_text(text)
    text = route_natural_query(text)
    if not text:
        return help_response()

    parts = text.split(maxsplit=1)
    raw_command = parts[0].strip()
    command = raw_command.lower()
    argument = parts[1].strip() if len(parts) > 1 else ""
    channel_key: str | None = None
    channel_display_name: str | None = None
    command_name = raw_command

    scoped_command = raw_command in PREFIXED_CHANNEL_COMMANDS or raw_command in CHANNEL_BROWSE_COMMANDS or raw_command in SHORTS_BROWSE_COMMANDS

    if raw_command in PREFIXED_CHANNEL_COMMANDS:
        command, channel_key = PREFIXED_CHANNEL_COMMANDS[raw_command]
        channel_display_name = get_channel_config(channel_key).display_name
    elif raw_command in CHANNEL_BROWSE_COMMANDS:
        channel_key, channel_display_name = CHANNEL_BROWSE_COMMANDS[raw_command]
        command = "channel-browse"
    elif raw_command in SHORTS_BROWSE_COMMANDS:
        channel_key, channel_display_name = SHORTS_BROWSE_COMMANDS[raw_command]
        command = "shorts-browse"

    command_aliases = {
        "도움말": "help",
        "examples": "examples",
        "추천": "examples",
        "추천질문": "examples",
        "recent": "recent",
        "latest": "recent",
        "최근": "recent",
        "최신이슈": "recent",
        "search": "search",
        "검색": "search",
        "topic": "search",
        "주제찾기": "search",
        "transcript": "transcript",
        "자막": "transcript",
        "mention": "transcript",
        "언급찾기": "transcript",
        "ads": "ads",
        "ad": "ads",
        "광고찾기": "ads",
        "full": "full",
        "전문": "full",
        "video": "video",
        "영상": "video",
        "썸네일": "thumbnail",
        "status": "collect-status",
        "상태": "collect-status",
    }
    if command_name == raw_command and not scoped_command:
        command = command_aliases.get(command, command)
        command_name = command

    try:
        conn = with_db(data_dir=data_dir)
        try:
            if command == "help":
                return help_response()

            if command == "examples":
                return examples_response()

            if command == "channel-browse":
                year, sort, limit, page = parse_world_options(argument)
                total_count = browse_video_count(conn, channel_key=channel_key, year=year)
                rows = browse_videos(conn, channel_key=channel_key, year=year, sort=sort, limit=limit, offset=(page - 1) * limit)
                if not rows:
                    year_label = f"{year}년 " if year else ""
                    return SlackResponse(text=f"{channel_display_name or '채널'}에서 {year_label}조건에 맞는 영상을 찾지 못했습니다.")
                sort_label = {
                    "latest": "최신순",
                    "likes": "좋아요순",
                    "views": "조회수순",
                }.get(sort, "최신순")
                if year:
                    header = f"{channel_display_name} {year}년"
                    text_title = f"{channel_display_name} {year}년 {sort_label}"
                else:
                    header = channel_display_name or "채널"
                    text_title = f"{channel_display_name or '채널'} {sort_label}"
                query_parts: list[str] = []
                if year:
                    query_parts.append(f"{year}년")
                if sort == "likes":
                    query_parts.append("좋아요")
                elif sort == "views":
                    query_parts.append("조회수")
                elif not year:
                    query_parts.append("최신")
                command_query = " ".join(query_parts)
                return browse_response(
                    rows,
                    header=header,
                    text_title=text_title,
                    command_name=command_name,
                    command_query=command_query,
                    limit=limit,
                    page=page,
                    total_count=total_count,
                )

            if command == "shorts-browse":
                year, sort, limit, page = parse_world_options(argument)
                total_count = browse_short_video_count(conn, channel_key=channel_key, year=year)
                rows = browse_short_videos(conn, channel_key=channel_key or "", year=year, sort=sort, limit=limit, offset=(page - 1) * limit)
                if not rows:
                    year_label = f"{year}년 " if year else ""
                    return SlackResponse(text=f"{channel_display_name or '채널'} 쇼츠에서 {year_label}조건에 맞는 영상을 찾지 못했습니다.")
                sort_label = {
                    "latest": "최신순",
                    "likes": "좋아요순",
                    "views": "조회수순",
                }.get(sort, "최신순")
                if year:
                    header = f"{channel_display_name} 쇼츠 {year}년"
                    text_title = f"{channel_display_name} 쇼츠 {year}년 {sort_label}"
                else:
                    header = f"{channel_display_name} 쇼츠"
                    text_title = f"{channel_display_name} 쇼츠 {sort_label}"
                query_parts: list[str] = []
                if year:
                    query_parts.append(f"{year}년")
                if sort == "likes":
                    query_parts.append("좋아요")
                elif sort == "views":
                    query_parts.append("조회수")
                elif not year:
                    query_parts.append("최신")
                command_query = " ".join(query_parts)
                return browse_response(
                    rows,
                    header=header,
                    text_title=text_title,
                    command_name=command_name,
                    command_query=command_query,
                    limit=limit,
                    page=page,
                    total_count=total_count,
                )

            if command == "recent":
                limit, page = parse_recent_options(argument)
                total_count = browse_video_count(conn)
                rows = recent_videos(conn, limit=limit, offset=(page - 1) * limit)
                if not rows:
                    return SlackResponse(text="아직 수집된 영상이 없습니다.")
                return browse_response(
                    rows,
                    header="최근 영상",
                    text_title="최근 영상",
                    command_name="recent",
                    limit=limit,
                    page=page,
                    total_count=total_count,
                )

            if command == "search":
                query, limit, page = parse_argument_options(argument)
                if not query:
                    return SlackResponse(text="사용법: `search <키워드>` 또는 `/syuka 주제찾기 <키워드>`")
                total_count = search_videos_count(conn, query, channel_key=channel_key)
                rows = search_videos(conn, query, limit=limit, offset=(page - 1) * limit, channel_key=channel_key)
                if not rows:
                    prefix = get_channel_config(channel_key).command_prefix if channel_key else None
                    return no_results_response(query, kind="search", channel_label=prefix)
                return search_response(query, rows, limit=limit, page=page, total_count=total_count, command_name=command_name)

            if command == "video":
                if not argument:
                    return SlackResponse(text="사용법: `video <video_id>` 또는 `/syuka 영상 <video_id>`")
                row = get_video(conn, argument, channel_key=channel_key)
                if not row:
                    return SlackResponse(text=f"`{argument}` 영상을 찾지 못했습니다.")
                return video_response(row)

            if command == "full":
                if not argument:
                    return SlackResponse(text="사용법: `full <video_id>` 또는 `/syuka 전문 <video_id>`")
                video_id, _, page = parse_argument_options(argument)
                row = get_video(conn, video_id, channel_key=channel_key)
                if not row:
                    return SlackResponse(text=f"`{video_id}` 영상을 찾지 못했습니다.")
                return full_transcript_response(row, page=page)

            if command == "thumbnail":
                if not argument:
                    return SlackResponse(text="사용법: `thumbnail <video_id 또는 키워드>` 또는 `/syuka 썸네일 <키워드>`")
                row = get_video(conn, argument, channel_key=channel_key)
                if row:
                    return thumbnail_response(row)
                query, limit, page = parse_argument_options(argument)
                if not query:
                    return SlackResponse(text="사용법: `thumbnail <video_id 또는 키워드>`")
                total_count = search_videos_count(conn, query, channel_key=channel_key)
                rows = search_videos(conn, query, limit=limit, offset=(page - 1) * limit, channel_key=channel_key)
                if not rows:
                    prefix = get_channel_config(channel_key).command_prefix if channel_key else None
                    return no_results_response(query, kind="thumbnail", channel_label=prefix)
                if len(rows) == 1:
                    return thumbnail_response(rows[0])
                return thumbnail_candidates_response(query, rows, limit=limit, page=page, total_count=total_count, command_name=command_name)

            if command == "transcript":
                query, limit, page = parse_argument_options(argument)
                if not query:
                    return SlackResponse(text="사용법: `transcript <키워드>` 또는 `/syuka 언급찾기 <키워드>`")
                total_count = transcript_snippets_count(conn, query, channel_key=channel_key)
                rows = transcript_snippets(conn, query, limit=limit, offset=(page - 1) * limit, channel_key=channel_key)
                if not rows:
                    prefix = get_channel_config(channel_key).command_prefix if channel_key else None
                    return no_results_response(query, kind="transcript", channel_label=prefix)
                return transcript_response(query, rows, limit=limit, page=page, total_count=total_count, command_name=command_name)

            if command == "ads":
                query, limit, page = parse_argument_options(argument)
                if not query:
                    return SlackResponse(text="사용법: `ads <업체명 또는 키워드>` 또는 `/syuka 광고찾기 <업체명>`")
                rows, total_count = ad_search_rows(conn, query=query, limit=limit, page=page, channel_key=channel_key)
                if not rows:
                    return SlackResponse(text=f"`{query}` 관련 광고 사례를 찾지 못했습니다.")
                return ad_search_response(query, rows, limit=limit, page=page, total_count=total_count, command_name=command_name)

            if command in {"collect-status", "status"}:
                stats = collection_stats(conn)
                attempts = recent_attempts(conn, stage="subtitle", limit=5)
                return collect_status_response(stats, attempts)

            return unknown_command_response(command)
        finally:
            conn.close()
    except Exception:
        logger.exception("Failed to handle query: %s", text)
        return friendly_error_response()


def main() -> None:
    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not bot_token or not app_token:
        raise ValueError("SLACK_BOT_TOKEN, SLACK_APP_TOKEN 환경 변수가 필요합니다.")

    app = App(token=bot_token)

    @app.use
    def log_incoming_requests(logger, body, next, client):
        request_type = body.get("type")
        command_name = body.get("command")
        event_type = (body.get("event") or {}).get("type")
        action_ids = [action.get("action_id") for action in body.get("actions", [])]
        user_id = extract_request_user_id(body)
        channel_id = extract_request_channel_id(body)
        user_label = slack_user_label(client, user_id) if user_id else "unknown"
        logger.info(
            "Incoming Slack request: type=%s command=%s event=%s actions=%s user=%s channel=%s",
            request_type,
            command_name,
            event_type,
            action_ids,
            user_label,
            channel_id,
        )
        next()

    @app.event("app_mention")
    def handle_mentions(event, say, client):
        bot_user_id = client.auth_test()["user_id"]
        channel_id = event.get("channel")
        user_id = event.get("user")
        if not is_request_allowed(channel_id=channel_id, user_id=user_id):
            say(**response_to_kwargs(access_denied_response()), thread_ts=event.get("thread_ts") or event.get("ts"))
            return
        text = strip_bot_mention(event.get("text", ""), bot_user_id)
        response = handle_query(text)
        say(**response_to_kwargs(response), thread_ts=event.get("thread_ts") or event.get("ts"))

    @app.event("message")
    def handle_direct_messages(event, say, logger, client):
        if event.get("channel_type") != "im":
            return
        if event.get("bot_id") or event.get("subtype"):
            return
        channel_id = event.get("channel")
        user_id = event.get("user")
        if not is_request_allowed(channel_id=channel_id, user_id=user_id):
            say(**response_to_kwargs(access_denied_response()), thread_ts=event.get("thread_ts") or event.get("ts"))
            return
        text = (event.get("text") or "").strip()
        if not text:
            response = help_response()
        else:
            response = handle_query(text)
        logger.info("Handled direct message: user=%s text=%s", slack_user_label(client, user_id), text)
        say(**response_to_kwargs(response), thread_ts=event.get("thread_ts") or event.get("ts"))

    @app.event("app_home_opened")
    def handle_app_home_opened(event, client, logger):
        user_id = event.get("user")
        if not user_id:
            return
        try:
            user_info = client.users_info(user=user_id)
            profile = (user_info.get("user") or {}).get("profile") or {}
            display_name = profile.get("display_name") or profile.get("real_name") or None
        except Exception:
            logger.exception("Failed to fetch Slack user info for App Home: %s", user_id)
            display_name = None
        client.views_publish(user_id=user_id, view=app_home_view(user_name=display_name))

    @app.command("/syuka")
    def slash_syuka(ack, command, logger, client):
        if not is_request_allowed(channel_id=command.get("channel_id"), user_id=command.get("user_id")):
            ack(**response_to_kwargs(access_denied_response()))
            return
        response = handle_query(command.get("text", ""))
        logger.info(
            "Handled slash command /syuka: user=%s text=%s",
            slack_user_label(client, command.get("user_id")),
            command.get("text", ""),
        )
        ack(**response_to_kwargs(response))

    @app.action(re.compile("^run_.*command.*$"))
    def run_command_action(ack, body, client, logger):
        ack()
        channel_payload = body.get("channel") or {}
        channel_id = channel_payload.get("id") if isinstance(channel_payload, dict) else None
        user_id = body.get("user", {}).get("id")
        container_type = (body.get("container") or {}).get("type")
        is_home_interaction = bool(body.get("view")) or container_type in {"home", "view"}
        command_value = (body.get("actions") or [{}])[0].get("value", "")
        logger.info(
            "Handled button action: user=%s command=%s channel=%s home=%s",
            slack_user_label(client, user_id),
            command_value,
            channel_id,
            is_home_interaction,
        )
        if not is_request_allowed(channel_id=channel_id, user_id=user_id):
            response = access_denied_response()
        else:
            response = handle_query(command_value)
        if is_home_interaction and user_id:
            client.views_publish(user_id=user_id, view=app_home_result_view(response, command=command_value))
            return
        if channel_id:
            kwargs = response_to_kwargs(response)
            client.chat_postMessage(channel=channel_id, thread_ts=thread_ts_from_body(body), **kwargs)
            return

    @app.action(re.compile("^open_.*link$"))
    def open_link_action(ack, body, logger, client):
        ack()
        user_id = body.get("user", {}).get("id")
        logger.info(
            "Acknowledged link button action: user=%s actions=%s",
            slack_user_label(client, user_id),
            [action.get("action_id") for action in body.get("actions", [])],
        )

    logger.info("Slack bot starting...")
    config = runtime_config()
    logger.info(
        "Slack runtime config: data_dir=%s allowed_channels=%d allowed_users=%d",
        config.data_dir,
        len(config.allowed_channel_ids),
        len(config.allowed_user_ids),
    )
    handler = SocketModeHandler(app, app_token)
    handler.start()


if __name__ == "__main__":
    main()

