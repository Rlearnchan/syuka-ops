from __future__ import annotations

import re


WHITESPACE_RE = re.compile(r"\s+")
INLINE_TIMESTAMP_RE = re.compile(r"<\d{2}:\d{2}:\d{2}\.\d{3}>")
VTT_TAG_RE = re.compile(r"</?c(?:\.[^>]+)?>|</?v[^>]*>|</?i>|</?b>|</?u>|<ruby>|</ruby>|<rt>|</rt>")
GENERIC_TAG_RE = re.compile(r"<[^>]+>")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?<=[다요죠네까]\.)\s+|(?<=[다요죠네까])(?=\s*[\"')\]]|$)")


def strip_caption_markup(text: str) -> str:
    cleaned = (text or "").replace("\u200b", " ")
    cleaned = INLINE_TIMESTAMP_RE.sub(" ", cleaned)
    cleaned = VTT_TAG_RE.sub("", cleaned)
    cleaned = GENERIC_TAG_RE.sub(" ", cleaned)
    return cleaned


def normalize_dialogue(text: str) -> str:
    cleaned = strip_caption_markup(text)
    return WHITESPACE_RE.sub(" ", cleaned.replace("\n", " ").replace("\r", " ")).strip()


def split_sentences(text: str) -> list[str]:
    normalized = normalize_dialogue(text)
    if not normalized:
        return []
    raw_parts = SENTENCE_SPLIT_RE.split(normalized)
    parts = [part.strip() for part in raw_parts if part.strip()]
    if len(parts) <= 1:
        fallback = re.split(r"(?<=[.!?])\s+", normalized)
        parts = [part.strip() for part in fallback if part.strip()]
    return parts or [normalized]


def preview_excerpt(text: str, *, max_chars: int = 520, max_sentences: int = 4) -> str:
    sentences = split_sentences(text)
    if not sentences:
        return ""

    selected: list[str] = []
    total = 0
    for sentence in sentences:
        extra = len(sentence) + (1 if selected else 0)
        if selected and (len(selected) >= max_sentences or total + extra > max_chars):
            break
        selected.append(sentence)
        total += extra

    if not selected:
        trimmed = normalize_dialogue(text)[:max_chars].strip()
        return trimmed

    excerpt = "\n".join(f"- {sentence}" for sentence in selected)
    if len(normalize_dialogue(text)) > len(" ".join(selected)):
        excerpt += "\n- ..."
    return excerpt


def compact_sentence(text: str, *, max_chars: int = 120) -> str:
    normalized = normalize_dialogue(text)
    if len(normalized) <= max_chars:
        return normalized
    trimmed = normalized[: max_chars - 1].rstrip(" ,.")
    return f"{trimmed}…"


def compact_around_query(text: str, query: str, *, max_chars: int = 220) -> str:
    normalized = normalize_dialogue(text)
    normalized_query = normalize_dialogue(query)
    if len(normalized) <= max_chars or not normalized_query:
        return normalized if len(normalized) <= max_chars else compact_sentence(normalized, max_chars=max_chars)

    lower_text = normalized.lower()
    lower_query = normalized_query.lower()
    query_index = lower_text.find(lower_query)
    if query_index < 0:
        return compact_sentence(normalized, max_chars=max_chars)

    focus_center = query_index + max(1, len(normalized_query) // 2)
    half_window = max_chars // 2
    start = max(0, focus_center - half_window)
    end = min(len(normalized), start + max_chars)
    start = max(0, end - max_chars)

    # Avoid slicing through the middle of a word if there is a nearby space.
    if start > 0:
        next_space = normalized.find(" ", start)
        if next_space != -1 and next_space < start + 18:
            start = next_space + 1
    if end < len(normalized):
        prev_space = normalized.rfind(" ", max(start, end - 18), end)
        if prev_space > start:
            end = prev_space

    snippet = normalized[start:end].strip(" ,.")
    if start > 0:
        snippet = f"…{snippet}"
    if end < len(normalized):
        snippet = f"{snippet}…"
    return snippet


def representative_points(text: str, *, limit: int = 3, min_chars: int = 8) -> list[str]:
    points: list[str] = []
    seen: set[str] = set()
    for sentence in split_sentences(text):
        candidate = compact_sentence(sentence)
        if len(candidate) < min_chars:
            continue
        normalized_key = re.sub(r"\W+", "", candidate)
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        points.append(candidate)
        if len(points) >= limit:
            break
    return points


def keyword_context_snippets(
    text: str,
    query: str,
    *,
    max_snippets: int = 2,
    context_sentences: int = 1,
    max_chars: int = 220,
) -> list[str]:
    sentences = split_sentences(text)
    if not sentences:
        return []

    query_lower = normalize_dialogue(query).lower()
    snippets: list[str] = []
    seen: set[str] = set()

    for index, sentence in enumerate(sentences):
        if query_lower not in sentence.lower():
            continue
        start = max(0, index - context_sentences)
        end = min(len(sentences), index + context_sentences + 1)
        snippet = compact_around_query(" ".join(sentences[start:end]), query, max_chars=max_chars)
        normalized_key = re.sub(r"\W+", "", snippet)
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        snippets.append(snippet)
        if len(snippets) >= max_snippets:
            break

    if snippets:
        return snippets

    fallback = preview_excerpt(text, max_chars=max_chars, max_sentences=2)
    return [line.removeprefix("- ").strip() for line in fallback.splitlines() if line.strip()][:max_snippets]


def chunk_for_llm(text: str, *, target_chars: int = 900, overlap_sentences: int = 1) -> list[str]:
    sentences = split_sentences(text)
    if not sentences:
        return []

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for sentence in sentences:
        sentence_len = len(sentence) + (1 if current else 0)
        if current and current_len + sentence_len > target_chars:
            chunks.append(" ".join(current).strip())
            overlap = current[-overlap_sentences:] if overlap_sentences > 0 else []
            current = overlap.copy()
            current_len = len(" ".join(current))
            if current:
                current_len += 1
        current.append(sentence)
        current_len += sentence_len

    if current:
        chunks.append(" ".join(current).strip())

    return [chunk for chunk in chunks if chunk]


def transcript_stats(text: str) -> dict[str, int]:
    normalized = normalize_dialogue(text)
    return {
        "char_count": len(normalized),
        "sentence_count": len(split_sentences(normalized)),
        "chunk_count": len(chunk_for_llm(normalized)),
    }


def build_llm_context(title: str, text: str, *, max_chars: int = 3200) -> str:
    normalized = normalize_dialogue(text)
    if not normalized:
        return f"제목: {title}\n전문 없음"

    trimmed = normalized[:max_chars].strip()
    if len(normalized) > len(trimmed):
        trimmed = f"{trimmed.rstrip()}…"
    return f"제목: {title}\n전문:\n{trimmed}"
