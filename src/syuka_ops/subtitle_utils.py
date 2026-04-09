from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path
import re

import srt

from .ad_utils import load_info_json
from .config import AppPaths, resolve_stored_path
from .text_utils import compact_sentence, normalize_dialogue


@dataclass(frozen=True)
class SubtitleSegment:
    start_seconds: int
    text: str


@dataclass(frozen=True)
class ChapterMarker:
    start_seconds: int
    end_seconds: int | None
    title: str


@dataclass(frozen=True)
class ChapterHighlight:
    start_seconds: int
    title: str
    excerpt: str


def format_timestamp(seconds: int) -> str:
    minutes, sec = divmod(max(0, int(seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def parse_vtt_to_subs(vtt_path: str | Path) -> list:
    path = Path(vtt_path)
    lines = path.read_text(encoding="utf-8").split("\n")

    srt_lines: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line and not (
            line.startswith("WEBVTT")
            or line.startswith("Kind:")
            or line.startswith("Language:")
            or line.startswith("Style:")
        ):
            break
        i += 1

    block_num = 1
    subtitle_block: list[str] = []
    while i < len(lines):
        line = lines[i]
        if "-->" in line:
            if subtitle_block and "-->" in subtitle_block[0]:
                _append_subtitle_block(srt_lines, subtitle_block, block_num)
                block_num += 1
            subtitle_block = [line]
        elif line.strip() == "":
            if subtitle_block and "-->" in subtitle_block[0]:
                _append_subtitle_block(srt_lines, subtitle_block, block_num)
                block_num += 1
            subtitle_block = []
        elif subtitle_block:
            cleaned = normalize_dialogue(line)
            if cleaned:
                subtitle_block.append(cleaned)
        i += 1

    if subtitle_block and "-->" in subtitle_block[0]:
        _append_subtitle_block(srt_lines, subtitle_block, block_num)

    if not any(line.strip() for line in srt_lines):
        return []
    try:
        return list(srt.parse("\n".join(srt_lines)))
    except Exception:
        return []


def _append_subtitle_block(target: list[str], subtitle_block: list[str], block_num: int) -> None:
    target.append(str(block_num))
    target.append(subtitle_block[0].strip().replace(".", ","))
    for text_line in subtitle_block[1:]:
        cleaned = normalize_dialogue(text_line)
        if cleaned:
            target.append(cleaned)
    target.append("")


def load_subtitle_segments(subtitle_path: str | None) -> list[SubtitleSegment]:
    if not subtitle_path:
        return []
    base_dir = os.environ.get("SYUKA_DATA_DIR", "./data")
    app_paths = AppPaths.from_base_dir(base_dir)
    path = resolve_stored_path(subtitle_path, base_dir=app_paths.base_dir, search_dirs=[app_paths.raw_dir])
    if path is None:
        path = Path(subtitle_path)
    if not path.exists():
        return []
    return _load_subtitle_segments_cached(str(path), path.stat().st_mtime_ns)


def load_chapter_markers(info_json_path: str | None) -> list[ChapterMarker]:
    data = load_info_json(info_json_path)
    raw_chapters = data.get("chapters")
    if not isinstance(raw_chapters, list):
        return []

    chapters: list[ChapterMarker] = []
    for item in raw_chapters:
        if not isinstance(item, dict):
            continue
        title = normalize_dialogue(str(item.get("title") or ""))
        if not title:
            continue
        start_time = item.get("start_time")
        end_time = item.get("end_time")
        if start_time is None:
            continue
        try:
            start_seconds = int(float(start_time))
        except (TypeError, ValueError):
            continue
        end_seconds: int | None = None
        if end_time is not None:
            try:
                end_seconds = int(float(end_time))
            except (TypeError, ValueError):
                end_seconds = None
        chapters.append(ChapterMarker(start_seconds=start_seconds, end_seconds=end_seconds, title=title))
    return chapters


def sampled_subtitle_segments(
    subtitle_path: str | None,
    *,
    limit: int = 5,
    min_chars: int = 12,
    max_chars: int = 150,
) -> list[SubtitleSegment]:
    segments = load_subtitle_segments(subtitle_path)
    if not segments:
        return []

    filtered: list[SubtitleSegment] = []
    seen: set[str] = set()
    for segment in segments:
        text = compact_sentence(segment.text, max_chars=max_chars)
        normalized_key = re.sub(r"\W+", "", text)
        if len(text) < min_chars or not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        filtered.append(SubtitleSegment(start_seconds=segment.start_seconds, text=text))

    if len(filtered) <= limit:
        return filtered

    if limit <= 1:
        return [filtered[0]]

    selected: list[SubtitleSegment] = []
    selected_indexes: set[int] = set()
    last_index = len(filtered) - 1
    for step in range(limit):
        index = round(step * last_index / (limit - 1))
        if index in selected_indexes:
            continue
        selected_indexes.add(index)
        selected.append(filtered[index])
    return selected


def dedupe_overlapping_segments(segments: list[SubtitleSegment]) -> list[SubtitleSegment]:
    max_merged_segment_chars = 520
    cleaned: list[SubtitleSegment] = []
    last_seen_starts: list[int] = []
    for segment in segments:
        text = normalize_dialogue(segment.text)
        if not text:
            continue
        current = SubtitleSegment(start_seconds=segment.start_seconds, text=text)
        if not cleaned:
            cleaned.append(current)
            last_seen_starts.append(current.start_seconds)
            continue

        previous = cleaned[-1]
        if current.start_seconds - last_seen_starts[-1] > 35:
            cleaned.append(current)
            last_seen_starts.append(current.start_seconds)
            continue

        merged_text = merge_overlapping_text(previous.text, current.text)
        if merged_text == previous.text:
            last_seen_starts[-1] = current.start_seconds
            continue
        if merged_text != f"{previous.text} {current.text}":
            if len(merged_text) > max_merged_segment_chars and merged_text.startswith(previous.text):
                continuation = merged_text[len(previous.text) :].strip()
                if continuation:
                    cleaned.append(SubtitleSegment(start_seconds=current.start_seconds, text=continuation))
                    last_seen_starts.append(current.start_seconds)
                else:
                    last_seen_starts[-1] = current.start_seconds
                continue
            cleaned[-1] = SubtitleSegment(start_seconds=previous.start_seconds, text=merged_text)
            last_seen_starts[-1] = current.start_seconds
            continue
        cleaned.append(current)
        last_seen_starts.append(current.start_seconds)
    return cleaned


def merge_overlapping_text(previous: str, current: str) -> str:
    previous = normalize_dialogue(previous)
    current = normalize_dialogue(current)
    if not previous:
        return current
    if not current:
        return previous
    if current == previous or current in previous:
        return previous
    if previous in current:
        return current

    max_overlap = min(len(previous), len(current))
    min_overlap = 5
    for size in range(max_overlap, min_overlap - 1, -1):
        if previous[-size:] == current[:size]:
            return normalize_dialogue(previous + current[size:])

    return f"{previous} {current}"


def chapter_highlights(
    info_json_path: str | None,
    subtitle_path: str | None,
    *,
    limit: int = 5,
    max_segments_per_chapter: int = 3,
    max_chars: int = 320,
) -> list[ChapterHighlight]:
    chapters = load_chapter_markers(info_json_path)
    segments = load_subtitle_segments(subtitle_path)
    if not chapters or not segments:
        return []

    selected_chapters = chapters[:limit]
    highlights: list[ChapterHighlight] = []
    for chapter in selected_chapters:
        in_range = [
            segment
            for segment in segments
            if segment.start_seconds >= chapter.start_seconds
            and (chapter.end_seconds is None or segment.start_seconds < chapter.end_seconds)
        ]
        if not in_range:
            fallback = next((segment for segment in segments if segment.start_seconds >= chapter.start_seconds), None)
            if fallback is None:
                fallback = next((segment for segment in reversed(segments) if segment.start_seconds <= chapter.start_seconds), None)
            in_range = [fallback] if fallback is not None else []

        excerpts: list[str] = []
        total = 0
        for segment in in_range[: max(1, max_segments_per_chapter)]:
            candidate = compact_sentence(segment.text, max_chars=max_chars)
            extra = len(candidate) + (1 if excerpts else 0)
            if excerpts and total + extra > max_chars:
                break
            excerpts.append(candidate)
            total += extra
        excerpt = " ".join(excerpts).strip()
        if not excerpt:
            continue
        highlights.append(
            ChapterHighlight(
                start_seconds=chapter.start_seconds,
                title=chapter.title,
                excerpt=excerpt,
            )
        )
    return highlights


@lru_cache(maxsize=256)
def _load_subtitle_segments_cached(subtitle_path: str, _mtime_ns: int) -> list[SubtitleSegment]:
    path = Path(subtitle_path)

    try:
        if path.suffix.lower() == ".srt":
            subs = list(srt.parse(path.read_text(encoding="utf-8")))
        else:
            subs = parse_vtt_to_subs(path)
    except Exception:
        return []

    segments: list[SubtitleSegment] = []
    for sub in subs:
        text = normalize_dialogue(sub.content)
        if not text:
            continue
        segments.append(SubtitleSegment(start_seconds=int(sub.start.total_seconds()), text=text))
    return dedupe_overlapping_segments(segments)


def match_seconds_for_text(subtitle_path: str | None, target_text: str) -> int | None:
    normalized_target = normalize_dialogue(target_text)
    if not normalized_target:
        return None

    for segment in load_subtitle_segments(subtitle_path):
        if normalized_target in segment.text or segment.text in normalized_target:
            return segment.start_seconds
    return None


def match_seconds_for_excerpt(subtitle_path: str | None, excerpt_text: str) -> int | None:
    direct = match_seconds_for_text(subtitle_path, excerpt_text)
    if direct is not None:
        return direct

    segments = load_subtitle_segments(subtitle_path)
    normalized_excerpt = normalize_dialogue(excerpt_text).lower()
    if not normalized_excerpt or not segments:
        return None

    excerpt_tokens = {token for token in re.split(r"\W+", normalized_excerpt) if len(token) >= 2}
    if not excerpt_tokens:
        return None

    best_score = 0
    best_offset: int | None = None
    for segment in segments:
        segment_text = segment.text.lower()
        segment_tokens = {token for token in re.split(r"\W+", segment_text) if len(token) >= 2}
        if not segment_tokens:
            continue
        overlap = len(excerpt_tokens & segment_tokens)
        if overlap > best_score:
            best_score = overlap
            best_offset = segment.start_seconds

    return best_offset if best_score > 0 else None


def match_timestamp_for_text(subtitle_path: str | None, target_text: str) -> str | None:
    seconds = match_seconds_for_text(subtitle_path, target_text)
    return format_timestamp(seconds) if seconds is not None else None


def find_query_offsets(subtitle_path: str | None, query: str, *, limit: int = 2) -> list[int]:
    query_normalized = normalize_dialogue(query).lower()
    if not query_normalized:
        return []

    offsets: list[int] = []
    for segment in load_subtitle_segments(subtitle_path):
        if query_normalized in segment.text.lower():
            offsets.append(segment.start_seconds)
            if len(offsets) >= limit:
                break
    return offsets


def find_query_timestamps(subtitle_path: str | None, query: str, *, limit: int = 2) -> list[str]:
    return [format_timestamp(offset) for offset in find_query_offsets(subtitle_path, query, limit=limit)]
