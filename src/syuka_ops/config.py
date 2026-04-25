from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_CHANNEL_URL = "https://www.youtube.com/@syukaworld/videos"
DEFAULT_CHANNEL_KEY = "syukaworld"


@dataclass(frozen=True)
class ChannelConfig:
    key: str
    display_name: str
    url: str
    command_prefix: str


CHANNELS: tuple[ChannelConfig, ...] = (
    ChannelConfig(
        key="syukaworld",
        display_name="슈카월드",
        url="https://www.youtube.com/@syukaworld/videos",
        command_prefix="월드",
    ),
    ChannelConfig(
        key="moneymoneycomics",
        display_name="머니코믹스",
        url="https://www.youtube.com/@moneymoneycomics/videos",
        command_prefix="머코",
    ),
)


@dataclass
class AppPaths:
    base_dir: Path
    db_path: Path
    raw_dir: Path
    thumbnails_dir: Path
    reports_dir: Path
    batches_dir: Path

    @classmethod
    def from_base_dir(cls, base_dir: str | Path) -> "AppPaths":
        base = Path(base_dir).resolve()
        return cls(
            base_dir=base,
            db_path=base / "db" / "syuka_ops.db",
            raw_dir=base / "scripts" / "raw",
            thumbnails_dir=base / "thumbnails",
            reports_dir=base / "reports",
            batches_dir=base / "batches",
        )

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.thumbnails_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.batches_dir.mkdir(parents=True, exist_ok=True)

    def to_portable_path(self, path_value: str | Path | None) -> str | None:
        return portable_data_path(path_value, base_dir=self.base_dir)

    def resolve_raw_path(self, path_value: str | Path | None) -> Path | None:
        return resolve_stored_path(path_value, base_dir=self.base_dir, search_dirs=[self.raw_dir])


def env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def channel_configs() -> tuple[ChannelConfig, ...]:
    return CHANNELS


def get_channel_config(channel_key: str) -> ChannelConfig:
    for channel in CHANNELS:
        if channel.key == channel_key:
            return channel
    raise KeyError(f"Unknown channel key: {channel_key}")


def get_channel_by_url(channel_url: str) -> ChannelConfig | None:
    normalized = str(channel_url or "").strip().rstrip("/")
    for channel in CHANNELS:
        if channel.url.rstrip("/") == normalized:
            return channel
        if normalized and f"@{channel.key}".lower() in normalized.lower():
            return channel
    return None


def _normalized_basename(path_value: str | Path | None) -> str:
    text = str(path_value or "").replace("\\", "/").rstrip("/")
    if not text:
        return ""
    return text.rsplit("/", 1)[-1]


def _video_id_from_stored_path(path_value: str | Path | None) -> str:
    basename = _normalized_basename(path_value)
    match = re.search(r"__([A-Za-z0-9_-]{6,})__", basename)
    return match.group(1) if match else ""


def portable_data_path(path_value: str | Path | None, *, base_dir: str | Path | None) -> str | None:
    if not path_value:
        return None
    if base_dir is None:
        return str(path_value)

    base = Path(base_dir).resolve()
    path = Path(path_value)
    try:
        resolved = path.resolve()
    except OSError:
        return str(path_value)

    try:
        return resolved.relative_to(base).as_posix()
    except ValueError:
        return str(path_value)


def resolve_stored_path(
    path_value: str | Path | None,
    *,
    base_dir: str | Path | None = None,
    search_dirs: Iterable[str | Path] = (),
) -> Path | None:
    if not path_value:
        return None

    raw_text = str(path_value).strip()
    if not raw_text:
        return None

    candidate = Path(raw_text)
    if candidate.exists():
        return candidate

    base = Path(base_dir).resolve() if base_dir is not None else None
    if base is not None and not candidate.is_absolute():
        relative_candidate = (base / candidate).resolve()
        if relative_candidate.exists():
            return relative_candidate

    normalized = raw_text.replace("\\", "/")
    if base is not None and "/data/" in normalized:
        relative_tail = normalized.split("/data/", 1)[1]
        remapped = (base / Path(relative_tail)).resolve()
        if remapped.exists():
            return remapped

    basename = _normalized_basename(raw_text)
    if basename:
        for directory in search_dirs:
            remapped = Path(directory).resolve() / basename
            if remapped.exists():
                return remapped
        video_id = _video_id_from_stored_path(raw_text)
        if video_id:
            for directory in search_dirs:
                root = Path(directory).resolve()
                try:
                    candidates = list(root.rglob(f"*{video_id}*"))
                except OSError:
                    candidates = []
                for candidate_path in candidates:
                    if candidate_path.is_file():
                        return candidate_path

    return None
