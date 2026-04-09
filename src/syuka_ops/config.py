from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_CHANNEL_URL = "https://www.youtube.com/@syukaworld/videos"


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


def _normalized_basename(path_value: str | Path | None) -> str:
    text = str(path_value or "").replace("\\", "/").rstrip("/")
    if not text:
        return ""
    return text.rsplit("/", 1)[-1]


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

    return None
