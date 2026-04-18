"""Small shared helpers for the Step 0 data interface layer."""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd


def normalize_date(value: object) -> pd.Timestamp:
    """Normalize date-like input into a midnight pandas timestamp."""

    return pd.Timestamp(value).normalize()


def format_tushare_date(value: object) -> str:
    """Format a date-like value as Tushare's YYYYMMDD string."""

    return normalize_date(value).strftime("%Y%m%d")


def ensure_list(values: str | Iterable[str]) -> list[str]:
    """Normalize a single string or iterable of strings into a list."""

    if isinstance(values, str):
        return [values]
    return [str(value) for value in values]


def safe_filename(value: str) -> str:
    """Turn a code-like string into a filesystem-safe filename stem."""

    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def merge_frames(
    existing: pd.DataFrame | None,
    fresh: pd.DataFrame | None,
    key_columns: tuple[str, ...],
    sort_columns: tuple[str, ...],
) -> pd.DataFrame:
    """Merge cached and fresh frames, keeping the newest row per key."""

    frames = [frame for frame in (existing, fresh) if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    if key_columns:
        merged = merged.drop_duplicates(subset=list(key_columns), keep="last")
    if sort_columns:
        merged = merged.sort_values(list(sort_columns), kind="stable")
    return merged.reset_index(drop=True)
