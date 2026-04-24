"""Shared constants and type aliases for the cache service."""

from __future__ import annotations

from typing import TypeAlias

import pandas as pd

DEFAULT_AGGREGATE_FRAME_MEMORY_ITEMS = 24
DEFAULT_AGGREGATE_METADATA_MEMORY_ITEMS = 96
DEFAULT_REQUEST_PANEL_MEMORY_ITEMS = 6

AggregateFrameMemoryKey: TypeAlias = tuple[
    str,
    str,
    str,
    str | None,
    tuple[str, ...] | None,
]
AggregateMetadataMemoryKey: TypeAlias = tuple[str, str, str]
RequestPanelMemoryKey: TypeAlias = tuple[
    str,
    str | None,
    str | None,
    tuple[str, ...] | None,
    str,
    str,
    int,
    str,
]
CoverageWindow: TypeAlias = dict[str, pd.Timestamp]
