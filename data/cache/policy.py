"""Path, projection, and key-normalization helpers for the cache service."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import pandas as pd

from ..schema import DataSchema
from ..utils import safe_filename
from .models import (
    AggregateFrameMemoryKey,
    AggregateMetadataMemoryKey,
    RequestPanelMemoryKey,
)


class CachePolicyMixin:
    """Provide path and key policies shared by the cache service."""

    def time_series_coverage_path(self, dataset_name: str, cache_key: str) -> Path:
        return (
            self.cache_store.base_dir
            / self.source_name
            / "time_series"
            / dataset_name
            / f"{cache_key}.coverage.json"
        )

    def time_series_aggregate_metadata_path(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
    ) -> Path:
        return (
            self.cache_store.base_dir
            / self.source_name
            / "time_series_aggregate"
            / dataset_name
            / safe_filename(profile)
            / f"{safe_filename(partition_key)}.meta.json"
        )

    def reference_frame_path(self, category: str, name: str) -> Path:
        return self.cache_store.reference_dir / category / f"{safe_filename(name)}.csv"

    def reference_metadata_path(self, category: str, name: str) -> Path:
        return self.cache_store.reference_dir / category / f"{safe_filename(name)}.meta.json"

    def _standardize_optional(
        self,
        standardized_name: str | None,
        frame: pd.DataFrame | None,
    ) -> pd.DataFrame | None:
        if frame is None or standardized_name is None:
            return frame
        return DataSchema.standardize(standardized_name, frame)

    def _normalize_requested_columns(
        self,
        standardized_name: str | None,
        columns: Iterable[str] | None,
        extra_columns: Iterable[str] = (),
    ) -> tuple[str, ...] | None:
        if columns is None:
            return None
        merged = [str(column) for column in columns]
        if standardized_name is not None:
            schema = DataSchema.get_schema(standardized_name)
            merged.extend(schema.key_columns)
        merged.extend(str(column) for column in extra_columns)
        return tuple(dict.fromkeys(merged))

    @staticmethod
    def _safe_min_timestamp(frame: pd.DataFrame, column: str) -> pd.Timestamp:
        values = pd.to_datetime(frame[column], errors="coerce").dropna()
        if values.empty:
            raise ValueError(f"{column} has no valid timestamps")
        return pd.Timestamp(values.min()).normalize()

    @staticmethod
    def _aggregate_frame_memory_key(
        dataset_name: str,
        profile: str,
        partition_key: str,
        standardized_name: str | None,
        requested_columns: tuple[str, ...] | None,
    ) -> AggregateFrameMemoryKey:
        return (
            dataset_name,
            profile,
            partition_key,
            standardized_name,
            requested_columns,
        )

    @staticmethod
    def _aggregate_metadata_memory_key(
        dataset_name: str,
        profile: str,
        partition_key: str,
    ) -> AggregateMetadataMemoryKey:
        return (dataset_name, profile, partition_key)

    @staticmethod
    def _request_panel_memory_key(
        dataset_name: str,
        standardized_name: str | None,
        profile: str | None,
        requested_columns: tuple[str, ...] | None,
        trade_day_strs: tuple[str, ...],
    ) -> RequestPanelMemoryKey:
        digest = hashlib.sha1(",".join(trade_day_strs).encode("ascii")).hexdigest()[:16]
        return (
            dataset_name,
            standardized_name,
            profile,
            requested_columns,
            trade_day_strs[0],
            trade_day_strs[-1],
            len(trade_day_strs),
            digest,
        )
