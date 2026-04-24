"""Unified cache coordination helpers between data sources and business layers."""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from typing import Iterable

import pandas as pd

from ..cache_store import CacheStore
from ..schema import DataSchema
from .coverage import CacheCoverageMixin
from .governance import CacheGovernanceMixin
from .models import (
    AggregateFrameMemoryKey,
    AggregateMetadataMemoryKey,
    DEFAULT_AGGREGATE_FRAME_MEMORY_ITEMS,
    DEFAULT_AGGREGATE_METADATA_MEMORY_ITEMS,
    DEFAULT_REQUEST_PANEL_MEMORY_ITEMS,
    RequestPanelMemoryKey,
)
from .observer import CacheObserverMixin
from .policy import CachePolicyMixin
from .writeback import CacheWritebackMixin


class DataCacheService(
    CacheWritebackMixin,
    CacheGovernanceMixin,
    CacheCoverageMixin,
    CacheObserverMixin,
    CachePolicyMixin,
):
    """Centralize cache loading, coverage checks, metadata, and writeback."""

    def __init__(
        self,
        cache_store: CacheStore,
        source_name: str,
        aggregate_frame_memory_items: int = DEFAULT_AGGREGATE_FRAME_MEMORY_ITEMS,
        aggregate_metadata_memory_items: int = DEFAULT_AGGREGATE_METADATA_MEMORY_ITEMS,
        request_panel_memory_items: int = DEFAULT_REQUEST_PANEL_MEMORY_ITEMS,
    ) -> None:
        self.cache_store = cache_store
        self.source_name = source_name
        self._stats: defaultdict[str, int] = defaultdict(int)
        self._runtime_content_generation = 0
        self._aggregate_frame_memory_items = max(int(aggregate_frame_memory_items), 0)
        self._aggregate_metadata_memory_items = max(
            int(aggregate_metadata_memory_items),
            0,
        )
        self._aggregate_frame_memory: OrderedDict[
            AggregateFrameMemoryKey,
            pd.DataFrame,
        ] = OrderedDict()
        self._aggregate_metadata_memory: OrderedDict[
            AggregateMetadataMemoryKey,
            dict[str, object],
        ] = OrderedDict()
        self._request_panel_memory_items = max(int(request_panel_memory_items), 0)
        self._request_panel_memory: OrderedDict[
            RequestPanelMemoryKey,
            pd.DataFrame,
        ] = OrderedDict()

    def load_calendar(
        self,
        exchange: str,
        standardized_name: str | None = None,
    ) -> pd.DataFrame | None:
        self._increment_stat("calendar_load_calls")
        self.record_file_scan("trading_calendar")
        frame = self.cache_store.load_calendar(self.source_name, exchange)
        return self._standardize_optional(standardized_name, frame)

    def save_calendar(self, exchange: str, frame: pd.DataFrame) -> None:
        self._increment_stat("calendar_save_calls")
        self.record_writeback("trading_calendar")
        self.cache_store.save_calendar(self.source_name, exchange, frame)
        self.mark_runtime_content_mutation()

    def load_static_frame(
        self,
        dataset_name: str,
        part: str,
        standardized_name: str | None = None,
    ) -> pd.DataFrame | None:
        self._increment_stat("static_load_calls")
        self._increment_stat(f"static_load_calls::{dataset_name}")
        self.record_file_scan(dataset_name)
        frame = self.cache_store.load_static_frame(self.source_name, dataset_name, part)
        return self._standardize_optional(standardized_name, frame)

    def save_static_frame(
        self,
        dataset_name: str,
        part: str,
        frame: pd.DataFrame,
    ) -> None:
        self._increment_stat("static_save_calls")
        self._increment_stat(f"static_save_calls::{dataset_name}")
        self.record_writeback(dataset_name)
        self.cache_store.save_static_frame(self.source_name, dataset_name, part, frame)
        self.mark_runtime_content_mutation()

    def load_time_series(
        self,
        dataset_name: str,
        cache_key: str,
        standardized_name: str | None = None,
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame | None:
        self._increment_stat("time_series_load_calls")
        self._increment_stat(f"time_series_load_calls::{dataset_name}")
        self.record_file_scan(dataset_name)
        requested_columns = self._normalize_requested_columns(
            standardized_name=standardized_name,
            columns=columns,
        )
        frame = self.cache_store.load_time_series(
            self.source_name,
            dataset_name,
            cache_key,
            columns=requested_columns,
        )
        return self._standardize_optional(standardized_name, frame)

    def save_time_series(
        self,
        dataset_name: str,
        cache_key: str,
        frame: pd.DataFrame,
    ) -> None:
        self._increment_stat("time_series_save_calls")
        self._increment_stat(f"time_series_save_calls::{dataset_name}")
        self.record_writeback(dataset_name)
        self.cache_store.save_time_series(self.source_name, dataset_name, cache_key, frame)
        self._invalidate_higher_level_caches_after_time_series_save(
            dataset_name=dataset_name,
            cache_key=cache_key,
        )
        self.mark_runtime_content_mutation()

    def load_request_panel(
        self,
        dataset_name: str,
        standardized_name: str | None,
        profile: str | None,
        trade_day_strs: Iterable[str],
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame | None:
        trade_days = tuple(str(item) for item in trade_day_strs if str(item))
        if not trade_days:
            return None

        memory_key = self._request_panel_memory_key(
            dataset_name=dataset_name,
            standardized_name=standardized_name,
            profile=profile,
            requested_columns=self._normalize_requested_columns(
                standardized_name=standardized_name,
                columns=columns,
            ),
            trade_day_strs=trade_days,
        )
        cached = self._request_panel_memory.get(memory_key)
        if cached is not None:
            self._request_panel_memory.move_to_end(memory_key)
            self._increment_panel_stat(
                "panel_memory_hit_calls",
                dataset_name=dataset_name,
                profile=profile,
            )
            return cached

        self._increment_panel_stat(
            "panel_memory_miss_calls",
            dataset_name=dataset_name,
            profile=profile,
        )
        return None

    def save_request_panel(
        self,
        dataset_name: str,
        standardized_name: str | None,
        profile: str | None,
        trade_day_strs: Iterable[str],
        frame: pd.DataFrame,
        columns: Iterable[str] | None = None,
    ) -> None:
        trade_days = tuple(str(item) for item in trade_day_strs if str(item))
        if not trade_days or self._request_panel_memory_items <= 0:
            return

        memory_key = self._request_panel_memory_key(
            dataset_name=dataset_name,
            standardized_name=standardized_name,
            profile=profile,
            requested_columns=self._normalize_requested_columns(
                standardized_name=standardized_name,
                columns=columns,
            ),
            trade_day_strs=trade_days,
        )
        self._request_panel_memory[memory_key] = frame
        self._request_panel_memory.move_to_end(memory_key)
        while len(self._request_panel_memory) > self._request_panel_memory_items:
            self._request_panel_memory.popitem(last=False)
        self._increment_panel_stat(
            "panel_memory_save_calls",
            dataset_name=dataset_name,
            profile=profile,
        )

    def invalidate_request_panels(
        self,
        dataset_name: str,
        profile: str | None = None,
    ) -> int:
        stale_keys = [
            memory_key
            for memory_key in self._request_panel_memory
            if memory_key[0] == dataset_name
            and (profile is None or memory_key[2] == profile)
        ]
        for memory_key in stale_keys:
            self._request_panel_memory.pop(memory_key, None)
        if stale_keys:
            self._increment_panel_stat(
                "panel_memory_invalidation_calls",
                dataset_name=dataset_name,
                profile=profile,
                value=len(stale_keys),
            )
        return len(stale_keys)

    def load_time_series_aggregate(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
        standardized_name: str | None = None,
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame | None:
        self._increment_stat("aggregate_load_calls")
        self._increment_stat(f"aggregate_load_calls::{dataset_name}")
        self._increment_stat(f"aggregate_load_calls::{dataset_name}::{profile}")
        requested_columns = self._normalize_requested_columns(
            standardized_name=standardized_name,
            columns=columns,
        )
        memory_key = self._aggregate_frame_memory_key(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
            standardized_name=standardized_name,
            requested_columns=requested_columns,
        )
        cached = self._load_aggregate_frame_from_memory(memory_key)
        if cached is not None:
            self._increment_stat("aggregate_memory_hit_calls")
            self._increment_stat(f"aggregate_memory_hit_calls::{dataset_name}")
            self._increment_stat(f"aggregate_memory_hit_calls::{dataset_name}::{profile}")
            return cached

        self._increment_stat("aggregate_memory_miss_calls")
        self._increment_stat(f"aggregate_memory_miss_calls::{dataset_name}")
        self._increment_stat(f"aggregate_memory_miss_calls::{dataset_name}::{profile}")
        self.record_file_scan(dataset_name, profile=profile)
        frame = self.cache_store.load_time_series_aggregate(
            self.source_name,
            dataset_name,
            profile,
            partition_key,
            columns=requested_columns,
        )
        standardized = self._standardize_optional(standardized_name, frame)
        if standardized is not None:
            self._save_aggregate_frame_to_memory(memory_key, standardized)
            return standardized
        return None

    def save_time_series_aggregate(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
        frame: pd.DataFrame,
        standardized_name: str | None = None,
    ) -> None:
        self._increment_stat("aggregate_save_calls")
        self._increment_stat(f"aggregate_save_calls::{dataset_name}")
        self._increment_stat(f"aggregate_save_calls::{dataset_name}::{profile}")
        self.record_writeback(dataset_name, profile=profile)
        self.cache_store.save_time_series_aggregate(
            self.source_name,
            dataset_name,
            profile,
            partition_key,
            frame,
        )
        self._invalidate_aggregate_frame_memory(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
        )
        self.invalidate_request_panels(dataset_name=dataset_name, profile=profile)
        requested_columns = self._normalize_requested_columns(
            standardized_name=standardized_name,
            columns=frame.columns,
        )
        memory_key = self._aggregate_frame_memory_key(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
            standardized_name=standardized_name,
            requested_columns=requested_columns,
        )
        standardized = self._standardize_optional(standardized_name, frame)
        if standardized is not None:
            self._save_aggregate_frame_to_memory(memory_key, standardized)
        self.mark_runtime_content_mutation()

    def load_reference_frame(
        self,
        category: str,
        name: str,
        standardized_name: str | None = None,
    ) -> pd.DataFrame | None:
        self._increment_stat("reference_load_calls")
        self._increment_stat(f"reference_load_calls::{category}")
        self.record_file_scan(f"reference::{category}")
        frame = self.cache_store.load_reference_frame(category, name)
        return self._standardize_optional(standardized_name, frame)

    def runtime_content_generation(self) -> int:
        return int(self._runtime_content_generation)

    def mark_runtime_content_mutation(self, value: int = 1) -> int:
        self._runtime_content_generation += max(int(value), 1)
        return int(self._runtime_content_generation)

    def _load_aggregate_frame_from_memory(
        self,
        memory_key: AggregateFrameMemoryKey,
    ) -> pd.DataFrame | None:
        cached = self._aggregate_frame_memory.get(memory_key)
        if cached is None:
            dataset_name, profile, partition_key, standardized_name, requested_columns = (
                memory_key
            )
            if requested_columns is None:
                return None
            requested_set = set(requested_columns)
            for candidate_key in reversed(self._aggregate_frame_memory):
                candidate_dataset, candidate_profile, candidate_partition, candidate_standardized, candidate_columns = candidate_key
                if (
                    candidate_dataset != dataset_name
                    or candidate_profile != profile
                    or candidate_partition != partition_key
                    or candidate_standardized != standardized_name
                ):
                    continue
                if candidate_columns is not None and not requested_set.issubset(
                    set(candidate_columns)
                ):
                    continue
                projected = self._aggregate_frame_memory[candidate_key]
                self._aggregate_frame_memory.move_to_end(candidate_key)
                available_columns = [
                    column
                    for column in requested_columns
                    if column in projected.columns
                ]
                if not available_columns:
                    return projected
                return projected.loc[:, available_columns]
            return None
        self._aggregate_frame_memory.move_to_end(memory_key)
        return cached

    def _save_aggregate_frame_to_memory(
        self,
        memory_key: AggregateFrameMemoryKey,
        frame: pd.DataFrame,
    ) -> None:
        if self._aggregate_frame_memory_items <= 0:
            return
        self._aggregate_frame_memory[memory_key] = frame
        self._aggregate_frame_memory.move_to_end(memory_key)
        while len(self._aggregate_frame_memory) > self._aggregate_frame_memory_items:
            self._aggregate_frame_memory.popitem(last=False)

    def _invalidate_aggregate_frame_memory(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
    ) -> None:
        stale_keys = [
            memory_key
            for memory_key in self._aggregate_frame_memory
            if memory_key[:3] == (dataset_name, profile, partition_key)
        ]
        for memory_key in stale_keys:
            self._aggregate_frame_memory.pop(memory_key, None)

    def _load_aggregate_metadata_from_memory(
        self,
        memory_key: AggregateMetadataMemoryKey,
    ) -> dict[str, object] | None:
        cached = self._aggregate_metadata_memory.get(memory_key)
        if cached is None:
            return None
        self._aggregate_metadata_memory.move_to_end(memory_key)
        return cached

    def _save_aggregate_metadata_to_memory(
        self,
        memory_key: AggregateMetadataMemoryKey,
        payload: dict[str, object],
    ) -> None:
        if self._aggregate_metadata_memory_items <= 0:
            return
        self._aggregate_metadata_memory[memory_key] = payload
        self._aggregate_metadata_memory.move_to_end(memory_key)
        while (
            len(self._aggregate_metadata_memory)
            > self._aggregate_metadata_memory_items
        ):
            self._aggregate_metadata_memory.popitem(last=False)

    def _invalidate_aggregate_metadata_memory(
        self,
        memory_key: AggregateMetadataMemoryKey,
    ) -> None:
        self._aggregate_metadata_memory.pop(memory_key, None)
