"""Writeback and invalidation helpers for the cache service."""

from __future__ import annotations

import pandas as pd

from ..utils import safe_filename


class CacheWritebackMixin:
    """Provide derived-field writeback and higher-level cache invalidation."""

    def writeback_derived_fields(
        self,
        dataset_name: str,
        updates: pd.DataFrame,
        columns: tuple[str, ...],
        base_frame: pd.DataFrame | None = None,
        key_columns: tuple[str, ...] = ("cb_code", "trade_date"),
        date_column: str = "trade_date",
        standardized_name: str = "cb_daily",
        group_as_trade_date_key: bool = True,
    ) -> None:
        """Fill derived fields back into cached time-series files."""

        if updates.empty or not columns:
            return

        available_columns = [
            column
            for column in (*key_columns, *columns)
            if column in updates.columns
        ]
        if any(key not in available_columns for key in key_columns):
            return

        working = updates.loc[:, available_columns].copy()
        working[date_column] = pd.to_datetime(working[date_column], errors="coerce")
        working = working.dropna(subset=list(key_columns))
        if working.empty:
            return

        working = working.drop_duplicates(subset=list(key_columns), keep="last")

        base_by_cache_key: dict[str, pd.DataFrame] = {}
        if base_frame is not None and not base_frame.empty:
            normalized_base = base_frame.copy()
            normalized_base[date_column] = pd.to_datetime(
                normalized_base[date_column],
                errors="coerce",
            )
            normalized_base = normalized_base.dropna(subset=list(key_columns))
            if group_as_trade_date_key:
                normalized_base["__cache_key"] = normalized_base[date_column].dt.strftime("%Y%m%d")
            else:
                normalized_base["__cache_key"] = normalized_base[key_columns[0]].astype(str)
            base_by_cache_key = {
                str(cache_key): frame.drop(columns="__cache_key").reset_index(drop=True)
                for cache_key, frame in normalized_base.groupby("__cache_key", sort=False)
            }

        for group_value, group in working.groupby(date_column if group_as_trade_date_key else key_columns[0], sort=False):
            if group_as_trade_date_key:
                cache_key = pd.Timestamp(group_value).strftime("%Y%m%d")
            else:
                cache_key = str(group_value)

            cached_frame = base_by_cache_key.get(cache_key)
            if cached_frame is None:
                cached = self.load_time_series(dataset_name, cache_key, standardized_name=standardized_name)
                if cached is None or cached.empty:
                    continue
                cached_frame = cached

            update_columns = [column for column in columns if column in group.columns]
            if not update_columns:
                continue

            merged = cached_frame.merge(
                group.loc[:, [*key_columns, *update_columns]],
                on=list(key_columns),
                how="left",
                suffixes=("", "__update"),
            )

            changed = False
            for column in update_columns:
                update_column = f"{column}__update"
                if update_column not in merged.columns:
                    continue
                fill_mask = merged[column].isna() & merged[update_column].notna()
                if fill_mask.any():
                    merged.loc[fill_mask, column] = merged.loc[fill_mask, update_column]
                    changed = True
                merged = merged.drop(columns=[update_column])

            if changed:
                self.save_time_series(dataset_name, cache_key, merged)
                self._increment_stat("derived_writeback_files")
                self._increment_stat(f"derived_writeback_files::{dataset_name}")

    def _invalidate_higher_level_caches_after_time_series_save(
        self,
        dataset_name: str,
        cache_key: str,
    ) -> None:
        if dataset_name != "cb_daily_cross_section":
            return

        self.invalidate_request_panels(dataset_name=dataset_name)
        cache_key_text = str(cache_key)
        if not (cache_key_text.isdigit() and len(cache_key_text) >= 6):
            return
        self.invalidate_time_series_aggregate_month(
            dataset_name=dataset_name,
            month_key=cache_key_text[:6],
        )

    def invalidate_time_series_aggregate_month(
        self,
        dataset_name: str,
        month_key: str,
    ) -> int:
        aggregate_root = (
            self.cache_store.base_dir
            / self.source_name
            / "time_series_aggregate"
            / dataset_name
        )
        touched_profiles: set[str] = set()
        if aggregate_root.exists():
            for profile_dir in aggregate_root.iterdir():
                if not profile_dir.is_dir():
                    continue
                profile = str(profile_dir.name)
                csv_path = profile_dir / f"{safe_filename(month_key)}.csv"
                meta_path = profile_dir / f"{safe_filename(month_key)}.meta.json"
                removed = False
                if csv_path.exists():
                    csv_path.unlink()
                    removed = True
                if meta_path.exists():
                    meta_path.unlink()
                    removed = True
                if removed:
                    touched_profiles.add(profile)

        touched_profiles.update(
            self._invalidate_aggregate_memory_month(dataset_name=dataset_name, month_key=month_key)
        )
        for profile in touched_profiles:
            self.invalidate_request_panels(dataset_name=dataset_name, profile=profile)
            self._increment_stat("aggregate_partition_invalidation_calls")
            self._increment_stat(
                f"aggregate_partition_invalidation_calls::{dataset_name}"
            )
            self._increment_stat(
                f"aggregate_partition_invalidation_calls::{dataset_name}::{profile}"
            )
        return len(touched_profiles)

    def _invalidate_aggregate_memory_month(
        self,
        dataset_name: str,
        month_key: str,
    ) -> set[str]:
        touched_profiles: set[str] = set()
        stale_frame_keys = [
            memory_key
            for memory_key in self._aggregate_frame_memory
            if memory_key[0] == dataset_name and memory_key[2] == month_key
        ]
        for memory_key in stale_frame_keys:
            touched_profiles.add(memory_key[1])
            self._aggregate_frame_memory.pop(memory_key, None)

        stale_metadata_keys = [
            memory_key
            for memory_key in self._aggregate_metadata_memory
            if memory_key[0] == dataset_name and memory_key[2] == month_key
        ]
        for memory_key in stale_metadata_keys:
            touched_profiles.add(memory_key[1])
            self._aggregate_metadata_memory.pop(memory_key, None)
        return touched_profiles
