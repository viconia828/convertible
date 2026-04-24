"""Coverage and local-history helpers for the cache service."""

from __future__ import annotations

import json
from typing import Iterable

import pandas as pd

from ..schema import DataSchema
from .models import CoverageWindow


class CacheCoverageMixin:
    """Provide coverage checks, grouped reads, and local-history inspection."""

    def load_grouped_time_series(
        self,
        dataset_name: str,
        cache_keys: list[str],
        standardized_name: str,
        group_column: str,
        group_as_trade_date_key: bool = False,
        columns: Iterable[str] | None = None,
    ) -> tuple[dict[str, pd.DataFrame], list[str]]:
        """Batch-load many cache files and standardize them only once."""

        if not cache_keys:
            return {}, []

        requested_columns = self._normalize_requested_columns(
            standardized_name=standardized_name,
            columns=columns,
            extra_columns=(group_column,),
        )
        raw_frames: list[pd.DataFrame] = []
        for cache_key in cache_keys:
            self.record_file_scan(dataset_name)
            cached = self.cache_store.load_time_series(
                self.source_name,
                dataset_name,
                cache_key,
                columns=requested_columns,
            )
            self._increment_stat("grouped_time_series_file_reads")
            self._increment_stat(f"grouped_time_series_file_reads::{dataset_name}")
            if cached is None or cached.empty:
                continue
            raw_frames.append(cached)

        if not raw_frames:
            return {}, cache_keys

        try:
            combined = DataSchema.standardize(
                standardized_name,
                pd.concat(raw_frames, ignore_index=True),
            )
        except Exception:  # noqa: BLE001
            return {}, cache_keys

        if combined.empty or group_column not in combined.columns:
            return {}, cache_keys

        grouped = combined.copy()
        if group_as_trade_date_key:
            grouped["__cache_key"] = (
                pd.to_datetime(grouped[group_column], errors="coerce").dt.strftime("%Y%m%d")
            )
        else:
            grouped["__cache_key"] = grouped[group_column].fillna("").astype(str)
        grouped = grouped.dropna(subset=["__cache_key"])

        cached_by_key = {
            str(cache_key): frame.drop(columns="__cache_key").reset_index(drop=True)
            for cache_key, frame in grouped.groupby("__cache_key", sort=False)
        }
        missing_keys = [cache_key for cache_key in cache_keys if cache_key not in cached_by_key]
        return cached_by_key, missing_keys

    def covers_time_series(
        self,
        frame: pd.DataFrame | None,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        date_column: str,
    ) -> bool:
        if frame is None or frame.empty or date_column not in frame.columns:
            return False
        values = frame[date_column].dropna()
        if values.empty:
            return False
        return bool(values.min() <= start_ts and values.max() >= end_ts)

    def covers_expected_dates(
        self,
        frame: pd.DataFrame | None,
        expected_dates: list[object],
        date_column: str,
        indicator_code: str | None = None,
    ) -> bool:
        if not expected_dates:
            return True
        if frame is None or frame.empty or date_column not in frame.columns:
            return False

        working = frame
        if indicator_code is not None and "indicator_code" in working.columns:
            working = working.loc[working["indicator_code"] == indicator_code]
        if working.empty:
            return False

        actual_dates = (
            pd.to_datetime(working[date_column], errors="coerce")
            .dropna()
            .dt.normalize()
            .unique()
        )
        if len(actual_dates) == 0:
            return False

        expected_index = pd.DatetimeIndex(pd.to_datetime(expected_dates, errors="coerce")).dropna()
        if expected_index.empty:
            return True
        return bool(expected_index.normalize().isin(actual_dates).all())

    def covers_sparse_range(
        self,
        coverage: CoverageWindow | None,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> bool:
        if coverage is None:
            return False
        return bool(coverage["start"] <= start_ts and coverage["end"] >= end_ts)

    def load_time_series_coverage(
        self,
        dataset_name: str,
        cache_key: str,
        standardized_name: str | None = None,
    ) -> CoverageWindow | None:
        metadata_path = self.time_series_coverage_path(dataset_name, cache_key)
        self.record_file_scan(dataset_name)
        if not metadata_path.exists():
            return None
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        self._increment_stat("coverage_load_calls")
        self._increment_stat(f"coverage_load_calls::{dataset_name}")
        return self.parse_time_series_coverage_payload(
            payload=payload,
            dataset_name=dataset_name,
            cache_key=cache_key,
            standardized_name=standardized_name,
        )

    def save_time_series_coverage(
        self,
        dataset_name: str,
        cache_key: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        standardized_name: str | None = None,
    ) -> None:
        metadata_path = self.time_series_coverage_path(dataset_name, cache_key)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.build_time_series_coverage_payload(
            dataset_name=dataset_name,
            cache_key=cache_key,
            start_ts=start_ts,
            end_ts=end_ts,
            standardized_name=standardized_name,
        )
        metadata_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        self._increment_stat("coverage_save_calls")
        self._increment_stat(f"coverage_save_calls::{dataset_name}")
        self.record_writeback(dataset_name)
        self.mark_runtime_content_mutation()

    def load_time_series_aggregate_metadata(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
        standardized_name: str | None = None,
        requested_columns: Iterable[str] | None = None,
    ) -> dict[str, object] | None:
        memory_key = self._aggregate_metadata_memory_key(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
        )
        cached = self._load_aggregate_metadata_from_memory(memory_key)
        if cached is not None:
            self._increment_stat("aggregate_metadata_load_calls")
            self._increment_stat(f"aggregate_metadata_load_calls::{dataset_name}")
            self._increment_stat(
                f"aggregate_metadata_load_calls::{dataset_name}::{profile}"
            )
            if not self.aggregate_metadata_matches_request(
                metadata=cached,
                dataset_name=dataset_name,
                profile=profile,
                partition_key=partition_key,
                standardized_name=standardized_name,
                requested_columns=requested_columns,
            ):
                return None
            self._increment_stat("aggregate_metadata_memory_hit_calls")
            self._increment_stat(
                f"aggregate_metadata_memory_hit_calls::{dataset_name}"
            )
            self._increment_stat(
                f"aggregate_metadata_memory_hit_calls::{dataset_name}::{profile}"
            )
            return cached

        metadata_path = self.time_series_aggregate_metadata_path(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
        )
        self.record_file_scan(dataset_name, profile=profile)
        if not metadata_path.exists():
            return None
        self._increment_stat("aggregate_metadata_load_calls")
        self._increment_stat(f"aggregate_metadata_load_calls::{dataset_name}")
        self._increment_stat(
            f"aggregate_metadata_load_calls::{dataset_name}::{profile}"
        )
        self._increment_stat("aggregate_metadata_memory_miss_calls")
        self._increment_stat(
            f"aggregate_metadata_memory_miss_calls::{dataset_name}"
        )
        self._increment_stat(
            f"aggregate_metadata_memory_miss_calls::{dataset_name}::{profile}"
        )
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        if not self.aggregate_metadata_matches_request(
            metadata=payload,
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
            standardized_name=standardized_name,
            requested_columns=requested_columns
            if requested_columns is not None
            else payload.get("projection_columns", []),
        ):
            return None
        self._save_aggregate_metadata_to_memory(memory_key, payload)
        return payload

    def save_time_series_aggregate_metadata(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
        payload: dict[str, object],
        standardized_name: str | None = None,
    ) -> None:
        metadata_path = self.time_series_aggregate_metadata_path(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
        )
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        governed_payload = self.build_time_series_aggregate_metadata_payload(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
            covered_trade_days=payload.get("covered_trade_days", []),
            projection_columns=payload.get("projection_columns", []),
            standardized_name=standardized_name,
        )
        metadata_path.write_text(
            json.dumps(governed_payload, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
        self._increment_stat("aggregate_metadata_save_calls")
        self._increment_stat(f"aggregate_metadata_save_calls::{dataset_name}")
        self._increment_stat(
            f"aggregate_metadata_save_calls::{dataset_name}::{profile}"
        )
        self.record_writeback(dataset_name, profile=profile)
        memory_key = self._aggregate_metadata_memory_key(
            dataset_name=dataset_name,
            profile=profile,
            partition_key=partition_key,
        )
        self._invalidate_aggregate_metadata_memory(memory_key)
        self._save_aggregate_metadata_to_memory(memory_key, governed_payload)
        self.invalidate_request_panels(dataset_name=dataset_name, profile=profile)
        self.mark_runtime_content_mutation()

    def inspect_local_env_history_start(
        self,
        calendar_exchange: str,
        treasury_curve_code: str,
        treasury_curve_type: str,
        treasury_curve_term: float,
    ) -> pd.Timestamp | None:
        frames = [
            self.load_calendar(calendar_exchange, standardized_name="trading_calendar"),
            self.load_time_series("index_daily", "000300.SH", standardized_name="index_daily"),
            self.load_time_series("index_daily", "H11001.CSI", standardized_name="index_daily"),
            self.load_time_series(
                "yield_curve",
                f"{treasury_curve_code}__{treasury_curve_type}__{treasury_curve_term:g}",
                standardized_name="yield_curve",
            ),
            self.load_time_series("cb_equal_weight", "ALL", standardized_name="macro_daily"),
            self.load_reference_frame("macro", "credit_spread", standardized_name="macro_daily"),
        ]
        if any(frame is None or frame.empty for frame in frames):
            return None
        candidates = [
            self._safe_min_timestamp(frames[0], "calendar_date"),
            self._safe_min_timestamp(frames[1], "trade_date"),
            self._safe_min_timestamp(frames[2], "trade_date"),
            self._safe_min_timestamp(frames[3], "trade_date"),
            self._safe_min_timestamp(frames[4], "trade_date"),
            self._safe_min_timestamp(frames[5], "trade_date"),
        ]
        return max(candidates)

    def inspect_local_factor_history_start(
        self,
        codes: Iterable[str],
    ) -> pd.Timestamp | None:
        cross_section_dir = (
            self.cache_store.base_dir
            / self.source_name
            / "time_series"
            / "cb_daily_cross_section"
        )
        if cross_section_dir.exists():
            csv_paths = [path for path in cross_section_dir.glob("*.csv")]
            self.record_file_scan("cb_daily_cross_section", value=len(csv_paths))
            trade_days = [
                pd.Timestamp(path.stem).normalize()
                for path in csv_paths
                if path.stem.isdigit() and len(path.stem) == 8
            ]
            if trade_days:
                return min(trade_days)

        normalized_codes = list(dict.fromkeys(str(code) for code in codes if str(code)))
        if not normalized_codes:
            return None
        frames = [
            self.load_time_series("cb_daily", code, standardized_name="cb_daily")
            for code in normalized_codes
        ]
        available_frames = [frame for frame in frames if frame is not None and not frame.empty]
        if not available_frames:
            return None
        starts = [self._safe_min_timestamp(frame, "trade_date") for frame in available_frames]
        return min(starts)

    def covers_aggregate_trade_days(
        self,
        metadata: dict[str, object] | None,
        required_trade_days: Iterable[str],
    ) -> bool:
        if metadata is None:
            return False
        covered = {
            str(item)
            for item in metadata.get("covered_trade_days", [])
            if str(item)
        }
        required = {str(item) for item in required_trade_days if str(item)}
        if not required:
            return True
        return required.issubset(covered)
