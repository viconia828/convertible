"""Metadata schema/version governance helpers for the cache service."""

from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd

from ..schema import DataSchema
from .models import CoverageWindow

CACHE_METADATA_FORMAT_VERSION = 1


class CacheGovernanceMixin:
    """Build and validate cache metadata governance blocks."""

    def build_time_series_coverage_payload(
        self,
        dataset_name: str,
        cache_key: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        standardized_name: str | None = None,
    ) -> dict[str, object]:
        schema_name = standardized_name or dataset_name
        return {
            "start": pd.Timestamp(start_ts).strftime("%Y-%m-%d"),
            "end": pd.Timestamp(end_ts).strftime("%Y-%m-%d"),
            "governance": {
                "metadata_format_version": CACHE_METADATA_FORMAT_VERSION,
                "dataset_name": str(dataset_name),
                "cache_key": str(cache_key),
                "standardized_name": str(schema_name),
                "schema_signature": DataSchema.schema_signature(schema_name),
            },
        }

    def parse_time_series_coverage_payload(
        self,
        payload: dict[str, object],
        dataset_name: str,
        cache_key: str,
        standardized_name: str | None = None,
    ) -> CoverageWindow | None:
        if not self._matches_metadata_governance(
            payload=payload,
            dataset_name=dataset_name,
            standardized_name=standardized_name or dataset_name,
            cache_key=cache_key,
        ):
            return None
        try:
            return {
                "start": pd.Timestamp(payload["start"]).normalize(),
                "end": pd.Timestamp(payload["end"]).normalize(),
            }
        except Exception:  # noqa: BLE001
            return None

    def build_time_series_aggregate_metadata_payload(
        self,
        dataset_name: str,
        profile: str,
        partition_key: str,
        covered_trade_days: Iterable[str],
        projection_columns: Iterable[str] | None,
        standardized_name: str | None = None,
    ) -> dict[str, object]:
        schema_name = standardized_name or dataset_name
        normalized_projection = self._normalize_projection_columns(projection_columns)
        return {
            "covered_trade_days": [str(day) for day in covered_trade_days if str(day)],
            "projection_columns": list(normalized_projection),
            "governance": {
                "metadata_format_version": CACHE_METADATA_FORMAT_VERSION,
                "dataset_name": str(dataset_name),
                "profile": str(profile),
                "partition_key": str(partition_key),
                "standardized_name": str(schema_name),
                "schema_signature": DataSchema.schema_signature(schema_name),
                "projection_signature": self._projection_signature(normalized_projection),
            },
        }

    def aggregate_metadata_matches_request(
        self,
        metadata: dict[str, object] | None,
        dataset_name: str,
        profile: str,
        partition_key: str,
        standardized_name: str | None,
        requested_columns: Iterable[str] | None,
    ) -> bool:
        if not self._matches_metadata_governance(
            payload=metadata,
            dataset_name=dataset_name,
            standardized_name=standardized_name or dataset_name,
            profile=profile,
            partition_key=partition_key,
        ):
            return False

        governance = metadata.get("governance") if isinstance(metadata, dict) else None
        if not isinstance(governance, dict):
            return False
        stored_projection = self._normalize_projection_columns(
            metadata.get("projection_columns", []) if isinstance(metadata, dict) else ()
        )
        if str(governance.get("projection_signature", "")) != self._projection_signature(
            stored_projection
        ):
            return False
        requested_projection = self._normalize_projection_columns(requested_columns)
        if not requested_projection:
            return True
        return set(requested_projection).issubset(set(stored_projection))

    def _matches_metadata_governance(
        self,
        payload: dict[str, object] | None,
        dataset_name: str,
        standardized_name: str,
        cache_key: str | None = None,
        profile: str | None = None,
        partition_key: str | None = None,
    ) -> bool:
        if not isinstance(payload, dict):
            return False
        governance = payload.get("governance")
        if not isinstance(governance, dict):
            return False
        try:
            metadata_format_version = int(
                governance.get("metadata_format_version", -1)
            )
        except (TypeError, ValueError):
            return False
        if metadata_format_version != CACHE_METADATA_FORMAT_VERSION:
            return False
        if str(governance.get("dataset_name", "")) != str(dataset_name):
            return False
        if str(governance.get("standardized_name", "")) != str(standardized_name):
            return False
        if str(governance.get("schema_signature", "")) != DataSchema.schema_signature(
            standardized_name
        ):
            return False
        if cache_key is not None and str(governance.get("cache_key", "")) != str(cache_key):
            return False
        if profile is not None and str(governance.get("profile", "")) != str(profile):
            return False
        if partition_key is not None and str(governance.get("partition_key", "")) != str(
            partition_key
        ):
            return False
        return True

    @staticmethod
    def _normalize_projection_columns(
        columns: Iterable[str] | None,
    ) -> tuple[str, ...]:
        if columns is None:
            return ()
        return tuple(dict.fromkeys(str(column) for column in columns if str(column)))

    @staticmethod
    def _projection_signature(columns: Iterable[str] | None) -> str:
        normalized = tuple(str(column) for column in columns or ())
        joined = ",".join(normalized)
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
