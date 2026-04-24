"""Observability and counter helpers for the cache service."""

from __future__ import annotations

from shared.cache_diagnostics import build_cache_observability_snapshot


class CacheObserverMixin:
    """Provide unified cache observability counters and summaries."""

    def stats_snapshot(self) -> dict[str, int]:
        """Return a snapshot of cache activity counters for diagnostics."""

        return dict(self._stats)

    def observability_snapshot(self) -> dict[str, object]:
        """Return a compact summary of unified cache observability metrics."""

        return build_cache_observability_snapshot(self._stats)

    def record_cache_resolution(
        self,
        outcome: str,
        dataset_name: str,
        profile: str | None = None,
        value: int = 1,
    ) -> None:
        normalized = str(outcome).strip().lower()
        if normalized not in {"hit", "miss", "partial_hit", "refresh_bypass"}:
            raise ValueError(f"Unsupported cache resolution outcome: {outcome}")
        self._increment_scoped_stat(
            f"cache_resolution_{normalized}_calls",
            dataset_name=dataset_name,
            profile=profile,
            value=value,
        )

    def record_file_scan(
        self,
        dataset_name: str,
        profile: str | None = None,
        value: int = 1,
    ) -> None:
        self._increment_scoped_stat(
            "cache_file_scan_calls",
            dataset_name=dataset_name,
            profile=profile,
            value=value,
        )

    def record_remote_fill(
        self,
        dataset_name: str,
        profile: str | None = None,
        value: int = 1,
    ) -> None:
        self._increment_scoped_stat(
            "remote_fill_calls",
            dataset_name=dataset_name,
            profile=profile,
            value=value,
        )

    def record_writeback(
        self,
        dataset_name: str,
        profile: str | None = None,
        value: int = 1,
    ) -> None:
        self._increment_scoped_stat(
            "cache_writeback_calls",
            dataset_name=dataset_name,
            profile=profile,
            value=value,
        )

    def record_stage_timing(
        self,
        stage_name: str,
        elapsed_seconds: float,
        dataset_name: str,
        profile: str | None = None,
    ) -> None:
        elapsed_ms = max(int(round(float(elapsed_seconds) * 1000.0)), 0)
        if float(elapsed_seconds) > 0.0 and elapsed_ms == 0:
            elapsed_ms = 1
        self._increment_scoped_stat(
            "stage_calls",
            dataset_name=dataset_name,
            profile=profile,
            stage_name=stage_name,
        )
        self._increment_scoped_stat(
            "stage_elapsed_ms",
            dataset_name=dataset_name,
            profile=profile,
            stage_name=stage_name,
            value=elapsed_ms,
        )

    def _increment_panel_stat(
        self,
        key: str,
        dataset_name: str,
        profile: str | None,
        value: int = 1,
    ) -> None:
        self._increment_stat(key, value)
        self._increment_stat(f"{key}::{dataset_name}", value)
        if profile is not None:
            self._increment_stat(f"{key}::{dataset_name}::{profile}", value)

    def _increment_scoped_stat(
        self,
        key: str,
        dataset_name: str | None = None,
        profile: str | None = None,
        stage_name: str | None = None,
        value: int = 1,
    ) -> None:
        self._increment_stat(key, value)
        if dataset_name is not None:
            scoped_key = f"{key}::{dataset_name}"
            self._increment_stat(scoped_key, value)
        else:
            scoped_key = key
        if profile is not None and dataset_name is not None:
            scoped_key = f"{scoped_key}::{profile}"
            self._increment_stat(scoped_key, value)
        if stage_name is not None:
            if dataset_name is None:
                self._increment_stat(f"{key}::{stage_name}", value)
            else:
                self._increment_stat(f"{scoped_key}::{stage_name}", value)

    def _increment_stat(self, key: str, value: int = 1) -> None:
        self._stats[key] += int(value)
