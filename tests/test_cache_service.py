from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

import pandas as pd

from data.cache import DataCacheService
from data.cache_store import CacheStore
from data.schema import DataSchema

TMP_ROOT = Path(__file__).resolve().parent / "_tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)


def make_case_dir(case_name: str) -> Path:
    case_dir = TMP_ROOT / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


class DataCacheServiceTests(unittest.TestCase):
    def test_observability_snapshot_summarizes_unified_metrics(self) -> None:
        case_dir = make_case_dir("cache_service_observability_summary")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        service.record_cache_resolution("hit", "cb_daily")
        service.record_cache_resolution("partial_hit", "cb_daily")
        service.record_cache_resolution("refresh_bypass", "cb_daily")
        service.record_file_scan("cb_daily", value=3)
        service.record_remote_fill("cb_daily", value=2)
        service.record_writeback("cb_daily", value=4)
        service.record_stage_timing(
            "remote_fetch",
            0.012,
            dataset_name="cb_daily",
        )

        summary = service.observability_snapshot()

        self.assertEqual(int(summary["cache_hits"]), 1)
        self.assertEqual(int(summary["cache_partial_hits"]), 1)
        self.assertEqual(int(summary["cache_refresh_bypass"]), 1)
        self.assertEqual(int(summary["file_scans"]), 3)
        self.assertEqual(int(summary["remote_fills"]), 2)
        self.assertEqual(int(summary["writebacks"]), 4)
        self.assertGreaterEqual(
            int(summary["stage_elapsed_ms"]["cb_daily::remote_fetch"]),
            1,
        )
        self.assertEqual(
            int(summary["stage_calls"]["cb_daily::remote_fetch"]),
            1,
        )

    def test_request_panel_runtime_memory_cache_hits_after_first_save(self) -> None:
        case_dir = make_case_dir("cache_service_request_panel_memory")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        panel = DataSchema.standardize(
            "cb_daily",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 101.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": pd.NA,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        service.save_request_panel(
            dataset_name="cb_daily_cross_section",
            standardized_name="cb_daily",
            profile="factor_history_v1",
            trade_day_strs=("20260401", "20260402"),
            frame=panel,
            columns=("cb_code", "trade_date", "close", "amount"),
        )

        loaded = service.load_request_panel(
            dataset_name="cb_daily_cross_section",
            standardized_name="cb_daily",
            profile="factor_history_v1",
            trade_day_strs=("20260401", "20260402"),
            columns=("cb_code", "trade_date", "close", "amount"),
        )
        assert loaded is not None
        self.assertAlmostEqual(float(loaded["close"].iloc[0]), 101.0, places=6)

        stats = service.stats_snapshot()
        self.assertEqual(int(stats.get("panel_memory_save_calls", 0)), 1)
        self.assertEqual(int(stats.get("panel_memory_hit_calls", 0)), 1)
        self.assertEqual(int(stats.get("panel_memory_miss_calls", 0)), 0)

    def test_save_time_series_invalidates_request_panel_and_monthly_aggregate(self) -> None:
        case_dir = make_case_dir("cache_service_request_panel_invalidation")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        aggregate = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": "2026-04-01",
                    "close": 101.0,
                    "amount": 20.0,
                    "premium_rate": 3.0,
                    "ytm": pd.NA,
                    "convert_value": 95.0,
                    "is_tradable": True,
                }
            ]
        )
        service.save_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            frame=aggregate,
        )
        service.save_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            payload={
                "covered_trade_days": ["20260401"],
                "projection_columns": ["cb_code", "trade_date", "close", "amount"],
            },
            standardized_name="cb_daily",
        )
        panel = DataSchema.standardize(
            "cb_daily",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 101.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": pd.NA,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )
        service.save_request_panel(
            dataset_name="cb_daily_cross_section",
            standardized_name="cb_daily",
            profile="factor_history_v1",
            trade_day_strs=("20260401",),
            frame=panel,
            columns=("cb_code", "trade_date", "close", "amount"),
        )

        aggregate_dir = (
            case_dir
            / "cache"
            / "tushare"
            / "time_series_aggregate"
            / "cb_daily_cross_section"
            / "factor_history_v1"
        )
        self.assertTrue((aggregate_dir / "202604.csv").exists())
        self.assertTrue((aggregate_dir / "202604.meta.json").exists())

        service.save_time_series(
            dataset_name="cb_daily_cross_section",
            cache_key="20260401",
            frame=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 101.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": 0.052,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        self.assertFalse((aggregate_dir / "202604.csv").exists())
        self.assertFalse((aggregate_dir / "202604.meta.json").exists())
        self.assertIsNone(
            service.load_request_panel(
                dataset_name="cb_daily_cross_section",
                standardized_name="cb_daily",
                profile="factor_history_v1",
                trade_day_strs=("20260401",),
                columns=("cb_code", "trade_date", "close", "amount"),
            )
        )
        stats = service.stats_snapshot()
        self.assertGreaterEqual(int(stats.get("panel_memory_invalidation_calls", 0)), 1)
        self.assertGreaterEqual(
            int(stats.get("aggregate_partition_invalidation_calls", 0)),
            1,
        )

    def test_time_series_aggregate_runtime_memory_cache_hits_after_first_disk_load(self) -> None:
        case_dir = make_case_dir("cache_service_aggregate_memory_load")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        cache_store.save_time_series_aggregate(
            "tushare",
            "cb_daily_cross_section",
            "factor_history_v1",
            "202604",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 101.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": pd.NA,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        loaded = service.load_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            columns=("cb_code", "trade_date", "close", "amount"),
        )
        assert loaded is not None
        self.assertAlmostEqual(float(loaded["close"].iloc[0]), 101.0, places=6)

        reloaded = service.load_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            columns=("cb_code", "trade_date", "close", "amount"),
        )

        assert reloaded is not None
        self.assertAlmostEqual(float(reloaded["close"].iloc[0]), 101.0, places=6)
        stats = service.stats_snapshot()
        self.assertEqual(int(stats.get("aggregate_memory_miss_calls", 0)), 1)
        self.assertEqual(int(stats.get("aggregate_memory_hit_calls", 0)), 1)
        self.assertEqual(
            int(
                stats.get(
                    "aggregate_memory_hit_calls::cb_daily_cross_section::factor_history_v1",
                    0,
                )
            ),
            1,
        )

    def test_time_series_aggregate_save_invalidates_and_primes_runtime_memory(self) -> None:
        case_dir = make_case_dir("cache_service_aggregate_memory_save")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        service.save_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            frame=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 101.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": pd.NA,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        first = service.load_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            columns=("cb_code", "trade_date", "close", "amount"),
        )
        assert first is not None
        self.assertAlmostEqual(float(first["close"].iloc[0]), 101.0, places=6)

        service.save_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            frame=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 102.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": pd.NA,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        reloaded = service.load_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            columns=("cb_code", "trade_date", "close", "amount"),
        )

        assert reloaded is not None
        self.assertAlmostEqual(float(reloaded["close"].iloc[0]), 102.0, places=6)
        stats = service.stats_snapshot()
        self.assertEqual(int(stats.get("aggregate_memory_miss_calls", 0)), 0)
        self.assertEqual(int(stats.get("aggregate_memory_hit_calls", 0)), 2)

    def test_time_series_aggregate_metadata_runtime_memory_cache_hits_after_first_disk_load(self) -> None:
        case_dir = make_case_dir("cache_service_aggregate_metadata_memory_load")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        metadata_path = service.time_series_aggregate_metadata_path(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
        )
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(
                service.build_time_series_aggregate_metadata_payload(
                    dataset_name="cb_daily_cross_section",
                    profile="factor_history_v1",
                    partition_key="202604",
                    covered_trade_days=["20260401", "20260402"],
                    projection_columns=[
                        "cb_code",
                        "trade_date",
                        "close",
                        "amount",
                    ],
                    standardized_name="cb_daily",
                ),
                ensure_ascii=True,
                indent=2,
            ),
            encoding="utf-8",
        )

        loaded = service.load_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
        )
        assert loaded is not None
        self.assertEqual(loaded["covered_trade_days"], ["20260401", "20260402"])

        reloaded = service.load_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
        )

        assert reloaded is not None
        self.assertEqual(reloaded["covered_trade_days"], ["20260401", "20260402"])
        stats = service.stats_snapshot()
        self.assertEqual(int(stats.get("aggregate_metadata_memory_miss_calls", 0)), 1)
        self.assertEqual(int(stats.get("aggregate_metadata_memory_hit_calls", 0)), 1)

    def test_time_series_aggregate_metadata_save_invalidates_and_primes_runtime_memory(self) -> None:
        case_dir = make_case_dir("cache_service_aggregate_metadata_memory_save")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        service.save_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            payload={
                "covered_trade_days": ["20260401"],
                "projection_columns": ["cb_code", "trade_date", "close"],
            },
            standardized_name="cb_daily",
        )

        first = service.load_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
        )
        assert first is not None
        self.assertEqual(first["covered_trade_days"], ["20260401"])

        service.save_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            payload={
                "covered_trade_days": ["20260401", "20260402"],
                "projection_columns": ["cb_code", "trade_date", "close", "amount"],
            },
            standardized_name="cb_daily",
        )

        reloaded = service.load_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
        )

        assert reloaded is not None
        self.assertEqual(reloaded["covered_trade_days"], ["20260401", "20260402"])
        stats = service.stats_snapshot()
        self.assertEqual(int(stats.get("aggregate_metadata_memory_miss_calls", 0)), 0)
        self.assertEqual(int(stats.get("aggregate_metadata_memory_hit_calls", 0)), 2)

    def test_load_grouped_time_series_batches_and_tracks_missing_keys(self) -> None:
        case_dir = make_case_dir("cache_service_grouped_load")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        for code, coupon in [("110001.SH", 5.0), ("110002.SH", 3.0)]:
            cache_store.save_time_series(
                "tushare",
                "cb_rate",
                code,
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "rate_frequency": 1,
                            "rate_start_date": "2025-04-01",
                            "rate_end_date": "2026-04-01",
                            "coupon_rate": coupon,
                        }
                    ]
                ),
            )

        cached_by_key, missing_keys = service.load_grouped_time_series(
            dataset_name="cb_rate",
            cache_keys=["110001.SH", "110002.SH", "110003.SH"],
            standardized_name="cb_rate",
            group_column="cb_code",
            columns=("cb_code", "rate_start_date", "coupon_rate"),
        )

        self.assertEqual(set(cached_by_key), {"110001.SH", "110002.SH"})
        self.assertEqual(missing_keys, ["110003.SH"])
        self.assertEqual(
            cached_by_key["110001.SH"]["cb_code"].iloc[0],
            "110001.SH",
        )
        self.assertNotIn("rate_frequency", cached_by_key["110001.SH"].columns)
        self.assertEqual(
            int(service.stats_snapshot()["grouped_time_series_file_reads"]),
            3,
        )
        self.assertEqual(
            int(service.stats_snapshot()["grouped_time_series_file_reads::cb_rate"]),
            3,
        )
        self.assertEqual(
            int(service.stats_snapshot()["cache_file_scan_calls"]),
            3,
        )
        self.assertEqual(
            int(service.stats_snapshot()["cache_file_scan_calls::cb_rate"]),
            3,
        )

    def test_time_series_coverage_round_trip(self) -> None:
        case_dir = make_case_dir("cache_service_coverage")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        start_ts = pd.Timestamp("2026-04-01")
        end_ts = pd.Timestamp("2026-04-10")
        service.save_time_series_coverage("cb_call", "ALL", start_ts, end_ts)
        loaded = service.load_time_series_coverage("cb_call", "ALL")

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["start"], start_ts)
        self.assertEqual(loaded["end"], end_ts)
        self.assertTrue(
            service.covers_sparse_range(
                loaded,
                pd.Timestamp("2026-04-03"),
                pd.Timestamp("2026-04-09"),
            )
        )
        self.assertFalse(
            service.covers_sparse_range(
                loaded,
                pd.Timestamp("2026-03-31"),
                pd.Timestamp("2026-04-09"),
            )
        )

    def test_runtime_content_generation_increments_on_write_paths(self) -> None:
        case_dir = make_case_dir("cache_service_runtime_content_generation")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        self.assertEqual(service.runtime_content_generation(), 0)

        service.save_time_series(
            dataset_name="cb_daily_cross_section",
            cache_key="20260401",
            frame=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 101.0,
                        "amount": 20.0,
                        "premium_rate": 3.0,
                        "ytm": pd.NA,
                        "convert_value": 95.0,
                        "is_tradable": True,
                    }
                ]
            ),
        )
        self.assertEqual(service.runtime_content_generation(), 1)

        service.save_time_series_coverage(
            dataset_name="cb_call",
            cache_key="ALL",
            start_ts=pd.Timestamp("2026-04-01"),
            end_ts=pd.Timestamp("2026-04-10"),
        )
        self.assertEqual(service.runtime_content_generation(), 2)

        service.save_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            payload={
                "covered_trade_days": ["20260401"],
                "projection_columns": ["cb_code", "trade_date", "close", "amount"],
            },
            standardized_name="cb_daily",
        )
        self.assertEqual(service.runtime_content_generation(), 3)

    def test_time_series_coverage_missing_governance_is_rejected(self) -> None:
        case_dir = make_case_dir("cache_service_coverage_missing_governance")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        metadata_path = service.time_series_coverage_path("cb_call", "ALL")
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(
                {
                    "start": "2026-04-01",
                    "end": "2026-04-10",
                },
                ensure_ascii=True,
                indent=2,
            ),
            encoding="utf-8",
        )

        self.assertIsNone(service.load_time_series_coverage("cb_call", "ALL"))

    def test_time_series_aggregate_round_trip_and_trade_day_coverage(self) -> None:
        case_dir = make_case_dir("cache_service_aggregate")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        aggregate = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": "2026-04-01",
                    "close": 101.0,
                    "amount": 20.0,
                    "premium_rate": 3.0,
                    "ytm": pd.NA,
                    "convert_value": 95.0,
                    "is_tradable": True,
                }
            ]
        )
        service.save_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            frame=aggregate,
        )
        service.save_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            payload={
                "covered_trade_days": ["20260401", "20260402"],
                "projection_columns": [
                    "cb_code",
                    "trade_date",
                    "close",
                    "amount",
                ],
            },
            standardized_name="cb_daily",
        )

        loaded = service.load_time_series_aggregate(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            columns=("cb_code", "trade_date", "close", "amount"),
        )
        metadata = service.load_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
        )

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertTrue(
            {"cb_code", "trade_date", "close", "amount"}.issubset(loaded.columns)
        )
        self.assertTrue(
            service.covers_aggregate_trade_days(metadata, ["20260401", "20260402"])
        )
        self.assertFalse(
            service.covers_aggregate_trade_days(metadata, ["20260401", "20260403"])
        )

    def test_time_series_aggregate_metadata_missing_governance_is_rejected(self) -> None:
        case_dir = make_case_dir("cache_service_aggregate_metadata_missing_governance")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        metadata_path = service.time_series_aggregate_metadata_path(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
        )
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(
                {
                    "covered_trade_days": ["20260401", "20260402"],
                    "projection_columns": ["cb_code", "trade_date", "close", "amount"],
                },
                ensure_ascii=True,
                indent=2,
            ),
            encoding="utf-8",
        )

        self.assertIsNone(
            service.load_time_series_aggregate_metadata(
                dataset_name="cb_daily_cross_section",
                profile="factor_history_v1",
                partition_key="202604",
                standardized_name="cb_daily",
                requested_columns=("cb_code", "trade_date", "close", "amount"),
            )
        )

    def test_time_series_aggregate_metadata_projection_signature_mismatch_is_rejected(
        self,
    ) -> None:
        case_dir = make_case_dir("cache_service_aggregate_metadata_projection_mismatch")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        payload = service.build_time_series_aggregate_metadata_payload(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            covered_trade_days=["20260401", "20260402"],
            projection_columns=["cb_code", "trade_date", "close", "amount"],
            standardized_name="cb_daily",
        )
        assert isinstance(payload.get("governance"), dict)
        payload["governance"]["projection_signature"] = "stale_signature"
        metadata_path = service.time_series_aggregate_metadata_path(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
        )
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

        self.assertIsNone(
            service.load_time_series_aggregate_metadata(
                dataset_name="cb_daily_cross_section",
                profile="factor_history_v1",
                partition_key="202604",
                standardized_name="cb_daily",
                requested_columns=("cb_code", "trade_date", "close", "amount"),
            )
        )

    def test_writeback_derived_fields_only_fills_missing_values(self) -> None:
        case_dir = make_case_dir("cache_service_writeback")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        cache_store.save_time_series(
            "tushare",
            "cb_daily_cross_section",
            "20260401",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "pre_close": 100.0,
                        "open": 101.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 101.0,
                        "price_change": 1.0,
                        "pct_change": 1.0,
                        "volume": 10.0,
                        "amount": 20.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    },
                    {
                        "cb_code": "110002.SH",
                        "trade_date": "2026-04-01",
                        "pre_close": 100.0,
                        "open": 100.0,
                        "high": 100.0,
                        "low": 99.0,
                        "close": 100.0,
                        "price_change": 0.0,
                        "pct_change": 0.0,
                        "volume": 8.0,
                        "amount": 16.0,
                        "ytm": 0.031,
                        "is_tradable": True,
                    },
                ]
            ),
        )

        service.writeback_derived_fields(
            dataset_name="cb_daily_cross_section",
            updates=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": pd.Timestamp("2026-04-01"),
                        "ytm": 0.052,
                    },
                    {
                        "cb_code": "110002.SH",
                        "trade_date": pd.Timestamp("2026-04-01"),
                        "ytm": 0.099,
                    },
                ]
            ),
            columns=("ytm",),
            standardized_name="cb_daily",
        )

        cached = service.load_time_series(
            "cb_daily_cross_section",
            "20260401",
            standardized_name="cb_daily",
        )
        assert cached is not None
        filled = cached.set_index("cb_code")["ytm"]
        self.assertAlmostEqual(float(filled["110001.SH"]), 0.052, places=6)
        self.assertAlmostEqual(float(filled["110002.SH"]), 0.031, places=6)
        self.assertEqual(service.stats_snapshot()["derived_writeback_files"], 1)
        self.assertEqual(
            int(service.stats_snapshot()["cache_writeback_calls::cb_daily_cross_section"]),
            1,
        )

    def test_inspect_local_env_history_start_uses_latest_required_start(self) -> None:
        case_dir = make_case_dir("cache_service_env_history_start")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        service.save_calendar(
            "SSE",
            pd.DataFrame(
                [
                    {"exchange": "SSE", "calendar_date": "2026-01-02", "is_open": 1},
                    {"exchange": "SSE", "calendar_date": "2026-01-03", "is_open": 1},
                ]
            ),
        )
        service.save_time_series(
            "index_daily",
            "000300.SH",
            pd.DataFrame(
                [
                    {
                        "index_code": "000300.SH",
                        "trade_date": "2026-01-05",
                        "close": 4000.0,
                        "open": 3990.0,
                        "high": 4010.0,
                        "low": 3980.0,
                        "pre_close": 3985.0,
                        "price_change": 15.0,
                        "pct_change": 0.38,
                        "volume": 1.0,
                        "amount": 1000.0,
                    }
                ]
            ),
        )
        service.save_time_series(
            "index_daily",
            "H11001.CSI",
            pd.DataFrame(
                [
                    {
                        "index_code": "H11001.CSI",
                        "trade_date": "2026-01-04",
                        "close": 250.0,
                        "open": 249.0,
                        "high": 251.0,
                        "low": 248.0,
                        "pre_close": 249.5,
                        "price_change": 0.5,
                        "pct_change": 0.2,
                        "volume": 1.0,
                        "amount": 100.0,
                    }
                ]
            ),
        )
        service.save_time_series(
            "yield_curve",
            "1001.CB__0__10",
            pd.DataFrame(
                [
                    {
                        "curve_code": "1001.CB",
                        "trade_date": "2026-01-06",
                        "curve_type": "0",
                        "curve_term": 10.0,
                        "yield_value": 1.8,
                    }
                ]
            ),
        )
        service.save_time_series(
            "cb_equal_weight",
            "ALL",
            pd.DataFrame(
                [
                    {
                        "indicator_code": "cb_equal_weight",
                        "trade_date": "2026-01-03",
                        "value": 100.0,
                        "source_table": "cb_daily_cross_section",
                    }
                ]
            ),
        )
        reference_path = service.reference_frame_path("macro", "credit_spread")
        reference_path.parent.mkdir(parents=True, exist_ok=True)
        DataSchema.standardize(
            "macro_daily",
            pd.DataFrame(
                [
                    {
                        "indicator_code": "credit_spread",
                        "trade_date": "2026-01-07",
                        "value": 1.2,
                        "source_table": "local_reference",
                    }
                ]
            ),
        ).to_csv(reference_path, index=False, encoding="utf-8-sig")

        history_start = service.inspect_local_env_history_start(
            calendar_exchange="SSE",
            treasury_curve_code="1001.CB",
            treasury_curve_type="0",
            treasury_curve_term=10.0,
        )

        self.assertEqual(history_start, pd.Timestamp("2026-01-07"))

    def test_inspect_local_factor_history_start_prefers_cross_section_then_code_cache(self) -> None:
        case_dir = make_case_dir("cache_service_factor_history_start")
        cache_store = CacheStore(case_dir / "cache")
        service = DataCacheService(cache_store=cache_store, source_name="tushare")

        cache_store.save_time_series(
            "tushare",
            "cb_daily_cross_section",
            "20260401",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "pre_close": 100.0,
                        "open": 101.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 101.0,
                        "price_change": 1.0,
                        "pct_change": 1.0,
                        "volume": 10.0,
                        "amount": 20.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    }
                ]
            ),
        )
        cache_store.save_time_series(
            "tushare",
            "cb_daily_cross_section",
            "20260403",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-03",
                        "pre_close": 101.0,
                        "open": 102.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 102.0,
                        "price_change": 1.0,
                        "pct_change": 0.99,
                        "volume": 11.0,
                        "amount": 21.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        history_start = service.inspect_local_factor_history_start(["110001.SH"])
        self.assertEqual(history_start, pd.Timestamp("2026-04-01"))

        shutil.rmtree(cache_store.base_dir / "tushare" / "time_series" / "cb_daily_cross_section")
        cache_store.save_time_series(
            "tushare",
            "cb_daily",
            "110001.SH",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-02",
                        "pre_close": 100.0,
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "price_change": 0.0,
                        "pct_change": 0.0,
                        "volume": 10.0,
                        "amount": 20.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    }
                ]
            ),
        )
        cache_store.save_time_series(
            "tushare",
            "cb_daily",
            "110002.SH",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110002.SH",
                        "trade_date": "2026-04-05",
                        "pre_close": 101.0,
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "price_change": 0.0,
                        "pct_change": 0.0,
                        "volume": 10.0,
                        "amount": 20.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        fallback_start = service.inspect_local_factor_history_start(
            ["110001.SH", "110002.SH"]
        )
        self.assertEqual(fallback_start, pd.Timestamp("2026-04-02"))


if __name__ == "__main__":
    unittest.main()
