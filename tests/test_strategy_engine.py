from __future__ import annotations

import unittest
from dataclasses import dataclass

import pandas as pd

from config.strategy_config import StrategyPortfolioParameters
from shared.cache_diagnostics import build_cache_diagnostics
from env.environment_detector import EnvironmentComputationResult
from env.macro_alignment import MacroAlignmentSummary
from strategy.engine import StrategyEngine
from strategy.portfolio import PortfolioBuilder
from strategy.snapshot import StrategyHistoryWindow, StrategySnapshot


class FakeDetector:
    def __init__(self) -> None:
        self.calls = 0

    def compute_aligned_with_warmup(self, macro_daily, trading_calendar):
        self.calls += 1
        scores = pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "equity_strength": 0.7,
                    "bond_strength": 0.4,
                    "trend_strength": 0.8,
                }
            ]
        )
        readiness = pd.DataFrame(
            [
                {"trade_date": pd.Timestamp("2026-04-20"), "fully_ready": True},
            ]
        )
        return (
            EnvironmentComputationResult(
                scores=scores,
                readiness=readiness,
                first_fully_ready_trade_date=pd.Timestamp("2026-04-20"),
            ),
            MacroAlignmentSummary(
                total_calendar_days=1,
                kept_days=1,
                dropped_days=0,
                filled_days_by_indicator={},
                invalid_days_by_indicator={},
            ),
        )


class NotReadyDetector(FakeDetector):
    def compute_aligned_with_warmup(self, macro_daily, trading_calendar):
        self.calls += 1
        scores = pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "equity_strength": 0.7,
                    "bond_strength": 0.4,
                    "trend_strength": 0.8,
                }
            ]
        )
        readiness = pd.DataFrame(
            [
                {"trade_date": pd.Timestamp("2026-04-20"), "fully_ready": False},
            ]
        )
        return (
            EnvironmentComputationResult(
                scores=scores,
                readiness=readiness,
                first_fully_ready_trade_date=pd.Timestamp("2026-04-21"),
            ),
            MacroAlignmentSummary(
                total_calendar_days=1,
                kept_days=1,
                dropped_days=0,
                filled_days_by_indicator={},
                invalid_days_by_indicator={},
            ),
        )


class FakeFactorEngine:
    SCORE_COLUMNS = (
        "value_score",
        "carry_score",
        "structure_score",
        "trend_score",
        "stability_score",
    )

    def compute_with_diagnostics(
        self,
        as_of_date,
        cb_daily,
        cb_basic,
        cb_call,
        cb_rate,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp(as_of_date),
                    "eligible": True,
                    "exclude_reason": "",
                    "value_score": 1.0,
                    "carry_score": 0.3,
                    "structure_score": 0.9,
                    "trend_score": 0.8,
                    "stability_score": 0.2,
                },
                {
                    "cb_code": "110002.SH",
                    "trade_date": pd.Timestamp(as_of_date),
                    "eligible": True,
                    "exclude_reason": "",
                    "value_score": 0.7,
                    "carry_score": 0.4,
                    "structure_score": 0.5,
                    "trend_score": 0.6,
                    "stability_score": 0.7,
                },
                {
                    "cb_code": "110003.SH",
                    "trade_date": pd.Timestamp(as_of_date),
                    "eligible": False,
                    "exclude_reason": "not_tradable",
                    "value_score": pd.NA,
                    "carry_score": pd.NA,
                    "structure_score": pd.NA,
                    "trend_score": pd.NA,
                    "stability_score": pd.NA,
                },
            ]
        )

    def append_weighted_total_score(self, frame, factor_weights, column_name="total_score"):
        result = frame.copy()
        total = pd.Series(0.0, index=result.index, dtype="float64")
        mapping = {
            "value": "value_score",
            "carry": "carry_score",
            "structure": "structure_score",
            "trend": "trend_score",
            "stability": "stability_score",
        }
        for factor_name, column in mapping.items():
            total = total + result[column].fillna(0.0).astype("float64") * float(
                factor_weights[factor_name]
            )
        result[column_name] = total
        result.loc[~result["eligible"].fillna(False), column_name] = pd.NA
        return result


class RecordingFactorEngine(FakeFactorEngine):
    def __init__(self) -> None:
        self.last_cb_daily: pd.DataFrame | None = None
        self.last_cb_basic: pd.DataFrame | None = None
        self.last_cb_call: pd.DataFrame | None = None
        self.last_cb_rate: pd.DataFrame | None = None

    def compute_with_diagnostics(
        self,
        as_of_date,
        cb_daily,
        cb_basic,
        cb_call,
        cb_rate,
    ) -> pd.DataFrame:
        self.last_cb_daily = cb_daily.copy()
        self.last_cb_basic = cb_basic.copy()
        self.last_cb_call = cb_call.copy()
        self.last_cb_rate = None if cb_rate is None else cb_rate.copy()
        return super().compute_with_diagnostics(
            as_of_date=as_of_date,
            cb_daily=cb_daily,
            cb_basic=cb_basic,
            cb_call=cb_call,
            cb_rate=cb_rate,
        )


class FakeWeightMapper:
    def compute(self, env):
        return {
            "value": 0.30,
            "carry": 0.20,
            "structure": 0.20,
            "trend": 0.20,
            "stability": 0.10,
        }


class StrategyEngineTests(unittest.TestCase):
    def _build_snapshot(self) -> StrategySnapshot:
        trade_date = pd.Timestamp("2026-04-20")
        return StrategySnapshot(
            trade_date=trade_date,
            history_window=StrategyHistoryWindow(
                trade_date=trade_date,
                requested_start=pd.Timestamp("2025-10-01"),
                used_start=pd.Timestamp("2025-10-01"),
            ),
            trading_calendar=pd.DataFrame(
                [{"calendar_date": trade_date, "is_open": 1}]
            ),
            macro_daily=pd.DataFrame(),
            cb_daily=pd.DataFrame(),
            cb_basic=pd.DataFrame(),
            cb_call=pd.DataFrame(),
        )

    def test_engine_is_deterministic_for_same_snapshot(self) -> None:
        engine = StrategyEngine(
            detector=FakeDetector(),
            factor_engine=FakeFactorEngine(),
            weight_mapper=FakeWeightMapper(),
            portfolio_builder=PortfolioBuilder(
                params=StrategyPortfolioParameters(
                    top_n=2,
                    min_names=1,
                    weighting_method="score_proportional",
                    single_name_max_weight=0.70,
                    cash_buffer=0.0,
                )
            ),
        )
        snapshot = self._build_snapshot()

        first = engine.run(snapshot)
        second = engine.run(snapshot)

        self.assertEqual(first.environment, second.environment)
        self.assertEqual(first.factor_weights, second.factor_weights)
        pd.testing.assert_frame_equal(first.total_scores, second.total_scores)
        pd.testing.assert_frame_equal(first.selected_portfolio, second.selected_portfolio)

    def test_engine_builds_portfolio_from_eligible_candidates_only(self) -> None:
        engine = StrategyEngine(
            detector=FakeDetector(),
            factor_engine=FakeFactorEngine(),
            weight_mapper=FakeWeightMapper(),
            portfolio_builder=PortfolioBuilder(
                params=StrategyPortfolioParameters(
                    top_n=2,
                    min_names=1,
                    weighting_method="score_proportional",
                    single_name_max_weight=0.70,
                    cash_buffer=0.0,
                )
            ),
        )

        decision = engine.run(self._build_snapshot())

        self.assertEqual(decision.eligible_count, 2)
        self.assertEqual(len(decision.selected_portfolio), 2)
        self.assertAlmostEqual(
            decision.selected_portfolio["target_weight"].sum(),
            1.0,
            places=8,
        )
        self.assertTrue(
            decision.selected_portfolio["cb_code"].isin(["110001.SH", "110002.SH"]).all()
        )
        self.assertEqual(decision.total_scores.iloc[0]["cb_code"], "110001.SH")

    def test_engine_marks_warning_when_trade_date_is_not_fully_ready(self) -> None:
        snapshot = self._build_snapshot()
        snapshot = StrategySnapshot(
            trade_date=snapshot.trade_date,
            history_window=snapshot.history_window,
            trading_calendar=snapshot.trading_calendar,
            macro_daily=snapshot.macro_daily,
            cb_daily=snapshot.cb_daily,
            cb_basic=snapshot.cb_basic,
            cb_call=snapshot.cb_call,
            refresh_requested=True,
        )
        engine = StrategyEngine(
            detector=NotReadyDetector(),
            factor_engine=FakeFactorEngine(),
            weight_mapper=FakeWeightMapper(),
            portfolio_builder=PortfolioBuilder(
                params=StrategyPortfolioParameters(
                    top_n=2,
                    min_names=1,
                    weighting_method="score_proportional",
                    single_name_max_weight=0.70,
                    cash_buffer=0.0,
                )
            ),
        )

        decision = engine.run(snapshot)

        self.assertEqual(decision.diagnostics.refresh_requested, True)
        self.assertEqual(decision.diagnostics.data_quality_status, "警告")
        self.assertTrue(
            any("fully-ready 首日" in note for note in decision.diagnostics.notes)
        )
        self.assertTrue(
            any("请勿直接据此做投资判断" in note for note in decision.diagnostics.notes)
        )

    def test_engine_carries_requested_codes_into_diagnostics(self) -> None:
        snapshot = self._build_snapshot()
        snapshot = StrategySnapshot(
            trade_date=snapshot.trade_date,
            history_window=snapshot.history_window,
            trading_calendar=snapshot.trading_calendar,
            macro_daily=snapshot.macro_daily,
            cb_daily=snapshot.cb_daily,
            cb_basic=snapshot.cb_basic,
            cb_call=snapshot.cb_call,
            runtime_snapshot_reused=True,
            requested_codes=("110001.SH", "128123.SZ"),
            cache_diagnostics=build_cache_diagnostics(
                {"panel_memory_hit_calls": 2},
                runtime_snapshot_reused=True,
            ),
        )
        engine = StrategyEngine(
            detector=FakeDetector(),
            factor_engine=FakeFactorEngine(),
            weight_mapper=FakeWeightMapper(),
            portfolio_builder=PortfolioBuilder(
                params=StrategyPortfolioParameters(
                    top_n=2,
                    min_names=1,
                    weighting_method="score_proportional",
                    single_name_max_weight=0.70,
                    cash_buffer=0.0,
                )
            ),
        )

        decision = engine.run(snapshot)

        self.assertTrue(decision.diagnostics.runtime_snapshot_reused)
        self.assertEqual(decision.diagnostics.requested_codes, ("110001.SH", "128123.SZ"))
        self.assertEqual(
            int(decision.diagnostics.cache_diagnostics["layers"]["request_panel_memory"]["hits"]),
            2,
        )

    def test_engine_limits_factor_inputs_to_codes_present_on_trade_date(self) -> None:
        trade_date = pd.Timestamp("2026-04-20")
        snapshot = StrategySnapshot(
            trade_date=trade_date,
            history_window=StrategyHistoryWindow(
                trade_date=trade_date,
                requested_start=pd.Timestamp("2025-10-01"),
                used_start=pd.Timestamp("2025-10-01"),
            ),
            trading_calendar=pd.DataFrame(
                [{"calendar_date": trade_date, "is_open": 1}]
            ),
            macro_daily=pd.DataFrame(),
            cb_daily=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": pd.Timestamp("2026-04-18"),
                        "close": 101.0,
                    },
                    {
                        "cb_code": "110002.SH",
                        "trade_date": pd.Timestamp("2026-04-20"),
                        "close": 102.0,
                    },
                    {
                        "cb_code": "110002.SH",
                        "trade_date": pd.Timestamp("2026-04-18"),
                        "close": 100.0,
                    },
                ]
            ),
            cb_basic=pd.DataFrame(
                [
                    {"cb_code": "110001.SH", "remain_size": 8.0},
                    {"cb_code": "110002.SH", "remain_size": 7.0},
                ]
            ),
            cb_call=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "call_type": "强赎",
                        "call_status": "公告强赎",
                        "announcement_date": pd.Timestamp("2026-04-01"),
                        "call_date": pd.NaT,
                    },
                    {
                        "cb_code": "110002.SH",
                        "call_type": "强赎",
                        "call_status": "公告强赎",
                        "announcement_date": pd.Timestamp("2026-04-10"),
                        "call_date": pd.NaT,
                    },
                ]
            ),
            cb_rate=pd.DataFrame(
                [
                    {"cb_code": "110001.SH", "coupon_rate": 0.5},
                    {"cb_code": "110002.SH", "coupon_rate": 0.6},
                ]
            ),
        )
        factor_engine = RecordingFactorEngine()
        engine = StrategyEngine(
            detector=FakeDetector(),
            factor_engine=factor_engine,
            weight_mapper=FakeWeightMapper(),
            portfolio_builder=PortfolioBuilder(
                params=StrategyPortfolioParameters(
                    top_n=2,
                    min_names=1,
                    weighting_method="score_proportional",
                    single_name_max_weight=0.70,
                    cash_buffer=0.0,
                )
            ),
        )

        engine.run(snapshot)

        self.assertEqual(
            set(factor_engine.last_cb_daily["cb_code"]),
            {"110002.SH"},
        )
        self.assertEqual(
            set(factor_engine.last_cb_basic["cb_code"]),
            {"110002.SH"},
        )
        self.assertEqual(
            set(factor_engine.last_cb_call["cb_code"]),
            {"110002.SH"},
        )
        self.assertEqual(
            set(factor_engine.last_cb_rate["cb_code"]),
            {"110002.SH"},
        )


if __name__ == "__main__":
    unittest.main()
