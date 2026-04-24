from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import pandas as pd

from config.strategy_config import load_strategy_parameters
from env.macro_alignment import MacroAlignmentSummary
from exports.strategy_exports import (
    STRATEGY_CACHE_DIAGNOSTICS_SHEET_NAME,
    STRATEGY_CANDIDATE_SHEET_NAME,
    STRATEGY_ENVIRONMENT_SHEET_NAME,
    STRATEGY_FACTOR_DIAGNOSTICS_SHEET_NAME,
    STRATEGY_FACTOR_WEIGHTS_SHEET_NAME,
    STRATEGY_PORTFOLIO_SHEET_NAME,
    STRATEGY_WATCHLIST_SHEET_NAME,
    build_strategy_observation_report,
    write_strategy_observation_xlsx,
)
from shared.cache_diagnostics import build_cache_diagnostics
from strategy.result import StrategyDecision, StrategyDiagnostics


TMP_ROOT = Path(__file__).resolve().parent / "_tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)


def make_case_dir(case_name: str) -> Path:
    case_dir = TMP_ROOT / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


class FakeStrategyService:
    def __init__(self, decision: StrategyDecision) -> None:
        self.decision = decision
        self.calls: list[tuple[object, tuple[str, ...], bool]] = []

    def run_for_date(self, trade_date, requested_codes=(), refresh=False):
        normalized_codes = tuple(requested_codes or ())
        self.calls.append((trade_date, normalized_codes, bool(refresh)))
        return self.decision


class StrategyExportsTests(unittest.TestCase):
    def _build_decision(
        self,
        requested_codes: tuple[str, ...] = (),
    ) -> StrategyDecision:
        portfolio = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.9123,
                    "target_weight": 0.55,
                    "rank": 1,
                },
                {
                    "cb_code": "110002.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.8456,
                    "target_weight": 0.45,
                    "rank": 2,
                },
            ]
        )
        diagnostics = StrategyDiagnostics(
            history_start_requested=pd.Timestamp("2025-10-01"),
            history_start_used=pd.Timestamp("2025-10-01"),
            refresh_requested=False,
            runtime_snapshot_reused=False,
            requested_codes=requested_codes,
            data_quality_status="正常",
            data_quality_hints=("基于本地缓存构造 snapshot。",),
            notes=("strategy 结果仅代表当前 trade_date 的静态决策。",),
            alignment_summary=MacroAlignmentSummary(
                total_calendar_days=150,
                kept_days=150,
                dropped_days=0,
                filled_days_by_indicator={},
                invalid_days_by_indicator={},
            ),
            first_fully_ready_trade_date=pd.Timestamp("2025-10-01"),
            cache_diagnostics=build_cache_diagnostics(
                {
                    "cache_resolution_hit_calls": 3,
                    "cache_resolution_partial_hit_calls": 1,
                    "remote_fill_calls": 1,
                    "cache_file_scan_calls": 5,
                    "cache_writeback_calls": 1,
                    "panel_memory_hit_calls": 2,
                    "panel_memory_save_calls": 1,
                    "aggregate_memory_hit_calls": 1,
                    "stage_calls::cb_daily_cross_section::request_panel_lookup": 1,
                    "stage_elapsed_ms::cb_daily_cross_section::request_panel_lookup": 14,
                },
                runtime_snapshot_reused=False,
            ),
        )
        factor_diagnostics = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "value_raw": 1.2,
                    "carry_raw": 0.4,
                    "structure_raw": 0.8,
                    "trend_raw": 0.7,
                    "stability_raw": 0.3,
                    "value_score": 0.9,
                    "carry_score": 0.4,
                    "structure_score": 0.8,
                    "trend_score": 0.7,
                    "stability_score": 0.3,
                    "total_score": 0.9123,
                    "eligible": True,
                    "exclude_reason": "",
                },
                {
                    "cb_code": "110003.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "value_raw": pd.NA,
                    "carry_raw": pd.NA,
                    "structure_raw": pd.NA,
                    "trend_raw": pd.NA,
                    "stability_raw": pd.NA,
                    "value_score": pd.NA,
                    "carry_score": pd.NA,
                    "structure_score": pd.NA,
                    "trend_score": pd.NA,
                    "stability_score": pd.NA,
                    "total_score": pd.NA,
                    "eligible": False,
                    "exclude_reason": "not_tradable",
                },
            ]
        )
        total_scores = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.9123,
                    "eligible": True,
                    "exclude_reason": "",
                    "value_score": 0.9,
                },
                {
                    "cb_code": "110002.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.8456,
                    "eligible": True,
                    "exclude_reason": "",
                    "value_score": 0.7,
                },
                {
                    "cb_code": "110003.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": pd.NA,
                    "eligible": False,
                    "exclude_reason": "not_tradable",
                    "value_score": pd.NA,
                },
            ]
        )
        return StrategyDecision(
            trade_date=pd.Timestamp("2026-04-20"),
            environment={
                "equity_strength": 0.71,
                "bond_strength": 0.43,
                "trend_strength": 0.82,
            },
            factor_weights={
                "value": 0.28,
                "carry": 0.20,
                "structure": 0.18,
                "trend": 0.24,
                "stability": 0.10,
            },
            factor_diagnostics=factor_diagnostics,
            total_scores=total_scores,
            selected_portfolio=portfolio,
            eligible_count=27,
            cash_weight=0.0,
            diagnostics=diagnostics,
        )

    def test_build_strategy_observation_report_collects_decision_outputs(self) -> None:
        config = load_strategy_parameters()
        service = FakeStrategyService(
            self._build_decision(
                requested_codes=("110001.SH", "110003.SH", "128123.SZ")
            )
        )

        report = build_strategy_observation_report(
            trade_date="2026-04-20",
            requested_codes="110001 110003 128123",
            service=service,
            config=config,
        )

        self.assertEqual(
            service.calls,
            [("2026-04-20", ("110001.SH", "110003.SH", "128123.SZ"), False)],
        )
        self.assertEqual(report.trade_date, pd.Timestamp("2026-04-20"))
        self.assertEqual(
            report.requested_codes,
            ["110001.SH", "110003.SH", "128123.SZ"],
        )
        self.assertEqual(list(report.environment.columns)[0], "trade_date")
        self.assertIn("value", report.factor_weights.columns)
        self.assertEqual(len(report.portfolio), 2)
        self.assertEqual(
            list(report.watchlist.columns),
            ["代码", "观察状态", "可建仓", "候选排序", "组合排序", "目标权重", "总分", "剔除原因"],
        )
        self.assertIn("已入选目标组合", set(report.watchlist["观察状态"]))
        self.assertIn("不可交易/已剔除", set(report.watchlist["观察状态"]))
        self.assertIn("当日无横截面记录", set(report.watchlist["观察状态"]))
        self.assertEqual(list(report.cache_diagnostics.columns), ["item", "value", "comment"])

    def test_write_strategy_observation_xlsx_writes_expected_sheets(self) -> None:
        config = load_strategy_parameters()
        service = FakeStrategyService(
            self._build_decision(requested_codes=("110001.SH", "110003.SH"))
        )
        report = build_strategy_observation_report(
            trade_date="2026-04-20",
            requested_codes="110001 110003",
            service=service,
            config=config,
        )

        case_dir = make_case_dir("strategy_observation_xlsx")
        output_path = case_dir / "strategy_observation.xlsx"
        written = write_strategy_observation_xlsx(
            report=report,
            output_path=output_path,
            config=config,
        )

        self.assertTrue(written.exists())
        with pd.ExcelFile(written) as workbook:
            sheets = workbook.sheet_names
        self.assertEqual(
            sheets,
            [
                STRATEGY_ENVIRONMENT_SHEET_NAME,
                STRATEGY_FACTOR_WEIGHTS_SHEET_NAME,
                STRATEGY_PORTFOLIO_SHEET_NAME,
                STRATEGY_WATCHLIST_SHEET_NAME,
                STRATEGY_CANDIDATE_SHEET_NAME,
                STRATEGY_FACTOR_DIAGNOSTICS_SHEET_NAME,
                STRATEGY_CACHE_DIAGNOSTICS_SHEET_NAME,
                config.exports.summary_sheet_name,
            ],
        )
        watchlist = pd.read_excel(written, sheet_name=STRATEGY_WATCHLIST_SHEET_NAME)
        self.assertEqual(set(watchlist["代码"]), {"110001.SH", "110003.SH"})
        summary = pd.read_excel(written, sheet_name=config.exports.summary_sheet_name)
        self.assertEqual(list(summary.columns), ["item", "value", "comment"])
        self.assertIn("portfolio_sheet_role", set(summary["item"]))
        self.assertIn("cache_diagnostics_sheet_columns", set(summary["item"]))


if __name__ == "__main__":
    unittest.main()
