from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from factor import FactorEngine
from scoring_exports import (
    FACTOR_DIAGNOSTIC_EXPORT_COLUMNS,
    FACTOR_SCORE_DISPLAY_COLUMNS,
    _build_factor_code_sheet_name_map,
    _recommended_factor_history_buffer_calendar_days,
    build_environment_score_report,
    build_factor_score_report,
    normalize_cb_codes,
    write_environment_score_xlsx,
    write_factor_score_xlsx,
)
from strategy_config import load_strategy_parameters


TMP_ROOT = Path(__file__).resolve().parent / "_tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)


def make_case_dir(case_name: str) -> Path:
    case_dir = TMP_ROOT / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


class StubLoader:
    def __init__(
        self,
        trading_calendar: pd.DataFrame,
        macro_daily: pd.DataFrame,
        cb_daily: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame,
    ) -> None:
        self._trading_calendar = trading_calendar
        self._macro_daily = macro_daily
        self._cb_daily = cb_daily
        self._cb_basic = cb_basic
        self._cb_call = cb_call
        self.requested_cb_daily_cross_section_columns: list[str] | None = None

    def get_trading_calendar(
        self,
        start_date: object,
        end_date: object,
        exchange: str | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        frame = self._trading_calendar.copy()
        return frame.loc[
            frame["calendar_date"].between(start_ts, end_ts)
        ].reset_index(drop=True)

    def get_macro_daily(
        self,
        indicators: list[str],
        start_date: object,
        end_date: object,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        frame = self._macro_daily.copy()
        return frame.loc[
            frame["indicator_code"].isin(indicators)
            & frame["trade_date"].between(start_ts, end_ts)
        ].reset_index(drop=True)

    def get_cb_daily(
        self,
        codes: list[str],
        start_date: object,
        end_date: object,
        refresh: bool = False,
        enrich: bool = False,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        frame = self._cb_daily.copy()
        return frame.loc[
            frame["cb_code"].isin(codes) & frame["trade_date"].between(start_ts, end_ts)
        ].reset_index(drop=True)

    def get_cb_daily_cross_section(
        self,
        start_date: object,
        end_date: object,
        refresh: bool = False,
        columns: list[str] | tuple[str, ...] | None = None,
        aggregate_profile: str | None = None,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        self.requested_cb_daily_cross_section_columns = (
            list(columns) if columns is not None else None
        )
        frame = self._cb_daily.copy()
        return frame.loc[
            frame["trade_date"].between(start_ts, end_ts)
        ].reset_index(drop=True)

    def get_cb_rate(
        self,
        codes: list[str],
        refresh: bool = False,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "cb_code",
                "rate_frequency",
                "rate_start_date",
                "rate_end_date",
                "coupon_rate",
            ]
        )

    def get_cb_basic(self) -> pd.DataFrame:
        return self._cb_basic.copy()

    def get_cb_call(
        self,
        start_date: object,
        end_date: object,
        codes: list[str] | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        frame = self._cb_call.copy()
        if codes is not None:
            frame = frame.loc[frame["cb_code"].isin(codes)].copy()
        return frame.loc[
            frame["announcement_date"].between(start_ts, end_ts)
        ].reset_index(drop=True)


class RecordingRateLoader(StubLoader):
    def __init__(
        self,
        trading_calendar: pd.DataFrame,
        macro_daily: pd.DataFrame,
        cb_daily: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame,
        cb_rate: pd.DataFrame,
    ) -> None:
        super().__init__(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        self._cb_rate = cb_rate
        self.get_cb_rate_calls = 0
        self.requested_cb_rate_codes: list[str] = []
        self.persisted_ytm_updates = pd.DataFrame()

    def get_cb_rate(
        self,
        codes: list[str],
        refresh: bool = False,
    ) -> pd.DataFrame:
        self.get_cb_rate_calls += 1
        self.requested_cb_rate_codes = list(codes)
        return self._cb_rate.loc[self._cb_rate["cb_code"].isin(codes)].reset_index(drop=True)

    def persist_cb_daily_cross_section_derived_fields(
        self,
        updates: pd.DataFrame,
        columns: tuple[str, ...] = ("ytm",),
        base_frame: pd.DataFrame | None = None,
    ) -> None:
        self.persisted_ytm_updates = updates.copy()


class ScoringExportsTests(unittest.TestCase):
    def _make_env_inputs(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        dates = pd.date_range("2024-01-01", periods=320, freq="B")
        trading_calendar = pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": 1,
            }
        )
        macro_daily = pd.DataFrame(
            {
                "trade_date": list(dates) * 6,
                "indicator_code": (
                    ["csi300"] * len(dates)
                    + ["csi300_amount"] * len(dates)
                    + ["bond_index"] * len(dates)
                    + ["treasury_10y"] * len(dates)
                    + ["credit_spread"] * len(dates)
                    + ["cb_equal_weight"] * len(dates)
                ),
                "value": np.concatenate(
                    [
                        np.linspace(3500, 4500, len(dates)),
                        np.linspace(8e11, 1.2e12, len(dates)),
                        np.linspace(200, 240, len(dates)),
                        np.linspace(2.8, 1.8, len(dates)),
                        np.linspace(1.8, 1.2, len(dates)),
                        np.linspace(100, 140, len(dates)),
                    ]
                ),
                "source_table": ["test"] * len(dates) * 6,
            }
        )
        return trading_calendar, macro_daily

    def _make_factor_inputs(
        self,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        dates = pd.date_range("2026-01-01", periods=80, freq="B")

        frames: list[pd.DataFrame] = []
        for idx, code in enumerate(["CB_A", "CB_B"]):
            base_close = 100 + idx * 5
            close_series = np.linspace(base_close, base_close + 10, len(dates))
            premium = np.full(len(dates), 10.0 + idx * 8.0)
            ytm = np.full(len(dates), 0.05 - idx * 0.01)
            amount = np.full(len(dates), 500.0)
            returns = np.diff(close_series, prepend=close_series[0]) / close_series
            frame = pd.DataFrame(
                {
                    "cb_code": code,
                    "trade_date": dates,
                    "pre_close": np.r_[close_series[0], close_series[:-1]],
                    "open": close_series,
                    "high": close_series,
                    "low": close_series,
                    "close": close_series,
                    "price_change": close_series - np.r_[close_series[0], close_series[:-1]],
                    "pct_change": returns * 100,
                    "volume": np.full(len(dates), 10_000.0),
                    "amount": amount,
                    "bond_value": close_series - 2.0,
                    "bond_premium_rate": np.full(len(dates), 2.0),
                    "convert_value": close_series - premium / 2.0,
                    "premium_rate": premium,
                    "ytm": ytm,
                    "is_tradable": True,
                }
            )
            frames.append(frame)

        cb_daily = pd.concat(frames, ignore_index=True)
        cb_basic = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_B"],
                "remain_size": [8e8, 8e8],
                "list_date": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01")],
                "delist_date": [pd.NaT, pd.NaT],
                "conv_stop_date": [pd.NaT, pd.NaT],
            }
        )
        cb_call = pd.DataFrame(
            {
                "cb_code": ["CB_X"],
                "call_type": ["寮鸿祹"],
                "call_status": ["鍏憡寮鸿祹"],
                "announcement_date": [pd.Timestamp("2026-03-01")],
                "call_date": [pd.Timestamp("2026-03-15")],
            }
        )
        trading_calendar = pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": 1,
            }
        )
        return cb_daily, cb_basic, cb_call, trading_calendar

    def test_normalize_cb_codes_supports_multiple_delimiters(self) -> None:
        result = normalize_cb_codes("110073.SH, 128044.SZ；110073.SH 113001.SH")
        self.assertEqual(result, ["110073.SH", "128044.SZ", "113001.SH"])

    def test_normalize_cb_codes_auto_appends_exchange_suffix_for_six_digit_input(self) -> None:
        result = normalize_cb_codes("110073, 128044；113001 110073")
        self.assertEqual(result, ["110073.SH", "128044.SZ", "113001.SH"])

    def test_build_environment_report_and_write_xlsx(self) -> None:
        trading_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, _ = self._make_factor_inputs()
        loader = StubLoader(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_environment_score_report(
            "2025-03-01",
            "2025-03-31",
            loader=loader,
            config=config,
        )

        self.assertGreater(len(report.scores), 0)
        self.assertIn("equity_strength", report.scores.columns)

        case_dir = make_case_dir("env_report_xlsx")
        output_path = case_dir / "env.xlsx"
        written = write_environment_score_xlsx(report, output_path=output_path, config=config)

        self.assertTrue(written.exists())
        with pd.ExcelFile(written) as workbook:
            sheets = workbook.sheet_names
        self.assertEqual(
            sheets,
            [config.exports.env_sheet_name, config.exports.summary_sheet_name],
        )
        summary = pd.read_excel(written, sheet_name=config.exports.summary_sheet_name)
        self.assertEqual(list(summary.columns), ["item", "value", "comment"])
        report_type_row = summary.loc[summary["item"] == "report_type"].iloc[0]
        self.assertIn("环境打分导出结果", str(report_type_row["comment"]))
        self.assertIn("warmup_first_ready_date", set(summary["item"]))
        self.assertIn("trend_first_ready_date", set(summary["item"]))
        self.assertIn("warmup_trade_days_excluded", set(summary["item"]))
        fetch_policy_row = summary.loc[summary["item"] == "fetch_policy"].iloc[0]
        self.assertIn("缓存优先", str(fetch_policy_row["value"]))
        filled_row = summary.loc[summary["item"] == "filled_days::credit_spread"].iloc[0]
        self.assertIn("信用利差", str(filled_row["comment"]))

    def test_environment_report_blanks_trend_until_trend_ready_date(self) -> None:
        dates = pd.date_range("2024-01-01", periods=160, freq="B")
        trading_calendar = pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": 1,
            }
        )
        macro_daily = pd.DataFrame(
            {
                "trade_date": list(dates) * 6,
                "indicator_code": (
                    ["csi300"] * len(dates)
                    + ["csi300_amount"] * len(dates)
                    + ["bond_index"] * len(dates)
                    + ["treasury_10y"] * len(dates)
                    + ["credit_spread"] * len(dates)
                    + ["cb_equal_weight"] * len(dates)
                ),
                "value": np.concatenate(
                    [
                        np.linspace(3500, 4500, len(dates)),
                        np.linspace(8e11, 1.2e12, len(dates)),
                        np.linspace(200, 240, len(dates)),
                        np.linspace(2.8, 1.8, len(dates)),
                        np.linspace(1.8, 1.2, len(dates)),
                        np.linspace(100, 140, len(dates)),
                    ]
                ),
                "source_table": ["test"] * len(dates) * 6,
            }
        )
        cb_daily, cb_basic, cb_call, _ = self._make_factor_inputs()
        loader = StubLoader(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_environment_score_report(
            dates[20],
            dates[80],
            loader=loader,
            config=config,
        )

        self.assertEqual(report.scores["trade_date"].min(), dates[20])
        self.assertIsNotNone(report.trend_first_ready_date)
        self.assertGreater(report.trend_first_ready_date, dates[20])
        pre_ready = report.scores.loc[
            report.scores["trade_date"] < report.trend_first_ready_date,
            "trend_strength",
        ]
        self.assertGreater(len(pre_ready), 0)
        self.assertTrue(pre_ready.isna().all())
        post_ready = report.scores.loc[
            report.scores["trade_date"] >= report.trend_first_ready_date,
            "trend_strength",
        ]
        self.assertTrue(post_ready.notna().any())

    def test_environment_report_keeps_requested_start_when_pre_request_warmup_is_sufficient(
        self,
    ) -> None:
        dates = pd.date_range("2025-01-01", periods=120, freq="B")
        trading_calendar = pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": 1,
            }
        )
        macro_daily = pd.DataFrame(
            {
                "trade_date": list(dates) * 6,
                "indicator_code": (
                    ["csi300"] * len(dates)
                    + ["csi300_amount"] * len(dates)
                    + ["bond_index"] * len(dates)
                    + ["treasury_10y"] * len(dates)
                    + ["credit_spread"] * len(dates)
                    + ["cb_equal_weight"] * len(dates)
                ),
                "value": np.concatenate(
                    [
                        np.linspace(3500, 4500, len(dates)),
                        np.linspace(8e11, 1.2e12, len(dates)),
                        np.linspace(200, 240, len(dates)),
                        np.linspace(2.8, 1.8, len(dates)),
                        np.linspace(1.8, 1.2, len(dates)),
                        np.linspace(100, 140, len(dates)),
                    ]
                ),
                "source_table": ["test"] * len(dates) * 6,
            }
        )
        cb_daily, cb_basic, cb_call, _ = self._make_factor_inputs()
        loader = StubLoader(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_environment_score_report(
            dates[60],
            dates[100],
            loader=loader,
            config=config,
        )

        self.assertEqual(report.warmup_first_ready_date, dates[60])
        self.assertEqual(report.warmup_trade_days_excluded, 0)
        self.assertEqual(report.scores["trade_date"].min(), dates[60])
        self.assertFalse(any("预热区间截至" in note for note in report.notes))

    def test_environment_report_auto_skips_warmup_days_when_pre_request_history_is_short(
        self,
    ) -> None:
        dates = pd.date_range("2025-01-01", periods=120, freq="B")
        trading_calendar = pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": 1,
            }
        )
        macro_daily = pd.DataFrame(
            {
                "trade_date": list(dates) * 6,
                "indicator_code": (
                    ["csi300"] * len(dates)
                    + ["csi300_amount"] * len(dates)
                    + ["bond_index"] * len(dates)
                    + ["treasury_10y"] * len(dates)
                    + ["credit_spread"] * len(dates)
                    + ["cb_equal_weight"] * len(dates)
                ),
                "value": np.concatenate(
                    [
                        np.linspace(3500, 4500, len(dates)),
                        np.linspace(8e11, 1.2e12, len(dates)),
                        np.linspace(200, 240, len(dates)),
                        np.linspace(2.8, 1.8, len(dates)),
                        np.linspace(1.8, 1.2, len(dates)),
                        np.linspace(100, 140, len(dates)),
                    ]
                ),
                "source_table": ["test"] * len(dates) * 6,
            }
        )
        cb_daily, cb_basic, cb_call, _ = self._make_factor_inputs()
        loader = StubLoader(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_environment_score_report(
            dates[10],
            dates[60],
            loader=loader,
            config=config,
        )

        self.assertEqual(report.warmup_first_ready_date, dates[20])
        self.assertEqual(report.warmup_trade_days_excluded, 10)
        self.assertEqual(report.scores["trade_date"].min(), dates[20])
        self.assertTrue(any("预热区间" in note for note in report.notes))
        self.assertFalse(any("首个可导出交易日" in note for note in report.notes))

    def test_environment_report_raises_when_requested_window_is_entirely_in_warmup(self) -> None:
        trading_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, _ = self._make_factor_inputs()
        cutoff = pd.Timestamp("2025-03-10")
        macro_daily = macro_daily.loc[
            ~(
                (macro_daily["indicator_code"] == "cb_equal_weight")
                & (macro_daily["trade_date"] < cutoff)
            )
        ].reset_index(drop=True)
        loader = StubLoader(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        with self.assertRaises(ValueError):
            build_environment_score_report(
                "2025-03-01",
                "2025-03-31",
                loader=loader,
                config=config,
            )

    def test_environment_report_does_not_flag_non_trading_requested_start_as_gap(self) -> None:
        trading_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, _ = self._make_factor_inputs()
        loader = StubLoader(trading_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_environment_score_report(
            "2025-03-01",
            "2025-03-31",
            loader=loader,
            config=config,
        )

        self.assertEqual(report.scores["trade_date"].min(), pd.Timestamp("2025-03-03"))
        self.assertFalse(any("首个可导出交易日" in note for note in report.notes))

    def test_build_factor_report_and_write_xlsx(self) -> None:
        env_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        loader = StubLoader(factor_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A", "CB_B"],
            loader=loader,
            config=config,
        )

        self.assertGreater(len(report.scores), 0)
        self.assertIn("baseline_total_score", report.scores.columns)
        self.assertEqual(report.codes, ["CB_A", "CB_B"])
        self.assertEqual(
            list(report.scores.columns),
            [column for column in FACTOR_SCORE_DISPLAY_COLUMNS if column in report.scores.columns],
        )
        self.assertEqual(
            list(report.diagnostics.columns),
            [
                column
                for column in FACTOR_DIAGNOSTIC_EXPORT_COLUMNS
                if column in report.diagnostics.columns
            ],
        )
        self.assertNotIn("value_raw", report.scores.columns)
        self.assertIn("value_raw", report.diagnostics.columns)

        case_dir = make_case_dir("factor_report_xlsx")
        output_path = case_dir / "factor.xlsx"
        written = write_factor_score_xlsx(report, output_path=output_path, config=config)

        self.assertTrue(written.exists())
        with pd.ExcelFile(written) as workbook:
            sheets = workbook.sheet_names
        self.assertEqual(
            sheets,
            [
                "CB_A",
                "CB_B",
                config.exports.diagnostics_sheet_name,
                config.exports.summary_sheet_name,
            ],
        )
        cb_a_scores = pd.read_excel(written, sheet_name="CB_A")
        self.assertEqual(set(cb_a_scores["cb_code"]), {"CB_A"})
        summary = pd.read_excel(written, sheet_name=config.exports.summary_sheet_name)
        self.assertEqual(list(summary.columns), ["item", "value", "comment"])
        summary_items = set(summary["item"])
        self.assertIn("score_sheet_role", summary_items)
        self.assertIn("diagnostics_sheet_role", summary_items)
        self.assertIn("score_sheet_columns", summary_items)
        self.assertIn("diagnostics_sheet_columns", summary_items)
        report_type_row = summary.loc[summary["item"] == "report_type"].iloc[0]
        self.assertIn("因子打分导出结果", str(report_type_row["comment"]))
        status_row = summary.loc[summary["item"] == "data_quality_status"].iloc[0]
        self.assertIn(str(status_row["value"]), {"正常", "警告"})

    def test_factor_sheet_name_map_avoids_reserved_names_and_duplicates(self) -> None:
        mapping = _build_factor_code_sheet_name_map(
            codes=["run_summary", "filter_diagnostics", "AA/BB", "AA:BB"],
            reserved_names=("run_summary", "filter_diagnostics"),
        )

        self.assertNotEqual(mapping["run_summary"], "run_summary")
        self.assertNotEqual(mapping["filter_diagnostics"], "filter_diagnostics")
        self.assertEqual(mapping["AA/BB"], "AA_BB")
        self.assertNotEqual(mapping["AA/BB"], mapping["AA:BB"])

    def test_factor_report_enforces_single_run_code_limit(self) -> None:
        trading_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        loader = StubLoader(factor_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters(
            overrides={"exports": {"factor_max_codes_per_run": 1}}
        )

        with self.assertRaises(ValueError):
            build_factor_score_report(
                "2026-04-13",
                "2026-04-22",
                codes=["CB_A", "CB_B"],
                loader=loader,
                config=config,
            )

    def test_factor_history_buffer_uses_dynamic_recommended_window(self) -> None:
        config = load_strategy_parameters()
        recommended_days = _recommended_factor_history_buffer_calendar_days(config)

        self.assertLess(recommended_days, config.exports.factor_history_buffer_calendar_days)
        self.assertGreaterEqual(recommended_days, 60)

    def test_factor_report_notes_when_no_eligible_scores_exist(self) -> None:
        trading_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        loader = StubLoader(factor_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        report = build_factor_score_report(
            "2026-01-05",
            "2026-01-12",
            codes=["CB_A"],
            loader=loader,
            config=config,
        )

        self.assertTrue(any("未产生可用因子分数" in note for note in report.notes))
        self.assertTrue(any("核心因子字段不足" in note for note in report.notes))
        self.assertTrue(any("上市观察期不足" in note for note in report.notes))
        self.assertTrue(any("请勿直接据此做投资判断" in note for note in report.notes))

    def test_single_code_factor_report_uses_full_cross_section_scoring(self) -> None:
        trading_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        loader = StubLoader(factor_calendar, macro_daily, cb_daily, cb_basic, cb_call)
        config = load_strategy_parameters()

        full_report = build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A", "CB_B"],
            loader=loader,
            config=config,
        )
        single_report = build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A"],
            loader=loader,
            config=config,
        )

        full_cb_a = (
            full_report.scores.loc[full_report.scores["cb_code"] == "CB_A"]
            .sort_values("trade_date", kind="stable")
            .reset_index(drop=True)
        )
        single_cb_a = single_report.scores.sort_values("trade_date", kind="stable").reset_index(drop=True)

        self.assertEqual(len(full_cb_a), len(single_cb_a))
        pd.testing.assert_frame_equal(
            full_cb_a.loc[
                :,
                [
                    "trade_date",
                    "cb_code",
                    "baseline_total_score",
                    "value_score",
                    "carry_score",
                    "structure_score",
                    "trend_score",
                    "stability_score",
                ],
            ].reset_index(drop=True),
            single_cb_a.loc[
                :,
                [
                    "trade_date",
                    "cb_code",
                    "baseline_total_score",
                    "value_score",
                    "carry_score",
                    "structure_score",
                    "trend_score",
                    "stability_score",
                ],
            ].reset_index(drop=True),
        )

    def test_factor_report_skips_cb_rate_when_requested_window_ytm_is_complete(self) -> None:
        env_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        cb_daily = cb_daily.copy()
        cb_daily.loc[cb_daily["trade_date"] < pd.Timestamp("2026-04-13"), "ytm"] = np.nan
        loader = RecordingRateLoader(
            factor_calendar,
            macro_daily,
            cb_daily,
            cb_basic,
            cb_call,
            cb_rate=pd.DataFrame(
                columns=[
                    "cb_code",
                    "rate_frequency",
                    "rate_start_date",
                    "rate_end_date",
                    "coupon_rate",
                ]
            ),
        )
        config = load_strategy_parameters()

        build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A", "CB_B"],
            loader=loader,
            config=config,
        )

        self.assertEqual(loader.get_cb_rate_calls, 0)

    def test_factor_report_persists_estimated_ytm_updates(self) -> None:
        env_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        cb_daily = cb_daily.copy()
        cb_daily["ytm"] = np.nan
        cb_rate = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_A", "CB_B", "CB_B"],
                "rate_frequency": [1, 1, 1, 1],
                "rate_start_date": pd.to_datetime(
                    ["2025-01-02", "2026-01-02", "2025-01-02", "2026-01-02"]
                ),
                "rate_end_date": pd.to_datetime(
                    ["2026-01-01", "2027-01-01", "2026-01-01", "2027-01-01"]
                ),
                "coupon_rate": [5.0, 5.0, 4.0, 4.0],
            }
        )
        cb_basic = cb_basic.copy()
        cb_basic["par_value"] = 100.0
        cb_basic["maturity_date"] = pd.Timestamp("2027-01-01")
        cb_basic["coupon_rate"] = [5.0, 4.0]
        cb_basic["pay_per_year"] = 1
        loader = RecordingRateLoader(
            factor_calendar,
            macro_daily,
            cb_daily,
            cb_basic,
            cb_call,
            cb_rate=cb_rate,
        )
        config = load_strategy_parameters()

        build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A", "CB_B"],
            loader=loader,
            config=config,
        )

        self.assertFalse(loader.persisted_ytm_updates.empty)
        self.assertIn("ytm", loader.persisted_ytm_updates.columns)

    def test_factor_report_only_loads_cb_rate_for_codes_missing_ytm_in_requested_window(self) -> None:
        env_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        cb_daily = cb_daily.copy()
        requested_start = pd.Timestamp("2026-04-13")
        requested_end = pd.Timestamp("2026-04-22")
        requested_mask = cb_daily["trade_date"].between(requested_start, requested_end)

        cb_daily.loc[(cb_daily["cb_code"] == "CB_A") & ~requested_mask, "ytm"] = np.nan
        cb_daily.loc[(cb_daily["cb_code"] == "CB_B") & requested_mask, "ytm"] = np.nan

        cb_rate = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_B"],
                "rate_frequency": [1, 1],
                "rate_start_date": pd.to_datetime(["2026-01-01", "2026-01-01"]),
                "rate_end_date": pd.to_datetime(["2027-01-01", "2027-01-01"]),
                "coupon_rate": [5.0, 4.0],
            }
        )
        cb_basic = cb_basic.copy()
        cb_basic["par_value"] = 100.0
        cb_basic["maturity_date"] = pd.Timestamp("2027-01-01")
        cb_basic["coupon_rate"] = [5.0, 4.0]
        cb_basic["pay_per_year"] = 1
        loader = RecordingRateLoader(
            factor_calendar,
            macro_daily,
            cb_daily,
            cb_basic,
            cb_call,
            cb_rate=cb_rate,
        )
        config = load_strategy_parameters()

        build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A", "CB_B"],
            loader=loader,
            config=config,
        )

        self.assertEqual(loader.get_cb_rate_calls, 1)
        self.assertEqual(loader.requested_cb_rate_codes, ["CB_B"])

    def test_factor_report_requests_minimal_cb_daily_cross_section_columns(self) -> None:
        env_calendar, macro_daily = self._make_env_inputs()
        cb_daily, cb_basic, cb_call, factor_calendar = self._make_factor_inputs()
        loader = RecordingRateLoader(
            factor_calendar,
            macro_daily,
            cb_daily,
            cb_basic,
            cb_call,
            cb_rate=pd.DataFrame(
                columns=[
                    "cb_code",
                    "rate_frequency",
                    "rate_start_date",
                    "rate_end_date",
                    "coupon_rate",
                ]
            ),
        )
        config = load_strategy_parameters()

        build_factor_score_report(
            "2026-04-13",
            "2026-04-22",
            codes=["CB_A", "CB_B"],
            loader=loader,
            config=config,
        )

        self.assertEqual(
            loader.requested_cb_daily_cross_section_columns,
            list(FactorEngine.HISTORY_COLUMNS),
        )


if __name__ == "__main__":
    unittest.main()
