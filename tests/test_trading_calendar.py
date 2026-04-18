from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import pandas as pd

from data.cache_store import CacheStore
from data.trading_calendar import TradingCalendar

TMP_ROOT = Path(__file__).resolve().parent / "_tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)


def make_case_dir(case_name: str) -> Path:
    case_dir = TMP_ROOT / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


class FakeTushareClient:
    def __init__(self, tables: dict[str, pd.DataFrame], should_fail: bool = False) -> None:
        self.tables = tables
        self.should_fail = should_fail
        self.calls: list[tuple[str, dict[str, object] | None, str | None]] = []

    def query(
        self,
        api_name: str,
        params: dict[str, object] | None = None,
        fields: str | None = None,
    ) -> pd.DataFrame:
        self.calls.append((api_name, params, fields))
        if self.should_fail:
            raise RuntimeError("network down")
        frame = self.tables[api_name].copy()
        if api_name == "trade_cal" and params is not None:
            start = pd.Timestamp(params["start_date"])
            end = pd.Timestamp(params["end_date"])
            exchange = params["exchange"]
            frame = frame.loc[
                (frame["exchange"] == exchange)
                & (pd.to_datetime(frame["cal_date"]) >= start)
                & (pd.to_datetime(frame["cal_date"]) <= end)
            ]
        return frame.reset_index(drop=True)


class TradingCalendarTests(unittest.TestCase):
    def test_calendar_fetches_and_caches_from_tushare(self) -> None:
        table = pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                {"exchange": "SSE", "cal_date": "20260403", "is_open": 0, "pretrade_date": "20260402"},
            ]
        )
        client = FakeTushareClient({"trade_cal": table})

        case_dir = make_case_dir("calendar_fetch")
        cache_store = CacheStore(case_dir / "cache")
        calendar = TradingCalendar(client=client, cache_store=cache_store)

        open_days = calendar.get_open_days("2026-04-01", "2026-04-03")

        self.assertEqual(
            open_days,
            [
                pd.Timestamp("2026-04-01").date(),
                pd.Timestamp("2026-04-02").date(),
            ],
        )
        cached_path = case_dir / "cache" / "tushare" / "calendar" / "SSE.csv"
        self.assertTrue(cached_path.exists())
        self.assertEqual(len(client.calls), 1)

    def test_calendar_can_fall_back_to_local_reference(self) -> None:
        case_dir = make_case_dir("calendar_fallback")
        cache_store = CacheStore(case_dir / "cache")
        reference_dir = case_dir / "local_reference" / "trading_calendar"
        reference_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "exchange": "SSE",
                    "calendar_date": "2026-04-01",
                    "is_open": 1,
                    "previous_open_date": "2026-03-31",
                },
                {
                    "exchange": "SSE",
                    "calendar_date": "2026-04-02",
                    "is_open": 0,
                    "previous_open_date": "2026-03-31",
                },
            ]
        ).to_csv(reference_dir / "SSE.csv", index=False, encoding="utf-8-sig")

        calendar = TradingCalendar(
            client=FakeTushareClient({}, should_fail=True),
            cache_store=cache_store,
        )

        frame = calendar.get_calendar("2026-04-01", "2026-04-02")

        self.assertEqual(len(frame), 2)
        self.assertEqual(int(frame.iloc[0]["is_open"]), 1)


if __name__ == "__main__":
    unittest.main()
