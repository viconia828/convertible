from __future__ import annotations

import shutil
import unittest
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from data.credit_spread_reference import CreditSpreadReferenceUpdater

TMP_ROOT = Path(__file__).resolve().parent / "_tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)


def make_case_dir(case_name: str) -> Path:
    case_dir = TMP_ROOT / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


class CreditSpreadReferenceUpdaterTests(unittest.TestCase):
    @dataclass(frozen=True)
    class FakeSource:
        name: str
        frame: pd.DataFrame | None = None
        error_message: str | None = None

        def fetch(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
            if self.error_message is not None:
                raise RuntimeError(self.error_message)
            if self.frame is None:
                raise RuntimeError("no data configured")
            return self.frame.copy()

    def test_refresh_writes_reference_and_metadata(self) -> None:
        case_dir = make_case_dir("credit_spread_refresh")

        def fake_fetcher(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
            self.assertEqual(start_ts, pd.Timestamp("2026-04-01"))
            self.assertEqual(end_ts, pd.Timestamp("2026-04-03"))
            return pd.DataFrame(
                {
                    "trade_date": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
                    "value": [0.91, 0.92, 0.93],
                    "indicator_code": ["credit_spread"] * 3,
                    "source_table": ["chinabond_queryYz_10y_AA_minus_treasury"] * 3,
                }
            )

        updater = CreditSpreadReferenceUpdater(
            reference_path=case_dir / "credit_spread.csv",
            metadata_path=case_dir / "credit_spread.meta.json",
            fetcher=fake_fetcher,
        )
        frame = updater.refresh("2026-04-01", "2026-04-03")
        status = updater.status(as_of_date="2026-04-05")

        self.assertEqual(len(frame), 3)
        self.assertTrue((case_dir / "credit_spread.csv").exists())
        self.assertTrue((case_dir / "credit_spread.meta.json").exists())
        self.assertEqual(status.mode, "fresh_primary")
        self.assertEqual(status.rows, 3)
        self.assertEqual(status.coverage_end, pd.Timestamp("2026-04-03"))
        self.assertEqual(status.stale_days, 2)

    def test_refresh_uses_backup_source_when_primary_fails(self) -> None:
        case_dir = make_case_dir("credit_spread_backup_source")
        primary = self.FakeSource(name="primary_fail", error_message="primary down")
        backup = self.FakeSource(
            name="manual_backup",
            frame=pd.DataFrame(
                {
                    "trade_date": pd.to_datetime(["2026-04-01", "2026-04-02"]),
                    "value": [0.81, 0.82],
                    "indicator_code": ["credit_spread"] * 2,
                    "source_table": ["manual_backup_import"] * 2,
                }
            ),
        )

        updater = CreditSpreadReferenceUpdater(
            reference_path=case_dir / "credit_spread.csv",
            metadata_path=case_dir / "credit_spread.meta.json",
            primary_source=primary,
            backup_sources=[backup],
        )
        frame = updater.refresh("2026-04-01", "2026-04-02")
        status = updater.status(as_of_date="2026-04-03")

        self.assertEqual(len(frame), 2)
        self.assertEqual(status.mode, "fresh_backup")
        self.assertEqual(status.active_source, "manual_backup")
        self.assertEqual(status.source_table, "manual_backup_import")

    def test_refresh_falls_back_to_existing_snapshot(self) -> None:
        case_dir = make_case_dir("credit_spread_fallback")

        def seed_fetcher(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "trade_date": pd.to_datetime(["2026-04-01", "2026-04-02"]),
                    "value": [0.91, 0.92],
                    "indicator_code": ["credit_spread"] * 2,
                    "source_table": ["chinabond_queryYz_10y_AA_minus_treasury"] * 2,
                }
            )

        updater = CreditSpreadReferenceUpdater(
            reference_path=case_dir / "credit_spread.csv",
            metadata_path=case_dir / "credit_spread.meta.json",
            fetcher=seed_fetcher,
        )
        updater.refresh("2026-04-01", "2026-04-02")

        def failing_fetcher(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
            raise RuntimeError("network cut off")

        fallback_updater = CreditSpreadReferenceUpdater(
            reference_path=case_dir / "credit_spread.csv",
            metadata_path=case_dir / "credit_spread.meta.json",
            fetcher=failing_fetcher,
        )
        frame = fallback_updater.refresh(
            "2026-04-01",
            "2026-04-05",
            use_existing_on_failure=True,
        )
        status = fallback_updater.status(as_of_date="2026-04-05")

        self.assertEqual(len(frame), 2)
        self.assertEqual(status.mode, "fallback_local")
        self.assertEqual(status.coverage_end, pd.Timestamp("2026-04-02"))
        self.assertEqual(status.stale_days, 3)
        self.assertEqual(status.active_source, "local_snapshot")


if __name__ == "__main__":
    unittest.main()
