"""Refresh and monitor the local credit-spread reference table."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandas as pd

from strategy_config import DataParameters, load_strategy_parameters

from .exceptions import DataSourceUnavailable
from .schema import DataSchema
from .utils import merge_frames, normalize_date


CHINABOND_QUERY_YZ_URL = "https://valuation.chinabond.com.cn/cbweb-mn/yc/queryYz"
TREASURY_CURVE_ID = "2c9081e50a2f9606010a3068cae70001"
CORP_AA_CURVE_ID = "2c90818812b319130112c279222836c3"


@dataclass(frozen=True)
class CreditSpreadReferenceStatus:
    """Coverage and freshness diagnostics for the local reference table."""

    mode: str
    rows: int
    coverage_start: pd.Timestamp | None
    coverage_end: pd.Timestamp | None
    stale_days: int | None
    source_table: str | None
    active_source: str | None


class CreditSpreadReferenceSource(Protocol):
    """Pluggable source interface for future backup providers."""

    name: str

    def fetch(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        """Return a `macro_daily`-compatible DataFrame."""


@dataclass(frozen=True)
class CallableCreditSpreadSource:
    """Adapter that wraps a plain callable into a named source."""

    name: str
    fetcher: Callable[[pd.Timestamp, pd.Timestamp], pd.DataFrame]

    def fetch(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        return self.fetcher(start_ts, end_ts)


class ChinabondQueryYzSource:
    """Primary source backed by Chinabond `queryYz`."""

    name = "chinabond_queryYz"

    def __init__(
        self,
        timeout: int | None = None,
        data_params: DataParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        params = data_params or load_strategy_parameters(config_path).data
        self.timeout = params.credit_spread_timeout if timeout is None else int(timeout)

    def fetch(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        params = {
            "bjlx": "no",
            "dcq": "10,10y;",
            "startTime": start_ts.strftime("%Y-%m-%d"),
            "endTime": end_ts.strftime("%Y-%m-%d"),
            "qxlx": "0,",
            "yqqxN": "N",
            "yqqxK": "K",
            "par": "day",
            "ycDefIds": f"{TREASURY_CURVE_ID},{CORP_AA_CURVE_ID}",
            "locale": "zh_CN",
        }
        request = urllib.request.Request(
            CHINABOND_QUERY_YZ_URL + "?" + urllib.parse.urlencode(params),
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))

        spread = None
        for item in payload:
            if item.get("ycDefId") == "yzdcqx" or item.get("ycDefName") == "鐐瑰樊鏇茬嚎":
                spread = item
                break
        if spread is None:
            raise DataSourceUnavailable("Chinabond response did not include the spread series.")

        rows = []
        for ts_ms, value in spread.get("seriesData", []):
            trade_date = pd.to_datetime(int(ts_ms), unit="ms").normalize()
            rows.append(
                {
                    "trade_date": trade_date,
                    "value": float(value),
                    "indicator_code": "credit_spread",
                    "source_table": "chinabond_queryYz_10y_AA_minus_treasury",
                }
            )
        if not rows:
            raise DataSourceUnavailable("Chinabond response returned an empty spread history.")
        return pd.DataFrame(rows)


class CreditSpreadReferenceUpdater:
    """Fetch credit-spread history via source plugins or fall back to the local snapshot."""

    def __init__(
        self,
        reference_path: str | Path | None = None,
        metadata_path: str | Path | None = None,
        fetcher: Callable[[pd.Timestamp, pd.Timestamp], pd.DataFrame] | None = None,
        primary_source: CreditSpreadReferenceSource
        | Callable[[pd.Timestamp, pd.Timestamp], pd.DataFrame]
        | None = None,
        backup_sources: list[
            CreditSpreadReferenceSource | Callable[[pd.Timestamp, pd.Timestamp], pd.DataFrame]
        ]
        | None = None,
        timeout: int | None = None,
        data_params: DataParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        base_dir = Path(__file__).resolve().parent / "local_reference" / "macro"
        self.reference_path = Path(reference_path or (base_dir / "credit_spread.csv"))
        self.metadata_path = Path(
            metadata_path or (self.reference_path.parent / "credit_spread.meta.json")
        )
        params = data_params or load_strategy_parameters(config_path).data
        self.timeout = params.credit_spread_timeout if timeout is None else int(timeout)

        if primary_source is not None and fetcher is not None:
            raise ValueError("Use either primary_source or fetcher, not both.")
        if primary_source is None and fetcher is not None:
            primary_source = CallableCreditSpreadSource("custom_primary", fetcher)

        self.primary_source = self._coerce_source(
            primary_source or ChinabondQueryYzSource(timeout=self.timeout),
            default_name="primary_source",
        )
        self.backup_sources = [
            self._coerce_source(source, default_name=f"backup_source_{index + 1}")
            for index, source in enumerate(backup_sources or [])
        ]

    def refresh(
        self,
        start_date: object,
        end_date: object,
        use_existing_on_failure: bool = True,
    ) -> pd.DataFrame:
        """Refresh the local reference table, or fall back to the last good file."""

        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        errors: list[str] = []

        try:
            for index, source in enumerate([self.primary_source, *self.backup_sources]):
                try:
                    fresh = DataSchema.standardize(
                        "macro_daily",
                        source.fetch(start_ts, end_ts),
                    )
                    if fresh.empty:
                        raise DataSourceUnavailable(
                            f"Fetched credit_spread reference is empty from {source.name}."
                        )
                    existing = self.load_existing()
                    merged = DataSchema.standardize(
                        "macro_daily",
                        merge_frames(
                            existing,
                            fresh,
                            key_columns=("indicator_code", "trade_date"),
                            sort_columns=("indicator_code", "trade_date"),
                        ),
                    )
                    self._save_frame(merged)
                    self._save_metadata(
                        {
                            "mode": "fresh_primary" if index == 0 else "fresh_backup",
                            "coverage_start": merged["trade_date"].min().strftime("%Y-%m-%d"),
                            "coverage_end": merged["trade_date"].max().strftime("%Y-%m-%d"),
                            "rows": int(len(merged)),
                            "source_table": str(merged["source_table"].iloc[-1]),
                            "active_source": source.name,
                            "registered_backups": [item.name for item in self.backup_sources],
                        }
                    )
                    return merged
                except Exception as source_exc:
                    errors.append(f"{source.name}: {source_exc}")
            raise DataSourceUnavailable("; ".join(errors) or "No credit_spread source succeeded.")
        except Exception as exc:
            existing = self.load_existing()
            if use_existing_on_failure and existing is not None and not existing.empty:
                self._save_metadata(
                    {
                        "mode": "fallback_local",
                        "coverage_start": existing["trade_date"].min().strftime("%Y-%m-%d"),
                        "coverage_end": existing["trade_date"].max().strftime("%Y-%m-%d"),
                        "rows": int(len(existing)),
                        "source_table": str(existing["source_table"].iloc[-1]),
                        "active_source": "local_snapshot",
                        "registered_backups": [item.name for item in self.backup_sources],
                        "last_error": str(exc),
                    }
                )
                return existing
            raise DataSourceUnavailable(
                f"Unable to refresh credit_spread reference: {exc}"
            ) from exc

    def load_existing(self) -> pd.DataFrame | None:
        """Load the last saved local reference table if it exists."""

        if not self.reference_path.exists():
            return None
        frame = pd.read_csv(self.reference_path, encoding="utf-8-sig")
        return DataSchema.standardize("macro_daily", frame)

    def status(self, as_of_date: object | None = None) -> CreditSpreadReferenceStatus:
        """Return coverage and freshness information for the local snapshot."""

        existing = self.load_existing()
        if existing is None or existing.empty:
            return CreditSpreadReferenceStatus(
                mode="missing",
                rows=0,
                coverage_start=None,
                coverage_end=None,
                stale_days=None,
                source_table=None,
                active_source=None,
            )

        coverage_start = existing["trade_date"].min()
        coverage_end = existing["trade_date"].max()
        stale_days = None
        if as_of_date is not None:
            stale_days = max(0, int((normalize_date(as_of_date) - coverage_end).days))

        metadata = self._load_metadata()
        return CreditSpreadReferenceStatus(
            mode=str(metadata.get("mode", "local_snapshot")),
            rows=int(len(existing)),
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            stale_days=stale_days,
            source_table=str(metadata.get("source_table", existing["source_table"].iloc[-1])),
            active_source=str(metadata.get("active_source", "local_snapshot")),
        )

    def _save_frame(self, frame: pd.DataFrame) -> None:
        self.reference_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(self.reference_path, index=False, encoding="utf-8-sig")

    def _save_metadata(self, payload: dict[str, object]) -> None:
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

    def _load_metadata(self) -> dict[str, object]:
        if not self.metadata_path.exists():
            return {}
        return json.loads(self.metadata_path.read_text(encoding="utf-8"))

    def _coerce_source(
        self,
        source: CreditSpreadReferenceSource | Callable[[pd.Timestamp, pd.Timestamp], pd.DataFrame],
        default_name: str,
    ) -> CreditSpreadReferenceSource:
        if hasattr(source, "fetch") and hasattr(source, "name"):
            return source  # type: ignore[return-value]
        if callable(source):
            return CallableCreditSpreadSource(default_name, source)
        raise TypeError("credit_spread source must provide `name` and `fetch(...)` or be callable")
