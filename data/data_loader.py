"""Unified data loader for Tushare-backed fixed and time-series datasets."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from strategy_config import DataParameters, StrategyParameters, load_strategy_parameters

from .cache_store import CacheStore
from .credit_spread_reference import (
    CreditSpreadReferenceSource,
    CreditSpreadReferenceStatus,
    CreditSpreadReferenceUpdater,
)
from .derived_metrics import enrich_cb_daily
from .exceptions import DataSourceUnavailable
from .schema import DataSchema
from .trading_calendar import TradingCalendar
from .tushare_client import TushareClient
from .utils import ensure_list, format_tushare_date, merge_frames, normalize_date


class DataLoader:
    """Primary Step 0 data interface using Tushare plus local caches."""

    def __init__(
        self,
        cache_dir: str | Path | None = None,
        token: str | None = None,
        client: TushareClient | None = None,
        source_name: str | None = None,
        config: StrategyParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        self.config = config or load_strategy_parameters(config_path)
        self.data_params: DataParameters = self.config.data
        default_cache_dir = Path(__file__).resolve().parent / "cache"
        self.source_name = source_name or self.data_params.source_name
        self.cache_store = CacheStore(cache_dir or default_cache_dir)
        self.client = client or TushareClient(
            token=token,
            data_params=self.data_params,
        )
        self.calendar = TradingCalendar(
            client=self.client,
            cache_store=self.cache_store,
            source_name=self.source_name,
        )

    def get_trading_calendar(
        self,
        start_date: object,
        end_date: object,
        exchange: str | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load trading calendar data with local caching."""

        return self.calendar.get_calendar(
            start_date=start_date,
            end_date=end_date,
            exchange=exchange or self.data_params.calendar_exchange,
            refresh=refresh,
        )

    def get_cb_basic(
        self,
        refresh_fixed: bool = False,
        refresh_mutable: bool = False,
    ) -> pd.DataFrame:
        """Load convertible-bond basic information with fixed/mutable split cache."""

        fixed = None if refresh_fixed else self.cache_store.load_static_frame(
            self.source_name, "cb_basic", "fixed"
        )
        mutable = None if refresh_mutable else self.cache_store.load_static_frame(
            self.source_name, "cb_basic", "mutable"
        )

        fixed = DataSchema.standardize("cb_basic", fixed) if fixed is not None else None
        mutable = DataSchema.standardize("cb_basic", mutable) if mutable is not None else None

        if fixed is not None and mutable is not None and not fixed.empty:
            return self._merge_cb_basic_parts(fixed, mutable)

        raw = self.client.query(
            api_name=DataSchema.get_schema("cb_basic").api_name or "cb_basic",
            fields=DataSchema.get_schema("cb_basic").source_fields,
        )
        standardized = DataSchema.standardize("cb_basic", raw)
        parts = DataSchema.split_by_mutability("cb_basic", standardized)
        self.cache_store.save_static_frame(
            self.source_name, "cb_basic", "fixed", parts["fixed"]
        )
        self.cache_store.save_static_frame(
            self.source_name, "cb_basic", "mutable", parts["mutable"]
        )
        return standardized

    def get_cb_daily(
        self,
        codes: str | list[str] | tuple[str, ...],
        start_date: object,
        end_date: object,
        refresh: bool = False,
        enrich: bool = False,
    ) -> pd.DataFrame:
        """Load convertible-bond daily price and volume data."""

        frame = self._get_time_series(
            dataset_name="cb_daily",
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            refresh=refresh,
        )
        if not enrich or frame.empty:
            return frame

        cb_codes = frame["cb_code"].dropna().unique().tolist()
        cb_basic = self.get_cb_basic()
        cb_rate = self.get_cb_rate(cb_codes)
        return DataSchema.standardize("cb_daily", enrich_cb_daily(frame, cb_basic, cb_rate))

    def get_cb_daily_cross_section(
        self,
        start_date: object,
        end_date: object,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load whole-market convertible-bond daily cross sections by trade date."""

        trade_days = self.calendar.get_open_days(start_date, end_date)
        schema = DataSchema.get_schema("cb_daily")
        frames: list[pd.DataFrame] = []
        remote_available = not getattr(self.client, "is_temporarily_unavailable", False)

        for trade_day in trade_days:
            trade_day_str = pd.Timestamp(trade_day).strftime("%Y%m%d")
            cached = None if refresh else self.cache_store.load_time_series(
                self.source_name, "cb_daily_cross_section", trade_day_str
            )
            cached = (
                DataSchema.standardize("cb_daily", cached) if cached is not None else None
            )
            if refresh or cached is None or cached.empty:
                if remote_available:
                    try:
                        fetched = self.client.query(
                            api_name=schema.api_name or "cb_daily",
                            params={"trade_date": trade_day_str},
                            fields=schema.source_fields,
                        )
                        merged = DataSchema.standardize("cb_daily", fetched)
                        self.cache_store.save_time_series(
                            self.source_name,
                            "cb_daily_cross_section",
                            trade_day_str,
                            merged,
                        )
                    except Exception:  # noqa: BLE001
                        remote_available = False
                        merged = (
                            cached
                            if cached is not None and not cached.empty
                            else DataSchema.empty_frame("cb_daily")
                        )
                else:
                    merged = (
                        cached
                        if cached is not None and not cached.empty
                        else DataSchema.empty_frame("cb_daily")
                    )
            else:
                merged = cached
            if merged is not None:
                frames.append(merged)

        if not frames:
            return DataSchema.empty_frame("cb_daily")
        return DataSchema.standardize("cb_daily", pd.concat(frames, ignore_index=True))

    def get_cb_rate(
        self,
        codes: str | list[str] | tuple[str, ...],
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load coupon schedules for convertible bonds."""

        schema = DataSchema.get_schema("cb_rate")
        frames: list[pd.DataFrame] = []

        for code in ensure_list(codes):
            cached = None if refresh else self.cache_store.load_time_series(
                self.source_name, "cb_rate", code
            )
            cached = DataSchema.standardize("cb_rate", cached) if cached is not None else None

            if refresh or cached is None or cached.empty:
                fetched = self.client.query(
                    api_name=schema.api_name or "cb_rate",
                    params={"ts_code": code},
                    fields=schema.source_fields,
                )
                merged = DataSchema.standardize("cb_rate", fetched)
                self.cache_store.save_time_series(
                    self.source_name, "cb_rate", code, merged
                )
            else:
                merged = cached

            frames.append(merged)

        if not frames:
            return DataSchema.empty_frame("cb_rate")
        return DataSchema.standardize("cb_rate", pd.concat(frames, ignore_index=True))

    def get_cb_call(
        self,
        start_date: object,
        end_date: object,
        codes: str | list[str] | tuple[str, ...] | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load convertible-bond call and non-call event announcements."""

        schema = DataSchema.get_schema("cb_call")
        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        cache_key = "ALL"

        cached = None if refresh else self.cache_store.load_time_series(
            self.source_name, "cb_call", cache_key
        )
        cached = DataSchema.standardize("cb_call", cached) if cached is not None else None
        coverage = self._load_time_series_coverage("cb_call", cache_key)

        if refresh or not self._covers_sparse_range(coverage, start_ts, end_ts):
            if getattr(self.client, "is_temporarily_unavailable", False):
                merged = (
                    cached
                    if cached is not None and not cached.empty
                    else DataSchema.empty_frame("cb_call")
                )
            else:
                try:
                    fetched = self.client.query(
                        api_name=schema.api_name or "cb_call",
                        params={
                            "start_date": format_tushare_date(start_ts),
                            "end_date": format_tushare_date(end_ts),
                        },
                        fields=schema.source_fields,
                    )
                    fetched = DataSchema.standardize("cb_call", fetched)
                    merged = DataSchema.standardize(
                        "cb_call",
                        merge_frames(
                            cached,
                            fetched,
                            key_columns=schema.key_columns,
                            sort_columns=schema.sort_columns,
                        ),
                    )
                    self.cache_store.save_time_series(
                        self.source_name, "cb_call", cache_key, merged
                    )
                    self._save_time_series_coverage(
                        "cb_call",
                        cache_key,
                        start_ts if coverage is None else min(start_ts, coverage["start"]),
                        end_ts if coverage is None else max(end_ts, coverage["end"]),
                    )
                except Exception:  # noqa: BLE001
                    merged = (
                        cached
                        if cached is not None and not cached.empty
                        else DataSchema.empty_frame("cb_call")
                    )
        else:
            merged = cached

        if merged is None or merged.empty:
            return DataSchema.empty_frame("cb_call")

        filtered = merged.loc[
            merged["announcement_date"].ge(start_ts)
            & merged["announcement_date"].le(end_ts)
        ].copy()
        if codes is not None:
            filtered = filtered.loc[filtered["cb_code"].isin(ensure_list(codes))].copy()
        return DataSchema.standardize("cb_call", filtered)

    def get_stock_daily(
        self,
        codes: str | list[str] | tuple[str, ...],
        start_date: object,
        end_date: object,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load stock daily price and volume data."""

        return self._get_time_series(
            dataset_name="stock_daily",
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            refresh=refresh,
        )

    def get_index_daily(
        self,
        codes: str | list[str] | tuple[str, ...],
        start_date: object,
        end_date: object,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load index daily price data."""

        return self._get_time_series(
            dataset_name="index_daily",
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            refresh=refresh,
        )

    def get_macro_daily(
        self,
        indicators: str | list[str] | tuple[str, ...],
        start_date: object,
        end_date: object,
    ) -> pd.DataFrame:
        """Load locally curated macro indicators.

        Macro data is intentionally local-reference-first at this stage because the
        concrete Tushare table mapping for each macro series will be finalized with
        the env module's exact indicator set.
        """

        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        frames: list[pd.DataFrame] = []

        for indicator in ensure_list(indicators):
            direct = self._get_direct_macro_indicator(indicator, start_ts, end_ts)
            if direct is not None:
                frames.append(direct)
                continue
            reference = self.cache_store.load_reference_frame("macro", indicator)
            if reference is None:
                if indicator == "credit_spread":
                    raise DataSourceUnavailable(
                        "Macro indicator 'credit_spread' is not available locally yet. "
                        "Run DataLoader.refresh_credit_spread_reference(...) first."
                    )
                raise DataSourceUnavailable(
                    f"Macro indicator '{indicator}' is not available in local_reference/macro yet."
                )
            reference = reference.copy()
            if "indicator_code" not in reference.columns:
                reference["indicator_code"] = indicator
            if "source_table" not in reference.columns:
                reference["source_table"] = "local_reference"
            standardized = DataSchema.standardize("macro_daily", reference)
            mask = (
                standardized["trade_date"].ge(start_ts)
                & standardized["trade_date"].le(end_ts)
            )
            frames.append(standardized.loc[mask].copy())

        if not frames:
            return DataSchema.empty_frame("macro_daily")
        return DataSchema.standardize("macro_daily", pd.concat(frames, ignore_index=True))

    def refresh_credit_spread_reference(
        self,
        start_date: object,
        end_date: object,
        use_existing_on_failure: bool = True,
        primary_source: CreditSpreadReferenceSource | None = None,
        backup_sources: list[CreditSpreadReferenceSource] | None = None,
    ) -> pd.DataFrame:
        """Refresh the local `credit_spread` reference or fall back to the last snapshot."""

        updater = self._credit_spread_reference_updater(
            primary_source=primary_source,
            backup_sources=backup_sources,
        )
        return updater.refresh(
            start_date=start_date,
            end_date=end_date,
            use_existing_on_failure=use_existing_on_failure,
        )

    def get_credit_spread_reference_status(
        self,
        as_of_date: object | None = None,
    ) -> CreditSpreadReferenceStatus:
        """Return local coverage/freshness diagnostics for `credit_spread`."""

        return self._credit_spread_reference_updater().status(as_of_date=as_of_date)

    def get_yield_curve(
        self,
        curve_code: str,
        start_date: object,
        end_date: object,
        curve_type: str | None = None,
        curve_term: float | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load yield-curve points from the Tushare `yc_cb` endpoint."""

        schema = DataSchema.get_schema("yield_curve")
        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        cache_key = f"{curve_code}__{curve_type}__{curve_term:g}"

        cached = None if refresh else self.cache_store.load_time_series(
            self.source_name, "yield_curve", cache_key
        )
        cached = (
            DataSchema.standardize("yield_curve", cached) if cached is not None else None
        )

        if refresh or not self._covers_time_series(cached, start_ts, end_ts, "trade_date"):
            if getattr(self.client, "is_temporarily_unavailable", False):
                merged = (
                    cached
                    if cached is not None and not cached.empty
                    else DataSchema.empty_frame("yield_curve")
                )
            else:
                try:
                    fetched = self.client.query(
                        api_name=schema.api_name or "yc_cb",
                        params={
                            "ts_code": curve_code,
                            "curve_type": curve_type or self.data_params.treasury_curve_type,
                            "curve_term": (
                                self.data_params.treasury_curve_term
                                if curve_term is None
                                else curve_term
                            ),
                            "start_date": format_tushare_date(start_ts),
                            "end_date": format_tushare_date(end_ts),
                        },
                        fields=schema.source_fields,
                    )
                    fetched = DataSchema.standardize("yield_curve", fetched)
                    merged = DataSchema.standardize(
                        "yield_curve",
                        merge_frames(
                            cached,
                            fetched,
                            key_columns=schema.key_columns,
                            sort_columns=schema.sort_columns,
                        ),
                    )
                    self.cache_store.save_time_series(
                        self.source_name, "yield_curve", cache_key, merged
                    )
                except Exception:  # noqa: BLE001
                    merged = (
                        cached
                        if cached is not None and not cached.empty
                        else DataSchema.empty_frame("yield_curve")
                    )
        else:
            merged = cached

        if merged is None or merged.empty:
            return DataSchema.empty_frame("yield_curve")

        filtered = merged.loc[
            merged["trade_date"].ge(start_ts) & merged["trade_date"].le(end_ts)
        ].copy()
        return DataSchema.standardize("yield_curve", filtered)

    def _get_time_series(
        self,
        dataset_name: str,
        codes: str | list[str] | tuple[str, ...],
        start_date: object,
        end_date: object,
        refresh: bool,
    ) -> pd.DataFrame:
        schema = DataSchema.get_schema(dataset_name)
        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        frames: list[pd.DataFrame] = []
        code_field = schema.key_columns[0]

        for code in ensure_list(codes):
            cached = None if refresh else self.cache_store.load_time_series(
                self.source_name, dataset_name, code
            )
            cached = DataSchema.standardize(dataset_name, cached) if cached is not None else None

            if refresh or not self._covers_time_series(cached, start_ts, end_ts, "trade_date"):
                if getattr(self.client, "is_temporarily_unavailable", False):
                    merged = (
                        cached
                        if cached is not None and not cached.empty
                        else DataSchema.empty_frame(dataset_name)
                    )
                else:
                    try:
                        fetched = self.client.query(
                            api_name=schema.api_name or dataset_name,
                            params={
                                "ts_code": code,
                                "start_date": format_tushare_date(start_ts),
                                "end_date": format_tushare_date(end_ts),
                            },
                            fields=schema.source_fields,
                        )
                        fetched = DataSchema.standardize(dataset_name, fetched)
                        merged = DataSchema.standardize(
                            dataset_name,
                            merge_frames(
                                cached,
                                fetched,
                                key_columns=schema.key_columns,
                                sort_columns=schema.sort_columns,
                            ),
                        )
                        self.cache_store.save_time_series(
                            self.source_name, dataset_name, code, merged
                        )
                    except Exception:  # noqa: BLE001
                        merged = (
                            cached
                            if cached is not None and not cached.empty
                            else DataSchema.empty_frame(dataset_name)
                        )
            else:
                merged = cached

            if merged is None or merged.empty:
                frames.append(DataSchema.empty_frame(dataset_name))
                continue

            subset = merged.loc[
                merged["trade_date"].ge(start_ts) & merged["trade_date"].le(end_ts)
            ].copy()
            if code_field in subset.columns:
                subset[code_field] = subset[code_field].fillna(code)
            frames.append(subset)

        if not frames:
            return DataSchema.empty_frame(dataset_name)
        return DataSchema.standardize(dataset_name, pd.concat(frames, ignore_index=True))

    def _covers_time_series(
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

    def _merge_cb_basic_parts(
        self, fixed: pd.DataFrame, mutable: pd.DataFrame
    ) -> pd.DataFrame:
        merged = pd.merge(
            fixed,
            mutable,
            on="cb_code",
            how="outer",
            suffixes=("", "_mutable"),
        )

        duplicate_columns = [
            column
            for column in merged.columns
            if column.endswith("_mutable") and column[:-8] in merged.columns
        ]
        if duplicate_columns:
            merged = merged.drop(columns=duplicate_columns)

        return DataSchema.standardize("cb_basic", merged)

    def _covers_sparse_range(
        self,
        coverage: dict[str, pd.Timestamp] | None,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> bool:
        if coverage is None:
            return False
        return bool(coverage["start"] <= start_ts and coverage["end"] >= end_ts)

    def _load_time_series_coverage(
        self, dataset_name: str, cache_key: str
    ) -> dict[str, pd.Timestamp] | None:
        metadata_path = self._time_series_coverage_path(dataset_name, cache_key)
        if not metadata_path.exists():
            return None
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        return {
            "start": pd.Timestamp(payload["start"]).normalize(),
            "end": pd.Timestamp(payload["end"]).normalize(),
        }

    def _save_time_series_coverage(
        self,
        dataset_name: str,
        cache_key: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> None:
        metadata_path = self._time_series_coverage_path(dataset_name, cache_key)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(
                {
                    "start": start_ts.strftime("%Y-%m-%d"),
                    "end": end_ts.strftime("%Y-%m-%d"),
                },
                ensure_ascii=True,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _time_series_coverage_path(self, dataset_name: str, cache_key: str) -> Path:
        return (
            self.cache_store.base_dir
            / self.source_name
            / "time_series"
            / dataset_name
            / f"{cache_key}.coverage.json"
        )

    def _credit_spread_reference_updater(
        self,
        primary_source: CreditSpreadReferenceSource | None = None,
        backup_sources: list[CreditSpreadReferenceSource] | None = None,
    ) -> CreditSpreadReferenceUpdater:
        reference_path = self.cache_store.reference_dir / "macro" / "credit_spread.csv"
        metadata_path = self.cache_store.reference_dir / "macro" / "credit_spread.meta.json"
        return CreditSpreadReferenceUpdater(
            reference_path=reference_path,
            metadata_path=metadata_path,
            primary_source=primary_source,
            backup_sources=backup_sources,
            data_params=self.data_params,
        )

    def _get_direct_macro_indicator(
        self,
        indicator: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame | None:
        if indicator == "csi300":
            frame = self.get_index_daily("000300.SH", start_ts, end_ts)
            return self._to_macro_indicator(frame, "close", indicator, "index_daily")
        if indicator == "csi300_amount":
            frame = self.get_index_daily("000300.SH", start_ts, end_ts)
            return self._to_macro_indicator(frame, "amount", indicator, "index_daily")
        if indicator == "bond_index":
            frame = self.get_index_daily("H11001.CSI", start_ts, end_ts)
            return self._to_macro_indicator(frame, "close", indicator, "index_daily")
        if indicator == "treasury_10y":
            frame = self.get_yield_curve(
                curve_code=self.data_params.treasury_curve_code,
                start_date=start_ts,
                end_date=end_ts,
                curve_type=self.data_params.treasury_curve_type,
                curve_term=self.data_params.treasury_curve_term,
            )
            if frame.empty:
                return None
            macro = pd.DataFrame(
                {
                    "indicator_code": indicator,
                    "trade_date": frame["trade_date"],
                    "value": frame["yield_value"],
                    "source_table": "yc_cb",
                }
            )
            return DataSchema.standardize("macro_daily", macro)
        if indicator == "cb_equal_weight":
            frame = self.get_cb_equal_weight_index(start_ts, end_ts)
            return frame
        return None

    def _to_macro_indicator(
        self,
        frame: pd.DataFrame,
        value_column: str,
        indicator: str,
        source_table: str,
    ) -> pd.DataFrame | None:
        if frame.empty or value_column not in frame.columns:
            return None
        macro = pd.DataFrame(
            {
                "indicator_code": indicator,
                "trade_date": frame["trade_date"],
                "value": frame[value_column],
                "source_table": source_table,
            }
        )
        return DataSchema.standardize("macro_daily", macro)

    def get_cb_equal_weight_index(
        self,
        start_date: object,
        end_date: object,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Build an equal-weight convertible-bond index from daily cross sections."""

        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        cache_key = "ALL"
        cached = None if refresh else self.cache_store.load_time_series(
            self.source_name, "cb_equal_weight", cache_key
        )
        cached = (
            DataSchema.standardize("macro_daily", cached) if cached is not None else None
        )

        if refresh or not self._covers_time_series(cached, start_ts, end_ts, "trade_date"):
            cross_section = self.get_cb_daily_cross_section(start_ts, end_ts, refresh=refresh)
            index_frame = self._build_cb_equal_weight_index(cross_section)
            merged = DataSchema.standardize(
                "macro_daily",
                merge_frames(
                    cached,
                    index_frame,
                    key_columns=("indicator_code", "trade_date"),
                    sort_columns=("indicator_code", "trade_date"),
                ),
            )
            self.cache_store.save_time_series(
                self.source_name, "cb_equal_weight", cache_key, merged
            )
        else:
            merged = cached

        if merged is None or merged.empty:
            return DataSchema.empty_frame("macro_daily")

        filtered = merged.loc[
            (merged["indicator_code"] == "cb_equal_weight")
            & merged["trade_date"].ge(start_ts)
            & merged["trade_date"].le(end_ts)
        ].copy()
        return DataSchema.standardize("macro_daily", filtered)

    def _build_cb_equal_weight_index(self, cross_section: pd.DataFrame) -> pd.DataFrame:
        if cross_section.empty:
            return DataSchema.empty_frame("macro_daily")

        valid = cross_section.loc[
            cross_section["pre_close"].gt(0) & cross_section["close"].gt(0)
        ].copy()
        if valid.empty:
            return DataSchema.empty_frame("macro_daily")

        valid["daily_return"] = valid["close"] / valid["pre_close"] - 1.0
        by_day = (
            valid.groupby("trade_date", as_index=False)["daily_return"].mean()
            .sort_values("trade_date", kind="stable")
            .reset_index(drop=True)
        )
        by_day["value"] = (1.0 + by_day["daily_return"]).cumprod() * 100.0
        macro = pd.DataFrame(
            {
                "indicator_code": "cb_equal_weight",
                "trade_date": by_day["trade_date"],
                "value": by_day["value"],
                "source_table": "cb_daily_cross_section",
            }
        )
        return DataSchema.standardize("macro_daily", macro)
