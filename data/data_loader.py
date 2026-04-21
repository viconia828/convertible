"""Unified data loader for Tushare-backed fixed and time-series datasets."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from strategy_config import DataParameters, StrategyParameters, load_strategy_parameters

from .cache import DataCacheService
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

    CB_DAILY_CROSS_SECTION_MAX_WORKERS = 4
    CB_DAILY_CROSS_SECTION_PARALLEL_THRESHOLD = 8
    CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD = 16
    CB_DAILY_CROSS_SECTION_AGGREGATE_MIN_DAYS = 8
    CODE_SERIES_MAX_WORKERS = 4
    CODE_SERIES_PARALLEL_THRESHOLD = 4
    CB_RATE_BATCH_CACHE_THRESHOLD = 16

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
        self.cache_service = DataCacheService(
            cache_store=self.cache_store,
            source_name=self.source_name,
        )
        self.client = client or TushareClient(
            token=token,
            data_params=self.data_params,
        )
        self.calendar = TradingCalendar(
            client=self.client,
            cache_store=self.cache_store,
            cache_service=self.cache_service,
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

        fixed = (
            None
            if refresh_fixed
            else self.cache_service.load_static_frame(
                "cb_basic",
                "fixed",
                standardized_name="cb_basic",
            )
        )
        mutable = (
            None
            if refresh_mutable
            else self.cache_service.load_static_frame(
                "cb_basic",
                "mutable",
                standardized_name="cb_basic",
            )
        )

        if fixed is not None and mutable is not None and not fixed.empty:
            return self._merge_cb_basic_parts(fixed, mutable)

        raw = self.client.query(
            api_name=DataSchema.get_schema("cb_basic").api_name or "cb_basic",
            fields=DataSchema.get_schema("cb_basic").source_fields,
        )
        standardized = DataSchema.standardize("cb_basic", raw)
        parts = DataSchema.split_by_mutability("cb_basic", standardized)
        self.cache_service.save_static_frame("cb_basic", "fixed", parts["fixed"])
        self.cache_service.save_static_frame("cb_basic", "mutable", parts["mutable"])
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
        columns: list[str] | tuple[str, ...] | None = None,
        aggregate_profile: str | None = None,
    ) -> pd.DataFrame:
        """Load whole-market convertible-bond daily cross sections by trade date."""

        trade_days = self.calendar.get_open_days(start_date, end_date)
        schema = DataSchema.get_schema("cb_daily")
        projected_columns = self._normalize_projected_columns("cb_daily", columns)
        if not trade_days:
            return self._project_frame_columns(
                DataSchema.empty_frame("cb_daily"),
                projected_columns,
            )

        frames: list[pd.DataFrame] = []
        cached_frames_by_day: dict[str, pd.DataFrame] = {}
        fallback_frames_by_day: dict[str, pd.DataFrame | None] = {}
        missing_trade_days: list[str] = []
        trade_day_strs = [
            pd.Timestamp(trade_day).strftime("%Y%m%d") for trade_day in trade_days
        ]
        use_request_panel_cache = bool(
            not refresh and aggregate_profile == "factor_history_v1"
        )
        if use_request_panel_cache:
            cached_panel = self.cache_service.load_request_panel(
                dataset_name="cb_daily_cross_section",
                standardized_name="cb_daily",
                profile=aggregate_profile,
                trade_day_strs=trade_day_strs,
                columns=projected_columns,
            )
            if cached_panel is not None:
                return cached_panel
        aggregate_frames, aggregate_covered_days, pending_aggregate_months = (
            self._load_cb_daily_cross_section_aggregate_months(
                trade_day_strs=trade_day_strs,
                projected_columns=projected_columns,
                aggregate_profile=aggregate_profile,
                refresh=refresh,
            )
        )
        frames.extend(aggregate_frames)
        remaining_trade_day_strs = [
            trade_day_str
            for trade_day_str in trade_day_strs
            if trade_day_str not in aggregate_covered_days
        ]
        resolved_frames_by_day: dict[str, pd.DataFrame] = {}

        if (
            not refresh
            and len(remaining_trade_day_strs)
            >= self.CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD
        ):
            cached_frames_by_day, missing_trade_days = self.cache_service.load_grouped_time_series(
                dataset_name="cb_daily_cross_section",
                cache_keys=remaining_trade_day_strs,
                standardized_name="cb_daily",
                group_column="trade_date",
                group_as_trade_date_key=True,
                columns=projected_columns,
            )
            fallback_frames_by_day = {
                trade_day_str: None for trade_day_str in missing_trade_days
            }
        else:
            for trade_day_str in remaining_trade_day_strs:
                cached = (
                    None
                    if refresh
                    else self.cache_service.load_time_series(
                        "cb_daily_cross_section",
                        trade_day_str,
                        standardized_name="cb_daily",
                        columns=projected_columns,
                    )
                )
                if refresh or cached is None or cached.empty:
                    missing_trade_days.append(trade_day_str)
                    fallback_frames_by_day[trade_day_str] = cached
                else:
                    cached_frames_by_day[trade_day_str] = cached

        fetched_frames_by_day: dict[str, pd.DataFrame] = {}
        if missing_trade_days and not getattr(
            self.client, "is_temporarily_unavailable", False
        ):
            fetched_frames_by_day = self._fetch_cb_daily_cross_sections(
                schema=schema,
                trade_day_strs=missing_trade_days,
            )

        for trade_day_str in remaining_trade_day_strs:
            merged = cached_frames_by_day.get(trade_day_str)
            if merged is None:
                merged = fetched_frames_by_day.get(trade_day_str)
            if merged is None:
                fallback = fallback_frames_by_day.get(trade_day_str)
                merged = (
                    fallback
                    if fallback is not None and not fallback.empty
                    else DataSchema.empty_frame("cb_daily")
                )
            projected = self._project_frame_columns(merged, projected_columns)
            resolved_frames_by_day[trade_day_str] = projected
            frames.append(projected)

        if aggregate_profile and pending_aggregate_months:
            self._materialize_cb_daily_cross_section_aggregate_months(
                aggregate_profile=aggregate_profile,
                projected_columns=projected_columns,
                pending_months=pending_aggregate_months,
                resolved_frames_by_day=resolved_frames_by_day,
            )

        if not frames:
            return self._project_frame_columns(
                DataSchema.empty_frame("cb_daily"),
                projected_columns,
            )
        final = pd.concat(frames, ignore_index=True)
        if use_request_panel_cache:
            self.cache_service.save_request_panel(
                dataset_name="cb_daily_cross_section",
                standardized_name="cb_daily",
                profile=aggregate_profile,
                trade_day_strs=trade_day_strs,
                frame=final,
                columns=projected_columns,
            )
        return final

    def get_cb_rate(
        self,
        codes: str | list[str] | tuple[str, ...],
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Load coupon schedules for convertible bonds."""

        schema = DataSchema.get_schema("cb_rate")
        frames: list[pd.DataFrame] = []
        codes_list = list(dict.fromkeys(ensure_list(codes)))
        cached_by_code: dict[str, pd.DataFrame] = {}
        fallback_by_code: dict[str, pd.DataFrame | None] = {}
        missing_codes: list[str] = []

        if not refresh and len(codes_list) >= self.CB_RATE_BATCH_CACHE_THRESHOLD:
            cached_by_code, missing_codes = self.cache_service.load_grouped_time_series(
                dataset_name="cb_rate",
                cache_keys=codes_list,
                standardized_name="cb_rate",
                group_column="cb_code",
            )
            fallback_by_code = {code: None for code in missing_codes}
        else:
            for code in codes_list:
                cached = (
                    None
                    if refresh
                    else self.cache_service.load_time_series(
                        "cb_rate",
                        code,
                        standardized_name="cb_rate",
                    )
                )
                if refresh or cached is None or cached.empty:
                    missing_codes.append(code)
                    fallback_by_code[code] = cached
                else:
                    cached_by_code[code] = cached

        fetched_by_code: dict[str, pd.DataFrame] = {}
        if missing_codes and not getattr(self.client, "is_temporarily_unavailable", False):
            fetched_by_code = self._fetch_cb_rate_codes(
                schema=schema,
                codes=missing_codes,
            )

        for code in codes_list:
            merged = cached_by_code.get(code)
            if merged is None:
                merged = fetched_by_code.get(code)
            if merged is None:
                fallback = fallback_by_code.get(code)
                merged = (
                    fallback
                    if fallback is not None and not fallback.empty
                    else DataSchema.empty_frame("cb_rate")
                )
            frames.append(merged)

        if not frames:
            return DataSchema.empty_frame("cb_rate")
        return DataSchema.standardize("cb_rate", pd.concat(frames, ignore_index=True))

    def persist_cb_daily_cross_section_derived_fields(
        self,
        updates: pd.DataFrame,
        columns: tuple[str, ...] = ("ytm",),
        base_frame: pd.DataFrame | None = None,
    ) -> None:
        """Fill derived fields back into cached cb_daily_cross_section files."""

        self.cache_service.writeback_derived_fields(
            dataset_name="cb_daily_cross_section",
            updates=updates,
            columns=columns,
            base_frame=base_frame,
            key_columns=("cb_code", "trade_date"),
            date_column="trade_date",
            standardized_name="cb_daily",
            group_as_trade_date_key=True,
        )

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

        cached = (
            None
            if refresh
            else self.cache_service.load_time_series(
                "cb_call",
                cache_key,
                standardized_name="cb_call",
            )
        )
        coverage = self.cache_service.load_time_series_coverage("cb_call", cache_key)

        if refresh or not self.cache_service.covers_sparse_range(coverage, start_ts, end_ts):
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
                    self.cache_service.save_time_series(
                        "cb_call",
                        cache_key,
                        merged,
                    )
                    self.cache_service.save_time_series_coverage(
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
            reference = (
                self.ensure_credit_spread_reference_coverage(start_ts, end_ts)
                if indicator == "credit_spread"
                else self.cache_service.load_reference_frame("macro", indicator)
            )
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

    def ensure_credit_spread_reference_coverage(
        self,
        start_date: object,
        end_date: object,
        refresh: bool = False,
        use_existing_on_failure: bool = True,
    ) -> pd.DataFrame | None:
        """Ensure the local credit-spread reference covers the requested date range."""

        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)
        cached = (
            None
            if refresh
            else self.cache_service.load_reference_frame(
                "macro",
                "credit_spread",
                standardized_name="macro_daily",
            )
        )
        if not refresh and self.cache_service.covers_time_series(
            cached,
            start_ts,
            end_ts,
            "trade_date",
        ):
            return cached
        return self.refresh_credit_spread_reference(
            start_ts,
            end_ts,
            use_existing_on_failure=use_existing_on_failure,
        )

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

        cached = (
            None
            if refresh
            else self.cache_service.load_time_series(
                "yield_curve",
                cache_key,
                standardized_name="yield_curve",
            )
        )

        if refresh or not self.cache_service.covers_time_series(
            cached,
            start_ts,
            end_ts,
            "trade_date",
        ):
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
                    self.cache_service.save_time_series(
                        "yield_curve",
                        cache_key,
                        merged,
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
        codes_list = ensure_list(codes)
        cached_by_code: dict[str, pd.DataFrame] = {}
        fallback_by_code: dict[str, pd.DataFrame | None] = {}
        missing_codes: list[str] = []

        for code in codes_list:
            cached = (
                None
                if refresh
                else self.cache_service.load_time_series(
                    dataset_name,
                    code,
                    standardized_name=dataset_name,
                )
            )
            if refresh or not self.cache_service.covers_time_series(
                cached,
                start_ts,
                end_ts,
                "trade_date",
            ):
                missing_codes.append(code)
                fallback_by_code[code] = cached
            else:
                cached_by_code[code] = cached

        fetched_by_code: dict[str, pd.DataFrame] = {}
        if missing_codes and not getattr(self.client, "is_temporarily_unavailable", False):
            fetched_by_code = self._fetch_time_series_codes(
                dataset_name=dataset_name,
                schema=schema,
                codes=missing_codes,
                start_ts=start_ts,
                end_ts=end_ts,
                cached_by_code=fallback_by_code,
            )

        for code in codes_list:
            merged = cached_by_code.get(code)
            if merged is None:
                merged = fetched_by_code.get(code)
            if merged is None:
                fallback = fallback_by_code.get(code)
                merged = (
                    fallback
                    if fallback is not None and not fallback.empty
                    else DataSchema.empty_frame(dataset_name)
                )

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

    def _fetch_time_series_codes(
        self,
        dataset_name: str,
        schema,
        codes: list[str],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        cached_by_code: dict[str, pd.DataFrame | None],
    ) -> dict[str, pd.DataFrame]:
        if not codes:
            return {}
        if self._should_parallel_fetch_code_series(len(codes)):
            return self._fetch_time_series_codes_parallel(
                dataset_name=dataset_name,
                schema=schema,
                codes=codes,
                start_ts=start_ts,
                end_ts=end_ts,
                cached_by_code=cached_by_code,
            )
        return self._fetch_time_series_codes_sequential(
            dataset_name=dataset_name,
            schema=schema,
            codes=codes,
            start_ts=start_ts,
            end_ts=end_ts,
            cached_by_code=cached_by_code,
        )

    def _fetch_time_series_codes_sequential(
        self,
        dataset_name: str,
        schema,
        codes: list[str],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        cached_by_code: dict[str, pd.DataFrame | None],
    ) -> dict[str, pd.DataFrame]:
        fetched_by_code: dict[str, pd.DataFrame] = {}
        for code in codes:
            try:
                fetched_by_code[code] = self._fetch_time_series_code(
                    dataset_name=dataset_name,
                    schema=schema,
                    code=code,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    cached=cached_by_code.get(code),
                )
            except Exception:  # noqa: BLE001
                continue
        return fetched_by_code

    def _fetch_time_series_codes_parallel(
        self,
        dataset_name: str,
        schema,
        codes: list[str],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        cached_by_code: dict[str, pd.DataFrame | None],
    ) -> dict[str, pd.DataFrame]:
        fetched_by_code: dict[str, pd.DataFrame] = {}
        max_workers = min(self.CODE_SERIES_MAX_WORKERS, len(codes))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_by_code = {
                executor.submit(
                    self._fetch_time_series_code,
                    dataset_name,
                    schema,
                    code,
                    start_ts,
                    end_ts,
                    cached_by_code.get(code),
                ): code
                for code in codes
            }
            for future in as_completed(future_by_code):
                code = future_by_code[future]
                try:
                    fetched_by_code[code] = future.result()
                except Exception:  # noqa: BLE001
                    continue
        return fetched_by_code

    def _fetch_time_series_code(
        self,
        dataset_name: str,
        schema,
        code: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        cached: pd.DataFrame | None,
    ) -> pd.DataFrame:
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
        self.cache_service.save_time_series(dataset_name, code, merged)
        return merged

    def _fetch_cb_rate_codes(
        self,
        schema,
        codes: list[str],
    ) -> dict[str, pd.DataFrame]:
        if not codes:
            return {}
        if self._should_parallel_fetch_code_series(len(codes)):
            return self._fetch_cb_rate_codes_parallel(schema, codes)
        return self._fetch_cb_rate_codes_sequential(schema, codes)

    def _fetch_cb_rate_codes_sequential(
        self,
        schema,
        codes: list[str],
    ) -> dict[str, pd.DataFrame]:
        fetched_by_code: dict[str, pd.DataFrame] = {}
        for code in codes:
            try:
                fetched_by_code[code] = self._fetch_cb_rate_code(schema, code)
            except Exception:  # noqa: BLE001
                continue
        return fetched_by_code

    def _fetch_cb_rate_codes_parallel(
        self,
        schema,
        codes: list[str],
    ) -> dict[str, pd.DataFrame]:
        fetched_by_code: dict[str, pd.DataFrame] = {}
        max_workers = min(self.CODE_SERIES_MAX_WORKERS, len(codes))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_by_code = {
                executor.submit(self._fetch_cb_rate_code, schema, code): code
                for code in codes
            }
            for future in as_completed(future_by_code):
                code = future_by_code[future]
                try:
                    fetched_by_code[code] = future.result()
                except Exception:  # noqa: BLE001
                    continue
        return fetched_by_code

    def _fetch_cb_rate_code(
        self,
        schema,
        code: str,
    ) -> pd.DataFrame:
        fetched = self.client.query(
            api_name=schema.api_name or "cb_rate",
            params={"ts_code": code},
            fields=schema.source_fields,
        )
        merged = DataSchema.standardize("cb_rate", fetched)
        self.cache_service.save_time_series("cb_rate", code, merged)
        return merged

    def _fetch_cb_daily_cross_sections(
        self,
        schema,
        trade_day_strs: list[str],
    ) -> dict[str, pd.DataFrame]:
        if not trade_day_strs:
            return {}
        if self._should_parallel_fetch_cb_daily_cross_sections(len(trade_day_strs)):
            return self._fetch_cb_daily_cross_sections_parallel(schema, trade_day_strs)
        return self._fetch_cb_daily_cross_sections_sequential(schema, trade_day_strs)

    def _fetch_cb_daily_cross_sections_sequential(
        self,
        schema,
        trade_day_strs: list[str],
    ) -> dict[str, pd.DataFrame]:
        fetched_frames: dict[str, pd.DataFrame] = {}
        for trade_day_str in trade_day_strs:
            try:
                fetched_frames[trade_day_str] = self._fetch_cb_daily_cross_section_for_day(
                    schema,
                    trade_day_str,
                )
            except Exception:  # noqa: BLE001
                break
        return fetched_frames

    def _fetch_cb_daily_cross_sections_parallel(
        self,
        schema,
        trade_day_strs: list[str],
    ) -> dict[str, pd.DataFrame]:
        fetched_frames: dict[str, pd.DataFrame] = {}
        max_workers = min(self.CB_DAILY_CROSS_SECTION_MAX_WORKERS, len(trade_day_strs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_by_day = {
                executor.submit(
                    self._fetch_cb_daily_cross_section_for_day,
                    schema,
                    trade_day_str,
                ): trade_day_str
                for trade_day_str in trade_day_strs
            }
            for future in as_completed(future_by_day):
                trade_day_str = future_by_day[future]
                try:
                    fetched_frames[trade_day_str] = future.result()
                except Exception:  # noqa: BLE001
                    continue
        return fetched_frames

    def _fetch_cb_daily_cross_section_for_day(
        self,
        schema,
        trade_day_str: str,
    ) -> pd.DataFrame:
        fetched = self.client.query(
            api_name=schema.api_name or "cb_daily",
            params={"trade_date": trade_day_str},
            fields=schema.source_fields,
        )
        merged = DataSchema.standardize("cb_daily", fetched)
        self.cache_service.save_time_series("cb_daily_cross_section", trade_day_str, merged)
        return merged

    def _project_frame_columns(
        self,
        frame: pd.DataFrame,
        columns: list[str] | tuple[str, ...] | None,
    ) -> pd.DataFrame:
        if columns is None:
            return frame
        selected = [column for column in columns if column in frame.columns]
        return frame.loc[:, selected].copy()

    def _normalize_projected_columns(
        self,
        dataset_name: str,
        columns: list[str] | tuple[str, ...] | None,
    ) -> list[str] | tuple[str, ...] | None:
        if columns is None:
            return None
        schema = DataSchema.get_schema(dataset_name)
        merged = [str(column) for column in columns]
        merged.extend(schema.key_columns)
        return list(dict.fromkeys(merged))

    def _load_cb_daily_cross_section_aggregate_months(
        self,
        trade_day_strs: list[str],
        projected_columns: list[str] | tuple[str, ...] | None,
        aggregate_profile: str | None,
        refresh: bool,
    ) -> tuple[list[pd.DataFrame], set[str], dict[str, list[str]]]:
        if refresh or aggregate_profile is None:
            return [], set(), {}

        grouped_days = self._group_trade_day_strs_by_month(trade_day_strs)
        aggregate_frames: list[pd.DataFrame] = []
        covered_trade_days: set[str] = set()
        pending_months: dict[str, list[str]] = {}

        for month_key, month_days in grouped_days.items():
            metadata = self.cache_service.load_time_series_aggregate_metadata(
                dataset_name="cb_daily_cross_section",
                profile=aggregate_profile,
                partition_key=month_key,
            )
            if self.cache_service.covers_aggregate_trade_days(metadata, month_days):
                aggregate = self.cache_service.load_time_series_aggregate(
                    dataset_name="cb_daily_cross_section",
                    profile=aggregate_profile,
                    partition_key=month_key,
                    standardized_name="cb_daily",
                    columns=projected_columns,
                )
                if aggregate is not None:
                    filtered = self._filter_frame_to_trade_day_keys(aggregate, month_days)
                    if not filtered.empty:
                        aggregate_frames.append(filtered)
                    covered_trade_days.update(month_days)
                    continue
            if len(month_days) >= self.CB_DAILY_CROSS_SECTION_AGGREGATE_MIN_DAYS:
                pending_months[month_key] = month_days

        return aggregate_frames, covered_trade_days, pending_months

    def _materialize_cb_daily_cross_section_aggregate_months(
        self,
        aggregate_profile: str,
        projected_columns: list[str] | tuple[str, ...] | None,
        pending_months: dict[str, list[str]],
        resolved_frames_by_day: dict[str, pd.DataFrame],
    ) -> None:
        if projected_columns is None:
            return

        for month_key, month_days in pending_months.items():
            if any(day not in resolved_frames_by_day for day in month_days):
                continue
            month_frames = [resolved_frames_by_day[day] for day in month_days]
            if month_frames:
                month_frame = pd.concat(month_frames, ignore_index=True)
            else:
                month_frame = self._project_frame_columns(
                    DataSchema.empty_frame("cb_daily"),
                    projected_columns,
                )
            self.cache_service.save_time_series_aggregate(
                dataset_name="cb_daily_cross_section",
                profile=aggregate_profile,
                partition_key=month_key,
                frame=month_frame,
                standardized_name="cb_daily",
            )
            self.cache_service.save_time_series_aggregate_metadata(
                dataset_name="cb_daily_cross_section",
                profile=aggregate_profile,
                partition_key=month_key,
                payload={
                    "covered_trade_days": list(month_days),
                    "projection_columns": list(projected_columns),
                },
            )

    def _group_trade_day_strs_by_month(
        self,
        trade_day_strs: list[str],
    ) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for trade_day_str in trade_day_strs:
            grouped.setdefault(trade_day_str[:6], []).append(trade_day_str)
        return grouped

    def _filter_frame_to_trade_day_keys(
        self,
        frame: pd.DataFrame,
        trade_day_strs: list[str],
    ) -> pd.DataFrame:
        if frame.empty or "trade_date" not in frame.columns:
            return frame.iloc[0:0].copy()
        requested = set(trade_day_strs)
        trade_day_keys = pd.to_datetime(frame["trade_date"], errors="coerce").dt.strftime(
            "%Y%m%d"
        )
        return frame.loc[trade_day_keys.isin(requested)].reset_index(drop=True)

    def _should_parallel_fetch_cb_daily_cross_sections(self, missing_days: int) -> bool:
        return bool(
            missing_days >= self.CB_DAILY_CROSS_SECTION_PARALLEL_THRESHOLD
            and self.CB_DAILY_CROSS_SECTION_MAX_WORKERS > 1
            and getattr(self.client, "supports_parallel_requests", False)
        )

    def _should_parallel_fetch_code_series(self, missing_codes: int) -> bool:
        return bool(
            missing_codes >= self.CODE_SERIES_PARALLEL_THRESHOLD
            and self.CODE_SERIES_MAX_WORKERS > 1
            and getattr(self.client, "supports_parallel_requests", False)
        )

    def _load_cached_grouped_time_series(
        self,
        dataset_name: str,
        cache_keys: list[str],
        standardized_name: str,
        group_column: str,
        group_as_trade_date_key: bool = False,
    ) -> tuple[dict[str, pd.DataFrame], list[str]]:
        return self.cache_service.load_grouped_time_series(
            dataset_name=dataset_name,
            cache_keys=cache_keys,
            standardized_name=standardized_name,
            group_column=group_column,
            group_as_trade_date_key=group_as_trade_date_key,
        )

    def _covers_time_series(
        self,
        frame: pd.DataFrame | None,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        date_column: str,
    ) -> bool:
        return self.cache_service.covers_time_series(
            frame=frame,
            start_ts=start_ts,
            end_ts=end_ts,
            date_column=date_column,
        )

    def _covers_expected_dates(
        self,
        frame: pd.DataFrame | None,
        expected_dates: list[object],
        date_column: str,
        indicator_code: str | None = None,
    ) -> bool:
        return self.cache_service.covers_expected_dates(
            frame=frame,
            expected_dates=expected_dates,
            date_column=date_column,
            indicator_code=indicator_code,
        )

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
        return self.cache_service.covers_sparse_range(coverage, start_ts, end_ts)

    def _load_time_series_coverage(
        self, dataset_name: str, cache_key: str
    ) -> dict[str, pd.Timestamp] | None:
        return self.cache_service.load_time_series_coverage(dataset_name, cache_key)

    def _save_time_series_coverage(
        self,
        dataset_name: str,
        cache_key: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> None:
        self.cache_service.save_time_series_coverage(
            dataset_name=dataset_name,
            cache_key=cache_key,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def _time_series_coverage_path(self, dataset_name: str, cache_key: str) -> Path:
        return self.cache_service.time_series_coverage_path(dataset_name, cache_key)

    def _credit_spread_reference_updater(
        self,
        primary_source: CreditSpreadReferenceSource | None = None,
        backup_sources: list[CreditSpreadReferenceSource] | None = None,
    ) -> CreditSpreadReferenceUpdater:
        reference_path = self.cache_service.reference_frame_path("macro", "credit_spread")
        metadata_path = self.cache_service.reference_metadata_path("macro", "credit_spread")
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
        cached = (
            None
            if refresh
            else self.cache_service.load_time_series(
                "cb_equal_weight",
                cache_key,
                standardized_name="macro_daily",
            )
        )
        expected_trade_days = self.calendar.get_open_days(start_ts, end_ts)

        if refresh or not self.cache_service.covers_expected_dates(
            cached,
            expected_trade_days,
            "trade_date",
            indicator_code="cb_equal_weight",
        ):
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
            self.cache_service.save_time_series("cb_equal_weight", cache_key, merged)
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
