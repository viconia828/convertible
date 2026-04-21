"""Trading calendar service with local caching and fallback support."""

from __future__ import annotations

from datetime import date

import pandas as pd

from .cache import DataCacheService
from .cache_store import CacheStore
from .exceptions import DataSourceUnavailable
from .schema import DataSchema
from .tushare_client import TushareClient
from .utils import format_tushare_date, merge_frames, normalize_date


class TradingCalendar:
    """Serve exchange trading calendars from Tushare with local persistence."""

    def __init__(
        self,
        client: TushareClient,
        cache_store: CacheStore | None = None,
        cache_service: DataCacheService | None = None,
        source_name: str = "tushare",
        default_exchange: str = "SSE",
    ) -> None:
        if cache_service is None and cache_store is None:
            raise ValueError("cache_store or cache_service is required")
        self.client = client
        self.source_name = source_name
        self.cache_store = cache_store or cache_service.cache_store
        self.cache_service = cache_service or DataCacheService(
            cache_store=self.cache_store,
            source_name=self.source_name,
        )
        self.default_exchange = default_exchange.upper()

    def get_calendar(
        self,
        start_date: object,
        end_date: object,
        exchange: str | None = None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Return the full calendar slice for the requested date range."""

        exchange_code = (exchange or self.default_exchange).upper()
        start_ts = normalize_date(start_date)
        end_ts = normalize_date(end_date)

        cached = (
            None
            if refresh
            else self.cache_service.load_calendar(
                exchange_code,
                standardized_name="trading_calendar",
            )
        )

        if not refresh and self.cache_service.covers_time_series(
            cached,
            start_ts,
            end_ts,
            "calendar_date",
        ):
            return self._slice(cached, start_ts, end_ts)

        try:
            remote = self.client.query(
                api_name="trade_cal",
                params={
                    "exchange": exchange_code,
                    "start_date": format_tushare_date(start_ts),
                    "end_date": format_tushare_date(end_ts),
                },
                fields=DataSchema.get_schema("trading_calendar").source_fields,
            )
            remote = DataSchema.standardize("trading_calendar", remote)
            merged = DataSchema.standardize(
                "trading_calendar",
                merge_frames(
                    cached,
                    remote,
                    key_columns=DataSchema.get_schema("trading_calendar").key_columns,
                    sort_columns=DataSchema.get_schema("trading_calendar").sort_columns,
                ),
            )
            self.cache_service.save_calendar(exchange_code, merged)
            return self._slice(merged, start_ts, end_ts)
        except Exception as exc:  # noqa: BLE001
            fallback = self._load_fallback(exchange_code)
            if fallback is not None and self.cache_service.covers_time_series(
                fallback,
                start_ts,
                end_ts,
                "calendar_date",
            ):
                return self._slice(fallback, start_ts, end_ts)
            if cached is not None and not cached.empty:
                return self._slice(cached, start_ts, end_ts)
            raise DataSourceUnavailable(
                f"Trading calendar unavailable for {exchange_code}: {exc}"
            ) from exc

    def get_open_days(
        self,
        start_date: object,
        end_date: object,
        exchange: str | None = None,
        refresh: bool = False,
    ) -> list[date]:
        """Return open trading days in the requested range."""

        frame = self.get_calendar(start_date, end_date, exchange=exchange, refresh=refresh)
        open_days = frame.loc[frame["is_open"] == 1, "calendar_date"]
        return [timestamp.date() for timestamp in open_days]

    def is_open_day(self, target_date: object, exchange: str | None = None) -> bool:
        """Return whether the given date is an open trading day."""

        target_ts = normalize_date(target_date)
        frame = self.get_calendar(target_ts, target_ts, exchange=exchange)
        if frame.empty:
            return False
        return bool(frame.iloc[0]["is_open"] == 1)

    def previous_open_day(
        self, target_date: object, exchange: str | None = None
    ) -> date | None:
        """Return the previous open day before the target date, if any."""

        target_ts = normalize_date(target_date)
        start_ts = target_ts - pd.Timedelta(days=31)
        frame = self.get_calendar(start_ts, target_ts, exchange=exchange)
        open_days = frame.loc[
            (frame["is_open"] == 1) & (frame["calendar_date"] < target_ts),
            "calendar_date",
        ]
        if open_days.empty:
            return None
        return open_days.iloc[-1].date()

    def next_open_day(
        self, target_date: object, exchange: str | None = None
    ) -> date | None:
        """Return the next open day after the target date, if any."""

        target_ts = normalize_date(target_date)
        end_ts = target_ts + pd.Timedelta(days=31)
        frame = self.get_calendar(target_ts, end_ts, exchange=exchange)
        open_days = frame.loc[
            (frame["is_open"] == 1) & (frame["calendar_date"] > target_ts),
            "calendar_date",
        ]
        if open_days.empty:
            return None
        return open_days.iloc[0].date()

    def _slice(
        self, frame: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp
    ) -> pd.DataFrame:
        mask = (
            frame["calendar_date"].ge(start_ts) & frame["calendar_date"].le(end_ts)
        )
        return frame.loc[mask].reset_index(drop=True)

    def _load_fallback(self, exchange_code: str) -> pd.DataFrame | None:
        return self.cache_service.load_reference_frame(
            "trading_calendar",
            exchange_code,
            standardized_name="trading_calendar",
        )
