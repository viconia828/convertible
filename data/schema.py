"""Centralized dataset schema definitions and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Literal

import pandas as pd

from .exceptions import SchemaValidationError

FieldMutability = Literal["fixed", "mutable", "derived"]
FieldDType = Literal["str", "float", "int", "date", "bool"]


@dataclass(frozen=True)
class FieldDefinition:
    """Single standardized field definition."""

    local_name: str
    source_name: str | None
    dtype: FieldDType
    mutability: FieldMutability = "mutable"


@dataclass(frozen=True)
class DatasetSchema:
    """Schema metadata for a standardized dataset."""

    name: str
    api_name: str | None
    key_columns: tuple[str, ...]
    sort_columns: tuple[str, ...]
    date_columns: tuple[str, ...]
    fields: tuple[FieldDefinition, ...]

    @property
    def all_columns(self) -> list[str]:
        return [field.local_name for field in self.fields]

    @property
    def rename_map(self) -> dict[str, str]:
        return {
            field.source_name: field.local_name
            for field in self.fields
            if field.source_name is not None
        }

    @property
    def source_fields(self) -> str:
        return ",".join(
            field.source_name for field in self.fields if field.source_name is not None
        )

    @property
    def fixed_columns(self) -> list[str]:
        return [
            field.local_name for field in self.fields if field.mutability == "fixed"
        ]

    @property
    def mutable_columns(self) -> list[str]:
        return [
            field.local_name for field in self.fields if field.mutability == "mutable"
        ]


class DataSchema:
    """Registry of all Step 0 standardized datasets."""

    _SCHEMAS: dict[str, DatasetSchema] = {
        "trading_calendar": DatasetSchema(
            name="trading_calendar",
            api_name="trade_cal",
            key_columns=("exchange", "calendar_date"),
            sort_columns=("exchange", "calendar_date"),
            date_columns=("calendar_date", "previous_open_date"),
            fields=(
                FieldDefinition("exchange", "exchange", "str", "fixed"),
                FieldDefinition("calendar_date", "cal_date", "date", "fixed"),
                FieldDefinition("is_open", "is_open", "int", "fixed"),
                FieldDefinition("previous_open_date", "pretrade_date", "date", "fixed"),
            ),
        ),
        "cb_basic": DatasetSchema(
            name="cb_basic",
            api_name="cb_basic",
            key_columns=("cb_code",),
            sort_columns=("cb_code",),
            date_columns=(
                "value_date",
                "maturity_date",
                "list_date",
                "delist_date",
                "conv_start_date",
                "conv_end_date",
                "conv_stop_date",
            ),
            fields=(
                FieldDefinition("cb_code", "ts_code", "str", "fixed"),
                FieldDefinition("bond_full_name", "bond_full_name", "str", "fixed"),
                FieldDefinition("bond_short_name", "bond_short_name", "str", "fixed"),
                FieldDefinition("stock_code", "stk_code", "str", "fixed"),
                FieldDefinition("stock_short_name", "stk_short_name", "str", "fixed"),
                FieldDefinition("maturity_years", "maturity", "float", "fixed"),
                FieldDefinition("par_value", "par", "float", "fixed"),
                FieldDefinition("issue_price", "issue_price", "float", "fixed"),
                FieldDefinition("issue_size", "issue_size", "float", "fixed"),
                FieldDefinition("remain_size", "remain_size", "float", "mutable"),
                FieldDefinition("value_date", "value_date", "date", "fixed"),
                FieldDefinition("maturity_date", "maturity_date", "date", "fixed"),
                FieldDefinition("rate_type", "rate_type", "str", "fixed"),
                FieldDefinition("coupon_rate", "coupon_rate", "float", "fixed"),
                FieldDefinition("add_rate", "add_rate", "float", "fixed"),
                FieldDefinition("pay_per_year", "pay_per_year", "int", "fixed"),
                FieldDefinition("list_date", "list_date", "date", "fixed"),
                FieldDefinition("delist_date", "delist_date", "date", "mutable"),
                FieldDefinition("exchange", "exchange", "str", "fixed"),
                FieldDefinition("conv_start_date", "conv_start_date", "date", "fixed"),
                FieldDefinition("conv_end_date", "conv_end_date", "date", "fixed"),
                FieldDefinition("conv_stop_date", "conv_stop_date", "date", "mutable"),
                FieldDefinition(
                    "first_conv_price", "first_conv_price", "float", "fixed"
                ),
                FieldDefinition("conv_price", "conv_price", "float", "mutable"),
                FieldDefinition("rate_clause", "rate_clause", "str", "fixed"),
            ),
        ),
        "cb_daily": DatasetSchema(
            name="cb_daily",
            api_name="cb_daily",
            key_columns=("cb_code", "trade_date"),
            sort_columns=("cb_code", "trade_date"),
            date_columns=("trade_date",),
            fields=(
                FieldDefinition("cb_code", "ts_code", "str"),
                FieldDefinition("trade_date", "trade_date", "date"),
                FieldDefinition("pre_close", "pre_close", "float"),
                FieldDefinition("open", "open", "float"),
                FieldDefinition("high", "high", "float"),
                FieldDefinition("low", "low", "float"),
                FieldDefinition("close", "close", "float"),
                FieldDefinition("price_change", "change", "float"),
                FieldDefinition("pct_change", "pct_chg", "float"),
                FieldDefinition("volume", "vol", "float"),
                FieldDefinition("amount", "amount", "float"),
                FieldDefinition("bond_value", "bond_value", "float"),
                FieldDefinition("bond_premium_rate", "bond_over_rate", "float"),
                FieldDefinition("convert_value", "cb_value", "float"),
                FieldDefinition("premium_rate", "cb_over_rate", "float"),
                FieldDefinition("ytm", None, "float", "derived"),
                FieldDefinition("is_tradable", None, "bool", "derived"),
            ),
        ),
        "cb_rate": DatasetSchema(
            name="cb_rate",
            api_name="cb_rate",
            key_columns=("cb_code", "rate_start_date", "rate_end_date"),
            sort_columns=("cb_code", "rate_start_date", "rate_end_date"),
            date_columns=("rate_start_date", "rate_end_date"),
            fields=(
                FieldDefinition("cb_code", "ts_code", "str", "fixed"),
                FieldDefinition("rate_frequency", "rate_freq", "int", "fixed"),
                FieldDefinition("rate_start_date", "rate_start_date", "date", "fixed"),
                FieldDefinition("rate_end_date", "rate_end_date", "date", "fixed"),
                FieldDefinition("coupon_rate", "coupon_rate", "float", "fixed"),
            ),
        ),
        "cb_call": DatasetSchema(
            name="cb_call",
            api_name="cb_call",
            key_columns=("cb_code", "announcement_date", "call_type"),
            sort_columns=("cb_code", "announcement_date", "call_type"),
            date_columns=("announcement_date", "call_date"),
            fields=(
                FieldDefinition("cb_code", "ts_code", "str", "mutable"),
                FieldDefinition("call_type", "call_type", "str", "mutable"),
                FieldDefinition("call_status", "is_call", "str", "mutable"),
                FieldDefinition("announcement_date", "ann_date", "date", "mutable"),
                FieldDefinition("call_date", "call_date", "date", "mutable"),
            ),
        ),
        "stock_daily": DatasetSchema(
            name="stock_daily",
            api_name="daily",
            key_columns=("stock_code", "trade_date"),
            sort_columns=("stock_code", "trade_date"),
            date_columns=("trade_date",),
            fields=(
                FieldDefinition("stock_code", "ts_code", "str"),
                FieldDefinition("trade_date", "trade_date", "date"),
                FieldDefinition("open", "open", "float"),
                FieldDefinition("high", "high", "float"),
                FieldDefinition("low", "low", "float"),
                FieldDefinition("close", "close", "float"),
                FieldDefinition("pre_close", "pre_close", "float"),
                FieldDefinition("price_change", "change", "float"),
                FieldDefinition("pct_change", "pct_chg", "float"),
                FieldDefinition("volume", "vol", "float"),
                FieldDefinition("amount", "amount", "float"),
                FieldDefinition("is_tradable", None, "bool", "derived"),
            ),
        ),
        "index_daily": DatasetSchema(
            name="index_daily",
            api_name="index_daily",
            key_columns=("index_code", "trade_date"),
            sort_columns=("index_code", "trade_date"),
            date_columns=("trade_date",),
            fields=(
                FieldDefinition("index_code", "ts_code", "str"),
                FieldDefinition("trade_date", "trade_date", "date"),
                FieldDefinition("close", "close", "float"),
                FieldDefinition("open", "open", "float"),
                FieldDefinition("high", "high", "float"),
                FieldDefinition("low", "low", "float"),
                FieldDefinition("pre_close", "pre_close", "float"),
                FieldDefinition("price_change", "change", "float"),
                FieldDefinition("pct_change", "pct_chg", "float"),
                FieldDefinition("volume", "vol", "float"),
                FieldDefinition("amount", "amount", "float"),
            ),
        ),
        "yield_curve": DatasetSchema(
            name="yield_curve",
            api_name="yc_cb",
            key_columns=("curve_code", "trade_date", "curve_type", "curve_term"),
            sort_columns=("curve_code", "trade_date", "curve_type", "curve_term"),
            date_columns=("trade_date",),
            fields=(
                FieldDefinition("curve_code", "ts_code", "str", "fixed"),
                FieldDefinition("trade_date", "trade_date", "date"),
                FieldDefinition("curve_type", "curve_type", "str", "fixed"),
                FieldDefinition("curve_term", "curve_term", "float", "fixed"),
                FieldDefinition("yield_value", "yield", "float"),
            ),
        ),
        "macro_daily": DatasetSchema(
            name="macro_daily",
            api_name=None,
            key_columns=("indicator_code", "trade_date"),
            sort_columns=("indicator_code", "trade_date"),
            date_columns=("trade_date",),
            fields=(
                FieldDefinition("indicator_code", "indicator_code", "str", "fixed"),
                FieldDefinition("trade_date", "trade_date", "date"),
                FieldDefinition("value", "value", "float"),
                FieldDefinition("source_table", "source_table", "str", "fixed"),
            ),
        ),
    }

    @classmethod
    def get_schema(cls, dataset_name: str) -> DatasetSchema:
        """Return the dataset schema for a registered dataset."""

        try:
            return cls._SCHEMAS[dataset_name]
        except KeyError as exc:
            raise SchemaValidationError(f"Unknown dataset schema: {dataset_name}") from exc

    @classmethod
    def empty_frame(cls, dataset_name: str) -> pd.DataFrame:
        """Return an empty standardized frame for the named dataset."""

        schema = cls.get_schema(dataset_name)
        return pd.DataFrame(columns=schema.all_columns)

    @classmethod
    def schema_signature(cls, dataset_name: str) -> str:
        """Return a stable signature for one standardized dataset definition."""

        schema = cls.get_schema(dataset_name)
        payload = {
            "name": schema.name,
            "api_name": schema.api_name,
            "key_columns": list(schema.key_columns),
            "sort_columns": list(schema.sort_columns),
            "date_columns": list(schema.date_columns),
            "fields": [
                {
                    "local_name": field.local_name,
                    "source_name": field.source_name,
                    "dtype": field.dtype,
                    "mutability": field.mutability,
                }
                for field in schema.fields
            ],
        }
        return hashlib.sha1(
            json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]

    @classmethod
    def standardize(cls, dataset_name: str, frame: pd.DataFrame | None) -> pd.DataFrame:
        """Normalize raw remote or cached data into the standardized schema."""

        schema = cls.get_schema(dataset_name)
        if frame is None:
            return cls.empty_frame(dataset_name)
        if frame.empty and len(frame.columns) == 0:
            return cls.empty_frame(dataset_name)

        normalized = frame.copy()
        normalized = normalized.rename(columns=schema.rename_map)

        for field in schema.fields:
            if field.local_name not in normalized.columns and field.source_name is None:
                normalized[field.local_name] = pd.NA

        missing_keys = [key for key in schema.key_columns if key not in normalized.columns]
        if missing_keys:
            raise SchemaValidationError(
                f"{dataset_name} is missing required key columns: {missing_keys}"
            )

        if dataset_name in {"cb_daily", "stock_daily"} and {
            "volume",
            "amount",
        }.issubset(normalized.columns):
            normalized["is_tradable"] = (
                normalized["volume"].fillna(0).gt(0)
                & normalized["amount"].fillna(0).gt(0)
            )

        projected_columns = [
            column for column in schema.all_columns if column in normalized.columns
        ]
        normalized = normalized.loc[:, projected_columns].copy()

        for field in schema.fields:
            if field.local_name not in normalized.columns:
                continue
            normalized[field.local_name] = cls._cast_series(
                normalized[field.local_name], field.dtype
            )

        normalized = normalized.drop_duplicates(
            subset=list(schema.key_columns), keep="last"
        )
        normalized = normalized.sort_values(list(schema.sort_columns), kind="stable")
        return normalized.reset_index(drop=True)

    @classmethod
    def split_by_mutability(
        cls, dataset_name: str, frame: pd.DataFrame
    ) -> dict[str, pd.DataFrame]:
        """Split a standardized frame into fixed and mutable subsets."""

        schema = cls.get_schema(dataset_name)
        standardized = cls.standardize(dataset_name, frame)
        key_columns = list(schema.key_columns)
        fixed_columns = key_columns + [
            column
            for column in schema.fixed_columns
            if column not in key_columns and column in standardized.columns
        ]
        mutable_columns = key_columns + [
            column
            for column in schema.mutable_columns
            if column not in key_columns and column in standardized.columns
        ]
        return {
            "fixed": standardized.loc[:, fixed_columns].copy(),
            "mutable": standardized.loc[:, mutable_columns].copy(),
        }

    @staticmethod
    def _cast_series(series: pd.Series, dtype: FieldDType) -> pd.Series:
        """Cast a series into the schema's target dtype."""

        if dtype == "date":
            return pd.to_datetime(series, errors="coerce")
        if dtype == "float":
            return pd.to_numeric(series, errors="coerce")
        if dtype == "int":
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        if dtype == "bool":
            if pd.api.types.is_bool_dtype(series):
                return series.astype("boolean")
            numeric = pd.to_numeric(series, errors="coerce")
            if not numeric.isna().all():
                return numeric.astype("Int64").astype("boolean")
            normalized = series.astype("string").str.strip().str.lower()
            mapped = normalized.map(
                {
                    "1": True,
                    "0": False,
                    "true": True,
                    "false": False,
                    "<na>": pd.NA,
                }
            )
            return mapped.astype("boolean")
        return series.astype("string")
