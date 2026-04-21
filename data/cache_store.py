"""Local cache helpers for fixed fields, mutable fields, and time-series data."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .utils import safe_filename


class CacheStore:
    """Filesystem-backed cache store for Step 0 datasets."""

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.reference_dir = self.base_dir.parent / "local_reference"

    def load_calendar(self, source_name: str, exchange: str) -> pd.DataFrame | None:
        """Load a cached trading calendar for a given source and exchange."""

        return self._load_csv(
            self.base_dir / source_name / "calendar" / f"{exchange.upper()}.csv"
        )

    def save_calendar(self, source_name: str, exchange: str, frame: pd.DataFrame) -> None:
        """Persist a trading calendar locally."""

        self._save_csv(
            self.base_dir / source_name / "calendar" / f"{exchange.upper()}.csv",
            frame,
        )

    def load_static_frame(
        self, source_name: str, dataset_name: str, part: str
    ) -> pd.DataFrame | None:
        """Load a cached static or mutable slice for a mostly-static dataset."""

        return self._load_csv(
            self.base_dir / source_name / "static" / f"{dataset_name}_{part}.csv"
        )

    def save_static_frame(
        self, source_name: str, dataset_name: str, part: str, frame: pd.DataFrame
    ) -> None:
        """Persist a static or mutable slice for a mostly-static dataset."""

        self._save_csv(
            self.base_dir / source_name / "static" / f"{dataset_name}_{part}.csv",
            frame,
        )

    def load_time_series(
        self,
        source_name: str,
        dataset_name: str,
        code: str,
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame | None:
        """Load a cached per-code time-series file."""

        filename = f"{safe_filename(code)}.csv"
        return self._load_csv(
            self.base_dir / source_name / "time_series" / dataset_name / filename,
            columns=columns,
        )

    def save_time_series(
        self, source_name: str, dataset_name: str, code: str, frame: pd.DataFrame
    ) -> None:
        """Persist a cached per-code time-series file."""

        filename = f"{safe_filename(code)}.csv"
        self._save_csv(
            self.base_dir / source_name / "time_series" / dataset_name / filename,
            frame,
        )

    def load_reference_frame(self, category: str, name: str) -> pd.DataFrame | None:
        """Load a local reference file, such as an exchange calendar fallback."""

        return self._load_csv(
            self.reference_dir / category / f"{safe_filename(name)}.csv"
        )

    def load_time_series_aggregate(
        self,
        source_name: str,
        dataset_name: str,
        profile: str,
        partition_key: str,
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame | None:
        """Load an aggregate time-series cache partition."""

        return self._load_csv(
            self.base_dir
            / source_name
            / "time_series_aggregate"
            / dataset_name
            / safe_filename(profile)
            / f"{safe_filename(partition_key)}.csv",
            columns=columns,
        )

    def save_time_series_aggregate(
        self,
        source_name: str,
        dataset_name: str,
        profile: str,
        partition_key: str,
        frame: pd.DataFrame,
    ) -> None:
        """Persist an aggregate time-series cache partition."""

        self._save_csv(
            self.base_dir
            / source_name
            / "time_series_aggregate"
            / dataset_name
            / safe_filename(profile)
            / f"{safe_filename(partition_key)}.csv",
            frame,
        )

    def _load_csv(
        self,
        path: Path,
        columns: Iterable[str] | None = None,
    ) -> pd.DataFrame | None:
        if not path.exists():
            return None
        if columns is not None:
            requested_columns = [str(column) for column in columns]
            try:
                return pd.read_csv(
                    path,
                    encoding="utf-8-sig",
                    usecols=requested_columns,
                )
            except ValueError:
                requested = set(requested_columns)
                return pd.read_csv(
                    path,
                    encoding="utf-8-sig",
                    usecols=lambda column: column in requested,
                )
        return pd.read_csv(path, encoding="utf-8-sig")

    def _save_csv(self, path: Path, frame: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False, encoding="utf-8-sig")
