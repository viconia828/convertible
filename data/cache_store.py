"""Local cache helpers for fixed fields, mutable fields, and time-series data."""

from __future__ import annotations

from pathlib import Path

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
        self, source_name: str, dataset_name: str, code: str
    ) -> pd.DataFrame | None:
        """Load a cached per-code time-series file."""

        filename = f"{safe_filename(code)}.csv"
        return self._load_csv(
            self.base_dir / source_name / "time_series" / dataset_name / filename
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

    def _load_csv(self, path: Path) -> pd.DataFrame | None:
        if not path.exists():
            return None
        return pd.read_csv(path, encoding="utf-8-sig")

    def _save_csv(self, path: Path, frame: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False, encoding="utf-8-sig")
