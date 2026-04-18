"""Reusable helpers for simple key=value text config files."""

from __future__ import annotations

from pathlib import Path


def load_key_value_config(path: str | Path) -> dict[str, str]:
    """Load a simple key=value config file.

    Notes:
    - Reads with ``utf-8-sig`` so UTF-8 files with or without BOM both work.
    - Ignores blank lines and lines starting with ``#``.
    - Keeps values as strings so callers can decide how to parse them.
    """

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Config file not found: {file_path}")

    config: dict[str, str] = {}
    with file_path.open("r", encoding="utf-8-sig") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
    return config


def parse_bool(raw_value: str, default: bool = False) -> bool:
    """Parse common bool strings such as true/false/yes/no/1/0."""

    if raw_value is None:
        return default

    value = str(raw_value).strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Cannot parse bool value: {raw_value}")
