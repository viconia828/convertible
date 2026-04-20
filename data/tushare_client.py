"""A tiny dependency-light Tushare HTTP client using urllib."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

from strategy_config import DataParameters, load_strategy_parameters

from .exceptions import DataSourceUnavailable, TushareAPIError


class TushareClient:
    """Minimal Tushare Pro client that avoids non-stdlib HTTP dependencies."""

    supports_parallel_requests = True

    def __init__(
        self,
        token: str | None = None,
        api_url: str | None = None,
        timeout: int | None = None,
        max_retries: int | None = None,
        retry_delay: float | None = None,
        data_params: DataParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        params = data_params or load_strategy_parameters(config_path).data
        self.token = token or os.environ.get("TUSHARE_TOKEN")
        self.api_url = api_url or params.tushare_api_url
        self.timeout = params.tushare_timeout if timeout is None else int(timeout)
        self.max_retries = (
            params.tushare_max_retries if max_retries is None else int(max_retries)
        )
        self.retry_delay = (
            params.tushare_retry_delay if retry_delay is None else float(retry_delay)
        )
        self.calendar_exchange = params.calendar_exchange
        self._temporarily_unavailable = False
        self._last_unavailable_error: str | None = None
        self._opener = self._build_opener()
        if not self.token:
            raise DataSourceUnavailable(
                "TUSHARE_TOKEN is not configured in the environment."
            )

    def query(
        self,
        api_name: str,
        params: dict[str, object] | None = None,
        fields: str | None = None,
    ) -> pd.DataFrame:
        """Send one Tushare query and return a DataFrame."""

        if self._temporarily_unavailable:
            raise DataSourceUnavailable(
                "Tushare is temporarily unavailable in the current run: "
                f"{self._last_unavailable_error or 'unknown error'}"
            )

        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params or {},
        }
        if fields:
            payload["fields"] = fields

        request = urllib.request.Request(
            self.api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

        body = None
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                with self._opener.open(request, timeout=self.timeout) as response:
                    body = response.read().decode("utf-8")
                last_error = None
                break
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in {502, 503, 504} or attempt >= self.max_retries:
                    break
                time.sleep(self.retry_delay * (attempt + 1))
            except urllib.error.URLError as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_delay * (attempt + 1))

        if body is None:
            self._temporarily_unavailable = True
            self._last_unavailable_error = str(last_error)
            raise DataSourceUnavailable(
                f"Tushare request failed for {api_name}: {last_error}"
            ) from last_error

        decoded = json.loads(body)
        if decoded.get("code") != 0:
            raise TushareAPIError(
                f"Tushare returned code {decoded.get('code')} for {api_name}: "
                f"{decoded.get('msg', '')}"
            )

        data = decoded.get("data") or {}
        fields_out = data.get("fields") or []
        items = data.get("items") or []
        return pd.DataFrame(items, columns=fields_out)

    def health_check(self) -> bool:
        """Return whether a minimal Tushare request succeeds."""

        try:
            self.query(
                api_name="trade_cal",
                params={"exchange": self.calendar_exchange},
                fields="exchange,cal_date,is_open,pretrade_date",
            )
        except (DataSourceUnavailable, TushareAPIError):
            return False
        return True

    @property
    def is_temporarily_unavailable(self) -> bool:
        """Whether Tushare has already failed in the current process."""

        return self._temporarily_unavailable

    def _build_opener(self) -> urllib.request.OpenerDirector:
        if self._should_bypass_env_proxy():
            return urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return urllib.request.build_opener()

    def _should_bypass_env_proxy(self) -> bool:
        for key in ("http", "https"):
            proxy_url = os.environ.get(f"{key.upper()}_PROXY") or os.environ.get(
                f"{key.lower()}_proxy"
            )
            if not proxy_url:
                continue
            if self._is_loopback_blackhole_proxy(proxy_url):
                return True
        return False

    def _is_loopback_blackhole_proxy(self, proxy_url: str) -> bool:
        parsed = urllib.parse.urlparse(proxy_url)
        host = (parsed.hostname or "").strip("[]").lower()
        port = parsed.port
        return host in {"127.0.0.1", "localhost", "::1"} and port == 9
