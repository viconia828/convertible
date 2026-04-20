from __future__ import annotations

import os
import unittest
import urllib.request
from unittest import mock

from data.tushare_client import TushareClient


class TushareClientProxyTests(unittest.TestCase):
    def test_blackhole_loopback_proxy_triggers_direct_connection(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"HTTP_PROXY": "http://127.0.0.1:9", "HTTPS_PROXY": "http://127.0.0.1:9"},
            clear=False,
        ):
            with mock.patch("urllib.request.build_opener") as build_opener:
                build_opener.return_value = mock.Mock()
                TushareClient(token="demo-token")

        self.assertEqual(build_opener.call_count, 1)
        handler = build_opener.call_args.args[0]
        self.assertIsInstance(handler, urllib.request.ProxyHandler)
        self.assertEqual(handler.proxies, {})

    def test_regular_proxy_is_left_unchanged(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"HTTP_PROXY": "http://127.0.0.1:7890", "HTTPS_PROXY": "http://127.0.0.1:7890"},
            clear=False,
        ):
            with mock.patch("urllib.request.build_opener") as build_opener:
                build_opener.return_value = mock.Mock()
                TushareClient(token="demo-token")

        self.assertEqual(build_opener.call_count, 1)
        self.assertEqual(build_opener.call_args.args, ())

    def test_blackhole_proxy_helper_is_precise(self) -> None:
        client = TushareClient(token="demo-token")

        self.assertTrue(client._is_loopback_blackhole_proxy("http://127.0.0.1:9"))  # noqa: SLF001
        self.assertTrue(client._is_loopback_blackhole_proxy("http://localhost:9"))  # noqa: SLF001
        self.assertFalse(client._is_loopback_blackhole_proxy("http://127.0.0.1:7890"))  # noqa: SLF001
        self.assertFalse(client._is_loopback_blackhole_proxy("http://10.0.0.1:9"))  # noqa: SLF001


if __name__ == "__main__":
    unittest.main()
