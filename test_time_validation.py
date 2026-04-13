"""Tests for _dispatch time-range validation in query_vlogs Python plugin."""

from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from mirastack_sdk.plugin import TimeRange
from mirastack_sdk.datetimeutils import format_rfc3339


class TestDispatchTimeValidation(unittest.TestCase):
    """Verify that _dispatch rejects invalid time params on fallback path."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _make_plugin(self):
        """Import and create plugin with a mock client."""
        from plugin import QueryLogsPlugin

        p = QueryLogsPlugin()
        p._client = MagicMock()
        p._client.query = AsyncMock(return_value=[{"msg": "test"}])
        p._client.stats = AsyncMock(return_value={"hits": 1})
        return p

    def test_timerange_preferred(self):
        p = self._make_plugin()
        tr = TimeRange(
            start_epoch_ms=1743379200000,
            end_epoch_ms=1743382800000,
        )

        self._run(p._dispatch("query", {"logsql": "*"}, tr))

        args = p._client.query.call_args
        expected_start = format_rfc3339(tr.start_epoch_ms)
        expected_end = format_rfc3339(tr.end_epoch_ms)
        self.assertEqual(args[1].get("start") or args[0][1], expected_start)

    def test_fallback_rejects_bare_dash(self):
        p = self._make_plugin()

        self._run(p._dispatch("query", {
            "logsql": "*",
            "start": "-",
            "end": "-",
        }, None))

        args = p._client.query.call_args
        # start and end should be None (rejected), not "-"
        # query signature: query(logsql, start, end, limit)
        call_start = args[0][1] if len(args[0]) > 1 else args[1].get("start")
        call_end = args[0][2] if len(args[0]) > 2 else args[1].get("end")
        self.assertIsNone(call_start, "bare '-' should be rejected to None")
        self.assertIsNone(call_end, "bare '-' should be rejected to None")

    def test_fallback_rejects_bare_plus(self):
        p = self._make_plugin()

        self._run(p._dispatch("query", {
            "logsql": "*",
            "start": "+",
            "end": "+",
        }, None))

        args = p._client.query.call_args
        call_start = args[0][1] if len(args[0]) > 1 else args[1].get("start")
        call_end = args[0][2] if len(args[0]) > 2 else args[1].get("end")
        self.assertIsNone(call_start, "bare '+' should be rejected to None")
        self.assertIsNone(call_end, "bare '+' should be rejected to None")

    def test_fallback_accepts_valid_relative(self):
        p = self._make_plugin()

        self._run(p._dispatch("query", {
            "logsql": "*",
            "start": "-1h",
            "end": "now",
        }, None))

        args = p._client.query.call_args
        call_start = args[0][1] if len(args[0]) > 1 else args[1].get("start")
        call_end = args[0][2] if len(args[0]) > 2 else args[1].get("end")
        self.assertEqual(call_start, "-1h")
        self.assertEqual(call_end, "now")

    def test_fallback_none_when_no_params(self):
        p = self._make_plugin()

        self._run(p._dispatch("query", {"logsql": "*"}, None))

        args = p._client.query.call_args
        call_start = args[0][1] if len(args[0]) > 1 else args[1].get("start")
        self.assertIsNone(call_start, "missing param should remain None")


if __name__ == "__main__":
    unittest.main()
