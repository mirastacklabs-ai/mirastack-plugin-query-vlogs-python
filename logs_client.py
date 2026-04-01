"""VictoriaLogs HTTP client using LogsQL."""

from __future__ import annotations

import httpx
from typing import Any


class LogsClient:
    """Client for VictoriaLogs LogsQL API."""

    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def query(
        self, logsql: str, start: str | None = None, end: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Execute a LogsQL query."""
        params: dict[str, Any] = {"query": logsql, "limit": limit}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        resp = await self._client.get("/select/logsql/query", params=params)
        resp.raise_for_status()
        # VictoriaLogs returns NDJSON (one JSON object per line)
        lines = resp.text.strip().split("\n")
        results = []
        for line in lines:
            if line.strip():
                import json
                results.append(json.loads(line))
        return results

    async def tail(self, logsql: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get the most recent log entries matching the query."""
        return await self.query(logsql, limit=limit)

    async def stats(self, logsql: str, start: str, end: str) -> dict[str, Any]:
        """Get aggregated stats for a LogsQL query."""
        params = {"query": logsql, "start": start, "end": end}
        resp = await self._client.get("/select/logsql/stats_query", params=params)
        resp.raise_for_status()
        return resp.json()

    async def field_names(self) -> list[str]:
        """Get all field names across logs."""
        resp = await self._client.get("/select/logsql/field_names")
        resp.raise_for_status()
        data = resp.json()
        return [item.get("value", "") for item in data] if isinstance(data, list) else []

    async def field_values(self, field: str, limit: int = 100) -> list[str]:
        """Get values for a specific field."""
        params = {"field": field, "limit": limit}
        resp = await self._client.get("/select/logsql/field_values", params=params)
        resp.raise_for_status()
        data = resp.json()
        return [item.get("value", "") for item in data] if isinstance(data, list) else []

    async def close(self):
        await self._client.aclose()
