"""MIRASTACK query_logs plugin — queries VictoriaLogs using LogsQL."""

from __future__ import annotations

import json
import os

from mirastack_sdk import (
    ConfigParam,
    Plugin,
    PluginInfo,
    PluginSchema,
    ParamSchema,
    Permission,
    DevOpsStage,
    ExecuteRequest,
    ExecuteResponse,
    serve,
)
from mirastack_sdk.datetimeutils import format_rfc3339
from mirastack_sdk.plugin import TimeRange
from logs_client import LogsClient


class QueryLogsPlugin(Plugin):
    """Plugin for querying VictoriaLogs."""

    def __init__(self):
        self._client: LogsClient | None = None
        # Bootstrap from env var; engine pushes runtime config via config_updated()
        url = os.environ.get("MIRASTACK_LOGS_URL", "")
        if url:
            self._client = LogsClient(url)

    def info(self) -> PluginInfo:
        return PluginInfo(
            name="query_logs",
            version="0.1.0",
            description="Query VictoriaLogs for log data using LogsQL",
            permissions=[Permission.READ],
            devops_stages=[DevOpsStage.OBSERVE],
            config_params=[
                ConfigParam(key="logs_url", type="string", required=True, description="VictoriaLogs base URL (e.g. http://victorialogs:9428)"),
            ],
        )

    def schema(self) -> PluginSchema:
        return PluginSchema(
            input_params=[
                ParamSchema(name="action", type="string", required=True,
                            description="One of: query, tail, stats, field_names, field_values"),
                ParamSchema(name="logsql", type="string", required=False,
                            description="LogsQL query expression"),
                ParamSchema(name="start", type="string", required=False,
                            description="Start time"),
                ParamSchema(name="end", type="string", required=False,
                            description="End time"),
                ParamSchema(name="limit", type="string", required=False,
                            description="Max results (default 100)"),
                ParamSchema(name="field", type="string", required=False,
                            description="Field name for field_values action"),
            ],
            output_params=[
                ParamSchema(name="result", type="json", required=True,
                            description="Query result as JSON"),
            ],
        )

    async def execute(self, req: ExecuteRequest) -> ExecuteResponse:
        if self._client is None:
            return ExecuteResponse(
                output={"error": "logs_url not configured — set MIRASTACK_LOGS_URL or push config via engine"},
                logs=["ERROR: no logs client configured"],
            )

        action = req.params.get("action", "")
        try:
            result = await self._dispatch(action, req.params, req.time_range)
            return ExecuteResponse(
                output={"result": json.dumps(result, default=str)},
            )
        except Exception as e:
            return ExecuteResponse(
                output={"error": str(e)},
                logs=[f"ERROR: {e}"],
            )

    async def _dispatch(self, action: str, params: dict, tr: TimeRange | None = None) -> dict | list:
        limit = int(params.get("limit", "100"))
        # Resolve start/end from engine TimeRange or raw params
        if tr and tr.start_epoch_ms > 0:
            start = format_rfc3339(tr.start_epoch_ms)
            end = format_rfc3339(tr.end_epoch_ms)
        else:
            start = params.get("start")
            end = params.get("end")
        match action:
            case "query":
                return await self._client.query(
                    params["logsql"], start, end, limit
                )
            case "tail":
                return await self._client.tail(params["logsql"], limit)
            case "stats":
                return await self._client.stats(
                    params["logsql"], start, end
                )
            case "field_names":
                return await self._client.field_names()
            case "field_values":
                return await self._client.field_values(params["field"], limit)
            case _:
                raise ValueError(f"Unknown action: {action}")

    async def health_check(self) -> None:
        # Pull config from engine (cached 15s in SDK)
        ec = getattr(self, "_engine_context", None)
        if ec is not None:
            try:
                config = await ec.get_config()
                await self._apply_config(config)
            except Exception:
                pass
        if self._client is None:
            raise RuntimeError("logs_url not configured")
        await self._client.field_names()

    async def config_updated(self, config: dict[str, str]) -> None:
        await self._apply_config(config)

    async def _apply_config(self, config: dict[str, str]) -> None:
        if "logs_url" in config:
            if self._client:
                await self._client.close()
            self._client = LogsClient(config["logs_url"])


def main():
    plugin = QueryLogsPlugin()
    serve(plugin)


if __name__ == "__main__":
    main()
