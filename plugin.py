"""MIRASTACK query_logs plugin — queries VictoriaLogs using LogsQL."""

from __future__ import annotations

import asyncio
import json

from mirastack_sdk import (
    Plugin,
    PluginInfo,
    PluginSchema,
    SchemaParam,
    EngineContext,
    Permission,
    DevOpsStage,
    ExecutionRequest,
    ExecutionResponse,
    serve,
)
from logs_client import LogsClient


class QueryLogsPlugin(Plugin):
    """Plugin for querying VictoriaLogs."""

    def __init__(self):
        self._client: LogsClient | None = None

    def info(self) -> PluginInfo:
        return PluginInfo(
            name="query_logs",
            version="0.1.0",
            description="Query VictoriaLogs for log data using LogsQL",
            permission=Permission.READ,
            devops_stages=[DevOpsStage.OBSERVE],
        )

    def schema(self) -> PluginSchema:
        return PluginSchema(
            params=[
                SchemaParam(name="action", type="string", required=True,
                           description="One of: query, tail, stats, field_names, field_values"),
                SchemaParam(name="logsql", type="string", required=False,
                           description="LogsQL query expression"),
                SchemaParam(name="start", type="string", required=False,
                           description="Start time"),
                SchemaParam(name="end", type="string", required=False,
                           description="End time"),
                SchemaParam(name="limit", type="string", required=False,
                           description="Max results (default 100)"),
                SchemaParam(name="field", type="string", required=False,
                           description="Field name for field_values action"),
            ],
        )

    async def execute(self, ctx: EngineContext, req: ExecutionRequest) -> ExecutionResponse:
        if self._client is None:
            config = await ctx.get_config()
            base_url = config.get("logs_url", "http://localhost:9428")
            self._client = LogsClient(base_url)

        action = req.params.get("action", "")
        try:
            result = await self._dispatch(action, req.params)
            return ExecutionResponse(
                output={"result": json.dumps(result, default=str)},
            )
        except Exception as e:
            return ExecutionResponse(
                output={"error": str(e)},
                error=str(e),
            )

    async def _dispatch(self, action: str, params: dict) -> dict | list:
        limit = int(params.get("limit", "100"))
        match action:
            case "query":
                return await self._client.query(
                    params["logsql"], params.get("start"), params.get("end"), limit
                )
            case "tail":
                return await self._client.tail(params["logsql"], limit)
            case "stats":
                return await self._client.stats(
                    params["logsql"], params["start"], params["end"]
                )
            case "field_names":
                return await self._client.field_names()
            case "field_values":
                return await self._client.field_values(params["field"], limit)
            case _:
                raise ValueError(f"Unknown action: {action}")

    async def health_check(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.field_names()
            return True
        except Exception:
            return False

    async def config_updated(self, config: dict):
        if "logs_url" in config:
            if self._client:
                await self._client.close()
            self._client = LogsClient(config["logs_url"])


def main():
    plugin = QueryLogsPlugin()
    asyncio.run(serve(plugin))


if __name__ == "__main__":
    main()
