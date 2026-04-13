"""MIRASTACK query_logs plugin — queries VictoriaLogs using LogsQL."""

from __future__ import annotations

import json
import os

from mirastack_sdk import (
    Action,
    ConfigParam,
    IntentPattern,
    Plugin,
    PluginInfo,
    PluginSchema,
    ParamSchema,
    Permission,
    PromptTemplate,
    DevOpsStage,
    ExecuteRequest,
    ExecuteResponse,
    respond_map,
    respond_error,
    serve,
)
from mirastack_sdk.datetimeutils import format_rfc3339
from mirastack_sdk.plugin import TimeRange
from logs_client import LogsClient
from output import enrich_logs_output


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
            version="0.2.0",
            description=(
                "Search and analyze logs from VictoriaLogs using LogsQL. "
                "Use this plugin to search log entries, discover fields and their values, "
                "compute server-side statistics, and tail live logs. "
                "Start with field_names for schema discovery, query for keyword search, and stats for aggregation."
            ),
            permissions=[Permission.READ],
            devops_stages=[DevOpsStage.OBSERVE],
            intents=[
                IntentPattern(pattern="search logs", description="Search log entries", priority=10),
                IntentPattern(pattern="find errors in logs", description="Search for error-level log entries", priority=9),
                IntentPattern(pattern="log field values", description="List field values from logs", priority=5),
                IntentPattern(pattern="logsql", description="Query using LogsQL syntax", priority=8),
                IntentPattern(pattern="log volume", description="Check log event volume and trends", priority=6),
                IntentPattern(pattern="application logs", description="View application log entries", priority=6),
                IntentPattern(pattern="log aggregation", description="Aggregate and summarize log data", priority=6),
            ],
            actions=[
                Action(
                    id="query",
                    description=(
                        "Search log entries using LogsQL expressions. "
                        "Use this for keyword search, error filtering, or pattern matching in logs. "
                        "Returns raw log lines in NDJSON format."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="search logs", description="Search log entries with LogsQL", priority=10),
                        IntentPattern(pattern="find errors in logs", description="Search for error-level log entries", priority=9),
                        IntentPattern(pattern="grep logs for", description="Search logs matching a pattern", priority=8),
                        IntentPattern(pattern="log entries containing", description="Find logs containing specific text", priority=7),
                    ],
                    input_params=[
                        ParamSchema(name="logsql", type="string", required=True, description="LogsQL query expression (e.g. '_msg:error AND service:payment')"),
                        ParamSchema(name="start", type="string", required=False, description="Start time"),
                        ParamSchema(name="end", type="string", required=False, description="End time"),
                        ParamSchema(name="limit", type="string", required=False, description="Maximum number of log entries to return (default: 100)"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Log entries in VictoriaLogs NDJSON format")],
                ),
                Action(
                    id="tail",
                    description=(
                        "Tail live log entries matching a LogsQL query. "
                        "Use this for real-time log monitoring and debugging active issues."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="tail logs", description="Tail live log entries", priority=9),
                        IntentPattern(pattern="live logs", description="View real-time log stream", priority=8),
                        IntentPattern(pattern="follow logs", description="Follow log output in real time", priority=7),
                    ],
                    input_params=[
                        ParamSchema(name="logsql", type="string", required=True, description="LogsQL query expression"),
                        ParamSchema(name="limit", type="string", required=False, description="Max results (default 100)"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Live log entries")],
                ),
                Action(
                    id="stats",
                    description=(
                        "Compute server-side aggregate statistics from logs using LogsQL stats pipes. "
                        "Use this for counting, grouping, averaging, and other aggregations. "
                        "Much faster than client-side aggregation of raw log entries."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="log statistics", description="Compute aggregate statistics from logs", priority=9),
                        IntentPattern(pattern="count log events", description="Count log entries matching criteria", priority=8),
                        IntentPattern(pattern="aggregate logs", description="Run server-side log aggregation", priority=7),
                    ],
                    input_params=[
                        ParamSchema(name="logsql", type="string", required=True, description="LogsQL stats expression (e.g. '_msg:error | stats count() by (service)')"),
                        ParamSchema(name="start", type="string", required=False, description="Start time"),
                        ParamSchema(name="end", type="string", required=False, description="End time"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Aggregated statistics result")],
                ),
                Action(
                    id="field_names",
                    description=(
                        "List all field names present in log entries. "
                        "Use this for schema discovery to understand what structured fields are available. "
                        "This is typically the first call for log exploration."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="log fields", description="List available log field names", priority=9),
                        IntentPattern(pattern="what log fields exist", description="Discover log schema fields", priority=8),
                        IntentPattern(pattern="log schema", description="Show log entry structure", priority=7),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Array of field names")],
                ),
                Action(
                    id="field_values",
                    description=(
                        "List values observed for a specific log field. "
                        "Use this to find unique values for service names, error codes, or status fields. "
                        "Helpful before building specific log queries."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="log field values", description="List values for a log field", priority=9),
                        IntentPattern(pattern="unique values in logs", description="Find unique field values in logs", priority=8),
                        IntentPattern(pattern="which log services", description="Find service names from log fields", priority=7),
                    ],
                    input_params=[
                        ParamSchema(name="field", type="string", required=True, description="Field name to get values for (e.g., 'service', 'level', 'status')"),
                        ParamSchema(name="limit", type="string", required=False, description="Max results (default 100)"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Array of field values")],
                ),
            ],
            prompt_templates=[
                PromptTemplate(
                    name="query_logs_guide",
                    description="Best practices for using VictoriaLogs query tools",
                    content=(
                        "You have access to VictoriaLogs log search tools. Follow these guidelines:\n\n"
                        "1. DISCOVERY FIRST: Use field_names to discover available fields.\n"
                        "2. LOGSQL BASICS: Use _msg:keyword for message search, field:value for exact match, _msg:~\"regex\" for regex.\n"
                        "3. BOOLEAN OPS: Combine with AND, OR, NOT. Example: _msg:error AND service:payment NOT _msg:timeout\n"
                        "4. TIME SCOPING: Engine provides time range automatically. Narrow scope for performance.\n"
                        "5. STATS for AGGREGATION: Use LogsQL stats pipe for server-side counts.\n"
                        "   Example: \"_msg:error | stats count() by (service)\" counts errors per service.\n"
                        "6. FIELD VALUES: Use field_values to enumerate possible values before filtering.\n"
                        "7. LIMIT results when exploring: start with limit=20, increase if needed.\n"
                        "8. COMMON PATTERNS:\n"
                        "   - Error search: _msg:error AND service:\"my-app\"\n"
                        "   - HTTP 5xx: status:~\"5[0-9]{2}\"\n"
                        "   - Slow requests: duration:>1000\n"
                        "   - Error count by service: _msg:error | stats count() by (service)"
                    ),
                ),
            ],
            config_params=[
                ConfigParam(key="logs_url", type="string", required=True, description="VictoriaLogs base URL (e.g. http://victorialogs:9428)"),
            ],
        )

    def schema(self) -> PluginSchema:
        info = self.info()
        return PluginSchema(actions=info.actions)

    async def execute(self, req: ExecuteRequest) -> ExecuteResponse:
        if self._client is None:
            resp = respond_error("logs_url not configured — set MIRASTACK_LOGS_URL or push config via engine")
            resp.logs = ["ERROR: no logs client configured"]
            return resp

        action = req.action_id or req.params.get("action", "")
        try:
            result = await self._dispatch(action, req.params, req.time_range)
            enriched = enrich_logs_output(action, result)
            return respond_map(enriched)
        except Exception as e:
            resp = respond_error(str(e))
            resp.logs = [f"ERROR: {e}"]
            return resp

    async def _dispatch(self, action: str, params: dict, tr: TimeRange | None = None) -> dict | list:
        limit = int(params.get("limit", "100"))
        # Resolve start/end from engine TimeRange or raw params
        if tr and tr.start_epoch_ms > 0:
            start = format_rfc3339(tr.start_epoch_ms)
            end = format_rfc3339(tr.end_epoch_ms)
        else:
            start = params.get("start")
            end = params.get("end")
            # Reject bare "-", "+" and whitespace-only values that would
            # cause VictoriaLogs parse errors on the direct invocation path.
            if start and start.strip() in ("", "-", "+"):
                start = None
            if end and end.strip() in ("", "-", "+"):
                end = None
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
