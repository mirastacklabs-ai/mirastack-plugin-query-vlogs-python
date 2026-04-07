"""Output enrichment helpers for the query_logs plugin."""

from __future__ import annotations

import json
from typing import Any

MAX_RESULT_LEN = 32000


def enrich_logs_output(action: str, result: Any) -> dict[str, Any]:
    """Wrap raw log result with metadata for LLM consumption."""
    raw = result if isinstance(result, str) else json.dumps(result, default=str)

    output: dict[str, Any] = {
        "action": action,
        "result": result,
    }

    if len(raw) > MAX_RESULT_LEN:
        output["result"] = raw[:MAX_RESULT_LEN]
        output["truncated"] = True

    # For query/tail actions, count NDJSON lines (each non-empty line is a log entry).
    if action in ("query", "tail") and isinstance(raw, str):
        lines = [line for line in raw.strip().split("\n") if line.strip()]
        output["result_count"] = len(lines)

    # For JSON array or values-key responses, extract count.
    parsed = result if isinstance(result, (dict, list)) else _try_parse(raw)
    if isinstance(parsed, list):
        output["result_count"] = len(parsed)
    elif isinstance(parsed, dict):
        if "values" in parsed and isinstance(parsed["values"], list):
            output["result_count"] = len(parsed["values"])

    return output


def _try_parse(raw: str) -> Any:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
