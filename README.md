# MIRASTACK Plugin: Query Logs

Python plugin for querying **VictoriaLogs** using LogsQL from MIRASTACK workflows. Part of the core observability plugin suite.

## Capabilities

| Action | Description |
|--------|-------------|
| `query` | Execute LogsQL query (returns NDJSON) |
| `tail` | Live tail logs matching a filter |
| `stats` | Get log volume statistics |
| `field_names` | List indexed field names |
| `field_values` | List values for a specific field |

## Configuration

Configure the VictoriaLogs URL via MIRASTACK settings:

```bash
miractl config set victorialogs.url http://victorialogs:9428
```

## Example Workflow Step

```yaml
- id: find-errors
  type: plugin
  plugin: query_logs
  params:
    action: query
    logsql: "service:api-gateway AND _severity:error"
    start: "-1h"
    end: "now"
    limit: "50"
```

## Development

```bash
pip install -e .
python -m mirastack_plugin_query_logs
```

## Requirements

- Python 3.12+
- httpx
- mirastack-sdk

## License

AGPL v3 — see [LICENSE](LICENSE).
