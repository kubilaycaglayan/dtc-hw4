# Incident responder

This lightweight HTTP service accepts Grafana webhooks at `POST /alerts` on port 8001. It saves each alert payload together with recent Order Tracker logs from Loki and traces from Tempo, then starts Claude Code in non-interactive mode to investigate. Each record contains the assistant's final response and can be read at `GET /incidents/{incident_id}`. Grafana's `incident-responder` contact point and default notification policy are provisioned to call this webhook; repeated firing notifications for the same alert are deduplicated, and resolved notifications clear the deduplication state.

Run it from the repository root after the observability stack is running and Claude Code is installed and authenticated:

```bash
python3 incident-response/responder.py
```

Environment settings:

- `RESPONDER_PORT` (default `8001`)
- `GRAFANA_URL`, `GRAFANA_USER`, `GRAFANA_PASSWORD` (defaults `http://localhost:3003`, `admin`, `admin`)
- `CLAUDE_BIN` (default `claude`)
- `AGENT_MAX_BUDGET_USD` (default `0.25`) and `AGENT_TIMEOUT_SECONDS` (default `1800`)
- `INCIDENTS_DIR` (default `incident-response/incidents`)

Test the receiver with:

```bash
curl -X POST http://localhost:8001/alerts \
  -H 'Content-Type: application/json' \
  -d '{"alerts":[{"status":"firing","labels":{"alertname":"ResponderTest","test":"true"},"annotations":{"summary":"Test notification; no incident to fix"}}]}'
```

The response includes an `incident_id` and `status_url`. Poll the status URL until `status` is `completed`, then read `assistant_response`. Test notifications are explicitly handled without repository changes.
