# xray-exporter

`xray-exporter` is the v1 translation layer for the Cloud Network Billing & Control Plane.

It polls raw Xray traffic counters, enriches them with account identity labels from
`accounts.svc.plus`, exposes Prometheus metrics, and publishes normalized snapshots
to a local Vector HTTP source for `billing-service`.

## Endpoints

- `GET /metrics`
- `GET /healthz`
- `GET /v1/snapshots/latest` with `Authorization: Bearer $INTERNAL_SERVICE_TOKEN`
- `GET /v1/snapshots/window` with `Authorization: Bearer $INTERNAL_SERVICE_TOKEN`
  and `since`, optional `until`, `limit` (capped at 1440), and `cursor` query
  parameters. Snapshots are retained locally for `SNAPSHOT_RETENTION` (72h by
  default) so Billing can catch up after a short outage.

## Environment

- `XRAY_STATS_URL`
- `XRAY_STATS_TOKEN`
- `ACCOUNTS_BASE_URL`
- `INTERNAL_SERVICE_TOKEN`
- `SNAPSHOT_STORE_PATH`
- `SNAPSHOT_RETENTION`
- `EXPORTER_NODE_ID`
- `EXPORTER_ENV`
- `SCRAPE_INTERVAL`
- `LISTEN_ADDR`
- `VECTOR_SNAPSHOT_URL` (optional; when set, POST normalized snapshots to this
  Vector HTTP source. The exporter never calls Billing directly.)

The snapshot history contains UUIDs and display-only email metadata. Keep the
exporter listener on a protected internal network; `/metrics` is intentionally
unauthenticated for Prometheus but must not be exposed publicly.

When `VECTOR_SNAPSHOT_URL` is configured, the exporter first stores each
snapshot locally and then posts it to Vector. A failed push marks the exporter
degraded and is retried during the next collection cycle; Vector owns disk
buffering and delivery to Billing.
