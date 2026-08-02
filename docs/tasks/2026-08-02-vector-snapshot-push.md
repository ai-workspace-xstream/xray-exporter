# Exporter snapshot push contract

The exporter remains based on `compassvpn/xray-exporter v0.6.0` and keeps its
Prometheus `/scrape` output for Vector/Grafana. When `VECTOR_SNAPSHOT_URL` is
configured, each successful normalized snapshot is also POSTed as JSON to the
local Vector HTTP source with `Authorization: Bearer $INTERNAL_SERVICE_TOKEN`.

The exporter never receives or stores a Billing URL. Vector owns the Billing
destination, retries, buffering, and fan-out policy. An unavailable Vector
sink is logged without preventing the exporter from serving Prometheus or
retaining its local snapshot history.
