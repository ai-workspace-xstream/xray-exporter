# Authenticated snapshot window history

> Status: 🟡 [PR #3](https://github.com/ai-workspace-xstream/xray-exporter/pull/3) [OPEN]
> Date: 2026-08-01

## Goal

Provide Billing with replayable `GET /v1/snapshots/window` data after a short
outage, while keeping the existing latest snapshot and metrics behavior. The
window contract carries the exporter `node_id` and `env`; Billing owns the
canonical UUID aggregation and rating decision.

## Contract

- `/v1/snapshots/latest` and `/v1/snapshots/window` require
  `Authorization: Bearer $INTERNAL_SERVICE_TOKEN`.
- `since` is required; `until` defaults to the last completed minute.
- `limit` defaults to 120 and is capped at 1440; `cursor` is an RFC3339
  timestamp for forward pagination.
- SQLite history is retained for `SNAPSHOT_RETENTION` (72h by default).
- Each exporter instance must use its own `EXPORTER_NODE_ID` and
  `SNAPSHOT_STORE_PATH`; deployment owns the multi-instance routing.

## Safety decisions

- Snapshot writes and retention pruning are one SQLite transaction.
- History files are chmodded to `0600`; newly-created parent directories use
  `0700`.
- The SQLite connection is single-writer with a busy timeout to avoid lock
  races between collection and window reads.
- Invalid/reversed windows and non-numeric limits are rejected; internal
  storage errors are not returned verbatim over HTTP.
- `/metrics` remains unauthenticated for Prometheus and must stay on a
  protected internal listener because metrics include UUID/email labels.

## Validation

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go mod verify`
- HTTP tests cover Bearer auth, pagination, invalid bounds, and limit caps.
- SQLite tests cover retention pruning, private file mode, persistence after
  reopen, and invalid windows.

## Deployment boundary

This change does not modify UAT hosts, GitOps source routing, PostgreSQL, or
production `svc.plus`. A follow-up infra PR must publish this API, render the
required environment variables into each systemd instance, and give Billing a
routable internal address instead of container-local `127.0.0.1`.
