# space_smoke

## What a scenario may assert on

Decide pass/fail from state the system exposes, not from what it logs.

- Prefer, in order: the public API's responses; an internal API, gRPC or CLI
  read the product already exposes; persisted state read through an existing
  named fixture operation (catalog rows, Kubernetes objects, files on disk); a
  documented metric.
- Wait for a condition by polling that state, never by waiting for a log line.
- Do not assert on log lines — their presence, absence, count, order or text.
  Free text changes without notice; stdout and stderr are captured separately,
  so their order in `kubectl logs` is not reliable; and a line can land after
  the poll that read the log.
- If the outcome you need is visible only in a log, expose it first (an
  inspect or status RPC, a CLI read, a field on an existing response) and
  assert on that. Where the record is itself a product contract that
  dashboards or monitors consume — a structured sandbox event
  (`event_stream`/`event_type`) — assert on its fields, poll for it, and never
  rely on its absence, its order relative to other lines, or an exact count.
- Do not assert on implementation mechanics no client can observe (which
  internal path ran, whether something was hashed). Assert on the outcome.
- Logs are evidence: attach them to the failure output.
- Exception: scenarios whose subject is telemetry delivery itself (for
  example `local.telemetry_reaches_local_backends`) assert on the telemetry.
