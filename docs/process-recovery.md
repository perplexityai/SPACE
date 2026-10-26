# Recovering process clients

Recovery is an explicit mode of the native command API. Existing commands,
terminal sessions and ECW calls keep their existing behavior. The wire contract
is in `pplx-space-public-api::recovery`; the
[recovery RFC](space/rfcs/2026-10-25-process-recovery.md) defines the engine's
bounds and failure semantics.

## Discover and create

Call `GET /v1/sandboxes/{sandbox_id}/process-execution` before the first start.
Require `process_recovery_start`, `process_recovery_handles` and
`process_mutation_receipts`, a nonempty activation ID and a positive
`idempotency_retention_ms`. Require `process_output_gap_resume` before requesting
counted gaps. A missing endpoint or capability means this path is unsupported.

Create a command at `POST /v1/sandboxes/{sandbox_id}/recovery-commands`
with the normal management credential and `Idempotency-Key`:

```json
{
  "contract_version": 1,
  "expected_instance_id": "<discovered instance>",
  "expected_activation_id": "<discovered activation>",
  "process": {
    "command": ["sh", "-c", "cat"],
    "stdin": true,
    "timeout": "5m"
  }
}
```

Older API servers return 404 for this endpoint. The stored session leaves its
legacy command empty, so an older data plane also rejects it before effects.
Creating a session does not start execution.
Persist the returned command and session identity before starting it. Retries
of creation use the same body and HTTP key; retries of execution use the same
session and fences. Never create a different session to retry an uncertain start.
The supplied environment, working directory, user, timeout and PTY use the
existing command fields and become part of the immutable start request.

## One operation per WebSocket

Connect to `attach.url`, resolved against your configured data-plane origin.
Validate the destination before sending the session's `attach.token`; never send
it to a redirect or an unverified origin. Authenticate with the existing
`{"type":"attach","token":"…"}` frame or authorization header. After the
server's `attached` frame, send exactly one recovery request:

| Request type | Fields | Result |
| --- | --- | --- |
| `recovery_start` | None | `recovery_started` with the fenced handle and PID. Identical retries return the same process. |
| `recovery_connect` | `handle`, `output_cursor`, `resume_after_output_gap` | `recovery_connected`, output/gap events, then exit. |
| `recovery_input` | `handle`, `sequence`, `data_base64` | `recovery_accepted` with the sequence. |
| `recovery_close_stdin` | `handle`, `sequence` | `recovery_accepted` with the sequence. |
| `recovery_signal` | `handle`, `sequence`, `signal` | `recovery_accepted` with the sequence. |
| `recovery_resize` | `handle`, positive `cols` and `rows` | `recovery_accepted` without a sequence. |

A handle contains `instance_id`, `activation_id`, `process_id` and
`process_incarnation`. Keep its sandbox and session identity alongside it. It
cannot be used through another command session or retargeted to a new activation.
The transport never starts a process on attach or reconnect, changes fences,
retries a dispatched operation, or falls back to legacy calls. SSE does not
support recovery commands and refuses them before starting work.

Each successful request ends with `recovery_complete`. A `recovery_error` carries
the numeric gRPC code, an optional stable recovery `reason`, and `input_written`
when a partial input outcome is known. EOF, timeout or a transport error before
completion is uncertain. No error licenses a retry with different fences or a
legacy request. A connect stream that ends without an exit is an error; callers
may also intentionally close it after their own read window.

## Client-owned progress

The first connect uses output cursor zero. Stdout and stderr frames carry the
**next** cursor after their bytes. Persist output and cursor together. Repeating
a read at the same cursor does not consume anything. Several connections can
read one command independently; they share the engine's bounded output journal.
A gap reports skipped events, the next cursor, and total output bytes before it.
Subtract bytes already accounted for to compute lost bytes; do not count them as
received output. The engine's primary-reader backpressure policy still applies.

Input, stdin closure and signals share one sequence starting at 1. Resolve one
sequence before issuing the next. Retry with the identical operation and payload;
changing either under a used sequence is rejected. The receipt acknowledges OS
delivery, not application processing. A signal receipt does not mean the program
handled the signal. Expired receipts and partial input remain explicit uncertain
outcomes, never success or rollback.

Bound start retries from the **first attempt**, using discovery's key window.
Completed records and output can expire sooner; a key does not retain either.
After an engine restart or memory restore, old requests are refused. Rediscovery
is for new work, not for silently replaying an old command under new fences.
The session's authentication expiry independently limits access.

The dependency-free [Node example](../examples/process_recovery.mjs) makes one
bounded request and exposes each event to a callback. It performs no retries or
output trimming. Supply your own durable handle/cursor storage and retry policy.
It rejects integers outside JavaScript's exact range; clients needing the full
64-bit cursor or sequence range must use an integer-preserving JSON implementation.
