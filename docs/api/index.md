# SPACE API

Use the SPACE API to create sandboxes, run commands, and collect output. Browse the endpoint reference for request fields, responses, and examples. Fields labeled “Experimental” are outside the supported contract.

This reference covers 14 supported HTTP operations: sandbox creation, inspection and lifecycle actions; command creation and process inspection/control; operation polling; and SSE event streams. Generated file-watch and WebSocket-related types do not make their session-creation or interactive APIs part of this supported surface. Operations outside this reference are incubating; some, including snapshots, checkpoint, rollback, and file-watch sessions, are hidden and answer as if they did not exist unless the deployment enables them for your organization. `/openapi.json` lists the incubating operations that are not hidden; `/openapi-full.json` lists every operation and requires API credentials.

Use the management API endpoint and bearer token for sandbox, command, process, and operation requests. Event streams use the separate data-plane endpoint and session-scoped token returned with the command. Response fields with open string values can gain new values; tolerate unknown values rather than treating the examples as a closed enum.

## Make your first request

Complete [installation and authentication](../installation.md#authenticate) first. Set `SPACE_API_URL` to your deployment's base URL (before `/v1`) and `SPACE_API_TOKEN` to your bearer token. This read-only request lists your sandboxes:

```bash
: "${SPACE_API_URL:?Set your API base URL}"
: "${SPACE_API_TOKEN:?Authenticate first}"
curl --noproxy '*' --fail-with-body --silent --show-error \
  --connect-timeout 5 --max-time 30 \
  --header "Authorization: Bearer $SPACE_API_TOKEN" \
  "${SPACE_API_URL%/}/v1/sandboxes"
```

The example uses `/v1`; shared-host deployments may use `/api/v1` instead. Use HTTPS except for local loopback development.

Ready to run code? Follow [Your first sandbox](../first-sandbox.md) for a complete create, execute, inspect, and cleanup workflow. When a request returns an operation ID, poll it and check that it succeeded before continuing.

## Run commands

- **Attach to start execution.** Creating a command alone does not run it. The process list shows running processes, not command history.
- **Use the session token**, not your management API token. Check the returned URL against your configured stream origin and session path; never follow redirects with credentials.
- **Create and attach in one request** by sending `Accept: text/event-stream` with the create. The response is the event stream, preceded by a `command` frame whose `command` field is the handle. If the server answers with the JSON handle instead, attach as usual.
- **Save output as it arrives.** Streams are live, with no replay. Check `process_exited`; a closed connection alone is not success. See [command output](../first-sandbox.md#read-command-output).
- **Reattach promptly to read a missed exit.** For up to 15 minutes after a command exits, reattaching reports its `process_exited`, and getting its process returns `state` `exited` with the same `exit_code` and `exit_status`. Test for `state` `exited`: a running process omits `state` today and may report `running` later. The window counts the sandbox's running time, so time it spends suspended does not count. It ends sooner once many newer processes start in the sandbox, and does not survive a restart of the sandbox's agent. Sandboxes from templates built before this window existed keep the earlier behavior until their template is rebuilt: reattaching reports the exit for about a minute, and getting the process returns `not_found` as soon as it exits. After the window, an HTTP reattach and getting the process return a non-retryable `404` `not_found`; a WebSocket reattach gets an `error` frame with `retryable` `false`. If the sandbox never recorded the session's first attach, a reattach after the window returns `400` "command could not be started" instead. Output is not replayed. To reattach once the attachment token has expired, resend the command's create request with its original `Idempotency-Key` and body; it returns the same session with a fresh token.

## Limits

Sandbox create (`POST /v1/sandboxes`) and update (`PATCH /v1/sandboxes/{sandbox_id}`) reject input outside these limits with `invalid_request` (400). The error's `details` names the field, for example `{"key": "field", "value": "metadata"}`. Sizes are UTF-8 bytes, and no string may contain NUL.

| Field | Limit |
| --- | --- |
| `name` | 256 bytes |
| `metadata` | 64 entries; keys up to 128 bytes; string values up to 1024 bytes; 16 KiB across keys and string values |
| `env` (create only) | 256 entries; keys up to 128 bytes; values up to 8192 bytes; 64 KiB in total |
| `resources.vcpu` | 1 to 32 |
| `resources.memory_mib` | 512 to 8192 |
| `resources.disk_mib` (create only) | 1 to 131072; it must also hold the template's root filesystem |

Every request that requires an `Idempotency-Key` header takes 1 to 1024 printable ASCII characters (space through `~`) or tabs; other control characters and non-ASCII bytes are rejected. A missing header and an invalid one return different messages.

A sandbox list page holds at most `limit` sandboxes, and ends early once it reaches 8 MiB of JSON. Follow `next_cursor` until it is null rather than treating a short page as the last one.

## Retry safely

Error samples are illustrative, not exhaustive. Messages can change, and the same HTTP status can have different codes or retryability. Follow the returned `retryable` flag and `Retry-After`, not the sample.

Only endpoints that declare a required `Idempotency-Key` deduplicate mutations, and not all for the same time:

- **Sandbox lifecycle operations and command creation:** the API keeps the key for 24 hours. Reuse the same method, path, body, and key within that window. Replaying the creation of a command whose session was revoked returns `409` `conflict` with the detail `{"key": "reason", "value": "session_ended"}` and no session token; create a new command with a new key instead.
- **File writes, batch writes, and renames:** the sandbox keeps the key in memory for 60 seconds after the change completes. A retry after that, or after the sandbox's agent restarts, can apply the change again, so an append can land twice. Retry these promptly.
- **File `mkdir`, `remove`, and change tracking:** they require the key but do not deduplicate.

Process kill and signal do not use this mechanism. Inspect an ambiguous outcome before repeating any of these, since a repeat can apply the action again. See [safe retries](../troubleshooting.md#retry-without-duplicating-work).

## Browse locally

From the repository root, build and serve this read-only reference:

```bash
bash scripts/build-api-docs.sh
python3 -m http.server 8000 --bind 127.0.0.1 --directory target/docs/api
```

Open <http://127.0.0.1:8000>. Requires Node.js 22.12+, npm 10+, the repository's Rust toolchain, a C compiler, and `protoc`.
