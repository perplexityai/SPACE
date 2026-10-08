# SPACE API

Use the SPACE API to create sandboxes, run commands, and collect output. Browse the endpoint reference for request fields, responses, and examples. Fields labeled “Experimental” are outside the supported contract.

This reference covers 14 supported HTTP operations: sandbox creation, inspection and lifecycle actions; command creation and process inspection/control; operation polling; and SSE event streams. Generated file-watch and WebSocket-related types do not make their session-creation or interactive APIs part of this supported surface.

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

- **Attach to start execution.** Creating a command alone does not run it. Process endpoints show running processes, not command history.
- **Use the session token**, not your management API token. Check the returned URL against your configured stream origin and session path; never follow redirects with credentials.
- **Save output as it arrives.** Streams are live, with no replay. Check `process_exited`; a closed connection alone is not success. See [command output](../first-sandbox.md#read-command-output).

## Retry safely

Error samples are illustrative, not exhaustive. Messages can change, and the same HTTP status can have different codes or retryability. Follow the returned `retryable` flag and `Retry-After`, not the sample.

Only endpoints that declare a required `Idempotency-Key` deduplicate mutations, and not all for the same time:

- **Sandbox lifecycle operations and command creation:** the API keeps the key for 24 hours. Reuse the same method, path, body, and key within that window.
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
