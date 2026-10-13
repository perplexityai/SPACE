# E2B Compatibility Wrapper

[Documentation](../README.md)

The E2B Compatibility Wrapper (ECW) lets an existing E2B client use SPACE as
its sandbox backend. ECW translates E2B-compatible HTTP requests into SPACE
operations; it is separate from the [SPACE API](api/index.md).

ECW is a compatibility target maintained by SPACE, not an implementation of a
versioned open E2B specification. The tested baseline is the Python packages
`e2b==2.21.1` and `e2b-code-interpreter==2.6.2`. Other releases and the
JavaScript SDK may work when they use the same wire shapes, but they are not
currently covered by the repository's compatibility tests.

## Configure the deployment

ECW uses one API hostname and one sandbox-traffic base hostname. Configure
`SPACE_ECW_HOSTS` on both the API and data-plane services as a JSON array:

```json
[
  {
    "apiHost": "api.sandbox.example.test",
    "host": "sandbox.example.test"
  }
]
```

Route `api.sandbox.example.test` to the SPACE API and wildcard
`*.sandbox.example.test` traffic to the SPACE data plane. The E2B client domain
for this pair is `sandbox.example.test`.

The API also requires `SPACE_ECW_CREDENTIAL_KEY_PATH`, the path to a mounted
file containing the key. Components that mint or verify ECW data-plane
credentials must share the current key. During rotation, supply the old key as a file referenced by
`SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_PATH` until credentials minted with it have
expired. The reference Kind and AWS deployment definitions wire these settings;
custom deployments must provide them explicitly.

Malformed host configuration currently disables matching routes after logging
a warning. Verify the configured hosts with an authenticated request before
directing client traffic to the deployment.

## Create an API key

ECW authenticates management requests with an E2B API key in the `X-API-Key`
header. SPACE stores its SHA-256 hash, not the raw key.

For an existing organization and user, generate a key and the corresponding
SQL without connecting to the database:

```bash
scripts/seed-api-key.sh \
  --org-id <organization-uuid> \
  --created-by <user-uuid> \
  --name application-name
```

The command prints a newly generated `E2B_API_KEY` once and emits the SQL an
operator must run against the SPACE control-plane database. Store the raw key
in a secret manager before closing the terminal. Pass `--token-file` to enroll
an existing key without printing it.

For a local Kind installation, the helper can create a dedicated organization
and execute the SQL through a temporary PostgreSQL client pod:

```bash
scripts/seed-api-key.sh \
  --bootstrap-org ecw-local \
  --bootstrap-org-name "Local ECW" \
  --bootstrap-user ecw-local@example.test \
  --name local-sdk \
  --execute \
  --context kind-space-local \
  --namespace space
```

Treat `--execute` as an administrative database operation. Select the context
explicitly and inspect the generated SQL before using it outside a disposable
local installation.

## Configure a client

Set the base domain and the raw key, then pass the same values to the SDK:

```bash
export E2B_DOMAIN=sandbox.example.test
export E2B_API_KEY=<raw-key>

uv run --no-project --python 3.11 --with e2b==2.21.1 python - <<'PY'
import os
from e2b import Sandbox

sandbox = Sandbox.create(
    api_key=os.environ["E2B_API_KEY"],
    domain=os.environ["E2B_DOMAIN"],
    template="seed",
    timeout=60,
)
try:
    print(sandbox.commands.run("printf 'Hello from SPACE\\n'").stdout)
finally:
    sandbox.kill()
PY
```

The selected template must already be published. `seed` is the compatibility
default when a create request omits `templateID`; a production deployment will
usually publish and select a purpose-built runtime template instead.

## Compatibility contract

ECW supports the E2B-shaped sandbox create, list, inspect, delete, connect,
timeout, refresh, pause, resume, and fork flows used by the tested Python SDK.
It also supports the SDK's template-build flow and the command, filesystem,
watch, and port operations exercised by the ECW smoke suite.

Lifecycle names differ across the two APIs:

| ECW operation | SPACE operation |
|---|---|
| `pause` | `suspend`: persist VM state and release worker resources |
| `resume` | `restore`: recreate the sandbox from the persisted state |

ECW does not expose the SPACE API's resident-memory pause/resume operations.

The following compatibility limits are important:

- Unsupported sandbox-create capabilities `mcp`, `volumeMounts`, and `iam`
  are rejected with an E2B-shaped `400` response. `autoPauseMemory=true` is
  accepted; `false` is rejected because SPACE cannot cold-resume from a
  filesystem-only E2B pause.
- The legacy create `secure` field is ignored, matching E2B's newer
  always-secure behavior. `network.allowPublicTraffic=true` is rejected because
  ECW data-plane access is token-authenticated.
- Sandbox `pause`, `connect`, and `resume` reject `memory=false`, and `refresh`
  rejects `duration`, because ECW cannot honor those variants yet. Resume
  applies `timeout` and the deprecated `autoPause` field before restoring, so a
  policy-write failure cannot return success for a partially honored request.
  Connect leaves the lifecycle policy unchanged when `timeout` is omitted. An
  explicit timeout replaces the current timeout, including with a shorter
  value; if no E2B policy exists, it creates an auto-delete policy.
- Template `startCmd` and `readyCmd` become the native app named `app` in the
  native declaration, compiled by the builder into systemd units. A TOML inspection
  copy is retained in the guest. Native recipes can declare multiple apps; ECW retains
  its single-command interface. Startup
  uses template ENV only, as root in `/` through `/bin/sh -lc`. Sandbox environment
  overrides apply to later commands, not the template application. Apps are optional:
  `readyCmd` controls the app unit's startup status, not sandbox readiness. Base
  snapshot preparation waits up to the app startup budget, returns early when ready,
  and logs a host warning before capturing if the app fails or times out. ECW apps
  use the default `best-effort` startup policy. Native recipes can opt into
  `required`, which rejects the snapshot on failure while allowing cold boot.
  Restore resumes captured state. Rootfs publication does not validate app readiness.
  A non-empty `readyCmd` without `startCmd` is now rejected; previously it was
  ignored. App failures do not fail sandbox start, resume, restore, or wake.
  Clients may receive connection refused immediately after create and must wait
  for their application separately.
  See [native template startup](native-template-runtime.md) for limits and rollout.
  Per-step `force=true` is promoted to SPACE's build-wide cache switch, which
  may rebuild more layers but will not reuse the requested layer. Positive
  `minFreeDiskMb` and `teamID` are rejected explicitly.
- Template aliases are flat. E2B `team/template` names and tagged alias lookup
  are not supported.
- `code-interpreter-v1` is translated to SPACE's `code-runtime` template. That
  template must exist if callers rely on the alias.
- Sandbox delete records a durable delete operation and waits briefly for it.
  It returns `204` when the delete has finished and `202` when the delete is
  queued but has not finished, for example while the sandbox's node is draining
  or restarting. Both are SDK success responses; repeated deletes join the same
  queued operation, so a `202` needs no retry. The sandbox can remain visible
  until the queued delete finishes. If the delete cannot be recorded, the
  response is a retryable `503` with `Retry-After`.

ECW routes are not included in the SPACE OpenAPI document. Treat the pinned SDK
tests and the behavior documented here as the compatibility baseline. When
upgrading an E2B dependency, run the ECW unit and Kind smoke suites before
deploying it.

Management requests outside this route and parameter contract return
E2B-shaped errors: unsupported endpoints return `501`, unsupported methods
return `405`, and malformed or explicitly unsupported parameter values return
`400`. Like E2B's OpenAPI schemas, ECW request objects remain open to unknown
JSON and query fields so an additive SDK release does not break existing calls;
fields whose values change semantics are modeled and validated explicitly.
Optional JSON bodies are absent only when the request body is empty; a non-empty
body without `Content-Type: application/json` returns `415`. Rejection messages
from the JSON and query extractors describe the invalid category without
copying caller-provided values into the response. Client-validation messages
are classified with a bounded description rather than copied into telemetry.

On the envd-compatible data plane, `/metrics`, `/envs`, and `/files/compose`
return an explicit E2B-shaped `501`; unsupported methods on implemented envd
routes return `405`. Service-port traffic is intercepted before envd routing,
so these paths remain available to applications on user sandbox ports.

`POST /files` accepts `multipart/form-data` and `application/octet-stream`
uploads of up to 256 MiB per request, which the data plane streams to a spool
file rather than holding in memory. For multipart, the 256 MiB includes part
headers and ignored fields. A multipart request may carry at most 1000 parts
and a boundary of at most 70 characters, and each part may carry at most 32
headers. Part headers beyond roughly 60 KiB may be rejected, depending on how
the body is framed and encoded. Only parts named `file` are written. Without a
`path` query, each `file` part's filename names its destination and may be at
most 4096 bytes. Such a filename may not contain `\\`: clients encode it as
either one backslash or two, so the request is rejected instead of guessing.
With a `path` query, the request may carry only one `file` part. JSON file writes (`{"path": …, "content": …}`,
also used when the request has no `Content-Type`) are capped at 1 MiB of
request body and return `413` above that; send larger files as multipart or
octet-stream.

### Retrying an ambiguous command start

A client can lose a `Process.Start` after writing it, for example when the
connection stops acknowledging data, and then cannot tell whether the command
ran. Send an `Idempotency-Key` header of 1 to 256 visible ASCII bytes with the
start, and retry the identical request with the same key. Within 60 seconds of
the first attempt, a retry that finds the original start receives that
process's stream from its first event instead of running the command again; a
retry that finds no earlier start runs it once. Reusing a key with a different
request starts that request as a new command. A later retry usually fails with
`failed_precondition`, but deduplication state is bounded and does not survive
a sandbox runtime restart, so treat it as retry protection rather than an
exactly-once guarantee. Starts without the header keep their existing
behavior.

## Choose the SPACE API for new integrations

Use ECW when an existing application needs E2B SDK compatibility. New SPACE
integrations should normally use the [SPACE API](api/index.md), which exposes
SPACE lifecycle concepts directly and has a generated OpenAPI contract.

### Reconnect timeout ordering

An explicit connect timeout extends the current execution's effective access-idle
deadline without changing its configured idle window. Ordinary refresh and
activity preserve that minimum. Explicit timeout-setting clears the minimum,
even when setting the existing configured timeout. Both operations are applied
under the worker lifecycle lock: an outstanding connect can apply after a timeout
reset and extend it again. Quiesce outstanding reconnects before a reset when
shortening must be deterministic. A timed-out client call is not proof that no
mutation happened. Cached status reads retain their existing publication delay;
a successful connect acknowledges application at the worker.

A successful reconnect or timeout update for a running execution requires
`x-space-access-idle-update: 1` acknowledgement metadata and an `Applied` or
`AlreadyCurrent` outcome from the sandbox manager. Ordinary access touches are
not sufficient acknowledgement of a deadline operation.

If the worker connection fails before a deadline update is sent, for example
while the node service restarts, the API retries it once on a fresh connection
within the same operation deadline. If the connection breaks after the update
may have reached the worker, the API does not resend it. A reconnect extension
succeeds only if the worker's state then shows the extended deadline; otherwise,
and always for a timeout reset, the request fails with a retryable `503` and
`Retry-After`.

When execution identity has not reached the API, it queries the current worker
identity before sending the generation-fenced deadline update. This lookup is
required during normal operation as well as upgrades; missing API identity does
not imply that the sandbox is dormant. The identity lookup does not wake or
recreate a sandbox, and an unavailable or unidentified worker cannot acknowledge
a deadline update.

### Timeout updates before execution activation

Setting a timeout while a sandbox is durably suspended saves the new idle
window without waking it. Its next restore uses that window; a reconnect
extension does not replace it. If an execution is still starting, the timeout
request may wait for it within the original operation deadline. An unavailable
or replaced execution remains an error rather than a successful clock reset.

The [activation policy contract](architecture.md#lifecycle-policy-at-activation)
describes how controllers and workers preserve this behavior across execution
changes and snapshot restores.
