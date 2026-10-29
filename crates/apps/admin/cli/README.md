# `space-admin-cli` — SPACE operator CLI

Thin wrapper over the SPACE control-plane admin surface at `/admin/api/v1/...`.
Subcommands let an operator inspect nodes, drive node-drain transitions, manage
warm pools, and build system templates without hand-crafting `curl` calls.

```text
space-admin-cli login [--print-token] [--api-url URL] [--auth-url URL] [--email EMAIL] [--password-stdin]
space-admin-cli template build seed [--publish-tag latest] [--async]
space-admin-cli template build code-runtime [--base seed@sha256:...] [--publish-tag latest]
space-admin-cli template status <seed|code-runtime> <job-id>
space-admin-cli drain-node     <node> [--forced] [--no-replace-after-terminate]
                                               POST /admin/api/v1/nodes/{node}:drain
space-admin-cli terminate-node <node>          POST /admin/api/v1/nodes/{node}:terminate
space-admin-cli undrain-node   <node>          POST /admin/api/v1/nodes/{node}:undrain
space-admin-cli list-nodes [--drain-phase X] [--role X] [--include-deleted] [--json]
space-admin-cli get-node       <node> [--json]
space-admin-cli warm-pool list [--json]
space-admin-cli warm-pool get <id|template-reference> [--json]
space-admin-cli warm-pool create --template NAME --vcpu N --memory-mib N \
  --target-ready-slots N --refill-per-tick N --max-warming-slots N
space-admin-cli warm-pool add ...             # alias for warm-pool create
space-admin-cli warm-pool update --template NAME@sha256:... --vcpu N ...
space-admin-cli runtime env list [--json]
space-admin-cli runtime env default <environment> --reason TEXT [--strand] [--if-generation N] [--json]
space-admin-cli runtime env state <environment> <allowed|deprecated|blocked> --reason TEXT [--strand] [--if-generation N] [--json]
space-admin-cli runtime env nodes [--after NODE_ID | --all] [--json]
space-admin-cli runtime env population [--json]
```

`<node>` accepts a full UUID or a unique short prefix (≥4 chars). Prefix
resolution is client-side via `GET /admin/api/v1/nodes` and surfaces every match
when the prefix is ambiguous.

For setting up warm pools and checking that creates use them, see
[Warm pools](../../../../docs/warm-pools.md).

`runtime env` reads and sets the cluster's execution environment policy, and
`<environment>` is a full environment reference (`sha256:` or `blake3:`). Every write needs `--reason`,
which is recorded with you on the policy. A write that stops an environment, a
block or the cluster's first default, is refused with the counts while work that
can still run is bound to an environment it stops (sandboxes, and template
releases a tag points to or an enabled pool config pins), or while any such
sandbox's or in-use release's environment can't be told (no binding, an
unreadable one, or no record), until you pass `--strand`. Blocking an environment that already doesn't
execute normally needs no `--strand`, unless another policy write lands first. The first default stops every environment with no
state, so mark the ones to keep `allowed` before setting it. `--if-generation`
takes the generation `runtime env list` showed and refuses the write if the
policy has changed since, so a write from a stale read can't undo someone
else's, such as re-allowing an environment another operator just blocked. See "Cluster desired policy" in
[Execution environments](../../../../docs/execution-environments.md).

## Install

```bash
cargo install --path crates/apps/admin/cli --locked
```

Installs `~/.cargo/bin/space-admin-cli`. Re-run after any source change to pick it up.

## Connect to the API

Set `SPACE_API_URL` to the API origin, without `/admin/api/v1`:

```bash
export SPACE_API_URL=https://api.example.com
```

The endpoint must serve `/admin/api/v1`. If session authentication lives on a
separate endpoint, pass its origin with `space-admin-cli login --auth-url URL`. Endpoint
names, ports, and credentials come from your deployment.

### Kubernetes access via `kubectl port-forward`

If the admin endpoint is only reachable inside Kubernetes, an authorized
operator must first forward the admin service. In one terminal, keep this running
with your deployment's values:

```bash
CTX=your-cluster-context
NAMESPACE=your-namespace
ADMIN_SERVICE=your-admin-service
ADMIN_PORT=your-admin-service-port
kubectl port-forward --context "$CTX" -n "$NAMESPACE" \
  --address 127.0.0.1 "svc/$ADMIN_SERVICE" "8081:$ADMIN_PORT"
```

In a second terminal, set `export SPACE_API_URL=http://127.0.0.1:8081`, then
[log in](#logging-in). Kubernetes access only establishes connectivity; it does
not replace SPACE authentication. A directly reachable admin endpoint needs no
port forward.

### Local Kind (`space-local`)

The [local setup](../../../../docs/installation.md) starts the API port forward;
Tilt also forwards it automatically. Set
`export SPACE_API_URL=http://127.0.0.1:8080` and log in below using the local-only
fixture account `root@local` / `password`. That account is an
`organization_admin` in the local cluster's managing organization.

## Auth model

Every admin request carries a bearer token from `SPACE_API_TOKEN`. This is a
SPACE session token from `POST /auth/v1/sessions`, not a Kubernetes token or an
E2B API key. Admin operations require role `organization_admin`; system-template
builds additionally require membership in the cluster's managing organization.
Session tokens are valid for 7 days; log in again when they expire.
The CLI does not provision accounts or refresh expired sessions automatically.

### Logging in

Use an administrator account provisioned for your deployment:

```bash
export SPACE_API_URL=https://api.example.com
SPACE_API_TOKEN="$(space-admin-cli login --print-token)" && export SPACE_API_TOKEN
space-admin-cli template build seed
```

`space-admin-cli login` reads `SPACE_API_URL` automatically; `--api-url URL` overrides it.
No existing token is required. It prompts for the email and a hidden password
on stderr, obtains a session, and verifies managing-organization admin access
with a read-only template-list request. `--print-token` prints only the token to
stdout. Without it, login reports success on stderr and does not print or save
the token.
The shell assignment above captures it for subsequent commands: a child process
cannot export a variable into its parent shell. Keep shell tracing (`set -x`)
disabled when handling tokens.

For separate auth and admin services forwarded to localhost:

```bash
export SPACE_API_URL=http://127.0.0.1:8081
SPACE_API_TOKEN="$(space-admin-cli login --print-token --auth-url http://127.0.0.1:8080)" && export SPACE_API_TOKEN
```

Noninteractive callers can supply `--email EMAIL --password-stdin` and pipe a
password from their secret manager; this flag rejects terminal input to avoid
echoing a password. Interactive hidden-password input requires
Unix; on other platforms, use `--email` with `--password-stdin`. There is no
password command-line flag. Ctrl-C, Ctrl-Z, and SIGTERM cancel login and restore the
terminal settings before exiting.
Login requires HTTPS except for localhost port forwards; credentials are never
sent through redirects. Login and admin requests to localhost bypass configured
HTTP proxies. HTTP errors, empty tokens, and failed admin checks
exit nonzero without printing a token.

The CLI saves no credentials or session files. `unset SPACE_API_TOKEN` clears
the token from the current shell; it does not revoke the server session. A `401`
means a missing, invalid, or expired token; a `403` means the account lacks the
required authorization.

## System templates

`space-admin-cli template build seed` and `space-admin-cli template build code-runtime` use the
canonical TOML recipes embedded in the deployed Admin API. They require an
administrator of the cluster's managing organization. The CLI submits a job,
waits for build and publication, and returns JSON with the content hash. Use
`--async` to return immediately, then `space-admin-cli template status TEMPLATE JOB_ID`.
Both build and publication failures exit nonzero; publication failures retain
the built content hash in the error. While waiting, transient connection errors
and HTTP 5xx responses are retried within the two-hour timeout. HTTP 4xx responses
fail immediately; build submission itself is not retried.

`--publish-tag latest` explicitly promotes the published release. Without it,
only the immutable digest tag is published. `--base seed@sha256:...` pins the
seed for code-runtime; otherwise the API resolves `seed:latest` before dispatch.
There is no local-recipe or internal-gRPC fallback.

Follow the [system-template release procedure](../../../../templates/firecracker/seed/README.md#build-and-publish-system-templates)
for the full seed → code-runtime sequence, prerequisites, and verification.

## Node drain

`space-admin-cli drain-node <node>` cordons the node and starts a drain. The
default drain is gentle: resident sandboxes keep serving traffic until they
suspend on inactivity, are suspended by their user, or are deleted. The node
advances to `drained` once its residents have left and suspended sandboxes'
snapshots have been uploaded. Unless the controller runs with
`SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE=true`, sandboxes in lifecycles
such as `stopped` don't hold the drain, so a drained node can still host
sandboxes that block its termination. Check
`node_drain.sandboxes_remaining{node}` and `node_drain.drain_blockers{node}`
first when a drain stalls; they don't cover every gate term. The
[`node_drain` module docs](../../../libs/cluster/controller/server/src/node_drain/mod.rs)
list the full gate.

A drained node waits for `terminate-node` unless the cluster controller runs
with `SPACE_DRAINED_NODE_TERMINATOR_ENABLED=true`. That reconciler moves a
drained node to `terminating` automatically once its own guards pass (no
sandbox still assigned to the node, no snapshot still on its local disk), and
never for a node whose Kubernetes Node carries the `space.pplx.net/drain-keep`
annotation.
`undrain-node` returns a draining or drained node to service. All three
commands mutate cluster state; see [Safety](#safety).

The cluster controller acts on drains only when it runs with
`SPACE_NODE_DRAIN_ENABLED=true`, which is off by default. Without it,
`drain-node` still cordons the node, and a forced drain still returns 409 to
user calls, but nothing evacuates residents and the node never reaches
`drained`. The controller also disables the drained-node terminator. Use
`undrain-node` to back out.

### `--forced`

`--forced` sets `mode=forced` in the request body. Instead of waiting, the drain
suspends and migrates every resident `started` or `paused` sandbox right away
and returns 409 to user calls against them for the whole drain window. It's
faster, but per-node disruption is proportional to how many live sandboxes the
node hosts.

Re-running `drain-node --forced` against a node that's already draining gently
escalates that drain in place: the node stays cordoned throughout, and the
drain keeps its original start time. Prefer this to `undrain-node` followed by
`drain-node --forced`, which uncordons the node between the two calls and lets
fresh sandboxes land on it. The server refuses the opposite downgrade
(`drain-node` without `--forced` against a forced drain); abort with
`undrain-node` instead.

Escalating also re-enables the stale-transient sweep, which only runs for
forced drains. Any resident wedged in a non-evacuable transient lifecycle
(creating, starting, resuming, and similar) past the cluster's abandon age is
flipped to `failed`, unrecoverably. There's no grace period. The age is
measured from the sandbox's own last lifecycle change, not from the
escalation, so a resident that has been wedged longer than the abandon age
while the node drained gently is already past it and is flipped on the first
tick after the write.

Before escalating a drain that has been sitting, read both
`node_drain.sandboxes_remaining{node}` and `node_drain.drain_blockers{node}`.
Neither is a superset of what the sweep destroys: they differ in both the
lifecycles and the node-pinning column they count. Two zeros is the only
reassuring answer, and a non-zero in either is a prompt to look at the rows
rather than proof that something will be lost.

### `--no-replace-after-terminate`

With the default `aws` terminator backend, the auto-scaling group replaces the
instance after `terminate-node`.
`--no-replace-after-terminate` sets `replace_after_terminate=false` in the
request body, which persists `terminate_decrement_capacity=true` on the node
row so termination shrinks the group instead. Use it for
[blue-green rollouts](../../../../infra/space/aws/eks/modules/rollout-mng/README.md)
across two managed node groups, where the incumbent group must shrink
monotonically.

The flag is honored only on the call that starts a drain. A drain's
replacement intent is fixed for its lifetime, so passing the flag against a
node that's already draining returns 409 if it disagrees with how that drain
was started. Omit it to escalate on the existing terms, or run `undrain-node`
and start a fresh drain to change them.

## Warm pools

`warm-pool create` creates or updates a cluster warm-pool config through
`PUT /admin/api/v1/warm-pools`; `warm-pool add` is an alias. The API upserts by
resolved template release, `vcpu`, and `memory_mib`.

Omit `--content-hash` to resolve the template's current `latest` release at the
time the command runs. The stored pool config is pinned to the resolved
`template_release_id`, so later `latest` promotions do not move that config.
Pass `--content-hash` to pin an exact release explicitly.

`warm-pool update` uses the same API but requires `--template` to be an exact
content-hash template reference such as `my-template@sha256:...`; tag references
and digest prefixes are rejected before any API call.

```bash
space-admin-cli warm-pool create \
  --template my-template \
  --vcpu 2 \
  --memory-mib 8192 \
  --target-ready-slots 10 \
  --refill-per-tick 10 \
  --max-warming-slots 10
```

Use `--disabled` to write the config without allowing the reconciler to fill
it yet, and `--json` to print the raw API response.

List configs:

```bash
space-admin-cli warm-pool list
```

Show one config by UUID prefix or template reference:

```bash
space-admin-cli warm-pool get my-template
space-admin-cli warm-pool get my-template@sha256:a710c90b3897
```

### Execution inventory collection

`space-admin-cli get-node <node>` displays **Execution Collection** and
**Execution Receipt** when the node has a status row. The same evidence is
available as `status.execution_inventory` in `get-node --json` and the admin
node list/get API responses.

- `complete`: collection succeeded, possibly with an empty inventory.
- `unavailable`: collection failed; the last artifact inventory is retained.
- `legacy`: inventory arrived without the collection-outcome protocol.
- `not reported`: no receipt is available, including from older API servers.

The receipt timestamp is independent of the node heartbeat. A recent heartbeat
does not refresh execution evidence, and `complete` does not certify a required
runtime, environment readiness, or completion of an upgrade. This readout does
not change placement or provision artifacts.

Use the readout to choose the next diagnostic step:

| Evidence | Operator action |
| --- | --- |
| No receipt | Check API/node-ctl versions and whether the node has published execution inventory. Cluster-role nodes do not collect it. |
| Receipt stops advancing across expected collection ticks | Inspect node-ctl reconciliation/publication errors and connectivity to the cluster API. A recent heartbeat alone does not clear this condition. |
| Recent `unavailable` | Inspect node-manager discovery/staging and node-ctl collection errors. First-boot staging can temporarily exceed the five-second collection budget. |
| `legacy` | Check the node-ctl version; inventory exists without the collection-outcome protocol. |
| Recent `complete` | Collection succeeded. Inspect the actual inventory before diagnosing a missing runtime; completion can describe an empty inventory. |

Judge receipt age against the configured collection period (60 seconds by
default) and reconciliation delays, not the node heartbeat's stale flag. This
diagnostic has no automatic stale/ready threshold. During mixed cluster-reader
rollouts, older readers can update artifact rows without updating receipts;
finish the reader rollout before relying on receipt advancement, and never
interpret it as certification that an upgrade is applied.

### Safety

Treat shared clusters as production-sensitive. `list-nodes` and
`get-node` are read-only and safe to run. `drain-node`, `terminate-node`,
and `undrain-node` mutate cluster state — get cluster-admin signoff and a
written rollback plan before running them against prod, same as any other
destructive operator action.

## Environment variables

| Name              | Required | Purpose                                                |
|-------------------|----------|--------------------------------------------------------|
| `SPACE_API_URL`   | yes | API origin; login also accepts `--api-url` |
| `SPACE_API_TOKEN` | except login | Session bearer token |

## Exit codes

| Code | Meaning                                                                  |
|------|--------------------------------------------------------------------------|
| 0    | Success                                                                  |
| 1    | Client-side validation error, transport error, or non-2xx from the API   |

On non-2xx admin operations forward the server's error body to stderr. Login
reports only the status to avoid exposing credentials from response bodies.
