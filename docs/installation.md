# SPACE: Install and configure locally

[Documentation](../README.md)

Deploy SPACE in a local Kind cluster, publish the `seed:latest` template, and authenticate. Then continue with [Your first sandbox](first-sandbox.md) to create a microVM and run a command.

This guide covers the Kubernetes-based local setup, not a Kubernetes-free deployment.

<a id="before-you-begin"></a>

## Prepare your environment

Use an apt-based **Linux x86_64 host with KVM**. The host needs `/dev/kvm`, `/dev/net/tun`, loop devices, btrfs support, and a local Docker daemon that permits privileged containers. A VM must expose the required devices and support nested virtualization; these instructions do not run directly on macOS. Keep at least 30 GiB free on the filesystem holding Docker's data root and on the one holding your home directory, where each Kind node keeps its images under `~/.kind-data`; the bring-up warns below that, and image builds can fail part-way.

Start with Bash, Git, curl, tar, OpenSSL, Python 3.8 or newer, and `ssh-keygen`. The prerequisite installer needs root or non-interactive sudo to install system packages.

Setup uses the existing [prerequisite installer](../infra/space/kind/scripts/install-prereqs.sh), [Firecracker installer](../infra/space/kind/scripts/install-fc-vendor.sh), and [bring-up command](../infra/space/kind/scripts/up.sh). The steps below configure and run them in order.

### Use Bash and keep the session alive

If your login shell is not Bash, switch before continuing:

```bash
exec bash
```

These setup blocks use Bash syntax. Run them in the same Bash session so the exported configuration and authentication token remain available. The next guide runs a standard-library Python example that inherits those variables; it does not require a virtual environment or package installation.

For an SSH installation, start a persistent session before beginning the build. Install tmux if necessary, then start it explicitly with Bash:

```bash
sudo apt-get update && sudo apt-get install -y tmux
tmux new-session -s space-setup bash
```

Inside that session, initialize the shell:

```bash
set +eu
set -o pipefail
export PATH="$HOME/.local/bin:$PATH"
```

A new tmux session starts a fresh shell, so run the PATH export inside it. Detach with Ctrl-B, then D; after reconnecting to the same host, run `tmux attach-session -t space-setup` rather than starting another build.

At a local console, tmux is optional. In either case, run one complete block at a time and resolve failures before continuing; the checks below stop without closing your interactive shell.

## Get the source

Clone the [SPACE repository](https://github.com/perplexityai/SPACE) and enter its root directory. If you already have a checkout, enter that directory instead.

```bash
git clone https://github.com/perplexityai/SPACE.git &&
  cd SPACE
```

## Check the host

Run this block from the checkout root. It checks the platform, required devices, host tools, and Docker connectivity.

```bash
(
  set -euo pipefail

  case "$(uname -s):$(uname -m)" in
    Linux:x86_64) ;;
    *)
      printf 'Use a Linux x86_64 host with KVM, not macOS.\n' >&2
      exit 1
      ;;
  esac

  for device in /dev/kvm /dev/net/tun; do
    if [[ ! -c "$device" ]]; then
      printf 'Required device is unavailable: %s\n' "$device" >&2
      exit 1
    fi
  done

  if [[ ! -r /dev/kvm || ! -w /dev/kvm ]]; then
    printf 'The current user needs read and write access to /dev/kvm.\n' >&2
    exit 1
  fi

  if [[ ! -f infra/space/kind/scripts/install-prereqs.sh ]]; then
    printf 'Run this block from the SPACE checkout root.\n' >&2
    exit 1
  fi

  missing=0
  for tool in git curl tar openssl python3 ssh-keygen docker; do
    if ! command -v "$tool" >/dev/null; then
      printf 'Missing host tool: %s\n' "$tool" >&2
      missing=1
    fi
  done

  if (( missing != 0 )); then
    printf 'Install all missing host tools before continuing.\n' >&2
    exit 1
  fi

  docker version --format '{{.Server.Version}}' >/dev/null
  printf 'Host checks passed.\n'
)
```

These checks do not verify privileged Docker operations, loop devices, or btrfs support.

This deployment uses development passwords and signing keys. Keep it private and do not use production credentials.

## Install and verify tools

Run the prerequisite installer before bringing the stack up. It supplies Kind, kubectl, Helm, grpcurl, Tilt, Go, and Linux support tools including jq, lsof, and the AWS CLI; the bring-up does not install missing host tools.

The following also adds the installation directory to your Bash startup files. It preserves existing configuration and avoids adding the same PATH entry repeatedly.

```bash
export INSTALL_DIR="$HOME/.local/bin"
export PATH="$INSTALL_DIR:$PATH"

(
  set -euo pipefail
  mkdir -p "$INSTALL_DIR"

  login_rc="$HOME/.profile"
  for rc in "$HOME/.bash_profile" "$HOME/.bash_login"; do
    if [[ -f "$rc" ]]; then
      login_rc="$rc"
      break
    fi
  done

  path_line='case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) export PATH="$HOME/.local/bin:$PATH" ;; esac'

  for rc in "$login_rc" "$HOME/.bashrc"; do
    touch "$rc"
    if ! grep -Fxq "$path_line" "$rc"; then
      printf '\n%s\n' "$path_line" >> "$rc"
    fi
  done

  installer_status=0
  time bash infra/space/kind/scripts/install-prereqs.sh ||
    installer_status=$?

  missing=0
  for tool in kind kubectl helm grpcurl tilt go jq; do
    if ! command -v "$tool"; then
      printf 'Missing from PATH: %s\n' "$tool" >&2
      missing=1
    fi
  done

  if (( installer_status != 0 || missing != 0 )); then
    printf 'Installation is incomplete. Resolve all errors above.\n' >&2
    exit 1
  fi

  printf 'Tool checks passed.\n'
)
```

Verify that a fresh Bash login can find every tool without inheriting the current PATH. This check reports all missing tools in one pass.

```bash
env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  bash -lic '
    set +eu
    missing=0

    for tool in kind kubectl helm grpcurl tilt go jq; do
      if ! command -v "$tool"; then
        printf "Missing from PATH: %s\n" "$tool" >&2
        missing=1
      fi
    done

    if (( missing != 0 )); then
      printf "Install the missing tools before continuing.\n" >&2
    fi

    exit "$missing"
  '
```

## Build and deploy

The bring-up creates the Kind cluster, then starts [Tilt](https://tilt.dev/) in the background on the repository's `Tiltfile`. Tilt builds every image, deploys every chart, and publishes the seed template. The command returns once Tilt has nothing left to build, every workload has rolled out, `seed:latest` is published, and every node is registered and schedulable. The first build compiles the Rust workspace and builds the seed template.

Continue in the same shell, checkout, and Docker environment.

Do not configure cloud credentials for this setup. Clear any inherited AWS credentials below: with `SPACE_KIND_STORAGE_CREDENTIALS=static` the bring-up copies them from the shell into a cluster Secret, for optional integrations this guide does not use. The default mode does not copy them.

A fresh setup has three users in the Local organization: `root@local` and `deploy@local` have the `organization_admin` role, and `dev@local` has the `member` role. `root@local` can sign in to both the API and Admin Console. `deploy@local` and `dev@local` cannot sign in by default; an admin must [set their passwords](#activate-the-other-local-users-optional) to enable login.

The bring-up sets the initial `root@local` password to `password` unless `SPACE_CONSOLE_PASSWORD` is set. To choose one without putting it in shell history, run this optional command in the same shell before the deployment block:

```bash
read -rsp 'Initial root@local password: ' SPACE_CONSOLE_PASSWORD
printf '\n'
export SPACE_CONSOLE_PASSWORD
```

The bring-up does not print or retain the password in a Kubernetes Secret. It sets the password only while the account is locked; rerunning the bring-up does not change an existing password.

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

export SPACE_VENDOR_ROOT="$HOME/.local/share/space/vendor"
export SPACE_KIND_CLUSTER=space-local

(
  set -euo pipefail

  time bash infra/space/kind/scripts/install-fc-vendor.sh
  time bash infra/space/kind/scripts/up.sh
)
```

Each timed command prints its elapsed duration as `real`. While the bring-up runs, the Tilt UI at `http://localhost:10350` shows each build and rollout; `~/.space-devbox/kind/tilt-space-local.log` has Tilt's full log. When it finishes, the command prints a summary of the endpoints below.

Tilt keeps running after the command returns, and redeploys whatever changes in the checkout. Running the command again on a live stack reuses that Tilt and returns once the stack has settled; it restarts Tilt only when Tilt was started with other settings. [Remove the local deployment](#remove-the-local-deployment) stops it.

The API and data plane are reachable on loopback through the cluster's own port mappings, which spread connections across their replicas as a load balancer does; Tilt holds loopback-only port forwards to the UI and object storage:

| Service | Local port | Reached through |
|---|---|---|
| API | `8080` | cluster port mapping |
| Data plane | `8090` | cluster port mapping |
| UI | `3000` | Tilt port forward |
| Object storage | `8333` | Tilt port forward |

If either API or data-plane port refuses connections, confirm the bring-up reached its final summary; the [local installation troubleshooting steps](troubleshooting.md#local-installation) cover what to check.

When Tilt redeploys a workload, it replaces pods, which can briefly interrupt the UI and object-storage port forwards; see [Troubleshoot setup](#troubleshoot-setup) before interpreting an empty reply as an authentication problem.

<a id="configuration"></a>

## Configure your environment

Keep installation settings consistent across steps. The guide explicitly sets the first three values below; the others retain an existing environment value or use their script default.

| Setting | Value | Purpose |
|---|---|---|
| `INSTALL_DIR` | Explicitly set to `$HOME/.local/bin` | Destination for prerequisite tools. |
| `SPACE_VENDOR_ROOT` | Explicitly set to `$HOME/.local/share/space/vendor` | Firecracker and guest kernel artifacts. |
| `SPACE_KIND_CLUSTER` | Explicitly set to `space-local` | Kind cluster name; context is `kind-space-local`. |
| `SPACE_CONSOLE_PASSWORD` | Defaults to `password` when unset | Initial `root@local` login password for a locked account. |
| `SPACE_KIND_DATA_ROOT` | Defaults to `$HOME/.kind-data` when not already set | Host-side backing storage for node containerd data. This guide does not override it. |
| `SPACE_BOOTSTRAP_TIMEOUT` | Defaults to `1800` seconds when not already set | The longest each wait of the bring-up may take: Tilt's start, the first build and the API, each Job, each rollout (the Datadog Agent's included), and node publication. A positive number of seconds. Not a sandbox or command timeout. |

The examples use the following client settings. Changing a client URL does not change the server listener or the port that reaches it.

| Client setting | Local value | Purpose |
|---|---|---|
| `SPACE_API_URL` | `http://127.0.0.1:8080` | Authentication and sandbox management. |
| `SPACE_DATA_PLANE_URL` | `http://127.0.0.1:8090` | Resolving relative command-event URLs. |
| `SPACE_TEMPLATE` | `seed:latest` | Template selected when creating a sandbox. |
| `SPACE_API_TOKEN` | Returned by login | Authorization for management requests. |

Set the two URLs in this shell; the steps below and the next guide read them:

```bash
export SPACE_API_URL=http://127.0.0.1:8080
export SPACE_DATA_PLANE_URL=http://127.0.0.1:8090
```

If you change the cluster name, update the explicit Kubernetes contexts in these examples. Keep management requests and event-stream requests on their respective ports.

## Verify the installation

Running pods alone do not mean that `seed:latest` is available. The bring-up returned only after the seed publication job completed and every node-service DaemonSet rolled out; this block confirms both and checks the API. Each node service (node-manager, sandbox-manager, template-manager and the rest) runs as its own DaemonSet, as in production; `infra/space/kind/lib/node-services.sh` lists them.

```bash
(
  set -euo pipefail

  kubectl --context kind-space-local -n space get pods

  kubectl --context kind-space-local -n space \
    wait --for=condition=complete \
    job/space-seed-publish --timeout=300s

  for ds in $(bash infra/space/kind/lib/node-services.sh daemonsets); do
    kubectl --context kind-space-local -n space \
      rollout status "ds/$ds" --timeout=300s
  done

  curl --noproxy '*' --fail-with-body --silent --show-error \
    --max-time 15 \
    "$SPACE_API_URL/openapi-m1.json" |
    jq -e '.openapi and .paths' >/dev/null
)
```

Proceed when the seed job reports `condition met`, the rollout succeeds, and the API check exits successfully without output. Resolve failures before moving to authentication or sandbox creation.

## Authenticate

A fresh Kind deployment uses `root@local` and the password selected before the bring-up (`password` by default). If the password has changed since then, use the current value; redeploying does not reset an existing password.

The helper obtains a bearer token and verifies it by listing sandboxes. A failed login clears any previous token rather than silently reusing it. It uses `SPACE_API_URL` from [Configure your environment](#configuration).

```bash
set +x

authenticate() {
  if [[ -z "${SPACE_API_URL:-}" ]]; then
    printf 'Set SPACE_API_URL first; see Configure your environment.\n' >&2
    return 1
  fi

  SPACE_API_TOKEN="$(
    SPACE_CONSOLE_PASSWORD="${SPACE_CONSOLE_PASSWORD:-password}" \
      jq -nc '{email:"root@local",password:env.SPACE_CONSOLE_PASSWORD}' |
    curl --noproxy '*' --fail-with-body --silent --show-error \
      --max-time 15 \
      -H 'Content-Type: application/json' \
      --data-binary @- \
      "$SPACE_API_URL/auth/v1/sessions" |
      jq -er '.token | select(type == "string" and length > 0)'
  )" || {
    unset SPACE_API_TOKEN
    return 1
  }

  export SPACE_API_TOKEN

  curl --noproxy '*' --fail-with-body --silent --show-error \
    --max-time 15 \
    -H "Authorization: Bearer $SPACE_API_TOKEN" \
    "$SPACE_API_URL/v1/sandboxes?limit=1" |
    jq '{data, next_cursor}'
}

authenticate
```

Do not print or commit the token. Keep `SPACE_API_URL`, `SPACE_DATA_PLANE_URL`, and `SPACE_API_TOKEN` exported, then launch the Python example in [Your first sandbox](first-sandbox.md) from this shell. A new terminal needs the configuration and authentication steps again.

### Activate the other local users (optional)

After authenticating as `root@local`, choose one of the other seeded users and set its password:

```bash
# Choose one:
USER_ID=00000000-0000-0000-0000-000000000012  # dev@local
# USER_ID=00000000-0000-0000-0000-000000000011  # deploy@local

(
  set -euo pipefail
  read -rsp 'New password: ' NEW_PASSWORD
  printf '\n'
  [[ -n "$NEW_PASSWORD" ]] || { printf 'Password must not be empty\n' >&2; exit 1; }

  NEW_PASSWORD="$NEW_PASSWORD" jq -nc '{password:env.NEW_PASSWORD}' |
    curl --noproxy '*' --fail-with-body --silent --show-error \
      --max-time 15 \
      -X PATCH \
      -H 'Content-Type: application/json' \
      -H "Authorization: Bearer $SPACE_API_TOKEN" \
      --data-binary @- \
      "$SPACE_API_URL/admin/api/v1/users/$USER_ID" |
    jq '{email, role}'
)
```

Sign in at `http://127.0.0.1:3000` with the chosen account. Repeat with the other `USER_ID` if needed; repeating for the same account changes its password.

<a id="shim-telemetry-selectors"></a>

## Select shim telemetry

The Go shim keeps DogStatsD metrics by default (`SPACE_METRICS_SINK=auto` or
`datadog`). Set `SPACE_METRICS_SINK=off` to disable metrics, or `otlp` to export
delta counters and exponential latency histograms over plaintext gRPC. OTLP
requires `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` (falling back to
`OTEL_EXPORTER_OTLP_ENDPOINT`), for example `http://collector:4317`; there is no
metrics endpoint default. The protocol must be `grpc`, and nonblank
`OTEL_EXPORTER_OTLP_METRICS_HEADERS` or `OTEL_EXPORTER_OTLP_HEADERS` are rejected.
A configuration that cannot work fails the shim's startup, naming the setting
that is wrong and never its value. Only an explicit `otlp` reaches those
checks: `auto` means DogStatsD in every shim build.

Metrics and traces share identity settings, resolved `SPACE_*` first, then
OpenTelemetry, then Datadog: `SPACE_SERVICE_NAME`, `SPACE_ENV` and
`SPACE_VERSION` outrank `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`,
which in turn outrank `DD_SERVICE`, `DD_ENV` and `DD_VERSION`. Existing metric
tag keys remain point attributes. Tracing is selected independently with
`SPACE_TRACE_EXPORTER=auto|datadog|otlp|off`. Set these variables in the shim
process environment; the Kind wrapper sets both selectors from its telemetry
mode.

### Go runtime metrics

`SPACE_METRICS_SINK=otlp` also exports Go runtime metrics, to the same endpoint
and resource as the `space.shim.*` metrics, under OpenTelemetry's standard
`go.*` names:

| Instrument | Meaning |
|---|---|
| `go.memory.used` | memory used by the Go runtime, split by type |
| `go.memory.limit` | the configured memory limit, when one exists |
| `go.memory.allocated` | bytes allocated to the heap |
| `go.memory.allocations` | count of heap allocations |
| `go.memory.gc.goal` | heap size target for the end of the GC cycle |
| `go.goroutine.count` | live goroutines |
| `go.processor.limit` | OS threads that can run Go code simultaneously |
| `go.config.gogc` | the configured heap growth percentage |
| `go.schedule.duration` | time goroutines spent runnable before running |

`go.memory.limit` appears only when `GOMEMLIMIT` is set. The runtime metrics
are cumulative, and `go.schedule.duration`, which comes from the
instrumentation's own producer, arrives as a **cumulative explicit-bucket**
histogram, while the `space.shim.*` histograms are delta exponential.

Every shim process reports under its own `service.instance.id`,
`<node>:<pid>`, where the node is `SPACE_K8S_NODE_NAME`, then `KUBE_NODE_NAME`,
then the host name. A Collector's delta-to-cumulative conversion needs one
writer per stream, so concurrent shims must not share an identity; aggregate a
node's shims in the query instead, for example with
`sum without (service_instance_id)`. The runtime metrics leave on a separate
provider with **cumulative** temporality, so the process's own state never
passes through that conversion. Over OTLP, latency histograms drop the `_ms`
their DogStatsD names carry, since the unit is declared
(`space.shim.operation.latency`, unit `ms`), and the `service`, `env` and
`version` tags are left to the resource. DogStatsD names and tags are
unchanged.

**This is narrower than Datadog's runtime metrics v2.** That route also reports
GC cycle counts, GC pause distributions, CPU time split by class, and mutex
wait time. The OTLP route emits none of those, and this release does not add
instruments to close the gap. A dashboard or monitor built on
`runtime.go.metrics.*` for those four families has no OTLP equivalent yet.

Two configurations are worth knowing about:

- **Duplication.** With metrics on `otlp` and `SPACE_TRACE_EXPORTER=datadog` or
  `auto`, the native Datadog tracer keeps sending its own
  `runtime.go.metrics.*` series to the Agent while these arrive over OTLP, so
  the same runtime data lands in both backends. Set both selectors to `otlp` to
  avoid it.
- **Loss.** With metrics on `datadog` and `SPACE_TRACE_EXPORTER=otlp` or `off`,
  **no runtime metrics are emitted at all**: the OTLP route is not active, and
  the Datadog tracer that would otherwise emit them is not running.

<a id="if-setup-fails"></a>

## Troubleshoot setup

Stop at the first failed command. [Local installation troubleshooting](troubleshooting.md#local-installation) covers missing tools, pod logs, authentication, and port forwards.

An empty reply can mean a replica was replaced mid-request or a port forward is reconnecting, not that the credentials are wrong. Before retrying a mutation, inspect its outcome and keep the original request body and idempotency key.

## Remove the local deployment

Teardown stops the Tilt watcher for this cluster automatically, whether the bring-up or a smoke test started it. If you started `tilt up` yourself in the foreground, stop that session with Ctrl-C first.

The script removes the cluster and its workloads while preserving host-side image caches, vendor files, seed artifacts, and the generated SSH keypair. Run it from the checkout and confirm that `space-local` no longer appears in the cluster list.

```bash
SPACE_KIND_CLUSTER=space-local \
  bash infra/space/kind/scripts/teardown.sh &&
  unset SPACE_API_TOKEN &&
  kind get clusters
```

## Volume-manager identity and credentials

The worker image provides the `space-volmgr` account (UID/GID 964) and
credential-client group `space-volcredclients` (GID 965). The Kind
volume-manager DaemonSet runs volume-manager as `space-volmgr` with
credential-manager's volume socket, the production shape; node-manager's btrfs
send-lease RPCs authorize only that identity, so deferred btrfs-send capture
depends on it. Adding these accounts to an existing direct node does not change
the running identity or migrate retained files.

Credential-manager supports an optional `--volume-socket
/space/run/volume-credentials/credmgr.sock` listener. It admits only the
volume-manager identity and only `ExchangeToken`, `IssueDek`, and `UnwrapDek`.
Certificate issuance and other privileged credential methods are denied before
handling or forwarding the request. Both the node agent and local authority
support this listener; existing sandbox-manager and template-manager clients
continue to use the standard listener.

To prepare the additional listener:

- Provision `/space/run/volume-credentials` with mode `0750`, owned by the
  credential-manager account `space-sbxmgr` (UID 950) and group 965. Give
  credential-manager and volume-manager that supplementary group and mount the
  directory into both services. The socket is mode `0660`. Parent directories
  must be root-owned and not writable by group or others; only `/space` may be
  a root-owned symlink, with the same protections on its target's ancestors.
- Give the directory one initializer. Preserve credential-manager's identity
  and standard socket directory ownership while older launchers remain in use.
- Deploy compatible binaries before enabling `--volume-socket`. Before
  advertising passive readiness or taking over from a running predecessor,
  credential-manager checks that the path is safe and distinct from the standard
  endpoint, then binds a temporary socket with the production permissions.
  Bad provisioning rejects the replacement while the predecessor keeps serving.
  The probe leaves existing sockets untouched.
- Verify both real listeners after takeover. Preflight does not cover changes
  made after its check, an upstream outage, or a rollout that stops the old
  instance first. A killed preflight can leave a `.p-*` probe socket; remove
  that file only after confirming its creating process has exited.
- Exercise token exchange and transfer-key issue/unwrap as UID 964, verify
  privileged methods are denied, and check sandbox-manager's UID-950 calls still work.
  Check the configured peer-process verification mode as well as pod readiness.

Shared restore and fork directories use `space-cache-rw` (GID 952). Run both
sandbox-manager and volume-manager with umask `0022`; restrictive masks can
leave restore parents or downloaded files inaccessible across the identities.
Validate retained transfers, snapshot restore, and fork with the intended image
and filesystem settings before changing identities.

Volume-manager uses one account, `space-volmgr`, and
`SPACE_CREDMGR_ENDPOINT=unix:///space/run/volume-credentials/credmgr.sock`.
It is denied access to the standard listener. This executable does not migrate
nodes from another service account: complete any ownership transition with
compatible earlier tooling before upgrading, preserving the service-lock inode
and retained data.

If volume-manager still runs as `space-sbxmgr` (UID 950), use a compatible earlier
executable and follow `docs/installation.md`, section `Retained-state migration
commands`, from the [pre-removal revision](https://github.com/perplexityai/SPACE/commit/671431592d2a9eb6a6e34236bf24d30601ecdd31).
Run `--prepare-volume-identity separate` before adopting this release of either
volume-manager or credential-manager. Stop new transfer admission and drain
queued pulls whose destinations are writable only by the old account before
stopping the predecessor. Start and validate the UID-964 service on that earlier
release, including its restricted endpoint and newly bound UID-964 socket,
before adopting the new executables.

On Unix listeners, credential-manager no longer treats UID 950 as a volume-manager
storage actor: both the node agent and authority deny `ExchangeToken` regardless
of the optional process-inspection setting. Sandbox-manager's certificate,
network-resolution, and data-key permissions are unchanged; process-inspection
enforcement on the authority's Unix listener additionally rejects the
volume-manager executable under that account.

### Startup after identity cutover

The identity-handoff command and journal engine have been removed.
`--prepare-volume-identity` and `--expected-volume-identity` are rejected,
including the bare preparation flag; there is no replacement migration mode.
Normal startup still acquires the existing service mutex before accessing the
database and processing transfers. It neither changes its own Unix identity
nor reassigns retained files to a different account.

Update launchers before adopting this executable: remove the handoff invocation
entirely, not just its mode argument. On fresh nodes, provision
`/space/run/volume-manager` as `space-volmgr:space-sbxclients` with mode `0750`.
The daemon creates its service mutex, database, socket, and missing staging
directory itself. New service mutexes are owner-only without changing the
service's normal umask; existing mutex inodes, contents and permissions are
preserved. If provisioning creates
`/space/cache/firecracker/.volmgr-staging` in advance, use
`space-volmgr:space-cache-rw` with mode `0750`.

Preserve the ownership, bytes, and lock inode of existing state. Do not substitute
recursive `chown`, recreate `service.mutex`, or attempt to repair retained state
during ordinary startup.

A privileged deployment preflight must refuse the upgrade if any pending
identity-handoff record remains at
`/space/run/.volume-manager-identity/pending.json`. Stop competing initializers
and complete that handoff with the compatible earlier helper before continuing;
do not delete or rewrite the record to bypass recovery. The new executable does
not read, resume, or clear old identity journals, and creates no new ones.

`--check-volume-credentials` remains available. Run it with the actual service
identity, groups, and endpoint: its invalid requests check privileged-method
denial and transfer-key admission under a 20-second timeout without minting
secrets. Real credential operations, retained transfers, encrypted and
unencrypted restore, and fork remain deployment checks.

A software rollback must keep UID/GID 964, its client groups, and the restricted
credential endpoint. Use an earlier release compatible with that configuration,
not an ownership transition back to the shared account.
