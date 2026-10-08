# SPACE: Install and configure locally

[Documentation](../README.md)

Deploy SPACE in a local Kind cluster, publish the `seed:latest` template, and authenticate. Then continue with [Your first sandbox](first-sandbox.md) to create a microVM and run a command.

This guide covers the Kubernetes-based local setup, not a Kubernetes-free deployment.

<a id="before-you-begin"></a>

## Prepare your environment

Use an apt-based **Linux x86_64 host with KVM**. The host needs `/dev/kvm`, `/dev/net/tun`, loop devices, btrfs support, and a local Docker daemon that permits privileged containers. A VM must expose the required devices and support nested virtualization; these instructions do not run directly on macOS.

Start with Bash, Git, curl, tar, OpenSSL, Python 3.8 or newer, and `ssh-keygen`. The prerequisite installer needs root or non-interactive sudo to install system packages.

Setup uses the existing [prerequisite installer](../infra/space/kind/scripts/install-prereqs.sh), [Firecracker installer](../infra/space/kind/scripts/install-fc-vendor.sh), and [bootstrap script](../infra/space/kind/scripts/bootstrap.sh). The steps below configure and run them in order.

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

Run the prerequisite installer before bootstrap. It supplies Kind, kubectl, Helm, grpcurl, Tilt, Go, and Linux support tools including jq; bootstrap does not install missing host tools.

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

The first build compiles the Rust workspace and builds the seed template.

Continue in the same shell, checkout, and Docker environment.

Do not configure cloud or telemetry credentials for this setup. Clear any inherited values below: bootstrap otherwise copies AWS and Datadog credentials from the shell into cluster secrets.

A fresh setup has three users in the Local organization: `root@local` and `deploy@local` have the `organization_admin` role, and `dev@local` has the `member` role. `root@local` can sign in to both the API and Admin Console. `deploy@local` and `dev@local` cannot sign in by default; an admin must [set their passwords](#activate-the-other-local-users-optional) to enable login.

Bootstrap sets the initial `root@local` password to `password` unless `SPACE_CONSOLE_PASSWORD` is set. To choose one without putting it in shell history, run this optional command in the same shell before the deployment block:

```bash
read -rsp 'Initial root@local password: ' SPACE_CONSOLE_PASSWORD
printf '\n'
export SPACE_CONSOLE_PASSWORD
```

Bootstrap does not print or retain the password in a Kubernetes Secret. It sets the password only while the account is locked; rerunning bootstrap does not change an existing password.

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
unset DD_API_KEY EPPO_SDK_KEY

export SPACE_VENDOR_ROOT="$HOME/.local/share/space/vendor"
export SPACE_KIND_CLUSTER=space-local

(
  set -euo pipefail

  time bash infra/space/kind/scripts/install-fc-vendor.sh
  time bash infra/space/kind/scripts/bootstrap.sh
)
```

Each timed command prints its elapsed duration as `real`. Bootstrap builds and publishes the seed template, deploys the cluster workloads, and starts loopback-only port forwards:

| Service | Local port |
|---|---|
| API | `8080` |
| Data plane | `8090` |
| UI | `3000` |
| Object storage | `8333` |

If either API or data-plane port refuses connections, confirm bootstrap reached its final summary. The [local installation troubleshooting steps](troubleshooting.md#local-installation) show how to restart a missing port forward without redeploying the cluster.

Tilt is optional for this API walkthrough, but some smoke tests start it automatically in the background. Starting Tilt can replace pods and briefly interrupt their local port forwards; see [Troubleshoot setup](#troubleshoot-setup) before interpreting an empty reply as an authentication problem.

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
| `SPACE_BOOTSTRAP_TIMEOUT` | Defaults to `600` seconds when not already set | Bootstrap's per-release wait, not a sandbox or command timeout. |

The examples use the following client settings. Changing a client URL does not change the server listener or its port forward.

| Client setting | Local value | Purpose |
|---|---|---|
| `SPACE_API_URL` | `http://127.0.0.1:8080` | Authentication and sandbox management. |
| `SPACE_DATA_PLANE_URL` | `http://127.0.0.1:8090` | Resolving relative command-event URLs. |
| `SPACE_TEMPLATE` | `seed:latest` | Template selected when creating a sandbox. |
| `SPACE_API_TOKEN` | Returned by login | Authorization for management requests. |

If you change the cluster name, update the explicit Kubernetes contexts in these examples. Keep management requests and event-stream requests on their respective ports.

## Verify the installation

Running pods alone do not mean that `seed:latest` is available. Wait for the seed publication job and node-stack rollout before creating a sandbox.

```bash
export SPACE_API_URL=http://127.0.0.1:8080
export SPACE_DATA_PLANE_URL=http://127.0.0.1:8090

(
  set -euo pipefail

  kubectl --context kind-space-local -n space get pods

  kubectl --context kind-space-local -n space \
    wait --for=condition=complete \
    job/space-seed-publish --timeout=300s

  kubectl --context kind-space-local -n space \
    rollout status ds/space-node-stack --timeout=300s

  curl --noproxy '*' --fail-with-body --silent --show-error \
    --max-time 15 \
    "$SPACE_API_URL/openapi-m1.json" |
    jq -e '.openapi and .paths' >/dev/null
)
```

Proceed when the seed job reports `condition met`, the rollout succeeds, and the API check exits successfully without output. Resolve failures before moving to authentication or sandbox creation.

## Authenticate

A fresh Kind deployment uses `root@local` and the bootstrap-selected password (`password` by default). If the password has changed since bootstrap, use the current value; redeploying does not reset an existing password.

The helper obtains a bearer token and verifies it by listing sandboxes. A failed login clears any previous token rather than silently reusing it.

```bash
set +x

authenticate() {
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
checks: `auto` means DogStatsD in every shim build. The shim does not support
Rust's `journald` metrics sink.

Metrics and traces share identity settings, resolved `SPACE_*` first, then
OpenTelemetry, then Datadog: `SPACE_SERVICE_NAME`, `SPACE_ENV` and
`SPACE_VERSION` outrank `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`,
which in turn outrank `DD_SERVICE`, `DD_ENV` and `DD_VERSION`. Existing metric
tag keys remain point attributes. Tracing is selected independently with
`SPACE_TRACE_EXPORTER=auto|datadog|otlp|off`. Set these variables in the shim
process environment; the Kind wrapper sets both selectors from its telemetry
mode.

### Go runtime metrics

`SPACE_METRICS_SINK=otlp` also exports Go runtime metrics through the same
provider and resource as the `space.shim.*` metrics, under OpenTelemetry's
standard `go.*` names:

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

`go.memory.limit` appears only when `GOMEMLIMIT` is set. `go.schedule.duration`
comes from the instrumentation's own producer, which bypasses the sink's
temporality and aggregation selectors, so it arrives as a **cumulative
explicit-bucket** histogram while the `space.shim.*` histograms are delta
exponential.

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

An empty reply can mean a port forward is reconnecting, not that the credentials are wrong. Before retrying a mutation, inspect its outcome and keep the original request body and idempotency key.

## Remove the local deployment

Teardown stops the Tilt watcher for this cluster automatically, including one started in the background by a smoke test. If you started `tilt up` yourself in the foreground, stop that session with Ctrl-C first.

The script removes the cluster and its workloads while preserving host-side image caches, vendor files, seed artifacts, and the generated SSH keypair. Run it from the checkout and confirm that `space-local` no longer appears in the cluster list.

```bash
SPACE_KIND_CLUSTER=space-local \
  bash infra/space/kind/scripts/teardown.sh &&
  unset SPACE_API_TOKEN &&
  kind get clusters
```

## Separating volume-manager's credential identity

The worker image provides a separate `space-volmgr` account (UID/GID 964) and
credential-client group `space-volcredclients` (GID 965). The local launcher
continues to run volume-manager as `space-sbxmgr` (UID 950), using the standard
credential socket. Adding these accounts does not change the running identity
or migrate retained files.

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
  privileged methods are denied, and check legacy UID-950 calls still work.
  Check the configured peer-process verification mode as well as pod readiness.

Shared restore and fork directories use `space-cache-rw` (GID 952). Run both
sandbox-manager and volume-manager with umask `0022`; restrictive masks can
leave restore parents or downloaded files inaccessible across the identities.
Validate retained transfers, snapshot restore, and fork with the intended image
and filesystem settings before changing identities.

Changing an existing node's identity requires separate migration tooling and
deployment orchestration. Stop and drain the predecessor, preserve and acquire
its existing service-lock inode, and transfer only volume-manager's retained
state. Change the identity and credential endpoint together; use the same safe
handoff for rollback. A running volume-manager under UID 964 must use
`SPACE_CREDMGR_ENDPOINT=unix:///space/run/volume-credentials/credmgr.sock` and is
denied access to the standard listener.

### Retained-state migration commands

The volume-manager executable provides two explicit deployment commands:

- `--check-volume-credentials` checks the configured listener using invalid
  requests under a 20-second timeout. Run it with the target service identity,
  groups, and endpoint. It verifies privileged-method denial and transfer-key
  method admission; real token exchange and key round trips remain separate
  deployment gates.
- `--prepare-volume-identity legacy|separate` is a Linux, root-only retained-state
  handoff. It requires `openat2`, acquires the existing volume-manager service
  lock, and preserves its inode and the database, journals, and staged upload
  bytes. `--service-mutex-timeout-secs` bounds lock acquisition. Custom database,
  socket, and staging paths and simultaneous probe mode are rejected.
  `--expected-volume-identity legacy` additionally refuses a different retained
  identity or an in-progress identity change while holding the lock.

Neither command runs during normal service startup or changes the launcher's
selected identity automatically.

The helper records checked inodes and both identities in the root-only
`/space/run/.volume-manager-identity` directory before changing ownership.
An interrupted handoff can be retried with either identity. Preserve this
record, the database and its journals, staging files, and `service.mutex`;
never delete the lock or use recursive ownership repair. Unknown or unsafe
entries are rejected before directory access changes. A path replaced during
handoff fails closed and requires inspection of the retained original.

The installation root may be one administrator-owned symlink with an absolute
target. Its ancestor chain must be protected from service users and groups;
additional symlinks and access ACLs are rejected. Private retained paths must
not cross mounts. Both shared cache parents must be root-owned and protected
against service renames: if group- or world-writable, they must already have the
sticky bit. Existing unprotected cache namespaces require a provenance audit or
node replacement; adding permissions now cannot authenticate retained files.
Existing root-owned service locks without a recovery record
require an administrator to verify their provenance and correct ownership
without replacing their inode.

Deployment preparation must install the compatible executable and ownership
initializer while the selected identity remains `legacy`. Keep that same
initializer and helper image in the rollback release; rolling back to recursive
ownership changes can bypass journal and retained-state checks.

For an identity transition, require all of the following:

1. Verify the restricted listener and compatible sandbox-manager on each target
   node, including replacement nodes, using real credential calls and file access.
2. Stop new transfer admission and drain old queued pulls: their destination
   directories may still be writable only by the legacy identity.
3. Stop volume-manager before acquiring its preserved service lock. Use a
   delete-first deployment; a new initializer waiting on a live predecessor can
   deadlock with a rollout waiting for the new process to become ready.
4. Change retained-state ownership, service identity, groups, and credential
   endpoint together. Select the switch explicitly for the prepared pool and
   validate one canary before widening it.
5. Exercise retained transfers, encrypted and unencrypted restore, and fork.
   Verify both services' umask and downloaded-file access.
6. For rollback, stop the separate-identity process and use the same helper in
   `legacy` mode under the same lock before starting the old identity. Preserve
   compatible receiver services throughout the rollback window.
