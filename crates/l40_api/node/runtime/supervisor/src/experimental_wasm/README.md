# Experimental Wasmtime support

## Existing command-service adapter

The default-off `experimental-wasm` feature on `spaced-svc` selects a bounded
Python/WASI process profile inside the existing guest command service. Clients
use the existing `Process.Start`, `ConnectProcess`, `ListProcesses`, `SendSignal`
and filesystem gRPC APIs. There is no new command-execution transport.

The native agent must already run as its non-root sandbox account, without
capabilities and with `no_new_privs`, inside a per-sandbox filesystem, PID and
network containment boundary. The image's `user` account must name that same
identity; process and filesystem requests cannot switch accounts. Its launch
owner must authenticate and read-only
mount the Wasmtime executable, Python WASM module and Python library tree, and
attach the sandbox's managed writable workspace and resource limits. The adapter
does not allocate those resources or provide containment by itself.

The operator supplies the contained agent's locators:

```text
spaced-svc --wasmtime-root /runtime --wasmtime-workspace /workspace \
  --wasmtime-bridge-socket /run/space/guest.sock \
  --wasmtime-host-bridge-socket /run/space/host.sock
```

The tool-root layout is `bin/wasmtime`, `python/python.wasm`, and
`python/lib`. Process and filesystem APIs use the same contained path namespace.
Native command fallback and PTYs are refused; command environment variables are
passed only to WASI, never to the native engine. The ordinary process supervisor
retains ownership, output, idempotency, deadlines and cancellation. The agent
advertises WASI rather than Linux, omits native-host resource measurements,
and does not adopt the native guest agent's update journal.

The manual typed scenario `wasm.spaced` uses the standard gRPC socket. Set
`SPACE_SMOKE_BACKEND=deployed`, `SPACE_SMOKE_API_SURFACE=node` and
`SPACE_SMOKE_SPACED_SOCKET` to the absolute socket path. For the normal
spaced-host endpoint, also set `SPACE_SMOKE_SPACED_ENDPOINT=host`; the default
`guest` addresses the contained agent directly. `SPACE_SMOKE_SPACED_SUDO=1`
runs the local RPC client with noninteractive sudo when the host endpoint
requires root authorization. It does not disable host authorization.

The host's `wasi-python-v1` profile requires both bridge UDS paths and the
contained agent's host-visible PID and non-root UID through
`--bridge-guest-pid` and `--bridge-guest-uid`. The host checks kernel peer
credentials before dispatching guest RPCs and checks the same identity on
guest-initiated bridge calls. Its launcher must arrange socket access without
giving other sandbox identities access. An agent replacement requires fresh
launch attestation; the old PID must not be silently reused.

This scenario covers command execution, filesystem exchange, idempotent retry,
timeout recovery, disconnected background ownership, cancellation and retained
output through the existing APIs. It also checks refusal of native commands
and account switching. Its evidence covers the command-service adapter, not
sandbox creation or placement.

The shared domain and additive wire schemas can describe Wasmtime/WASI without
claiming KVM or hardware-VM isolation. No Wasmtime environment is approved in the
compiled execution catalog. API/scheduler selection, node launch, endpoint
attestation, managed volumes, gateway policy and reconciled lifecycle integration
remain required before workloads can select this runtime through the platform.
Unknown runtime/OS values fail closed on older services; default features and
existing native guest contracts are unchanged. Do not route new WASI executions
until the entire mixed-version path is qualified.

## Legacy single-request diagnostic

The optional `experimental-wasm` feature adds a dedicated, single-request mode:
`space-runtime-supervisor-svc --experimental-wasm-config CONFIG`.
It does not install or qualify a managed runtime, API listener, scheduler backend
or fleet provider. This diagnostic protocol is not a platform integration path;
new integration work belongs in the existing APIs and lifecycle owners.
Existing supervisor behavior is unchanged without the flag.

Configuration is supplied by a trusted operator:

```json
{
  "root": "/var/lib/wasm-smoke",
  "tools": "/opt/wasm-tools",
  "module": "/opt/wasm-tools/python/python.wasm",
  "preopens": {"/lib": "/opt/wasm-tools/python/lib"}
}
```

Install a pinned Wasmtime CLI at `tools/bin/wasmtime`. The configured module
must be a trusted WASI command. Additional preopens must be backed by read-only
mounts; the runner does not make writable host directories read-only. Each
sandbox gets separate writable `/workspace` and `/tmp` preopens. Host paths
and environment are otherwise not inherited, and networking is disabled.

Requests are JSON on stdin; a successful response is JSON on stdout.
Protocol version 2 operations are `capabilities`, `create`, `inspect`, `stop`,
`start`, `upload`, `download`, `upload_binary`, `download_binary`, `execute`,
`cancel`, and `delete`.
Execution takes `sandbox_id`, `argv`, `timeout_secs` (1 through 120), and an
optional guest `env` map. Arguments are passed directly to the configured
module, never interpreted as a shell command or a host executable:

```json
{"operation":"execute","sandbox_id":"01900000-0000-7000-8000-000000000001","argv":["-c","print(6 * 7)"],"timeout_secs":60,"env":{"PYTHONHOME":"/"}}
```

`capabilities` returns SPACE's shared `RuntimeCapabilities` contract. Gateway
networking, Volume Manager integration, snapshots, pause/resume, fork and warm
pools are not qualified and are all reported false. `create` accepts that same
type as `required_capabilities` and the shared `startup_policy`; unsupported
requirements are rejected before allocating a sandbox, not silently downgraded.

A caller may supply a UUIDv7 `sandbox_id` to make create retries idempotent.
Creation atomically publishes both writable directories. `stop` persists a
node-local command-admission marker and `start` removes it without losing files.
State survives separate supervisor invocations; `delete` is idempotent.
State observations reuse the shared `SandboxLifecycle` vocabulary.
Operations on one sandbox are single-flight; independent sandboxes may execute
concurrently. A busy sandbox fails admission rather than queuing. `stop` and
`delete` are quiescent operations, not VM suspension or durable control-plane
reconciliation.
The storage is still an operator-provided experimental root, not Volume Manager
storage. These additions enforce more of the platform contract without claiming
a registered production backend.

## Platform integration boundary

| Platform concern | Current experimental behavior | Required before qualification |
| --- | --- | --- |
| Capability admission | Uses shared `RuntimeCapabilities` and `StartupPolicy`; rejects unsupported requirements before sandbox allocation | Scheduler and API select a registered backend using those requirements |
| Lifecycle | UUIDv7 create retries, atomic directory publication, inspect, quiescent stop/start, idempotent delete | Persist intent through the owning desired-state store and existing reconciliation path; recover after node loss |
| Execution | Per-sandbox single flight, concurrent independent sandboxes, identity-scoped cancellation, bounded deadline/output, process-group cleanup | Per-sandbox resource accounting, restart adoption and isolation review |
| Storage | Two node-local writable preopens; deployment-mounted read-only tools | Volume Manager remains the only managed-storage authority; integrate its attachment, quota and reclamation lifecycle instead of inventing another one |
| Networking | Guest networking disabled | A mediated WASI networking/HTTP path must enforce the existing gateway's identity, destination policy, credentials and audit contracts |
| Snapshots and warm capacity | Explicitly unsupported | Define and implement backend-specific semantics before advertising any capability |
| Operations | Dedicated local protocol; deployment supplies cgroups and identity | Authenticated service integration, tenancy, observability, drain and recovery procedures |

Host sockets opened by a Wasmtime process must not be assumed to traverse a
VM-oriented gateway interception path.
Enabling WASI sockets or inheriting the host network would bypass this qualification
work rather than complete it.
Similarly, a preopened host directory is not evidence of managed-volume support.
This runner intentionally does not register itself as a schedulable runtime until
those owning platform boundaries are integrated.

## Execution ownership and cancellation

An optional UUIDv7 `execution_id` correlates `execute` with its response and
`cancel`. Cancellation bypasses that sandbox's admission lock and records a
marker for that identity, including before startup. The owning supervisor checks
it while waiting, kills and reaps its process group, and returns exit status
130 with `cancelled: true`. The cancel acknowledgment alone does not prove the
guest has exited: the caller must join the original execute request before
deleting the sandbox or acknowledging cancellation to its parent.

Cancellation never accepts a caller-supplied PID. Consumed markers are removed;
a late cancellation may leave a marker until sandbox deletion. Do not reuse
execution IDs. Stable lock files under `.locks/` survive sandbox deletion to
avoid two locks for a deleted and recreated identity. Reclaim that coordination
directory only after draining every caller of the experimental root.

Version 2 callers hold the compatibility root lock shared, excluding an older
version 1 caller that holds it exclusively. The child arms Linux parent-death
SIGKILL before executing, closing the startup race by checking its parent
afterward. Losing the supervisor therefore kills the guest instead of adopting
it. Graceful cancellation must still join the supervisor to reap the child;
crash adoption and durable execution ownership are not implemented.

The deadline includes compilation. The request, guest file size, and each
captured output stream are limited to 1 MiB. Overflow returns execution status
125, and deadline expiry returns 124. Process-group cleanup precedes file
operations. A deployment must provide memory/CPU cgroups, a non-root identity,
and read-only tool mounts. This experimental implementation is not an audited
multi-tenant security boundary.

Compiled-module caching and memory-initialization COW are disabled because
Wasmtime's generated cache and memory-image files can exceed the guest's
file-size limit. Native runtime worker pools are bounded separately from
guest code. The host's per-user process limit is not a per-sandbox PID limit.
No startup-speed or cost improvement is claimed by this configuration.

The manual typed scenario `wasm.python` exercises WASI CPython execution,
file persistence, fast output overflow, deadline enforcement, recovery, and
cleanup. Configure `SPACE_SMOKE_NODE_RUNTIME_COMMAND` with the supervisor's
argument vector and run it through the normal `space_smoke` entrypoint.

This is not Linux/OCI compatibility. Shell scripts, fork/exec, arbitrary native
packages, browsers, Docker, networking, snapshots, and concurrent requests are
not provided. Port an application's environment and verifier explicitly, then
check behavior against its original environment before routing real work.
