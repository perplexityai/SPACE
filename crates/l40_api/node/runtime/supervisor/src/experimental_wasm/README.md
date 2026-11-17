# Experimental Wasmtime runner

The optional `experimental-wasm` feature adds a dedicated, single-request mode:
`space-runtime-supervisor-svc --experimental-wasm-config CONFIG`.
It does not register a runtime kind, API listener, scheduler backend, or fleet
provider. Existing supervisor behavior is unchanged without the flag.

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
Operations are `capabilities`, `create`, `inspect`, `stop`, `start`, `upload`,
`download`, `execute`, and `delete`.
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
These are quiescent operations under the runner's single-flight lock, not
concurrent cancellation, VM suspension, or durable control-plane reconciliation.
The storage is still an operator-provided experimental root, not Volume Manager
storage. These additions enforce more of the platform contract without claiming
a registered production backend.

## Platform integration boundary

| Platform concern | Current experimental behavior | Required before qualification |
| --- | --- | --- |
| Capability admission | Uses shared `RuntimeCapabilities` and `StartupPolicy`; rejects unsupported requirements before sandbox allocation | Scheduler and API select a registered backend using those requirements |
| Lifecycle | UUIDv7 create retries, atomic directory publication, inspect, quiescent stop/start, idempotent delete | Persist intent through the owning desired-state store and existing reconciliation path; recover after node loss |
| Execution | Single flight, bounded deadline/output, process-group cleanup | Per-sandbox resource accounting, concurrent cancellation, restart adoption and isolation review |
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
