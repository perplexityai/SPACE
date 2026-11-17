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
Operations are `create`, `upload`, `download`, `execute`, and `delete`.
Execution takes `sandbox_id`, `argv`, `timeout_secs` (1 through 120), and an
optional guest `env` map. Arguments are passed directly to the configured
module, never interpreted as a shell command or a host executable:

```json
{"operation":"execute","sandbox_id":"01900000-0000-7000-8000-000000000001","argv":["-c","print(6 * 7)"],"timeout_secs":60,"env":{"PYTHONHOME":"/"}}
```

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
