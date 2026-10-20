# Guest execution service

`pplx-space-spaced-guest-execution` owns the guest daemon's process and filesystem
implementation. `pplx-space-spaced-guest-server` is its production consumer and
delegates through the existing generated `Process` and `Filesystem` service
contracts. Requests, responses, streams and failures retain their protobuf/Tonic
representation. The daemon owns guest readiness, listeners, host communication,
cgroup initialization and the overall shutdown deadline.

The guest bridge and worker schemas live in `pplx-space-spaced-guest-protocol`.
`spaced-common` re-exports those types for existing callers. This service is
distinct from `execution/common`, which describes portable sandbox runtimes and
execution metadata.

## Guest process requirements

- Linux identity workers require `ExecutionServiceConfig::worker_executable`.
  Select a trusted native ELF artifact with `WorkerExecutable::open(absolute_path)`;
  the service never infers the hosting application's executable. The opened inode
  stays pinned across path replacement. The caller must protect its contents,
  loader and libraries; file validation and the protocol handshake do not establish
  trust. The caller needs read access for ELF inspection as well as execute
  permission at launch; execute-only files are insufficient without a read-access
  override. Scripts are unsupported. Linux `/proc` must be mounted.
- The daemon explicitly selects its running executable and dispatches worker mode
  before application initialization. Other hosts can build the dedicated worker
  with `cargo build -p pplx-space-spaced-guest-worker` and select the resulting
  `spaced-filesystem-worker` binary. It runs worker mode only and requires an
  execution service's inherited channel and identity configuration.
  The standalone build has telemetry disabled by default. Add `--features otel`
  (or `--features datadog`) when building the worker to enable its exporters.
  Host features and forwarded environment variables cannot enable exporters in
  a separately built worker that lacks them.
- Execution and daemon constructors return typed errors. Missing worker
  configuration fails construction; incompatible worker protocols fail startup
  without dispatching filesystem operations. IdentityWorkers on non-Linux returns
  an error; Direct remains the non-Linux default. The daemon validates its
  executable during construction: an unreadable or invalid `/proc/self/exe`
  intentionally fails startup before readiness, rather than deferring that
  failure to the first filesystem request.
- Linux identity workers require the daemon's privilege to install the requested
  OS account's groups and IDs; an unprivileged worker host is unsupported. Direct mode
  runs filesystem syscalls with the daemon's credentials; its `user` selects path
  interpretation and ownership handling, not impersonation. Neither mode creates
  a filesystem sandbox. The daemon runs inside the guest's isolation boundary.
- The daemon registers `filesystem_grpc_service()` and `process_grpc_service()`
  to retain transport payload admission as well as the execution limits. Calling
  generated traits directly does not install the transport admission layer.
- The daemon cancels the shared token and coordinates listener shutdown with draining work.
  `drain()` joins tracked work; it is not a reusable shutdown-and-reject contract
  for subsequent direct calls. Direct filesystem calls can still have effects.
- Linux PTY descriptors are created with close-on-exec atomically. Other Unix
  targets serialize PTY allocation and workload spawning through a private lock;
  unrelated spawners do not participate in that coordination.
- Process supervision tracks command leaders and their output. Termination uses
  process groups while supervision is active. Background descendants that close
  or redirect inherited output can outlive a completed leader; draining this
  service does not establish that every descendant has exited.

This crate retains the daemon contract. General application embedding requires
credential/capability policy, native admission and shutdown semantics, and
stronger descendant ownership. Explicit launching alone does not establish those
guarantees or advertise general embedding support.

The Linux `embedding` integration in the worker app uses the execution crate
without the guest server. It covers a separate worker artifact, atomic path
replacement, account-specific writes, idempotency, process output, failed startup,
cancelled callers and worker reaping. Run it as root in a disposable environment
with a provisioned `nobody` account using
`cargo test -p pplx-space-spaced-guest-worker --test embedding`.
CI assigns it to the privileged test group. Broader embedding acceptance remains
tracked by the RFC.

The proposed support profile, ownership contract, and implementation acceptance
criteria are in the [embedding RFC](../../../../../docs/space/rfcs/2026-10-06-guest-execution-embedding.md).
It is a proposal; the requirements above describe current behavior.
