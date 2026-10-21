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
- Linux execution services default to `IdentityPolicy::Restricted`. This requires
  identity workers and a root launcher with effective `CAP_SETUID` and `CAP_SETGID`;
  Direct mode and insufficient launcher privileges fail construction. This
  preflight checks current authority; securebits, capability bounding sets and
  executable attributes may remove required authority across exec, in which case
  worker startup fails closed before serving operations.
  Command children and filesystem workers install the account's real, effective,
  saved and filesystem IDs and supplementary groups. They clear effective,
  permitted, inheritable and ambient capabilities and set `no_new_privs`, preventing
  set-ID and file-capability executables from granting additional authority.
  Accounts resolving to UID 0 are rejected with `FailedPrecondition` for both
  process and filesystem calls: root retains ownership and signal authority even
  without capabilities. Primary and supplementary groups, including GID 0, are
  preserved: callers must provision accounts whose group-based access is appropriate
  for the workload. Process children enter their working directory only after the
  drop, enforcing the account's access to every ancestor. The capability
  bounding set is retained as a ceiling; it grants no active capabilities.
  Only child credentials change, before worker threads or command exec. Custom
  entrypoints must dispatch worker mode before starting threads; restricted
  workers reject startup if another thread already exists.
- The daemon explicitly selects `IdentityPolicy::Guest`, retaining its existing
  account switching, root authority and executable privilege semantics. Other
  platforms retain Guest/Direct defaults. Restricted is a Linux-only policy;
  neither policy restricts reachable paths or replaces caller-provided isolation.
  Changing policy requires constructing a new execution service. Use a worker
  built with the same library: older worker protocols are rejected before dispatch.
- Native callers use the generated `Process` trait and the owned, cloneable
  `Filesystem` implementation returned by `filesystem_service()`. Unary calls
  check the existing protobuf message-size limits. Payload-bearing calls share
  the same weighted capacity with gRPC; filesystem mutations retain admission
  through owned execution and cleanup, including after caller cancellation.
  Native callers remain responsible for allocations made before calling the service.
  Oversized messages return `OutOfRange`; exhausted payload capacity returns
  `ResourceExhausted`. Use `pplx_space_common_grpc::is_refused_before_start`
  to identify refusal before execution rather than parsing error messages.
  An error without that marker does not establish that retrying is safe.
- The daemon registers `filesystem_grpc_service()` and `process_grpc_service()`
  to retain predecode protection. Handlers reuse that admission without charging
  twice. Native calls cannot install predecode protection retroactively.
  Uploads retain the generated trait's Tonic input-stream type and bounded relay.
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

This crate retains the daemon contract. Native upload input, bounded
process-environment configuration, consistent shutdown semantics and complete
operation acceptance remain follow-up work. General application embedding also
requires stronger descendant ownership. Explicit launching alone does not
establish those guarantees or advertise general embedding support.

The Linux `embedding` integration in the worker app uses the execution crate
without the guest server. It covers a separate worker artifact, atomic path
replacement, account-specific writes, idempotency, process output, failed startup,
cancelled callers and worker reaping. It checks child IDs/groups and capability
sets with and without a PTY, set-ID/file-capability privilege suppression,
root-account and launcher rejection, working-directory traversal, late worker
entrypoint rejection, and unchanged authority on every surviving host thread.
Run it as root in a disposable environment with a provisioned `nobody` account,
`CAP_NET_RAW` and `CAP_SETFCAP`, and a writable filesystem that supports executable
set-ID and file capabilities (not `nosuid`), using
`cargo test -p pplx-space-spaced-guest-worker --test embedding`.
CI assigns it to the privileged test group. Broader embedding acceptance remains
tracked by the RFC.

The proposed support profile, ownership contract, and implementation acceptance
criteria are in the [embedding RFC](../../../../../docs/space/rfcs/2026-10-06-guest-execution-embedding.md).
It is a proposal; the requirements above describe current behavior.
