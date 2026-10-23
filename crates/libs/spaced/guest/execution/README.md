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

## Rust API compatibility

This embedding API is experimental and has no Rust source-compatibility promise
across revisions. Independent consumers must pin a revision and adapt when
upgrading, including configuration literals and exhaustive error matches. The
[service versioning policy](../../../../../docs/versioning.md) does not cover Rust
crate APIs. A stable external API, including supported configuration construction
and error evolution, requires a separate crate-level policy before stabilization.

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
  `FilesystemHandle` returned by `filesystem_service()`, which implements the
  generated `Filesystem` trait and has an inherent `write_file`. Unary calls
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
- Native uploads use `ExecutionService::write_file` with
  `Request<impl Stream<Item = Result<WriteRequest, Status>> + Send>`, or the same
  method on `FilesystemHandle`. Both use the same service state.
  The stream starts with `Open`, then yields `Data` messages using the existing
  protobuf types. It shares gRPC's upload path, admission, per-message size checks
  and bounded relay. Admission precedes the first poll; EOF completes the write.
  Input errors and dropped callers use the same cancellation and partial-write
  semantics as gRPC. Borrowed streams and streams without `Unpin` are supported.
  Each native wait for input, including `Open`, has a 30-second idle timeout,
  configurable through `ExecutionServiceConfig::native_upload_idle_timeout`.
  Zero gives pending input no grace period; it does not disable the timeout.
  Config struct literals must supply this field or use `..Default::default()`.
  Expiry returns `DeadlineExceeded` and cancels the upload. Execution backpressure
  is not input idle time; gRPC retains its existing transport behavior. Callers
  can additionally bound total upload duration with `tokio::time::timeout`.
  Cancellation can retain capacity until cleanup settles and does not undo append
  effects or establish that a fresh-key retry is safe.
  After an upload reserves its idempotency key, a stalled upload also blocks
  retries for the same user and key. Each admitted retry holds its own payload
  slot while waiting, for up to the retention window.
- `configure_process_env(EnvMap)` replaces defaults for future process starts;
  request entries override them and `clean_env` skips them. Running processes
  retain their environment. `EnvMap` already enforces the shared key/value,
  entry-count and total-byte [limits](../../../guest-process/src/env/contract.rs).
  Native callers can use `EnvMap::try_from` to reject invalid maps. The daemon
  retains its wire adapter's sanitization of invalid entries before configuration.
  Native `StartRequest.env` still uses `parse_env`: invalid entries are dropped
  with a warning, rather than rejecting the start. This deliberately preserves
  the generated `Process` contract for both native and gRPC callers; transport
  selection does not change environment semantics. Callers requiring rejection
  should validate with `EnvMap::try_from` before constructing `StartRequest.env`
  (use `pplx_space_spaced_guest_protocol::proto_env` to convert the validated map).
- `close()` closes operation admission and signals cancellation across existing
  clones and filesystem handles. Execution entrypoints refuse new process,
  filesystem and environment-update calls with `Unavailable`; transport admission
  retains its earlier rejections. Uploads waiting for their first message wake
  on cancellation. The service owns a child of the supplied token: caller
  cancellation closes admission, but `close()` and `drain()` do not cancel the
  caller's token. Closure signals cancellation of admitted work immediately;
  service or parent-token cancellation can terminate identity workers immediately,
  leaving outcomes unknown. `cancellation_grace` applies to supervised work after
  request cancellation, not service shutdown. Direct execution still waits for
  outstanding I/O. This is abortive shutdown, not graceful
  admission-only closure; it neither waits nor rolls back effects. Already-admitted
  same-key retries can still wait for the owner or the retry retention deadline;
  closure does not directly wake them.
- The daemon coordinates listener shutdown with `drain()`, which first closes
  admission and then joins its tracked work. The filesystem drain still logs an
  internal timeout rather than reporting incomplete cleanup, and some detached
  or blocking work is not covered by its trackers. A complete embedding drain
  contract, including graceful admission-only closure and explicit deadline-driven
  cancellation, remains follow-up work. Returning from `drain()` is not yet proof
  that every previously admitted effect has settled.
- Linux PTY descriptors are created with close-on-exec atomically. Other Unix
  targets serialize PTY allocation and workload spawning through a private lock;
  unrelated spawners do not participate in that coordination.
- Process supervision tracks command leaders and their output. Termination uses
  process groups while supervision is active. Background descendants that close
  or redirect inherited output can outlive a completed leader; draining this
  service does not establish that every descendant has exited.
- A completed process stays connectable for 15 minutes after it exits, so a
  client that reattaches still reads its exit, and the process list includes it
  with its exit when asked to (`include_exited`); by default it lists running
  processes only. A list request can name one `process_id`. The window is
  measured on the guest's monotonic clock, so time the sandbox spends suspended
  does not count. Starts beyond the 128-record limit evict the oldest completed
  records first, which can end it early. The record keeps its output only for
  the start idempotency window (60 seconds), then just its command and exit,
  dropping its environment with the output. It closes its stdin and terminal
  descriptors when the process exits; a write still in flight then closes them
  when it returns. A caller-assigned process id stays in use for the whole
  window. Records live in daemon memory and do not survive a daemon restart.

This crate retains the running daemon contract. Complete drain semantics and
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
