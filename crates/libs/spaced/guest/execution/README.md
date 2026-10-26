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
  [Payload metrics](../../../grpc-metrics/src/payload.rs) distinguish transport and
  handler admission decisions and measure slots retained after observed
  cancellation until their final release. Handler acquisition includes normal
  native calls; it must not be treated as evidence of a lost transport permit.
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
- `close()` closes admission across existing clones and filesystem handles without
  cancelling admitted work. Every new process, filesystem and environment call
  returns a refused `Unavailable`; transport admission retains its earlier
  rejections. `cancel()` closes admission and requests cancellation immediately.
  Caller token cancellation has the same effect, but the service owns a child
  token: neither method nor drain cancels the caller's token.
- `drain()` closes admission and waits up to ten seconds; `drain_with_timeout`
  accepts another duration. Success means admitted operations, physical I/O,
  worker startup/retirement, watch teardown and command supervisors have settled,
  and owned filesystem workers have been reaped. Long-lived filesystem `watch`
  streams have no natural completion: worker-backed streams end with recoverable
  `Unavailable` at close; Direct streams end quietly during drain or cancellation.
  A caller must continue polling or drop its admitted futures and consume or drop
  its read/list/process output streams; otherwise graceful drain
  can wait for their input or backpressure. Send required input, close stdin and
  send any signals before closing admission: `send_input`, `close_stdin` and
  `send_signal` are all refused afterward.
- Expiry requests cancellation and returns `IncompleteExecutionDrain`. It bounds
  waiting, not OS completion, and does not prove that effects have stopped. Keep
  the Tokio runtime alive and call drain again; concurrent and repeated drains
  observe retained ownership. Dropping a drain future keeps admission closed,
  without implicitly cancelling work. Dropping service handles or the runtime is
  not a substitute for successful drain.
- Cancellation ends interrupted read/list streams with `Unavailable` after any
  buffered results, including when the output queue is full. Partial output is
  not successful completion. Graceful close allows admitted reads and lists to
  finish. Worker watches retire with recoverable `Unavailable` at close or
  cancellation, so downstream SSE/WebSocket consumers can reconnect. Direct
  watches retire quietly during drain or cancellation. A read/list/relay producer
  that exits without reporting completion ends its stream with `Internal`.
- Cancellation wakes uploads awaiting `Open` or subsequent input. Request
  cancellation gives supervised filesystem mutations `cancellation_grace` before
  terminating stalled identity workers. Service/parent cancellation, including
  drain-deadline expiry, can terminate workers immediately without that grace;
  outcomes may be unknown. Direct execution waits for outstanding I/O, which may
  outlast any deadline. Already-admitted same-key retries can still wait for the
  owner or the retry retention deadline;
  closure does not directly wake them. Cancellation does not undo effects or make
  a fresh-key retry safe. Successful drain implies neither flush nor persistence.
- The daemon still cancels its own token before draining and retains its overall
  shutdown deadline. Wire schemas, identity policy and process exit semantics are
  unchanged; interrupted streams now report failure instead of successful EOF.
  Admission-only close is for hosts that choose a graceful wait before requesting
  cancellation.
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

This crate retains the running daemon contract. Embedders must provide isolation,
authorization, account provisioning, final descendant teardown and a Tokio runtime
with I/O, timers and process support alive through drain. Reap only your own
child PIDs (for example, with `std::process::Child::wait`), never library-owned
children or `waitpid(-1, ...)`. A caller notification-only
`SIGCHLD` handler may be installed before Tokio initializes signal handling;
Tokio chains it, and callers may also subscribe with `tokio::signal`. Keep that
handler arrangement intact while the service or its cleanup is alive. Replacing
it, ignoring or blocking `SIGCHLD`, or setting `SA_NOCLDWAIT` is unsupported.
Tokio's process-wide handler may outlive the service; signal-handler neutrality
is not promised. The Linux profile requires real identity workers and non-root
workload accounts. This is not a standalone isolation boundary or a portability
guarantee.

The Linux `embedding` integration in the worker app uses the execution crate
without the guest server. It covers a separate worker artifact, atomic path
replacement, account-specific writes, idempotency, process output, failed startup,
cancelled callers, graceful/repeated drain and worker reaping. It checks child
IDs/groups and capability sets with and without a PTY, set-ID/file-capability privilege suppression,
root-account and launcher rejection, working-directory traversal, late worker
entrypoint rejection, and unchanged authority on every surviving host thread.
It also checks real cgroup v2 placement for workloads and identity workers,
PTY/placement/worker descriptors during repeated concurrent worker and PTY
startup, and caller ownership of unrelated children through drain. Placement
handles must be opened atomically close-on-exec; `ExecutionCgroups::new` neither
validates nor changes their flags. The probe checks that precondition, a
preinstalled SIGCHLD notification handler, a Tokio signal subscriber, and
PID-specific caller reaping. A redirected descendant survives drain, then exits
when the caller tears down its workload cgroup.

Before any library work, the single-threaded test host marks launcher-inherited
descriptors above stderr close-on-exec, keeping them usable in the host. Child
probes still inspect every inherited descriptor without an allowlist, so newly
leaked library descriptors remain visible.

The concurrent descriptor check samples unrelated execs during 16 startup
rounds. It detects leaks only when an exec overlaps the inheritable window; it
does not guarantee a spawn in each round or detection of microsecond
open-then-fcntl races. Local negative controls widened the PTY and worker-socket
windows to 50 ms to verify detection, not to measure detection rates for shorter
windows.

Run it as root in a disposable container with a private cgroup v2 namespace
(`--privileged --cgroupns private`) and a writable `/sys/fs/cgroup`. Explicitly
set `SPACE_TEST_EXECUTION_PRIVATE_CGROUP_NS=1` to authorize creation and
cleanup of a test-owned subtree; the test never moves the host or kills the root
cgroup. These prerequisites apply to the whole `embedding` test, including its
existing identity suite. The environment also needs a provisioned `nobody` account,
`CAP_NET_RAW` and `CAP_SETFCAP`, and a writable filesystem that supports executable
set-ID and file capabilities (not `nosuid`), using
`cargo test -p pplx-space-spaced-guest-worker --test embedding`.
CI assigns it to the privileged test group. Library regressions also hold account
lookup and filesystem syscalls open across cancellation, assert incomplete drain,
and then release them to observe completion. They cover every closed entrypoint,
borrowed uploads and output backpressure. Kind/Firecracker covers the daemon
integration separately; privileged container tests do not replace it. Other Linux
architectures and downstream packaging require their own acceptance evidence.
The placement unit test uses kernel-authenticated sender credentials on the
placement write and an executable that cannot be loaded: placement must happen
in the forked child before exec can fail. The real cgroup probe additionally
verifies membership by start publication and inherited descendant membership.
These checks cover placement and caller teardown, not resource limits or
a complete sandbox isolation policy.

The proposed support profile, ownership contract, and implementation acceptance
criteria are in the [embedding RFC](../../../../../docs/space/rfcs/2026-10-06-guest-execution-embedding.md).
It is a proposal; the requirements above describe current behavior.
