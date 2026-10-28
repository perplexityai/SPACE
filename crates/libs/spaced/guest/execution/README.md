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
- `ExecutionService::process_execution_info()` reports the service instance and
  implemented process capabilities before starting work. Clones share an instance;
  a newly constructed service has a new one. Guest readiness forwards the identity
  and supported capabilities through host and manager metadata; older peers omit it. Memory
  snapshots preserve it, so handles must also identify their sandbox. Discovery is
  only an observation: it does not yet fence requests or make mutations retry-safe.
- `ExecutionServiceConfig::process_retention` independently sets completed-exit,
  completed-output and completed-start idempotency windows. Defaults preserve
  guest behavior: 15 minutes for exits, 60 seconds for output and idempotency.
  Records are lost on restart and the record that completed first is evicted at
  capacity. Output/environment retention ends no later than record retention;
  retaining output longer keeps its share of the shared output budget. A long
  idempotency window lowers the sustained keyed-start rate the fixed key store
  admits. In-progress starts keep their fixed 60-second bound regardless of these
  windows. Each configured window must be nonzero and at most seven days, and
  output retention must not exceed completed-record retention.
  A key may outlive output or the record: a keyed Start retry after output expiry
  returns Started followed by a lagged `ResourceExhausted`; after record expiry
  it returns `FailedPrecondition` (process replay state expired). Neither outcome
  starts a replacement process.
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
- `ConnectProcessRequest::resume_after_output_gap` opts into explicit loss
  reports followed by retained output and the exit. `ProcessOutputGap` names the
  next cursor, skipped events and cumulative bytes before that cursor. It also
  applies to gaps that arise after connecting and takes precedence over
  `resume_from_oldest_retained` for reporting loss. When backpressure is enabled
  and the primary reader position is free, a stale gap reader holds the oldest
  retained output before reporting the gap. Observers do not displace a primary
  reader and can encounter further gaps. Callers that omit the option keep
  existing reconnect behavior. Cursor-based opt-ins require an affirmative first
  `Connected` acknowledgement; upstream errors are propagated. Cursorless
  completed replay retains its Started/Exit shape without claiming capability
  support. Ordinary connections remain available to older guests.
- By default, a completed process stays connectable for 15 minutes after it exits, so a
  client that reattaches still reads its exit, and the process list includes it
  with its exit when asked to (`include_exited`); by default it lists running
  processes only. A list request can name one `process_id`. The window is
  measured on the guest's monotonic clock, so time the sandbox spends suspended
  does not count. Starts beyond the 128-record limit evict the oldest completed
  records first, which can end it early. The record keeps its output only for
  the default output window (60 seconds), then just its command and exit,
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

Process-retention configuration is a separate extension described in the
[process recovery RFC](../../../../../docs/space/rfcs/2026-10-25-process-recovery.md).
It is not required to host the execution service under the embedding contract.

The proposed support profile, ownership contract, and implementation acceptance
criteria are in the [embedding RFC](../../../../../docs/space/rfcs/2026-10-06-guest-execution-embedding.md).
It is a proposal; the requirements above describe current behavior.

## Recovery starts

Native hosts can opt into request-bound starts using `StartRequest.recovery`.
Recovery starts are disabled at construction. The host first calls
`configure_process_recovery(Some(activation))` with a `ProcessRecoveryToken`
supplied from outside any memory snapshot, then obtains `instance_id`,
`activation_id` and `idempotency_retention` from `process_execution_info()`.
The request uses contract version 1, both expected IDs, a required idempotency
key and the command inside `RecoveryStart`; the legacy `command` stays empty.

Same-key retries replay the original launch before account lookup, mutable
configuration or caller-assigned process-ID collision checks. A different
supplied request is `InvalidArgument`; mixing legacy and recovery starts under
one key is `FailedPrecondition`. UUID spelling and environment map order are
immaterial, but omitted
values and explicit defaults are distinct. Recovery environment entries must be
valid; legacy sanitization is unchanged. Only a fixed-size digest is retained,
not another copy of the command or environment. Recovery `Started` events carry
the engine instance and a process incarnation, preventing a retained key from
attaching to a later reuse of the same process ID.

Before capturing memory, await `configure_process_recovery(None)`. This waits
for admitted recovery starts to publish their outcomes, including starts whose
callers disconnected. If it fails or is cancelled, do not take the snapshot.
After restoring memory, install a fresh externally supplied activation before
accepting recovery requests. Repeating an activation without a restore is
idempotent. A quiesced service returns `Unavailable` with
`temporarily_unavailable`; a different installed activation returns
`FailedPrecondition` with `activation_changed`. Cancelling configuration leaves
recovery disabled until another configuration completes. Legacy calls are
unaffected by this gate. Discovery lists implemented contracts; an absent
activation means recovery is not ready to accept calls.

Keep the original key, request and expected IDs when retrying, and bound retries
from the first attempt by the advertised retention window. Output and process
records can expire or be evicted before the key; a retry then reports the
existing lagged-output or expired-replay error instead of starting again.
Changing an expected ID or key after an uncertain result can run new work.
These tokens are lifetime fences, not authorization credentials.

The sandbox-manager and host forward recovery requests without converting them
to legacy requests or implicitly retrying starts or mutations. Discover support
through sandbox-manager readiness before choosing this encoding. The host
advertises recovery only after the manager confirms activation under snapshot
exclusion. Node-manager supplies the current execution identity at host launch;
restore changes it, while a sidecar restart or resident pause/resume retains it.
Older peers and hosts without activation configuration do not advertise support.
An older service sees no legacy command and rejects the request before launching.
Legacy calls keep their existing defaults. Ordinary readiness observes the guest
first, then gives recovery rearming at most 100 ms of the remaining budget. A
busy snapshot lock or failed rearm removes recovery capabilities from that
response without failing readiness. Snapshot preparation waits briefly for a
concurrent rearm rather than refusing solely because it holds the lifecycle
permit.

Recovery requests validate the external activation before waking a resident
paused sandbox. They never restore a suspended sandbox on behalf of an old
handle: restore must complete first, followed by fresh discovery. A quiesced
activation is temporarily unavailable, not evidence of a new lifetime.

## Fenced process handles

Native `ConnectProcess`, `SendInput`, `CloseStdin`, `SendSignal` and `ResizePty`
accept a version-1 `RecoveryHandle` in `recovery`, with the legacy `process_id`
left empty. Keep the engine instance and activation from discovery, and the
process ID and incarnation returned by recovery Start. The caller also binds
these values to the sandbox being addressed.

Each call checks both lifetime fences and the selected record's incarnation
before replay or effects. A missing record remains `NotFound`; a changed engine,
activation or incarnation is `FailedPrecondition`, with a machine-readable
`space-process-recovery-reason` exposed by `RecoveryReason::from_status`.
The selected record remains owned through the operation, so concurrent eviction
and process-ID reuse cannot redirect it to a replacement process. A new activation
retires handles for processes started under the previous activation; replacing
only the token in an old handle cannot renew it.

Disabling recovery interrupts blocked recovery controls before waiting for their
leases; it does not wait for read streams or legacy calls. Version 1 adds fencing
only: a cancelled write may have delivered a prefix, and input or signals must
not be automatically retried. Version-2 mutation receipts are described below.
The routed path advertises `process_recovery_handles` only after activation;
reconnect and control requests preserve the complete handle end to end.

## Sequenced process mutations

With `process_mutation_receipts`, native `SendInput`, `CloseStdin` and
`SendSignal` accept a version-2 `RecoveryHandle` with a nonzero `sequence`.
Use one ordered sequence per handle, starting at 1, shared across those three
methods. Resolve each result before submitting the next sequence. Reconnect and
PTY resize continue to use version 1 without a sequence. Older implementations
reject version 2 before effects; never fall back to an unsequenced request after
an uncertain result.

A duplicate with the same operation and payload waits for its owner or replays
its terminal success or error, including after process exit. A conflicting
payload is `InvalidArgument`. An out-of-order sequence or an evicted receipt is
`FailedPrecondition`, with a typed `RecoveryReason`. The last 64 terminal receipts
stay with the process record; eviction never resets its high-water mark. Each
receipt stores a fixed-size digest and bounded outcome, not stdin bytes.

All sequenced calls, including duplicate waiters, hold payload admission. Once
an effect begins, its tracked owner keeps capacity and the activation lease
until the outcome is published, even if the caller drops its future. Quiescing
recovery cancels blocked effects and waits for their receipts before completing.
A caller deadline alone does not cancel the owner. Receipt waiters do not hold an
activation lease. Service shutdown also interrupts these effects.

Interrupted input reports `Aborted` with `InputOutcomeUncertain` and the known
written prefix in `space-process-input-written`. This counts bytes accepted by
the writer, not bytes consumed by the child. Do not resend the remainder.
An owner lost after beginning records `MutationOutcomeUnknown`; retries preserve
that uncertainty. Signals acknowledge OS delivery attempts, not handling by the
child. An I/O failure before any byte was written is a known failure, preserving
its status metadata, with diagnostic messages bounded to 1024 bytes. Interrupted
system calls yield before retrying. Cancellation before any effect is a temporary
refusal and does not consume the sequence. Once an unbuffered write accepts all
bytes, its result is applied even if cancellation follows. Version-1 fenced input
uses the same prefix reporting; closing its pipe is either not begun or applied,
never a sequence-only unknown outcome.

A next-sequence signal cannot overtake a pending input. To terminate a process
with blocked input, use version-1 fenced `SendSignal(SIGKILL)` without a sequence,
or the legacy kill operation where only a legacy ID is available. These controls
remain outside sequence ordering; callers must coordinate any mixing. Routed
mutations preserve the sequence and receipt status; callers own retries and
must retain the original handle and payload.
