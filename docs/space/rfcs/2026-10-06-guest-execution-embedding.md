# Guest execution embedding contract

**Status: proposal.** Reuse SPACE's process/filesystem implementation inside an
independently isolated application, without guest listeners or daemon startup.
The [execution crate README](../../../crates/libs/spaced/guest/execution/README.md)
describes implemented behavior. [Implementation coverage](#implementation-coverage)
maps the initial profile's guarantees to checks and their evidence limits.
That does not establish stable Rust APIs or validate downstream packaging.

## Decisions requested

1. **One contract:** daemon and embedding callers use `ExecutionService`, retaining
   generated service traits, protobuf values and Tonic operation errors.
2. **Initial profile:** privileged Linux hosting, provisioned non-root workloads,
   and an explicitly supplied, trusted worker-only executable.
3. **Ownership:** SPACE owns operations and their supervisors; the caller owns
   isolation, authorization, account provisioning and final descendant teardown.

Unprivileged hosting, privileged workloads, non-Linux embedding, and a new SPACE
sandbox backend require separate designs. Existing daemon support is unchanged.
Configurable process retention, output-gap recovery, retry-safe process input and
restart fencing are separate API extensions, not prerequisites for embedding.

## Before: the extracted daemon

At the proposal's baseline, the library was separated, but its production
integration supplied whole-daemon assumptions. Filesystem workers re-executed the
daemon through its early dispatch hook; native calls lacked some transport
admission checks and a complete shutdown-and-reject contract.

```mermaid
flowchart LR
    accTitle: Current guest execution integration
    accDescr: Inside a Firecracker guest, daemon listeners call ExecutionService. The daemon supplies readiness, placement and shutdown coordination. ExecutionService supervises command processes and launches filesystem workers by re-executing the daemon in worker mode.
    subgraph guest["Firecracker guest: isolation boundary"]
        subgraph daemon["Guest daemon process"]
            grpc["gRPC listeners<br/>transport admission"]
            setup["Daemon lifecycle<br/>readiness / cgroups<br/>shutdown"]
            service["ExecutionService<br/>processes + files"]
            grpc --> service
            setup -.-> service
        end
        commands["Commands / PTYs"]
        workers["Filesystem workers<br/>daemon worker mode"]
        service -->|spawn and supervise| commands
        service -->|daemon re-exec| workers
    end
```

## After: the proposed shared contract

Each deployment hosts its own service instance. Both integrations use the same
operation boundary; the embedding application has no guest-server dependency.
The daemon retains listeners, readiness and cgroup initialization. Embedding
supplies a worker-only binary and workload placement; the library never
rearranges the host's cgroup hierarchy.

```mermaid
flowchart LR
    accTitle: Proposed guest execution embedding integration
    accDescr: Within caller-provided isolation, either the daemon adapter or an embedding application calls the same ExecutionService contract. The shared service enforces admission and lifecycle rules, supervises commands and launches an explicitly configured filesystem worker executable. The caller owns final environment teardown.
    subgraph boundary["Caller-provided isolation and final teardown"]
        subgraph host["Hosting process"]
            caller["Daemon adapter<br/>OR embedding app"]
            core["ExecutionService<br/>shared admission<br/>and lifecycle"]
            caller -->|service traits| core
        end
        commands["Commands / PTYs"]
        workers["Filesystem workers<br/>explicit executable"]
        core -->|spawn and supervise| commands
        core -->|configured launcher| workers
    end
```

Selecting an account or normalizing a path does not create isolation or confine
access to a workspace root. A separate convenience facade or duplicate error
hierarchy is unnecessary; application-specific wrappers remain downstream.

## Required contract

### Operations and admission

Cover the full [guest bridge operation inventory](../../../proto/spaced/bridge/guest/v1/guest.proto):
process start/reconnect/list/signal/stdin/PTY, filesystem stat/list/read/write/
**batch writes**, mkdir/remove/rename, watches and **file-change tracking**,
**process-environment configuration**, and **workload placement**. Environment
updates affect future starts only. Deprecated RPCs remain adapters; metadata and
networking remain daemon concerns, despite sharing the guest protocol crate.

Use existing [process](../../../crates/libs/spaced/guest/protocol/src/process_limits.rs)
and [filesystem](../../../crates/libs/spaced/guest/protocol/src/filesystem_limits.rs)
budgets at the shared operation boundary. Transport adapters keep predecode
protection and pass admission through without double charging. Native calls must
reject oversized values and bound streams, operations, workers, watches and retry
records; they cannot prevent the caller's prior allocations. Keep permits until
owned work settles, even if the response is dropped.

### Worker launch and identity

Construction requires a caller-provisioned, protected worker artifact, built and
tested with the library. Define executable resolution/replacement and ensure
validation and execution refer to the intended artifact. Never discover support
by running an arbitrary application's `current_exe()`; the daemon may explicitly
configure its known worker entrypoint. A handshake does not establish artifact trust.

Validate configuration at construction and protocol version/installed identity
before dispatch. Missing launchers, accounts, privileges, incompatible workers
and startup timeouts fail explicitly. Use typed construction/lifecycle errors.
Retain startup, cancellation, termination and reaping ownership after a dropped
future or timeout; replacement must not accumulate untracked workers. Inherit
only the worker channel and documented environment/telemetry inputs.

Install the selected UID, GID and supplementary groups in children, with no
inherited supervisor capabilities or privilege gain through set-ID/file-capability
executables. Check capability state as well as IDs. Never change existing host
threads' credentials, capabilities or cgroup membership.
**Direct filesystem mode is unsupported in this profile:** it uses host authority
and must neither masquerade as impersonation nor serve as a worker-start fallback.
Keep daemon policy differences explicit within the same identity implementation.

### Shutdown and ownership

The caller keeps a Tokio runtime with I/O, timers and process support alive
through drain. Caller reapers wait only on caller-owned PIDs; they must not use
`waitpid(-1, ...)` or reap library-owned children. A notification-only `SIGCHLD`
handler installed before Tokio signal initialization is supported alongside
Tokio subscribers. Do not replace that arrangement, ignore/block `SIGCHLD` or
set `SA_NOCLDWAIT` while the service or cleanup is alive. Tokio can install a
[process-wide handler](https://docs.rs/tokio/1.53.1/tokio/signal/unix/struct.Signal.html#caveats)
that outlives the service. Signal-handler neutrality is not promised.

Order operation registration with admission closure: **running → closing → drained**.
Closing rejects every new operation, including reads, mutations, watches and
environment updates, without cancelling admitted finite operations. Long-lived
filesystem `watch` streams have no natural completion: worker-backed streams
retire with recoverable `Unavailable` at close; Direct streams
retire quietly during drain or cancellation. Explicit
cancellation or a drain deadline requests abortive cleanup; caller cancellation
propagates into the service, never from the service into its caller. Repeated
shutdown observes the same transition. Successful drain means owned operations, worker startup/cleanup and command supervisors have
settled, filesystem workers are reaped, and no later library-dispatched effects
can begin.

A deadline bounds waiting, not OS completion. Timeout reports **incomplete cleanup**,
retains ownership and closed admission, and permits later observation/drain.
Cancellation preserves retry records, stream outcomes and uncertainty; it does
not roll back dispatched effects or make a fresh-key retry safe. Drain implies
neither flush nor crash persistence; retain the [guest I/O contract](../design/guest-io.md).

Process ownership covers leaders, streams and groups while supervision is active,
not every descendant. Detached/redirected descendants can survive and keep writing
after successful drain; **the caller must tear down its isolation boundary**.
Placement alone is not containment. Never kill unrelated host processes. Preserve
Linux's atomic close-on-exec PTY allocation alongside unrelated application spawns.
Caller-supplied placement handles must be opened atomically close-on-exec before
concurrent spawning; `ExecutionCgroups::new` does not establish that precondition.
Dropping a service handle is not proof of cleanup.

## Acceptance and rollout

Maintain an upstream application that uses the execution crate and real Linux
identity workers, without guest-server or Direct mode. Use synthetic accounts in
disposable isolation. Support each Linux architecture only after its kernel,
privilege and integration requirements are exercised; CI must fail on missing
requirements rather than silently skip them.

| Focus | Evidence required |
| --- | --- |
| Launch / identity | Invalid launcher, protocol mismatch, failed/cancelled startup, replacement and reaping; UID/GID/groups, access checks, inherited capabilities and executable privilege gain. |
| Operations / budgets | Complete inventory above, overload, oversized input, streaming backpressure and permits retained through cancellation; equivalent transport/native execution budgets. |
| Lifecycle / ownership | Admission-close races, every post-close entrypoint, interrupted uploads, repeat/incomplete drain, retry uncertainty, runtime/signal compatibility, unrelated spawns, descriptor inheritance and surviving descendants. |
| Integration | Embedding exercises the supported worker path and caller teardown; existing daemon tests and Kind/Firecracker checks cover shared behavior changes. |

Implement in reviewable steps: **launcher + representative integration → identity
policy → shared admission/shutdown → complete CI and consumption documentation**.
Each PR identifies established and pending guarantees; launcher work alone does
not establish embedding support. Preserve wire/import compatibility and follow
[versioning policy](../../versioning.md). Packaging and hermetic consumer builds
need separate validation.

## Implementation coverage

The initial Linux profile uses the execution crate with a separate worker binary;
it does not depend on the process-API extensions listed above. These are the
owning acceptance checks, rather than a second implementation of the contract:

| Requirement | Implementation and acceptance coverage |
| --- | --- |
| Explicit worker artifact and startup ownership | [Worker executable tests](../../../crates/libs/spaced/guest/execution/src/worker_executable/tests.rs) and the [embedding host](../../../crates/apps/spaced/guest/worker/tests/embedding/linux.rs): artifact replacement, invalid/incompatible workers, cancelled startup and reaping. |
| Restricted child identity, unchanged host authority | [Identity probes](../../../crates/apps/spaced/guest/worker/tests/embedding/identity.rs): IDs, groups, capabilities, PTYs, privilege-gaining executables and directory traversal; the [entrypoint](../../../crates/apps/spaced/guest/worker/tests/embedding.rs) also rejects dispatch after threads start. |
| Native operations and bounded admission | [Ingress tests](../../../crates/libs/spaced/guest/execution/src/filesystem/router/ingress_tests.rs), [process tests](../../../crates/libs/spaced/guest/execution/src/process/tests.rs) and the embedding host: payload limits, shared permits, uploads, environment updates and cancellation. |
| Admission closure and retained cleanup ownership | [Execution tests](../../../crates/libs/spaced/guest/execution/src/execution/tests.rs) and [router tests](../../../crates/libs/spaced/guest/execution/src/filesystem/router/tests.rs): every closed entrypoint, admitted work, incomplete/repeated drain and interrupted streams. |
| Placement before exec and final teardown | [Placement tests](../../../crates/libs/spaced/guest/execution/src/placement/tests.rs) authenticate the placement writer as a forked child even when exec fails. The [boundary probe](../../../crates/apps/spaced/guest/worker/tests/embedding/boundary.rs) checks real cgroup membership, descendant inheritance and caller teardown. |
| Concurrent descriptor inheritance | [Startup probe](../../../crates/apps/spaced/guest/worker/tests/embedding/boundary/descriptors.rs): samples unrelated execs during fresh PTY and identity-worker startup. It catches inheritable windows only when an exec overlaps them; it does not guarantee per-round overlap or detection of microsecond races. Local PTY/socket negative controls widened those windows to 50 ms. The boundary fixture checks the caller's close-on-exec placement-handle precondition. |
| Runtime and signal ownership | [Caller handler](../../../crates/apps/spaced/guest/worker/tests/embedding/sigchld.rs) installed before Tokio; the boundary probe observes both handlers and retains PID-specific caller reaping across service drain. |

The [README](../../../crates/libs/spaced/guest/execution/README.md) specifies the
privileged probe's disposable environment and fail-closed prerequisites. Routine
Rust tests cover operation behavior; Kind/Firecracker checks cover the daemon
integration. Passing the Linux checks does not establish native macOS coverage,
acceptance on another Linux architecture, resource-limit policy or hermetic
consumer builds. Those need evidence in their own environments.
