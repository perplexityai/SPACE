# Guest execution embedding contract

Status: proposal for review. The guest execution crate currently supports the
daemon contract described in its [README](../../../crates/libs/spaced/guest/execution/README.md).
This proposal defines acceptance criteria for future embedding support; it does
not add a supported API or change the daemon's behavior.

## Purpose and boundary

Allow an application inside an independently isolated environment to reuse
SPACE's process and filesystem operations without running the guest daemon's
listeners, readiness protocol, networking service, or cgroup initialization.
The application supplies isolation, authorization, account provisioning, and
final teardown of that environment. The library supplies operation execution,
worker supervision, admission, retry ownership, and an explicit drain contract.

This is not a new SPACE sandbox backend. Such a backend needs a separate
runtime-adapter and isolation design. Filesystem path normalization and Unix
account selection do not create a sandbox or restrict access to a workspace root.

## One operation contract

Keep `ExecutionService` as the shared implementation used by the daemon and the
upstream embedding integration. Initially retain the existing protobuf values,
generated service contracts, and Tonic operation errors. Do not restore a
parallel convenience facade or an error enum that copies gRPC status categories.
Fallible construction and lifecycle operations should have their own small typed
errors where a caller must distinguish configuration, startup, or drain failures.

Move admission and lifecycle enforcement to the common operation boundary before
supporting direct calls. Transport adapters retain predecode protection and pass
their admission through; an operation must neither bypass nor double-charge its
shared budget. The daemon retains its additional readiness gate. Downstream
convenience wrappers remain downstream.

The operation inventory is the current [guest bridge schema](../../../proto/spaced/bridge/guest/v1/guest.proto),
plus construction and environment configuration:

| Surface | Required embedding behavior |
| --- | --- |
| Processes | Start, reconnect, list, signal, stdin, stdin close, and PTY resize; preserve deadlines, output retention, and retry semantics. |
| Files | Stat/list, streaming read/write, batch write, mkdir, remove, and rename; preserve partial-effect and retry outcomes. |
| Watches | Root normalization, streaming and polling watchers, removal, overflow, file-change tracking, and modified-file queries. |
| Environment | Configure defaults for future process starts without changing already-running processes. |
| Placement | Accept explicit caller-owned workload placement; never initialize or rearrange the host's cgroup hierarchy. |
| Lifecycle | Fallible construction, admission closure, cancellation, drain, and observable incomplete cleanup. |

Deprecated filesystem RPCs remain compatibility adapters, not a second core.
Metadata and networking stay daemon concerns even though the guest protocol
crate owns their schemas. File acknowledgement and durability continue to follow
the [guest I/O contract](../design/guest-io.md); embedding drain does not imply a
filesystem flush, rollback, or crash persistence.

## Proposed initial support profile

Start with Linux and privileged identity-worker operation inside caller-provided
isolation. Support both repository Linux architectures only after their worker
and integration checks pass. Required kernel facilities and privileges must be
checked or documented with a tested failure path, rather than inferred from UID 0.

The first embedding profile selects provisioned non-root workload accounts and
requires sufficient supervisor authority to install their UID, GID, and
supplementary groups. Privileged workloads, unprivileged same-identity hosting,
and non-Linux embedding are follow-ups requiring their own policy and evidence.
These restrictions apply to the proposed embedding profile, not the existing
daemon's supported users.

Direct filesystem execution remains a daemon/test mode. It is not an alternative
implementation of selected-account impersonation: it uses the hosting process's
authority. Embedding construction must reject it under this initial profile;
failure to start an identity worker must never fall back to Direct execution.

The library must not change credentials, capability state, signal handlers, or
cgroup membership of the application's existing threads. Platform-specific
requirements belong in explicit configuration and validation, not defaults that
silently select a weaker mode.

## Worker launching and authority

Require an explicit trusted worker executable for embedding, built and tested
with the library. Provide an upstream worker-only entrypoint whose only purpose
is worker startup. An embedding application must not implicitly re-execute
`current_exe()` or depend on remembering a worker dispatch hook before its normal
initialization. The daemon can retain its known entrypoint through explicit
daemon configuration using the same worker implementation.

The caller owns provisioning and protecting the worker artifact. A path or a
successful protocol handshake cannot prove that an arbitrary executable is safe
to run. Do not probe the host application's entrypoint to discover support. The
implementation must define executable resolution and replacement behavior and
ensure validation and execution refer to the intended artifact.

Construction validates the launcher configuration; worker startup validates the
protocol version and installed identity before dispatching any user operation.
A missing launcher, unavailable account, credential failure, incompatible worker,
or startup timeout fails explicitly. Retain worker ownership through failed
startup, cancellation, termination, and reaping, including when the caller drops
the initiating future. A timed-out worker must not silently consume an untracked
process slot while replacements continue to launch.

Define inherited environment and descriptors as part of the launcher contract.
Pass only the worker channel and documented startup/telemetry inputs, with no
accidental inheritance of application secrets or unrelated descriptors.

For the initial embedding profile, worker and workload authority must match the
selected account, with no inherited supervisor capabilities and no privilege
gain through set-ID or file-capability executables. Verify the relevant IDs,
groups, and capability state; matching IDs alone is insufficient. The identity
implementation must enforce this policy in children and test it before claiming
support. Keep any daemon policy differences explicit, using the same identity
implementation rather than a separate embedding credential path.

## Admission, cancellation, and shutdown

Use the existing [process](../../../crates/libs/spaced/guest/protocol/src/process_limits.rs)
and [filesystem](../../../crates/libs/spaced/guest/protocol/src/filesystem_limits.rs)
limits as the starting budgets. Direct callers supply already-allocated values,
so native validation cannot promise transport-style protection before allocation;
it must still reject oversized operations and bound retained execution resources.
Bound streaming frames, active operations, workers, watches, and retry records.
Keep permits until their owned work settles, not merely until a response drops.

Lifecycle states are running, closing, and drained. Closing admission and
registering an operation's owner must be ordered together so every accepted
operation is accounted for. After closing, all operation entrypoints, including
filesystem reads, mutations, watches, and environment updates, reject new work.
Existing response subscriptions may terminate while their operation owners finish
cleanup. Repeated shutdown calls observe the same transition and outcome.

Successful drain means all library-owned operations and worker startup/cleanup
tasks have settled, filesystem workers are reaped, and tracked command supervisors
have finished. Later calls cannot start library-dispatched effects. Cancellation
does not roll back effects already dispatched, erase an uncertain outcome, or make
retrying with a new key safe. Preserve existing router-owned retry records and
stream completion/error semantics through this transition.

A caller-supplied deadline bounds waiting, not operating-system completion.
Timeout returns incomplete cleanup, retains ownership and admission closure, and
allows subsequent observation or drain. It must not report successful shutdown
while a blocking operation can still apply effects. Dropping a handle is not proof
of cleanup; the caller must await drain or tear down its isolation boundary.

## Process and descriptor ownership

The initial contract owns command leaders, their streams, and process groups
while their supervisors are active. It does not promise ownership of every
descendant: a detached child that redirects output can outlive its leader.
Successful operation drain therefore does not prove that the environment is
empty or that such descendants have stopped writing files. The caller owns final
environment teardown. Supporting library-enforced all-descendant termination
requires an additional containment contract and tests before advertising it.

Workload placement alone must not be described as descendant cleanup. The library
must never kill unrelated application processes to satisfy drain. Linux PTYs
already use atomic close-on-exec allocation; retain it and validate descriptor
inheritance alongside unrelated application spawns. Non-Linux private spawn locks
do not establish that guarantee for other spawners and are outside this profile.

## Upstream acceptance evidence

Maintain a small application that depends on the execution crate without the
guest-server crate, supplies the worker-only executable, and exercises the shared
contract. Use synthetic accounts and a disposable Linux isolation environment.
The integration must run the identity-worker path, not select Direct mode to
avoid its startup and privilege requirements.

| Contract | Required evidence before embedding is supported |
| --- | --- |
| Launch | Missing/invalid launcher, incompatible protocol, startup failure/cancellation, replacement, reaping, and no fallback into ordinary application startup. |
| Identity | Correct UID/GID/groups and filesystem access; rejected unsupported profiles; inherited and executable-granted capability cases. |
| Operations | Process reconnect/stdin/PTY plus filesystem streams, batch, watches, file-change tracking, environment, and placement. Extend existing coverage where it already exercises the common layer. |
| Admission | Equivalent transport/native execution budgets, overload, oversized input, stream backpressure, and permit retention through cancellation. |
| Lifecycle | Concurrent admission/close, post-close filesystem calls, interrupted uploads, incomplete cleanup, repeat drain, and preserved retry uncertainty. |
| Ownership | Unrelated subprocess spawning, descriptor inheritance, leader exit, redirected/detached descendants, and caller-owned teardown. |
| Daemon | Existing daemon tests plus Kind/Firecracker validation after changes to shared execution behavior. |

Use owning-layer regression tests for individual failures and the application
integration for the cross-process contract. CI must provide required privileges
or fail explicitly; skipped worker tests do not establish support. Kind daemon
smokes and Direct-mode unit tests remain useful but cannot replace this evidence.

## Implementation sequence and review decisions

1. Agree on this support profile, single operation contract, and ownership limits.
2. Add explicit worker launching and the minimal upstream application/worker
   integration, retaining the daemon's existing behavior.
3. Enforce and test identity/capability policy for the supported profile.
4. Move native admission and lifecycle enforcement into shared operations, with
   regression tests and daemon validation.
5. Complete integration/CI coverage and document the supported consumption path.
   Validate packaging and the consumer's build graph separately; extraction does
   not establish hermetic builds or publishability.

Each implementation PR must identify the clauses it establishes and those still
pending. Do not advertise general embedding support after launcher work alone.
Retain existing wire/import compatibility; evolve internal Rust construction
deliberately and follow the repository's [versioning policy](../../versioning.md).

Review should settle three choices before implementation: privileged Linux with
non-root workloads as the first profile; a supplied worker-only executable rather
than arbitrary application re-exec; and caller-owned final descendant teardown.
If the first consumer needs unprivileged hosting or library-owned descendant
termination, expand the contract and evidence before implementing that use case.
