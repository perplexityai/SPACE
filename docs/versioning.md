# Versioning and compatibility

[Documentation](../README.md)

SPACE 0.8.0 establishes the public release baseline. Release identity and runtime
compatibility are separate: upgrading a host must not reject an existing guest
merely because the release number changed.

## Release progression

- **0.8.0:** Public baseline, with room to refine the supported service APIs.
- **0.9.0:** Final planned breaking-change release. The 0.9.x line is for
  compatible improvements and stabilization. Any further breaking change requires
  revising this plan rather than shipping it in a patch.
- **1.0.0:** Stable service and sandbox lifecycle contracts, gated on compatibility
  evidence.

Before 1.0, breaking changes increment the minor version; patch releases remain
backward-compatible. From 1.0, compatible features increment the minor version,
compatible fixes increment the patch version, and breaking changes require a new
major version.

## Service contracts

At 1.0, backward compatibility throughout 1.x covers:

- The native HTTP API.
- The worker `node-api` gRPC/protobuf API for custom cluster management.
- The `spaced-bridge` gRPC/protobuf API for custom in-guest integrations.

These contracts include documented behavior, request and response shapes,
errors, streaming, authentication requirements, and lifecycle semantics.
Additions must preserve existing callers. Deprecated functionality remains
supported until the next major release. Rust crate APIs and generated Rust
client interfaces are outside this guarantee; Rust source compatibility needs
a separate crate-level policy.

## Guest compatibility in 0.8

The host supports stable `spaced` guest versions **>=0.1.0 and <0.9.0**.
Prereleases are not admitted. Build metadata does not change compatibility.
The supported range is declared in `pplx-space-spaced-common`; changing the host
release number does not change that range implicitly.

From 0.8.1, guests report their OS and supported operations in the readiness
response. The host intersects these claims with trusted image policy. Linux
guests through 0.8.0 may omit the descriptor; the host derives their supported
operations from their release. Newer guests must provide it. An empty explicit
operation list grants nothing. The host's supported guest range is unchanged,
including 0.1.4 and 0.8.0.

Hosts that understand the descriptor send readiness contract revision 1 or
higher. Windows guests refuse readiness to hosts that send zero, including an
older host relaunched against a resident Windows VM after node rollback. This
is a readiness compatibility check, not authorization of later RPCs.

Guests and templates retain their actual release version and derived
compatibility family (`0.1.x` reports `0.1`; `0.8.x` reports `0.8`). The family
must agree with the guest version, but need not match the host's family.
Missing, malformed, inconsistent, or unsupported guest identity fails admission.
Do not relabel old templates or snapshots as 0.8 to make them usable.

Template admission and live guest readiness use the same policy. Cold starts,
recovered guest readiness, running snapshot restores, and resident resumes
validate the live guest identity. The manager also checks identities reported by
older host sidecars that survive a rolling upgrade. A restore whose final state
is paused defers its live check until resume; it never thaws the VM merely to
query its version.
Running restores have a 30-second guest readiness budget before the existing
network readiness gate. Resident resumes check readiness even when the caller
omits a blocking readiness wait; a failed check uses the existing pause rollback
path. Unsupported identity returns `FailedPrecondition`, rather than retrying
until it appears to be a timeout. Missing connectivity remains bounded by the
readiness deadline. Restore failures retain the existing activation cleanup and
snapshot retry paths.

Existing RPC fallbacks remain necessary at the 0.1.0 minimum. Features introduced
later may require a newer guest and return an explicit unsupported-feature
response; existing operations must continue working. Raising the minimum to
0.1.4 is a separate compatibility-retirement change, requiring a migration plan
for deployed hosts, live guests, templates, and dormant snapshots.

Compatibility is directional. A new host accepting an old guest does not imply
that an old host accepts a new guest. In particular, older hosts using exact
family admission reject 0.8 templates.

The range is enforced by host services, not by guests. If 0.9 preserves the
guest protocol, its host services can retain the 0.1.0 floor and widen the upper
bound to `<0.10.0` after validating compatibility. Existing 0.1.4 and 0.8 guests
then continue running against 0.9 hosts without a guest update or identity
change. The 0.8 host's `<0.9.0` bound only limits which newer guests that host
accepts; it does not constrain the support policy of a future host release.

## Sandbox lifetime from 1.0

Every sandbox created on 1.0 must remain fully supported throughout 1.x. Later
1.x hosts must also support sandboxes created on earlier 1.x releases, including
execution, management, recovery, and snapshot restore, without requiring sandbox
recreation or changes to in-guest integrations.

This includes persisted state and the runtime dependencies needed to restore it.
Format or runtime changes must preserve a compatible restore path or provide a
validated, state-preserving migration. Fleet age-out cannot justify removing
support for an earlier 1.x guest or persisted sandbox. A future major release
may provide migration tooling; such tooling is not currently promised.

## Release and rollout gates

For 0.8:

1. Validate the compatibility policy and legacy bridge fallbacks before publishing
   release artifacts. Exercise old guest binaries and retained snapshots on a
   Linux/KVM environment: create, execute, manage, restart/recover, suspend, and
   restore. Include paused restores and subsequent resume, and rejected guest
   identities. Mock protocol tests do not replace this runtime evidence.
2. Upgrade host services before promoting 0.8 guest templates. During a mixed
   rollout, retain 0.1 templates and prevent 0.8 guests from being placed on
   older hosts. Keep existing guest and snapshot artifacts and their required
   runtime/kernel versions available.
3. Associate the `v0.8.0` source tag with the exact tested commit and record the
   corresponding immutable artifact digests in the release notes. Bundle CI
   currently tags images by commit; a Cargo version bump does not publish a
   version-tagged image or a GitHub release.
4. Validate rollback combinations before promotion. Once 0.8 guests or snapshots
   exist, rollback to an older host is supported only with a tested compatible
   host/guest combination. Otherwise retain compatible hosts for those sandboxes
   and stop new 0.8 template selection while rolling back other components.

Before 0.9, review all three service contracts, consolidate planned breaking
changes at 0.9.0, and publish migration guidance.

Before 1.0, freeze the three service baselines and gate release on mixed-version
and retained-sandbox fixtures covering execution, management, recovery, and
restore. Establish supported upgrade and rollback combinations, preserving a
rollback-compatible path when introducing new guest artifacts. Run these gates
for every 1.x release, retaining the 1.0 baseline and extending coverage as
capabilities and persisted formats evolve.
