# Execution environment identities

[Documentation](../README.md)

An execution environment binds a runtime, its host requirements, a guest
platform, boot inputs, isolation semantics, and an adapter compatibility profile.
Its reference is the SHA-256 of that complete definition. An artifact digest
identifies bytes; an environment reference identifies a particular combination
of bytes and execution requirements. Neither reference implies that those bytes
are installed, usable, or approved for execution on a node.

The original schema-1 contract describes the compiled **E0** definition: Linux/x86_64 host,
KVM, Firecracker 1.15.1, Linux/x86_64 guest, kernel 6.1.155, a hardware VM boundary,
and Firecracker snapshot format 9.0.0. The runtime digest covers the pinned
Firecracker and jailer binaries, not their download archive.

The implementation includes domain types, protobuf messages, a versioned JSON
codec, and explicit seed/build binding. E0 remains the default. Designated nodes
can select other approved compiled bindings through the regular APIs when the node execution policy allows them;
installation alone never authorizes selection. Cluster policy tables and fleet
selection are separate operations.

## Ownership and extension boundaries

| Concept | Meaning and owner |
|---|---|
| `HostPlatform` | Host OS family plus architecture. Policy cohorts must use both. Linux, Windows and macOS are distinguishable identities, not claims that this build implements their execution adapters. |
| `GuestPlatform` | Existing guest OS family plus architecture. It is independent of the host. Distribution/release, image provenance and mutable boot state remain template/sandbox concerns. |
| `RuntimeBinding` | A tagged provider reference. Schema 1 implements only `ManagedRelease`, with an exact binary digest and host target. A host facility must get a distinct provider when its adapter is implemented; it must not be represented by an invented installable artifact. |
| `BootProfile` | Schema 2 uses `ArtifactsV1`: a versioned boot contract identifier and immutable inputs with unique roles, kinds, formats, versions and digests. It does not prescribe a kernel, a particular guest OS, or executable filenames. Schema 1 retains `LinuxDirectV1`. Template rootfs and mutable per-VM state keep their existing owners. |
| Backend, payload layout, isolation | Backend and isolation describe mechanisms rather than runtime brands. Schema 2 uses `CatalogReleaseV1`: the trusted catalog owns binary roles and file layout, including single-binary releases. Download packaging and local paths are not wire contracts. The Firecracker layout remains only for schema-1 compatibility. Unknown/unspecified enum values are unsupported. |
| `ExecutionCompatibilityProfile` | Schema 2 uses `RuntimeV1`: an immutable machine/device contract identifier and an optional `StateFormatIdentity`. Absence explicitly means no memory-state contract; presence is a format requirement, not permission to restore. Schema 1 retains `FirecrackerSnapshotV1`. Approved replacements and per-operation capabilities remain catalog/adapter policy. |

Schema 1 describes the existing SPACE Firecracker direct-boot adapter. Its
command-line construction and device configuration remain adapter-owned; CPU,
memory, disks, and guest userspace remain workload/template inputs. Any future
environment-selectable boot or device configuration must participate in the
profile identity before it can affect execution. Schema-2 contracts are data:
adding a runtime that uses the existing managed-artifact mechanisms requires no
runtime-specific protobuf arm. Its adapter implements the boot and machine
contracts, and its catalog qualifies exact artifacts. A genuinely new shared
concept, such as a host-provided runtime facility, still requires its own model;
it must not masquerade as a managed release with invented artifacts.

Contract identifiers are bounded validated identifiers (for example a versioned
adapter ABI name), not free-form JSON, filenames or executable commands. New
semantics require a new contract identifier and therefore a new environment
reference. Adding a qualified replacement does not change an existing contract.
Readers preserve unsupported contracts as descriptions; execution admission
requires a reference in the node's trusted catalog and an implementing adapter.
No generic parser infers support from a name or matches arbitrary state versions.

Structural decoding and execution approval are separate:

1. `EnvironmentDescriptor::try_from` requires complete fields and consistent
   host target and profiles, rejecting mixed schema-1/schema-2 profiles and
   duplicate boot roles. The legacy profile remains Firecracker/Linux-specific;
   the schema-2 envelope is runtime-neutral. Structural acceptance does not
   certify a host/guest combination or implement its named contracts.
2. `EnvironmentDefinition` computes the content reference. Wire/document readers
   recompute it and reject a mismatched claimed reference.
3. The compiled environment catalog resolves qualified definitions. A
   well-formed candidate descriptor or approved downloadable artifact does not
   expand that set. This protocol extension adds no new qualified environments,
   installable artifacts or execution adapters.
4. Execution consumers must separately verify node usability and allowed
   selection. No caller may turn successful decoding into execution permission.

No source URL, cache/vendor path, environment variable, operator label, observed
health, enforcement mode, or installation job revision is part of this identity.
Local source/path configuration cannot become a second selection authority.

## Runtime-neutral encoding and rollout

Firecracker catalog definitions and decoded legacy definitions normalize to the
same generic boot and compatibility profiles used by other managed runtimes.
The Firecracker builder consumes these profiles; it no longer depends on a
Firecracker-specific protobuf profile. Runtime adapters still implement their
own boot and machine contracts.

Representation and identity versions are separate. `schema_version` describes
the wire representation; `identity_schema_version` describes the canonical
bytes hashed by the reference (zero retains the historical rule of using the
wire version). Existing Firecracker definitions use generic schema-2 transport
with identity version 1. The decoder accepts this combination only when every
generic requirement maps exactly to the original Firecracker contract. It
rejects extra artifacts, changed roles/formats/contracts, missing state format,
and incompatible host/guest/runtime combinations. New definitions use identity
version 2. Neither encoding conversion nor deployment changes persisted
references, selection policy, template hashes, or snapshot provenance.

The immutable old representation is retained privately at the codec boundary
for canonical hashing and stored schema-1 JSON. Consumers see generic profiles.
Historical records that omitted component formats keep their original binding
checks; native schema-2 identities require the declared formats.

The environment gRPC protocol accepts and emits only generic schema 2. Retired
profile and selection fields are removed, including their reservation declarations;
live field numbers and enum values are unchanged.
ResolveExecutionEnvironment callers continue advertising schema 2: transitional
nodes use that capability to return generic definitions during a rolling
upgrade. Nodes reject clients advertising less than 2. No legacy environment
response, reader, or Firecracker-specific environment protobuf message remains.
Selection acknowledgements and persist-before-activation rules are unchanged.

Deployment prerequisite: node-manager, builder, sandbox-manager and operator CLI
readers must first run the transitional migration that understands generic
transport with historical identities. Include rollback images in this gate.
Both deployment orders between that transitional version and this removal are
supported; jumping from a pre-migration version is not. A later rollback to a
pre-migration reader requires restoring the transitional server first.

The historical canonical/JSON reader remains because persisted references still
need it; it is not a legacy gRPC API. Runtime control also uses only the generic
launch, VM-state, capture and restore routes; the old routes and fallbacks are gone.

Schema-2 artifacts are canonicalized by unique role. Every artifact's kind,
version, digest and format, both contract identifiers, and the presence/value of
memory state participate in native schema-2 identity. A kernel-free boot profile
is representable; its adapter still has to implement it. Synthetic round-trip
and canonical fixtures exercise the wire shape without qualifying a runtime.

## Persisted-state provenance and restore compatibility

Snapshot metadata records the producing runtime, guest platform, environment
binding when present, and `StateFormatIdentity { format, version }`. This applies
to sandbox snapshots and template base snapshots. The state format identifies
the persisted VM state, independently of the runtime's release version. The
compiled Firecracker catalog currently maps 1.15.1 to format 9.0.0 and 1.17.0 to
12.0.0; those releases cannot load each other's state under the current adapter.

Producer provenance and loader compatibility are separate facts. Preserve the
snapshot's original producer when a compatible loader is selected; record the
runtime that actually executes the restored VM separately. A memory restore
resumes the snapshotted guest kernel. Selecting another guest kernel requires a
cold boot, even if another VMM can read the memory-state format.

Today, bound snapshots require their exact qualified environment. The legacy
Firecracker loader predicate in
[`runtime_inventory.rs`](../crates/libs/node/runtime/core/src/runtime_inventory.rs)
requires the same format name and major version, with the snapshot's minor
version no greater than the loader's; patch versions do not affect that check.
Passing this predicate alone does not authorize cross-environment restore.
CPU features, host/KVM requirements, device configuration and attached resources
also constrain restoration, as described in
[Firecracker's snapshot compatibility documentation](https://github.com/firecracker-microvm/firecracker/blob/v1.17.0/docs/snapshotting/versioning.md#snapshot-compatibility).

Keep `StateFormatIdentity` as a format identity. Its version is an identifier;
SemVer interpretation belongs to the Firecracker adapter, not generic SQL or
protobuf consumers. A future adapter may require an opaque version, a machine
and device profile, an explicit compatibility set, or an exact build pair. It
may have no memory-state support. Equal version strings across providers do not
establish compatibility. Add the concrete profile and evidence with that
adapter's first restore consumer, without changing existing format meanings.

## Disk images and boot compatibility

Disk compatibility is independent of persisted VM-state compatibility. SPACE's
current `rootfs.ext4` is a raw block image containing an ext4 filesystem;
Firecracker exposes the block device, while the guest kernel interprets ext4.
The filesystem must be supported by that kernel, as described in
[Firecracker's rootfs setup](https://github.com/firecracker-microvm/firecracker/blob/v1.17.0/docs/rootfs-and-kernel-setup.md).
An ext4 feature can also constrain guest kernels and host tools that check,
mount or resize the image; unknown incompatible features cannot simply be
ignored. See the [ext4 feature rules](https://docs.kernel.org/filesystems/ext4/super.html).
Neither the filename nor a VMM release number proves bootability across all
future kernels and adapters.

The first consumer of richer disk metadata must distinguish these requirements:

| Requirement | Meaning and owner |
|---|---|
| Representation | Raw block image, a versioned disk container, or a filesystem tree. Template/image metadata owns the representation and required container features; the consuming adapter reports what it can attach. |
| Dependencies | Backing images and other required disk resources must retain resolvable identities and lifetimes. A digest of the top-level container alone does not establish a complete disk. |
| Layout and filesystem | Whole-device filesystem versus partitioned disk, filesystem types and required feature flags. Image metadata owns requirements; guest kernels and any host-side image tools must support the operations they perform. |
| Boot and guest requirements | Guest OS/architecture, direct-kernel versus firmware boot, required devices and guest drivers. The template/image owns guest requirements; the environment and adapter supply compatible boot and device semantics. |
| Packaging | Compression and transfer encoding remain separate from the representation presented to the VM. Unpacking an image does not establish runtime compatibility. |

Guest distribution/release, such as Ubuntu 22.04, belongs to the image or recipe
identity in addition to the guest OS family. Mutable per-VM boot state belongs
to the sandbox or its state artifacts, not immutable runtime installation.
Runtime-manager provisions runtime components; template/image owners describe
disk requirements, and execution admission matches them to adapter capabilities.
Prefer those requirements over a growing image-level allowlist of VMM versions.

These distinctions do not allocate new disk schema variants. Keep existing
`ArtifactComponentMetadata.format` and manifest fields unchanged; introduce a
versioned representation with its first implemented consumer and migration.
Do not reinterpret an old field or add guessed disk/container variants to E0's
canonical definition.

## Requirements for environment observations and admission

The next environment inventory and placement implementation must:

- Distinguish installation, verified usability, execution approval and qualified
  operation support. Report snapshot creation and loading independently, including
  the writer format and supported loader profiles. The legacy inventory's
  `can_create`/`can_load` projection is not sufficient environment-level evidence.
- Preserve producer metadata through snapshot publication, upload, restore and
  template base-snapshot paths. Keep the snapshot's disk dependencies associated
  with its memory state; a compatible state loader does not prove disk usability.
- Apply the same adapter compatibility rules at placement and node admission.
  Direct placement's current major-only state-format comparison is insufficient
  evidence where node admission also checks minor versions. Preserve the legacy
  projection for old readers during migration; do not use it to admit new
  environment-selected work.
- Treat unknown profiles, absent operation support and stale observations as
  insufficient evidence. Node admission must recheck usability because published
  observations are not reservations. An incompatible request must not silently
  choose another environment or degrade restore into cold boot.
- Keep cross-environment restore disabled until a separate qualified change
  covers loader selection, CPU/host/device constraints, disk dependencies,
  producer-versus-loader recording and mixed-version rejection tests. Include
  compatible, incompatible, missing-evidence and unsupported-operation cases.

This is guidance for the owning implementations. It adds no policy tables,
protobuf fields, configuration variables or execution behavior. Concrete disk
profiles and other runtime adapters remain separate changes with real consumers.

### Inventory collection evidence

`PublishNodeState.execution_observation` distinguishes successful collection
(including an empty inventory) from an unavailable node-manager. An omitted
observation with no legacy inventory means this publication did not sample
execution inventory. It neither clears nor refreshes the previous observation.

The cluster records `execution_inventory_status` and
`execution_inventory_received_at` on `node_status_snapshots` in the same
transaction as the runtime/component projection. Both are NULL until a report
is received; unrelated publications preserve them. A complete empty inventory
clears that projection. Unavailable collection retains the last artifact rows
for diagnostics but marks the observation unavailable. Legacy-only reports
replace the old projection and record `legacy`, including after a node-ctl
rollback; they do not establish support for the observation protocol.

New publishers may duplicate a complete inventory in the legacy field for old
cluster readers. New readers reject disagreement between the two fields,
unavailable reports carrying an inventory, and unknown or missing outcomes.
These are protocol errors: the whole request is rejected with `InvalidArgument`
rather than acknowledging contradictory evidence as a collection failure.
Readers supporting the observation envelope serialize replacements by locking
the registered node, including when the inventory is empty. Artifact writes
still precede the status upsert, preserving the lock order of legacy readers.
A failed publication rolls back both the observation and its artifact
projection, together with other writes in that request.

`execution_inventory_received_at` is control-plane receipt time, not a node
sampling timestamp or an environment readiness attestation. Inventory receipt
does not establish host prerequisites, operation compatibility, or freshness of
particular bytes.
Placement remains unchanged until environment observations, freshness rules and
the separately activated enforcement gate are implemented. Deploy the migration
and cluster readers before node publishers; older readers ignore the new field.
During a mixed cluster-reader rollout, old readers can update artifact rows
without updating observation receipts. Neither strict rejection nor an
unavailable observation is a placement quarantine.

Node-ctl samples on the node-status facet's periodic reconciliation.
Cluster-role nodes omit the sample. The shared node-manager channel is reused;
connection readiness plus inventory RPC share a five-second budget.
Failure, an unsupported RPC, or timeout publishes an
unavailable observation; it does not abort publication of the remaining node
status. The next periodic tick samples again rather than replaying a cached
inventory. Other facets omit the observation, leaving its receipt unchanged.
A successful collection populates both the report and the identical legacy
inventory so old cluster readers retain their existing behavior.

Execution collection accompanies only the node-status facet. Sandbox and
template censuses run as separate outbound publications and can also refresh
node status; an execution protocol error does not reject those separate
requests. Failed publications remain reconciliation errors and the next
periodic tick retries with a new sample. A persistent protocol defect needs a
publisher fix, not a successful acknowledgement of the rejected report.

The inventory polling period (60 seconds by default) isn't itself a heartbeat
freshness guarantee. The node-status freshness window is derived from the
node's reported poll interval with margin: two intervals plus 30 seconds,
150 seconds by default and never more than 10 minutes, so a node that publishes
only on its tick doesn't read stale between ticks. node-ctl caps its poll
interval at 285 seconds, the longest whose two ticks plus slack fit in the
10-minute cap. A publication that reports no interval keeps the node's stored
window. First-boot artifact staging may exceed the five-second collection
budget and legitimately report unavailable until a later sample succeeds; the
previous artifact rows remain in place.

Repeated complete samples advance the collection receipt without replacing
unchanged runtime, guest, state-format or component rows. Reconciliation deletes
missing entries, inserts new ones and updates changed flags; a capability can
also be revoked on a retained runtime. Duplicate entries are combined within
the current sample, never with flags from an older sample. Unchanged artifact
row timestamps stay unchanged, so use the receipt to diagnose collection age.
This still issues one separate status publication per tick and performs scoped
database reconciliation; it avoids full inventory rewrites, not all database
work. Keeping this publication separate isolates protocol errors from censuses.

## Canonical identity and stored representation

The schema-1 canonical encoding is defined by
[`canonical_descriptor`](../crates/libs/execution/common/src/environment.rs)
and the independent [E0 golden vector](../proto/execution/v1/fixtures/e0.canonical).
It uses UTF-8 ASCII text, a domain separator, schema version, fixed field order,
canonical scalar spellings, and a final LF. Identifiers exclude delimiters and
newlines; digests use lowercase `sha256:` plus 64 hex digits. Schema 1 has named
runtime and kernel slots, not unordered artifact arrays. A future collection
needs an explicit ordering/duplicate rule in its new schema.

Hash the exact canonical bytes with SHA-256. E0's reference is:

```text
sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b
```

The [JSON fixture](../proto/execution/v1/fixtures/e0.json) is the versioned stored
representation: `{schema_version, reference, descriptor}`. JSON ordering and
whitespace do not affect identity. The stored codec requires canonical scalar
spellings, rejects duplicate/unknown fields (including nested legacy types), and
verifies the reference on every load. Protobuf conversion validates required
messages, enums and oneofs, rejects noncanonical digest spellings, and checks the
same reference. JSON/protobuf serialization bytes are never hashed.

The first owning policy store must persist the immutable definition and reference
consistently and check them on load. It must use forward migrations and preserve
authoritative execution records and retained jobs. This change defines that
document boundary without adding unused SQL tables or modifying applied
migrations. Document round-trip tests establish codec compatibility, not a claim
that a database backfill or end-to-end rollout has been exercised.

## Rollout and unknown data

Existing `ExecutionMetadata`, inventory messages, and enum field numbers retain
their wire and JSON meanings. They are not evidence of environment support and
cannot be converted into an approved environment merely from a version string.
Legacy-to-E0 attribution must be implemented by the binding consumer under the
stock-image compatibility rules; missing or ambiguous evidence must not become
an inferred Linux/E0 binding.

New environment consumers require positive evidence that the peer supports both
the schema and the qualified environment reference. The builder now requires
`ResolveExecutionEnvironment` to return a checked definition and matching bound
execution metadata; `Unimplemented`, missing fields, and conflicting evidence
fail the build. Sandbox-manager requires a bound prepare request to receive the
same reference before persisting the response or launching. An old node-manager
that drops the field cannot acknowledge a bound request.

Protobuf readers normally discard unknown fields. Therefore **any new required
meaning**, including a new field or provider/profile arm, requires a new
environment schema version. Readers reject unknown/zero schema versions, unknown
enums, and absent/unknown required oneofs. A sender cannot rely on an old reader
to notice an extra field under schema 1. Additive transport compatibility does
not substitute for explicit capability negotiation. Do not infer support from
successful protobuf decoding or the old installation projection.

Tests cover frozen legacy metadata wire bytes, E0 wire/document round trips,
reference mismatches, missing/unknown values, unsupported combinations, and
catalog rejection of well-formed but unqualified candidates. Binding tests also
cover missing peer acknowledgement, contradictory execution observations,
manifest publication, SQLite JSON round trips, and PostgreSQL write-once storage. A real Linux/KVM build,
publish, pull and boot remains the deployment acceptance test.

## Seed/build binding and deployment

`ResolveExecutionEnvironment` is a read-only node-local RPC with the node-status
peer allowlist. An omitted reference requests compiled E0; an explicit reference
must resolve to an admitted compiled definition. Node-manager requires Linux and a
non-overlapping managed-store layout, verifies that binding through the execution inventory,
and returns the definition with matching execution observations. Prototype layouts
that retain unverified external execution cannot produce this binding. The RPC
is an observation, not a reservation or an activation operation; prepare and
launch resolve again. E0 artifacts remain protected from removal.

The builder distinguishes verified bindings from legacy execution before resolution:

- An explicit scratch request resolves exactly its admitted environment; any
  recipe kernel pin must agree. Without a selection, unpinned Linux/x86_64 scratch
  builds resolve E0, while custom-kernel recipes and ARM scratch builds retain
  their unbound contract and supplied kernel pin. Neither path scans for
  the newest runtime, and inherited selector variables are overwritten. Binding
  changes only the execution/schema fields, preserving seed image provenance.
- Derived builds require readable base and actual sandbox execution metadata.
  An explicit or inherited environment reference must resolve and match both observations.
  Unbound bases remain unbound; missing kernel identity is not silently attributed
  to E0. Kind's bootstrap seed records the actual staged kernel digest so new
  seeds carry complete evidence. Missing execution metadata still fails closed.

Resolver errors never downgrade a bound build to legacy execution. Constructing
an image records its target; it is not evidence of a successful VM boot.

New bound `metadata.json` and `manifest.json` use schema 3 and carry
`execution.environment`. Schema 2 remains the unbound shape. A schema/binding
mismatch fails validation; the old schema-2 reader rejects schema 3. The
reference survives normalization, immutable manifest publication, the execution
JSON stored by template-manager and the control plane, worker preparation, and
sandbox metadata. No table migration or backfill is required. The reference is
not a substitute for binary verification or an arbitrary execution permission.
A same-rootfs registration cannot add, remove or replace the retained manifest's
binding by refreshing metadata; it fails before changing the existing release.
Control-plane registration likewise rejects contradictory execution metadata
before changing tags or storage locations. Identical retries and metadata-free
tag updates preserve the original evidence; a NULL record may be filled once.

Deploy the new control-plane, template-manager, sandbox-manager and node-manager
readers before enabling builds with the new builder. Until readers are upgraded,
new builds or bound-template consumption may fail closed; existing unbound
workloads retain their previous execution behavior. Production qualification
covers only Linux/x86_64 E0; ARM builders continue producing unbound templates.
A legacy base needs complete matching evidence before opting into a binding.
Do not strip a binding to make a template load on an old reader.

For rollback, stop producing bound releases and use retained schema-2 releases
with the old services. Keep compatible readers while bound templates or their
sandbox/snapshot records remain in use: older JSON/protobuf readers outside the
template schema gate can discard the new field. This change does not claim
fleet-wide capability placement or snapshot-version negotiation. Explicit environment selection adds local admission; cluster-wide provisioning
follows separately. Installing 1.17.0 still does not select it.

## Bounded audit of existing contracts

This audit concerns the six boundaries touched by the environment-contract and
provisioning work. It is based on the repository schema and readers/writers,
not inspection of live deployment versions. The owning implementation must
establish deployed migration/peer versions before a cutover. None of the
deferred replacements below authorizes destructive resets or reinterpretation
of existing data.

| Existing boundary | Disposition and target semantics | Owner and delivery | Compatibility/removal criterion |
|---|---|---|---|
| `RuntimeIdentity`, `RuntimeInstallation`, `NodeExecutionInventory`; PostgreSQL `node_runtime_installations` keyed by `(node_id, runtime, runtime_version)` and its dependent projections | **Keep** release metadata. **Replace with first consumer** its use as sufficient environment capability: observations need complete environment reference plus host/usability evidence. This PR adds the separate definition/reference, not a renamed installation row. | Execution-common owns definitions; node-ctl publication and cluster storage own the later provisioning projection; placement migration follows publication. | Preserve old inventory for old readers during rollout. New admission requires new evidence. Remove the old admission dependency only after every reader migrates; old release diagnostics may remain useful. |
| `GuestOperatingSystem`, `GuestPlatform`, `ArtifactComponentKind` | **Keep** their current meanings. **Replace now** missing host identity with separate `HostPlatform`; do not infer host OS from guest OS. | Common-core/execution-common, this contract. Template owner adds distribution/release with its first guest-image consumer. | Additive messages preserve old guest fields and enum numbers. There is no guest-field migration/removal in this change. |
| Runtime-manager `AvailableArtifact.architecture` and `ExecutionArtifact.architecture` | **Keep** legacy architecture evidence; it is not a full target or placement requirement. **Replace now** in new descriptors with explicit host target for runtime bytes and guest platform for the direct-boot kernel. | Execution-common owns descriptor targets now; runtime-manager owns richer diagnostic target fields with the first additional installer/target. | Do not silently change the old strings' meaning. Retire their placement use when consumers use typed targets; keep diagnostic evidence as needed. |
| `ExecutionMetadata`, node `requested_execution`, string runtime selectors and CLI `--runtime fc` | **Keep** current adapter selection/recorded metadata. **Replace with first consumer** as complete selection authority: explicit environment references and actual resolved execution metadata through seed/build, then benchmark launch. | Template/build and node-manager owners: E0 binding is implemented here; explicit selection uses the compiled catalog and deployment admission. | Validate legacy aliases and reject contradictory old/new selectors at the consuming boundary. Unbound legacy paths remain unchanged until stock-image attribution is implemented; removal requires migrated writers/readers and durable records. |
| Runtime-manager SQLite `operation_results`, shared `jobs`, `InstallOperation` JSON and dedupe | **Keep** `install_kernel`, `uninstall_kernel`, `install_release`, `uninstall_release` as accurate existing operations. Environment policy does not change their identity or replay semantics. | Runtime-manager owns the first new operation kind and any versioned payload migration. | Preserve retained-job decode/replay and `(action, artifact identity)` dedupe; policy revision is correlation only. Verify deployed migration level before evolution. The historical prototype reset is not a future migration strategy. |
| Firecracker paths, sources, local catalog JSON and POSIX protection configuration | **Keep** inside the Linux/Firecracker installer/store adapter. Portable policy references approved environments, not paths or arbitrary hashes. New identity has no ENV selection authority. | Node-manager/runtime-manager own adapter configuration; provisioning/source integration owns managed-store/bootstrap precedence. | Existing configuration keeps its semantics in this PR. The first replacement must define precedence and remove old knobs only after bootstrap, restart and rollback consumers migrate. |

Schema anchors:
[`PostgreSQL baseline`](../db/postgres/20261002070625_baseline.up.sql),
[`runtime-manager artifact operations`](../db/runtime-manager/20261012064915_artifact_operations.up.sql),
[`durable operation payloads`](../crates/libs/node/runtime/manager/server/src/installs/operations.rs),
[`execution protocol`](../proto/execution/v1/execution.proto), and
[`artifact diagnostics protocol`](../proto/node/runtime/manager/v1/manager.proto).

The artifact catalog's kernel/runtime-line association is source/compatibility
evidence, not a new kernel identity. Benchmark admission must validate exact
pairs without relabeling or duplicating the 6.1.155 pin. Lifecycle portability,
host-provided runtimes, firmware boot, guest release metadata and richer snapshot
compatibility remain owned by their first real adapter/guest consumer. These
follow-ups do not block this contract slice or expand it into a whole-platform
schema redesign.

## Explicit environment selection

### Execution artifact integrity

Artifact identity, execution approval and host integrity enforcement are separate
properties. The supervisor's `VerifiedRuntimeLaunch` input names a complete
environment and requires authenticated, immutable runtime and boot sources.
It contains no host paths or filesystem mechanism. An adapter must reject unknown
integrity requirements; a signature check alone does not establish immutability.
Host and guest platforms retain their independent meanings.

On Linux hosts supporting the fixed fs-verity profile, node-manager creates
root-owned execution copies of the approved Firecracker, jailer and guest kernel
under the cache's `managed` namespace. The durable artifact store remains on its
existing filesystem. Initial copies are checked against their content SHA-256;
cache files are sealed and measured against independently pinned verity digests.
The profile uses SHA-256, 4096-byte blocks and no salt. Verity digests are separate
catalog metadata: E0 and the other environment references do not change.

Managed runtime and kernel directories use `<version>--<12 hex digits>`, where
that suffix is a SHA-256 prefix used only as a locator. Catalog pins, execution
identities and verification retain full digests. An existing directory whose
bytes do not match the requested full pins is refused; a prefix collision never
overwrites or adopts another artifact. Removal also checks the full digest.
The same naming rule applies to the execution cache. Upgrade the supervisor
first: it accepts both full-digest and compact locators before node-manager or
runtime-manager can publish compact names. Historical installs require no rename;
readers and removal retain full-digest support and verification. Before rolling
back to a supervisor that accepts only full-digest names, drain executions using
compact artifacts and restore compatible full-digest installations. Baked E0
paths are unchanged.

Existing cache files are sealed in place. Node-manager warms the cache in tracked
background work at startup; inventory consumers await the same serialized discovery,
while unrelated lifecycle RPCs can serve. For missing copies, source presence is
checked first. A read-only fs-verity measurement ioctl on the existing cache
parent (or its nearest existing ancestor) selects the filesystem backend before
sealed-layout checks or namespace creation. No probe files or directories are
created. Definitive API support or absence is memoized per device within one
inventory rebuild; I/O and permission failures are not cached as unsupported.
Unsupported filesystems retain the previous source-specific SHA-256 checks and
do not acquire the sealed cache's additional layout requirements. Managed source
protection remains mandatory in either backend. Once sealed execution is selected,
file-level refusals, including unsupported parameters or a signature-policy
refusal, never authorize legacy fallback. Existing sealed copies are measured
without rehashing their entire contents or issuing fsyncs. An empty protected
cache directory, including a release with only an empty `bin/` remaining, is
republished. Existing files, symlinks and unexpected entries are retained for
operator repair. Baked kernel sources retain the execution cache's sticky-parent
protection; durable store checks stay strict. Both verification and sealed staging
require a source image to be named `vmlinux.bin` when reading that source;
another filename receives an explicit layout error. Reusing a complete sealed
cache copy does not consult the unused source's filename.

Cache publication and cache recovery have distinct outcomes:

| Observed cache state | Admission and recovery |
| --- | --- |
| Missing target, valid source | Copy while checking SHA-256 and size, seal, then compare the compiled verity measurement. Fresh publication under the namespace lock needs no second SHA-256 pass. |
| Valid unsealed file | Check its size and SHA-256 through the opened descriptor before sealing that inode. Existing readers and hard links retain it. |
| Corrupt or truncated unsealed file | Refuse the binding without enabling verity. The file remains mutable for repair from an approved source; a changed file invalidates inventory. |
| Correct sealed file | Measure and reuse it without rehashing content. The original source need not remain present. |
| Wrong sealed measurement or unsafe ownership | Refuse the binding. Repair requires replacing the invalid copy or correcting its protection; there is no legacy downgrade. |
| Empty planned directory skeleton | Reclaim and republish it. Partial files, links and unexpected entries are retained for explicit repair. |
| Transient staging I/O | Retain the failure and retry discovery on an inventory query after the backoff, or earlier when watched paths change. |

Equivalent baked kernel labels are alternatives for one artifact identity. A
verified copy produces one usable component without an unavailable entry. Only
exhausting the alternatives records a single failure; a transient alternative
keeps the binding eligible for retry. Managed copies remain authoritative and
are never bypassed by trying baked alternatives.

A failed binding is not advertised as usable. Repeated permanent refusals need
operator repair; retrying a sandbox request alone does not repair cache contents.
There are no mutable verification receipts. A mismatch withdraws the affected
binding and never authorizes another copy with different bytes.

Node-manager supplies locations through the Firecracker adapter's existing binary
path fields. The portable `VerifiedRuntimeLaunch` envelope remains independent of
host paths and mechanisms. The supervisor validates that both locations name the
compiled release layout, checks the same full directory ancestry as staging, and
authenticates bytes against its own cached catalog. No shared location pointer or independent
supervisor environment-variable setting is needed.

Resolution carries the selected environment and each boot component's integrity
into launch preparation. Compiled protected boots reject missing component
protection. Historical unbound custom-kernel execution remains a deliberate legacy
path, with a warning sampled once per minute per process and a
`node_mgr.execution.legacy_source_launch` counter for every successful launch; it does
not claim complete-environment integrity. Adoption of a resident VM skips cold-launch
integrity admission and does not count as a legacy launch. The supervisor validates
request identity and the persisted execution fence before adoption; it consults the
current catalog and source layout only when spawning a new process. An unknown
catalog reference therefore blocks a new launch without blocking recovery of an
already resident execution. Adoption acknowledges the request contract, not the
resident process's artifact identity. An empty node reports
absent artifacts rather than failed verification.
At launch, runtime-supervisor opens and measures the jailer, then executes that
same descriptor. It also authenticates the Firecracker source in the protected
cache. The stock jailer then copies Firecracker by path; the acknowledgement
does not attest the copied bytes, and path replacement by host root is outside
this guarantee. Explicitly tagged protected-launch admission refusals preserve their
`InvalidArgument` or `FailedPrecondition` status through node-manager and occur
before recording a CPU launch budget or spawning a process. Transient errors
opening, measuring or inspecting sources retain retryable `Internal` status and
carry no admission-refusal tag. Other supervisor failures keep the previous `Internal` mapping. The upstream
jailer retains its independent per-VM executable copy and isolation behavior. Node-manager authenticates the kernel descriptor and links
that inode into the jail. Stacked rootfs, memory and encrypted-snapshot directory
binds are drained by the same bounded cleanup operation for launch recovery and
teardown. Final-component symlinks are inspected without following them and
removed as links. Device teardown and recursive jail removal only follow a successful
drain; an unmount failure, exhausted cleanup budget or failed device teardown
preserves the remaining artifacts and cleanup evidence. Launch refuses reuse,
and teardown returns an error before releasing cleanup ownership. Each attempt
allows at most 32 unmount calls. If all 32 detach mounts, a subsequent attempt
confirms that the path is clear or continues draining it. Durable teardown jobs
retry with backoff.
A start request may need retrying; persistent unmount failures need operator
inspection and repair. Failed cleanup must not permit reuse of an old executable.
Mutable rootfs and snapshot artifacts are outside this sealing contract.

The privileged Rust CI lane runs the sealing and authenticated-link tests on its
disposable btrfs filesystem (`SPACE_NOCOW_TEST_BTRFS_DIR`). With
`SPACE_CI_REQUIRE_TEST_PRIVILEGE` enabled, missing privilege, a missing fixture,
or unsupported fs-verity fails these tests rather than skipping the assertions.

These protections trust the host kernel, node-manager, supervisor and namespace
ownership. They do not establish supply-chain provenance, attest an adopted
running VM, or defend against host-root compromise. Containment and slot-UID
cleanup remain necessary even when source artifacts are sealed.

Deploy supervisor readers before node-manager writers on capable hosts. Verified
requests omit the legacy sandbox ID, so an old supervisor rejects
them before spawning; successful replies acknowledge the environment reference.
Legacy supervisor requests remain accepted with their existing protection. Unsupported
host kernels or filesystems emit an explicit warning and retain SHA-256-verified
legacy execution without claiming the new integrity guarantee. Integrity failures
on a supported backend do not take this fallback. Staging resource or I/O failures
also leave the affected binding unavailable instead of downgrading it. Transient
failures, including disk exhaustion and I/O errors, permit rediscovery on subsequent
inventory requests after a ten-second backoff. Unsafe directories require repair;
changes to any execution-cache ancestor invalidate the observation. Fleet-wide
mandatory enforcement requires a separate capability/admission rollout.

Commissioning must check the running host kernel's `CONFIG_FS_VERITY` and the
execution-cache filesystem, including the loopback cache used without NVMe.
Before rollout, inspect the complete configured cache path with `namei -l` on a
canary of each image type; a symlinked or writable non-sticky ancestor is refused.
Node-manager must retain `CAP_DAC_READ_SEARCH`: linking the authenticated kernel
uses [`linkat(AT_EMPTY_PATH)`](https://man7.org/linux/man-pages/man2/linkat.2.html).
Sealing a kernel binary cannot enable fs-verity in the host running SPACE.
Keep host filesystem and kernel support while sealed cache files remain in use;
reverting a node service does not unseal those files.
Measure warm-cache and cold-cache launches independently; no activation-latency
guarantee follows from the digest ioctl's constant-time behavior alone.

To author additional trusted measurements, first authenticate the artifact's
existing SHA-256, then use `fsverity digest --hash-alg sha256 --block-size 4096`
with no salt. Source/signing changes must preserve existing environment identities
or introduce separately qualified environments. See the
[Linux fs-verity documentation](https://docs.kernel.org/filesystems/fsverity.html).

### Selection and lifecycle

The compiled execution catalog approves complete runtime, kernel and
compatibility bindings independently of downloadable artifact pins. It includes
three Linux/x86_64 bindings: E0 (Firecracker 1.15.1 / Linux 6.1.155),
Firecracker 1.17.0 / Linux 6.1.155, and Firecracker 1.17.0 / Linux 6.18.44.
The 1.17.0 / 6.18.44 binding has reference:

```text
sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4
```

The 1.17.0 / 6.1.155 binding has reference:

```text
sha256:6e97c03b2523af6cc7b8e67bcde3fd89cf756c1958b10c24472aeadd68a32e0a
```

Installation does not select an environment. Node-manager loads an explicit
allowed/default policy at startup. Absent settings retain E0 and historical
unbound contracts; `SPACE_ENV` is not an admission signal. Compiled names resolve
to complete references, while `legacy` separately permits historical contracts.
The policy is rechecked on exact prepare and launch admission.

Artifacts belonging to compiled bindings are removal-protected independently of
deployment mode, installation state and service restarts. Retiring a binding will
require reference-aware fleet and node guards. Keep compatible readers while
bound templates and sandbox records remain. Removing execution permission is not
a workload retirement or artifact deletion operation.

Node-manager verifies each environment's release and kernel from the managed
store and stages its kernel into the execution cache. Cache loss triggers
reconstruction. Missing or corrupt non-default artifacts cannot replace or
withdraw E0. Each compiled binding records its qualified lifecycle support; the
1.17.0 / 6.18.44 and 1.17.0 / 6.1.155 bindings support cold execution and
same-environment memory snapshots. Cross-environment restore requires a
qualified compatible loader; each snapshot retains its original producer binding.
The 6.1.155 kernel keeps its existing artifact identity when paired with 1.17.0.
Non-default snapshot capture carries an environment reference alongside the
sandbox identity on `CaptureVmState`. The serving node verifies the running
sandbox's metadata and resolves its exact binding for restore before capture.
This enforces qualification even if the node restarts between a preflight and
the mutation. During the bridge rollout, fallback to an older route preserves
the bound identity and refuses peers that cannot honor it. After removal of the
legacy routes, all peers and rollback images must meet the rollout gate below.
Retain qualified node versions while their snapshots exist: downgrading a loader
is not a retirement operation.

`StartBuild` and `Create` accept an additive environment input. It contains the
reference and recipe or template; the environment determines the runtime. Legacy
recipe/template/runtime fields are mutually exclusive with that input. An old
builder sees no required recipe; an old sandbox manager sees no required runtime.
Both reject before doing work, including after a capability preflight followed
by a service restart. `GetExecutionCapabilities.environment_selection` advertises
understanding of this protocol, **not support for every environment**. Successful
selected responses must acknowledge the exact reference. Clients never retry a
missing acknowledgement as an unselected request. A preflight on an old service
returns a retryable availability error; rejection after a reconnect remains an
explicit error, with no fallback. A timeout after submission is ambiguous: inspect
the returned job/sandbox identity if available before retrying.

Roll out readers first: node-manager, template-manager, sandbox-manager, then
builder and operator clients. Ordinary unselected E0 traffic continues during
mixed versions. Submit non-default work only after compatible readers are
deployed on the target nodes with the requested policy. Work sent early fails explicitly; it does
not require a cluster-wide outage. A queued selected build retains its reference
in a versioned job payload. A downgraded worker cannot decode that payload as an
ordinary build and silently substitute E0; such work must be inspected/resubmitted
after upgrading, not treated as successful.

A selected cold-boot consumer must match the template's guest, runtime family
and kernel digest. Its runtime version may differ from the template producer.
The template's stored metadata is never relabelled. Create saves the selected
execution with the sandbox and cleanup intent before acknowledging success.
First Start, service restarts and later cold starts retain that exact consumer.
Disk metadata records its producer; it cannot override the saved selection, and
conflicting guest or kernel inputs fail. Snapshot producer evidence stays in the
snapshot manifest, independently of the sandbox's current consumer.

Unpinned scratch builds use the configured default, refusing when it is unusable.
Derived builds and new node-local cold sandboxes prefer the bound source when
allowed and usable. Only a revoked or unavailable source permits fallback to the
newest allowed qualified consumer with the same kernel. Adding an allowed binding
therefore does not upgrade a usable source or contaminate an exact benchmark arm.
Explicit requests never fall back. Existing sandbox stop/start retains its saved
consumer. After a failed restore that saved consumer may be the newly selected
loader; its kernel identity is unchanged and cold admission rechecks policy.
Unselected cluster-directed work, new pool members and warm creates persist the
template's exact binding instead. A warm start restores through a consumer that the node
selects from the base snapshot's producer, so it can differ from that binding,
and Start refuses a base snapshot recorded under another environment, or one
without provenance for a non-default binding. Pool
adoption and clones keep the donor's own binding. Adoption passes over a donor
whose binding the node refuses, leaving it in the pool, and takes the next one
it admits, looking at a build's oldest 32 donors; with none, or when concurrent
adoptions take the rest, Create boots without the pool. A clone also carries its
donor's snapshot producer record (`snapshot/runtime.json`), so its first warm
start restores through a consumer selected from the donor's producer, not the
initial environment. Before Create persists such a
sandbox or adopts a pool donor, and before a clone, the serving node must
resolve that non-default binding exactly.

Retained instance directories grant sandbox-manager traversal without group write.
Normal Start repairs old traversal permissions through node-manager before choosing
an execution source; inspection errors never imply an absent disk. If an interrupted
Start has no runtime and retained-rootfs inspection fails during startup recovery,
only that row is deferred, with a warning; sibling recovery and service admission
continue. Database failures still fail startup admission. The row stays Starting
until an operator repairs the reported filesystem condition and restarts
sandbox-manager to rerun startup recovery. This case has no periodic recovery owner.

For an approved traversal-permission repair, run the following on the affected node
with an updated node-manager. Set `space_checkout` to a matching SPACE source tree
and `sandbox_id` to the full UUID from the warning. Node-manager has no reflection
service, so `grpcurl` needs the explicit proto and its imports. The RPC pins the
existing instance directory and sets its group and traversal mode; a bare
`chmod 2710` does not repair the group.

```bash
space_checkout=/path/to/SPACE
sandbox_id='<full-sandbox-uuid>'
sudo grpcurl -plaintext -unix -max-time 30 \
  -import-path "$space_checkout/proto" -proto node/v1/node.proto \
  -d "{\"sandbox_id\":\"$sandbox_id\"}" \
  /space/run/node-manager/node-manager.sock \
  space.node.v1.NodeManager/PrepareSandboxArtifactDir &&
  sudo systemctl restart space-sbxmgr.service &&
  sudo /space/bin/space-sbxmgr list
```

This uses the standard systemd unit; use the deployment's equivalent restart on
other installations. Only restart after the RPC succeeds. Verify that the row
recovers to Stopped, its retained `instance/rootfs.ext4` is present, and a later
Start preserves its recorded execution binding. If the RPC fails, inspect that
error before restarting. It repairs permissions on a valid directory, not a
regular-file instance, symlink loop, or storage I/O fault; those conditions need
filesystem repair without discarding the retained disk or rewriting its binding.

Unbound derived builds and custom kernel recipes retain their legacy contracts
while the default-enabled `legacy` permission remains present.
Every resolved build, including E0, sends its exact selected environment to Create
so inventory changes cannot cause reselection. Selected and non-default bindings
may use cluster-owned identities and snapshots at create; non-default bindings
may also fill and adopt pools and boot from base snapshots. Paths that capture or load
memory require a binding qualified for memory state. A non-default base snapshot
boots warm only under the environment that captured it; otherwise Create boots
cold. Pool donors are matched by build rather than by binding, so every member
of a build is created with the build's own binding: selected Create can't create pool
members, and a pool clone saves its donor's binding. Selected Create always
boots cold: it never adopts a donor, and it never boots from a base snapshot,
whose warm start restores through whichever loader the node selects. A
requested-ID retry with a selection reuses the existing sandbox only if its saved
binding is the selected one. Because the response acknowledges that environment,
such a retry also needs the node to still admit it; an unselected retry reuses the
row without asking.
Later snapshot and suspend operations still check memory-state qualification
independently.

During mixed versions, an old resolver's missing selection acknowledgement keeps
the pre-existing exact source contract. Scratch builds use ordinary default
resolution, including during rollout. Other errors never cause fallback. A new
non-default binding requires all its readers on the target node to be upgraded;
older sandbox-managers reject cross-binding Create
before execution. Roll out node-manager, then sandbox-manager, then builders.
Managed non-default bindings are omitted from the legacy cluster placement
projection. Historical external runtime and kernel installations remain available
to legacy execution and placement. Fleet placement for complete environments,
policy distribution, cross-environment snapshot compatibility and mixed-environment pools are later
integrations. Use the node-local clients for explicit non-default selection.

See the [runtime benchmark procedure](../scripts/benchmarks/README.md#runtime-environment-benchmark)
for building distinct templates and checking the identities that actually ran.

### Restore inputs

A memory snapshot retains the identity of its guest kernel, but restoring it does
not read a boot kernel image. Node-manager verifies the runtime independently of
boot-component availability. A missing or damaged local kernel therefore blocks
cold boots using that kernel, without preventing restore through an otherwise
usable, approved runtime. Restore never substitutes a local default kernel into
the snapshot's execution evidence. Pause/resume also keeps the resident kernel.

### Execution policy and compatible resolution

Scratch builds call `ResolveExecutionEnvironment` without a reference to resolve
the configured default, then validate the verified runtime and guest locally.
Cold boots from recorded execution and snapshot restores use compatible-selection
requests. The resolver acknowledges the intent and returns a complete verified
consumer. Callers persist the exact result before effects; prepare and launch
recheck eligibility. Old resolvers
without the acknowledgement retain the previous exact contract during rollout,
never authorize an unacknowledged upgrade.

Node-manager reads two startup settings:

```text
SPACE_ALLOWED_EXECUTION_ENVIRONMENTS=e0,legacy
SPACE_DEFAULT_EXECUTION_ENVIRONMENT=e0
```

On x86_64, absent settings mean `e0,legacy` and default `e0`. Compiled names
include `e0`, `fc-1.17.0-linux-6.1.155`, `fc-1.17.0-linux-6.18.44`, and
`ch-54.0-linux-7.2.8`.
`legacy` is a separate permission for unbound historical contracts, not an
environment reference or a selectable default on x86_64. The default must be
allowed. Surrounding whitespace is ignored. Empty, duplicate or unknown names
fail startup. Compiled names resolve to immutable references and cannot approve arbitrary downloads or unqualified
pairs. On aarch64, where the initial Firecracker contract has no compiled
environment descriptor, absent settings mean `legacy` for both settings: builders
and activations retain the existing unbound initial contract. That default report
has no compiled references or default and has `legacy_unbound_allowed=true`.
The compiled `ch-54.0-linux-7.2.8` name can be explicitly allowed on x86-64.
ARM64 release and kernel pins remain installable for qualification tooling, but
no ARM64 CH environment is admitted until its execution qualification passes.
Installing artifacts alone does not add an environment to the effective policy.

The configured default is used only for work without source or exact selection.
A bound source prefers its exact environment, then compatible fallback if it is
revoked, absent or permanently unavailable. A transient exact-source failure
returns the existing retryable inventory error (`Internal`) without choosing or
persisting a new consumer.
Cold fallback preserves the guest kernel; restore fallback
also requires the same compiled state-format profile and qualified memory-state
support. CPU, memory and device checks remain in the restore path. Firecracker
1.17 cannot load E0 format 9 snapshots. Ordering of fallback candidates belongs
to the runtime adapter, and never assembles unqualified runtime/kernel pairs.

Bound activations always require an allowed compiled environment. Unbound
metadata that exactly identifies a compiled contract also obeys its eligibility;
other historical contracts require `legacy`. Existing inventory validation,
custom-kernel matching and the ban on unbound managed non-default runtimes still
apply. A historical restore retains its kernel provenance without requiring a
boot image. Removing `legacy` deliberately disables custom unbound contracts;
audit templates, sandboxes and stored snapshots before doing so. Removing a
compiled environment can strand its bound work without a compatible consumer.
A runtime/kernel incident affecting unbound work also requires removing `legacy`;
revoking a compiled pair alone cannot classify every historical custom contract.
Neither permission affects artifact retention or rewrites producer metadata.

Successful fallback decisions emit a warning with source and consumer references
and increment `node_mgr.execution.consumer_fallback`. This counts resolver
attempts, including retries, rather than completed launches. Transient exact-source
errors follow the existing inventory refresh backoff and the caller's activation
retry/deadline; selection itself does not wait or write a consumer.

Cluster desired policy (below) adds restrictions within these node settings;
the node settings stay the upper bound on what a node may execute.
Until placement consumes environment eligibility and fresh usability evidence,
keep allowed/default policy uniform within each role/host-platform cohort and
verify that builder outputs work on every worker cohort that may receive them.
Keep shared fleet defaults at E0: cluster-owned non-default Create remains refused.
Additional bindings can be exercised through designated node-local paths without
publishing their templates for shared fleet consumption.

`GetExecutionInventory` reports resolved references and the separate legacy permission.
Node-ctl forwards it through periodic publication; the cluster records it with
its own database receipt time in node status, and the admin node response exposes
both. Older cluster writers can refresh inventory without refreshing policy; use
`policy_received_at` to assess policy freshness independently.
Missing, invalid, future-schema or failed policy observations withhold policy
evidence. An unrelated publication does not renew it. Policy is not proof of
installation or usability, and reporting does not implement placement gating.
Deploy the additive migration and cluster readers before node publishers. Inspect
cohort drift and collection freshness before enabling workloads. Upgrades no longer
infer permission from `SPACE_ENV`: absent settings revoke non-E0 bindings even on
Kind and development nodes. Before upgrading a deployment that needs those existing
bindings, configure its explicit allowlist while keeping the default at E0. Do not
expand every development deployment's policy merely because of its name.

### Cluster desired policy

Cluster policy lives in the control-plane tables `runtime_environment_policy`
and `runtime_environment_states`. Services read and write them only through
`pplx-space-runtime-environment-policy-store`. The exceptions are the direct SQL
a cluster administrator runs to reset the policy before rolling back its
migration, and the `ON DELETE SET NULL` cascade that clears a deleted user from
the audit columns. Each environment with a state is
`allowed`, `deprecated` or `blocked`; the first two execute. The policy row names
the cluster default, the legacy permission and a generation that every store
write raises. The store keeps the default `allowed`, replaces it but never clears it,
and refuses a block while the cluster has no default.

Operators change the policy through the admin API under
`/admin/api/v1/runtime-environments`, or `space-admin-cli runtime env`: `PUT
.../default` sets the default and `PUT .../{environment}/state` sets a state,
each with a required reason recorded with the caller. Both write through the
store, so a write it refuses, such as blocking the default, comes back as a 409
that names the rule. The API adds one check of its own, for a write that stops
an environment executing: blocking stops the blocked environment, and the first
default stops every environment with no state, along with any whose state
doesn't execute (a block only direct SQL can leave without a default). Such a
write needs `strand: true` while sandboxes, or template releases still in use,
are bound to an environment it stops, or while any sandbox has no environment
binding, an unreadable one, or no binding record. Unbound legacy sandboxes count
because they execute as the environment they match, which only a node can
resolve, and a sandbox with no observation from its node may be bound to one
that stops. A release is in use while a tag points to it, which creates by tag
resolve to, or an enabled pool config pins it, which keeps creating slots. An
in-use release counts when it's bound to an environment that stops, or when its
environment can't be told, since new creates from it may run there. A superseded
release doesn't count: it strands work only through the sandboxes made from it,
which do count. Nor do sandboxes that can't run again: `cleanup_required` and
`deleting` ones, and failed ones with no evidence the repair loop reads as live
(an observation with a non-failed status, a pid or a node assignment, or an
execution still active or holding a pod IP) that the repair loop, which runs
only on Kubernetes, has nothing to revert to. Later defaults stop nothing, and
nor does blocking an environment that already doesn't execute, so neither is
checked, unless another policy write lands while the block waits for the lock.
Both writes take an optional `If-Match: <generation>` (`--if-generation` in the
CLI) and answer 412 without changing anything unless the policy is still at
that generation, so an operator writing from a stale read can't undo another's
change, such as re-allowing an environment that was just blocked.
`GET .../nodes` compares each node's published policy with the cluster's, a page
of 100 nodes at a time, and `GET .../population` counts sandboxes and template
releases per environment binding. Node status reports the generation each node
applied and whether it's current, but judges conformance by content, so a node
behind on generation still conforms when nothing it would change has changed.

Cluster API serves the policy in the `execution_policy` field of
`GetNodeRuntimeDesiredState` when `SPACE_INTERNAL_API_RUNTIME_DESIRED_STATE_ENABLED`
is truthy. It's off by default, and the field is then absent, so nodes keep their
startup settings. An unset default or legacy permission in a served policy
likewise leaves the node's configured value in place. A policy that can't be read
fails the whole call rather than coming back absent, since absence would send a
node back to its startup settings. The setting is read per cluster API replica at
startup, so while it changes, or during a rolling deploy that changes it, some
replicas serve the field and others don't. Rolling the cluster API back to a
binary without the field has the same effect. Nodes apply the field, so a
missing field sends a node back to its startup settings and lifts any cluster
block: while replicas disagree, a node alternates between the two on each pass.
Turning the setting off, or rolling the cluster API back, lifts every block
for as long as it lasts. Lift blocks deliberately, or keep the cluster API
serving the field, rather than relying on either.
Node-manager combines the policy with its own settings by these rules, so policy
narrows what a node allows, and may pick its default among the environments
left, but never widens it:

- **Allowed set.** With no cluster default, state rows narrow nothing: the node
  keeps its configured allowlist. Since the store refuses a block without a
  default, those rows are normally `allowed` or `deprecated`; only direct SQL
  can leave a block behind. Once the cluster has a default, the node
  executes the intersection of its configured allowlist and the policy's
  executing set. A reference the node doesn't compile or allow is ignored. An
  environment with no state then doesn't execute, just like a blocked one, so
  setting the first default narrows every node to the environments that have
  a state. Mark the environments nodes must keep `allowed` before setting it.
- **Default.** The node adopts the cluster default when it's in that
  intersection and its artifacts are installed and verified, by the same
  inventory resolution a new cold boot uses. Otherwise it keeps its configured
  default if that's still in the intersection, and has no compiled default if
  it isn't: a request for the default is then refused rather than run on an
  environment the cluster removed or one this node can't verify. Unbound work
  that matches an environment still in the intersection keeps running. The refusal is logged,
  returned to node-ctl as the `ApplyExecutionPolicy` outcome
  (`not_allowed` or `unusable`), and visible as a published default that
  differs from the cluster's at the same applied generation. Install the
  environment, through the recipe, before making it the default.
- **Legacy permission.** The node allows unbound historical contracts only when
  both its own setting and the cluster's allow them. The cluster can revoke
  `legacy`, never grant it.
- **aarch64.** No compiled environment qualifies, so the intersection is empty
  and the node has no compiled default whatever the cluster says. Only the
  legacy permission applies, and the cluster can revoke it.

**How nodes apply it.** Node-ctl's runtime-provisioning facet forwards the
`execution_policy` of every `GetNodeRuntimeDesiredState` response to
node-manager's `ApplyExecutionPolicy`, on each periodic tick and each trigger,
before that pass's installations so a block doesn't wait for them. An absent
field, or an older cluster API answering `Unimplemented`, is forwarded as no
policy, and node-manager returns to its startup settings. A failed fetch
forwards nothing and keeps the applied policy. Older node-managers answer
`Unimplemented`, which node-ctl logs and tolerates; they keep enforcing their
startup settings. Node-manager authorizes only root and node-ctl. It refuses no
policy for what it names, since a refusal would leave a block unenforced. It
ignores executing references it can't parse, repeats and ones it doesn't
compile, treats the default as executing whether listed or not, refuses an
unparsable default as not allowed while still narrowing, and, like the
cluster's store, caps no count of references. Applying has its own 10-second
budget. Within it, node-manager waits at most 1 second for the previous
application's record write and bounds its check of the default at 3 seconds. A
slow first inventory discovery refuses a default not yet verified for that
pass, while a slow or retryable check keeps one already verified, so a rescan
can't flap it. A verdict restored from the record counts until a check this
process completes says otherwise. A block is enforced within those 4 seconds of
the call arriving. A hung write leaves the new policy enforced but not
persisted, with an `Unavailable` error, rather than cancelling the whole
application. Applications take effect in arrival order: one that finishes after
a later arrival has enforced its policy is dropped with `Aborted`, so a slow
call can't swap an older policy back in.

Node-manager enforces the narrowed policy for every admission decision: exact
and compatible resolution, launches, preparations and restores. Each re-reads
the policy for its last check, after any inventory discovery or metadata read,
and launches and preparations after their lifecycle permit and sandbox lock, so
a policy applied while it waited still gates it. Later steps, such as the jail
setup after a launch's check, aren't re-checked. A request that passed that
check before an application returned can still complete; a block doesn't recall
it. A restore's state load follows its launch's admission: the launch
re-checks, and the load into the VM it created doesn't. Captures are the
exception: they execute nothing, and the restore a capture feeds is narrowed at
admission, so a capture obeys the node's startup settings, not the cluster
policy, even while the record is unreadable. A blocked environment's VMs can
still be suspended and drained, and restore selection resumes them on an
allowed compatible environment. It's enforced on the resolved environment, so
unbound work matching a removed compiled environment is refused too. Inventory
discovery and artifact publication keep using the startup settings, the
ceiling, so narrowing never needs a rescan. Node-manager persists the received
cluster policy, with whether its default was verified, in
`/space/run/node-manager/execution-policy.json`, through pinned descriptors
that never follow a link, and on restart narrows it against that process's
startup settings before serving. A restart reuses the recorded verdict on the
default; the next application checks it again.

It swaps a new policy in before persisting it, so a failed write doesn't delay
a block in the running process; the failure is returned and the write retried
on the next application. A restart before that retry succeeds restores the
previous record, which may not carry the block, until node-ctl next applies
the policy.

An unreadable record (corrupt, a link, an unknown record schema, or larger than
8 MiB, twice node-manager's 4 MiB gRPC request bound, so any accepted policy
fits) fails closed: node-manager admits no new work, with no allowed
environment, no default and no `legacy`, until node-ctl next applies the
policy, so a damaged file can never lift a cluster block. An application
carrying no policy, such as from a cluster that stopped serving the field,
reopens the node on its startup settings. Running VMs are untouched, and can
still be captured. The next application replaces the file, or removes it if the
cluster serves no policy. While the cluster API is unreachable no application
arrives, so the node stays closed. Node-manager logs the error and reports
`node_mgr.execution.policy_record_unreadable` as 1 on every inventory read, and
the published observation carries `cluster_policy_unreadable` (below) whenever
inventory discovery succeeds; a node whose discovery fails publishes no policy
at all.

The record outlives the process that wrote it, so whatever stops node-ctl
applying leaves node-manager on its last record across restarts: node-ctl
rolled back to a build that doesn't call `ApplyExecutionPolicy`, the facet's
sync disabled, or a node whose role skips the facet. A node-manager rolled
back to a build without this RPC ignores the record and enforces its startup
settings; rolled forward again, it restores that record until the next
application. A build rolled back under a record with fields it doesn't know
treats the record as unreadable and fails closed until the next application.
A node-ctl rolled back to a build without the observation's new fields drops
them when it forwards the inventory, so the cluster withholds that node's
schema 2 policy evidence as malformed.

A changed enforced policy marks node status dirty, so node-ctl republishes it.
The published `ExecutionPolicyObservation` then has `schema_version` 2 and
either `applied_generation`, possibly with allowed environments and no
default, or `cluster_policy_unreadable` with nothing allowed. A node
reporting an unreadable cluster policy has no usable policy evidence, and
placement should avoid it once placement reads node policy. Nothing reads the
flag yet: reporting doesn't gate placement, so such a node keeps receiving work
it refuses with `FailedPrecondition` until node-ctl reapplies the policy. A startup-only node keeps publishing schema 1 with
neither, which the control plane treats as running its startup policy. Deploy
cluster readers that accept schema 2, the cluster API and the admin API, before
node-managers that apply a policy: an older reader withholds schema 2 policy
evidence as a future schema.

Apply each response in full, and never skip one because of its generation.
Rolling the schema back and reapplying it restarts the generation at 0. A node
that accepted only higher generations would then ignore changes until the
generation passed the last value it saw, and one that skipped equal generations
could keep a different policy that happened to share a number. The generation
orders and identifies policy versions only within one schema lifetime. `updated_at`
on either table isn't policy history: deleting a user changes it through the
`ON DELETE SET NULL` cascade.

### Selecting a restore consumer

The snapshot manifest retains original runtime, kernel and state-format identity.
After claiming activation ownership, sandbox-manager saves the selected loader in
the sandbox row before preparing files or allocating a slot. Prepare and Launch
receive that exact consumer. A failed attempt can leave it recorded; cleanup
retains the checkpoint and a later restore attempt resolves again from producer
evidence. A subsequent snapshot describes the runtime that actually ran.

Pause/resume keeps the resident VM. This policy change does not kill running VMs
or add isolated resume refusal: incident-driven stopping, resource reclamation and
resume revocation must ship together. Stop and delete remain available. Operators
must stop affected resident VMs when revoking execution during an incident.

An incompatible-state upgrade requires explicit memory-loss consent, activation
fencing and a preserved matching disk/checkpoint pair. Keep the old pair until
the new cold boot is verified; a failed cold boot must not mutate the only
rollback disk. Retention is not permission to restore a revoked runtime. Retire
the checkpoint through a durable lifecycle operation before later object garbage
collection, never by deleting S3 vmstate and editing lifecycle rows. "Upgrading a
sandbox's runtime" below is that operation for a sandbox with a local disk.

Moving an existing sandbox to another runtime with the same kernel by cold boot
discards its memory, so it needs everything above whether or not the state
format changes; E0 → Firecracker 1.17.0 / Linux 6.1.155 also changes it, from
9.0.0 to 12.0.0. The compiled catalog records which of these same-kernel
cold-boot moves are certified, as upgrade edges. `CERTIFIED_UPGRADE_EDGES` in
`crates/libs/node/runtime/core/src/catalog/environment.rs` lists them, and
`CompiledEnvironment::upgrade_targets` exposes each environment's targets.
Listing a pair is the certification decision. An edge must change the runtime,
and its target must pass the environment-level cold-boot source check
(`check_cold_boot_source`): the same runtime kind, guest platform and a single
boot component with the same kernel bytes. The state format may differ,
because a cold boot loads no memory. A kernel upgrade has no edge and needs its
own qualification. If any listed edge breaks these rules, names an unknown
environment or repeats, the catalog withdraws every edge, logs a warning and
keeps every environment; tests reject such a table before it ships. Two edges
are certified: E0 → Firecracker 1.17.0 / Linux 6.1.155, and its reverse for an
upgrade drill. No edge reaches or leaves 1.17.0 / 6.18.44, whose kernel
differs. The runtime upgrade below follows only certified edges. Compatible
cold-boot selection for a new sandbox or build, described above, uses the same
check and doesn't consult edges. An edge doesn't replace the consent, fencing and preserved pair above,
doesn't move work, and doesn't permit a restore: it carries no memory and
doesn't qualify a loader.

### Upgrading a sandbox's runtime

Sandbox-manager's internal `UpgradeRuntime` moves a stopped or suspended sandbox
to another environment by cold boot. Files survive; memory and processes don't.
Root, a local operator and node-api may call it. A request names a caller-chosen
operation id, the target reference, the operator authorization it runs under, the
blocking cold `Start` to run, and whether the sandbox ends started or stopped.

1. **Fence.** One transaction checks that the sandbox is stopped or suspended,
   that no cleanup or other upgrade owns it, and that the authorization accepts
   memory loss when the sandbox is suspended. It then inserts a
   `sandbox_runtime_upgrades` row recording the authorization, the previous
   consumer and the suspend artifact the sandbox pins. The row is inserted under
   the sandbox's artifact lock, which snapshot rollback, export, warm start and
   checkpoint restore hold while they check for it, so none of them can slip in
   between the check and the fence. The insert is refused if the sandbox's
   state, pin or saved consumer changed since it was inspected. Until the boot
   is verified or the old pair is back, the activation claim refuses every
   other cold start and restore with a retryable `Unavailable`. Export, artifact
   backup, snapshot rollback and snapshot deletion refuse too, and a warm start
   or checkpoint restore refuses before it stages anything into the checkpoint
   directory. Until the old checkpoint is retired, warm start, checkpoint
   restore and snapshot rollback (which can restage the checkpoint) keep
   refusing. Only the upgrade's own boot, once its consumer is saved, passes
   the fence.
2. **Preserve.** Node-manager takes a read-only btrfs snapshot of the instance
   subvolume under the internal `d-operation-runtime-upgrade-<operation>` name,
   which users can't list, delete or roll back to. It holds the rootfs, and for an
   encrypted sandbox `cow.luks` and the suspend volume. The sandbox's
   `metadata.json`, the disk's record of its execution, sits outside the
   subvolume and the boot rewrites it, so sandbox-manager keeps its bytes beside
   it. The checkpoint directory isn't touched. A sandbox without its local rootfs, or an encrypted one without
   `cow.luks`, is refused before the fence: a cold start would otherwise boot a
   fresh template clone.
3. **Select the consumer.** The move must follow a certified upgrade edge from
   the environment the sandbox last ran under: its checkpoint's producer when
   suspended, otherwise its saved consumer. Unbound metadata counts as the
   compiled environment it satisfies, and a stock E0 snapshot from before boot
   components were recorded counts as E0; anything else has no edge and is
   refused before the fence. The target must also keep the disk's runtime kind,
   guest platform and components, kernel included. Its runtime version and snapshot
   format may differ: a cold start loads no memory, and neither sandbox-manager's
   disk binding nor node-manager's cold-boot admission compares state formats.
   The target is saved as the sandbox's consumer in the same transaction that
   moves the row to `booting`. The checkpoint's producer evidence isn't rewritten.
4. **Boot.** The ordinary blocking cold start runs as the caller. It reads the
   saved consumer, and guest readiness verifies the boot: the transaction that
   marks the sandbox started also records the upgrade verified, so a later
   stop or suspend can never make a verified boot look interrupted.
5. **Settle.** After a verified boot, the old checkpoint is retired and the
   preserved copy deleted. Then a request to end stopped stops the execution the
   boot started, never a later one, and only while it's started or paused: the
   stop's cleanup claim checks that state and execution under the fence every
   activation claims under. The row becomes
   `retired`, the record of the authorization. A retired upgrade never touches
   the sandbox's lifecycle again, so replaying it can't stop a later execution.
   A stopped sandbox can still hold the checkpoint of a suspend before its
   stop, which a warm start would load over the disk the boot wrote, so that
   checkpoint is retired too. Retirement goes by identity, under the artifact
   lock: it takes a delete lease on the pinned suspend artifact, or for any
   other checkpoint matches the directory's device, inode and birth time
   (change time where the filesystem records no birth time), so a newer
   checkpoint at the same path is never taken. A directory catalogued since is retired through its catalog row.
   Bytes several rows claim are left, as the reaper leaves them, and the upgrade
   stays `verified`, refusing warm start and restore, until the conflict
   clears: the sandbox's next suspend replaces every older suspend row, or an
   operator resolves it. Moving the bytes off the checkpoint path is the
   durable retirement: a catalogued checkpoint goes to the snapshot reaper's GC
   staging, an uncatalogued one to the suspend-garbage lane, and each lane's
   sweep deletes what the immediate reclaim leaves. An encrypted sandbox's next suspend writes a fresh suspend
   volume, as after any cold start. The checkpoint's remote copy is the
   cluster's to retire. A failed boot waits for its cleanup to return the
   sandbox to its old state, or to stopped if auto-lifecycle stopped a
   suspended sandbox meanwhile, restores the preserved copy over the instance
   without touching any other snapshot, puts the kept `metadata.json` back,
   restores the previous consumer (for a
   sandbox that had none, the execution its disk records, which its next cold
   start would bind anyway) and releases the fence. The sandbox is then restorable
   on its old environment.

A retry with the same operation id resumes from the row: an interrupted upgrade
rolls back and starts again, and a verified one finishes its retirement. A
rolled-back upgrade leaves no row, so a retry after it starts over. An upgrade
that must end stopped leaves the sandbox as it finds it if the sandbox stopped,
suspended or started another execution first. A
sandbox-manager recovery loop settles unfinished upgrades at startup and every
minute without booting or stopping anything: it rolls back, or retires, and
leaves a sandbox it can't settle yet for the next pass. An upgrade that must end
stopped stays verified while its sandbox runs, until its caller's retry stops it
or the sandbox stops some other way. Deleting the sandbox deletes its
upgrade rows; evicting it removes the preserved copy with the rest of its local
presence, and the upgrade fails at its next claim.

### Node runtime provisioning

The whole-node `runtime-provisioning` facet reads
`ClusterApi.GetNodeRuntimeDesiredState` on each periodic inbound tick, even with
no installed artifacts or templates. The response optionally carries a
`RuntimeProvisioningRecipe` of immutable compiled environment references. The
recipe contains neither execution permissions/defaults nor arbitrary download URLs,
paths or artifact hashes; the response's separate `execution_policy` field
(see "Cluster desired policy") carries execution policy, which the facet hands
to node-manager, and installation never reads it. The cluster authenticates the registered node by the
existing origin check before answering.

The current cluster handler returns no recipe. A recipe store and producer must
be connected before deployment can request installations; this endpoint does not
implicitly commission E0. Older cluster services return `Unimplemented`, which
node-ctl also treats as no recipe. Other fetch errors remain visible failures,
including a cluster execution policy the cluster API can't read while
`SPACE_INTERNAL_API_RUNTIME_DESIRED_STATE_ENABLED` is on: that fails the whole
call, recipe included.
Neither absence nor an empty recipe uninstalls anything.

Node-ctl validates the complete recipe's size, reference syntax and uniqueness
before submitting work. It expands host-supported compiled environments into
distinct artifact identities; unsupported references are reported while supported
entries continue installing. An unsupported entry prevents reporting convergence,
but never authorizes unknown artifacts or changes execution selection.

Only valid, digest-matching managed copies satisfy installation intent. Positive
evidence remains useful when unrelated inventory roots have scan issues: uncertain
requirements go through idempotent installation, while an inventory RPC failure
retries on the next tick. Missing identities use the existing `InstallArtifact`
API with at most four simultaneous polling/submission calls. The entire pass has
a shared 30-second deadline; timeout errors identify the stage that exhausted it.

Accepted jobs are durable in rtmgr. Node-ctl remembers their IDs and polls them on
later ticks rather than submitting again. Pending downloads are normal progress,
not facet failures. Terminal failures and successful jobs whose bytes remain
unverified are reported, with five minutes of backoff before a fresh submission.
A success response gets one subsequent inventory pass to observe publication.
Transient submission RPC failures retry on the next pass without terminal-job
backoff; a lost acknowledgement rejoins an active job by identity.
rtmgr owns bounded transfer retries and job deadlines; node-ctl owns retries of
failed jobs for as long as the recipe requires the artifact. Persistent failures
remain visible during backoff and need source, node or recipe repair.

The job-ID memo is bounded by the current recipe and cleared for observed
installations or withdrawn intent. It is not persisted: after a node-ctl restart
or a lost reply, repeated submissions rejoin active rtmgr jobs. A restart can
therefore reset the node-ctl backoff. Replacing or withdrawing a recipe never
cancels accepted jobs; extra artifacts remain installed. No cached intent is
replayed when recipe lookup fails.

Structured progress logs distinguish installing from converged; the existing
facet status carries errors for failed operations and unsupported references.
Unsupported references are included in its `last_error`, published in
`PublishNodeState.node_status.node_ctl`; the facet has no per-element wire keys.
Facet success means a successful pass, not an environment-readiness or
recipe-revision attestation: it can also mean no recipe was assigned. Runtime
inventory and execution-policy observations continue through `PublishNodeState`;
node-manager remains the independent publication and execution authority. The
cluster must compare desired identities against fresh observations, not use the
facet's last-success time as proof that an environment can run. Recipe changes
never change the allowlist, default, placement or running sandboxes.

Node-ctl is authorized to read rtmgr and install approved artifacts, but not to
uninstall or cancel operations. Deploy the rtmgr peer authorization and node-ctl's
`space-rtmgrclients` supplementary group before assigning a recipe. Existing
node processes must restart after direct-host group membership changes. New
node-ctl against old rtmgr reports permission failures without claiming success.
New cluster endpoints and old node-ctl coexist because provisioning is not
mandatory until the policy producer explicitly requires it.

The control-plane integration owns recipe storage and admin mutations, cohort
selection, rollout/convergence tracking, and fresh environment-aware placement.
It must keep required installations separate from allowed/default execution
policy and retain compatibility checks at node admission. Retirement and garbage
collection require a separate reference-aware operation; recipe omission alone
never grants deletion authority.


## Runtime control transport

Environment descriptors and runtime control have separate contracts. Generic
`LaunchRuntime`, `GetVmState`, `CaptureVmState` and `RestoreVmState` routes cover
supervisor process launch, resident state, capture and restore. State artifacts
are named by the qualified runtime profile; the wire format does not require a
Firecracker snapshot. The FC adapter requires exactly `machine-state` and
`memory`, and retains the existing path validation, execution fences, encrypted
volume ownership and restore admission.

`VmStateStorage.directory` and `VmStateStorage.encrypted_volume_mount` are a
narrow exception to node-manager's rule that new RPCs take domain identifiers
and derive host paths. These two fields preserve the existing snapshot storage
contract through the transport migration and remain after legacy routes retire.
They retain peer authorization, execution fences, sandbox-scoped canonical-root
validation, and pinned or otherwise authenticated filesystem handling. They do
not authorize arbitrary host paths or establish a precedent for new path fields.

Prepare/launch at node-manager, pause/resume, cleanup, resource updates, artifact
installation and guest reporting already use shared method shapes. Configuration
payloads remain adapter-owned. Diagnostic fields name the runtime process,
isolation root and outputs rather than a particular VMM. Their protobuf field
numbers and types are unchanged; generated clients and JSON field names use the
new runtime terminology. JSON consumers must regenerate with the migration.

The supervisor is a node-local host adapter interface. Its Linux isolation and
resource inputs describe the host, not the guest OS. An optional containment
executable supports jailer-based and direct VMM adapters; source locators cannot
supply trust or replace the requested execution identity. This does not claim a
Windows or macOS host adapter exists.

The preceding migration release serves both routes and moves FC clients onto the
generic methods. This release removes the old routes and all method fallbacks.
Deploy the migration across every participating service, finish in-flight work,
and update supported rollback images before deploying this release. A rollback
to an old caller requires restoring the migration server first; it is not a
supported mixed-version configuration after retirement. Removing protobuf
reservations does not renumber any live field or change stored identities.
Persisted environment JSON/canonical compatibility is independent and remains.

For the preceding bridge release only, measure the end-to-end VM-state probe
latency and timeout rate against nodes that have not yet been upgraded. That
release tries the generic RPC and then the legacy RPC within the same 500 ms
deadline; this cleanup release calls only the generic RPC. A bridge-release
peer-support cache must not be permanent: a reconnecting channel can reach an
upgraded node where the legacy RPC has been removed. If measurements justify a
cache, bound its lifetime and invalidate it when the legacy method is absent.

When retiring the legacy RPCs, update lifecycle-admission metric queries for
`method:LoadSnapshot` to `method:RestoreVmState`; during that rollout, include
both labels. This is the limiter's metric label, not a change to its capacity
settings. Keep the existing `LOAD_SNAPSHOT` configuration suffix so deployed
limits continue to apply. Generic snapshot RPC timing already uses the generic
method names during the bridge.

## Runtime adapter boundaries

Shared lifecycle code owns sandbox identity, durable activation intent, resource
ownership, selection and cleanup fences. It asks adapters to interpret a
cold-boot profile or resident state; cold-boot JSON belongs to those adapters.
The existing Firecracker restore configuration is still rendered in lifecycle
code. Snapshot support is never inferred from the presence of an executable.

The Linux direct-boot adapters currently render Firecracker and Cloud Hypervisor
configuration. They share validated resource inputs, but own their device model,
configuration keys and feature admission. Both adapters retain independent RX/TX
network buckets. Cloud Hypervisor is configured with one queue pair so additional
queues cannot multiply the assigned traffic budget.

Control is separate from process containment. The `RuntimeControl` interface does
not require a process, jailer or HTTP endpoint. A library-backed runtime can own a
runner and implement that interface without teaching sandbox lifecycle about its
implementation. Socket construction belongs to the standalone Linux adapters;
node-manager owns those host-capable control endpoints. Guest communication is a
separate channel.

Installing a release does not approve it for execution. The compiled
`ch-54.0-linux-7.2.8` environment pairs the x86-64 static Cloud Hypervisor 54.0
executable with its upstream 7.2.8+ ELF kernel with a PVH entry. SHA-256 and
fs-verity measurements are pinned independently. The binding has no memory-state
format or cross-release replacement qualification. Existing ARM64 unbound Firecracker defaults stay
unchanged; CH requires explicit policy permission and an exact environment.

Node-manager owns the CH control API and parses the supported boot device
model into a strict typed configuration before sending it. The child launcher
creates a root-owned, owner-only API listener and passes its descriptor to CH.
Each control connection verifies its peer PID against the registered runtime
and its pinned process lifetime; reconnects repeat that proof. Sandbox-manager receives only the separate
guest communication endpoint. The supervisor opens and authenticates the runtime,
then a fresh single-threaded launcher creates mount, network, IPC, UTS and PID
isolation, enters the VM cgroup, drops privilege, and executes the same authenticated
descriptor. The namespace runner reports the actual VMM PID for guest transport
peer authentication and reaps it on exit. An interrupted boot is cleaned through
the existing fenced sandbox teardown path; boot mutations are never replayed.

Local CH execution supports cold creation, start, stop, delete and resident
pause/resume. Memory capture and restore additionally require the explicit
`ch-54.0-linux-7.2.8-state-v1` environment. It uses the same executable and kernel
but declares the `cloud-hypervisor-snapshot` state format, version `54.0.1`.
The historical cold-only environment retains its reference and cannot capture or
restore memory; existing templates do not silently gain a new contract.

The state environment stores a bounded, versioned machine-state document and a
RAM artifact alongside the rootfs through the existing snapshot pipeline. Restore
preserves guest device IDs, MAC and vsock CID while rebinding host network locators
and resource limits to the new execution. CH also requires the authenticated boot
kernel at restore: its configuration validation and Landlock setup still reference
that artifact. RAM is restored into private memory rather than a writable mapping
of the shared snapshot. Same-environment suspend/restore can use local checkpoints
or S3 materialization; producer identity and execution fences remain mandatory.

Encrypted checkpoints use the existing volume-manager and credential-provider
contract. Node-manager binds the opened encrypted volume into the contained
runtime's state workspace; provider state and RAM staging remain on that volume.
Capture stops the VMM and drains its encrypted bind before sealing. Restore
records guest RAM separately from host cgroup overhead, validates the decrypted
payload against that allocation, and copies RAM into the VMM's private memory.
Workers need the existing encrypted-volume prerequisites, including `cryptsetup`,
and a credential authority that can recover the sandbox's key after restart.
Older plaintext checkpoints can still derive RAM size from their memory file;
an encrypted checkpoint without a recorded guest allocation fails explicitly.

Upgrade all participating services before allowing the new state environment.
Keep those versions available while its snapshots exist. Test repeated restore,
guest-agent readiness, network traffic and manager restart before enabling automatic
suspend/pause. Local base snapshots require the qualified state environment.
Automatic in-node pool refill boots only the template's latest ready Firecracker
build; when only CH builds are cached, their pools remain empty. Runtime pool
capability and clone admission do not override that template-manager selection
restriction. Ballooning, huge pages and live disk-I/O ceiling updates remain
refused. GPU assignment is not implemented.
Template finalization requires acknowledged Stop before copying the rootfs.
Future runtime adapters own their boot and control implementation; the shared
lifecycle retains selection, execution identity, resource fences and cleanup.

### Local x86-64 builder and worker exercise

ARM64 CH admission remains disabled until build, guest traffic, manager restart,
VMM crash and cleanup qualification passes on that host platform. Installing its
artifacts for upstream qualification does not enable SPACE execution.

Use an isolated Linux x86-64 KVM builder/worker with the same SPACE revision across
runtime-manager, supervisor, node-manager, sandbox-manager, template-manager and
build-manager. Deploy the protocol migration before removing old RPCs as described
above. CH authenticates the executable and boot kernel against its compiled catalog.
It uses fs-verity when supported. On unsupported hosts or filesystems, the
single-threaded launcher hashes the opened executable immediately before using
that descriptor with `execveat`; node-manager hashes and links the opened kernel
in its blocking launch work. Files and their ancestry must remain root-protected.
Unverified external sources are refused. A missing or wrong seal on a capable
filesystem is an error, not permission to fall back to SHA-256. When the
execution cache lacks verity, CH publication creates a fresh, SHA-verified
executable copy in the execution cache. Discovery and the launcher therefore inspect the same backend;
the request cannot select a weaker integrity check.

The SHA-256 path relies on immutable root-owned publication and the existing
host-root trust boundary. It re-reads the full executable and kernel at launch;
it does not provide fs-verity's continuous verification of storage reads.
Firecracker's existing unsupported-host behavior is unchanged. Upgrade the
supervisor before exercising CH through the updated node-manager: older
supervisors refuse unsealed sources before spawning. Verify the host can boot the
CH PCI device model before using it for workloads. Nested KVM availability alone does not prove working
virtio block, network or vsock interrupts.

Add `ch-54.0-linux-7.2.8` to the node-manager startup allowlist. The
configuration `SPACE_ALLOWED_EXECUTION_ENVIRONMENTS=e0,legacy,ch-54.0-linux-7.2.8`
keeps the existing E0 default. Leave `SPACE_DEFAULT_EXECUTION_ENVIRONMENT`
unset. Restart node-manager through the normal deployment mechanism. Installing
artifacts does not change either setting.

For persisted-state operations, also allow `ch-54.0-linux-7.2.8-state-v1` and use
that environment's reference for `CH_ENV` throughout the build and create steps.
Build a template with that binding; do not relabel an existing cold-only template.
The runtime and kernel install commands are the same for both environments.

On that node, install and resolve the exact sources:

```sh
space-rtmgr runtimes install cloud-hypervisor-54.0
space-rtmgr kernels install '7.2.8+' --runtime cloud-hypervisor-54.0
# Set CH_ENV to the x86-64 reference from this build's compiled environment catalog.
: "${CH_ENV:?set the installed environment reference}"
space-nodemgr environment --reference "$CH_ENV"
```

Use the normal node-local build API with the recipes from this checkout. Copy the
recipes onto the builder when the checkout is on another machine. The builder
requires its normal seed inputs: containerd, guest `spaced` binaries and builder
SSH trust. Both recipes register their outputs through template-manager; no
metadata editing or database writes are involved.

```sh
space-buildmgr build --recipe templates/cloud-hypervisor/recipes/seed.toml \
  --id ch-seed --environment "$CH_ENV"
# Use the exact hash returned by the seed build; mutable tags require the cluster catalog.
CH_SEED_HASH='hash-returned-by-seed-build'
space-buildmgr build --recipe templates/cloud-hypervisor/recipes/smoke.toml \
  --id ch-smoke --base "ch-seed@$CH_SEED_HASH" --environment "$CH_ENV"
space-sbxmgr create --template ch-smoke --environment "$CH_ENV" \
  --autostart --blocking --auto-lifecycle-off
```

The derived build writes `/opt/space/ch-build-result` inside the guest. Verify that
file through the normal guest command transport, then stop and start the returned
sandbox UUID and verify it again. Delete the test sandbox through the CLI when
finished. A successful seed build alone is insufficient evidence: it constructs
the disk without booting a VM. Qualification requires the derived build and cold
sandbox readiness, including guest command transport and stop/start persistence.

Do not enable CH for fleet placement based on these local results. Environment
placement, multi-node qualification and additional lifecycle capabilities remain
separate admission requirements.

### Runtime exit and rollback

Periodic lifecycle recovery examines bounded batches every 60 seconds. A Started
sandbox whose recorded process has exited is admitted under its cleanup fence to
Stopping, then the durable cleanup worker converges it to Stopped while retaining
the disk. A missing identity, permission error or socket timeout is inconclusive,
not permission to stop another execution. Cleanup retries survive manager restart;
the bounded scan queues fairly for storage capacity without holding a sandbox
cleanup fence, and advances past executions already being cleaned. This recovery
also applies to fenced Firecracker executions; it is not a CH-only deployment
change. Include Firecracker crash recovery in the canary. The interval is not a
per-sandbox deadline on a large node. Live crash/restart qualification remains
required before general multi-tenant enablement.

The contained launcher publishes the child's PID before releasing it to exec,
so losing the supervisor reply cannot leave an executed VMM without registration.
It pivots into its minimal root and detaches the inherited host mount tree;
privileged device creation occurs only in a private root-controlled tmpfs.

Before rolling node-manager or template-manager back to a build without CH,
stop and drain all CH sandboxes and withdraw CH template use. Preserve the
upgraded supervisor when compact execution-cache paths exist. The SHA-256
fallback remains a distinct authenticated integrity mode on filesystems lacking
fs-verity; a capable filesystem with an invalid or missing seal still fails closed.
