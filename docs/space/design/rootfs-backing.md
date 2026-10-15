# Template rootfs backing: opt-in NoCoW

Template-manager accepts `SPACE_TEMPLATE_ROOTFS_BACKING_POLICY=inherit|nocow`.
The default, `inherit`, preserves existing filesystem behavior. On a conventional
btrfs cache this is CoW with data checksums. No deployment or guest I/O limit is
changed by adding this setting.

`nocow` is an experimental choice for **newly materialized template rootfs files**
on Linux btrfs. Template-manager parses it at startup; unknown values fail
startup. An explicit `nocow` also validates the template cache filesystem before
serving requests and fails startup if it is not btrfs. Each materialization still
verifies the inode flag and fails if it cannot be applied.
The startup log records the policy and its scope. Configure template-manager,
not sandbox-manager or node-manager. Before activation, upgrade template-manager,
node-manager, sandbox-manager and volume-manager: contained copies, full-format
local restores, upload staging and shadow reflinks all need the shared copy
helper that preserves and verifies a source's NoCoW flag. Template refresh and
registration staging also preserve source backing and timestamps. A mixed
deployment with old copy consumers can silently fall back to full copies and CoW output.

The decoder sets NoCoW on the empty output before writing expanded bytes. Raw
imports/downloads need one sparse copy in their unpublished staging directory;
re-adopting a file already marked NoCoW does not rewrite it. The raw rewrite
retains zero-block sparsification as well as existing holes.
Other template artifacts, including compressed files and VM memory, retain their
existing policy. Artifact hashes and decoded-digest checks remain in place.
No image rebuild or change to the published template digest is necessary for a
new cache entry. Raw materialization costs extra temporary space and I/O; prefer
the compressed artifact path for canary measurement.

Existing cache hits and recovered entries keep their original backing. This
setting does not convert ready templates, replace immutable parents, rewrite
running guests or assert that all restore formats produce NoCoW backing. A
subvolume snapshot inherits the template rootfs policy; contained file staging
preserves a NoCoW source and clears incompatible inherited compression flags
before attempting a reflink. Unsupported reflinks still use the existing sparse
copy fallback, preserving source isolation. Linux restore/upload copies publish
from a temporary file after completion, including read-only sources and retries.
Mandatory shadow reflinks still fail instead of falling back to a byte copy.

`cargo arch`'s `rootfs_copy` pattern ratchet rejects new direct `cp` launches and
plain `fs::copy` calls outside the shared helper. Because it cannot infer which
path variables contain rootfs images, it conservatively covers other copies too.
The baseline records the existing builder/local paths, non-Linux fallbacks and
non-rootfs copies; increasing their counts fails the check. New baseline entries
are explicit review findings, initially advisory to seed the rule. This is a
line-based lint, not dataflow analysis: aliases, multiline spellings and shell
scripts still require review.

Keep builder template-managers on `inherit` with CoW base caches in this phase.
Build-manager still uses an elevated `cp --reflink=auto` to extract the stopped
build sandbox rootfs; a NoCoW source can fall back to a full CoW copy there.
Do not use NoCoW builder bases until that privileged handoff is qualified.

Linux shared copies reject symlink sources and non-regular destinations, and
replace completed outputs atomically. A crash can leave `.reflink-*` temporary
files; task staging or sandbox-directory cleanup owns their eventual removal.

## Why qualify it

Random buffered overwrites of VM images can make btrfs CoW metadata work delay
both guests and management operations. NoCoW permits in-place overwrite of
exclusive extents. Shared extents still copy on their first write, and a snapshot
shares them again. A private-file overwrite benchmark therefore does not qualify
fresh clones or repeatedly snapshotted guests.

NoCoW disables btrfs data checksums and filesystem compression for these files.
Artifact-level compression remains. Corruption detection is lost even on RAID0;
redundant profiles additionally lose checksum-guided data repair. Corrupt cached
bytes can propagate into guests and later snapshots. Reverting the setting does
not recreate checksums on existing NoCoW extents. Explicit opt-in acknowledges
these costs; this release makes no automatic choice based on a filesystem's RAID
or compression profile.

## First canary and rollback

Use a fresh, isolated worker cache with explicit sandbox placement. Do not prune
or replace a shared cache to enable the experiment. Keep guest rate, bandwidth
and burst settings fixed. Pull a template through the normal API and verify `C`
in `lsattr` on both its rootfs and the sandbox's actual backing file. Record the
filesystem data profile, compression options, artifacts and effective flags;
configuration alone is not evidence of NoCoW. Exercise normal create/start,
suspend/restore, transfer and snapshot paths, inspecting flags after each handoff.
Treat unchanged CoW output as an uncovered path, not a migrated guest.

This is a process-start environment setting, not a per-request or cluster API
switch. Scope it with a separate template-manager workload/release constrained
to labeled canary workers, with non-overlapping node selectors. Setting the
environment on the ordinary fleet-wide workload enables every matching worker.
An isolated testing deployment can be the first scope instead. Existing cached
CoW entries are not evidence that enabled materialization was exercised.

Compare CoW and NoCoW at idle, ordinary developer load and the supported dense
write envelope, including first shared overwrites and repeated snapshot cycles.
Measure lifecycle p50/p95/p99, maxima and errors separately. Require no regression
for create, start, suspend and restore against the same-rate control. Include
raw materialization and any background verification as interference scenarios.
Finite noisy samples can be inconclusive; a nonsignificant difference is not
proof of equivalence. Raising guest IOPS is a separate experiment against the
preceding qualified backing/rate, with the same lifecycle constraint.

Unset the variable or set `inherit` to stop requesting NoCoW for new
materializations. Already cached NoCoW files and descendants remain NoCoW.
Rollback does not mutate their bytes, UUIDs or parent relationships. Use the
following one-time recovery procedure, rehearsed in testing before production
activation, rather than introducing concurrent cache generations:

1. Set `inherit` on the affected template-manager workloads and restart them.
2. Stop placement and warm-pool claims on the affected workers. Cordoning prevents
   new Kubernetes scheduling, but allow reconciliation to retire unclaimed warm
   slots and resolve in-flight claims; verify no new sandbox is admitted there.
   Quiesce admissions during this fence if the deployment cannot enforce it per
   worker. Do not delete active guests or prune their template parents.
3. Bring up replacement workers with fresh caches, normal CoW directory policy
   and `inherit`. Verify their template files actually have CoW backing.
4. Preserve each workload through a qualified snapshot/restore transfer to those
   workers. Verify restored bytes, actual backing, successful resume and required
   retained artifacts before releasing its source. Local reflinks preserve NoCoW;
   turning the setting off does not convert them. Full-file downloads into a
   fresh CoW cache do not carry inode flags; incremental receives require a
   verified CoW parent. Stop if the chosen route fails either check.
5. Retire an affected worker only after its workloads are recovered and retained
   artifacts no longer depend on local data. Do not clear NoCoW on populated files.

This recovery preserves data relationships; it does not undo any undetected
corruption that occurred while checksums were absent. It is a procedure to
qualify, not a claim that replacement alone migrates every restore format safely.

## Migration boundary

New template content provides a gradual rollforward path on existing workers.
Publish a build with a **new content hash**, then ensure the applicable warm pools
and new creates use that release. Autoscaled pools select releases through their
retention policy and observed demand; manually pinned consumers need updating.
Merely publishing another release record for the same content does not replace
its cached file. Retained releases,
claimed slots, existing guests and snapshot dependencies can keep old CoW entries
alive; retirement follows the normal retention and eviction rules. Release
publication alone does not move every consumer or guarantee complete adoption.
Worker replacement with fresh caches is a backstop for persistent old entries,
not a prerequisite for adopting new template content.

Full-image portable restores bypass the template: the downloaded snapshot rootfs
becomes the instance's source. This setting does not apply to that download, so
on a normal CoW cache these instances remain CoW. Local snapshot copies preserve
their source backing. Incremental receive depends on its actual selected parent;
do not infer the resulting policy from the node's configuration. Inspect the
instance after each restore format before counting that path as migrated.

Automatic cache-generation migration is deliberately absent. Incremental send
requires the instance's physical `parent_uuid` to match the selected local
template UUID. Deterministic `received_uuid` alone is insufficient. Publishing
CoW and NoCoW parents with the same received identity also makes receive-parent
selection ambiguous. Resolve and test generation selection, retention and garbage
collection before introducing a second canonical generation. A successful codec
round trip is not evidence that mixed-generation lifecycle operations are safe.

Default promotion additionally requires real-path compatibility and crash
qualification, lifecycle non-regression, and a soak covering long-lived guests,
retained snapshot chains, extent growth and exclusive space. This first-phase
setting supplies a controlled materialization mechanism; it does not certify
fleet migration or higher IOPS.

## Adoption inventory

Node-manager emits `space.node_disk.rootfs_backing_count` every five minutes,
including an initial census after startup. It checks actual inode flags without
reading rootfs contents. The bounded, read-only census runs on a blocking worker
outside lifecycle requests. The `space` namespace is configurable.

| Tag | Values and meaning |
| --- | --- |
| `kind` | `template`: one canonical rootfs per name/content-hash cache entry; `instance`: local sandbox instance; `suspend_checkpoint`: retained full rootfs; `disk_snapshot`: retained snapshot rootfs |
| `backing` | `cow`, `nocow`: btrfs inode flag; `non_btrfs`: another filesystem; `unknown`: a present entry could not be safely classified |

The telemetry transport attaches `cluster_id` from `SPACE_CLUSTER_ID`; the
collector must retain host identity to distinguish workers. For Datadog, sum
the node gauges by `{cluster_id,kind,backing}` for cluster totals, and add `host`
to inspect uneven adoption. For example:

```text
sum:space.node_disk.rootfs_backing_count{*} by {cluster_id,kind,backing}.fill(last,600)
```

`space.node_disk.rootfs_backing_inventory_success` is 1 for a completed directory
census and 0 on failure. Failures leave prior counts unchanged. Check
`space.node_disk.rootfs_backing_last_success_timestamp_seconds` per host and
exclude evidence older than ten minutes; missing reporters must be shown as
missing, not zero. Successful censuses emit explicit zeroes for empty categories.
An otherwise successful census can still contain `unknown` entries. Count those
separately instead of dropping them from the adoption denominator.

This measures canonical local `rootfs.ext4` entries, including incomplete
checkpoints and stopped instances. It excludes staging directories, alternate
artifact filenames, encrypted device backing and remote artifacts. Absent
rootfs files are not counted. Counts are neither distinct templates across the
cluster nor a count of running VMs. No template, digest or sandbox identity is
added as a metric tag; use release history and node inventory for attribution.

Counts also do not measure write traffic. A few busy CoW guests can still cause
shared-filesystem contention despite a high NoCoW file percentage. Qualifying
the dense-host benefit requires per-node workload and lifecycle measurements;
the current Firecracker latency metrics do not supply write bytes attributed to
backing policy. Such attribution needs per-root-drive counters and inspection of
the actual attached file, with unknown coverage reported. Even a measured high
NoCoW write share would be adoption evidence, not a proven safety threshold.

## Filesystem regression coverage

The privileged Rust CI phase runs the NoCoW tests with
`SPACE_NOCOW_TEST_BTRFS_DIR` set by `ci/scripts/with-nocow-test-filesystem.sh`.
The wrapper creates its own loop image, formats and mounts btrfs, then unmounts
and detaches it on exit. It never formats an operator-provided device. The tests
cover materialization, descriptor staging, full-format restore copying, upload
and shadow staging, and actual backing classification. They are filesystem
correctness tests; the lifecycle performance and canary gates above still apply.
