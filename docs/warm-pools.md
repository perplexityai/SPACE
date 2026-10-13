# Warm pools and base snapshots

[Documentation](../README.md)

A cold sandbox create boots a Firecracker VM from the template's root
filesystem and waits for the guest to report ready. SPACE has two ways to skip
most of that work:

- A **base snapshot** is a memory and VM-state snapshot taken from a booted
  template. A create restores the snapshot instead of booting from scratch.
- A **warm pool** keeps already-booted sandboxes for a template ready ahead of
  demand. A create claims one of them and attaches it to the caller instead of
  building a new sandbox.

How you configure them depends on how SPACE is deployed:

| Deployment | What to configure | Tools |
|---|---|---|
| Kubernetes (including the local Kind cluster) | Cluster warm pools, stored in the control-plane PostgreSQL database | UI, `space-admin-cli`, Admin API |
| Single node driven directly by its node managers | Node base snapshots and node warm pools | `space-tmplmgr`, `space-sbxmgr` |

Both kinds are optional. When nothing ready matches, a create still succeeds:
it falls back to a cold boot. Measure latency with the matching option configured;
otherwise every create takes the cold path.

## Template releases

Warm pools and base snapshots are keyed to a **template release**. Every
successful template build publishes one immutable release, identified by its
content hash (`sha256:...`). Tags such as `latest` are movable names that point
at one release of a template. A create that names `my-template` or
`my-template:latest` resolves the tag to the exact release at create time. A
create that names `my-template@sha256:...` always uses that release.

A build creates a new release, and moves a tag only when you ask it to:

- **System templates** (`seed`, `code-runtime`):
  `space-admin-cli template build seed --publish-tag latest` builds and
  publishes a release, then prints JSON with its `content_hash`. Without
  `--publish-tag latest`, only the immutable digest release is published. See
  the [system-template release procedure](../templates/firecracker/seed/README.md#build-and-publish-system-templates).
- **E2B SDK builds** through [ECW](ecw.md): an untagged build moves `latest`;
  a build with explicit tags moves only those tags.
- **Custom templates through the Admin API**: create the template once, then
  submit a recipe build with the tags to publish:

```bash
curl -sS -X POST "$SPACE_API_URL/admin/api/v1/templates" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-template", "description": "Benchmark template"}'

curl -sS -X POST "$SPACE_API_URL/admin/api/v1/templates/my-template/builds" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "recipe": {
      "metadata": {"description": "Benchmark template"},
      "origin": {"from_template": {"base": "seed:latest"}},
      "steps": [
        {"display_name": "tools", "package_install": {"manager": "apt", "packages": ["curl"]}},
        {"display_name": "setup", "run": {"command": "echo ready > /etc/benchmark"}}
      ]
    },
    "publish_tags": ["latest"]
  }'
```

The build response contains a `job_id`. Poll
`GET /admin/api/v1/templates/my-template/builds/{job_id}` until `state` is
`succeeded`; the response then carries the new release's `content_hash`.
`failed` and `publish_failed` are terminal failures.

To list releases or move a tag after the fact, including back to an
earlier release for rollback:

```bash
curl -sS "$SPACE_API_URL/admin/api/v1/template-releases" \
  -H "Authorization: Bearer $SPACE_API_TOKEN"

curl -sS -X PUT "$SPACE_API_URL/admin/api/v1/template-releases/<release-id>/tag" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"tag": "latest"}'
```

The UI's **Template Releases** table shows the same releases with their tags,
content hashes and warm pools.

Existing sandboxes keep the release they were created from. A manual warm pool
also keeps its release: after a new build moves `latest`, creates for `latest`
miss that pool until you create a pool for the new release or use a
[rolling policy](#rolling-policies-for-frequently-published-templates). A base
snapshot is also built from one specific release, so after a new build, check
`BASE SNAP` again before you measure snapshot restores.

## Cluster warm pools on Kubernetes

A cluster warm pool is a config row that names one immutable template release
(its content hash) and one VM shape (vCPU and memory). The cluster controller
reconciles each enabled config: it creates warming slots, marks them `ready`
once their sandbox is booted, and refills the pool as creates claim slots.

You can manage cluster warm pools in two ways:

- **Manual pool config.** Fixed target for one exact release and shape. The
  controller never changes or removes it. Use this for benchmarks and other
  stable workloads.
- **Rolling warm-pool policy.** Template name and shape, plus bounds. The
  controller generates and retires a pool config for each recent release. Use
  this when you publish new template releases regularly.

All warm-pool administration requires an `organization_admin` session in the
cluster's managing organization. On the local Kind cluster, the `root@local`
fixture account has that role; see
[Install and configure locally](installation.md).

### Choose the shape

A create claims a slot only when all of the following hold:

- The create resolves to the same template release as the pool. Tags such as
  `latest` resolve at create time, so a pool pinned to an older content hash
  does not serve creates for a newer `latest`.
- If the create specifies both `vcpu` and `memory_mib`, they equal the pool's
  shape. If it specifies neither, any enabled pool for that release can serve
  it. Specifying only one of them is rejected.
- The create does not request a custom `disk_mib` or file-change tracking.
- The create's volume-encryption requirement matches the pool. Pools are
  plaintext unless created with `"encryptable": true` through the Admin API;
  encryptable pools serve only encrypted creates.
- The create waits for the sandbox to be ready, or its auto-lifecycle policy
  wakes it on access or on command.

E2B-compatible creates (see [ECW](ecw.md)) do not send a shape and always wait
for readiness, so any enabled pool for the resolved release serves them.

Native `POST /v1/sandboxes` creates publish their sandbox ID before execution,
which a pool slot cannot adopt. They claim a warm slot only when admission-time
claiming is on. Set `SPACE_NATIVE_CREATE_POOL_CLAIM_ENABLED=true` on the API
service, and have your feature-flag provider return `true` for
`space-native-create-pool-claim`. Without a provider, the flag resolves to off.
Every `space-cluster-controller` replica must run a version that understands
claim-carrying creates before you enable this; see
[configuration](configuration.md). Otherwise native creates cold-boot even
when a matching pool is ready.

### Create a pool in the UI

1. Open the SPACE UI (`http://127.0.0.1:3000` on the local Kind cluster) and
   sign in as a managing-organization admin.
2. Go to **Templates**. The **Template Releases** table lists each published
   release with its tags and content hash.
3. On the release you want to keep warm, choose **Create Pool**.
4. Set **vCPU** and **Memory (MiB)** to the shape your creates use, then set
   **Target Ready Slots**, **Refill Per Tick**, and **Max Warming Slots** (see
   [Sizing fields](#sizing-fields)). Leave **Enabled** checked and save.

The release's **Warm Pool** column shows a tag for each pool, labelled
`manual` or `autoscaler`. Select the tag to change the pool's target, shape or
enabled state.

### Create a pool with `space-admin-cli`

Install and log in as described in the
[operator CLI guide](../crates/apps/admin/cli/README.md), then create the pool:

```bash
export SPACE_API_URL=http://127.0.0.1:8080
SPACE_API_TOKEN="$(space-admin-cli login --print-token)" && export SPACE_API_TOKEN

space-admin-cli warm-pool create \
  --template my-template \
  --vcpu 2 \
  --memory-mib 4096 \
  --target-ready-slots 10 \
  --refill-per-tick 5 \
  --max-warming-slots 10
```

Without `--content-hash`, the CLI resolves the template's current `latest`
release when the command runs and pins the pool to that release. Promoting
`latest` later does not move the pool. Pass `--content-hash sha256:...` to pin
a specific release. Running `create` again for the same release and shape
updates that pool rather than creating a second one.

Check progress:

```bash
space-admin-cli warm-pool list
space-admin-cli warm-pool get my-template
```

Each config reports `warming_slots`, `ready_slots` and `claimed_slots`. Wait
until `ready_slots` reaches the target before you start measuring.

To change an existing pool, use `warm-pool update` with an exact content-hash
reference. Tag references are rejected:

```bash
space-admin-cli warm-pool update \
  --template my-template@sha256:<content-hash> \
  --vcpu 2 --memory-mib 4096 \
  --target-ready-slots 20 --refill-per-tick 5 --max-warming-slots 10
```

Add `--disabled` to either command to stop the pool from filling while keeping
its config.

### Manage pools with the Admin API

The UI and CLI call these endpoints under `/admin/api/v1`. Use the Admin API
for fields the CLI does not expose, such as `encryptable` or `disk_mib`.

| Method and path | Purpose |
|---|---|
| `GET /warm-pools` | List manual and autoscaler-generated pool configs with slot counts. |
| `PUT /warm-pools` | Create or update a pool config, keyed by release and shape. |
| `DELETE /warm-pools/{pool_id}` | Disable a pool config; its unclaimed slots drain. |
| `GET /warm-pool-policies` | List rolling warm-pool policies. |
| `PUT /warm-pool-policies` | Create or update a rolling policy, keyed by template and shape. |

```bash
curl -sS -X PUT "$SPACE_API_URL/admin/api/v1/warm-pools" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "template_name": "my-template",
    "vcpu": 2,
    "memory_mib": 4096,
    "target_ready_slots": 10,
    "refill_per_tick": 5,
    "max_warming_slots": 10,
    "enabled": true
  }'
```

Omit `content_hash` to pin the current `latest` release, or set it to pin an
exact release.

### Sizing fields

| Field | Meaning |
|---|---|
| `target_ready_slots` | Ready sandboxes to keep for this release and shape. |
| `refill_per_tick` | Maximum new warming slots the controller starts on one reconcile tick. |
| `max_warming_slots` | Maximum slots booting at once for this pool. Must be at least `refill_per_tick` for a policy. |

Every ready slot holds a booted sandbox with the pool's vCPU and memory. Check
that worker nodes can hold the target alongside your other sandboxes:
Kubernetes scheduling, not the pool config, decides whether slots fit.

For a burst benchmark, set `target_ready_slots` to at least the burst size, so
that the burst is served from ready slots. Claims beyond that size cold-boot
until the pool refills.

### Rolling policies for frequently published templates

A manual pool covers one release, so it stops serving creates once callers move
to a newer `latest`. A rolling policy follows releases instead. It keeps the
newest `retain_releases` releases of a template prepared at one shape. It also
keeps a few older releases warm while they still see demand: at least 10
sandboxes for that exact release and shape in the last 24 hours.

Create a policy from **Templates** → **Rolling Warm Pool Policies** in the UI,
or with the Admin API:

```bash
curl -sS -X PUT "$SPACE_API_URL/admin/api/v1/warm-pool-policies" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "template_name": "my-template",
    "vcpu": 2,
    "memory_mib": 4096,
    "min_ready_slots": 5,
    "max_ready_slots": 30,
    "refill_per_tick": 5,
    "max_warming_slots": 10,
    "retain_releases": 2,
    "enabled": true
  }'
```

| Field | Meaning |
|---|---|
| `min_ready_slots`, `max_ready_slots` | Ready-slot floor and ceiling for each selected release. Demand sizes the pool between them, at about one ready slot per 10 recent sandboxes. |
| `retain_releases` | Newest releases to keep prepared even with no demand. Also caps how many extra older releases demand can add. |
| `refill_per_tick`, `max_warming_slots` | Pacing for each generated pool config. Defaults to 5 and 10 when omitted. |

`min_ready_slots` applies to each retained release. The example above
keeps at least 10 sandboxes booted with no traffic: 5 for each of the two newest
releases.

The controller labels the configs it generates `autoscaler` and drains them
when their release leaves retention. It never changes `manual` configs. To keep
one autoscaler-owned release warm beyond retention, select its pool tag in
**Template Releases** and choose **Detach and Save**. Detaching converts it to a
manual pool with an explicit target.

Preparation is asynchronous. A create that arrives right after a release is
published can still cold-boot before the new release's pool fills.

Disabling a policy, from the UI or with `"enabled": false`, drains every pool
config it generated.

### Verify that creates use the pool

- `space-admin-cli warm-pool list`: `ready_slots` should drop as creates claim
  slots and recover as the pool refills.
- Metrics, if a sink is configured (see [Observability](observability.md)):
  - `warm_pool.slots` gauges slot counts by `content_hash`, `managed_by` and
    `state` (`warming`, `ready`, `claimed`, `bound`, `cleanup_pending`).
  - `warm_pool.create_path.duration_ms` reports each create's `outcome`:
    `warm_hit` served from a pool; `ordinary_miss` and `cold_fallback`
    cold-booted.
  - For native creates, `warm_pool.admission_claim` reports `hit`, `miss`,
    `ineligible` or `disabled`.

If creates keep missing, check the release, shape and eligibility rules in
[Choose the shape](#choose-the-shape). The most common causes are a pool pinned
to an older release than `latest`, and native creates without admission-time
claiming enabled.

## Node base snapshots and warm pools

On a single worker node driven directly with the node-local managers, base
snapshots and warm pools are node-local settings for the template manager. The
commands below run on the worker, where the binaries are installed in
`/space/bin`.

These settings are for nodes that are not managed by a cluster control plane.
On a managed node, reconciliation from the control plane overrides them, and
the CLI prints a warning when that applies. For base snapshots, the
control-plane setting is the **Base Snap** switch in the UI's **Templates**
table. Configure Kubernetes deployments with
[cluster warm pools](#cluster-warm-pools-on-kubernetes) instead.

### Create a base snapshot

Request a base snapshot for a registered template:

```bash
sudo /space/bin/space-tmplmgr base-snapshot --template my-template --state true
```

The template manager boots the template, waits for the guest agent and any
configured template apps, and saves the snapshot. Pass `--vcpu` and
`--memory-mib` to snapshot a specific shape. `--state false` removes the
snapshot.

`space-tmplmgr list` shows `BASE SNAP` for each template; wait until it is
`on` before you measure.

### Create a node warm pool

```bash
sudo /space/bin/space-tmplmgr create-pool --template my-template --boot-mode snapshot --count 5
sudo /space/bin/space-tmplmgr pool-status --template my-template
```

`--boot-mode snapshot` fills the pool from the base snapshot, so create the
snapshot first. `--boot-mode cold` fills it with full boots. `--count` defaults
to 3. `pool-status` prints `current/target ready, pending`. The `POOL` column
of `space-tmplmgr list` shows the same `current/target`. Remove the pool with
`space-tmplmgr delete-pool --template my-template`.

In the UI, a single-node deployment shows a **Pool** column next to **Base Snap** in
the **Templates** table instead of the cluster warm-pool controls.

### Start sandboxes with a boot mode

```bash
sudo /space/bin/space-sbxmgr create --runtime fc --template my-template \
  --boot-mode pool --blocking
```

| `--boot-mode` | Behavior |
|---|---|
| `cold` (default) | Full boot from the template root filesystem. |
| `base-snapshot` | Restore the base snapshot, or cold-boot if none exists. |
| `pool` | Adopt a ready pool sandbox, else restore the base snapshot, else cold-boot. |

A requested mode falls back without failing, so check the `Boot mode:` line in
the create output to see which path ran. `--rootfs-size-mb` requires a cold
boot: `base-snapshot` rejects it, and `pool` falls back to cold.

The lifecycle benchmark in `scripts/benchmarks/bench-sbx-lifecycle.sh` detects
the template's base snapshot and pool through `space-tmplmgr list`. It runs the
base-snapshot and pool cases only when they exist, and otherwise reports them
as skipped.

## Design background

The original design, its slot contract and its fallback rules are recorded in
the [cluster-wide sandbox pool RFC](space/design/cluster-wide-sandbox-pool-rfc.md).
The guide above describes current behavior where the two differ.
