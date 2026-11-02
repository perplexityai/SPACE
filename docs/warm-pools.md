# Warm pools

[Documentation](../README.md)

A warm pool keeps booted sandboxes for a template ready on a Kubernetes
deployment. A create takes one of them instead of booting a new sandbox, which
removes most of the create latency. If no ready sandbox matches, the create
still succeeds with a normal cold boot.

For single-node deployments without Kubernetes, see
[Base snapshots](base-snapshots.md).

## Before you start

- A Kubernetes deployment of SPACE, such as the local Kind cluster from
  [Install and configure locally](installation.md).
- An account that is an `organization_admin` in the managing organization.
  On the local Kind cluster, use `root@local`.
- A published template. See [Template releases](#template-releases).

## Create a warm pool

A warm pool is for one **template release** and one VM size (vCPU and memory).

### In the UI

1. Open the SPACE UI (`http://127.0.0.1:3000` on Kind) and sign in.
2. Go to **Templates** and find the release in **Template Releases**.
3. Choose **Create Pool**.
4. Set **vCPU** and **Memory (MiB)** to the size your sandboxes use, and set
   **Target Ready Slots** to how many sandboxes to keep ready. Save.

### With `space-admin-cli`

[Install and log in](../crates/apps/admin/cli/README.md), then run:

```bash
space-admin-cli warm-pool create \
  --template my-template \
  --vcpu 2 \
  --memory-mib 4096 \
  --target-ready-slots 10 \
  --refill-per-tick 5 \
  --max-warming-slots 10
```

This pins the pool to the template's current `latest` release. Use
`--content-hash sha256:...` to pick a different release. `--refill-per-tick`
and `--max-warming-slots` limit how fast the pool fills. SPACE does not check
`--target-ready-slots` against cluster capacity, so budget pool sizes to fit
your nodes.

## Check that it works

```bash
space-admin-cli warm-pool list
```

Wait until `ready_slots` reaches your target. When creates use the pool,
`ready_slots` drops and then refills.

If creates still cold-boot, check these first:

- **Wrong release.** The pool is pinned to one release. After you build a new
  release and move `latest`, creates for `latest` no longer match the old pool.
- **Wrong size.** If a create sets `vcpu` and `memory_mib`, both must equal the
  pool's size. Creates that set neither can use any pool for the release.
- **Native API creates.** On Kubernetes, `POST /v1/sandboxes` creates use warm
  pools only when admission-time claiming is turned on; see
  [Native API creates](#native-api-creates). E2B-compatible creates through
  [ECW](ecw.md) use warm pools without extra setup.
- **Pool too small.** Creates beyond the ready count cold-boot until the pool
  refills. For a burst test, set the target to at least the burst size.

Creates that request a custom `disk_mib` or file-change tracking always
cold-boot.

## Where pool configs are stored

Each warm pool is one row in the `kubernetes_pool_configs` table in the
control-plane PostgreSQL database. A row names a template release
(`template_release_id`), the VM size (`vcpu`, `memory_mib`), the sizing fields
(`target_ready_slots`, `refill_per_tick`, `max_warming_slots`) and `enabled`.
There is at most one row per release, size and `encryptable` value. Running
`warm-pool create` again for the same release and size updates that row.

`managed_by` is `manual` for pools you create, or `autoscaler` for pools that a
[rolling policy](#keep-pools-on-the-newest-release) creates and removes. The
controller keeps the ready sandboxes for each row in `kubernetes_pool_slots`.
Rolling policies are stored in `warm_pool_policies`.

Create and change pools through the UI, CLI or Admin API rather than with SQL.
These check the release and fill in the Kubernetes namespace. A query is useful
for inspection:

```sql
SELECT c.id, r.template_name, r.hash AS content_hash, c.vcpu, c.memory_mib,
       c.target_ready_slots, c.enabled, c.managed_by
FROM kubernetes_pool_configs c
JOIN template_releases r ON r.id = c.template_release_id
ORDER BY r.template_name, c.created_at;
```

## Template releases

Every successful template build publishes a new **release** with a fixed
content hash (`sha256:...`). A tag such as `latest` points to one release and
can be moved. A create for `my-template` uses whatever `latest` points to at
that moment.

Build a new release and move `latest` to it:

- **System templates:** `space-admin-cli template build seed --publish-tag latest`.
  It prints the new `content_hash`. See the
  [system-template release procedure](../templates/firecracker/seed/README.md#build-and-publish-system-templates).
- **E2B SDK builds** through [ECW](ecw.md) move `latest` unless you give other tags.
- **Your own templates:** create the template once, then submit a build:

```bash
curl -sS -X POST "$SPACE_API_URL/admin/api/v1/templates" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"name": "my-template", "description": "Benchmark template"}'

curl -sS -X POST "$SPACE_API_URL/admin/api/v1/templates/my-template/builds" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" -H "Content-Type: application/json" \
  -d '{
    "recipe": {
      "metadata": {"description": "Benchmark template"},
      "origin": {"from_template": {"base": "seed:latest"}},
      "steps": [{"package_install": {"manager": "apt", "packages": ["curl"]}}]
    },
    "publish_tags": ["latest"]
  }'
```

The build returns a `job_id`. Poll
`GET /admin/api/v1/templates/my-template/builds/{job_id}` until `state` is
`succeeded`. The response then includes the new `content_hash`.

The **Template Releases** table in the UI lists every release with its tags.
Existing sandboxes and existing warm pools stay on their release when `latest`
moves.

## Keep pools on the newest release

If you publish new releases often, use a rolling policy instead of creating a
pool for each release. The policy keeps the newest releases of a template
warm and drains pools for releases that are no longer used.

Create one in the UI under **Templates** → **Rolling Warm Pool Policies**, or
with the Admin API:

```bash
curl -sS -X PUT "$SPACE_API_URL/admin/api/v1/warm-pool-policies" \
  -H "Authorization: Bearer $SPACE_API_TOKEN" -H "Content-Type: application/json" \
  -d '{
    "template_name": "my-template",
    "vcpu": 2,
    "memory_mib": 4096,
    "min_ready_slots": 5,
    "max_ready_slots": 30,
    "retain_releases": 2,
    "enabled": true
  }'
```

This keeps at least 5 ready sandboxes for each of the two newest releases,
and up to 30 when demand is higher. A new release's pool fills a short time
after the release is published.

## Native API creates

On Kubernetes, native `POST /v1/sandboxes` creates cold-boot unless
admission-time claiming is on. To turn it on:

1. Set `SPACE_NATIVE_CREATE_POOL_CLAIM_ENABLED=true` on the API service.
2. Have your feature-flag provider return `true` for
   `space-native-create-pool-claim`. Without a provider, the flag is off.

Upgrade every `space-cluster-controller` replica before turning this on; see
[configuration](configuration.md).

## Learn more

- [Operator CLI](../crates/apps/admin/cli/README.md): `warm-pool update`,
  `get` and `--disabled`.
- [Observability](observability.md): the `warm_pool.slots` and
  `warm_pool.create_path.duration_ms` metrics show pool size and hits.
- [Warm-pool design](space/design/warm-pools.md): how slots, claims, the
  controller and rolling policies work.
