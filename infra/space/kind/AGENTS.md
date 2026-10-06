# `infra/space/kind` harness

This Kind cluster runs the production worker image and SPACE binaries against
the real Kubernetes substrate, containerd shim, jailer, and Firecracker on a
Linux/KVM devbox. It is the only local runtime tier: the docker-compose
harness with its stub substrate was deleted on 2026-09-22.

## Load detail by task

- Setup, topology, service identities, production differences, and teardown:
  `infra/space/kind/README.md`.
- Smoke selection and suite-specific contracts: the `space-validation` skill's
  smoke-testing reference and `infra/space/kind/README.md`.
- Loop-device mechanics and sizing: `scripts/lib/space-loop-devices.sh` and
  the production/Kind comparison in `infra/space/kind/README.md`.
- Chart-specific resource and storage constraints: the comments next to the
  values in `infra/space/kind/charts/`.

Read only the source relevant to the change. The scripts themselves own their
current flags, prerequisites, cleanup traps, and fault-injection details.

## API boundary

Tests drive the public REST API at `http://localhost:8080` and the public data
plane. Do not substitute WorkerApi, BuilderApi, node-local RPCs, or direct SQL
for user-visible behavior. Control-plane state flows through ClusterApi and
node-ctl publishing.

Direct state manipulation is limited to faults or state with no public setup
path: the run-scoped ECW API key, stale node tombstones contradicted by
Kubernetes, the opt-in DirectSubstrate bench registration, stranded lifecycle
rows created by the sweeper smoke, and Kubernetes Node patches exercised by the
node-ctl admission smoke. Keep these exceptions inside their existing helpers
or suites.

## Harness invariants

- Every `kubectl` command includes `--context kind-space-local`.
- Keep `bootstrap.sh` and `build-seed.sh` idempotent. Follow
  `docs/installation.md` to provision the harness, then keep the live Tilt
  cluster for iteration instead of rebuilding it for each change.
- Keep the builder node outside `node.kubernetes.io/space-enabled=true`.
- Do not add Postgres access to node services. The existing exceptions are
  credmgr's legacy authority role (`SPACE_CREDMGR_DATABASE_URL`, passed only to
  credmgr as `DATABASE_URL`) and opt-in `directSubstrate.enabled` bench SQL
  registration in `scripts/k8s/node-stack-start.sh`. Normal node registration
  and template publication use ClusterApi; templates are not baked into or
  side-staged onto workers.
- Preserve cache-loop autoclear, the entrypoint's `mount --make-rshared /`, and
  per-jail vhost-vsock device creation. They are required for restart safety and
  snapshot restore.
- `/dev/kvm` permission widening is Kind-only and must not be copied to worker
  images.
- Deployment environment overrides go through the typed harness ops in
  `space_smoke/adapters.py` (capture the standing literals, restore from the
  capture, verify running pods); shell ECW key seeding goes through
  `scripts/tests/lib/ecw-ops.sh`.
- Object-store data remains on its node-local hostPath. Preserve that
  persistence, 40 GiB sparse worker cache, and Kind-specific disk-admission values.

## Operational boundary

Use teardown only for a task-owned cluster. Deleting `.kind-data` invalidates
cluster state; delete the Kind cluster first. `crictl rmi --prune` inside a Kind
node removes unreferenced images from that node, but `docker builder prune` is
host-global and requires explicit approval with its shared-cache impact stated.

If migration startup reports an applied version absent from the binary, use the
recovery procedure in `infra/space/kind/README.md`; it destroys the task-owned
local database and must republish `seed:latest` afterward.

## Validation

Choose the narrowest smoke that exercises the affected public behavior. A live
stack uses `SKIP_BOOTSTRAP=1`. Green Tilt status is readiness evidence, not
runtime proof; report the selected smoke result and any remaining environment
limits.
