# `infra/space/kind` harness

This Kind cluster runs the production worker image and SPACE binaries against
either control-plane substrate (Kubernetes by default, or Direct; see "Direct
substrate lane" in `README.md`), the containerd shim, jailer, and Firecracker
on a Linux/KVM devbox. It is the only local runtime tier: the docker-compose
harness with its stub substrate was deleted on 2026-09-22.

## Load detail by task

- Setup, topology, node services (pod shapes, rollouts, the host
  runtime-supervisor, the operator shell), service identities, production
  differences, and teardown: `infra/space/kind/README.md`.
- Smoke selection and suite-specific contracts: the `space-validation` skill's
  smoke-testing reference and `infra/space/kind/README.md`.
- What each Kind SPACE node provides before its pods start (btrfs cache,
  cgroup, loop devices, device permissions, layout) and how its image is
  built and distributed: "The node layer" in `infra/space/kind/README.md` and
  `infra/space/kind/node/`.
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
Kubernetes, stranded lifecycle rows created by the sweeper smoke, the
backdated reconcile-queue rows of the reconcile-queue GC smoke's synthetic
nodes, and Kubernetes Node patches exercised by the node-ctl admission smoke. Keep these exceptions inside their existing helpers
or suites.

## Harness invariants

- Every `kubectl` command includes `--context kind-space-local`.
- `scripts/up.sh` is the one bring-up: it creates the cluster and starts Tilt
  on the `Tiltfile`, which owns every build and deploy. Follow
  `docs/installation.md` to provision the harness, then keep the live Tilt
  cluster for iteration instead of rebuilding it for each change; rerun
  `up.sh` rather than starting a second `tilt up`. Keep `up.sh` and
  `build-seed.sh` idempotent.
- Keep the builder node outside `node.kubernetes.io/space-enabled=true`.
- Do not add Postgres access to node services. Credential state belongs to the
  control-plane `space-credmgr-authority` Deployment; each node's
  credential-manager runs as its agent without a database URL. Normal node
  registration and template publication use ClusterApi; templates are not
  baked into or side-staged onto workers.
- Internal RPC (ClusterApi, NodeApi, BuildManager, the credential-manager
  authority) is authenticated TLS, as deployed: do not set
  `SPACE_INTERNAL_RPC_INSECURE_DEV` or plaintext flags in Kind charts.
  `scripts/internal-rpc-tls.sh` owns the cluster's CA and listener Secrets;
  keep Tilt running it before workloads. The one plaintext hop is
  the authority's call to the Kind-only `space-kind-key-broker` fixture, which
  has no TLS listener. The authority reaches the Kind-only STS bridge
  (`space-kind-sts-bridge`), and the bridge the object store's IAM service,
  over TLS from that CA, as the cluster-controller reaches the Kind-only fake
  EC2 endpoint (`space-kind-fake-ec2`). Never ship these fixtures outside Kind.
- Node services run one DaemonSet per service and role from
  `charts/space-<service>-local` on the `space-node-service` library, listed in
  `lib/node-services.sh`, which the Tiltfile and the bring-up read. Each pod runs the unmodified
  production worker image and launch path under its `node-identities.sh`
  identity, with only the privileges, host namespaces, propagation and
  capabilities its service's code needs, each commented with the code that
  requires it. Do not reintroduce a shared pod, an entrypoint that supervises
  several services, or an orchestrated startup order: services tolerate peers
  that are not ready and restart per pod.
- runtime-supervisor is the host systemd unit node-manager's init container
  installs and upgrades through the unmodified `scripts/k8s/node-manager-init.sh`;
  do not run it in a pod.
- The smoke harness execs node-local CLIs and on-node checks only in the
  Kind-only `space-node-debug` shell, and reads logs and env from the owning
  service's pod. Do not add tools, identities or privileges to a service
  container for the harness.
- The kind node provides what a production node image does: the btrfs
  `/space/cache`, cgroup subtree, loop-device headroom, device permissions and
  `/space` skeleton (with the SPACE accounts and the Kind-only vhost-vsock jail
  unit) come from units baked into the SPACE Kind node image (`node/Dockerfile`)
  that sandbox and builder nodes boot, ordered before kubelet. Install nothing
  onto nodes at bring-up; change the image. Its tag is the hash of the files
  listed in `scripts/kind-node-image.sh`, so a file the Dockerfile copies must be
  listed there. Clusters on another node image are recreated, not patched
  (`scripts/check-kind-node-image.sh`). Do not move these duties back into a
  pod. The one exception is the Direct lane, which seeds only the per-node
  identity file (`scripts/register-direct-nodes.sh seed-ids`) and registers
  nodes through the admin API; nothing else is written onto nodes.
- Preserve cache-loop autoclear and the shared `/space/cache` mount (set by
  `space-cache.service`), node-manager's `Bidirectional` view of `/space/cache`
  and `/var/run/netns`, every other pod's `HostToContainer` view of the cache
  (build-manager's mounts must never reach the node's mount table), and
  per-jail vhost-vsock device creation by `space-kind-vsock-jails.service`.
  They are required for restart safety, mount containment and snapshot
  restore.
- `/dev/kvm` and vhost permission widening is Kind-only and must not be copied
  to worker images.
- Deployment environment overrides go through the typed harness ops in
  `space_smoke/adapters.py` (capture the standing literals, restore from the
  capture, verify running pods); shell ECW key seeding goes through
  `scripts/tests/lib/ecw-ops.sh`.
- Object-store data remains on its node-local hostPath. Preserve that
  persistence, 40 GiB sparse worker cache, and Kind-specific disk-admission values.
  The release-build lane (`SPACE_KIND_RUST_BUILD=release`) renders no test hook,
  the reservation hook included; mark a scenario that needs one `test_hooks=`.

## Operational boundary

Use teardown only for a task-owned cluster. Deleting `.kind-data` invalidates
cluster state; delete the Kind cluster first. Tilt prunes
superseded SPACE builds from the node stores (`scripts/prune-superseded-images.py`);
do not run `crictl rmi --prune` inside a node, which also removes SPACE images
Kind cannot pull back. `docker builder prune` is host-global and requires
explicit approval with its shared-cache impact stated.

If migration startup reports an applied version absent from the binary, use the
recovery procedure in `infra/space/kind/README.md`; it destroys the task-owned
local database and must republish `seed:latest` afterward.

## Validation

Choose the narrowest smoke that exercises the affected public behavior. A live
stack uses `SKIP_BOOTSTRAP=1`. Green Tilt status is readiness evidence, not
runtime proof; report the selected smoke result and any remaining environment
limits.
