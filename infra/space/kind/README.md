# Local SPACE on KinD

A multi-node KinD cluster that runs SPACE in the production topology:
api-server on `KubernetesSubstrate` (each sandbox is its own K8s Pod whose
pause container is hijacked by `containerd-shim-space-v2` into a real
Firecracker microVM), the production worker service set on every sandbox
node, a dedicated builder role that runs actual ECW recipes, node registration
by the cluster controller's Kubernetes inventory sync with node state published
through node-ctl → internal-api, templates distributed through the production
publish/pull path, and real S3 snapshot transit via an in-cluster versitygw object store,
with sandbox workers' volume-manager holding no object-store keys: like a deployment's, it
exchanges each transfer's credentials through credential-manager. The
remaining deviations from production are listed in "How it differs from
production" below.

## Default workflow: Tilt

Tilt is the one Kind bring-up. `scripts/up.sh` creates the cluster, stages the
Firecracker vendor tree, starts `tilt up` on the repository's `Tiltfile`
detached in its own session, and returns once the stack is ready, printing its
endpoints; [docs/installation.md](../../../docs/installation.md) walks through
it on a fresh host:

```bash
bash infra/space/kind/scripts/install-prereqs.sh    # kind, kubectl, helm, grpcurl, tilt, go
bash infra/space/kind/scripts/install-fc-vendor.sh  # Firecracker, jailer, guest kernel
bash infra/space/kind/scripts/up.sh                 # creates space-local, starts Tilt, waits
```

"Ready" is what the smoke suites wait for, since both run
`scripts/tests/lib/bootstrap-kind-stack.sh`: Tilt has nothing pending, every
workload in the namespace has rolled, `space-seed-publish` has completed, every
node is published and schedulable, and the API and data plane answer on
`localhost:8080` and `:8090`. Rerun on a live stack, `up.sh` reuses the running
Tilt and returns once it has settled, or fails with the log of any resource
whose update failed. It restarts Tilt when Tilt was started with other values of
the settings Tilt reads from its environment once, at start: the list is
`TILT_SETTINGS` in `up.sh` (the telemetry, Datadog and storage credentials
modes, the prune and parallel-update switches, `SPACE_LOOP_DEVICE_TARGET`,
`SPACE_SEED_OUTPUT_DIR`, the Rust artifact mode and profile,
`SPACE_KIND_RUST_BUILD` and `SPACE_KIND_SUBSTRATE`), each compared with its default, plus the optional
AWS credentials when storage credentials are `static`. Nothing else restarts
it, so a refreshed AWS session token in the default mode keeps the running
Tilt. Before stopping it, `up.sh` checks its own refusals (the timeout, storage
mode, Rust build and profile, substrate, and parallelism, and the cluster's
mounts, node image, layout, Postgres data, substrate and nodes' vendor copies,
after staging the vendor tree as the bring-up does), and the telemetry
configuration and substrate request that the Tiltfile also refuses at load (an
invalid `SPACE_KIND_SUBSTRATE`, or `direct` without static storage credentials).
Tilt's log
is `~/.space-devbox/kind/tilt-space-local.log`, its pid is in
`~/.space-devbox/kind/tilt-space-local.pid`, and `teardown.sh` stops it.

Tilt does not create the cluster itself. `up.sh` starts it with
`--context kind-space-local`, and the `Tiltfile` refuses any other context
before it runs a command. Do not create the cluster by hand: `kind-cluster.yaml` bind-mounts
per-node containerd host directories and the staged vendor tree, which must
exist before `kind create`, and Tilt's bootstrap resources run only after the
cluster is up. `up.sh` performs that preparation first.

Once the cluster is up, Tilt is the everyday loop: it builds any image
that is missing, then watches the tree, rebuilding affected images and rolling
the corresponding workloads without recreating the cluster or its persistent
data. Tilt orders resources only on their first build: on a later update an
API image can roll before that revision's `space-db-migrate` Job has finished,
which a fresh bring-up never does. Every Rust binary comes from one artifact image,
`space-rust-build-kind:local` (the union of the production artifact sets),
owned by `scripts/build-rust-artifacts.sh`: on Linux/x86_64 it runs **one
incremental cargo build inside the production `chef-base` builder image** —
bookworm libc and the `rust-toolchain.toml` pin, exactly what CI compiles with —
against a persistent target dir under `~/.space-devbox/kind/`, then assembles
the image from the staged `/artifacts` layout (no Rust compile as a BuildKit
layer, an incremental compile per edit, no build-cache growth). The assembly
refuses any binary whose required GLIBC symbol version exceeds the runtime
images' libc, which is why the compile does not run with the devbox's own
toolchain. Elsewhere it runs `docker/Dockerfile.rust-build-kind` (one cargo-chef
cook and build, as BuildKit stages). `SPACE_KIND_RUST_ARTIFACTS=incremental|docker`
forces either; `SPACE_KIND_RUST_PROFILE=dev-release` uses the incremental-codegen
profile with debug info for the fastest edit loop. Either way a Rust change
compiles once and the affected runtime images then re-copy their binaries from
it. Both paths take their cargo features from the build stage of
`docker/Dockerfile.rust-build-kind`, which also enables
`pplx-space-common-core/test-hooks`: the test-only environment overrides the
Kind charts set (for example the sweeper timings and the `*_TOKEN_PATH` files)
are compiled in only for these images, and release images never enable the
feature. `SPACE_KIND_RUST_BUILD=release` builds and deploys binaries without
it instead (see [Release-build lane](#release-build-lane)).
Follow build progress at `http://localhost:10350`.
`bash infra/space/kind/scripts/teardown.sh` stops Tilt and deletes the cluster.

If startup reports that the Tilt server on `:10350` could not be verified as
`kind-space-local`, inspect `tilt get cluster default --port 10350 -o yaml`.
A watcher for another cluster must be stopped in its owning session before
starting this stack. An initializing watcher can be retried
once its cluster context is available. The bring-up leaves a responsive unverified
watcher running rather than terminating it.

A watcher that *does* verify as `kind-space-local` can still be bound to a
cluster that was deleted and recreated underneath it: the context name is the
same, only the apiserver's host port changed, and Tilt loaded the old one at
startup. The bring-up (`scripts/tests/lib/bootstrap-kind-stack.sh`)
restarts such a watcher instead of waiting on it, judging it stale when Tilt
reports a cluster connection error or
when its process predates the current `space-local-control-plane` container.
It likewise restarts a watcher that serves this cluster from a different
checkout, or from one deleted since it started, so the code built and deployed
is always the checkout the bring-up ran from; the cluster and its pods stay up.
The same bring-up ends with a Tilt build error, printing that resource's log
(including an error Tilt has settled with on a warm stack, whose old pods still
pass every rollout), or with a failed `space-db-migrate` Job once Tilt has
finished replacing it, printing its log, rather than consuming its timeout — Tilt does not retry a failed build until a
watched file changes or you run `tilt trigger <resource>` — and it returns only
once Tilt has nothing pending and every workload in the namespace has finished
rolling. When `:10350` does not answer, it stops only a `tilt up` its pidfile
names or one started from this checkout, and leaves any other watcher on the
host running. Before it starts Tilt it also stops the self-restarting
`port-forward.sh` supervisors the retired scripted bring-up left on `:3000`,
`:8333` and the telemetry ports, so Tilt's own forwards can bind. `teardown.sh`
also stops a watcher serving this context that its
pidfile does not name, so a `kind-down` cannot leave the stale watcher behind.

### Build identity and provenance

Every image Tilt builds goes through
`scripts/image-provenance.py`, which gives it a build identity when it is
built, not when Tilt starts: the checkout's commit (`git rev-parse --short=12
HEAD`) when the image is what that commit builds, and
`<commit>-dirty-<content>` when the worktree's changes reach it, where
`<content>` is the first 12 hex digits of the image's config digest as built
with the plain commit. An image is what the commit builds when its build's
inputs were the commit's for the whole build, or when its content equals that
of such a build at the same commit, which every one records under
`~/.space-devbox/kind/provenance/clean/`. So after an edit to one service only
the images that edit changes report `-dirty-`, and editing it back returns them
to the commit once the rebuilt image matches the clean one again. That holds
while BuildKit's cache still has the clean build's layers: the config digest
also holds each layer's creation time, so once the cache has dropped them
(Tilt's pruner removes cache unused for six hours; see
[Diagnosing iteration disk pressure](#diagnosing-iteration-disk-pressure)), a
rebuilt image, edited or not, reports `-dirty-` until a clean build records
its content again. That errs towards `-dirty-`, never the other way. A clean
build runs its command once, with the commit, as
before, so CI's clean checkouts build what they did; a dirty build whose
content differs runs a second time with its dirty identity, which rebuilds only
the layers from the identity on in the image's final stage (0.8-1.4 s for
data-plane and worker on a warm cache, 2026-10-20 bench), never a compile or a
package install.

"The commit's for the whole build" is judged from two snapshots of the
build's inputs (`image-provenance.py snapshot`): one taken before the build's
first step (the start of each Tilt build chain), since
the chain compiles Rust and assembles the seed before the image's
`docker build`, and one after that `docker build`. Each snapshot holds HEAD,
`git status` and the stat data of every tracked and untracked file, so an edit
made and reverted during the build still counts. The Rust artifact image
(`space-rust-build-kind:local`) and the seed directory are shared by every
checkout on the host; `build-rust-artifacts.sh` and `build-seed.sh` mark each
run (`lib/shared-build-inputs.sh`), and a build during which another
checkout's run was in progress or started is not certified clean either.

The identity reaches the image as the `SPACE_GIT_SHA` build argument, which
each Dockerfile's final stage writes to `/etc/space/build-git-sha` and the
`org.opencontainers.image.revision` label. The script sets that label itself
only on the UI and seed-publish images, whose Dockerfiles take no argument, so
on every other image the label shows that the argument reached the build.
`pplx_space_observability::git_sha` reads that file, so the identity is what
the startup banner's `git_sha`, the `git_sha` metric tags and the
net-gateway's reported build carry, and what sandbox-manager compares to mark
a net-gateway stale. `service.version` (`SPACE_VERSION`, `DD_VERSION`,
`OTEL_RESOURCE_ATTRIBUTES`) is still the commit read when Tilt or bootstrap
starts, or the experiment's `SPACE_KIND_VERSION` (see
[Shared experiment identity](#shared-experiment-identity)): the charts set
it, and Tilt renders them once per Tiltfile evaluation, not per build. Every
image is built with `--provenance=false`: BuildKit's default attestation
records the build time, so the image ID would change on every build and its
workloads would roll whether or not anything in them changed.

Each build records the image's ID, config digest and identity in
`~/.space-devbox/kind/provenance/<cluster>/<image>.json`. Once Tilt has
settled, the smoke bring-up runs `image-provenance.py check`: for every
container in the `space` namespace that runs a SPACE image it records the image
ID the kubelet reports (the config digest) and that image's revision label in
its node's containerd store, compares them with the last build of the image,
and writes the result to `$SPACE_KIND_RUN_DIR/provenance.json` (by default a
new directory under `~/.space-devbox/kind/runs/`), which a report can cite.
Pods being deleted and Failed pods (evicted or rejected, which no rollout
removes) run nothing and are listed rather than compared. A container on any
other build, such as one left by a Tilt stopped before it rolled a build or one
pinned to an earlier image, fails the bring-up, naming its workload and the
remedy. With `SKIP_BOOTSTRAP=1` the bring-up does not wait for Tilt; it runs
the same check when Tilt has nothing pending or in progress, and skips it with
a warning when Tilt is still updating. To run it by hand:

```bash
python3 infra/space/kind/scripts/image-provenance.py check \
  --cluster space-local --output /tmp/provenance.json
```

Nothing prunes `provenance/` or `runs/`; their files are a few kilobytes each
and can be deleted at any time. Neither the prunes nor `teardown.sh` remove
`image-refs/<cluster>/` either, the references the last build of each image
handed Tilt ([Image delivery](#image-delivery)), a few bytes per image.

### Image delivery

Each image build ends by putting the image onto the nodes itself, with Tilt's
own "Loading image to KIND" step turned off: the "Image delivery" section of
the `Tiltfile` and `scripts/kind-load-image.py`. Tilt's step loaded every image
into all six nodes, and on Docker's containerd image store `kind load` cannot
tell that a node already holds an image (Docker names an image by its manifest
digest, the node's CRI by its config digest), so every Tilt start copied every
image to every node again, changed or not. Instead each build:

- is tagged `<name>:tilt-<the first 16 hex digits of its config digest>`, the
  reference Tilt deploys, so an unchanged image keeps its reference and its
  workloads do not roll;
- is loaded onto each node a pod running it can schedule onto, except a node
  whose CRI already holds that reference at that config digest. A digest that
  cannot be read counts as absent, so a changed image is never skipped.

Where an image's pods can schedule comes from the manifests Tilt deploys:
every manifest goes through the Tiltfile's `deploy_yaml`, which records the
`nodeSelector`, required node affinity and `nodeName` of each pod template by
the images it runs, and a node is a target when it satisfies any of them. In
the default layout the worker image goes to the sandbox, builder and infra
nodes, every other SPACE image to the infra node only, and nothing to the
control plane. The set only errs toward more nodes: taints and tolerations
are ignored, a label key that no node carries yet (before `label-kind-infra`
has run) matches every node, and an image with no recorded pod template, or
a cluster whose node labels cannot be read, gets every node. Nodes keep their
images when a pod is evicted or a Node object is deleted and registers again,
so a drained pod rescheduled within its pool finds its image. A node that
gains a matching label later gets the image at the image's next build. Pods
SPACE creates at runtime are not in these manifests and get no image from this
step; today they all run `registry.k8s.io/pause`.

The load is the last step of each build in Tilt's log, its lines prefixed
`[kind-load]`, ending with how many nodes it loaded and how long it took.

## Template publication authentication

ClusterApi validates projected ServiceAccount tokens with the `space-cluster-api`
audience; static fallback is disabled on that listener. Direct registration by
worker identities is denied. The seed-publish Job has its own operator-only
ServiceAccount. Inventory reports and sandbox creation cannot publish templates,
releases, or tags; this is unconditional and has no compatibility switch.

Each node service runs under its own DaemonSet's ServiceAccount and projects its
own ClusterApi token, and only build-manager's builder account
(`space-builder-build-manager`) carries operator and tenant publisher authority
(`clusterApiAuth` in the internal-api chart). The build path before publication
is reviewed the same way: the builder node-api admits `BuildTemplate` only from
the api's account, which holds both system and tenant build authority, and
build-manager admits only the builder node-api's account. Neither listener
accepts the shared static token, so a worker cannot submit a build through
either (see [Service identities](#service-identities)).

## CI smoke partitions

Kind CI produces one Rust artifact image per run and hands it to six isolated
runners. Main builds it from its commit; a PR reuses main's image when its Rust
inputs are the same (see [Smoke test](#smoke-test)). Each runner boots its own Kind stack, proves the baseline,
and runs its assigned typed scenarios serially. Cluster-wide fault injection
must never run concurrently on a shared stack. Every shard must pass.

The shards prepare their node image, vendor mounts, cluster, and chart-image
preloads while the Rust producer compiles or pulls the image. Application startup remains behind
the verified artifact handoff; preparation never starts Tilt or compiles Rust.
The manifest identifies the repository, source commit, run, producer attempt,
archive digest, and Docker image ID, plus the commit the image was built from
and its Rust input key. Failed-jobs-only reruns may use the nearest
successful producer from the same run, with the same source verification. A
failed producer, expired artifact, or mismatched identity fails closed instead
of compiling separately on each shard.

Overlap needs runner capacity for the producer and six prepared shards: seven
concurrent 16-core runners per run, and an eighth on a main push for Main CI's
`kind_rust_state` (below). This
does not add shard jobs or enlarge the cross-run cache, but runners start
earlier. Verify both producer queue time and main CI completion time after a
workflow change; shorter setup steps alone do not prove a faster critical path.

`scripts/tests/smoke-kind-typed.sh all INDEX/COUNT` partitions the eligible ECW
and native inventories independently, after the normal backend, template, and
quarantine filters. The telemetry and storage credentials mode filters read the
bench and run after partitioning, so on a bench in another mode a shard can be
left with nothing to run; it prints its skips, says so and exits 0. A runner
that names a mode the bench doesn't run (CI names `otel`) filters nothing, so
any shard holding a scenario that declares a mode of that kind fails instead.
`space_smoke/sharding.py` assigns longest scenarios first to the
least-loaded partition, with stable ID/index tie-breaking, then executes each
partition in ID order. Unsharded runs keep their existing order.

The duration hints are median scenario wall times (including fixture setup and
cleanup) over the successful Kind runs that `space_smoke/sharding.py` names,
except where a hint's own comment names another source.
To refresh them, measure matching `=== RUN` / `=== END` timestamps separately
for each API surface across all shards of several successful runs, and take
each scenario's median. They are scheduling
hints only: new or renamed scenarios receive a 30-second estimate and still run;
obsolete hints do not add scenarios. No assertion, timeout, or scenario is removed.
Six shards shorten the serial critical path but duplicate cluster startup and
consume six concurrent runners; queue time remains a separate constraint.

## Release-build lane

Every Kind binary is normally built with the `test-hooks` feature (see
[Default workflow: Tilt](#default-workflow-tilt)), so each environment variable
read through `pplx_space_common_core::env::test_hook` takes effect, and the
charts set several on every run. `SPACE_KIND_RUST_BUILD=release`
(`lib/rust-build.sh`; the default is `test-hooks`) builds, deploys and smokes
the Kind services with no crate's `test-hooks` feature enabled:

- **Build.** `build-rust-artifacts.sh` compiles the default's bin and feature
  lists, in either artifact mode, with both test-hook feature lists of
  `docker/Dockerfile.rust-build-kind` empty and with thin LTO. Turning every
  `test-hooks` feature off also requires leaving the test-support member
  `pplx-space-db-test-fixtures` out of the package selection: the host builds
  pass `--workspace --exclude pplx-space-db-test-fixtures`, and the Docker
  cook names every other member with `-p` (`SPACE_KIND_CARGO_EXCLUDE`), so the
  cook resolves the same features as the build. The lane refuses
  `SPACE_KIND_RUST_PROFILE=dev-release` and never exports to the layer cache,
  whatever `SPACE_KIND_BUILD_CACHE_WRITE` says. The artifact image records its
  build in the `space.pplx.net/kind-rust-build` label, `prebuilt` mode refuses
  an image labelled for the other build, and each Rust runtime image copies the
  label from the artifact image it was built from (`runtime-image-cache.sh`).
- **Charts.** The Tiltfile renders every chart with `rustBuild=release`,
  which drops each hook the charts otherwise set on every
  run: sandbox-manager's `SPACE_SBX_GC_TARGET_FRACTION`,
  `SPACE_SBX_GC_INTERVAL_SECS`, `SPACE_SBX_GC_GRACE_SECS` and
  `SPACE_SBX_GC_IDLE_SECS`, template-manager's `SPACE_TEMPLATE_GC_INTERVAL_SECS`,
  `SPACE_TEMPLATE_GC_GRACE_SECS` and `SPACE_TEMPLATE_PREFETCH_LATEST_N`, and
  `SPACE_NODE_DISK_ADMISSION_ALLOCATION_RESERVATION_BYTES` on the services that
  admit cache allocations, as well as node-manager's `SPACE_LOOP_DEVICE_TARGET`
  when the bring-up passes one. The release-read settings beside them stay.
  Every Rust workload's pods (the API, admin API, internal API, credential
  authority, key broker, data plane, cluster controller, each node service and
  the operator shell) carry the `space.pplx.net/kind-rust-build` label of the
  build they were rendered for, so a change of build rolls them all.
- **What runs.** `scripts/deployed-rust-build.py` reads the build from the
  image each running container of a SPACE Rust image uses, on its node, and
  requires every one of them, and each pod's rendered label, to agree. A pod
  or image without the label predates the lane and counts as `test-hooks`.
  The bring-up (`up.sh` and the smoke bring-up) fails unless every container
  runs the requested build once Tilt settles.
- **Scenarios.** A scenario declared `test_hooks=` (naming the hooks it needs)
  is skipped, and printed as `SKIPPED`, on a bench whose Rust containers run
  the release build. With `SPACE_KIND_RUST_BUILD` set, the smoke bring-up
  restarts Tilt in that build, or with `SKIP_BOOTSTRAP=1` refuses a bench in
  the other one, and the typed suite fails on such a bench.

| Skipped in the release lane | Hooks it needs | Why it cannot pass without them |
|---|---|---|
| `local.template_cache_lru_eviction_reclaims` | the chart's template GC interval and grace, and prefetch latest-N | release GC ticks every 300 s with an 1800 s grace, past the scenario's 900 s budget |
| `local.snapshot_reaper_reclaims_backed_fork_snapshot` | the chart's `SPACE_SBX_GC_*` target, interval, grace and idle | a release candidate is idle for a day before the reaper takes it |
| `local.retention_claim_finish_forward`, `local.retention_restore_source_guard` | the retention sweeper's dry-run, tick and minimum age, which they stage | a release sweeper ticks every 300 s and keeps snapshots younger than an hour |
| `local.rate_limiters_enforce_and_fail_open` | `SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST`, which it shrinks to 2 | draining the release burst of 60 would take sixty real boots |
| `local.leader_election_brownout_keeps_one_leader` | the cluster-controller's lease TTL and liveness budget, `SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_TTL_SECS` and `..._LEASE_FAILURE_GRACE_SECS` | at the release 60 s TTL and 600 s budget, a replica spends its budget long after the outage's bound |

The 40 GiB cache needs no replacement for the reservation hook: release
binaries reserve 8 GiB per pending cache allocation
(`CacheDiskAdmissionConfig::from_env`), and a node reads as full only while
those reservations bring its available space under the floor, the larger of
5 % of the cache and `SPACE_NODE_DISK_ADMISSION_MIN_AVAILABLE_BYTES`, a release
setting the charts keep at 1 GiB. Every other scenario, the concurrent ones
included, runs in the lane.

Bring a cluster up in the lane and run it as CI does (with the admin token and
cold-receipts setup of [CI smoke partitions](#ci-smoke-partitions)):

```bash
SPACE_KIND_RUST_BUILD=release SPACE_KIND_TELEMETRY_MODE=otel \
  DATA_PLANE_URL=http://127.0.0.1:8090 bash scripts/tests/smoke-kind-e2e.sh
SPACE_KIND_RUST_BUILD=release SPACE_KIND_TELEMETRY_MODE=otel SKIP_BOOTSTRAP=1 \
  bash scripts/tests/smoke-kind-typed.sh all
```

On demand, `Kind Release-Build CI` (`.github/workflows/kind-release-ci.yml`,
`workflow_dispatch` only) runs `kind-ci.yml` with `rust_build: release`: its
producer builds the release image in Docker mode, and the six shards check its
label and run the baseline and their partitions. No required check depends on
it and nothing schedules it.

## Direct substrate lane

The harness normally runs the Kubernetes substrate: a pod per sandbox, placed
by kube-scheduler. `SPACE_KIND_SUBSTRATE=direct` (`lib/substrate.sh`; the
default is `kubernetes`) runs the same cluster and images with the API and
cluster controller on the Direct substrate
(`crates/libs/api/src/controllers/substrate/direct.rs`): they place node-local
VMs on the nodes in the Direct node registry and create no sandbox pods. The
selection is per cluster, for its lifetime. Its database holds one substrate's
node and sandbox rows, so the bring-up refuses a cluster running the other one
and names the teardown that switches it.

- **Charts.** The Tiltfile renders `substrate=direct` into `space-api-local`
  and `space-cluster-controller-local`, which set `SPACE_K8S_CONTROL_PLANE`
  to `false`. The default `substrate: kubernetes` renders every chart
  byte-identically to a chart without the value. The controller keeps its
  two replicas: in a pod it contends for the Lease on either substrate, so
  only the holder runs the reconcilers, as on the default lane. The node
  services take `node.substrate=direct` (see below).
- **Node identity.** A Direct node is known by a node UUID: the admin
  registry records the id its node-api reports, and ClusterApi resolves a
  kube node name only to a Kubernetes registration. On this lane the
  node-service charts leave `SPACE_NODE_ID` unset
  (`space-node-service.env.identity`). node-api and node-ctl then read
  `/space/etc/node-id`, mounted on its own (`mount.nodeId`), as on a Direct
  host. sandbox-manager reads it through its own `/space/etc` mount for its
  activation identity only: its restore placement and quarantine recovery
  read only `SPACE_NODE_ID`. The other node services get no node id. Before
  the node services start, Tilt's
  `seed-direct-node-ids` writes a random UUID there on each sandbox and
  builder node that has none (`scripts/register-direct-nodes.sh seed-ids`).
- **Node registration.** Kubernetes node sync doesn't run on Direct. Once the
  API and every node-api serve, Tilt's `register-direct-nodes`
  (`register-direct-nodes.sh register`) registers each sandbox and builder
  node with `POST /admin/api/v1/nodes`: its kube name, its InternalIP as the
  address and `<InternalIP>:9800` as the node-api endpoint, with
  `verify: true`. The API then checks the node-api's id and role before
  writing the `nodes` and `node_direct` rows, and enqueues the node's first
  publish. The address must be the source address of the node's host-network
  ClusterApi calls, which ClusterApi checks against it. The bring-up's
  publication wait (`wait-node-publication.sh`) matches a Direct registration
  by its name.
- **Storage credentials.** The lane runs only with
  `SPACE_KIND_STORAGE_CREDENTIALS=static`, and the bring-up refuses it
  otherwise. In `exchange` mode the credential-manager authority admits a
  node service by its pod's Kubernetes node registration (`node_k8s`,
  `check_node_registration` in
  `crates/libs/credential/mgr/server/src/k8s_identity.rs`). A Direct node has
  none, so the authority refuses every template pull with
  `node_identity_mismatch` and no worker ever holds a template. The same check
  guards the authority's other node calls, such as volume keys and network
  credentials, in either mode.
- **Scenarios.** The typed suite reads the bench's substrate from the API
  Deployment's `SPACE_K8S_CONTROL_PLANE`, runs the scenarios it serves and
  prints the rest as `SKIPPED`; a run whose `SPACE_SMOKE_SUBSTRATE` or
  `SPACE_KIND_SUBSTRATE` names the other substrate fails. On Direct it runs
  the scenarios whose fixture needs no Kind-only Kubernetes wiring (`Ecw`,
  `CommandSandboxFixture`, `NativeHardeningFixture`). A scenario narrows that
  set with `substrates=` only for what Direct doesn't support by contract: the
  fork scenarios, and a scenario that asserts sandbox pods, narrow to
  `kubernetes`. Every Kind scenario serves Kubernetes, so the default lane's
  inventory is unchanged. On the deployed backend, `SPACE_SMOKE_SUBSTRATE`
  names the deployment's substrate; unset, it filters nothing.
- **Defects.** A Direct-only defect is tracked by its issue and fails on the
  Direct lane. Don't quarantine it: a quarantine withholds the scenario on
  every substrate, the required Kubernetes lane included.

Bring a cluster up in the lane, run the baseline, then the typed suite:

```bash
export SPACE_KIND_SUBSTRATE=direct SPACE_KIND_STORAGE_CREDENTIALS=static
bash infra/space/kind/scripts/up.sh
python3 examples/first_sandbox.py
SKIP_BOOTSTRAP=1 bash scripts/tests/smoke-kind-typed.sh all
```

The suite needs no `SPACE_SMOKE_SUBSTRATE`: it reads the substrate from the
bench and checks the exported `SPACE_KIND_SUBSTRATE` against it, as the
bring-up does under `SKIP_BOOTSTRAP=1`. `first_sandbox.py` needs the
environment from [the first-sandbox guide](../../../docs/first-sandbox.md). No
CI job runs the lane.

## Bring it up

### CI layer cache

Kind CI produces the Rust artifact image once per run and transfers that exact
image to every smoke shard. The run-local handoff is the same whether the
producer built the image or reused main's from
`ghcr.io/perplexityai/space-kind-rust-artifacts` (see [Smoke test](#smoke-test)):
each shard verifies its run, source and image ID before use.

New cross-run BuildKit layers go to `ghcr.io/perplexityai/space-kind-build-cache`,
not GitHub Actions' shared cache quota. The three stable tags are
`kind-rust-chef-base`, `kind-grpc-health-probe`, and `kind-rust-build`.
PR builds import anonymously, without uploading layers or receiving package-write
permission. The main-push caller alone grants `packages: write`, logs in with
its job-scoped `GITHUB_TOKEN`, and exports `mode=max` layers. No personal token or
new secret is used. Smoke jobs retain only `contents: read`.

During migration, CI also imports the existing Actions cache scopes, using the
job's exposed Actions runtime. Neither PRs nor main export BuildKit layers to
Actions; the incremental build state (below) is a separate export. This
read-only bridge preserves available cache hits while GHCR is unseeded or still
private, without adding to the shared quota. BuildKit can import both sources;
an Actions cache lookup has a 60-second timeout per target. An unavailable or
evicted cache still means a cold build, not a green build without compilation.

Main cache exports are best-effort. The main workflow also keeps the ten newest
untagged versions of this dedicated cache package and preserves all tagged
heads; cleanup failures are visible warnings and must be investigated to keep
storage bounded. No release package or existing Actions cache is deleted.
If neither source has the layers, builds run cold. Cache failure must never hide
a Rust build or smoke failure. Local builds do not use a registry unless explicitly
configured with `SPACE_KIND_BUILD_CACHE_REPOSITORY`.

Rollout:

1. Merge and let a trusted main run seed the package. The organization must allow
   this repository's Actions token to create it, with repository-linked access
   for publication and retention. Check export/cleanup logs for permission errors.
2. A package administrator must make **this OSS-only cache package public** so
   fork PRs can import anonymously. Packages start private; do not expose private
   repository caches. Verify the three tags can be inspected without credentials.
3. Compare cold and warm runs: prove main exports, PRs import without exports,
   and unchanged dependency layers are cache hits. Confirm retained version counts
   and every smoke shard's result. Do not claim a cache speedup from a cold run.
4. Only after that evidence, remove the Actions import bridge and separately
   retire obsolete Kind Actions cache entries if needed. Other compiler/image
   caches are outside this change; no shared cache is deleted by this rollout.

This removes cache export from the PR build path, not from main, and does not
change smoke scenario coverage, timeout budgets, or runner capacity.

The same package holds the dependency layers of the images each shard's Tilt
bring-up builds, so an unchanged package list, lockfile or download is not
fetched again from Debian, Ubuntu, npm, PyPI, GitHub releases or kernel.org.
Each runtime Dockerfile ends those layers at a named stage before its first
SPACE binary, script or seed (`runtime-deps`, plus `smoke-build` for the API's
smoke environment and `deps` for the UI's `npm ci`), and each stage has its own
`kind-<image>-<stage>-e<epoch>` tag; `infra/space/kind/scripts/runtime-image-cache.sh`
lists them and runs every runtime image build. The seed builder's tag, `kind-seed-builder-e<epoch>`, also holds the
recipe's dated Ubuntu cloud image with its signed checksums, which
`rootfs-builder.sh` verifies again before use. Only the Kind seed build reuses
cached checksums (`SPACE_SEED_REUSE_SIGNED_CHECKSUMS=1`); the production build
manager still downloads the dated `SHA256SUMS` on every run, so its persistent
cache cannot change which image a seed is built from. The copies of a commit's
binaries are never exported.

Only `kind_runtime_image_cache` in `main-ci.yml` writes these tags, after
every Kind shard of that commit has passed; `space-ci` does not require
it, and a failed export leaves the previous layers. It skips the stages of a
Dockerfile whose mirrored base it could not reach, since layers built from
the base's source would match no reader. Runs of successive main commits are
not ordered, so an older run that finishes last can put back its layers for
the stages that changed in between, until the next push exports again.
The shards read the tags with `SPACE_KIND_BUILD_CACHE_REPOSITORY` and build
any missing layer cold. A build that cannot read a layer the cache or a
mirrored base names (a missing or corrupt blob), or whose error names the
cache repository or a mirrored image, is built once more without the cache
and with its bases from their FROM sources.

A layer stays in use while its inputs are unchanged, even one captured while
an archive served a broken package. To retire every runtime and seed builder
layer, bump `SPACE_KIND_LAYER_CACHE_EPOCH` in
`infra/space/kind/lib/build-cache.sh` and merge: readers move to the new, empty
tags at once and build cold until main's next export has rebuilt each layer
from upstream. Pull requests based on an older main keep reading the old tags
until they rebase.

The seed's in-chroot `apt-get update` and package installation still reach the
Ubuntu archive on every seed build, and `|| dpkg --configure -a` lets a seed
build complete without its packages when that installation fails.

A layer's key depends on where its base image came from: a base taken from the
GHCR mirror and the same base pulled from its source give different keys. Main
exports with the mirror, so a local bring-up reads main's layers only with
`SPACE_CI_IMAGE_MIRROR=ghcr`, read access to the mirror and cache packages
(`docker login ghcr.io` while they are private), and
`SPACE_KIND_BUILD_CACHE_REPOSITORY=ghcr.io/perplexityai/space-kind-build-cache`.
A registry on the loopback interface (`localhost:<port>/<name>`) is accepted
for trying the cache locally. Without these, or when the cache or the mirror
cannot be reached, builds fetch from upstream as before.

### Scripted steps

The same first-time steps with the validation suites, for CI and scripted fixture setup:

```bash
# 1. user-local prereqs (kind, kubectl, helm, grpcurl, go 1.25)
bash infra/space/kind/scripts/install-prereqs.sh

# 2. Firecracker binary + jailer + vmlinux into the vendor tree
bash infra/space/kind/scripts/install-fc-vendor.sh

# 3. the actual cluster, under Tilt
bash infra/space/kind/scripts/up.sh

# 4. validate
SKIP_BOOTSTRAP=1 bash scripts/tests/smoke-kind-e2e.sh

# 5. build an ECW template, boot it, and run verification commands
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw \
  uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots
```

No AWS credentials are needed anywhere in the default bring-up: the seed
template is built locally by the production seed builder and published
in-cluster (see "Seed template" below).

`up.sh` auto-invokes `install-fc-vendor.sh` unless
`install-fc-vendor.sh --check` finds the vendor tree staged: this Firecracker
release, and the guest kernel at the sha256 the installer pins (set
`SPACE_FC_KERNEL_VERSION` and `SPACE_FC_KERNEL_SHA256` together to change it).
A missing tree, another Firecracker release, a missing kernel or any other
kernel is staged again, so step 2 is optional in practice. A change to the
host tree reaches only nodes created after it: each node copies the tree on
its first boot and keeps its copy ([The node layer](#the-node-layer)), so on
a reused cluster the bring-up refuses nodes whose copy differs and names the
remedy, recreating the cluster (`scripts/check-node-vendor.sh`). Before
anything else it validates the telemetry selection and the console
password, checks for the tools and a reachable Docker daemon, and warns when
either disk is short (see [Diagnosing iteration disk pressure](#diagnosing-iteration-disk-pressure)).
It then creates the kind cluster (if missing) with the SPACE Kind node image
on the sandbox workers and builders (see [The node layer](#the-node-layer)),
and starts Tilt, which labels nodes, installs the local observability backends
when the `otel` profile is selected, installs the containerd shim on the
sandbox workers, applies the `space` and `sandboxes` Namespaces +
`RuntimeClass` and the sandbox namespace's NetworkPolicies, builds every image
and loads it into the nodes that run it (first run is the long step; the
incremental build reuses its target dir and the Docker build is
cargo-chef-cached after that),
and applies every chart rendered with `helm template`. `up.sh` waits for all
of that, then verifies the Datadog Agent when a Datadog backend is selected.

### Metrics (opt-in local backend)

The explicit `otel` profile installs Collector → Prometheus to receive SPACE
OTLP metrics; see [the metrics runbook](metrics/README.md) for queries and
independent release management. Synthetic validation workloads are separate
and opt-in. Grafana is also installed by this profile but remains an independent
release, not a requirement for ingestion or query validation. These Kind
defaults do not change shared deployments or reconfigure an existing
Datadog Agent.

### Logs (profile-selected collection)

The `otel` profile uses [Alloy → Loki](logs/README.md) for selected SPACE
container and existing Kind node journal sources. It excludes the managed SPACE
producer Pods from Datadog log collection, including when an Agent remains
installed. The native profile pauses only the owned Alloy collector, retaining
Loki and its data. Builder log exclusions remain in both collection paths.

### Querying telemetry

In `otel` mode the Tiltfile forwards the selected local backends to localhost, as it
does the UI and the object store, and
restarts a forward whenever its pod restarts. External metrics/traces receivers
have no local query forward; Grafana is forwarded only when it is installed:

| Backend | URL |
| --- | --- |
| Prometheus | <http://localhost:19090> |
| Tempo | <http://localhost:13200> |
| Loki | <http://localhost:13101> |
| Grafana | <http://localhost:13000> |

Each backend's own `local.sh ui` helper uses the same port, for a backend
installed on its own. The typed `local.telemetry_reaches_local_backends`
scenario reads these forwards, so it needs a running Tilt. The suite skips it on a bench running
another mode, but fails if that is not the mode the runner's
`SPACE_KIND_TELEMETRY_MODE` names. Every signal can be read with `curl`:

```bash
now=$(date +%s)

# PromQL. OTLP resource attributes are labels: service_name, k8s_pod_name, ...
curl -sG localhost:19090/api/v1/query \
  --data-urlencode 'query=count by (service_name) ({__name__=~"space_.+"})'
curl -sG localhost:19090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.9, sum by (op) (rate(space_sandbox_api_latency_milliseconds[15m])))'

# TraceQL search, then the whole trace by an ID it returned.
curl -sG localhost:13200/api/search \
  --data-urlencode 'q={resource.service.name="space-api" && status=error}' \
  -d start=$((now - 3600)) -d end=$now -d limit=20
curl -s localhost:13200/api/v2/traces/TRACE_ID_FROM_THE_SEARCH

# LogQL. Lines are JSON; `| json` exposes their fields, such as sandbox_id.
curl -sG localhost:13101/loki/api/v1/query_range \
  --data-urlencode 'query={namespace="space"} | json | sandbox_id="SANDBOX_ID"' \
  -d start=$((now - 3600))000000000 -d end=${now}000000000 -d limit=100
```

Tempo search never returns the newest 5 seconds, whatever `end` says
(`query_end_cutoff`, which the Kind profile lowers from Tempo's 30 s default),
and rejects a window that the cutoff leaves empty, so poll for a trace you just
caused; fetching a trace by ID has no such delay.
For tag values use `/api/v2/search/tag/resource.service.name/values`: the v1
route answers scoped names with an empty list. Span IDs in fetched traces are
base64. A Rust service's log line written inside a traced span carries that
span's `trace_id` and `span_id` in lowercase hex, so
`{namespace="space"} | json | trace_id="TRACE_ID"` finds a trace's lines and the
`trace_id` opens it in Tempo; lines also carry `sandbox_id` where the service
knows it. Grafana shows the same three datasources and the packaged
dashboards, and its Loki datasource links each `trace_id` to Tempo.

#### Following one sandbox

Every service names a sandbox by its bare UUID in the `sandbox_id` span
attribute and log field; the native API's `sbx_` id is the same UUID with a
prefix (see [Sandbox identity](../../../docs/observability.md#sandbox-identity)).
From a create response, or the `sandbox_id` a failed smoke scenario prints:

```bash
sbx=sbx_01a1324d-32be-7235-a41c-1586423506c8   # as the API returned it
id=${sbx#sbx_}
now=$(date +%s)

# 1. Logs: every SPACE pod and node journal line for the sandbox, oldest
#    first. A line written inside a sandbox's span carries its sandbox_id even
#    when the message does not name it.
curl -sG localhost:13101/loki/api/v1/query_range \
  --data-urlencode "query={namespace=~\"space|host\"} | json | sandbox_id=\"$id\"" \
  -d start=$((now - 3600))000000000 -d end=${now}000000000 \
  -d direction=forward -d limit=1000 \
  | jq -r '.data.result[] | .stream.service as $s | .values[] | "\($s) \(.[1])"'

# 2. Traces: every trace with a span for the sandbox, in any service. A line
#    from step 1 also names its trace_id, which you can fetch directly.
curl -sG localhost:13200/api/search \
  --data-urlencode "q={span.sandbox_id=\"$id\"}" \
  -d start=$((now - 3600)) -d end=$now -d limit=50 \
  | jq -r '.traces[] | [.traceID, .rootServiceName, .rootTraceName] | @tsv'
curl -s localhost:13200/api/v2/traces/TRACE_ID_FROM_THE_SEARCH

# 3. Metrics carry no sandbox_id (it would be unbounded cardinality): read the
#    node and services the trace names over its time range, e.g. the shim
#    operations on that node (`k8s.node.name` on its spans). Each sandbox has
#    its own shim process, so read its series' last value, not a rate.
curl -sG localhost:19090/api/v1/query \
  --data-urlencode 'query=histogram_count(sum by (op, result) (last_over_time(space_shim_operation_latency_milliseconds{k8s_node_name="NODE"}[15m])))'
```

Each sandbox's net-gateway and spaced-host run on the node and log to its
journal without a systemd unit, from the sandbox's cgroup; the local log
collection keeps those lines by that cgroup, so step 1 finds them in
`namespace` `host` under `service` `space-net-gateway` and
`space-spaced-host`, except on builder nodes, whose sidecars stay
uncollected. A logs release installed before this collection existed keeps it
off until `logs/local.sh up` is passed the logs chart's `profiles/kind.yaml`;
see [Collection and semantics](logs/README.md#collection-and-semantics).
Their spans reach Tempo.

For a native API request, the `x-request-id` response header (`req_...`) is
the `request_id` attribute of its `http.server` span and a field of the log
lines it wrote, and the `traceparent` response header names that span:
its second field is the trace ID to fetch.

### Datadog (optional)

The Kind-only `datadog-agent-local` chart supports two explicit backends:

| `SPACE_KIND_DATADOG_MODE` | Backend | Credential |
| -- | -- | -- |
| `fakeintake` | In-cluster Datadog Fake Intake | Chart-rendered dummy key |
| `saas` | Approved Datadog site | Separate, existing dev-only Kubernetes Secret |
| `off` | Skip Agent reconciliation; leave any existing Agent running | None |

### One observability profile

`datadog` remains the default producer transport. It does not authorize a
Datadog SaaS installation: `SPACE_KIND_DATADOG_MODE` defaults to `off`. Use one
non-secret JSON profile for transport, owned log routing and backend policy:

```bash
SPACE_KIND_OBSERVABILITY_CONFIG="$PWD/infra/space/kind/profiles/observability-otel.json" \
  bash infra/space/kind/scripts/up.sh
# Native transport and owned Alloy quiescence, without installing an Agent:
SPACE_KIND_OBSERVABILITY_CONFIG="$PWD/infra/space/kind/profiles/observability-datadog.json" \
  bash infra/space/kind/scripts/up.sh
```

The loader accepts one flat JSON object of supported string-valued options.
File entries override matching environment variables; it does not deep-merge
multiple files. Copy a profile before customizing it. Do not put credentials in
it. `otel` selects OTLP metrics/traces and local Alloy/Loki logs; `datadog`
selects native metrics/traces and permits Agent collection of the managed Pods.
`off` disables metrics and traces without changing application stdout logging.

Tilt reconciles the profile before changing producer manifests.
With `SPACE_KIND_OBSERVABILITY=auto`, `otel` installs the necessary local
metrics/traces receivers plus Alloy/Loki/Grafana before updating producers.
An explicit per-signal receiver skips that signal's local backend and disables
its local Grafana datasource: an OTLP ingestion URL is not a query URL.
No external Grafana datasource or external Loki provisioning is inferred.
The fixed node-side Collector addresses keep exporter configuration renderable
without a cluster, and exporters retry if a receiver is later restarted.

On a cold OTLP start, image loading and receiver installation happen during
Tiltfile evaluation, before Tilt registers its cluster context. The
bring-up allows its existing `BOOTSTRAP_TIMEOUT` budget (1800 seconds by
default; `SPACE_BOOTSTRAP_TIMEOUT` for `up.sh`) for that initialization, while still requiring the selected context.
An exited Tilt process or failed evaluation ends the wait early with a log
tail. Detached bring-up also waits for this initialization, but not application
builds or rollouts; the timeout is per stage, not a whole-bring-up deadline.
Kind CI runs `install-observability.sh` in a step of its own while the Rust
producer builds, so Tilt's evaluation finds each release unchanged.

Before installing, `install-observability.sh` loads each backend image onto
every node at the linux/amd64 digest `ci/mirrored-images.txt` lists for the
charts' pin, under the tag the Kind profiles name, so the pods start without a
registry pull. GitHub CI takes these images from the GHCR mirror; elsewhere
they come from Docker Hub. A load that fails leaves the pull to the kubelet.

The `off` and `datadog` modes install no local OSS backends:

```bash
# No metrics or traces; pause owned Alloy without deleting its backend.
SPACE_KIND_TELEMETRY_MODE=off bash infra/space/kind/scripts/up.sh

# Native Datadog with a local intake instead of SaaS.
SPACE_KIND_TELEMETRY_MODE=datadog SPACE_KIND_DATADOG_MODE=fakeintake \
  bash infra/space/kind/scripts/up.sh
```

`off` sets `SPACE_METRICS_SINK=off` and `SPACE_TRACE_EXPORTER=off` for every
Rust service and the shim, with no OTLP endpoint or identity overrides, for a
leaner bench. The Kind CI shards explicitly pin `otel`;
the typed `local.telemetry_reaches_local_backends` scenario, in their normal
inventory, polls Tempo, Prometheus and Loki for its own sandbox's trace, shim
metrics and log lines.

For external metrics/traces, set `SPACE_KIND_OTLP_METRICS_ENDPOINT` and/or
`SPACE_KIND_OTLP_TRACES_ENDPOINT` in the same JSON. Each explicit plaintext
OTLP/gRPC URL must be reachable from both Pods and Kind nodes. URLs with
credentials, query/fragment/path, loopback destinations or Helm separators are
rejected. Implicit local destinations use Pod service DNS for applications and
the corresponding fixed Service ClusterIP for the node-hosted shim,
runtime-supervisor, and sandbox-manager's host-sidecar route. The existing
`SPACE_KIND_SHIM_OTLP_ENDPOINT` remains an override for that node-reachable
route for both signals, including node-loopback receivers; the other
application destinations stay unchanged.
Do not combine it with the new global endpoints: mixed configuration is
rejected. Remove the legacy override to configure Pods and host processes together.

`SPACE_KIND_OBSERVABILITY=off` never installs or deletes a backend. In `otel`
mode it requires an existing owned local Alloy/Loki release, resumes its
collector, and verifies any implicit local receivers at their fixed addresses
before changing producers.
A fresh cluster with only external metrics/traces is not a complete log route
and is refused. Existing Grafana configuration is retained in this reuse mode;
it is not rewritten to query external receivers. Native transitions pause
owned Alloy if present and leave all backend data intact.
Before any automatic receiver installation, the log helper verifies the owned
namespace and release chart identity, not equality with the current checkout's
render. Normal `logs/local.sh up` can upgrade a drifted chart or retry a failed
owned release without deleting its data. It retains saved values, including
image pins and storage overrides; adopting new Kind profile defaults is an
explicit `up -f .../profiles/kind.yaml` operation with any custom overrides
applied afterward. Inspect storage changes before doing that.

Pause/resume retain the recorded non-Alloy manifests through a Helm
post-renderer. Resume selects a collector from retained release history with
matching backend resources, or uses the current chart only if those resources
still match. If no compatible collector remains in history, run `up` to
reconcile the owned chart rather than deleting its namespace. The actual
proposed transition must still change only `alloy.enabled` and the known
Alloy DaemonSet. Concurrent revision changes are refused, and Helm's pending
operation locks are never stolen; inspect an interrupted operation and recover
it through Helm, without deleting release records to bypass the lock.

Healthy unchanged receiver renders do not create another Helm revision.
`tilt down` skips receiver reconciliation entirely. A detached bring-up uses
`SPACE_TILT_DETACH_TIMEOUT` for context registration (60 seconds by default,
at most 240); a timeout reports failure but leaves its detached Tilt process
running. Cold initialization can continue in the background, and the normal
blocking bring-up still uses `BOOTSTRAP_TIMEOUT` for its longer readiness
wait.

With implicit local destinations, Rust workloads send metrics to
`http://collector.space-metrics.svc.cluster.local:4317` and traces to
`http://collector.space-traces.svc.cluster.local:4317` over plaintext
OTLP/gRPC. sandbox-manager is the exception: it forwards its endpoints to the
net-gateway and spaced-host processes it launches on the node, outside cluster
DNS, so it uses the shim's node-reachable addresses instead. The profile
projects its per-signal endpoints into the existing
`telemetry.otlpMetricsEndpoint` and `telemetry.otlpTracesEndpoint` Helm values
and the node-reachable destinations, configuring applications, the shim,
runtime-supervisor, and sandbox-manager's host sidecars together. Endpoint and
protocol environment variables are per-signal and emitted only for OTLP
exporters. Logs stay on stdout for Alloy to collect; no log endpoint is needed.

Set `SPACE_KIND_TELEMETRY_MODE=datadog` to return
to native Datadog metrics and traces; pair it with
`SPACE_KIND_DATADOG_MODE=fakeintake` for local capture.

`SPACE_KIND_DATADOG_MODE` independently chooses the owned Agent's backend.
In `otel`, generated Agent configuration excludes SPACE logs and managed
producer Pods carry log-specific exclusion annotations, which take precedence
over Agent inclusion rules. These annotations do not suppress Agent metrics.
An existing foreign Agent is never reconfigured by this profile; its unrelated
Pods, infrastructure and host journal inputs are outside this contract.
The local Datadog chart does not configure equivalent host journal collection.
Existing SaaS consent and duration guards are unchanged, including the explicit
node-local receiver. That override does not redirect the other Pod-DNS routes.
Enabling an Agent may not adopt an unproven existing Agent. The helper recognizes
its prior `helm template` + `kubectl apply` installations by chart/release labels,
the selected Kind identity in Agent `DD_TAGS`, a Secret reference, and matching
identity in the stored last-applied configuration. It does not require a new
ownership label or a Helm release record that this installer never created.
Missing or inconsistent evidence requires an operator ownership review; it is
not automatically adopted. No Secret contents are read for ownership checks.

Profile changes are not an atomic or loss-free handoff. Workload annotations
and process environments take effect on rollout; already-running shims and
host children retain their old environment until normal restart/relaunch.
No user guests are reconfigured. The Kind builder keeps build-manager traces
off and its command-output logs excluded, even in `otel`. The same selectors
reach the normal host services and newly launched net-gateway/spaced-host
children, not arbitrary guest workloads or external EKS/systemd deployments.

There is no dual-publish or non-counter mode. The `otel` mode uses `always_on`
diagnostic trace sampling, not the production sampling policy, and exports the
Rust services' metrics cumulative rather than production's delta, so the
metrics Collector has no idle stream to forget; the shim's stay delta
([Collector requirements](../../../docs/observability.md#collector-requirements)).
Logs remain structured JSON on stdout in every mode; there is no OTLP log
exporter.

`SPACE_KIND_DATADOG_MODE` defaults to `off`, even when `DD_API_KEY` is set.
**`off` and stopping Tilt do not stop an existing Agent's export.** Use the
explicit rollback below. For native Datadog metrics/traces/logs capture with no
telemetry egress to Datadog SaaS:

```bash
SPACE_KIND_TELEMETRY_MODE=datadog SPACE_KIND_DATADOG_MODE=fakeintake \
  bash infra/space/kind/scripts/up.sh
```

This installs Agent `7.81.1`, exposes Fake Intake at
`http://localhost:8088`, and sets the Agent and SPACE workloads to the same
local environment; `up.sh` then verifies the running Agents' configuration.
Environment-only settings require a Tilt restart, which `up.sh` performs when
they changed; the optional `SPACE_KIND_OBSERVABILITY_CONFIG` JSON file is
watched and authoritative over the launch environment, including during
rollback.

The explicit `otel` profile sends Rust custom metrics and traces through
the upstream OpenTelemetry SDKs and local Collectors. On an existing Kind
cluster, full-stack Tilt installs the backends automatically:

```bash
SPACE_KIND_TELEMETRY_MODE=otel bash infra/space/kind/scripts/up.sh
```

Add `SPACE_KIND_DATADOG_MODE=fakeintake` alongside it to keep an Agent running
for a side-by-side capture; `otel` does not require one.

`datadog` remains the default and rollback option. The explicit `otel` mode selects
the exclusive `SPACE_METRICS_SINK=otlp` and `SPACE_TRACE_EXPORTER=otlp` paths with an
`always_on` diagnostic trace sampler, and `off` turns both signals off. In
`otel` mode the shim moves to OTLP as well.
There is no dual-publish or non-counter mode. Counters use the same metric
names: native intake reports RATE and OTLP reports COUNT. Compare event totals
rather than raw values, and keep a same-name type comparison off any backend
shared with other work — two types under one metric name is a conflict the
backend cannot resolve for you.
The metrics stack also enables OTLP in supported filesystem worker processes.
These selectors do not install the application stack: a main-only binary still
needs its owning exporter changes to support OTLP. Logs remain structured JSON
on stdout, collected by whatever is installed to read it — the Agent when one
is running, Alloy in this stack. There is no OTLP log exporter.

The Agent chart is supported only in `kube-system`; a different namespace is
rejected because its host-port deployment and intake URLs belong to this Kind
harness. A direct render must choose `values-fakeintake.yaml` or an existing
SaaS Secret explicitly. The ordinary chart defaults do not deploy a backend.

Tilt applies the optional Datadog Agent with the SPACE releases, and `up.sh`
verifies it only once the stack is ready, so an Agent rollout failure is
reported after the SPACE releases are up; selected local OTLP backends are
installed before the shim. Ownership and configuration errors fail before
producer changes: the Tiltfile refuses to replace an Agent this harness did not
deploy (`datadog_backend.py guard-owner`) when it loads.
Comparison runs still require verified Agents and received payloads.

Fake Intake is a single-replica, in-memory test backend. It captures the real
Agent payloads but does not implement Datadog dashboards or SaaS-side indexing.
Inspect received routes and reset the capture between comparison phases with:

```bash
curl -fsS http://localhost:8088/fakeintake/routestats | jq
curl -fsS http://localhost:8088/fakeintake/flushPayloads >/dev/null
```

For durable, phase-labelled evidence, drain and reset the receiver before one
bounded smoke window, then capture it afterward:

```bash
scripts/tests/capture-kind-fakeintake.sh prepare
# Run one smoke test or one repeated test group.
scripts/tests/capture-kind-fakeintake.sh capture after-otel happy-path
```

Captures are written below `target/kind-telemetry-comparison/` and include raw
metrics, sketches, logs, trace payloads, route counts, workload configuration,
and stable cluster identities. Existing capture directories are never replaced.
The helper records the prepared-window timestamp and the first overlapping
ten-second DogStatsD bucket. Normalized metrics include that bucket (which can
contain up to nine seconds of earlier activity); logs use the exact timestamp.
Raw payloads are retained for interpreting boundary and late-arrival effects.

When the workloads export traces through the native Datadog tracer, capture
then checks the SPACE traces it received. With the evidence already written,
it fails if there are none or if SPACE parented a span other than a request's
entry span on remote context, and it reports the chunks the tracer split.
`space-trace-chunks.json` holds the examples and counts; see
[Datadog trace chunks](../../../docs/observability.md#datadog-trace-chunks).

To size ingestion for a class of trace, such as the lifecycle traces a
sampling rule would keep in full, capture a window that runs the operations a
few times and group `traces-raw.json` by trace: decode its payloads as
`datadog_trace_chunks.py` does, collect every chunk's spans under their trace
ID, and take the span with no parent as the trace's root. Its service and
operation are what a sampling rule matches, and the trace's span count per
service is what keeping it costs. Work that a request causes but does not wait
for, such as a public operation's execution, a job or a shim create, roots its
own trace with a link to the request, so size each root separately.

The three-state smoke specification, pinned inputs, measured results and
guest/worker coverage limits are recorded with the run's evidence rather than
in this repository.

### SaaS experiments

`saas` exports to a real Datadog organization, so it is deliberately harder to
reach than `fakeintake`:

1. Get the export approved by whoever owns the target organization's ingestion
   budget, and agree a window. There is no spending cap in this tooling.
2. Create the Secret out of band and name it; the renderer never accepts a key
   as an argument or writes one into values or captures. Fake Intake uses
   `datadog-fakeintake-key`; SaaS rejects that name and the legacy
   `datadog-secret`.
3. Set an isolated experiment identity (`SPACE_KIND_ENV`, `SPACE_KIND_OWNER`,
   `SPACE_KIND_PHASE`) so the payloads are attributable and separable from
   anything else in that organization.
4. Use `session` for a best-effort timed rollback after
   `SPACE_KIND_DATADOG_DURATION_MINUTES`. The timer runs in that foreground
   process and is not a hard spending cap. Direct `apply` and Tilt
   require the additional `SPACE_KIND_DATADOG_UNBOUNDED=yes` acknowledgement;
   they export until explicitly stopped and do not run that timer.
5. Stop explicitly with the rollback below when finished. Neither `off` nor
   stopping Tilt stops an Agent that is already exporting.

After a successful helper-driven switch to SaaS, the helper deletes only its
labelled Fake Intake Deployment, Service and dummy Secret. Rollback recreates
those resources; a failed switch leaves the receiver available.

Tilt and the helper's own commands use the same preflight and renderer. None creates a
real API-key Secret or copies a key into arguments, rendered values or captures.
Fake Intake uses `datadog-fakeintake-key`; SaaS rejects that and the legacy
`datadog-secret` name. Normal SaaS endpoint TLS verification remains enabled;
Kind's kubelet certificate exception is separate.

With the same watched experiment configuration exported in your shell:

```bash
python3 infra/space/kind/scripts/datadog_backend.py stop
python3 infra/space/kind/scripts/datadog_backend.py verify
```

`stop` persists `fakeintake` into the file, applies freshly rendered local-only
values, and waits for Agent rollout before inspecting the running pods. It
preserves the real Secret. A failed rollout is not a successful stop; retry and
verify. Previously ingested SaaS data remains subject to backend retention.

The same mode selects the Go containerd shim's trace exporter and its metrics
sink: `datadog` keeps DogStatsD, `otel` moves the shim to OTLP alongside the
workloads, and `off` disables both. In `datadog` and `off` modes the wrapper
leaves the SPACE and OTel identity settings unset, so the shim's DogStatsD tags
are exactly what they were before this mode existed; `off` sets no OTLP
endpoint either. Because containerd starts
the shim on the Kind node rather than in a Pod, `install-shim.sh` installs a
small production-shaped wrapper around the compiled binary instead of relying
on workload environment inheritance. Kind nodes cannot resolve pod DNS, but
kube-proxy routes a Service's ClusterIP, so in `otel` mode the wrapper sends
each signal to `http://<ClusterIP>:4317` of its Collector. The addresses are
fixed by `collector.clusterIP` in each chart's `profiles/kind.yaml`:
`10.96.0.20` for `space-metrics` and `10.96.0.21` for `space-traces`, in the
first 256 addresses of Kind's `10.96.0.0/16` Service range, which dynamic
allocation avoids. The shim can therefore be installed before the Collectors,
and reinstalling them does not leave the wrapper with a stale address. Set
`SPACE_KIND_SHIM_OTLP_ENDPOINT` to send both signals to another OTLP/gRPC
receiver reachable from the Kind node instead.

The host runtime-supervisor is the other SPACE process outside a pod. Its unit
reads `/space/config/space.env`, which node-manager's Kind-only `kind-node-env`
init container writes from the same selector: the pods' telemetry settings,
with each OTLP endpoint replaced by the fixed address above, or by
`SPACE_KIND_SHIM_OTLP_ENDPOINT` when that is set. Check it with
`docker exec <node> cat /space/config/space.env`.

A Collector installed before its address was pinned keeps the ClusterIP it was
allocated, which Kubernetes will not change in place: `local.sh up` fails with
`spec.clusterIPs[0]: ... may not change once set`. Only that Service is
immutable; the data lives in the release's PersistentVolumeClaim. Delete the
Service and run `up` again, which recreates it at the pinned address and keeps
the stored metrics and traces:

```bash
kubectl --context kind-space-local -n space-metrics delete svc collector
bash infra/space/kind/metrics/local.sh up
```

Do the same for `space-traces`, then re-run `install-shim.sh` (Tilt's
`install-space-shim` resource does) so the wrapper names the pinned addresses.

The shim takes `SPACE_METRICS_SINK` from the same telemetry mode as the Rust
workloads: `datadog` keeps DogStatsD, `otel` selects OTLP, and `off` selects
`off`. Left unset the shim would default to `auto`, meaning native Datadog, and
would keep emitting DogStatsD metrics through an otel comparison.
Datadog mode keeps the existing node-local shim settings.

Updating the shim binary or wrapper never restarts containerd: the node image
registers the `space` runtime, and `install-shim.sh` only replaces the files
containerd runs for each new sandbox. Existing sandbox shims keep their launch
settings; create fresh test-owned sandboxes when changing phases.

`install-prereqs.sh` falls back to user-local installs (`$HOME/.local/bin`,
`$HOME/.local/go`) when run without root, so it works on sudo-less hosts
for everything it downloads. Its apt step (the suites' prerequisites,
listed in `scripts/lib/space-smoke-prereqs.sh`) has no user-local equivalent:
when one of those is missing and the run can neither sudo nor act as root, the
script fails instead of continuing, because a box that skips them cannot run
the smoke suites. `install-fc-vendor.sh` creates its vendor root
unprivileged when it can, and escalates once in either of two cases — the
root already exists and belongs to root (the shape the devbox platform
pre-provisions), or something above it is unwritable — after which it keeps
writing without escalation. A
root-owned parent it had to *create* is handed over as well; one that already
existed is not, so a pre-existing root-owned `~/.space-devbox` still needs the
manual chown the smoke-test runbook describes.

### Shared experiment identity

Tilt and the node-hosted shim read the same optional non-secret
JSON file named by `SPACE_KIND_OBSERVABILITY_CONFIG`. Tilt watches this file;
changing it reconciles workload identity and the shim wrapper. File values
override the corresponding environment variables. Never store API keys here.

Replace that file by rename (`mv` a fully written temporary over it) rather
than editing in place: a truncated or half-written file aborts `up.sh` and
Tiltfile evaluation, and `datadog_backend.py stop` already writes it this way.

The identity options are `SPACE_KIND_ENV`, `SPACE_KIND_OWNER`,
`SPACE_KIND_VERSION` (the source revision), and `SPACE_KIND_PHASE`
(`before-main`, `after-datadog`, or `after-otel`). When
revision and phase are supplied, the application version is `<revision>-<phase>`
so even native trace/log clients can distinguish phases. Agent-side tags alone
do not override an application's explicit environment/version. Without
`SPACE_KIND_VERSION`, every Rust service and the shim report the checkout's
commit (`git rev-parse --short=12 HEAD`) as their version, read when Tilt
starts. The build SHA each image records, which the startup banner's
`git_sha` and the node's host sidecars report, is computed when the image is
built instead ([Build identity and provenance](#build-identity-and-provenance)).

These values align `DD_ENV`/`DD_VERSION`, neutral log identity, and OTel resource
identity without changing service names. `SPACE_KIND_CLUSTER` names the Kind
cluster and supplies `kind_cluster` / `k8s.cluster.name`. `SPACE_KIND_CLUSTER_ID`
is the separate SPACE/database identity, defaults to `kind-local`, and reaches
both service charts and the shim as `SPACE_CLUSTER_ID`. Keep that value stable
for an existing database; experiment environment/phase changes do not rename it.
Old binaries ignore unsupported neutral variables; native identity still works.
The Agent uses an experiment-specific backend cluster identity; correlate it
with workloads by the shared `kind_cluster` tag. Evidence capture lives in
`scripts/tests/observability/datadog_evidence.py`; chart resources and backend
reconciliation stay under `infra/space/kind`.

Existing long-lived shims retain their launch environment: create a new sandbox
after changing phases. The installer accepts `SPACE_KIND_SHIM_ENV` as a legacy
fallback when `SPACE_KIND_ENV` is absent.

### Host paths (and running as a user other than `dev`)

`kind create` reads plain YAML and expands nothing, so the cluster config is
rendered per host rather than checked in: `kind-cluster.yaml.tmpl` carries
`${SPACE_VENDOR_ROOT}` and `${SPACE_KIND_DATA_ROOT}` placeholders, and
`scripts/lib/kind-host-paths.sh` resolves them, writes the result to
`$SPACE_KIND_CONFIG`, creates the containerd-storage directories, and refuses
to hand `kind` a config whose vendor tree or `/dev/kvm` is missing — Docker
answers a missing bind-mount source by creating an empty directory, so an
unstaged vendor tree used to reach the workers as an empty `/space/vendor`
and fail on the first sandbox with a healthy-looking cluster.

| Knob | Default | What uses it |
| -- | -- | -- |
| `SPACE_VENDOR_ROOT` | `~/.space-devbox/space/vendor` | mounted read-only at `/opt/space-kind/host-vendor` on the sandbox and builder nodes, each of which copies it into its own `/space/vendor` on its first boot ([The node layer](#the-node-layer)) |
| `SPACE_KIND_DATA_ROOT` | `~/.kind-data` | mounted at `/var/lib/containerd` on every node |
| `SPACE_KIND_PRUNE_SUPERSEDED_IMAGES` | on | `0` keeps every superseded build of SPACE's own images in the `SPACE_KIND_DATA_ROOT` stores; see [Diagnosing iteration disk pressure](#diagnosing-iteration-disk-pressure) |
| `SPACE_TILT_PRUNE_HOST_IMAGES` | on | `0` keeps every superseded build of SPACE's own images in the host's Docker store; see [Diagnosing iteration disk pressure](#diagnosing-iteration-disk-pressure) |
| `SPACE_KIND_CONFIG` | `~/.space-devbox/kind/kind-cluster.yaml` | where `render-cluster-config.sh` publishes a verified render |

That path is where `render-cluster-config.sh` *publishes* a completed render,
not the file any given run hands to `kind create`. The bring-up renders its
own copy and creates from that, because the published path is per-user rather
than per-run: two bring-ups on one box would otherwise be able to replace each
other's config in the window between rendering it and reading it back, and the
cluster that won would come up on the other run's host roots with every mount
it declares present.

The node images preload kind's add-on images (kindnetd and the local-path
provisioner from Docker Hub, the control-plane images from registry.k8s.io) in
`/var/lib/containerd`, which the `SPACE_KIND_DATA_ROOT` mount hides. Without a
copy of them, a fresh host's kubelets pull every add-on again, and while
Docker Hub is unreachable no node becomes Ready. So before `kind create`,
[`scripts/seed-node-containerd.sh`](scripts/seed-node-containerd.sh) copies
each node image's `/var/lib/containerd` into that node's directory if the
directory is absent or empty. That is the image's stock preload, the same set
on every node, about 430 MB each.
- The copy goes into a staging directory beside the target and is renamed
  onto it only when complete and only while the target is still empty.
- Seeds of one directory take turns under a lock.
- A directory that holds anything, such as the cache a deleted cluster leaves,
  is never touched.
- A seed that fails, is killed, or overlaps another bring-up leaves the
  directory as it found it, and the kubelets pull as before.

`SPACE_KIND_SEED_CONTAINERD=0` turns the copy off.

`scripts/lib/kind-host-paths.sh` resolves the vendor path for both staging and
cluster mounts. `SPACE_VENDOR_ROOT` defaults to
`$SPACE_DEVBOX_SPACE_TARGET/vendor` when that target is set, otherwise
`$SPACE_DEVBOX_PERSIST_ROOT/space/vendor`, falling back to
`$HOME/.space-devbox/space/vendor`. `SPACE_KIND_CONFIG` follows only
`SPACE_DEVBOX_PERSIST_ROOT` — it lives beside the other kind state, not under
the `space/` target — so `SPACE_DEVBOX_SPACE_TARGET` moves the vendor mount
without moving the rendered config. An explicit `SPACE_VENDOR_ROOT` outranks
all of them.

If you already have a `space-local` cluster that was created before these
paths were parameterised, it keeps the mounts it was built with — Docker fixes
bind mounts when it creates the node container, and `kind create` is the only
thing that reads the config. The bring-up detects that and stops, naming
each drifted mount and its rendered value; `kind delete cluster --name
space-local` once is the remedy, and a delete leaves the containerd image
caches on disk so the rebuild does not re-pull.

The defaults follow `$HOME`, so none of the three needs setting on a devbox,
and none needs setting under another username either. One other path in this
directory is still `/home/dev` by default and is *not* covered by that:
`infra/space/kind/scripts/start-dind.sh`'s `DIND_DATA_ROOT`. The script runs
under `sudo`, where `$HOME` may be root's, so deriving it from `$HOME` would
put the dind data root under root's home. Set it explicitly if you run the
script for another user.

The bring-up (`scripts/tests/lib/bootstrap-kind-stack.sh`, behind `up.sh` and
the smoke suites) renders before creating. For a hand-run `kind create`, use
`infra/space/kind/scripts/render-cluster-config.sh`, which prints the path of a
render private to that invocation — pass that to `kind create` rather than
`$SPACE_KIND_CONFIG`, so a second run cannot replace the file between the two
commands. Point `SPACE_VENDOR_ROOT` at a tree you staged elsewhere and
`install-fc-vendor.sh`, its `--check`, and the mount all follow it
together.

Once `up.sh` returns, the API and data plane answer on the cluster's own
host port mappings (see [Load-balanced entry and replicas](#load-balanced-entry-and-replicas)),
and Tilt's port forwards expose the single-replica rest for as long as Tilt
runs:

| Service     | URL                       | Through |
|-------------|---------------------------|---------|
| API         | http://localhost:8080     | host port mapping → `space-api-entry` NodePort |
| Data-plane  | http://localhost:8090     | host port mapping → `space-data-plane-entry` NodePort |
| UI          | http://localhost:3000     | Tilt port-forward |
| Object store (S3) | http://localhost:8333 | Tilt port-forward |
| Fake Intake (when enabled) | http://localhost:8088 | Tilt port-forward |

Log in as `root@local` with the initial password (`password` by default). Set `SPACE_CONSOLE_PASSWORD` before the bring-up starts Tilt to choose a custom password. It is never printed or retained in a Kubernetes Secret. It is set only while the seeded account is locked; rerunning does not reset an existing password.

### Recovering from an unbundled migration version

On a long-lived task-owned bench, the `space-db-migrate` Job may fail, and
`space-api` never start, because the database contains an applied migration
version that the current binary does not bundle. This can happen when a branch applies a queued migration before its
timestamp changes on merge. No later build can satisfy the orphaned version.

After confirming `kind-space-local` is the intended disposable cluster, reset
the local control-plane database and republish `seed:latest`:

```bash
kubectl delete statefulset space-postgres -n space --context kind-space-local
kubectl delete pvc data-space-postgres-0 -n space --context kind-space-local
kubectl delete job space-seed-publish space-db-migrate -n space --context kind-space-local
tilt trigger space-postgres
tilt trigger space-db-migrate
tilt trigger space-api
tilt trigger space-seed-publish
```

Deleting the publish Job is required because the database reset removes the
template-release tags that resolve `seed:latest`.

The same reset applies to a cluster whose database was created by an earlier
revision of the `space-postgres` chart (PostgreSQL 16, superuser connection
string): PostgreSQL 17 cannot start on that data directory, and the role
setup the pod's init script provides runs only when it initializes an empty
one. The bring-up, `scripts/tests/lib/bootstrap-kind-stack.sh`
(under `SKIP_BOOTSTRAP=1` too), and the `Tiltfile` run
`infra/space/kind/scripts/check-postgres-data.sh` before they apply anything
or query the database. It reads the data directory's `PG_VERSION` on the node
that holds the volume, so it answers the same whether the old server is still
running or the new one is crash-looping, and on a mismatch it prints the reset
commands and stops without changing anything (the scripts exit 1; Tilt
reports a Tiltfile load error). Without a running Tilt, rerun `up.sh` (or the
smoke suite) after the three `kubectl delete` commands above; a running Tilt
reloads with `tilt trigger "(Tiltfile)"`. Recreating the cluster has the same
effect.

### Diagnosing iteration disk pressure

Every build loads its images into the containerd store of each Kind node
that can run them ([Image delivery](#image-delivery)), which lives under
`$SPACE_KIND_DATA_ROOT` and survives `kind delete cluster`. A rebuilt
`:local` image leaves its previous build behind untagged, and every changed
build adds another `:tilt-<hash>` tag, so unpruned stores keep every image of
every build on each node that runs it. A full store reports image extraction
or build failures while the host root filesystem may still have space.

The bring-up therefore removes superseded builds itself: the Tilt resource
`prune-superseded-images` does whenever an update has settled.
`scripts/prune-superseded-images.py` removes an image from a node only when
all of these hold:

- It is one of the images in `lib/space-images.sh`, which the harness builds
  locally and Kind cannot pull: each of its tags names one of them or, once
  untagged, its `org.opencontainers.image.title` label does.
- Nothing current names it. Current means the `:local` tags (the retired
  scripted bring-up loaded its builds under them), every image a workload in the cluster references (the rendered charts
  as applied, including Jobs whose pods have finished), and the images the Tilt
  serving the cluster last built.
- No container on that node uses it, running or exited.

It stands down while that Tilt has an update pending or in progress, because a
build it just loaded is not deployed yet, checks again before every removal,
and removes nothing when it cannot read the cluster. It prints nothing when
nothing is superseded; otherwise it logs each image it removes and their total
image size per node, as `crictl images` reports it (unpacked layers free more).
The Tilt resource retries an image a container still held every few minutes
until it is gone.
Third-party images, and images of repositories the harness no longer builds,
are never touched. `--dry-run` lists what it would remove;
`SPACE_KIND_PRUNE_SUPERSEDED_IMAGES=0` turns it off.

The host's Docker store, where the images are built, keeps old builds too.
Every Tilt build of an image adds two tags to it: `:tilt-build-<time>-<nonce>`,
the `$SPACE_BUILD_REF` the Tiltfile's build commands tag, and the
`:tilt-<hash>` Tilt deploys. Tilt's built-in pruner (`docker_prune_settings`)
never removes these builds, whatever their labels, because it passes over every
image with more than one tag. On Docker's graph-driver store, a rebuilt
`:local` image also leaves its previous build behind untagged; the containerd
image store drops it. So the same script runs with `--host`: as the Tilt
resource `prune-host-images` whenever an update has settled. It applies the
rules above to the host's images, with any container on the host holding an
image, and also keeps each image's newest build that nothing current names, so
the build before the current one stays. `SPACE_TILT_PRUNE_HOST_IMAGES=0` turns
it off.

Do not run `crictl rmi --prune` in a node instead. It removes every image no
container is using at that moment, which includes SPACE images Kind cannot pull
back: the seed publisher between publishes, or the worker image while a builder
or debug pod on that node is restarting. The next pod that needs one fails with
`ImagePullBackOff` until the image is rebuilt and loaded.

Before it creates anything, `up.sh` warns, without refusing, when the
Docker data root or `$SPACE_KIND_DATA_ROOT` has less than
`SPACE_KIND_MIN_FREE_GIB` (default 30) GiB free, and names the cleanups that
cannot remove anything a bring-up needs. To inspect a node:

```bash
docker exec space-local-worker df -h /var/lib/containerd
python3 infra/space/kind/scripts/prune-superseded-images.py --dry-run
python3 infra/space/kind/scripts/prune-superseded-images.py --host --dry-run
```

The BuildKit cache is host-global and shared by other worktrees. Do not run
`docker builder prune` without explicit approval after stating that impact.
Tilt's built-in pruner, on by default because the Tiltfile does not configure
it, already removes BuildKit cache that no build has used for six hours, from
every worktree: Docker (29.x) accepts the label filter it sends with that request but
does not apply it.
The Docker data root may live on a separate volume, so check available space
inside a Kind node before starting a large build. Cold Rust builds can consume
substantial space; the incremental artifact build keeps its `target/` under
`$HOME` instead.

### Kind apiserver timeouts during parallel builds

Kind runs the whole control plane, apiserver and etcd, in one container that
shares the host's CPU, memory and disk with the image builds and with Tilt's
loads of every image into every node. When many of those run at once, etcd's
write latency can spike until the apiserver stops answering: Tilt or `tilt ci`
then fails on a Kind API liveness timeout, or the apiserver returns TLS
handshake timeouts. Start Tilt with `SPACE_TILT_MAX_PARALLEL_UPDATES=1` (or `2`)
in its environment to bound its parallel updates; the `Tiltfile` reads it into
`update_settings(max_parallel_updates)`, defaults to 8, and `tilt up` has no flag
for it. The bring-up is slower but keeps the control plane responsive. If the
host is under-provisioned, give it more CPU and memory.

## Seed template

The seed is built locally by the **production seed builder**
(`templates/firecracker/seed/Dockerfile` + `rootfs-builder.sh` — the same
code path that builds the seed production publishes to S3), so the guest has
the vsock-listening `spaced-svc` and boots under real Firecracker:

- `infra/space/kind/scripts/build-seed.sh` runs the builder (privileged
  docker run; downloads the Ubuntu minimal cloud squashfs on first run),
  injects the `spaced`/`spaced-svc` binaries from the worker Rust build
  image, bakes the per-host sandbox SSH public key, and authors the
  `manifest.json` next to the artifacts. Idempotent — it skips the build
  when the inputs (guest binaries, builder script, recipe, pubkey) are
  unchanged.
- the `space-seed-publish` Job then plays the role the production builder
  plays for distribution: it uploads `metadata.json` + `manifest.json` +
  `rootfs.ext4` to the object store under `templates/seed/<content_hash>/` and
  registers the template + release + `latest` tag + storage location
  through the real `ClusterApi` RPCs on `space-internal-api`.
- worker template-managers then resolve `seed:latest` via
  `ClusterApi.FindTemplateRelease` and pull the artifacts through
  volume-manager — the production pull path. Nothing is baked into the
  node image or side-staged onto workers.

Tear it down with `bash infra/space/kind/scripts/teardown.sh`.

## Topology

```
kind cluster: space-local (kind-cluster.yaml.tmpl — 6 nodes)
├── control-plane                       (k8s API server, scheduler, etcd)
├── worker  [pool=sandbox, space-enabled=true]
│   ├── space-*.service node layer      (btrfs /space/cache etc., baked into the node image)
│   ├── space-runtime-supervisor.service (systemd; installed by node-manager's init
│   │                                     container; runs every Firecracker VM and
│   │                                     each sandbox's space-netgtw-svc)
│   ├── containerd-shim-space-v2        (runtime registered by the node image, binary installed by install-shim.sh)
│   ├── one pod per node-service DaemonSet (release `space`):
│   │   ├── space-node-manager        → host supervisor (root, privileged)
│   │   ├── space-runtime-manager     → node-manager    (space-rtmgr)
│   │   ├── space-volume-manager      → object store    (space-volmgr)
│   │   ├── space-credential-manager                    (space-sbxmgr)
│   │   ├── space-template-manager                      (space-tmplmgr)
│   │   ├── space-sandbox-manager                       (space-sbxmgr)
│   │   ├── space-node-ctl            → internal-api    (space-nodectl)
│   │   └── space-node-api            (hostNetwork, host_ip:9800, component=node-api) (space-nodeapi)
│   ├── space-node-debug              (Kind-only operator shell, root)
│   └── per-sandbox pods in namespace `sandboxes` (pause:3.10 hijacked into Firecracker microVMs)
├── worker2 [pool=sandbox, space-enabled=true]
│   └── identical to worker
├── worker3 [pool=infra,    space.pplx.net/infra=true]
│   ├── space-postgres        (StatefulSet; PostgreSQL 17 serving TLS only, non-superuser master user)
│   ├── space-object-store    (versitygw S3 gateway + bucket-init Job)
│   ├── space-object-store-iam (Deployment; versitygw's standalone IAM service: the storage exchange's STS and sessions)
│   ├── space-kind-sts-bridge (Deployment; Kind-only STS front for the authority's storage exchange, and the token issuer the IAM service trusts)
│   ├── space-redis           (Deployment; the rate limiters' store, which api, admin-api and cluster-controller reach through SPACE_REDIS_URL)
│   ├── space-api             (Deployment, 2–3 replicas by HPA; SPACE_K8S_CONTROL_PLANE=true on the default (Kubernetes) lane, see "Direct substrate lane")
│   ├── space-admin-api       (Deployment; the same binary on the dedicated admin surface)
│   ├── space-internal-api    (Deployment, 2 replicas; ClusterApi — node state + template releases)
│   ├── space-credmgr-authority (Deployment, 2 replicas; credential-manager authority — credential tables + platform KEK)
│   ├── space-kind-key-broker (Deployment; Kind-only intermediate-key broker fixture behind the platform KEK)
│   ├── space-data-plane      (Deployment, 2–3 replicas by HPA)
│   ├── space-cluster-controller (Deployment, 2 replicas, one leader; materializes per-sandbox pods on the default (Kubernetes) lane)
│   ├── space-kind-fake-ec2   (Deployment; Kind-only fake EC2 endpoint the reconcile-queue GC asks about non-ready nodes)
│   ├── metrics-server        (Deployment in kube-system; resource metrics for the HPAs)
│   ├── space-seed-publish    (Job; publishes the locally built seed)
│   └── space-ui              (Deployment)
├── worker4 [pool=builder]
│   ├── the same node layer and space-runtime-supervisor.service
│   ├── the same node-service DaemonSets (release `space-builder`, node.role=builder)
│   │   plus space-builder-build-manager (recipe VM + publish → templates bucket; root)
│   ├── space-node-debug
│   └── Service space-builder-node-api:9800 → the builders' node-api pods
└── worker5 [pool=builder]
    └── identical to worker4
```

Sandbox-scoped traffic (`CreateSandbox`, `Exec`, `SpacedWrite`, …) flows
api-server → per-sandbox pod IP at `:9800`. Node-scoped traffic (`NodeInfo`,
`ListNodeTemplates`, network ops) flows api-server → each node-api pod's
`host_ip:9800`.

## Sandbox namespace and network policy

Sandbox pods run in their own namespace, `sandboxes`, and everything else in
`space`, as in a deployment that confines sandboxes with namespace-scoped
policy. `k8s.sandboxNamespace` in the api, cluster-controller and
internal-api charts names it (`SPACE_SANDBOX_NAMESPACE`), their RBAC for sandbox
pods is granted there, and `SPACE_NODE_API_NAMESPACE` keeps the node-api lookup
in `space`: unset, the substrate would look for node-api pods in the sandbox
namespace. `manifests/namespace.yaml` creates both namespaces; warm-pool
configs are filed under the API's sandbox namespace whatever a request names.

Kind v0.30.0's kindnet enforces NetworkPolicy, and
[`charts/space-sandbox-network-policy`](charts/space-sandbox-network-policy)
confines the namespace with two of them, each rule commented with the code
that needs it:

- `space-sandbox-default-deny` isolates every pod in the namespace, ingress and
  egress.
- `space-sandbox-allow` gives sandbox pods (`app.kubernetes.io/name=space-sandbox`)
  what they carry: ingress on TCP 9800 from the two components that dial a
  sandbox pod IP, `space-api` and `space-cluster-controller`; egress to the IPv4
  internet outside `10.0.0.0/8`, `100.64.0.0/10`, `172.16.0.0/12`,
  `192.168.0.0/16` and `169.254.0.0/16`; and DNS to the cluster DNS pods only.
  There is no IPv6 egress.

The guest needs nothing more. Its traffic leaves the pod masqueraded to the
pod IP, its DNS goes to public resolvers over that internet rule (the gateway
answers the guest's reserved resolvers over DNS-over-TLS), and the shim's
redirect of `<pod IP>:9800` to the node-local node-api is pod-to-own-node
traffic, which NetworkPolicy always allows. internal-api and the data plane
dial node-api at node addresses, never a sandbox pod. The policy sits beneath
the per-sandbox net-gateway's tenant rules and holds for anything that reaches
the pod's network namespace outside the VM.

Because the allow policy selects sandbox pods for both directions, it isolates
them on its own; the default-deny is what isolates any other pod placed in the
namespace. The typed `local.sandbox_network_isolation_holds` scenario checks
both against a live cluster from the `space-node-debug` shell, which enters a
pod's network namespace by its netns handle under `/var/run/netns`.
## Load-balanced entry and replicas

The control plane runs as a deployment's does: several replicas behind
load-balanced entry, scaled on load.

- **Entry.** `localhost:8080` and `:8090` are `extraPortMappings` of the infra
  node (`kind-cluster.yaml.tmpl`, listening on 127.0.0.1) onto the
  `space-api-entry` and `space-data-plane-entry` NodePort Services (30080,
  30090). kube-proxy picks a ready replica for each new connection, so clients
  reach every replica, where a `kubectl port-forward` pins every request to one
  pod; requests on one kept-alive connection stay on its replica. The entry
  Services carry the public HTTP port only; in-cluster callers keep the
  `space-api` and `space-data-plane` ClusterIP Services, and the API's ingress
  activation port stays off the nodes. No restart loop keeps these forwards
  alive any more; the UI, object store and Fake Intake are still forwarded.
- **Autoscaling.** metrics-server (`charts/space-metrics-server`, the upstream
  release manifest with `--kubelet-insecure-tls` for Kind's self-signed
  kubelets, in kube-system) serves the resource metrics API. space-api and
  space-data-plane each have a HorizontalPodAutoscaler between 2 and 3
  replicas, on CPU (80% of the request) and memory (384 MiB per replica). Two
  so every path runs against more than one replica; three so the worst case of
  both tiers at their 1 CPU / 512 MiB limits stays within a 16-core runner's
  infra node. Their Deployments roll one replica at a time rather than
  `Recreate`. The UI chart pins one replica, so it has no autoscaler.
- **Migrations** run once per rollout, as the `space-db-migrate` Job, before
  any API replica starts, the way a deployment's provisioning Job runs them
  ahead of its application releases; the replicas no longer each run them.
  The migrator asks for this: it serializes on a session advisory lock, but a
  competing runner can turn a retried lock race into a failed run
  (`crates/libs/db/migrator/src/migration_retry.rs`). Tilt makes `space-api`
  wait for the Job.
- **cluster-controller** runs two replicas with leader election on, on either
  substrate: the Lease holder runs every reconciler and the other stands by,
  Ready, to take over.
  The typed scenario `local.leader_election_brownout_keeps_one_leader`
  exercises the handoff. It needs `otel` mode. For its own run it lowers the
  controller's lease TTL to its 18 s floor and the liveness budget to the
  72 s floor at that TTL, through two test hooks that are inert unless the
  `test-hooks` feature is compiled in, as the Kind build does. At the end it
  removes the TTL hook and puts the budget's standing value back; it takes
  about 8 minutes. Run it alone with
  `uv run --frozen --python 3.11 -m space_smoke run
  local.leader_election_brownout_keeps_one_leader`.
- **internal-api** (ClusterApi) runs two replicas behind its ClusterIP
  Service and rolls one at a time. It needs no election: its state is in
  Postgres, written under row locks and claim tokens, its RPCs are unary, and
  its per-replica caches are bounded (the chart's values cite the code).
- **Admin API.** `space-admin-api` runs the same binary with
  `--surface admin`, the dedicated admin surface, in its own Deployment and
  ClusterIP Service. Clients and the harness keep using the shared surface's
  `/admin/api/v1` on `localhost:8080`; reach the dedicated one with
  `kubectl --context kind-space-local -n space port-forward svc/space-admin-api 8081:8080`.

Harness operations that need a fixed replica count take it from the
autoscaler rather than fight it: the concurrent-restore scenario pins the API
autoscaler's bounds to its contention count and restores them, and the native
recovery faults pin every target's autoscaler at its current count until the
Deployments are restored.

A cluster created before the port mappings cannot gain them, since Docker fixes
a node container's ports when it creates it. `check-node-layout.sh` refuses it
with the commands to recreate it (see
[Upgrading from the node-stack layout](#upgrading-from-the-node-stack-layout)).

## Shutdown, disruption and self-repair

The control plane stops, moves and repairs itself as deployments run it, so
the code paths for draining, voluntary disruption and stuck rows run here
too. Each chart's values cite the code behind its numbers.

- **Graceful shutdown.** space-api and space-admin-api (one binary) render
  `--shutdown-grace-secs` at the binary's 150 s default. On SIGTERM the
  process reports draining on `/readyz`, stops accepting, and finishes
  in-flight requests, SSE streams and the substrate work it tracks (a
  restore leader, a suspend continuation) within that grace. A 5 s preStop
  sleep keeps the pod serving while it leaves the entry Services' endpoints,
  and the 180 s termination grace covers both plus exit. The data plane renders
  its own `--shutdown-grace-secs` at 150 s: on SIGTERM it stops accepting and
  refuses new streams, keeps attached streams (native SSE and WebSockets, ECW
  process and file streams, upgraded proxies and ingress) running until they
  end, and cancels whatever is left at the deadline, giving those streams 5 s
  to close. It gets the same preStop delay, and its 180 s grace covers the
  preStop, the drain, those 5 s and exit; the chart refuses a grace that
  doesn't exceed the first three. internal-api waits for in-flight ClusterApi calls,
  which the server does not bound and every client times out at 30 s, with a
  5 s preStop and a 50 s grace that covers both and the telemetry flush.
  cluster-controller bounds its reconciler drain at 10 s inside its 30 s
  grace.
- **Startup and rollouts.** The API's startupProbe on `/readyz` is the boot
  budget its Pod reflector leaves to Kubernetes (a replica whose initial LIST
  never completes is restarted); the data plane's covers waiting for
  ClusterApi before it binds. API, data plane and internal-api replicas count
  as available after 10 s Ready (`minReadySeconds`), and each rolls one
  replica at a time.
- **Disruption budgets.** space-api, space-admin-api, space-data-plane,
  space-cluster-controller, space-internal-api and space-credmgr-authority
  each have a PodDisruptionBudget of `maxUnavailable: 1`: an eviction takes
  one replica at a time and waits for its replacement. They all run on the
  infra node; the node-drain scenarios drain sandbox workers, which carry
  none of them. A Deployment rollout is not an eviction and follows its own
  strategy.
- **Priority.** `manifests/priorityclasses.yaml`, applied by Tilt before any
  workload, defines `space-control-plane` (the control-plane
  Deployments and the in-cluster Postgres, object store, Redis and key
  broker) and `space-replaceable` (the UI and the one-shot Jobs; it never
  preempts). Node-service DaemonSets run as `system-node-critical`.
- **Backstop reconcilers.** cluster-controller runs the ones deployments run:
  the transient-lifecycle sweeper (restoring rows after 600 s, the other
  transient lifecycles effectively off, failed-sandbox cleanup on), the
  stuck-snapshot reaper, the started-sandbox and failed-restorable repairs
  (live), the gone-node queue GC (dry-run) and the orphaned-restore-pod GC
  (dry-run). At these ages a row a scenario holds mid-transition is not old
  enough to touch; a row one of them acts on during a run was stuck past what
  a deployment tolerates. The reconcile-queue GC runs live at the binary's
  unchanged retention (24 h for dead letters and for a non-ready node's live
  refs) and asks the Kind-only fake EC2 endpoint, not EC2, whether such a
  node is gone (see the differences table).

## How the node registers itself

This section describes the default (Kubernetes) lane; for the Direct one, see
[Direct substrate lane](#direct-substrate-lane).

Every node-service chart passes `SPACE_NODE_ID` as the raw kube node name
(fieldRef `spec.nodeName`) plus its worker or builder `SPACE_NODE_ROLE`;
the services that call ClusterApi also get
`SPACE_INTERNAL_API_ENDPOINT=http://space-internal-api:9800` and their own
projected token. node-ctl's
outbound reconcilers call `ClusterApi.PublishNodeState`, and the internal
API derives the node UUID from `(node name, role)` via `space_k8s_node_id`
and upserts `nodes` / `node_k8s` / `node_status_snapshots` /
`node_template_inventory`. The cluster-controller's `kube_node_sync`
reconciler covers kube-Node inventory. **Nothing on the node touches
Postgres**: credential state belongs to the control-plane credential-manager
authority (below). Node status uses the normal freshness window, derived from
node-ctl's poll interval. The bring-up ends by waiting until
the admin node list (`GET /admin/api/v1/nodes`) reports every sandbox and
builder node reachable, with scheduling status `ready`, and observed within the
five node-ctl ticks the API trusts a census for
(`scripts/wait-node-publication.sh`): ready pods alone are not a registered
node. The wait doesn't key on `stale: false`, which says only that the node
published something recently, not that it sent a census.

## Node services

Production runs one DaemonSet per node service on nodes whose image provides
the node layer. So does this harness: each service has a chart under
`charts/space-<service>-local`, built on the `space-node-service` library
chart, rendered once per role — release `space` on the sandbox workers and
release `space-builder` (`--set node.role=builder` plus the chart's
`values-builder.yaml`) on the builders, which also run build-manager. The list
lives in `lib/node-services.sh`, which Tilt and the bring-up's DaemonSet
waits read. Every pod runs the production worker image
(`docker/Dockerfile.worker`) unchanged, with its service's production launch
path, under the identity `infra/space/identity/node-identities.sh` assigns (the
library reads that file through each chart's `identity/` link). Pods carry
`app.kubernetes.io/name=space-<role>`, `app.kubernetes.io/component=<service>`
(the names the cluster-controller's node-service readiness and the API's
reflector use) and `app.kubernetes.io/part-of=space-node`.

| Service | Identity | Pod shape | Why (public code) |
|---|---|---|---|
| node-manager | root | privileged, hostNetwork, hostPID; `/space/cache` and `/var/run/netns` Bidirectional; `/sys/fs/cgroup`, `/space/bin`, `/space/etc`, `/dev/kvm`; `/space/vendor` writable; projected token for the credential-manager authority | mounts jail rootfs and late snapshot-memory binds that must reach the host jailer, netns binds the host jailer and sidecar launcher open, loop and device-mapper devices, cgroup limits, root-namespace veth/nft/ip_forward, pidfds on host processes (`crates/libs/node/mgr/server`); creates the managed store `/space/vendor/artifacts` and publishes the runtime manager's staged releases and kernels into it (`node_artifacts.rs`); init container `node-manager-init.sh` writes the node's systemd unit via `nsenter --target 1` |
| runtime-supervisor | root | host systemd unit, no pod; environment from `/space/config/space.env`, which node-manager's Kind-only `kind-node-env` init container writes | `scripts/k8s/node-manager-init.sh`, `node-manager-runtime-supervisor.sh` |
| runtime-manager | space-rtmgr + SPACE_RUNTIME_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs; `/space/vendor` and `/space/bin` read-only, `/space/cache` HostToContainer | lists runtimes and kernels, and installs the catalog's: downloads them from the compiled sources over the pod network into its private `/space/cache/.rtmgr-staging`, then asks node-manager over its socket to publish them (`crates/libs/node/runtime/manager/server`); the account writes nothing else in the cache or the store |
| volume-manager | space-volmgr + SPACE_VOLUME_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs; `/space/cache` HostToContainer | file transfers only (`crates/libs/vol/mgr/server`); the account `docs/installation.md` describes: node-manager's btrfs send-lease RPCs authorize it and credential-manager serves it on the volume socket |
| credential-manager | space-sbxmgr + SPACE_CREDENTIAL_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs; `/space/run` only; projected token for the authority | the node credential agent (`--role=agent`): sockets only, forwarding to `space-credmgr-authority`; no database, KEK or AWS credential; its peer-process check (`SPACE_UDS_PEER_PROCESS_CHECK`) is off, so no host PID namespace |
| template-manager | space-tmplmgr + SPACE_TEMPLATE_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs, pod network; `/space/cache` HostToContainer | btrfs work delegated to node-manager (`btrfs_via_nodemgr.rs`); template-release reads bind the reviewed token's node name/UID |
| sandbox-manager | space-sbxmgr + SPACE_SANDBOX_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs, hostPID, hostNetwork; `/space/cache` HostToContainer, `/space/etc` read-only | gateway callers are authorized by their host pid, and Firecracker pids go to the node's shim (`service/gateway_identity.rs`, `network_runtime.rs`); host network for its node-bound ClusterApi RPCs |
| node-ctl | space-nodectl + SPACE_NODECTL_GROUPS | non-root, no capabilities, read-only rootfs, hostNetwork; its own ServiceAccount with Node RBAC, fenced by `node-fence.yaml` | the Kubernetes node-local controller; host network for its node-bound ClusterApi RPCs |
| node-api | space-nodeapi + worker/builder groups | non-root, no capabilities, read-only rootfs, hostNetwork, port `grpc` 9800; its ServiceAccount's API credentials (TokenReview only) and projected node-api and, on builders, build-manager tokens | reviews callers' tokens; the API dials `host_ip:9800` and reads the `grpc` port; on builders it reaches build-manager through the node-local `space-builder-build-manager` Service and sleeps 5 s in preStop (below) |
| build-manager | root | privileged, hostPID, pod network behind the node-local `space-builder-build-manager` Service (`internalTrafficPolicy: Local`); `/var/run/netns` and `/space/cache` HostToContainer, the node's containerd; its ServiceAccount's API credentials (TokenReview only) | reviews the builder node-api's token; loop and bind mounts for OCI assembly, `nsenter --net` into build VMs through netns handles node-manager mounts after it starts, `ctr` against the node's containerd (`crates/libs/build/mgr/src`); host PID to watch the build VM's host Firecracker pid exit (`vm.rs`); no host network: SSH into build VMs runs inside `nsenter --net` |
| space-node-debug (Kind only) | root | privileged; `/space/run`, `/space/cache` HostToContainer, `/space/bin`, `/space/vendor`, `/run/containerd`, `/var/run/netns` HostToContainer read-only | the operator shell; see below |

internal-api binds every node-bound ClusterApi RPC — `PublishNodeState`, the
node desired-state and reconcile calls (node-ctl), quarantine recovery plans and
artifact backups (sandbox-manager) — to the node's registered address, the
Kubernetes Node's InternalIP that cluster-controller's node sync writes, and
refuses a call from any other source address
(`validate_origin_remote_addr` in `crates/libs/internal/api/server/src/lib.rs`).
The check reads neither the credential nor the transport: it applies alike to
the static token and to TokenReview-authenticated projected tokens, and internal
RPC TLS, being server-auth only, does not change it. So node-ctl and
sandbox-manager run in the node's network namespace. template-manager makes no
node-bound call. Its template-release reads use the reviewed token's node name
and UID to match live inventory (`template_read_scope`,
`crates/libs/internal/api/server/src/adapter.rs`), so it runs on the pod network.
This makes `local.ecw_template_build_from_oci_boots` and
`local.private_template_build_isolation` exercise private-template resolution
when the caller's pod IP differs from the registered node IP. Missing claims
retain address matching; malformed claims and unmatched complete bindings do
not fall back. build-manager's only source-scoped read checks for an existing
upload, and an org-private base is resolved by template-manager, so it runs on
the pod network; node-api reaches it through the node-local
`space-builder-build-manager` Service, named as its TLS leaf.

Services whose `/space/run` directory the node image does not bake (volume-manager,
node-ctl, runtime-manager, and credential-manager's volume-socket directory)
create it in a `prepare-run-dir` init container with only CHOWN and FOWNER; build-manager, which does not migrate its SQLite database
itself, applies its schema in a `migrate-sqlite` init container. Every other
service migrates its own store after taking its service mutex.

**Startup and probes.** Nothing orders the services. Each tolerates a peer that
is not ready yet by failing its own startup or retrying, and the kubelet restarts
that pod alone (sandbox-manager, for example, exits until node-manager answers;
node-api retries sandbox-manager's capability check). Services with an
instance-local health socket (`--health-socket=/run/health/<service>.sock` on a
pod-private emptyDir) are probed on it: liveness answers once the socket is bound,
readiness is SERVING while a successor waits passively for the service mutex and
follows the production socket once active
(`crates/libs/common/svc/src/local_health.rs`). runtime-manager is probed on its
service socket and build-manager on its gRPC health service.

**Rollouts and termination.** Services that hold a node-wide service mutex
(`/space/run/<service>/service.mutex`) roll with `maxSurge: 1, maxUnavailable: 0`:
the successor starts beside the predecessor, reports ready while passive, and
takes over, sockets replaced by rename, when the predecessor drains and releases
the lock. That covers node-manager, credential-manager, template-manager,
sandbox-manager and node-ctl. volume-manager, runtime-manager, node-api and
build-manager delete first. volume-manager runs as `space-volmgr`; clusters
created with a different volume-manager account must be recreated rather than
upgraded in place. runtime-manager holds no mutex, so a successor would
take its socket by rename at once, beside a predecessor its probes can no longer
reach, and its kernel installer empties shared staging on open, which is only
safe when nothing in staging is live. node-api's declared port is a host port the
scheduler will not place twice on a node. build-manager has no mutex and recovers
jobs before binding its listener. Termination grace follows each service's drain: 60 s for
the 25 s drains the shared lifecycle defaults are sized against, 120 s for
volume-manager's 100 s transfer drain plus 10 s server drain, 180 s for
node-api's 150 s shutdown grace, and 7500 s for build-manager's 7200 s build
drain. Each chart's values carry the comment and the citation. Builder node-api
pods also sleep 5 s in preStop: on SIGTERM node-api closes its listener and
answers new calls "draining" at once, while the API reaches builders through the
`space-builder-node-api` Service and does not retry a draining build start, so
the delay lets the pod leave the Service's endpoints first.

**The runtime-supervisor.** node-manager's init container runs the production
`node-manager-init.sh` unchanged: it stages the host binaries under
`/space/bin/releases/<tag>`, switches the `/space/bin` links, stages the guest
kernel, and installs, starts or schedules the restart of
`space-runtime-supervisor.service` on the node through `nsenter --target 1` and
systemctl. A changed supervisor binary or compatibility file touches
`/space/run/runtime-supervisor/restart-required`; while the old node-manager runs,
the restart is deferred, and the successor node-manager performs it once it
holds its mutex (`SPACE_NODEMGR_ACTIVATE_RUNTIME_SUPERVISOR_ON_START=true`,
`node-manager-runtime-supervisor.sh activate`). Production keys the release
directory by its image tag; every Kind build is tagged `local`, so the chart
passes the digest of the staged binaries and the compatibility file as
`SPACE_IMAGE_TAG`. Firecracker jails therefore live in the node's mount
namespace and outlive every service pod. The unit reads
`/space/config/space.env`, which a production node's bootstrap writes; on Kind
the chart's `kind-node-env` init container writes it first (see the shim's
telemetry below) and touches `restart-required` when it changes, so a new
telemetry mode reaches a running supervisor through the same deferred restart.
Inspect it with
`docker exec <node> systemctl status space-runtime-supervisor` and
`docker exec <node> journalctl -u space-runtime-supervisor`.

**The operator shell.** The smoke harness's on-node checks and fault injection —
the node-local CLIs (`space-sbxmgr`, `space-tmplmgr`, `space-rtmgr`,
`space-volmgr`), `btrfs`, `dmsetup`, `cryptsetup`, `ctr`, files under
`/space/cache` and `nsenter --net` into a pod's network namespace — run in the Kind-only `space-node-debug` DaemonSet on each
sandbox worker and builder, a root shell standing in for an operator's shell on
the node host. Logs and environment are read from the pod of the service that
owns them. This keeps every service container to the identity, tools and
privileges its own code needs; the shell holds no ServiceAccount token and no
`component` label, so neither the API nor node-service readiness ever sees it.

Look at one service with
`kubectl --context kind-space-local -n space logs ds/space-<service>` (builder:
`ds/space-builder-<service>`), and at all of a node's pods with
`kubectl --context kind-space-local -n space get pods -o wide -l app.kubernetes.io/part-of=space-node --field-selector spec.nodeName=<node>`.

## Upgrading from the node-stack layout

A cluster created before the per-service DaemonSets runs one `space-node-stack`
pod per node, with runtime-supervisor and every Firecracker VM inside that pod.
Those VMs cannot move to the node's supervisor, and the two layouts must not run
on one node, so such a cluster is recreated once rather than upgraded.
The bring-up (`scripts/tests/lib/bootstrap-kind-stack.sh`) and the `Tiltfile`
run `infra/space/kind/scripts/check-node-layout.sh` against
an existing cluster before they apply anything; when it still has resources of
the old `space-node-stack` chart, they list them and stop without changing
anything (the scripts exit 1; Tilt reports a Tiltfile load error). The same
check refuses a cluster whose node containers do not publish every host port
mapping `kind-cluster.yaml.tmpl` declares, the load-balanced entry's among
them, naming each missing one. Recreating
deletes the cluster's sandboxes, snapshots and templates and keeps the
containerd image caches:

```bash
bash infra/space/kind/scripts/teardown.sh
bash infra/space/kind/scripts/up.sh
```

A cluster already on the per-service layout takes the move to internal RPC TLS
in place: the next bring-up or Tilt reload issues the TLS Secrets and every
listener and caller rolls onto TLS. While they roll, a plaintext caller and a TLS
listener refuse each other, and the services' own restarts carry them until both
sides have moved.

Organizations enrolled for BYOK before the authority used the key broker keep
an `in-memory` master-key row that no authority serves any more, so their
encrypted sandboxes and fields stay unreadable, as they already were after any
restart of the old single authority. Recreate the cluster to start them clean.

The single-worker DirectSubstrate bench (`space-local-3486`, Tilt
`SPACE_LOCAL_MINIMAL=1`) was retired with that layout: its node side was the old
entrypoint's SQL registration of the node, Postgres access from the node that
nothing else needs, and no suite ran on it.
Delete a leftover cluster with `kind delete cluster --name space-local-3486`.

## Service identities

Each node service runs under the Unix account and supplementary groups the
systemd units and EKS charts declare (`infra/space/identity/node-identities.sh`):
sandbox-manager, volume-manager and credential-manager as `space-sbxmgr`,
template-manager as `space-tmplmgr`, node-ctl as `space-nodectl`, node-api as
`space-nodeapi`, the runtime manager as `space-rtmgr`; runtime-supervisor,
node-manager and the builder's build-manager stay root. The per-sandbox
net-gateway is launched by the host runtime-supervisor with that sandbox's
gateway identity. Directory ownership matches packer `07-sysctl.sh` and the
DaemonSet init containers, so **UDS peer authorization (SO_PEERCRED) is live in
this harness** — e.g. warm-pool fills are authorized as `Service(space-tmplmgr)`,
not silently as root.

The runtime manager runs as on a deployed node: it lists the node's
Firecracker releases and kernels (`space-rtmgr list`) and installs the
catalog's (`space-rtmgr runtimes install`, `space-rtmgr kernels install`; see
[the catalog](../../../docs/runtime-manager-catalog.md)). An install downloads
from Firecracker's GitHub releases or its public CI bucket over the pod
network, checks the bytes against the compiled pins, stages them in
`/space/cache/.rtmgr-staging`, and node-manager copies them, checked again,
into the node's root-owned `/space/vendor/artifacts`. Nothing triggers an
install on its own and nothing depends on one: an operator runs the CLI from
the node's `space-node-debug` shell. Its installer needs the staging directory
node-manager creates at startup, after preparing the store
(`prepare_artifact_store`), and `space-cache.service` formats the cache on
every boot; a runtime manager that starts first exits (`Installer::open`) and
restarts on its own, as sandbox-manager does until node-manager answers.
Execution prefers a managed copy of the compiled E0 release and kernel once one
is installed. The baseline Kind smoke
checks the JSON listing on both a worker and a builder, requiring a complete
scan with a valid vendor runtime and a staged cache kernel; no suite runs an
install. These checks do not gate node readiness.

**Credential-manager.** The authority runs on the control plane as the
`space-credmgr-authority` Deployment (`charts/space-credmgr-authority-local`,
named as in the EKS module): `--role=authority --no-uds` on TCP port 9810, with
the control-plane database, the platform KEK and the optional `space-aws-creds`
for its AWS integrations. It authenticates callers by TokenReview of their
projected ServiceAccount tokens (audience `space-credential-manager`), maps each
ServiceAccount to its client service, and never accepts the static internal
token. On every request it also requires the token's pod to be current and
Running in its namespace, with the connection's source address among that
pod's IPs, and the token's node to be registered under that name and UID; its
Role lists and watches the namespace's pods for this. internal-api never
registers a node; it resolves a published name only to an existing row. The
node inventory sync records each node, with its UID, for the sandbox-selector
nodes and for every node running a node-api pod, builders
included (`crates/libs/api/src/controllers/substrate/kubernetes/node_sync.rs`);
a sandbox pod placed on a node records it too, and so does an admin node
listing (node-status refresh, reconcile or template listing), which upserts
every node-api pod's node. Until one of these records a new or replaced
builder's UID, the authority refuses that builder's callers with
`node_identity_mismatch`, and tenant template publication from it waits. With
the sync off (`SPACE_KUBE_NODE_SYNC_ENABLED`), only an admin listing or a
sandbox placement records it; turning the sync off doesn't erase a recorded
UID. Unless the storage credentials mode is `static`, volume-manager on sandbox
workers and builders exchanges its object-store credentials through its node
agent, and the builders' build-manager exchanges its own directly with the
authority ([Object-store credentials](#object-store-credentials)). Each node's
credential-manager runs `--role=agent`: it holds the node's credential sockets
under its service mutex and forwards every call upstream as the node credential
agent, with no database, KEK or AWS credential. node-manager
calls the authority directly for sandbox volume DEKs, which the authority grants
only to node-manager. The platform KEK takes the production provider path, the
intermediate-key broker: the authority mints each org's intermediate key there
on first use, keeps only the opaque wrapped key in Postgres and unwraps it
through the broker. Kind's broker is the in-repo fixture
(`charts/space-kind-key-broker`, Kind-only image `docker/Dockerfile.kind-key-broker`),
reached over plaintext because the fixture has no TLS listener. It derives keys
from the tenant reference and keeps no state, so keys survive restarts of the
broker and the authority and every replica serves every org, and so Kind's keys
protect nothing. The authority runs two replicas and rolls one at a time. Agents
started before the authority exit and are restarted until it answers.

**node-api and build-manager authentication.** As deployed services do, both
node-api roles and build-manager authenticate every caller by TokenReview of
its projected ServiceAccount token, with the static fallback off
(`SPACE_K8S_TOKEN_AUTH_ENABLED`, `SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED=false`).
The shared `SPACE_INTERNAL_TOKEN` and `SPACE_INTERNAL_CONTROL_TOKEN` are still
mounted, because the binaries require them at startup, but admit nobody. The
audiences are the code's: `space-node-api` for WorkerApi, BuilderApi and
NodeInfo, `space-node-api-internal` for WorkerApiInternal, and
`space-build-manager`. Each surface admits the accounts the code shows calling
it, set in `charts/space-node-api-local/values.yaml` (workers),
`values-builder.yaml` (builders) and `charts/space-build-manager-local/values.yaml`.
Nothing in Kind starts a peer template transfer by itself: the admin
template-pull route serves only the direct substrate, so on the default
(Kubernetes) lane it answers 501, as on any Kubernetes deployment, and only the
[Direct substrate lane](#direct-substrate-lane) serves it. An operator starts one with `PullTemplate` on the
target node-api and a `space-api` token, and the transfer list admits that call
and the target node-api's export request to the source:

| Surface | Admits |
|---|---|
| worker WorkerApi, NodeInfo | `space-api` (shared and admin api), `space-cluster-controller`, `space-data-plane` (guest bridge, guest calls only), `space-internal-api`, both node-api accounts (template transfers) |
| worker WorkerApiInternal | `space-api`, `space-cluster-controller` |
| worker lifecycle-probe `SpacedWaitReady`, caller-supplied policy, template deletion | `space-api`, `space-cluster-controller` |
| builder BuilderApi, NodeInfo | `space-api`, `space-cluster-controller`, both node-api accounts |
| builder `BuildTemplate` (system and tenant), template deletion | `space-api` |
| peer template transfers (`PullTemplate`, `NodeExportTemplate`), both roles | `space-api` (starts a pull), both node-api accounts (the target asks the source for the export) |
| build-manager (system and tenant builds) | `space-builder-node-api` |

Every caller mounts its tokens under `/var/run/space-auth` and names them with
`SPACE_NODE_API_TOKEN_PATH`, `SPACE_NODE_API_INTERNAL_TOKEN_PATH` (api and
cluster-controller) or `SPACE_BUILD_MANAGER_TOKEN_PATH` (builder node-api). An
explicit path makes a missing token an error instead of a fall back to the
static token. The tokens live 600 s, the builder's build-manager token 7200 s,
and node services' ClusterApi and credential-manager tokens 600 s, as deployed;
the kubelet refreshes each at 80% of its lifetime and the clients reread the
file. node-api and build-manager mount their own ServiceAccount's API
credentials, which may only create TokenReviews, and read the namespace their
allowlists name from that mount, as a release binary does: no chart sets the
`SPACE_K8S_TOKEN_AUTH_NAMESPACE` test hook. node-ctl's own authentication is
unchanged.

The smoke harness and the `space-node-debug` CLIs call neither surface: the
harness drives the public API, and the shell's CLIs use node-local Unix
sockets. An operator who calls build-manager directly with `space-buildmgr`
does what they would on a deployed builder: mint a short-lived token for an
admitted account with the build-manager audience, and give the CLI that token
and the cluster's internal RPC CA. A job exists only in the node-local SQLite
store of the build-manager that ran it, and the CLI's
`space-builder-build-manager` Service is `internalTrafficPolicy: Local`, so ask
from each builder's shell until one knows the job; the other answers
`NotFound`. The job ID is the `job_id` the admin template build API returns:

```bash
k() { kubectl --context kind-space-local -n space "$@"; }
job=<job-id>
for builder in $(kubectl --context kind-space-local get nodes -l space.pplx.net/node-pool=builder \
    -o jsonpath='{.items[*].metadata.name}'); do
  shell=$(k get pods -l app.kubernetes.io/name=space-node-debug \
    --field-selector spec.nodeName="$builder" -o jsonpath='{.items[0].metadata.name}')
  k create token space-builder-node-api --audience space-build-manager --duration 10m \
    | k exec -i "$shell" -c shell -- sh -c 'cat >/tmp/build-manager.token'
  k get secret space-internal-rpc-ca -o jsonpath='{.data.ca\.crt}' | base64 -d \
    | k exec -i "$shell" -c shell -- sh -c 'cat >/tmp/rpc-ca.crt'
  k exec "$shell" -c shell -- env SPACE_BUILD_MANAGER_TOKEN_PATH=/tmp/build-manager.token \
    SPACE_INTERNAL_RPC_TLS_CA=/tmp/rpc-ca.crt \
    SPACE_BUILD_MANAGER_ENDPOINT=https://space-builder-build-manager:9900 \
    space-buildmgr status --job-id "$job" && echo "ran on $builder" && break
done
```

The public template build API needs none of this and is the supported path.

**Internal RPC TLS.** Kind runs ClusterApi, NodeApi, BuildManager,
IngressActivationApi and the credential-manager authority over authenticated
TLS, as deployed services do
([Internal RPC TLS](../../../docs/internal-rpc-tls.md));
`SPACE_INTERNAL_RPC_INSECURE_DEV` is not set anywhere. Tilt runs
[`scripts/internal-rpc-tls.sh`](scripts/internal-rpc-tls.sh) before any
workload, which keeps the cluster's material in Secrets named as on EKS
(`internal_rpc_tls.tf`, `credmgr_authority_tls.tf`): a per-cluster ECDSA P-256
CA (`space-internal-rpc-ca`, mounted by every caller at `/etc/space-rpc-ca`),
one-year leaves reissued within 60 days of expiry for internal-api
(`space-cluster-api-tls`: `space-internal-api`, `space-cluster-api`), every
node-api (`space-node-api-tls`: `space-node-api`), build-manager
(`space-build-manager-tls`: `space-builder-build-manager`) and the api's
ingress activation listener (`space-ingress-activation-tls`: `space-api`, the
Service internal-api dials; the EKS module issues this Secret's leaf for
`space-ingress-activation` instead), each mounted only by
its listener at `/etc/space-rpc-tls` with the key readable by the listener's own
identity, and the authority's self-signed end-entity certificate
(`space-credmgr-authority-tls`, trusted by its clients through
`space-credmgr-authority-trust`). The leaves carry service names only, so
callers that dial node-api by node IP set `SPACE_NODE_API_TLS_SERVER_NAME=space-node-api`,
while builder node-api dials build-manager by the leaf's own name, through the
node-local `space-builder-build-manager` Service. The CA's key
stays in the Kind-only `space-internal-rpc-ca-signing` Secret, which no workload
mounts. The script restarts a listener whose identity it reissued, and every
workload if the CA itself changed, because neither reloads them while running.
TLS here is server-authenticated: projected ServiceAccount tokens, reviewed by
each listener, authorize callers (see node-api and build-manager authentication
above), and internal-api's node-address check is unchanged. The api's ingress activation
listener admits only internal-api's ServiceAccount, by TokenReview of its
projected `space-cluster-api` token with no static fallback, as
[Ingress activation delegation](../../../docs/internal-rpc-tls.md#ingress-activation-delegation)
asks of the hosting deployment.

## Differences from the EKS deployment modules

| EKS modules                                               | Local kind                                                                    |
|-----------------------------------------------------------|-------------------------------------------------------------------------------|
| One DaemonSet per node service, systemd watchdogs via k8s probes | One DaemonSet per node service and role from the Kind charts (`charts/space-<service>-local`), probed on each service's health socket; runtime-supervisor is the same host systemd unit node-manager's init container installs |
| Node image (packer) provides `/space/cache`, the cgroup subtree, loop devices, device permissions, SPACE accounts and the `/space` skeleton before any pod starts | Sandbox workers and builders boot the SPACE Kind node image, which bakes in the same as systemd units that run before kubelet on every node start (see [The node layer](#the-node-layer)) |
| `/space/cache` is btrfs on instance-store NVMe, reformatted on every boot | btrfs on a sparse loopback image at `/var/lib/space-cache.img` on the kind node (40 GiB, same code paths, less I/O headroom), mounted `nosuid,noatime` on the node's `/space/cache` by `space-cache.service`, which reformats it on every start of the node container, so like the NVMe it outlives pod restarts but not node restarts |
| The node bootstrap installs a checksum-verified `containerd-shim-space-v2` before containerd starts, and registers the `space` runtime under `io.containerd.cri.v1.runtime` with a `CniConfDir` option | the node image registers the same runtime with the same options (in kind's version 2 config format, which containerd migrates to that plugin), so containerd loads it on its first start; the binary arrives after the cluster exists, copied onto each running sandbox worker by `install-shim.sh`, which checks its SHA-256 on the node. Installing it at boot would need the shim built and staged on a host path before `kind create` |
| Every node group's bootstrap sets kubelet `eventRecordQPS: 0` and `--fail-swap-on=false` | the same settings on sandbox workers and builders, baked into the node image as a kubelet `--config-dir` drop-in (`node/kubelet/50-space.conf`); the infra node runs the unmodified base image, so its kubelet keeps kind's event rate limit (`failSwapOn` is already `false` there). No sandbox pod runs on it, so none of their events pass through that limit |
| Release binaries read no test hooks | by default every binary is built with `test-hooks`, and the charts set these hooks on every run: sandbox-manager's on-node snapshot reaper at target fraction 0, interval 15 s, grace 60 s and idle 60 s, against release values of 0.40, 300 s, 1800 s and 86400 s (`charts/space-sandbox-manager-local/values.yaml`, `snapshot_gc.rs`); template-manager's cache GC at interval 15 s and grace 60 s, against 300 s and 1800 s, and its prefetch latest-N at 1, against 5 (`charts/space-template-manager-local/values.yaml`, `eviction.rs`, `prefetch.rs`); and the disk-admission reservation below. Scenarios shorten further hooks, such as the snapshot-retention sweeper's minimum age and the activation-admission burst, for their own run and restore them. The public-operation dispatcher's tick, claim limit and claim staleness stay at their release values. The release lane (`SPACE_KIND_RUST_BUILD=release`) builds every Kind service with no crate's `test-hooks` feature, renders none of these chart hooks, and skips the scenarios that need one ([Release-build lane](#release-build-lane)) |
| space-api waits up to 120 s for a new sandbox pod's IP and proxy (`SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS`, binary default) | 45 s, set only by the Kind bring-up (`charts/space-api-local/values-kind.yaml`, which the Tiltfile passes and the caller-provided-cluster chart does not). A restore that cannot complete keeps its pod retrying, so the two btrfs-send failure scenarios wait the whole timeout; healthy creates and restores on Kind take a few seconds |
| Services export OTLP metrics every 10 s (the SDK default) | in `otel` mode the Rust services in Pods export every 2 s (`telemetry.otlpMetricExportIntervalMillis`, `scripts/telemetry_config.py`) to the bench's own Prometheus, and Tempo search withholds the newest 5 s instead of 30 s (`tempo.queryEndCutoff` in the traces chart's Kind profile); the shim and runtime-supervisor keep 10 s |
| 8 GiB disk-admission reservation per cache allocation (release default) | 1 GiB through the `SPACE_NODE_DISK_ADMISSION_ALLOCATION_RESERVATION_BYTES` test hook, with the 1 GiB `MIN_AVAILABLE_BYTES` floor, so allocations fit the 40 GiB cache (`charts/space-node-service/values.yaml`); the release lane renders only the floor, and the cache admits its binaries' release 8 GiB reservation too ([Release-build lane](#release-build-lane)) |
| Postgres on RDS, secrets via ExternalSecrets / AWS Secrets Manager | In-cluster `space-postgres` StatefulSet, one pod on a 1 GiB volume, static Kubernetes Secrets |
| PostgreSQL 17 | The same major version (`postgres:17`; CI takes it from the image mirror at a pinned digest) |
| Every client connects with `sslmode=require`              | The same `sslmode=require` on every connection: the services and the migrate Job through the `space-postgres` Secret's connection string, and the harness's `psql` helpers over loopback TLS inside the pod. The certificate is self-signed per pod start, which `require` accepts because it does not verify the server. Kind's server also refuses unencrypted TCP, so no Kind client runs in plaintext and one that sets `sslmode=disable` fails. A client that omits `sslmode` still connects over TLS, because libpq and sqlx default to `prefer`, so Kind cannot catch a connection string that loses `sslmode=require` |
| Bootstrap and migrations run as the managed service's master user, which is not a superuser but a member of `rds_superuser` | The same branch of `bootstrap-roles`: every connection string names `space_admin`, which has `CREATEDB CREATEROLE` and `rds_superuser` WITH ADMIN OPTION and owns the database. A stock server has none of that, so the pod's init script creates it, including the `rds_superuser` role itself and its `SET` privilege on `log_statement`, which a managed service grants that role and `bootstrap-roles` needs before writing passwords. The image's superuser remains, used only by the image's init and the harness's direct-state `psql` |
| Per-service pools: the api tier far above the binary's default of 25, internal-api somewhat above it, every other service on the default; no connection pooler | `POSTGRES_POOL_MAX_CONNECTIONS` 100 for space-api and space-admin-api, 30 for internal-api, unset elsewhere; no pooler. `max_connections` (1400) holds every replica's pools at their HPA maxima plus a rollout surge, the migrate Job and headroom, so a service runs out of its own pool before the server refuses it. The pod's memory limit (8 GiB) is sized for that many backends rather than for the suites' usual load |
| Server logs lock waits; the application roles' `lock_timeout` is off (`SPACE_DB_APP_LOCK_TIMEOUT` `0`) | `log_lock_waits = on`; `SPACE_DB_APP_LOCK_TIMEOUT` is unset, which leaves `lock_timeout` off the same way. Other server parameters are the image's defaults rather than a managed parameter group |
| The migration Job fails fast: one attempt, no restarts, its deadline passed to the migrator | The same `space-db-migrate` Job shape (`backoffLimit: 0`, `restartPolicy: Never`, `activeDeadlineSeconds` = `SPACE_DB_MIGRATE_JOB_DEADLINE_SECONDS`), bootstrap-roles → migrate → bootstrap-roles. The bring-up stops on a failed Job with its logs; `tilt trigger space-db-migrate` replaces the Job |
| S3 for snapshots + template artifacts                     | In-cluster versitygw (real S3 protocol, sigv4, path-style)                    |
| volume-manager gets each transfer's credentials from its node's credential-manager agent; the authority authorizes the subject and object and assumes the S3 vend role through STS with a session policy for that one object, which S3 enforces on every request | the same path on sandbox workers and builders ([Object-store credentials](#object-store-credentials)): the authority's unchanged `AssumeRole` reaches the Kind-only STS bridge (`space-kind-sts-bridge`), which exchanges it at versitygw's IAM service; that service checks the vend role's trust and maximum duration and stores the session, whose policy the gateway enforces. What stays different: the authority signs `AssumeRole` with the bridge's static vend keys (a Secret `sts-bridge-secret.sh` issues per cluster) where a deployment uses its workload identity, so those keys replace `space-aws-creds` and the authority's AWS Secrets Manager integration is off in this mode; and STS runs beside the store: the IAM service is its own Deployment, so an outage refuses every session-signed request while the gateway stays Ready and root keys keep working, but no AWS account boundary separates STS from S3. `SPACE_KIND_STORAGE_CREDENTIALS=static` opts out: volume-manager and build-manager then read static `SPACE_OBJECT_STORE_STATIC_*` creds (the documented local escape hatch) and no vend role is configured |
| Rate limiters (cluster activation admission at the binary defaults, per-client concurrency in `observe`) on a Redis-compatible store reached over TLS (`rediss://`), its URL delivered through the `space-redis` Secret (`app_deploy.tf`) | the same limiters, modes and defaults on the in-cluster `space-redis` Deployment (one in-memory replica on the infra node), reached over plaintext `redis://space-redis:6379` rendered as a literal: the workspace's Redis clients trust only the webpki roots (`tls-rustls-webpki-roots` in `Cargo.toml`), so they would refuse a CA `internal-rpc-tls.sh` could issue, and the `#insecure` URL fragment would be an environment-only switch. `local.rate_limiters_enforce_and_fail_open` shrinks the caps and scales the store to zero for its fail-open half |
| `SPACE_REDIS_URL` on api, admin-api, cluster-controller and internal-api | on api, admin-api and cluster-controller only: the limiters that read it live in `crates/libs/api` (api and admin-api build both, the cluster-controller's public-operation dispatcher builds the activation limiter), and nothing `space-internal-api-svc` links reads it |
| `space-credmgr-authority` Deployment whose platform KEK comes from an external intermediate-key broker, with TLS from its own certificate (`credmgr_authority_tls.tf`; the direct KMS backend is gone, `credmgr_authority_kms.tf`) | two replicas on the same broker path against the in-repo broker fixture (`space-kind-key-broker`), which derives keys instead of protecting them; TLS from a self-signed certificate `internal-rpc-tls.sh` issues per cluster |
| Internal RPC CA and listener leaves issued by Terraform and synced by External Secrets (`internal_rpc_tls.tf`) | the same Secrets issued per cluster by `scripts/internal-rpc-tls.sh`; the ingress activation leaf names `space-api` rather than `space-ingress-activation` |
| Builder volume-manager and build-manager exchange their credentials through credential-manager | the same in the default `exchange` mode, with no store keys: builder volume-manager through its node's agent, and build-manager directly with the authority over TLS, under its projected ServiceAccount token (audience `space-credential-manager`), which the authority maps to `space-build-manager` ([Object-store credentials](#object-store-credentials)). The authority admits a builder's callers once the node inventory sync has recorded the builder's Kubernetes UID ([Service identities](#service-identities)). In `static` mode both read the store's static keys, like worker volume-manager |
| cluster-controller reaches S3 with its workload identity   | the store's static root keys as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in both modes: Kind has no workload identity provider to reproduce |
| `/dev/kvm` access via the AMI's kvm group                 | `space-kind-devices.service` makes `/dev/kvm`, `/dev/vhost-vsock` and `/dev/vhost-net` mode 0666 on the node (kind-only; never replicate on a worker) |
| Loop devices from the AMI's `max_loop=10000` on a real devtmpfs | The node's tmpfs `/dev` starts with 8 nodes; `space-loop-nodes.service` mknods the rest before kubelet, pods copy the node's `/dev`, and node-manager raises its pod's pool further. The cache device has autoclear set, so a node stop releases it (`scripts/lib/space-loop-devices.sh`) |
| vhost-vsock jail nodes provided by the AMI                | the Kind-only `space-kind-vsock-jails.service` mknods them into each jail under `/space/cache/firecracker/jails` as it appears (kind-only) |
| Sandbox SSH key generated by Packer per AMI               | generated per host by `build-seed.sh` under `~/.space-devbox/kind/ssh` |
| ALB ingress in front of api / data-plane / ui             | NodePort entry Services behind the cluster's host port mappings for api and data-plane (kube-proxy balances per connection); Tilt's port forward for the single-replica ui |
| preStop delays on api, admin-api, data-plane and internal-api sized for the load balancer's deregistration | the same hooks at 5 s: kube-proxy removes a terminating pod from the NodePort entry and ClusterIP Services in about a second, so a deployment-sized delay would only slow every rollout |
| Control-plane replicas spread across nodes | no anti-affinity or topology spread: every control-plane pod runs on the one infra node, where a spread constraint has nothing to spread over and a required anti-affinity would leave second replicas Pending |
| space-admin-api under a PodDisruptionBudget like the public API | one replica (operator traffic only) under the same `maxUnavailable: 1` budget, so it never holds a drain and does not keep the admin surface up during one; it rolls by `Recreate`, so a rollout leaves the admin surface down while the old pod drains and the new one starts |
| Reconcile-queue GC (`SPACE_RECONCILE_QUEUE_GC_*`) prunes dead letters and asks EC2 whether a non-ready node's instance is gone | the same GC, on at the binary's fixed timers (a 300 s tick; 24 h before a dead letter, or a non-ready node's live ref, can be pruned), asking the Kind-only fake EC2 endpoint (`space-kind-fake-ec2`) instead of EC2. The controller reaches it through `AWS_ENDPOINT_URL_EC2` over TLS from the internal RPC CA, and its pod resolves `ec2.<region>.amazonaws.com` to loopback, so a lost override fails closed (the GC reads the error as unknown and keeps the refs) rather than calling AWS. The fake answers `DescribeInstances` by private DNS name from the Node of that name: Ready is `running` and NotReady `stopped`, which keep the refs, and no Node is no instance, which reads as gone. It checks that a request is signed for EC2 in the region, not the signature, which the controller makes with the object store's keys rather than a workload identity. No public path makes a row 24 h old, so `local.reconcile_queue_gc_prunes_only_gone_nodes` seeds two synthetic non-ready node rows with 25 h-old refs and waits out one tick: a 25 h-old dead letter and the ref of the node no Node names are pruned, while a 1 h-old dead letter and the ref of the node named after a Ready Node are kept. Otherwise a cluster younger than 24 h holds no candidate, and the fake is not called. The gone-node queue GC runs, dry-run |
| The runtime manager installs releases and kernels, which node-manager publishes into the root-owned store `/space/vendor/artifacts` | the same: each sandbox worker and builder has its own root-owned `/space/vendor`, copied from the read-only host vendor tree on the node's first boot, and the runtime manager runs without `--list-only`. Until a managed E0 is installed, execution reads the baked release from `/space/bin/runtime/firecracker` (node-manager's `SPACE_FIRECRACKER_VENDOR_DIR`), the node layer's copy, rather than from `/space/vendor/firecracker`. node-manager seals execution copies with fs-verity on the execution cache `/space/cache/firecracker`, not on the store (`require_verity_api` in `stage_release_at`, `stage_sealed_kernel_at`); that cache is the node's btrfs loop image on the host's kernel, and where that kernel gives it no fs-verity node-manager logs `fs-verity unavailable` and keeps its verified legacy execution protection. No suite runs an install |
| Operators run node-local CLIs from the node host's shell   | the same CLIs run in the Kind-only `space-node-debug` root shell DaemonSet     |
| Release images record the CI commit as their build SHA (`ci/scripts/run-space-bundle-builds.py`) | the same from a clean worktree, as its 12-digit short form; an image the worktree's changes reach records `<commit>-dirty-<content>` instead, computed when it is built. `service.version` stays the commit read when Tilt or bootstrap started. The smoke bring-up refuses a SPACE container that does not run the latest build of its image; under `SKIP_BOOTSTRAP=1` only once Tilt has nothing pending, and it warns instead while Tilt is updating ([Build identity and provenance](#build-identity-and-provenance)) |
| Release binaries, built without `test-hooks`              | by default binaries built with `test-hooks` and LTO off (`docker/Dockerfile.rust-build-kind`), so smokes can shrink timers and flip scenario switches; the release lane builds them with thin LTO and no crate's `test-hooks` feature; no chart sets a hook for authentication, and the TokenReview listeners read their namespace from the ServiceAccount mount as release binaries do |
| Build sizing, concurrency, and multipart sizes are configurable | 20 GiB default build disk, one concurrent build, and smaller multipart parts fit the 40 GiB local btrfs cache |

`space-seed-publish` remains the bootstrap for `seed:latest`; it is not a
substitute for recipe builds. ECW `Template.build` reaches the builder
node-api, build-manager launches a real build VM, and the result is published
back through the `templates` bucket. The builder sandbox-manager enables
incomplete-route compatibility because build-manager's node-local recipe VMs
have no public control-plane route identity. The worker release keeps that
compatibility disabled.

> **Note for the node-drain e2e test**: draining a sandbox worker terminates
> its k8s Node object (`SPACE_NODE_DRAIN_TERMINATOR_KIND: kube-delete`).
> Recover the cluster with
> `docker exec space-local-worker<N> systemctl restart kubelet`.

## Node layout

The cluster has six nodes with dedicated roles so that draining a sandbox
worker cannot disrupt control-plane components:

| Node                          | Pool label  | Purpose                                                                                 |
|-------------------------------|-------------|-----------------------------------------------------------------------------------------|
| `space-local-control-plane`   | —           | k8s control-plane only                                                                  |
| `space-local-worker`          | `sandbox`   | Runs the worker node-service DaemonSets + receives SPACE sandbox pods                    |
| `space-local-worker2`         | `sandbox`   | Same, second worker                                                                      |
| `space-local-worker3`         | `infra`     | Runs Postgres, object store, space-api, internal-api, data-plane, cluster-controller, UI    |
| `space-local-worker4`         | `builder`   | Runs the builder node-service DaemonSets, including build-manager and builder node-api    |
| `space-local-worker5`         | `builder`   | Same, second builder                                                                       |

Sandbox pods carry `nodeSelector: node.kubernetes.io/space-enabled=true`,
which is only applied to the two sandbox workers. Control-plane components
carry `nodeSelector: space.pplx.net/infra=true`, which targets only the
infra node. The builder release selects only `pool=builder`, and the builder
nodes are deliberately not `space-enabled`, so user sandbox pods cannot land
there.

Each sandbox pod requests `space.pplx.net/network-slots: 1`. As on a deployed
node, each sandbox worker's node-ctl advertises that extended resource on its
Node's `status.capacity`, from its node-manager's `network_pool_max_slots`
(`crates/libs/node/ctl/server/src/kubernetes_controller/network_slots.rs`), and
re-advertises it on every periodic tick, including after the Node object is
recreated. node-manager keeps no warm pool of slots, as deployed nodes do not
(`SPACE_NODEMGR_NETWORK_POOL_TARGET`, `_RETAIN_READY` and `_MIN_READY` are `0`
in `charts/space-node-manager-local`): each slot, a sandbox's or a build's, is
set up when it is allocated and torn down when it is released, and no slot is
kept ready for the next allocation.

## The node layer

A production worker or builder boots from a node image that already holds its
node layer (`infra/space/packer`): the packages, scripts and enabled systemd
units that give it a btrfs `/space/cache`, a cgroup subtree, loop-device
headroom, device permissions, SPACE accounts and the `/space` skeleton before
any SPACE pod starts. Kind does the same with the **SPACE Kind node image**,
[`node/Dockerfile`](node/Dockerfile): `kindest/node:v1.34.0` (the default of
the pinned kind v0.30.0, pinned by digest) plus `btrfs-progs`,
[`node/space-kind-node.sh`](node/space-kind-node.sh) with its libraries, and
these units, enabled in the image:

| Unit | Does | Production counterpart |
|---|---|---|
| `space-loop-nodes.service` | mknods loop-device nodes in the node's `/dev` (64, raised past any index in use); pods copy the node's `/dev` when they start | `max_loop=10000` (07-sysctl.sh) |
| `space-kind-devices.service` | mode 0666 on `/dev/kvm`, `/dev/vhost-vsock`, `/dev/vhost-net` | the `kvm` group; Kind-only, never on a worker |
| `space-cgroup.service` | `/sys/fs/cgroup/space` with `+cpu +memory +io +pids` delegated | `space-cgroup.service` (07-sysctl.sh) |
| `space-layout.service` | the SPACE groups and service accounts; the `/space/run` skeleton and dependency-drain gates; the node's root-owned `/space/vendor`, copied from the host vendor tree on first boot; protected Firecracker copies under `/space/bin/runtime/firecracker` and `/space/bin/{firecracker,jailer}` links; `/space/etc/sandbox.toml`; `/var/run/netns` | 07-sysctl.sh accounts and directory skeleton, 04-firecracker.sh runtime installation |
| `space-cache.service` | formats `/var/lib/space-cache.img` on every boot (sparse, 40 GiB), mounts it `nosuid,noatime` on `/space/cache` with loop autoclear and shared propagation, then creates the cache skeleton | 01-partitions.sh / 11-nvme-cache.sh |
| `space-kind-vsock-jails.service` | long-running: mknods `/dev/vhost-vsock` into each Firecracker jail under `/space/cache/firecracker/jails` as it appears (the jailer does not) | the node image provides it; Kind-only |

They are ordered `Before=kubelet` and run on every boot of the node container,
including after `docker restart`, which gives it a fresh `/dev`, mount table
and cgroups; all but the vhost-vsock unit are oneshots. Nothing is installed
when the cluster comes up. The sandbox workers and builders run this image
(`kind-cluster.yaml.tmpl`); the control-plane and infra nodes run its
base unchanged, as production's non-SPACE nodes carry no SPACE node layer, so
every node runs the same Kubernetes version. The cache survives pod restarts but
not node restarts, as the instance-store cache of a deployed node does: on every
start of the node container `space-cache.service` truncates the image on the
node's `/var` and formats it afresh, so templates, sandboxes and snapshots on the
cache are gone and the node services re-create what they need (template-manager
pulls templates again, node-manager's init container stages the guest kernel
again). What was put on the cache by hand is not re-created: templates the
optional `scripts/bootstrap-prod-seed.sh` staged must be staged again by
re-running it, which skips the workers that still have them. Re-running the
unit within one boot keeps the mounted cache. Defaults are baked like the rest
of the layer; a node's `/etc/space/kind-node.env` can
override `SPACE_CACHE_SIZE_MB` and `SPACE_LOOP_DEVICE_TARGET` for its next boot.

The image also carries the kubelet and containerd settings a deployed node's
bootstrap applies (`infra/space/aws/eks/modules/space/templates/`): a kubelet
`--config-dir` drop-in (`node/kubelet/50-space.conf`) with no client-side event
rate limit (`eventRecordQPS: 0`) and `failSwapOn: false`, and the `space`
runtime handler with its `CniConfDir` option (`node/containerd/space-runtime.toml`).
The drop-in is written in the version 2 format of kind's own containerd config,
which imports it, so containerd migrates both into the CRI v1 runtime plugin
where a deployed node has the handler, and kind's runc settings (the systemd
cgroup driver, which kubelet adopts from containerd) still apply; the image
build fails otherwise. Check them with
`kubectl get --raw /api/v1/nodes/<node>/proxy/configz` and `docker exec <node>
containerd config dump`.

The host vendor tree (`SPACE_VENDOR_ROOT`) is a developer-owned download
cache, mounted read-only at `/opt/space-kind/host-vendor` and never written. On
a node's first boot `space-layout.service` copies it into a fresh directory,
makes the copy root-owned and closed to group and other writes, and renames it
to `/space/vendor`, the root-owned tree a deployed node's image bakes; the
node's rootfs keeps it, and what node-manager publishes into
`/space/vendor/artifacts`, across node restarts, and a changed host tree
reaches a node only when the node is recreated. `scripts/check-node-vendor.sh`,
which `up.sh` (before it stops a running Tilt), the smoke harness and Tilt
run on a reused cluster,
refuses one whose nodes' copies differ from the host tree (contents and link
targets, `artifacts/` aside). A `/space/vendor` that is a
mount, the layout of a cluster created before this copy, fails the unit. At
every boot the unit then copies the bootstrap Firecracker 1.15.1 binaries from
the node's tree into root-owned `/space/bin/runtime/firecracker`; node-manager
uses this path and verifies the compiled binary digests before execution. Repeated boots keep the
installed files unchanged. A differing installed copy fails staging and requires
node recreation rather than overwriting binaries that a sandbox may be using.
The production init container still stages the guest kernel into the cache.
Recreate existing Kind clusters to pick up this node-image change.

**Distribution.** [`kind-node-image.sh`](scripts/kind-node-image.sh) follows
`ci/scripts/ci-image.sh`. The tag is the SHA-256 of the image's inputs (the
Dockerfile and every file it copies), and the build context holds exactly those
files. `ensure` uses a local image with that tag, else pulls it from
`ghcr.io/perplexityai/space-kind-node`, else builds it (about ten seconds on a
warm Docker cache). The bring-up runs it before `kind create`, and the
renderer writes the tag into the cluster config. Main CI publishes each new tag
once; Kind CI jobs pull it and fall back to a build. Builds take the
`kindest/node` base from the third-party image mirror (`ci/mirrored-images.txt`)
when `SPACE_CI_IMAGE_MIRROR` is set, and from the source otherwise.

With `SPACE_CI_IMAGE_MIRROR` set, `ensure` also pulls that base for the
control-plane and infra nodes from the mirror. The mirror holds the index's
linux/amd64 manifest, which is the only digest the pulled copy resolves under.
So the renderer names the base by that digest, and `kind create` uses the copy
instead of pulling the index from Docker Hub.
- This holds only while main's mirror job has a record that the index lists
  that manifest (`ci/scripts/mirror-images.sh`). The renderer checks the record
  with one bounded read.
- With no record (a list change main has not published yet), or a record that
  names another digest, nothing uses the listed digest. `ensure` takes the
  pinned index from Docker Hub, and the config names the index.
- The lookup needs Docker's containerd image store, which Kind CI enables. On
  the classic store, `kind create` pulls the manifest from Docker Hub as before.

Elsewhere the config names the index and kind pulls it. A package
administrator has to make the `space-kind-node` package public for bring-ups
outside CI to pull it; until then they build it, with the same result.

Firecracker, its jails and the per-sandbox sidecars run on the node under
`space-runtime-supervisor.service`, so the jails live in the node's mount
namespace, a slave of the node's shared `/space/cache`. node-manager mounts the
cache `Bidirectional`: the bind mounts it makes into a jail — the instance
rootfs before launch, the snapshot memory file after Firecracker is already
running on restore — propagate to the node and from there into the jail. Every
other pod receives the cache `HostToContainer` (`shared,slave` inside the pod):
it sees the node's cache, its remounts and node-manager's mounts, and the
mounts it makes itself (build-manager's OCI rootfs and assembly mounts) stay in
the pod and never reach the node's mount table. Check a node's view with
`docker exec <node> findmnt -o TARGET,FSTYPE,PROPAGATION`.

Diagnose a node with `docker exec <node> systemctl status 'space-*'` and
`docker exec <node> journalctl -u space-cache`. To start a node's cache clean,
`docker restart <node>`; for a new size, first add `SPACE_CACHE_SIZE_MB=<MiB>`
to the node's `/etc/space/kind-node.env`. Recreating the cluster also starts
clean.

**Clusters created before this image, or from an older build of it, must be
recreated.** A kind node keeps the image it was created from, so such a
cluster's SPACE nodes lack the layer or carry a stale one. The bring-up and
Tilt's `check-kind-node-image` resource run
[`check-kind-node-image.sh`](scripts/check-kind-node-image.sh), which compares
each SPACE node's image label with the tag this tree builds and, on a mismatch,
stops with the command to run:

```bash
kind delete cluster --name space-local   # keeps the containerd image caches
bash infra/space/kind/scripts/up.sh
```

Recreating rather than patching nodes in place is the production model: a new
node image reaches a fleet by replacing nodes, not by installing onto running
ones. Keeping an in-place path would have meant a second delivery mechanism
(installing packages and units into live node containers) that has to track
every change to the image and is exercised only on old clusters, which is where
it would drift. Recreation costs a few minutes because the containerd image
caches stay on disk; it does delete the cluster's sandboxes, snapshots and
published templates.

## Smoke test

Kind CI produces the Kind Rust artifact image once per run and hands that exact
image to six isolated runners. Each runner brings up its own Kind/Firecracker stack,
runs the baseline smoke, then runs one of six duration-balanced partitions of
the eligible ECW and native inventories. Scenarios remain serial within each
shard because some mutate cluster-wide state. Every shard must pass for
`space-ci` to pass.

The CI-only `SPACE_KIND_RUST_ARTIFACTS=prebuilt` mode checks
`SPACE_KIND_PREBUILT_RUST_IMAGE_ID` against the loaded Docker image and
`SPACE_KIND_PREBUILT_RUST_SHA`, the commit the image was built from, against
the checkout. For an image built from another commit, it instead requires the
image's input key label, `SPACE_KIND_PREBUILT_RUST_KEY` from the handoff, and
this checkout's recomputed key to agree. A missing or mismatched artifact fails
rather than falling back to compilation. Local build defaults are unchanged.

A PR's producer skips the build when main already built an image from the same
Rust inputs. `build-rust-artifacts.sh --input-key` hashes everything the
Docker build reads:

- both Rust Dockerfiles, and the git mode and blob ID of every path they `COPY`;
- the `.dockerignore`, any `<Dockerfile>.dockerignore`, and every
  `.gitattributes` from the root down to each input;
- the build script and the architecture;
- the digest the effective mirrored-image list pins for each registry base.

Commits and times are not part of it. There is no key when an input is
uncommitted or untracked, or when a Dockerfile reads the context in a way the
key cannot list, such as `ADD` or a bind mount.

Main builds on every push, which proves the producer still builds at its
commit. A build from committed inputs, with every base from its mirrored
digest, labels the image with its key and commit. A separate main-only job then
pushes it to `ghcr.io/perplexityai/space-kind-rust-artifacts:<key>`, replacing
the key's earlier image, and keeps the twenty newest versions. The shards
never wait for that push.

Each PR first tries to pull its key's image. Any miss or failure builds as
before, with a notice, including a fork, or any PR whose token cannot read the
package. Setting the repository variable `SPACE_KIND_RUST_REUSE` to `off`
turns PR reuse off without a code change. A reused image goes through the same
handoff. The runtime images Tilt builds still record the checkout's commit.

A PR whose Rust inputs match no published image builds incrementally from
main's build state instead. Main CI's `kind_rust_state` job builds each main
commit cold in incremental mode, beside the Kind shards and outside the
required `space-ci`. It saves three directories to the GitHub Actions cache as
zstd tar parts (`infra/space/kind/scripts/rust-state-cache.sh`): the Cargo
target directory, the content-synced source mirror, and the Cargo home. A pull
request's producer restores the newest state for the same toolchain, Cargo
config, builder image and its pinned base, build lists and build scripts
(`rust-state-cache.sh key`), preferring one built from the same `Cargo.lock`,
and runs `build-rust-artifacts.sh` in incremental mode. The mirror keeps the
mtimes of files whose content did not change, so Cargo recompiles only the
crates whose inputs changed since main's build. Without a restored state, or
when a part is missing or fails to unpack, the producer builds in Docker mode as
before. The merge queue runs no Kind smoke, so for a PR that selects Kind its
own run is the last Kind check before merge, and it tests incrementally built
artifacts whenever a state restores. Building main's state cold keeps a saved
state from inheriting a stale output of an earlier build, but an input Cargo
does not track would carry main's compiled crate into a restoring PR; crates
that embed a directory (`sqlx::migrate!`) watch it from a build script for that
reason. Main CI's `rust_cache_cleanup` prunes superseded states with the other
target-directory caches, keeping the newest state for each of the last three
key inputs, so up to four states (about 0.8 GB each) can exist while a push
saves. Setting the repository variable `SPACE_KIND_RUST_STATE` to `off` turns
both the save and the restore off.

To reproduce a shard, use `scripts/tests/smoke-kind-typed.sh all 1/6` (through
`6/6`) with the same admin-token setup as a full `all` run, on a bench brought
up with `SPACE_KIND_TELEMETRY_MODE=otel` and that variable still set, as CI
does. With the variable set, a bench in another mode fails any shard holding a
scenario that declares a telemetry mode, the brownout's included; without it,
each such shard skips those scenarios and can pass. The direct CLI equivalent
is `python -m space_smoke run --all --shard 1/6` for the selected API surface.
Shards partition the eligible scenarios by duration hint, as described above;
run every shard from the same checkout. Never run shards concurrently against
the same cluster. Without a shard argument, the full inventory and its existing
ordering are unchanged.

Several smokes target this cluster:

```bash
# 1. Portable typed ECW inventory on the real Kind/KVM substrate
SKIP_BOOTSTRAP=1 scripts/tests/smoke-kind-typed.sh ecw

# 2. Minimal e2e: login → create → mkdir → write → read → delete
SKIP_BOOTSTRAP=1 scripts/tests/smoke-kind-e2e.sh

# 3. Full native API surface: + metadata, identity boundary, command SSE,
#    data-plane WS, pause/resume, suspend/restore. Set
#    SPACE_SMOKE_SKIP_INTERACTIVE=1 to skip the interactive WS helper
#    (which was finicky behind kubectl port-forward) and reach the
#    lifecycle stages.
TOKEN=$(SPACE_CONSOLE_PASSWORD="${SPACE_CONSOLE_PASSWORD:-password}" \
  jq -nc '{email:"root@local",password:env.SPACE_CONSOLE_PASSWORD}' | \
  curl -sS -X POST http://localhost:8080/auth/v1/sessions \
  -H 'content-type: application/json' \
  --data-binary @- | jq -r .token)
SPACE_API_URL=http://127.0.0.1:8080 \
  SPACE_DATA_PLANE_URL=http://127.0.0.1:8090 \
  SPACE_API_TOKEN="$TOKEN" SPACE_TEMPLATE=seed:latest SPACE_RUNTIME=fc \
  SPACE_SMOKE_SKIP_INTERACTIVE=1 SPACE_READY_TIMEOUT_MS=300000 \
  scripts/tests/smoke-spaced-rest.sh

# 4. Forced node drain: the resident is evacuated to the second worker through
#    a real suspend snapshot in the object store and its filesystem survives. The
#    admin endpoint's default is `gentle`, which cordons and waits for
#    residents to leave instead; this scenario sends `forced`.
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw \
  uv run --frozen --python 3.11 -m space_smoke run local.forced_drain_evacuates_sandbox

# 5. Public ECW Template.build → ready template → sandbox → command checks
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw \
  uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots

# 6. Explicit AWS Secrets Manager network-credential injection. Before
#    up.sh, export short-lived sandbox-account AWS_ACCESS_KEY_ID,
#    AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, and
#    SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN (for example from the gitignored
#    infra/space/kind/private.env); with static storage credentials, Tilt's
#    space-aws-creds resource copies them into the authority's Secret. The selected
#    identity must be able to sts:AssumeRole and sts:TagSession on that role;
#    the role reads the task-owned ARN only when the secret's space-org-id tag
#    equals the smoke organization's ID (docs/configuration.md).
#    The controlled HTTPS endpoint must return only the lowercase SHA-256 of
#    the Authorization header it receives. Bring the cluster up with
#    SPACE_KIND_STORAGE_CREDENTIALS=static: with exchanged storage
#    credentials the authority's AWS credentials and STS endpoint are the
#    STS bridge's (see "Object-store credentials").
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=native \
  SPACE_API_URL=http://127.0.0.1:8080 \
  SPACE_DATA_PLANE_URL=http://127.0.0.1:8090 \
  SPACE_API_TOKEN="$TOKEN" SPACE_TEMPLATE=seed:latest \
  SPACE_SMOKE_SECRET_ARN="$TASK_OWNED_SECRET_ARN" \
  SPACE_SMOKE_ECHO_URL="$CONTROLLED_DIGEST_ECHO_URL" \
  SPACE_SMOKE_SECRET_HEADER_SHA256="$SECRET_HEADER_SHA256" \
  uv run --frozen --python 3.11 -m space_smoke run \
    native.aws_secret_network_credential_injection
```

The AWS scenario is excluded from implicit inventory runs because its secret
and echo service are intentionally caller-owned. It deletes its sandbox,
detaches the policy rule, and retires the SPACE credential reference; the
caller remains responsible for deleting the AWS secret and echo fixture.

The template-build scenario (typed, run by name; the shell suite was retired
2026-09-23) seeds a run-scoped local ECW key, uses the pinned E2B Python SDK
against `http://127.0.0.1:8080` with the allowlisted loopback Host header, and
routes command checks through the public data-plane on `:8090`.
Using loopback for both the URL and Host also makes the SDK's separate upload
client resolve the absolute upload URL without local DNS configuration.
`Template.build` polls the public build-status endpoint until the recipe and
publication phases report ready. The smoke publishes both `latest` (so
`Sandbox.create(alias)` resolves the release) and a `kind-smoke` marker tag,
then requires the first public sandbox launch to succeed. Guest commands use
the public data plane, where the API route token is available.

### Debugging a template build

All commands below pin the kind context explicitly:

```bash
kubectl --context kind-space-local -n space get ds,po,svc -o wide \
  -l app.kubernetes.io/name=space-builder
kubectl --context kind-space-local -n space describe ds/space-builder-build-manager
kubectl --context kind-space-local -n space logs ds/space-builder-build-manager --tail=200
kubectl --context kind-space-local -n space logs ds/space-builder-node-api --tail=200
kubectl --context kind-space-local -n space logs ds/space-builder-template-manager --tail=200

aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://templates/ --recursive
```

`ds/<name>` picks one of the two builders' pods; name a pod to read a specific
builder. build-manager is Ready once its `:9900` gRPC health service serves,
and node-api once its health socket reports ready. A Pending pod usually means
the `pool=builder` node is missing; a running but unready pod should be
diagnosed from its own log and, after a restart, from
`kubectl logs <pod> --previous`. The build VM's commands log through
build-manager, which the Datadog Agent and the Alloy log stack exclude.

**Current pass/fail map** (as of this revision):
- `smoke-kind-e2e.sh` — **PASS** (create → real Firecracker boot of the
  locally built seed → file round-trip → delete).
- `smoke-spaced-rest.sh` — **PASS** through metadata, identity boundary,
  command SSE, file ops, pause/resume (~1s each), and suspend/restore
  (~6s round trip incl. the object-store upload). The trailing immediate
  DELETE can hit a retryable "suspend-snapshot upload in flight" conflict
  when issued within seconds of the lifecycle churn — retry it.

Two harness pitfalls worth knowing about, both fixed here and both
producing the same wedge (snapshot rows stuck in `creating`, restores
never firing) if reintroduced:
- **Mount propagation**: the Firecracker jailer unshares a mount
  namespace, and node-manager bind-mounts the snapshot memory file into
  the jail *after* FC is running. On a prod host the systemd-`rshared`
  root propagates that mount into the jail; from a pod it propagates only
  if the pod's mount is `Bidirectional` and the node's `/space/cache` is
  shared, and otherwise FC opens the 0-byte bind target ("Total sum of
  memory regions exceeds largest possible file offset"). node-manager's
  chart mounts the cache `Bidirectional`; `space-cache.service` makes the
  node's mount shared.
- **Object store layout**: versitygw's POSIX backend stores each bucket as a
  directory and each object as a file under the infra node's
  `/var/lib/space-object-store`, with ETags and user metadata in `user.*`
  xattrs. Beside the buckets, the IAM service keeps its state in `.iam/`
  (the OIDC provider, the vend role and every live session), and the gateway
  its object locks in `.vgwlocks/`; neither is a valid bucket name, so
  neither is listed or served as a bucket. kind mounts every node's `/var`
  as an anonymous Docker volume and the infra node's only extraMount is
  `/var/lib/containerd`, so the data and its xattrs live on the
  filesystem backing that volume (under Docker's data root), not on the
  node's overlay root. There are no
  pre-allocated volumes or volume slots to exhaust (the SeaweedFS chart it
  replaced needed `-master.volumeSizeLimitMB=1024 -volume.max=48`), but
  CompleteMultipartUpload assembles the parts with `copy_file_range`, which on
  ext4 rewrites the object once.

One product-hardening observation stands: after a genuinely failed restore
attempt, sbxmgr's restore guard only self-heals `Suspended`/`NotFound`
local states, so a sandbox whose local state reached `failed` wedges
(every retry FailedPreconditions, DELETE stays blocked on the in-flight
upload). Only reachable after a real restore failure.

## Real S3 snapshot + template path (object store + volume-manager)

`space-object-store` runs versitygw, an S3-compatible gateway, on the infra node, with
versitygw's standalone IAM service in its own Deployment beside it
(`space-object-store-iam`) and a one-shot Job that creates the `snapshots` and `templates` buckets. The S3
API is port-forwarded to [localhost:8333](http://localhost:8333).

`space-volmgr-svc` performs real S3 transfers to the object store — both
suspend/restore snapshot artifacts (`snapshots` bucket) and template pulls
(`templates` bucket, populated by the `space-seed-publish` Job) — with the
credentials the storage credentials mode gives it (below). See
`crates/libs/vol/mgr/server/src/run.rs`.

Verify the round-trips with the object-store port-forward:

```bash
aws --endpoint-url http://localhost:8333 s3 ls s3://snapshots/ --recursive
aws --endpoint-url http://localhost:8333 s3 ls s3://templates/ --recursive
```

### Object-store credentials

`SPACE_KIND_STORAGE_CREDENTIALS` selects how the node services that transfer
objects, volume-manager on sandbox workers and builders and the builders'
build-manager, authenticate to the object store
([`lib/storage-credentials.sh`](lib/storage-credentials.sh)):

- `exchange` (the default): as deployed, volume-manager holds no keys. For
  each transfer it asks its node's credential-manager agent, on the volume
  socket, for a session scoped to the transfer's one object
  (`exchange_credentials`, `crates/libs/vol/mgr/server/src/credentials.rs`):
  scope `space.storage.read` for a pull and `space.storage.read
  space.storage.write` for a push (which verifies its upload with HeadObject),
  the SPACE subject (`sandbox:<id>` or `template:<name>`) and a one-hour
  lifetime. The agent forwards the request as itself, with the caller's UDS
  identity as the actor. The authority checks the caller's pod and node, then
  the subject against the object (`authorize_exchange`,
  `crates/libs/credential/mgr/server/src/storage_exchange.rs`): a sandbox may
  reach only its own `suspend_snapshots/<shard>/<sandbox>/…` keys inside its
  org's snapshot storage, with writes fenced to the nodes that own it, and a
  template only `templates/<name>/<hash>/…`, with published releases
  read-only. It then calls STS `AssumeRole` on the vend role
  (`SPACE_CREDMGR_S3_VEND_ROLE_ARN`, `arn:aws:iam::000000000000:role/kind-storage-vend`)
  with a session policy for exactly that object, and logs one
  `credential_security_audit` event per decision. build-manager holds no keys
  either, and asks the authority itself, not its node's agent, whose sockets
  admit only volume-manager and template-manager to the exchange
  (`object_store_credentials`, `crates/libs/build/mgr/src/object_store.rs`):
  at `https://space-credmgr-authority:9810`, trusting the authority's
  certificate, with its projected ServiceAccount token, which the authority
  maps from `space-builder-build-manager` to `space-build-manager`. It asks
  for write scope on a release's `templates/<name>/<hash>/` prefix as the
  `template:<name>` subject before publishing it, which the authority refuses
  once the release is published, and for a build input's
  `build-inputs/sha256/<digest>` object as that digest.
- `static`: the opt-out. volume-manager and build-manager read the store's
  root keys from the `space-object-store` Secret, the authority has no vend
  role, and its storage exchange is disabled. The authority's optional AWS
  integrations (`space-aws-creds`, such as the AWS Secrets Manager
  network-credential scenario) need this mode: in `exchange` its one default
  credential chain and STS endpoint belong to the vend identity and the
  bridge.

Kind's STS is two pieces, both running in either mode:

- **versitygw's standalone IAM service**, the `space-object-store-iam`
  Deployment on the infra node (`versitygw iam`, the same pinned image). It
  answers `AssumeRoleWithWebIdentity`, checks the role's trust policy and
  maximum duration (3600 s), and keeps each session and its policy in `.iam/`
  on the store's hostPath, so sessions survive restarts. The gateway asks it
  about every request signed with a session's keys, on its private port over
  mTLS, and enforces the session policy as an intersection with the role's own
  policy (Get, Put and AbortMultipartUpload on both buckets' objects,
  ListBucket on both). Root keys are resolved and authorized by the gateway
  alone, and the gateway only warns at startup when the service is
  unreachable, so the IAM service has its own pod: an IAM crash or restart
  refuses session-signed requests while the gateway stays Ready and root-key
  clients (cluster-controller, the Jobs, and static-mode node services) keep
  working. Both its ports serve TLS with leaves from the internal RPC CA
  (`internal-rpc-tls.sh`): the IAM and STS API for the bridge and the
  bucket-init Job, and the private API, which requires the gateway's client
  leaf. A NetworkPolicy admits only those three callers,
  since a captured bridge token is all `AssumeRoleWithWebIdentity` needs. The
  bucket-init Job registers, idempotently on every run, the OIDC
  provider (the bridge's issuer, pinned by its CA's thumbprint), the vend role
  with its trust policy, and the role's policy
  ([charts/space-object-store](charts/space-object-store/values.yaml)).
- **The STS bridge** (`space-kind-sts-bridge`, source in
  [`sts-bridge/`](sts-bridge/main.go), Go standard library only). The
  authority's AWS SDK reaches it as `AWS_ENDPOINT_URL_STS=https://sts-bridge:8080`,
  over TLS from the internal RPC CA, signing with the vend identity's static
  keys. The bridge verifies the SigV4 signature, refuses any parameter but
  `RoleArn`, `RoleSessionName`, `Policy` and `DurationSeconds`, mints a
  five-minute RS256 token for the vend identity's subject, calls
  `AssumeRoleWithWebIdentity` with it and the same parameters (over TLS,
  verifying the IAM service's leaf against the internal RPC CA), and answers in
  `AssumeRole`'s format; a token the IAM service refuses comes back as
  `AccessDenied`, as AWS STS reports a role that does not trust its caller.
  On `:8443` it serves the issuer's discovery document and JWKS, which the IAM
  service fetches from the bridge's Service with TLS verified against the
  pinned CA, while tokens name an issuer, `https://sts-bridge.kind.invalid`,
  that never resolves. The vend identity is no store user, so its keys open
  nothing on S3 by themselves. [`scripts/sts-bridge-secret.sh`](scripts/sts-bridge-secret.sh)
  issues the bridge's Secret once per cluster (the vend identity, the signing
  key, and the issuer's CA and leaf) and keeps it across upgrades; the
  Tiltfile runs it before the charts and passes its checksum to the bridge,
  the authority and the object store's bucket-init Job, which roll or rerun
  when it changes. Tilt runs it when the Tiltfile loads, so a smoke bring-up
  that reuses a running Tilt does not reissue a deleted Secret;
  `tilt trigger '(Tiltfile)'` does. The IAM service reads nothing from it and needs no restart
  on a reissue: versitygw keys its cached JWKS on the provider's thumbprints,
  which the rerun Job updates, and refetches it for an unknown `kid`, which the
  bridge derives from its signing key. Both bridge listeners bound header,
  body and response times and their open connections, since they read a
  request before authenticating it.

The bridge's SigV4 check covers the algorithm, the credential scope (its
date matching `X-Amz-Date`, service `sts`, `aws4_request`), `X-Amz-Date`
within 15 minutes, that `host` and `x-amz-date` are signed, the method, path,
query, every signed header, the body actually received and
`X-Amz-Content-Sha256` when sent, with a constant-time comparison. It refuses a
missing signature, a wrong secret, an unknown key, a changed body, an expired
or future date, and a session token beside the keys or a session's own keys
(no role chaining). It does not cover presigned or SigV4A requests, chunked
payload signing, the scope's region, or replay of a captured request within
the 15-minute window (which AWS accepts too), and it applies no IAM policy to
its callers: the role's trust policy is the only authorization. Anyone with
its signing key can mint tokens the IAM service trusts, so that key is as
sensitive as the vend secret. The unit tests in `sts-bridge/` check it
against a request botocore signed, and gate the image build.

An out-of-scope write is refused before versitygw reads the body, and it
closes the connection after a refusal, so a client that writes a large body
first, such as the AWS SDK for Rust, usually sees a connection reset instead
of the 403 (with static keys too). Checks of refused writes therefore use
bodies of at most 64 KiB or assert that the object is unchanged, never the
status of a large refused PUT.

Both modes run the same store, IAM service and bridge over the same data, so
an existing cluster switches on its next bring-up, without recreation:

```bash
SPACE_KIND_STORAGE_CREDENTIALS=static bash scripts/tests/smoke-kind-e2e.sh   # opt out
bash scripts/tests/smoke-kind-e2e.sh                                         # back to exchange
```

A switch re-renders the authority and the volume-manager and build-manager
DaemonSets, which roll; a running Tilt reads the mode once, so the bring-up
(`up.sh` or a smoke run) restarts a Tilt that runs the other one. The
authority's Deployment records the mode in its
`space.pplx.net/kind-storage-credentials` label, and the worker and builder
volume-manager and build-manager DaemonSets must agree, in their pod templates
and in every pod they run: in `exchange` no container, init containers
included, holds a static key, whether by `env` or through an `envFrom` Secret
or ConfigMap; in `static` the service's own container holds both. A smoke run
with `SKIP_BOOTSTRAP=1` refuses a cluster in the other mode or with a
disagreeing node service, the bring-up checks both after Tilt settles, and
`local.storage_exchange_scoping` fails on a cluster where they disagree. The
refusal names each disagreeing DaemonSet and pod, and its remedy is one
bring-up (`up.sh`, or a smoke run without `SKIP_BOOTSTRAP=1`), which runs Tilt
from this checkout, re-applies the charts and waits for their rollouts. A
cluster brought up before the builders exchanged their credentials needs
exactly that once.
`local.storage_exchange_scoping` exchanges through a worker's agent socket as
volume-manager does, then shows the store refusing the session for another
sandbox's snapshot, the templates bucket and a write under read scope, and
the authority refusing the subject for another sandbox's object with its
resource denial; the session and a fresh exchange still read the object
afterwards. It then exchanges through a builder's agent socket for one
published file of the sandboxes' template, as the builder volume-manager pulls
a template. That session reads the file, and the store refuses it another file
of the same release (inside every broader prefix the policy could have
named), the sandbox's snapshot and, when another template is published, that
template's file. The authority refuses the template's subject for another
template's file with its resource denial, and the session and a fresh builder
exchange still read the file afterwards.

To see the exchange at work, read the authority's audit events on both
replicas, the bridge's request log, and confirm that volume-manager and
build-manager have no keys:

```bash
k() { kubectl --context kind-space-local -n space "$@"; }
k logs -l app.kubernetes.io/name=space-credmgr-authority --prefix --tail=-1 \
  | grep credential_security_audit | grep ExchangeToken
k logs deploy/space-kind-sts-bridge
k logs deploy/space-object-store-iam
for ds in space-volume-manager space-builder-volume-manager space-builder-build-manager; do
  k get ds "$ds" -o jsonpath='{.spec.template.spec.containers[0].env[*].name}'; echo
done
```

### Object store transition

The object store was SeaweedFS (`space-seaweedfs` chart, Service
`seaweedfs:8333`, data in `/var/lib/space-seaweedfs` on the infra node). It
is now versitygw (`space-object-store` chart, Service `object-store:8333`,
data in `/var/lib/space-object-store`). The two stores share no data, and the
database of a SeaweedFS-era cluster records every suspend snapshot and
published template at `http://seaweedfs:8333`. Applying the new charts to such
a cluster would leave its restores and template pulls depending on a
deployment nothing manages any more, and pointing the old Service at the new
gateway would not help, because the objects are not in it.

An existing cluster is therefore recreated, not migrated. The bring-up
(`scripts/tests/lib/bootstrap-kind-stack.sh`) and the `Tiltfile`
run `infra/space/kind/scripts/check-object-store-transition.sh` against an
existing cluster before they apply anything; when it still has resources from
the `space-seaweedfs` chart, they list those resources and stop without
changing anything: the bring-up exits with status 1, and Tilt reports a
Tiltfile load error. The same check refuses a cluster whose
`space-object-store` Deployment itself runs SeaweedFS, as an early prototype
of [exchanged credentials](#object-store-credentials) did. Switching
`SPACE_KIND_STORAGE_CREDENTIALS` is not refused. Recreating deletes the cluster's sandboxes,
snapshots and templates, and keeps the containerd image caches:

```bash
bash infra/space/kind/scripts/teardown.sh
bash infra/space/kind/scripts/up.sh
```

A smoke suite such as `scripts/tests/smoke-kind-e2e.sh` creates the cluster
itself after the teardown. For a non-default cluster, set `SPACE_KIND_CLUSTER`
on both commands.

## Tearing down

```bash
bash infra/space/kind/scripts/teardown.sh           # scripted path: removes everything
bash infra/space/kind/scripts/teardown.sh --keep-cluster   # remove workloads, keep kind cluster
```

Tilt users can still do the previous `tilt down && kind delete cluster --name space-local`.
