# Tiltfile for the local KinD SPACE stack.
#
# Brings up the SPACE control plane on a multi-node KinD cluster:
#   * postgres + SeaweedFS (S3)
#   * space-api-server
#   * space-internal-api
#   * space-data-plane
#   * space-cluster-controller + space-ui (full harness only)
#   * the space-node-stack DaemonSet — the production worker service set
#     (runtime-supervisor, node-manager, volume-manager, credential-manager,
#     template-manager, sandbox-manager, node-ctl, node-api, plus per-sandbox
#     net-gateways launched on demand) on real Firecracker, under production
#     unix identities
#   * full harness only: a dedicated builder node-stack with that runtime set
#     plus build-manager and a stable builder node-api Service
#   * the space-seed-publish Job — builds the production seed locally and
#     publishes it through the real ClusterApi + object-store path; workers
#     pull it via template-manager prefetch → volume-manager
#   * optional: Datadog Agent + in-cluster Fake Intake for local telemetry
#     capture (`SPACE_KIND_DATADOG_MODE=fakeintake`)
#
# Two modes:
#   * Full harness (default, SPACE_LOCAL_MINIMAL=0, cluster `space-local`): api-server
#     on the KubernetesSubstrate; every sandbox is its own K8s pod whose pause
#     container is hijacked by containerd-shim-space-v2 into a Firecracker
#     microVM — the production topology.
#   * MINIMAL (SPACE_LOCAL_MINIMAL=1, cluster `space-local-3486`): DirectSubstrate bench —
#     api-server dials the single worker's node-api directly; no per-sandbox
#     pods, no cluster-controller, no UI. Fits a 16 GiB Docker Desktop VM.
#
# Prereqs (do once on a fresh machine):
#   bash infra/space/kind/scripts/install-prereqs.sh
#   bash infra/space/kind/scripts/install-fc-vendor.sh   # space-local only
#   # two steps, because inline as `--config "$(...)"` the renderer's failure
#   # is unchecked and `kind create --config ""` does not error — it builds a
#   # DEFAULT cluster: one node, no extraMounts, no labels. An assignment
#   # keeps the exit status, so `&&` stops before kind runs at all.
#   config="$(bash infra/space/kind/scripts/render-cluster-config.sh)" \
#     && kind create cluster --config "$config"
#   # ... or, for the MINIMAL bench, --config infra/space/kind/kind-cluster-3486.yaml
#   # (no extraMounts, so nothing to render)
#
# Then:
#   tilt up

KIND_CLUSTER = os.getenv("SPACE_KIND_CLUSTER", "space-local")
NAMESPACE = "space"
allow_k8s_contexts("kind-" + KIND_CLUSTER)

# The full Kubernetes stack is the development default. The single-worker
# DirectSubstrate bench is opt-in: set SPACE_LOCAL_MINIMAL=1 and
# SPACE_KIND_CLUSTER=space-local-3486 when testing that specific substrate.
MINIMAL = os.getenv("SPACE_LOCAL_MINIMAL", "false") in ("1", "true", "True", "yes")

# Select one metrics sink per process. The Datadog and OTel comparison modes
# also select the matching trace exporter; journald leaves traces on Datadog.
# These local selectors do not activate any deployed environment.
TELEMETRY_SCRIPT = "infra/space/kind/scripts/telemetry_config.py"
OBSERVABILITY_CONFIG = os.getenv("SPACE_KIND_OBSERVABILITY_CONFIG", "")
read_file(TELEMETRY_SCRIPT)
if OBSERVABILITY_CONFIG:
    read_file(OBSERVABILITY_CONFIG)
DATADOG_SCRIPT = "infra/space/kind/scripts/datadog_backend.py"
read_file(DATADOG_SCRIPT)
# SaaS credential/isolation checks run before any local resource or deployment.
TELEMETRY_CONFIG = decode_json(local(["python3", DATADOG_SCRIPT, "preflight", "--for-deployment"], quiet = True))
DATADOG_MODE = TELEMETRY_CONFIG["backend"]
TELEMETRY_MODE = TELEMETRY_CONFIG["mode"]
TELEMETRY_SET = TELEMETRY_CONFIG["helm_set"]
watch_file("infra/space/kind/charts/space-kind-telemetry")
if TELEMETRY_MODE == "otel":
    print("OTLP uses the Collectors, not the Agent; install infra/space/kind/metrics/local.sh up and infra/space/kind/traces/local.sh up before selecting otel mode.")

# The Redis limiter store is opt-in; see its block below. `bootstrap.sh` reads
# the same variable (and accepts `--with-redis` for it), so both paths agree.
REDIS = os.getenv("SPACE_LOCAL_REDIS", "0") in ("1", "true", "True", "yes")

# ── Image builds ──────────────────────────────────────────────────────────
#
# Each rust runtime image chains its FROM base build into the same
# `custom_build` invocation. Tilt won't build a `custom_build` whose image
# isn't referenced by a deployed k8s resource — and our rust artifact base
# images are referenced only by other Dockerfiles' `FROM`, never by k8s
# manifests — so a standalone `custom_build` for them would just emit
# "unused image" and never run. Chaining `docker build (base) && docker
# build (runtime)` puts the ordering in our hands.
#
# One Rust build, not four. The production Dockerfile compiles each artifact
# set (worker, api, data-plane, cluster-controller) in its own stage; here
# every runtime image takes its binaries from `space-rust-build-kind:local`,
# the union of those sets. infra/space/kind/scripts/build-rust-artifacts.sh
# owns that image: on Linux/x86_64 it runs ONE incremental cargo build inside
# the production `chef-base` builder image (bookworm libc, pinned rustc) with a
# persistent host target dir bind-mounted in — no Rust compile as a BuildKit
# layer, no build-cache growth — and assembles a scratch image from the staged
# /artifacts layout, refusing binaries the runtime images' glibc cannot load;
# elsewhere it runs docker/Dockerfile.rust-build-kind (one cook, one build, as
# BuildKit stages). SPACE_KIND_RUST_ARTIFACTS=incremental|docker forces either.
# Each custom_build still runs the script first — it is idempotent and
# serialized by a file lock, so no image can start from a stale artifact image
# whatever order Tilt updates them in.

RUST_ARTIFACT_IMAGE = "space-rust-build-kind:local"
RUST_ARTIFACT_DEPS = ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config.toml",
                      "crates", "proto", "db",
                      "docker/Dockerfile.rust-build-common",
                      "docker/Dockerfile.rust-build-kind",
                      "docker/Dockerfile.rust-artifacts-host",
                      "infra/space/kind/scripts/build-rust-artifacts.sh",
                      "scripts/build-runtime-supervisor.sh",
                      "infra/space/identity/node-identities.sh",
                      "templates/firecracker/seed/rootfs-builder.sh",
                      "templates/firecracker/recipes/seed.toml",
                      "templates/firecracker/recipes/code-runtime.toml"]

RUST_ARTIFACT_BUILD = "bash infra/space/kind/scripts/build-rust-artifacts.sh"

# Every image build first pulls whichever of its Dockerfiles' registry bases
# are missing, retrying failed pulls, so a registry rate limit during a cold
# bring-up delays the build instead of failing it (see the script's header).
def pull_base_images(*dockerfiles):
    return "bash infra/space/kind/scripts/pull-base-images.sh " + " ".join(dockerfiles)

# In GitHub CI (SPACE_CI_IMAGE_MIRROR set) a build takes each registry base
# listed in ci/mirrored-images.txt from the repository's GHCR mirror, through
# --build-context options; the Dockerfiles are unchanged. Elsewhere this
# expands to nothing.
def mirrored_bases(dockerfile):
    return "$(bash ci/scripts/mirror-images.sh build-contexts {})".format(dockerfile)

# Up to eight updates at once in the full harness instead of Tilt's default
# three. Every Rust image waits on the artifact lock anyway, so the default mostly
# kept image builds and kind loads that could overlap in three strict waves after
# the compile: 124-176 s of a cold or warm bring-up with the host under 30 % CPU
# (2026-09-23 bench).
#
# The MINIMAL bench keeps Tilt's three: it targets a Docker Desktop VM, where
# parallel image builds and kind loads starve kind's etcd (see
# scripts/k8s/projected-tokens/README.md, "Troubleshooting").
# SPACE_TILT_MAX_PARALLEL_UPDATES in Tilt's environment overrides either default;
# `tilt up` has no flag for it.
MAX_PARALLEL_UPDATES = int(os.getenv("SPACE_TILT_MAX_PARALLEL_UPDATES", "3" if MINIMAL else "8"))
update_settings(max_parallel_updates = MAX_PARALLEL_UPDATES)

# No `kind load` in the custom_build commands: Tilt loads every custom_build
# image into every node of a kind cluster itself (its "Pushing … Loading image
# to KIND" step). A pool-scoped load before it only repeated the image export
# for the first nodes, 5-19 s per image, and could not keep the image off the
# other pools.

def rust_runtime_image(name, dockerfile, runtime_deps):
    cmd = " && ".join([
        pull_base_images(dockerfile),
        RUST_ARTIFACT_BUILD,
        ("docker build -t $EXPECTED_REF -f {dockerfile} {mirrored} " +
         "--build-arg SPACE_RUST_BUILD_IMAGE={rust_image} .").format(
            dockerfile = dockerfile,
            mirrored = mirrored_bases(dockerfile),
            rust_image = RUST_ARTIFACT_IMAGE,
        ),
    ])
    custom_build(
        ref = "{}:local".format(name),
        command = cmd,
        # The Rust workspace is the common base for all rust images; any
        # source change rebuilds the shared artifact image once (an
        # incremental compile against the persistent target dir, or a
        # cargo-chef-cached Docker build) and
        # every runtime image then re-copies its binaries from it.
        deps = RUST_ARTIFACT_DEPS + runtime_deps,
        skips_local_docker = False,
    )

rust_runtime_image(
    "space-api",
    "docker/Dockerfile.api",
    runtime_deps = ["docker/Dockerfile.api", "scripts/space-db-migrate.sh"],
)
rust_runtime_image(
    "space-data-plane",
    "docker/Dockerfile.data-plane",
    runtime_deps = ["docker/Dockerfile.data-plane"],
)
rust_runtime_image(
    "space-cluster-controller",
    "docker/Dockerfile.cluster-controller",
    runtime_deps = ["docker/Dockerfile.cluster-controller"],
)

# space-node-stack: the production worker image (docker/Dockerfile.worker —
# same binaries, cargo features, unix identities, nftables policies as the
# prod EKS worker/builder DaemonSets) plus the kind entrypoint layer. LTO disabled
# for faster local iteration. Runs as a DaemonSet on the sandbox workers
# against real Firecracker, wired to SeaweedFS for snapshot artifacts.
custom_build(
    ref = "space-node-stack:local",
    command = " && ".join([
        pull_base_images("docker/Dockerfile.worker", "docker/Dockerfile.node-stack"),
        RUST_ARTIFACT_BUILD,
        # --provenance=false keeps buildx from emitting a multi-platform
        # manifest list, which `kind load docker-image` cannot ingest
        # ("content digest ... not found").
        ("docker build --provenance=false --sbom=false -t space-worker:local " +
         "-f docker/Dockerfile.worker {} " +
         "--build-arg SPACE_RUST_BUILD_IMAGE={} .").format(
            mirrored_bases("docker/Dockerfile.worker"), RUST_ARTIFACT_IMAGE),
        ("docker build --provenance=false --sbom=false -t $EXPECTED_REF " +
         "-f docker/Dockerfile.node-stack " +
         "--build-arg SPACE_WORKER_IMAGE=space-worker:local ."),
    ]),
    deps = RUST_ARTIFACT_DEPS + [
            "docker/Dockerfile.worker",
            "docker/Dockerfile.node-stack",
            "scripts/k8s/node-stack-start.sh",
            "scripts/k8s/stage-guest-kernel.sh",
            "scripts/lib/space-loop-devices.sh"],
    skips_local_docker = False,
)

# space-seed-publish: carries the locally built production seed template
# (templates/firecracker/seed builder — vsock spaced, FC-bootable) plus the
# publisher that uploads it to SeaweedFS and registers the release through
# the real ClusterApi RPCs. build-seed.sh skips the privileged rootfs build
# when its inputs (guest binaries, builder script, recipe, pubkey) are
# unchanged, so watch-mode rebuilds are cheap unless spaced changed.
custom_build(
    ref = "space-seed-publish:local",
    command = " && ".join([
        pull_base_images("docker/Dockerfile.seed-publish"),
        RUST_ARTIFACT_BUILD,
        "SPACE_RUST_BUILD_IMAGE={} bash infra/space/kind/scripts/build-seed.sh".format(RUST_ARTIFACT_IMAGE),
        ("docker build --provenance=false --sbom=false -t $EXPECTED_REF " +
         "-f docker/Dockerfile.seed-publish " + mirrored_bases("docker/Dockerfile.seed-publish") + " " +
         "--build-context seed=$HOME/.space-devbox/kind/seed/build ."),
    ]),
    deps = RUST_ARTIFACT_DEPS + [
            "templates/firecracker/seed",
            "templates/firecracker/recipes/seed.toml",
            "docker/Dockerfile.seed-publish",
            "infra/space/kind/scripts/build-seed.sh",
            "infra/space/kind/scripts/seed-publish-entry.sh"],
    skips_local_docker = False,
)

# UI: no rust, single Dockerfile. NEXT_PUBLIC_API_URL must be passed at
# BUILD time — Next.js evaluates rewrites in next.config.js once at build
# and freezes the result in .next/required-server-files.json. Runtime env
# overrides are ignored.
custom_build(
    ref = "space-ui:local",
    command = " && ".join([
        pull_base_images("docker/Dockerfile.ui"),
        ("docker build -t $EXPECTED_REF -f docker/Dockerfile.ui " +
         mirrored_bases("docker/Dockerfile.ui") + " " +
         "--build-arg NEXT_PUBLIC_API_URL=http://space-api:8080 ."),
    ]),
    deps = ["docker/Dockerfile.ui", "ui"],
    skips_local_docker = False,
)

# ── Cluster bootstrap (RuntimeClass + node labels + Namespace) ───────────
#
# `KubernetesSubstrate::build_sandbox_pod` pins per-sandbox pods to
# `runtimeClassName: space` and `nodeSelector: node.kubernetes.io/space-enabled=true`.
# Neither exists in vanilla KinD, so we materialize them before the charts
# land. RuntimeClass `space` resolves to containerd-shim-space-v2 once
# install-shim.sh has wired it into each sandbox worker's containerd (the
# `install-space-shim` bootstrap resource below, full harness only).

# Namespace + RuntimeClass live in infra/space/kind/manifests/ so the scripted
# bootstrap (infra/space/kind/scripts/bootstrap.sh) and tilt up share one
# source of truth. NAMESPACE above must stay "space" — the manifest file
# hardcodes that name and the charts default to it.
k8s_yaml("infra/space/kind/manifests/namespace.yaml")
k8s_yaml("infra/space/kind/manifests/runtimeclass.yaml")

# Label sandbox workers so the api-server's K8s scheduler can place sandbox
# pods there. Targets only nodes with node-pool=sandbox (set in kind-cluster.yaml.tmpl)
# so the infra node is never labeled space-enabled and never receives sandbox pods.
local_resource(
    "label-kind-workers",
    cmd = "kubectl --context kind-{cluster} label nodes -l 'space.pplx.net/node-pool=sandbox' node.kubernetes.io/space-enabled=true --overwrite".format(cluster = KIND_CLUSTER),
    labels = ["bootstrap"],
)

# Advertise the network-slot extended resource on sandbox workers. Every
# sandbox pod requests `space.pplx.net/network-slots: "1"` (see
# `KubernetesSubstrate::build_sandbox_pod`), so without a node advertising
# capacity the scheduler leaves every sandbox pod Pending. Node-controller
# advertises node-manager's `network_pool_max_slots`; locally we advertise a
# static pool to exercise scheduling. JSON-patch `add` on a map key is idempotent
# across re-runs.
local_resource(
    "advertise-kind-worker-network-slots",
    cmd = """
for node in $(kubectl --context kind-{cluster} get nodes -l 'space.pplx.net/node-pool=sandbox' -o name); do
  kubectl --context kind-{cluster} patch "$node" --subresource=status --type=json \\
    -p '[{{"op":"add","path":"/status/capacity/space.pplx.net~1network-slots","value":"100"}}]'
done
""".format(cluster = KIND_CLUSTER),
    labels = ["bootstrap"],
    resource_deps = ["label-kind-workers"],
)

# Label the infra node so Postgres, space-api, and space-cluster-controller
# can pin there via nodeSelector. These components must not run on sandbox
# worker nodes that the drain reconciler can delete.
local_resource(
    "label-kind-infra",
    cmd = "kubectl --context kind-{cluster} label nodes -l 'space.pplx.net/node-pool=infra' space.pplx.net/infra=true --overwrite".format(cluster = KIND_CLUSTER),
    labels = ["bootstrap"],
)

# Firecracker vendor tree (binary + jailer + vmlinux) at /space/vendor inside
# each sandbox or builder node. `space-local` gets it via kind extraMounts; the
# 3486 bench can't bind-mount macOS host paths so the script docker-cps it.
# No-ops when already present.
local_resource(
    "stage-fc-vendor",
    cmd = "bash infra/space/kind/scripts/stage-fc-vendor.sh {}".format(KIND_CLUSTER),
    labels = ["bootstrap"],
)

# Sandbox SSH keypair Secret — the local stand-in for the AMI's
# Packer-generated /space/etc/ssh. build-seed.sh generates the keypair (and
# bakes the public half into the seed); the private half is mounted into the
# node-stack pods so `space-ssh` works. ssh-keygen here is idempotent and
# uses the same path build-seed.sh does.
local_resource(
    "sandbox-ssh-secret",
    cmd = """
set -eu
dir="$HOME/.space-devbox/kind/ssh"
mkdir -p "$dir"
# k8s_yaml resources and local_resources reconcile concurrently on a cold
# cluster. Apply the same idempotent Namespace manifest before the Secret.
kubectl --context kind-{cluster} apply -f infra/space/kind/manifests/namespace.yaml >/dev/null
[ -f "$dir/sandbox_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -C space-sandbox-local -f "$dir/sandbox_ed25519"
kubectl --context kind-{cluster} -n {ns} create secret generic space-sandbox-ssh \\
  --from-file=sandbox_ed25519="$dir/sandbox_ed25519" \\
  --from-file=sandbox_ed25519.pub="$dir/sandbox_ed25519.pub" \\
  --dry-run=client -o yaml | kubectl --context kind-{cluster} apply -f -
""".format(cluster = KIND_CLUSTER, ns = NAMESPACE),
    labels = ["bootstrap"],
)

# containerd-shim-space-v2 on each sandbox worker — required by the full
# harness (KubernetesSubstrate) so per-sandbox pause pods are hijacked into
# real Firecracker sandboxes. The MINIMAL bench (DirectSubstrate) creates no
# per-sandbox pods and needs no shim.
if not MINIMAL:
    local_resource(
        "install-space-shim",
        cmd = "SPACE_KIND_CLUSTER={} SPACE_KIND_TELEMETRY_MODE={} bash infra/space/kind/scripts/install-shim.sh".format(
            KIND_CLUSTER,
            TELEMETRY_MODE,
        ),
        deps = [TELEMETRY_SCRIPT, "infra/space/kind/scripts/install-shim.sh"] + ([OBSERVABILITY_CONFIG] if OBSERVABILITY_CONFIG else []),
        labels = ["bootstrap"],
    )

# The Rust build, started as soon as the bootstrap resources above are done
# instead of when the first image build may start: Tilt holds a resource's image
# build until its resource_deps are ready, and space-api's include the Postgres
# pod, which kept the compiler idle for 20-31 s of every cold bring-up
# (2026-09-23 bench). The custom_builds still run the same script first; they
# find its lock held and wait, or find the build up to date. allow_parallel is
# required: Tilt runs nothing else beside a serial local resource, and this one
# runs for minutes. It waits for the serial bootstrap resources because Tilt
# holds those while anything else is building.
RUST_ARTIFACTS_AFTER = ["label-kind-workers", "advertise-kind-worker-network-slots",
                        "label-kind-infra", "stage-fc-vendor", "sandbox-ssh-secret"]
if not MINIMAL:
    RUST_ARTIFACTS_AFTER = RUST_ARTIFACTS_AFTER + ["install-space-shim"]
local_resource(
    "rust-artifacts",
    cmd = RUST_ARTIFACT_BUILD,
    deps = RUST_ARTIFACT_DEPS,
    allow_parallel = True,
    resource_deps = RUST_ARTIFACTS_AFTER,
    labels = ["build"],
)

# ── Helm releases (in dependency order) ───────────────────────────────────

def helm_release(name, chart):
    return helm(chart, name = name, namespace = NAMESPACE, set = TELEMETRY_SET)

# Both entry points use a fresh, shared render for backend transitions.
# Fake mode never consumes SaaS credentials; SaaS uses an existing Secret only.
if DATADOG_MODE != "off":
    DATADOG_CHART = "infra/space/kind/charts/datadog-agent-local"
    local("helm repo add datadog https://helm.datadoghq.com --force-update", quiet = True)
    local("helm dependency build {}".format(DATADOG_CHART), quiet = True)
    # Helm rewrites charts/ and tmpcharts-* during dependency builds. Watching
    # those generated files would retrigger the Tiltfile indefinitely.
    for source in ["Chart.yaml", "Chart.lock", "values.yaml", "values.schema.json", "values-fakeintake.yaml", "templates"]:
        watch_file(DATADOG_CHART + "/" + source)
    k8s_yaml(local(["python3", DATADOG_SCRIPT, "render"], quiet = True))
if DATADOG_MODE == "fakeintake":
    k8s_resource(
        "datadog-fakeintake",
        port_forwards = ["8088:8080"],
        labels = ["observability"],
    )

# Postgres first; the api-server's migrate initContainer blocks on it.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-postgres"))
k8s_resource(
    "space-postgres",
    labels = ["data"],
    resource_deps = ["label-kind-infra"],
)

# API server.
SPACE_API_DEPS = ["space-postgres", "label-kind-workers", "label-kind-infra",
                  "advertise-kind-worker-network-slots"]
if not MINIMAL:
    # Full harness: per-sandbox pods need the shim wired into containerd
    # before the first create lands.
    SPACE_API_DEPS = SPACE_API_DEPS + ["install-space-shim"]
# k8s.enabled tracks the harness mode: the full harness runs the api on the
# KubernetesSubstrate (per-sandbox pods, cluster-controller); the MINIMAL
# bench uses DirectSubstrate. The chart defaults k8s.enabled=true (prod
# shape); override to false for the bench.
k8s_yaml(helm(
    "infra/space/kind/charts/space-api-local",
    name = "space",
    namespace = NAMESPACE,
    set = TELEMETRY_SET + ["k8s.enabled={}".format("false" if MINIMAL else "true")],
))
k8s_resource(
    "space-api",
    port_forwards = ["8080:8080"],
    labels = ["control-plane"],
    resource_deps = SPACE_API_DEPS,
)

# The Kind user stays locked in shared migrations. Provision only after the
# API pod finishes migrations; this also covers clusters brought up by Tilt
# without running the scripted bootstrap first.
local_resource(
    "provision-local-console",
    cmd = "SPACE_KIND_CLUSTER={} bash infra/space/kind/scripts/provision-local-console.sh".format(KIND_CLUSTER),
    deps = ["infra/space/kind/scripts/provision-local-console.sh"],
    labels = ["bootstrap"],
    resource_deps = ["space-api"],
)

# Internal (cluster) API — node-ctl publishes node state here (this is node
# registration), template-manager resolves releases, and the seed publish
# registers the seed release against it.
k8s_yaml(helm(
    "infra/space/kind/charts/space-internal-api-local",
    name = "space",
    namespace = NAMESPACE,
    set = ["k8s.enabled={}".format("false" if MINIMAL else "true")],
))
k8s_resource(
    "space-internal-api",
    labels = ["control-plane"],
    resource_deps = ["space-api", "provision-local-console", "label-kind-infra"],
)

# Public data-plane service for native sessions and ECW wildcard hosts.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-data-plane-local"))
k8s_resource(
    "space-data-plane",
    port_forwards = ["8090:8090"],
    labels = ["control-plane"],
    resource_deps = ["space-api", "space-internal-api", "label-kind-infra"],
)

# Cluster-controller + UI are full-harness components: the controller
# materializes per-sandbox pods under the KubernetesSubstrate and hosts the
# cluster-singleton reconcilers; the MINIMAL bench needs neither.
if not MINIMAL:
    k8s_yaml(helm_release("space", "infra/space/kind/charts/space-cluster-controller-local"))
    k8s_resource(
        "space-cluster-controller",
        labels = ["control-plane"],
        resource_deps = ["space-api", "label-kind-infra"],
    )

    k8s_yaml(helm_release("space", "infra/space/kind/charts/space-ui-local"))
    k8s_resource(
        "space-ui",
        port_forwards = ["3000:3000"],
        labels = ["frontend"],
        resource_deps = ["space-api", "provision-local-console"],
    )

# ── Object store + node stack + seed publish ─────────────────────────────────
#
# SeaweedFS (Apache-2.0; replaces MinIO, whose community images went behind a
# paywall) is a real S3-compatible object store on the infra node. The
# bucket-init Job creates the `snapshots` + `templates` buckets. It is a
# public docker.io image the kind nodes pull directly, so no `kind load`
# step is needed.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-seaweedfs"))
k8s_resource(
    "space-seaweedfs",
    port_forwards = ["8333:8333"],  # S3 API (also browsable filer UI on :8888 in-cluster)
    labels = ["data"],
    resource_deps = ["label-kind-infra"],
)
k8s_resource(
    "space-seaweedfs-bucket-init",
    labels = ["data"],
    resource_deps = ["space-seaweedfs"],
)

# Redis: shared store for the two API rate limiters (cluster activation
# admission + per-client concurrency). Public docker.io image pulled directly by
# the kind nodes (no `kind load`). Opt-in, defaulted off to match `bootstrap.sh`:
# nothing in the kind tree sets SPACE_REDIS_URL, so both limiters fail open with
# or without it. The rate-limiter smoke scenario
# (`local.rate_limiters_enforce_and_fail_open`) creates it when it is absent.
if REDIS:
    k8s_yaml(helm_release("space", "infra/space/kind/charts/space-redis-local"))
    k8s_resource(
        "space-redis",
        labels = ["data"],
        resource_deps = ["label-kind-infra"],
    )
    # Asking for the store makes it the bench's, even one a killed smoke
    # created and recorded as its own; `bootstrap.sh --with-redis` does the same.
    local_resource(
        "claim-redis-store",
        cmd = [
            "bash",
            "infra/space/kind/scripts/claim-redis-store.sh",
            "kind-" + KIND_CLUSTER,
            NAMESPACE,
        ],
        labels = ["data"],
        resource_deps = ["space-redis"],
    )
else:
    # Tilt never deletes what an earlier session applied, so a store left by a
    # `SPACE_LOCAL_REDIS=1` session or a killed smoke would otherwise outlive
    # the knob. This keeps a default stack store-free, as `bootstrap.sh` does;
    # it runs once per Tilt session, and `bootstrap_kind_stack` repeats it when
    # it reuses a live default-mode Tilt (one that has this resource). The
    # rate-limiter scenario provides its own store.
    local_resource(
        "remove-redis-store",
        cmd = [
            "bash",
            "infra/space/kind/scripts/remove-redis-store.sh",
            "kind-" + KIND_CLUSTER,
            NAMESPACE,
        ],
        labels = ["data"],
    )

# node-stack: the production worker service set on real Firecracker, on every
# labeled sandbox worker, wired to SeaweedFS. Needs Postgres up (credmgr
# authority role), the internal API (node-ctl registration + template pulls),
# the vendor tree, and the sandbox SSH Secret. The MINIMAL bench opts into
# the legacy DirectSubstrate SQL registration; the full harness registers
# purely through node-ctl → internal-api.
# SPACE_LOOP_DEVICE_TARGET is the one knob for the loop-device pool: the
# entrypoint stages nodes from it before mounting /space/cache, and node-manager
# reads the same variable once it is up. Inherited from the environment so
# `SPACE_LOOP_DEVICE_TARGET=128 tilt up` reaches the pod;
# unset renders nothing and the library's default (64) applies.
LOOP_DEVICE_TARGET = os.getenv("SPACE_LOOP_DEVICE_TARGET", "")
NODE_STACK_SET = TELEMETRY_SET + ["directSubstrate.enabled={}".format("true" if MINIMAL else "false")]
if LOOP_DEVICE_TARGET:
    NODE_STACK_SET.append("loopDeviceTarget={}".format(LOOP_DEVICE_TARGET))

k8s_yaml(helm(
    "infra/space/kind/charts/space-node-stack",
    name = "space",
    namespace = NAMESPACE,
    set = NODE_STACK_SET,
))
k8s_resource(
    "space-node-stack",
    labels = ["node-stack"],
    resource_deps = ["label-kind-workers", "space-postgres", "space-internal-api",
                     "space-seaweedfs-bucket-init", "stage-fc-vendor",
                     "sandbox-ssh-secret"],
)

# seed publish: uploads the locally built production seed to SeaweedFS and
# registers the release through the real ClusterApi RPCs. Workers then pull
# it via template-manager prefetch → volume-manager — the production path.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-seed-publish"))
k8s_resource(
    "space-seed-publish",
    labels = ["bootstrap"],
    resource_deps = ["space-internal-api", "space-seaweedfs-bucket-init"],
)

# Full-harness builder role: a second render of the node-stack chart targets
# only the dedicated builder pool. It adds build-manager, exposes the stable
# space-builder-node-api Service expected by space-api, and publishes recipe
# outputs to SeaweedFS's templates bucket. MINIMAL remains worker-only.
if not MINIMAL:
    k8s_yaml(helm(
        "infra/space/kind/charts/space-node-stack",
        name = "space-builder",
        namespace = NAMESPACE,
        values = ["infra/space/kind/charts/space-node-stack/values-builder.yaml"],
        set = TELEMETRY_SET + (["loopDeviceTarget={}".format(LOOP_DEVICE_TARGET)] if LOOP_DEVICE_TARGET else []),
    ))
    k8s_resource(
        "space-builder-node-stack",
        labels = ["node-stack"],
        resource_deps = ["space-postgres", "space-internal-api",
                         "space-seaweedfs-bucket-init", "space-seed-publish",
                         "stage-fc-vendor", "sandbox-ssh-secret"],
    )
