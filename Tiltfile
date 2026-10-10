# Tiltfile for the local KinD SPACE stack.
#
# Brings up the SPACE control plane on a multi-node KinD cluster:
#   * postgres + versitygw object store (S3)
#   * space-api-server
#   * space-internal-api
#   * space-credmgr-authority, the control-plane credential-manager authority
#     each node's credential-manager agent forwards to
#   * space-data-plane
#   * space-cluster-controller + space-ui (full harness only)
#   * one DaemonSet per node service on every sandbox worker — node-manager,
#     runtime-manager, volume-manager, credential-manager, template-manager,
#     sandbox-manager, node-ctl and node-api — from the production worker
#     image, under production unix identities; node-manager's init container
#     installs runtime-supervisor as a systemd unit on each node, which runs
#     Firecracker and the per-sandbox net-gateways
#   * the same set plus build-manager on the dedicated builder nodes, with a
#     stable builder node-api Service
#   * a Kind-only operator shell DaemonSet (space-node-debug) on those nodes
#   * the space-seed-publish Job — builds the production seed locally and
#     publishes it through the real ClusterApi + object-store path; workers
#     pull it via template-manager prefetch → volume-manager
#   * optional: Datadog Agent + in-cluster Fake Intake for local telemetry
#     capture (`SPACE_KIND_DATADOG_MODE=fakeintake`)
#
# The api-server runs on the KubernetesSubstrate: every sandbox is its own K8s
# pod whose pause container is hijacked by containerd-shim-space-v2 into a
# Firecracker microVM — the production topology.
#
# Prereqs (do once on a fresh machine):
#   bash infra/space/kind/scripts/install-prereqs.sh
#   bash infra/space/kind/scripts/install-fc-vendor.sh
#   # two steps, because inline as `--config "$(...)"` the renderer's failure
#   # is unchecked and `kind create --config ""` does not error — it builds a
#   # DEFAULT cluster: one node, no extraMounts, no labels. An assignment
#   # keeps the exit status, so `&&` stops before kind runs at all.
#   config="$(bash infra/space/kind/scripts/render-cluster-config.sh)" \
#     && kind create cluster --config "$config"
#
# Then:
#   tilt up

KIND_CLUSTER = os.getenv("SPACE_KIND_CLUSTER", "space-local")
NAMESPACE = "space"
allow_k8s_contexts("kind-" + KIND_CLUSTER)

# A cluster from before the versitygw object store keeps its objects in
# SeaweedFS; refuse it rather than apply the new charts over it. See "Object
# store transition" in infra/space/kind/README.md.
local(["bash", "infra/space/kind/scripts/check-object-store-transition.sh", KIND_CLUSTER], quiet = True)

# A cluster from before the per-service node DaemonSets still runs the single
# node-stack pod per node; refuse it rather than run both layouts at once. See
# "Upgrading from the node-stack layout" in infra/space/kind/README.md.
local(["bash", "infra/space/kind/scripts/check-node-layout.sh", KIND_CLUSTER], quiet = True)

# Select one metrics sink per process. The datadog and otel modes also select
# the matching trace exporter; off disables both.
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

# The local backends otel mode exports to, installed as on the bootstrap path.
# Nothing waits on them: the shim addresses the Collectors by fixed ClusterIPs,
# and every exporter retries until a receiver answers.
if TELEMETRY_MODE == "otel":
    local_resource(
        "install-observability-backends",
        cmd = "SPACE_KIND_CLUSTER={} bash infra/space/kind/scripts/install-observability.sh".format(KIND_CLUSTER),
        deps = ["infra/space/kind/scripts/install-observability.sh", "infra/space/observability/charts"],
        # Each install runs `helm dependency build` on its chart
        # (release_init, infra/space/kind/lib/release.sh), which rewrites the
        # vendored charts/<chart>/charts/*.tgz and stages tmpcharts-* beside
        # them. Watching that output reran this resource after every run, so
        # Tilt never went idle and the smoke bring-up's wait for it never ended.
        ignore = [
            "infra/space/observability/charts/*/charts",
            "infra/space/observability/charts/*/tmpcharts-*",
        ],
        labels = ["bootstrap"],
    )

    # Each backend on the port its own `local.sh ui` helper uses; bootstrap.sh
    # forwards the same. Tilt does not deploy these releases, so it tracks no
    # pods for port_forwards to follow: port-forward.sh holds each forward
    # instead, restarting it whenever its pod does.
    for name, namespace, mapping in [
        ("prometheus", "space-metrics", "19090:9090"),
        ("tempo", "space-traces", "13200:3200"),
        ("loki", "space-logs", "13101:3100"),
        ("grafana", "space-grafana", "13000:3000"),
    ]:
        local_resource(
            "{}-port-forward".format(name),
            serve_cmd = [
                "bash", "infra/space/kind/scripts/port-forward.sh", "kind-" + KIND_CLUSTER,
                namespace, "svc/" + name, mapping, "space-kind-pf:kind-" + KIND_CLUSTER,
            ],
            links = [link("http://127.0.0.1:" + mapping.split(":")[0], name)],
            labels = ["observability"],
            resource_deps = ["install-observability-backends"],
        )

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

# Up to eight updates at once instead of Tilt's default three. Every Rust image
# waits on the artifact lock anyway, so the default mostly kept image builds and
# kind loads that could overlap in three strict waves after the compile: 124-176 s
# of a cold or warm bring-up with the host under 30 % CPU (2026-09-23 bench).
# SPACE_TILT_MAX_PARALLEL_UPDATES in Tilt's environment overrides the default;
# `tilt up` has no flag for it.
MAX_PARALLEL_UPDATES = int(os.getenv("SPACE_TILT_MAX_PARALLEL_UPDATES", "8"))
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
# Kind-only: the intermediate-key broker fixture the credential-manager
# authority uses as its external key provider.
rust_runtime_image(
    "space-kind-key-broker",
    "docker/Dockerfile.kind-key-broker",
    runtime_deps = ["docker/Dockerfile.kind-key-broker"],
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

# space-worker: the production worker image (docker/Dockerfile.worker — the
# binaries, cargo features, unix identities, init scripts, nftables policies and
# recipes the production node DaemonSets run). Every node-service DaemonSet,
# the operator shell and space-internal-api run it unchanged. LTO disabled for
# faster local iteration.
custom_build(
    ref = "space-worker:local",
    command = " && ".join([
        pull_base_images("docker/Dockerfile.worker"),
        RUST_ARTIFACT_BUILD,
        # --provenance=false keeps buildx from emitting a multi-platform
        # manifest list, which `kind load docker-image` cannot ingest
        # ("content digest ... not found").
        ("docker build --provenance=false --sbom=false -t $EXPECTED_REF " +
         "-f docker/Dockerfile.worker {} " +
         "--build-arg SPACE_RUST_BUILD_IMAGE={} .").format(
            mirrored_bases("docker/Dockerfile.worker"), RUST_ARTIFACT_IMAGE),
    ]),
    deps = RUST_ARTIFACT_DEPS + [
            "docker/Dockerfile.worker",
            "docker/space/nftables",
            "scripts/k8s/node-manager-init.sh",
            "scripts/k8s/node-manager-runtime-supervisor.sh",
            "scripts/k8s/stage-guest-kernel.sh",
            "scripts/k8s/sandbox-manager-start.sh",
            "scripts/space-sbx-db-migrate.sh",
            "templates/firecracker/recipes"],
    skips_local_docker = False,
)

# space-seed-publish: carries the locally built production seed template
# (templates/firecracker/seed builder — vsock spaced, FC-bootable) plus the
# publisher that uploads it to the object store and registers the release through
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

# Sandbox SSH keypair Secret — the local stand-in for the AMI's
# Packer-generated key. build-seed.sh generates the keypair (and bakes the
# public half into the seed); the private half is mounted into build-manager,
# which SSHes into build VMs. ssh-keygen here is idempotent and
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

# Internal RPC TLS: the cluster's CA and the ClusterApi, NodeApi, BuildManager
# and credential-manager authority identities as Secrets, before any workload
# that mounts them. Idempotent; keeps valid material and reissues a leaf near
# its expiry (docs/internal-rpc-tls.md).
local_resource(
    "internal-rpc-tls",
    cmd = """
set -eu
kubectl --context kind-{cluster} apply -f infra/space/kind/manifests/namespace.yaml >/dev/null
SPACE_KIND_CLUSTER={cluster} bash infra/space/kind/scripts/internal-rpc-tls.sh
""".format(cluster = KIND_CLUSTER),
    deps = ["infra/space/kind/scripts/internal-rpc-tls.sh"],
    labels = ["bootstrap"],
)

# containerd-shim-space-v2 on each sandbox worker, so per-sandbox pause pods
# are hijacked into real Firecracker sandboxes.
local_resource(
    "install-space-shim",
    cmd = "SPACE_KIND_CLUSTER={} SPACE_KIND_TELEMETRY_MODE={} bash infra/space/kind/scripts/install-shim.sh".format(
        KIND_CLUSTER,
        TELEMETRY_MODE,
    ),
    deps = [TELEMETRY_SCRIPT, "infra/space/kind/scripts/install-shim.sh"] + ([OBSERVABILITY_CONFIG] if OBSERVABILITY_CONFIG else []),
    labels = ["bootstrap"],
)

# Sandbox workers and builders boot the SPACE Kind node image, which bakes in
# the node layer a production node image provides (btrfs /space/cache, cgroup
# subtree, loop-device headroom, device permissions, SPACE accounts, /space
# layout). A node keeps the image it was created from, so this refuses a cluster
# whose SPACE nodes predate the image or run an older build of it, with the
# remedy (recreate the cluster), instead of letting the node services wait for a
# cache that never comes. Every node-service release depends on it.
local_resource(
    "check-kind-node-image",
    cmd = "SPACE_KIND_CLUSTER={} bash infra/space/kind/scripts/check-kind-node-image.sh".format(KIND_CLUSTER),
    deps = ["infra/space/kind/scripts/check-kind-node-image.sh",
            "infra/space/kind/scripts/kind-node-image.sh", "infra/space/kind/node",
            "scripts/lib/space-loop-devices.sh", "infra/space/identity/node-identities.sh"],
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
                        "label-kind-infra", "sandbox-ssh-secret", "internal-rpc-tls",
                        "check-kind-node-image", "install-space-shim"]
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
# Per-sandbox pods need the shim wired into containerd before the first create
# lands.
SPACE_API_DEPS = ["space-postgres", "label-kind-workers", "label-kind-infra",
                  "advertise-kind-worker-network-slots", "install-space-shim",
                  "internal-rpc-tls"]
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-api-local"))
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
    set = TELEMETRY_SET,
))
k8s_resource(
    "space-internal-api",
    labels = ["control-plane"],
    resource_deps = ["space-api", "provision-local-console", "label-kind-infra"],
)

# Control-plane credential-manager authority: owns the credential tables and
# the platform KEK; each node's credential-manager runs as its agent, and
# node-manager calls it for sandbox volume DEKs. It needs the API's migrations
# and the space_creds role the API's bootstrap provisions.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-credmgr-authority-local"))
k8s_resource(
    "space-credmgr-authority",
    labels = ["control-plane"],
    resource_deps = ["space-api", "label-kind-infra"],
)
# The authority's external key provider: the Kind-only intermediate-key broker
# fixture, whose derived keys survive restarts of either side.
k8s_yaml(helm("infra/space/kind/charts/space-kind-key-broker", name = "space", namespace = NAMESPACE))
k8s_resource(
    "space-kind-key-broker",
    labels = ["control-plane"],
    resource_deps = ["label-kind-infra"],
)

# Public data-plane service for native sessions and ECW wildcard hosts.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-data-plane-local"))
k8s_resource(
    "space-data-plane",
    port_forwards = ["8090:8090"],
    labels = ["control-plane"],
    resource_deps = ["space-api", "space-internal-api", "label-kind-infra"],
)

# The cluster-controller materializes per-sandbox pods under the
# KubernetesSubstrate and hosts the cluster-singleton reconcilers.
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

# ── Object store + node services + seed publish ──────────────────────────────
#
# Versity S3 Gateway (versitygw, Apache-2.0; replaced SeaweedFS, which replaced
# MinIO) is a real S3-compatible object store on the infra node, serving its
# POSIX backend from a node-local hostPath. The bucket-init Job creates the
# `snapshots` + `templates` buckets through the S3 API. Both are public
# registry images the kind nodes pull directly, so no `kind load` step is
# needed.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-object-store"))
k8s_resource(
    "space-object-store",
    port_forwards = ["8333:8333"],  # S3 API
    labels = ["data"],
    resource_deps = ["label-kind-infra"],
)
k8s_resource(
    "space-object-store-bucket-init",
    labels = ["data"],
    resource_deps = ["space-object-store"],
)

# Redis: shared store for the two API rate limiters (cluster activation
# admission + per-client concurrency). Public docker.io image pulled directly by
# the kind nodes (no `kind load`). Idle unless SPACE_REDIS_URL points at it; the
# rate-limiter smoke (scripts/tests/smoke-kind-rate-limiters.sh) wires it up.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-redis-local"))
k8s_resource(
    "space-redis",
    labels = ["data"],
    resource_deps = ["label-kind-infra"],
)

# Node services: one release of each per role — `space` on the sandbox workers,
# `space-builder` (node.role=builder) on the builder nodes, which add
# build-manager — mirroring production's per-service DaemonSets. Each service
# tolerates peers that are not ready yet and restarts on its own, so nothing
# here orders them; they wait only for the node layer and the bootstrap
# resources their pods mount. A chart's values-builder.yaml carries its builder
# overrides.
# SPACE_LOOP_DEVICE_TARGET sets the loop-device pool node-manager raises each
# node to once it is up; the node image stages the library's floor of 64 before
# that. Inherited from the environment so `SPACE_LOOP_DEVICE_TARGET=128 tilt up`
# reaches the pod; unset renders nothing and the default applies.
LOOP_DEVICE_TARGET = os.getenv("SPACE_LOOP_DEVICE_TARGET", "")
# The release list is shared with bootstrap.sh and the smoke bring-up.
NODE_RELEASES = str(local(["bash", "infra/space/kind/lib/node-services.sh", "releases"], quiet = True)).strip().split("\n")
watch_file("infra/space/kind/lib/node-services.sh")
watch_file("infra/space/kind/charts/space-node-service")
for line in NODE_RELEASES:
    release, role, service = line.split(" ")
    chart = "infra/space/kind/charts/space-{}-local".format(service)
    values = []
    if role == "builder" and os.path.exists(chart + "/values-builder.yaml"):
        values = [chart + "/values-builder.yaml"]
    node_set = TELEMETRY_SET + ["node.role={}".format(role)]
    if service == "node-manager" and LOOP_DEVICE_TARGET:
        node_set.append("loopDeviceTarget={}".format(LOOP_DEVICE_TARGET))
    k8s_yaml(helm(chart, name = release, namespace = NAMESPACE, values = values, set = node_set))
    k8s_resource(
        "{}-{}".format(release, service),
        labels = ["node-" + role],
        resource_deps = ["check-kind-node-image", "label-kind-workers", "sandbox-ssh-secret",
                         "internal-rpc-tls"],
    )

# Kind-only operator shell on every sandbox worker and builder; the smoke
# harness's on-node checks run here.
k8s_yaml(helm("infra/space/kind/charts/space-node-debug", name = "space", namespace = NAMESPACE))
k8s_resource(
    "space-node-debug",
    labels = ["node-debug"],
    resource_deps = ["check-kind-node-image", "label-kind-workers"],
)

# seed publish: uploads the locally built production seed to the object store and
# registers the release through the real ClusterApi RPCs. Workers then pull
# it via template-manager prefetch → volume-manager — the production path.
k8s_yaml(helm_release("space", "infra/space/kind/charts/space-seed-publish"))
k8s_resource(
    "space-seed-publish",
    labels = ["bootstrap"],
    resource_deps = ["space-internal-api", "space-object-store-bucket-init"],
)
