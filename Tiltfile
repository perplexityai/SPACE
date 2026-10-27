# Tiltfile for the local KinD SPACE stack.
#
# Brings up the SPACE control plane on a multi-node KinD cluster:
#   * postgres + versitygw object store (S3), with its IAM service and the
#     Kind-only STS bridge for credential-manager's storage exchange
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
# Bring-up, after docs/installation.md's prerequisites:
#   bash infra/space/kind/scripts/up.sh
# It creates the cluster, starts `tilt up` on this file detached, and returns
# once the stack is ready; rerun on a live stack, it reuses that Tilt.

KIND_CLUSTER = os.getenv("SPACE_KIND_CLUSTER", "space-local")
NAMESPACE = "space"
allow_k8s_contexts("kind-" + KIND_CLUSTER)
# allow_k8s_contexts only permits a context; Tilt applies to whichever one the
# kubeconfig currently names. The bring-up passes `--context`, and anything
# else is refused before a single command runs against it.
if k8s_context() != "kind-" + KIND_CLUSTER:
    fail("Tilt is pointed at context '{}', not kind-{}; start it with `tilt up --context kind-{}` (up.sh does)".format(
        k8s_context(), KIND_CLUSTER, KIND_CLUSTER))

# A cluster from before the versitygw object store keeps its objects in
# SeaweedFS; refuse it rather than apply the new charts over it. See "Object
# store transition" in infra/space/kind/README.md.
local(["bash", "infra/space/kind/scripts/check-object-store-transition.sh", KIND_CLUSTER], quiet = True)

# A cluster from before the per-service node DaemonSets still runs the single
# node-stack pod per node, and one created before the load-balanced entry has
# no host port mappings for it; refuse either rather than run on it. See
# "Upgrading from the node-stack layout" in infra/space/kind/README.md.
local(["bash", "infra/space/kind/scripts/check-node-layout.sh", KIND_CLUSTER], quiet = True)

# A cluster whose Postgres data directory an earlier space-postgres chart
# initialized (PostgreSQL 16) cannot run this chart's PostgreSQL 17; refuse it
# with the database reset rather than crash-loop the pod. See "Recovering from
# an unbundled migration version" in infra/space/kind/README.md.
local(["bash", "infra/space/kind/scripts/check-postgres-data.sh", KIND_CLUSTER], quiet = True)

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
# How node services get object-store credentials: credential-manager's storage
# exchange (the default) or the store's static keys. Both run the same store,
# IAM service and STS bridge; see "Object-store credentials" in
# infra/space/kind/README.md. The STS bridge's Secret is kept outside Helm, so
# its checksum goes to every chart whose pods read it, and a reissue rolls
# them. Teardown evaluates this file too and must not create it.
STORAGE_CREDENTIALS = str(local(["bash", "infra/space/kind/lib/storage-credentials.sh"], quiet = True)).strip()
watch_file("infra/space/kind/lib/storage-credentials.sh")
STS_BRIDGE_SECRET_CHECKSUM = ""
if config.tilt_subcommand != "down":
    STS_BRIDGE_SECRET_CHECKSUM = str(local(
        ["bash", "infra/space/kind/scripts/sts-bridge-secret.sh"],
        env = {"SPACE_KIND_CLUSTER": KIND_CLUSTER},
        quiet = True,
    )).strip()
    watch_file("infra/space/kind/scripts/sts-bridge-secret.sh")
# Which Rust binaries to build and deploy: test-hooks (the default) or the
# release lane, which builds without the test-hooks feature and renders no test
# hook into the charts. See "Release-build lane" in infra/space/kind/README.md.
RUST_BUILD = str(local(["bash", "infra/space/kind/lib/rust-build.sh"], quiet = True)).strip()
watch_file("infra/space/kind/lib/rust-build.sh")
CHART_SET = TELEMETRY_SET + [
    "storageCredentials=" + STORAGE_CREDENTIALS,
    "storageExchange.secretChecksum=" + STS_BRIDGE_SECRET_CHECKSUM,
    "rustBuild=" + RUST_BUILD,
]
watch_file("infra/space/kind/charts/space-kind-telemetry")
# Reconcile before returning any changed producer manifests, including on a
# watched profile transition. Resource startup dependencies alone do not gate
# subsequent updates. This helper never deletes backend data.
watch_file("infra/space/kind/lib/release.sh")
watch_file("infra/space/kind/scripts/install-observability.sh")
watch_file("ci/mirrored-images.txt")
watch_file("ci/scripts/mirror-images.sh")
watch_file("ci/scripts/lib/test-deps.sh")
# Watch source inputs, not Helm's generated dependency archives. Watching the
# whole chart tree would retrigger evaluation on every dependency build.
for signal in ["metrics", "traces", "logs", "grafana", "common"]:
    chart = "infra/space/observability/charts/" + signal
    for source in ["Chart.yaml", "values.yaml", "templates"]:
        watch_file(chart + "/" + source)
    if signal != "common":
        watch_file(chart + "/Chart.lock")
        for source in ["config", "profiles"]:
            watch_file(chart + "/" + source)
        watch_file("infra/space/kind/" + signal + "/local.sh")
        watch_file("infra/space/kind/" + signal + "/namespace.yaml")
# Show progress while a cold profile loads images and waits for its receivers.
# Teardown evaluates this file too; it must not install or resume receivers.
if config.tilt_subcommand != "down":
    local(["bash", "infra/space/kind/scripts/install-observability.sh"])

if TELEMETRY_MODE == "otel":
    # Each backend on the port its own `local.sh ui` helper uses. Tilt does not
    # deploy these releases, so it tracks no pods for port_forwards to follow:
    # port-forward.sh holds each forward instead, restarting it whenever its
    # pod does.
    for name, namespace, mapping, signal in [
        ("prometheus", "space-metrics", "19090:9090", "metrics"),
        ("tempo", "space-traces", "13200:3200", "traces"),
        ("loki", "space-logs", "13101:3100", "logs"),
        ("grafana", "space-grafana", "13000:3000", "grafana"),
    ]:
        if signal in ["metrics", "traces"] and signal not in TELEMETRY_CONFIG["local_signals"]:
            continue
        # Reuse-only mode requires logs and implicit receivers, not Grafana.
        if signal == "grafana" and not TELEMETRY_CONFIG["install_observability"]:
            existing = str(local([
                "kubectl", "--context", "kind-" + KIND_CLUSTER, "-n", namespace,
                "get", "svc", name, "--ignore-not-found", "-o", "name",
            ], quiet = True)).strip()
            if not existing:
                continue
        local_resource(
            "{}-port-forward".format(name),
            serve_cmd = [
                "bash", "infra/space/kind/scripts/port-forward.sh", "kind-" + KIND_CLUSTER,
                namespace, "svc/" + name, mapping, "space-kind-pf:kind-" + KIND_CLUSTER,
            ],
            links = [link("http://127.0.0.1:" + mapping.split(":")[0], name)],
            labels = ["observability"],
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
# Both build RUST_BUILD, the mode the charts are rendered for, and every
# runtime image built from the artifact image carries its build label
# (runtime-image-cache.sh), which scripts/deployed-rust-build.py reads back.
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
                      "infra/space/kind/lib/rust-build.sh",
                      "scripts/build-runtime-supervisor.sh",
                      "infra/space/identity/node-identities.sh",
                      "templates/firecracker/seed/rootfs-builder.sh",
                      "templates/firecracker/recipes/seed.toml",
                      "templates/firecracker/recipes/code-runtime.toml",
                      "templates/firecracker/recipes/code-runtime"]

RUST_ARTIFACT_BUILD = "SPACE_KIND_RUST_BUILD={} bash infra/space/kind/scripts/build-rust-artifacts.sh".format(RUST_BUILD)

# Every image build first pulls whichever of its Dockerfiles' registry bases
# are missing, retrying failed pulls, so a registry rate limit during a cold
# bring-up delays the build instead of failing it (see the script's header).
def pull_base_images(*dockerfiles):
    return "bash infra/space/kind/scripts/pull-base-images.sh " + " ".join(dockerfiles)

# Every runtime image is built by infra/space/kind/scripts/runtime-image-cache.sh,
# which adds the image's Dockerfile and its bases and cache imports to the given
# arguments. In GitHub CI (SPACE_CI_IMAGE_MIRROR set) a build takes each
# registry base listed in ci/mirrored-images.txt from the repository's GHCR
# mirror, through --build-context options; the Dockerfiles are unchanged. With
# SPACE_KIND_BUILD_CACHE_REPOSITORY set as well, as in Kind CI, it imports its
# dependency stages from the registry layer cache main CI writes, so an
# unchanged package list downloads nothing. A miss, an unreachable cache or a
# cached layer that cannot be read builds those layers cold.
#
# Each build goes through infra/space/kind/scripts/image-provenance.py, which
# computes the image's build identity when the build runs, not when this file
# loads: the commit, or `<commit>-dirty-<content>` for an image the worktree's
# changes reach. It passes the identity as the SPACE_GIT_SHA build argument
# (the bare `--build-arg SPACE_GIT_SHA` below takes it from the environment);
# for the UI and seed-publish, whose Dockerfiles take no such argument, it also
# sets the revision label (revision_label). It records the image for the smoke
# bring-up's provenance check. It certifies an image as the commit's only when
# the build's inputs were the commit's from the chain's first step, which
# compiles Rust or assembles the seed, to the end of its docker build:
# BUILD_INPUTS_SNAPSHOT runs first in every chain, and image_build hands its
# result to the script as --since. Every build sets --provenance=false:
# BuildKit's default attestation records the build time, so the image ID would
# change on every build, unchanged content or not, and the workload would roll.
BUILD_INPUTS_SNAPSHOT = 'SPACE_BUILD_INPUTS="$(python3 infra/space/kind/scripts/image-provenance.py snapshot)"'

def image_build(image, revision_label = False):
    return ("python3 infra/space/kind/scripts/image-provenance.py build --cluster {cluster} --image {image} " +
            '--since "$SPACE_BUILD_INPUTS"{label} -- ' +
            "bash infra/space/kind/scripts/runtime-image-cache.sh build {image} " +
            "--provenance=false --sbom=false").format(
                cluster = KIND_CLUSTER,
                image = image,
                label = " --revision-label" if revision_label else "",
            )

# Up to eight updates at once instead of Tilt's default three. Every Rust image
# waits on the artifact lock anyway, so the default mostly kept image builds and
# kind loads that could overlap in three strict waves after the compile: 124-176 s
# of a cold or warm bring-up with the host under 30 % CPU (2026-09-23 bench).
# SPACE_TILT_MAX_PARALLEL_UPDATES in Tilt's environment overrides the default;
# `tilt up` has no flag for it.
MAX_PARALLEL_UPDATES = int(os.getenv("SPACE_TILT_MAX_PARALLEL_UPDATES", "8"))
update_settings(max_parallel_updates = MAX_PARALLEL_UPDATES)

# Each image is declared here with space_image and built by a custom_build
# issued under "Image delivery" below, once every manifest is rendered: its
# command ends by loading the build into the nodes itself, onto the nodes its
# pods can schedule onto, and Tilt's own load step is off. See there. Tilt
# sets no $EXPECTED_REF for a build that names its own reference, so each
# build tags $SPACE_BUILD_REF, `<name>:tilt-build-<time>-<nonce>`.
IMAGE_BUILDS = []

def space_image(ref, command, deps):
    IMAGE_BUILDS.append({"ref": ref, "command": command, "deps": deps})

def rust_runtime_image(name, dockerfile, runtime_deps):
    cmd = " && ".join([
        BUILD_INPUTS_SNAPSHOT,
        pull_base_images(dockerfile),
        RUST_ARTIFACT_BUILD,
        ("{build} -t $SPACE_BUILD_REF " +
         "--build-arg SPACE_RUST_BUILD_IMAGE={rust_image} " +
         "--build-arg SPACE_GIT_SHA .").format(
            build = image_build(name),
            rust_image = RUST_ARTIFACT_IMAGE,
        ),
    ])
    space_image(
        ref = "{}:local".format(name),
        command = cmd,
        # The Rust workspace is the common base for all rust images; any
        # source change rebuilds the shared artifact image once (an
        # incremental compile against the persistent target dir, or a
        # cargo-chef-cached Docker build) and
        # every runtime image then re-copies its binaries from it.
        deps = RUST_ARTIFACT_DEPS + runtime_deps,
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
# the operator shell and space-internal-api run it unchanged. LTO is off for
# faster local iteration, except in the release lane (RUST_BUILD), which links
# with thin LTO.
space_image(
    ref = "space-worker:local",
    command = " && ".join([
        BUILD_INPUTS_SNAPSHOT,
        pull_base_images("docker/Dockerfile.worker"),
        RUST_ARTIFACT_BUILD,
        # --provenance=false (image_build) also keeps buildx from emitting a
        # multi-platform manifest list, which `kind load docker-image` cannot
        # ingest ("content digest ... not found").
        (image_build("space-worker") + " -t $SPACE_BUILD_REF " +
         "--build-arg SPACE_RUST_BUILD_IMAGE={} --build-arg SPACE_GIT_SHA .").format(
            RUST_ARTIFACT_IMAGE),
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
)

# space-seed-publish: carries the locally built production seed template
# (templates/firecracker/seed builder — vsock spaced, FC-bootable) plus the
# publisher that uploads it to the object store and registers the release through
# the real ClusterApi RPCs. build-seed.sh skips the privileged rootfs build
# when its inputs (guest binaries, builder script, recipe, pubkey) are
# unchanged, so watch-mode rebuilds are cheap unless spaced changed. One
# directory for both steps, resolved once: build-seed.sh writes
# SPACE_SEED_OUTPUT_DIR (default below), and the image is built from what it
# wrote.
space_image(
    ref = "space-seed-publish:local",
    command = " && ".join([
        BUILD_INPUTS_SNAPSHOT,
        pull_base_images("docker/Dockerfile.seed-publish"),
        RUST_ARTIFACT_BUILD,
        'seed_dir="${SPACE_SEED_OUTPUT_DIR:-$HOME/.space-devbox/kind/seed/build}"',
        'SPACE_SEED_OUTPUT_DIR="$seed_dir" SPACE_RUST_BUILD_IMAGE={} bash infra/space/kind/scripts/build-seed.sh'.format(
            RUST_ARTIFACT_IMAGE),
        (image_build("space-seed-publish", revision_label = True) + " -t $SPACE_BUILD_REF " +
         '--build-context seed="$seed_dir" .'),
    ]),
    deps = RUST_ARTIFACT_DEPS + [
            "templates/firecracker/seed",
            "templates/firecracker/recipes/seed.toml",
            "docker/Dockerfile.seed-publish",
            "infra/space/kind/scripts/build-seed.sh",
            "infra/space/kind/scripts/seed-publish-entry.sh"],
)

# Kind-only: the STS bridge fixture the credential-manager authority's storage
# exchange calls in place of AWS STS. Go, standard library only; the build runs
# its unit tests.
space_image(
    ref = "space-kind-sts-bridge:local",
    command = " && ".join([
        BUILD_INPUTS_SNAPSHOT,
        pull_base_images("docker/Dockerfile.kind-sts-bridge"),
        image_build("space-kind-sts-bridge") + " -t $SPACE_BUILD_REF --build-arg SPACE_GIT_SHA .",
    ]),
    deps = ["docker/Dockerfile.kind-sts-bridge", "infra/space/kind/sts-bridge"],
)

# Kind-only: the fake EC2 endpoint the cluster-controller's reconcile-queue GC
# calls in place of EC2. Go, standard library only.
space_image(
    ref = "space-kind-fake-ec2:local",
    command = " && ".join([
        BUILD_INPUTS_SNAPSHOT,
        pull_base_images("docker/Dockerfile.kind-fake-ec2"),
        image_build("space-kind-fake-ec2") + " -t $SPACE_BUILD_REF --build-arg SPACE_GIT_SHA .",
    ]),
    deps = ["docker/Dockerfile.kind-fake-ec2", "infra/space/kind/fake-ec2"],
)

# UI: no rust, single Dockerfile. NEXT_PUBLIC_API_URL and
# NEXT_PUBLIC_DATA_PLANE_URL must be passed at BUILD time — Next.js evaluates
# rewrites in next.config.js once at build and freezes the result in
# .next/required-server-files.json. Runtime env overrides are ignored. Both
# name the in-cluster Services: the Dockerfile's localhost defaults would point
# the UI pod's session proxy at itself.
space_image(
    ref = "space-ui:local",
    command = " && ".join([
        BUILD_INPUTS_SNAPSHOT,
        pull_base_images("docker/Dockerfile.ui"),
        (image_build("space-ui", revision_label = True) + " -t $SPACE_BUILD_REF " +
         "--build-arg NEXT_PUBLIC_API_URL=http://space-api:8080 " +
         "--build-arg NEXT_PUBLIC_DATA_PLANE_URL=http://space-data-plane:8090 ."),
    ]),
    deps = ["docker/Dockerfile.ui", "ui"],
)

# Every manifest goes to Tilt through deploy_yaml, which also records where
# each image's pods can schedule: the nodeSelector, required node affinity and
# nodeName of every pod template, by the repository of each image it runs
# (any tag: a superset of the references Tilt injects into). The image's load
# reads them (kind-load-image.py), so call k8s_yaml nowhere else: a pod
# template that bypassed deploy_yaml could land on a node its image was never
# loaded onto, and wait there in ImagePullBackOff.
POD_PLACEMENT = {}

def image_repository(image):
    return image.split("@")[0].split("/")[-1].split(":")[0]

def pod_specs(obj):
    spec = obj.get("spec")
    if type(spec) != "dict":
        return []
    if obj.get("kind") == "Pod":
        return [spec]
    if obj.get("kind") == "CronJob":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    template = spec.get("template")
    if type(template) != "dict" or type(template.get("spec")) != "dict":
        return []
    return [template["spec"]]

def deploy_yaml(yaml):
    k8s_yaml(yaml)
    for obj in decode_yaml_stream(read_file(yaml) if type(yaml) == "string" else yaml):
        if type(obj) != "dict":
            continue
        for spec in pod_specs(obj):
            required = ((spec.get("affinity") or {}).get("nodeAffinity") or {}).get(
                "requiredDuringSchedulingIgnoredDuringExecution") or {}
            constraint = {
                "nodeSelector": spec.get("nodeSelector") or {},
                "nodeSelectorTerms": required.get("nodeSelectorTerms") or [],
                "nodeName": spec.get("nodeName") or "",
            }
            for container in ((spec.get("initContainers") or []) + (spec.get("containers") or []) +
                              (spec.get("ephemeralContainers") or [])):
                known = POD_PLACEMENT.setdefault(image_repository(container.get("image") or ""), [])
                if constraint not in known:
                    known.append(constraint)

# ── Cluster bootstrap (RuntimeClass + node labels + Namespace) ───────────
#
# `KubernetesSubstrate::build_sandbox_pod` pins per-sandbox pods to
# `runtimeClassName: space` and `nodeSelector: node.kubernetes.io/space-enabled=true`.
# Neither exists in vanilla KinD, so we materialize them before the charts
# land. The SPACE Kind node image registers RuntimeClass `space`'s handler with
# containerd; it resolves to containerd-shim-space-v2 once install-shim.sh has
# installed that binary on each sandbox worker (the `install-space-shim`
# bootstrap resource below, full harness only).

# Namespace + RuntimeClass live in infra/space/kind/manifests/, which the
# resources below also apply before they need the namespace. NAMESPACE above
# must stay "space" — the manifest file hardcodes that name and the charts
# default to it. The same file creates
# `sandboxes`, the charts' k8s.sandboxNamespace, where the API creates the
# per-sandbox pods.
deploy_yaml("infra/space/kind/manifests/namespace.yaml")
deploy_yaml("infra/space/kind/manifests/runtimeclass.yaml")

# The sandbox namespace's NetworkPolicies: a default-deny plus the traffic a
# sandbox pod carries (the chart cites the code behind each rule). The API
# waits for them, so no sandbox pod runs before its namespace is confined.
# internal-rpc-tls applies the Namespace manifest first.
deploy_yaml(helm("infra/space/kind/charts/space-sandbox-network-policy", name = "space", namespace = NAMESPACE))
k8s_resource(
    new_name = "space-sandbox-network-policy",
    objects = [
        "space-sandbox-default-deny:networkpolicy:sandboxes",
        "space-sandbox-allow:networkpolicy:sandboxes",
    ],
    labels = ["bootstrap"],
    resource_deps = ["internal-rpc-tls"],
)

# Label sandbox workers so the api-server's K8s scheduler can place sandbox
# pods there. Targets only nodes with node-pool=sandbox (set in kind-cluster.yaml.tmpl)
# so the infra node is never labeled space-enabled and never receives sandbox pods.
local_resource(
    "label-kind-workers",
    cmd = "kubectl --context kind-{cluster} label nodes -l 'space.pplx.net/node-pool=sandbox' node.kubernetes.io/space-enabled=true --overwrite".format(cluster = KIND_CLUSTER),
    labels = ["bootstrap"],
)

# Label the infra node so Postgres, space-api, and space-cluster-controller
# can pin there via nodeSelector. These components must not run on sandbox
# worker nodes that the drain reconciler can delete. Every workload that names
# a SPACE PriorityClass waits on this resource, and admission rejects a pod
# whose class does not exist yet, so the classes are applied here too (the
# k8s_yaml keeps them Tilt-managed, as the Namespace manifest is).
deploy_yaml("infra/space/kind/manifests/priorityclasses.yaml")
local_resource(
    "label-kind-infra",
    cmd = """
set -eu
kubectl --context kind-{cluster} apply -f infra/space/kind/manifests/priorityclasses.yaml >/dev/null
kubectl --context kind-{cluster} label nodes -l 'space.pplx.net/node-pool=infra' space.pplx.net/infra=true --overwrite
""".format(cluster = KIND_CLUSTER),
    deps = ["infra/space/kind/manifests/priorityclasses.yaml"],
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
# cache that never comes. Every node-service release depends on it. It also
# refuses nodes whose vendor tree, copied from the host tree on their first boot
# and kept since, no longer matches that tree (check-node-vendor.sh).
local_resource(
    "check-kind-node-image",
    cmd = ("SPACE_KIND_CLUSTER={0} bash infra/space/kind/scripts/check-kind-node-image.sh && " +
           "SPACE_KIND_CLUSTER={0} bash infra/space/kind/scripts/check-node-vendor.sh").format(KIND_CLUSTER),
    deps = ["infra/space/kind/scripts/check-kind-node-image.sh",
            "infra/space/kind/scripts/check-node-vendor.sh",
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
RUST_ARTIFACTS_AFTER = ["label-kind-workers", "label-kind-infra", "sandbox-ssh-secret",
                        "internal-rpc-tls", "check-kind-node-image", "install-space-shim"]
local_resource(
    "rust-artifacts",
    cmd = RUST_ARTIFACT_BUILD,
    deps = RUST_ARTIFACT_DEPS,
    allow_parallel = True,
    resource_deps = RUST_ARTIFACTS_AFTER,
    labels = ["build"],
)

# ── Helm releases (in dependency order) ───────────────────────────────────

def helm_release(name, chart, values = []):
    return helm(chart, name = name, namespace = NAMESPACE, values = values, set = CHART_SET)

# A fresh render on every evaluation, so a backend transition never reuses the
# other backend's values. Fake mode never consumes SaaS credentials; SaaS uses
# an existing Secret only.
if DATADOG_MODE != "off":
    DATADOG_CHART = "infra/space/kind/charts/datadog-agent-local"
    # install-observability.sh, above, has already refused an Agent this
    # harness did not deploy (its guard-owner step runs in every profile).
    local("helm repo add datadog https://helm.datadoghq.com --force-update", quiet = True)
    local("helm dependency build {}".format(DATADOG_CHART), quiet = True)
    # Helm rewrites charts/ and tmpcharts-* during dependency builds. Watching
    # those generated files would retrigger the Tiltfile indefinitely.
    for source in ["Chart.yaml", "Chart.lock", "values.yaml", "values.schema.json", "values-fakeintake.yaml", "templates"]:
        watch_file(DATADOG_CHART + "/" + source)
    deploy_yaml(local(["python3", DATADOG_SCRIPT, "render"], quiet = True))
if DATADOG_MODE == "fakeintake":
    k8s_resource(
        "datadog-fakeintake",
        port_forwards = ["8088:8080"],
        labels = ["observability"],
    )

# Postgres first; the space-db-migrate Job blocks on it.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-postgres"))
k8s_resource(
    "space-postgres",
    labels = ["data"],
    resource_deps = ["label-kind-infra"],
)

# API server.
# Per-sandbox pods need the shim wired into containerd before the first create
# lands, and the rate limiters' store should answer before the first create
# reaches them (they would admit it open, not fail it).
SPACE_API_DEPS = ["space-postgres", "label-kind-workers", "label-kind-infra",
                  "install-space-shim", "internal-rpc-tls", "space-sandbox-network-policy",
                  "space-db-migrate", "space-redis"]
# No port_forward: localhost:8080 is the cluster's host port mapping onto the
# space-api-entry NodePort (kind-cluster.yaml.tmpl), which spreads connections
# across the autoscaled replicas as a deployment's load balancer does.
# values-kind.yaml carries the bench's non-test-hook overrides, which the
# caller-provided-cluster chart must not inherit.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-api-local",
                         values = ["infra/space/kind/charts/space-api-local/values-kind.yaml"]))
k8s_resource(
    "space-api",
    labels = ["control-plane"],
    resource_deps = SPACE_API_DEPS,
)
# The control-plane migrations, once, before any API replica starts (the
# chart's space-db-migrate Job), as a deployment runs them ahead of its
# application releases.
k8s_resource(
    "space-db-migrate",
    labels = ["data"],
    resource_deps = ["space-postgres", "label-kind-infra"],
)
# The dedicated admin API, its own process (--surface admin).
k8s_resource(
    "space-admin-api",
    labels = ["control-plane"],
    resource_deps = ["space-api"],
)

# The resource metrics API the API and data-plane HorizontalPodAutoscalers
# read, in kube-system as a managed cluster's add-on is.
deploy_yaml(helm("infra/space/kind/charts/space-metrics-server", name = "space", namespace = "kube-system"))
k8s_resource(
    "metrics-server",
    labels = ["control-plane"],
    resource_deps = ["label-kind-infra"],
)

# The Kind user stays locked in shared migrations. Provision only after the
# API pod finishes migrations.
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
deploy_yaml(helm(
    "infra/space/kind/charts/space-internal-api-local",
    name = "space",
    namespace = NAMESPACE,
    set = TELEMETRY_SET + ["rustBuild=" + RUST_BUILD],
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
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-credmgr-authority-local"))
k8s_resource(
    "space-credmgr-authority",
    labels = ["control-plane"],
    resource_deps = ["space-api", "label-kind-infra"] + (["space-aws-creds"] if STORAGE_CREDENTIALS == "static" else []),
)
# Optional credentials for the authority's own AWS integrations, such as AWS
# Secrets Manager network-credential references: the `space-aws-creds` Secret,
# from the AWS_* variables and SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN in Tilt's
# environment, when AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set. Only
# with static storage credentials: the chart reads the Secret in that mode
# alone (see "Object-store credentials" in infra/space/kind/README.md), so in
# exchange mode nothing is copied into the cluster. Sandbox-account,
# short-lived credentials only. The chart reads it at pod start, and Tilt reads
# its environment once, so rotating them means restarting Tilt (up.sh does when
# they changed) and then the authority. Fed to kubectl on stdin so the values
# stay off every command line.
if STORAGE_CREDENTIALS == "static":
    local_resource(
        "space-aws-creds",
        cmd = """
set -eu
if [ -z "${{AWS_ACCESS_KEY_ID:-}}" ] || [ -z "${{AWS_SECRET_ACCESS_KEY:-}}" ]; then
  echo "AWS credentials unset; the authority's optional AWS integrations stay disabled"
  exit 0
fi
kubectl --context kind-{cluster} apply -f infra/space/kind/manifests/namespace.yaml >/dev/null
printf 'AWS_ACCESS_KEY_ID=%s\\nAWS_SECRET_ACCESS_KEY=%s\\nAWS_SESSION_TOKEN=%s\\nSPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN=%s\\n' \\
  "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "${{AWS_SESSION_TOKEN:-}}" "${{SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN:-}}" \\
  | kubectl --context kind-{cluster} -n {ns} create secret generic space-aws-creds --from-env-file=/dev/stdin \\
      --dry-run=client -o yaml \\
  | kubectl --context kind-{cluster} apply -f -
""".format(cluster = KIND_CLUSTER, ns = NAMESPACE),
        allow_parallel = True,
        labels = ["bootstrap"],
    )
# The authority's external key provider: the Kind-only intermediate-key broker
# fixture, whose derived keys survive restarts of either side.
deploy_yaml(helm("infra/space/kind/charts/space-kind-key-broker", name = "space", namespace = NAMESPACE,
                 set = ["rustBuild=" + RUST_BUILD]))
k8s_resource(
    "space-kind-key-broker",
    labels = ["control-plane"],
    resource_deps = ["label-kind-infra"],
)

# Public data-plane service for native sessions and ECW wildcard hosts.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-data-plane-local"))
# localhost:8090 is the host port mapping onto space-data-plane-entry, as for
# the API above.
k8s_resource(
    "space-data-plane",
    labels = ["control-plane"],
    resource_deps = ["space-api", "space-internal-api", "label-kind-infra"],
)

# The cluster-controller materializes per-sandbox pods under the
# KubernetesSubstrate and hosts the cluster-singleton reconcilers.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-cluster-controller-local"))
k8s_resource(
    "space-cluster-controller",
    labels = ["control-plane"],
    resource_deps = ["space-api", "label-kind-infra"],
)
# Kind-only fake EC2 endpoint: the cluster-controller reconcile-queue GC's
# DescribeInstances, answered from the cluster's Nodes. Its leaf comes from
# internal-rpc-tls.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-kind-fake-ec2"))
k8s_resource(
    "space-kind-fake-ec2",
    labels = ["control-plane"],
    resource_deps = ["label-kind-infra", "internal-rpc-tls"],
)

deploy_yaml(helm_release("space", "infra/space/kind/charts/space-ui-local"))
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
# POSIX backend from a node-local hostPath, with versitygw's standalone IAM
# service in its own Deployment for the storage exchange's sessions; both take
# their TLS leaves from internal-rpc-tls. The bucket-init Job
# creates the `snapshots` + `templates` buckets through the S3 API and the
# exchange's OIDC provider and vend role through the IAM API. Both are public
# registry images the kind nodes pull directly, so no `kind load` step is
# needed.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-object-store"))
k8s_resource(
    "space-object-store",
    port_forwards = ["8333:8333"],  # S3 API
    labels = ["data"],
    resource_deps = ["label-kind-infra", "internal-rpc-tls"],
)
k8s_resource(
    "space-object-store-iam",
    objects = ["space-object-store-iam:networkpolicy"],
    labels = ["data"],
    resource_deps = ["label-kind-infra", "internal-rpc-tls"],
)
k8s_resource(
    "space-object-store-bucket-init",
    labels = ["data"],
    resource_deps = ["space-object-store", "space-object-store-iam"],
)
# Kind-only STS bridge: the authority's AssumeRole in front of the store's IAM
# service, and the token issuer that service trusts. Its STS listener's leaf
# comes from internal-rpc-tls.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-kind-sts-bridge"))
k8s_resource(
    "space-kind-sts-bridge",
    labels = ["data"],
    resource_deps = ["label-kind-infra", "internal-rpc-tls"],
)

# Redis: shared store for the two API rate limiters (cluster activation
# admission + per-client concurrency), which space-api, space-admin-api and
# space-cluster-controller reach through SPACE_REDIS_URL. Public docker.io image
# pulled directly by the kind nodes (no `kind load`). space-api waits for it.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-redis-local"))
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
# resources their pods mount, and for label-kind-infra, so that the worker
# image, which space-internal-api also runs on the infra node, is loaded
# knowing which node that is. A chart's values-builder.yaml carries its
# builder overrides.
# SPACE_LOOP_DEVICE_TARGET sets the loop-device pool node-manager raises each
# node to once it is up; the node image stages the library's floor of 64 before
# that. Inherited from the environment so `SPACE_LOOP_DEVICE_TARGET=128 tilt up`
# reaches the pod; unset renders nothing and the default applies.
LOOP_DEVICE_TARGET = os.getenv("SPACE_LOOP_DEVICE_TARGET", "")
# The release list is shared with the bring-up's DaemonSet waits.
NODE_RELEASES = str(local(["bash", "infra/space/kind/lib/node-services.sh", "releases"], quiet = True)).strip().split("\n")
watch_file("infra/space/kind/lib/node-services.sh")
watch_file("infra/space/kind/charts/space-node-service")
for line in NODE_RELEASES:
    release, role, service = line.split(" ")
    chart = "infra/space/kind/charts/space-{}-local".format(service)
    values = []
    if role == "builder" and os.path.exists(chart + "/values-builder.yaml"):
        values = [chart + "/values-builder.yaml"]
    node_set = CHART_SET + ["node.role={}".format(role)]
    if service == "node-manager" and LOOP_DEVICE_TARGET:
        node_set.append("loopDeviceTarget={}".format(LOOP_DEVICE_TARGET))
    deploy_yaml(helm(chart, name = release, namespace = NAMESPACE, values = values, set = node_set))
    k8s_resource(
        "{}-{}".format(release, service),
        labels = ["node-" + role],
        resource_deps = ["check-kind-node-image", "label-kind-workers", "label-kind-infra",
                         "sandbox-ssh-secret", "internal-rpc-tls"],
    )

# Kind-only operator shell on every sandbox worker and builder; the smoke
# harness's on-node checks run here.
deploy_yaml(helm("infra/space/kind/charts/space-node-debug", name = "space", namespace = NAMESPACE,
                 set = ["rustBuild=" + RUST_BUILD]))
k8s_resource(
    "space-node-debug",
    labels = ["node-debug"],
    resource_deps = ["check-kind-node-image", "label-kind-workers", "label-kind-infra"],
)

# seed publish: uploads the locally built production seed to the object store and
# registers the release through the real ClusterApi RPCs. Workers then pull
# it via template-manager prefetch → volume-manager — the production path.
deploy_yaml(helm_release("space", "infra/space/kind/charts/space-seed-publish"))
k8s_resource(
    "space-seed-publish",
    labels = ["bootstrap"],
    resource_deps = ["space-internal-api", "space-object-store-bucket-init"],
)

# ── Image delivery ───────────────────────────────────────────────────────────
#
# The images declared under "Image builds", each a custom_build whose command
# ends by putting the build onto the nodes itself (kind-load-image.py), with
# Tilt's own load step off (disable_push). That step loaded every image into
# all six nodes, and on Docker's containerd image store kind cannot tell that a
# node already holds an image (Docker names it by its manifest digest, the
# node's CRI by its config digest), so every Tilt start copied every image to
# every node again. The script tags the build `<name>:tilt-<config digest>`,
# loads it only onto the nodes its pods can schedule onto (POD_PLACEMENT, from
# the manifests above) that do not already hold that digest, and writes the tag
# to the file Tilt then deploys it under (outputs_image_ref_to). An unchanged
# image keeps its tag, so its workloads do not roll. Every checkout on the host
# builds into the same Docker store, so the build tag carries the shell's pid
# and a random nonce besides the time: two builds of one image in the same
# second, from two checkouts, must not share a tag (Tilt runs the command
# with `sh`, which may have no $RANDOM).
IMAGE_REF_DIR = "{}/image-refs/{}".format(
    os.getenv("SPACE_TILT_STATE_DIR", "") or os.getenv("HOME", "") + "/.space-devbox/kind", KIND_CLUSTER)
for image in IMAGE_BUILDS:
    name = image["ref"].split(":")[0]
    ref_file = "{}/{}".format(IMAGE_REF_DIR, name)
    custom_build(
        ref = image["ref"],
        command = " && ".join([
            "SPACE_BUILD_REF={}:tilt-build-$(date +%s)-$$-$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')".format(name),
            image["command"],
            ("python3 infra/space/kind/scripts/kind-load-image.py --cluster {} --image {} " +
             "--build-ref \"$SPACE_BUILD_REF\" --ref-file '{}'").format(KIND_CLUSTER, name, ref_file),
        ]),
        deps = image["deps"],
        disable_push = True,
        outputs_image_ref_to = ref_file,
        env = {"SPACE_KIND_IMAGE_PLACEMENT": str(encode_json(POD_PLACEMENT.get(name, [])))},
    )

# ── Superseded image prune ───────────────────────────────────────────────────
#
# Every changed build adds a `:tilt-<hash>` tag to the containerd store of each
# node it is loaded onto, under $SPACE_KIND_DATA_ROOT, which outlives the
# cluster. Whenever Tilt
# has nothing pending or in progress and what the cluster deploys has changed,
# this removes the builds of SPACE's own images that nothing deployed, built or
# running still names; third-party images are never touched. The rule and its
# safety are in prune-superseded-images.py and the Kind README.
# SPACE_KIND_PRUNE_SUPERSEDED_IMAGES=0 leaves the resource out.
if os.getenv("SPACE_KIND_PRUNE_SUPERSEDED_IMAGES", "1") != "0":
    local_resource(
        "prune-superseded-images",
        serve_cmd = ["python3", "infra/space/kind/scripts/prune-superseded-images.py",
                     "--cluster", KIND_CLUSTER, "--watch", "30",
                     "--tilt-resource", "prune-superseded-images"],
        deps = ["infra/space/kind/scripts/prune-superseded-images.py",
                "infra/space/kind/lib/space-images.sh"],
        labels = ["bootstrap"],
    )

# The host's Docker store, where those builds are made, keeps every one of them
# too: each Tilt build adds a `:tilt-build-<time>-<nonce>` tag (the
# $SPACE_BUILD_REF the build commands above tag) and the `:tilt-<hash>` Tilt
# deploys. Tilt's built-in pruner (docker_prune_settings) cannot remove them,
# labelled or not: it passes over every image with more than one tag
# (internal/engine/dockerprune/docker_pruner.go in tilt-dev/tilt). So whenever
# an update has settled, the same script removes from the host the builds of
# SPACE's images it would remove from the nodes, except that the build before
# the current one stays too.
# SPACE_TILT_PRUNE_HOST_IMAGES=0 leaves the resource out.
if os.getenv("SPACE_TILT_PRUNE_HOST_IMAGES", "1") != "0":
    local_resource(
        "prune-host-images",
        serve_cmd = ["python3", "infra/space/kind/scripts/prune-superseded-images.py",
                     "--host", "--cluster", KIND_CLUSTER, "--watch", "30",
                     "--tilt-resource", "prune-host-images"],
        deps = ["infra/space/kind/scripts/prune-superseded-images.py",
                "infra/space/kind/lib/space-images.sh"],
        labels = ["bootstrap"],
    )
