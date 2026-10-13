#!/usr/bin/env bash
# bootstrap.sh — bring up the full production-shaped SPACE stack on a local
# kind cluster.
#
# Targets the multi-worker `space-local` kind cluster (config rendered from
# infra/space/kind/kind-cluster.yaml.tmpl), brings up the production-shaped
# topology: api-server on KubernetesSubstrate, cluster-controller materializing
# per-sandbox pods, one DaemonSet per node service on both sandbox workers and
# on the dedicated builder nodes (plus build-manager there), runtime-supervisor
# as a systemd unit on each of those nodes, the versitygw object store for S3,
# and the seed template published through the real ClusterApi + object-store
# path (space-seed-publish Job).
#
# Idempotent: re-running picks up where it left off. Each step is its own
# guard.
#
# Usage:
#   bash infra/space/kind/scripts/bootstrap.sh
#   bash infra/space/kind/scripts/bootstrap.sh --skip-images   # reuse last builds
#   bash infra/space/kind/scripts/bootstrap.sh --no-port-forward
#
# Env knobs:
#   SPACE_KIND_CLUSTER       kind cluster name (default: space-local)
#   SPACE_CONSOLE_PASSWORD   initial root@local password (default: password;
#                            only applied while the account is locked)
#   SPACE_KIND_PARALLEL=1    forward to build-images.sh (parallel builds)
#   SPACE_BOOTSTRAP_TIMEOUT  per-release wait, in seconds (default: 600)
#   SPACE_KIND_DATADOG_MODE  off (default), fakeintake, or explicit saas
#   SPACE_KIND_OBSERVABILITY_CONFIG watched non-secret experiment JSON file;
#                            required for SaaS. See the Kind README.
#   SPACE_KIND_TELEMETRY_MODE otel (default), datadog, or off. The otel
#                            mode installs the local metrics, traces, logs and
#                            visualization backends and sends SPACE traces and
#                            all metric types, the Go shim's included, to the
#                            Collectors. datadog uses the native exporters; off
#                            disables metrics and traces. Metrics sinks are
#                            exclusive; logs stay on stdout in every mode.
#
# Host-path knobs, shared with the rendered cluster config and documented in
# infra/space/kind/scripts/lib/kind-host-paths.sh:
#   SPACE_VENDOR_ROOT        Firecracker vendor tree (default: ~/.space-devbox/space/vendor)
#   SPACE_KIND_DATA_ROOT     per-node containerd storage (default: ~/.kind-data)
#   SPACE_KIND_CONFIG        rendered cluster config (default: ~/.space-devbox/kind/kind-cluster.yaml)
set -euo pipefail
set +x

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
NAMESPACE=space
TIMEOUT="${SPACE_BOOTSTRAP_TIMEOUT:-600}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
KIND_DIR="$REPO_ROOT/infra/space/kind"
SSH_KEY_DIR="$HOME/.space-devbox/kind/ssh"
PF_PIDFILE="${SPACE_PORT_FORWARD_PIDFILE:-/tmp/space-local-pf.pids}"
CONSOLE_PASSWORD="${SPACE_CONSOLE_PASSWORD-password}"
export -n CONSOLE_PASSWORD
unset SPACE_CONSOLE_PASSWORD

SKIP_IMAGES=0
PORT_FORWARD=1
while (( $# > 0 )); do
  case "$1" in
    --skip-images)     SKIP_IMAGES=1 ;;
    --no-port-forward) PORT_FORWARD=0 ;;
    -h|--help)
      sed -n '2,/^set/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      printf 'unknown flag: %s\n' "$1" >&2
      exit 2
      ;;
  esac
  shift
done

log() { printf '[bootstrap] %s\n' "$*"; }
die() { printf '[bootstrap] ERROR: %s\n' "$*" >&2; exit 1; }

[[ -n "$CONSOLE_PASSWORD" ]] || die "SPACE_CONSOLE_PASSWORD must not be empty"
[[ "$CONSOLE_PASSWORD" != *$'\n'* ]] || die "SPACE_CONSOLE_PASSWORD must not contain a newline"
password_bytes=$(printf '%s' "$CONSOLE_PASSWORD" | wc -c)
(( password_bytes <= 72 )) || die "SPACE_CONSOLE_PASSWORD must be at most 72 bytes"

# Validate before creating or changing any cluster resources.
TELEMETRY_JSON="$(python3 "$KIND_DIR/scripts/datadog_backend.py" preflight --for-deployment)"
DATADOG_MODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["backend"])' <<<"$TELEMETRY_JSON")"
TELEMETRY_MODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["mode"])' <<<"$TELEMETRY_JSON")"
telemetry_values="$(python3 "$KIND_DIR/scripts/telemetry_config.py" helm-set)"
TELEMETRY_HELM_ARGS=()
while IFS= read -r value; do
  TELEMETRY_HELM_ARGS+=(--set-string "$value")
done <<<"$telemetry_values"

# ── Step 0: prereqs ──────────────────────────────────────────────────────────
require_bin() {
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || die "required tool not on PATH: $bin"
  done
}
require_bin docker kind kubectl helm openssl

# Helm 3 is required (helm v2 used a different syntax for `upgrade --install`).
helm_major=$(helm version --short 2>/dev/null | sed -n 's/^v\([0-9]\+\).*/\1/p')
[[ "$helm_major" == "3" ]] || die "helm 3 required (found: $(helm version --short 2>/dev/null))"

# Docker daemon must be reachable. We DON'T require start-dind.sh — if the
# host's existing daemon works (e.g. the devbox platform provisions a
# dockerd at tcp://localhost:2375 by default), use it directly. start-dind
# is only needed when the host's docker partition is too small for kind
# images, or when we deliberately want isolation from a shared daemon.
docker info >/dev/null 2>&1 \
  || die "docker daemon unreachable via DOCKER_HOST=${DOCKER_HOST:-(unset)}; run start-dind.sh first or point DOCKER_HOST at a working daemon"
log "using docker daemon at ${DOCKER_HOST:-default} ($(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unknown))"

# ── Step 0a: render the kind cluster config ──────────────────────────────────
# `kind create` expands nothing, so the host paths it bind-mounts are resolved
# and stamped into the config here. Sourcing the library also exports
# $SPACE_VENDOR_ROOT and $SPACE_KIND_DATA_ROOT, which is what keeps the mount,
# the staging check below, and install-fc-vendor.sh on one path.
# shellcheck source=lib/kind-host-paths.sh
source "$KIND_DIR/scripts/lib/kind-host-paths.sh"

# The config `kind create` reads has to be this process's own. The default is
# per-user, so a second bootstrap.sh — a sibling worktree, a concurrent devbox
# session — can replace it between this run's render and its create, and with
# different roots that builds a cluster from the other run's paths. The window
# is wide: the vendor tree is staged in between. A verified copy is published
# at the documented path afterwards, so the knob still names something real.
kind_config_published="$SPACE_KIND_CONFIG"
SPACE_KIND_CONFIG="$SPACE_KIND_CONFIG.$$"
kind_config_staged="$kind_config_published.$$.pub"
trap 'rm -f "$SPACE_KIND_CONFIG" "$kind_config_staged"' EXIT
log "rendering kind cluster config → $kind_config_published"
kind_render_cluster_config "$KIND_DIR/kind-cluster.yaml.tmpl"

# ── Step 0b: stage the Firecracker vendor tree ───────────────────────────────
# The sandbox and builder kind nodes bind-mount $SPACE_VENDOR_ROOT at
# /space/vendor, where the host runtime-supervisor and the node services read
# it. Reuse an already staged vendor tree
# or run the artifact installer here. The root is
# named at the call site even though sourcing the library already exported it:
# one resolved value reaching both the mount and the writer is the whole point
# of the fix, so the call that stages the tree says which value it is rather
# than leaving it to an inherited environment.
if [[ ! -x "$SPACE_VENDOR_ROOT/$KIND_HOST_PATHS_VENDOR_PROBE" ]]; then
  log "staging Firecracker vendor tree (no sudo) → $SPACE_VENDOR_ROOT"
  SPACE_VENDOR_ROOT="$SPACE_VENDOR_ROOT" bash "$KIND_DIR/scripts/install-fc-vendor.sh"
else
  log "Firecracker vendor already staged ($SPACE_VENDOR_ROOT)"
fi

# ── Step 0c: create and verify the bind-mount sources ────────────────────────
# Creates the per-node containerd storage dirs, which are meant to start
# empty, and fails on any other missing source, which is not. `kind create`
# accepts an extraMount whose hostPath does not exist — Docker creates it and
# the node gets an empty directory — so nothing downstream can tell an
# unstaged vendor tree from a staged one.
kind_prepare_host_mounts

# Published only once it has verified, so the documented path never holds a
# config that failed its preflight — and published by rename, because `cp`
# truncates before it writes: a concurrent reader of the shared path (the
# documented manual render-and-create flow) could otherwise hand `kind create`
# a partial YAML. Staged in the destination's own directory, so the rename is
# a same-filesystem one and cannot degrade to copy-then-unlink. Same idiom as
# the renderer and as render-cluster-config.sh.
cp -f "$SPACE_KIND_CONFIG" "$kind_config_staged"
mv -f "$kind_config_staged" "$kind_config_published"

# ── Step 1: kind cluster ────────────────────────────────────────────────────
if kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  log "kind cluster '$KIND_CLUSTER' already exists; skipping create"
  # Skipping the create also skips the only read of the config, so a cluster
  # built against a different vendor root would silently keep it.
  kind_assert_live_cluster_mounts "$KIND_CLUSTER"
  # A cluster from before the versitygw object store keeps its objects in
  # SeaweedFS; stop before applying anything rather than half-switch it.
  bash "$KIND_DIR/scripts/check-object-store-transition.sh" "$KIND_CLUSTER" || exit 1
  # Sandbox workers and builders get their node layer from the image they were
  # created from, so a cluster from before the SPACE Kind node image, or from
  # an older build of it, has to be recreated rather than patched.
  bash "$KIND_DIR/scripts/check-kind-node-image.sh" "$KIND_CLUSTER"
  # A cluster from before the per-service node DaemonSets runs every node
  # service, and its sandboxes, in one pod per node; stop rather than run both
  # layouts on a node.
  bash "$KIND_DIR/scripts/check-node-layout.sh" "$KIND_CLUSTER" || exit 1
else
  # The sandbox workers and builders boot the SPACE Kind node image, which
  # bakes in the node layer a production node image provides (btrfs
  # /space/cache, cgroup subtree, loop devices, /space skeleton). The config
  # names its content-addressed tag; make it present: local, else GHCR, else a
  # local build.
  log "providing the SPACE Kind node image"
  bash "$KIND_DIR/scripts/kind-node-image.sh" ensure >/dev/null
  log "creating kind cluster '$KIND_CLUSTER' from $SPACE_KIND_CONFIG"
  kind create cluster --name "$KIND_CLUSTER" --config "$SPACE_KIND_CONFIG"
fi
KCTX="kind-$KIND_CLUSTER"

# ── Step 2: node labels ──────────────────────────────────────────────────────
# Sandbox workers get the placement key the api-server pins sandbox pods to.
# The infra worker gets the key the control-plane charts pin to. These are
# label selectors (driven by the pool label the cluster config already set),
# so the script doesn't need to know individual node names.
#
# Applied as a function because admin-applied labels have been observed to
# vanish between this step and the chart deploys on a cold first bring-up
# (node churn while the shim installs / images side-load), which strands
# every infra-pinned chart Pending until its rollout wait times out. The
# labels are re-asserted right before the charts, and verified each time so
# a wipe fails loudly instead of as a 10-minute timeout.
apply_node_labels() {
  # Pool labels are applied by each kubelet as it registers; right after
  # `kind create` returns they may not be visible yet, and `kubectl label
  # -l <pool selector>` against zero matches exits 0 having labeled nothing
  # — the original silent-miss. Wait for every pool to be visible first.
  local attempt
  for ((attempt = 1; attempt <= 60; attempt++)); do
    if [[ -n "$(kubectl --context "$KCTX" get nodes -l space.pplx.net/node-pool=sandbox -o name 2>/dev/null)" && \
          -n "$(kubectl --context "$KCTX" get nodes -l space.pplx.net/node-pool=infra -o name 2>/dev/null)" && \
          -n "$(kubectl --context "$KCTX" get nodes -l space.pplx.net/node-pool=builder -o name 2>/dev/null)" ]]; then
      break
    fi
    sleep 2
  done

  log "labeling sandbox workers (node.kubernetes.io/space-enabled=true)"
  kubectl --context "$KCTX" label nodes -l space.pplx.net/node-pool=sandbox \
    node.kubernetes.io/space-enabled=true --overwrite >/dev/null

  log "labeling infra worker (space.pplx.net/infra=true)"
  kubectl --context "$KCTX" label nodes -l space.pplx.net/node-pool=infra \
    space.pplx.net/infra=true --overwrite >/dev/null

  [[ -n "$(kubectl --context "$KCTX" get nodes -l node.kubernetes.io/space-enabled=true -o name)" ]] \
    || { echo "[bootstrap] ERROR: no node carries node.kubernetes.io/space-enabled=true after labeling" >&2; exit 1; }
  [[ -n "$(kubectl --context "$KCTX" get nodes -l space.pplx.net/infra=true -o name)" ]] \
    || { echo "[bootstrap] ERROR: no node carries space.pplx.net/infra=true after labeling" >&2; exit 1; }
  [[ -n "$(kubectl --context "$KCTX" get nodes -l space.pplx.net/node-pool=builder -o name)" ]] \
    || { echo "[bootstrap] ERROR: no node carries space.pplx.net/node-pool=builder" >&2; exit 1; }
}
apply_node_labels

# ── Step 3: extended resource — network slots ─────────────────────────────────
# Every sandbox pod requests `space.pplx.net/network-slots: "1"` (set by
# KubernetesSubstrate::build_sandbox_pod). Without a node advertising
# capacity, the kube scheduler leaves sandbox pods Pending forever. In prod
# space-node-ctl patches each Node's status.capacity from node-manager's
# `network_pool_max_slots`; locally we advertise a static pool.
log "advertising network-slots capacity on sandbox workers"
for node in $(kubectl --context "$KCTX" get nodes -l space.pplx.net/node-pool=sandbox -o name); do
  # JSON-patch `add` on a map key is idempotent. ~1 in the json-pointer is
  # the escape for `/` inside the key `space.pplx.net/network-slots`.
  kubectl --context "$KCTX" patch "$node" --subresource=status --type=json \
    -p '[{"op":"add","path":"/status/capacity/space.pplx.net~1network-slots","value":"100"}]' \
    >/dev/null || true
done

# ── Step 4: install the SPACE containerd shim on each sandbox worker ─────────
# Without this, RuntimeClass `space` (handler: space) is unresolvable and
# every per-sandbox pod fails to start. install-shim.sh is idempotent: it
# only builds the shim if needed and only edits containerd config.toml if
# the runtime stanza isn't already present.
# The local backends otel mode exports to come up early so start-up telemetry
# has a receiver, but nothing depends on them: the shim addresses the
# Collectors by fixed ClusterIPs, and every exporter retries until a receiver
# answers. Each helper is idempotent and refuses any cluster but this bench.
if [[ "$TELEMETRY_MODE" == otel ]]; then
  bash "$KIND_DIR/scripts/install-observability.sh"
fi

log "installing containerd-shim-space-v2 on sandbox workers"
bash "$KIND_DIR/scripts/install-shim.sh"

# ── Step 5: shared manifests (namespace + RuntimeClass) ──────────────────────
log "applying namespace + RuntimeClass"
kubectl --context "$KCTX" apply -f "$KIND_DIR/manifests/namespace.yaml" >/dev/null
kubectl --context "$KCTX" apply -f "$KIND_DIR/manifests/runtimeclass.yaml" >/dev/null

# Internal RPC TLS: the cluster's CA and the listeners' identities as Secrets,
# before any workload that mounts them (docs/internal-rpc-tls.md).
SPACE_KIND_CLUSTER="$KIND_CLUSTER" bash "$KIND_DIR/scripts/internal-rpc-tls.sh"

# ── Step 5: images ───────────────────────────────────────────────────────────
if (( SKIP_IMAGES )); then
  log "--skip-images given; assuming every build-images.sh image is already loaded"
else
  log "building + loading SPACE images (this is the long step on a cold cache)"
  SPACE_KIND_CLUSTER="$KIND_CLUSTER" \
    bash "$KIND_DIR/scripts/build-images.sh"
fi

# Re-assert node labels: the shim install and image side-loading above can
# churn nodes enough to drop admin-applied labels (pool labels from
# the cluster config survive because the kubelet owns them).
apply_node_labels

# ── Step 7: render + apply charts ────────────────────────────────────────────
#
# Every kind chart names its resources with `{{ .Release.Name }}-<role>` (see
# e.g. space-postgres/templates/statefulset.yaml -> name: {{ .Release.Name }}-postgres)
# and the chart set is designed to be deployed AS ONE under release name
# "space". Real `helm upgrade --install space CHART` for each chart treats
# them as different revisions of the SAME release, which makes helm remove
# the previous chart's resources on the next install. The Tilt setup
# sidesteps this by using `helm template` + kubectl apply (no release
# management), which is what we mirror here. Every rendered object names its
# own namespace (`--namespace` sets the release's), so the apply takes no
# `-n`: the API, cluster-controller and internal-api charts also render RBAC
# into the sandbox namespace, which a `-n space` apply would refuse.
helm_apply() {
  local release=$1 chart=$2; shift 2
  log "helm template + apply: $release ← $(basename "$chart")"
  helm template "$release" "$chart" \
    --namespace "$NAMESPACE" \
    --include-crds \
    ${TELEMETRY_HELM_ARGS[@]+"${TELEMETRY_HELM_ARGS[@]}"} \
    "$@" \
    | kubectl --context "$KCTX" apply --validate=false -f -
}

# ── Optional AWS creds for independent credmgr integrations ──────────────────
# When AWS_ACCESS_KEY_ID is set (typically sourced from
# infra/space/kind/private.env — SANDBOX ACCOUNT ONLY), create the
# `space-aws-creds` Secret the credential-manager authority chart maps into its
# env for features such as Private CA. KEK wrapping uses the Kind key broker.
# These are short-lived STS creds — re-run bootstrap after refreshing
# private.env to rotate them.
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  log "creating space/space-aws-creds Secret for optional credmgr AWS integrations"
  kubectl --context "$KCTX" -n "$NAMESPACE" create secret generic space-aws-creds \
    --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    --from-literal=AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    --from-literal=SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN="${SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN:-}" \
    --dry-run=client -o yaml \
    | kubectl --context "$KCTX" apply -n "$NAMESPACE" -f - >/dev/null
else
  log "AWS creds unset — optional credmgr AWS integrations stay disabled."
fi

# Postgres first; the space-db-migrate Job blocks on it. Wait
# for it to be ready BEFORE installing other charts that try to talk to it,
# so initContainer / Job retries don't waste backoff slots.
helm_apply space "$KIND_DIR/charts/space-postgres"
log "waiting for postgres StatefulSet to be ready"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status sts/space-postgres --timeout="${TIMEOUT}s"

# Object store (versitygw) — Deployment plus a Job that creates the buckets once
# the deployment is reachable.
# A Job's spec.template is immutable, so re-applying this chart against a
# cluster whose copy of the Job was rendered from a different commit fails the
# whole object-store step with `field is immutable` — before anything downstream
# runs. Pre-delete it, exactly as space-seed-publish is pre-deleted below. The
# Job is built to be re-run: it waits for the S3 API, then creates whichever
# bucket is missing.
kubectl --context "$KCTX" -n "$NAMESPACE" delete job space-object-store-bucket-init --ignore-not-found >/dev/null
helm_apply space "$KIND_DIR/charts/space-object-store"
log "waiting for object-store Deployment to be ready"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-object-store --timeout="${TIMEOUT}s"

# Redis — shared store for the API rate limiters (cluster activation admission
# + per-client concurrency). Idle unless SPACE_REDIS_URL points at it; the
# rate-limiter smoke (scripts/tests/smoke-kind-rate-limiters.sh) wires that up.
helm_apply space "$KIND_DIR/charts/space-redis-local"
log "waiting for redis Deployment to be ready"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-redis --timeout="${TIMEOUT}s"

# The sandbox namespace's NetworkPolicies, before the API can create the first
# sandbox pod there.
helm_apply space "$KIND_DIR/charts/space-sandbox-network-policy"

# The control-plane migrations run once, as the api chart's space-db-migrate
# Job, against the now-ready postgres before any API replica starts. A Job's
# pod template is immutable, so the previous run's Job is replaced.
kubectl --context "$KCTX" -n "$NAMESPACE" delete job space-db-migrate --ignore-not-found >/dev/null
helm template space "$KIND_DIR/charts/space-api-local" --namespace "$NAMESPACE" \
  ${TELEMETRY_HELM_ARGS[@]+"${TELEMETRY_HELM_ARGS[@]}"} --show-only templates/migrate-job.yaml \
  | kubectl --context "$KCTX" apply -n "$NAMESPACE" --validate=false -f -
log "waiting for the space-db-migrate Job (control-plane migrations)"
kubectl --context "$KCTX" -n "$NAMESPACE" wait --for=condition=complete \
  job/space-db-migrate --timeout="${TIMEOUT}s" >/dev/null

# api-server. The main container is gated on /readyz, which only turns 200
# after the K8s pod reflector's initial LIST completes (empty list is fine at
# this stage — no node-api pods yet).
helm_apply space "$KIND_DIR/charts/space-api-local"
log "waiting for space-api Deployment to be ready"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-api --timeout="${TIMEOUT}s"

SPACE_CONSOLE_PASSWORD="$CONSOLE_PASSWORD" bash "$KIND_DIR/scripts/provision-local-console.sh"

# ── Sandbox SSH keypair Secret ────────────────────────────────────────────────
# Generated once per devbox by build-seed.sh (the local stand-in for the
# AMI's Packer-generated key). The public half is baked into the seed rootfs;
# the private half is mounted into build-manager, which SSHes into build VMs.
if [[ -f "$SSH_KEY_DIR/sandbox_ed25519" ]]; then
  log "creating/refreshing space-sandbox-ssh Secret from $SSH_KEY_DIR"
  kubectl --context "$KCTX" -n "$NAMESPACE" create secret generic space-sandbox-ssh \
    --from-file=sandbox_ed25519="$SSH_KEY_DIR/sandbox_ed25519" \
    --from-file=sandbox_ed25519.pub="$SSH_KEY_DIR/sandbox_ed25519.pub" \
    --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f - >/dev/null
else
  log "WARNING: $SSH_KEY_DIR/sandbox_ed25519 missing (build-seed.sh not run?); space-ssh will not work"
fi

# The resource metrics API the API and data-plane HorizontalPodAutoscalers
# read, in kube-system as a managed cluster's add-on is (so not through
# helm_apply, whose apply is pinned to $NAMESPACE).
log "helm template + apply: space ← space-metrics-server (kube-system)"
helm template space "$KIND_DIR/charts/space-metrics-server" --namespace kube-system \
  | kubectl --context "$KCTX" apply -n kube-system --validate=false -f -

# Once api-server has the control-plane schema, drive everything else.
helm_apply space "$KIND_DIR/charts/space-internal-api-local"
helm_apply space "$KIND_DIR/charts/space-kind-key-broker"
helm_apply space "$KIND_DIR/charts/space-credmgr-authority-local"
helm_apply space "$KIND_DIR/charts/space-data-plane-local"
helm_apply space "$KIND_DIR/charts/space-cluster-controller-local"
helm_apply space "$KIND_DIR/charts/space-ui-local"
# Node services: one release of each per role (lib/node-services.sh) —
# `space` on the sandbox workers, `space-builder` with node.role=builder and
# the chart's values-builder.yaml on the builders — mirroring production's
# per-service DaemonSets. They need no order: each service tolerates peers that
# are not ready yet and restarts on its own.
# SPACE_LOOP_DEVICE_TARGET sets the loop-device pool node-manager raises each
# node to once it is up; the node image stages the library's floor of 64 before
# that. Inherited so setting it for a bring-up reaches both node-manager
# renders; unset passes nothing and the default applies.
# shellcheck source=../lib/node-services.sh
. "$KIND_DIR/lib/node-services.sh"
while read -r release role service; do
  chart="$KIND_DIR/charts/space-$service-local"
  node_args=(--set "node.role=$role")
  if [[ "$role" == builder && -f "$chart/values-builder.yaml" ]]; then
    node_args+=(-f "$chart/values-builder.yaml")
  fi
  if [[ "$service" == node-manager && -n "${SPACE_LOOP_DEVICE_TARGET:-}" ]]; then
    node_args+=(--set "loopDeviceTarget=$SPACE_LOOP_DEVICE_TARGET")
  fi
  helm_apply "$release" "$chart" "${node_args[@]}"
done < <(space_kind_node_releases)
# Kind-only operator shell on the sandbox workers and builders.
helm_apply space "$KIND_DIR/charts/space-node-debug"

# Seed publish: one-shot Job that uploads the locally built production seed
# to the versitygw object store and registers the release through the real ClusterApi RPCs.
# Job specs are immutable, so drop any completed prior run before re-applying
# (the publish itself is idempotent).
kubectl --context "$KCTX" -n "$NAMESPACE" delete job space-seed-publish --ignore-not-found >/dev/null
helm_apply space "$KIND_DIR/charts/space-seed-publish"

# ── Step 8: readiness gates ──────────────────────────────────────────────────
log "waiting for space-admin-api Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-admin-api --timeout="${TIMEOUT}s"

log "waiting for metrics-server Deployment"
kubectl --context "$KCTX" -n kube-system rollout status deploy/metrics-server --timeout="${TIMEOUT}s"

log "waiting for space-internal-api Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-internal-api --timeout="${TIMEOUT}s"

log "waiting for space-kind-key-broker Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-kind-key-broker --timeout="${TIMEOUT}s"

log "waiting for space-credmgr-authority Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-credmgr-authority --timeout="${TIMEOUT}s"

log "waiting for space-data-plane Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-data-plane --timeout="${TIMEOUT}s"

log "waiting for space-cluster-controller Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-cluster-controller --timeout="${TIMEOUT}s"

log "waiting for space-ui Deployment"
kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-ui --timeout="${TIMEOUT}s"

log "waiting for space-object-store bucket-init Job"
kubectl --context "$KCTX" -n "$NAMESPACE" wait --for=condition=complete \
  job/space-object-store-bucket-init --timeout="${TIMEOUT}s" >/dev/null

log "waiting for space-seed-publish Job (uploads seed + registers the release)"
kubectl --context "$KCTX" -n "$NAMESPACE" wait --for=condition=complete \
  job/space-seed-publish --timeout="${TIMEOUT}s" >/dev/null

while read -r daemonset; do
  log "waiting for $daemonset DaemonSet to roll out"
  kubectl --context "$KCTX" -n "$NAMESPACE" rollout status \
    "ds/$daemonset" --timeout="${TIMEOUT}s" >/dev/null
done < <(space_kind_node_daemonsets)

# Ready pods are not a registered node: wait until each node's node-ctl has
# published it through ClusterApi and the API reports it schedulable.
log "waiting for every SPACE node to be published and ready"
SPACE_KIND_CLUSTER="$KIND_CLUSTER" SPACE_CONSOLE_PASSWORD="$CONSOLE_PASSWORD" \
  bash "$KIND_DIR/scripts/wait-node-publication.sh" "$TIMEOUT"
unset CONSOLE_PASSWORD

# SPACE is ready before optional telemetry is reconciled. Comparison commands
# still require successful Agent verification and never accept missing capture.
# ── Datadog ──────────────────────────────────────────────────────────────────
# All validation happens before cluster mutations above. No path creates,
# prints, or copies a SaaS key; both entry points consume the same renderer.
if [[ "$DATADOG_MODE" != off ]]; then
  log "Datadog $DATADOG_MODE mode enabled"
  log "helm repo add + dependency build for datadog-agent-local (pulls upstream chart)"
  helm repo add datadog https://helm.datadoghq.com --force-update >/dev/null
  helm dependency build "$KIND_DIR/charts/datadog-agent-local" >/dev/null
  python3 "$KIND_DIR/scripts/datadog_backend.py" apply
else
  log "Datadog installation skipped (existing Agents are unchanged). Use fakeintake for local capture or the backend helper's stop command for rollback."
fi

# ── Step 9: optional port-forwards ───────────────────────────────────────────
# The API and data plane need none: localhost:8080 and :8090 are the cluster's
# host port mappings onto their NodePort entry Services (kind-cluster.yaml.tmpl),
# which reach every replica and outlive any pod. The single-replica services
# below are still forwarded. Without supervision the smoke tests can't be
# re-run later in the session because those endpoints have silently gone away:
# kubectl port-forward dies on its own when its pod restarts. port-forward.sh
# restarts each forward whenever kubectl exits, and `setsid` lets it survive
# our parent shell and its tty and emits a pid we can later kill via PF_PIDFILE.
start_pf() {
  # Note: assigning all three on one `local` line evaluates `${target##*/}`
  # before `target=$1` lands in the local scope, which trips `set -u`.
  local target=$1
  local mapping=$2
  local namespace=${3:-$NAMESPACE}
  local name=${target##*/}
  log "port-forward $namespace/$target -> localhost:${mapping%%:*} (self-restarting)"
  # The trailing `space-kind-pf:<ctx>` argument is a label, not input (see
  # port-forward.sh).
  setsid bash "$KIND_DIR/scripts/port-forward.sh" \
    "$KCTX" "$namespace" "$target" "$mapping" "space-kind-pf:$KCTX" \
    >>/tmp/space-local-pf-"$name".log 2>&1 < /dev/null &
  echo "$!" >>"$PF_PIDFILE"
}

if (( PORT_FORWARD )); then
  # Reap the previous run's supervisors *before* forgetting their pids. This
  # file is the only record teardown has of them, so truncating first orphans
  # self-restarting loops that then hold the very ports the new forwards want —
  # and a later teardown, knowing only the new pids, cannot clean them up.
  # `kill -- -PID` because start_pf spawns each supervisor under `setsid`, making
  # it a process-group leader: the group kill takes the loop and its current
  # kubectl child together, so the loop cannot respawn one last time.
  if [[ -s "$PF_PIDFILE" ]]; then
    log "reaping port-forward supervisors from a previous run ($PF_PIDFILE)"
    while IFS= read -r stale_pid; do
      [[ "$stale_pid" =~ ^[0-9]+$ ]] || continue
      kill -- "-$stale_pid" 2>/dev/null || kill "$stale_pid" 2>/dev/null || true
    done <"$PF_PIDFILE"
  fi
  : >"$PF_PIDFILE"
  start_pf svc/space-ui         3000:3000
  # The object store's S3 API: the node-drain smoke asserts the evacuation's snapshot
  # artifacts through it, so a stack brought up this way has to expose it just
  # like the Tilt path does. Without it that suite fails after a successful
  # evacuation, blaming the data path for an absent port-forward.
  # The Service is `object-store` while its Deployment is `space-object-store`
  # (see the chart's `service.name`) — not a typo to "fix".
  start_pf svc/object-store     8333:8333
  if [[ "$DATADOG_MODE" == fakeintake ]]; then
    start_pf svc/datadog-fakeintake 8088:80 kube-system
  fi
  # The local observability backends exist only in otel mode. Each takes the
  # port its own `local.sh ui` helper uses; the Tiltfile forwards the same.
  if [[ "$TELEMETRY_MODE" == otel ]]; then
    start_pf svc/prometheus 19090:9090 space-metrics
    start_pf svc/tempo      13200:3200 space-traces
    start_pf svc/loki       13101:3100 space-logs
    start_pf svc/grafana    13000:3000 space-grafana
  fi

  log "port-forward pids in $PF_PIDFILE (kill them with: xargs -a $PF_PIDFILE kill -- ; pkill -f \"space-kind-pf:$KCTX|--context $KCTX.*port-forward\")"
fi

# Callers (e.g. the devbox kind command) run smokes against localhost:8080
# immediately after we return. Wait for the API through its entry, which
# serves whatever the port-forward choice.
log "waiting for the API entry on localhost:8080"
entry_ready=0
for ((i = 1; i <= 60; i++)); do
  if curl -fsS -m 2 http://localhost:8080/healthz >/dev/null 2>&1; then
    entry_ready=1
    break
  fi
  sleep 1
done
(( entry_ready )) || { log "ERROR: the API did not answer on localhost:8080 (its entry NodePort)"; exit 1; }

# ── Summary ──────────────────────────────────────────────────────────────────
cat <<MSG

[bootstrap] kind cluster '$KIND_CLUSTER' is up.

  API:        http://localhost:8080
  Data-plane: http://localhost:8090
  UI:         http://localhost:3000
  Datadog:    $DATADOG_MODE
  Telemetry:  $TELEMETRY_MODE
  Login:      root@local

  Smoke:      bash scripts/tests/smoke-kind-e2e.sh
  ECW build:  SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots
  Teardown:   bash $KIND_DIR/scripts/teardown.sh

MSG
