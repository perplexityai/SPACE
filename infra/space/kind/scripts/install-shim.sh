#!/usr/bin/env bash
# install-shim.sh — build `containerd-shim-space-v2` and install it on each
# kind sandbox worker, so per-sandbox pods scheduled with
# `runtimeClassName: space` get hijacked by the SPACE shim (which delegates
# to sandbox-manager → node-manager → guest runtime) instead of running as
# a plain runc container. The node image registers the runtime with
# containerd; this installs the binary containerd runs for it.
#
# Without the shim, every per-sandbox pod that RuntimeClass `space` routes to
# it fails to start.
#
# Idempotent. Safe to re-run after a kind cluster recreate.
#
# Usage:
#   bash infra/space/kind/scripts/install-shim.sh
#
# Env knobs:
#   SPACE_KIND_CLUSTER   kind cluster name (default: space-local)
#   SPACE_SHIM_BIN       prebuilt shim path (default: build from ./shim with go)
#   GO_BIN               go binary to use when building (default: PATH or /usr/local/go/bin/go)
#   SPACE_KIND_TELEMETRY_MODE datadog (default), otel, or off
#   SPACE_KIND_SHIM_OTLP_ENDPOINT OTLP/gRPC trace endpoint visible from the
#                         kind node (otel default: the traces Collector's fixed
#                         ClusterIP; other modes retain http://127.0.0.1:4317);
#                         the host runtime-supervisor follows it too
#   SPACE_KIND_SHIM_ENV   trace environment identity (default: local-<hostname>)
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SHIM_BIN="${SPACE_SHIM_BIN:-$REPO_ROOT/.cache/containerd-shim-space-v2}"
GO_BIN="${GO_BIN:-$(command -v go || true)}"
[[ -z "$GO_BIN" && -x /usr/local/go/bin/go ]] && GO_BIN=/usr/local/go/bin/go
SHIM_REAL_BIN=/usr/local/lib/space/containerd-shim-space-v2

log() { printf '[install-shim] %s\n' "$*"; }
die() { printf '[install-shim] ERROR: %s\n' "$*" >&2; exit 1; }

TELEMETRY_SCRIPT="$REPO_ROOT/infra/space/kind/scripts/telemetry_config.py"
python3 "$TELEMETRY_SCRIPT" json >/dev/null

# ── 1. Build the shim unless a current build is already cached ───────────────
# The cached binary survives branch switches and cluster teardowns, so "a
# binary exists" is not "the binary matches the tree". A stale shim poisons
# the whole bench: every sandbox pod fails at FailedCreatePodSandBox with
# sbx-manager rejections that read like product bugs (missing Wake fields,
# incomplete route identity), not like a harness problem. Rebuild whenever any
# shim/ source is newer than the cached binary — go's build cache makes the
# no-change rebuild cheap. A caller-provided SPACE_SHIM_BIN is trusted as-is;
# there is no source tree to compare a prebuilt binary against.
build_shim() {
  if [[ -x "$SHIM_BIN" ]]; then
    if [[ -n "${SPACE_SHIM_BIN:-}" ]]; then
      log "using prebuilt shim at $SHIM_BIN"
      return
    fi
    if [[ -z "$(find "$REPO_ROOT/shim" -type f -newer "$SHIM_BIN" -print -quit)" ]]; then
      log "cached shim at $SHIM_BIN is current (no shim/ source is newer)"
      return
    fi
    log "cached shim at $SHIM_BIN is stale (shim/ sources changed); rebuilding"
  fi
  [[ -n "$GO_BIN" && -x "$GO_BIN" ]] \
    || die "go not found; install with infra/space/kind/scripts/install-prereqs.sh"
  mkdir -p "$(dirname "$SHIM_BIN")"
  log "building shim with $GO_BIN ($(${GO_BIN} version))"
  (
    cd "$REPO_ROOT/shim"
    # Fetch the modules first, retrying a dropped download: a failed build
    # cannot say whether the proxy or the code was at fault.
    bash "$REPO_ROOT/ci/scripts/go-mod-download.sh" "$GO_BIN"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 "$GO_BIN" build \
      -trimpath \
      -o "$SHIM_BIN" \
      .
  )
  log "built $SHIM_BIN ($(wc -c <"$SHIM_BIN") bytes)"
}

# containerd launches the shim outside Kubernetes, so pod environment settings
# do not reach it. Keep the expected binary name as a small wrapper and install
# the compiled shim separately. This mirrors the production worker layout and
# makes the Kind telemetry selector apply to both Rust services and the Go shim.
# In otel mode the wrapper names each Collector by the fixed ClusterIP its Kind
# profile pins: the node can reach a ClusterIP but cannot resolve cluster DNS.
render_wrapper() {
  local output=$1 shim_version=$2
  python3 "$TELEMETRY_SCRIPT" shim-wrapper --version "$shim_version" \
    --binary "$SHIM_REAL_BIN" >"$output"
  chmod 0755 "$output"
}

# ── 2. Discover kind worker nodes (sandbox pool only) ────────────────────────
list_kind_workers() {
  # Sandbox workers are labeled `space.pplx.net/node-pool=sandbox` in
  # kind-cluster.yaml.tmpl. The shim only needs to run on these — sandbox pods
  # never schedule on the infra worker or the control-plane node.
  docker ps --format '{{.Names}}' --filter "label=io.x-k8s.kind.cluster=$KIND_CLUSTER" \
    | while read -r name; do
        role=$(docker inspect -f '{{ index .Config.Labels "io.x-k8s.kind.role" }}' "$name" 2>/dev/null)
        pool=$(docker exec "$name" cat /run/kind/node-config.yaml 2>/dev/null \
          | awk '/^[[:space:]]*space\.pplx\.net\/node-pool:/ { gsub(/"/,"",$NF); print $NF }')
        if [[ "$role" == "worker" && "$pool" == "sandbox" ]]; then
          echo "$name"
        fi
      done
}

# Fallback: kind doesn't always copy our labels into /run/kind/node-config.yaml,
# so also list any container whose name carries the cluster prefix and isn't
# the control-plane or dedicated builder nodes.
list_workers_fallback() {
  docker ps --format '{{.Names}}' \
    | grep -E "^${KIND_CLUSTER}-worker[0-9]*$" \
    | while read -r name; do
        # worker3 is infra; worker4 and worker5 are builders in
        # kind-cluster.yaml.tmpl. None accepts KubernetesSubstrate sandbox pods.
        if [[ "$name" == "${KIND_CLUSTER}-worker3" || \
              "$name" == "${KIND_CLUSTER}-worker4" || \
              "$name" == "${KIND_CLUSTER}-worker5" ]]; then
          continue
        fi
        echo "$name"
      done
}

# ── 3. Install the shim binary on one kind node ─────────────────────────────
# The `space` runtime itself is registered by the SPACE Kind node image
# (infra/space/kind/node/containerd/space-runtime.toml), under containerd's CRI
# v1 runtime plugin as a deployed node registers it, so containerd has it from
# its first start and is never edited or restarted here. containerd resolves
# /usr/local/bin/containerd-shim-space-v2 when each sandbox pod starts, so a
# replaced binary takes effect for the next sandbox.
install_on_node() {
  local node=$1
  log "installing shim on $node"
  docker exec "$node" containerd config dump 2>/dev/null \
    | grep -F "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.space]" >/dev/null \
    || die "$node's containerd has no 'space' runtime; the node predates the SPACE Kind node image that registers it, so recreate the cluster (scripts/check-kind-node-image.sh)"

  docker exec "$node" mkdir -p "$(dirname "$SHIM_REAL_BIN")"
  install_file "$node" "$SHIM_BIN" "$SHIM_REAL_BIN"
  install_file "$node" "$SHIM_WRAPPER" /usr/local/bin/containerd-shim-space-v2
}

# Copy beside the destination, check the copy against the source's SHA-256 (as
# a deployed node's bootstrap checks the shim it downloads), then rename it
# into place.
install_file() {
  local node=$1 source=$2 dest=$3 want got
  local staged
  staged="$(dirname "$dest")/.$(basename "$dest").tmp"
  want="$(sha256sum "$source" | cut -d' ' -f1)"
  docker cp "$source" "$node:$staged"
  got="$(docker exec "$node" sha256sum "$staged" | cut -d' ' -f1)"
  if [[ "$got" != "$want" ]]; then
    docker exec "$node" rm -f "$staged" || true
    die "$dest on $node: copied file has SHA-256 $got, expected $want"
  fi
  docker exec "$node" sh -c "chown root:root '$staged' && chmod 0755 '$staged' && mv -f '$staged' '$dest'"
}

# ── Driver ───────────────────────────────────────────────────────────────────
build_shim
SHIM_VERSION="$(sha256sum "$SHIM_BIN" | cut -c1-12)"
SHIM_WRAPPER="$(mktemp)"
trap 'rm -f "$SHIM_WRAPPER"' EXIT
render_wrapper "$SHIM_WRAPPER" "$SHIM_VERSION"

# Refuse to operate against a missing cluster.
if ! docker ps --format '{{.Names}}' --filter "label=io.x-k8s.kind.cluster=$KIND_CLUSTER" \
     | grep -q .; then
  die "no kind containers labelled cluster=$KIND_CLUSTER; bring it up first with bootstrap.sh"
fi

workers=$(list_kind_workers)
if [[ -z "$workers" ]]; then
  log "label-based discovery returned no sandbox workers; falling back to name match"
  workers=$(list_workers_fallback)
fi
[[ -n "$workers" ]] || die "could not identify any sandbox worker kind nodes"

for node in $workers; do
  install_on_node "$node"
done

log "shim installed on: $(echo "$workers" | tr '\n' ' ')"
log "shim telemetry: $(python3 "$TELEMETRY_SCRIPT" json)"
log "next: ensure RuntimeClass 'space' uses handler 'space' (manifests/runtimeclass.yaml)"
