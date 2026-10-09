#!/usr/bin/env bash
# install-shim.sh — build `containerd-shim-space-v2` and wire it into each
# kind worker's containerd, so per-sandbox pods scheduled with
# `runtimeClassName: space` get hijacked by the SPACE shim (which delegates
# to sandbox-manager → node-manager → guest runtime) instead of running as
# a plain runc container.
#
# Without this script, RuntimeClass `space` is unresolvable and every
# per-sandbox pod fails to start with "runtime handler not configured".
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
#   SPACE_KIND_TELEMETRY_MODE otel (default), datadog, or journald
#   SPACE_KIND_SHIM_OTLP_ENDPOINT OTLP/gRPC trace endpoint visible from the
#                         kind node (otel default: traces Collector ClusterIP;
#                         other modes retain http://127.0.0.1:4317)
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
# In otel mode, resolve space-traces/collector through kubectl when rendering
# the wrapper; the node can reach its ClusterIP but cannot resolve cluster DNS.
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

# ── 3. Install the shim binary + containerd config on one kind node ───────────
install_on_node() {
  local node=$1
  log "installing shim on $node"
  local prior_config
  prior_config=$(docker exec "$node" sha256sum /etc/containerd/config.toml)

  docker exec "$node" mkdir -p "$(dirname "$SHIM_REAL_BIN")"
  docker cp "$SHIM_BIN" "$node:${SHIM_REAL_BIN}.tmp"
  docker exec "$node" sh -c \
    "chown root:root '${SHIM_REAL_BIN}.tmp' && chmod 0755 '${SHIM_REAL_BIN}.tmp' && mv -f '${SHIM_REAL_BIN}.tmp' '$SHIM_REAL_BIN'"
  docker cp "$SHIM_WRAPPER" "$node:/usr/local/bin/.containerd-shim-space-v2.tmp"
  docker exec "$node" sh -c \
    "chown root:root /usr/local/bin/.containerd-shim-space-v2.tmp && chmod 0755 /usr/local/bin/.containerd-shim-space-v2.tmp && mv -f /usr/local/bin/.containerd-shim-space-v2.tmp /usr/local/bin/containerd-shim-space-v2"

  # Add the `space` CRI runtime to containerd's config if not already there.
  # The runtime_type is the shim's protocol name — kind's containerd looks for
  # /usr/local/bin/containerd-shim-<basename-with-dashes>-<version>. The bash
  # wrapper installed above now occupies that path and execs the compiled
  # binary from /usr/local/lib/space.
  # NOTE: heredoc delimiter is QUOTED ('"'"'EOF'"'"') so sh doesn't run command
  # substitution on backticks inside the comment block (a previous version
  # without quotes had sh trying to exec the literal word "space").
  #
  # pod_annotations is the allowlist containerd uses to decide which K8s pod
  # annotations get propagated into the OCI runtime spec. The SPACE shim
  # reads its instructions (sandbox-id, owner, vcpu, memory, template, etc.)
  # off the OCI spec, so without this allowlist every pod create fails with
  # "space.pplx.net/sandbox-owner or space.pplx.net/sandbox-user is required".
  # `space.pplx.net/*` matches every SPACE annotation in one rule.
  docker exec "$node" sh -c '
    cfg=/etc/containerd/config.toml
    grep -q "io.containerd.space.v2" "$cfg" && exit 0
    cat >>"$cfg" <<'"'"'EOF'"'"'

# Added by infra/space/kind/scripts/install-shim.sh — kind-only.
# Provides RuntimeClass "space" -> containerd-shim-space-v2, which bridges
# kubelet pod-start into the per-node sandbox-manager UDS so per-sandbox
# pods run as real SPACE sandboxes (guest VM via node-manager) instead of
# plain runc containers.
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.space]
  runtime_type = "io.containerd.space.v2"
  pod_annotations = ["space.pplx.net/*"]
EOF
  '

  # Restart containerd inside the kind node so it picks up the new runtime.
  # kind nodes run systemd, so the standard systemctl path works. (We
  # tolerate failure here — on the first install the kubelet briefly loses
  # the apiserver socket; it reconnects within a few seconds.)
  if [[ "$(docker exec "$node" sha256sum /etc/containerd/config.toml)" != "$prior_config" ]]; then
    docker exec "$node" systemctl restart containerd
  else
    log "containerd configuration unchanged; new sandbox shims use the updated binary"
  fi
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
