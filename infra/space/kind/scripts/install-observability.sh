#!/usr/bin/env bash
# Reconcile the selected Kind profile without deleting backend data.
#
# Both supported bring-up paths call this before producer changes: bootstrap.sh
# and Tiltfile evaluation. The local node-side endpoints use fixed ClusterIPs,
# so already-running exporters reconnect if a backend is later reinstalled.
#
# Each helper is idempotent and refuses any cluster but the selected bench.
set -Eeuo pipefail
readonly kind_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repo_root="$kind_dir/../../.."
readonly charts_dir="$kind_dir/../observability/charts"
log() { echo "[install-observability] $*"; }

# Put each backend image the charts pin onto every Kind node, under the tag
# the Kind profiles name, so that tag resolves to the pinned digest and the
# kubelets start the pods without a registry pull. GitHub CI
# (SPACE_CI_IMAGE_MIRROR set) takes each image from the GHCR mirror, falling
# back to its source; elsewhere the same linux/amd64 digest
# (ci/mirrored-images.txt) comes from its source. Never fails the install: an
# image neither gives is left to the kubelet's own pull of the tag, as before.
load_pinned_images() {
  local cluster="${SPACE_KIND_CLUSTER:-space-local}" pinned name tag digest source node
  local -A digests=() missing=()
  local nodes=()
  # repo:tag@sha256:<index>, exactly as the charts' values pin them.
  mapfile -t pinned < <(grep -hoE '[a-z0-9./-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}' \
    "$charts_dir"/{common,metrics,traces,logs,grafana}/values.yaml | sort -u)
  if [[ -n "${SPACE_CI_IMAGE_MIRROR:-}" ]]; then
    bash "$repo_root/ci/scripts/mirror-images.sh" pull "${pinned[@]}" ||
      echo "::warning::Could not take the observability images from the mirror; the kubelets pull them"
  fi
  for name in "${pinned[@]}"; do
    tag="${name%%@*}"
    digest="$(awk -v name="$name" '$1 == name { print $2; exit }' "$repo_root/ci/mirrored-images.txt")"
    if [[ -z "$digest" ]]; then
      echo "::warning::$name is not listed in ci/mirrored-images.txt; the kubelet pulls $tag itself"
      continue
    fi
    if [[ -z "${SPACE_CI_IMAGE_MIRROR:-}" ]]; then
      source="${tag%:*}@$digest"
      if docker image inspect "$source" >/dev/null 2>&1 || docker pull --quiet "$source" >/dev/null; then
        docker tag "$source" "$tag"
      fi
    fi
    if docker image inspect "$tag" >/dev/null 2>&1; then
      digests[$tag]="$digest"
    else
      echo "::warning::Could not pull $tag at $digest; the kubelet pulls it itself"
    fi
  done
  # Every node: Alloy is a DaemonSet, and the other backends carry no node
  # selector. `kind load` cannot tell that a node already has an image when
  # Docker uses the containerd image store, so ask each node's containerd
  # which digest the tag names and load only what is absent or different.
  # The pinned images are Docker Hub's, which containerd names under docker.io.
  mapfile -t nodes < <(kind get nodes --name "$cluster" 2>/dev/null)
  for node in "${nodes[@]}"; do
    for tag in "${!digests[@]}"; do
      [[ "$(docker exec "$node" ctr -n k8s.io images ls "name==docker.io/$tag" 2>/dev/null |
        awk 'NR == 2 { print $3 }')" == "${digests[$tag]}" ]] || missing[$tag]+="$node,"
    done
  done
  for tag in "${!missing[@]}"; do
    if ! kind load docker-image --name "$cluster" --nodes "${missing[$tag]%,}" "$tag"; then
      echo "::warning::Could not load $tag into kind; the kubelets pull it"
    fi
  done
}

plan=$(python3 "$kind_dir/scripts/telemetry_config.py" json)
field() { python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1" <<<"$plan"; }
source "$kind_dir/lib/release.sh"
cluster=$(field kind_cluster)
control_plane=$cluster-control-plane
context=kind-$cluster
kube=(kubectl --context "$context")
guard_cluster
python3 "$kind_dir/scripts/datadog_backend.py" guard-owner
# Refuse an unrelated log namespace/release before installing any receiver.
bash "$kind_dir/logs/local.sh" preflight

if [[ $(field log_route) != alloy ]]; then
  # A missing owned log namespace is a no-op, not permission to create one.
  bash "$kind_dir/logs/local.sh" pause
  log 'native profile: owned Alloy paused; backends and data retained'
elif [[ $(field install_observability) == True ]]; then
  load_pinned_images
  signals=$(python3 -c 'import json,sys; print("\n".join(json.load(sys.stdin)["local_signals"]))' <<<"$plan")
  grafana=(--set datasources.prometheus.enabled=false --set datasources.tempo.enabled=false)
  while IFS= read -r signal; do
    [[ -n $signal ]] || continue
    bash "$kind_dir/$signal/local.sh" up
    if [[ $signal == metrics ]]; then datasource=prometheus; else datasource=tempo; fi
    grafana+=(--set "datasources.$datasource.enabled=true")
  done <<<"$signals"
  bash "$kind_dir/logs/local.sh" up
  # External OTLP ingestion URLs are not Prometheus/Tempo query URLs.
  bash "$kind_dir/grafana/local.sh" up "${grafana[@]}"
  log 'OSS profile: required local receivers and local Alloy/Loki/Grafana ready'
else
  # External metrics/traces do not supply a log backend. Require the explicit
  # owned local log route before allowing producers to change profile.
  bash "$kind_dir/logs/local.sh" check
  # Fixed node-side addresses no longer require endpoint discovery. Reuse-only
  # mode still requires each implicit local receiver at its configured address;
  # the legacy host-only override does not replace application receivers.
  python3 - "$kind_dir/scripts" "$plan" <<'PY'
import json, subprocess, sys
sys.path.insert(0, sys.argv[1])
from telemetry_config import NODE_COLLECTORS
config = json.loads(sys.argv[2])
scope = ["kubectl", "--context", f"kind-{config['kind_cluster']}"]
for signal in config["local_signals"]:
    namespace = f"space-{signal}"
    owner = subprocess.check_output(
        [*scope, "get", "namespace", namespace, "-o", "jsonpath={.metadata.labels.space-local-owner}"],
        text=True,
    ).strip()
    address = subprocess.check_output(
        [*scope, "-n", namespace, "get", "svc", "collector", "-o", "jsonpath={.spec.clusterIP}"],
        text=True,
    ).strip()
    if owner != signal or address != NODE_COLLECTORS[signal]:
        sys.exit(f"Refusing unowned or incorrectly addressed local {signal} receiver")
PY
  bash "$kind_dir/logs/local.sh" resume
  log 'OSS profile: existing receivers and owned local Alloy/Loki reused'
fi
