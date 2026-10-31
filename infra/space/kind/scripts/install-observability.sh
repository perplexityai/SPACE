#!/usr/bin/env bash
# Reconcile the selected Kind profile without deleting backend data.
#
# Tiltfile evaluation calls this before producer changes. The local node-side
# endpoints use fixed ClusterIPs,
# so already-running exporters reconnect if a backend is later reinstalled.
#
# Each helper is idempotent and refuses any cluster but the selected bench.
set -Eeuo pipefail
readonly kind_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repo_root="$kind_dir/../../.."
readonly charts_dir="$kind_dir/../observability/charts"
log() { echo "[install-observability] $*"; }

# Run each queued command in the background, then print each one's output in
# queue order, so the log reads as if they ran one after another. Fails, after
# printing everything, when any of them failed. The mirror pulls, the image
# loads and the metrics and traces releases are each independent of the
# others; run one by one they took most of a CI shard's two-minute install.
parallel_queue=()
queue() { parallel_queue+=("$*"); }
run_queued() {
  local label=$1 dir status=0 i pids=()
  dir=$(mktemp -d)
  for i in "${!parallel_queue[@]}"; do
    bash -c "${parallel_queue[$i]}" >"$dir/$i" 2>&1 &
    pids+=("$!")
  done
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
      status=1
      echo "$dir/$i failed" >"$dir/$i.failed"
    fi
  done
  for i in "${!parallel_queue[@]}"; do
    cat "$dir/$i"
    [[ ! -e "$dir/$i.failed" ]] || log "$label failed: ${parallel_queue[$i]}"
  done
  rm -rf "$dir"
  parallel_queue=()
  return "$status"
}

# Put each backend image the charts pin onto every Kind node, under the tag
# the Kind profiles name, so that tag resolves to the pinned digest and the
# kubelets start the pods without a registry pull. GitHub CI
# (SPACE_CI_IMAGE_MIRROR set) takes each image from the GHCR mirror, falling
# back to its source; elsewhere the same linux/amd64 digest
# (ci/mirrored-images.txt) comes from its source. Never fails the install: an
# image neither gives is left to the kubelet's own pull of the tag, as before.
load_pinned_images() {
  local cluster="${SPACE_KIND_CLUSTER:-space-local}" pinned name tag listed digest source node
  local -A digests=() missing=()
  local nodes=()
  # repo:tag@sha256:<index>, exactly as the charts' values pin them.
  mapfile -t pinned < <(grep -hoE '[a-z0-9./-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}' \
    "$charts_dir"/{common,metrics,traces,logs,grafana}/values.yaml | sort -u)
  if [[ -n "${SPACE_CI_IMAGE_MIRROR:-}" ]]; then
    # The first pull logs in with GHCR_TOKEN when it is set; the rest reuse
    # that login, so no two `docker login`s write the Docker config at once.
    local first=1
    for name in "${pinned[@]}"; do
      if (( first )); then
        queue "$(printf '%q ' bash "$repo_root/ci/scripts/mirror-images.sh" pull "$name")"
        run_queued "mirror pull" ||
          echo "::warning::Could not take $name from the mirror; the kubelets pull it"
        first=0
      else
        queue "$(printf '%q ' env -u GHCR_TOKEN bash "$repo_root/ci/scripts/mirror-images.sh" pull "$name")"
      fi
    done
    (( ${#parallel_queue[@]} == 0 )) || run_queued "mirror pull" ||
      echo "::warning::Could not take the observability images from the mirror; the kubelets pull them"
  fi
  for name in "${pinned[@]}"; do
    tag="${name%%@*}"
    if ! listed="$(bash "$repo_root/ci/scripts/mirror-images.sh" pinned "$name")"; then
      echo "::warning::$name is not listed in ci/mirrored-images.txt; the kubelet pulls $tag itself"
      continue
    fi
    digest="${listed##*@}"
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
    queue "$(printf '%q ' kind load docker-image --name "$cluster" --nodes "${missing[$tag]%,}" "$tag")" \
      "|| echo $(printf '%q' "::warning::Could not load $tag into kind; the kubelets pull it")"
  done
  run_queued "image load" || true
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
  # The metrics and traces receivers are their own charts in their own
  # namespaces, so they install at once. Logs and then Grafana keep their
  # place after them.
  while IFS= read -r signal; do
    [[ -n $signal ]] || continue
    queue "$(printf '%q ' bash "$kind_dir/$signal/local.sh" up)"
    if [[ $signal == metrics ]]; then datasource=prometheus; else datasource=tempo; fi
    grafana+=(--set "datasources.$datasource.enabled=true")
  done <<<"$signals"
  (( ${#parallel_queue[@]} == 0 )) || run_queued "release"
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
