#!/usr/bin/env bash
# Install the local observability backends for this Kind bench.
#
# Both supported bring-up paths call this in otel mode: bootstrap.sh, and the
# Tiltfile's install-observability-backends resource. It may run before or
# after the shim and workloads: they address the Collectors by fixed ClusterIPs
# and retry exports until a receiver answers.
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

load_pinned_images
for signal in metrics traces logs grafana; do
  log "installing the $signal backend"
  bash "$kind_dir/$signal/local.sh" up
done
log "installed: metrics traces logs grafana"
