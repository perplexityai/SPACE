#!/usr/bin/env bash
# check-kind-node-image.sh — refuse a kind cluster whose SPACE nodes do not run
# the SPACE Kind node image this tree builds.
#
# Sandbox workers and builders get their node layer (btrfs /space/cache,
# cgroup subtree, loop devices, device permissions, /space skeleton) from that
# image, baked in at build time the way a production node image bakes it
# (infra/space/kind/node/Dockerfile). Docker fixes a container's image when it
# creates the container, so a cluster created before the image existed, or from
# an older build of it, keeps its node layer for its whole life: the node-stack
# pods would wait for a btrfs /space/cache that never appears, or run on a layer
# the tree no longer describes. As with a production node image, the remedy is
# to replace the nodes; for kind, that is recreating the cluster. Each node's
# image carries the tag of the inputs it was built from as the label
# org.opencontainers.image.version, which containers inherit.
#
# Read-only. Exits 0 when the cluster does not exist or every SPACE node runs
# this tree's image, 1 (with the remedy on stderr) otherwise or when the
# cluster cannot be read.
#
# Usage: check-kind-node-image.sh [cluster]   (default: $SPACE_KIND_CLUSTER or space-local)
set -euo pipefail

KIND_CLUSTER="${1:-${SPACE_KIND_CLUSTER:-space-local}}"
KCTX="kind-$KIND_CLUSTER"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { printf '[check-kind-node-image] ERROR: %s\n' "$*" >&2; exit 1; }

kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || exit 0

expected_ref="$(bash "$SCRIPT_DIR/kind-node-image.sh" ref)"
expected_tag="${expected_ref##*:}"

# Pool labels are applied by each kubelet as it registers.
nodes=""
for ((attempt = 1; attempt <= 15; attempt++)); do
  nodes="$(kubectl --context "$KCTX" get nodes \
    -l 'space.pplx.net/node-pool in (sandbox,builder)' \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  [[ -z "$nodes" ]] || break
  sleep 2
done
[[ -n "$nodes" ]] || die "no node labelled space.pplx.net/node-pool=sandbox or =builder in $KCTX"

stale=()
for node in $nodes; do
  read -r title tag < <(docker inspect "$node" --format \
    '{{index .Config.Labels "org.opencontainers.image.title"}} {{index .Config.Labels "org.opencontainers.image.version"}}' \
    2>/dev/null || echo "missing missing")
  if [[ "$title" != space-kind-node ]]; then
    stale+=("$node runs $(docker inspect "$node" --format '{{.Config.Image}}' 2>/dev/null || echo 'an unknown image'), not the SPACE Kind node image")
  elif [[ "$tag" != "$expected_tag" ]]; then
    stale+=("$node runs SPACE Kind node image $tag; this tree builds $expected_tag")
  fi
done

if (( ${#stale[@]} > 0 )); then
  {
    printf '[check-kind-node-image] ERROR: kind cluster %s has SPACE nodes from another node image:\n' "$KIND_CLUSTER"
    printf '       %s\n' "${stale[@]}"
    printf '       A node gets its node layer (btrfs /space/cache, cgroup subtree, loop\n'
    printf '       devices, /space skeleton) from its image, fixed when kind creates it,\n'
    printf '       just as a production node gets it from its node image. Recreate the\n'
    printf '       cluster to roll the nodes onto %s:\n' "$expected_ref"
    printf '           kind delete cluster --name %s\n' "$KIND_CLUSTER"
    printf '       then re-run the bring-up. The delete keeps the containerd image caches\n'
    printf '       on disk, so the rebuild does not re-pull. See "The node layer" in\n'
    printf '       infra/space/kind/README.md.\n'
  } >&2
  exit 1
fi
printf '[check-kind-node-image] %s: SPACE nodes run %s\n' "$KIND_CLUSTER" "$expected_ref"
