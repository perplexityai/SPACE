#!/usr/bin/env bash
# seed-node-containerd.sh — give each kind node's containerd storage the images
# its node image preloads, before `kind create` boots the node.
#
# A kind node image carries its add-on images already unpacked in
# /var/lib/containerd: kindnetd, local-path-provisioner and local-path-helper
# from Docker Hub, and the control-plane images from registry.k8s.io. The
# cluster config mounts a per-node host directory over that path
# (kind-cluster.yaml.tmpl), so a node created over an empty directory sees none
# of them and its kubelet pulls each one it runs. With Docker Hub unreachable,
# kindnet never starts and no node becomes Ready. This copies each node image's
# own /var/lib/containerd into the node's directory first, so the node boots
# with what its image carries and pulls nothing. Run it after the images the
# config names are local (`kind-node-image.sh ensure`); one that is not is
# pulled, as `kind create` would pull it next.
#
# Only an absent or empty directory is seeded. One that holds anything — a
# running cluster's state, or the containerd cache a deleted cluster left — is
# left exactly as it is. The copy runs in the node image as root
# (lib/seed-containerd-dir.sh): into a staging directory beside the target,
# renamed onto it only when complete and only while it is still empty, with
# seeds of one directory taking turns under a lock. So a seed that fails, is
# killed, or races another bring-up leaves the target as it found it, and the
# kubelets pull as they did before. Never fails the bring-up.
#
# The copy is the stock preload of the node image, about 430 MB per node for
# kindest/node v1.34.0, the same set on every node: the workers' copies include
# control-plane images they never run. Not hardlinks: each node's containerd
# rewrites its metadata database and snapshots in place, so linked files would
# share that state between nodes and with the image. Not reflinks: the source
# is Docker's image storage and the destination the kind data root, which are
# different filesystems on a typical host.
#
# Usage: seed-node-containerd.sh <rendered cluster config>
# Env:   SPACE_KIND_SEED_CONTAINERD=0 skips it.
#        SPACE_DOCKER_PULL_TIMEOUT_SECONDS bounds the pull of an absent image,
#        and the copy gets twice as long.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
# shellcheck source=../../../../ci/scripts/lib/test-deps.sh
source "$REPO_ROOT/ci/scripts/lib/test-deps.sh"
# shellcheck source=lib/kind-host-paths.sh
source "$SCRIPT_DIR/lib/kind-host-paths.sh"

log() { printf '[seed-node-containerd] %s\n' "$*" >&2; }

PULL_TIMEOUT_SECONDS="${SPACE_DOCKER_PULL_TIMEOUT_SECONDS:-300}"
COPY_SCRIPT="$(cat "$SCRIPT_DIR/lib/seed-containerd-dir.sh")"

seed() {
  local image="$1" dir="$2" started="$SECONDS" status=0
  if [[ -e "$dir" ]] && [[ ! -d "$dir" || -n "$(ls -A "$dir" 2>/dev/null || echo unreadable)" ]]; then
    log "$dir already holds containerd state; left as it is"
    return 0
  fi
  if [[ -z "$image" ]]; then
    log "WARNING: the config names no image for the node mounting $dir; it stays empty and the kubelet pulls"
    return 0
  fi
  if ! mkdir -p "$dir"; then
    log "WARNING: cannot create $dir; kind create reports it, and the kubelet pulls"
    return 0
  fi
  if ! docker image inspect "$image" >/dev/null 2>&1 &&
    ! space_with_deadline "$PULL_TIMEOUT_SECONDS" docker pull --quiet "$image" >/dev/null; then
    log "WARNING: $image is not local and could not be pulled; $dir stays empty and the kubelet pulls"
    return 0
  fi
  space_with_deadline $((PULL_TIMEOUT_SECONDS * 2)) \
    docker run --rm --privileged --network none \
    --volume "$(dirname "$dir"):/node" --env SEED_TARGET="$(basename "$dir")" \
    --entrypoint /bin/sh "$image" -c "$COPY_SCRIPT" >&2 || status=$?
  case "$status" in
    0) log "seeded $dir from $image in $((SECONDS - started)) s" ;;
    3) log "$dir was filled while it was being seeded; left as it is" ;;
    *) log "WARNING: could not seed $dir from $image (status $status); it is as it was, and the kubelet pulls" ;;
  esac
}

(( $# == 1 )) || { echo "usage: $0 <rendered cluster config>" >&2; exit 64; }
if [[ "${SPACE_KIND_SEED_CONTAINERD:-1}" == 0 ]]; then
  log "SPACE_KIND_SEED_CONTAINERD=0; the kubelets pull the node images' add-ons"
  exit 0
fi
seeded=0
while IFS=$'\t' read -r image container dir; do
  [[ "$container" == /var/lib/containerd && -n "$dir" ]] || continue
  seeded=$((seeded + 1))
  seed "$image" "$dir"
done < <(kind_config_node_mounts "$1")
if (( seeded == 0 )); then
  log "WARNING: $1 mounts no directory over /var/lib/containerd; nothing to seed"
fi
