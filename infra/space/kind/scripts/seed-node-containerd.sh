#!/usr/bin/env bash
# seed-node-containerd.sh — give each kind node's containerd storage the images
# its node image preloads, before `kind create` boots the node.
#
# A kind node image carries its add-on images already unpacked in
# /var/lib/containerd: kindnetd, local-path-provisioner and local-path-helper
# from Docker Hub, and the control-plane images from registry.k8s.io. The
# cluster config mounts a per-node host directory over that path
# (kind-cluster.yaml.tmpl), so a node created over an empty directory sees none
# of them and its kubelet pulls each again. With Docker Hub unreachable, kindnet
# never starts and no node becomes Ready. This copies each node image's own
# /var/lib/containerd into the node's directory first, so the node boots with
# what its image carries and pulls nothing. Run it after the images the config
# names are local (`kind-node-image.sh ensure`); one that is not is pulled, as
# `kind create` would pull it next.
#
# Only an absent or empty directory is seeded. One that holds anything — a
# running cluster's state, or the containerd cache a deleted cluster left — is
# left exactly as it is. A copy that fails, or that the directory's filesystem
# has no room for, is removed again, so the directory stays empty and the
# kubelets pull as they did before. Never fails the bring-up.
#
# A copy, about 430 MB per node for kindest/node v1.34.0, of what the nodes
# would otherwise pull into the same directories. Not hardlinks: each node's
# containerd rewrites its metadata database and snapshots in place, so linked
# files would share that state between nodes and with the image. Not reflinks:
# the source is Docker's image storage and the destination the kind data root,
# which are different filesystems on a typical host.
#
# Usage: seed-node-containerd.sh <rendered cluster config>
# Env:   SPACE_KIND_SEED_CONTAINERD=0 skips it.
#        SPACE_DOCKER_PULL_TIMEOUT_SECONDS bounds the pull of an absent image.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
# shellcheck source=../../../../ci/scripts/lib/test-deps.sh
source "$REPO_ROOT/ci/scripts/lib/test-deps.sh"

log() { printf '[seed-node-containerd] %s\n' "$*" >&2; }

# "<image>\t<host directory>" for each node that mounts one over
# /var/lib/containerd. Same one-key-per-line shape as kind_config_mounts.
node_containerd_dirs() {
  sed -e 's/[[:space:]]\{1,\}#.*$//' "$1" \
    | awk -F': *' '
        /^[[:space:]]*-[[:space:]]*role:/                 { image = ""; host = ""; container = "" }
        /^[[:space:]]*image:/ {
          # The whole value: an image reference has colons of its own.
          image = $0
          sub(/^[[:space:]]*image:[[:space:]]*/, "", image)
        }
        /^[[:space:]]*-?[[:space:]]*hostPath:/            { host = $2 }
        /^[[:space:]]*-?[[:space:]]*containerPath:/       { container = $2 }
        host != "" && container != "" {
          if (container == "/var/lib/containerd") print image "\t" host
          host = ""
          container = ""
        }
      ' \
    | sed -e 's/"//g' -e "s/'//g"
}

# Copy the image's /var/lib/containerd into /seed and give /seed back the
# owner and mode it had, which the bring-up's mount preflight expects. Runs in
# the node image as root, privileged like the node itself, so the copy keeps
# every owner, device node and extended attribute the snapshots hold. Refuses
# unless the filesystem has room for the copy and as much again for the
# node's own later pulls; any failure empties /seed again.
# shellcheck disable=SC2016 # expanded by the container's shell
SEED_SCRIPT='
set -u
owner="$(stat -c %u:%g /seed)"
mode="$(stat -c %a /seed)"
need="$(du -sk /var/lib/containerd | cut -f1)"
free="$(df -Pk /seed | awk "NR == 2 { print \$4 }")"
if [ "$free" -lt $((need * 2)) ]; then
  echo "only ${free} KiB free for a ${need} KiB copy" >&2
  exit 75
fi
if cp -a /var/lib/containerd/. /seed/ && chown "$owner" /seed && chmod "$mode" /seed; then
  exit 0
fi
find /seed -mindepth 1 -delete
chown "$owner" /seed
chmod "$mode" /seed
exit 1
'

seed() {
  local image="$1" dir="$2" started="$SECONDS"
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
    ! space_with_deadline "${SPACE_DOCKER_PULL_TIMEOUT_SECONDS:-300}" docker pull --quiet "$image" >/dev/null; then
    log "WARNING: $image is not local and could not be pulled; $dir stays empty and the kubelet pulls"
    return 0
  fi
  if docker run --rm --privileged --network none --volume "$dir:/seed" \
    --entrypoint /bin/sh "$image" -c "$SEED_SCRIPT" >&2; then
    log "seeded $dir from $image in $((SECONDS - started)) s"
  else
    log "WARNING: could not seed $dir from $image; it stays empty and the kubelet pulls"
  fi
}

(( $# == 1 )) || { echo "usage: $0 <rendered cluster config>" >&2; exit 64; }
if [[ "${SPACE_KIND_SEED_CONTAINERD:-1}" == 0 ]]; then
  log "SPACE_KIND_SEED_CONTAINERD=0; the kubelets pull the node images' add-ons"
  exit 0
fi
while IFS=$'\t' read -r image dir; do
  [[ -n "$dir" ]] || continue
  seed "$image" "$dir"
done < <(node_containerd_dirs "$1")
