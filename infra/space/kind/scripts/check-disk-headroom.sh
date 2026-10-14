#!/usr/bin/env bash
# check-disk-headroom.sh — warn, without refusing, when the Docker data root or
# $SPACE_KIND_DATA_ROOT is short of free space before a bring-up.
#
# A bring-up writes to both: image builds and their BuildKit cache under the
# Docker data root, and a copy of every loaded image in each node's containerd
# store under $SPACE_KIND_DATA_ROOT. Either running out mid-bring-up surfaces
# as an image extraction or build failure far from the cause. The warning names
# the cleanup commands that cannot remove anything a later bring-up needs.
#
# Always exits 0. A Docker data root this host cannot see (a remote daemon) is
# not checked.
#
# Env knobs:
#   SPACE_KIND_DATA_ROOT     per-node containerd storage (default: ~/.kind-data)
#   SPACE_KIND_MIN_FREE_GIB  warn below this many GiB free (default: 30)
#   SPACE_KIND_CLUSTER       kind cluster name the commands name (default: space-local)
set -euo pipefail

min_gib="${SPACE_KIND_MIN_FREE_GIB:-30}"
data_root="${SPACE_KIND_DATA_ROOT:-$HOME/.kind-data}"
cluster="${SPACE_KIND_CLUSTER:-space-local}"
kind_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

warn() { printf '[disk-headroom] WARNING: %s\n' "$*" >&2; }
say() { printf '[disk-headroom]   %s\n' "$*" >&2; }

# Free KiB on the filesystem holding $1, measured at its nearest existing
# ancestor: the data root is created later in a first bring-up.
free_kib() {
  local path=$1
  while [[ ! -e "$path" && "$path" != / && "$path" != . ]]; do
    path=$(dirname -- "$path")
  done
  df -Pk -- "$path" 2>/dev/null | awk 'NR == 2 { print $4 }'
}

# Prints the warning header when $2 has less than the minimum free; returns 1
# when it has enough or cannot be measured.
low() {
  local what=$1 path=$2 kib
  kib="$(free_kib "$path")"
  [[ "$kib" =~ ^[0-9]+$ ]] || return 1
  (( kib < min_gib * 1024 * 1024 )) || return 1
  warn "$what ($path) has $(awk -v k="$kib" 'BEGIN { printf "%.1f", k / 1048576 }') GiB free, under ${min_gib} GiB."
}

docker_root="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
if [[ -n "$docker_root" && -e "$docker_root" ]] && low "the Docker data root" "$docker_root"; then
  say "Image builds may fail part-way. Safe to run:"
  say "  docker image prune -f"
  say "      removes dangling images only"
  say "docker builder prune frees the BuildKit cache as well, but that cache is host-global"
  say "and shared with other worktrees: run it only with their owners' agreement."
fi

if low "\$SPACE_KIND_DATA_ROOT" "$data_root"; then
  say "Every Kind node keeps its own copy of each loaded image there. Safe to run:"
  say "  python3 $kind_dir/scripts/prune-superseded-images.py --cluster $cluster"
  say "      with the cluster up, removes only superseded builds of SPACE's own images;"
  say "      bootstrap.sh and Tilt run it too unless SPACE_KIND_PRUNE_SUPERSEDED_IMAGES=0"
  say "  kind delete cluster --name $cluster && sudo rm -rf $data_root"
  say "      once no other cluster uses it: discards every node's images for the next"
  say "      bring-up to load again"
  say "Not 'crictl rmi --prune' in a node: it also removes SPACE images no container uses"
  say "at that moment, such as the seed publisher, and Kind cannot pull those back."
fi
