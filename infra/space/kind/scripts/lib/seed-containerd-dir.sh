# seed-containerd-dir.sh — the copy seed-node-containerd.sh runs as root inside
# a node image, with the directory that holds the node's containerd directory
# mounted at $SEED_PARENT. POSIX sh: the node image's /bin/sh.
#
# Copies $SEED_SOURCE (the image's preload) into a private staging directory
# beside the target and renames it onto the target only once it is complete,
# so the target is either untouched or whole. rename(2) onto a directory that
# is not empty fails, so a target another run filled in the meantime is left
# exactly as it is, and this run removes only its own staging directory.
# Seeds of one directory take turns under a lock, so a staging directory found
# while holding it belongs to a run that died, and is removed. Without
# `flock` ($SEED_FLOCK) they still cannot damage each other; dead runs'
# staging directories are then only reported.
#
# Exit status: 0 seeded; 3 the target holds state, so it was left as it is;
# 75 too little room; 76 another seed held the lock too long; 1 the copy
# failed. On every path but 0 the target is as this run found it.
set -u

source_dir="${SEED_SOURCE:-/var/lib/containerd}"
parent="${SEED_PARENT:-/node}"
name="${SEED_TARGET:-containerd}"
target="$parent/$name"
staging_prefix="$parent/.$name.seed."
stage=""

cleanup() {
  if [ -n "$stage" ]; then
    rm -rf "$stage"
  fi
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

holds_state() {
  [ -e "$1" ] && { [ ! -d "$1" ] || [ -n "$(ls -A "$1" 2>/dev/null || echo unreadable)" ]; }
}

if command -v "${SEED_FLOCK:-flock}" >/dev/null 2>&1; then
  exec 9>>"$parent/.$name.seed-lock"
  if ! "${SEED_FLOCK:-flock}" -w "${SEED_LOCK_WAIT_SECONDS:-600}" 9; then
    echo "another seed of $target held its lock for ${SEED_LOCK_WAIT_SECONDS:-600} s" >&2
    exit 76
  fi
  for dead in "$staging_prefix"*; do
    if [ -d "$dead" ]; then
      echo "removing $dead, left by a seed that did not finish" >&2
      rm -rf "$dead"
    fi
  done
else
  for dead in "$staging_prefix"*; do
    [ ! -d "$dead" ] || echo "without flock, leaving $dead, which may be another seed's" >&2
  done
fi

if holds_state "$target"; then
  exit 3
fi
reference="$target"
[ -d "$reference" ] || reference="$parent"
owner="$(stat -c %u:%g "$reference")"
mode="$(stat -c %a "$reference")"
need="$(du -sk "$source_dir" | cut -f1)"
free="$(df -Pk "$parent" | awk 'NR == 2 { print $4 }')"
if [ "$free" -lt $((need * 2)) ]; then
  echo "only ${free} KiB free for a ${need} KiB copy" >&2
  exit 75
fi

stage="$(mktemp -d "${staging_prefix}XXXXXX")" || exit 1
cp -a "$source_dir/." "$stage/" || exit 1
chown "$owner" "$stage" && chmod "$mode" "$stage" || exit 1
# The recheck: renamed only onto an absent or empty target.
if ! mv -T "$stage" "$target" 2>/dev/null; then
  holds_state "$target" && exit 3
  echo "could not rename $stage onto $target" >&2
  exit 1
fi
stage=""
