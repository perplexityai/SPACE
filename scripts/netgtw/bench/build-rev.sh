#!/usr/bin/env bash
# Build the bench harness against one git rev of the gateway library.
#
# The harness links `pplx-space-net-gateway`, so "before" and "after" are two
# builds of the same harness source over two revs of that library. The crate did
# not exist at older revs, so a detached worktree is checked out and today's
# crate is copied into it. Only the library differs between the two binaries;
# the workloads, the upstream, and the sampler are identical by construction.
#
# Usage: build-rev.sh <git-rev> <out-dir>
# Writes: <out-dir>/netgtw-bench-<short-sha>-<harness-hash>, reused if present.
#
# The name carries both halves of what the binary is built from. Keying the cache
# on the gateway rev alone would hand back a stale binary after an edit to the
# harness, so a comparison would silently mix two harness versions.
#
# The harness and the gateway service moved into the layer directories, the
# harness under a new package name, and not necessarily in the same change, so
# each is read off each tree on its own. This file names both layouts, which is
# why scripts/move_crates.toml keeps it out of the move's rewrite.
set -euo pipefail

REV=${1:?usage: build-rev.sh <git-rev> <out-dir>}
OUT_DIR=${2:?usage: build-rev.sh <git-rev> <out-dir>}

REPO=$(git rev-parse --show-toplevel)

die() { echo "ERROR: $*" >&2; exit 1; }

HARNESS_DIRS=(crates/tools/net/gateway/bench crates/apps/net/gateway/bench)
GATEWAY_MEMBERS=('"crates/l50_app/net/gateway/svc",' '"crates/apps/net/gateway/svc",')

# harness <tree>: the harness directory and package in that tree, or nothing.
harness() {
  local dir
  for dir in "${HARNESS_DIRS[@]}"; do
    if [[ -f "$1/$dir/Cargo.toml" ]]; then
      echo "$dir" "$(sed -n 's/^name = "\(.*\)"$/\1/p' "$1/$dir/Cargo.toml" | head -n 1)"
      return
    fi
  done
}

# gateway_member <tree>: the gateway service's workspace member line, which
# a copied-in harness is listed beside.
gateway_member() {
  local member
  for member in "${GATEWAY_MEMBERS[@]}"; do
    if grep -qF "$member" "$1/Cargo.toml"; then
      echo "$member"
      return
    fi
  done
}

read -r CRATE PACKAGE < <(harness "$REPO") || true
[[ -n "${CRATE:-}" ]] || die "no bench harness in $REPO (looked in ${HARNESS_DIRS[*]})"

SHA=$(git -C "$REPO" rev-parse "$REV") || die "unknown rev: $REV"
SHORT=$(git -C "$REPO" rev-parse --short "$SHA")

# Working-tree contents, not a git tree hash: uncommitted harness edits must
# invalidate the cache too.
HARNESS_HASH=$(cd "$REPO" && find "$CRATE" -type f \( -name '*.rs' -o -name 'Cargo.toml' \) \
  -print0 | sort -z | xargs -0 cat | sha256sum | cut -c1-8)

mkdir -p "$OUT_DIR"
BINARY="$OUT_DIR/netgtw-bench-$SHORT-$HARNESS_HASH"

# Stdout carries the path and nothing else; callers capture it.
if [[ -x "$BINARY" ]]; then
  echo "reusing the cached build of $SHORT (harness $HARNESS_HASH)" >&2
  echo "$BINARY"
  exit 0
fi

# Dependency artifacts are shared across revs; only the workspace crates rebuild.
export CARGO_TARGET_DIR="$OUT_DIR/target"

WORKTREE=$(mktemp -d "${TMPDIR:-/tmp}/netgtw-bench-$SHORT.XXXXXX")
cleanup() { git -C "$REPO" worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE"; }
trap cleanup EXIT

git -C "$REPO" worktree add --detach --quiet "$WORKTREE" "$SHA"

# The rev's own harness, if it has one; otherwise today's, copied in beside the
# rev's gateway service.
REV_CRATE='' REV_PACKAGE=''
read -r REV_CRATE REV_PACKAGE < <(harness "$WORKTREE") || true
if [[ -n "$REV_CRATE" ]]; then
  BUILD_PACKAGE=$REV_PACKAGE
else
  MEMBER_ANCHOR=$(gateway_member "$WORKTREE")
  [[ -n "$MEMBER_ANCHOR" ]] ||
    die "cannot add the bench crate to $SHORT: workspace members list none of ${GATEWAY_MEMBERS[*]}"
  mkdir -p "$(dirname "$WORKTREE/$CRATE")"
  cp -R "$REPO/$CRATE" "$WORKTREE/$CRATE"
  sed -i "s|$MEMBER_ANCHOR|\"$CRATE\",\n    $MEMBER_ANCHOR|" "$WORKTREE/Cargo.toml"
  BUILD_PACKAGE=$PACKAGE
fi

echo "building $SHORT ($(git -C "$REPO" log -1 --format=%s "$SHA" | cut -c1-60))" >&2
cargo build --manifest-path "$WORKTREE/Cargo.toml" --release -p "$BUILD_PACKAGE" >&2
cp "$CARGO_TARGET_DIR/release/netgtw-bench" "$BINARY"
echo "$BINARY"
