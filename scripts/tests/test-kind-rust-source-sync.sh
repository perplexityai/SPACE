#!/usr/bin/env bash
# infra/space/kind/scripts/sync-rust-sources.py keeps the copy of the Rust build
# inputs that the incremental artifact build compiles from. Cargo judges a
# workspace crate fresh by source mtime, so the copy must rewrite exactly the
# files whose content changed and leave every other mtime alone — a fresh
# worktree of the same commit otherwise recompiled every workspace crate
# (2026-09-23 bench). A normal bring-up never exercises deletions, links or mode
# changes, so these cases pin them:
#
#   1. The first sync copies everything; syncing a fresh checkout of the same
#      content writes nothing and keeps the mirror's mtimes.
#   2. An edited file is rewritten with the time of the sync as its mtime —
#      never the checkout's, which can be older than the last build (`git
#      archive`, `cp -p`) and would pass for fresh; its neighbours are untouched.
#   3. A file or directory gone from the checkout is gone from the mirror.
#   4. An executable-bit change is carried over; a symlink stays a symlink.
#   5. A missing input fails the sync instead of leaving a partial copy.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sync="$repo_root/infra/space/kind/scripts/sync-rust-sources.py"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
status=0
fail() {
    echo "FAIL: $1" >&2
    [[ -z "${2:-}" ]] || printf '%s\n' "$2" | sed 's/^/    /' >&2
    status=1
}
mtime() { python3 -c 'import os, sys; print(os.lstat(sys.argv[1]).st_mtime_ns)' "$1"; }

make_checkout() { # dir
    mkdir -p "$1/crates/a/src" "$1/crates/b/src" "$1/proto"
    printf '[workspace]\n' >"$1/Cargo.toml"
    printf 'fn a() {}\n' >"$1/crates/a/src/lib.rs"
    printf 'fn b() {}\n' >"$1/crates/b/src/lib.rs"
    printf 'syntax = "proto3";\n' >"$1/proto/x.proto"
    printf '#!/bin/sh\n' >"$1/crates/a/build.sh"
    ln -s lib.rs "$1/crates/a/src/alias.rs"
}
inputs=(Cargo.toml crates proto)
mirror="$work/mirror"

# Case 1.
make_checkout "$work/one"
out="$(python3 "$sync" "$work/one" "$mirror" "${inputs[@]}")"
[[ "$out" == "6 0" ]] || fail "the first sync must copy every input file" "$out"
# Give the mirror an explicit old timestamp: freshness assertions must not
# depend on waiting for the host filesystem's clock to advance.
touch -t 200001010000 "$mirror/crates/a/src/lib.rs" "$mirror/crates/b/src/lib.rs"
before="$(mtime "$mirror/crates/b/src/lib.rs")"
make_checkout "$work/two"
out="$(python3 "$sync" "$work/two" "$mirror" "${inputs[@]}")"
[[ "$out" == "0 0" ]] || fail "a fresh checkout of identical content must write nothing" "$out"
[[ "$(mtime "$mirror/crates/b/src/lib.rs")" == "$before" ]] \
    || fail "an unchanged file must keep its mirror mtime"

# Case 2.
printf 'fn a() { 1; }\n' >"$work/two/crates/a/src/lib.rs"
touch -t 200001010000 "$work/two/crates/a/src/lib.rs"
out="$(python3 "$sync" "$work/two" "$mirror" "${inputs[@]}")"
[[ "$out" == "1 0" ]] || fail "one edit must rewrite one file" "$out"
cmp -s "$work/two/crates/a/src/lib.rs" "$mirror/crates/a/src/lib.rs" \
    || fail "the edited file must carry the new content"
(( $(mtime "$mirror/crates/a/src/lib.rs") > before )) \
    || fail "the edited file must get the sync's mtime, not the checkout's"
[[ "$(mtime "$mirror/crates/b/src/lib.rs")" == "$before" ]] \
    || fail "an edit must not touch its neighbours"

# Case 3.
rm -rf "$work/two/crates/b"
rm "$work/two/proto/x.proto"
out="$(python3 "$sync" "$work/two" "$mirror" "${inputs[@]}")"
[[ "$out" == "0 2" ]] || fail "a deleted file and a deleted crate must be removed" "$out"
[[ ! -e "$mirror/crates/b" ]] || fail "a crate gone from the checkout must be gone from the mirror"

# Case 4.
chmod +x "$work/two/crates/a/build.sh"
out="$(python3 "$sync" "$work/two" "$mirror" "${inputs[@]}")"
[[ "$out" == "1 0" ]] || fail "an executable-bit change must rewrite the file" "$out"
[[ -x "$mirror/crates/a/build.sh" ]] || fail "the mirror must carry the executable bit"
[[ -L "$mirror/crates/a/src/alias.rs" && "$(readlink "$mirror/crates/a/src/alias.rs")" == lib.rs ]] \
    || fail "a symlink must be mirrored as the same symlink"

# Case 5.
if out="$(python3 "$sync" "$work/two" "$mirror" Cargo.toml no-such-dir 2>&1)"; then
    fail "a missing input must fail the sync" "$out"
fi
grep -q "build input missing from the checkout: no-such-dir" <<<"$out" \
    || fail "the failure must name the missing input" "$out"

if [ "$status" -eq 0 ]; then
    echo "ok: the Rust source copy rewrites only changed files, keeps every other mtime," \
        "follows deletions and mode changes, keeps links, and fails on a missing input"
fi
exit "$status"
