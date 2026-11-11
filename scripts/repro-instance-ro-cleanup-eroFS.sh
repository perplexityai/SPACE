#!/usr/bin/env bash
# Real-btrfs e2e for the node-mgr `instance.ro`-aware teardown fix.
#
# Reproduces, on a real btrfs filesystem, the failure that wedges a retained
# sandbox slot into quarantine: a leftover read-only `instance.ro` subvolume
# (from a crashed/in-flight suspend or restore) makes the retained-cache
# ownership rewrite's recursive `chmod`/`chown` fail with EROFS. Then proves
# the fix — clear-RO + `btrfs subvolume delete` of the two singular
# `instance.ro` locations before the rewrite — lets the teardown complete with
# no EROFS and no leaked subvolume, while preserving the live `instance/`.
#
# Mirrors the exact command sequences emitted by:
#   crates/l40_api/node/mgr/server/src/service/cleanup_helpers.rs
#     - remove_subvolume_or_dir_with_runner  (clear RO `-ts -f`, then delete)
#     - remove_instance_ro_snapshots         (instance.ro + snapshot/instance.ro)
#     - neutralize_retained_cache_dir*        (prune snapshots/*, rewrite owner)
#
# Requires: root (chown/btrfs), a real btrfs mount. Defaults to /space/cache.
# Usage: sudo scripts/repro-instance-ro-cleanup-eroFS.sh [BTRFS_MOUNT]
set -euo pipefail

BTRFS_MOUNT="${1:-/space/cache}"
BASE="${BTRFS_MOUNT}/.instance-ro-e2e-$$"

log()  { printf '\n=== %s ===\n' "$*"; }
pass() { printf '  PASS: %s\n' "$*"; }
fail() { printf '  FAIL: %s\n' "$*" >&2; exit 1; }

if [[ "$(stat -f -c %T "${BTRFS_MOUNT}")" != "btrfs" ]]; then
  fail "${BTRFS_MOUNT} is not btrfs (got $(stat -f -c %T "${BTRFS_MOUNT}")). Pass a real btrfs mount."
fi

cleanup() {
  # Force-delete any subvolume we created, then the dir.
  for sv in \
    "${BASE}/instance.ro" "${BASE}/snapshot/instance.ro" \
    "${BASE}/snapshots/d-stop-0" "${BASE}/instance"; do
    [[ -e "${sv}" ]] || continue
    btrfs property set -ts -f "${sv}" ro false 2>/dev/null || true
    btrfs subvolume delete -- "${sv}" >/dev/null 2>&1 || true
  done
  rm -rf "${BASE}" 2>/dev/null || true
}
trap cleanup EXIT

# Build a realistic retained cache dir:
#   instance/                live writable instance subvolume (must survive)
#   instance.ro              restore-receive landing (RO subvolume) — leftover
#   snapshot/instance.ro     suspend-codec send source (RO subvolume) — leftover
#   snapshots/d-stop-0       published d-stop artifact (RO subvolume)
build_cache() {
  cleanup
  mkdir -p "${BASE}/snapshot" "${BASE}/snapshots"
  btrfs subvolume create "${BASE}/instance"            >/dev/null
  echo live  > "${BASE}/instance/rootfs.ext4"
  btrfs subvolume create "${BASE}/instance.ro"         >/dev/null
  echo land  > "${BASE}/instance.ro/rootfs.ext4"
  btrfs subvolume create "${BASE}/snapshot/instance.ro" >/dev/null
  echo send  > "${BASE}/snapshot/instance.ro/rootfs.ext4"
  btrfs subvolume create "${BASE}/snapshots/d-stop-0"  >/dev/null
  echo dstop > "${BASE}/snapshots/d-stop-0/rootfs.ext4"
  # Read-only the throwaway + published snapshots, as the real lifecycle does.
  btrfs property set -ts "${BASE}/instance.ro"          ro true
  btrfs property set -ts "${BASE}/snapshot/instance.ro" ro true
  btrfs property set -ts "${BASE}/snapshots/d-stop-0"   ro true
}

# The cache-wide ownership rewrite as the code runs it: prune snapshots/* so it
# never descends into the published RO snapshots, then chown + chmod the rest.
# Diagnostics go to stderr (find's natural channel) so callers can capture them
# with `2>&1 >/dev/null`. Returns nonzero if any -exec fails (e.g. EROFS).
cache_wide_rewrite() {
  find "${BASE}" -path "${BASE}/snapshots/*" -prune -o -exec chown -h root:root {} +
  find "${BASE}" -path "${BASE}/snapshots/*" -prune -o ! -type l -exec chmod ug+rwX,o-rwx {} +
}

# ---------------------------------------------------------------------------
log "BUG REPRODUCTION: rewrite WITHOUT removing instance.ro first"
build_cache
if err="$(cache_wide_rewrite 2>&1 >/dev/null)"; then
  fail "expected EROFS from the rewrite descending into a read-only instance.ro, but it succeeded"
else
  if grep -qi "read-only file system" <<<"${err}"; then
    pass "rewrite hit EROFS as expected (this is the quarantine-loop bug):"
    grep -i "read-only file system" <<<"${err}" | sed 's/^/        /'
  else
    fail "rewrite failed but not with EROFS: ${err}"
  fi
fi

# ---------------------------------------------------------------------------
log "FIX: force-delete instance.ro (clear RO -ts -f, then delete) BEFORE rewrite"
build_cache
# This is exactly remove_instance_ro_snapshots -> remove_subvolume_or_dir.
for ro in "${BASE}/instance.ro" "${BASE}/snapshot/instance.ro"; do
  btrfs property set -ts -f "${ro}" ro false        # clear RO (handles received uuid)
  btrfs subvolume delete -- "${ro}" >/dev/null      # delete the throwaway snapshot
  [[ -e "${ro}" ]] && fail "instance.ro not removed: ${ro}"
done
pass "both instance.ro subvolumes force-deleted"

if err="$(cache_wide_rewrite 2>&1 >/dev/null)"; then
  pass "cache-wide ownership rewrite now succeeds (no EROFS)"
else
  fail "rewrite still failed after instance.ro removal: ${err}"
fi

# instance/ must survive and be writable (it is the live instance the reset keeps).
[[ -d "${BASE}/instance" ]] || fail "instance/ was destroyed — must be preserved"
echo still-writable > "${BASE}/instance/probe" \
  && pass "instance/ preserved and writable" \
  || fail "instance/ not writable after rewrite"

# The published snapshots/* subvolume was pruned out, so it is untouched + RO.
ro_state="$(btrfs property get -ts "${BASE}/snapshots/d-stop-0" ro)"
[[ "${ro_state}" == "ro=true" ]] \
  && pass "published snapshots/d-stop-0 left read-only (pruned, untouched)" \
  || fail "published snapshot RO state changed: ${ro_state}"

# Neither instance.ro location may survive as a subvolume or a dir.
{ [[ ! -e "${BASE}/instance.ro" ]] && [[ ! -e "${BASE}/snapshot/instance.ro" ]]; } \
  && pass "no instance.ro subvolume leaked under the cache dir" \
  || fail "leaked instance.ro path under ${BASE}"

# ---------------------------------------------------------------------------
log "FIX: remove_subvolume_or_dir clears RO before delete (no remove_dir_all fallback leak)"
build_cache
# A naive remove_dir_all on a RO subvolume root leaves the subvolume behind;
# the hardened path clears RO then deletes the subvolume root cleanly.
btrfs property set -ts -f "${BASE}/snapshots/d-stop-0" ro false
btrfs subvolume delete -- "${BASE}/snapshots/d-stop-0" >/dev/null
[[ -e "${BASE}/snapshots/d-stop-0" ]] \
  && fail "RO snapshot subvolume not deleted" \
  || pass "RO snapshot subvolume deleted cleanly (clear-RO + subvolume delete)"

log "ALL E2E CHECKS PASSED"
