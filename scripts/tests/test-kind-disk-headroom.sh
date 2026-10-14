#!/usr/bin/env bash
# Exercise check-disk-headroom.sh with stub `docker` and `df`; never reads the
# host's disks or daemon.
#   1. Both disks with room: silent.
#   2. A low Docker data root warns with its safe cleanups, the host image
#      prune among them, and only that.
#   3. A low data root that does not exist yet is measured at its nearest
#      existing ancestor and warns with the superseded-image prune, never
#      recommending `crictl rmi --prune`.
#   4. A Docker data root this host cannot see, or a failing df, is not a
#      warning. Every case exits 0: the check never refuses a bring-up.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/infra/space/kind/scripts/check-disk-headroom.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/docker-root" "$work/home"
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == info ]] || { echo "unexpected docker call: $*" >&2; exit 99; }
printf '%s\n' "$STUB_DOCKER_ROOT"
EOF
cat >"$work/bin/df" <<'EOF'
#!/usr/bin/env bash
[[ -z "${STUB_DF_FAILS:-}" ]] || exit 1
path="${*: -1}"
free="$STUB_DATA_FREE_KIB"
[[ "$path" != "$STUB_DOCKER_ROOT" ]] || free="$STUB_DOCKER_FREE_KIB"
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf '/dev/stub 104857600 0 %s 0%% /stub\n' "$free"
echo "$path" >>"$STUB_DF_LOG"
EOF
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"
export STUB_DOCKER_ROOT="$work/docker-root" STUB_DF_LOG="$work/df.log"
export SPACE_KIND_DATA_ROOT="$work/home/.kind-data" SPACE_KIND_MIN_FREE_GIB=30
plenty=$((40 * 1024 * 1024)) short=$((12 * 1024 * 1024))

status=0
fail() {
    printf 'FAIL: %s\n' "$1" >&2
    [ -f "$work/output" ] && cat "$work/output" >&2
    status=1
}
run() {
    : >"$STUB_DF_LOG"
    bash "$script" >"$work/output" 2>&1 || fail "exited $?, want 0"
}
has() { grep -qF -- "$1" "$work/output" || fail "output lacks: $1"; }
lacks() { ! grep -qF -- "$1" "$work/output" || fail "output has: $1"; }

# Case 1.
STUB_DOCKER_FREE_KIB=$plenty STUB_DATA_FREE_KIB=$plenty run
[[ ! -s "$work/output" ]] || fail "plenty of room still printed something"

# Case 2.
STUB_DOCKER_FREE_KIB=$short STUB_DATA_FREE_KIB=$plenty run
has "the Docker data root ($work/docker-root) has 12.0 GiB free, under 30 GiB."
has "docker image prune -f"
has "prune-superseded-images.py --host --cluster space-local"
lacks "SPACE_KIND_DATA_ROOT ("

# Case 3.
STUB_DOCKER_FREE_KIB=$plenty STUB_DATA_FREE_KIB=$short run
has "\$SPACE_KIND_DATA_ROOT ($work/home/.kind-data) has 12.0 GiB free, under 30 GiB."
has "prune-superseded-images.py --cluster space-local"
has "Not 'crictl rmi --prune'"
lacks "the Docker data root ("
grep -qxF "$work/home" "$STUB_DF_LOG" || fail "the missing data root was not measured at $work/home"

# Case 4.
STUB_DOCKER_ROOT=/nonexistent/docker STUB_DOCKER_FREE_KIB=$short STUB_DATA_FREE_KIB=$plenty run
[[ ! -s "$work/output" ]] || fail "an invisible Docker data root was reported"
STUB_DF_FAILS=1 STUB_DOCKER_FREE_KIB=$short STUB_DATA_FREE_KIB=$short run
[[ ! -s "$work/output" ]] || fail "a failing df was reported as low"

exit "$status"
