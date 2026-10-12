#!/usr/bin/env bash
# scripts/lib/space-loop-devices.sh: the loop-device pool arithmetic, node
# staging, attach and detach, and the diagnostics it prints when a pool runs
# out. Hermetic and unprivileged: /dev is a temporary directory, the kernel's
# files are fixtures, and losetup, mknod, chmod, chown and stat are stubs on
# PATH. No real device is opened, created or attached.
# shellcheck disable=SC2016 # run scripts expand in the shell that sources the library
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lib="$repo_root/scripts/lib/space-loop-devices.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

dev="$work/dev"
state="$work/state"
img="$work/images/cache.img"

# ── Stubs ────────────────────────────────────────────────────────────────────
# A node the stubs make is a regular file holding one line,
# `<type> <major> <minor> <mode> <uid> <gid>`, since creating a real block
# device needs root. Every stub appends its argv to $FAKE_STATE/calls.
mkdir -p "$work/bin" "$work/images"
cat >"$work/bin/mknod" <<'EOF'
#!/usr/bin/env bash
printf 'mknod %s\n' "$*" >>"$FAKE_STATE/calls"
path="$1"
if [[ -n ${FAKE_MKNOD_FAIL:-} && $path == "$FAKE_MKNOD_FAIL" ]]; then
  echo "mknod: $path: Operation not permitted" >&2
  exit 1
fi
[[ -z ${FAKE_MKNOD_NOOP:-} ]] || exit 0
if [[ -e $path ]]; then
  echo "mknod: $path: File exists" >&2
  exit 1
fi
# mknod masks its mode with the umask: 0666 & ~022.
printf '%s %s %s 644 0 0\n' "${FAKE_MKNOD_TYPE:-$2}" "$3" "$4" >"$path"
EOF
cat >"$work/bin/chmod" <<'EOF'
#!/usr/bin/env bash
printf 'chmod %s\n' "$*" >>"$FAKE_STATE/calls"
[[ -z ${FAKE_CHMOD_FAIL:-} ]] || { echo "chmod: Operation not permitted" >&2; exit 1; }
mode="$(printf '%o' "$((8#$1))")"
shift
for path in "$@"; do
  read -r type major minor _ uid gid <"$path"
  printf '%s %s %s %s %s %s\n' "$type" "$major" "$minor" "$mode" "$uid" "$gid" >"$path"
done
EOF
cat >"$work/bin/chown" <<'EOF'
#!/usr/bin/env bash
printf 'chown %s\n' "$*" >>"$FAKE_STATE/calls"
owner="$1"
shift
for path in "$@"; do
  read -r type major minor mode _ _ <"$path"
  printf '%s %s %s %s %s %s\n' "$type" "$major" "$minor" "$mode" "${owner%%:*}" "${owner#*:}" >"$path"
done
EOF
# GNU `stat -c FORMAT PATH` for the formats the library reads; device numbers
# in hex, as GNU stat prints them.
cat >"$work/bin/stat" <<'EOF'
#!/usr/bin/env bash
[[ -z ${FAKE_STAT_FAIL:-} && $1 == -c && $# == 3 ]] || exit 1
read -r _ major minor mode uid gid <"$3" 2>/dev/null || exit 1
case "$2" in
  %a) echo "$mode" ;;
  %u) echo "$uid" ;;
  %g) echo "$gid" ;;
  %t) printf '%x\n' "$major" ;;
  %T) printf '%x\n' "$minor" ;;
  *) exit 1 ;;
esac
EOF
# `-a` prints $FAKE_STATE/listing. `--find --show` takes its next answer from
# $FAKE_STATE/find: a device path to print, or `busy` for a lost race; an empty
# queue is the ENOENT an exhausted container gets. `-j` answers from the listing
# so a stub-visible detach by backing file names a real-looking device.
cat >"$work/bin/losetup" <<'EOF'
#!/usr/bin/env bash
printf 'losetup %s\n' "$*" >>"$FAKE_STATE/calls"
listing="$FAKE_STATE/listing"
for ((i = 1; i <= $#; i++)); do
  if [[ ${!i} == -j || ${!i} == --associated ]]; then
    j=$((i + 1))
    [[ -f $listing ]] && grep -F "(${!j})" "$listing" | sed 's/:.*//'
    exit 0
  fi
done
case "$1" in
  -a)
    [[ -f $listing ]] && cat "$listing"
    exit "${FAKE_LOSETUP_A_RC:-0}"
    ;;
  --find)
    queue="$FAKE_STATE/find"
    next=""
    if [[ -s $queue ]]; then
      next="$(head -n 1 "$queue")"
      tail -n +2 "$queue" >"$queue.rest"
      mv "$queue.rest" "$queue"
    fi
    case "$next" in
      busy) echo "losetup: $3: failed to set up loop device: Device or resource busy" >&2; exit 1 ;;
      '') echo "losetup: $3: failed to set up loop device: No such file or directory" >&2; exit 1 ;;
      *) echo "$next" ;;
    esac
    ;;
  -d) exit "${FAKE_LOSETUP_D_RC:-0}" ;;
  *) exit 1 ;;
esac
EOF
# The library's privileged-runner seam, so mknod/chmod/chown reach the stubs
# instead of `sudo -n`.
cat >"$work/bin/privileged" <<'EOF'
#!/usr/bin/env bash
printf 'privileged %s\n' "$1" >>"$FAKE_STATE/calls"
exec "$@"
EOF
chmod 0755 "$work/bin/"*

# Sourced by every run before the library. `-b` is the one check the library
# cannot be pointed away from a real device with, so it answers from the stub
# node's type field; every other test goes to the builtin.
cat >"$work/harness.sh" <<'EOF'
set -euo pipefail
fake_is_block() {
  local type _
  [[ -f $1 ]] || return 1
  read -r type _ <"$1" 2>/dev/null || return 1
  [[ $type == b ]]
}
[() {
  if [[ $# == 3 && $1 == -b && $3 == "]" ]]; then
    fake_is_block "$2"
    return
  fi
  builtin [ "$@"
}
# shellcheck source=/dev/null
. "$SPACE_LOOP_TEST_LIB"
EOF

# A fresh host: an empty /dev, no kernel parameters, nothing attached.
reset_host() {
  rm -rf "$dev" "$state" "$work/sys" "$work/modprobe" "$work/cmdline" \
    "$work/max_loop" "$work/max_part"
  mkdir -p "$dev" "$state" "$work/sys/block" "$work/modprobe/etc" "$work/modprobe/lib"
  : >"$state/calls"
}

# seed <first> <last> [mode] [gid] [stride]: block nodes loop<first>..<last>,
# as the runtime's snapshot of the host's /dev leaves them.
seed() {
  local idx mode="${3:-660}" gid="${4:-6}" stride="${5:-1}"
  for ((idx = $1; idx <= $2; idx++)); do
    printf 'b 7 %s %s 0 %s\n' "$((idx * stride))" "$mode" "$gid" >"$dev/loop$idx"
  done
}

listing() { printf '%s\n' "$@" >"$state/listing"; }

# run <script> [VAR=value...]: a fresh shell that sources the library with the
# host above, then runs the script. Each script is kept so the coverage check at
# the end can tell which public functions were called.
run() {
  local script="$1"
  shift
  printf '%s\n' "$script" >>"$work/scripts"
  env -u SPACE_LOOP_DEVICE_NODES -u SPACE_LOOP_DEVICE_TARGET -u SPACE_LOOP_LOSETUP \
    -u SPACE_LOOP_HIGHWATER_SLACK -u SPACE_LOOP_ATTACH_ATTEMPTS -u SPACE_LOOP_NODE_MODE \
    PATH="$work/bin:$PATH" FAKE_STATE="$state" SPACE_LOOP_TEST_LIB="$lib" \
    SPACE_LOOP_DEV_DIR="$dev" \
    SPACE_LOOP_SYS_BLOCK_DIR="$work/sys/block" \
    SPACE_LOOP_CMDLINE_FILE="$work/cmdline" \
    SPACE_LOOP_MAX_PARAM_FILE="$work/max_loop" \
    SPACE_LOOP_MAX_PART_FILE="$work/max_part" \
    SPACE_LOOP_MODPROBE_DIRS="$work/modprobe/etc $work/modprobe/lib" \
    SPACE_LOOP_PRIVILEGED_RUNNER="$work/bin/privileged" \
    SPACE_LOOP_ATTACH_DELAY=0 \
    "$@" bash -c '. "$0"; eval "$1"' "$work/harness.sh" "$script"
}

# expect <name> <want> <got>
expect() {
  [[ "$3" == "$2" ]] || fail "$1: want [$2], got [$3]"
}

calls() { grep -c "^$1" "$state/calls" || true; }

node() { cat "$dev/$1"; }

# The only devices the library may detach are ones it was handed or that
# losetup printed for it: never one looked up by the backing file, which can
# name a device another caller still uses.
assert_detach_by_device_only() { # <backing file> [allowed device...]
  local file="$1" target
  shift
  if grep -Eq '^losetup .*(^| )(-j|--associated|-D|--detach-all)( |$)' "$state/calls"; then
    fail "losetup was asked about or to detach by backing file: $(grep '^losetup' "$state/calls")"
  fi
  while read -r target; do
    [[ $target != "$file" ]] || fail "detached the backing file $file itself"
    [[ " $* " == *" $target "* ]] || fail "detached $target, which nobody handed the library"
  done < <(sed -n 's/^losetup -d //p' "$state/calls")
}

# ── Target: SPACE_LOOP_DEVICE_TARGET, clamped to node-manager's range ────────
reset_host
expect "default target" 64 "$(run 'space_loop_effective_target')"
expect "SPACE_LOOP_DEVICE_TARGET" 200 "$(run 'space_loop_effective_target' SPACE_LOOP_DEVICE_TARGET=200)"
expect "SPACE_LOOP_DEVICE_NODES over the target" 96 \
  "$(run 'space_loop_effective_target' SPACE_LOOP_DEVICE_NODES=96 SPACE_LOOP_DEVICE_TARGET=200)"
expect "a leading zero is decimal" 100 "$(run 'space_loop_effective_target' SPACE_LOOP_DEVICE_TARGET=0100)"
expect "the ceiling is accepted" 10000 "$(run 'space_loop_effective_target' SPACE_LOOP_DEVICE_TARGET=10000)"
for bad in 10 63 10001 100000; do
  out="$(run 'space_loop_effective_target' SPACE_LOOP_DEVICE_TARGET="$bad" 2>"$work/err")"
  expect "target $bad" 64 "$out"
  expect "target $bad warning" "space-loop-devices: loop device target $bad is outside 64..10000; using 64" \
    "$(cat "$work/err")"
done
out="$(run 'space_loop_effective_target' SPACE_LOOP_DEVICE_TARGET=lots 2>"$work/err")"
expect "a non-count target" 64 "$out"
expect "non-count warning" "space-loop-devices: loop device target lots is not a count; using 64" "$(cat "$work/err")"

# An explicit argument is the caller's and is not clamped to the floor.
expect "explicit target" 12 "$(run 'space_loop_effective_target 12')"
expect "explicit target with a leading zero" 80 "$(run 'space_loop_effective_target 080')"
rc=0
out="$(run 'space_loop_effective_target 1x' 2>"$work/err")" || rc=$?
expect "a non-count argument status" 2 "$rc"
expect "a non-count argument message" "space_loop_effective_target: not a count: 1x" "$(cat "$work/err")"

# Never fewer than the highest index the kernel has handed out, plus slack, and
# never past the ceiling. The index comes from the device column alone: a
# backing file named like a loop device does not raise it.
listing "$dev/loop2: [0005]:150 ($work/images/loop99)" "$dev/loop70: [2049]:12 ($work/images/peer.img)"
expect "high-water target" 79 "$(run 'space_loop_effective_target')"
expect "high-water over a larger explicit target" 100 "$(run 'space_loop_effective_target 100')"
expect "high-water slack" 73 "$(run 'space_loop_effective_target' SPACE_LOOP_HIGHWATER_SLACK=2)"
out="$(run 'space_loop_effective_target' SPACE_LOOP_HIGHWATER_SLACK=some 2>"$work/err")"
expect "a non-count slack" 79 "$out"
expect "non-count slack warning" "space-loop-devices: high-water slack some is not a count; using 8" "$(cat "$work/err")"
listing "$dev/loop9998: [2049]:12 ($work/images/peer.img)"
expect "high-water clamped to the ceiling" 10000 "$(run 'space_loop_effective_target')"

# Trailing slashes come off the dev dir, and the root falls back to /dev; the
# variable alone is read, no device under it.
expect "dev dir slashes" "$dev" "$(run 'printf %s "$SPACE_LOOP_DEV_DIR"' SPACE_LOOP_DEV_DIR="$dev///")"
expect "root dev dir" /dev "$(run 'printf %s "$SPACE_LOOP_DEV_DIR"' SPACE_LOOP_DEV_DIR=/)"

# ── Accounting: visible, attached, free and usable ───────────────────────────
reset_host
expect "empty dev count" 0 "$(run 'space_loop_node_count')"
expect "empty dev indices" "" "$(run 'space_loop_node_indices')"
expect "idle attached count" 0 "$(run 'space_loop_attached_count')"
expect "idle listing" "" "$(run 'space_loop_attached')"

seed 0 7
seed 10 10
echo occupant >"$dev/loop9"                # a name taken by something not a device
printf 'b 259 1 660 0 6\n' >"$dev/loop3p1" # a partition, not a pool index
echo control >"$dev/loop-control"
# A listing that names a backing file: loop2 is backed by $dev/loop5 (a loop
# on a loop), which must not mark loop5 attached; loop70 is a peer's device
# this container has no node for.
listing \
  "$dev/loop0: [0:720]:1999855 ($img)" \
  "$dev/loop2: [0005]:150 ($dev/loop5)" \
  "$dev/loop70: [2049]:12 ($work/images/loop99)"
expect "indices, numerically" "0 1 2 3 4 5 6 7 9 10" "$(run 'space_loop_node_indices' | xargs)"
expect "visible nodes" 10 "$(run 'space_loop_node_count')"
expect "listing is losetup -a verbatim" "$(cat "$state/listing")" "$(run 'space_loop_attached')"
expect "attached, read from losetup" 3 "$(run 'space_loop_attached_count')"
# A listing handed in is read instead of losetup's, one count per device.
printf '%s\n' "$dev/loop1: []: (/tmp/a.img)" "$dev/loop1: []: (/tmp/a.img)" "$dev/loop4: []: (/tmp/b.img)" \
  >"$work/given"
expect "attached, from a given listing" 2 "$(run 'space_loop_attached_count "$(cat "$GIVEN")"' GIVEN="$work/given")"
expect "free: names not attached, occupant included" 8 "$(run 'space_loop_free_node_count')"
expect "usable: unattached block devices" 7 "$(run 'space_loop_usable_node_count')"
expect "free from an empty listing" 10 "$(run 'space_loop_free_node_count ""')"
expect "usable from an empty listing" 9 "$(run 'space_loop_usable_node_count ""')"
# A trailing slash on the dev dir must not turn attached nodes into free ones.
expect "free with a slashed dev dir" 8 "$(run 'space_loop_free_node_count' SPACE_LOOP_DEV_DIR="$dev/")"
# A failing losetup -a is not an error, and an absent one reads as nothing attached.
run 'space_loop_attached' FAKE_LOSETUP_A_RC=1 >/dev/null || fail "a failing losetup -a was an error"
expect "losetup absent" "0 10" \
  "$(run 'echo "$(space_loop_attached_count) $(space_loop_free_node_count)"' SPACE_LOOP_LOSETUP="$work/missing/losetup")"

# A specified max_loop leaves an index at or past it usable only once the kernel
# device exists (/sys/block/loopN).
echo "loop.max_loop=8" >"$work/cmdline"
expect "usable under a ceiling" 6 "$(run 'space_loop_usable_node_count')"
mkdir "$work/sys/block/loop10"
expect "usable past the ceiling once materialized" 7 "$(run 'space_loop_usable_node_count')"
rm "$work/cmdline"
rmdir "$work/sys/block/loop10"

echo 8 >"$work/max_loop"
expected="$(printf '%s\n' \
  "loop pool: 10 device nodes visible, 3 attached (kernel-global), 8 free, 7 usable" \
  "loop pool: counted but not usable (not block devices): loop9" \
  "loop pool: max_loop=8 (nothing specifies it, so it is not a ceiling: the driver allocates on demand)" \
  "loop pool: attached (kernel-global, includes other containers):" \
  "  $dev/loop0: [0:720]:1999855 ($img)" \
  "  $dev/loop2: [0005]:150 ($dev/loop5)" \
  "  $dev/loop70: [2049]:12 ($work/images/loop99)")"
expect "report" "$expected" "$(run 'space_loop_report')"
expect "report from a given listing" \
  "$(printf '%s\n' \
    "loop pool: 10 device nodes visible, 0 attached (kernel-global), 10 free, 9 usable" \
    "loop pool: counted but not usable (not block devices): loop9" \
    "loop pool: max_loop=8 (nothing specifies it, so it is not a ceiling: the driver allocates on demand)")" \
  "$(run 'space_loop_report ""')"
expect "losetup -a calls for a report" 1 "$(: >"$state/calls"; run 'space_loop_report' >/dev/null; calls 'losetup -a')"

# ── The max_loop ceiling: command line first, then modprobe configs ──────────
# cap <want> <cmdline or -> [<dir>/<file> <content>]...
cap() {
  local want="$1" cmdline="$2"
  shift 2
  rm -rf "$work/cmdline" "$work/modprobe"
  mkdir -p "$work/modprobe/etc" "$work/modprobe/lib"
  [[ $cmdline == - ]] || printf '%s\n' "$cmdline" >"$work/cmdline"
  while (($#)); do
    printf '%b' "$2" >"$work/modprobe/$1"
    shift 2
  done
  expect "max_loop from [$cmdline]" "$want" "$(run 'space_loop_hard_cap')"
}
cap "" -
cap "" "ro quiet"
cap 16 "ro loop.max_loop=16"
cap 16 "max_loop=0x10"
cap 8 "max_loop=010"
cap 5 "loop.max_loop=+5"
cap 12 "max_loop=4 loop.max_loop=12"
cap 2147483647 "loop.max_loop=2147483647"
cap "" "loop.max_loop=2147483648"
cap "" "loop.max_loop=0x100000000"
cap "" "loop.max_loop=64junk"
# Specified on the command line, whatever it says, outranks a modprobe config.
cap "" "max_loop=0" etc/loop.conf 'options loop max_loop=32\n'
cap "" "max_loop=" etc/loop.conf 'options loop max_loop=32\n'
cap "" "max_loop=08" etc/loop.conf 'options loop max_loop=32\n'
cap 32 "ro" etc/loop.conf 'options loop max_loop=32\n'
# One file per basename, from the first directory that has it; later basenames win.
cap 40 "ro" lib/10-loop.conf 'options loop max_loop=8\n' etc/10-loop.conf 'options loop max_loop=40\n'
cap 20 "ro" etc/10-a.conf 'options loop max_loop=10\n' lib/90-z.conf 'options loop max_loop=20\n'
cap 48 "ro" etc/loop.conf 'options loop \\\n  max_part=0 max_loop=48\n'
cap 40 "ro" etc/loop.conf 'options loop max_loop=40 # was max_loop=8\n'
cap 24 "ro" etc/loop.conf 'options loop max_loop=24'
cap "" "ro" etc/loop.conf '# options loop max_loop=7\noptions loop2 max_loop=5\ninstall loop max_loop=6\n'
cap "" "ro" etc/loop.conf 'options loop max_loop=0\n'
cap "" "ro" etc/loop.txt 'options loop max_loop=9\n'
rm -rf "$work/cmdline"

# ── Minor stride: measured off loop0/loop1, else from max_part ───────────────
reset_host
expect "stride, nothing to measure" 1 "$(run 'space_loop_minor_stride')"
for pair in 1:2 3:4 15:16 abc:1; do
  echo "${pair%%:*}" >"$work/max_part"
  expect "stride from max_part ${pair%%:*}" "${pair##*:}" "$(run 'space_loop_minor_stride')"
done
seed 0 1 660 6 16
echo 0 >"$work/max_part"
expect "stride measured" 16 "$(run 'space_loop_minor_stride')"
# A stat that cannot read the nodes falls through to max_part, not to 1.
echo 3 >"$work/max_part"
expect "stride when stat fails" 4 "$(run 'space_loop_minor_stride' FAKE_STAT_FAIL=1)"

# ── Staging nodes: count, device numbers, mode and owner ─────────────────────
reset_host
seed 0 7
expect "ensure to 12" 4 "$(run 'space_loop_ensure_nodes 12')"
expect "count after ensure" 12 "$(run 'space_loop_node_count')"
for idx in 8 9 10 11; do
  # Mode and group copied off the runtime's own nodes, not the umask's 0644 root.
  expect "loop$idx" "b 7 $idx 660 0 6" "$(node "loop$idx")"
done
expect "nodes created through the privileged runner" 4 "$(calls 'privileged mknod')"
expect "one batched chmod" "chmod 660 $dev/loop8 $dev/loop9 $dev/loop10 $dev/loop11" "$(grep '^chmod' "$state/calls")"
expect "one batched chown" "chown 0:6 $dev/loop8 $dev/loop9 $dev/loop10 $dev/loop11" "$(grep '^chown' "$state/calls")"
: >"$state/calls"
expect "ensure is idempotent" 0 "$(run 'space_loop_ensure_nodes 12')"
expect "no calls on a staged pool" "" "$(cat "$state/calls")"

# No argument grows to the effective target, high-water mark included.
expect "ensure to the default target" 52 "$(run 'space_loop_ensure_nodes')"
expect "count at the default target" 64 "$(run 'space_loop_node_count')"
listing "$dev/loop70: [2049]:12 ($work/images/peer.img)"
expect "ensure past the high-water mark" 15 "$(run 'space_loop_ensure_nodes')"
expect "count past the high-water mark" 79 "$(run 'space_loop_node_count')"
expect "highest staged node" "b 7 78 660 0 6" "$(node loop78)"

# Nothing to copy from: the fallback mode, root's group.
reset_host
expect "ensure on an empty dev, leading zero" 8 "$(run 'space_loop_ensure_nodes 08')"
expect "fallback mode" "b 7 0 660 0 0" "$(node loop0)"
reset_host
run 'space_loop_ensure_nodes 2' SPACE_LOOP_NODE_MODE=0600 >/dev/null
expect "configured fallback mode" "b 7 1 600 0 0" "$(node loop1)"

# Minors follow the measured stride.
reset_host
seed 0 1 660 6 16
expect "ensure with stride" 2 "$(run 'space_loop_ensure_nodes 4')"
expect "minor at stride 16" "b 7 48 660 0 6" "$(node loop3)"

rc=0
out="$(run 'space_loop_ensure_nodes 4x' 2>"$work/err")" || rc=$?
expect "a non-count count status" 2 "$rc"
expect "a non-count count message" "space_loop_ensure_nodes: not a count: 4x" "$(cat "$work/err")"

# An occupant that is not a device is skipped with a word, and not counted.
reset_host
seed 0 7
echo occupant >"$dev/loop9"
out="$(run 'space_loop_ensure_nodes 11' 2>"$work/err")"
expect "ensure around an occupant" 2 "$out"
expect "occupant warning" "space_loop_ensure_nodes: $dev/loop9 exists and is not a block device" "$(cat "$work/err")"
expect "occupant untouched" occupant "$(node loop9)"
expect "occupant not chmodded" "chmod 660 $dev/loop8 $dev/loop10" "$(grep '^chmod' "$state/calls")"

# mknod failing stops the staging but keeps the partial count and the
# permissions on what it made.
reset_host
seed 0 7
rc=0
out="$(run 'space_loop_ensure_nodes 12' FAKE_MKNOD_FAIL="$dev/loop10" 2>"$work/err")" || rc=$?
expect "mknod failure status" 1 "$rc"
expect "mknod failure count" 2 "$out"
grep -qxF "mknod: $dev/loop10: Operation not permitted" "$work/err" || fail "mknod's own stderr was swallowed"
grep -qxF "space_loop_ensure_nodes: mknod $dev/loop10 failed; created 2 node(s) before it" "$work/err" \
  || fail "no partial-staging message: $(cat "$work/err")"
expect "permissions after a partial staging" "chmod 660 $dev/loop8 $dev/loop9" "$(grep '^chmod' "$state/calls")"
expect "owner after a partial staging" "b 7 9 660 0 6" "$(node loop9)"

# A mknod that exits 0 and makes nothing is not a staged node.
reset_host
seed 0 7
rc=0
out="$(run 'space_loop_ensure_nodes 10' FAKE_MKNOD_NOOP=1 2>"$work/err")" || rc=$?
expect "phantom mknod status" 1 "$rc"
expect "phantom mknod count" 0 "$out"
expect "phantom mknod message" "space_loop_ensure_nodes: $dev/loop8 absent after a successful mknod" "$(cat "$work/err")"

# A node that is not a block device after mknod is still counted, with a word.
reset_host
out="$(run 'space_loop_ensure_nodes 1' FAKE_MKNOD_TYPE=c 2>"$work/err")"
expect "non-block mknod count" 1 "$out"
expect "non-block mknod message" "space_loop_ensure_nodes: $dev/loop0 is not a block device after mknod" "$(cat "$work/err")"

# Failing permissions are reported, not fatal.
reset_host
seed 0 7
out="$(run 'space_loop_ensure_nodes 9' FAKE_CHMOD_FAIL=1 2>"$work/err")"
expect "chmod failure count" 1 "$out"
grep -qxF "space_loop_ensure_nodes: chmod 660 failed for at least one of 1 node(s); see its stderr" "$work/err" \
  || fail "no chmod failure message: $(cat "$work/err")"

# ── space_loop_stage_pool: the entrypoints' staging, with the caller's logger ─
say='say() { printf "[node] %s\n" "$*"; }; '
reset_host
seed 0 7
expect "stage the pool" "[node] created 56 loop device node(s) (pool target 64)" \
  "$(run "${say}space_loop_stage_pool say")"
expect "stage an already staged pool" "" "$(run "${say}space_loop_stage_pool say")"
listing "$dev/loop70: [2049]:12 ($work/images/peer.img)"
expect "stage past the high-water mark" "[node] created 15 loop device node(s) (pool target 79)" \
  "$(run "${say}space_loop_stage_pool say")"
expect "pool after staging" 79 "$(run 'space_loop_node_count')"

# A failed staging warns, puts the report on stderr, and still succeeds: the
# caller's mount decides.
reset_host
seed 0 7
rc=0
out="$(run "${say}space_loop_stage_pool say" FAKE_MKNOD_FAIL="$dev/loop9" 2>"$work/err")" || rc=$?
expect "failed staging status" 0 "$rc"
expect "failed staging warning" "[node] WARNING: could not extend the loop-device pool to 64 nodes" "$out"
grep -qxF "[node] loop pool: 9 device nodes visible, 0 attached (kernel-global), 9 free, 9 usable" "$work/err" \
  || fail "the failed staging did not report the pool on stderr: $(cat "$work/err")"
grep -qxF "space_loop_ensure_nodes: mknod $dev/loop9 failed; created 1 node(s) before it" "$work/err" \
  || fail "the failed staging hid the mknod failure"

# A target that cannot be computed falls back to the floor and says so.
reset_host
out="$(run "${say}SPACE_LOOP_DEVICE_NODES=none; space_loop_stage_pool say" 2>/dev/null)"
expect "uncomputable target" \
  "$(printf '%s\n' \
    "[node] WARNING: could not compute the loop-device target; staging the floor of none, which does not cover a higher index a peer may already hold" \
    "[node] WARNING: could not extend the loop-device pool to none nodes")" \
  "$(grep WARNING <<<"$out")"

# ── Attach and detach ────────────────────────────────────────────────────────
reset_host
seed 0 8
listing "$dev/loop2: [0:41]:12 ($img)" # a stale device on the same backing file
: >"$state/calls"
printf '%s\n' "$dev/loop8" >"$state/find"
expect "attach prints the device" "$dev/loop8" "$(run "space_loop_attach '$img'")"
expect "attach calls" "losetup --find --show $img" "$(cat "$state/calls")"
assert_detach_by_device_only "$img"

# A lost race is retried, and losetup's own message reaches stderr.
: >"$state/calls"
printf '%s\n' busy busy "$dev/loop8" >"$state/find"
expect "attach after two lost races" "$dev/loop8" "$(run "space_loop_attach '$img'" 2>"$work/err")"
expect "attach attempts after races" 3 "$(calls 'losetup --find')"
expect "busy messages" 2 "$(grep -c 'Device or resource busy' "$work/err")"
assert_detach_by_device_only "$img"

# An exhausted pool fails after exactly the configured attempts.
: >"$state/calls"
: >"$state/find"
rc=0
run "space_loop_attach '$img'" SPACE_LOOP_ATTACH_ATTEMPTS=3 >/dev/null 2>"$work/err" || rc=$?
expect "exhausted attach status" 1 "$rc"
expect "exhausted attach attempts" 3 "$(calls 'losetup --find')"
expect "ENOENT reaches stderr" 3 "$(grep -c 'No such file or directory' "$work/err")"
expect "nothing detached after failed attaches" 0 "$(calls 'losetup -d')"
assert_detach_by_device_only "$img"

for pair in 00:1 x:1 08:8; do
  : >"$state/calls"
  run "space_loop_attach '$img'" SPACE_LOOP_ATTACH_ATTEMPTS="${pair%%:*}" >/dev/null 2>&1 || true
  expect "attempts ${pair%%:*}" "${pair##*:}" "$(calls 'losetup --find')"
done

# An attach that printed something that is not a block device is undone by
# that device's name, never by the backing file's: loop2, the stale device on
# the same file, stays attached.
: >"$state/calls"
printf '%s\n' "$work/not-a-device" "$work/not-a-device" >"$state/find"
rc=0
run "space_loop_attach '$img'" SPACE_LOOP_ATTACH_ATTEMPTS=2 >/dev/null 2>&1 || rc=$?
expect "non-block attach status" 1 "$rc"
expect "non-block attach undone" "$(printf 'losetup -d %s\n' "$work/not-a-device" "$work/not-a-device")" \
  "$(grep '^losetup -d' "$state/calls")"
assert_detach_by_device_only "$img" "$work/not-a-device"

: >"$state/calls"
run "space_loop_detach '$dev/loop8'"
expect "detach" "losetup -d $dev/loop8" "$(cat "$state/calls")"
assert_detach_by_device_only "$img" "$dev/loop8"
rc=0
run "space_loop_detach '$dev/loop8'" FAKE_LOSETUP_D_RC=1 || rc=$?
expect "detach failure status" 1 "$rc"

: >"$state/calls"
rc=0
run "space_loop_attach '$img'" SPACE_LOOP_LOSETUP="$work/missing/losetup" || rc=$?
expect "attach without losetup" 1 "$rc"
rc=0
run "space_loop_detach '$dev/loop8'" SPACE_LOOP_LOSETUP="$work/missing/losetup" || rc=$?
expect "detach without losetup" 1 "$rc"
expect "no calls without losetup" "" "$(cat "$state/calls")"

# Every losetup the library runs is one of these three; none resolves a device
# from a backing file.
expect "losetup invocations in the library" \
  "$(printf '%s\n' '"$bin" --find' '"$bin" -a' '"$bin" -d')" \
  "$(grep -o '"\$bin" [^ ]*' "$lib" | LC_ALL=C sort -u)"

# ── Exhaustion diagnostics ───────────────────────────────────────────────────
remedy="$(printf '%s\n' \
  "The errno from losetup is misleading: 'No such file or directory' means" \
  "the kernel handed out an index this container has no /dev/loopN node for," \
  "not that the image is missing. A container's /dev is a tmpfs snapshot taken" \
  "at creation and nothing adds nodes to it later." \
  "Remedy: create more nodes (space_loop_ensure_nodes in" \
  "scripts/lib/space-loop-devices.sh, or mknod /dev/loopN b 7 N) and retry;" \
  "raise the target to change it — SPACE_LOOP_DEVICE_TARGET in a compose" \
  "worker or on the devbox, loopDeviceTarget for the kind node-manager chart.")"
not_out="$(printf '%s\n' \
  "The pool is not out, so creating more nodes is not the remedy. Two things" \
  "reach this state: two callers handed the same index by /dev/loop-control," \
  "where the loser's bind fails with EBUSY and a retry settles it; and an" \
  "index whose node exists but which a specified max_loop refuses.")"
ceiling="$(printf '%s\n' \
  "max_loop is a ceiling only where it is specified — on the kernel command" \
  "line, or as 'options loop max_loop=N' in a modprobe config — and it gates" \
  "only this autoload path: node-manager's LOOP_CTL_ADD path (loop_devices.rs)" \
  "is ungated, so a node already running can still be given devices.")"

# Every node attached: exhaustion, with the remedy.
reset_host
seed 0 3
listing "$dev/loop0: []: ($img)" "$dev/loop1: []: (/tmp/b.img)" "$dev/loop2: []: (/tmp/c.img)" "$dev/loop3: []: (/tmp/d.img)"
expect "exhausted report" "$(printf '%s\n' \
  "loop-device exhaustion: no free loop device to attach $img." \
  "loop pool: 4 device nodes visible, 4 attached (kernel-global), 0 free, 0 usable" \
  "loop pool: max_loop=unknown (nothing specifies it, so it is not a ceiling: the driver allocates on demand)" \
  "loop pool: attached (kernel-global, includes other containers):" \
  "  $dev/loop0: []: ($img)" "  $dev/loop1: []: (/tmp/b.img)" "  $dev/loop2: []: (/tmp/c.img)" "  $dev/loop3: []: (/tmp/d.img)" \
  "$remedy" "$ceiling")" "$(run "space_loop_exhaustion_report '$img'")"
assert_detach_by_device_only "$img"
expect "losetup -a calls for an exhaustion report" 1 "$(calls 'losetup -a')"

# Usable nodes left: an attach failure that is not exhaustion, and no remedy
# telling the reader to make more nodes.
seed 4 6
expect "not-exhausted report" "$(printf '%s\n' \
  "loop-device attach failed for $img with 3 node(s) apparently free." \
  "loop pool: 7 device nodes visible, 4 attached (kernel-global), 3 free, 3 usable" \
  "loop pool: max_loop=unknown (nothing specifies it, so it is not a ceiling: the driver allocates on demand)" \
  "loop pool: attached (kernel-global, includes other containers):" \
  "  $dev/loop0: []: ($img)" "  $dev/loop1: []: (/tmp/b.img)" "  $dev/loop2: []: (/tmp/c.img)" "  $dev/loop3: []: (/tmp/d.img)" \
  "$not_out" "$ceiling")" "$(run "space_loop_exhaustion_report '$img'")"

# Free names past a specified ceiling the kernel will not back are exhaustion.
echo "loop.max_loop=4" >"$work/cmdline"
echo 4 >"$work/max_loop"
out="$(run 'space_loop_exhaustion_report')"
expect "exhaustion under a ceiling" "loop-device exhaustion: no free loop device to attach a filesystem image." \
  "$(head -n 1 <<<"$out")"
grep -qxF "loop pool: 7 device nodes visible, 4 attached (kernel-global), 3 free, 0 usable" <<<"$out" \
  || fail "the ceiling report miscounts: $out"
grep -qxF "loop pool: max_loop=4 (specified as 4, so treat it as a hard ceiling)" <<<"$out" \
  || fail "the ceiling report does not name the ceiling: $out"
grep -qF "Remedy: create more nodes" <<<"$out" || fail "the ceiling report has no remedy"
rm "$work/cmdline" "$work/max_loop"

# Without losetup the counts cannot be read, so it leads with that and never
# says the pool looks free.
expect "report without losetup" "$(printf '%s\n' \
  "loop-device attach failed for $img: losetup is not installed." \
  "Install util-linux; the counts below cannot be read without it." \
  "loop-device exhaustion: no free loop device to attach $img." \
  "loop pool: 7 device nodes visible, 0 attached (kernel-global), 7 free, 7 usable" \
  "loop pool: losetup not found, so the attached count above is 0 by default" \
  "loop pool: max_loop=unknown (nothing specifies it, so it is not a ceiling: the driver allocates on demand)" \
  "$remedy" "$ceiling")" \
  "$(run "space_loop_exhaustion_report '$img'" SPACE_LOOP_LOSETUP="$work/missing/losetup")"

# ── Every public function is exercised here ──────────────────────────────────
while read -r fn; do
  grep -qw "$fn" "$work/scripts" || fail "$fn is defined by the library but no case here calls it"
done < <(sed -n 's/^\(space_loop_[a-z_]*\)() {.*/\1/p' "$lib" | grep -v '^space_loop__')

echo "ok: the loop-device library stages, counts, attaches, detaches by device and diagnoses exhaustion as specified"
