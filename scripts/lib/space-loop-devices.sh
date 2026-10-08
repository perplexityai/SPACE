# shellcheck shell=bash
# The loop-device pool on a containerized SPACE dev host, and how to grow it.
#
# Every dev host that backs a filesystem with a file needs loop devices: the
# devbox mounts /space/cache from a btrfs image, each kind SPACE node mounts
# /space/cache from its own, and each encrypted sandbox attaches two more (the
# dm-snapshot origin and the LUKS2 COW). All of them are containers, and a
# container's /dev is a tmpfs the runtime populates ONCE at creation from the
# device nodes its own host had at that moment. No udev runs inside, so nothing
# ever adds a node afterwards: the pool is capped by that snapshot at
# /dev/loop0../dev/loop7 — the kernel's CONFIG_BLK_DEV_LOOP_MIN_COUNT default of
# 8 preallocated devices — for the life of the container.
#
# The kernel has no such cap. Its own words, from drivers/block/loop.c:
#
#   If max_loop is specified, create that many devices upfront. This also
#   becomes a hard limit. If max_loop is not specified, the default isn't a hard
#   limit […] Loop devices can be requested on-demand with the /dev/loop-control
#   interface, or be instantiated by accessing a 'dead' device node.
#
# That last clause is the lever: `mknod /dev/loopN b 7 N` plus the first open of
# the node reaches `loop_probe()` → `loop_add()` and the device exists. Measured
# on a 6.12 devbox at the compiled-in max_loop=8 with all 8 nodes bound —
# mknod /dev/loop8, attach, mkfs.btrfs, mount all succeed.
#
# One config dependency, worth knowing before porting this reasoning elsewhere:
# `loop_probe` and the `max_loop_specified` flag that gates it only exist under
# CONFIG_BLOCK_LEGACY_AUTOLOAD. Without it `loop_probe` is NULL, a dead node
# stays dead, and only /dev/loop-control can extend the pool. The devbox platform
# has it (the measurement above is the proof), so the pool grows there.
#
# What the missing node costs when nobody creates it: `losetup --find` asks the
# kernel for a free index through /dev/loop-control, is told one past the
# snapshot, opens /dev/loop8 and fails with ENOENT —
#
#   losetup: /var/lib/space-cache.img: failed to set up loop device:
#            No such file or directory
#
# — which names the wrong problem entirely. It reads as a missing backing image,
# and the image is fine; the pool is full. That misdirection has cost a bring-up:
# the devbox's host btrfs cache held loops, the kind nodes' cache images held
# the rest, and the last pod to attach one crash-looped on an errno about a file
# that exists.
#
# node-manager owns this at runtime — `crates/libs/node/mgr/server/src/loop_devices.rs`
# raises a node's capacity at startup, deriving a target from the slot budget
# (4 devices/sandbox, clamped to [64, 10000]) and doing it in two halves:
# `LOOP_CTL_ADD` for devices the kernel lacks, then `mknod` for devices with no
# node. Prefer that path; it is ungated by `max_loop` where this one is not.
#
# This library exists for the window before it: each harness mounts /space/cache
# before its services start (on kind, the node's space-cache.service, see
# infra/space/kind/node/space-kind-node.sh), and node-manager has not started yet.
# It honors the same `SPACE_LOOP_DEVICE_TARGET`, defaults to that module's floor
# of 64, and gives nodes the same mode and group, so the two cannot disagree
# about what a node should look like. It also never stages fewer than the highest
# index already handed out (see space_loop_effective_target): indices are
# kernel-global and that module raises the pool well past this floor, so a fixed
# count would wedge a restart once peers hold 0..N-1.
#
# Production nodes need neither: the worker AMI pins `max_loop=10000`
# (infra/space/packer/scripts/base/07-sysctl.sh) and runs on a real devtmpfs
# where the driver's nodes appear by themselves.
#
# Sourced, never executed. Callers need nothing set up first; growing the pool
# needs root or passwordless sudo, everything else is read-only.

# Target pool size. One node per kind SPACE node's cache (four in the topology) plus
# the host cache is the floor; ~2 per concurrent encrypted sandbox is what makes
# the floor the wrong number to provision — the BYOK fleet test asks for 24
# sandboxes by default, i.e. ~52 devices. Concurrent sandboxes are not the only
# term: a warm-pool member with a prepared snapshot rootfs holds two from boot,
# before anything claims it, so
# demand scales with pool size as well. That path is off unless the variable is
# truthy and neither the kind nor the compose harness sets it, which is why the
# fleet gate's arithmetic still counts two per sandbox and nothing per member.
# A node costs one inode in a 64 MiB tmpfs and the kernel device behind it is
# created lazily on first open, so this is sized for headroom rather than for
# measured demand.
# One knob for both layers: node-manager reads SPACE_LOOP_DEVICE_TARGET (and its
# chart renders it as `loopDeviceTarget`), so an operator who raises the target
# should not also have to know a second name.
SPACE_LOOP_DEVICE_NODES="${SPACE_LOOP_DEVICE_NODES:-${SPACE_LOOP_DEVICE_TARGET:-64}}"
# Bounded to the range node-manager enforces (LoopDeviceTarget::MIN/MAX in
# loop_devices.rs). Operator input, so a typo has to cost a warning rather than
# 100k mknod calls into a 64 MiB tmpfs before that module ever sees the value.
# An explicit argument to space_loop_ensure_nodes is a caller's deliberate choice
# and is not clamped.
SPACE_LOOP_DEVICE_NODES_MIN=64
SPACE_LOOP_DEVICE_NODES_MAX=10000
case "$SPACE_LOOP_DEVICE_NODES" in
    '' | *[!0-9]*)
        printf 'space-loop-devices: loop device target %s is not a count; using %s\n' \
            "$SPACE_LOOP_DEVICE_NODES" "$SPACE_LOOP_DEVICE_NODES_MIN" >&2
        SPACE_LOOP_DEVICE_NODES="$SPACE_LOOP_DEVICE_NODES_MIN"
        ;;
    *)
        SPACE_LOOP_DEVICE_NODES="$((10#$SPACE_LOOP_DEVICE_NODES))"
        if [ "$SPACE_LOOP_DEVICE_NODES" -lt "$SPACE_LOOP_DEVICE_NODES_MIN" ] \
            || [ "$SPACE_LOOP_DEVICE_NODES" -gt "$SPACE_LOOP_DEVICE_NODES_MAX" ]; then
            printf 'space-loop-devices: loop device target %s is outside %s..%s; using %s\n' \
                "$SPACE_LOOP_DEVICE_NODES" "$SPACE_LOOP_DEVICE_NODES_MIN" \
                "$SPACE_LOOP_DEVICE_NODES_MAX" "$SPACE_LOOP_DEVICE_NODES_MIN" >&2
            SPACE_LOOP_DEVICE_NODES="$SPACE_LOOP_DEVICE_NODES_MIN"
        fi
        ;;
esac

# Seams, so the arithmetic and the messages are testable off a Linux host.
SPACE_LOOP_DEV_DIR="${SPACE_LOOP_DEV_DIR:-/dev}"
# Trailing slashes off: the membership test below compares built paths against
# `losetup -a` output, and `/dev//loop0` matches nothing — every node would read
# as free and an exhausted pool would report as "not exhaustion".
while [ "${SPACE_LOOP_DEV_DIR%/}" != "$SPACE_LOOP_DEV_DIR" ]; do
    SPACE_LOOP_DEV_DIR="${SPACE_LOOP_DEV_DIR%/}"
done
# Root is not a usable dev dir: every built path would be `//loopN`, which
# matches nothing in `losetup -a` — the same inversion the strip above prevents.
[ -n "$SPACE_LOOP_DEV_DIR" ] || SPACE_LOOP_DEV_DIR=/dev
SPACE_LOOP_MAX_PARAM_FILE="${SPACE_LOOP_MAX_PARAM_FILE:-/sys/module/loop/parameters/max_loop}"
SPACE_LOOP_MAX_PART_FILE="${SPACE_LOOP_MAX_PART_FILE:-/sys/module/loop/parameters/max_part}"
SPACE_LOOP_CMDLINE_FILE="${SPACE_LOOP_CMDLINE_FILE:-/proc/cmdline}"
# Where a materialized loop device shows up. A node that has been opened once —
# or that node-manager created through LOOP_CTL_ADD — has an entry here; a node
# that has only been mknod'ed does not.
SPACE_LOOP_SYS_BLOCK_DIR="${SPACE_LOOP_SYS_BLOCK_DIR:-/sys/block}"
# Space-separated, in modprobe's own precedence order.
SPACE_LOOP_MODPROBE_DIRS="${SPACE_LOOP_MODPROBE_DIRS:-/etc/modprobe.d /run/modprobe.d /usr/local/lib/modprobe.d /usr/lib/modprobe.d /lib/modprobe.d}"
# Bounded retry for an attach that lost a race rather than ran out of pool.
SPACE_LOOP_ATTACH_ATTEMPTS="${SPACE_LOOP_ATTACH_ATTEMPTS:-5}"
SPACE_LOOP_ATTACH_DELAY="${SPACE_LOOP_ATTACH_DELAY:-0.2}"
# Empty means "resolve it"; set it to pin one binary.
SPACE_LOOP_LOSETUP="${SPACE_LOOP_LOSETUP:-}"
# Command prefix for the one privileged operation (mknod). Empty means: run it
# directly as root, or through `sudo -n` when we are not root. Set only by the
# regression test, which points it at a recorder so the arithmetic is testable
# without root — it is a seam, not a supported way to configure a real host, and
# setting it bypasses the `sudo` a non-root caller needs.
SPACE_LOOP_PRIVILEGED_RUNNER="${SPACE_LOOP_PRIVILEGED_RUNNER:-}"

# Loop devices are major 7 (LOOP_MAJOR, a kernel constant).
SPACE_LOOP_MAJOR=7
# Mode for a node this library creates, when there is no existing node to copy
# from. Matches FALLBACK_NODE_MODE in loop_devices.rs.
SPACE_LOOP_NODE_MODE="${SPACE_LOOP_NODE_MODE:-0660}"

# Mode and group to give a new node, copied off a node the runtime already made
# so the two layers cannot disagree. The mode matters: `mknod` masks its mode
# argument with the process umask, so a node created here lands 0644 root:root
# under the usual 022 while the snapshot's own nodes are 0660 root:disk — and a
# non-root service that can open loop0..7 would then get EACCES on the ones this
# library added. `chmod` is not masked, which is why it is a second step.
space_loop__node_mode_and_gid() {
    local idx node mode gid
    while read -r idx; do
        node="$SPACE_LOOP_DEV_DIR/loop$idx"
        [ -b "$node" ] || continue
        mode="$(stat -c '%a' "$node" 2>/dev/null || true)"
        gid="$(stat -c '%g' "$node" 2>/dev/null || true)"
        if space_loop__is_number "$mode" && space_loop__is_number "$gid"; then
            printf '%s %s\n' "$mode" "$gid"
            return 0
        fi
    done < <(space_loop_node_indices)
    printf '%s %s\n' "$SPACE_LOOP_NODE_MODE" 0
}

space_loop__is_number() {
    case "${1:-}" in
        '' | *[!0-9]*) return 1 ;;
    esac
    return 0
}

space_loop__privileged() {
    if [ -n "$SPACE_LOOP_PRIVILEGED_RUNNER" ]; then
        # Deliberate word splitting: the runner is a command plus its arguments.
        # shellcheck disable=SC2086
        $SPACE_LOOP_PRIVILEGED_RUNNER "$@"
    elif [ "$(id -u)" = "0" ]; then
        "$@"
    else
        sudo -n "$@"
    fi
}

# Indices of the /dev/loopN entries this container can see, ascending.
#
# `-e`, not `-b`: this answers "is the name taken", which is the question both
# the mknod collision test and the free count need — a name an occupant holds is
# not capacity `mknod` can add. It does mean a non-block occupant counts toward
# the visible total, so space_loop_report names any it finds rather than letting
# the count read as usable devices.
space_loop_node_indices() {
    local path idx
    for path in "$SPACE_LOOP_DEV_DIR"/loop[0-9]*; do
        [ -e "$path" ] || continue
        idx="${path##*/loop}"
        case "$idx" in
            '' | *[!0-9]*) continue ;;
        esac
        printf '%s\n' "$idx"
    done | sort -n
}

space_loop_node_count() {
    space_loop_node_indices | grep -c '^' || true
}

# losetup lives in /usr/sbin, which the devbox image leaves off a non-root PATH
# — the same reason this repo calls `/sbin/mkfs.btrfs` by absolute path. Resolve
# it instead of making every caller export a PATH.
space_loop__losetup_bin() {
    if [ -n "$SPACE_LOOP_LOSETUP" ]; then
        # A pinned binary still has to exist: reporting a path nobody can run
        # makes every count read as "nothing attached", and the caller then says
        # the pool looks free.
        command -v "$SPACE_LOOP_LOSETUP" >/dev/null 2>&1 || return 1
        printf '%s\n' "$SPACE_LOOP_LOSETUP"
        return 0
    fi
    local candidate
    for candidate in losetup /usr/sbin/losetup /sbin/losetup; do
        if command -v "$candidate" >/dev/null 2>&1; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

# `losetup -a` lines, e.g. `/dev/loop3: [0:720]:1999855 (/var/lib/space-cache.img)`.
# Readable unprivileged (it comes out of sysfs; only the backing dev:inode field
# needs root). Empty when losetup is absent or the pool is idle; never an error,
# because every caller treats "cannot tell" as "nothing attached" and says so.
space_loop_attached() {
    local bin
    bin="$(space_loop__losetup_bin)" || return 0
    "$bin" -a 2>/dev/null || true
}

# Device names only. The kernel pool is global — a kind node or pod's
# `losetup -a` lists devices attached on other nodes and by the devbox itself,
# and it can name a device this container has no node for.
space_loop__attached_devices() {
    space_loop__attached_devices_from "$(space_loop_attached)"
}

# Same, from a listing the caller already read.
space_loop__attached_devices_from() {
    printf '%s\n' "${1:-}" | sed -n 's/^\([^:]*\):.*/\1/p' | sort -u
}

# Optional listing, same contract as space_loop_free_node_count.
space_loop_attached_count() {
    if [ "$#" -gt 0 ]; then
        space_loop__attached_devices_from "$1" | grep -c '^' || true
    else
        space_loop__attached_devices | grep -c '^' || true
    fi
}

# Nodes this container can see that nothing has attached — the pool it can
# actually allocate from, which is the number that matters and the number
# `max_loop` does not report.
# Takes an optional `losetup -a` listing so a caller reporting several numbers
# reads the pool once.
space_loop_free_node_count() {
    local listing="${1-}" attached free=0 idx
    [ "$#" -gt 0 ] || listing="$(space_loop_attached)"
    # Newline-delimited on both sides so the membership test is a `case` glob
    # rather than a grep per node — 64 spawns at the default target, in a
    # pod-startup path.
    attached=$'\n'"$(space_loop__attached_devices_from "$listing")"$'\n'
    while read -r idx; do
        [ -n "$idx" ] || continue
        case "$attached" in
            *$'\n'"$SPACE_LOOP_DEV_DIR/loop$idx"$'\n'*) continue ;;
        esac
        free=$((free + 1))
    done < <(space_loop_node_indices)
    printf '%s\n' "$free"
}

# The highest index the kernel has already handed out, from the global listing.
# Empty when nothing is attached.
space_loop__highest_attached_index() {
    local listing="${1-}"
    [ "$#" -gt 0 ] || listing="$(space_loop_attached)"
    space_loop__attached_devices_from "$listing" \
        | sed -n 's#.*/loop\([0-9]\{1,\}\)$#\1#p' \
        | sort -n \
        | tail -1
}

# How many nodes this container should stage: the configured target, but never
# fewer than the indices the kernel is already using.
#
# A fixed count is the wrong shape on its own. Indices are kernel-global, and
# node-manager raises the pool well past this floor once it starts (1200 on a kind
# box, 80 in compose). So a container that restarts after peers have taken
# 0..63 gets index 64 from `losetup --find`, has no node for it, and crash-loops
# on the same ENOENT this file exists to prevent — with a pool that looks full at
# 64. Staging past the high-water mark plus a little slack for the attaches
# racing this one keeps that from happening.
SPACE_LOOP_HIGHWATER_SLACK="${SPACE_LOOP_HIGHWATER_SLACK:-8}"
# Validated like the other numeric knobs, and for a sharper reason: it is
# interpolated into arithmetic that only runs once something is attached, so a bad
# value is inert on an idle pool and fatal exactly when the staging matters. What
# makes that failure quiet is the callers' `|| target="$SPACE_LOOP_DEVICE_NODES"`
# fallback, which turns "could not compute the target" into "stage the un-raised
# floor" — the very wedge the high-water floor exists to prevent. Validating here
# also keeps an operator-supplied string out of arithmetic evaluation.
case "$SPACE_LOOP_HIGHWATER_SLACK" in
    '' | *[!0-9]*)
        printf 'space-loop-devices: high-water slack %s is not a count; using 8\n' \
            "$SPACE_LOOP_HIGHWATER_SLACK" >&2
        SPACE_LOOP_HIGHWATER_SLACK=8
        ;;
    *) SPACE_LOOP_HIGHWATER_SLACK="$((10#$SPACE_LOOP_HIGHWATER_SLACK))" ;;
esac
space_loop_effective_target() {
    local configured="${1:-$SPACE_LOOP_DEVICE_NODES}" highest target
    case "$configured" in
        '' | *[!0-9]*)
            printf 'space_loop_effective_target: not a count: %s\n' "$configured" >&2
            return 2
            ;;
        *) configured="$((10#$configured))" ;;
    esac
    # One read of the pool, passed down.
    highest="$(space_loop__highest_attached_index "$(space_loop_attached)")"
    target="$configured"
    if space_loop__is_number "$highest"; then
        local needed=$(( highest + 1 + SPACE_LOOP_HIGHWATER_SLACK ))
        [ "$needed" -gt "$target" ] && target="$needed"
    fi
    # Same ceiling the operator-supplied target is held to, so a runaway global
    # index cannot turn into a runaway mknod loop either.
    [ "$target" -gt "$SPACE_LOOP_DEVICE_NODES_MAX" ] && target="$SPACE_LOOP_DEVICE_NODES_MAX"
    printf '%s\n' "$target"
}

# Nodes that are block devices and unattached — capacity something can actually
# attach to. Distinct from space_loop_free_node_count, which answers "names not
# taken" and so counts a non-block occupant: that is the right question for a
# mknod collision and the wrong one for "is the pool out", which is what the
# entrypoints and the exhaustion report branch on.
# Whether the kernel can put a device behind this index — the whole of the
# decision above, split out because the loop that uses it needs real block devices
# to reach and this does not. Either answer alone is enough:
#
#   - /sys/block/loopN exists, so the device is already there. Nothing gates that:
#     node-manager's LOOP_CTL_ADD pass creates devices past a specified max_loop,
#     so an index above the ceiling can be real capacity in a long-running worker.
#   - the index is below any specified ceiling, so autoload creates it on first
#     open. This is what this library relies on, and why /sys/block alone is the
#     wrong test — it would call a freshly-staged pool unusable.
#
# So only the index that is both above a specified ceiling and not yet
# materialized is a dead end.
space_loop__index_can_back_a_device() { # <index> <specified cap, empty for none>
    local idx="$1" cap="${2-}"
    [ -e "$SPACE_LOOP_SYS_BLOCK_DIR/loop$idx" ] && return 0
    [ -n "$cap" ] || return 0
    [ "$idx" -lt "$cap" ]
}

space_loop_usable_node_count() {
    local listing="${1-}" attached usable=0 idx cap
    [ "$#" -gt 0 ] || listing="$(space_loop_attached)"
    attached=$'\n'"$(space_loop__attached_devices_from "$listing")"$'\n'
    # Two ways a node backs a device, and a node needs only one of them:
    #
    #   - The kernel device already exists, which /sys/block/loopN shows. Nothing
    #     gates this — node-manager's LOOP_CTL_ADD pass creates devices *past* a
    #     specified max_loop, so an index above the ceiling can be real, usable
    #     capacity in a worker that has been running a while.
    #   - The kernel will create it on first open. That lazy creation is what this
    #     library relies on and is the reason /sys/block alone is the wrong test:
    #     it would call an entire freshly-staged pool unusable. But it *is* gated
    #     — a specified max_loop gates loop_probe — so a node at or above the
    #     ceiling with no device behind it yet is a dead end.
    #
    # Hence the disjunction rather than either check alone: reject only the node
    # that is both above a specified ceiling and not already materialized.
    cap="$(space_loop_hard_cap)"
    while read -r idx; do
        [ -n "$idx" ] || continue
        [ -b "$SPACE_LOOP_DEV_DIR/loop$idx" ] || continue
        space_loop__index_can_back_a_device "$idx" "$cap" || continue
        case "$attached" in
            *$'\n'"$SPACE_LOOP_DEV_DIR/loop$idx"$'\n'*) continue ;;
        esac
        usable=$((usable + 1))
    done < <(space_loop_node_indices)
    printf '%s\n' "$usable"
}

# The kernel maps loop index → minor as `idx << part_shift`, where part_shift is
# `fls(max_part)` and max_part defaults to 0. Measure the stride off the nodes
# the driver already made rather than re-deriving fls() in shell, and fall back
# to max_part only when there are too few nodes to measure.
space_loop__is_hex() {
    case "${1:-}" in
        '' | *[!0-9a-fA-F]*) return 1 ;;
    esac
    return 0
}

space_loop_minor_stride() {
    local m0 m1 stride
    if [ -b "$SPACE_LOOP_DEV_DIR/loop0" ] && [ -b "$SPACE_LOOP_DEV_DIR/loop1" ]; then
        # stat prints device numbers in hex.
        # Empty, not a fabricated 0/1: a `stat` that failed or is not GNU must
        # fall through to max_part rather than win the guard with stride 1, which
        # on a max_part>0 host would mknod every index at the wrong minor.
        #
        # Each value is checked on its own. Testing the concatenation let one
        # empty half through — `16#` with an empty operand is `0` on bash 3.2 but
        # a fatal arithmetic error on bash 5, which aborted this function and
        # left every caller reporting a pool it never staged.
        m0="$(stat -c '%T' "$SPACE_LOOP_DEV_DIR/loop0" 2>/dev/null || true)"
        m1="$(stat -c '%T' "$SPACE_LOOP_DEV_DIR/loop1" 2>/dev/null || true)"
        if space_loop__is_hex "$m0" && space_loop__is_hex "$m1"; then
            stride=$(( 16#$m1 - 16#$m0 ))
            if [ "$stride" -ge 1 ]; then
                printf '%s\n' "$stride"
                return 0
            fi
        fi
    fi
    local max_part=0
    [ -r "$SPACE_LOOP_MAX_PART_FILE" ] && max_part="$(cat "$SPACE_LOOP_MAX_PART_FILE")"
    case "$max_part" in
        '' | *[!0-9]*) max_part=0 ;;
    esac
    stride=1
    while [ "$max_part" -gt 0 ]; do
        stride=$((stride * 2))
        max_part=$((max_part / 2))
    done
    printf '%s\n' "$stride"
}

# The specified `max_loop`, which is the only form that makes it a hard ceiling
# — the driver sets `max_loop_specified` when the parameter is passed, and that
# flag is what gates `loop_probe()`. Empty when nothing specifies it.
#
# Three ways to specify it, and all have to be checked: `loop.max_loop=N` on the
# kernel command line (the module-parameter form), a bare `max_loop=N` there
# (`__setup("max_loop=")`, which is how a built-in driver takes it), and
# `options loop max_loop=N` in a modprobe config, which is how the AMI pins 10000
# (infra/space/packer/scripts/base/07-sysctl.sh). A modprobe config only applies
# if the driver is a module that loaded after the config was written — on a
# built-in driver it is inert — so a config-sourced value is a cap that *may* be
# in force, and the attach is the arbiter either way.
space_loop_hard_cap() {
    local cap
    # Precedence first: the probes report what they found, 0 included, because
    # "the cmdline said 0" and "the cmdline said nothing" are different facts and
    # collapsing them let a modprobe conf override an explicitly-unbounded
    # kernel — then this reported a hard ceiling on a host that has none.
    cap="$(space_loop__cap_from_cmdline)"
    # Fall through only when the command line said nothing at all: a cmdline that
    # specified an unbounded value is an answer, and a modprobe conf must not
    # override it.
    if [ -z "$cap" ] && ! space_loop__cmdline_specifies_max_loop; then
        cap="$(space_loop__cap_from_modprobe_conf)"
    fi
    # Zero semantics last: `max_loop=0` is specified-but-unbounded. The driver's
    # gate is `max_loop_specified && max_loop && idx >= max_loop`, so zero never
    # blocks, and 07-sysctl.sh records that zero resolves to 2^20.
    [ -n "$cap" ] || return 0
    [ "$cap" -eq 0 ] && return 0
    printf '%s\n' "$cap"
}

space_loop__cap_from_cmdline() {
    [ -r "$SPACE_LOOP_CMDLINE_FILE" ] || return 0
    # Whole-word match on the kernel's own tokenization, in bash rather than
    # through `sed \b` (a GNU extension; this file is read by BSD sed too). Last
    # assignment wins, as the kernel's parameter parsing does.
    local -a words=()
    read -r -a words <"$SPACE_LOOP_CMDLINE_FILE" || true
    local word cap=""
    if [ "${#words[@]}" -gt 0 ]; then
        for word in "${words[@]}"; do
            case "$word" in
                loop.max_loop=*) cap="${word#loop.max_loop=}" ;;
                max_loop=*) cap="${word#max_loop=}" ;;
            esac
        done
    fi
    space_loop__print_if_number "$cap"
}

# The config files kmod would actually apply, in the order it applies them: one
# file per basename, taken from the highest-priority directory that has it, then
# sorted by basename so a later file's assignment wins — which is why the caller
# below keeps overwriting `cap` rather than stopping at the first hit.
space_loop__modprobe_conf_files() {
    local dir file
    for dir in $SPACE_LOOP_MODPROBE_DIRS; do
        [ -d "$dir" ] || continue
        for file in "$dir"/*.conf; do
            [ -r "$file" ] || continue
            printf '%s|%s\n' "${file##*/}" "$file"
        done
    done | sort -t'|' -k1,1 -s | awk -F'|' '!seen[$1]++ { print $2 }'
}

space_loop__cap_from_modprobe_conf() {
    local file cap="" directive line continuation word
    while read -r file; do
        [ -n "$file" ] || continue
        {
            # One directive per line: `options loop … max_loop=N …`. The
            # `|| [ -n "$directive" ]` keeps the last line when the file has no
            # trailing newline — without it a one-line conf written by hand is
            # read as empty, and a real ceiling reports as absent.
            # `read` without -r would eat the backslashes; with -r a trailing
            # one has to be joined by hand, because modprobe honors line
            # continuations and a continued `options loop` line would otherwise
            # parse as two useless lines — reporting no ceiling for a host that
            # has one, which is the wrong way for a diagnostic to fail.
            while read -r line || [ -n "$line" ]; do
                while [ "${line%\\}" != "$line" ]; do
                    line="${line%\\}"
                    # `|| :` and not `|| continuation=""`: on EOF `read` has
                    # already assigned what it read, and discarding it loses a
                    # continued directive in a file with no trailing newline —
                    # the same partial-line case the outer loop preserves.
                    read -r continuation || :
                    line="$line$continuation"
                    continuation=""
                done
                # Strip from `#` to end of line. A deliberate local choice, not
                # a claim about kmod: modprobe.d(5) documents ignoring lines that
                # *begin* with `#`, and whether kmod also strips a trailing
                # comment could not be measured (no kmod on the devbox image).
                # Strip anyway, because a comment mentioning an older
                # `max_loop=` must not outrank the live assignment beside it; if
                # kmod is stricter, this errs toward the lower ceiling.
                line="${line%%#*}"
                # `set --` does pathname expansion as well as word splitting, and
                # this content comes from a file: disable globbing for the split.
                # Restore the caller's setting rather than assuming globbing
                # was on: this is confined to a subshell today, but that is a
                # property of the one call site, not of this function.
                case "$-" in *f*) local had_noglob=1 ;; *) local had_noglob=0 ;; esac
                set -f
                # shellcheck disable=SC2086
                set -- $line
                [ "$had_noglob" -eq 1 ] || set +f
                directive="${1:-}"
                [ "$directive" = "options" ] || continue
                shift
                # The rest of the line is a module name followed by its
                # parameters.
                [ "${1:-}" = "loop" ] || continue
                shift
                for word in "$@"; do
                    case "$word" in
                        max_loop=*) cap="${word#max_loop=}" ;;
                    esac
                done
            done <"$file"
        }
    done < <(space_loop__modprobe_conf_files)
    space_loop__print_if_number "$cap"
}

# Prints the value the kernel would parse, or nothing when it would parse none.
#
# Base 0, matching the kernel: the built-in form goes through
# `simple_strtol(str, NULL, 0)` and the module-parameter form through
# `kstrtoint(val, 0, …)`, so `0x10` is 16 and `010` is 8. Decimal-normalizing
# everything reinterpreted the octal form and dropped the hex one entirely, and
# dropping it reports "nothing specifies max_loop" on a host that has a ceiling —
# the fail-open direction. Anything neither parses is treated as unspecified,
# which is also what the kernel does: `param_set_int` failing leaves
# `max_loop_specified` false.
# `max_loop` is an `int`, so the kernel's parse rejects anything above INT_MAX and
# leaves the parameter unspecified. Shell arithmetic is 64-bit and would instead
# wrap a huge value into a plausible-looking ceiling.
SPACE_LOOP_MAX_INT=2147483647

# Can this digit string be an int without wrapping? Judged on the text, because
# `$((…))` is 64-bit and silently wraps — including back into a plausible range.
# INT_MAX is 2147483647: 10 decimal digits, 8 hex, 11 octal.
space_loop__digits_fit_int() { # <digits> <base>
    local digits="$1" base="$2" max_digits
    # Leading zeros carry no magnitude.
    while [ "${digits#0}" != "$digits" ]; do
        digits="${digits#0}"
    done
    [ -n "$digits" ] || return 0
    case "$base" in
        16) max_digits=8 ;;
        8) max_digits=11 ;;
        *) max_digits=10 ;;
    esac
    [ "${#digits}" -le "$max_digits" ] || return 1
    # At the boundary length the value can still exceed INT_MAX, so let the
    # (now safely small) arithmetic decide.
    return 0
}

space_loop__print_if_number() {
    local raw="${1:-}" parsed=""
    # A leading `+` is skipped by the kernel's kstrtoull, so `loop.max_loop=+5` is
    # a ceiling of 5. Trailing garbage is the other direction and is left alone:
    # `simple_strtol` would take the numeric prefix for the built-in form, but the
    # module-parameter form rejects it outright, and reporting no ceiling for
    # `max_loop=64junk` errs toward recommending nodes that will simply work.
    raw="${raw#+}"
    case "$raw" in
        '') return 0 ;;
        0x* | 0X*)
            case "${raw#0[xX]}" in
                '' | *[!0-9a-fA-F]*) return 0 ;;
            esac
            # Magnitude first, on the text: 64-bit arithmetic can wrap an
            # oversized value back into the accepted range
            # (18446744073709551715 wraps to 99), so the bounds below would pass
            # a ceiling the kernel's parser rejected outright.
            space_loop__digits_fit_int "${raw#0[xX]}" 16 || return 0
            parsed="$((raw))"
            ;;
        0*)
            # Any leading zero is octal to a base-0 parse, so an invalid octal
            # digit makes the whole value unparseable — `08` is not 8, it is
            # "the kernel rejected this and left max_loop unspecified".
            case "${raw#0}" in
                *[!0-7]*) return 0 ;;
            esac
            space_loop__digits_fit_int "${raw#0}" 8 || return 0
            parsed="$((raw))"
            ;;
        *[!0-9]*) return 0 ;;
        *)
            space_loop__digits_fit_int "$raw" 10 || return 0
            parsed="$((10#$raw))"
            ;;
    esac
    # Out of an int's range is unspecified, as it is to the kernel — without this,
    # 64-bit shell arithmetic turns an overlong value into a ceiling that looks
    # real.
    # Upper bound only: the magnitude checks above cap the text at 10 decimal
    # digits, 8 hex or 11 octal, so the largest value that reaches here is
    # 9999999999 — the 64-bit wraparound that used to produce a negative
    # "ceiling" is no longer reachable, and a guard against it would be dead.
    [ "$parsed" -le "$SPACE_LOOP_MAX_INT" ] || return 0
    printf '%s\n' "$parsed"
}

# Did this source name the parameter at all, whatever value it gave? `max_loop=`
# with nothing after it and `max_loop=0` both mean specified-and-unbounded, and
# reporting either as "nothing specified" let a lower-priority source override a
# command line that had in fact specified it.
space_loop__cmdline_specifies_max_loop() {
    [ -r "$SPACE_LOOP_CMDLINE_FILE" ] || return 1
    local -a words=()
    read -r -a words <"$SPACE_LOOP_CMDLINE_FILE" || true
    local word
    if [ "${#words[@]}" -gt 0 ]; then
        for word in "${words[@]}"; do
            case "$word" in
                loop.max_loop=* | max_loop=*) return 0 ;;
            esac
        done
    fi
    return 1
}

space_loop__max_param() {
    [ -r "$SPACE_LOOP_MAX_PARAM_FILE" ] || return 0
    cat "$SPACE_LOOP_MAX_PARAM_FILE" 2>/dev/null || true
}

# Create the /dev/loopN nodes this container is missing, up to `count` (default
# SPACE_LOOP_DEVICE_NODES). Prints the number created. Idempotent: an existing
# node is left alone, so re-running costs one stat per index.
#
# mknod succeeding does not prove the kernel will allocate that index — it will
# refuse when `loop.max_loop` was specified below it — so this does not claim the
# pool grew, only that the nodes are there. The first attach is what proves it,
# and space_loop_exhaustion_report covers the case where it does not.
space_loop_ensure_nodes() {
    # No argument means "the target for this host", which includes covering the
    # indices the kernel is already handing out. An explicit argument is a
    # caller's deliberate count and is taken as given.
    #
    # Callers that log a target should call space_loop_effective_target and pass
    # the result, so the number in the message is the number staged: the two
    # differ whenever peers hold a high index, and a message naming the
    # configured 64 while the run staged 1209 is this file's own failure mode.
    local want="${1:-$(space_loop_effective_target)}"
    case "$want" in
        '' | *[!0-9]*)
            printf 'space_loop_ensure_nodes: not a count: %s\n' "$want" >&2
            return 2
            ;;
        # Normalized for the same reason as the attach clamp: `08` in a `for ((…))`
        # bound is an invalid octal constant, and the loop then ran zero times
        # while this function printed 0 and returned success — a silent no-op
        # reported as a clean pass.
        *) want="$((10#$want))" ;;
    esac
    local stride idx node created=0
    # Read into temporaries and adopt only a complete answer: on EOF `read`
    # assigns empty rather than leaving the names alone, which would put `chmod ""`
    # and `chown "0:"` on the command line.
    local mode="$SPACE_LOOP_NODE_MODE" gid=0 probed_mode="" probed_gid=""
    local -a made=()
    stride="$(space_loop_minor_stride)"
    read -r probed_mode probed_gid < <(space_loop__node_mode_and_gid) || true
    if space_loop__is_number "$probed_mode" && space_loop__is_number "$probed_gid"; then
        mode="$probed_mode"
        gid="$probed_gid"
    fi
    for ((idx = 0; idx < want; idx++)); do
        node="$SPACE_LOOP_DEV_DIR/loop$idx"
        # The name being taken is what stops a `mknod`, so that is the test —
        # `-e`, matching space_loop_node_indices, which answers the same question
        # for the free count. An occupant that is not a block device is not
        # usable capacity, and `mknod` cannot replace it; that is worth a word
        # rather than a hard failure, because nothing writes files into /dev and
        # refusing to stage the remaining 60-odd nodes over one odd entry would
        # cost more than the entry does.
        if [ -e "$node" ]; then
            [ -b "$node" ] \
                || printf 'space_loop_ensure_nodes: %s exists and is not a block device\n' \
                    "$node" >&2
            continue
        fi
        # mknod's stderr is deliberately not suppressed: "sudo: a password is
        # required" and "File exists" are different problems, and swallowing them
        # to print a generic failure is the habit this file exists to break.
        if ! space_loop__privileged mknod "$node" b "$SPACE_LOOP_MAJOR" \
            "$((idx * stride))"; then
            # A node that appeared underneath us (a concurrent run in this same
            # container) is success, not failure.
            if [ -b "$node" ]; then
                continue
            fi
            printf 'space_loop_ensure_nodes: mknod %s failed; created %s node(s) before it\n' \
                "$node" "$created" >&2
            # Permissions on what was made, before returning: the nodes exist and
            # something will try to use them.
            space_loop__apply_node_permissions "$mode" "$gid" "${made[@]+"${made[@]}"}"
            # The count still goes to stdout: a partial success is not a total
            # failure, and the caller's log should not say otherwise.
            printf '%s\n' "$created"
            return 1
        fi
        # `mknod` exiting 0 without a node means the runner seam is not really
        # creating them; counting it would report a pool that does not exist.
        # Existence is the portable "did anything appear" check, and it is the
        # one that catches the case worth catching: a runner that exits 0 and
        # creates nothing, which would report a pool that does not exist. A
        # wrong *type* cannot come out of `mknod … b`, so that only gets a word.
        if [ ! -e "$node" ]; then
            printf 'space_loop_ensure_nodes: %s absent after a successful mknod\n' "$node" >&2
            space_loop__apply_node_permissions "$mode" "$gid" "${made[@]+"${made[@]}"}"
            # Same contract as the mknod-failure path above: report how far it
            # got, so a partial success does not read as a total failure.
            printf '%s\n' "$created"
            return 1
        fi
        # Counted either way — "created" means the node now exists, which is what
        # the caller's log is reporting. Usability is a different question, and
        # the one decisions turn on: space_loop_usable_node_count excludes a
        # non-block entry, so withholding it from this count too would only cost
        # the permissions below without changing any decision.
        [ -b "$node" ] \
            || printf 'space_loop_ensure_nodes: %s is not a block device after mknod\n' \
                "$node" >&2
        made+=("$node")
        created=$((created + 1))
    done
    space_loop__apply_node_permissions "$mode" "$gid" "${made[@]+"${made[@]}"}"
    printf '%s\n' "$created"
}

# Unmasked mode, and the group the runtime's own nodes carry — batched, one exec
# each rather than two per node, because the target now scales with another
# component's high-water mark.
#
# `mknod` masks its mode with the umask, so without the chmod a node lands 0644
# root:root while the snapshot's own are 0660 root:disk, and a non-root service
# gets EACCES on exactly the ones staged here. Best effort: a node at the wrong
# mode is still usable by root, so this is not worth failing the staging over —
# but it is worth saying, because that EACCES is what it looks like downstream.
#
# Its own function because every exit from the staging loop has to reach it. When
# batching first moved these calls after the loop, the two mid-loop bailouts
# returned without them, and a partial staging left every node it had created
# unusable by anything but root — while space_loop_usable_node_count, which
# checks `-b` and not the mode, went on counting them as capacity.
space_loop__apply_node_permissions() { # <mode> <gid> [node...]
    local mode="$1" gid="$2"
    shift 2
    [ "$#" -gt 0 ] || return 0
    space_loop__privileged chmod "$mode" "$@" \
        || printf 'space_loop_ensure_nodes: chmod %s failed for at least one of %s node(s); see its stderr\n' \
            "$mode" "$#" >&2
    space_loop__privileged chown "0:$gid" "$@" \
        || printf 'space_loop_ensure_nodes: chown 0:%s failed for at least one of %s node(s); see its stderr\n' \
            "$gid" "$#" >&2
}

# Attach `file` to a free loop device and print the device.
#
# Retried, because allocation is two steps and the kernel does not hold them
# together: `loop_control_get_free` returns the id of an unbound device *after*
# dropping `loop_ctl_mutex`, and the caller binds it in a second ioctl. Two
# callers racing — four kind nodes attach their caches at once on a cold bring-up — can be
# handed the same index, and the loser's bind fails with EBUSY. Retrying is not
# only about recovering: a lost race reported as pool exhaustion would be the
# same misdiagnosis this file exists to remove.
#
# stderr is left alone so the tool's own message reaches the caller's log.
space_loop_attach() { # space_loop_attach <file>
    local file="$1" bin attempt dev attempts="$SPACE_LOOP_ATTACH_ATTEMPTS"
    # A zero-attempt loop would return failure without ever calling losetup, and
    # the caller would report an exhaustion that was never measured. Decimal-
    # normalized first: `00` matched none of these patterns and `08` is a fatal
    # invalid octal constant, and both ran the body zero times.
    case "$attempts" in
        '' | *[!0-9]*) attempts=1 ;;
        *) attempts="$((10#$attempts))"; [ "$attempts" -ge 1 ] || attempts=1 ;;
    esac
    bin="$(space_loop__losetup_bin)" || return 1
    for ((attempt = 1; attempt <= attempts; attempt++)); do
        if dev="$("$bin" --find --show "$file")" && [ -b "$dev" ]; then
            printf '%s\n' "$dev"
            return 0
        fi
        # An attach that succeeded while printing something that is not a block
        # device would otherwise be retried on top of itself, holding one device
        # per attempt.
        if [ -n "${dev:-}" ]; then
            "$bin" -d "$dev" 2>/dev/null || true
        fi
        if [ "$attempt" -lt "$attempts" ]; then
            # `|| true` because callers run under `set -e`: a bad delay value
            # would otherwise abort a pod entrypoint with an error about sleep
            # instead of about loop devices.
            sleep "$SPACE_LOOP_ATTACH_DELAY" || true
        fi
    done
    return 1
}

# Detach, or — on a mounted device — arm LO_FLAGS_AUTOCLEAR so the kernel
# reclaims it when the filesystem goes away. Exists so callers do not reach for a
# bare `losetup`, which is not on a non-root PATH here.
space_loop_detach() { # space_loop_detach <device>
    local bin
    bin="$(space_loop__losetup_bin)" || return 1
    "$bin" -d "$1"
}

# One line per fact a reader needs to tell an exhausted pool from a missing file.
# Optional listing, so a caller that already read the pool does not read it again.
# The staging every entrypoint does, in one place, called with the caller's own
# logger so each harness keeps its log prefix.
#
# It is one function rather than a wrapper per entrypoint because the three copies
# had already drifted: two reported the pool only on failure and to stderr, the
# third reported unconditionally and to stdout. The failure report belongs on
# stderr, beside the warning that precedes it and any `die` that follows, so the
# lines stay in order in one stream instead of interleaving across two.
#
# A staging failure is deliberately not fatal: the eight preallocated nodes may
# still be enough for this container, and the caller's own mount is what decides.
space_loop_stage_pool() { # <log function>
    local log_fn="$1" created target
    # The effective target, not the configured floor: it covers the highest index
    # already handed out, so a message naming the floor would name a number this
    # run never staged.
    if ! target="$(space_loop_effective_target)"; then
        target="$SPACE_LOOP_DEVICE_NODES"
        "$log_fn" "WARNING: could not compute the loop-device target; staging the floor of $target, which does not cover a higher index a peer may already hold"
    fi
    if created="$(space_loop_ensure_nodes "$target")"; then
        if [ "$created" -gt 0 ]; then
            "$log_fn" "created $created loop device node(s) (pool target $target)"
        fi
        return 0
    fi
    "$log_fn" "WARNING: could not extend the loop-device pool to $target nodes"
    space_loop_report | while IFS= read -r line; do "$log_fn" "$line" >&2; done
    return 0
}

space_loop_report() {
    local max_param cap attached
    max_param="$(space_loop__max_param)"
    cap="$(space_loop_hard_cap)"
    # One `losetup -a` for the whole report: the count and the free count each
    # ran their own before this was hoisted.
    if [ "$#" -gt 0 ]; then
        attached="$1"
    else
        attached="$(space_loop_attached)"
    fi
    printf 'loop pool: %s device nodes visible, %s attached (kernel-global), %s free, %s usable\n' \
        "$(space_loop_node_count)" \
        "$(space_loop_attached_count "$attached")" \
        "$(space_loop_free_node_count "$attached")" \
        "$(space_loop_usable_node_count "$attached")"
    if ! space_loop__losetup_bin >/dev/null; then
        printf 'loop pool: losetup not found, so the attached count above is 0 by default\n'
    fi
    local idx occupants=""
    while read -r idx; do
        [ -n "$idx" ] || continue
        [ -b "$SPACE_LOOP_DEV_DIR/loop$idx" ] && continue
        occupants="$occupants loop$idx"
    done < <(space_loop_node_indices)
    if [ -n "$occupants" ]; then
        printf 'loop pool: counted but not usable (not block devices):%s\n' "$occupants"
    fi
    printf 'loop pool: max_loop=%s (%s)\n' "${max_param:-unknown}" \
        "$(if [ -n "$cap" ]; then
            printf 'specified as %s, so treat it as a hard ceiling' "$cap"
        else
            printf 'nothing specifies it, so it is not a ceiling: the driver allocates on demand'
        fi)"
    if [ -n "$attached" ]; then
        printf 'loop pool: attached (kernel-global, includes other containers):\n'
        printf '%s\n' "$attached" | sed 's/^/  /'
    fi
}

# What to print when an attach failed for want of a device. Takes what was being
# attached so the message names the real constraint instead of the errno.
space_loop_exhaustion_report() {
    local what="${1:-a filesystem image}" free listing
    if ! space_loop__losetup_bin >/dev/null; then
        # Leading with this, because without losetup every count below reads as
        # "nothing attached" and the report would say the pool looks free.
        printf 'loop-device attach failed for %s: losetup is not installed.\n' "$what"
        printf 'Install util-linux; the counts below cannot be read without it.\n'
    fi
    # One read for this line and for the report below it.
    listing="$(space_loop_attached)"
    # The usable count, not the free count: a name an unusable occupant holds is
    # not something the next attach can take, and this line decides whether the
    # reader is told the pool is out.
    free="$(space_loop_usable_node_count "$listing")"
    local exhausted=1
    if [ "$free" -gt 0 ] && space_loop__losetup_bin >/dev/null; then
        exhausted=0
        printf 'loop-device attach failed for %s with %s node(s) apparently free.\n' \
            "$what" "$free"
    else
        printf 'loop-device exhaustion: no free loop device to attach %s.\n' "$what"
    fi
    space_loop_report "$listing"
    # The explanation has to match the branch above. "The node is missing, make
    # more" is right for an exhausted pool and wrong — actively misleading — when
    # the pool still has usable devices, which is the case this whole file exists
    # to stop happening.
    if [ "$exhausted" -eq 1 ]; then
        printf '%s\n' \
            "The errno from losetup is misleading: 'No such file or directory' means" \
            "the kernel handed out an index this container has no /dev/loopN node for," \
            "not that the image is missing. A container's /dev is a tmpfs snapshot taken" \
            "at creation and nothing adds nodes to it later." \
            "Remedy: create more nodes (space_loop_ensure_nodes in" \
            "scripts/lib/space-loop-devices.sh, or mknod /dev/loopN b 7 N) and retry;" \
            "raise the target to change it — SPACE_LOOP_DEVICE_TARGET in a compose" \
            "worker or on the devbox, loopDeviceTarget for the kind node-manager chart."
    else
        printf '%s\n' \
            "The pool is not out, so creating more nodes is not the remedy. Two things" \
            "reach this state: two callers handed the same index by /dev/loop-control," \
            "where the loser's bind fails with EBUSY and a retry settles it; and an" \
            "index whose node exists but which a specified max_loop refuses."
    fi
    printf '%s\n' \
        "max_loop is a ceiling only where it is specified — on the kernel command" \
        "line, or as 'options loop max_loop=N' in a modprobe config — and it gates" \
        "only this autoload path: node-manager's LOOP_CTL_ADD path (loop_devices.rs)" \
        "is ungated, so a node already running can still be given devices."
}
