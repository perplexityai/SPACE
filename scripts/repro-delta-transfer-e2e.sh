#!/usr/bin/env bash
# Run sandbox-manager's delta-transfer round trip against a real
# template-manager in a disposable node root.
#
# Usage, as root:
#   scripts/repro-delta-transfer-e2e.sh TMPLMGR_BIN_DIR TEST_BINARY SCENARIO
#
# TMPLMGR_BIN_DIR holds the space-tmplmgr-svc and space-tmplmgr binaries of
# the template-manager version under test. As REGISTER_DIR:SERVE_DIR, the
# build is registered by one version, which then stops, and another serves the
# same cache: an upgrade in place. TEST_BINARY is pplx-space-sbx-mgr's
# unit-test binary (`cargo test -p pplx-space-sbx-mgr --no-run`). As
# EXPORT_BINARY:IMPORT_BINARY, one sandbox-manager version exports and another,
# with its own state, imports what it sent. SCENARIO is:
#   verified     a build registered with the template-manager CLI; transfers
#                use a delta against it
#   rowless      a complete build dir that appears after startup without a
#                catalog row; transfers send a full rootfs, delta imports are
#                refused
#   unavailable  a registered build, but no template-manager answers;
#                transfers fall back as for rowless
#
# Everything happens in a private mount namespace: /space is a fresh
# loopback btrfs and /etc/group and /etc/passwd gain the node identities. The
# host sees only an empty /space directory and, while it runs, a loop device.
set -euo pipefail

if [ "$#" -ne 3 ]; then
	sed -n '2,26p' "$0" >&2
	exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
	echo "run as root: the scenario mounts a private /space" >&2
	exit 2
fi
if [ -z "${SPACE_DELTA_E2E_IN_NAMESPACE:-}" ]; then
	mkdir -p /space
	SPACE_DELTA_E2E_IN_NAMESPACE=1 exec unshare --mount --propagation private "$0" "$@"
fi

# Never mount anything outside a private mount namespace: the variable above
# can be inherited, the namespace can't.
if [ "$(readlink /proc/self/ns/mnt)" = "$(readlink /proc/1/ns/mnt)" ]; then
	echo "refusing to run outside a private mount namespace" >&2
	exit 2
fi

# Whatever namespace this is, nothing mounted below may propagate out of it.
mount --make-rprivate /

register_bin=$(cd "${1%%:*}" && pwd)
tmplmgr_bin=$(cd "${1##*:}" && pwd)
export_binary=${2%%:*}
import_binary=${2##*:}
scenario=$3
repo=$(cd "$(dirname "$0")/.." && pwd)
export PATH=/usr/sbin:/sbin:$PATH
export SPACE_TEMPLATE_HASH_ALGORITHM=sha256

scratch=$(mktemp -d)
tmplmgr_pid=
mounted=()
cleanup() {
	if [ -n "$tmplmgr_pid" ]; then
		kill "$tmplmgr_pid" 2>/dev/null || true
		wait "$tmplmgr_pid" 2>/dev/null || true
	fi
	# Only what this run mounted, newest first.
	local index
	for ((index = ${#mounted[@]} - 1; index >= 0; index--)); do
		umount "${mounted[index]}" || true
	done
	rm -rf "$scratch"
}
trap cleanup EXIT

truncate -s 4G "$scratch/space.img"
mkfs.btrfs -q "$scratch/space.img"
mount -o loop "$scratch/space.img" /space
mounted+=(/space)

# shellcheck source=../infra/space/identity/node-identities.sh
. "$repo/infra/space/identity/node-identities.sh"
cp /etc/group "$scratch/group"
cp /etc/passwd "$scratch/passwd"
for spec in $SPACE_GROUP_SPECS; do
	echo "${spec%%:*}:x:${spec##*:}:" >>"$scratch/group"
done
for spec in $SPACE_USER_SPECS; do
	IFS=: read -r _ user uid group <<<"$spec"
	gid=$(awk -F: -v g="$group" '$1 == g { print $3 }' "$scratch/group")
	echo "$user:x:$uid:$gid::/nonexistent:/usr/sbin/nologin" >>"$scratch/passwd"
done
mount --bind "$scratch/group" /etc/group
mounted+=(/etc/group)
mount --bind "$scratch/passwd" /etc/passwd
mounted+=(/etc/passwd)

templates=/space/cache/firecracker/templates
socket=/space/run/template-manager/template-manager.sock
mkdir -p "$templates/seed" /space/run/template-manager /space/data \
	/space/cache/firecracker/sandboxes /space/run/firecracker/sandboxes

# Start template-manager and wait until it serves and its startup recovery
# has finished, so a fixture installed afterwards isn't swept by it. A
# stopped predecessor's socket file is removed first, so it can't satisfy the
# wait.
start_tmplmgr() {
	local recovered
	rm -f "$socket"
	recovered=$(grep -c 'startup recovery finished' "$scratch/tmplmgr.log" 2>/dev/null || true)
	"$serving_bin/space-tmplmgr-svc" >>"$scratch/tmplmgr.log" 2>&1 &
	tmplmgr_pid=$!
	for _ in $(seq 1 300); do
		if [ -S "$socket" ] &&
			[ "$(grep -c 'startup recovery finished' "$scratch/tmplmgr.log")" -gt "${recovered:-0}" ]; then
			return 0
		fi
		kill -0 "$tmplmgr_pid" 2>/dev/null || break
		sleep 0.1
	done
	cat "$scratch/tmplmgr.log" >&2
	echo "template-manager did not start" >&2
	exit 1
}

stop_tmplmgr() {
	kill "$tmplmgr_pid"
	wait "$tmplmgr_pid" 2>/dev/null || true
	tmplmgr_pid=
}

# A 64 MiB ext4 rootfs, registered through the template-manager CLI the way
# a local template build registers.
truncate -s 64M "$templates/seed/rootfs.ext4"
mkfs.ext4 -q -F "$templates/seed/rootfs.ext4"
echo '{}' >"$templates/seed/metadata.json"
serving_bin=$register_bin
start_tmplmgr
"$register_bin/space-tmplmgr" register --template seed >/dev/null
build=$(cd "$templates/seed" && ls -d sha256:*)
hash=$build
if [ "$tmplmgr_bin" != "$register_bin" ]; then
	stop_tmplmgr
	serving_bin=$tmplmgr_bin
	start_tmplmgr
fi

case "$scenario" in
verified)
	expect="delta"
	refusal="failed_precondition"
	;;
rowless)
	# Same bytes as a valid build, under a digest the catalog has no row
	# for: a dir only an existence check would accept.
	hash="sha256:$(printf '%064d' 1)"
	cp -a "$templates/seed/$build" "$templates/seed/$hash"
	expect="full"
	refusal="failed_precondition"
	;;
unavailable)
	stop_tmplmgr
	expect="full"
	refusal="unavailable"
	;;
*)
	echo "unknown scenario: $scenario" >&2
	exit 2
	;;
esac

# A template-manager that predates cached_only answers without saying it made
# cached_only's checks, so this PR's sandbox-manager never takes its build as
# a delta base: SPACE_DELTA_E2E_EXPECT=full sets that for such a pairing. It
# answers a rowless digest by trying to pull it, so that refusal is
# unavailable rather than failed_precondition: SPACE_DELTA_E2E_REFUSAL sets it.
expect=${SPACE_DELTA_E2E_EXPECT:-$expect}
refusal=${SPACE_DELTA_E2E_REFUSAL:-$refusal}

echo "scenario=$scenario hash=$hash expect=$expect refusal=$refusal"
echo "template-manager: registered by $register_bin, served by $serving_bin"
echo "sandbox-manager: exported by $export_binary, imported by $import_binary"
# A binary without the test would run zero tests and exit 0.
test_name=service::grpc_sandbox_transfer::delta_e2e_tests::delta_transfers_round_trip_through_a_running_template_manager
for binary in "$export_binary" "$import_binary"; do
	if ! "$binary" --list --exact "$test_name" 2>/dev/null | grep -qx "$test_name: test"; then
		echo "$binary has no $test_name" >&2
		exit 1
	fi
done
run_phase() {
	SPACE_TEST_DELTA_TRANSFER_E2E=1 \
		SPACE_TEST_DELTA_TRANSFER_TEMPLATE=seed \
		SPACE_TEST_DELTA_TRANSFER_HASH="$hash" \
		SPACE_TEST_DELTA_TRANSFER_EXPECT="$expect" \
		SPACE_TEST_DELTA_TRANSFER_REFUSAL="$refusal" \
		SPACE_TEST_DELTA_TRANSFER_HANDOFF="$scratch/handoff" \
		"$@" --exact "$test_name" --nocapture
}
if [ "$export_binary" = "$import_binary" ]; then
	run_phase "$export_binary"
else
	SPACE_TEST_DELTA_TRANSFER_PHASE="export" run_phase "$export_binary"
	SPACE_TEST_DELTA_TRANSFER_PHASE="import" run_phase "$import_binary"
fi
