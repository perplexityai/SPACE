#!/usr/bin/env bash
# Build as the caller; all privileged operations run in disposable namespaces.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
export PATH="$PATH:/usr/sbin:/sbin"

if [[ ${1:-} == __isolated ]]; then
  [[ $$ == 1 ]] || { echo 'Internal stage requires the disposable PID namespace.' >&2; exit 1; }
  shift
  output=$1 gateway=$2 control=$3 owner=$4 relay=$5 python=$6
  shift 6
  # Mount propagation must be private before replacing /run or /space. The
  # latter may be a devbox symlink; the mount still exists only in this child.
  mount --make-rprivate /
  mount -t tmpfs -o mode=755 tmpfs /run
  mkdir -p /run/netns "$output/runtime/space"
  if [[ ! -d /space ]]; then
    # CI images provide the mountpoint. Never create it in the host filesystem.
    echo 'Missing /space mountpoint; use the SPACE CI image or SPACE devbox.' >&2
    exit 1
  fi
  mount --bind "$output/runtime/space" /space
  cleanup() {
    status=$?
    trap - EXIT INT TERM
    bash "$SCRIPT_DIR/../bench/topology.sh" down >>"$output/topology.log" 2>&1 || true
    umount /space
    rm -rf -- "$output/runtime"
    chown -R "$owner" "$output"
    exit "$status"
  }
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  bash "$SCRIPT_DIR/../bench/topology.sh" up >"$output/topology.log" 2>&1
  # Exercise the packaged early guardrails omitted by the throughput topology.
  ip netns exec ngb-gw nft add table ip sandbox
  ip netns exec ngb-gw nft 'add chain ip sandbox pre_tproxy_guardrails { type filter hook prerouting priority -175; }'
  python3 -c 'import pathlib,sys; print("{\"nftables\":[" + pathlib.Path(sys.argv[1]).read_text() + "]}")' \
    "$REPO_ROOT/docker/space/nftables/sandbox-pre-tproxy-guardrails.json" | ip netns exec ngb-gw nft -j -f -
  ip -n ngb-gw route add default via 198.18.1.2
  for address in 198.18.2.2 198.18.53.53 1.1.1.1 8.8.8.8 169.254.169.254; do
    ip -n ngb-upstream addr add "$address/32" dev lo
  done
  ip -n ngb-client addr add 169.254.0.21/30 dev eth0
  ip -n ngb-gw addr add 169.254.0.22/30 dev tap0
  export SPACE_TRAFFIC_ISOLATED=1 TOKIO_WORKER_THREADS=2
  # A background child plus wait lets PID 1 handle cancellation immediately.
  "$python" "$SCRIPT_DIR/run.py" --gateway "$gateway" --control "$control" --output "$output" \
    --relay-socket "$relay" "$@" &
  wait "$!"
  exit
fi

for argument in "$@"; do
  if [[ $argument == --list || $argument == --help || $argument == -h ]]; then
    exec python3 "$SCRIPT_DIR/run.py" "$@"
  fi
done
# Fail schema errors and empty selections before spending time on a build.
python3 "$SCRIPT_DIR/run.py" --list "$@"

for tool in cargo python3 uv openssl ip nft sysctl unshare mount timeout; do
  command -v "$tool" >/dev/null || { echo "Missing dependency: $tool" >&2; exit 1; }
done
cd "$REPO_ROOT"
uv_bin=$(command -v uv)
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/space-uv-cache}"
environment="$REPO_ROOT/target/gateway-traffic/venv"
if [[ ! -x $environment/bin/python ]]; then
  "$uv_bin" venv --python python3 "$environment"
fi
"$uv_bin" pip sync --python "$environment/bin/python" "$SCRIPT_DIR/requirements.txt"
# Match the other Rust jobs' cache normalization across Buildkite checkouts.
export SCCACHE_BASEDIRS="${SCCACHE_BASEDIRS:-$REPO_ROOT}"
target_dir=${CARGO_TARGET_DIR:-$REPO_ROOT/target}
if [[ ${NETGTW_TRAFFIC_SKIP_BUILD:-0} != 1 ]]; then
  # One invocation compiles the shared graph once. Unifying with the fixture
  # adds only net-common/serde and rcgen/pem to the gateway's graph; the
  # production worker image already builds the gateway with both.
  cargo build --locked -p pplx-space-net-gateway-svc -p pplx-space-netgtw-bench \
    --bin space-netgtw-svc --bin netgtw-traffic-control
  if [[ ${RUSTC_WRAPPER:-} == sccache ]]; then
    sccache --show-stats || true
    # CI decides from these whether main refreshes the cache snapshot.
    if [[ -n ${SPACE_CI_SCCACHE_STATS:-} ]]; then
      sccache --show-stats --stats-format=json >"${SPACE_CI_SCCACHE_STATS}" || true
    fi
  fi
fi
gateway=$(realpath "${NETGTW_TRAFFIC_GATEWAY:-$target_dir/debug/space-netgtw-svc}")
control=$(realpath "${NETGTW_TRAFFIC_CONTROL:-$target_dir/debug/netgtw-traffic-control}")
[[ -x $gateway && -x $control ]] || { echo 'Gateway/control executable missing.' >&2; exit 1; }
mkdir -p "$REPO_ROOT/target/gateway-traffic"
output=$(mktemp -d "$REPO_ROOT/target/gateway-traffic/run.XXXXXXXX")
owner=$(id -u):$(id -g)
echo "Traffic artifacts: $output"
root=()
if [[ $(id -u) != 0 ]]; then
  root=(sudo)
fi
# Only reserved DNS-over-TLS leaves the isolated topology. This unprivileged
# relay copies encrypted bytes to the two production resolvers without changing
# the gateway's TLS trust or exposing a listener on the host network.
relay_dir=$(mktemp -d /tmp/space-traffic-relay.XXXXXXXX)
relay_pid= namespace_pid=
cleanup_outer() {
  status=$?
  trap - EXIT INT TERM
  for child in "$namespace_pid" "$relay_pid"; do
    if [[ -n $child ]]; then
      kill "$child" 2>/dev/null || true
      wait "$child" 2>/dev/null || true
    fi
  done
  rm -rf -- "$relay_dir"
  exit "$status"
}
trap cleanup_outer EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 "$SCRIPT_DIR/peer.py" relay --socket "$relay_dir/relay.sock" >"$output/relay.log" 2>&1 &
relay_pid=$!
# Binding takes well under a second on an idle host. The budget is for a runner
# still loaded by the builds above, where a 2 s budget once expired with the
# relay alive; the loop returns as soon as the socket exists.
relay_startup_seconds=30
relay_deadline=$((SECONDS + relay_startup_seconds))
until [[ -S $relay_dir/relay.sock ]]; do
  if ! kill -0 "$relay_pid" 2>/dev/null; then
    wait "$relay_pid" && relay_status=0 || relay_status=$?
    relay_pid=
    echo "DNS relay exited with status $relay_status before binding. relay.log:" >&2
    cat "$output/relay.log" >&2
    exit 1
  fi
  if ((SECONDS >= relay_deadline)); then
    echo "DNS relay did not bind within ${relay_startup_seconds}s. relay.log:" >&2
    cat "$output/relay.log" >&2
    exit 1
  fi
  sleep 0.02
done
# PID namespace teardown kills every descendant, even if the harness crashes.
# The outer network namespace also makes topology.sh's fixed names safe for
# concurrent runs and prevents its cleanup from touching host resources.
# unshare ignores TERM while waiting for PID 1. timeout forwards cancellation to
# the whole child process group, with a five-second bound on stuck teardown;
# duration zero imposes no wall-clock limit on an otherwise healthy run.
"${root[@]}" timeout --kill-after=5s 0 unshare --mount --net --pid --fork --kill-child=TERM --mount-proc \
  bash "$SCRIPT_DIR/run.sh" __isolated "$output" "$gateway" "$control" "$owner" "$relay_dir/relay.sock" "$environment/bin/python" "$@" &
namespace_pid=$!
wait "$namespace_pid"
namespace_pid=
