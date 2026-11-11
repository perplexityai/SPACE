#!/usr/bin/env bash
# Smoke and benchmark the net-gateway TCP proxy, optionally comparing two revs.
#
#   run.sh                                    # smoke + bench the working tree
#   run.sh --rev HEAD~1 --rev HEAD            # before/after, with a comparison
#   run.sh --smoke-only --scale 10            # quick correctness pass
#   run.sh --workloads bulk-echo,tls-sni      # one or a few workloads
#
# Each rev gets its own harness binary (see build-rev.sh), then per rev:
#   1. start the upstream and the gateway in their namespaces
#   2. prove the gateway is enforcing: a deny-all gateway must refuse the upstream
#   3. smoke every workload once
#   4. bench every workload --iterations times, writing raw samples to JSON
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)

REVS=()
ITERATIONS=5
WARMUP=1
SCALE=100
WORKLOADS=""
SMOKE_ONLY=0
OUT_DIR=${OUT_DIR:-/tmp/netgtw-bench}
KEEP_TOPOLOGY=0

# The namespace names and the upstream address come from topology.sh, which owns
# them. These are this script's own.
PROTOCOL_PORT=9000
RAW_ECHO_PORT=9001
GUARDRAILED=169.254.169.254:80
READY_TIMEOUT=100

die() { echo "ERROR: $*" >&2; exit 1; }
note() { echo "==> $*"; }

while (($#)); do
  case "$1" in
    --rev) REVS+=("${2:?--rev needs a value}"); shift 2 ;;
    --iterations) ITERATIONS=${2:?}; shift 2 ;;
    --warmup) WARMUP=${2:?}; shift 2 ;;
    --scale) SCALE=${2:?}; shift 2 ;;
    --workloads) WORKLOADS=${2:?}; shift 2 ;;
    --out-dir) OUT_DIR=${2:?}; shift 2 ;;
    --smoke-only) SMOKE_ONLY=1; shift ;;
    --keep-topology) KEEP_TOPOLOGY=1; shift ;;
    -h|--help) sed -n '2,13p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ $(id -u) == 0 ]] || die "must run as root (try: sudo -E $0 $*)"
((${#REVS[@]} <= 2)) || die "at most two --rev arguments (baseline then candidate)"
command -v nstat >/dev/null || die "nstat is required (iproute2); the run asserts on kernel drop counters"

SELECT=()
[[ -n "$WORKLOADS" ]] && SELECT=(--workloads "$WORKLOADS")

# Everything here needs root — namespaces, nftables, IP_TRANSPARENT — but cargo
# and git must not have it, or the build leaves root-owned artifacts in the
# developer's target directory and worktree. PATH is carried across because
# sudo's secure_path would drop ~/.cargo/bin.
as_builder() {
  if [[ -n "${SUDO_USER:-}" ]]; then
    sudo -u "$SUDO_USER" -H env PATH="$PATH" "$@"
  else
    "$@"
  fi
}

as_builder mkdir -p "$OUT_DIR"

# --- harness binaries -------------------------------------------------------

BINARIES=()
LABELS=()
if ((${#REVS[@]} == 0)); then
  note "building the working tree"
  as_builder cargo build --manifest-path "$REPO/Cargo.toml" --release \
    -p pplx-space-net-gateway-bench >&2
  BINARIES+=("$REPO/target/release/netgtw-bench")
  LABELS+=("working-tree")
else
  for rev in "${REVS[@]}"; do
    BINARIES+=("$(as_builder "$SCRIPT_DIR/build-rev.sh" "$rev" "$OUT_DIR")")
    LABELS+=("$(as_builder git -C "$REPO" rev-parse --short "$rev")")
  done
fi

# --- topology ---------------------------------------------------------------

UPSTREAM_PID=""
GATEWAY_PID=""
# The shell jobs. `ip netns exec` may or may not exec into the binary in place,
# so a launcher pid is not reliably the gateway's — kill both, or a surviving
# gateway holds the proxy port and the next rev fails to bind.
ROLE_JOBS=()

stop_roles() {
  local pid
  for pid in "$GATEWAY_PID" "$UPSTREAM_PID" ${ROLE_JOBS[@]+"${ROLE_JOBS[@]}"}; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
  done
  for pid in ${ROLE_JOBS[@]+"${ROLE_JOBS[@]}"}; do
    wait "$pid" 2>/dev/null || true
  done
  GATEWAY_PID=""
  UPSTREAM_PID=""
  ROLE_JOBS=()
}

cleanup() {
  local rc=$?
  set +e
  stop_roles
  ((KEEP_TOPOLOGY)) || "$SCRIPT_DIR/topology.sh" down >/dev/null
  # The roles wrote their logs and result files as root; hand them back so they
  # can be re-rendered and deleted without sudo.
  [[ -n "${SUDO_USER:-}" ]] && chown -R "$SUDO_USER" "$OUT_DIR" 2>/dev/null
  exit "$rc"
}
trap cleanup EXIT

note "building the namespaces"
TOPOLOGY=$("$SCRIPT_DIR/topology.sh" up)
sed 's/^/    /' <<<"$TOPOLOGY"
topo() {
  local value
  value=$(sed -n "s/^$1=//p" <<<"$TOPOLOGY")
  [[ -n "$value" ]] || die "topology.sh did not report $1"
  printf '%s' "$value"
}
CLIENT_NS=$(topo client_ns)
GW_NS=$(topo gw_ns)
UPSTREAM_NS=$(topo upstream_ns)
UPSTREAM_IP=$(topo upstream_ip)

# start_roles <binary> <policy>
# Leaves GATEWAY_PID set to the gateway process, which the client samples.
start_roles() {
  local binary=$1 policy=$2
  local ready="$OUT_DIR/gateway.ready"
  rm -f "$ready"

  ip netns exec "$UPSTREAM_NS" "$binary" serve-upstream \
    --port "$PROTOCOL_PORT" --raw-echo-port "$RAW_ECHO_PORT" >"$OUT_DIR/upstream.log" 2>&1 &
  UPSTREAM_PID=$!
  ROLE_JOBS+=("$UPSTREAM_PID")

  ip netns exec "$GW_NS" "$binary" serve-gateway \
    --policy "$policy" --ready-file "$ready" >"$OUT_DIR/gateway.log" 2>&1 &
  local launcher=$!
  ROLE_JOBS+=("$launcher")

  for _ in $(seq 1 "$READY_TIMEOUT"); do
    [[ -f "$ready" ]] && break
    kill -0 "$launcher" 2>/dev/null || { tail -20 "$OUT_DIR/gateway.log" >&2; die "gateway exited during startup"; }
    sleep 0.1
  done
  [[ -f "$ready" ]] || { tail -20 "$OUT_DIR/gateway.log" >&2; die "gateway was not ready in time"; }

  # The pid comes from the gateway itself rather than from $!, which under
  # `ip netns exec` is the launcher, not always the process being sampled.
  GATEWAY_PID=$(sed -n 's/.*"pid":\([0-9]*\).*/\1/p' "$ready")
  [[ -n "$GATEWAY_PID" ]] || die "could not read the gateway pid from $ready"
}

# drive <binary> <mode> [extra args...]
# Carries only the flags every invocation shares; --denied and --scale are the
# caller's, because the enforcement check varies both.
drive() {
  local binary=$1 mode=$2; shift 2
  ip netns exec "$CLIENT_NS" "$binary" "$mode" \
    --upstream "$UPSTREAM_IP:$PROTOCOL_PORT" \
    --raw-echo "$UPSTREAM_IP:$RAW_ECHO_PORT" \
    --gateway-pid "$GATEWAY_PID" \
    "$@"
}

# Handshakes the kernel dropped in this namespace because an accept queue was
# full. Absolute, so successive reads subtract.
listen_overflows() {
  ip netns exec "$1" nstat -az 2>/dev/null |
    awk '/TcpExtListenOverflows/ {print $2; found = 1} END {if (!found) print 0}'
}

# Assert the harness did not become the bottleneck during the measured run. An
# overflowing accept queue does not error: the kernel drops the handshake, the
# client retransmits a second later, and the workload reports that second as the
# gateway's latency. The concurrency workloads are sized against these queues,
# so a non-zero count means the sizing no longer holds and the numbers from this
# run are not the gateway's.
#
# Zero is the expectation rather than a threshold because the enforcement check
# and the smoke pass have already run every workload once by this point, and
# --warmup covers the rest of the cold start.
assert_no_dropped_handshakes() {
  local phase=$1 gw_before=$2 upstream_before=$3
  local gw upstream
  gw=$(( $(listen_overflows "$GW_NS") - gw_before ))
  upstream=$(( $(listen_overflows "$UPSTREAM_NS") - upstream_before ))
  note "dropped handshakes during $phase: gateway=$gw upstream=$upstream"
  ((gw == 0 && upstream == 0)) ||
    die "accept queues overflowed during $phase (gateway=$gw upstream=$upstream); \
the concurrency workloads outran a listener's backlog, so this run's latencies \
include kernel retransmits rather than the gateway's cost"
}

# The enforcement check. A gateway holding a default-deny policy must refuse the
# upstream the allow-path workloads use, so `--denied` is aimed at it. Without
# this, a harness that had silently stopped intercepting would pass every
# workload — traffic would simply be reaching the destination directly.
prove_enforcement() {
  local binary=$1
  note "checking the gateway enforces: deny-all must refuse $UPSTREAM_IP:$PROTOCOL_PORT"
  start_roles "$binary" deny-all
  drive "$binary" smoke --workloads denied-destination \
    --denied "$UPSTREAM_IP:$PROTOCOL_PORT" --scale 10 ||
    die "a deny-all gateway carried traffic to the upstream; the harness is not measuring enforcement"
  stop_roles
}

# --- run --------------------------------------------------------------------

RESULTS=()
for index in "${!BINARIES[@]}"; do
  binary=${BINARIES[$index]}
  label=${LABELS[$index]}
  echo
  note "$label  ($binary)"

  prove_enforcement "$binary"
  start_roles "$binary" allow-all

  note "smoke"
  drive "$binary" smoke --denied "$GUARDRAILED" --scale "$SCALE" \
    "${SELECT[@]+"${SELECT[@]}"}"

  if ((!SMOKE_ONLY)); then
    result="$OUT_DIR/$label.json"
    note "bench: $ITERATIONS iterations per workload"
    gw_overflows=$(listen_overflows "$GW_NS")
    upstream_overflows=$(listen_overflows "$UPSTREAM_NS")
    drive "$binary" bench --denied "$GUARDRAILED" --scale "$SCALE" \
      --iterations "$ITERATIONS" --warmup "$WARMUP" \
      --label "$label" --out "$result" "${SELECT[@]+"${SELECT[@]}"}"
    assert_no_dropped_handshakes "$label" "$gw_overflows" "$upstream_overflows"
    RESULTS+=("$result")
  fi

  stop_roles
done

if ((${#RESULTS[@]} == 2)); then
  echo
  note "comparison"
  "${BINARIES[1]}" report --baseline "${RESULTS[0]}" --candidate "${RESULTS[1]}"
fi
