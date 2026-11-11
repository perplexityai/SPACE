#!/usr/bin/env bash
# Build (or tear down) the three network namespaces the net-gateway bench runs in.
#
#   [client]  198.18.0.2 --veth-- 198.18.0.1  [gw]  198.18.1.1 --veth-- 198.18.1.2  [upstream]
#     guest                     tap0                 up0                    eth0
#
# The client's default route points at the gw namespace, so its TCP goes in on
# tap0 and hits the gw namespace's prerouting hook — which is the only place
# TPROXY can run, and the reason the client needs a namespace of its own rather
# than being a process next to the gateway. The gateway then dials the real
# destination out of up0, its socket carrying OUTBOUND_MARK so its own rules
# leave it alone.
#
# The nft table and the policy routing mirror `setup_netgtw_tproxy_rules` and
# `setup_tproxy` in crates/l40_api/node/mgr/server (same table, chains, priorities,
# marks, and rule order), with one deliberate omission: the pre-TPROXY guardrail
# chain that drops platform hard-deny CIDRs before the gateway sees them. This
# harness wants the gateway to make that decision so the deny path is what gets
# measured, not an nft drop.
#
# Usage: topology.sh up | down | status
set -euo pipefail

CLIENT_NS=${CLIENT_NS:-ngb-client}
GW_NS=${GW_NS:-ngb-gw}
UPSTREAM_NS=${UPSTREAM_NS:-ngb-upstream}

# RFC 2544 benchmarking range: routable-looking, and clear of every CIDR in
# PLATFORM_EGRESS_HARD_DENY_CIDRS, so an allow-all policy really does allow it.
CLIENT_IP=198.18.0.2
GW_CLIENT_IP=198.18.0.1
GW_UPSTREAM_IP=198.18.1.1
UPSTREAM_IP=198.18.1.2

# Must match pplx_space_net_gateway_client.
TCP_PROXY_PORT=15001
UDP_PROXY_PORT=15002
TPROXY_MARK=1
OUTBOUND_MARK=2
TPROXY_RT_TABLE=100

# The gateway runs as root here; production passes the per-sandbox uid.
GW_UID=${GW_UID:-0}

die() { echo "ERROR: $*" >&2; exit 1; }
in_ns() { ip netns exec "$@"; }

require_root() {
  [[ $(id -u) == 0 ]] || die "must run as root (try: sudo -E $0 $*)"
  command -v nft >/dev/null || die "nftables is required"
}

# A veth pair, one end in each namespace, each named as the namespace wants it.
# Created under scratch names first: `ip link add` puts both ends in the current
# namespace before moving them, so building one directly as `eth0` collides with
# the host's own eth0.
link_pair() {
  local left_ns=$1 left_name=$2 right_ns=$3 right_name=$4
  ip link add ngb-scratch0 type veth peer name ngb-scratch1
  ip link set ngb-scratch0 netns "$left_ns"
  ip link set ngb-scratch1 netns "$right_ns"
  in_ns "$left_ns" ip link set ngb-scratch0 name "$left_name"
  in_ns "$right_ns" ip link set ngb-scratch1 name "$right_name"
}

up() {
  down_quiet

  for ns in "$CLIENT_NS" "$GW_NS" "$UPSTREAM_NS"; do
    ip netns add "$ns"
    in_ns "$ns" ip link set lo up
  done

  # client <-> gw. The gw end is named tap0 because that is the guest-facing
  # device name the production nft rules match on.
  link_pair "$CLIENT_NS" eth0 "$GW_NS" tap0
  in_ns "$CLIENT_NS" ip addr add "$CLIENT_IP/30" dev eth0
  in_ns "$CLIENT_NS" ip link set eth0 up
  in_ns "$CLIENT_NS" ip route add default via "$GW_CLIENT_IP"
  in_ns "$GW_NS" ip addr add "$GW_CLIENT_IP/30" dev tap0
  in_ns "$GW_NS" ip link set tap0 up

  # gw <-> upstream
  link_pair "$GW_NS" up0 "$UPSTREAM_NS" eth0
  in_ns "$GW_NS" ip addr add "$GW_UPSTREAM_IP/30" dev up0
  in_ns "$GW_NS" ip link set up0 up
  in_ns "$UPSTREAM_NS" ip addr add "$UPSTREAM_IP/30" dev eth0
  in_ns "$UPSTREAM_NS" ip link set eth0 up

  # TPROXY hands the packet to a local transparent socket while leaving its
  # destination address untouched; that only routes if the marked packet is
  # looked up in a table that treats every address as local.
  in_ns "$GW_NS" ip rule add fwmark "$TPROXY_MARK" lookup "$TPROXY_RT_TABLE"
  in_ns "$GW_NS" ip route replace local default dev lo table "$TPROXY_RT_TABLE"

  # Reverse-path filtering would drop the intercepted packets, whose source is
  # not on the interface they arrived on once the gateway replies as the
  # destination.
  in_ns "$GW_NS" sysctl -qw net.ipv4.conf.all.rp_filter=0
  in_ns "$GW_NS" sysctl -qw net.ipv4.conf.tap0.rp_filter=0
  # Forwarding stays off: nothing may reach the upstream except through the
  # gateway's own sockets, so a flow the gateway denies cannot leak.
  in_ns "$GW_NS" sysctl -qw net.ipv4.ip_forward=0

  # Both the client and the gateway dial from a single address, so the
  # connection-churn workloads are bounded by ephemeral ports against
  # TIME_WAIT's fixed 60s. Left at the defaults, a long iteration count first
  # inflates connect latency and then fails outright with EADDRNOTAVAIL — a
  # property of the harness that would read as a regression in the gateway.
  for ns in "$CLIENT_NS" "$GW_NS"; do
    in_ns "$ns" sysctl -qw net.ipv4.ip_local_port_range="1024 65000"
    in_ns "$ns" sysctl -qw net.ipv4.tcp_tw_reuse=1
  done

  in_ns "$GW_NS" nft -f - <<EOF
table ip proxy {
  chain prerouting {
    type filter hook prerouting priority -150; policy accept;
    meta mark $OUTBOUND_MARK return
    meta skuid $GW_UID return
    iifname "tap0" ct state established,related ct direction reply return
    iifname "tap0" meta l4proto tcp tproxy to :$TCP_PROXY_PORT meta mark set $TPROXY_MARK accept
    iifname "tap0" meta l4proto udp tproxy to :$UDP_PROXY_PORT meta mark set $TPROXY_MARK accept
    iifname "tap0" return
  }
  chain divert {
    type filter hook prerouting priority -149; policy accept;
    meta l4proto tcp socket transparent 1 meta mark set $TPROXY_MARK accept
    meta l4proto udp socket transparent 1 meta mark set $TPROXY_MARK accept
  }
}
EOF

  # One `key=value` per line: this is the interface run.sh reads, so neither
  # script has to carry its own copy of the names and addresses.
  cat <<EOF
client_ns=$CLIENT_NS
gw_ns=$GW_NS
upstream_ns=$UPSTREAM_NS
upstream_ip=$UPSTREAM_IP
EOF
}

down_quiet() {
  for ns in "$CLIENT_NS" "$GW_NS" "$UPSTREAM_NS"; do
    ip netns pids "$ns" 2>/dev/null | xargs -r kill 2>/dev/null || true
  done
  sleep 0.2
  for ns in "$CLIENT_NS" "$GW_NS" "$UPSTREAM_NS"; do
    ip netns pids "$ns" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    ip netns delete "$ns" 2>/dev/null || true
  done
  # Deleting a namespace takes its links with it, but a crash between creating a
  # pair and moving it leaves these behind in the host namespace, where the next
  # `up` collides with them.
  for scratch in ngb-scratch0 ngb-scratch1; do
    ip link delete "$scratch" 2>/dev/null || true
  done
}

status() {
  for ns in "$CLIENT_NS" "$GW_NS" "$UPSTREAM_NS"; do
    if ip netns list | grep -qw "$ns"; then
      echo "== $ns"
      in_ns "$ns" ip -brief addr show | sed 's/^/   /'
    else
      echo "== $ns (absent)"
    fi
  done
  if ip netns list | grep -qw "$GW_NS"; then
    echo "== $GW_NS nft"
    in_ns "$GW_NS" nft list table ip proxy | sed 's/^/   /'
  fi
}

case "${1:-}" in
  up) require_root "$@"; up ;;
  down) require_root "$@"; down_quiet; echo "torn down" ;;
  status) require_root "$@"; status ;;
  *) die "usage: $0 up|down|status" ;;
esac
