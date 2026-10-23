#!/usr/bin/env bash
# port-forward-supervisors.sh — recognise and stop the self-restarting
# `port-forward.sh` supervisors of one Kind context, for the bring-up's reap
# and for teardown.
#
# A supervisor runs as
#   bash <…>/port-forward.sh <ctx> <namespace> <target> <mapping> space-kind-pf:<ctx>
# (the Tiltfile's observability forwards, and those the retired scripted
# bring-up left under `setsid`, recorded in its pidfile). A process is one only
# when its own command line has exactly that shape: a label alone, or a pid in
# a pidfile, can belong to anything — a search whose last argument is the
# label, or an unrelated process that reused a recorded pid.
#
# Sourced, never executed.

# The pidfile the retired scripted bring-up recorded its supervisors in.
space_kind_pf_pidfile() {
  printf '%s\n' "${SPACE_PORT_FORWARD_PIDFILE:-/tmp/space-local-pf.pids}"
}

# Whether pid $1 is a supervisor for context $2, or for any context when $2 is
# empty.
space_kind_pf_is_supervisor() {
  local pid="$1" ctx="${2:-}" command
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  command="$(ps -o command= -p "$pid" 2>/dev/null)" || return 1
  [[ "$command" =~ ^[[:space:]]*([^[:space:]]*/)?bash[[:space:]]+([^[:space:]]*/)?port-forward\.sh[[:space:]]+([^[:space:]]+)([[:space:]]+[^[:space:]]+){3}[[:space:]]+space-kind-pf:([^[:space:]]+)[[:space:]]*$ ]] \
    || return 1
  [[ "${BASH_REMATCH[3]}" == "${BASH_REMATCH[5]}" && ( -z "$ctx" || "${BASH_REMATCH[3]}" == "$ctx" ) ]]
}

# Stop supervisor $1 of context $2 (verified by the caller), then those of its
# own children that are `kubectl … --context <ctx> … port-forward`. The loop
# first, so it cannot start another kubectl.
space_kind_pf_stop_supervisor() {
  local pid="$1" ctx="$2" children child command
  children="$(ps -A -o pid=,ppid= 2>/dev/null | awk -v p="$pid" '$2 == p { print $1 }')"
  kill "$pid" 2>/dev/null || true
  for child in $children; do
    command="$(ps -o command= -p "$child" 2>/dev/null)" || continue
    [[ "$command" =~ ^[[:space:]]*([^[:space:]]*/)?kubectl[[:space:]].*--context[[:space:]=]${ctx}([[:space:]]|$).*port-forward ]] \
      || continue
    kill "$child" 2>/dev/null || true
  done
}
