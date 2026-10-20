#!/usr/bin/env bash
# start-dind.sh — start a side-by-side docker daemon on a roomier filesystem.
#
# WHY: when the host docker daemon's /var/lib/docker partition is full (or the
# only docker available is shared with an unrelated stack you must not touch),
# kind can't pull node images or store image tarballs from `kind load`. This
# script spins up a SECOND dockerd on /run/docker-kind.sock backed by
# /var/lib/docker-kind under whatever filesystem `DIND_DATA_ROOT` resolves to,
# default /home/dev/.docker-kind. Choose a filesystem with enough free space.
# kind picks the side-by-side daemon up via DOCKER_HOST.
#
# Idempotent: re-running while the daemon is up is a no-op.
#
# Usage:
#   sudo bash infra/space/kind/scripts/start-dind.sh             # start
#   sudo bash infra/space/kind/scripts/start-dind.sh --stop      # stop
#   sudo bash infra/space/kind/scripts/start-dind.sh --status    # status
#
# Then in your shell (and in any script that needs to talk to this daemon):
#   export DOCKER_HOST=unix:///run/docker-kind.sock
#
# up.sh, and the Tilt it starts, inherit DOCKER_HOST from the calling env. kind / docker / docker
# buildx CLIs all honor DOCKER_HOST.
set -euo pipefail

DIND_DATA_ROOT="${DIND_DATA_ROOT:-/home/dev/.docker-kind}"
DIND_EXEC_ROOT="${DIND_EXEC_ROOT:-/var/run/docker-kind}"
DIND_SOCK="${DIND_SOCK:-/run/docker-kind.sock}"
DIND_PIDFILE="${DIND_PIDFILE:-/run/docker-kind.pid}"
DIND_LOG="${DIND_LOG:-/var/log/docker-kind.log}"

log() { printf '[start-dind] %s\n' "$*"; }
die() { printf '[start-dind] ERROR: %s\n' "$*" >&2; exit 1; }

require_root() {
  [[ $EUID -eq 0 ]] || die "must run as root (re-invoke with sudo)"
}

daemon_alive() {
  [[ -S "$DIND_SOCK" ]] || return 1
  [[ -f "$DIND_PIDFILE" ]] || return 1
  local pid
  pid=$(cat "$DIND_PIDFILE" 2>/dev/null || echo "")
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start() {
  require_root
  if daemon_alive; then
    log "dind already running (pid $(cat "$DIND_PIDFILE"), socket $DIND_SOCK)"
    log "export DOCKER_HOST=unix://$DIND_SOCK"
    return 0
  fi

  command -v /usr/sbin/dockerd >/dev/null \
    || die "dockerd missing — run install-prereqs.sh first or 'apt install docker.io'"

  mkdir -p "$DIND_DATA_ROOT" "$DIND_EXEC_ROOT" "$(dirname "$DIND_LOG")"

  # This daemon runs INSIDE the dev container, which has its own network
  # namespace independent of whatever the host daemon does, so iptables /
  # ip-forward / ip-masq are safe to enable and required (kind publishes
  # the kube-api port via dockerd's NAT rules; per-cluster bridge containers
  # use MASQUERADE for outbound pulls; `docker build` needs a default bridge
  # for its temporary build containers to do apt-get over the network).
  local opts=(
    --data-root "$DIND_DATA_ROOT"
    --exec-root "$DIND_EXEC_ROOT"
    --host "unix://$DIND_SOCK"
    --pidfile "$DIND_PIDFILE"
    --containerd-namespace docker-kind
    --containerd-plugins-namespace docker-kind-plugins
    --iptables=true
    --ip-forward=true
    --ip-masq=true
    --userland-proxy=false
  )

  log "starting dockerd-kind (data-root=$DIND_DATA_ROOT, sock=$DIND_SOCK)"
  # Clear any stale pidfile dockerd would reject on startup. The --pidfile
  # arg makes dockerd write the real PID itself once initialized.
  rm -f "$DIND_PIDFILE"
  nohup /usr/sbin/dockerd "${opts[@]}" >>"$DIND_LOG" 2>&1 &
  disown

  # Wait up to 60s for the daemon to bind its socket. First-start needs to
  # initialize containerd snapshots under the new data-root, which can take
  # 10-20s on a cold filesystem.
  for i in {1..60}; do
    if [[ -S "$DIND_SOCK" ]] && DOCKER_HOST="unix://$DIND_SOCK" docker info >/dev/null 2>&1; then
      # Side-by-side daemon for local dev; allow any user in the container to
      # talk to it without needing docker-group membership. The host daemon's
      # socket is unaffected.
      chmod 0666 "$DIND_SOCK" || true
      log "dind ready (pid $(cat "$DIND_PIDFILE" 2>/dev/null || echo "?"))"
      log "export DOCKER_HOST=unix://$DIND_SOCK"
      return 0
    fi
    sleep 1
  done
  log "dind did not become ready in 60s; tail of $DIND_LOG:"
  tail -60 "$DIND_LOG" >&2 || true
  die "dind startup failed"
}

stop() {
  require_root
  if ! daemon_alive; then
    log "dind not running"
    return 0
  fi
  local pid
  pid=$(cat "$DIND_PIDFILE")
  log "stopping dind (pid $pid)"
  kill "$pid" 2>/dev/null || true
  for i in {1..15}; do
    kill -0 "$pid" 2>/dev/null || { rm -f "$DIND_PIDFILE"; log "stopped"; return 0; }
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$DIND_PIDFILE"
  log "force-killed"
}

status() {
  if daemon_alive; then
    log "running (pid $(cat "$DIND_PIDFILE"), socket $DIND_SOCK)"
    DOCKER_HOST="unix://$DIND_SOCK" docker version --format 'server: {{.Server.Version}}'
    DOCKER_HOST="unix://$DIND_SOCK" docker info --format 'data-root: {{.DockerRootDir}}'
    DOCKER_HOST="unix://$DIND_SOCK" docker info --format 'containers: {{.Containers}}, images: {{.Images}}'
    return 0
  fi
  log "not running"
  return 1
}

case "${1:-start}" in
  start)  start ;;
  stop)   stop ;;
  status) status ;;
  -h|--help)
    sed -n '2,/^set/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    die "unknown flag: $1"
    ;;
esac
