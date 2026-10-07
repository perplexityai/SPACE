#!/bin/bash
# node-stack-start.sh — entrypoint for the local kind node-stack container.
#
# Boots a production worker or builder service set against real Firecracker:
# runtime-supervisor, node-manager, volume-manager, credential-manager,
# template-manager, sandbox-manager, node-ctl, and node-api, plus build-manager
# for the builder role and the read-only runtime manager — the same binaries, features, and unix identities as
# the production node DaemonSets. sandbox-manager launches the per-sandbox
# net-gateway through node-manager when a VM starts. The pod runs every
# node service in one container, so this script also stands in for the
# per-service DaemonSet init containers production runs: the services' own
# directories + ownership (node-manager-init.sh, the node-ctl / volume-manager
# chart inits), the guest-kernel staging, and the /space/etc files. What the
# node image provides in production — the btrfs /space/cache, the cgroup
# subtree, loop-device headroom, device permissions and the packer directory
# skeleton — the kind node provides, from the units baked into the SPACE Kind
# node image (infra/space/kind/node/Dockerfile).
#
# Node registration is NOT done here. The node publishes itself through the
# production path: node-ctl's outbound reconcilers call
# ClusterApi.PublishNodeState on space-internal-api (SPACE_INTERNAL_API_ENDPOINT),
# which derives the node id from (kube node name, role) and upserts the
# nodes/node_k8s/node_status_snapshots/node_template_inventory rows.
# Templates are NOT baked or seeded here either. template-manager's prefetch
# resolves `seed:latest` via ClusterApi.FindTemplateRelease (published by the
# space-seed-publish Job) and pulls the artifacts through volume-manager.
set -euo pipefail

. /opt/space/etc/node-identities.sh

LOG_DIR="/space/logs"
# Logs live in a pod-scoped emptyDir so a container retry cannot erase recovery
# evidence. PIDs must stay container-local: a previous attempt's PID may have
# been reused by an unrelated process in the shared host PID namespace.
PID_DIR="/run/space-node-stack"
# Services whose exit must not restart the node's service set; they restart
# on their own and supervise_services does not watch them.
INDEPENDENT_PID_DIR="$PID_DIR/independent"
NODE_MANAGER_SOCKET="/space/run/node-manager/node-manager.sock"
SBXMGR_SOCKET="/space/run/sandbox-manager/sandbox-manager.sock"
TMPLMGR_SOCKET="/space/run/template-manager/template-manager.sock"
VOLMGR_SOCKET="/space/run/volume-manager/volume-manager.sock"
CREDMGR_SOCKET="/space/run/credential-manager/credmgr.sock"
RUNTIME_SUPERVISOR_SOCKET="/space/run/runtime-supervisor/runtime-supervisor.sock"
RUNTIME_MANAGER_SOCKET="/space/run/runtime-manager/runtime-manager.sock"

log() { printf '[node-stack] %s\n' "$*"; }
die() { printf '[node-stack] ERROR: %s\n' "$*" >&2; exit 1; }

require_env() {
  local name
  for name in "$@"; do
    [[ -n "${!name:-}" ]] || die "required env var not set: $name"
  done
}

# SPACE_NODE_ID is the raw kube node name (fieldRef spec.nodeName), exactly
# like the prod node-service DaemonSets. Services and the cluster API derive
# the UUID from (name, role) via space_k8s_node_id; nothing here hand-rolls
# a node identity.
load_node_identity() {
  require_env SPACE_NODE_ID SPACE_NODE_ROLE SPACE_INTERNAL_TOKEN \
    SPACE_INTERNAL_CONTROL_TOKEN SPACE_INTERNAL_API_ENDPOINT
  case "$SPACE_NODE_ROLE" in
    worker|builder) ;;
    *) die "SPACE_NODE_ROLE must be worker or builder (got: $SPACE_NODE_ROLE)" ;;
  esac

  # ── Legacy DirectSubstrate registration (3486 bench only) ──────────────────
  # The single-worker `space-local-3486` bench runs the api-server on
  # DirectSubstrate, which schedules off nodes/node_direct rows and has no
  # real registration RPC (production uses the Kubernetes substrate, where the
  # pod reflector + node-ctl publish cover discovery). When the chart opts in,
  # fall back to the bench-era identity (md5 uuid of the node name) and register
  # the node_direct endpoint by SQL after node-api is serving. Requires the
  # chart to render POSTGRES_CONNECTION_STRING; never enabled on the
  # production-shaped `space-local` stack.
  DIRECT_REGISTRATION="${SPACE_DIRECT_SUBSTRATE_REGISTRATION:-0}"
  if [[ "$DIRECT_REGISTRATION" == "1" ]]; then
    [[ "$SPACE_NODE_ROLE" == "worker" ]] \
      || die "DirectSubstrate registration is supported only for the worker role"
    require_env POSTGRES_CONNECTION_STRING SPACE_NODE_API_ENDPOINT
    local hash
    hash=$(printf '%s' "$SPACE_NODE_ID" | md5sum | awk '{print $1}')
    SPACE_NODE_ID="${hash:0:8}-${hash:8:4}-${hash:12:4}-${hash:16:4}-${hash:20:12}"
    export SPACE_NODE_ID
    log "DirectSubstrate registration enabled (bench mode); node_id=$SPACE_NODE_ID"
  fi
}

# ── /space/cache: the node's btrfs cache ─────────────────────────────────────
# sandbox-manager clones template rootfs into each sandbox's instance dir with
# btrfs subvolume snapshots, so /space/cache MUST be btrfs. As on a production
# node, the node provides it: the kind node's space-cache.service (baked into
# the SPACE Kind node image, infra/space/kind/node) mounts a loopback btrfs image
# on the node's /space/cache before kubelet starts, and the chart passes it in
# HostToContainer. A cache the node mounts after this pod started still
# propagates in, which is why this waits briefly before failing.
CACHE_WAIT_SECONDS="${SPACE_CACHE_WAIT_SECONDS:-60}"
require_node_cache() {
  local waited=0
  until findmnt -n -t btrfs --mountpoint /space/cache >/dev/null 2>&1; do
    if (( waited == 0 )); then
      log "waiting up to ${CACHE_WAIT_SECONDS}s for the node's btrfs /space/cache"
    fi
    if (( waited >= CACHE_WAIT_SECONDS )); then
      die "/space/cache is not a btrfs mount ($(findmnt -n -o SOURCE,FSTYPE -T /space/cache 2>/dev/null || echo unknown)); the kind node provides it from the SPACE Kind node image (check it with infra/space/kind/scripts/check-kind-node-image.sh) and 'docker exec $SPACE_NODE_ID journalctl -u space-cache'"
    fi
    sleep 1
    waited=$((waited + 1))
  done
  log "/space/cache is the node's btrfs cache ($(findmnt -n -o SOURCE --mountpoint /space/cache))"
}

# The Firecracker jailer mknods /dev/kvm + /dev/urandom + /dev/userfaultfd +
# /dev/net/tun into the chroot, but NOT /dev/vhost-vsock — that is a known FC
# limitation, see docs/jailer.md#known-limitations upstream. Without it FC
# silently disables the vsock device described in vm-config, the host bridge
# can't talk to the guest, and `wait_ready` deadline-expires at the 30s mark.
#
# Production handles this on the worker AMI; locally we run a tight watcher
# that races the jailer: as soon as a new sandbox's jail/dev appears, mknod
# the vhost-vsock node into it. FC reads vm-config and opens its vsock device
# very shortly after exec, so the watcher polls at 10ms and `mknod -p`-style
# best-effort retries. The host's /dev/vhost-vsock is the source of major:minor
# (10:241 on recent kernels but read from the host node to avoid hardcoding).
start_vhost_vsock_watcher() {
  local src=/dev/vhost-vsock
  if [[ ! -c $src ]]; then
    log "skipping vhost-vsock watcher (/dev/vhost-vsock not present)"
    return 0
  fi
  local major minor
  major=$(stat -c '%t' "$src")
  minor=$(stat -c '%T' "$src")
  major=$((16#$major))
  minor=$((16#$minor))
  log "starting vhost-vsock jail watcher (mknod c $major $minor)"
  (
    while true; do
      for jail_dev in /space/cache/firecracker/jails/firecracker/*/root/dev; do
        [[ -d "$jail_dev" ]] || continue
        [[ -e "$jail_dev/vhost-vsock" ]] && continue
        # mknod must run as root (we are); chmod 0666 so the sandbox uid that
        # FC switches to can open the device.
        if mknod "$jail_dev/vhost-vsock" c "$major" "$minor" 2>/dev/null; then
          chmod 0666 "$jail_dev/vhost-vsock" 2>/dev/null
          # Best-effort log via the supervisor log; suppress if not writable.
          echo "[vhost-vsock-watcher] $(date -Is) mknod $jail_dev/vhost-vsock" \
            >> /space/logs/runtime-supervisor.log 2>/dev/null || true
        fi
      done
      sleep 0.01
    done
  ) &
  echo $! > "$PID_DIR/vhost-vsock-watcher.pid" 2>/dev/null || true
}

preserve_service_logs() {
  local attempt=1 archive file
  # Each retry keeps the previous attempt's files in order. Keep current logs
  # separate so tail -F does not replay old events with fresh pod timestamps.
  while true; do
    printf -v archive '%s/previous-%06d' "$LOG_DIR" "$attempt"
    [[ ! -e "$archive" ]] && break
    ((attempt += 1))
  done
  for file in "$LOG_DIR"/*.log; do
    [[ -f "$file" ]] || continue
    mkdir -p "$archive"
    mv -- "$file" "$archive/"
  done
}

setup_layout() {
  # Make the pod's mount tree shared, like systemd makes `/` rshared on a
  # production host. The Firecracker jailer unshares a mount namespace when
  # it pivots into the jail, and node-manager bind-mounts the snapshot
  # memory file into the jail AFTER Firecracker is already running (the
  # restore path). Under the host's rshared root those late bind mounts
  # propagate into the jail's (slave) namespace; under a container's
  # default-private mount tree they don't — Firecracker then opens the
  # 0-byte bind-target placeholder and every snapshot restore fails with
  # "Total sum of memory regions exceeds largest possible file offset".
  mount --make-rshared / \
    || die "mount --make-rshared / failed (snapshot restore would break)"

  require_node_cache
  # Explicitly for the cache as well: a node mount that propagated in after the
  # rshared `/` above (a late node unit) arrives only as a slave. Shared, the
  # mounts node-manager makes under it after a jail exists (the snapshot memory
  # file on restore) propagate into the jail's namespace; still a slave of the
  # node's mount (HostToContainer), it sends none of them back to the node.
  mount --make-rshared /space/cache \
    || die "mount --make-rshared /space/cache failed (snapshot restore would break)"

  local sbxclients_gid tmplclients_gid nodectlclients_gid rtmgrclients_gid gate gate_path
  sbxclients_gid="$(space_expected_gid_for_group space-sbxclients)"
  rtmgrclients_gid="$(space_expected_gid_for_group space-rtmgrclients)"
  tmplclients_gid="$(space_expected_gid_for_group space-tmplclients)"
  nodectlclients_gid="$(space_expected_gid_for_group space-nodectlclients)"

  install -d -m 0755 -o 0 -g 0 \
    "$LOG_DIR" \
    "$PID_DIR" \
    "$INDEPENDENT_PID_DIR" \
    /space/config \
    /space/etc \
    /run/health
  preserve_service_logs

  # Service directories + ownership, as the per-service DaemonSet init
  # containers create them in production (node-manager-init.sh, and the
  # node-ctl / volume-manager chart inits). Like node-manager-init.sh, this
  # re-asserts the parts of the packer 07-sysctl.sh skeleton those services
  # depend on; the kind node's space-layout and space-cache units provide that
  # skeleton itself, including the /space/cache tree.
  install -d -m 0755 -o 0 -g 0 /space/run/node-manager /space/run/runtime-supervisor
  install -d -m 0750 -o 0 -g "$sbxclients_gid" \
    /space/run/dependency-drain /space/run/dependency-drain/sandbox-manager
  for gate in capability-v1.lock admission-v1.lock work-v1.lock; do
    gate_path="/space/run/dependency-drain/sandbox-manager/$gate"
    if [[ -L "$gate_path" || ( -e "$gate_path" && ! -f "$gate_path" ) ]]; then
      die "dependency-drain gate is not a regular file: $gate_path"
    fi
    touch "$gate_path"
    chown "0:$sbxclients_gid" "$gate_path"
    chmod 0660 "$gate_path"
  done
  install -d -m 0750 -o "$SPACE_SBXMGR_UID" -g "$sbxclients_gid" \
    /space/run/sandbox-manager /space/run/credential-manager /space/run/volume-manager
  install -d -m 0755 -o "$SPACE_SBXMGR_UID" -g "$SPACE_SBXMGR_GID" /space/run/sandbox-host-control
  install -d -m 0750 -o "$SPACE_TMPLMGR_UID" -g "$tmplclients_gid" /space/run/template-manager
  install -d -m 0750 -o "$SPACE_NODECTL_UID" -g "$nodectlclients_gid" /space/run/node-ctl
  install -d -m 0750 -o "$SPACE_RTMGR_UID" -g "$rtmgrclients_gid" /space/run/runtime-manager
  install -d -m 0755 -o 0 -g 0 /space/run/firecracker/sandboxes
  if [[ "$SPACE_NODE_ROLE" == "builder" ]]; then
    install -d -m 0750 -o 0 -g 0 /space/run/build-manager
  fi
  install -d -m 0755 -o 0 -g 0 /space/cache/firecracker/jails

  chmod 1777 /run/health

  # /space/run is a hostPath, so sockets from a terminated pod survive the
  # pod itself. Remove them before launching services; otherwise a stale UDS
  # can make the startup waits succeed even when its new owner has already
  # exited (for example while Postgres is still returning after a devbox
  # resume).
  rm -f \
    "$NODE_MANAGER_SOCKET" \
    "$SBXMGR_SOCKET" \
    "$TMPLMGR_SOCKET" \
    "$VOLMGR_SOCKET" \
    "$CREDMGR_SOCKET" \
    "$RUNTIME_SUPERVISOR_SOCKET" \
    "$RUNTIME_MANAGER_SOCKET" \
    /run/health/*.sock

  # Default per-VM settings; snapshot restore uses file-backed memory.
  cat > /space/etc/sandbox.toml <<'EOF'
# Defaults
EOF

  printf '%s\n' "$SPACE_NODE_ID" > /space/etc/node-id

  # nodemgr writes /sys/fs/cgroup/space/{cpu,memory}.max at startup and
  # delegates its controllers to per-sandbox children. The node creates that
  # parent (space-cgroup.service on the kind node, as packer 07-sysctl.sh does
  # on a production image); say so here rather than as a nodemgr startup crash.
  [[ -d /sys/fs/cgroup/space ]] \
    || die "/sys/fs/cgroup/space is missing; the kind node's space-cgroup.service creates it (check the node image with infra/space/kind/scripts/check-kind-node-image.sh)"

  # Stage the Firecracker kernel asset where the runtime inventory expects it,
  # through the same script the production worker's node-manager init container
  # runs. Sharing it keeps one implementation of "which layout does the vendor
  # tree use, and where does the inventory read it from", and gives that
  # implementation real coverage: the kind smokes boot actual Firecracker VMs
  # off whatever it stages.
  /opt/space/bin/stage-guest-kernel.sh
}

# ── SQLite migrations for the services that own on-node state ─────────────────
# Run as the service user that owns each database, like the per-service
# migrate-sqlite init containers do in the EKS charts, so the created files
# carry the same ownership as production.
run_sqlite_migrations() {
  [[ -x /opt/space/bin/space-db-migrator ]] || die "space-db-migrator missing"
  local component user group
  for spec in \
    "template-manager:$SPACE_TMPLMGR_USER:$SPACE_TMPLMGR_GROUP" \
    "sandbox-manager:$SPACE_SBXMGR_USER:$SPACE_SBXMGR_GROUP" \
    "node-ctl:$SPACE_NODECTL_USER:$SPACE_NODECTL_GROUP"; do
    IFS=: read -r component user group <<<"$spec"
    log "migrating $component sqlite schema (as $user)"
    setpriv --reuid "$user" --regid "$group" --clear-groups \
      space-db-migrator "$component" up
  done
  if [[ "$SPACE_NODE_ROLE" == "builder" ]]; then
    log "migrating build-manager sqlite schema (as root)"
    space-db-migrator build-manager \
      --database-path /space/run/build-manager/build-manager.db up
  fi
}

# ── Service launcher with production unix identities ─────────────────────────
# Mirrors the service identities of systemd-managed direct nodes and the
# securityContexts of the EKS DaemonSets: runtime-supervisor / nodemgr stay
# root (privileged runtime path), everything else drops to its
# service account with the exact SupplementaryGroups production grants. The
# UDS peer authorization (SO_PEERCRED) between services is therefore live in
# this harness, not collapsed to Root-peer-for-everyone.
# Launcher arguments can contain credentials in environment assignments or
# flags. Log only service identity, never the command or argument values.
start_service() {
  local name="$1"
  shift
  log "starting $name (root)"
  "$@" >"$LOG_DIR/$name.log" 2>&1 &
  echo $! >"$PID_DIR/$name.pid"
}

start_service_as() {
  local name="$1" user="$2" group="$3" groups="$4"
  shift 4
  log "starting $name (as $user, groups: $groups)"
  setpriv --reuid "$user" --regid "$group" --groups "${groups// /,}" -- \
    "$@" >"$LOG_DIR/$name.log" 2>&1 &
  echo $! >"$PID_DIR/$name.pid"
}

# Like start_service_as, but the service restarts itself in a loop instead of
# being watched by supervise_services, so its exit never takes the node's
# service set down with it. For services nothing else depends on yet.
start_independent_service_as() {
  local name="$1" user="$2" group="$3" groups="$4"
  shift 4
  log "starting $name (as $user, groups: $groups; restarts independently)"
  (
    child=""
    trap 'if [[ -n "$child" ]]; then kill -TERM "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi; exit 0' TERM
    while true; do
      setpriv --reuid "$user" --regid "$group" --groups "${groups// /,}" -- \
        "$@" >>"$LOG_DIR/$name.log" 2>&1 &
      child=$!
      wait "$child" || true
      child=""
      printf '[node-stack] %s exited; restarting\n' "$name" >>"$LOG_DIR/$name.log"
      sleep 1
    done
  ) &
  echo $! >"$INDEPENDENT_PID_DIR/$name.pid"
}

wait_for_socket() {
  local name="$1" path="$2" attempts="${3:-60}" pid_file="$PID_DIR/$1.pid" pid state i
  for ((i = 1; i <= attempts; i++)); do
    [[ -S "$path" ]] && { log "$name socket ready"; return 0; }
    if [[ -f "$pid_file" ]]; then
      pid="$(cat "$pid_file")"
      state="$(ps -o stat= -p "$pid" 2>/dev/null || true)"
      if [[ ! "$pid" =~ ^[0-9]+$ ]] || [[ -z "$state" ]] || [[ "$state" == Z* ]]; then
        die "$name exited before socket $path became ready (see $LOG_DIR/$name.log)"
      fi
    fi
    sleep 1
  done
  die "$name socket $path did not appear after $attempts s"
}

supervise_services() {
  local tail_pid pid_file pid name state
  # From the first line: services have been logging since before node-api
  # was ready, and a bare `tail -F` would forward only the last ten lines of
  # each file — template-manager's startup recovery, for one, would never
  # reach `kubectl logs`.
  tail -n +1 -F "$LOG_DIR"/*.log &
  tail_pid=$!

  while sleep 1; do
    for pid_file in "$PID_DIR"/*.pid; do
      [[ -f "$pid_file" ]] || continue
      name="$(basename "$pid_file" .pid)"
      pid="$(cat "$pid_file")"
      state="$(ps -o stat= -p "$pid" 2>/dev/null || true)"
      if [[ ! "$pid" =~ ^[0-9]+$ ]] || [[ -z "$state" ]] || [[ "$state" == Z* ]]; then
        log "ERROR: $name exited; terminating node-stack so Kubernetes restarts the service set"
        kill "$tail_pid" 2>/dev/null || true
        wait "$tail_pid" 2>/dev/null || true
        return 1
      fi
    done
  done
}

# node-api serves gRPC over TCP :9800 (not a UDS), so probe it with the same
# health prober the DaemonSet uses.
wait_for_node_api() {
  local addr="127.0.0.1:9800" attempts="${1:-90}" i
  for ((i = 1; i <= attempts; i++)); do
    if /opt/space/bin/grpc_health_probe -addr="$addr" -connect-timeout=2s >/dev/null 2>&1; then
      log "node-api serving on $addr"
      return 0
    fi
    sleep 1
  done
  die "node-api did not become healthy on $addr after $attempts s"
}

wait_for_build_manager() {
  local addr="$SPACE_BUILD_MANAGER_LISTEN" attempts="${1:-90}" i
  for ((i = 1; i <= attempts; i++)); do
    if /opt/space/bin/grpc_health_probe -addr="$addr" \
      -service=build.manager.v1.BuildManager -connect-timeout=2s >/dev/null 2>&1; then
      log "build-manager serving on $addr"
      return 0
    fi
    sleep 1
  done
  die "build-manager did not become healthy on $addr after $attempts s"
}

# Legacy DirectSubstrate node registration (see the bench-mode block at the
# top). Only compiled into the flow when SPACE_DIRECT_SUBSTRATE_REGISTRATION=1;
# the production-shaped path never touches Postgres from a node.
register_direct_node() {
  local addr
  addr="$(hostname -i 2>/dev/null | awk '{print $1}')"
  [[ -n "$addr" ]] || die "could not determine pod IP for direct node registration"
  local node_name="${KUBE_NODE_NAME:-$SPACE_NODE_ID}"
  log "registering DirectSubstrate node $SPACE_NODE_ID (name=$node_name endpoint=$SPACE_NODE_API_ENDPOINT)"
  local attempt
  for attempt in $(seq 1 30); do
    if psql "$DIRECT_POSTGRES_CONNECTION_STRING" -v ON_ERROR_STOP=1 \
      -v node_id="$SPACE_NODE_ID" \
      -v node_name="$node_name" \
      -v node_endpoint="$SPACE_NODE_API_ENDPOINT" \
      -v node_addr="$addr" >"$LOG_DIR/direct-register.log" 2>&1 <<'SQL'
INSERT INTO nodes (id, role, managed_by, state, address)
VALUES (:'node_id', 'worker', 'space', 'registered', :'node_addr'::inet)
ON CONFLICT (id) DO UPDATE SET
  role = excluded.role,
  managed_by = excluded.managed_by,
  state = excluded.state,
  address = excluded.address;

INSERT INTO node_direct (node_id, name, node_api_endpoint, metadata)
VALUES (:'node_id', :'node_name', :'node_endpoint', '{}'::jsonb)
ON CONFLICT (node_id) DO UPDATE SET
  name = excluded.name,
  node_api_endpoint = excluded.node_api_endpoint,
  updated_at = NOW(),
  deleted_at = NULL;

INSERT INTO node_status_snapshots (
  node_id, observed_at, stale_after, reachable, role,
  scheduling_status, health_healthy, services, updated_at
)
VALUES (
  :'node_id', NOW(), NOW() + INTERVAL '365 days', TRUE, 'worker',
  'ready', TRUE, '[]'::jsonb, NOW()
)
ON CONFLICT (node_id) DO UPDATE SET
  observed_at = NOW(),
  stale_after = NOW() + INTERVAL '365 days',
  reachable = TRUE,
  scheduling_status = 'ready',
  health_healthy = TRUE,
  updated_at = NOW();
SQL
    then
      log "DirectSubstrate node registration complete"
      return 0
    fi
    sleep 2
  done
  die "DirectSubstrate node registration failed after retries (see direct-register.log)"
}

# Every service inherits this script's exported environment through setpriv.
# Move values that belong to a single service into shell locals and hand
# each back only to the service that reads it (credmgr, volmgr, buildmgr,
# and the bench-only direct registration below).
scope_service_environment() {
  CREDMGR_DATABASE_URL="${SPACE_CREDMGR_DATABASE_URL:-}"
  CREDMGR_DB_CREDS_PASSWORD="${SPACE_DB_CREDS_PASSWORD:-}"
  CREDMGR_AWS_ACCESS_KEY_ID="${SPACE_CREDMGR_AWS_ACCESS_KEY_ID:-}"
  CREDMGR_AWS_SECRET_ACCESS_KEY="${SPACE_CREDMGR_AWS_SECRET_ACCESS_KEY:-}"
  CREDMGR_AWS_SESSION_TOKEN="${SPACE_CREDMGR_AWS_SESSION_TOKEN:-}"
  DIRECT_POSTGRES_CONNECTION_STRING="${POSTGRES_CONNECTION_STRING:-}"
  OBJECT_STORE_STATIC_ENV=()
  local name
  for name in SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID \
    SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY SPACE_OBJECT_STORE_STATIC_SESSION_TOKEN; do
    if [[ -n "${!name:-}" ]]; then
      OBJECT_STORE_STATIC_ENV+=("$name=${!name}")
    fi
    unset "$name"
  done
  unset SPACE_CREDMGR_DATABASE_URL SPACE_DB_CREDS_PASSWORD \
    SPACE_CREDMGR_AWS_ACCESS_KEY_ID SPACE_CREDMGR_AWS_SECRET_ACCESS_KEY \
    SPACE_CREDMGR_AWS_SESSION_TOKEN POSTGRES_CONNECTION_STRING
}

start_all() {
  # Production runtime service ordering (matches the node DaemonSets):
  #   runtime-supervisor → nodemgr
  # runtime-supervisor must be up first because nodemgr.Launch on the first
  # sandbox immediately dials it. nodemgr opens the NodeManager UDS that
  # sbxmgr will dial. sandbox-manager launches space-netgtw-svc with its
  # required sandbox socket and identity arguments through nodemgr; there is
  # no node-wide net-gateway process to start or supervise.
  start_service runtime-supervisor space-runtime-supervisor-svc
  wait_for_socket runtime-supervisor "$RUNTIME_SUPERVISOR_SOCKET"
  start_service nodemgr space-nodemgr-svc

  # The runtime manager only lists the node's runtimes and kernels. No other
  # service depends on it, so nothing waits for its socket and its exit does
  # not restart the service set.
  start_independent_service_as rtmgr "$SPACE_RTMGR_USER" "$SPACE_RTMGR_GROUP" \
    "$SPACE_RUNTIME_MANAGER_GROUPS" space-rtmgr-svc

  # Storage + auth (no socket dependencies on each other). Privileged BYOK
  # volume operations live in node-manager; volume-manager runs under the
  # production service identity with cache access through space-cache-rw.
  start_service_as volmgr "$SPACE_SBXMGR_USER" "$SPACE_SBXMGR_GROUP" "$SPACE_VOLUME_MANAGER_GROUPS" \
    env ${OBJECT_STORE_STATIC_ENV[@]+"${OBJECT_STORE_STATIC_ENV[@]}"} space-volmgr-svc
  # credmgr runs its (pre-agent-migration) authority role, which resolves org
  # KEK rows from the control-plane DB. Hand the DB URL to credmgr only; no
  # other node service sees Postgres, matching production workers. The
  # optional AWS creds (space-aws-creds Secret, sandbox account only) are
  # scoped to credmgr so its independent AWS integrations do not require
  # every node service to inherit AWS credentials.
  credmgr_env=(
    "DATABASE_URL=${CREDMGR_DATABASE_URL}"
    "SPACE_DB_CREDS_PASSWORD=${CREDMGR_DB_CREDS_PASSWORD}"
    "SPACE_CREDMGR_IN_MEMORY_KEK_ENABLED=${SPACE_CREDMGR_IN_MEMORY_KEK_ENABLED:-}"
    "SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN=${SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN:-}"
  )
  if [[ -n "$CREDMGR_AWS_ACCESS_KEY_ID" ]]; then
    credmgr_env+=("AWS_ACCESS_KEY_ID=${CREDMGR_AWS_ACCESS_KEY_ID}")
    credmgr_env+=("AWS_SECRET_ACCESS_KEY=${CREDMGR_AWS_SECRET_ACCESS_KEY}")
    [[ -n "$CREDMGR_AWS_SESSION_TOKEN" ]] \
      && credmgr_env+=("AWS_SESSION_TOKEN=${CREDMGR_AWS_SESSION_TOKEN}")
  fi
  start_service_as credmgr "$SPACE_SBXMGR_USER" "$SPACE_SBXMGR_GROUP" "$SPACE_CREDENTIAL_MANAGER_GROUPS" \
    env "${credmgr_env[@]}" space-credmgr-svc
  wait_for_socket credmgr "$CREDMGR_SOCKET"

  # template-manager. Prefetch resolves seed:latest against the cluster API
  # (space-seed-publish registered it) and pulls through volmgr — the same
  # path production workers use; nothing is registered by hand here.
  start_service_as tmplmgr "$SPACE_TMPLMGR_USER" "$SPACE_TMPLMGR_GROUP" "$SPACE_TEMPLATE_MANAGER_GROUPS" \
    space-tmplmgr-svc --health-socket /run/health/tmplmgr.sock
  wait_for_socket tmplmgr "$TMPLMGR_SOCKET"

  # sandbox-manager depends on node-manager (the runtime) + template-manager.
  wait_for_socket nodemgr "$NODE_MANAGER_SOCKET"
  start_service_as sbxmgr "$SPACE_SBXMGR_USER" "$SPACE_SBXMGR_GROUP" "$SPACE_SANDBOX_MANAGER_GROUPS" \
    space-sbxmgr-svc --health-socket /run/health/sbxmgr.sock

  # node-ctl publishes node status + template inventory to the cluster API —
  # this IS the node registration path (no Postgres writes from this pod).
  start_service_as nodectl "$SPACE_NODECTL_USER" "$SPACE_NODECTL_GROUP" "$SPACE_NODECTL_GROUPS" \
    space-nodectl-svc

  # node-api dials sbxmgr/nodemgr/tmplmgr at startup; wait for all three.
  wait_for_socket sbxmgr "$SBXMGR_SOCKET"
  wait_for_socket volmgr "$VOLMGR_SOCKET"
  local nodeapi_groups="$SPACE_NODEAPI_WORKER_GROUPS"
  if [[ "$SPACE_NODE_ROLE" == "builder" ]]; then
    require_env SPACE_BUILD_MANAGER_ENDPOINT SPACE_OBJECT_STORE_BUCKET \
      SPACE_OBJECT_STORE_REGION SPACE_OBJECT_STORE_ENDPOINT \
      SPACE_BUILD_MANAGER_MAX_CONCURRENT_BUILDS \
      SPACE_BUILD_MANAGER_DRAIN_TIMEOUT_SECONDS SPACE_BUILD_MANAGER_LISTEN
    # Scope any exporter override to build-manager; the other services in this
    # pod keep the exporter their own environment selects.
    local buildmgr_env=(${OBJECT_STORE_STATIC_ENV[@]+"${OBJECT_STORE_STATIC_ENV[@]}"})
    if [[ -n "${SPACE_BUILD_MANAGER_TRACE_EXPORTER:-}" ]]; then
      buildmgr_env+=("SPACE_TRACE_EXPORTER=$SPACE_BUILD_MANAGER_TRACE_EXPORTER")
    fi
    start_service buildmgr env ${buildmgr_env[@]+"${buildmgr_env[@]}"} space-buildmgr-svc \
      --max-concurrent-builds="$SPACE_BUILD_MANAGER_MAX_CONCURRENT_BUILDS" \
      --db=/space/run/build-manager/build-manager.db \
      --drain-timeout-seconds="$SPACE_BUILD_MANAGER_DRAIN_TIMEOUT_SECONDS" \
      --listen="$SPACE_BUILD_MANAGER_LISTEN"
    wait_for_build_manager
    nodeapi_groups="$SPACE_NODEAPI_BUILDER_GROUPS"
  fi
  start_service_as node-api "$SPACE_NODEAPI_USER" "$SPACE_NODEAPI_GROUP" "$nodeapi_groups" \
    space-node-api-svc --role "$SPACE_NODE_ROLE" --listen 0.0.0.0:9800
}

signal_service() {
  local name="$1" pid_file="$PID_DIR/$1.pid" pid
  [[ -f "$pid_file" ]] || return 0
  pid="$(cat "$pid_file")"
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    kill -TERM "$pid" 2>/dev/null || true
  fi
}

wait_for_service_exit() {
  local name="$1" pid_file="$PID_DIR/$1.pid" pid
  [[ -f "$pid_file" ]] || return 0
  pid="$(cat "$pid_file")"
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    wait "$pid" 2>/dev/null || true
  fi
}

on_term() {
  log "received SIGTERM, draining services"
  local pid_file pid
  trap - TERM INT

  if [[ "$SPACE_NODE_ROLE" == "builder" ]]; then
    # Stop accepting new node-api work, then give build-manager its production
    # drain window while the runtime/storage managers it needs remain alive.
    signal_service node-api
    signal_service buildmgr
    log "waiting for build-manager drain before stopping its dependencies"
    wait_for_service_exit buildmgr
  fi

  for pid_file in "$PID_DIR"/*.pid "$INDEPENDENT_PID_DIR"/*.pid; do
    [[ -f "$pid_file" ]] || continue
    pid="$(cat "$pid_file")"
    [[ "$pid" =~ ^[0-9]+$ ]] && kill -TERM "$pid" 2>/dev/null || true
  done
  for pid_file in "$PID_DIR"/*.pid "$INDEPENDENT_PID_DIR"/*.pid; do
    [[ -f "$pid_file" ]] || continue
    pid="$(cat "$pid_file")"
    [[ "$pid" =~ ^[0-9]+$ ]] && wait "$pid" 2>/dev/null || true
  done
  exit 0
}

load_node_identity
scope_service_environment
trap on_term TERM INT

log "node-stack starting (node_id=$SPACE_NODE_ID role=$SPACE_NODE_ROLE cluster_api=$SPACE_INTERNAL_API_ENDPOINT)"
setup_layout
start_vhost_vsock_watcher
run_sqlite_migrations
start_all
wait_for_node_api
if [[ "$DIRECT_REGISTRATION" == "1" ]]; then
  register_direct_node
fi
log "all services started; tailing logs"
supervise_services
