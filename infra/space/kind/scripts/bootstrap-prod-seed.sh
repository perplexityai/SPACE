#!/usr/bin/env bash
# Register externally built SPACE templates in Kind and stage their artifacts
# on each worker. This optional path requires an explicitly configured S3
# source. The default bring-up instead builds the seed locally and publishes
# it through ClusterApi and the object store (build-seed.sh and space-seed-publish).
#
# The kind workers' volmgr only has object-store credentials, so the real-S3
# prefetch path can't pull these artifacts itself. This script is the
# bridge: it runs `scripts/bootstrap-templates.sh` to write the four
# control-plane rows
# (templates / template_releases / template_release_tags /
# template_release_storage_locations), THEN pulls the artifacts down from
# the same S3 bucket on the devbox and stages them onto each worker's
# `/space/cache/firecracker/templates/<name>/` directory via `kubectl cp`.
# Each worker's `space-tmplmgr register` then re-hashes the staged files
# and the local tmplmgr db catches up with the control plane.
#
# Required env (the bootstrap-templates.sh half):
#   POSTGRES_CONNECTION_STRING — control-plane DB. Defaults to a forwarded
#                                postgresql://spaceuser:spacepassword@127.0.0.1:5433/spacedb
#                                so callers don't need to know the secret.
#   AWS_*                      — sandbox creds (read-only suffices); load
#                                with `set -a; source infra/space/kind/private.env`.
#   TEMPLATE_BOOTSTRAP_BUCKET  — required; S3 bucket name (no scheme).
#   TEMPLATE_BOOTSTRAP_NAMES   — comma/space-separated. Default: seed.
#
# This script is idempotent: re-running it skips work that's already done.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

log() { printf '[bootstrap-prod-seed] %s\n' "$*"; }
die() { printf '[bootstrap-prod-seed] ERROR: %s\n' "$*" >&2; exit 1; }

require_bin() {
  local bin
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || die "missing required binary: $bin"
  done
}
[[ -n "${TEMPLATE_BOOTSTRAP_BUCKET:-}" ]] || die "TEMPLATE_BOOTSTRAP_BUCKET is required; set it in infra/space/kind/private.env"
[[ "$TEMPLATE_BOOTSTRAP_BUCKET" != *://* ]] || die "TEMPLATE_BOOTSTRAP_BUCKET must be a bucket name, not a URI"
export TEMPLATE_BOOTSTRAP_BUCKET
require_bin aws jq kubectl /bin/psql

export TEMPLATE_BOOTSTRAP_NAMES="${TEMPLATE_BOOTSTRAP_NAMES:-seed}"
# Default to the hashed layout — flat layout often lacks rootfs.ext4.zst,
# which volmgr's pull path expects. See bootstrap-templates.sh comments.
export TEMPLATE_BOOTSTRAP_PREFER="${TEMPLATE_BOOTSTRAP_PREFER:-hashed}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

[[ -n "${AWS_ACCESS_KEY_ID:-}" ]] \
  || die "AWS_ACCESS_KEY_ID unset; source infra/space/kind/private.env first"
aws sts get-caller-identity >/dev/null 2>&1 \
  || die "AWS creds invalid; refresh via SSO or update private.env"

# ── Postgres port-forward (idempotent) ────────────────────────────────────
PG_PF_PORT="${PG_PF_PORT:-5433}"
ensure_pg_forwarded() {
  if /bin/psql "postgresql://spaceuser:spacepassword@127.0.0.1:${PG_PF_PORT}/spacedb" \
       -c 'SELECT 1' >/dev/null 2>&1; then
    log "postgres reachable at 127.0.0.1:${PG_PF_PORT}"
    return 0
  fi
  log "starting kubectl port-forward svc/postgres ${PG_PF_PORT}:5432"
  nohup kubectl -n space port-forward svc/postgres "${PG_PF_PORT}:5432" \
    >/tmp/space-kind-pf-pg.log 2>&1 &
  echo $! > /tmp/space-kind-pf-pg.pid
  local i
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sleep 1
    if /bin/psql "postgresql://spaceuser:spacepassword@127.0.0.1:${PG_PF_PORT}/spacedb" \
         -c 'SELECT 1' >/dev/null 2>&1; then
      log "postgres reachable after ${i}s"
      return 0
    fi
  done
  die "postgres port-forward never came up; see /tmp/space-kind-pf-pg.log"
}

# ── Step 1: control-plane registration via the shared bootstrap script ────
register_control_plane() {
  log "registering templates in control-plane Postgres"
  PATH=/bin:$PATH POSTGRES_CONNECTION_STRING="postgresql://spaceuser:spacepassword@127.0.0.1:${PG_PF_PORT}/spacedb" \
    bash "$REPO_ROOT/scripts/bootstrap-templates.sh"
}

# ── Step 2: stage artifacts onto each worker (kind-only) ──────────────────
# One line per sandbox worker: its operator shell pod (space-node-debug), where
# the staging and `space-tmplmgr register` run as root, and the node.
worker_pods() {
  local node
  for node in $(kubectl -n space get pods \
      -l app.kubernetes.io/name=space-worker,app.kubernetes.io/component=template-manager \
      -o jsonpath='{.items[*].spec.nodeName}'); do
    printf '%s %s\n' "$(kubectl -n space get pods -l app.kubernetes.io/name=space-node-debug \
      --field-selector "spec.nodeName=$node" -o jsonpath='{.items[0].metadata.name}')" "$node"
  done
}

stage_artifacts_on_workers() {
  local name="$1"
  log "staging $name artifacts onto every worker"
  local row
  while read -r row; do
    [[ -z "$row" ]] && continue
    local pod node hash storage_uri tmp
    pod="$(echo "$row" | awk '{print $1}')"
    node="$(echo "$row" | awk '{print $2}')"
    log "  ${node} (${pod})"

    # Pull manifest + metadata fresh from S3, derive the hash + storage prefix
    # to avoid relying on whatever the prior step persisted.
    hash="$(/bin/psql "postgresql://spaceuser:spacepassword@127.0.0.1:${PG_PF_PORT}/spacedb" \
      -tA -v ON_ERROR_STOP=1 -v name="$name" <<'SQL'
SELECT tr.hash
FROM template_release_tags t
JOIN template_releases tr ON tr.id = t.template_release_id
WHERE t.template_name = :'name' AND t.tag = 'latest';
SQL
)"
    [[ -n "$hash" ]] || die "no 'latest' release for $name in control-plane"
    storage_uri="$(/bin/psql "postgresql://spaceuser:spacepassword@127.0.0.1:${PG_PF_PORT}/spacedb" \
      -tA -v ON_ERROR_STOP=1 -v name="$name" -v hash="$hash" <<'SQL'
SELECT l.storage_connection_string
FROM template_release_storage_locations l
JOIN template_releases tr ON tr.id = l.template_release_id
WHERE tr.template_name = :'name' AND tr.hash = :'hash'
LIMIT 1;
SQL
)"
    [[ -n "$storage_uri" ]] || die "no storage_connection_string for $name@$hash"

    # storage_uri is `s3://BUCKET/PREFIX/?region=…`. Drop the query string.
    local s3_path="${storage_uri%%\?*}"
    s3_path="${s3_path%/}"

    # If the worker already has this hash registered, skip — idempotency.
    # `space-tmplmgr list` truncates the hash to ~12 hex chars, so grep on
    # the bare prefix (post-`sha256:`).
    local hex_prefix="${hash#sha256:}"
    hex_prefix="${hex_prefix:0:12}"
    if kubectl -n space exec "$pod" -c shell -- bash -c "space-tmplmgr list 2>/dev/null | grep -q $hex_prefix" 2>/dev/null; then
      log "    $name@${hash:0:24}… already staged locally; skip"
      continue
    fi

    tmp="$(mktemp -d)"
    # shellcheck disable=SC2064
    trap "rm -rf '$tmp'" RETURN

    log "    pulling rootfs.ext4.zst + manifests from $s3_path"
    aws s3 cp "$s3_path/rootfs.ext4.zst" "$tmp/rootfs.ext4.zst" >/dev/null
    aws s3 cp "$s3_path/manifest.json"   "$tmp/manifest.json"   >/dev/null
    aws s3 cp "$s3_path/metadata.json"   "$tmp/metadata.json"   >/dev/null

    log "    copying into pod + decompressing + registering"
    # apt-install zstd in the shell if missing (the worker image doesn't bake it).
    kubectl -n space exec "$pod" -c shell -- bash -c '
      command -v zstd >/dev/null 2>&1 && exit 0
      apt-get update >/dev/null 2>&1
      apt-get install -y zstd >/dev/null 2>&1
    '
    kubectl -n space cp "$tmp/rootfs.ext4.zst" "space/${pod}:/tmp/${name}-rootfs.ext4.zst" -c shell
    kubectl -n space cp "$tmp/manifest.json"   "space/${pod}:/tmp/${name}-manifest.json"   -c shell
    kubectl -n space cp "$tmp/metadata.json"   "space/${pod}:/tmp/${name}-metadata.json"   -c shell

    kubectl -n space exec "$pod" -c shell -- bash -c "
      set -e
      stage=/space/cache/firecracker/templates/${name}
      install -d -m 0755 \$stage
      zstd -d -f /tmp/${name}-rootfs.ext4.zst -o \$stage/rootfs.ext4 >/dev/null
      cp /tmp/${name}-manifest.json \$stage/manifest.json
      cp /tmp/${name}-metadata.json \$stage/metadata.json
      space-tmplmgr register --template ${name} --tag latest 2>&1 | tail -3
    "
    rm -rf "$tmp"
    trap - RETURN
  done < <(worker_pods)
}

# ── Main ─────────────────────────────────────────────────────────────────
main() {
  ensure_pg_forwarded
  register_control_plane

  # Stage each requested template onto every worker.
  local name
  for name in $(printf '%s' "$TEMPLATE_BOOTSTRAP_NAMES" | tr ', ' '\n'); do
    [[ -z "$name" ]] && continue
    stage_artifacts_on_workers "$name"
  done

  log "done. cluster + workers ready with prod-built templates: $TEMPLATE_BOOTSTRAP_NAMES"
}

main "$@"
