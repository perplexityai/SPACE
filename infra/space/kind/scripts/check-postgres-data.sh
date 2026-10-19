#!/usr/bin/env bash
# check-postgres-data.sh — refuse a kind cluster whose Postgres data directory
# was initialized by another PostgreSQL major version than the one the
# `space-postgres` chart in this checkout runs.
#
# Earlier revisions of the chart ran PostgreSQL 16 without TLS and with a
# superuser connection string. PostgreSQL 17 cannot start on a 16 data
# directory, so re-applying the chart crash-loops the pod and the bring-up
# waits out its rollout timeout; and until it is re-applied the old server
# keeps running without TLS, so the harness's sslmode=require psql fails. The
# role setup the pod's init script provides (templates/configmap.yaml) runs
# only when the image initializes an empty data directory, so the database is
# reset rather than upgraded in place. Every bring-up path stops here instead.
# See "Recovering from an unbundled migration version" in
# infra/space/kind/README.md.
#
# The version is the data directory's own PG_VERSION file, read on the node
# that holds the volume rather than from the server, so the answer is the same
# whether the old server is running, the new one is crash-looping on the old
# data, or no pod runs at all.
#
# Read-only. Exits 0 when the cluster, the volume or its PG_VERSION does not
# exist yet, or the data directory matches the chart's major version; 1 (with
# the remedy on stderr) otherwise, or when the volume cannot be read.
#
# Usage:
#   bash infra/space/kind/scripts/check-postgres-data.sh [cluster]
#
# The cluster defaults to $SPACE_KIND_CLUSTER, then space-local.
set -euo pipefail

KIND_CLUSTER="${1:-${SPACE_KIND_CLUSTER:-space-local}}"
NAMESPACE=space
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/charts/space-postgres"
# The StatefulSet's volumeClaimTemplate (`data`) and PGDATA, a subdirectory of
# that volume (templates/statefulset.yaml).
PVC=data-space-postgres-0
PGDATA_SUBDIR=pgdata

kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || exit 0

want="$(awk -F'"' '/^appVersion:/ { print $2 }' "$CHART/Chart.yaml")"
[[ -n "$want" ]] || { printf 'ERROR: no appVersion in %s/Chart.yaml\n' "$CHART" >&2; exit 1; }

cannot_read() {
  printf 'ERROR: cannot read the space-postgres data directory version in kind cluster %s:\n%s\n' \
    "$KIND_CLUSTER" "$1" >&2
  exit 1
}

# No claim yet, or one not yet bound: the chart will initialize an empty one.
if ! pv="$(kubectl --context "kind-$KIND_CLUSTER" -n "$NAMESPACE" \
    get pvc "$PVC" --ignore-not-found -o jsonpath='{.spec.volumeName}' 2>&1)"; then
  cannot_read "$pv"
fi
[[ -n "$pv" ]] || exit 0

# Kind's default StorageClass (local-path) provisions a directory on one node
# and pins the volume to that node, whose container has the node's name.
if ! location="$(kubectl --context "kind-$KIND_CLUSTER" get pv "$pv" -o jsonpath=\
'{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]} {.spec.hostPath.path}{.spec.local.path}' 2>&1)"; then
  cannot_read "$location"
fi
read -r node path <<<"$location"
[[ -n "${node:-}" && -n "${path:-}" ]] \
  || cannot_read "persistent volume $pv names no node and host path ('$location')"
if ! have="$(docker exec "$node" sh -c '[ ! -e "$1" ] || cat "$1"' sh \
    "$path/$PGDATA_SUBDIR/PG_VERSION" 2>&1)"; then
  cannot_read "$have"
fi
# initdb has not written it yet: nothing to be incompatible with.
[[ -n "$have" ]] || exit 0
[[ "$have" == "$want" ]] && exit 0

kctl="kubectl --context kind-$KIND_CLUSTER -n $NAMESPACE"
env_prefix=""
[[ "$KIND_CLUSTER" == space-local ]] || env_prefix="SPACE_KIND_CLUSTER=$KIND_CLUSTER "
{
  printf 'ERROR: the space-postgres data directory in kind cluster %s was initialized\n' "$KIND_CLUSTER"
  printf '       by PostgreSQL %s; this checkout runs PostgreSQL %s, which cannot start on\n' "$have" "$want"
  printf '       it, with TLS and a non-superuser master user whose roles the pod creates\n'
  printf '       only on an empty data directory. Reset the database; this deletes the\n'
  printf '       control-plane state (sandbox records, templates, API keys) and keeps the\n'
  printf '       cluster and its image caches:\n'
  printf '         %s delete statefulset space-postgres\n' "$kctl"
  printf '         %s delete pvc %s\n' "$kctl" "$PVC"
  printf '         %s delete job space-db-migrate space-seed-publish --ignore-not-found\n' "$kctl"
  printf '       then bring the stack up from this checkout again:\n'
  printf '         %sbash infra/space/kind/scripts/bootstrap.sh\n' "$env_prefix"
  printf '       (or re-run the smoke suite without SKIP_BOOTSTRAP, or "tilt up"; a Tilt\n'
  printf '       that is already running reloads with: tilt trigger "(Tiltfile)").\n'
  printf '       Recreating the cluster (teardown.sh, then bootstrap.sh) also works.\n'
  printf '       Nothing was changed. See "Recovering from an unbundled migration\n'
  printf '       version" in infra/space/kind/README.md.\n'
} >&2
exit 1
