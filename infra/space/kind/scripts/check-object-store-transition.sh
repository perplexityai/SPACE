#!/usr/bin/env bash
# check-object-store-transition.sh — refuse a kind cluster that still runs
# the SeaweedFS object store the stack replaced with versitygw.
#
# The two stores share nothing: versitygw serves a new data directory behind
# a new Service (`object-store:8333`), while a SeaweedFS-era cluster keeps its
# suspend snapshots and published templates in SeaweedFS, and its database
# records their storage locations as `http://seaweedfs:8333`. Applying the
# new charts to such a cluster would leave every existing restore and template
# pull depending on the old deployment, so every bring-up path stops here
# instead, and the operator recreates the cluster. See "Object store
# transition" in infra/space/kind/README.md.
#
# Read-only. Exits 0 when the cluster does not exist or carries no resource of
# the old `space-seaweedfs` chart, 1 (with the remedy on stderr) when it does
# or when the cluster cannot be read.
#
# Usage:
#   bash infra/space/kind/scripts/check-object-store-transition.sh [cluster]
#
# The cluster defaults to $SPACE_KIND_CLUSTER, then space-local.
set -euo pipefail

KIND_CLUSTER="${1:-${SPACE_KIND_CLUSTER:-space-local}}"
NAMESPACE=space

kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || exit 0

# Every resource the old chart rendered carries this label, its Service
# (`seaweedfs`) included. A namespace that does not exist yet lists nothing.
if ! legacy="$(kubectl --context "kind-$KIND_CLUSTER" -n "$NAMESPACE" \
    get deployments,services,secrets,jobs,pods \
    -l app.kubernetes.io/name=space-seaweedfs -o name 2>&1)"; then
  printf 'ERROR: cannot check kind cluster %s for the old SeaweedFS object store:\n%s\n' \
    "$KIND_CLUSTER" "$legacy" >&2
  exit 1
fi
[[ -z "$legacy" ]] && exit 0

env_prefix=""
[[ "$KIND_CLUSTER" == space-local ]] || env_prefix="SPACE_KIND_CLUSTER=$KIND_CLUSTER "
{
  printf 'ERROR: kind cluster %s still runs the SeaweedFS object store, which this\n' "$KIND_CLUSTER"
  printf '       checkout replaced with versitygw (Service object-store:8333). Found in\n'
  printf '       namespace %s:\n' "$NAMESPACE"
  while IFS= read -r resource; do printf '         %s\n' "$resource"; done <<<"$legacy"
  printf '       Its suspend snapshots and published templates live only in SeaweedFS,\n'
  printf '       and the database records their locations as http://seaweedfs:8333, so\n'
  printf '       neither can move to the new store. Recreate the cluster; this deletes\n'
  printf '       its sandboxes, snapshots and templates, and keeps the image caches:\n'
  printf '         %sbash infra/space/kind/scripts/teardown.sh\n' "$env_prefix"
  printf '         %sbash infra/space/kind/scripts/bootstrap.sh\n' "$env_prefix"
  printf '       (or re-run the smoke suite, which creates the cluster itself; Tilt\n'
  printf '       users then run "tilt up"). Nothing was changed. See "Object store\n'
  printf '       transition" in infra/space/kind/README.md.\n'
} >&2
exit 1
