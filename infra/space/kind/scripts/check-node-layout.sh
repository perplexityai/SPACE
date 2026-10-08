#!/usr/bin/env bash
# check-node-layout.sh — refuse a kind cluster that still runs the single
# `space-node-stack` pod per node the per-service node DaemonSets replaced.
#
# In that layout one pod ran every node service, and the runtime-supervisor
# with every Firecracker VM ran inside it, in the pod's mount namespace. Now
# runtime-supervisor is a systemd unit on the node and each service has its
# own DaemonSet. Applying the new charts beside the old pod would run two
# node-managers, sandbox-managers and supervisors over the same /space/run and
# /space/cache, and the old pod's VMs cannot move to the node's supervisor, so
# every bring-up path stops here instead and the operator recreates the
# cluster once. See "Upgrading from the node-stack layout" in
# infra/space/kind/README.md.
#
# Read-only. Exits 0 when the cluster does not exist or carries no resource of
# the old `space-node-stack` chart, 1 (with the remedy on stderr) when it does
# or when the cluster cannot be read.
#
# Usage:
#   bash infra/space/kind/scripts/check-node-layout.sh [cluster]
#
# The cluster defaults to $SPACE_KIND_CLUSTER, then space-local.
set -euo pipefail

KIND_CLUSTER="${1:-${SPACE_KIND_CLUSTER:-space-local}}"
NAMESPACE=space

kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || exit 0

# Every DaemonSet, ServiceAccount and pod the old chart rendered carries this
# label. A namespace that does not exist yet lists nothing.
if ! legacy="$(kubectl --context "kind-$KIND_CLUSTER" -n "$NAMESPACE" \
    get daemonsets,serviceaccounts,pods \
    -l space.pplx.net/chart=space-node-stack -o name 2>&1)"; then
  printf 'ERROR: cannot check kind cluster %s for the old node-stack layout:\n%s\n' \
    "$KIND_CLUSTER" "$legacy" >&2
  exit 1
fi
[[ -z "$legacy" ]] && exit 0

env_prefix=""
[[ "$KIND_CLUSTER" == space-local ]] || env_prefix="SPACE_KIND_CLUSTER=$KIND_CLUSTER "
{
  printf 'ERROR: kind cluster %s still runs the single node-stack pod per node, which\n' "$KIND_CLUSTER"
  printf '       this checkout replaced with one DaemonSet per node service and a\n'
  printf '       runtime-supervisor systemd unit on each node. Found in namespace %s:\n' "$NAMESPACE"
  while IFS= read -r resource; do printf '         %s\n' "$resource"; done <<<"$legacy"
  printf '       Its sandboxes run inside that pod and cannot move to the node, and the\n'
  printf '       two layouts must not share a node. Recreate the cluster once; this\n'
  printf '       deletes its sandboxes, snapshots and templates, and keeps the image\n'
  printf '       caches:\n'
  printf '         %sbash infra/space/kind/scripts/teardown.sh\n' "$env_prefix"
  printf '         %sbash infra/space/kind/scripts/bootstrap.sh\n' "$env_prefix"
  printf '       (or re-run the smoke suite, which creates the cluster itself; Tilt\n'
  printf '       users then run "tilt up"). Nothing was changed. See "Upgrading from\n'
  printf '       the node-stack layout" in infra/space/kind/README.md.\n'
} >&2
exit 1
