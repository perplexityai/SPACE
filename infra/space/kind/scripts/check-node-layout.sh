#!/usr/bin/env bash
# check-node-layout.sh — refuse a kind cluster whose layout this checkout
# cannot run on: one that still runs the single `space-node-stack` pod per node
# the per-service node DaemonSets replaced, or one created without the host
# port mappings of the load-balanced entry.
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
# The entry: localhost reaches the API and data plane through host port
# mappings onto their NodePort Services, which kind-cluster.yaml.tmpl declares
# as `extraPortMappings`. Docker fixes a container's port mappings when it
# creates it, so a cluster created before them cannot gain them, and its
# localhost ports would answer nothing (or a stray forward) instead.
#
# Read-only. Exits 0 when the cluster does not exist, or carries no resource of
# the old `space-node-stack` chart and publishes every host port mapping the
# template declares; 1 (with the remedy on stderr) otherwise, or when the
# cluster cannot be read.
#
# Usage:
#   bash infra/space/kind/scripts/check-node-layout.sh [cluster]
#
# The cluster defaults to $SPACE_KIND_CLUSTER, then space-local.
set -euo pipefail

KIND_CLUSTER="${1:-${SPACE_KIND_CLUSTER:-space-local}}"
NAMESPACE=space
TEMPLATE="${SPACE_KIND_CLUSTER_TEMPLATE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kind-cluster.yaml.tmpl}"

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
env_prefix=""
[[ "$KIND_CLUSTER" == space-local ]] || env_prefix="SPACE_KIND_CLUSTER=$KIND_CLUSTER "

recreate_remedy() {
  printf '         %sbash infra/space/kind/scripts/teardown.sh\n' "$env_prefix"
  printf '         %sbash infra/space/kind/scripts/bootstrap.sh\n' "$env_prefix"
  printf '       (or re-run the smoke suite, which creates the cluster itself; Tilt\n'
  printf '       users then run "tilt up"). Nothing was changed.'
}

if [[ -n "$legacy" ]]; then
{
  printf 'ERROR: kind cluster %s still runs the single node-stack pod per node, which\n' "$KIND_CLUSTER"
  printf '       this checkout replaced with one DaemonSet per node service and a\n'
  printf '       runtime-supervisor systemd unit on each node. Found in namespace %s:\n' "$NAMESPACE"
  while IFS= read -r resource; do printf '         %s\n' "$resource"; done <<<"$legacy"
  printf '       Its sandboxes run inside that pod and cannot move to the node, and the\n'
  printf '       two layouts must not share a node. Recreate the cluster once; this\n'
  printf '       deletes its sandboxes, snapshots and templates, and keeps the image\n'
  printf '       caches:\n'
  recreate_remedy
  printf ' See "Upgrading from\n'
  printf '       the node-stack layout" in infra/space/kind/README.md.\n'
} >&2
exit 1
fi

# `<containerPort>/<protocol> <listenAddress> <hostPort>` for each mapping the
# template declares, in the form Docker reports a published port.
declared="$(awk '
  function emit() { if (container != "") print container "/" tolower(protocol) " " address " " host }
  /- containerPort:/ { emit(); container = $3; host = ""; address = "0.0.0.0"; protocol = "TCP"; next }
  container != "" && $1 == "hostPort:" { host = $2 }
  container != "" && $1 == "listenAddress:" { address = $2 }
  container != "" && $1 == "protocol:" { protocol = $2 }
  container != "" && /^  - role:/ { emit(); container = "" }
  END { emit() }' "$TEMPLATE" | sort -u)"
[[ -n "$declared" ]] || exit 0

if ! containers="$(docker ps -a --filter "label=io.x-k8s.kind.cluster=$KIND_CLUSTER" --format '{{.Names}}' 2>&1)" \
    || [[ -z "$containers" ]]; then
  printf 'ERROR: cannot list the node containers of kind cluster %s:\n%s\n' "$KIND_CLUSTER" "$containers" >&2
  exit 1
fi
# shellcheck disable=SC2086 # one argument per container name
published="$(docker inspect --format \
  '{{range $port, $bindings := .HostConfig.PortBindings}}{{range $bindings}}{{$port}} {{.HostIp}} {{.HostPort}}{{"\n"}}{{end}}{{end}}' \
  $containers | sort -u)"
missing="$(comm -23 <(printf '%s\n' "$declared") <(printf '%s\n' "$published"))"
[[ -z "$missing" ]] && exit 0

{
  printf 'ERROR: kind cluster %s does not publish the host ports this checkout\n' "$KIND_CLUSTER"
  printf '       reaches the API and data plane through (kind-cluster.yaml.tmpl,\n'
  printf '       extraPortMappings onto their NodePort Services). Missing, as\n'
  printf '       <node port>/<protocol> <address> <host port>:\n'
  while IFS= read -r mapping; do printf '         %s\n' "$mapping"; done <<<"$missing"
  printf '       Docker fixes port mappings when it creates a node, so recreate the\n'
  printf '       cluster once; this deletes its sandboxes, snapshots and templates, and\n'
  printf '       keeps the image caches:\n'
  recreate_remedy
  printf ' See "Upgrading from\n'
  printf '       the node-stack layout" in infra/space/kind/README.md.\n'
} >&2
exit 1
