#!/usr/bin/env bash
# check-node-vendor.sh — refuse a kind cluster whose SPACE nodes' vendor trees
# no longer match the host vendor tree they were copied from.
#
# Each sandbox worker and builder copies the host tree (SPACE_VENDOR_ROOT,
# mounted read-only at /opt/space-kind/host-vendor) into its own root-owned
# /space/vendor on its first boot, and keeps that copy on later boots
# (stage_vendor in node/space-kind-node.sh). install-fc-vendor.sh replaces the
# host tree's kernel in place when its pin changes (SPACE_FC_KERNEL_VERSION,
# SPACE_FC_KERNEL_SHA256), so on a reused cluster the change never reaches the
# nodes, and sandboxes fail later against node-manager's pins. Nothing else
# notices: the mount source is unchanged and the host tree is no input of the
# node image (check-kind-node-image.sh).
#
# Compares contents and link targets (`diff -rq --no-dereference`), not
# ownership or modes, which the copy changes. `artifacts` is excluded: the
# copy leaves out the host tree's, and node-manager publishes the node's own
# managed store there.
#
# Read-only. Exits 0 when the cluster does not exist or every SPACE node's copy
# matches, 1 (with the remedy on stderr) otherwise or when a node cannot be
# compared.
#
# Usage: check-node-vendor.sh [cluster]   (default: $SPACE_KIND_CLUSTER or space-local)
set -euo pipefail

KIND_CLUSTER="${1:-${SPACE_KIND_CLUSTER:-space-local}}"
KCTX="kind-$KIND_CLUSTER"
HOST_VENDOR_DIR=/opt/space-kind/host-vendor
VENDOR_DIR=/space/vendor

die() { printf '[check-node-vendor] ERROR: %s\n' "$*" >&2; exit 1; }

kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || exit 0

# Pool labels are applied by each kubelet as it registers.
nodes=""
for ((attempt = 1; attempt <= 15; attempt++)); do
  nodes="$(kubectl --context "$KCTX" get nodes \
    -l 'space.pplx.net/node-pool in (sandbox,builder)' \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  [[ -z "$nodes" ]] || break
  sleep 2
done
[[ -n "$nodes" ]] || die "no node labelled space.pplx.net/node-pool=sandbox or =builder in $KCTX"

drifted=()
for node in $nodes; do
  rc=0
  out="$(docker exec "$node" diff -rq --no-dereference -x artifacts \
    "$HOST_VENDOR_DIR" "$VENDOR_DIR" 2>&1)" || rc=$?
  case "$rc" in
    0) ;;
    1) drifted+=("$node:" "${out//$'\n'/$'\n'  }") ;;
    *) die "cannot compare $node's $VENDOR_DIR with the host vendor tree: $out" ;;
  esac
done

if (( ${#drifted[@]} > 0 )); then
  env_prefix=""
  [[ "$KIND_CLUSTER" == space-local ]] || env_prefix="SPACE_KIND_CLUSTER=$KIND_CLUSTER "
  {
    printf '[check-node-vendor] ERROR: kind cluster %s has SPACE nodes whose %s differs\n' "$KIND_CLUSTER" "$VENDOR_DIR"
    printf '       from the host vendor tree (%s):\n' "${SPACE_VENDOR_ROOT:-SPACE_VENDOR_ROOT}"
    printf '  %s\n' "${drifted[@]}"
    printf '       A node copies the host tree on its first boot and keeps its copy, as a\n'
    printf '       deployed node keeps the tree its image baked, so a host tree changed\n'
    printf '       since (install-fc-vendor.sh with another SPACE_FC_KERNEL_VERSION, for\n'
    printf '       example) never reaches it. Recreate the cluster to copy the current tree:\n'
    printf '           %sbash infra/space/kind/scripts/teardown.sh\n' "$env_prefix"
    printf '           %sbash infra/space/kind/scripts/bootstrap.sh\n' "$env_prefix"
    printf '       or restore the host tree the cluster was created from. Nothing was\n'
    printf '       changed. See "The node layer" in infra/space/kind/README.md.\n'
  } >&2
  exit 1
fi
printf '[check-node-vendor] %s: every SPACE node'"'"'s %s matches the host vendor tree\n' "$KIND_CLUSTER" "$VENDOR_DIR"
