#!/usr/bin/env bash
# Canonical SPACE node runtime identities.
#
# This file is sourced by direct-node deploy tooling, Packer provisioning, the
# worker image build, and the EKS Helm overlay renderer.

SPACE_GROUP_SPECS="
space-nodeclients:950
space-sbxclients:951
space-cache-rw:952
space-run-rw:953
space-tmplclients:954
space-tmplmgr:955
space-sidecarclients:956
space-buildclients:957
space-nodectlclients:958
space-nodectl:959
space-sbxmgr:960
space-nodeapi:961
space-rtmgrclients:962
space-rtmgr:963
space-volmgr:964
space-volcredclients:965
"

# Groups managed as supplementary access groups for local operators/services.
# Service primary groups are intentionally excluded.
SPACE_MANAGED_GROUPS="space-volcredclients space-nodeclients space-sbxclients space-cache-rw space-run-rw space-tmplclients space-sidecarclients space-buildclients space-nodectlclients space-rtmgrclients"

# Local deploy/debug accounts need only client socket groups. Direct cache/run
# writes stay scoped to services so RPC authorization remains the control point.
SPACE_OPERATOR_GROUPS="space-nodeclients space-sbxclients space-tmplclients space-nodectlclients space-rtmgrclients"

SPACE_VOLMGR_USER=space-volmgr
SPACE_VOLMGR_UID=964
SPACE_VOLMGR_GROUP=space-volmgr
SPACE_VOLMGR_GID=964

SPACE_SBXMGR_USER=space-sbxmgr
SPACE_SBXMGR_UID=950
SPACE_SBXMGR_GROUP=space-sbxmgr
SPACE_SBXMGR_GID=960

SPACE_TMPLMGR_USER=space-tmplmgr
SPACE_TMPLMGR_UID=955
SPACE_TMPLMGR_GROUP=space-tmplmgr
SPACE_TMPLMGR_GID=955

SPACE_NODEAPI_USER=space-nodeapi
SPACE_NODEAPI_UID=954
SPACE_NODEAPI_GROUP=space-nodeapi
SPACE_NODEAPI_GID=961

SPACE_NODECTL_USER=space-nodectl
SPACE_NODECTL_UID=959
SPACE_NODECTL_GROUP=space-nodectl
SPACE_NODECTL_GID=959

SPACE_KUBERNETES_NODE_CONTROLLER_USER=space-k8s-node-controller
SPACE_KUBERNETES_NODE_CONTROLLER_UID=958
SPACE_KUBERNETES_NODE_CONTROLLER_GROUP=space-nodectlclients
SPACE_KUBERNETES_NODE_CONTROLLER_GID=958

SPACE_RTMGR_USER=space-rtmgr
SPACE_RTMGR_UID=963
SPACE_RTMGR_GROUP=space-rtmgr
SPACE_RTMGR_GID=963

# alias:name:uid:primary_group. Alias names are used by generators that need
# stable Rust/Python identifiers while shell consumers use the expanded values.
SPACE_USER_SPECS="
SPACE_VOLMGR:$SPACE_VOLMGR_USER:$SPACE_VOLMGR_UID:$SPACE_VOLMGR_GROUP
SPACE_SBXMGR:$SPACE_SBXMGR_USER:$SPACE_SBXMGR_UID:$SPACE_SBXMGR_GROUP
SPACE_TMPLMGR:$SPACE_TMPLMGR_USER:$SPACE_TMPLMGR_UID:$SPACE_TMPLMGR_GROUP
SPACE_NODEAPI:$SPACE_NODEAPI_USER:$SPACE_NODEAPI_UID:$SPACE_NODEAPI_GROUP
SPACE_NODECTL:$SPACE_NODECTL_USER:$SPACE_NODECTL_UID:$SPACE_NODECTL_GROUP
SPACE_K8S_NODE_CONTROLLER:$SPACE_KUBERNETES_NODE_CONTROLLER_USER:$SPACE_KUBERNETES_NODE_CONTROLLER_UID:$SPACE_KUBERNETES_NODE_CONTROLLER_GROUP
SPACE_RTMGR:$SPACE_RTMGR_USER:$SPACE_RTMGR_UID:$SPACE_RTMGR_GROUP
"

SPACE_SANDBOX_MANAGER_GROUPS="space-nodeclients space-sbxclients space-tmplclients space-cache-rw space-sidecarclients space-nodectlclients"
SPACE_TEMPLATE_MANAGER_GROUPS="space-nodeclients space-sbxclients space-tmplclients space-cache-rw space-nodectlclients"
SPACE_CREDENTIAL_MANAGER_GROUPS="space-sbxclients space-volcredclients"
# sbxclients is needed to publish volume-manager's own client socket; it does
# not authorize this UID on the standard credential-manager listener.
SPACE_VOLUME_MANAGER_GROUPS="space-sbxclients space-cache-rw space-volcredclients space-nodeclients"
# The runtime manager reads the world-readable vendor and cache layouts. It
# needs its own socket group so it can hand that group to its socket, and
# node-manager's client group so it can ask node-manager to publish installs.
SPACE_RUNTIME_MANAGER_GROUPS="space-rtmgrclients space-nodeclients"
SPACE_NODECTL_GROUPS="space-nodeclients space-sbxclients space-tmplclients space-nodectlclients space-rtmgrclients"
SPACE_NODEAPI_WORKER_GROUPS="space-nodeclients space-sbxclients space-tmplclients space-nodectlclients"
SPACE_NODEAPI_BUILDER_GROUPS="$SPACE_NODEAPI_WORKER_GROUPS space-buildclients"
SPACE_NODEAPI_CLUSTER_GROUPS="space-sbxclients space-tmplclients"
SPACE_BUILD_MANAGER_GROUPS="space-cache-rw space-run-rw space-buildclients"
SPACE_KUBERNETES_NODE_CONTROLLER_GROUPS="space-tmplclients space-tmplmgr"

space_primary_group_for_user() {
  local lookup="$1" alias user uid group
  while IFS=: read -r alias user uid group; do
    [[ -z "$user" ]] && continue
    if [[ "$user" == "$lookup" ]]; then
      echo "$group"
      return 0
    fi
  done <<< "$SPACE_USER_SPECS"
  echo ""
}

space_uid_for_user() {
  local lookup="$1" alias user uid group
  while IFS=: read -r alias user uid group; do
    [[ -z "$user" ]] && continue
    if [[ "$user" == "$lookup" ]]; then
      echo "$uid"
      return 0
    fi
  done <<< "$SPACE_USER_SPECS"
  echo ""
}

space_expected_gid_for_group() {
  local group="$1" spec
  for spec in $SPACE_GROUP_SPECS; do
    if [[ "${spec%%:*}" == "$group" ]]; then
      echo "${spec##*:}"
      return 0
    fi
  done
  return 1
}

space_is_service_primary_group() {
  local lookup="$1" alias user uid group managed_group
  while IFS=: read -r alias user uid group; do
    [[ -z "$group" ]] && continue
    [[ "$group" == "$lookup" ]] || continue
    for managed_group in $SPACE_MANAGED_GROUPS; do
      [[ "$managed_group" == "$group" ]] && return 1
    done
    return 0
  done <<< "$SPACE_USER_SPECS"
  return 1
}
