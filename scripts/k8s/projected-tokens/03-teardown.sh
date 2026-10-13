#!/usr/bin/env bash
# Revert the projected-token wiring applied by 01-apply-auth.sh: drop the
# TokenReview RBAC, the node-api k8sAuth env, and the space-api projected
# token volume/env. Returns node-api to static-only auth.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
require kubectl
assert_local_context

log "remove TokenReview RBAC"
kubectl --context "$KCTX" delete clusterrole space-node-api-tokenreview --ignore-not-found
kubectl --context "$KCTX" delete clusterrolebinding space-node-api-tokenreview --ignore-not-found

log "remove k8sAuth env from node-api"
k set env "daemonset/${NODE_API_DS}" -c "${NODE_API_CONTAINER}" \
  SPACE_K8S_TOKEN_AUTH_ENABLED- \
  SPACE_K8S_TOKEN_AUTH_NAMESPACE- \
  SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED- \
  SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS- \
  SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS- \
  SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS- \
  SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS- || true

log "remove projected token + env from space-api"
k set env "deploy/${SPACE_API_DEPLOY}" -c "${SPACE_API_CONTAINER}" \
  SPACE_NODE_API_TOKEN_PATH- SPACE_NODE_API_INTERNAL_TOKEN_PATH- || true
# Delete the space-auth volume AND its mount by NAME, never by positional index:
# anything (e.g. a tilt reconcile) may reorder volumes, so removing /volumes/0
# could drop an unrelated volume, and an orphaned mount would wedge the pod.
# Strategic-merge `$patch: delete` keys on the volume name / mount path and is a
# no-op if they're already gone.
k patch "deploy/${SPACE_API_DEPLOY}" --type strategic --patch "$(cat <<EOF
spec:
  template:
    spec:
      volumes:
        - \$patch: delete
          name: space-auth
      containers:
        - name: ${SPACE_API_CONTAINER}
          volumeMounts:
            - \$patch: delete
              mountPath: ${TOKEN_MOUNT}
EOF
)" || warn "couldn't remove the space-auth volume/mount; check the ${SPACE_API_DEPLOY} spec by hand"

log "done."
