#!/usr/bin/env bash
# Wire the projected-token (TokenReview) auth path onto a running local stack:
#   - server (node-api DaemonSet): a tokenreviews/create ClusterRole bound to its SA,
#     plus the SPACE_K8S_TOKEN_AUTH_* env that flips node-api into
#     k8s_token_review mode (static fallback kept on).
#   - client (space-api): projected ServiceAccount tokens for the normal and
#     internal node-api audiences, with token paths set on both client surfaces.
#
# Idempotent: safe to re-run, including after a `tilt up` reconcile re-applies
# the charts and drops the env/volume patches.
#
# The Kind node-api is built with the `k8s` Cargo feature
# (docker/Dockerfile.rust-build-kind); without it node-api fails loud at startup
# with KubernetesAuthFeatureDisabled instead of booting k8s mode.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require kubectl
assert_local_context

log "1/3 apply TokenReview RBAC (ClusterRole + binding -> ${NS}/${NODE_API_SA})"
kubectl --context "$KCTX" apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: space-node-api-tokenreview
rules:
  - apiGroups: ["authentication.k8s.io"]
    resources: ["tokenreviews"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: space-node-api-tokenreview
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: space-node-api-tokenreview
subjects:
  - kind: ServiceAccount
    name: ${NODE_API_SA}
    namespace: ${NS}
YAML

log "2/3 enable k8sAuth on node-api (rolls the DaemonSet)"
k set env "daemonset/${NODE_API_DS}" -c "${NODE_API_CONTAINER}" \
  SPACE_K8S_TOKEN_AUTH_ENABLED=true \
  SPACE_K8S_TOKEN_AUTH_NAMESPACE="${NS}" \
  SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED=true \
  SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS="${NODE_API_ALLOWED_SAS}" \
  SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS="${NODE_API_TRUSTED_WAIT_READY_SAS}" \
  SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS="${NODE_API_TRUSTED_POLICY_SAS}" \
  SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS="${NODE_API_INTERNAL_ALLOWED_SAS}"

log "3/3 mount projected NodeApi tokens on ${SPACE_API_DEPLOY} + point the clients at them"
k patch "deploy/${SPACE_API_DEPLOY}" --type strategic --patch "
spec:
  template:
    spec:
      volumes:
        - name: space-auth
          projected:
            sources:
              - serviceAccountToken:
                  path: ${TOKEN_FILE}
                  audience: ${NODE_API_AUDIENCE}
                  expirationSeconds: 600
              - serviceAccountToken:
                  path: ${INTERNAL_TOKEN_FILE}
                  audience: ${NODE_API_INTERNAL_AUDIENCE}
                  expirationSeconds: 600
      containers:
        - name: ${SPACE_API_CONTAINER}
          volumeMounts:
            - name: space-auth
              mountPath: ${TOKEN_MOUNT}
              readOnly: true
"
k set env "deploy/${SPACE_API_DEPLOY}" -c "${SPACE_API_CONTAINER}" \
  SPACE_NODE_API_TOKEN_PATH="${TOKEN_PATH}" \
  SPACE_NODE_API_INTERNAL_TOKEN_PATH="${INTERNAL_TOKEN_PATH}"

log "waiting for rollouts..."
k rollout status "daemonset/${NODE_API_DS}" --timeout=300s
k rollout status "deploy/${SPACE_API_DEPLOY}" --timeout=120s

log "done. node-api auth mode:"
k logs "$(node_api_pod)" -c "${NODE_API_CONTAINER}" 2>/dev/null | grep -E '"node-api auth initialized"' | tail -1 \
  || warn "no auth-init line yet; check 'kubectl -n ${NS} logs <node-api-pod>'"
