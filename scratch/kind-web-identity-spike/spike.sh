#!/usr/bin/env bash
# SPIKE prototype (scratch/kind-web-identity-spike/REPORT.md): switch a running
# Kind cluster's credential authority and cluster-controller to web-identity
# base credentials, and back. Patches live objects; Tilt re-applying a chart
# reverts its part. Usage: spike.sh apply | revert | status
# SPIKE_CONTROLLER_IDENTITY=pod-identity gives cluster-controller EKS Pod
# Identity's container-provider settings, served by the bridge, instead of
# web identity.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
STATE="${SPIKE_STATE:-$HOME/.space-devbox/kind/web-identity-spike}"
BRIDGE_IMAGE=space-kind-sts-bridge:spike-relay
K8S_ISSUER=kubernetes.default.svc.cluster.local
k() { kubectl --context "kind-$CLUSTER" -n space "$@"; }
TOKEN_DIR=/var/run/secrets/eks.amazonaws.com/serviceaccount

# Saves an object once, so revert restores what the charts applied.
save() {
  mkdir -p "$STATE"
  [[ -s "$STATE/$1-$2.json" ]] || k get "$1" "$2" -o json \
    | jq 'del(.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp,.metadata.generation,.metadata.managedFields,.status)' \
    >"$STATE/$1-$2.json"
}
CONTROLLER_IDENTITY="${SPIKE_CONTROLLER_IDENTITY:-web-identity}"
CONTROLLER_ROLE=arn:aws:iam::000000000000:role/kind-cluster-controller
patch_json() { k get "$1" "$2" -o json | jq "$3" | k replace -f - >/dev/null; }

# The SDK's web-identity provider (AWS_ROLE_ARN, AWS_WEB_IDENTITY_TOKEN_FILE)
# in place of static keys, with a projected token as IRSA projects it.
web_identity_env() {
  cat <<JQ
  .spec.template.spec.containers |= map(if .name == "$1" then
      .env = ([.env[] | select(.name != "AWS_ACCESS_KEY_ID" and .name != "AWS_SECRET_ACCESS_KEY"
                and .name != "AWS_ROLE_ARN" and .name != "AWS_WEB_IDENTITY_TOKEN_FILE"
                and .name != "AWS_ENDPOINT_URL_STS" and .name != "SSL_CERT_DIR"
                and .name != "AWS_CONTAINER_CREDENTIALS_FULL_URI" and .name != "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")]
        + [{name: "AWS_ROLE_ARN", value: "$2"},
           {name: "AWS_WEB_IDENTITY_TOKEN_FILE", value: "$TOKEN_DIR/token"},
           {name: "AWS_ENDPOINT_URL_STS", value: "$3"},
           {name: "SSL_CERT_DIR", value: "/etc/ssl/certs:/etc/space-rpc-ca"}])
    | .volumeMounts = ([.volumeMounts[] | select(.name != "aws-iam-token")]
        + [{name: "aws-iam-token", mountPath: "$TOKEN_DIR", readOnly: true}])
    else . end)
  | .spec.template.spec.volumes = ([.spec.template.spec.volumes[] | select(.name != "aws-iam-token")]
      + [{name: "aws-iam-token", projected: {sources: [{serviceAccountToken:
          {audience: "sts.amazonaws.com", expirationSeconds: 3600, path: "token"}}]}}])
JQ
}

# EKS Pod Identity's settings, as its webhook injects them.
pod_identity_env() {
  local dir=/var/run/secrets/pods.eks.amazonaws.com/serviceaccount
  cat <<JQ
  .spec.template.spec.containers |= map(if .name == "$1" then
      .env = ([.env[] | select(.name != "AWS_ACCESS_KEY_ID" and .name != "AWS_SECRET_ACCESS_KEY"
                and .name != "AWS_ROLE_ARN" and .name != "AWS_WEB_IDENTITY_TOKEN_FILE"
                and .name != "AWS_ENDPOINT_URL_STS" and .name != "SSL_CERT_DIR"
                and .name != "AWS_CONTAINER_CREDENTIALS_FULL_URI" and .name != "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")]
        + [{name: "AWS_CONTAINER_CREDENTIALS_FULL_URI", value: "https://sts-bridge:8080/v1/credentials"},
           {name: "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", value: "$dir/eks-pod-identity-token"},
           {name: "SSL_CERT_DIR", value: "/etc/ssl/certs:/etc/space-rpc-ca"}])
    | .volumeMounts = ([.volumeMounts[] | select(.name != "aws-iam-token" and .name != "eks-pod-identity-token")]
        + [{name: "eks-pod-identity-token", mountPath: "$dir", readOnly: true}])
    else . end)
  | .spec.template.spec.volumes = ([.spec.template.spec.volumes[] | select(.name != "aws-iam-token" and .name != "eks-pod-identity-token")]
      + [{name: "eks-pod-identity-token", projected: {sources: [{serviceAccountToken:
          {audience: "pods.eks.amazonaws.com", expirationSeconds: 86400, path: "eks-pod-identity-token"}}]}}])
JQ
}

apiserver_thumbprint() {
  local server host port
  server="$(kubectl config view -o jsonpath="{.clusters[?(@.name==\"kind-$CLUSTER\")].cluster.server}")"
  host="${server#https://}"; port="${host##*:}"; host="${host%:*}"
  # The top certificate of the chain the apiserver serves, as versitygw pins.
  python3 - "$host" "$port" <<'PY'
import hashlib, socket, ssl, sys
ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
with socket.create_connection((sys.argv[1], int(sys.argv[2]))) as s:
    with ctx.wrap_socket(s, server_hostname="kubernetes.default.svc") as t:
        chain = t.get_unverified_chain() if hasattr(t, "get_unverified_chain") else None
        der = chain[-1].public_bytes(ssl._ssl.ENCODING_DER) if chain else t.getpeercert(True)
        print(hashlib.sha1(der).hexdigest())
PY
}

apply() {
  docker build -q -f "$DIR/Dockerfile.sts-bridge" -t "$BRIDGE_IMAGE" "$DIR" >/dev/null
  local infra; infra="$(kubectl --context "kind-$CLUSTER" get nodes -l space.pplx.net/infra=true -o jsonpath='{.items[*].metadata.name}')"
  kind load docker-image --name "$CLUSTER" --nodes "${infra// /,}" "$BRIDGE_IMAGE"
  kubectl --context "kind-$CLUSTER" apply -f "$DIR/manifests/oidc-discovery-rbac.yaml"
  for o in deployment/space-object-store-iam networkpolicy/space-object-store-iam deployment/space-kind-sts-bridge \
           deployment/space-credmgr-authority deployment/space-cluster-controller; do save "${o%/*}" "${o#*/}"; done

  # versitygw fetches the cluster issuer's discovery document from the
  # apiserver's Service; the override also admits that private address.
  local map="https://$K8S_ISSUER=https://kubernetes.default.svc/.well-known/openid-configuration"
  patch_json deployment space-object-store-iam \
    '.spec.template.spec.containers[0].args |= (if any(.[]; . == "'"$map"'") then . else . + ["--oidc-discovery-url", "'"$map"'"] end)'
  # cluster-controller calls the IAM service's STS directly.
  patch_json networkpolicy space-object-store-iam \
    '.spec.ingress[0].from |= (map(select(.podSelector.matchLabels["app.kubernetes.io/name"] != "space-cluster-controller")) + [{podSelector: {matchLabels: {"app.kubernetes.io/name": "space-cluster-controller"}}}])'
  k rollout status deployment/space-object-store-iam --timeout=180s

  local tp; tp="$(apiserver_thumbprint)"; echo "apiserver thumbprint $tp"
  k delete job spike-web-identity-iam-setup --ignore-not-found >/dev/null
  SPIKE_THUMBPRINT="$tp" envsubst '${SPIKE_THUMBPRINT}' <"$DIR/manifests/iam-setup-job.yaml" | k apply -f -
  k wait --for=condition=complete job/spike-web-identity-iam-setup --timeout=300s
  k logs job/spike-web-identity-iam-setup | tail -20

  patch_json deployment space-kind-sts-bridge \
    '.spec.template.spec.containers[0].image = "'$BRIDGE_IMAGE'" | .spec.template.spec.containers[0].imagePullPolicy = "Never" | .spec.template.spec.containers[0].args |= (map(select(. != "-relay-web-identity" and (startswith("-pod-identity-roles=") | not))) + ["-relay-web-identity", "-pod-identity-roles=space/space-cluster-controller='$CONTROLLER_ROLE'"])'
  patch_json deployment space-credmgr-authority "$(web_identity_env credmgr-authority arn:aws:iam::000000000000:role/kind-credmgr-authority https://sts-bridge:8080)"
  if [[ "$CONTROLLER_IDENTITY" == pod-identity ]]; then
    patch_json deployment space-cluster-controller "$(pod_identity_env cluster-controller)"
  else
    patch_json deployment space-cluster-controller "$(web_identity_env cluster-controller "$CONTROLLER_ROLE" https://object-store-iam:7070)"
  fi
  for d in space-kind-sts-bridge space-credmgr-authority space-cluster-controller; do k rollout status "deployment/$d" --timeout=300s; done
}

revert() {
  for f in "$STATE"/*.json; do [[ -e "$f" ]] && k replace -f "$f" >/dev/null && echo "restored $(basename "$f" .json)"; done
  rm -rf "$STATE"
  kubectl --context "kind-$CLUSTER" delete -f "$DIR/manifests/oidc-discovery-rbac.yaml" --ignore-not-found
  k delete -f "$DIR/manifests/probe.yaml" --ignore-not-found --wait=false
  k delete job spike-web-identity-iam-setup spike-web-identity-iam-cleanup --ignore-not-found >/dev/null
  local trust; trust="$(k get job space-object-store-bucket-init -o json | jq -r '.spec.template.spec.containers[0].env[] | select(.name == "TRUST_POLICY") | .value')"
  SPIKE_TRUST_POLICY="$trust" envsubst '${SPIKE_TRUST_POLICY}' <"$DIR/manifests/iam-cleanup-job.yaml" | k apply -f -
  k wait --for=condition=complete job/spike-web-identity-iam-cleanup --timeout=300s
  k logs job/spike-web-identity-iam-cleanup | tail -5
  k delete job spike-web-identity-iam-setup spike-web-identity-iam-cleanup --ignore-not-found >/dev/null
  for d in space-object-store-iam space-kind-sts-bridge space-credmgr-authority space-cluster-controller; do k rollout status "deployment/$d" --timeout=300s; done
}

status() {
  for d in space-kind-sts-bridge space-credmgr-authority space-cluster-controller; do
    printf '%s: ' "$d"
    k get deployment "$d" -o json | jq -c '[.spec.template.spec.containers[0] | .image, ([.env[]?.name] | map(select(startswith("AWS_")))), (.args // [] | map(select(startswith("-relay") or startswith("-pod"))))]'
  done
}

"${1:?apply|revert|status}"
