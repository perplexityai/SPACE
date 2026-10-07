#!/usr/bin/env bash
# internal-rpc-tls.sh — provision the Kind cluster's internal RPC TLS
# identities as Kubernetes Secrets, the way the EKS module does
# (infra/space/aws/eks/modules/space/internal_rpc_tls.tf,
# credmgr_authority_tls.tf; runtime contract in docs/internal-rpc-tls.md):
#
#   space-internal-rpc-ca        ca.crt           a per-cluster CA every
#                                                 ClusterApi/NodeApi/BuildManager
#                                                 caller trusts
#   space-cluster-api-tls        tls.crt/tls.key  internal-api's leaf:
#                                                 space-internal-api, space-cluster-api
#   space-node-api-tls           tls.crt/tls.key  every node-api's shared leaf:
#                                                 space-node-api
#   space-build-manager-tls      tls.crt/tls.key  build-manager's leaf:
#                                                 space-builder-build-manager
#   space-ingress-activation-tls tls.crt/tls.key  the api's IngressActivationApi
#                                                 leaf: space-api, the Service
#                                                 internal-api dials
#   space-credmgr-authority-tls  tls.crt/tls.key  the credential-manager
#                                                 authority's self-signed
#                                                 end-entity certificate
#   space-credmgr-authority-trust ca.crt          that certificate alone, the
#                                                 trust anchor its clients mount
#
# Keys are ECDSA P-256. The CA lives ten years and each internal RPC leaf one
# year, reissued once it is within 60 days of expiry; the credential-manager
# certificate lives ten years and is not a CA, as on EKS. Leaves carry the
# short service names only, no IP SANs: callers that dial a node IP or
# loopback set the matching *_TLS_SERVER_NAME. The CA's key is kept in the
# Kind-only space-internal-rpc-ca-signing Secret, which no workload mounts,
# so later runs can reissue leaves under the same trust.
#
# Idempotent: present, valid material is kept. A reissued listener identity
# takes effect at the listener's next start (listeners load it only then), so
# this restarts the listeners whose Secret it replaced.
#
# Usage:
#   bash infra/space/kind/scripts/internal-rpc-tls.sh
#
# Env knobs:
#   SPACE_KIND_CLUSTER   kind cluster name (default: space-local)
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
KCTX="kind-$KIND_CLUSTER"
NAMESPACE=space
CA_SECRET=space-internal-rpc-ca
CA_SIGNING_SECRET=space-internal-rpc-ca-signing
CREDMGR_SECRET=space-credmgr-authority-tls
CREDMGR_TRUST_SECRET=space-credmgr-authority-trust
CA_DAYS=3650
LEAF_DAYS=365
RENEW_WITHIN_SECS=$((60 * 24 * 3600))

# secret | common name | DNS SANs | workloads that load it at start
LEAVES=(
  "space-cluster-api-tls|space-cluster-api|space-internal-api,space-cluster-api|deployment/space-internal-api"
  "space-node-api-tls|space-node-api|space-node-api|daemonset/space-node-api daemonset/space-builder-node-api"
  "space-build-manager-tls|space-build-manager|space-builder-build-manager|daemonset/space-builder-build-manager"
  "space-ingress-activation-tls|space-ingress-activation|space-api|deployment/space-api"
)

log() { printf '[internal-rpc-tls] %s\n' "$*"; }
k() { kubectl --context "$KCTX" -n "$NAMESPACE" "$@"; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
umask 077

# Writes key $2 of Secret $1 to file $3; fails if absent.
secret_key() {
  local data
  data="$(k get secret "$1" -o "jsonpath={.data.${2//./\\.}}" 2>/dev/null)" || return 1
  [[ -n "$data" ]] || return 1
  base64 -d <<<"$data" >"$3"
}

# Creates or replaces Secret $1 from the remaining key=file pairs.
apply_secret() {
  local name=$1; shift
  local args=()
  for pair in "$@"; do args+=(--from-file="$pair"); done
  k create secret generic "$name" "${args[@]}" --dry-run=client -o yaml \
    | kubectl --context "$KCTX" apply -f - >/dev/null
}

# True when certificate file $1 is still valid beyond the renewal window.
fresh() { openssl x509 -in "$1" -noout -checkend "$RENEW_WITHIN_SECS" >/dev/null 2>&1; }

ext_file() { # $1 DNS SANs (comma-separated), $2 basicConstraints, $3 keyUsage, [$4 extendedKeyUsage]
  local sans="" dns
  IFS=, read -r -a dns <<<"$1"
  for d in "${dns[@]}"; do sans+="${sans:+,}DNS:$d"; done
  {
    printf 'basicConstraints = critical, %s\nkeyUsage = critical, %s\n' "$2" "$3"
    [[ -z "${4:-}" ]] || printf 'extendedKeyUsage = %s\n' "$4"
    [[ -z "$sans" ]] || printf 'subjectAltName = %s\n' "$sans"
    printf 'subjectKeyIdentifier = hash\n'
  } >"$work/ext.cnf"
}

# ── CA ────────────────────────────────────────────────────────────────────────
if secret_key "$CA_SIGNING_SECRET" tls.crt "$work/ca.crt" \
  && secret_key "$CA_SIGNING_SECRET" tls.key "$work/ca.key" \
  && fresh "$work/ca.crt"; then
  log "keeping the cluster's internal RPC CA"
else
  log "issuing a new internal RPC CA for $KCTX"
  openssl ecparam -name prime256v1 -genkey -noout -out "$work/ca.key"
  ext_file "" "CA:TRUE" "keyCertSign, cRLSign"
  openssl req -x509 -new -key "$work/ca.key" -sha256 -days "$CA_DAYS" \
    -subj "/CN=$KIND_CLUSTER internal RPC CA" -extensions v3 \
    -config <(printf '[req]\ndistinguished_name=dn\n[dn]\n[v3]\n'; cat "$work/ext.cnf") \
    -out "$work/ca.crt" 2>/dev/null
  apply_secret "$CA_SIGNING_SECRET" "tls.crt=$work/ca.crt" "tls.key=$work/ca.key"
fi
existing_ca="$work/existing-ca.crt"
ca_replaced=false
if ! secret_key "$CA_SECRET" ca.crt "$existing_ca"; then
  apply_secret "$CA_SECRET" "ca.crt=$work/ca.crt"
elif ! cmp -s "$existing_ca" "$work/ca.crt"; then
  apply_secret "$CA_SECRET" "ca.crt=$work/ca.crt"
  ca_replaced=true
fi

# ── Listener leaves ───────────────────────────────────────────────────────────
restart=()
for leaf in "${LEAVES[@]}"; do
  IFS='|' read -r secret cn sans workloads <<<"$leaf"
  if secret_key "$secret" tls.crt "$work/leaf.crt" && secret_key "$secret" tls.key "$work/leaf.key" \
    && fresh "$work/leaf.crt" \
    && openssl verify -CAfile "$work/ca.crt" "$work/leaf.crt" >/dev/null 2>&1; then
    continue
  fi
  replaced=false
  k get secret "$secret" >/dev/null 2>&1 && replaced=true
  log "issuing $secret ($sans)"
  openssl ecparam -name prime256v1 -genkey -noout -out "$work/leaf.key"
  openssl pkcs8 -topk8 -nocrypt -in "$work/leaf.key" -out "$work/leaf.p8"
  openssl req -new -key "$work/leaf.p8" -subj "/CN=$cn" -out "$work/leaf.csr"
  ext_file "$sans" "CA:FALSE" "digitalSignature" "serverAuth"
  openssl x509 -req -in "$work/leaf.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" \
    -CAcreateserial -sha256 -days "$LEAF_DAYS" -extfile "$work/ext.cnf" \
    -out "$work/leaf.crt" 2>/dev/null
  apply_secret "$secret" "tls.crt=$work/leaf.crt" "tls.key=$work/leaf.p8"
  $replaced && read -r -a more <<<"$workloads" && restart+=("${more[@]}")
done

# ── Credential-manager authority ─────────────────────────────────────────────
if secret_key "$CREDMGR_SECRET" tls.crt "$work/credmgr.crt" \
  && secret_key "$CREDMGR_SECRET" tls.key "$work/credmgr.key" \
  && fresh "$work/credmgr.crt"; then
  :
else
  replaced=false
  k get secret "$CREDMGR_SECRET" >/dev/null 2>&1 && replaced=true
  log "issuing $CREDMGR_SECRET (self-signed end-entity)"
  openssl ecparam -name prime256v1 -genkey -noout -out "$work/credmgr.ec"
  openssl pkcs8 -topk8 -nocrypt -in "$work/credmgr.ec" -out "$work/credmgr.key"
  ext_file "space-credmgr-authority,space-credmgr-authority.$NAMESPACE,space-credmgr-authority.$NAMESPACE.svc,space-credmgr-authority.$NAMESPACE.svc.cluster.local" \
    "CA:FALSE" "digitalSignature" "serverAuth"
  openssl req -x509 -new -key "$work/credmgr.key" -sha256 -days "$CA_DAYS" \
    -subj "/CN=space-credmgr-authority" -extensions v3 \
    -config <(printf '[req]\ndistinguished_name=dn\n[dn]\n[v3]\n'; cat "$work/ext.cnf") \
    -out "$work/credmgr.crt" 2>/dev/null
  apply_secret "$CREDMGR_SECRET" "tls.crt=$work/credmgr.crt" "tls.key=$work/credmgr.key"
  $replaced && restart+=(deployment/space-credmgr-authority)
fi
existing_trust="$work/existing-trust.crt"
if ! secret_key "$CREDMGR_TRUST_SECRET" ca.crt "$existing_trust" || ! cmp -s "$existing_trust" "$work/credmgr.crt"; then
  apply_secret "$CREDMGR_TRUST_SECRET" "ca.crt=$work/credmgr.crt"
fi

# Callers load trust once per client (docs/internal-rpc-tls.md), so a new CA
# reaches them only through a restart; listeners got new leaves above.
if $ca_replaced; then
  log "the internal RPC CA changed; restarting every workload so callers load it"
  while read -r workload; do restart+=("$workload"); done \
    < <(k get deployments,daemonsets -o name)
fi

for workload in ${restart[@]+"${restart[@]}"}; do
  if k get "$workload" >/dev/null 2>&1; then
    log "restarting $workload to load its reissued identity"
    k rollout restart "$workload" >/dev/null
  fi
done
log "internal RPC TLS identities are in place in $KCTX/$NAMESPACE"
