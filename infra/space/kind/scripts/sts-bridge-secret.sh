#!/usr/bin/env bash
# sts-bridge-secret.sh — provision the Kind-only STS bridge's Secret
# (charts/space-kind-sts-bridge), once per cluster, and print a checksum of
# it for the charts whose pods read it.
#
#   space-kind-sts-bridge   vend-access-key-id      the vend identity: the key
#                           vend-secret-access-key  pair the credential-manager
#                                                   authority signs AssumeRole
#                                                   with, and only the bridge
#                                                   verifies (no store user)
#                           identities              the same, as the bridge's
#                                                   BRIDGE_IDENTITIES entry
#                           signing.key             the token issuer's RSA key
#                                                   (PKCS#8)
#                           issuer.crt, issuer.key  the issuer's TLS leaf for
#                                                   sts-bridge, chained to
#                           issuer-ca.crt           the issuer CA, whose SHA-1
#                           issuer-ca.sha1          thumbprint versitygw's OIDC
#                                                   provider pins
#
# Idempotent: present, valid material is kept, so the vend identity, the
# signing key and the registered thumbprint stay stable across upgrades. The
# issuer CA lives ten years and its leaf one, reissued within 60 days of
# expiry. A reissue changes the printed checksum, which the charts put on the
# bridge and the authority, which roll, and on the object store's bucket-init
# Job, which reruns and moves the provider's thumbprint to the new CA. The IAM
# service reads nothing from the Secret and needs no restart: versitygw keys
# its cached JWKS on the provider's thumbprints and refetches it for a token
# whose kid it does not know (VerifyWebIdentitySignature, cachedJWKS), and
# the bridge derives the kid from the signing key.
#
# The bridge's STS listener has its own leaf from the internal RPC CA
# (space-kind-sts-bridge-tls, internal-rpc-tls.sh), which the authority trusts.
#
# Usage:
#   bash infra/space/kind/scripts/sts-bridge-secret.sh      # prints the checksum
#
# Env knobs:
#   SPACE_KIND_CLUSTER   kind cluster name (default: space-local)
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
KCTX="kind-$KIND_CLUSTER"
NAMESPACE=space
SECRET=space-kind-sts-bridge
# The OIDC subject the bridge mints for the vend identity, which the vend
# role's trust policy names (charts/space-object-store/values.yaml).
SUBJECT=kind-storage-vend
CA_DAYS=3650
LEAF_DAYS=365
RENEW_WITHIN_SECS=$((60 * 24 * 3600))
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"

log() { printf '[sts-bridge-secret] %s\n' "$*" >&2; }
k() { kubectl --context "$KCTX" -n "$NAMESPACE" "$@"; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
umask 077

secret_key() { # key file: writes key $1 of the Secret to $2; fails if absent
  local data
  data="$(k get secret "$SECRET" -o "jsonpath={.data.${1//./\\.}}" 2>/dev/null)" || return 1
  [[ -n "$data" ]] || return 1
  base64 -d <<<"$data" >"$2"
}
fresh() { openssl x509 -in "$1" -noout -checkend "$RENEW_WITHIN_SECS" >/dev/null 2>&1; }
# Random printable material. No "space" can appear in it: Tilt redacts
# Secret values from command lines (see space-object-store's values.yaml).
random_hex() { openssl rand -hex "$1"; }

kubectl --context "$KCTX" apply -f "$REPO_ROOT/infra/space/kind/manifests/namespace.yaml" >/dev/null

changed=false
if secret_key vend-access-key-id "$work/vend-access-key-id" \
  && secret_key vend-secret-access-key "$work/vend-secret-access-key"; then
  :
else
  log "issuing the vend identity"
  printf 'KINDVEND%s' "$(random_hex 6 | tr a-f A-F)" >"$work/vend-access-key-id"
  random_hex 20 | tr -d '\n' >"$work/vend-secret-access-key"
  changed=true
fi
printf '%s:%s:%s' "$(<"$work/vend-access-key-id")" "$(<"$work/vend-secret-access-key")" "$SUBJECT" \
  >"$work/identities"

if ! secret_key signing.key "$work/signing.key"; then
  log "issuing the token signing key"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/signing.key" 2>/dev/null
  changed=true
fi

if secret_key issuer-ca.crt "$work/issuer-ca.crt" && secret_key issuer-ca.key "$work/issuer-ca.key" \
  && fresh "$work/issuer-ca.crt"; then
  :
else
  log "issuing the issuer CA"
  openssl ecparam -name prime256v1 -genkey -noout -out "$work/issuer-ca.key"
  openssl req -x509 -new -key "$work/issuer-ca.key" -sha256 -days "$CA_DAYS" \
    -subj "/CN=$KIND_CLUSTER STS bridge issuer CA" -extensions v3 \
    -config <(printf '[req]\ndistinguished_name=dn\n[dn]\n[v3]\nbasicConstraints = critical, CA:TRUE\nkeyUsage = critical, keyCertSign, cRLSign\nsubjectKeyIdentifier = hash\n') \
    -out "$work/issuer-ca.crt" 2>/dev/null
  changed=true
  rm -f "$work/issuer.crt"
fi

if secret_key issuer.crt "$work/issuer.crt" && secret_key issuer.key "$work/issuer.key" \
  && fresh "$work/issuer.crt" \
  && openssl verify -CAfile "$work/issuer-ca.crt" "$work/issuer.crt" >/dev/null 2>&1; then
  :
else
  log "issuing the issuer's TLS leaf (sts-bridge)"
  openssl ecparam -name prime256v1 -genkey -noout -out "$work/issuer.ec"
  openssl pkcs8 -topk8 -nocrypt -in "$work/issuer.ec" -out "$work/issuer.key"
  openssl req -new -key "$work/issuer.key" -subj "/CN=sts-bridge" -out "$work/issuer.csr"
  printf 'basicConstraints = critical, CA:FALSE\nkeyUsage = critical, digitalSignature\nextendedKeyUsage = serverAuth\nsubjectAltName = DNS:sts-bridge\n' \
    >"$work/issuer.ext"
  openssl x509 -req -in "$work/issuer.csr" -CA "$work/issuer-ca.crt" -CAkey "$work/issuer-ca.key" \
    -CAcreateserial -sha256 -days "$LEAF_DAYS" -extfile "$work/issuer.ext" -out "$work/issuer.leaf" 2>/dev/null
  # versitygw pins the top certificate of the chain the issuer presents.
  cat "$work/issuer.leaf" "$work/issuer-ca.crt" >"$work/issuer.crt"
  changed=true
fi
openssl x509 -in "$work/issuer-ca.crt" -noout -fingerprint -sha1 \
  | cut -d= -f2 | tr -d : | tr 'A-F' 'a-f' | tr -d '\n' >"$work/issuer-ca.sha1"

if $changed || ! secret_key issuer-ca.sha1 "$work/stored.sha1" || ! cmp -s "$work/stored.sha1" "$work/issuer-ca.sha1" \
  || ! secret_key identities "$work/stored.identities" || ! cmp -s "$work/stored.identities" "$work/identities"; then
  args=()
  for key in vend-access-key-id vend-secret-access-key identities signing.key \
    issuer.crt issuer.key issuer-ca.crt issuer-ca.key issuer-ca.sha1; do
    args+=(--from-file="$key=$work/$key")
  done
  k create secret generic "$SECRET" "${args[@]}" --dry-run=client -o yaml \
    | kubectl --context "$KCTX" apply -f - >/dev/null
  log "wrote $SECRET in $KCTX/$NAMESPACE"
fi

k get secret "$SECRET" -o jsonpath='{.data}' | sha256sum | cut -c1-16
