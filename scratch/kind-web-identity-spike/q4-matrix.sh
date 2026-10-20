#!/usr/bin/env bash
# SPIKE Q4: with web-identity base credentials (spike.sh apply), take out each
# base-identity source and record what the authority answers an exchange,
# for an authority built from main and from main + #642.
# Usage: q4-matrix.sh <variant>...   (variant = image tag suffix: main, pr642)
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; REPO="$(cd "$DIR/../.." && pwd)"
k() { kubectl --context kind-space-local -n space "$@"; }
# SUBJ, RES: the sandbox and object q4-fixture.sh prints.
: "${SUBJ:?eval \"$(q4-fixture.sh)\" first}" "${RES:?}"
NODE=space-local-worker
exchange() { (cd "$REPO" && PYTHONPATH="$REPO" uv run --frozen --python 3.11 python "$DIR/q4_exchange.py" "$NODE" "$SUBJ" "$RES"); }
ready() { k rollout status "deployment/$1" --timeout=180s >/dev/null; }
scale() { k scale "deployment/$1" --replicas="$2" >/dev/null; if [[ $2 == 0 ]]; then k wait --for=delete pod -l "app.kubernetes.io/name=$1" --timeout=120s >/dev/null 2>&1 || true; else ready "$1"; sleep 5; fi; }
row() { local t0=$SECONDS out; out="$(exchange)"; printf '| %s | %s | %s | %ss |\n' "$variant" "$1" "$out" "$((SECONDS - t0))"; }

for variant in "$@"; do
  k set image deployment/space-credmgr-authority "credmgr-authority=spike-credmgr-authority:$variant" >/dev/null
  ready space-credmgr-authority
  for p in $(k get pods -l app.kubernetes.io/name=space-credmgr-authority -o name); do
    echo "# $variant $p binary sha256: $(k exec "${p#pod/}" -- sha256sum /opt/space/bin/space-credmgr-svc | cut -c1-16)"
  done
  row "healthy"

  scale space-kind-sts-bridge 0; row "STS bridge down (web-identity endpoint refused)"; scale space-kind-sts-bridge 1

  scale space-object-store-iam 0; row "IAM service down (bridge answers 503)"; scale space-object-store-iam 1
  row "healthy after IAM restart"

  # Issuer outage: the apiserver refuses anonymous discovery; the IAM restart
  # drops versitygw's five-minute JWKS cache.
  kubectl --context kind-space-local delete clusterrolebinding space-kind-oidc-discovery-anonymous >/dev/null
  scale space-object-store-iam 0; scale space-object-store-iam 1
  row "issuer refuses discovery/JWKS (403)"
  kubectl --context kind-space-local apply -f "$DIR/manifests/oidc-discovery-rbac.yaml" >/dev/null
  row "healthy after issuer restored"
done
