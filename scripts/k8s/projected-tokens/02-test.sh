#!/usr/bin/env bash
# Prove the projected-token (mode:k8s) path end-to-end against node-api:
#   1. read space-api's actual projected JWT,
#   2. port-forward node-api,
#   3. POSITIVE: call WorkerApi/ListSandboxes + CreateSandbox with that JWT and
#      show node-api accept it via TokenReview (verified SA + audience),
#   4. NEGATIVE: call with a bogus JWT and show node-api reject it,
#   5. INTERNAL surface: mint a space-cluster-controller token and show
#      WorkerApiInternal accepts it (nodeApiInternal allowlist), while a SA that
#      is not on that allowlist is denied (the cluster-controller warm-pool edge).
#
# Requires `grpcurl` (brew install grpcurl) and ./01-apply-auth.sh already
# applied.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

require kubectl
require grpcurl
assert_local_context

POD="$(node_api_pod)"; [[ -n "$POD" ]] || die "node-api pod not found"
APIPOD="$(k get pod -l app.kubernetes.io/name="${SPACE_API_DEPLOY}" -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$APIPOD" ]] || die "space-api pod not found"

TOK="$(mktemp)"; CA="$(mktemp)"
trap 'rm -f "$TOK" "$CA"; [[ -n "${PF_PID:-}" ]] && kill "$PF_PID" 2>/dev/null || true' EXIT
# node-api serves authenticated TLS with the cluster's shared space-node-api
# identity (infra/space/kind/scripts/internal-rpc-tls.sh); verify it against
# the cluster CA under that name, since the dial address is a port-forward.
k get secret space-internal-rpc-ca -o jsonpath='{.data.ca\.crt}' | base64 -d >"$CA"
[[ -s "$CA" ]] || die "no space-internal-rpc-ca Secret (run infra/space/kind/scripts/internal-rpc-tls.sh)"
log "reading space-api projected token"
k exec "$APIPOD" -c "${SPACE_API_CONTAINER}" -- cat "${TOKEN_PATH}" > "$TOK" 2>/dev/null
[[ -s "$TOK" ]] || die "no projected token at ${TOKEN_PATH} on ${APIPOD} (run ./01-apply-auth.sh first)"

log "port-forward node-api ${LOCAL_NODE_API_PORT}->${NODE_API_PORT}"
kubectl --context "$KCTX" -n "$NS" port-forward "$POD" "${LOCAL_NODE_API_PORT}:${NODE_API_PORT}" >/dev/null 2>&1 &
PF_PID=$!
for _ in $(seq 1 15); do nc -z localhost "${LOCAL_NODE_API_PORT}" 2>/dev/null && break; sleep 1; done

GRPC=(grpcurl -cacert "$CA" -servername space-node-api -import-path "${REPO_ROOT}/proto" -proto node/api/v1/worker.proto)
ADDR="localhost:${LOCAL_NODE_API_PORT}"
WAPI="space.node.api.v1.WorkerApi"

pass=0; fail=0
ok()   { printf '\033[1;32m  PASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }

echo
log "POSITIVE 1/2: WorkerApi/ListSandboxes with space-api's projected JWT"
out="$("${GRPC[@]}" -H "authorization: Bearer $(cat "$TOK")" -d '{}' "$ADDR" "${WAPI}/ListSandboxes" 2>&1 || true)"
echo "$out" | sed 's/^/    /'
echo "$out" | grep -q "Unauthenticated\|PermissionDenied" \
  && bad "ListSandboxes was rejected (projected token not accepted)" \
  || ok "ListSandboxes accepted (TokenReview validated the projected JWT)"

echo
log "POSITIVE 2/2: WorkerApi/CreateSandbox with the projected JWT (auth runs before the handler)"
out="$("${GRPC[@]}" -H "authorization: Bearer $(cat "$TOK")" \
  -d '{"template":"seed:latest","runtime":"fc","owner":"root@local","sandbox_id":"sbx_pt_poc"}' \
  "$ADDR" "${WAPI}/CreateSandbox" 2>&1 || true)"
echo "$out" | sed 's/^/    /' | head -4
# Auth passed if we got past it: a clean response, or a handler error that is
# NOT an auth/permission rejection of the bearer.
if echo "$out" | grep -q "Unauthenticated"; then
  bad "CreateSandbox rejected at auth"
else
  ok "CreateSandbox authenticated (handler ran past auth; any error here is post-auth)"
fi

echo
log "NEGATIVE: WorkerApi/ListSandboxes with a BOGUS JWT (expect Unauthenticated)"
BOGUS="eyJhbGciOiJSUzI1NiIsImtpZCI6ImJvZ3VzIn0.eyJzdWIiOiJmYWtlIiwiYXVkIjpbInNwYWNlLW5vZGUtYXBpIl19.ZmFrZXNpZ25hdHVyZQ"
out="$("${GRPC[@]}" -H "authorization: Bearer ${BOGUS}" -d '{}' "$ADDR" "${WAPI}/ListSandboxes" 2>&1 || true)"
echo "$out" | sed 's/^/    /' | head -4
echo "$out" | grep -q "Unauthenticated" \
  && ok "bogus token rejected (TokenReview rejected the JWT)" \
  || bad "bogus token was NOT rejected"

# ── Internal surface (WorkerApiInternal): client-identity allowlist ──
# Mirrors the cluster-controller -> node-api/WorkerApiInternal edge (warm-pool
# WakeSandbox). node-api's nodeApiInternal allowlist (deployed by 01-apply-auth.sh
# from NODE_API_INTERNAL_ALLOWED_SAS) must accept INTERNAL_ALLOWED_SA and reject
# any SA not on it. Tokens are minted per-SA directly via the TokenRequest API.
WIAPI="space.node.api.v1.WorkerApiInternal"
mint_internal_token() {  # $1=ServiceAccount -> projected JWT for the internal audience
  k get sa "$1" >/dev/null 2>&1 || k create sa "$1" >/dev/null 2>&1 || true
  k create token "$1" --audience="${NODE_API_INTERNAL_AUDIENCE}" 2>/dev/null
}

echo
log "POSITIVE 3/3: WorkerApiInternal/WakeSandbox as ${INTERNAL_ALLOWED_SA} (on the nodeApiInternal allowlist)"
tok="$(mint_internal_token "${INTERNAL_ALLOWED_SA}")"
if [[ -z "$tok" ]]; then
  bad "could not mint an internal-audience token for ${INTERNAL_ALLOWED_SA} (kubectl create token failed)"
else
  out="$("${GRPC[@]}" -H "authorization: Bearer ${tok}" -d '{}' "$ADDR" "${WIAPI}/WakeSandbox" 2>&1 || true)"
  echo "$out" | sed 's/^/    /' | head -4
  if echo "$out" | grep -qE "Unauthenticated|PermissionDenied"; then
    bad "${INTERNAL_ALLOWED_SA} rejected on the internal surface (is it on the nodeApiInternal allowlist?)"
  else
    ok "${INTERNAL_ALLOWED_SA} authenticated on nodeApiInternal (handler ran past auth)"
  fi
fi

echo
log "NEGATIVE 2/2: WorkerApiInternal/WakeSandbox as ${INTERNAL_DENIED_SA} (NOT on the allowlist)"
tok="$(mint_internal_token "${INTERNAL_DENIED_SA}")"
if [[ -z "$tok" ]]; then
  bad "could not mint an internal-audience token for ${INTERNAL_DENIED_SA} (kubectl create token failed)"
else
  out="$("${GRPC[@]}" -H "authorization: Bearer ${tok}" -d '{}' "$ADDR" "${WIAPI}/WakeSandbox" 2>&1 || true)"
  echo "$out" | sed 's/^/    /' | head -4
  # Require PermissionDenied specifically: the token is valid (TokenReview +
  # audience OK) but the SA is not on the allowlist — that is the allowlist doing
  # its job. Unauthenticated means a pre-allowlist failure (bad/empty token, wrong
  # audience, TokenReview rejection) and must NOT count as "enforced".
  if echo "$out" | grep -q "PermissionDenied"; then
    ok "${INTERNAL_DENIED_SA} denied with PermissionDenied (allowlist enforced)"
  elif echo "$out" | grep -q "Unauthenticated"; then
    bad "${INTERNAL_DENIED_SA} got Unauthenticated — token/audience failed before the allowlist (inconclusive)"
  else
    bad "${INTERNAL_DENIED_SA} was NOT denied on the internal surface"
  fi
fi

echo
# Supplementary: the human-readable acceptance log, if a fresh (uncached)
# TokenReview happened during this run. Verdicts cache (~k8sAuth.cacheTtlSeconds),
# so a cache hit legitimately leaves no new line — the gRPC results above are the
# authoritative signal.
acc="$(k logs "$POD" --since=10m 2>/dev/null | grep '"accepted Kubernetes service account"' | tail -1 || true)"
if [[ -n "$acc" ]]; then
  log "node-api also logged (most recent acceptance):"
  echo "$acc" | python3 -c "import sys,json
d=json.load(sys.stdin); print('  ACCEPT  sa=%s  aud=%s  surface=%s' % (d.get('service_account'), d.get('audience'), d.get('surface')))" 2>/dev/null || true
else
  log "node-api: positive verdicts served from the TokenReview cache (no fresh log line; expected)."
fi

echo
[[ "$fail" -eq 0 ]] && log "RESULT: all ${pass} checks passed — mode:k8s works end-to-end." \
                    || die "RESULT: ${fail} check(s) failed."
