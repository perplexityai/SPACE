#!/usr/bin/env bash
# SPIKE: one suspended sandbox whose snapshot object Q4's exchanges target.
# Prints the SUBJ and RES q4-matrix.sh reads: eval "$(q4-fixture.sh)".
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
API_URL="${API_URL:-http://localhost:8080}"
source "$REPO/scripts/tests/lib/space-api-ops.sh"
TOKEN=$(api_curl -X POST "$API_URL/auth/v1/sessions" -H 'content-type: application/json' \
  -d '{"email":"root@local","password":"password"}' | jq -r .token)
auth=(-H "authorization: Bearer $TOKEN")
id=$(post_operation /v1/sandboxes '{"template":"seed:latest","runtime":"fc","env":{},"metadata":{},"resources":null}' | jq -r .sandbox_id)
post_operation "/v1/sandboxes/$id:start" '{}' >/dev/null
sleep 5
post_operation "/v1/sandboxes/$id:suspend" '{}' >/dev/null
uuid="${id#sbx_}"
secret() { kubectl --context kind-space-local -n space get secret space-object-store -o "jsonpath={.data.$1}" | base64 -d; }
key=$(AWS_ACCESS_KEY_ID="$(secret SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID)" \
  AWS_SECRET_ACCESS_KEY="$(secret SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY)" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:8333 s3api list-objects-v2 --bucket snapshots --prefix suspend_snapshots/ \
  --query "Contents[?contains(Key, '$uuid') && Size > \`0\`] | sort_by(@, &Size)[0].Key" --output text)
echo "export SUBJ=sandbox:$uuid RES=arn:aws:s3:::snapshots/$key"
