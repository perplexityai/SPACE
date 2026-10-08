#!/usr/bin/env bash
# Smoke-test the auto-pull path in template-manager's GetTemplate
# (cluster-service-driven discovery + S3 download).
#
# Synthesizes a fake template build, uploads it to S3, registers the
# release with `ClusterApi.RegisterTemplate(Release)`, then drives
# `space-tmplmgr-cli get` to exercise the auto-pull. On a local cache
# miss the manager:
#
#   * calls `ClusterApi.FindTemplateRelease(name, "fc", tag)`
#     via the cluster-role node-api endpoint;
#   * exchanges AWS credentials via the credmgr `ExchangeToken` RPC
#     (which today resolves AWS_* env vars or the SDK default chain
#     server-side, pending real per-resource issuance);
#   * downloads metadata.json + manifest.json + rootfs.ext4 from the
#     storage URI returned by the cluster service;
#   * verifies the rootfs SHA-256 against the resolved release hash;
#   * atomic-renames staging → digest dir + inserts a `ready` build row.
#
# Prerequisites:
#   - template-manager running locally (Unix socket at
#     /space/run/template-manager.sock).
#   - cluster-role node-api running and reachable via NODE_API_ENDPOINT.
#   - A `template_releases`-bearing Postgres backing the cluster API.
#   - `space-tmplmgr-cli` and `grpcurl` on PATH.
#   - `aws` CLI for the upload step.
#
# Required env:
#   SPACE_OBJECT_STORE_BUCKET     target bucket
#   SPACE_OBJECT_STORE_REGION     bucket region; baked into the
#                                 connection_string registered with
#                                 the cluster service so template-mgr
#                                 can resolve the bucket without
#                                 object-store env vars on the worker.
#   NODE_API_ENDPOINT              ClusterApi TCP address or https URL
#   SPACE_INTERNAL_RPC_TLS_CA      cluster CA PEM file
#   SPACE_CLUSTER_API_TLS_SERVER_NAME expected certificate identity (optional)
#   SPACE_INTERNAL_RPC_INSECURE_DEV true for an explicitly plaintext local fixture
#   SPACE_INTERNAL_TOKEN          bearer for cluster API calls
#   AWS_ACCESS_KEY_ID             creds for both upload and pull
#   AWS_SECRET_ACCESS_KEY
#   AWS_SESSION_TOKEN             when using STS
#
# Optional env:
#   SPACE_OBJECT_STORE_ENDPOINT   custom S3-compatible endpoint;
#                                 baked into the connection_string
#                                 when set.
#   TEMPLATE                      template name; defaults to smoke-<ts>
#   ROOTFS_SIZE_MB                rootfs payload size; defaults to 16
#   ARCH                          manifest arch; defaults to aarch64
#   TMPLMGR_CLI                   path to space-tmplmgr-cli; default
#                                 `space-tmplmgr-cli` on PATH
#   KEEP_ARTIFACTS                "1" to skip S3 cleanup on exit

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

: "${SPACE_OBJECT_STORE_BUCKET:?SPACE_OBJECT_STORE_BUCKET is required}"
: "${SPACE_OBJECT_STORE_REGION:?SPACE_OBJECT_STORE_REGION is required}"
: "${NODE_API_ENDPOINT:?NODE_API_ENDPOINT is required}"
: "${SPACE_INTERNAL_TOKEN:?SPACE_INTERNAL_TOKEN is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

source "$REPO_ROOT/scripts/lib/space-cluster-rpc.sh"
space_cluster_rpc_configure

TEMPLATE="${TEMPLATE:-smoke-$(date +%s)}"
ROOTFS_SIZE_MB="${ROOTFS_SIZE_MB:-16}"
ARCH="${ARCH:-aarch64}"
TMPLMGR_CLI="${TMPLMGR_CLI:-space-tmplmgr-cli}"
KEY_PREFIX_PATH=""

TMP="$(mktemp -d)"
cleanup_local() { rm -rf "$TMP"; }
trap cleanup_local EXIT

echo "==> synthesizing fake template build in $TMP"
dd if=/dev/urandom of="$TMP/rootfs.ext4" bs=1m count="$ROOTFS_SIZE_MB" status=none
ROOTFS_HASH="$(shasum -a 256 "$TMP/rootfs.ext4" | awk '{print $1}')"
CONTENT_HASH="sha256:$ROOTFS_HASH"
ROOTFS_SIZE="$(wc -c < "$TMP/rootfs.ext4" | tr -d ' ')"
BUILT_AT="$(date -u +%FT%TZ)"

cat > "$TMP/metadata.json" <<EOF
{
  "id": "$TEMPLATE",
  "name": "$TEMPLATE",
  "base": "seed",
  "built_at": "$BUILT_AT",
  "recipe": "smoke-test",
  "steps_count": 0,
  "vcpu": 2,
  "memory_mib": 512
}
EOF

# v2 manifest carrying execution metadata: registration is the canonical
# writer of a release's execution metadata (the node publish path does not
# backfill it), and space-upload-template.sh refuses manifests whose schema
# doesn't vouch for the execution payload.
cat > "$TMP/manifest.json" <<EOF
{
  "schema_version": 2,
  "name": "$TEMPLATE",
  "content_hash": "$CONTENT_HASH",
  "build_date": "$BUILT_AT",
  "runtime": "fc",
  "arch": "$ARCH",
  "config": {},
  "bootable_artifact": "rootfs.ext4",
  "execution": {
    "runtime": { "kind": "firecracker", "version": "1.15.1" },
    "guest": { "os": "linux", "architecture": "$ARCH" },
    "state_format": { "format": "firecracker-snapshot", "version": "9.0.0" },
    "components": []
  },
  "files": {
    "rootfs.ext4": { "digest": "$CONTENT_HASH", "size": $ROOTFS_SIZE }
  }
}
EOF

S3_BASE="s3://${SPACE_OBJECT_STORE_BUCKET}/${KEY_PREFIX_PATH}templates/${TEMPLATE}/${CONTENT_HASH}"

# Self-describing connection_string for the cluster service. Worker-
# side template-manager parses bucket / region / (optional) endpoint
# straight from this URI — no object-store env vars on the worker.
CONN_STRING_QUERY="region=${SPACE_OBJECT_STORE_REGION}"
if [[ -n "${SPACE_OBJECT_STORE_ENDPOINT:-}" ]]; then
  CONN_STRING_QUERY="${CONN_STRING_QUERY}&endpoint=${SPACE_OBJECT_STORE_ENDPOINT}"
fi
CONN_STRING="${S3_BASE}?${CONN_STRING_QUERY}"

echo "    template     = $TEMPLATE"
echo "    content_hash = $CONTENT_HASH"
echo "    s3 prefix    = $S3_BASE"
echo "    conn string  = $CONN_STRING"

echo "==> uploading artifacts via scripts/space-upload-template.sh"
"$REPO_ROOT/scripts/space-upload-template.sh" "$TMP" "$TEMPLATE" >/dev/null

cleanup_s3() {
  if [[ "${KEEP_ARTIFACTS:-}" == "1" ]]; then
    echo "==> KEEP_ARTIFACTS=1: leaving $S3_BASE in place"
  else
    echo "==> cleaning up $S3_BASE (set KEEP_ARTIFACTS=1 to skip)"
    aws s3 rm --quiet --recursive "$S3_BASE" || true
  fi
}
trap 'cleanup_local; cleanup_s3' EXIT

echo "==> registering template + release with cluster service"
# RegisterTemplate may already exist for this name — ignore ALREADY_EXISTS.
space_cluster_rpc space.node.api.v1.ClusterApi/RegisterTemplate \
  -H "authorization: Bearer $SPACE_INTERNAL_TOKEN" \
  -d "{\"name\":\"$TEMPLATE\"}" 2>&1 | tail -n5 || true

# Register from the request file space-upload-template.sh generated — it
# carries the manifest's execution metadata, which registration (not node
# publication) is responsible for writing. Add the latest tag the smoke
# resolves by.
REQUEST_FILE="$TMP/register-release.json"
python3 - "$REQUEST_FILE" <<'PY'
import json
import sys

path = sys.argv[1]
request = json.load(open(path))
request["tags"] = ["latest", request["hash"]]
json.dump(request, open(path, "w"), indent=2)
PY
space_cluster_rpc space.node.api.v1.ClusterApi/RegisterTemplateRelease \
  -H "authorization: Bearer $SPACE_INTERNAL_TOKEN" \
  -d @ < "$REQUEST_FILE" >/dev/null

echo "==> driving GetTemplate via $TMPLMGR_CLI (auto-pull)"
echo
"$TMPLMGR_CLI" get --template "$TEMPLATE" --content-hash "$CONTENT_HASH"
echo
echo "==> smoke passed"
