#!/usr/bin/env bash
# seed-publish-entry.sh — entrypoint of the space-seed-publish Job.
#
# Publishes the baked seed template build the way the production builder
# publishes template releases:
#
#   1. uploads metadata.json + manifest.json + rootfs.ext4 to the cluster
#      object store under templates/<name>/<content_hash>/ (real S3 protocol,
#      sigv4-signed — versitygw locally, S3 in prod),
#   2. registers the template + release + tag + storage location through the
#      real ClusterApi RPCs (RegisterTemplate / RegisterTemplateRelease) on
#      space-internal-api.
#
# Worker template-managers then resolve `seed:latest` via
# ClusterApi.FindTemplateRelease and pull the artifacts through volume-manager
# — the same path production workers use. Nothing here writes Postgres
# directly and nothing is copied onto workers out of band.
#
# Idempotent: RegisterTemplate tolerates AlreadyExists, RegisterTemplateRelease
# is an upsert keyed by (template_name, hash), and the S3 upload is skipped
# when the objects already exist with the right sizes.
#
# Required env:
#   SPACE_INTERNAL_API_GRPC_ADDR   host:port of space-internal-api (plaintext gRPC)
#   SPACE_CLUSTER_API_TOKEN_PATH   projected bearer token file for ClusterApi
#   SPACE_OBJECT_STORE_ENDPOINT    e.g. http://object-store:8333
#   SPACE_OBJECT_STORE_BUCKET      e.g. templates
#   SPACE_OBJECT_STORE_REGION     e.g. us-east-1
#   SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID / _SECRET_ACCESS_KEY
set -euo pipefail

SEED_DIR=/opt/space/seed
PROTO_DIR=/opt/space/proto
CLUSTER_PROTO=node/api/v1/cluster.proto

log() { printf '[seed-publish] %s\n' "$*"; }
die() { printf '[seed-publish] ERROR: %s\n' "$*" >&2; exit 1; }

require_env() {
  local name
  for name in "$@"; do
    [[ -n "${!name:-}" ]] || die "required env var not set: $name"
  done
}
require_env SPACE_INTERNAL_API_GRPC_ADDR SPACE_CLUSTER_API_TOKEN_PATH \
  SPACE_OBJECT_STORE_ENDPOINT SPACE_OBJECT_STORE_BUCKET SPACE_OBJECT_STORE_REGION \
  SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY

export AWS_ACCESS_KEY_ID="$SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="$SPACE_OBJECT_STORE_REGION"

TEMPLATE_NAME=$(python3 -c 'import json;print(json.load(open("/opt/space/seed/manifest.json"))["name"])')
CONTENT_HASH=$(python3 -c 'import json;print(json.load(open("/opt/space/seed/manifest.json"))["content_hash"])')
[[ "$CONTENT_HASH" == sha256:* ]] || die "manifest content_hash is not a sha256 digest: $CONTENT_HASH"

# Registration is the canonical writer of a release's execution metadata; the
# node publish path does not backfill it, so a release registered without
# `execution` keeps NULL metadata forever. The converter is the shared
# scripts/lib/manifest_execution_to_proto.py, staged into this image next to
# this entrypoint by docker/Dockerfile.seed-publish; it fails (and fails this
# publish) when the manifest's schema doesn't vouch for its execution payload.
EXECUTION_PROTO_JSON=$(python3 /opt/space/bin/manifest_execution_to_proto.py "$SEED_DIR/manifest.json") \
  || die "seed manifest execution metadata is unusable; refusing to register a metadata-less release"

S3_PREFIX="templates/${TEMPLATE_NAME}/${CONTENT_HASH}"
# Self-describing storage URI — carries everything volume-manager needs to
# issue the GET (see the storage-URI format doc in
# crates/libs/template/mgr/src/volume.rs).
STORAGE_URI="s3://${SPACE_OBJECT_STORE_BUCKET}/${S3_PREFIX}/?region=${SPACE_OBJECT_STORE_REGION}&endpoint=${SPACE_OBJECT_STORE_ENDPOINT}&path_style=true"

cluster_api() {
  local rpc=$1 body=$2 token
  token=$(<"$SPACE_CLUSTER_API_TOKEN_PATH")
  [[ -n "$token" ]] || die "projected ClusterApi token is empty"
  grpcurl -plaintext \
    -import-path "$PROTO_DIR" -proto "$CLUSTER_PROTO" \
    -H "authorization: Bearer ${token}" \
    -d "$body" \
    "$SPACE_INTERNAL_API_GRPC_ADDR" "space.node.api.v1.ClusterApi/${rpc}"
}

# ── Wait for the object store and the cluster API ────────────────────────────
log "waiting for object store at $SPACE_OBJECT_STORE_ENDPOINT (bucket $SPACE_OBJECT_STORE_BUCKET)"
for attempt in $(seq 1 90); do
  if aws --endpoint-url "$SPACE_OBJECT_STORE_ENDPOINT" \
       s3 ls "s3://${SPACE_OBJECT_STORE_BUCKET}" >/dev/null 2>&1; then
    break
  fi
  [[ "$attempt" == 90 ]] && die "object store bucket not reachable after 90 attempts"
  sleep 2
done

log "waiting for ClusterApi at $SPACE_INTERNAL_API_GRPC_ADDR"
for attempt in $(seq 1 90); do
  # Any well-formed gRPC response (including NotFound) proves the API is up;
  # only transport-level failures keep us waiting.
  err=$(cluster_api FindTemplateReleaseByHash \
    "{\"template_name\":\"${TEMPLATE_NAME}\",\"hash\":\"${CONTENT_HASH}\"}" 2>&1) && break
  if grep -qi "NotFound" <<<"$err"; then
    break
  fi
  [[ "$attempt" == 90 ]] && die "ClusterApi not reachable after 90 attempts: $err"
  sleep 2
done

# ── Upload artifacts (skip when already present with matching sizes) ─────────
upload_needed=0
for f in metadata.json manifest.json rootfs.ext4; do
  local_size=$(stat -c %s "$SEED_DIR/$f")
  remote_size=$(aws --endpoint-url "$SPACE_OBJECT_STORE_ENDPOINT" \
    s3api head-object --bucket "$SPACE_OBJECT_STORE_BUCKET" --key "$S3_PREFIX/$f" \
    --query ContentLength --output text 2>/dev/null || echo missing)
  if [[ "$remote_size" != "$local_size" ]]; then
    upload_needed=1
    break
  fi
done
if [[ "$upload_needed" == 1 ]]; then
  for f in metadata.json manifest.json rootfs.ext4; do
    log "uploading $f → s3://${SPACE_OBJECT_STORE_BUCKET}/${S3_PREFIX}/$f"
    # rootfs.ext4 goes multipart, and versitygw's CompleteMultipartUpload
    # copies the parts into the object before it answers: about 50 s for a
    # 1.5 GB rootfs, too close to the CLI's 60 s default read timeout.
    aws --endpoint-url "$SPACE_OBJECT_STORE_ENDPOINT" --cli-read-timeout 300 \
      s3 cp "$SEED_DIR/$f" "s3://${SPACE_OBJECT_STORE_BUCKET}/${S3_PREFIX}/$f" --no-progress
  done
else
  log "artifacts already present under s3://${SPACE_OBJECT_STORE_BUCKET}/${S3_PREFIX}/ — skipping upload"
fi

# ── Register template + release through the ClusterApi ───────────────────────
log "RegisterTemplate ${TEMPLATE_NAME}"
if ! out=$(cluster_api RegisterTemplate \
    "{\"name\":\"${TEMPLATE_NAME}\",\"description\":\"Minimal Ubuntu Firecracker seed (local build)\"}" 2>&1); then
  if grep -qi "AlreadyExists" <<<"$out"; then
    log "template ${TEMPLATE_NAME} already registered"
  else
    die "RegisterTemplate failed: $out"
  fi
fi

log "RegisterTemplateRelease ${TEMPLATE_NAME}@${CONTENT_HASH} (tag latest)"
cluster_api RegisterTemplateRelease "$(cat <<JSON
{
  "template_name": "${TEMPLATE_NAME}",
  "runtime": "fc",
  "hash": "${CONTENT_HASH}",
  "tags": ["latest"],
  "storage_connection_strings": ["${STORAGE_URI}"],
  "execution": ${EXECUTION_PROTO_JSON}
}
JSON
)"

log "published ${TEMPLATE_NAME}@${CONTENT_HASH} → ${STORAGE_URI}"
