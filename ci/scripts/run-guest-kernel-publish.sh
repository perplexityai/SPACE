#!/usr/bin/env bash
# Build, boot-check, and publish the independently versioned guest kernel.
# Every S3 key contains both the version label and binary digest. There is no
# mutable alias: selecting a fleet default remains a separate rollout action.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
KERNEL_DIR="${REPO_ROOT}/templates/firecracker/kernel"

KERNEL_VERSION="${KERNEL_VERSION:-6.1.155}"
BUILD_VERSION="vmlinux-${KERNEL_VERSION}${CONFIG_FRAGMENT:+-bpf-modsig}"
ARTIFACT_VERSION="${SPACE_GUEST_KERNEL_ARTIFACT_VERSION:-${BUILD_VERSION}}"
AWS_REGION="${AWS_REGION:-us-east-1}"
WORK_DIR=""
S3_ACCESS_KEY_ID=""
S3_SECRET_ACCESS_KEY=""
S3_SESSION_TOKEN=""

die() {
  echo "ERROR: $*" >&2
  exit 1
}

# Keep separate builds of one Linux release addressable side by side without
# permitting path traversal or publishing a mutable fleet-default alias.
if [[ ! "$ARTIFACT_VERSION" =~ ^[a-zA-Z0-9._+-]{1,128}$ ]] \
  || [[ "$ARTIFACT_VERSION" == "." || "$ARTIFACT_VERSION" == ".." ]] \
  || [[ "$ARTIFACT_VERSION" == "default" || "$ARTIFACT_VERSION" == "legacy-default" ]]; then
  die "invalid guest kernel artifact version: ${ARTIFACT_VERSION}"
fi

resolve_target_buckets() {
  local branch="${BUILDKITE_BRANCH:-$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)}"
  local pull_request="${BUILDKITE_PULL_REQUEST:-false}"
  branch="${branch#refs/heads/}"
  if [[ "$branch" != "main" || "$pull_request" != "false" ]]; then
    return
  fi

  [[ -n "${SPACE_GUEST_KERNEL_BUCKETS:-}" ]] \
    || die "SPACE_GUEST_KERNEL_BUCKETS must list the publication buckets (comma-separated)"
  printf '%s' "$SPACE_GUEST_KERNEL_BUCKETS"
}

wait_for_docker() {
  timeout 30 sh -c 'until docker info >/dev/null 2>&1; do sleep 1; done'
}

build_kernel() {
  local output_root="$1"
  echo "--- :docker: Waiting for the agent Docker daemon"
  wait_for_docker || die "Docker daemon is not available on this agent"
  OUTPUT_DIR="$output_root" KERNEL_VERSION="$KERNEL_VERSION" \
    "${KERNEL_DIR}/build.sh"
}

verify_artifacts() {
  local artifact_dir="$1"
  [[ -f "${artifact_dir}/vmlinux.bin" ]] || die "build produced no vmlinux.bin"
  [[ -f "${artifact_dir}/vmlinux.bin.sha256" ]] || die "build produced no checksum"
  [[ -f "${artifact_dir}/config" ]] || die "build produced no resolved config"
  (
    cd "$artifact_dir"
    sha256sum --check --strict vmlinux.bin.sha256
  )
}

write_manifest() {
  local artifact_dir="$1"
  local kernel_digest config_digest kernel_size
  kernel_digest="$(sha256sum "${artifact_dir}/vmlinux.bin" | awk '{print $1}')"
  config_digest="$(sha256sum "${artifact_dir}/config" | awk '{print $1}')"
  kernel_size="$(stat -c '%s' "${artifact_dir}/vmlinux.bin")"

  jq -n \
    --arg version "$ARTIFACT_VERSION" \
    --arg kernel_version "$KERNEL_VERSION" \
    --arg kernel_digest "sha256:${kernel_digest}" \
    --arg config_digest "sha256:${config_digest}" \
    --argjson kernel_size "$kernel_size" \
    '{
      schemaVersion: 1,
      architecture: "x86_64",
      version: $version,
      kernelVersion: $kernel_version,
      digest: $kernel_digest,
      files: {
        "vmlinux.bin": {digest: $kernel_digest, size: $kernel_size},
        "config": {digest: $config_digest}
      }
    }' >"${artifact_dir}/manifest.json"
}

compare_published_file() {
  local bucket="$1" key="$2" source_file="$3"
  local downloaded
  downloaded="$(mktemp)"
  if ! aws s3 cp "s3://${bucket}/${key}" "$downloaded" --region "$AWS_REGION" --only-show-errors; then
    rm -f "$downloaded"
    return 1
  fi
  if ! cmp -s "$source_file" "$downloaded"; then
    rm -f "$downloaded"
    die "immutable object s3://${bucket}/${key} already contains different bytes"
  fi
  rm -f "$downloaded"
}

load_s3_credentials() {
  local credentials
  credentials="$(aws configure export-credentials --format process)" \
    || die "failed to resolve AWS credentials for guest kernel publication"
  S3_ACCESS_KEY_ID="$(jq -er .AccessKeyId <<<"$credentials")" \
    || die "resolved AWS credentials contain no access key"
  S3_SECRET_ACCESS_KEY="$(jq -er .SecretAccessKey <<<"$credentials")" \
    || die "resolved AWS credentials contain no secret key"
  S3_SESSION_TOKEN="$(jq -er '.SessionToken // ""' <<<"$credentials")" \
    || die "failed to read the resolved AWS session token"
}

encode_s3_key() {
  jq -nr --arg key "$1" '$key | split("/") | map(@uri) | join("/")'
}

report_s3_error() {
  local response_file="$1"
  local code message
  code="$(sed -n 's#.*<Code>\([^<]*\)</Code>.*#\1#p' "$response_file" | head -n 1)"
  message="$(sed -n 's#.*<Message>\([^<]*\)</Message>.*#\1#p' "$response_file" | head -n 1)"
  if [[ -n "$code" || -n "$message" ]]; then
    echo "S3 error ${code:-Unknown}: ${message:-no message}" >&2
  fi
}

put_object_if_absent() {
  local bucket="$1" key="$2" source_file="$3" content_type="$4"
  local base_url="${5:-https://${bucket}.s3.${AWS_REGION}.amazonaws.com}"
  local attempt=1
  local max_attempts=3
  local checksum curl_config object_url payload_hash response status

  if [[ -z "$S3_ACCESS_KEY_ID" ]]; then
    load_s3_credentials
  fi

  payload_hash="$(sha256sum "$source_file" | awk '{print $1}')"
  checksum="$(openssl dgst -sha256 -binary "$source_file" | openssl base64 -A)"
  object_url="${base_url%/}/$(encode_s3_key "$key")"
  curl_config="$(mktemp)"
  response="$(mktemp)"
  chmod 0600 "$curl_config"
  {
    printf 'aws-sigv4 = "aws:amz:%s:s3"\n' "$AWS_REGION"
    printf 'user = "%s:%s"\n' "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY"
    if [[ -n "$S3_SESSION_TOKEN" ]]; then
      printf 'header = "x-amz-security-token: %s"\n' "$S3_SESSION_TOKEN"
    fi
  } >"$curl_config"

  while (( attempt <= max_attempts )); do
    if ! status="$(curl \
      --config "$curl_config" \
      --silent \
      --show-error \
      --output "$response" \
      --write-out '%{http_code}' \
      --request PUT \
      --upload-file "$source_file" \
      --header 'If-None-Match: *' \
      --header "Content-Type: ${content_type}" \
      --header "x-amz-content-sha256: ${payload_hash}" \
      --header "x-amz-checksum-sha256: ${checksum}" \
      "$object_url")"; then
      report_s3_error "$response"
      rm -f "$curl_config" "$response"
      return 1
    fi

    case "$status" in
      200)
        rm -f "$curl_config" "$response"
        return 0
        ;;
      412)
        rm -f "$curl_config" "$response"
        return 3
        ;;
      409)
        # S3 requires retrying conflicts caused by concurrent conditional writes.
        if grep -Fq '<Code>ConditionalRequestConflict</Code>' "$response" \
          && (( attempt < max_attempts )); then
          echo "S3 conditional PUT conflicted; retrying (${attempt}/${max_attempts})" >&2
          sleep "$attempt"
          attempt="$((attempt + 1))"
          continue
        fi
        ;;
    esac

    echo "S3 conditional PUT failed with HTTP ${status}" >&2
    report_s3_error "$response"
    rm -f "$curl_config" "$response"
    return 1
  done
}

publish_immutable_file() {
  local bucket="$1" key="$2" source_file="$3" content_type="$4"
  local status=0
  put_object_if_absent "$bucket" "$key" "$source_file" "$content_type" \
    || status=$?
  case "$status" in
    0)
      echo "Published s3://${bucket}/${key}"
      ;;
    3)
      compare_published_file "$bucket" "$key" "$source_file" \
        || die "failed to verify existing s3://${bucket}/${key}"
      echo "Unchanged s3://${bucket}/${key}"
      ;;
    *)
      die "failed to publish s3://${bucket}/${key}"
      ;;
  esac
}

publish_to_bucket() {
  local bucket="$1" artifact_dir="$2" digest="$3"
  local prefix="guest-kernels/${ARTIFACT_VERSION}/sha256:${digest}"

  publish_immutable_file "$bucket" "${prefix}/vmlinux.bin" \
    "${artifact_dir}/vmlinux.bin" application/octet-stream
  publish_immutable_file "$bucket" "${prefix}/vmlinux.bin.sha256" \
    "${artifact_dir}/vmlinux.bin.sha256" text/plain
  publish_immutable_file "$bucket" "${prefix}/config" \
    "${artifact_dir}/config" text/plain
  # Manifest last: its presence means the bundle is complete.
  publish_immutable_file "$bucket" "${prefix}/manifest.json" \
    "${artifact_dir}/manifest.json" application/json
}

main() {
  local artifact_dir digest buckets_csv
  buckets_csv="$(resolve_target_buckets)"
  WORK_DIR="$(mktemp -d)"
  trap 'rm -rf "$WORK_DIR"' EXIT
  artifact_dir="${WORK_DIR}/${BUILD_VERSION}"

  echo "--- :linux: Building ${ARTIFACT_VERSION}"
  build_kernel "$WORK_DIR"
  verify_artifacts "$artifact_dir"
  write_manifest "$artifact_dir"

  echo "--- :fire: Smoke-booting ${ARTIFACT_VERSION}"
  "${KERNEL_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "$KERNEL_VERSION"

  unset AWS_PROFILE AWS_DEFAULT_PROFILE
  digest="$(sha256sum "${artifact_dir}/vmlinux.bin" | awk '{print $1}')"
  if [[ -z "$buckets_csv" ]]; then
    echo "Guest kernel artifact validated: ${ARTIFACT_VERSION}@sha256:${digest}"
    echo "S3 publication is skipped outside non-PR main builds"
    return
  fi
  echo "--- :s3: Publishing ${ARTIFACT_VERSION}@sha256:${digest} to ${buckets_csv}"

  local IFS=','
  read -r -a buckets <<<"$buckets_csv"
  unset IFS
  local bucket
  for bucket in "${buckets[@]}"; do
    [[ -n "$bucket" ]] || continue
    publish_to_bucket "$bucket" "$artifact_dir" "$digest"
  done

  echo "Guest kernel artifact: ${ARTIFACT_VERSION}@sha256:${digest}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
