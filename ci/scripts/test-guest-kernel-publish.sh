#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
publish_script="${script_dir}/run-guest-kernel-publish.sh"
tmp_dir="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

targets_for() {
  # SC2016: the nested shell expands $1.
  # shellcheck disable=SC2016
  env -u SPACE_GUEST_KERNEL_BUCKETS \
    BUILDKITE_BRANCH="$1" BUILDKITE_PULL_REQUEST="$2" \
    bash -c 'source "$1"; resolve_target_buckets' _ "$publish_script"
}

[[ -z "$(targets_for feature/kernel false)" ]]
[[ -z "$(targets_for main 4244)" ]]
if targets_for main false >"${tmp_dir}/targets" 2>"${tmp_dir}/error"; then
  echo "main publication accepted missing bucket configuration" >&2
  exit 1
fi
[[ ! -s "${tmp_dir}/targets" ]]
grep -Fq SPACE_GUEST_KERNEL_BUCKETS "${tmp_dir}/error"

configured_targets_for() {
  SPACE_GUEST_KERNEL_BUCKETS=one,two \
    BUILDKITE_BRANCH="$1" BUILDKITE_PULL_REQUEST="$2" \
    bash -c 'source "$1"; resolve_target_buckets' _ "$publish_script"
}

[[ -z "$(configured_targets_for feature/kernel false)" ]]
[[ -z "$(configured_targets_for main 4244)" ]]
[[ "$(configured_targets_for main false)" == "one,two" ]]
[[ "$(configured_targets_for refs/heads/main false)" == "one,two" ]]

artifact_dir="${tmp_dir}/artifact"
mkdir -p "$artifact_dir"
printf 'kernel bytes\n' >"${artifact_dir}/vmlinux.bin"
printf 'CONFIG_TEST=y\n' >"${artifact_dir}/config"
(
  cd "$artifact_dir"
  sha256sum vmlinux.bin >vmlinux.bin.sha256
)

# shellcheck source=run-guest-kernel-publish.sh disable=SC1091
source "$publish_script"
verify_artifacts "$artifact_dir"
write_manifest "$artifact_dir"

digest="$(sha256sum "${artifact_dir}/vmlinux.bin" | awk '{print $1}')"
[[ "$(jq -r .digest "${artifact_dir}/manifest.json")" == "sha256:${digest}" ]]
[[ "$(jq -r .version "${artifact_dir}/manifest.json")" == "$ARTIFACT_VERSION" ]]

# Exercise the real CI-image tools used by the conditional PUT path. In
# particular, this must stay compatible with the Debian Bookworm AWS CLI.
exported_credentials="$(
  AWS_ACCESS_KEY_ID=test-access-key \
    AWS_SECRET_ACCESS_KEY=test-secret-key \
    AWS_SESSION_TOKEN=test-session-token \
    aws configure export-credentials --format process
)"
[[ "$(jq -r .AccessKeyId <<<"$exported_credentials")" == test-access-key ]]
curl --help all | grep -Fq -- '--aws-sigv4'

cat >"${tmp_dir}/conditional-put-server.py" <<'PY'
import base64
import hashlib
import http.server
import pathlib
import sys

port_file = pathlib.Path(sys.argv[1])
result_file = pathlib.Path(sys.argv[2])
expected_body = pathlib.Path(sys.argv[3]).read_bytes()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_PUT(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        assert self.path == "/guest-kernels/test/sha256%3Atest/vmlinux.bin"
        assert self.headers["If-None-Match"] == "*"
        assert self.headers["Content-Type"] == "application/octet-stream"
        authorization = self.headers["Authorization"]
        assert authorization.startswith("AWS4-HMAC-SHA256 ")
        assert "if-none-match" in authorization
        assert "x-amz-checksum-sha256" in authorization
        expected_checksum = base64.b64encode(hashlib.sha256(expected_body).digest()).decode()
        assert self.headers["x-amz-checksum-sha256"] == expected_checksum
        assert self.headers["x-amz-content-sha256"] == hashlib.sha256(expected_body).hexdigest()
        assert body == expected_body
        result_file.write_text("ok\n")
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
port_file.write_text(f"{server.server_port}\n")
server.handle_request()
PY
port_file="${tmp_dir}/conditional-put-port"
result_file="${tmp_dir}/conditional-put-result"
python3 "${tmp_dir}/conditional-put-server.py" \
  "$port_file" "$result_file" "${artifact_dir}/vmlinux.bin" &
server_pid=$!
for _ in $(seq 1 100); do
  [[ -s "$port_file" ]] && break
  kill -0 "$server_pid" 2>/dev/null || break
  sleep 0.01
done
[[ -s "$port_file" ]]
S3_ACCESS_KEY_ID=test-access-key
S3_SECRET_ACCESS_KEY=test-secret-key
S3_SESSION_TOKEN=test-session-token
put_object_if_absent \
  test-bucket \
  guest-kernels/test/sha256:test/vmlinux.bin \
  "${artifact_dir}/vmlinux.bin" \
  application/octet-stream \
  "http://127.0.0.1:$(cat "$port_file")"
wait "$server_pid"
server_pid=""
[[ "$(cat "$result_file")" == ok ]]
# These globals are consumed by functions sourced from the publish script.
# shellcheck disable=SC2034
S3_ACCESS_KEY_ID=""
# shellcheck disable=SC2034
S3_SECRET_ACCESS_KEY=""
# shellcheck disable=SC2034
S3_SESSION_TOKEN=""

docker_ready_bin="${tmp_dir}/docker-ready-bin"
docker_ready_state="${tmp_dir}/docker-ready-state"
mkdir -p "$docker_ready_bin"
cat >"${docker_ready_bin}/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
state="$(cat "$DOCKER_READY_STATE" 2>/dev/null || printf '0')"
if [[ "$1" == "info" ]]; then
  state="$((state + 1))"
  printf '%s\n' "$state" >"$DOCKER_READY_STATE"
  (( state >= 3 ))
  exit
fi
if [[ "$1" == "build" ]]; then
  if (( state < 3 )); then
    echo 'ERROR: Cannot connect to the Docker daemon at tcp://localhost:2375. Is the docker daemon running?' >&2
    exit 1
  fi
  exit
fi
exit 1
EOF
cat >"${docker_ready_bin}/sleep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "${docker_ready_bin}/docker" "${docker_ready_bin}/sleep"
DOCKER_READY_STATE="$docker_ready_state" PATH="${docker_ready_bin}:${PATH}" \
  build_kernel "${tmp_dir}/docker-ready-output" >/dev/null
[[ "$(cat "$docker_ready_state")" == 3 ]]

cat >"${tmp_dir}/docker" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == "create" ]]; then
  printf '%s\n' smoke-container-id
elif [[ "$1" == "start" ]]; then
  printf '%s\n' \
    'Linux version 6.1.155 (space@kernel-builder)' \
    'Kernel panic - not syncing: VFS: Unable to mount root fs on unknown-block(254,0)'
  exit 124
fi
EOF
chmod +x "${tmp_dir}/docker"
PATH="${tmp_dir}:${PATH}" "${KERNEL_DIR}/smoke-boot.sh" \
  "${artifact_dir}/vmlinux.bin" "$KERNEL_VERSION" >/dev/null

# A variant with boot-log expectations boots with initcall_debug and must show
# each expected initcall; the default profile boots without it.
variant_bin="${tmp_dir}/variant-bin"
mkdir -p "$variant_bin"
cat >"${variant_bin}/docker" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == "create" ]]; then
  printf '%s\n' "$*" >"$SMOKE_CREATE_ARGS"
  printf '%s\n' smoke-container-id
elif [[ "$1" == "start" ]]; then
  printf '%s\n' 'Linux version 6.1.155 (space@kernel-builder)'
  if [[ -n "${SMOKE_INITCALLS:-}" ]]; then
    for initcall in nf_tables_module_init nft_compat_module_init iptable_raw_init ip6table_raw_init state_mt_init; do
      printf '[    0.9] initcall %s+0x0/0xe8 returned 0 after 4 usecs\n' "$initcall"
    done
  fi
  printf '%s\n' 'Kernel panic - not syncing: VFS: Unable to mount root fs on unknown-block(254,0)'
  exit 124
fi
EOF
chmod +x "${variant_bin}/docker"
create_args="${tmp_dir}/smoke-create-args"
SMOKE_INITCALLS=1 SMOKE_CREATE_ARGS="$create_args" PATH="${variant_bin}:${PATH}" \
  "${KERNEL_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "$KERNEL_VERSION" nft >/dev/null
grep -Fq 'initcall_debug ignore_loglevel' "$create_args"
if SMOKE_CREATE_ARGS="$create_args" PATH="${variant_bin}:${PATH}" \
    "${KERNEL_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "$KERNEL_VERSION" nft >/dev/null 2>&1; then
  echo "smoke-boot accepted an nft boot without nf_tables" >&2
  exit 1
fi
SMOKE_CREATE_ARGS="$create_args" PATH="${variant_bin}:${PATH}" \
  "${KERNEL_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "$KERNEL_VERSION" >/dev/null
if grep -Fq initcall_debug "$create_args"; then
  echo "smoke-boot added initcall_debug to the default profile" >&2
  exit 1
fi
if SMOKE_CREATE_ARGS="$create_args" PATH="${variant_bin}:${PATH}" \
    "${KERNEL_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "$KERNEL_VERSION" missing-variant >/dev/null 2>&1; then
  echo "smoke-boot accepted an unknown variant" >&2
  exit 1
fi

# The artifact version follows the variant; a caller fragment stays bpf-modsig.
artifact_version_for() {
  # SC2016: the nested shell expands $1 and ARTIFACT_VERSION.
  # shellcheck disable=SC2016
  env -u KERNEL_VARIANT -u CONFIG_FRAGMENT "$@" \
    bash -c 'source "$1"; echo "$ARTIFACT_VERSION"' _ "$publish_script"
}
[[ "$(artifact_version_for KERNEL_VARIANT=nft)" == vmlinux-6.1.155-nft ]]
[[ "$(artifact_version_for CONFIG_FRAGMENT=custom.config)" == vmlinux-6.1.155-bpf-modsig ]]
[[ "$(artifact_version_for)" == vmlinux-6.1.155 ]]

fake_bin="${tmp_dir}/bin"
calls="${tmp_dir}/publish-calls"
mkdir -p "$fake_bin"
cat >"${fake_bin}/aws" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$AWS_CALLS"
if [[ "$1 $2" == "configure export-credentials" ]]; then
  printf '%s\n' '{"Version":1,"AccessKeyId":"test-access-key","SecretAccessKey":"test-secret-key","SessionToken":"test-session-token"}'
  exit
fi
if [[ "$1 $2" == "s3api head-object" ]]; then
  exit 1
fi
if [[ "$1 $2" == "s3api put-object" ]]; then
  if [[ " $* " == *" --if-none-match "* ]]; then
    echo 'Unknown options: --if-none-match,*' >&2
    exit 2
  fi
  exit
fi
if [[ "$1 $2" == "s3 cp" ]]; then
  cp "$AWS_EXISTING_FILE" "$4"
fi
EOF
cat >"${fake_bin}/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'curl' >>"$AWS_CALLS"
printf ' %q' "$@" >>"$AWS_CALLS"
printf '\n' >>"$AWS_CALLS"
output_file=""
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    output_file="$2"
    shift 2
    continue
  fi
  shift
done
if [[ -n "$output_file" ]]; then
  printf '%s' "${CURL_RESPONSE:-}" >"$output_file"
fi
status="${CURL_STATUS:-200}"
if [[ -n "${CURL_STATUS_SEQUENCE:-}" ]]; then
  attempt="$(cat "$CURL_STATUS_STATE" 2>/dev/null || printf '0')"
  attempt="$((attempt + 1))"
  printf '%s\n' "$attempt" >"$CURL_STATUS_STATE"
  IFS=',' read -r -a statuses <<<"$CURL_STATUS_SEQUENCE"
  status="${statuses[$((attempt - 1))]:-${statuses[$((${#statuses[@]} - 1))]}}"
fi
printf '%s' "$status"
EOF
cat >"${fake_bin}/sleep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "${fake_bin}/aws"
chmod +x "${fake_bin}/curl"
chmod +x "${fake_bin}/sleep"

AWS_CALLS="$calls" PATH="${fake_bin}:${PATH}" \
  publish_to_bucket test-bucket "$artifact_dir" "$digest" >/dev/null

prefix="guest-kernels/${ARTIFACT_VERSION}/sha256:${digest}"
encoded_prefix="${prefix/:/%3A}"
for file in vmlinux.bin vmlinux.bin.sha256 config manifest.json; do
  grep -Fq -- "/${encoded_prefix}/${file}" "$calls"
done
[[ "$(tail -n 1 "$calls")" == *"/${encoded_prefix}/manifest.json"* ]]
if grep -Eq '/(default|legacy-default)(/| )' "$calls"; then
  echo "publish path unexpectedly contains a fleet-default alias" >&2
  exit 1
fi

retry_calls="${tmp_dir}/retry-calls"
retry_state="${tmp_dir}/retry-state"
if ! AWS_CALLS="$retry_calls" CURL_STATUS_SEQUENCE=409,409,200 CURL_STATUS_STATE="$retry_state" \
  CURL_RESPONSE='<Error><Code>ConditionalRequestConflict</Code><Message>conflict</Message></Error>' \
  PATH="${fake_bin}:${PATH}" \
  bash -c 'source "$1"; publish_immutable_file test-bucket guest-kernels/raced "$2" text/plain' \
    _ "$publish_script" "${artifact_dir}/config" >/dev/null; then
  echo "publisher did not retry a concurrent conditional PUT conflict" >&2
  exit 1
fi
[[ "$(grep -c '^curl ' "$retry_calls")" == 3 ]]

bounded_retry_calls="${tmp_dir}/bounded-retry-calls"
bounded_retry_error="${tmp_dir}/bounded-retry-error"
if AWS_CALLS="$bounded_retry_calls" CURL_STATUS=409 \
  CURL_RESPONSE='<Error><Code>ConditionalRequestConflict</Code><Message>conflict</Message></Error>' \
  PATH="${fake_bin}:${PATH}" \
  bash -c 'source "$1"; publish_immutable_file test-bucket guest-kernels/conflicted "$2" text/plain' \
    _ "$publish_script" "${artifact_dir}/config" >/dev/null 2>"$bounded_retry_error"; then
  echo "publisher retried a conditional PUT conflict without a bound" >&2
  exit 1
fi
[[ "$(grep -c '^curl ' "$bounded_retry_calls")" == 3 ]]
grep -Fq 'S3 conditional PUT failed with HTTP 409' "$bounded_retry_error"

printf 'different bytes\n' >"${tmp_dir}/different"
if AWS_CALLS="$calls" AWS_EXISTING_FILE="${tmp_dir}/different" CURL_STATUS=412 \
  PATH="${fake_bin}:${PATH}" \
  bash -c 'source "$1"; publish_immutable_file test-bucket guest-kernels/existing "$2" text/plain' \
    _ "$publish_script" "${artifact_dir}/config" >/dev/null 2>&1; then
  echo "publisher overwrote an immutable key with different bytes" >&2
  exit 1
fi

error_log="${tmp_dir}/publish-error"
if AWS_CALLS="$calls" CURL_STATUS=403 \
  CURL_RESPONSE='<Error><Code>AccessDenied</Code><Message>denied</Message><SessionToken>sensitive-token</SessionToken></Error>' \
  PATH="${fake_bin}:${PATH}" \
  bash -c 'source "$1"; publish_immutable_file test-bucket guest-kernels/denied "$2" text/plain' \
    _ "$publish_script" "${artifact_dir}/config" >/dev/null 2>"$error_log"; then
  echo "publisher unexpectedly accepted an S3 error" >&2
  exit 1
fi
grep -Fq 'S3 error AccessDenied: denied' "$error_log"
if grep -Fq 'sensitive-token' "$error_log"; then
  echo "publisher leaked credential material from an S3 error response" >&2
  exit 1
fi

echo "Guest kernel publish guards passed"
