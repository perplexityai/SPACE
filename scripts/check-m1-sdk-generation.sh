#!/usr/bin/env bash
# Generate a Python SDK from an M1 OpenAPI document with the pinned OpenAPI
# Generator and check it compiles with all 14 release operations.
#
# Exits 1 when the generator rejects the document or the SDK fails the check,
# and 69 when the generator could not run (no image, a Docker error, a
# missing tool, a killed generator), so a caller can tell a contract break
# from an unavailable environment. SPACE_M1_GENERATED_SDK_DIR receives the
# generated SDK; SPACE_M1_GENERATOR_LABEL (key=value) labels the generator's
# container, so a caller that has to stop this script can still remove it.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GENERATOR_UNAVAILABLE=69

# v7.25.0: pin the generator so neither upstream nor build configuration can
# redefine conformance before the generated package is imported with a token.
GENERATOR_IMAGE="openapitools/openapi-generator-cli@sha256:2ab0a9680222de65dc9d3baf861aa02b99e1b80c211d8221ebf3ae8f8a102524"
# The index's linux/amd64 image. GitHub CI takes it by this digest from its
# GHCR mirror (ci/mirrored-images.txt), whose repository it names in
# SPACE_M1_GENERATOR_MIRROR.
GENERATOR_AMD64_DIGEST="sha256:d859876eefb0c9ac2b2aac9935a1ff775e6a8af0da175d2a159b05811deff32f"
SPEC_SOURCE="${1:-}"

unavailable() {
  echo "ERROR: $*" >&2
  exit "$GENERATOR_UNAVAILABLE"
}

if [[ -z "$SPEC_SOURCE" ]]; then
  echo "usage: $0 <openapi-json-path-or-url>" >&2
  exit 64
fi

for bin in curl docker python3; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    unavailable "required command not found: $bin"
  fi
done
# GNU coreutils' timeout, which Homebrew installs as gtimeout. --foreground
# keeps each bounded pull in this script's process group, so a caller that
# ends the group ends the pull too.
timeout_bin=""
for candidate in timeout gtimeout; do
  if command -v "$candidate" >/dev/null 2>&1; then
    timeout_bin="$candidate"
    break
  fi
done
if [[ -z "$timeout_bin" ]]; then
  unavailable "required command not found: timeout (GNU coreutils; gtimeout on macOS)"
fi

# The mirror's copy cannot satisfy the index's reference, so name it directly.
# Take it from the mirror only while ci/mirrored-images.txt pairs it with this
# index, the pairing the mirror's publisher verified; otherwise use the index.
if [[ -n "${SPACE_M1_GENERATOR_MIRROR:-}" ]]; then
  mirrored="$SPACE_M1_GENERATOR_MIRROR@$GENERATOR_AMD64_DIGEST"
  if ! docker image inspect "$mirrored" >/dev/null 2>&1; then
    if ! awk -v index_digest="${GENERATOR_IMAGE#*@}" -v digest="$GENERATOR_AMD64_DIGEST" \
      '$1 ~ "@" index_digest "$" && $2 == digest { found = 1 } END { exit !found }' \
      "$REPO_ROOT/ci/mirrored-images.txt" 2>/dev/null; then
      echo "WARNING: ci/mirrored-images.txt does not pair $GENERATOR_AMD64_DIGEST with $GENERATOR_IMAGE; not using the mirror" >&2
    elif ! "$timeout_bin" --foreground --kill-after=10 300 docker pull --quiet "$mirrored" >/dev/null; then
      echo "WARNING: could not pull $mirrored; using $GENERATOR_IMAGE" >&2
    fi
  fi
  if docker image inspect "$mirrored" >/dev/null 2>&1; then
    GENERATOR_IMAGE="$mirrored"
  fi
  echo "Generating with $GENERATOR_IMAGE"
fi
if ! docker image inspect "$GENERATOR_IMAGE" >/dev/null 2>&1 &&
  ! "$timeout_bin" --foreground --kill-after=10 600 docker pull --quiet "$GENERATOR_IMAGE" >/dev/null; then
  unavailable "could not pull $GENERATOR_IMAGE"
fi

work_dir="$(mktemp -d)"
container=""
cleanup() {
  if [[ -n "$container" ]]; then
    docker rm --force "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

spec="$work_dir/openapi.json"
case "$SPEC_SOURCE" in
  http://*|https://*) curl -fsS "$SPEC_SOURCE" -o "$spec" ;;
  *) cp -- "$SPEC_SOURCE" "$spec" ;;
esac

labels=()
if [[ -n "${SPACE_M1_GENERATOR_LABEL:-}" ]]; then
  labels=(--label "$SPACE_M1_GENERATOR_LABEL")
fi
container="$(docker create ${labels[@]+"${labels[@]}"} "$GENERATOR_IMAGE" generate \
  -g python \
  -i /opt/openapi-generator/openapi.json \
  -o /opt/openapi-generator/out \
  --global-property apiTests=false,modelTests=false,apiDocs=false,modelDocs=false)" ||
  unavailable "could not create the generator container"
docker cp "$spec" "$container:/opt/openapi-generator/openapi.json" ||
  unavailable "could not copy the document into the generator container"
docker start "$container" >/dev/null || unavailable "could not start the generator container"
status="$(docker wait "$container")" || unavailable "could not wait for the generator container"
docker logs "$container" >"$work_dir/generator.log" 2>&1 ||
  unavailable "could not read the generator's output"
# 137 is SIGKILL, as when the kernel's OOM killer ends the generator.
if [[ "$status" == "137" ]]; then
  cat "$work_dir/generator.log" >&2
  unavailable "the generator was killed (exit 137)"
fi
if [[ "$status" != "0" ]] || grep -q '^\[main\] ERROR ' "$work_dir/generator.log"; then
  cat "$work_dir/generator.log" >&2
  echo "ERROR: OpenAPI Generator rejected the M1 document" >&2
  exit 1
fi
docker cp "$container:/opt/openapi-generator/out/." "$work_dir/generated" ||
  unavailable "could not copy the generated SDK out of the generator container"
docker rm "$container" >/dev/null
container=""

python3 -m compileall -q "$work_dir/generated/openapi_client"
python3 - "$work_dir/generated/openapi_client/api/default_api.py" <<'PY'
import ast
import pathlib
import sys

expected = {
    "create_command",
    "create_sandbox",
    "delete_sandbox",
    "get_operation",
    "get_process",
    "get_sandbox",
    "kill_process",
    "list_processes",
    "list_sandboxes",
    "pause_sandbox",
    "signal_process",
    "stream_session_events",
    "suspend_sandbox",
    "wake_sandbox",
}
module = ast.parse(pathlib.Path(sys.argv[1]).read_text())
api = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "DefaultApi")
generated = {
    node.name
    for node in api.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    and not node.name.startswith("_")
    and not node.name.endswith(("_with_http_info", "_without_preload_content"))
}
if generated != expected:
    raise SystemExit(
        f"generated SDK operation mismatch: missing={sorted(expected - generated)}, "
        f"unexpected={sorted(generated - expected)}"
    )
PY

if [[ -n "${SPACE_M1_GENERATED_SDK_DIR:-}" ]]; then
  cp -R "$work_dir/generated/." "$SPACE_M1_GENERATED_SDK_DIR"
fi

echo "M1 OpenAPI generated a compilable Python SDK with all 14 release operations"
