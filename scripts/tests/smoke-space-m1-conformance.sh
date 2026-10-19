#!/usr/bin/env bash
# Run the M1 generated-SDK conformance check against a running SPACE API.
#
# The check lives in space_smoke/m1_conformance.py; Kind CI runs it as the
# typed smoke scenario native.m1_generated_sdk_conformance. This runs it
# directly, outside the typed suite, against whichever API the environment
# names, local or remote, with no Kind tooling.
#
# Env: SPACE_API_TOKEN (required), SPACE_API_URL (http://127.0.0.1:8080),
# SPACE_DATA_PLANE_URL (the API URL), SPACE_TEMPLATE (seed:latest),
# SPACE_RUNTIME (fc), SPACE_READY_TIMEOUT_SECS (180), and
# SPACE_M1_GENERATOR_MIRROR to take the pinned generator from that mirror
# (see scripts/check-m1-sdk-generation.sh).
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ -z "${SPACE_API_TOKEN:-}" ]]; then
  echo "ERROR: SPACE_API_TOKEN is required" >&2
  exit 1
fi

uv_bin="$(bash "$REPO_ROOT/ci/scripts/ensure-uv.sh")"
# space_smoke is imported from the checkout, not installed.
cd "$REPO_ROOT"
exec "$uv_bin" run --frozen --python 3.11 python -m space_smoke.m1_conformance
