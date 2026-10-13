#!/usr/bin/env bash
# Prepare a node/image-isolated cluster without starting application builds.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
API_URL="${API_URL:-http://localhost:8080}"
KIND_CLUSTER="${KIND_CLUSTER:-space-local}"
# shellcheck source=../../../../scripts/tests/lib/bootstrap-kind-stack.sh
source "$REPO_ROOT/scripts/tests/lib/bootstrap-kind-stack.sh"
kind_stack_prepare_cluster
