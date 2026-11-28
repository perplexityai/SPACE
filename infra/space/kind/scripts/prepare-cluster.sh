#!/usr/bin/env bash
# Prepare a node/image-isolated cluster without starting application builds.
#
# Usage: prepare-cluster.sh [--observability] [--prepare-mirror <mirror>]
#   --observability  also install the selected observability profile, as the
#                    Tiltfile's evaluation would (its Datadog preflight, then
#                    install-observability.sh), once the cluster exists and
#                    beside the load of the infra charts' images into it.
#                    Kind CI's shards and the scripts/tests/smoke-kind-parallel.sh
#                    benches prepare this way while the Rust image is built:
#                    run after the preparation, the install added about a
#                    minute to it.
#   --prepare-mirror <mirror>
#                    SPACE_CI_IMAGE_MIRROR for the cluster preparation only;
#                    the observability install keeps the caller's. The benches
#                    take the infra charts' images through the mirror, falling
#                    back to their sources, but install the observability
#                    images from their sources' platform digests as before.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
API_URL="${API_URL:-http://localhost:8080}"
KIND_CLUSTER="${KIND_CLUSTER:-${SPACE_KIND_CLUSTER:-space-local}}"
usage() { echo "usage: $0 [--observability] [--prepare-mirror <mirror>]" >&2; exit 2; }
observability=0
prepare_mirror=""
prepare_mirror_given=0
while (( $# > 0 )); do
  case "$1" in
    --observability) observability=1; shift ;;
    --prepare-mirror)
      (( $# >= 2 )) || usage
      prepare_mirror="$2"; prepare_mirror_given=1; shift 2 ;;
    *) usage ;;
  esac
done
# shellcheck source=../../../../scripts/tests/lib/bootstrap-kind-stack.sh
source "$REPO_ROOT/scripts/tests/lib/bootstrap-kind-stack.sh"

caller_mirror_set="${SPACE_CI_IMAGE_MIRROR+1}"
caller_mirror="${SPACE_CI_IMAGE_MIRROR-}"
(( ! prepare_mirror_given )) || export SPACE_CI_IMAGE_MIRROR="$prepare_mirror"
if (( ! observability )); then
  kind_stack_prepare_cluster
  exit 0
fi

kind_stack_prepare_cluster --defer-chart-images
# The chart image load, already running, keeps the preparation's mirror.
if [[ -n "$caller_mirror_set" ]]; then
  export SPACE_CI_IMAGE_MIRROR="$caller_mirror"
else
  unset SPACE_CI_IMAGE_MIRROR
fi
status=0
# The CI layout pins the backends to the infra node, which install-observability.sh
# finds by its label; kind sets it as the node's kubelet registers.
for _ in $(seq 1 30); do
  [[ -z "$(kubectl --context "kind-$KIND_CLUSTER" get nodes -l space.pplx.net/node-pool=infra \
    -o name 2>/dev/null)" ]] || break
  sleep 2
done
cd "$REPO_ROOT"
# A failed preflight fails the preparation, as it fails the Tiltfile.
SPACE_KIND_CLUSTER="$KIND_CLUSTER" python3 "$REPO_ROOT/infra/space/kind/scripts/datadog_backend.py" \
  preflight --for-deployment >/dev/null \
  && SPACE_KIND_CLUSTER="$KIND_CLUSTER" bash "$REPO_ROOT/infra/space/kind/scripts/install-observability.sh" \
  || status=$?
kind_stack_finish_chart_images
exit "$status"
