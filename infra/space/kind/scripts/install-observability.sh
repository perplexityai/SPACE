#!/usr/bin/env bash
# Install the local observability backends for this Kind bench.
#
# Both supported bring-up paths call this: bootstrap.sh, and the Tiltfile's
# install-observability-backends resource. It may run before or after the shim
# and workloads: they address the Collectors by fixed ClusterIPs and retry
# exports until a receiver answers.
#
# Each helper is idempotent and refuses any cluster but the selected bench.
set -Eeuo pipefail
readonly kind_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
log() { echo "[install-observability] $*"; }
for signal in metrics traces logs grafana; do
  log "installing the $signal backend (SPACE_KIND_OBSERVABILITY=off to skip)"
  bash "$kind_dir/$signal/local.sh" up
done
log "installed: metrics traces logs grafana"
