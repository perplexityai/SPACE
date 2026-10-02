#!/usr/bin/env bash
# Install the local observability backends for this Kind bench.
#
# Both supported bring-up paths call this: bootstrap.sh, and the Tiltfile's
# install-observability-backends resource. install-shim.sh resolves the metrics
# and traces Collector Services to ClusterIPs and exits 2 if they are absent, so
# every path that installs the shim must run this first.
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
