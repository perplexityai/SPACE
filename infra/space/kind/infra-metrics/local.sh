#!/usr/bin/env bash
# Never inherit the caller's active Kubernetes context.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/../lib/release.sh"
action=${1:-status}
shift || true
release_init infra-metrics
case "$action" in
  up)
    helm_options "$@"
    helm_upgrade 3m
    ;;
  status) "${kube[@]}" -n "$namespace" get pods,deploy,daemonset,svc ;;
  ui)
    exec "${kube[@]}" -n "$namespace" port-forward --address 127.0.0.1 svc/kube-state-metrics 18080:8080
    ;;
  cleanup)
    confirm_cleanup "${1:-}" 'Requires --confirm-delete-local-data; deletes this namespace; these exporters hold no data'
    "${kube[@]}" delete namespace "$namespace" --wait=false
    ;;
  *) echo 'Usage: local.sh up [helm flags] | status | ui | cleanup --confirm-delete-local-data' >&2; exit 2 ;;
esac
