#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/../lib/release.sh"
action=${1:-status}
shift || true
release_init logs
case "$action" in
  up)
    # Fail rather than silently deploying a journal receiver at an absent path.
    # kind reports a cluster with no nodes on stderr and exits 0, so an empty
    # list fails too instead of checking nothing.
    nodes=$(kind get nodes --name "$cluster")
    [[ -n $nodes ]] || { echo "No kind nodes found for cluster \"$cluster\"" >&2; exit 1; }
    while IFS= read -r node; do
      docker exec "$node" test -d /run/log/journal
      docker exec "$node" test -d /var/log/pods
    done <<<"$nodes"
    helm_options "$@"
    # Do not advance source cursors while a brand-new Loki is still starting.
    if ! helm status "$release" --kube-context "$context" -n "$namespace" >/dev/null 2>&1; then
      helm_upgrade 5m --set alloy.enabled=false
    fi
    helm_upgrade 5m --set alloy.enabled=true
    ;;
  status) "${kube[@]}" -n "$namespace" get pods,deploy,ds,job,svc,pvc ;;
  ui)
    exec "${kube[@]}" -n "$namespace" port-forward --address 127.0.0.1 svc/loki 13101:3100
    ;;
  restart)
    "${kube[@]}" -n "$namespace" rollout restart daemonset/alloy
    "${kube[@]}" -n "$namespace" rollout status daemonset/alloy --timeout=3m
    ;;
  cleanup)
    confirm_cleanup "${1:-}" 'Requires --confirm-delete-local-data; destroys the local Loki PVC'
    "${kube[@]}" delete namespace "$namespace" --wait=false
    echo 'Node-local Alloy cursors remain at /var/lib/space-logs/alloy; shared logs and other stacks are untouched.'
    ;;
  *) echo 'Usage: local.sh up [--set key=value] | status | ui | restart | cleanup --confirm-delete-local-data' >&2; exit 2 ;;
esac
