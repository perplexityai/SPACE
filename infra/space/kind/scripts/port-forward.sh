#!/usr/bin/env bash
# Hold one localhost port-forward to a Kind Service open, restarting it
# whenever kubectl exits.
#
# kubectl port-forward dies on its own when the target pod restarts (e.g.,
# during a chart redeploy) or when node-side gRPC keepalives time out, and a
# forward that has silently gone away fails whatever later reads its port.
# bootstrap.sh runs this under `setsid` for every forward it opens. The
# Tiltfile runs it as the serve_cmd of the forwards to the local observability
# backends: install-observability.sh installs those with Helm, not k8s_yaml,
# so Tilt tracks no pods for them and its own port_forwards cannot reach them.
#
# Usage: port-forward.sh <context> <namespace> <svc/name> <local:remote> [label]
#
# The label is not read. Callers pass `space-kind-pf:<context>` as the LAST
# argument so that this loop's own command line carries one cluster-scoped
# token, and a pattern-based reap can find the loop and not only its kubectl
# child.
set -uo pipefail
kctx="$1" namespace="$2" target="$3" mapping="$4"
while true; do
  kubectl --context "$kctx" -n "$namespace" port-forward --address=127.0.0.1 "$target" "$mapping"
  # kubectl exited (target rolled, conn dropped); back off then retry.
  sleep 2
done
