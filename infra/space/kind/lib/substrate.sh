#!/usr/bin/env bash
# substrate.sh — which substrate the Kind control plane runs, selected by
# SPACE_KIND_SUBSTRATE for every bring-up path (the Tiltfile, up.sh and
# scripts/tests/lib/bootstrap-kind-stack.sh share it; the smoke runner reads a
# bench's substrate as space_kind_deployed_substrate does):
#
#   kubernetes  (default) the KubernetesSubstrate: a pod per sandbox, placed by
#               kube-scheduler, nodes registered by the cluster controller's
#               Kubernetes node sync.
#   direct      the DirectSubstrate on the same cluster and images: the API and
#               cluster controller place node-local VMs on nodes registered
#               through the admin node registry
#               (scripts/register-direct-nodes.sh), with no pod per sandbox.
#               See "Direct substrate lane" in infra/space/kind/README.md.
#
# A cluster runs one substrate for its lifetime: its database holds the other
# substrate's node and sandbox rows, so switching means recreating it.
#
# Source it for the functions, or run it to print the mode, or
# `substrate.sh check <cluster>` to refuse a cluster running the other one.

# Prints the validated mode; fails on any other value.
space_kind_substrate() {
  local mode="${SPACE_KIND_SUBSTRATE:-kubernetes}"
  case "$mode" in
    kubernetes|direct) printf '%s\n' "$mode" ;;
    *)
      printf 'SPACE_KIND_SUBSTRATE must be kubernetes or direct, not %q\n' "$mode" >&2
      return 1
      ;;
  esac
}

# Prints the substrate kind cluster $1 runs, read from the API Deployment's
# SPACE_K8S_CONTROL_PLANE ("true" kubernetes, "false" direct), and nothing
# when the cluster or the Deployment does not exist yet. Fails when the
# cluster cannot be read or the value is neither. kubectl's stderr is kept out
# of the value, since it may warn on a read that succeeds.
space_kind_deployed_substrate() {
  local cluster="$1" value errors rc=0
  kind get clusters 2>/dev/null | grep -qx "$cluster" || return 0
  errors="$(mktemp)"
  value="$(kubectl --context "kind-$cluster" -n space get deployments \
      -l app.kubernetes.io/name=space-api \
      -o jsonpath='{range .items[*]}{range .spec.template.spec.containers[?(@.name=="api")]}{range .env[?(@.name=="SPACE_K8S_CONTROL_PLANE")]}{.value}{"\n"}{end}{end}{end}' \
      2>"$errors")" || rc=$?
  if (( rc != 0 )); then
    printf 'cannot read the substrate of kind cluster %s: %s\n' "$cluster" "$(cat "$errors")" >&2
    rm -f "$errors"
    return 1
  fi
  rm -f "$errors"
  case "$value" in
    "") ;;
    true) printf 'kubernetes\n' ;;
    false) printf 'direct\n' ;;
    *)
      printf 'kind cluster %s runs no known substrate: its space-api SPACE_K8S_CONTROL_PLANE is %q\n' \
        "$cluster" "$value" >&2
      return 1
      ;;
  esac
}

# Refuses kind cluster $1 when it runs a substrate other than the requested
# one, with the remedy on stderr.
space_kind_check_substrate() {
  local cluster="$1" requested deployed env_prefix=""
  requested="$(space_kind_substrate)" || return 1
  deployed="$(space_kind_deployed_substrate "$cluster")" || return 1
  [[ -z "$deployed" || "$deployed" == "$requested" ]] && return 0
  [[ "$cluster" == space-local ]] || env_prefix="SPACE_KIND_CLUSTER=$cluster "
  {
    printf 'ERROR: kind cluster %s runs the %s substrate, and this run asks for\n' "$cluster" "$deployed"
    printf '       SPACE_KIND_SUBSTRATE=%s. Its database keeps the %s node and\n' "$requested" "$deployed"
    printf '       sandbox rows, so the cluster cannot switch. Set SPACE_KIND_SUBSTRATE=%s,\n' "$deployed"
    printf '       or recreate the cluster in the requested substrate:\n'
    printf '         %sbash infra/space/kind/scripts/teardown.sh\n' "$env_prefix"
    printf '         %sSPACE_KIND_SUBSTRATE=%s bash infra/space/kind/scripts/up.sh\n' "$env_prefix" "$requested"
    printf '       Nothing was changed.\n'
  } >&2
  return 1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  case "${1:-mode}" in
    mode) space_kind_substrate ;;
    check) space_kind_check_substrate "${2:-${SPACE_KIND_CLUSTER:-space-local}}" ;;
    *) echo "usage: $0 [mode|check [cluster]]" >&2; exit 2 ;;
  esac
fi
