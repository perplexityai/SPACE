#!/usr/bin/env bash
# install.sh — install the SPACE Kind charts on a caller-provided cluster
# through the space-cluster umbrella (README.md).
#
#   install.sh -f VALUES --template [--observability-namespace NS]
#       Print what would be applied and exit. Needs no cluster, but when the
#       observability chart is in the tree it runs `helm dependency build` on
#       it, which writes that chart's charts/ and Chart.lock.
#   install.sh -f VALUES --kube-context CONTEXT [--namespace space]
#              [--observability-namespace space-observability] [--timeout 600s]
#       Check, then install, in this order: read-only checks of the namespaces
#       and Secrets every render references and of RuntimeClass space; the
#       node-local receivers (release space-observability from
#       infra/space/observability/charts/space-observability, in its own
#       namespace, reconciled in every mode when that chart is in the tree;
#       without it only mode off installs); the space-db-migrate Job; then
#       the space and space-builder releases.
#
# The kube context is never inferred. Nothing here creates Secrets,
# namespaces, a database, an object store or a key broker; those are
# prerequisites (README.md).
set -euo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
chart=$here/chart
namespace=space
obs_namespace=space-observability
context=
values=
template=false
timeout=600s

usage() { sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }
while (($#)); do
  case "$1" in
    -f|--values) (($# >= 2)) || usage; values=$2; shift 2 ;;
    --kube-context) (($# >= 2)) || usage; context=$2; shift 2 ;;
    --namespace|-n) (($# >= 2)) || usage; namespace=$2; shift 2 ;;
    --observability-namespace) (($# >= 2)) || usage; obs_namespace=$2; shift 2 ;;
    --timeout) (($# >= 2)) || usage; timeout=$2; shift 2 ;;
    --template) template=true; shift ;;
    -h|--help) usage 0 ;;
    *) echo "install.sh: unsupported argument: $1" >&2; usage ;;
  esac
done
[[ -n $values ]] || { echo "install.sh: -f VALUES is required" >&2; exit 2; }
[[ -f $values ]] || { echo "install.sh: $values does not exist" >&2; exit 2; }
if [[ $template == false && -z $context ]]; then
  echo "install.sh: --kube-context is required (the current context is never used)" >&2
  exit 2
fi

if [[ $obs_namespace == "$namespace" ]]; then
  echo "install.sh: the receivers need their own namespace (--observability-namespace)" >&2
  exit 2
fi
log() { printf 'install.sh: %s\n' "$*" >&2; }

# The two releases, each one Helm release of the same umbrella: `space` (the
# control plane and the worker node services) and `space-builder` (the builder
# node services). The builder file goes before the caller's, so the caller's
# images, endpoints and Secrets apply to both; the chart's validation rejects
# a caller file that changes the builder role's shape.
worker_args=(space "$chart" --namespace "$namespace" -f "$values")
builder_args=(space-builder "$chart" --namespace "$namespace" -f "$chart/values-builder.yaml" -f "$values")
migrate_args=(space "$chart" --namespace "$namespace" -f "$values"
  --set space-api-local.migrate.enabled=true
  --show-only charts/space-api-local/templates/migrate-job.yaml)

render() { helm template "$@" 2> >(grep -v 'found symbolic link in path' >&2); }
worker=$(render "${worker_args[@]}")
builder=$(render "${builder_args[@]}")
migrate=$(render "${migrate_args[@]}")

# The telemetry mode, as the render resolved it (metrics always follow it).
mode=$(grep -A1 -- '- name: SPACE_METRICS_SINK' <<<"$worker" | sed -n 's/.*value: "\(.*\)"/\1/p' | sort -u)
if [[ $(wc -w <<<"$mode") -ne 1 ]]; then
  echo "install.sh: could not read one telemetry mode from the render (got: $mode)" >&2
  exit 1
fi
# Node-local receivers, from the observability chart in their own namespace.
# When the chart is in the tree it is reconciled in every mode, off included,
# so switching to off also reconfigures receivers a previous mode installed.
# The values file is shared, so it also carries that chart's settings (its
# README: the producer namespaces it serves, and for datadog the API key
# Secret).
obs_chart=$here/../observability/charts/space-observability
obs=
obs_available=false
problem() { # $1 message: a warning for --template, fatal for an install
  if [[ $template == true ]]; then log "warning: $1"; else echo "install.sh: $1" >&2; exit 1; fi
}
if [[ -f $obs_chart/Chart.yaml ]]; then
  helm dependency build "$obs_chart" >/dev/null
  obs_args=(space-observability "$obs_chart" --namespace "$obs_namespace" -f "$values")
  obs=$(render "${obs_args[@]}")
  obs_available=true
elif [[ $mode != off ]]; then
  problem "mode $mode needs node-local receivers, and $obs_chart is not in this tree; use mode off"
fi

# kind/namespace/name of every rendered object. Namespaced objects may omit
# metadata.namespace and inherit their Helm release's namespace. The cluster
# scoped kinds used by these charts retain namespace "-".
objects() {
  awk -v ns="${1:-$namespace}" '
    function flush() {
      cluster = k ~ /^(Namespace|ClusterRole|ClusterRoleBinding|ValidatingAdmissionPolicy|ValidatingAdmissionPolicyBinding|RuntimeClass)$/
      if (k != "") print k "/" (cluster ? "-" : (n_s == "" ? ns : n_s)) "/" nm
      k = ""; nm = ""; n_s = ""; inmeta = 0
    }
    /^---/ { flush(); next }
    /^kind: / { k = $2 }
    /^metadata:/ { inmeta = 1; next }
    /^[^ #]/ { inmeta = 0 }
    inmeta && /^  name: / && nm == "" { nm = $2 }
    inmeta && /^  namespace: / && n_s == "" { n_s = $2 }
    END { flush() }'
}
if [[ $mode == otlp && -n $obs ]] && ! objects "$obs_namespace" <<<"$obs" | grep -q "^DaemonSet/$obs_namespace/.*node-collector"; then
  problem "mode otlp needs the node-local OTLP collector DaemonSet, which the space-observability render does not include"
fi

dupes=$( { objects <<<"$worker"; objects <<<"$builder"; objects <<<"$migrate"; objects "$obs_namespace" <<<"$obs"; } | sort | uniq -d)
if [[ -n $dupes ]]; then
  echo "install.sh: objects rendered by more than one release or twice:" >&2
  printf '  %s\n' $dupes >&2
  exit 1
fi

if [[ $template == true ]]; then
  printf '# space-db-migrate Job (applied first, outside both releases)\n%s\n' "$migrate"
  printf -- '---\n# release space\n%s\n' "$worker"
  printf -- '---\n# release space-builder\n%s\n' "$builder"
  if [[ $obs_available == true ]]; then
    printf -- '---\n# release space-observability (namespace %s)\n%s\n' "$obs_namespace" "$obs"
  fi
  exit 0
fi

# Secrets the renders mount or reference and do not create, minus optional ones.
secret_refs() {
  awk '
    function indent(s) { match(s, /^ */); return RLENGTH }
    function emit() { if (want && cand != "" && !opt) print cand; want = 0 }
    {
      if (want && (indent($0) <= base || /^---/)) emit()
      if (/secretKeyRef:$/ || /^ *(- )?secret:$/) { want = 1; base = indent($0); cand = ""; opt = 0; next }
      if (want && /^ *(name|secretName): /) cand = $2
      if (want && /^ *optional: true/) opt = 1
    }
    END { emit() }' | tr -d '"' | sort -u
}
created=$( { objects <<<"$worker"; objects <<<"$builder"; } | awk -F/ '$1 == "Secret" { print $3 }' | sort -u)
needed=$(comm -23 <( { secret_refs <<<"$worker"; secret_refs <<<"$builder"; secret_refs <<<"$migrate"; } | sort -u) <(printf '%s\n' "$created"))
namespaces=$( { objects <<<"$worker"; objects <<<"$builder"; } | awk -F/ '$2 != "-" { print $2 }' | sort -u)

k() { kubectl --context "$context" "$@"; }
missing=()
# Sandbox pods run with runtimeClassName space (README.md, Prerequisites).
handler=$(k get runtimeclass space -o 'jsonpath={.handler}' 2>/dev/null || true)
[[ $handler == space ]] || missing+=("runtimeclass/space with handler space (infra/space/kind/manifests/runtimeclass.yaml)")
# The control plane and its Jobs name the SPACE PriorityClasses (README.md,
# Prerequisites); the system-* classes are built in.
for pc in $( { cat <<<"$worker"; cat <<<"$builder"; cat <<<"$migrate"; } \
    | sed -n 's/^ *priorityClassName: *"\{0,1\}\([^" ]*\)"\{0,1\} *$/\1/p' | grep -v '^system-' | sort -u); do
  k get priorityclass "$pc" >/dev/null 2>&1 || missing+=("priorityclass/$pc (infra/space/kind/manifests/priorityclasses.yaml)")
done
if [[ $obs_available == false && $mode == off ]] \
    && helm status space-observability --kube-context "$context" --namespace "$obs_namespace" >/dev/null 2>&1; then
  echo "install.sh: release space-observability exists in $obs_namespace, but this tree has no chart to reconcile it for mode off; run from the tree that installed it" >&2
  exit 1
fi
for ns in $namespaces; do
  k get namespace "$ns" >/dev/null 2>&1 || missing+=("namespace/$ns")
done
for s in $needed; do
  k -n "$namespace" get secret "$s" >/dev/null 2>&1 || missing+=("secret/$namespace/$s")
done
if [[ $obs_available == true ]]; then
  k get namespace "$obs_namespace" >/dev/null 2>&1 || missing+=("namespace/$obs_namespace")
  obs_created=$(objects "$obs_namespace" <<<"$obs" | awk -F/ '$1 == "Secret" { print $3 }' | sort -u)
  for s in $(comm -23 <(secret_refs <<<"$obs") <(printf '%s\n' "$obs_created")); do
    k -n "$obs_namespace" get secret "$s" >/dev/null 2>&1 || missing+=("secret/$obs_namespace/$s")
  done
fi
if ((${#missing[@]})); then
  echo "install.sh: missing prerequisites in $context (README.md):" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

if [[ $obs_available == true ]]; then
  log "reconciling node-local receivers (mode $mode) as space-observability in $obs_namespace"
  helm upgrade --install "${obs_args[@]}" --kube-context "$context" --wait --timeout "$timeout"
fi

# Migrations, as the Kind harness runs them: once, before any API replica. A Job's
# pod template is immutable, so the previous run's Job is replaced.
log "running space-db-migrate"
k -n "$namespace" delete job space-db-migrate --ignore-not-found >/dev/null
k -n "$namespace" apply -f - <<<"$migrate" >/dev/null
# Wait for whichever terminal condition comes first: a Job that exhausts its
# backoffLimit is Failed, never Complete, and would otherwise hold the install
# for the whole timeout.
k wait --for=condition=complete job/space-db-migrate --timeout="$timeout" -n "$namespace" >/dev/null 2>&1 &
complete_pid=$!
k wait --for=condition=failed job/space-db-migrate --timeout="$timeout" -n "$namespace" >/dev/null 2>&1 &
failed_pid=$!
while kill -0 "$complete_pid" 2>/dev/null && kill -0 "$failed_pid" 2>/dev/null; do
  sleep 2
done
if ! kill -0 "$complete_pid" 2>/dev/null && wait "$complete_pid"; then
  kill "$failed_pid" 2>/dev/null || true
  wait "$failed_pid" 2>/dev/null || true
else
  outcome="did not complete within $timeout"
  if ! kill -0 "$failed_pid" 2>/dev/null && wait "$failed_pid"; then outcome=failed; fi
  kill "$complete_pid" "$failed_pid" 2>/dev/null || true
  wait "$complete_pid" "$failed_pid" 2>/dev/null || true
  echo "install.sh: space-db-migrate $outcome:" >&2
  k logs job/space-db-migrate --all-containers --tail=50 -n "$namespace" >&2 || true
  exit 1
fi

# Plain calls rather than a loop over namerefs: `declare -n` needs bash 4.3,
# and on an older bash it would fail here, after the migrations ran.
release() {
  log "helm upgrade --install $1"
  helm upgrade --install "$@" --kube-context "$context" --wait --timeout "$timeout" \
    2> >(grep -v 'found symbolic link in path' >&2)
}
release "${worker_args[@]}"
release "${builder_args[@]}"
log "installed space and space-builder in $context/$namespace"
