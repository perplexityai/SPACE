# Shared guards and Helm plumbing for the opt-in observability releases.
# Source it, then call release_init with the release's short name, e.g. traces.
#
# The cluster guard is the safety boundary for every one of these helpers: it
# refuses anything that is not the verified local Docker Kind bench, and it
# refuses a namespace this release does not own. It lives here so the four
# helpers cannot drift from each other.

# Refuse any cluster that is not the loopback API of this Docker Kind bench.
# Checked three ways: the context's server must be loopback, the control-plane
# container must carry Kind's cluster label, and the published port must be the
# one the context addresses. A context name alone is not identity.
guard_cluster() {
  local server
  server=$("${kube[@]}" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  [[ $server =~ ^https://127\.0\.0\.1:[0-9]+$ ]] ||
    { echo 'Refusing non-loopback cluster' >&2; exit 1; }
  [[ $(docker inspect "$control_plane" \
    --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}') == "$cluster" ]]
  [[ https://$(docker port "$control_plane" 6443/tcp) == "$server" ]] ||
    { echo 'Context does not address this Docker Kind cluster' >&2; exit 1; }
  "${kube[@]}" get node "$control_plane" >/dev/null
}

# Create the task namespace on `up`, or refuse one this release does not own.
# Creating from the manifest is atomic, so a race cannot adopt someone else's
# namespace after the fact.
guard_namespace() {
  if ! "${kube[@]}" get namespace "$namespace" >/dev/null 2>&1; then
    [[ $action == up ]] || { echo 'Task namespace does not exist' >&2; exit 1; }
    "${kube[@]}" create -f "$harness_dir/namespace.yaml"
  fi
  [[ $("${kube[@]}" get namespace "$namespace" -o jsonpath='{.metadata.labels.space-local-owner}') == "$owner" ]] ||
    { echo 'Refusing unowned namespace' >&2; exit 1; }
  if [[ $action == up ]]; then
    # Converge security labels on an existing owned namespace as well as a new
    # one. Reads and cleanup must not change namespace policy.
    "${kube[@]}" apply -f "$harness_dir/namespace.yaml" >/dev/null
  fi
}

# Sets context, namespace, owner, harness_dir, chart_dir, release and kube for
# the caller, then runs both guards and vendors the chart's library dependency.
# $1 is the release short name, e.g. traces.
release_init() {
  local name=$1
  # The chart directory, namespace and release are all the short signal name.
  # The space-kind-telemetry OTLP endpoints resolve these namespaces by name;
  # they move in the same commit as this.
  owner=$name
  # bootstrap.sh and the Tiltfile both support a non-default bench through
  # SPACE_KIND_CLUSTER, and they now invoke these helpers automatically. The
  # identity checks follow that selection instead of assuming space-local.
  cluster=${SPACE_KIND_CLUSTER:-space-local}
  control_plane=$cluster-control-plane
  context=kind-$cluster
  namespace=space-$owner
  release=space-$owner
  harness_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[1]}")" && pwd)
  chart_dir=$(cd -- "$harness_dir/../../observability/charts/$name" && pwd)
  kube=(kubectl --context "$context")
  guard_cluster
  guard_namespace
  # The chart consumes the common library chart; Helm needs it vendored
  # into charts/ before lint, template or upgrade. Cheap and idempotent.
  helm dependency build "$chart_dir" >/dev/null
}

# Accept only the Helm flags these helpers support, so a stray argument cannot
# reach Helm unreviewed. --reuse-values becomes --reset-then-reuse-values: the
# plain form keeps stale defaults from an older chart revision.
helm_options() {
  helm_options=()
  while (($#)); do
    case "$1" in
      --reuse-values) helm_options+=(--reset-then-reuse-values); shift ;;
      --set|--set-string|-f|--values)
        (($# >= 2)) || { echo "Missing value for $1" >&2; exit 2; }
        helm_options+=("$1" "$2"); shift 2 ;;
      *) echo "Unsupported Helm option: $1" >&2; exit 2 ;;
    esac
  done
}

# Install or upgrade with the Kind overlay always applied last-but-one, so a
# caller's --set still wins. $1 is the wait timeout; later arguments are extra
# Helm flags appended after the parsed options.
helm_upgrade() {
  local timeout=$1; shift
  helm upgrade --install "$release" "$chart_dir" --kube-context "$context" \
    --namespace "$namespace" --wait --timeout "$timeout" \
    -f "$chart_dir/profiles/kind.yaml" "${helm_options[@]}" "$@"
}

# Namespace deletion destroys this release's local data, so require the caller
# to say so explicitly. $1 is the refusal message.
confirm_cleanup() {
  [[ ${1:-} == --confirm-delete-local-data ]] || { echo "$2" >&2; exit 1; }
}
