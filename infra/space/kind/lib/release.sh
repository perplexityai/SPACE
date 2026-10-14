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
  local found
  found=$("${kube[@]}" get namespace "$namespace" --ignore-not-found -o name)
  if [[ -z $found ]]; then
    # Quiescing an optional collector must not create a namespace or a backend.
    if [[ $action == pause || $action == preflight ]]; then namespace_absent=true; return; fi
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
  [[ ${namespace_absent:-false} == false ]] || return 0
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
  local args=(upgrade --install "$release" "$chart_dir" --kube-context "$context"
    --namespace "$namespace" --wait --timeout "$timeout"
    -f "$chart_dir/profiles/kind.yaml" "${helm_options[@]}" "$@")
  local listing state stored proposed comparison target healthy fresh
  listing=$(helm list --all --filter "^$release$" --kube-context "$context" \
    --namespace "$namespace" -o json) || return 1
  state=$(printf '%s' "$listing" | python3 -c '
import json,re,sys
release, namespace = sys.argv[1:]
entries = json.load(sys.stdin)
if not entries:
    print("absent")
elif (len(entries) == 1 and entries[0].get("name") == release
      and entries[0].get("namespace") == namespace
      and re.fullmatch(re.escape(release) + r"-[0-9]+\.[0-9]+\.[0-9]+", entries[0].get("chart", ""))):
    print(entries[0].get("status", "unknown"))
else:
    sys.exit("Refusing an unowned Helm release in the selected namespace")
' "$release" "$namespace") || return 1
  # An existing release records its Kind overlay and caller overrides. Keep
  # those on reuse, but a first install must still receive the Kind overlay.
  if [[ "$state" != absent && " ${helm_options[*]} " == *" --reset-then-reuse-values "* ]]; then
    args=(upgrade --install "$release" "$chart_dir" --kube-context "$context"
      --namespace "$namespace" --wait --timeout "$timeout" "${helm_options[@]}" "$@")
  fi
  if [[ "$state" == deployed ]]; then
    stored=$(helm get manifest "$release" --kube-context "$context" --namespace "$namespace") || return 1
    proposed=$(helm "${args[@]}" --dry-run=client -o json) || return 1
    # Compare complete rendered resources, ignoring only Helm document framing.
    # Keep manifests on stdin rather than exposing them in process arguments.
    comparison=$(printf '%s\0%s' "$stored" "$proposed" | python3 -c '
import collections,json,re,sys
old, proposal = sys.stdin.read().split("\0", 1)
def documents(value):
    return collections.Counter(
        re.sub(r"\A# Source: [^\n]*\n", "", part.strip())
        for part in re.split(r"(?m)^---\s*$", value) if part.strip())
desired = documents(json.loads(proposal)["manifest"])
if documents(old) != desired:
    print("different")
else:
    targets = []
    for document in desired:
        kind = re.search(r"(?m)^kind: (Deployment|DaemonSet|StatefulSet)$", document)
        if kind:
            name = re.search(r"(?m)^metadata:\n  name: ([^\n]+)$", document)
            if not name:
                print("different")
                sys.exit(0)
            targets.append(kind[1].lower() + "/" + name[1])
    print("same")
    print("\n".join(targets))
') || return 1
    if [[ "${comparison%%$'\n'*}" == same ]]; then
      healthy=true
      while IFS= read -r target; do
        [[ -n "$target" && "$target" != same ]] || continue
        if ! "${kube[@]}" -n "$namespace" rollout status "$target" --timeout=1s >/dev/null 2>&1; then
          healthy=false
          break
        fi
      done <<<"$comparison"
      fresh=$(helm list --all --filter "^$release$" --kube-context "$context" \
        --namespace "$namespace" -o json) || return 1
      [[ "$fresh" == "$listing" ]] || {
        echo "Helm release changed during reconciliation; retry without a competing operator" >&2
        return 1
      }
      if [[ "$healthy" == true ]]; then
        echo "$release: rendered resources unchanged"
        return 0
      fi
    fi
    fresh=$(helm list --all --filter "^$release$" --kube-context "$context" \
      --namespace "$namespace" -o json) || return 1
    [[ "$fresh" == "$listing" ]] || {
      echo "Helm release changed during reconciliation; retry without a competing operator" >&2
      return 1
    }
  fi
  # Failed releases are retried normally. Helm retains its pending-operation
  # lock; this helper never deletes release history or data to bypass it.
  helm "${args[@]}"
}

# Namespace deletion destroys this release's local data, so require the caller
# to say so explicitly. $1 is the refusal message.
confirm_cleanup() {
  [[ ${1:-} == --confirm-delete-local-data ]] || { echo "$2" >&2; exit 1; }
}
