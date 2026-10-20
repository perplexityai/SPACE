#!/usr/bin/env bash
# up.sh — bring up the full production-shaped SPACE stack on a local kind
# cluster under Tilt, and return once it is ready.
#
# Creates the multi-worker `space-local` cluster from
# infra/space/kind/kind-cluster.yaml.tmpl when it does not exist, starts
# `tilt up` detached in its own session (the repo Tiltfile builds every image
# and applies every chart), and waits until Tilt has nothing left to build,
# every workload has rolled, the seed template is published and every SPACE
# node is published and schedulable. The bring-up itself is
# scripts/tests/lib/bootstrap-kind-stack.sh, the one the smoke suites use.
#
# Idempotent: on a live stack it reuses the running Tilt and returns once the
# stack has settled. Tilt is restarted only when it serves a deleted cluster or
# another checkout, or was started with other settings than this run's (the
# knobs below), since Tilt reads those once, at start. Tilt keeps running
# afterwards and redeploys whatever changes in the checkout; teardown.sh
# stops it.
#
# Usage:
#   bash infra/space/kind/scripts/up.sh
#
# Env knobs:
#   SPACE_KIND_CLUSTER       kind cluster name (default: space-local)
#   SPACE_CONSOLE_PASSWORD   initial root@local password (default: password;
#                            only applied while the account is locked)
#   SPACE_BOOTSTRAP_TIMEOUT  seconds each bring-up stage may take (default: 1800)
#   SPACE_KIND_DATADOG_MODE, SPACE_KIND_TELEMETRY_MODE, SPACE_KIND_OBSERVABILITY,
#   SPACE_KIND_OBSERVABILITY_CONFIG, SPACE_KIND_STORAGE_CREDENTIALS,
#   SPACE_LOOP_DEVICE_TARGET, SPACE_KIND_PRUNE_SUPERSEDED_IMAGES,
#   SPACE_TILT_PRUNE_HOST_IMAGES, SPACE_TILT_MAX_PARALLEL_UPDATES
#                            see infra/space/kind/README.md
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
#   SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN
#                            optional; copied into the `space-aws-creds` Secret
#                            for the credential-manager authority's AWS
#                            integrations (sandbox account only)
#   SPACE_KIND_MIN_FREE_GIB  warn before bring-up when the Docker data root or
#                            SPACE_KIND_DATA_ROOT has less free (default: 30)
#   SPACE_VENDOR_ROOT, SPACE_KIND_DATA_ROOT
#                            host paths the cluster bind-mounts; see
#                            infra/space/kind/scripts/lib/kind-host-paths.sh
set -euo pipefail
set +x

case "${1:-}" in
  "") ;;
  -h|--help)
    sed -n '2,/^set/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *)
    printf 'unknown argument: %s\n' "$1" >&2
    exit 2
    ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
KIND_DIR="$REPO_ROOT/infra/space/kind"
KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
API_URL=http://localhost:8080
DATA_PLANE_URL=http://localhost:8090
BOOTSTRAP_TIMEOUT="${SPACE_BOOTSTRAP_TIMEOUT:-1800}"

log() { printf '[up] %s\n' "$*"; }
die() { printf '[up] ERROR: %s\n' "$*" >&2; exit 1; }

# Validate before creating or changing any cluster resources. The Tiltfile
# repeats both checks, but only after the cluster exists and, for the password,
# after the full build.
TELEMETRY_JSON="$(python3 "$KIND_DIR/scripts/datadog_backend.py" preflight --for-deployment)"
DATADOG_MODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["backend"])' <<<"$TELEMETRY_JSON")"
TELEMETRY_MODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["mode"])' <<<"$TELEMETRY_JSON")"
# The rules provision-local-console.sh applies when Tilt runs it.
password="${SPACE_CONSOLE_PASSWORD-password}"
[[ -n "$password" ]] || die "SPACE_CONSOLE_PASSWORD must not be empty"
[[ "$password" != *$'\n'* ]] || die "SPACE_CONSOLE_PASSWORD must not contain a newline"
(( $(printf '%s' "$password" | wc -c) <= 72 )) || die "SPACE_CONSOLE_PASSWORD must be at most 72 bytes"
unset password

for bin in docker kind kubectl helm openssl tilt; do
  command -v "$bin" >/dev/null 2>&1 || die "required tool not on PATH: $bin (install-prereqs.sh provides it)"
done
[[ "$(helm version --short 2>/dev/null)" == v3.* ]] \
  || die "helm 3 required (found: $(helm version --short 2>/dev/null))"
docker info >/dev/null 2>&1 \
  || die "docker daemon unreachable via DOCKER_HOST=${DOCKER_HOST:-(unset)}; run start-dind.sh first or point DOCKER_HOST at a working daemon"

# Warns, never refuses: a bring-up that runs either disk out fails as an image
# extraction or build error far from the cause.
SPACE_KIND_CLUSTER="$KIND_CLUSTER" bash "$KIND_DIR/scripts/check-disk-headroom.sh"

# shellcheck source=../../../../scripts/tests/lib/bootstrap-kind-stack.sh
source "$REPO_ROOT/scripts/tests/lib/bootstrap-kind-stack.sh"
SKIP_BOOTSTRAP=0
KIND_STACK_DETACH=0

# The settings the Tiltfile and its helpers read from the environment. The
# cluster name and host roots are left out: the bring-up checks those against
# the cluster itself, and sets them in Tilt's environment when unset here.
tilt_setting() {
  [[ "$1" =~ ^(SPACE_KIND_|SPACE_TILT_(MAX_PARALLEL_UPDATES|PRUNE_HOST_IMAGES)$|SPACE_LOOP_DEVICE_TARGET$|AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)$|SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN$) ]] \
    && [[ ! "$1" =~ ^SPACE_KIND_(CLUSTER|DATA_ROOT|CONFIG)$ ]]
}

# Prints the first setting whose value differs between this environment and
# the one Tilt $1 started with; fails when they agree or Tilt's is unreadable.
tilt_settings_differ() {
  local entry name
  local -A started=()
  [[ -r "/proc/$1/environ" ]] || return 1
  while IFS= read -r -d '' entry; do
    name="${entry%%=*}"
    tilt_setting "$name" && started[$name]="${entry#*=}"
  done <"/proc/$1/environ"
  for name in "${!started[@]}"; do
    [[ -v "$name" && "${!name}" == "${started[$name]}" ]] || { printf '%s\n' "$name"; return 0; }
  done
  while IFS= read -r name; do
    tilt_setting "$name" && [[ ! -v "started[$name]" ]] && { printf '%s\n' "$name"; return 0; }
  done < <(compgen -e)
  return 1
}

kind_stack_assert_smoke_prereqs
if bootstrap_kind_stack__tilt_running \
    && tilt_pid="$(bootstrap_kind_stack__tilt_listener_pid)" \
    && changed="$(tilt_settings_differ "$tilt_pid")"; then
  log "tilt (pid $tilt_pid) started with another $changed; restarting it with this run's settings"
  bootstrap_kind_stack__stop_stale_tilt "$tilt_pid" || exit 1
fi
bootstrap_kind_stack

if [[ "$DATADOG_MODE" != off ]]; then
  log "verifying the Datadog Agent ($DATADOG_MODE)"
  python3 "$KIND_DIR/scripts/datadog_backend.py" verify >/dev/null
fi

tilt_log="${SPACE_TILT_LOG:-${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}/tilt-${KIND_CLUSTER}.log}"
cat <<MSG

[up] kind cluster '$KIND_CLUSTER' is up; Tilt (pid $(bootstrap_kind_stack__tilt_pid)) keeps it in sync with this checkout.

  API:        $API_URL
  Data-plane: $DATA_PLANE_URL
  UI:         http://localhost:3000
  Tilt UI:    http://localhost:10350 (log: $tilt_log)
  Datadog:    $DATADOG_MODE
  Telemetry:  $TELEMETRY_MODE
  Login:      root@local

  Smoke:      bash scripts/tests/smoke-kind-e2e.sh
  ECW build:  SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots
  Teardown:   bash $KIND_DIR/scripts/teardown.sh

MSG
