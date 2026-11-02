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
# stack has settled, or fails with the log of any resource whose update failed.
# Tilt is restarted only when it serves a deleted cluster or another checkout,
# or was started with other values of the settings listed in TILT_SETTINGS
# below, since Tilt reads those once, at start. Before a running Tilt is
# stopped, this run's own refusals are checked: the timeout, storage mode,
# Rust build and profile, and parallelism, and the cluster's mounts, node
# image, layout, data, substrate and the nodes' vendor copies, against the
# vendor tree this run stages (assert_restartable), and the telemetry
# configuration and substrate request that the Tiltfile also refuses at
# load (the preflight below, kind_stack_assert_substrate_request). A
# load-time refusal not checked here would leave no watcher.
# Tilt keeps running afterwards and redeploys whatever changes in the
# checkout; teardown.sh stops it.
#
# Usage:
#   bash infra/space/kind/scripts/up.sh
#
# Env knobs:
#   SPACE_KIND_CLUSTER       kind cluster name (default: space-local)
#   SPACE_CONSOLE_PASSWORD   initial root@local password (default: password;
#                            only applied while the account is locked)
#   SPACE_BOOTSTRAP_TIMEOUT  seconds each wait may take: Tilt's start, the
#                            API, each Job, each rollout (the Datadog Agent's
#                            included) and node publication (default: 1800)
#   SPACE_KIND_DATADOG_MODE, SPACE_KIND_TELEMETRY_MODE, SPACE_KIND_OBSERVABILITY,
#   SPACE_KIND_OBSERVABILITY_CONFIG, SPACE_KIND_STORAGE_CREDENTIALS,
#   SPACE_KIND_RUST_BUILD, SPACE_KIND_SUBSTRATE, SPACE_LOOP_DEVICE_TARGET,
#   SPACE_KIND_PRUNE_SUPERSEDED_IMAGES,
#   SPACE_TILT_PRUNE_HOST_IMAGES, SPACE_TILT_MAX_PARALLEL_UPDATES,
#   SPACE_SEED_OUTPUT_DIR    see infra/space/kind/README.md
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
#   SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN
#                            optional, with SPACE_KIND_STORAGE_CREDENTIALS=static
#                            only; copied into the `space-aws-creds` Secret for
#                            the credential-manager authority's AWS integrations
#                            (sandbox account only)
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

# Validate before creating or changing any cluster resources, or stopping a
# running Tilt. The Tiltfile repeats the telemetry and password checks, but
# only after the cluster exists and, for the password, after the full build;
# the library refuses a bad timeout only once it is starting Tilt.
[[ "$BOOTSTRAP_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
  || die "SPACE_BOOTSTRAP_TIMEOUT must be a positive integer number of seconds, not '$BOOTSTRAP_TIMEOUT'"
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
# As the Tiltfile refuses it, but before a running Tilt is stopped or a
# cluster created for it.
kind_stack_assert_substrate_request
SKIP_BOOTSTRAP=0
KIND_STACK_DETACH=0

# The settings Tilt reads from its environment once, at start — in the
# Tiltfile, telemetry_config.py and the build scripts it runs — each with the
# default its reader applies, so unset, empty and an explicit default compare
# equal. Anything else (a refreshed token, a knob only this script reads) never
# restarts Tilt, which would interrupt whatever else is using the stack. The
# cluster name and host roots are left out: the bring-up checks those against
# the cluster itself.
TILT_SETTINGS=(
  SPACE_KIND_STORAGE_CREDENTIALS=exchange
  SPACE_KIND_RUST_BUILD=test-hooks
  SPACE_KIND_SUBSTRATE=kubernetes
  SPACE_KIND_OBSERVABILITY_CONFIG=
  SPACE_TILT_MAX_PARALLEL_UPDATES=8
  SPACE_LOOP_DEVICE_TARGET=
  SPACE_KIND_PRUNE_SUPERSEDED_IMAGES=1
  SPACE_TILT_PRUNE_HOST_IMAGES=1
  SPACE_SEED_OUTPUT_DIR="$HOME/.space-devbox/kind/seed/build"
  SPACE_KIND_RUST_ARTIFACTS=auto
  SPACE_KIND_RUST_PROFILE=release
  SPACE_KIND_BUILD_CACHE_REPOSITORY=
  # telemetry_config.py OPTIONS
  SPACE_KIND_DATADOG_MODE=off
  SPACE_KIND_DATADOG_UNBOUNDED=
  SPACE_KIND_DATADOG_SITE=
  SPACE_KIND_DATADOG_API_KEY_SECRET=
  SPACE_KIND_DATADOG_DURATION_MINUTES=0
  SPACE_KIND_DATADOG_ACKNOWLEDGE_SHARED_BACKEND=
  SPACE_KIND_TELEMETRY_MODE=datadog
  SPACE_KIND_CLUSTER_ID=kind-local
  SPACE_KIND_ENV=
  SPACE_KIND_VERSION=
  SPACE_KIND_PHASE=
  SPACE_KIND_OWNER=
  SPACE_KIND_SHIM_ENV=
  SPACE_KIND_SHIM_OTLP_ENDPOINT=
  SPACE_KIND_OTLP_METRICS_ENDPOINT=
  SPACE_KIND_OTLP_TRACES_ENDPOINT=
  SPACE_KIND_OBSERVABILITY=auto
)
# Read by the Tiltfile's space-aws-creds resource, which exists with static
# storage credentials only.
STATIC_TILT_SETTINGS=(
  AWS_ACCESS_KEY_ID=
  AWS_SECRET_ACCESS_KEY=
  AWS_SESSION_TOKEN=
  SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN=
)

# Prints the first setting whose value differs between this environment and
# the one Tilt $1 started with; fails when they agree or Tilt's is unreadable.
tilt_settings_differ() {
  local entry name default now was settings=("${TILT_SETTINGS[@]}")
  local -A started=()
  [[ -r "/proc/$1/environ" ]] || return 1
  while IFS= read -r -d '' entry; do
    started[${entry%%=*}]="${entry#*=}"
  done <"/proc/$1/environ"
  if [[ "$(space_kind_storage_credentials 2>/dev/null)" == static ]]; then
    settings+=("${STATIC_TILT_SETTINGS[@]}")
  fi
  for entry in "${settings[@]}"; do
    name="${entry%%=*}"
    default="${entry#*=}"
    now="${!name:-$default}"
    was="${started[$name]:-$default}"
    [[ "$now" == "$was" ]] || { printf '%s\n' "$name"; return 0; }
  done
  return 1
}

# Everything the bring-up would refuse about this run's settings or the
# existing cluster, so a refusal leaves a running Tilt running rather than
# stopping it first.
assert_restartable() {
  space_kind_storage_credentials >/dev/null || exit 1
  local rust_build
  rust_build="$(space_kind_rust_build)" || exit 1
  # build-rust-artifacts.sh's refusal, which the restarted Tilt would reach
  # only after this one had been stopped.
  [[ "$rust_build" != release || "${SPACE_KIND_RUST_PROFILE:-release}" == release ]] \
    || die "SPACE_KIND_RUST_BUILD=release builds the release profile; unset SPACE_KIND_RUST_PROFILE (got '$SPACE_KIND_RUST_PROFILE')"
  [[ "${SPACE_TILT_MAX_PARALLEL_UPDATES:-8}" =~ ^[1-9][0-9]*$ ]] \
    || die "SPACE_TILT_MAX_PARALLEL_UPDATES must be a positive integer"
  kind_stack_assert_cluster_mounts
  kind_stack_assert_postgres_data
  # As the bring-up does, so the nodes' vendor copies are compared with the
  # tree this run would stage, not the one it would replace.
  kind_stack_stage_vendor
  kind_stack_assert_existing_cluster
}

kind_stack_assert_smoke_prereqs
if bootstrap_kind_stack__tilt_running \
    && tilt_pid="$(bootstrap_kind_stack__tilt_listener_pid)" \
    && changed="$(tilt_settings_differ "$tilt_pid")"; then
  assert_restartable
  log "tilt (pid $tilt_pid) started with another $changed; restarting it with this run's settings"
  bootstrap_kind_stack__stop_stale_tilt "$tilt_pid" || exit 1
fi
# A fresh Tilt starts after the bring-up reaps the port-forward supervisors a
# scripted bring-up left behind (kind_stack_reap_stale_port_forwards), so its
# own forwards can bind.
bootstrap_kind_stack

if [[ "$DATADOG_MODE" != off ]]; then
  log "verifying the Datadog Agent ($DATADOG_MODE)"
  python3 "$KIND_DIR/scripts/datadog_backend.py" verify --timeout "$BOOTSTRAP_TIMEOUT" >/dev/null
fi

tilt_log="${SPACE_TILT_LOG:-${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}/tilt-${KIND_CLUSTER}.log}"
served_pid="$(bootstrap_kind_stack__served_tilt_pid)"
cat <<MSG

[up] kind cluster '$KIND_CLUSTER' is up; Tilt${served_pid:+ (pid $served_pid)} keeps it in sync with this checkout.

  API:        $API_URL
  Data-plane: $DATA_PLANE_URL
  UI:         http://localhost:3000
  Tilt UI:    http://localhost:10350 (log: $tilt_log)
  Datadog:    $DATADOG_MODE
  Telemetry:  $TELEMETRY_MODE
  Rust build: $(space_kind_rust_build)
  Substrate:  $(space_kind_substrate)
  Login:      root@local

  Smoke:      bash scripts/tests/smoke-kind-e2e.sh
  ECW build:  SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots
  Teardown:   bash $KIND_DIR/scripts/teardown.sh

MSG
