#!/usr/bin/env bash
# Persist phase-labelled Fake Intake evidence from the local Kind cluster.
#
# Usage:
#   scripts/tests/capture-kind-fakeintake.sh prepare
#   # Run one bounded smoke window.
#   scripts/tests/capture-kind-fakeintake.sh capture after-datadog happy-path
#
# `prepare` drains Agent buffers before resetting Fake Intake twice. This keeps
# payloads emitted before the window from arriving after the reset and being
# misattributed to the next phase. Background Agent and Kubernetes telemetry is
# still expected; comparison summaries select only SPACE signals.
#
# When the workloads export traces through the native Datadog tracer, capture
# then fails, after writing everything, when it holds no SPACE trace or SPACE
# parented a span on remote context; see
# scripts/tests/observability/datadog_trace_chunks.py.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

FAKE_INTAKE_URL="${FAKE_INTAKE_URL:-http://localhost:8088}"
KIND_CONTEXT="${KIND_CONTEXT:-kind-space-local}"
NAMESPACE="${NAMESPACE:-space}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$REPO_ROOT/target/kind-telemetry-comparison}"
DRAIN_SECS="${DRAIN_SECS:-30}"
SETTLE_SECS="${SETTLE_SECS:-15}"

usage() {
  cat <<'EOF'
usage:
  capture-kind-fakeintake.sh prepare
  capture-kind-fakeintake.sh capture <phase> <window>

phase/window must use lower-case letters, digits, and hyphens. Capture refuses
to overwrite an existing directory.
EOF
}

require_tools() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || {
      echo "ERROR: required command not found: $tool" >&2
      exit 1
    }
  done
}

validate_label() {
  [[ "$1" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]] || {
    echo "ERROR: invalid comparison label: $1" >&2
    exit 1
  }
}

fakeintake_payloads() { # endpoint format output
  curl -fsS --get "$FAKE_INTAKE_URL/fakeintake/payloads" \
    --data-urlencode "endpoint=$1" \
    --data-urlencode "format=$2" \
    --output "$3"
}

prepare() {
  local prepared_at prepared_unix
  curl -fsS "$FAKE_INTAKE_URL/fakeintake/routestats" >/dev/null
  echo "draining Agent buffers for ${DRAIN_SECS}s"
  sleep "$DRAIN_SECS"
  curl -fsS "$FAKE_INTAKE_URL/fakeintake/flushPayloads" >/dev/null
  echo "settling for ${SETTLE_SECS}s before the final reset"
  sleep "$SETTLE_SECS"
  curl -fsS "$FAKE_INTAKE_URL/fakeintake/flushPayloads" >/dev/null
  mkdir -p "$OUTPUT_ROOT"
  prepared_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  prepared_unix="$(date -u +%s)"
  jq -n --arg prepared_at "$prepared_at" --argjson prepared_unix "$prepared_unix" \
    '{prepared_at: $prepared_at, prepared_unix: $prepared_unix}' \
    >"$OUTPUT_ROOT/.prepared-window.json"
  echo "Fake Intake is ready for a new comparison window"
}

capture() { # phase window
  local phase="$1" window="$2" output captured_at git_sha prepared_state prepared_at prepared_unix
  validate_label "$phase"
  validate_label "$window"
  output="$OUTPUT_ROOT/$phase/$window"
  if [[ -e "$output" ]]; then
    echo "ERROR: refusing to overwrite existing capture: $output" >&2
    exit 1
  fi
  prepared_state="$OUTPUT_ROOT/.prepared-window.json"
  if [[ ! -f "$prepared_state" ]]; then
    echo "ERROR: no prepared comparison window; run '$0 prepare' first" >&2
    exit 1
  fi
  prepared_at="$(jq -er '.prepared_at' "$prepared_state")"
  prepared_unix="$(jq -er '.prepared_unix' "$prepared_state")"
  mkdir -p "$output"

  echo "waiting ${DRAIN_SECS}s for the final Agent exports"
  sleep "$DRAIN_SECS"

  curl -fsS "$FAKE_INTAKE_URL/fakeintake/routestats" \
    --output "$output/route-stats.json"
  fakeintake_payloads /api/intake/metrics/v3/series json \
    "$output/metrics-series.json"
  fakeintake_payloads /api/beta/sketches json \
    "$output/metric-sketches.json"
  fakeintake_payloads /api/v2/logs json "$output/logs.json"
  fakeintake_payloads /api/v0.2/traces raw "$output/traces-raw.json"
  fakeintake_payloads /api/v0.2/stats raw "$output/trace-stats-raw.json"

  # DogStatsD timestamps identify the start of a ten-second aggregation bucket,
  # so the first in-window event may have a timestamp just before prepare.
  local metrics_window_start=$((prepared_unix / 10 * 10))
  jq --argjson window_start "$metrics_window_start" '[.payloads[].data[]
      | select(.metric | startswith("space."))
      | select(any(.points[]?; .timestamp >= $window_start))
      | {metric, tags: (.tags | sort)}]
      | unique
      | sort_by(.metric, .tags)' \
    "$output/metrics-series.json" >"$output/space-metric-series-shapes.json"
  jq --argjson window_start "$metrics_window_start" '[.payloads[].data[]
      | select(.metric | startswith("space."))
      | select(any(.dogsketches[]?; .ts >= $window_start))
      | {metric, tags: (.tags | sort)}]
      | unique
      | sort_by(.metric, .tags)' \
    "$output/metric-sketches.json" >"$output/space-metric-sketch-shapes.json"
  jq --argjson window_start_ms "$((prepared_unix * 1000))" '[.payloads[].data[]
      | select((.ddtags // "") | contains("kube_namespace:space"))
      | select((.timestamp // 0) >= $window_start_ms)
      | {service, status, ddsource,
         tags: ((.ddtags // "") | split(",")
           | map(select(test("^(env|version|service|kube_namespace|kube_container_name):")))
           | sort)}]
      | unique
      | sort_by(.service, .status, .ddsource, .tags)' \
    "$output/logs.json" >"$output/space-log-shapes.json"

  kubectl --context "$KIND_CONTEXT" -n "$NAMESPACE" get deploy,ds -o json \
    | jq '[.items[]
      | select((.metadata.name | test("^space-(api|cluster-controller|data-plane|internal-api)$"))
          or .metadata.labels["app.kubernetes.io/part-of"] == "space-node")
      | {kind, name: .metadata.name, uid: .metadata.uid,
         images: [.spec.template.spec.containers[].image],
         telemetry_env: [.spec.template.spec.containers[].env[]?
           | select(.name | test("^(SPACE_METRICS_SINK|SPACE_TRACE_EXPORTER|SPACE_TRACE_FILTER|SPACE_SERVICE_NAME|SPACE_ENV|SPACE_VERSION|SPACE_CLUSTER_ID|DD_ENV|DD_VERSION|DD_TAGS|OTEL_SERVICE_NAME|OTEL_RESOURCE_ATTRIBUTES|OTEL_TRACES_SAMPLER|OTEL_METRIC_EXPORT_INTERVAL|OTEL_EXPORTER_OTLP_(METRICS_|TRACES_)?(ENDPOINT|PROTOCOL))$"))
           | {name, value: (.value // .valueFrom)}] }]
      | sort_by(.name)' >"$output/workloads.json"

  captured_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  git_sha="$(git -C "$REPO_ROOT" rev-parse HEAD)"
  jq -n \
    --arg phase "$phase" \
    --arg window "$window" \
    --arg captured_at "$captured_at" \
    --arg prepared_at "$prepared_at" \
    --argjson prepared_unix "$prepared_unix" \
    --argjson metrics_window_start "$metrics_window_start" \
    --arg git_sha "$git_sha" \
    --arg context "$KIND_CONTEXT" \
    --arg namespace "$NAMESPACE" \
    --arg kube_system_uid "$(kubectl --context "$KIND_CONTEXT" get namespace kube-system -o jsonpath='{.metadata.uid}')" \
    --arg postgres_uid "$(kubectl --context "$KIND_CONTEXT" -n "$NAMESPACE" get pod space-postgres-0 -o jsonpath='{.metadata.uid}')" \
    --arg seed_job_uid "$(kubectl --context "$KIND_CONTEXT" -n "$NAMESPACE" get job -l app.kubernetes.io/name=space-seed-publish -o jsonpath='{.items[0].metadata.uid}')" \
    '{phase: $phase, window: $window,
      window_started_at: $prepared_at, window_started_unix: $prepared_unix,
      metrics_window_started_unix: $metrics_window_start,
      captured_at: $captured_at,
      git_sha: $git_sha, context: $context, namespace: $namespace,
      identities: {kube_system_uid: $kube_system_uid,
                   postgres_uid: $postgres_uid,
                   seed_job_uid: $seed_job_uid}}' >"$output/manifest.json"

  mv "$prepared_state" "$output/prepare.json"

  echo "$output"
  if jq -e '[.[].telemetry_env[] | select(.name == "SPACE_TRACE_EXPORTER"
      and .value != "off") | .value] | unique == ["datadog"]' \
    "$output/workloads.json" >/dev/null; then
    require_tools python3 zstd
    python3 "$SCRIPT_DIR/observability/datadog_trace_chunks.py" \
      "$output/traces-raw.json" "$output/space-trace-chunks.json"
  fi
}

require_tools curl git jq kubectl
case "${1:-}" in
  prepare)
    [[ $# -eq 1 ]] || { usage >&2; exit 2; }
    prepare
    ;;
  capture)
    [[ $# -eq 3 ]] || { usage >&2; exit 2; }
    capture "$2" "$3"
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
