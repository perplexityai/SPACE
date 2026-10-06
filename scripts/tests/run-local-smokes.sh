#!/usr/bin/env bash
# run-local-smokes.sh — run SPACE smoke groups against the local kind + Tilt
# stack and capture each group's outcome/signature.
#
# This does NOT try to make the smokes pass — the point is to run them against a
# laptop stack (with real object-store-backed snapshots) and reproduce/record the
# current failure signatures.
#
# What it does:
#   1. ensures port-forwards to the local space-api, data-plane and object-store Services
#   2. logs in as the seeded root@local user to mint a session token
#   3. seeds a local-dev E2B API key into Postgres (idempotent) for the ECW smokes
#   4. runs the requested smoke groups with local env, teeing logs under a results dir
#   5. prints a one-line PASS/FAIL summary per smoke with the captured signature
#
# Exit code: 0 if every smoke that ran PASSed, nonzero if any FAILed (SKIPs do
# not count). The per-smoke PASS/FAIL table still prints either way, so it works
# both as a signature printer and as a CI-style verifier.
#
# Usage:
#   scripts/tests/run-local-smokes.sh [s3-roundtrip|native|ecw-identity|all]
#
#   s3-roundtrip drives the real volume-manager → object-store push/pull (no api needed).
#   It first ensures the snapshots bucket exists (idempotent), so an object-store pod
#   recreation that wiped the hostPath-backed bucket can't fail the round-trip.
#
# Env knobs:
#   KCTX                 kube context           (default kind-space-local)
#   NS                   namespace              (default space)
#   SPACE_API_URL        api base url           (default http://127.0.0.1:18080;
#                        if set explicitly it is trusted as-is, otherwise the
#                        runner port-forwards $KCTX's svc/space-api to it)
#   LOCAL_API_PORT       local port for the self-managed forward (default 18080)
#   SPACE_DATA_PLANE_URL data-plane base url    (default http://127.0.0.1:18090;
#                        if set explicitly it is trusted as-is, otherwise the
#                        runner port-forwards $KCTX's svc/space-data-plane)
#   LOCAL_DATA_PLANE_PORT local port for data-plane forward (default 18090)
#   LOCAL_EMAIL/PASSWORD seeded login           (default root@local / SPACE_CONSOLE_PASSWORD or password)
#   E2B_API_KEY          ECW api key to seed+use (default local_dev_key)
#   E2B_DOMAIN           ECW host suffix         (default local.e2b.test)
#   RESULTS_DIR          where logs land         (default /tmp/space-local-smokes)
#   SNAPSHOT_BUCKET      object-store bucket to ensure (default snapshots)
#   OBJECT_STORE_ENDPOINT  host S3 endpoint for the bucket ensure (default http://127.0.0.1:18333;
#                        if set explicitly it is trusted as-is, otherwise the
#                        runner port-forwards $KCTX's svc/object-store to it)
#   LOCAL_OBJECT_STORE_PORT local port for the object-store forward (default 18333)
set -Eeuo pipefail

WHICH="${1:-all}"
KCTX="${KCTX:-kind-space-local}"
NS="${NS:-space}"
# Local port for the runner's self-managed port-forward. Deliberately NOT 8080:
# a parallel `tilt up` for a second kind cluster (e.g. a sibling worktree) squats
# 8080, and the old "is something already on 8080?" trust check would silently run
# the smokes against that other cluster's api. 18080 is the runner's own lane.
LOCAL_API_PORT="${LOCAL_API_PORT:-18080}"
LOCAL_DATA_PLANE_PORT="${LOCAL_DATA_PLANE_PORT:-18090}"
LOCAL_OBJECT_STORE_PORT="${LOCAL_OBJECT_STORE_PORT:-18333}"
# Track whether the caller pinned the api url explicitly. If they did, we trust it
# and only health-check it; if not, we always stand up our own forward to $KCTX so
# the smokes can't accidentally talk to a foreign cluster's api.
if [[ -n "${SPACE_API_URL:-}" ]]; then SPACE_API_URL_EXPLICIT=1; else SPACE_API_URL_EXPLICIT=0; fi
SPACE_API_URL="${SPACE_API_URL:-http://127.0.0.1:${LOCAL_API_PORT}}"
if [[ -n "${SPACE_DATA_PLANE_URL:-}" ]]; then SPACE_DATA_PLANE_URL_EXPLICIT=1; else SPACE_DATA_PLANE_URL_EXPLICIT=0; fi
SPACE_DATA_PLANE_URL="${SPACE_DATA_PLANE_URL:-http://127.0.0.1:${LOCAL_DATA_PLANE_PORT}}"
LOCAL_EMAIL="${LOCAL_EMAIL:-root@local}"
LOCAL_PASSWORD="${LOCAL_PASSWORD:-${SPACE_CONSOLE_PASSWORD:-password}}"
E2B_API_KEY="${E2B_API_KEY:-local_dev_key}"
E2B_DOMAIN="${E2B_DOMAIN:-local.e2b.test}"
RESULTS_DIR="${RESULTS_DIR:-/tmp/space-local-smokes}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PF_PIDS=()
# Count of smokes that FAILed (not SKIPs/PASSes). The runner exits nonzero when
# any smoke fails so it can be used as a verifier, not just a signature-capture
# printer.
FAILURES=0
# Object-store bucket-ensure knobs (mirror the space-object-store chart defaults).
# Like the api url, an explicit endpoint is trusted; otherwise the runner
# forwards its own port rather than Tilt's 8333, which may be another cluster's.
if [[ -n "${OBJECT_STORE_ENDPOINT:-}" ]]; then OBJECT_STORE_ENDPOINT_EXPLICIT=1; else OBJECT_STORE_ENDPOINT_EXPLICIT=0; fi
OBJECT_STORE_ENDPOINT="${OBJECT_STORE_ENDPOINT:-http://127.0.0.1:${LOCAL_OBJECT_STORE_PORT}}"
OBJECT_STORE_SECRET="${OBJECT_STORE_SECRET:-space-object-store}"
SNAPSHOT_BUCKET="${SNAPSHOT_BUCKET:-snapshots}"
mkdir -p "$RESULTS_DIR"

# Resolve a wall-clock timeout command. `timeout` ships with GNU coreutils
# (Linux, or macOS via `brew install coreutils` — sometimes as `gtimeout`).
# Stock macOS has neither, so fall back to running uncapped rather than failing.
if command -v timeout >/dev/null 2>&1; then TIMEOUT_CMD=(timeout)
elif command -v gtimeout >/dev/null 2>&1; then TIMEOUT_CMD=(gtimeout)
else TIMEOUT_CMD=(); fi

# run_capped <seconds> <cmd...> — runs the command under a timeout when one is
# available, otherwise runs it directly. Env vars for the command should be
# passed via an explicit `env VAR=val ...` so they survive either path.
run_capped() {
  local secs="$1"; shift
  if ((${#TIMEOUT_CMD[@]})); then "${TIMEOUT_CMD[@]}" "$secs" "$@"; else "$@"; fi
}

log()  { printf '[local-smokes] %s\n' "$*"; }
die()  { printf '[local-smokes] ERROR: %s\n' "$*" >&2; exit 1; }

sha256_hex() {
  if command -v sha256sum >/dev/null 2>&1; then printf '%s' "$1" | sha256sum | awk '{print $1}';
  else printf '%s' "$1" | shasum -a 256 | awk '{print $1}'; fi
}

cleanup() {
  local pid
  for pid in "${PF_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

ensure_port_forward() {
  # When the caller pinned SPACE_API_URL we trust it and only health-check —
  # they own routing to the right cluster. Otherwise we always stand up our own
  # forward to $KCTX rather than reusing whatever happens to be listening on the
  # local port (which may belong to another cluster's `tilt up`).
  if [[ "$SPACE_API_URL_EXPLICIT" == "1" ]]; then
    if curl -fsS -o /dev/null "${SPACE_API_URL%/}/healthz" 2>/dev/null; then
      log "using caller-provided api at $SPACE_API_URL"
      return
    fi
    die "caller-provided SPACE_API_URL ($SPACE_API_URL) is not reachable"
  fi
  log "starting port-forward svc/space-api ${LOCAL_API_PORT}:8080 (context $KCTX)"
  kubectl --context "$KCTX" -n "$NS" port-forward "svc/space-api" "${LOCAL_API_PORT}:8080" \
    >"$RESULTS_DIR/port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  for _ in $(seq 1 30); do
    curl -fsS -o /dev/null "${SPACE_API_URL%/}/healthz" 2>/dev/null && { log "api up at $SPACE_API_URL"; return; }
    sleep 1
  done
  die "space-api did not become reachable at $SPACE_API_URL (context $KCTX)"
}

ensure_data_plane_port_forward() {
  if [[ "$SPACE_DATA_PLANE_URL_EXPLICIT" == "1" ]]; then
    if curl -fsS -o /dev/null "${SPACE_DATA_PLANE_URL%/}/healthz" 2>/dev/null; then
      log "using caller-provided data-plane at $SPACE_DATA_PLANE_URL"
      return
    fi
    die "caller-provided SPACE_DATA_PLANE_URL ($SPACE_DATA_PLANE_URL) is not reachable"
  fi
  log "starting port-forward svc/space-data-plane ${LOCAL_DATA_PLANE_PORT}:8090 (context $KCTX)"
  kubectl --context "$KCTX" -n "$NS" port-forward "svc/space-data-plane" "${LOCAL_DATA_PLANE_PORT}:8090" \
    >"$RESULTS_DIR/data-plane-port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  for _ in $(seq 1 30); do
    curl -fsS -o /dev/null "${SPACE_DATA_PLANE_URL%/}/healthz" 2>/dev/null && { log "data-plane up at $SPACE_DATA_PLANE_URL"; return; }
    sleep 1
  done
  die "space-data-plane did not become reachable at $SPACE_DATA_PLANE_URL (context $KCTX)"
}

ensure_object_store_port_forward() {
  if [[ "$OBJECT_STORE_ENDPOINT_EXPLICIT" == "1" ]]; then
    if curl -fsS -o /dev/null "${OBJECT_STORE_ENDPOINT%/}/healthz" 2>/dev/null; then
      log "using caller-provided object store at $OBJECT_STORE_ENDPOINT"
      return
    fi
    die "caller-provided OBJECT_STORE_ENDPOINT ($OBJECT_STORE_ENDPOINT) is not reachable"
  fi
  log "starting port-forward svc/object-store ${LOCAL_OBJECT_STORE_PORT}:8333 (context $KCTX)"
  kubectl --context "$KCTX" -n "$NS" port-forward "svc/object-store" "${LOCAL_OBJECT_STORE_PORT}:8333" \
    >"$RESULTS_DIR/object-store-port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  for _ in $(seq 1 30); do
    curl -fsS -o /dev/null "${OBJECT_STORE_ENDPOINT%/}/healthz" 2>/dev/null && { log "object store up at $OBJECT_STORE_ENDPOINT"; return; }
    sleep 1
  done
  die "object-store did not become reachable at $OBJECT_STORE_ENDPOINT (context $KCTX)"
}

login_token() {
  curl -fsS -X POST "${SPACE_API_URL%/}/auth/v1/sessions" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$LOCAL_EMAIL\",\"password\":\"$LOCAL_PASSWORD\"}" | jq -r .token
}

seed_e2b_api_key() {
  local hash; hash="$(sha256_hex "$E2B_API_KEY")"
  log "seeding local-dev E2B api key (hash ${hash:0:12}…)"
  # space-postgres is a single-pod StatefulSet; address the pod directly.
  kubectl --context "$KCTX" -n "$NS" exec -i space-postgres-0 -- \
    psql -U spaceuser -d spacedb -v ON_ERROR_STOP=1 >/dev/null <<SQL || die "failed to seed E2B api key"
INSERT INTO api_keys (org_id, name, key_hash, created_by)
VALUES ('00000000-0000-0000-0000-000000000001', 'local-dev-smoke', '$hash',
        '00000000-0000-0000-0000-000000000010')
ON CONFLICT (key_hash) DO NOTHING;
SQL
}

summarize() {
  local name="$1" code="$2" logf="$3" sig
  # `|| true`: grep exits 1 when nothing matches (a passing smoke, or output with
  # no error keyword); without it the bare assignment would trip `set -e` and
  # abort the runner before printing the result.
  sig="$(grep -aiE 'ERROR|not implemented|HTTP [45][0-9][0-9]|failed|assert|Traceback' "$logf" 2>/dev/null | tail -1 || true)"
  if [[ "$code" == "0" ]]; then
    printf 'PASS  %-14s (log: %s)\n' "$name" "$logf"
  else
    printf 'FAIL  %-14s exit=%s :: %s\n' "$name" "$code" "${sig:-<no signature captured>}"
    FAILURES=$((FAILURES + 1))
  fi
}

# Re-create the snapshots bucket idempotently before the S3 round-trip. The
# object store's bucket-init Job creates the buckets once; with versitygw's
# POSIX backend they are directories on the infra node's hostPath and survive
# pod restarts, but a recreated node (or a wiped hostPath) loses them and
# nothing re-creates them — a fresh round-trip would then fail "bucket not
# found". Goes through the S3 API on the runner's object-store port-forward with
# the chart's own credentials, and drops any caller AWS session or profile so
# they cannot override them; an existing bucket is left alone, so this is safe to
# run every time.
ensure_snapshot_bucket() {
  local key secret
  if ! command -v aws >/dev/null 2>&1; then
    log "WARN: aws CLI not found; skipping bucket ensure"
    return
  fi
  key="$(kubectl --context "$KCTX" -n "$NS" get secret "$OBJECT_STORE_SECRET" \
    -o jsonpath='{.data.SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  secret="$(kubectl --context "$KCTX" -n "$NS" get secret "$OBJECT_STORE_SECRET" \
    -o jsonpath='{.data.SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [[ -z "$key" || -z "$secret" ]]; then
    log "WARN: no $OBJECT_STORE_SECRET credentials; skipping bucket ensure"
    return
  fi
  log "ensuring object-store bucket '$SNAPSHOT_BUCKET' exists"
  local s3=(env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE
    AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null
    AWS_ACCESS_KEY_ID="$key" AWS_SECRET_ACCESS_KEY="$secret" AWS_DEFAULT_REGION=us-east-1
    aws --no-cli-pager --endpoint-url "$OBJECT_STORE_ENDPOINT" s3api)
  if "${s3[@]}" head-bucket --bucket "$SNAPSHOT_BUCKET" >/dev/null 2>&1; then
    log "object-store bucket '$SNAPSHOT_BUCKET' exists"
  elif "${s3[@]}" create-bucket --bucket "$SNAPSHOT_BUCKET" >/dev/null 2>&1; then
    log "created object-store bucket '$SNAPSHOT_BUCKET'"
  else
    log "WARN: bucket ensure failed; round-trip will report the real error"
  fi
}

run_native() {
  local logf="$RESULTS_DIR/native-rest.log" code=0
  log "running native REST smoke (smoke-spaced-rest.sh)"
  run_capped "${SMOKE_TIMEOUT:-300}" \
    env SPACE_API_URL="$SPACE_API_URL" SPACE_API_TOKEN="$TOKEN" \
    SPACE_DATA_PLANE_URL="$SPACE_DATA_PLANE_URL" \
    SPACE_READY_TIMEOUT_MS="${SPACE_READY_TIMEOUT_MS:-30000}" \
    "$REPO_ROOT/scripts/tests/smoke-spaced-rest.sh" \
    >"$logf" 2>&1 || code=$?
  summarize native-rest "$code" "$logf"
}

run_ecw_identity() {
  local logf="$RESULTS_DIR/ecw-identity.log" code=0 scenario scenario_code
  local kind_cluster="${KCTX#kind-}"
  local scenarios=(
    ecw.process_identity
    ecw.filesystem_write_identity
    ecw.filesystem_read_identity
    ecw.filesystem_private_boundary
    ecw.watcher_identity
  )
  log "running typed ECW identity scenarios"
  if ! command -v uv >/dev/null 2>&1; then
    printf 'SKIP  %-14s :: uv not installed (identity scenarios use the project runtime)\n' "ecw-identity"
    return
  fi
  [[ "$KCTX" == kind-* ]] || die "ECW identity scenarios require a kind-* context, got $KCTX"
  # The api-server serves only ECW control-plane requests; ECW filesystem,
  # process, and service traffic goes through space-data-plane with the matching
  # sandbox host suffix.
  : >"$logf"
  for scenario in "${scenarios[@]}"; do
    scenario_code=0
    (
      cd "$REPO_ROOT"
      run_capped "${SMOKE_TIMEOUT:-300}" \
        env SPACE_SMOKE_BACKEND=kind SKIP_BOOTSTRAP=1 KIND_CLUSTER="$kind_cluster" \
        API_URL="$SPACE_API_URL" ECW_DATA_PLANE_BASE_URL="$SPACE_DATA_PLANE_URL" \
        E2B_DOMAIN="$E2B_DOMAIN" E2B_API_KEY="$E2B_API_KEY" E2B_TEMPLATE=seed \
        ECW_API_HOST="api.$E2B_DOMAIN" ECW_DATA_PLANE_DOMAIN="$E2B_DOMAIN" \
        uv run --frozen --python 3.11 -m space_smoke run "$scenario"
    ) >>"$logf" 2>&1 || scenario_code=$?
    if [[ "$code" == "0" && "$scenario_code" != "0" ]]; then
      code="$scenario_code"
    fi
  done
  summarize ecw-identity "$code" "$logf"
}

# Direct check of the real snapshot-storage path: drive the real volume-manager
# (on the node-stack DaemonSet) to push a payload to the object store and pull it back,
# asserting the bytes round-trip. This exercises the production object-store S3
# code against the local object-store endpoint — independent of the sandbox guest.
run_s3_roundtrip() {
  local code=0 logf="$RESULTS_DIR/s3-roundtrip.log"
  log "verifying real vol-mgr → object-store S3 round-trip"
  ensure_snapshot_bucket
  local pod
  pod="$(kubectl --context "$KCTX" -n "$NS" get pods \
    -l space.pplx.net/chart=space-node-stack \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    printf 'FAIL  %-14s :: no space-node-stack pod (is the node-stack DaemonSet up?)\n' "s3-roundtrip"
    FAILURES=$((FAILURES + 1))
    return
  fi
  local sbx="019e9e00-0000-7000-8000-0000000000aa"
  # Unique key + local path per run. vol-mgr dedups transfer jobs by
  # (subject, s3_uri, local_path) — a fixed key would make re-runs return the
  # prior run's object (stale bytes) instead of a fresh round-trip.
  local uniq; uniq="$(date +%s)-$$"
  local uri="s3://snapshots/local-smoke/rt-$uniq.bin?region=us-east-1&endpoint=http://object-store:8333&path_style=true"
  kubectl --context "$KCTX" -n "$NS" exec "$pod" -- bash -c "
    set -e
    src=/space/cache/rt-$uniq.bin
    out=/space/cache/rt-$uniq.out
    head -c 65536 /dev/urandom > \$src
    space-volmgr push-file --subject 'sandbox:$sbx' --local-path \$src --s3-uri '$uri'
    space-volmgr pull-file --subject 'sandbox:$sbx' --s3-uri '$uri' --local-path \$out
    cmp -s \$src \$out
    rm -f \$src \$out
  " >"$logf" 2>&1 || code=$?
  if [[ "$code" == "0" ]]; then
    printf 'PASS  %-14s real bytes pushed+pulled through the object store\n' "s3-roundtrip"
  else
    printf 'FAIL  %-14s exit=%s (log: %s)\n' "s3-roundtrip" "$code" "$logf"
    FAILURES=$((FAILURES + 1))
  fi
}

main() {
  command -v jq >/dev/null 2>&1 || die "jq is required"

  case "$WHICH" in
    s3-roundtrip|all) ensure_object_store_port_forward ;;
  esac
  # Only the api-driven smokes need a port-forward + session token + E2B key.
  if [[ "$WHICH" != "s3-roundtrip" ]]; then
    ensure_port_forward
    case "$WHICH" in
      native|ecw-identity|all) ensure_data_plane_port_forward ;;
    esac
    TOKEN="$(login_token)"
    [[ -n "$TOKEN" && "$TOKEN" != "null" ]] || die "login failed for $LOCAL_EMAIL"
    log "got session token (len ${#TOKEN})"
    seed_e2b_api_key
  fi

  echo "==================== local smoke results ===================="
  case "$WHICH" in
    s3-roundtrip)  run_s3_roundtrip ;;
    native)        run_native ;;
    ecw-identity)  run_ecw_identity ;;
    all)           run_s3_roundtrip; run_native; run_ecw_identity ;;
    *) die "unknown target: $WHICH (use s3-roundtrip|native|ecw-identity|all)" ;;
  esac
  echo "============================================================="
  log "logs under $RESULTS_DIR"
  if [[ "$FAILURES" -gt 0 ]]; then
    log "$FAILURES smoke(s) failed"
    return 1
  fi
  log "all smokes passed"
}

main "$@"
