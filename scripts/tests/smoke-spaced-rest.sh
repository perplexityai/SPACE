#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Only `idem_key` is taken from the shared helpers: this suite's
# `wait_operation` is deadline-based and takes a timeout argument, which is a
# different contract from the lib's poll-count version.
SPACE_OPS_PREFIX="native-api-smoke"
. "$SCRIPT_DIR/lib/space-api-ops.sh"

SPACE_API_URL="${SPACE_API_URL:-http://127.0.0.1:8080}"
SPACE_DATA_PLANE_URL="${SPACE_DATA_PLANE_URL:-}"
SPACE_TEMPLATE="${SPACE_TEMPLATE:-seed:latest}"
SPACE_RUNTIME="${SPACE_RUNTIME:-fc}"
SPACE_READY_TIMEOUT_MS="${SPACE_READY_TIMEOUT_MS:-60000}"
# Set =1 to skip the interactive process / terminal session WebSocket
# stages. Used when running the smoke against environments where the
# data-plane WebSocket isn't routed (e.g. behind some kubectl
# port-forwards) so the rest of the lifecycle — including pause/resume
# and suspend/restore — can still be exercised.
SPACE_SMOKE_SKIP_INTERACTIVE="${SPACE_SMOKE_SKIP_INTERACTIVE:-0}"

if [[ -z "${SPACE_API_TOKEN:-}" ]]; then
  echo "ERROR: SPACE_API_TOKEN is required" >&2
  exit 1
fi

for bin in base64 curl date jq mktemp node od sed sleep tr; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $bin" >&2
    exit 1
  fi
done

RUN_ID="native-api-smoke-$(date +%s)-$$"
SANDBOX_ID=""
START_SECONDS=$SECONDS

api_url() {
  printf '%s%s' "${SPACE_API_URL%/}" "$1"
}

hex_of() {
  printf '%s' "$1" | od -An -tx1 | tr -d ' \n'
}

file_hex() {
  od -An -tx1 "$1" | tr -d ' \n'
}

timestamp() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

log() {
  printf '[%s +%ss] %s\n' "$(timestamp)" "$((SECONDS - START_SECONDS))" "$*"
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local label="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "ERROR: $label mismatch" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    exit 1
  fi
}


api_request() {
  local method="$1"
  local path="$2"
  local body="${3-}"
  local has_body=0
  if (($# >= 3)); then
    has_body=1
  fi

  local tmp
  tmp="$(mktemp)"
  local code
  if ((has_body)); then
    code="$(
      curl -sS -o "$tmp" -w '%{http_code}' \
        -X "$method" \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        -H "Content-Type: application/json" \
        --data "$body" \
        "$(api_url "$path")"
    )"
  else
    code="$(
      curl -sS -o "$tmp" -w '%{http_code}' \
        -X "$method" \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        "$(api_url "$path")"
    )"
  fi

  if [[ "$code" -lt 200 || "$code" -ge 300 ]]; then
    echo "ERROR: $method $path returned HTTP $code" >&2
    cat "$tmp" >&2
    rm -f "$tmp"
    exit 1
  fi

  cat "$tmp"
  rm -f "$tmp"
}

api_request_idempotent() {
  local method="$1"
  local path="$2"
  local body="${3-}"
  local has_body=0
  if (($# >= 3)); then
    has_body=1
  fi

  local tmp
  tmp="$(mktemp)"
  local code
  if ((has_body)); then
    code="$(
      curl -sS -o "$tmp" -w '%{http_code}' \
        -X "$method" \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $(idem_key)" \
        --data "$body" \
        "$(api_url "$path")"
    )"
  else
    code="$(
      curl -sS -o "$tmp" -w '%{http_code}' \
        -X "$method" \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        -H "Idempotency-Key: $(idem_key)" \
        "$(api_url "$path")"
    )"
  fi

  if [[ "$code" -lt 200 || "$code" -ge 300 ]]; then
    echo "ERROR: $method $path returned HTTP $code" >&2
    cat "$tmp" >&2
    rm -f "$tmp"
    exit 1
  fi

  cat "$tmp"
  rm -f "$tmp"
}

api_expect_status() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  shift 3 || true
  local tmp
  tmp="$(mktemp)"
  local code
  if [[ -n "$body" ]]; then
    code="$(
      curl -sS -o "$tmp" -w '%{http_code}' \
        -X "$method" \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $(idem_key)" \
        --data "$body" \
        "$(api_url "$path")"
    )"
  else
    code="$(
      curl -sS -o "$tmp" -w '%{http_code}' \
        -X "$method" \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        -H "Idempotency-Key: $(idem_key)" \
        "$(api_url "$path")"
    )"
  fi
  local expected
  for expected in "$@"; do
    if [[ "$code" == "$expected" ]]; then
      rm -f "$tmp"
      return 0
    fi
  done
  echo "ERROR: $method $path returned HTTP $code; expected one of: $*" >&2
  cat "$tmp" >&2
  rm -f "$tmp"
  exit 1
}

api_get() {
  api_request GET "$1"
}

api_post() {
  api_request POST "$1" "$2"
}

api_post_idempotent() {
  api_request_idempotent POST "$1" "$2"
}

api_delete_idempotent() {
  api_request_idempotent DELETE "$1"
}

wait_operation() {
  local operation_id="$1"
  local timeout_secs="${2:-$ready_timeout_secs}"
  local deadline=$((SECONDS + timeout_secs))
  local response
  local state

  while ((SECONDS < deadline)); do
    response="$(api_get "/v1/operations/$operation_id")"
    state="$(jq -r '.state // empty' <<<"$response")"
    case "$state" in
      succeeded)
        return 0
        ;;
      failed|canceled|expired)
        echo "ERROR: operation $operation_id ended in $state" >&2
        echo "$response" >&2
        exit 1
        ;;
    esac
    sleep 1
  done

  echo "ERROR: operation $operation_id did not complete within ${timeout_secs}s" >&2
  exit 1
}

operation_request() {
  local method="$1"
  local path="$2"
  local body="${3-}"
  local timeout_secs="${4:-$ready_timeout_secs}"
  local response

  if [[ "$method" == "DELETE" ]]; then
    response="$(api_delete_idempotent "$path")"
  else
    response="$(api_request_idempotent "$method" "$path" "$body")"
  fi

  local operation_id
  operation_id="$(jq -r '.operation_id // empty' <<<"$response")"
  if [[ -z "$operation_id" ]]; then
    echo "ERROR: $method $path did not return operation_id" >&2
    echo "$response" >&2
    exit 1
  fi
  wait_operation "$operation_id" "$timeout_secs"
  printf '%s' "$response"
}

wait_for_sandbox_state() {
  local label="$1"
  shift
  local deadline=$((SECONDS + ready_timeout_secs))
  local sandbox_response
  local sandbox_state
  local expected_state

  while true; do
    sandbox_response="$(api_get "/v1/sandboxes/$SANDBOX_ID")"
    sandbox_state="$(jq -r '.state // empty' <<<"$sandbox_response")"
    for expected_state in "$@"; do
      if [[ "$sandbox_state" == "$expected_state" ]]; then
        return 0
      fi
    done
    case "$sandbox_state" in
      failed|stopped|deleted)
        echo "ERROR: sandbox entered terminal state while waiting for $label: $sandbox_state" >&2
        echo "$sandbox_response" >&2
        exit 1
        ;;
    esac
    if ((SECONDS >= deadline)); then
      echo "ERROR: sandbox did not reach $label within ${ready_timeout_secs}s; last state: $sandbox_state" >&2
      echo "$sandbox_response" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_file_api() {
  local deadline=$((SECONDS + ready_timeout_secs))
  local code
  while true; do
    code="$(
      curl -sS -o /dev/null -w '%{http_code}' \
        -X POST \
        -H "Authorization: Bearer $SPACE_API_TOKEN" \
        -H "Content-Type: application/json" \
        --data '{"path":"/home/user","user":null}' \
        "$(api_url "/v1/sandboxes/$SANDBOX_ID/files:stat")"
    )"
    if [[ "$code" == "200" ]]; then
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "ERROR: file API did not become ready within ${ready_timeout_secs}s; last status=$code" >&2
      exit 1
    fi
    sleep 1
  done
}

write_file() {
  local path="$1"
  local content="$2"
  local user="${3:-null}"
  local content_b64
  content_b64="$(printf '%s' "$content" | base64 | tr -d '\n')"
  api_post_idempotent \
    "/v1/sandboxes/$SANDBOX_ID/files:write" \
    "$(jq -n --arg path "$path" --arg data "$content_b64" --argjson user "$user" \
      '{path: $path, data_base64: $data, append: false, user: $user}')" \
    | jq -e '.bytes_written > 0' >/dev/null
}

read_file_to_path() {
  local guest_path="$1"
  local local_path="$2"
  api_post "/v1/sandboxes/$SANDBOX_ID/files:read" \
    "$(jq -n --arg path "$guest_path" '{path: $path, offset: 0, length: 16777216, user: null}')" \
    >"$local_path"
}

run_process_stream() {
  local command="$1"
  local tmp_session
  tmp_session="$(mktemp)"
  api_post_idempotent \
    "/v1/sandboxes/$SANDBOX_ID/process-sessions" \
    "$(jq -n --arg command "$command" \
      '{command: ["sh", "-lc", $command], cwd: null, env: {}, pty: null, stdin: false}')" \
    >"$tmp_session"

  run_session_websocket "$tmp_session" passive "$RUN_ID"
  rm -f "$tmp_session"
}

run_interactive_process_stream() {
  local tmp_session
  tmp_session="$(mktemp)"
  api_post_idempotent \
    "/v1/sandboxes/$SANDBOX_ID/process-sessions" \
    "$(jq -n \
      '{command: ["sh", "-lc", "while IFS= read -r line; do printf \"stdin:%s\\n\" \"$line\"; printf \"stderr:%s\\n\" \"$line\" >&2; done"], cwd: null, env: {}, pty: null, stdin: true}')" \
    >"$tmp_session"

  run_session_websocket "$tmp_session" interactive "$RUN_ID"
  rm -f "$tmp_session"
}

run_terminal_session_stream() {
  local tmp_session
  tmp_session="$(mktemp)"
  api_post_idempotent \
    "/v1/sandboxes/$SANDBOX_ID/terminal-sessions" \
    "$(jq -n '{command: [], cwd: null, env: {}, size: {cols: 80, rows: 24}}')" \
    >"$tmp_session"

  run_session_websocket "$tmp_session" terminal "$RUN_ID"
  rm -f "$tmp_session"
}

run_session_websocket() {
  local session_file="$1"
  local mode="$2"
  local run_id="$3"

  node - "$session_file" "$SPACE_API_URL" "$SPACE_DATA_PLANE_URL" "$mode" "$run_id" <<'NODE'
const fs = require('fs');

const session = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const apiBase = process.argv[3];
const dataPlaneBase = process.argv[4];
const mode = process.argv[5];
const runId = process.argv[6];

if (typeof WebSocket !== 'function') {
  console.error('Node runtime does not provide a built-in WebSocket client');
  process.exit(2);
}
if (!session.attach || !session.attach.url || !session.attach.token) {
  console.error('process session did not return attach.url and attach.token');
  process.exit(2);
}

function attachUrl(rawUrl) {
  const base = rawUrl.startsWith('/') && dataPlaneBase ? dataPlaneBase : apiBase;
  const parsed = new URL(rawUrl, base);
  if (parsed.protocol === 'https:') {
    parsed.protocol = 'wss:';
  } else if (parsed.protocol === 'http:') {
    parsed.protocol = 'ws:';
  }
  return parsed.toString();
}

let stdout = '';
let stderr = '';
let processId = null;
let exitCode = null;
let status = null;
let attached = false;
let inputAccepted = false;
let closeStdinAccepted = false;
let heartbeatAccepted = false;
let sentControls = false;

const socket = new WebSocket(attachUrl(session.attach.url));
const timeout = setTimeout(() => {
  console.error('timed out waiting for process stream');
  socket.close();
  process.exit(2);
}, 30000);

socket.addEventListener('open', () => {
  socket.send(JSON.stringify({ type: 'attach', token: session.attach.token }));
});

function sendFrame(frame) {
  socket.send(JSON.stringify(frame));
}

function sendBase64Input(seq, text) {
  sendFrame({
    type: 'input',
    seq,
    data_base64: Buffer.from(text, 'utf8').toString('base64'),
  });
}

function sendScenarioControls() {
  if (sentControls || !processId) {
    return;
  }
  sentControls = true;
  if (mode === 'interactive') {
    sendBase64Input(1, `${runId}\n`);
    sendFrame({ type: 'close_stdin', seq: 2 });
    sendFrame({ type: 'heartbeat', seq: 3 });
  } else if (mode === 'terminal') {
    sendFrame({ type: 'resize', cols: 100, rows: 30 });
    sendBase64Input(1, `printf '${runId}'; exit 0\n`);
    sendFrame({ type: 'heartbeat', seq: 2 });
  }
}

socket.addEventListener('message', (event) => {
  let frame;
  try {
    frame = JSON.parse(String(event.data));
  } catch (error) {
    clearTimeout(timeout);
    console.error(`invalid JSON frame: ${error}`);
    socket.close();
    process.exit(2);
  }

  switch (frame.type) {
    case 'attached':
      attached = true;
      break;
    case 'process_started':
      processId = frame.process_id;
      sendScenarioControls();
      break;
    case 'stdout':
      stdout += Buffer.from(frame.data_base64, 'base64').toString('utf8');
      break;
    case 'stderr':
      stderr += Buffer.from(frame.data_base64, 'base64').toString('utf8');
      break;
    case 'input_accepted':
      if (frame.seq === 1) inputAccepted = true;
      break;
    case 'control_accepted':
      if (mode === 'interactive' && frame.seq === 2) closeStdinAccepted = true;
      if (
        (mode === 'interactive' && frame.seq === 3) ||
        (mode === 'terminal' && frame.seq === 2)
      ) {
        heartbeatAccepted = true;
      }
      break;
    case 'process_exited':
      processId = frame.process_id || processId;
      exitCode = frame.exit_code;
      status = frame.status;
      clearTimeout(timeout);
      socket.close();
      console.log(JSON.stringify({
        session_id: session.id,
        process_id: processId,
        attached,
        stdout,
        stderr,
        exit_code: exitCode,
        status,
        input_accepted: inputAccepted,
        close_stdin_accepted: closeStdinAccepted,
        heartbeat_accepted: heartbeatAccepted,
      }));
      break;
    case 'error':
      clearTimeout(timeout);
      console.error(frame.message || frame.code || 'data-plane stream error');
      socket.close();
      process.exit(2);
      break;
  }
});

socket.addEventListener('error', () => {
  clearTimeout(timeout);
  console.error('data-plane WebSocket failed');
  process.exit(2);
});

socket.addEventListener('close', () => {
  if (exitCode === null) {
    clearTimeout(timeout);
    console.error('data-plane WebSocket closed before process exit');
    process.exit(2);
  }
});
NODE
}

run_command_sse() {
  local command="$1"
  local tmp_command
  tmp_command="$(mktemp)"
  api_post_idempotent \
    "/v1/sandboxes/$SANDBOX_ID/commands" \
    "$(jq -n --arg command "$command" \
      '{command: ["sh", "-lc", $command], cwd: null, env: {}, pty: null, stdin: false, timeout: null}')" \
    >"$tmp_command"

  node - "$tmp_command" "$SPACE_API_URL" "$SPACE_DATA_PLANE_URL" <<'NODE'
const fs = require('fs');

const command = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const apiBase = process.argv[3];
const dataPlaneBase = process.argv[4];

if (typeof fetch !== 'function') {
  console.error('Node runtime does not provide a built-in fetch client');
  process.exit(2);
}
if (!command.attach || !command.attach.token) {
  console.error('command did not return attach.token');
  process.exit(2);
}

function eventsUrl(commandHandle) {
  let rawUrl = commandHandle.attach.events_url;
  if (!rawUrl) {
    if (!commandHandle.attach.url) {
      console.error('command did not return attach.url or attach.events_url');
      process.exit(2);
    }
    rawUrl = commandHandle.attach.url.replace(/\/attach$/, '/events');
  }
  const base = rawUrl.startsWith('/') && dataPlaneBase ? dataPlaneBase : apiBase;
  return new URL(rawUrl, base).toString();
}

let stdout = '';
let stderr = '';
let processId = null;
let exitCode = null;
let status = null;
let attached = false;
let lastEventId = null;
let eventIdsMonotonic = true;
let eventIdsOnOutputOnly = true;

const timeout = setTimeout(() => {
  console.error('timed out waiting for command event stream');
  process.exit(2);
}, 30000);

// Split a text/event-stream body into {event, id, data} records, keeping the
// SSE id so the second connection can resume with Last-Event-ID.
function parseEvents(buffer) {
  const events = [];
  const parts = buffer.split('\n\n');
  const rest = parts.pop() || '';
  for (const part of parts) {
    let event = null;
    let id = null;
    const data = [];
    for (const line of part.split('\n')) {
      if (line.startsWith(':')) continue;
      const colon = line.indexOf(':');
      const name = colon === -1 ? line : line.slice(0, colon);
      const value = colon === -1 ? '' : line.slice(colon + 1).trimStart();
      if (name === 'event') event = value;
      else if (name === 'id') id = value;
      else if (name === 'data') data.push(value);
    }
    // Ids are opaque: keep them as received, to echo them exactly.
    if (data.length) events.push({ event, id, data: JSON.parse(data.join('\n')) });
  }
  return { events, rest };
}

// Open one SSE connection and feed events to `onEvent` until it returns true.
// Every identified frame must carry a greater id than the one before, and
// only output and resync_required frames may carry one.
async function readStream(resumeFrom, onEvent) {
  const headers = { Accept: 'text/event-stream', Authorization: `Bearer ${command.attach.token}` };
  if (resumeFrom !== null) headers['Last-Event-ID'] = resumeFrom;
  const response = await fetch(eventsUrl(command), { headers });
  if (!response.ok || !response.body) {
    throw new Error(`command event stream returned HTTP ${response.status}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return false;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
    const { events, rest } = parseEvents(buffer);
    buffer = rest;
    for (const event of events) {
      if (event.id !== null) {
        if (lastEventId !== null && BigInt(event.id) <= BigInt(lastEventId)) eventIdsMonotonic = false;
        lastEventId = event.id;
      }
      if (event.event !== null && event.event !== event.data.type) eventIdsMonotonic = false;
      const identified = ['stdout', 'stderr', 'resync_required'].includes(event.data.type);
      if ((event.id !== null) !== identified) eventIdsOnOutputOnly = false;
      if (onEvent(event)) {
        await reader.cancel().catch(() => {});
        return true;
      }
    }
  }
}

(async () => {
  // First connection: the one-shot command runs to completion.
  const finished = await readStream(null, (event) => {
    const frame = event.data;
    switch (frame.type) {
      case 'attached':
        attached = true;
        return false;
      case 'process_started':
        processId = frame.process_id;
        return false;
      case 'stdout':
        stdout += Buffer.from(frame.data_base64, 'base64').toString('utf8');
        return false;
      case 'stderr':
        stderr += Buffer.from(frame.data_base64, 'base64').toString('utf8');
        return false;
      case 'process_exited':
        processId = frame.process_id || processId;
        exitCode = frame.exit_code;
        status = frame.status;
        return true;
      case 'error':
        throw new Error(frame.message || frame.code || 'command event stream error');
      default:
        return false;
    }
  });
  if (!finished || exitCode === null) {
    throw new Error('command event stream closed before process exit');
  }

  // Second connection: resume with Last-Event-ID. The contract is that the
  // stream restarts with attached, resumes after Last-Event-ID without
  // replaying output already received or asking to resync, and the exited
  // process still reports its exit.
  const resumeFrom = lastEventId;
  const resume = { attached_first: null, resync: null, stdout: '', exit_code: null };
  let seen = 0;
  await readStream(resumeFrom, (event) => {
    const frame = event.data;
    seen += 1;
    if (seen === 1) resume.attached_first = frame.type === 'attached' && event.id === null;
    if (frame.type === 'resync_required' && resume.resync === null) resume.resync = frame.reason;
    if (frame.type === 'stdout') resume.stdout += Buffer.from(frame.data_base64, 'base64').toString('utf8');
    if (frame.type === 'process_exited') {
      resume.exit_code = frame.exit_code;
      return true;
    }
    if (frame.type === 'error') throw new Error(frame.message || frame.code || 'resumed event stream error');
    return frame.type === 'closed';
  });

  clearTimeout(timeout);
  console.log(JSON.stringify({
    session_id: command.session_id || command.id,
    process_id: processId,
    attached,
    stdout,
    stderr,
    exit_code: exitCode,
    status,
    event_ids_monotonic: eventIdsMonotonic,
    event_ids_on_output_only: eventIdsOnOutputOnly,
    resume_from: resumeFrom,
    resume,
  }));
  process.exit(0);
})().catch((error) => {
  clearTimeout(timeout);
  console.error(error && error.message ? error.message : String(error));
  process.exit(2);
});
NODE
  rm -f "$tmp_command"
}

ready_timeout_secs=$(((SPACE_READY_TIMEOUT_MS + 999) / 1000))

cleanup() {
  if [[ -n "$SANDBOX_ID" && "${KEEP_SANDBOX:-0}" != "1" ]]; then
    log "deleting sandbox: $SANDBOX_ID"
    operation_request DELETE "/v1/sandboxes/$SANDBOX_ID" "" "$ready_timeout_secs" >/dev/null || true
  fi
}
trap cleanup EXIT

log "creating sandbox through native public API"
create_started=$SECONDS
create_body="$(
  jq -n \
    --arg name "$RUN_ID" \
    --arg template "$SPACE_TEMPLATE" \
    --arg runtime "$SPACE_RUNTIME" \
    --arg smoke_env "$RUN_ID" \
    --arg run_id "$RUN_ID" \
    '{
      name: $name,
      template: $template,
      runtime: $runtime,
      env: {SPACE_SMOKE_ENV: $smoke_env},
      metadata: {smoke: true, run_id: $run_id, attempt: 1},
      resources: null
    }'
)"
create_response="$(operation_request POST /v1/sandboxes "$create_body" "$ready_timeout_secs")"
SANDBOX_ID="$(jq -r '.sandbox_id // empty' <<<"$create_response")"
if [[ -z "$SANDBOX_ID" ]]; then
  echo "ERROR: create response did not include sandbox_id" >&2
  echo "$create_response" >&2
  exit 1
fi
log "sandbox: $SANDBOX_ID (create_elapsed=$((SECONDS - create_started))s)"

log "checking metadata round-trip"
sandbox_response="$(api_get "/v1/sandboxes/$SANDBOX_ID")"
assert_eq "$(jq -r '.metadata.run_id' <<<"$sandbox_response")" "$RUN_ID" "metadata.run_id"
assert_eq "$(jq -r '.metadata.smoke' <<<"$sandbox_response")" "true" "metadata.smoke"
assert_eq "$(jq -r '.metadata.attempt' <<<"$sandbox_response")" "1" "metadata.attempt"
log "metadata round-trip ok"

log "starting sandbox and waiting for file API readiness"
operation_request POST "/v1/sandboxes/$SANDBOX_ID:start" '{}' "$ready_timeout_secs" >/dev/null
wait_for_sandbox_state "running state" running
wait_for_file_api
log "sandbox running"

tmp_dir="/home/user/.space-smoke/$RUN_ID"
file_path="$tmp_dir/file.txt"
renamed_path="$tmp_dir/renamed.txt"
content="hello-$RUN_ID"
root_dir="/root/$RUN_ID"
root_file="$root_dir/root.txt"

log "exercising native file API"
api_post_idempotent \
  "/v1/sandboxes/$SANDBOX_ID/files:mkdir" \
  "$(jq -n --arg path "$tmp_dir" '{path: $path, parents: true, user: null}')" \
  >/dev/null
api_post "/v1/sandboxes/$SANDBOX_ID/files:stat" \
  "$(jq -n --arg path "$tmp_dir" '{path: $path, user: null}')" \
  | jq -e '.entry.file_type == "directory"' >/dev/null
write_file "$file_path" "$content"
api_post "/v1/sandboxes/$SANDBOX_ID/files:stat" \
  "$(jq -n --arg path "$file_path" '{path: $path, user: null}')" \
  | jq -e '.entry.file_type == "file" and .entry.size_bytes > 0' >/dev/null
read_file="$(mktemp)"
read_file_to_path "$file_path" "$read_file"
assert_eq "$(file_hex "$read_file")" "$(hex_of "$content")" "file read data"
rm -f "$read_file"
api_post "/v1/sandboxes/$SANDBOX_ID/files:list" \
  "$(jq -n --arg path "$tmp_dir" '{path: $path, depth: 1, user: null}')" \
  | jq -e --arg path "$file_path" '.entries[] | select(.path == $path)' >/dev/null
api_post_idempotent \
  "/v1/sandboxes/$SANDBOX_ID/files:rename" \
  "$(jq -n --arg old_path "$file_path" --arg new_path "$renamed_path" \
    '{old_path: $old_path, new_path: $new_path, user: null}')" \
  >/dev/null
read_file="$(mktemp)"
read_file_to_path "$renamed_path" "$read_file"
assert_eq "$(file_hex "$read_file")" "$(hex_of "$content")" "renamed file read data"
rm -f "$read_file"
log "native file API ok"

log "validating explicit root filesystem override and default-user boundary"
api_post_idempotent \
  "/v1/sandboxes/$SANDBOX_ID/files:mkdir" \
  "$(jq -n --arg path "$root_dir" '{path: $path, parents: true, user: "root"}')" \
  >/dev/null
write_file "$root_file" "root-$RUN_ID" '"root"'
api_post "/v1/sandboxes/$SANDBOX_ID/files:stat" \
  "$(jq -n --arg path "$root_file" '{path: $path, user: "root"}')" \
  | jq -e '.entry.owner_uid == 0' >/dev/null
api_expect_status POST "/v1/sandboxes/$SANDBOX_ID/files:stat" \
  "$(jq -n --arg path "$root_file" '{path: $path, user: null}')" 403
api_expect_status POST "/v1/sandboxes/$SANDBOX_ID/files:mkdir" \
  "$(jq -n --arg path "$root_dir/default-mkdir" '{path: $path, parents: true, user: null}')" 403
api_expect_status POST "/v1/sandboxes/$SANDBOX_ID/files:write" \
  "$(jq -n --arg path "$root_dir/default-write.txt" --arg data "$(printf should-not-write | base64 | tr -d '\n')" \
    '{path: $path, data_base64: $data, append: false, user: null}')" 403
log "filesystem identity boundary ok"

log "exercising one-shot command event stream"
command_result="$(
  run_command_sse "printf '$RUN_ID'; printf command-err >&2; exit 6"
)"
assert_eq "$(jq -r '.attached' <<<"$command_result")" "true" "command stream attached"
assert_eq "$(jq -r '.stdout' <<<"$command_result")" "$RUN_ID" "command stdout"
assert_eq "$(jq -r '.stderr' <<<"$command_result")" "command-err" "command stderr"
assert_eq "$(jq -r '.exit_code' <<<"$command_result")" "6" "command exit code"
assert_eq "$(jq -r '.event_ids_monotonic' <<<"$command_result")" "true" "command stream ids increase"
assert_eq "$(jq -r '.event_ids_on_output_only' <<<"$command_result")" "true" "only output and resync_required carry ids"
assert_eq "$(jq -r '.resume.attached_first' <<<"$command_result")" "true" "resumed stream starts with attached"
assert_eq "$(jq -r '.resume.resync' <<<"$command_result")" "null" "lossless resume does not resync"
assert_eq "$(jq -r '.resume.stdout' <<<"$command_result")" "" "resume does not replay received output"
assert_eq "$(jq -r '.resume.exit_code' <<<"$command_result")" "6" "resumed stream reports the process exit"
log "one-shot command event stream ok (Last-Event-ID resume verified)"

log "exercising data-plane process session stream"
process_result="$(
  run_process_stream "printf '$RUN_ID'; printf smoke-err >&2; exit 7"
)"
assert_eq "$(jq -r '.attached' <<<"$process_result")" "true" "process stream attached"
assert_eq "$(jq -r '.stdout' <<<"$process_result")" "$RUN_ID" "process stdout"
assert_eq "$(jq -r '.stderr' <<<"$process_result")" "smoke-err" "process stderr"
assert_eq "$(jq -r '.exit_code' <<<"$process_result")" "7" "process exit code"
api_get "/v1/sandboxes/$SANDBOX_ID/sessions?kind=process" \
  | jq -e --arg session_id "$(jq -r '.session_id' <<<"$process_result")" \
    '.data[] | select(.id == $session_id and .attach == null)' >/dev/null
log "data-plane process stream ok"

if (( SPACE_SMOKE_SKIP_INTERACTIVE )); then
  log "skipping interactive process session controls (SPACE_SMOKE_SKIP_INTERACTIVE=1)"
else
  log "exercising interactive process session controls"
  interactive_result="$(
    run_interactive_process_stream
  )"
  jq -e --arg output "stdin:$RUN_ID" '.stdout | contains($output)' <<<"$interactive_result" >/dev/null
  jq -e --arg output "stderr:$RUN_ID" '.stderr | contains($output)' <<<"$interactive_result" >/dev/null
  assert_eq "$(jq -r '.input_accepted' <<<"$interactive_result")" "true" "interactive input accepted"
  assert_eq "$(jq -r '.close_stdin_accepted' <<<"$interactive_result")" "true" "interactive close_stdin accepted"
  assert_eq "$(jq -r '.heartbeat_accepted' <<<"$interactive_result")" "true" "interactive heartbeat accepted"
  assert_eq "$(jq -r '.exit_code' <<<"$interactive_result")" "0" "interactive process exit code"
  log "interactive process session controls ok"
fi

if (( SPACE_SMOKE_SKIP_INTERACTIVE )); then
  log "skipping terminal session stream (SPACE_SMOKE_SKIP_INTERACTIVE=1)"
else
  log "exercising terminal session stream"
  terminal_result="$(
    run_terminal_session_stream
  )"
  jq -e --arg output "$RUN_ID" '.stdout | contains($output)' <<<"$terminal_result" >/dev/null
  assert_eq "$(jq -r '.input_accepted' <<<"$terminal_result")" "true" "terminal input accepted"
  assert_eq "$(jq -r '.heartbeat_accepted' <<<"$terminal_result")" "true" "terminal heartbeat accepted"
  assert_eq "$(jq -r '.exit_code' <<<"$terminal_result")" "0" "terminal exit code"
  api_get "/v1/sandboxes/$SANDBOX_ID/sessions?kind=terminal" \
    | jq -e --arg session_id "$(jq -r '.session_id' <<<"$terminal_result")" \
      '.data[] | select(.id == $session_id and .attach == null)' >/dev/null
  log "terminal session stream ok"
fi

log "exercising sandbox pause/resume lifecycle"
lifecycle_started=$SECONDS
operation_request POST "/v1/sandboxes/$SANDBOX_ID:pause" '{}' "$ready_timeout_secs" >/dev/null
wait_for_sandbox_state "paused state" paused suspended
log "sandbox paused (elapsed=$((SECONDS - lifecycle_started))s)"

resume_started=$SECONDS
operation_request POST "/v1/sandboxes/$SANDBOX_ID:resume" '{}' "$ready_timeout_secs" >/dev/null
wait_for_sandbox_state "running state after resume" running
wait_for_file_api
log "sandbox resumed (elapsed=$((SECONDS - resume_started))s)"

read_file="$(mktemp)"
read_file_to_path "$renamed_path" "$read_file"
assert_eq "$(file_hex "$read_file")" "$(hex_of "$content")" "post-resume file contents"
rm -f "$read_file"
log "post-resume guest state verified (total lifecycle elapsed=$((SECONDS - lifecycle_started))s)"

log "exercising sandbox suspend/restore lifecycle"
suspend_restore_started=$SECONDS
operation_request POST "/v1/sandboxes/$SANDBOX_ID:suspend" '{}' "$ready_timeout_secs" >/dev/null
wait_for_sandbox_state "suspended state" suspended
log "sandbox suspended (elapsed=$((SECONDS - suspend_restore_started))s)"

restore_started=$SECONDS
operation_request POST "/v1/sandboxes/$SANDBOX_ID:restore" '{}' "$ready_timeout_secs" >/dev/null
wait_for_sandbox_state "running state after restore" running
wait_for_file_api
log "sandbox restored (elapsed=$((SECONDS - restore_started))s)"

read_file="$(mktemp)"
read_file_to_path "$renamed_path" "$read_file"
assert_eq "$(file_hex "$read_file")" "$(hex_of "$content")" "post-restore file contents"
rm -f "$read_file"
log "post-restore guest state verified (total suspend/restore elapsed=$((SECONDS - suspend_restore_started))s)"

log "cleaning up guest filesystem"
cleanup_started=$SECONDS
api_post_idempotent \
  "/v1/sandboxes/$SANDBOX_ID/files:remove" \
  "$(jq -n --arg path "$tmp_dir" '{path: $path, recursive: true, user: null}')" \
  >/dev/null
log "guest filesystem cleanup ok (elapsed=$((SECONDS - cleanup_started))s)"

if [[ "${KEEP_SANDBOX_ON_SUCCESS:-0}" == "1" ]]; then
  kept_sandbox_id="$SANDBOX_ID"
  SANDBOX_ID=""
  log "retaining sandbox for follow-up checks: $kept_sandbox_id"
else
  log "deleting sandbox through native public API"
  delete_started=$SECONDS
  operation_request DELETE "/v1/sandboxes/$SANDBOX_ID" "" "$ready_timeout_secs" >/dev/null
  verify_code="$(
    curl -sS -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $SPACE_API_TOKEN" \
      "$(api_url "/v1/sandboxes/$SANDBOX_ID")"
  )"
  if [[ "$verify_code" != "404" ]]; then
    echo "ERROR: expected HTTP 404 for deleted sandbox, got $verify_code" >&2
    exit 1
  fi
  deleted_sandbox_id="$SANDBOX_ID"
  SANDBOX_ID=""
  log "sandbox deleted (elapsed=$((SECONDS - delete_started))s id=$deleted_sandbox_id)"
fi

log "native public API smoke test passed (total_elapsed=$((SECONDS - START_SECONDS))s)"
