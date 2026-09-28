# shellcheck shell=bash
# shellcheck disable=SC2154  # API_URL, ECW_API_HOST and E2B_API_KEY are caller-provided inputs
# ECW (E2B-compatible) helpers for the kind smoke suites.
#
# The home for this block, so the ECW side stops repeating what the native side
# already consolidated into `space-api-ops.sh`. Three suites had grown their own
# copy of the request helper, the key seeding, and the SHA-256 shim, and the
# copies had already diverged on the request timeout — which is the same drift
# `space-api-ops.sh`'s header describes paying for once.
#
# Consumers: no shell smoke suite sources this any more (`test-ecw-ops.sh` pins
# its contract). Three others seed a key and are deliberately not consumers:
# `smoke-kind-sandbox-fork.py` is Python and
# cannot source this; `run-local-smokes.sh` seeds a persistent `local-dev-smoke`
# key it never drops, which is not a run-scoped credential and is in fact the row
# `ecw_drop_api_key`'s name scoping exists to protect. `scripts/seed-api-key.sh`
# is the standalone operator CLI, not a suite.
#
# Inputs the caller must set before use:
#   API_URL        cluster API base URL
#   ECW_API_HOST   the `apiHost` from SPACE_ECW_HOSTS (Host header override)
#   E2B_API_KEY    raw ECW key, seeded into Postgres by `ecw_seed_api_key`
#
# Optional:
#   ECW_CLIENT_ID           sent as `x-client-id` when set; omitted otherwise, so
#                           a suite that does not exercise the per-client limiter
#                           does not silently acquire a client identity
#   ECW_CURL_TIMEOUT_SECS   per-request bound (default 30). A suite whose create
#                           waits for a booted sandbox needs this well above the
#                           default; one that only wants the rejection does not.
#   ECW_DELETE_DEADLINE_SECS  wall-clock bound on `ecw_delete_sandbox` (default 120)
#
# `ecw_seed_api_key` and `ecw_drop_api_key` additionally need
# `kind_stack_psql` from `bootstrap-kind-stack.sh`, which every kind suite
# already sources.

# The seeded org and service user on a fresh kind database. Named once here
# because a schema or seed change to `api_keys` otherwise means finding
# every suite that spelled the sentinels out.
ECW_SEED_ORG_ID="00000000-0000-0000-0000-000000000001"
ECW_SEED_USER_ID="00000000-0000-0000-0000-000000000010"

# Returned by the seed's `INSERT … RETURNING` so a real insert is distinguishable
# from psql's own command tag. Deliberately not a number.
ECW_INSERTED_SENTINEL="ECW_ROW_INSERTED"

ecw_key_hash() { # raw-key
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$1" | sha256sum | awk '{print $1}'
  else
    printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
  fi
}

# A fresh run-scoped raw key. **Cryptographically random**, not derived from the
# pid and the clock: `e2b_..._$$_$(date +%s)` is a live credential to the seeded
# org that any other local user can reconstruct from a process listing and a
# narrow start time, and hashing that input does not add entropy. `openssl rand`
# is the pattern `scripts/seed-api-key.sh` already uses for the same reason.
#
# Here rather than in each suite because three of them minted their own, all with
# the same weakness.
ecw_new_api_key() { # prefix
  local random=""
  if command -v openssl >/dev/null 2>&1; then
    # `|| random=""` and then a shape check, because every caller invokes this in
    # an `... ||` list, which suppresses `errexit` inside the function. Without
    # both, an `openssl rand` that *exists but fails* left `random` empty and this
    # returned `prefix_` — exit 0, and a wholly predictable credential. Failing
    # open on the one property the function exists to provide.
    random="$(openssl rand -hex 32)" || random=""
  fi
  if [[ ! "$random" =~ ^[a-f0-9]{64}$ ]] \
    && [[ -r /dev/urandom ]] && command -v od >/dev/null 2>&1; then
    # `od` over a **bounded** read, not `tr -dc … | head -c 64`. That form runs an
    # endless producer into `head`, which exits at 64 bytes and leaves `tr` with
    # SIGPIPE — exit 141, and under the `pipefail` every suite sets, the command
    # substitution then fails and `set -e` aborts a run whose key was in fact
    # generated. `-N32` reads exactly 32 bytes and emits 64 hex characters, so
    # nothing is left producing into a closed pipe.
    random="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')" || random=""
  fi
  # Checked once, after both sources have had their turn: 64 lower-case hex
  # characters or nothing. A short or empty value is refused rather than returned,
  # so a caller can never receive a key weaker than advertised.
  if [[ ! "$random" =~ ^[a-f0-9]{64}$ ]]; then
    echo "ERROR: could not obtain 32 bytes of randomness for an ECW key. Need" >&2
    echo "       a working 'openssl rand', or 'od' plus a readable /dev/urandom." >&2
    return 1
  fi
  printf '%s_%s' "$1" "$random"
}

# Seed `$E2B_API_KEY` for the seeded org. Idempotent, so a re-run with a pinned
# key does not fail on a stale row.
# The name this run seeded under, so the drop can tell its own row from one that
# predated it. Empty until `ecw_seed_api_key` runs, which makes `ecw_drop_api_key`
# a no-op on a cleanup path that never got that far.
ECW_SEEDED_KEY_NAME=""

ecw_seed_api_key() { # key-name
  local hash inserted
  # The name is interpolated into a single-quoted SQL literal below, so a quote
  # in it would break out. Every call site passes a literal today; refusing
  # anything else keeps that from being load-bearing.
  if [[ ! "$1" =~ ^[A-Za-z0-9_-]+$ ]]; then
    echo "ERROR: ECW key name '$1' must match [A-Za-z0-9_-]+" >&2
    return 1
  fi
  hash="$(ecw_key_hash "$E2B_API_KEY")"
  # `RETURNING` so the run learns whether it actually inserted. With
  # `ON CONFLICT DO NOTHING` a caller who pinned `E2B_API_KEY` to a key that
  # already has a row seeds nothing, and a run that believes it seeded would go on
  # to own a row it did not create.
  inserted="$(kind_stack_psql <<SQL
INSERT INTO api_keys (org_id, name, key_hash, created_by)
VALUES ('$ECW_SEED_ORG_ID', '$1', '$hash', '$ECW_SEED_USER_ID')
ON CONFLICT (key_hash) DO NOTHING
RETURNING '$ECW_INSERTED_SENTINEL';
SQL
  )" || return 1
  # A word no psql command tag can produce, matched as a whole line.
  #
  # `kind_stack_psql` is `-tA` but **not** `-q`, so psql writes its command tag to
  # stdout alongside any returned row: `INSERT 0 1` on an insert, `INSERT 0 0` on
  # a conflict. `RETURNING 1` therefore made ownership depend on telling a
  # returned digit from a digit inside a tag — which is how comparing the capture
  # against `1` came to never match, leaving each run convinced it had seeded
  # nothing and skipping its own cleanup. A sentinel takes the formatting out of
  # the decision entirely.
  if grep -qx "$ECW_INSERTED_SENTINEL" <<<"$inserted"; then
    ECW_SEEDED_KEY_NAME="$1"
  fi
  return 0
}

# Best effort, and only for a row this run actually inserted.
#
# `ON CONFLICT DO NOTHING` means a caller who pins `E2B_API_KEY` to a key that
# already has a row seeds nothing — so an unconditional delete on the hash alone
# would destroy a credential the run did not create. The bench's own local-dev
# key (`run-local-smokes.sh`) is exactly that case.
#
# So this removes only the row *this* run inserted: `ECW_SEEDED_KEY_NAME` is set
# only on a real insert, and the hash is this run's. A row a previous run
# abandoned has a different hash and survives — deliberately, since nothing here
# can tell an abandoned row from one another session is still using.
# What to tell an operator when a run keeps its ECW key row on purpose.
#
# Here rather than in each suite because `ecw_drop_api_key` owns the row's
# lifetime, and the instruction printed on the run that most needs it must not
# drift from what the code does — the same argument the override library makes
# for owning its own hand-paste hint. Two suites had this verbatim, and the copies
# had already diverged on wording.
#
# Keyed on the **full** hash, which is the only unique handle: `api_keys.name`
# is explicitly non-unique, so after two interrupted runs a name+org predicate
# matches both retained rows and re-pointing them at one replacement hash
# violates the unique constraint on `key_hash` — the recovery path breaking
# exactly when stale rows have accumulated. The hash is a SHA-256 *of* the key,
# not the key, so printing it in full exposes nothing. The raw key is never
# printed: it is a live credential, and this text lands in runtime evidence.
ecw_print_kept_key_hint() { # indent [what-is-left]
  local pad="$1" what="${2:-anything left behind}"
  if [[ -z "$ECW_SEEDED_KEY_NAME" ]]; then
    echo "${pad}this run seeded no ECW key row, so there is none to keep." >&2
    echo "${pad}$what must be removed through the admin API." >&2
    return 0
  fi
  echo "${pad}keeping this run's ECW key row (name '$ECW_SEEDED_KEY_NAME', org" >&2
  echo "${pad}$ECW_SEED_ORG_ID) so $what stays reachable. The raw key is not" >&2
  echo "${pad}printed. To reach it, re-point that one row at a key you choose:" >&2
  echo "${pad}  UPDATE api_keys SET key_hash = '<sha256 of your key>'" >&2
  echo "${pad}   WHERE key_hash = '$(ecw_key_hash "$E2B_API_KEY")';" >&2
  echo "${pad}then DELETE /sandboxes/<id> with that key — or use the admin API." >&2
}

ecw_drop_api_key() {
  local hash
  [[ -n "$ECW_SEEDED_KEY_NAME" ]] || return 0
  hash="$(ecw_key_hash "$E2B_API_KEY")"
  # Reported, not swallowed. `|| true` hid both the error and the status, so a
  # Postgres blip during cleanup left a live credential row for the seeded org
  # with nothing said and the run still green — and the raw key is deliberately
  # never printed, so the operator got no hint either. Returns non-zero so a
  # caller can fail its run; both callers do.
  if ! kind_stack_psql >/dev/null 2>&1 <<SQL
DELETE FROM api_keys WHERE key_hash = '$hash' AND name = '$ECW_SEEDED_KEY_NAME';
SQL
  then
    echo "ERROR: could not drop this run's ECW key row (name '$ECW_SEEDED_KEY_NAME'," >&2
    echo "       hash ${hash:0:12}…). It still grants API access to org" >&2
    echo "       $ECW_SEED_ORG_ID; remove it by hand:" >&2
    echo "         DELETE FROM api_keys WHERE key_hash = '$hash';" >&2
    return 1
  fi
}

# One ECW request. Echoes a single line: CODE|RETRY_AFTER|SANDBOX_ID|MESSAGE.
#
# The four fields come back together because every caller wants the status with
# the reason: a bare code cannot distinguish a limiter rejection from an auth or
# routing failure, and that distinction is what the suites assert on.
#
# **The message is last, and that ordering is load-bearest.** `@tsv` escapes tab,
# newline, CR and backslash but not `|`, and a server message containing one
# would shift every field after it — putting a fragment of the message into the
# caller's sandbox id, which then names no sandbox, deletes cleanly as a 404, and
# leaves the real sandbox running while the run reports a clean bench. With the
# message last, `IFS='|' read -r CODE RETRY SID MSG` gives the remainder to
# `MSG`, which callers only prefix-match.
# Whether `$E2B_API_KEY` can be carried verbatim in a curl config document.
#
# Checked by the entry points **before** they run, not inside the curl wrapper.
# A config document honours `\"` and `\\` inside a quoted value, so a
# caller-pinned key containing either would silently send a *different* header;
# and both call sites wrap their curl in `2>/dev/null` for transfer noise, which
# swallowed this explanation too. In `ecw_delete_sandbox` that turned a rejected
# key into forty `sleep 3` iterations ending in "did not succeed within 120s
# (last HTTP 000)", with nothing anywhere saying no request was ever made.
ecw__key_is_safe() {
  [[ "$E2B_API_KEY" =~ ^[A-Za-z0-9_.:-]+$ ]] && return 0
  echo "ERROR: E2B_API_KEY contains characters this library cannot pass to curl" >&2
  echo "       safely (allowed: letters, digits, '_', '.', ':', '-'). No request" >&2
  echo "       was made." >&2
  return 1
}

# `curl` with the api key fed through a `--config -` document on stdin.
#
# The caller must include `--config -` in its arguments; without it curl never
# reads the document and the request goes out unauthenticated, which surfaces as
# a puzzling 401. Checked rather than left to the comment.
ecw__curl_with_key() { # curl-args...
  # Walked argument by argument rather than matched against `" $* "`, because `$*`
  # joins on the first character of `IFS` and this function must not care what a
  # caller's `IFS` happens to be.
  #
  # Concretely: `IFS='|' read -r … <<<"$(ecw_request …)"` is one command, and the
  # prefix assignment applies to its expansions too — so `ecw_request` runs with
  # `IFS='|'` and the joined form came out `--config|-`, refusing the request.
  # `test-ecw-ops.sh` uses that one-line shape and caught it; the suites happen to
  # assign `LINE="$(ecw_request …)"` first and read on a separate line, so they
  # were unaffected. Which of the two shapes a caller picks is not something this
  # library should depend on.
  local arg saw_config=0
  for arg in "$@"; do
    if [[ "$arg" == "--config" ]]; then
      saw_config=1
    elif ((saw_config)) && [[ "$arg" == "-" ]]; then
      saw_config=2
      break
    else
      saw_config=0
    fi
  done
  if ((saw_config != 2)); then
    echo "ERROR: ecw__curl_with_key called without '--config -'; the api key" >&2
    echo "       would not be read and the request would be unauthenticated." >&2
    return 1
  fi
  printf 'header = "x-api-key: %s"\n' "$E2B_API_KEY" | curl "$@"
}

ecw_request() { # method path [body]
  local method="$1" path="$2" body="${3:-}"
  ecw__key_is_safe || return 1
  local hdr body_file code retry fields
  # Under `${TMPDIR}`, and removed at the end of the function. A signal between
  # the two `mktemp`s and the `rm -f` leaks them; `fire_concurrent` has one pair
  # per concurrent request in flight, so an interrupted burst leaves a handful of
  # empty temp files. Not trapped here because a `trap` in a library function
  # would replace the *caller's* EXIT handler, which is where the bench revert
  # lives — leaking two empty files is much cheaper than that.
  hdr="$(mktemp)"
  body_file="$(mktemp)"
  local args=(-sS -m "${ECW_CURL_TIMEOUT_SECS:-30}" -o "$body_file" -D "$hdr"
    -w '%{http_code}'
    -X "$method" "$API_URL$path"
    -H "Host: $ECW_API_HOST"
    -H 'content-type: application/json')
  [[ -n "${ECW_CLIENT_ID:-}" ]] && args+=(-H "x-client-id: $ECW_CLIENT_ID")
  [[ -n "$body" ]] && args+=(-d "$body")
  # The api key goes in on **stdin**, never in argv. Every process on the box can
  # read `/proc/<pid>/cmdline`, so `-H "x-api-key: $E2B_API_KEY"` published the
  # run-scoped credential to any other local user for the life of each request —
  # which would undo the whole point of minting it unpredictably.
  args+=(--config -)
  # Assigned, then defaulted on failure. `$(curl … || echo 000)` *concatenates*:
  # a transfer error after the status was written yields `200000`, which an
  # unanchored match reads as success.
  code="$(ecw__curl_with_key "${args[@]}" 2>/dev/null)" || code=000
  [[ -n "$code" ]] || code=000
  # Matched on the whole lower-cased line, not on `$1`. Awk's default field
  # split is on whitespace, so a `$1` test also requires a space after the colon
  # — and `Retry-After:7` with no optional whitespace is legal (RFC 9110). The
  # suites hard-fail on a missing Retry-After, so both the casing and the spacing
  # have to be tolerated or a conforming proxy turns a passing assertion red.
  retry="$(awk 'tolower($0) ~ /^retry-after:/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }' \
    "$hdr" | tr -d '\r')"
  # Digits only. `Retry-After` is delta-seconds here, and an unvalidated header
  # value carrying a `|` would shift every later field — putting a fragment of it
  # into the caller's sandbox id, which is the same failure the message-last
  # ordering below exists to prevent. Dropped rather than passed through, so a
  # caller asserting on it fails loudly instead of parsing nonsense.
  [[ "$retry" =~ ^[0-9]+$ ]] || retry=""
  # One parse of the body for both fields; `@tsv` keeps an empty field empty
  # rather than collapsing the pair.
  fields="$(jq -r '[.sandboxID // "", .message // ""] | @tsv' "$body_file" 2>/dev/null)" || fields=""
  # `jq` on an empty body prints nothing and exits 0, which would emit a
  # three-field line and break the shape this function documents.
  [[ -n "$fields" ]] || fields="$(printf '\t')"
  rm -f "$hdr" "$body_file"
  printf '%s|%s|%s\n' "$code" "$retry" "$(tr '\t' '|' <<<"$fields")"
}

# The ECW delete, which needs no idempotency key — unlike the native
# `DELETE /v1/sandboxes/<id>`, where omitting one returns
# `400 invalid_request: Idempotency-Key header is required` and silently leaks
# the sandbox pod. Retried, because a delete issued shortly after a state change
# comes back as a retryable conflict.
#
# 0 when the API **accepted** the delete (2xx) or the sandbox is already absent
# (404) — not proof the pod is gone. A caller that needs that has to wait for it;
# `smoke-kind-node-selector-extras.sh` uses `kubectl wait --for=delete`.
#
# Bounded on wall clock, not on an attempt count: each attempt spends a `curl`
# timeout plus a sleep, so a count advertises a duration several times shorter
# than it takes.
ecw_delete_sandbox() { # sandbox-id
  local code deadline remaining bound
  # Before the retry loop, so a locally-rejected key fails immediately instead of
  # spinning out the whole deadline on requests that were never sent.
  ecw__key_is_safe || return 1
  deadline=$((SECONDS + ${ECW_DELETE_DEADLINE_SECS:-120}))
  while :; do
    # Each attempt's own timeout is clamped to what is left, so the deadline is
    # the whole bound rather than the bound plus one request. A deadline that
    # only reserved the sleep would overrun by a full `-m`.
    remaining=$((deadline - SECONDS))
    ((remaining > 0)) || break
    bound=15
    ((remaining < bound)) && bound=$remaining
    # Key on stdin, not in argv — same reason as `ecw_request`: every local user
    # can read `/proc/<pid>/cmdline`, and this path runs repeatedly in a retry
    # loop, so it is the longest-lived exposure of the two.
    code="$(ecw__curl_with_key -sS -m "$bound" -o /dev/null -w '%{http_code}' \
      -X DELETE "$API_URL/sandboxes/$1" \
      -H "Host: $ECW_API_HOST" --config - 2>/dev/null)" || code=000
    # Anchored: an unanchored `^(2|404)` also matches the `200000` a status-then-
    # transfer-error produces.
    [[ "$code" =~ ^(2[0-9][0-9]|404)$ ]] && return 0
    ((SECONDS + 3 < deadline)) || break
    sleep 3
  done
  echo "ERROR: DELETE of sandbox $1 did not succeed within" \
    "${ECW_DELETE_DEADLINE_SECS:-120}s (last HTTP ${code:-000})" >&2
  return 1
}
