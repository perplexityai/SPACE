#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

base_ref() {
  local base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
  printf '%s\n' "${MIGRATION_SQUAWK_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-origin/${base_branch}}}"
}

ensure_base_ref() {
  local base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
  local override="${MIGRATION_SQUAWK_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-}}"

  # Kept in step with scripts/check-migration-versions.sh, which carries the
  # full rationale. In short: freshen every run rather than only when the ref is
  # absent, because a persistent CI agent can carry a present-but-stale
  # `origin/<base>` and the stale case is the one that misleads the diff. Skip
  # only when there is no remote to fetch from (the self-tests' synthetic repos),
  # never merely because a base ref was pinned — a pin is usually a
  # remote-tracking ref, which is exactly what goes stale.
  local override_branch="${override#origin/}"
  if [[ -n "${override}" ]]; then
    if [[ "${override}" == origin/* ]] \
      && [[ -n "${override_branch}" ]] \
      && [[ "${override_branch}" != HEAD ]] \
      && [[ "${override_branch}" != *[~^:]* ]] \
      && [[ "${override_branch}" != *"@{"* ]] \
      && git check-ref-format --branch "${override_branch}" >/dev/null 2>&1; then
      base_branch="${override_branch}"
    else
      # A raw SHA, `upstream/main`, `HEAD~1`: no branch to derive. Fetching the
      # default base instead would force-update a ref nobody asked about — in a
      # ref store shared with every sibling worktree — without making the
      # requested base any more resolvable. Leave the pin alone.
      return 0
    fi
  fi

  if ! git remote get-url origin >/dev/null 2>&1; then
    if [[ -n "${BUILDKITE_BUILD_ID:-}" ]] \
      && [[ "${BUILDKITE_BRANCH:-}" != trunk-merge/* ]]; then
      echo "ERROR: origin remote is not configured; cannot verify" \
        "$(base_ref) is current in CI." >&2
      return 1
    fi
    return 0
  fi

  # `+refs/heads/<base>` in full: the `+` forces a diverged update (a plain
  # fetch refuses it), and the qualified source stops git deleting the
  # destination and failing to recreate it. A second `--deepen=50` fetch runs
  # only when a shallow checkout still lacks a merge base after the refresh.
  # Preserve Buildkite-provided askpass and SSH authentication while keeping
  # terminal prompts disabled. Local runs still clear askpass helpers so an IDE
  # integration cannot open a UI or hang the guard. The version guard carries
  # the full precedence rationale.
  local configured_ssh_command
  configured_ssh_command="$(git config --get core.sshCommand 2>/dev/null || true)"

  local -a fetch_env=(env GIT_TERMINAL_PROMPT=0)
  local -a git_command=(git)
  if [[ -z "${BUILDKITE_BUILD_ID:-}" ]]; then
    fetch_env+=(GIT_ASKPASS= SSH_ASKPASS=)
    git_command+=(-c core.askPass=)
  fi
  if [[ -z "${GIT_SSH_COMMAND:-}" ]] \
    && [[ -z "${configured_ssh_command}" ]] \
    && [[ -z "${GIT_SSH:-}" ]] \
    && { [[ -z "${BUILDKITE_BUILD_ID:-}" ]] || [[ -z "${SSH_ASKPASS:-}" ]]; }; then
    fetch_env+=(GIT_SSH_COMMAND="ssh -oBatchMode=yes")
  fi

  local fetch_timeout_secs="${MIGRATION_GUARD_FETCH_TIMEOUT_SECS:-60}"
  local timeout_bin=""
  local fetch_failure_reason=""
  local candidate
  for candidate in timeout gtimeout; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      timeout_bin="$(command -v "${candidate}")"
      break
    fi
  done
  if [[ ! "${fetch_timeout_secs}" =~ ^[1-9][0-9]*$ ]]; then
    fetch_failure_reason="the configured fetch timeout is invalid"
  elif [[ -z "${timeout_bin}" ]]; then
    fetch_failure_reason="timeout/gtimeout is unavailable"
  fi

  local -a timeout_command=()
  if [[ -z "${fetch_failure_reason}" ]]; then
    timeout_command=("${timeout_bin}" --kill-after=5s "${fetch_timeout_secs}s")
  fi
  local -a fetch_command=("${git_command[@]}" fetch --no-tags origin)

  if [[ -z "${fetch_failure_reason}" ]] \
    && ! "${timeout_command[@]}" "${fetch_env[@]}" "${fetch_command[@]}" \
    "+refs/heads/${base_branch}:refs/remotes/origin/${base_branch}" \
    >/dev/null 2>&1; then
    fetch_failure_reason="git fetch failed or timed out"
  fi
  if [[ -n "${fetch_failure_reason}" ]]; then
    # Git and remote helpers can include credential-bearing URLs in stderr.
    echo "WARN: could not freshen origin/${base_branch};" \
      "${fetch_failure_reason}." >&2
    if [[ -n "${BUILDKITE_BUILD_ID:-}" ]] \
      && [[ "${BUILDKITE_BRANCH:-}" != trunk-merge/* ]]; then
      if git rev-parse --verify --quiet "$(base_ref)^{commit}" >/dev/null; then
        echo "ERROR: could not verify origin/${base_branch} is current in CI." >&2
        echo "  Refusing to compare against a cached ref after refresh failed; it" >&2
        echo "  may be stale and can hide or invent changes relative to the real base." >&2
        return 1
      fi
      # Let the no-base gate below report the more specific missing-ref error.
      return 0
    fi
    echo "WARN: comparing against the cached ref, which may be stale." >&2
    return 0
  fi

  if git merge-base "$(base_ref)" HEAD >/dev/null 2>&1 \
    || [[ "$(git rev-parse --is-shallow-repository 2>/dev/null || true)" != true ]]; then
    return 0
  fi

  local -a deepen_command=("${git_command[@]}" fetch --no-tags --deepen=50 origin)
  if ! "${timeout_command[@]}" "${fetch_env[@]}" "${deepen_command[@]}" \
    "+refs/heads/${base_branch}:refs/remotes/origin/${base_branch}" \
    >/dev/null 2>&1; then
    echo "WARN: could not deepen history for origin/${base_branch};" \
      "the direct tree fallback may include inherited files." >&2
  fi
}

# Fail closed in CI when no base ref could be established. Without one the diff
# narrows to HEAD's own commit, so migrations added in an earlier commit are never linted.
# A guard that cannot compare must not report success. Scoped to CI: offline
# local runs still degrade to the WARN above and stay usable.
#
# Called from the main shell on purpose — the consumer below runs inside a
# process substitution, where a non-zero return could not stop the script.
require_base_ref_in_ci() {
  is_git_worktree || return 0
  [[ -n "${BUILDKITE_BUILD_ID:-}" ]] || return 0
  # Exempt Trunk merge-queue builds, as check-postgres-migrations-isolated.sh
  # does: BUILDKITE_PULL_REQUEST is false there so the host-side pre-command
  # hook never fetches a base, and the batched PRs were each guarded already.
  [[ "${BUILDKITE_BRANCH:-}" != trunk-merge/* ]] || return 0

  local ref
  ref="$(base_ref)"
  git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null && return 0

  echo "ERROR: no base ref to compare against; wanted ${ref}." >&2
  echo "  Refusing to report success in CI with no base: the diff would narrow" >&2
  echo "  to HEAD alone and miss anything added in an earlier commit." >&2
  echo "  If a WARN above names a fetch failure, that is the cause; otherwise no" >&2
  echo "  remote is configured in this checkout." >&2
  echo "  To pin a base explicitly, set MIGRATION_SQUAWK_BASE_REF to a ref" >&2
  echo "  that resolves in this checkout." >&2
  exit 1
}

is_git_worktree() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1
}

sapling_changed_postgres_migration_files() {
  if ! command -v sl >/dev/null 2>&1 || ! sl root >/dev/null 2>&1; then
    echo "WARN: not in a Git worktree and Sapling is unavailable; cannot discover changed Postgres migrations." >&2
    return 0
  fi

  sl status \
    | while read -r status file; do
        if [[ "${file}" != db/postgres/*.up.sql && "${file}" != db/postgres/*.down.sql ]]; then
          continue
        fi
        if [[ "${status}" == "?" || "${status}" == "A" || "${status}" == "M" ]]; then
          printf '%s\n' "${file}"
        fi
      done
}

changed_postgres_migration_files() {
  if ! is_git_worktree; then
    sapling_changed_postgres_migration_files
    return 0
  fi

  local ref
  ref="$(base_ref)"

  if git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    if git merge-base "${ref}" HEAD >/dev/null 2>&1; then
      git diff --name-only --diff-filter=ACMRT "${ref}...HEAD" -- 'db/postgres/*.up.sql' 'db/postgres/*.down.sql'
    else
      echo "WARN: no merge-base with ${ref}; using a direct tree diff for changed Postgres migrations." >&2
      echo "      That diff reports every difference between the two trees, not" >&2
      echo "      what this branch added, so it can name files inherited from the" >&2
      echo "      base. A shallow checkout whose fork point is deeper than the" >&2
      echo "      fetch reached is the usual cause." >&2
      git diff --name-only --diff-filter=ACMRT "${ref}" HEAD -- 'db/postgres/*.up.sql' 'db/postgres/*.down.sql'
    fi
  elif git rev-parse --verify --quiet HEAD^ >/dev/null; then
    git diff --name-only --diff-filter=ACMRT HEAD^..HEAD -- 'db/postgres/*.up.sql' 'db/postgres/*.down.sql'
  else
    echo "WARN: could not establish a committed diff base for changed Postgres migrations." >&2
  fi

  git status --porcelain -- 'db/postgres/*.up.sql' 'db/postgres/*.down.sql' \
    | while read -r status file; do
        if [[ "${status}" == "??" || "${status:0:1}" == "A" || "${status:0:1}" == "M" || "${status:1:1}" == "M" ]]; then
          printf '%s\n' "${file}"
        fi
      done
}

run_squawk() {
  local pg_version="${SQUAWK_PG_VERSION:-17.0}"

  if [[ -n "${SQUAWK_BIN:-}" ]]; then
    "${SQUAWK_BIN}" --pg-version="${pg_version}" --assume-in-transaction "$@"
    return
  fi

  if command -v squawk >/dev/null 2>&1; then
    squawk --pg-version="${pg_version}" --assume-in-transaction "$@"
    return
  fi

  # Pinned: a squawk release can add default rules, and then fail a migration
  # that passed the day before.
  local uv_bin
  uv_bin="$(bash "${repo_root}/ci/scripts/ensure-uv.sh")"
  "${uv_bin}" tool run --from "${SQUAWK_CLI_PACKAGE:-squawk-cli==2.69.0}" squawk \
    --pg-version="${pg_version}" \
    --assume-in-transaction \
    "$@"
}

check_uuid_v4_policy() {
  local failed=0
  local file line_number line matches grep_status

  for file in "$@"; do
    if matches="$(grep -niE \
      -e 'gen_random_uuid[[:space:]]*\(' \
      -e 'uuid_generate_v4[[:space:]]*\(' \
      "${file}")"; then
      grep_status=0
    else
      grep_status=$?
    fi

    if ((grep_status == 1)); then
      continue
    fi
    if ((grep_status != 0)); then
      echo "ERROR: unable to scan ${file} for UUIDv4 generators." >&2
      return "${grep_status}"
    fi

    while IFS=: read -r line_number line; do
      [[ -n "${line_number}" ]] || continue
      if [[ "${line}" =~ space-allow-uuid-v4:[[:space:]]*[[:alnum:]] ]]; then
        continue
      fi

      echo "ERROR: ${file}:${line_number} generates UUIDv4 without a justified exception." >&2
      echo "  Durable SPACE-owned identifiers must use space_uuid_v7()." >&2
      echo "  UUIDv4 is allowed only when revealing creation time would violate a concrete" >&2
      echo "  privacy or protocol requirement. Document that reason on the same line with:" >&2
      echo "    /* space-allow-uuid-v4: <reason> */" >&2
      failed=1
    done <<<"${matches}"
  done

  return "${failed}"
}

# Freshen before the gate below reads the result. Both must happen in the main
# shell: the consumer runs inside a process substitution, so a fetch from there
# would land after the gate had already decided the base was unrecoverable.
if is_git_worktree; then
  if ! ensure_base_ref; then
    exit 1
  fi
fi

require_base_ref_in_ci

# The initial public schema preserves inherited indexes and constraints. Pin the
# reviewed SQL: subsequent edits must pass the ordinary migration safety rules.
is_public_baseline() {
  local file="$1"
  case "$file" in
    db/postgres/20261002070625_baseline.up.sql|db/postgres/20261002070625_baseline.down.sql) ;;
    *) return 1 ;;
  esac
  case "$file:$(git hash-object --no-filters -- "$file")" in
    db/postgres/20261002070625_baseline.up.sql:7f332552a634ba1894768d1ab5815590d32f49bc|\
    db/postgres/20261002070625_baseline.down.sql:26174049e4071299749ba668484d4997863b6e82) return 0 ;;
    *) return 1 ;;
  esac
}

# Existing DDL is not being deployed again when only comments change. Use the
# PostgreSQL lexer so comment markers inside string literals remain executable.
is_comment_only_migration() {
  local file="$1" base uv_bin
  base="$(git merge-base "$(base_ref)" HEAD 2>/dev/null)" || return 1
  git cat-file -e "${base}:${file}" 2>/dev/null || return 1
  uv_bin="$(bash "${repo_root}/ci/scripts/ensure-uv.sh")" || return 1
  git show "${base}:${file}" | "${uv_bin}" run --script \
    "${repo_root}/scripts/check_postgres_partial_indexes.py" --comment-only "${file}"
}

export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/space-uv-cache}"
export UV_TOOL_DIR="${UV_TOOL_DIR:-${TMPDIR:-/tmp}/space-uv-tools}"
changed_files=()
up_files=()
while IFS= read -r file; do
  [[ -n "${file}" ]] || continue
  [[ -f "${file}" ]] || continue
  if is_public_baseline "${file}"; then
    echo "Verified initial public baseline: ${file}"
    continue
  fi
  if is_comment_only_migration "${file}"; then
    echo "Verified unchanged executable SQL and transaction mode: ${file}"
    continue
  fi
  changed_files+=("${file}")
  if [[ "${file}" == *.up.sql ]]; then
    up_files+=("${file}")
  fi
done < <(changed_postgres_migration_files | sort -u)

if ((${#changed_files[@]} == 0)); then
  echo "No changed Postgres migrations to check."
  exit 0
fi

echo "Checking changed Postgres migrations:"
printf '  %s\n' "${changed_files[@]}"

if ((${#up_files[@]} != 0)); then
  check_uuid_v4_policy "${up_files[@]}"
fi

uv_bin="$(bash "${repo_root}/ci/scripts/ensure-uv.sh")"
"${uv_bin}" run --script "${repo_root}/scripts/check_postgres_partial_indexes.py" \
  "${changed_files[@]}"

if ((${#up_files[@]} != 0)); then
  run_squawk "${up_files[@]}"
fi
