#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

if [[ "${BUILDKITE_BRANCH:-}" == trunk-merge/* ]]; then
  echo "Trunk merge queue PRs were checked individually; migration isolation check passed."
  exit 0
fi

base_ref() {
  local base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
  printf '%s\n' "${MIGRATION_ISOLATION_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-origin/${base_branch}}}"
}

ensure_base_ref() {
  local base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
  local override="${MIGRATION_ISOLATION_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-}}"

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
# narrows to HEAD's own commit, so a PR whose earlier commit adds a migration and whose
# later commit edits runtime app code slips through the isolation gate.
# A guard that cannot compare must not report success. Scoped to CI: offline
# local runs still degrade to the WARN above and stay usable.
#
# Called from the main shell on purpose — the consumer below runs inside a
# process substitution, where a non-zero return could not stop the script.
require_base_ref_in_ci() {
  is_git_worktree || return 0
  [[ -n "${BUILDKITE_BUILD_ID:-}" ]] || return 0

  local ref
  ref="$(base_ref)"
  git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null && return 0

  echo "ERROR: no base ref to compare against; wanted ${ref}." >&2
  echo "  Refusing to report success in CI with no base: the diff would narrow" >&2
  echo "  to HEAD alone and miss anything added in an earlier commit." >&2
  echo "  If a WARN above names a fetch failure, that is the cause; otherwise no" >&2
  echo "  remote is configured in this checkout." >&2
  echo "  To pin a base explicitly, set MIGRATION_ISOLATION_BASE_REF to a ref" >&2
  echo "  that resolves in this checkout." >&2
  exit 1
}

is_git_worktree() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1
}

sapling_changed_files() {
  if ! command -v sl >/dev/null 2>&1 || ! sl root >/dev/null 2>&1; then
    echo "WARN: not in a Git worktree and Sapling is unavailable; cannot discover changed files." >&2
    return 0
  fi

  sl status \
    | while read -r status file; do
        if [[ "${status}" == "?" || "${status}" == "A" || "${status}" == "M" || "${status}" == "R" || "${status}" == "!" ]]; then
          printf '%s\n' "${file}"
        fi
      done
}

git_status_changed_files() {
  git status --porcelain \
    | while IFS= read -r line; do
        local status file
        status="${line:0:2}"
        file="${line:3}"
        if [[ "${file}" == *" -> "* ]]; then
          file="${file##* -> }"
        fi
        if [[ "${status}" == "??" || "${status:0:1}" =~ [AMDRCT] || "${status:1:1}" =~ [AMDRCT] ]]; then
          printf '%s\n' "${file}"
        fi
      done
}

comparison_base_commit=""
comparison_uses_merge_base=false

resolve_changed_files_base() {
  local ref
  ref="$(base_ref)"

  if git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    if comparison_base_commit="$(git merge-base "${ref}" HEAD 2>/dev/null)"; then
      comparison_uses_merge_base=true
    else
      comparison_base_commit="$(git rev-parse "${ref}^{commit}")"
    fi
    return 0
  fi

  git rev-parse --verify --quiet HEAD^ >/dev/null || return 1
  comparison_base_commit="$(git rev-parse HEAD^)"
}

changed_files() {
  if ! is_git_worktree; then
    sapling_changed_files
    return 0
  fi

  if [[ -n "${comparison_base_commit}" ]]; then
    if [[ "${comparison_uses_merge_base}" == true ]]; then
      git diff --name-only --diff-filter=ACMRTD "${comparison_base_commit}...HEAD"
    else
      echo "WARN: no merge-base with $(base_ref); using a direct tree diff for changed files." >&2
      echo "      That diff reports every difference between the two trees, not" >&2
      echo "      what this branch added, so it can name files inherited from the" >&2
      echo "      base. A shallow checkout whose fork point is deeper than the" >&2
      echo "      fetch reached is the usual cause." >&2
      git diff --name-only --diff-filter=ACMRTD "${comparison_base_commit}" HEAD
    fi
  else
    echo "WARN: could not establish a committed diff base for changed files." >&2
  fi

  git_status_changed_files
}

fence_lists_views() {
  local source="$1"
  local projection="$2"
  local entries="$3"
  local denylist_entries="$4"

  : >"${projection}"
  : >"${entries}"
  : >"${denylist_entries}"
  awk -v projection="${projection}" -v entries="${entries}" \
    -v denylist_entries="${denylist_entries}" '
    BEGIN {
      in_credential_tables = 0
      in_denylist = 0
      declarations = 0
      closes = 0
      denylist_declarations = 0
      denylist_closes = 0
    }

    /^const CREDENTIAL_TABLES: &\[&str\] = &\[$/ {
      declarations++
      in_credential_tables = 1
      print > projection
      next
    }

    /^const RO_COLUMN_DENYLIST: &\[\(&str, &\[&str\]\)\] = &\[$/ {
      denylist_declarations++
      in_denylist = 1
      print > projection
      next
    }

    in_credential_tables && /^\];$/ {
      closes++
      in_credential_tables = 0
      print > projection
      next
    }

    in_denylist && /^\];$/ {
      denylist_closes++
      in_denylist = 0
      print > projection
      next
    }

    in_credential_tables {
      if ($0 !~ /^[[:space:]]*"[a-z_][a-z0-9_]*",[[:space:]]*$/) {
        invalid = 1
        exit
      }
      entry = $0
      sub(/^[[:space:]]*"/, "", entry)
      sub(/",[[:space:]]*$/, "", entry)
      if (seen[entry]++) {
        invalid = 1
        exit
      }
      print entry > entries
      next
    }

    # One canonical `    ("table", &["column", ...]),` tuple per line. The
    # projection is written as `table column...` so the shell can compare the
    # columns fenced for each table.
    in_denylist {
      if ($0 !~ /^    \("[a-z_][a-z0-9_]*", &\[("[a-z_][a-z0-9_]*", )*"[a-z_][a-z0-9_]*"\]\),$/) {
        invalid = 1
        exit
      }
      entry = $0
      sub(/^    \("/, "", entry)
      sub(/"\]\),$/, "", entry)
      gsub(/", &\["/, " ", entry)
      gsub(/", "/, " ", entry)
      split(entry, fields, " ")
      if (denylist_seen[fields[1]]++) {
        invalid = 1
        exit
      }
      print entry > denylist_entries
      next
    }

    { print > projection }

    END {
      if (invalid || declarations != 1 || closes != 1 || in_credential_tables \
        || denylist_declarations != 1 || denylist_closes != 1 || in_denylist) {
        exit 1
      }
    }
  ' "${source}"
}

credential_table_is_dropped() {
  relation_is_dropped "$1" TABLE
}

# A denylist entry fences a table or, while a rename keeps a compatibility
# view under the old name, a view. Either may be the relation a migration drops.
denylisted_relation_is_dropped() {
  relation_is_dropped "$1" "(TABLE|VIEW)"
}

relation_is_dropped() {
  local table="$1"
  local kind="$2"
  local executable_line_count
  local migration

  for migration in "${postgres_migrations[@]}"; do
    [[ "${migration}" == *.up.sql && -f "${migration}" ]] || continue
    executable_line_count="$(
      awk 'NF && $1 !~ /^--/ { count++ } END { print count + 0 }' "${migration}"
    )"
    ((executable_line_count == 1)) || continue
    if grep -Eiq \
      "^[[:space:]]*DROP[[:space:]]+${kind}[[:space:]]+(IF[[:space:]]+EXISTS[[:space:]]+)?(\"?public\"?[[:space:]]*\.[[:space:]]*)?\"?${table}\"?[[:space:]]*(CASCADE|RESTRICT)?[[:space:]]*;[[:space:]]*(--.*)?$" \
      "${migration}"; then
      return 0
    fi
  done

  return 1
}

# Compare RO_COLUMN_DENYLIST projections. Adding a table or adding columns to
# an existing entry only tightens the space_ro fence, so it may travel with the
# migration that creates the column. Removing a denied column never may, and
# removing a whole entry requires a standalone migration that drops that exact
# relation. Prints `changed` when the entry set differs.
compare_ro_column_denylist() {
  local base_entries="$1"
  local current_entries="$2"
  local -A base_columns=()
  local -A current_columns=()
  local table
  local columns
  local column
  local changed=0
  local result=0

  while read -r table columns; do
    [[ -n "${table}" ]] || continue
    base_columns["${table}"]=" ${columns} "
  done <"${base_entries}"
  while read -r table columns; do
    [[ -n "${table}" ]] || continue
    current_columns["${table}"]=" ${columns} "
  done <"${current_entries}"

  for table in "${!current_columns[@]}"; do
    if [[ -z "${base_columns[${table}]+set}" ]]; then
      changed=1
      continue
    fi
    for column in ${base_columns[${table}]}; do
      if [[ "${current_columns[${table}]}" != *" ${column} "* ]]; then
        echo "ERROR: RO_COLUMN_DENYLIST may not stop fencing ${table}.${column}" >&2
        echo "  in a migration PR." >&2
        result=1
      fi
    done
    for column in ${current_columns[${table}]}; do
      [[ "${base_columns[${table}]}" == *" ${column} "* ]] || changed=1
    done
  done

  for table in "${!base_columns[@]}"; do
    [[ -z "${current_columns[${table}]+set}" ]] || continue
    changed=1
    if ! denylisted_relation_is_dropped "${table}"; then
      echo "ERROR: RO_COLUMN_DENYLIST entry ${table} may be removed only when" >&2
      echo "  a changed Postgres up migration drops that exact table or view." >&2
      result=1
    fi
  done

  ((changed == 0)) || echo changed
  return "${result}"
}

is_credential_tables_only_change() {
  local file="$1"
  local check_dir
  local removed_table
  local denylist_change
  local result=1

  if ! is_git_worktree; then
    echo "ERROR: the CREDENTIAL_TABLES exemption requires a Git worktree." >&2
    return 1
  fi
  if [[ -z "${comparison_base_commit}" ]]; then
    echo "ERROR: the CREDENTIAL_TABLES exemption requires a resolvable diff base." >&2
    return 1
  fi
  if [[ ! -f "${file}" ]]; then
    echo "ERROR: the CREDENTIAL_TABLES exemption does not permit deleting ${file}." >&2
    return 1
  fi

  # An exact-path exemption would allow arbitrary deployable role changes.
  # The projections compare every source line outside the two canonical fence
  # list bodies (CREDENTIAL_TABLES and RO_COLUMN_DENYLIST). Canonicalized entry
  # sets distinguish real fence changes from formatting or ordering edits;
  # removals additionally require the matching relation drop.
  check_dir="$(mktemp -d)" || return 1
  [[ -n "${check_dir}" && -d "${check_dir}" ]] || return 1
  if ! git show "${comparison_base_commit}:${file}" >"${check_dir}/base.rs"; then
    echo "ERROR: ${file} must exist at the migration diff base." >&2
  elif ! fence_lists_views "${check_dir}/base.rs" \
    "${check_dir}/base-projection" "${check_dir}/base-entries" \
    "${check_dir}/base-denylist"; then
    echo "ERROR: the base ${file} has an invalid CREDENTIAL_TABLES declaration." >&2
  elif ! fence_lists_views "${file}" \
    "${check_dir}/current-projection" "${check_dir}/current-entries" \
    "${check_dir}/current-denylist"; then
    echo "ERROR: cannot verify a CREDENTIAL_TABLES-only change in ${file}." >&2
    # shellcheck disable=SC2016  # Rust syntax is intentionally literal.
    echo '  Keep exactly one multiline `const CREDENTIAL_TABLES: &[&str] = &[` declaration' >&2
    # shellcheck disable=SC2016  # Rust syntax is intentionally literal.
    echo '  whose body contains only canonical `    "table_name",` entries and a `];` close,' >&2
    # shellcheck disable=SC2016  # Rust syntax is intentionally literal.
    echo '  and one `const RO_COLUMN_DENYLIST` declaration of canonical' >&2
    # shellcheck disable=SC2016  # Rust syntax is intentionally literal.
    echo '  `    ("table", &["column"]),` entries.' >&2
  elif ! cmp -s "${check_dir}/base-projection" "${check_dir}/current-projection" \
    || ! cmp -s <(tail -c 1 "${check_dir}/base.rs") <(tail -c 1 "${file}"); then
    echo "ERROR: ${file} changes content outside CREDENTIAL_TABLES entries" >&2
    echo "  or RO_COLUMN_DENYLIST entries." >&2
  else
    LC_ALL=C sort "${check_dir}/base-entries" >"${check_dir}/base-entries-sorted"
    LC_ALL=C sort "${check_dir}/current-entries" >"${check_dir}/current-entries-sorted"
    comm -13 "${check_dir}/base-entries-sorted" \
      "${check_dir}/current-entries-sorted" >"${check_dir}/added-entries"
    comm -23 "${check_dir}/base-entries-sorted" \
      "${check_dir}/current-entries-sorted" >"${check_dir}/removed-entries"

    result=0
    if ! denylist_change="$(compare_ro_column_denylist \
      "${check_dir}/base-denylist" "${check_dir}/current-denylist")"; then
      result=1
    fi

    if [[ ! -s "${check_dir}/added-entries" && ! -s "${check_dir}/removed-entries" \
      && -z "${denylist_change}" ]]; then
      echo "ERROR: ${file} does not add or remove CREDENTIAL_TABLES entries" >&2
      echo "  or RO_COLUMN_DENYLIST entries;" >&2
      echo "  formatting-only and ordering-only changes are not allowed." >&2
      result=1
    fi
    while IFS= read -r removed_table; do
      [[ -n "${removed_table}" ]] || continue
      if credential_table_is_dropped "${removed_table}"; then
        continue
      fi
      echo "ERROR: CREDENTIAL_TABLES entry ${removed_table} may be removed only when" >&2
      echo "  a changed Postgres up migration drops that exact table." >&2
      result=1
    done <"${check_dir}/removed-entries"
  fi

  rm -rf -- "${check_dir}"
  return "${result}"
}

is_postgres_bootstrap_test_append() {
  local file="$1"
  local check_dir
  local base_size
  local result=1

  if ! is_credential_tables_only_change \
    "crates/libs/db/migrator/src/bootstrap_roles.rs"; then
    echo "ERROR: ${file} is allowed only with a valid CREDENTIAL_TABLES change." >&2
    return 1
  fi
  [[ -n "${comparison_base_commit}" && -f "${file}" ]] || return 1

  check_dir="$(mktemp -d)" || return 1
  [[ -n "${check_dir}" && -d "${check_dir}" ]] || return 1
  if git show "${comparison_base_commit}:${file}" >"${check_dir}/base.rs"; then
    base_size="$(wc -c <"${check_dir}/base.rs")"
    if (( $(wc -c <"${file}") <= base_size )) \
      || ! head -c "${base_size}" "${file}" | cmp -s "${check_dir}/base.rs" -; then
      echo "ERROR: ${file} may only append regression coverage; existing" >&2
      echo "  PostgreSQL bootstrap-role tests must remain byte-for-byte unchanged." >&2
    else
      tail -c "+$((base_size + 1))" "${file}" >"${check_dir}/appended.rs"
      if grep -Eq \
        -e '^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?(const|static|type|mod|struct|enum|trait|union|fn)[[:space:]]+(CREDENTIAL_TABLES|RO_COLUMN_DENYLIST)([^[:alnum:]_]|$)' \
        -e '^[[:space:]]*use[[:space:]].*(CREDENTIAL_TABLES|RO_COLUMN_DENYLIST)([^[:alnum:]_]|$)' \
        "${check_dir}/appended.rs"; then
        echo "ERROR: appended bootstrap-role tests may not shadow or replace fence constants." >&2
      else
        result=0
      fi
    fi
  else
    echo "ERROR: ${file} must exist at the migration diff base." >&2
  fi

  rm -rf -- "${check_dir}"
  return "${result}"
}

is_postgres_migration() {
  local file="$1"
  [[ "${file}" == db/postgres/*.sql ]]
}

# Reuse the Squawk guard's PostgreSQL lexer: edits to existing comments do not
# deploy DDL, while string literals and SQLx's transaction directive still count.
is_comment_only_migration() {
  local file="$1" uv_bin
  [[ "${comparison_uses_merge_base}" == true && -f "${file}" ]] || return 1
  git cat-file -e "${comparison_base_commit}:${file}" 2>/dev/null || return 1
  uv_bin="$(bash "${repo_root}/ci/scripts/ensure-uv.sh")" || return 1
  source "${repo_root}/ci/scripts/lib/uv-run.sh"
  git show "${comparison_base_commit}:${file}" | space_uv_run "${uv_bin}" --script -- \
    "${repo_root}/scripts/check_postgres_partial_indexes.py" --comment-only "${file}"
}

is_allowed_with_migrations() {
  local file="$1"

  # shellcheck disable=SC2221,SC2222  # README.* intentionally overlaps *.md.
  case "${file}" in
    *.md|README|README.*|AGENTS.md|docs/*)
      return 0
      ;;
    db/postgres/*.sql|db/README.md)
      return 0
      ;;
    crates/libs/db/migrator/src/bootstrap_roles.rs)
      is_credential_tables_only_change "${file}"
      return
      ;;
    crates/libs/db/migrator/src/bootstrap_roles/tests/postgres_bootstrap_tests.rs)
      is_postgres_bootstrap_test_append "${file}"
      return
      ;;
    .buildkite/pipeline.yml|.buildkite/pipeline.*.yml)
      return 0
      ;;
    scripts/check-migration-versions.sh|scripts/check-postgres-migrations-*.sh)
      return 0
      ;;
    scripts/tests/test-check-migration-versions.sh|scripts/tests/test-check-postgres-migrations-*.sh)
      return 0
      ;;
    scripts/tests/*)
      return 0
      ;;
  esac

  return 1
}

is_runtime_app_change() {
  local file="$1"

  if is_allowed_with_migrations "${file}"; then
    return 1
  fi

  case "${file}" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml)
      return 0
      ;;
    crates/*|proto/*|shim/*|ui/*|docker/*)
      return 0
      ;;
    infra/space/*)
      return 0
      ;;
    .buildkite/scripts/*|ci/scripts/*)
      return 0
      ;;
    scripts/space-*.sh|scripts/space-deploy.py)
      return 0
      ;;
  esac

  return 1
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

if is_git_worktree; then
  resolve_changed_files_base || true
fi

postgres_migrations=()
runtime_changes=()
all_changed_files=()
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/space-uv-cache}"

while IFS= read -r file; do
  [[ -n "${file}" ]] || continue
  all_changed_files+=("${file}")
  if is_postgres_migration "${file}"; then
    if is_comment_only_migration "${file}"; then
      echo "Verified unchanged executable SQL and transaction mode: ${file}"
      continue
    fi
    postgres_migrations+=("${file}")
  fi
done < <(changed_files | sort -u)

if ((${#postgres_migrations[@]} == 0)); then
  echo "No Postgres migrations changed; migration isolation check passed."
  exit 0
fi

for file in "${all_changed_files[@]}"; do
  if ! is_postgres_migration "${file}" && is_runtime_app_change "${file}"; then
    runtime_changes+=("${file}")
  fi
done

if ((${#runtime_changes[@]} == 0)); then
  echo "Postgres migrations changed without runtime app changes; migration isolation check passed."
  exit 0
fi

echo "ERROR: split Postgres migrations from runtime app changes." >&2
echo "  Postgres migrations should land in a PR that does not also change deployable app/runtime code." >&2
echo "  This keeps migration lock risk, rollback, and rollout review separate from behavior changes." >&2
echo >&2
echo "  Changed Postgres migration file(s):" >&2
printf '    %s\n' "${postgres_migrations[@]}" >&2
echo >&2
echo "  Runtime app file(s) changed in the same diff:" >&2
printf '    %s\n' "${runtime_changes[@]}" >&2
exit 1
