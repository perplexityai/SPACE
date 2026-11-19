#!/usr/bin/env bash
# Rejects changed SQLite up migrations that the release before them can't run
# on; db/README.md, "Rollback-safe SQLite migrations", has the rules and the
# exception marker. scripts/check_sqlite_migrations_rollback.py compares each
# affected store's schema before and after the change.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

base_ref() {
  local base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
  printf '%s\n' "${MIGRATION_SQLITE_ROLLBACK_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-origin/${base_branch}}}"
}

ensure_base_ref() {
  local base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}"
  local override="${MIGRATION_SQLITE_ROLLBACK_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-}}"

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
    if [[ -n "${BUILDKITE_BUILD_ID:-}" ]]; then
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
    if [[ -n "${BUILDKITE_BUILD_ID:-}" ]]; then
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

# Fail closed in CI when no base ref could be established: the diff would
# narrow to HEAD's own commit and miss a migration added earlier on the branch.
# Unlike the other migration guards, there is no merge-queue exemption: the
# merge queue passes its group's base, and a rollback-unsafe migration must not
# merge there either. Offline local runs still degrade to the WARNs.
require_base_ref_in_ci() {
  [[ -n "${BUILDKITE_BUILD_ID:-}" ]] || return 0
  local ref
  ref="$(base_ref)"
  git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null && return 0

  echo "ERROR: no base ref to compare SQLite migrations against; wanted ${ref}." >&2
  echo "  If a WARN above names a fetch failure, that is the cause; otherwise no" >&2
  echo "  remote is configured in this checkout. To pin a base explicitly, set" >&2
  echo "  MIGRATION_SQLITE_ROLLBACK_BASE_REF to a ref that resolves here." >&2
  exit 1
}

# The commit changed files are compared with: the merge base with the base
# ref, the ref itself without one, HEAD^ without a ref, or none.
diff_base() {
  local ref
  ref="$(base_ref)"
  if git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    if git merge-base "${ref}" HEAD 2>/dev/null; then
      return 0
    fi
    echo "WARN: no merge-base with ${ref}; using a direct tree diff for changed SQLite migrations," \
      "which can include files inherited from the base." >&2
    git rev-parse "${ref}^{commit}"
  elif git rev-parse --verify --quiet HEAD^ >/dev/null; then
    echo "WARN: ${ref} is unresolvable; inspecting only HEAD's own changes," \
      "so SQLite migrations changed earlier on this branch are not checked." >&2
    git rev-parse HEAD^
  else
    echo "WARN: could not establish a committed diff base for changed SQLite migrations." >&2
  fi
}

# Lists SQLite up migrations changed since <base>, committed or not, in every
# db/<store>/ catalog but db/postgres, db/common included. Rename detection is
# off, so a file moved into a catalog is listed as an addition, and one moved
# out of it as a deletion. <filter> is AMT for added or modified files and D
# for deleted ones.
sqlite_migration_files() {
  local base="$1" filter="$2"
  {
    if [[ -n "${base}" ]]; then
      git -c core.quotePath=false diff --name-only --no-renames --diff-filter="${filter}" \
        "${base}" HEAD -- 'db/*/*.up.sql' || exit 1
    fi
    git -c core.quotePath=false status --porcelain --no-renames --untracked-files=all \
      -- 'db/*/*.up.sql' \
      | while IFS= read -r entry; do
          local status="${entry:0:2}" file="${entry:3}"
          if [[ "${filter}" == D ]]; then
            if [[ "${status:0:1}" == D || "${status:1:1}" == D ]]; then
              printf '%s\n' "${file}"
            fi
          elif [[ "${status}" == "??" || "${status:0:1}" =~ [AMT] || "${status:1:1}" =~ [MT] ]]; then
            printf '%s\n' "${file}"
          fi
        done || exit 1
  } | while IFS= read -r file; do
    [[ "${file}" =~ ^db/[^/]+/[^/]+\.up\.sql$ ]] || continue
    [[ "${file}" != db/postgres/* ]] || continue
    printf '%s\n' "${file}"
  done
}

if ! ensure_base_ref; then
  exit 1
fi
require_base_ref_in_ci

base="$(diff_base)"
# Collected first, so a git command that fails fails the check rather than
# reading as no changes.
changed="$(sqlite_migration_files "${base}" AMT)"
deleted="$(sqlite_migration_files "${base}" D)"
# A symlink or other non-regular file in a catalog is rejected, not skipped:
# a dangling one would otherwise read as a migration the change removed
# without the deletion check seeing it.
files=()
irregular=()
while IFS= read -r file; do
  if [[ -z "${file}" ]]; then
    continue
  elif [[ -L "${file}" || (-e "${file}" && ! -f "${file}") ]]; then
    irregular+=("${file}")
  elif [[ -f "${file}" ]]; then
    files+=("${file}")
  fi
done < <(sort -u <<<"${changed}")
# Only a file the base has: one the change itself added and then deleted
# was never merged.
removed=()
while IFS= read -r file; do
  if [[ -n "${file}" && -n "${base}" && ! -e "${file}" ]] \
    && git cat-file -e "${base}:${file}" 2>/dev/null; then
    removed+=("${file}")
  fi
done < <(sort -u <<<"${deleted}")

if ((${#files[@]} == 0 && ${#removed[@]} == 0 && ${#irregular[@]} == 0)); then
  echo "No changed SQLite migrations to check for rollback safety."
  exit 0
fi

failed=0
for file in "${irregular[@]}"; do
  failed=1
  echo "ERROR: ${file}: a migration must be a regular file, not a symlink or another kind of entry." >&2
done
for file in "${removed[@]}"; do
  failed=1
  echo "ERROR: ${file}: deleted or renamed, but the base has it." >&2
  echo "  A merged migration must be assumed already applied wherever main is deployed," >&2
  echo "  and a release that doesn't bundle an applied migration refuses the database." >&2
  echo "  Keep the file, and undo its change with a forward migration, as" >&2
  echo "  db/sandbox-manager/20261113190000_restore_sandbox_legacy_state.up.sql does." >&2
  echo "  No marker exempts this. See db/README.md, \"Rollback-safe SQLite migrations\"." >&2
done
if ((${#files[@]} > 0)); then
  echo "Checking changed SQLite migrations for rollback safety against ${base:-no base}:"
  printf '  %s\n' "${files[@]}"
  python3 "${repo_root}/scripts/check_sqlite_migrations_rollback.py" --base "${base}" "${files[@]}" \
    || failed=1
fi
if ((failed)); then
  exit 1
fi
echo "Changed SQLite migrations are rollback-safe."
