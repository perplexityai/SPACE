#!/usr/bin/env bash
# Rejects changed SQLite up migrations that the release before them can't run
# on; db/README.md, "Rollback-safe SQLite migrations", has the rules and the
# exception marker. scripts/check_sqlite_migrations_rollback.py compares each
# affected store's schema before and after the change.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# shellcheck source=lib/migration-base.sh
source "${repo_root}/scripts/lib/migration-base.sh"
# Its own pin first, then the version guard's: CI sets both to the same base.
# Strict in the merge queue too; see require_base_ref_in_ci.
migration_base_configure \
  "${MIGRATION_SQLITE_ROLLBACK_BASE_REF:-${MIGRATION_VERSION_GUARD_BASE_REF:-}}" main strict

# Fail closed in CI when no base ref could be established: the diff would
# narrow to HEAD's own commit and miss a migration added earlier on the branch.
# Unlike the other migration guards, there is no merge-queue exemption: the
# merge queue passes its group's base, and a rollback-unsafe migration must not
# merge there either. Offline local runs still degrade to the WARNs.
require_base_ref_in_ci() {
  migration_base_fails_closed || return 0
  local ref name
  ref="$(migration_base_ref)"
  name="$(migration_base_name)"
  git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null && return 0

  echo "ERROR: no base ref to compare SQLite migrations against; wanted ${name}." >&2
  echo "  If a WARN above names a fetch failure, that is the cause; otherwise no" >&2
  echo "  remote is configured in this checkout. To pin a base explicitly, set" >&2
  echo "  MIGRATION_SQLITE_ROLLBACK_BASE_REF to a ref that resolves here." >&2
  exit 1
}

# The commit changed files are compared with: the merge base with the base
# ref, the ref itself without one, HEAD^ without a ref, or none.
diff_base() {
  local ref name
  ref="$(migration_base_ref)"
  name="$(migration_base_name)"
  if git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    if git merge-base "${ref}" HEAD 2>/dev/null; then
      return 0
    fi
    echo "WARN: no merge-base with ${name}; using a direct tree diff for changed SQLite migrations," \
      "which can include files inherited from the base." >&2
    git rev-parse "${ref}^{commit}"
  elif git rev-parse --verify --quiet HEAD^ >/dev/null; then
    echo "WARN: ${name} is unresolvable; inspecting only HEAD's own changes," \
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

if ! migration_base_ensure; then
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
