#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

shopt -s nullglob

readonly LEGACY_MAX_VERSION=93
# Existing db/postgres migrations 001..093 are legacy sequential versions.
# The guard only checks newly added Postgres files, so historical migrations
# remain valid while every additive migration must move to timestamp versions.
readonly POSTGRES_TIMESTAMP_REGEX='^(20[0-9]{2}|21[0-9]{2})(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])([01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]_.+\.(up|down)\.sql$'

# shellcheck source=lib/migration-base.sh
source "${repo_root}/scripts/lib/migration-base.sh"
# A Buildkite PR build names its target branch, which may be a stacked PR's
# parent rather than main.
migration_base_configure "${MIGRATION_VERSION_GUARD_BASE_REF:-}" \
  "${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-main}" exempt

# Lists files under <pathspec> that this branch adds relative to the base.
# Rename detection is off throughout, so a file moved into <pathspec> is listed
# as an addition rather than hidden behind a rename entry.
newly_added_migration_files() {
  local label="$1"
  local pathspec="$2"
  local ref name
  ref="$(migration_base_ref)"
  name="$(migration_base_name)"

  if git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    if git merge-base "${ref}" HEAD >/dev/null 2>&1; then
      git diff --name-only --no-renames --diff-filter=A "${ref}...HEAD" -- "${pathspec}"
    else
      echo "WARN: no merge-base with ${name}; using a direct tree diff for new ${label} migrations." >&2
      git diff --name-only --no-renames --diff-filter=A "${ref}" HEAD -- "${pathspec}"
    fi
  elif git rev-parse --verify --quiet HEAD^ >/dev/null; then
    # Last resort, and it narrows the guard to a single commit: a bad migration
    # added earlier on a multi-commit branch is outside this diff and passes.
    echo "WARN: ${name} is unresolvable; inspecting only HEAD's own additions," \
      "so ${label} migrations added earlier on this branch are not checked." >&2
    git diff --name-only --no-renames --diff-filter=A HEAD^..HEAD -- "${pathspec}"
  else
    echo "WARN: could not establish a committed diff base for new ${label} migrations." >&2
  fi

  git status --porcelain --no-renames -- "${pathspec}" \
    | while read -r status file; do
        if [[ "${status}" == "??" || "${status:0:1}" == "A" ]]; then
          printf '%s\n' "${file}"
        fi
      done
}

# Prints the greatest up-migration version directly in <dir> on the base, or 0.
# Subdirectories such as template-manager's history/ are not part of the
# catalog, so the listing is deliberately not recursive.
base_max_up_version() {
  local dir="$1"
  local label="$2"
  local ref
  local name
  local file
  local filename
  local max_version=0
  local version
  ref="$(migration_base_ref)"
  name="$(migration_base_name)"

  if git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    while IFS= read -r file; do
      filename="${file##*/}"
      if [[ "${filename}" =~ ^([0-9]+)_.+\.up\.sql$ ]]; then
        version=$((10#${BASH_REMATCH[1]}))
        if ((version > max_version)); then
          max_version="${version}"
        fi
      fi
    done < <(git ls-tree --name-only "${ref}" -- "${dir}/")
  else
    # Returning 0 disables the not-newer-than-base check in the caller, so say
    # so rather than quietly enforcing half the contract. A resolvable base
    # whose tree simply holds no numbered migrations returns 0 too and is not
    # worth warning about — that is a legitimately empty base, not a lost base.
    echo "WARN: base ref ${name} is unresolvable; skipping the" \
      "not-newer-than-base check for new ${label} migrations." >&2
  fi

  printf '%s\n' "${max_version}"
}

establish_base_ref() {
  # The only freshening call, hoisted so it runs once before both consumers.
  # Calling it from each instead costs a fetch per consumer: they each read the
  # base ref inside a subshell, and while the fetched ref lands on disk and is
  # perfectly visible from there, nothing dedupes the later fetches.
  if ! migration_base_ensure; then
    return 1
  fi

  # Fail closed in CI when no base could be established. With no base, the
  # not-newer-than-base check is skipped outright and the newly-added set
  # narrows to HEAD's own commit, so a bad migration added earlier on a
  # multi-commit branch merges green behind nothing but a WARN. A guard that
  # cannot compare must not report success.
  #
  # Scoped to CI on purpose: offline and air-gapped local runs are legitimate
  # and still degrade to the WARNs below, so a developer without network keeps a
  # usable check instead of a hard stop.
  # Trunk merge-queue builds are exempt, matching
  # check-postgres-migrations-isolated.sh: BUILDKITE_PULL_REQUEST is false there,
  # so the host-side pre-command hook never fetches a base, and the queue must
  # not go red on a path this change has no way to exercise. The PRs it batches
  # were each guarded individually.
  if migration_base_fails_closed \
    && git rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    && ! git rev-parse --verify --quiet "$(migration_base_ref)^{commit}" >/dev/null; then
    echo "ERROR: no base ref to compare new migrations against." >&2
    echo "  Wanted: $(migration_base_name)" >&2
    echo "  Refusing to pass the migration checks with no base in CI; the" >&2
    echo "  not-newer-than-base check cannot run and the newly-added set would" >&2
    echo "  narrow to HEAD alone. If a WARN above names a fetch failure, that is" >&2
    echo "  the cause; otherwise no remote is configured in this checkout." >&2
    echo "  To pin a base explicitly, set MIGRATION_VERSION_GUARD_BASE_REF to a" >&2
    echo "  ref that resolves in this checkout." >&2
    return 1
  fi
}

validate_new_postgres_migration_versions() {
  local file
  local filename
  local version_text
  local version
  local base_max_version
  local legacy_cutoff_files=()
  local invalid_timestamp_files=()
  local not_newer_than_base_files=()

  base_max_version="$(base_max_up_version db/postgres Postgres)"

  while IFS= read -r file; do
    [[ -n "${file}" ]] || continue
    filename="${file##*/}"
    if [[ ! "${filename}" =~ ^([0-9]+)_.+\.(up|down)\.sql$ ]]; then
      invalid_timestamp_files+=("${file}")
      continue
    fi

    version_text="${BASH_REMATCH[1]}"
    version=$((10#${version_text}))

    if ((version <= LEGACY_MAX_VERSION)); then
      legacy_cutoff_files+=("${file}")
    fi

    if [[ ! "${filename}" =~ ${POSTGRES_TIMESTAMP_REGEX} ]]; then
      invalid_timestamp_files+=("${file}")
    fi

    if ((base_max_version > 0 && version <= base_max_version)); then
      not_newer_than_base_files+=("${file}")
    fi
  done < <(newly_added_migration_files Postgres 'db/postgres/*.sql' | sort -u)

  if ((${#legacy_cutoff_files[@]} == 0 && ${#invalid_timestamp_files[@]} == 0 && ${#not_newer_than_base_files[@]} == 0)); then
    return 0
  fi

  echo "ERROR: new Postgres migrations must use UTC timestamp versions." >&2
  echo "  Expected: db/postgres/YYYYMMDDHHMMSS_description.{up,down}.sql" >&2
  echo "  Example:  db/postgres/$(date -u +%Y%m%d%H%M%S)_add_example.up.sql" >&2
  echo "  Legacy cutoff: existing sequential migrations end at ${LEGACY_MAX_VERSION}; new numeric versions must be greater." >&2
  if ((base_max_version > 0)); then
    echo "  Base branch max Postgres version: ${base_max_version}; new versions must be greater to avoid out-of-order application." >&2
  fi

  if ((${#legacy_cutoff_files[@]} > 0)); then
    echo "  Newly added file(s) at or below the legacy cutoff:" >&2
    printf '    %s\n' "${legacy_cutoff_files[@]}" >&2
  fi

  if ((${#invalid_timestamp_files[@]} > 0)); then
    echo "  Newly added file(s) without a valid UTC YYYYMMDDHHMMSS timestamp:" >&2
    printf '    %s\n' "${invalid_timestamp_files[@]}" >&2
  fi

  if ((${#not_newer_than_base_files[@]} > 0)); then
    echo "  Newly added file(s) not newer than the base branch max Postgres version:" >&2
    printf '    %s\n' "${not_newer_than_base_files[@]}" >&2
  fi
  return 1
}

# Prints "<catalog> <max up version>" for each SQLite catalog on the base: every
# db/<store>/ directory except db/postgres.
base_sqlite_catalog_max_versions() {
  local ref
  local name
  local dir
  ref="$(migration_base_ref)"
  name="$(migration_base_name)"

  if ! git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    echo "WARN: base ref ${name} is unresolvable; skipping the" \
      "not-newer-than-base check for new SQLite migrations." >&2
    return 0
  fi

  while IFS= read -r dir; do
    [[ "${dir}" != db/postgres ]] || continue
    printf '%s %s\n' "${dir}" "$(base_max_up_version "${dir}" "SQLite ${dir}")"
  done < <(git ls-tree -d --name-only "${ref}" -- db/)
}

# Node-local SQLite catalogs are every other db/<store>/ directory.
# `space-db-migrator <store> down` reverts the newest applied version first,
# across the store's catalog and the shared db/common catalog when the store
# consumes it, and reverts the store's migration on a tie. A migration that
# merges at or below a version already on the base is therefore not the first
# one down reverts, so rolling back to the release before it reverts that
# release's own migration instead. A new db/common migration is compared with
# every catalog rather than only the consumers, which the migrator lists; a new
# migration elsewhere is compared with its catalog and db/common. A catalog
# absent from the base has no earlier release to roll back to and is skipped.
validate_new_sqlite_migration_versions() {
  local file
  local dir
  local filename
  local version
  local catalog_max
  local all_max=0
  local all_max_dir=""
  local threshold
  local -a new_files=()
  local -A base_max_versions=()
  local -A thresholds=()
  local -A threshold_dirs=()
  local -A not_newer_than_base_files=()

  while IFS= read -r file; do
    [[ -n "${file}" ]] || continue
    dir="${file%/*}"
    if [[ ! "${dir}" =~ ^db/[^/]+$ ]] || [[ "${dir}" == db/postgres ]]; then
      continue
    fi
    # The directory checks above already report names without a numeric version.
    [[ "${file##*/}" =~ ^[0-9]+_.+\.up\.sql$ ]] || continue
    new_files+=("${file}")
  done < <(newly_added_migration_files SQLite 'db/*/*.up.sql' | sort -u)

  if ((${#new_files[@]} == 0)); then
    return 0
  fi

  while read -r dir catalog_max; do
    base_max_versions["${dir}"]="${catalog_max}"
    if ((catalog_max > all_max)); then
      all_max="${catalog_max}"
      all_max_dir="${dir}"
    fi
  done < <(base_sqlite_catalog_max_versions)

  for dir in "${!base_max_versions[@]}"; do
    if [[ "${dir}" == db/common ]]; then
      thresholds["${dir}"]="${all_max}"
      threshold_dirs["${dir}"]="${all_max_dir}"
    elif ((${base_max_versions[db/common]:-0} > base_max_versions["${dir}"])); then
      thresholds["${dir}"]="${base_max_versions[db/common]}"
      threshold_dirs["${dir}"]=db/common
    else
      thresholds["${dir}"]="${base_max_versions["${dir}"]}"
      threshold_dirs["${dir}"]="${dir}"
    fi
  done

  for file in "${new_files[@]}"; do
    dir="${file%/*}"
    [[ -n "${thresholds["${dir}"]+set}" ]] || continue
    threshold="${thresholds["${dir}"]}"
    filename="${file##*/}"
    [[ "${filename}" =~ ^([0-9]+)_ ]]
    version=$((10#${BASH_REMATCH[1]}))
    if ((threshold > 0 && version <= threshold)); then
      not_newer_than_base_files["${dir}"]+="${file}"$'\n'
    fi
  done

  if ((${#not_newer_than_base_files[@]} == 0)); then
    return 0
  fi

  echo "ERROR: new SQLite migrations must be newer than the versions down could revert before them on the base branch." >&2
  echo "  A db/common migration must be newer than every SQLite catalog's max version;" >&2
  echo "  any other catalog's must be newer than the max of that catalog and db/common." >&2
  echo "  Renumber a migration overtaken by a newer one with a fresh UTC timestamp:" >&2
  echo "  Example:  db/<store>/$(date -u +%Y%m%d%H%M%S)_add_example.up.sql" >&2
  while IFS= read -r dir; do
    echo "  New ${dir} versions must be greater than ${thresholds["${dir}"]}, the base branch max version in ${threshold_dirs["${dir}"]}." >&2
    echo "  Newly added file(s) in ${dir} not newer than ${thresholds["${dir}"]}:" >&2
    while IFS= read -r file; do
      [[ -n "${file}" ]] && printf '    %s\n' "${file}" >&2
    done <<<"${not_newer_than_base_files["${dir}"]}"
  done < <(printf '%s\n' "${!not_newer_than_base_files[@]}" | sort)
  return 1
}

migration_dirs=()
for dir in db/*/; do
  up_files=("${dir}"*.up.sql)
  if ((${#up_files[@]} > 0)); then
    migration_dirs+=("${dir%/}")
  fi
done

if ((${#migration_dirs[@]} == 0)); then
  echo "No SQLx migration directories found under db/."
  exit 0
fi

overall_status=0

for dir in "${migration_dirs[@]}"; do
  unset version_counts version_files
  declare -A version_counts=()
  declare -A version_files=()
  invalid_names=()
  missing_down=()
  duplicate_versions=()
  up_files=("${dir}"/*.up.sql)

  for up_file in "${up_files[@]}"; do
    filename="${up_file##*/}"
    if [[ "${filename}" =~ ^([0-9]+)_.+\.up\.sql$ ]]; then
      version="${BASH_REMATCH[1]}"
      version_counts["${version}"]="$(( ${version_counts["${version}"]:-0} + 1 ))"
      version_files["${version}"]+="${up_file}"$'\n'
    else
      invalid_names+=("${up_file}")
    fi

    down_file="${up_file%.up.sql}.down.sql"
    if [[ ! -f "${down_file}" ]]; then
      missing_down+=("${down_file}")
    fi
  done

  for version in "${!version_counts[@]}"; do
    if ((version_counts["${version}"] > 1)); then
      duplicate_versions+=("${version}")
    fi
  done

  if ((${#invalid_names[@]} == 0 && ${#missing_down[@]} == 0 && ${#duplicate_versions[@]} == 0)); then
    echo "OK: ${dir} (${#up_files[@]} up migration(s))"
    continue
  fi

  overall_status=1
  echo "ERROR: ${dir} has invalid SQLx migration files:" >&2

  if ((${#duplicate_versions[@]} > 0)); then
    printf '  Duplicate version number(s): %s\n' "${duplicate_versions[*]}" >&2
    for version in "${duplicate_versions[@]}"; do
      printf '    version %s:\n' "${version}" >&2
      while IFS= read -r file; do
        [[ -n "${file}" ]] && printf '      %s\n' "${file}" >&2
      done <<<"${version_files["${version}"]}"
    done
  fi

  if ((${#missing_down[@]} > 0)); then
    echo "  Missing down migration(s):" >&2
    printf '    %s\n' "${missing_down[@]}" >&2
  fi

  if ((${#invalid_names[@]} > 0)); then
    echo "  Up migrations without a numeric_version_description.up.sql name:" >&2
    printf '    %s\n' "${invalid_names[@]}" >&2
  fi
done

if ! establish_base_ref; then
  overall_status=1
else
  if ! validate_new_postgres_migration_versions; then
    overall_status=1
  fi
  if ! validate_new_sqlite_migration_versions; then
    overall_status=1
  fi
fi

if ((overall_status != 0)); then
  exit "${overall_status}"
fi

echo "All SQLx migration directories passed duplicate-version, down-migration, Postgres timestamp, and SQLite version-order checks."
