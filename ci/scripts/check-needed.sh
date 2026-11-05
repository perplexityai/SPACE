#!/usr/bin/env bash
# Print `needed=true` when the change under test touches something the check
# scope SCOPE reads, `needed=false` when it does not. Each check's job calls
# this right after its checkout and gates its remaining steps on the answer,
# so a check a change cannot affect passes without running. The scopes are
# in ci/check-scopes/ (see its README.md).
#
# Usage: ci/scripts/check-needed.sh SCOPE [BASE [PULL_REQUEST_HEAD]]
#
# Prints `needed=true` or `needed=false`, then `base=<sha>` when it compared
# the change with a base.
#
# An empty BASE means "run every check": Main CI passes none. SCOPE `always`
# is always needed. Anything this script cannot decide, a missing base or an
# empty change included, also answers `needed=true`, so an error costs time,
# never coverage. A scope file without patterns is an error: it would skip
# its check for every change.
#
# Run from the checkout of the commit under test. With PULL_REQUEST_HEAD, that
# commit is GitHub's synthetic merge of that head, and the change is the merge
# against its first parent, the base the run actually tests; Pull CI's Kind
# selector, migration guards, archcheck and reuse record use the same one.
# Without it (the merge queue), the change is the commit against BASE, tree to
# tree.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCOPES_DIR="${SPACE_CHECK_SCOPES_DIR:-${SCRIPT_DIR}/../check-scopes}"

scope="${1:-}"
base="${2:-}"
pull_request_head="${3:-}"
if [[ -z "${scope}" ]]; then
    echo "usage: $0 SCOPE [BASE [PULL_REQUEST_HEAD]]" >&2
    exit 2
fi

# Prints the answer, and the base it was decided against once there is one,
# which a check can use to narrow its own work (rust-isolated-crates.sh).
answer() {
    printf 'needed=%s\n' "$1"
    if [[ -n "${decided_base:-}" ]]; then
        printf 'base=%s\n' "${decided_base}"
    fi
    exit 0
}

# Reads the patterns of scope file $1, comments and blank lines dropped, into
# PATTERNS; a missing or empty file is an error.
read_patterns() {
    local file="$1" line
    PATTERNS=()
    if [[ ! -f "${file}" ]]; then
        echo "Missing check scope file ${file}." >&2
        exit 2
    fi
    while IFS= read -r line || [[ -n "${line}" ]]; do
        line="${line%%#*}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -n "${line}" ]] && PATTERNS+=("${line}")
    done <"${file}"
    if [[ "${#PATTERNS[@]}" -eq 0 ]]; then
        echo "${file} lists no paths." >&2
        exit 2
    fi
}

if [[ "${scope}" == always ]]; then
    answer true
fi
if [[ -f "${SCOPES_DIR}/${scope}.skip" ]]; then
    mode=skip
elif [[ -f "${SCOPES_DIR}/${scope}.run" ]]; then
    mode=run
else
    echo "No check scope ${scope} in ${SCOPES_DIR}." >&2
    exit 2
fi
# Read both lists before deciding anything, so a broken list fails every run.
# matches() reads them by name.
read_patterns "${SCOPES_DIR}/run-all.txt"
# shellcheck disable=SC2034
RUN_ALL=("${PATTERNS[@]}")
read_patterns "${SCOPES_DIR}/${scope}.${mode}"
# shellcheck disable=SC2034
SCOPE_PATTERNS=("${PATTERNS[@]}")

if [[ -z "${base}" ]]; then
    echo "No base to compare with: running the check." >&2
    answer true
fi

# Fetches commit $1 when a shallow checkout lacks it.
have_commit() {
    git cat-file -e "$1^{commit}" 2>/dev/null ||
        git fetch --quiet --no-tags --depth=1 origin "$1" 2>/dev/null
}

if [[ -n "${pull_request_head}" ]]; then
    # The raw commit's parents: a shallow checkout grafts HEAD as parentless,
    # but the object still names them.
    mapfile -t parents < <(git cat-file -p HEAD | sed -n 's/^parent //p')
    if [[ "${#parents[@]}" -ne 2 || "${parents[1]}" != "${pull_request_head}" ]]; then
        echo "HEAD is not the synthetic merge of ${pull_request_head}: running the check." >&2
        answer true
    fi
    base="${parents[0]}"
fi
if ! have_commit "${base}"; then
    echo "Cannot read base ${base}: running the check." >&2
    answer true
fi

# Both sides of a rename (--no-renames), so moving an input out of a scope
# still counts as changing it, and raw NUL-separated names (-z), which git
# would otherwise quote when they hold unusual characters.
listing="$(mktemp)"
trap 'rm -f "${listing}"' EXIT
if ! git diff --name-only --no-renames -z "${base}" HEAD >"${listing}"; then
    echo "Cannot list the change against ${base}: running the check." >&2
    answer true
fi
mapfile -d '' -t changed <"${listing}"
if [[ "${#changed[@]}" -eq 0 ]]; then
    echo "No change against ${base}: running the check." >&2
    answer true
fi
# Named only once there is a change to decide on: a check that narrows its
# work to the change would otherwise find nothing to do.
decided_base="$(git rev-parse --verify "${base}^{commit}")"

# Whether path $1 matches the patterns in the array named $2: the last
# pattern that matches decides, and a `!` pattern un-matches.
matches() {
    local path="$1" pattern matched=false
    local -n list="$2"
    for pattern in "${list[@]}"; do
        if [[ "${pattern}" == '!'* ]]; then
            # shellcheck disable=SC2053  # The pattern is a glob on purpose.
            [[ "${path}" == ${pattern#!} ]] && matched=false
        else
            # shellcheck disable=SC2053
            [[ "${path}" == ${pattern} ]] && matched=true
        fi
    done
    [[ "${matched}" == true ]]
}

for path in "${changed[@]}"; do
    if matches "${path}" RUN_ALL; then
        echo "${path} is in ci/check-scopes/run-all.txt: running the check." >&2
        answer true
    fi
done

reads=()
for path in "${changed[@]}"; do
    if [[ "${mode}" == skip ]] && ! matches "${path}" SCOPE_PATTERNS; then
        reads+=("${path}")
    elif [[ "${mode}" == run ]] && matches "${path}" SCOPE_PATTERNS; then
        reads+=("${path}")
    fi
done

if [[ "${#reads[@]}" -gt 0 ]]; then
    echo "The ${scope} checks read changed paths, among them:" >&2
    printf '  %s\n' "${reads[@]:0:10}" >&2
    answer true
fi
message="Nothing the ${scope} checks read changed since ${base} (ci/check-scopes/${scope}.${mode}); skipping the check."
if [[ "${GITHUB_ACTIONS:-}" == true ]]; then
    echo "::notice title=Check not needed::${message}" >&2
else
    echo "${message}" >&2
fi
answer false
