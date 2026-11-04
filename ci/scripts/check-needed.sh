#!/usr/bin/env bash
# Print `needed=true` when the change since BASE touches something the check
# scope SCOPE reads, `needed=false` when it does not. Each check's job calls
# this right after its checkout and gates its remaining steps on the answer,
# so a check a change cannot affect passes without running. The scopes are
# in ci/check-scopes/ (see its README.md).
#
# Usage: ci/scripts/check-needed.sh SCOPE [BASE]
#
# An empty BASE means "run every check": Main CI passes none. SCOPE `always`
# is always needed. Anything this script cannot decide, a missing base or an
# empty change included, also answers `needed=true`, so an error costs time,
# never coverage.
#
# Run from the checkout of the commit under test (a pull request's synthetic
# merge, a merge group's commit). The change set is that commit against BASE,
# tree to tree.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCOPES_DIR="${SPACE_CHECK_SCOPES_DIR:-${SCRIPT_DIR}/../check-scopes}"

scope="${1:-}"
base="${2:-}"
if [[ -z "${scope}" ]]; then
    echo "usage: $0 SCOPE [BASE]" >&2
    exit 2
fi

answer() {
    printf 'needed=%s\n' "$1"
    exit 0
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
if [[ -z "${base}" ]]; then
    echo "No base to compare with: running the check." >&2
    answer true
fi

# A shallow checkout lacks the base; fetch just that commit.
if ! git cat-file -e "${base}^{commit}" 2>/dev/null &&
    ! git fetch --quiet --no-tags --depth=1 origin "${base}" 2>/dev/null; then
    echo "Cannot read base ${base}: running the check." >&2
    answer true
fi
if ! changed="$(git diff --name-only "${base}" HEAD)"; then
    echo "Cannot list the change against ${base}: running the check." >&2
    answer true
fi
if [[ -z "${changed}" ]]; then
    echo "No change against ${base}: running the check." >&2
    answer true
fi

# The patterns of a scope file, comments and blank lines dropped.
patterns() {
    local line
    while IFS= read -r line || [[ -n "${line}" ]]; do
        line="${line%%#*}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -n "${line}" ]] && printf '%s\n' "${line}"
    done <"$1"
}

# Whether PATH matches the patterns read into PATTERNS: the last pattern that
# matches decides, and a `!` pattern un-matches.
matches() {
    local path="$1" pattern matched=false
    for pattern in "${PATTERNS[@]}"; do
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

mapfile -t PATTERNS < <(patterns "${SCOPES_DIR}/always.txt")
while IFS= read -r path; do
    if matches "${path}"; then
        echo "${path} changes CI itself: running the check." >&2
        answer true
    fi
done <<<"${changed}"

mapfile -t PATTERNS < <(patterns "${SCOPES_DIR}/${scope}.${mode}")
reads=()
while IFS= read -r path; do
    if [[ "${mode}" == skip ]] && ! matches "${path}"; then
        reads+=("${path}")
    elif [[ "${mode}" == run ]] && matches "${path}"; then
        reads+=("${path}")
    fi
done <<<"${changed}"

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
