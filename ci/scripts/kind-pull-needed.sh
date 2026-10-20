#!/usr/bin/env bash
# Print `true` when a pull request changes a path in ci/kind-pull-paths.txt,
# `false` otherwise. Pull CI uses the answer to decide whether the required
# space-pull-ci check must also see a passing Kind smoke.
#
# Usage: ci/scripts/kind-pull-needed.sh <pull-request-head-sha>
#
# Run from GitHub's synthetic merge of the PR (the default pull_request
# checkout) with its parents fetched. The change set is that merge against its
# first parent: exactly what the PR adds to the base it is tested on. The merge
# is checked first so a wrong checkout fails instead of reading as "no change".
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATHS_FILE="${SPACE_KIND_PULL_PATHS:-${SCRIPT_DIR}/../kind-pull-paths.txt}"

pull_request_head="${1:-}"
if [[ -z "${pull_request_head}" ]]; then
    echo "usage: $0 <pull-request-head-sha>" >&2
    exit 2
fi

read -r base_parent head_parent extra_parents < <(git show -s --format=%P HEAD)
if ! resolved_head="$(git rev-parse --verify "${pull_request_head}^{commit}" 2>/dev/null)" \
    || [[ -z "${base_parent}" || -z "${head_parent}" || -n "${extra_parents}" ]] \
    || [[ "${head_parent}" != "${resolved_head}" ]]; then
    echo "Kind path selection requires the synthetic merge of the expected PR head." >&2
    exit 1
fi

pathspecs=()
while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%%#*}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -n "${line}" ]] && pathspecs+=(":(top,glob)${line}")
done <"${PATHS_FILE}"
if [[ "${#pathspecs[@]}" -eq 0 ]]; then
    echo "${PATHS_FILE} lists no paths." >&2
    exit 1
fi

changed="$(git diff --name-only "${base_parent}" HEAD -- "${pathspecs[@]}")"
if [[ -n "${changed}" ]]; then
    echo "Kind smoke required by:" >&2
    while IFS= read -r path; do printf '  %s\n' "${path}"; done <<<"${changed}" >&2
    printf 'true\n'
else
    echo "No Kind smoke path changed." >&2
    printf 'false\n'
fi
