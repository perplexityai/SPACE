#!/usr/bin/env bash
# Require protobuf schema changes to land in their own PR.
#
# Wire-contract changes are easy to miss inside a large behavior change, and
# they outlive the code that motivated them. Like Postgres migrations, they get
# a dedicated PR so reviewers see the schema change on its own.
#
# A proto PR may also touch documentation, protobuf codegen wiring (build.rs),
# and this guard. Everything else lands in a follow-up PR.
#
# Usage: check-proto-changes-isolated.sh [base-ref]
#   base-ref defaults to $PROTO_ISOLATION_BASE_REF, then origin/main.
#   PROTO_ISOLATION_PER_COMMIT=true checks each first-parent commit after the
#   base against its own parent, for pushes that land several PRs at once.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

base="${1:-${PROTO_ISOLATION_BASE_REF:-origin/main}}"

is_proto_change() {
  [[ "$1" == proto/* && "$1" != *.md ]]
}

is_allowed_with_proto() {
  case "$1" in
    proto/*|*.md|docs/*) return 0 ;;
    crates/*/build.rs) return 0 ;;
    scripts/check-proto-changes-isolated.sh) return 0 ;;
    scripts/tests/test-check-proto-changes-isolated.sh) return 0 ;;
  esac
  return 1
}

# check_range <base-commit> <head-commit> <label>
check_range() {
  local from="$1" to="$2" label="$3" file
  local -a protos=() others=()

  while IFS= read -r file; do
    [[ -n "${file}" ]] || continue
    if is_proto_change "${file}"; then
      protos+=("${file}")
    elif ! is_allowed_with_proto "${file}"; then
      others+=("${file}")
    fi
  done < <(git diff --name-only --diff-filter=ACMRTD "${from}" "${to}")

  if ((${#protos[@]} == 0 || ${#others[@]} == 0)); then
    return 0
  fi

  echo "ERROR: split protobuf changes from other changes (${label})." >&2
  echo "  Proto files define wire contracts; land them in a PR that only changes" >&2
  echo "  proto/, docs, or codegen build.rs, then follow up with the code." >&2
  echo >&2
  echo "  Changed proto file(s):" >&2
  printf '    %s\n' "${protos[@]}" >&2
  echo "  Other file(s) changed in the same diff:" >&2
  printf '    %s\n' "${others[@]}" >&2
  return 1
}

if ! base_commit="$(git rev-parse --verify --quiet "${base}^{commit}")"; then
  echo "ERROR: proto isolation base ${base} does not resolve to a commit." >&2
  exit 1
fi

if [[ "${PROTO_ISOLATION_PER_COMMIT:-false}" == true ]]; then
  failed=0
  while IFS= read -r commit; do
    check_range "${commit}^1" "${commit}" "$(git log -1 --format='%h %s' "${commit}")" \
      || failed=1
  done < <(git rev-list --first-parent --reverse "${base_commit}..HEAD")
  ((failed == 0)) || exit 1
  echo "Proto isolation passed for each commit after ${base}."
  exit 0
fi

if ! merge_base="$(git merge-base "${base_commit}" HEAD)"; then
  echo "ERROR: no merge base between ${base} and HEAD." >&2
  exit 1
fi

check_range "${merge_base}" HEAD "${base}...HEAD"
echo "Proto isolation check passed."
