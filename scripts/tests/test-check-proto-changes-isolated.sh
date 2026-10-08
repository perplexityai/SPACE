#!/usr/bin/env bash
# Self-test for scripts/check-proto-changes-isolated.sh using synthetic repos.
set -euo pipefail

export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
guard="${repo_root}/scripts/check-proto-changes-isolated.sh"
tmp="$(mktemp -d)"
trap 'rm -rf -- "${tmp}"' EXIT

new_repo() {
  local repo="${tmp}/$1"
  git init -q -b main "${repo}"
  git -C "${repo}" config user.email test@example.com
  git -C "${repo}" config user.name test
  mkdir -p "${repo}/proto/demo/v1" "${repo}/crates/demo/src"
  echo 'syntax = "proto3";' >"${repo}/proto/demo/v1/demo.proto"
  echo 'fn main() {}' >"${repo}/crates/demo/build.rs"
  echo '// lib' >"${repo}/crates/demo/src/lib.rs"
  git -C "${repo}" add -A
  git -C "${repo}" commit -qm base
  printf '%s\n' "${repo}"
}

commit_change() {
  local repo="$1" msg="$2"
  shift 2
  local file
  for file in "$@"; do
    mkdir -p "$(dirname "${repo}/${file}")"
    echo "// ${msg}" >>"${repo}/${file}"
  done
  git -C "${repo}" add -A
  git -C "${repo}" commit -qm "${msg}"
}

expect() {
  local want="$1" name="$2" repo="$3"
  shift 3
  local got=0
  (cd "${repo}" && "$@" bash "${guard}" main~0 >/dev/null 2>"${tmp}/stderr") || got=$?
  if [[ "${want}" == pass && "${got}" -ne 0 ]] || [[ "${want}" == fail && "${got}" -eq 0 ]]; then
    echo "FAIL: ${name} (exit ${got})" >&2
    cat "${tmp}/stderr" >&2
    exit 1
  fi
  echo "ok: ${name}"
}

# Each case branches from main and compares the branch against it.
branch_case() {
  local want="$1" name="$2"
  shift 2
  local repo
  case_id=$((case_id + 1))
  repo="$(new_repo "case-${case_id}")"
  git -C "${repo}" checkout -qb feature
  commit_change "${repo}" change "$@"
  expect "${want}" "${name}" "${repo}" env
}

case_id=0
branch_case pass "proto-only change" proto/demo/v1/demo.proto
branch_case pass "proto with docs and codegen wiring" \
  proto/demo/v1/demo.proto docs/demo.md crates/demo/build.rs
branch_case pass "code-only change" crates/demo/src/lib.rs
branch_case pass "proto README with code" proto/demo/README.md crates/demo/src/lib.rs
branch_case fail "proto with runtime code" proto/demo/v1/demo.proto crates/demo/src/lib.rs
branch_case fail "proto with Cargo.lock" proto/demo/v1/demo.proto Cargo.lock

# Earlier commits on the branch count too.
repo="$(new_repo split-commits)"
git -C "${repo}" checkout -qb feature
commit_change "${repo}" proto proto/demo/v1/demo.proto
commit_change "${repo}" code crates/demo/src/lib.rs
expect fail "proto and code in separate commits of one PR" "${repo}" env

# Per-commit mode judges each landed change on its own.
repo="$(new_repo per-commit)"
git -C "${repo}" tag before
commit_change "${repo}" proto proto/demo/v1/demo.proto
commit_change "${repo}" code crates/demo/src/lib.rs
(cd "${repo}" && PROTO_ISOLATION_PER_COMMIT=true bash "${guard}" before >/dev/null) \
  || { echo "FAIL: per-commit separate landings" >&2; exit 1; }
echo "ok: per-commit separate landings"
commit_change "${repo}" mixed proto/demo/v1/demo.proto crates/demo/src/lib.rs
if (cd "${repo}" && PROTO_ISOLATION_PER_COMMIT=true bash "${guard}" before >/dev/null 2>&1); then
  echo "FAIL: per-commit mixed landing passed" >&2
  exit 1
fi
echo "ok: per-commit mixed landing"

# An unresolvable base fails closed.
repo="$(new_repo bad-base)"
if (cd "${repo}" && bash "${guard}" does-not-exist >/dev/null 2>&1); then
  echo "FAIL: unresolvable base passed" >&2
  exit 1
fi
echo "ok: unresolvable base fails"
