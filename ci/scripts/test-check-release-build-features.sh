#!/usr/bin/env bash
# Regression test for ci/scripts/check-release-build-features.sh against a
# synthetic workspace and fixture Dockerfiles, so it needs only Cargo:
#
#   1. A root `cargo build --bin` with no package selection resolves every
#      member, so a member that enables a test-only feature in its normal
#      dependencies fails the check. `-p` on the binary's package passes.
#   2. `test-support` fails like `test-hooks`, and so does a test-only feature
#      a selected package turns on itself with `-F`, on itself or forwarded.
#   3. A stage added to a passing file is checked without listing it anywhere.
#   4. A `cargo chef cook` case branch is checked and named.
#   5. An invocation the check cannot read, an unknown cargo subcommand, and a
#      file with no build at all fail instead of passing unchecked.
#
# The fixture Dockerfiles keep their shell expansions literal.
# shellcheck disable=SC2016
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
check="${repo_root}/ci/scripts/check-release-build-features.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

workspace="${tmp_dir}/workspace"
add_crate() { # name manifest-tail
    mkdir -p "${workspace}/$1/src"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n\n%s\n' "$1" "$2" \
        >"${workspace}/$1/Cargo.toml"
    : >"${workspace}/$1/src/lib.rs"
}
mkdir -p "${workspace}"
printf '[workspace]\nresolver = "2"\nmembers = ["base", "fixtures", "support", "app"]\n' \
    >"${workspace}/Cargo.toml"
# The workspace's own toolchain, so rustup resolves the one CI already has.
cp "${repo_root}/rust-toolchain.toml" "${workspace}/"
add_crate base '[features]
test-hooks = []
test-support = []'
add_crate fixtures '[dependencies]
base = { path = "../base", features = ["test-hooks"] }'
add_crate support '[dependencies]
base = { path = "../base", features = ["test-support"] }'
add_crate app '[features]
extra = []
test-hooks = []
forward = ["base/test-hooks"]

[dependencies]
base = { path = "../base" }

[[bin]]
name = "app"
path = "src/main.rs"'
printf 'fn main() {}\n' >"${workspace}/app/src/main.rs"
(cd "${workspace}" && CARGO_NET_OFFLINE=true cargo generate-lockfile --quiet)

status=0
fail() {
    echo "FAIL: $1" >&2
    [ -z "${2:-}" ] || sed 's/^/    /' "$2" >&2
    status=1
}

# expect <pass|fail> <name> <dockerfile-contents> [text the output must contain]...
expect() {
    local want="$1" name="$2" contents="$3"
    shift 3
    local dockerfile="${tmp_dir}/${name}.Dockerfile" log="${tmp_dir}/${name}.log"
    printf '%s\n' "${contents}" >"${dockerfile}"
    local got=pass
    bash "${check}" "${dockerfile}" "${workspace}" >"${log}" 2>&1 || got=fail
    if [ "${got}" != "${want}" ]; then
        fail "${name}: expected the check to ${want}, it did ${got}" "${log}"
        return
    fi
    local text
    for text in "$@"; do
        if ! grep -qF -- "${text}" "${log}"; then
            fail "${name}: output lacks ${text}" "${log}"
        fi
    done
}

clean='FROM rust AS base
RUN rustup toolchain install && cargo --version
RUN cargo install --locked --version 0.1.78 cargo-chef

FROM base AS build-app
# A comment inside a continued RUN, as Docker allows.
RUN --mount=type=secret,id=token \
    run-wrapper cargo build --locked --release \
# dropped by Docker
      -p app \
      --features "app/extra" \
      --bin app'

expect fail root-bin-unifies-the-workspace 'FROM rust AS build-app
RUN cargo build --locked --release --bin app' \
    'base feature "test-hooks"' 'base feature "test-support"' '(build-app)'

expect pass package-selection-is-clean "${clean}" \
    '1 release build selection(s)' '-p app --features app/extra'

expect fail test-support-leaks 'FROM rust AS build-support
RUN cargo build --locked --release -p support' 'base feature "test-support"'

expect fail own-feature-is-seen 'FROM rust AS build-app
RUN cargo build --locked --release -p app -F app/test-hooks --bin app' 'app feature "test-hooks"'

expect fail forwarded-feature-is-seen 'FROM rust AS build-app
RUN cargo build --locked --release -p app -F app/forward --bin app' 'base feature "test-hooks"'

expect fail added-stage-is-checked "${clean}

FROM base AS build-new
RUN cargo build --locked --release -F app/extra --bin app" '(build-new)'

expect fail cook-branch-is-named 'FROM rust AS cook
ARG SET=app
RUN set -eux; \
      case "${SET}" in \
        app) \
          cargo chef cook --locked --release --recipe-path recipe.json -p app --bin app \
          ;; \
        all) \
          cargo chef cook --locked --release --recipe-path recipe.json \
          ;; \
      esac' '(cook, all) cargo chef cook enables' '2 release build selection(s) resolved'
if grep -qF '(cook, app) cargo chef cook enables' "${tmp_dir}/cook-branch-is-named.log"; then
    fail "cook-branch-is-named: the clean app branch was reported" \
        "${tmp_dir}/cook-branch-is-named.log"
fi

expect fail shell-expansion-is-unreadable 'FROM rust AS build-app
RUN cargo build --locked --release ${FLAGS} -p app' 'cannot read argument ${FLAGS}'

expect fail test-targets-are-unsupported 'FROM rust AS build-app
RUN cargo build --locked --release -p app --all-targets' 'unsupported argument --all-targets'

expect fail unknown-subcommand-fails 'FROM rust AS build-app
RUN cargo rustc --locked --release -p app' 'unrecognized cargo rustc'

expect fail no-build-fails 'FROM rust AS base
RUN cargo --version' 'found no cargo build or cargo chef cook invocation'

exit "${status}"
