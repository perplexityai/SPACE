#!/usr/bin/env bash
# Regression test for ci/scripts/check-release-build-features.sh against a
# synthetic workspace and fixture Dockerfiles, so it needs only Cargo:
#
#   1. A root `cargo build --bin` with no package selection resolves every
#      member, so a member that enables a test-only feature in its normal
#      dependencies fails the check. `-p` on the binary's package passes.
#   2. `test-support` fails like `test-hooks`, and so does a test-only feature
#      a selected package turns on itself with `-F`, on itself or forwarded.
#      Any other `test-*` feature fails on a package built from the checkout.
#   3. A stage added to a passing file is checked without listing it anywhere.
#   4. A `cargo chef cook` case branch is checked and named.
#   5. An invocation the check cannot read, an unknown cargo subcommand, and a
#      file with no build at all fail instead of passing unchecked. So do the
#      forms outside the check's grammar: a heredoc, exec form, `cargo` inside
#      a command string, substitution or after `;`, `cargo install --path=`,
#      an expansion in a dropped option's value, cargo in another
#      instruction, a SHELL instruction, and an escape directive. A blank line
#      inside a continued RUN is dropped as Docker drops it, not taken as its
#      end, and a word split across a continuation is read whole.
#   7. A `cargo install` of local code fails however its `--path` is written,
#      and so does one with an expansion other than a quoted `--version`
#      variable. An expansion that may run cargo, such as `${CARGO:-cargo}`,
#      fails. A stage before the first named one is checked.
#   6. A failure to process the graph fails the check rather than reading as
#      clean: a leaking selection still fails with an unwritable TMPDIR, and
#      with a `sort` that fails.
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
printf '[workspace]\nresolver = "2"\nmembers = ["base", "fixtures", "support", "capture", "app"]\n' \
    >"${workspace}/Cargo.toml"
# The workspace's own toolchain, so rustup resolves the one CI already has.
cp "${repo_root}/rust-toolchain.toml" "${workspace}/"
add_crate base '[features]
test-hooks = []
test-support = []
test-capture = []'
add_crate fixtures '[dependencies]
base = { path = "../base", features = ["test-hooks"] }'
add_crate support '[dependencies]
base = { path = "../base", features = ["test-support"] }'
add_crate capture '[dependencies]
base = { path = "../base", features = ["test-capture"] }'
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
RUN cargo install --locked --version "${CARGO_CHEF_VERSION}" cargo-chef

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

expect fail test-capture-on-a-checkout-package 'FROM rust AS build-capture
RUN cargo build --locked --release -p capture' 'base feature "test-capture"'

expect fail blank-continuation-lines-are-dropped "FROM rust AS build-app
RUN cargo build --locked --release -p app \\

   
      -F app/test-hooks --bin app" 'app feature "test-hooks"'

# Each form below hides the same leaking build; the clean one keeps the file
# from failing only for having nothing to check.
hidden() {
    printf '%s\n' "${clean}" "" "FROM base AS build-hidden" "$1"
}
leak='cargo build --locked --release -p app -F app/test-hooks --bin app'
unread='cargo appears where this check cannot read it'
expect fail heredoc-fails "$(hidden "RUN <<EOF
${leak}
EOF")" 'cannot read a RUN with a heredoc'
expect fail exec-form-fails "$(hidden 'RUN ["cargo", "build", "-p", "app", "-F", "app/test-hooks"]')" \
    'cannot read a RUN in exec form'
expect fail command-string-fails "$(hidden "RUN sh -c \"${leak}\"")" "${unread}"
expect fail separator-without-space-fails "$(hidden "RUN true;${leak}")" "${unread}"
expect fail command-substitution-fails "$(hidden "RUN echo \$(${leak})")" "${unread}"
expect fail backticks-fail "$(hidden "RUN echo \`${leak}\`")" "${unread}"
expect fail install-path-equals-fails "$(hidden 'RUN cargo install --locked --path=app')" \
    'cargo install --path=app builds local code'
expect fail expansion-in-a-dropped-value-fails \
    "$(hidden 'RUN cargo build --locked --release -p app --bin ${BIN_FLAGS}')" \
    'cannot read --bin ${BIN_FLAGS}'
expect fail cargo-in-another-instruction-fails "$(hidden "ONBUILD RUN ${leak}")" \
    'cannot read cargo in a ONBUILD instruction'
expect fail shell-instruction-fails "$(hidden 'SHELL ["/bin/bash", "-c"]')" \
    'cannot read RUN instructions under a SHELL instruction'
expect fail install-path-before-a-separator-fails \
    "$(hidden 'RUN cargo install --locked --features test-hooks --path=app; true')" \
    'cargo install --path=app builds local code'
expect fail install-double-quoted-path-fails "$(hidden 'RUN cargo install --locked "--path=app"')" \
    'cargo install --path=app builds local code'
expect fail install-single-quoted-path-fails "$(hidden "RUN cargo install --locked '--path' app")" \
    'cargo install --path builds local code'
expect fail install-expansion-fails "$(hidden 'RUN cargo install --locked ${INSTALL_ARGS}')" \
    'cannot read cargo install argument ${INSTALL_ARGS}'
expect fail install-unquoted-version-expansion-fails \
    "$(hidden 'RUN cargo install --locked --version ${V} cargo-chef')" \
    'cannot read cargo install argument ${V}'
expect fail cargo-in-an-expansion-fails "$(hidden "RUN \${CARGO:-cargo} ${leak#cargo }")" \
    'which may run cargo'
expect fail word-split-across-a-continuation-is-read "$(hidden "RUN car\\
go ${leak#cargo }")" 'app feature "test-hooks"'
expect fail unnamed-first-stage-is-checked "FROM rust
RUN cargo build --locked --release --bin app

${clean}" '(no stage) cargo build enables' 'base feature "test-hooks"'
expect fail escape-directive-fails "# escape=\`
${clean}" 'cannot read a file with an escape parser directive'

# A graph larger than a pipe buffer, so a here-string of it would need a
# temporary file: a package with 1200 dependencies of its own.
wide="${tmp_dir}/wide"
mkdir -p "${wide}/base/src" "${wide}/wide/src"
cp "${repo_root}/rust-toolchain.toml" "${wide}/"
printf '[workspace]\nresolver = "2"\nmembers = ["base", "wide", "leaves/*"]\n' >"${wide}/Cargo.toml"
cp "${workspace}/base/Cargo.toml" "${wide}/base/"
: >"${wide}/base/src/lib.rs"
{
    printf '[package]\nname = "wide"\nversion = "0.1.0"\nedition = "2021"\n\n'
    printf '[features]\nforward = ["base/test-hooks"]\n\n[dependencies]\n'
    printf 'base = { path = "../base" }\n'
    for leaf in $(seq 1 1200); do
        mkdir -p "${wide}/leaves/leaf-${leaf}/src"
        printf '[package]\nname = "leaf-%s"\nversion = "0.1.0"\nedition = "2021"\n' "${leaf}" \
            >"${wide}/leaves/leaf-${leaf}/Cargo.toml"
        : >"${wide}/leaves/leaf-${leaf}/src/lib.rs"
        printf 'leaf-%s = { path = "../leaves/leaf-%s" }\n' "${leaf}" "${leaf}"
    done
} >"${wide}/wide/Cargo.toml"
: >"${wide}/wide/src/lib.rs"
(cd "${wide}" && CARGO_NET_OFFLINE=true cargo generate-lockfile --quiet)
read_only="${tmp_dir}/read-only"
mkdir "${read_only}"
chmod 0500 "${read_only}"
printf 'FROM rust AS build-wide\nRUN cargo build --locked --release -p wide -F wide/forward\n' \
    >"${tmp_dir}/wide.Dockerfile"
if TMPDIR="${read_only}" bash "${check}" "${tmp_dir}/wide.Dockerfile" "${wide}" \
    >"${tmp_dir}/wide.log" 2>&1; then
    fail "unwritable-tmpdir-still-fails: expected the check to fail, it did pass" \
        "${tmp_dir}/wide.log"
elif ! grep -qF 'base feature "test-hooks"' "${tmp_dir}/wide.log" \
    && ! grep -qF 'could not resolve the selection' "${tmp_dir}/wide.log"; then
    fail "unwritable-tmpdir-still-fails: failed without naming the leak or the error" \
        "${tmp_dir}/wide.log"
fi
chmod 0700 "${read_only}"

# Bash falls back to /tmp when TMPDIR is unwritable, so as a non-root user the
# case above passes either way. A failing tool in the graph's pipeline does not.
shims="${tmp_dir}/shims"
mkdir "${shims}"
printf '#!/bin/sh\nexit 1\n' >"${shims}/sort"
chmod +x "${shims}/sort"
printf 'FROM rust AS build-app\nRUN cargo build --locked --release -p app -F app/forward\n' \
    >"${tmp_dir}/sort.Dockerfile"
if PATH="${shims}:${PATH}" bash "${check}" "${tmp_dir}/sort.Dockerfile" "${workspace}" \
    >"${tmp_dir}/sort.log" 2>&1; then
    fail "failing-sort-fails: expected the check to fail, it did pass" "${tmp_dir}/sort.log"
elif ! grep -qF 'could not resolve the selection' "${tmp_dir}/sort.log"; then
    fail "failing-sort-fails: failed without naming the error" "${tmp_dir}/sort.log"
fi

exit "${status}"
