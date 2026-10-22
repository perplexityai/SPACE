#!/usr/bin/env bash
# Run one independently schedulable part of SPACE Rust validation.
# The Buildkite command image already contains the pinned Rust toolchain; the
# install helpers remain as a fallback for local use and older agents.
# Called by the Buildkite pipeline to block PRs with formatting, compilation,
# lint, or test failures. The test mode deliberately has no package or test-name
# selectors: Cargo discovers the workspace and nextest executes that complete
# inventory, including ignored tests.
#
# clippy lints every feature of workspace libraries and binaries while every
# target is compiled in the same Cargo graph. isolated-crates lints each
# library that lib/rust-isolated-crates.sh selects in a Cargo graph of its own.
#
# GitHub CI splits `tests` across two containers on one runner, built once:
# `tests-unprivileged` builds everything and runs the nextest inventory outside
# the `privileged` test group (.config/nextest.toml) as an ordinary user, with
# no Docker socket and its Postgres and Redis started beside it by the runner;
# `tests-privileged` then runs that group as root from the same build, together
# with the M1 generator check that needs Docker. `tests` and `all` still run the
# whole inventory in one process.
#
# Designed to run inside a K8s pod. Installs gcc via apt if no C compiler is
# found (required by Rust's `cc` crate for build scripts). Buildkite runs fmt,
# clippy, and the complete nextest suite on separate agents.
set -euo pipefail

# Test-dependency strictness is a runtime switch read by
# pplx-space-db-test-fixtures. Unavailable Postgres/Redis testcontainers fail
# their tests unless SPACE_TEST_DEPS_OPTIONAL opts out; on Buildkite,
# SPACE_CI_REQUIRE_TEST_DEPS=1 also overrides an opt-out the agent inherits.
if [ -n "${BUILDKITE:-}" ]; then
    export SPACE_CI_REQUIRE_TEST_DEPS=1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=lib/test-deps.sh
source "${SCRIPT_DIR}/lib/test-deps.sh"
# shellcheck source=lib/rust-side-checks.sh
source "${SCRIPT_DIR}/lib/rust-side-checks.sh"
# shellcheck source=lib/rust-isolated-crates.sh
source "${SCRIPT_DIR}/lib/rust-isolated-crates.sh"

PROTOC_VERSION="${PROTOC_VERSION:-35.0}"
CARGO_NEXTEST_VERSION="${CARGO_NEXTEST_VERSION:-0.9.133}"
LOCAL_BIN_DIR="${REPO_ROOT}/ci/bin"
TRANSIENT_NETWORK_EXIT_STATUS=75
TEST_PROFILE_ARGS=()
# The two halves of the inventory GitHub CI runs in separate containers. They
# are complements by definition; check_privileged_group checks that the group
# selects what its filter's clauses name.
PRIVILEGED_TESTS_FILTERSET='group(privileged)'
UNPRIVILEGED_TESTS_FILTERSET="not ${PRIVILEGED_TESTS_FILTERSET}"
# Set by main for tests-unprivileged: the nextest arguments that select its
# half, and whether the M1 generator check waits for the privileged step.
NEXTEST_PARTITION_ARGS=()
test_partition="all"
m1_generator_check="here"

curl_with_retries() {
    local status=0
    curl --retry 5 --retry-delay 2 --retry-max-time 180 --retry-all-errors "$@" || status="$?"
    if [ "${status}" -eq 0 ]; then
        return
    fi
    echo "Tool download failed after retries; requesting a fresh CI agent." >&2
    return "${TRANSIENT_NETWORK_EXIT_STATUS}"
}

# ── Install dependencies ────────────────────────────────────────────────────

# Extract a .zip file using whatever tool is available on the system.
# Tries: unzip, python3, perl, busybox. At least one is present on any Linux.
extract_zip() {
    local zip_file="$1" dest_dir="$2"
    if command -v unzip &>/dev/null; then
        unzip -q "$zip_file" -d "$dest_dir"
    elif command -v busybox &>/dev/null && busybox unzip -l "$zip_file" &>/dev/null; then
        busybox unzip -q "$zip_file" -d "$dest_dir"
    elif command -v perl &>/dev/null; then
        perl -MIO::Uncompress::Unzip=unzip -e '
            use IO::Uncompress::Unzip qw(unzip $UnzipError);
            use File::Path qw(make_path);
            use File::Basename qw(dirname);
            my ($src, $dst) = @ARGV;
            my $u = IO::Uncompress::Unzip->new($src) or die "Cannot open $src: $UnzipError\n";
            my $status;
            for ($status = 1; $status > 0; $status = $u->nextStream()) {
                my $name = $u->getHeaderInfo()->{Name};
                my $out = "$dst/$name";
                if ($name =~ m{/$}) { make_path($out); next; }
                make_path(dirname($out));
                open my $fh, ">", $out or die "Cannot write $out: $!\n";
                binmode $fh;
                my $buf;
                while ($u->read($buf) > 0) { print $fh $buf; }
                close $fh;
            }
        ' "$zip_file" "$dest_dir"
    elif command -v python3 &>/dev/null; then
        python3 -m zipfile -e "$zip_file" "$dest_dir"
    else
        echo "Error: no zip extraction tool found (need unzip, busybox, perl, or python3)" >&2
        return 1
    fi
}

install_protoc() {
    if command -v protoc &>/dev/null; then
        echo "protoc already installed ($(protoc --version))"
        return
    fi
    echo "Installing protoc v${PROTOC_VERSION} from GitHub releases..."
    local tmp
    tmp="$(mktemp -d)"
    curl_with_retries -fsSL \
        "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" \
        -o "${tmp}/protoc.zip"
    extract_zip "${tmp}/protoc.zip" "${tmp}/protoc"
    install -m 0755 "${tmp}/protoc/bin/protoc" /usr/local/bin/protoc
    # Install well-known types so protoc can resolve google/protobuf/*.proto
    cp -r "${tmp}/protoc/include/"* /usr/local/include/ 2>/dev/null || true
    rm -rf "${tmp}"
    echo "protoc $(protoc --version)"
}

install_c_toolchain() {
    if command -v cc &>/dev/null || command -v gcc &>/dev/null; then
        echo "C compiler already installed"
        return
    fi
    echo "Installing C toolchain (gcc, libc-dev, make)..."
    local run_root=""
    if [ "$(id -u)" -ne 0 ] && command -v sudo &>/dev/null; then
        run_root="sudo"
    fi
    $run_root apt-get update -qq
    $run_root env DEBIAN_FRONTEND=noninteractive \
        apt-get install -y -qq --no-install-recommends gcc libc-dev make
    echo "gcc $(gcc --version | head -1)"
}

install_test_runtime_dependencies() {
    local packages=()
    if ! command -v openssl &>/dev/null; then
        packages+=(openssl)
    fi
    if [ "${#packages[@]}" -eq 0 ]; then
        return
    fi

    local run_root=""
    if [ "$(id -u)" -ne 0 ] && command -v sudo &>/dev/null; then
        run_root="sudo"
    fi
    $run_root apt-get update -qq
    $run_root env DEBIAN_FRONTEND=noninteractive \
        apt-get install -y -qq --no-install-recommends "${packages[@]}"
}

install_rust() {
    if command -v rustup &>/dev/null; then
        echo "rustup already installed"
        rustup show active-toolchain
    else
        echo "Installing Rust via rustup..."
        curl_with_retries --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
        # shellcheck source=/dev/null
        source "$HOME/.cargo/env"
    fi
    # The exact toolchain is pinned in rust-toolchain.toml; rustup installs it
    # automatically when a cargo/rustc command runs in the repo. We deliberately
    # do NOT `rustup update` here: chasing the latest `stable` is what reintro-
    # duces local/CI lint drift (a newer clippy adds default-warn lints that fail
    # `-D warnings`). The pin is bumped deliberately in-repo instead.
    # Ensure rustfmt and clippy are available for the pinned toolchain.
    rustup component add rustfmt clippy 2>/dev/null || true
}

# Deterministic guard: refuse to run when the active toolchain is below the
# workspace `rust-version` floor declared in the root Cargo.toml. The pinned
# toolchain should always satisfy it, but the guard makes a bad pin (or a stale
# agent image ignoring the pin) a hard, self-explaining CI failure instead of
# confusing lint drift.
# Must run after `cd "$REPO_ROOT"` so rustc resolves via rust-toolchain.toml.
assert_msrv_floor() {
    local required active lowest
    required="$(grep -m1 -E '^[[:space:]]*rust-version[[:space:]]*=' \
        "${REPO_ROOT}/Cargo.toml" \
        | sed -E 's/.*"([0-9]+\.[0-9]+(\.[0-9]+)?)".*/\1/')"
    if [ -z "${required}" ]; then
        echo "Error: could not read rust-version floor from Cargo.toml" >&2
        return 1
    fi
    active="$(rustc --version | awk '{print $2}')"
    lowest="$(printf '%s\n%s\n' "${required}" "${active}" | sort -V | head -n1)"
    if [ "${lowest}" != "${required}" ]; then
        echo "Error: active rustc ${active} is below the workspace MSRV floor ${required}." >&2
        echo "CI must run on a toolchain >= ${required}; check the agent image / rustup." >&2
        return 1
    fi
    echo "MSRV floor OK: active rustc ${active} >= floor ${required}"
}

install_cargo_nextest() {
    if cargo nextest --version &>/dev/null; then
        echo "cargo-nextest already installed ($(cargo nextest --version))"
        return
    fi

    echo "Installing cargo-nextest ${CARGO_NEXTEST_VERSION} from prebuilt release..."
    local cargo_home="${CARGO_HOME:-${HOME}/.cargo}"
    local tmp
    tmp="$(mktemp -d)"
    mkdir -p "${cargo_home}/bin"
    curl_with_retries -fsSL "https://get.nexte.st/${CARGO_NEXTEST_VERSION}/linux" \
        -o "${tmp}/cargo-nextest.tar.gz"
    tar -xzf "${tmp}/cargo-nextest.tar.gz" -C "${tmp}"
    install -m 0755 "${tmp}/cargo-nextest" "${cargo_home}/bin/cargo-nextest"
    rm -rf "${tmp}"
    cargo nextest --version
}

run_install_tools() {
    local uv_bin
    uv_bin="$(bash "${SCRIPT_DIR}/ensure-uv.sh")"
    export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/space-uv-cache}"
    mkdir -p "${UV_CACHE_DIR}"
    "${uv_bin}" run --script "${SCRIPT_DIR}/install-tools.py" "$@"
}

wait_for_docker() {
    timeout 30 sh -c 'until docker info >/dev/null 2>&1; do sleep 1; done' 2>/dev/null
}

run_fmt_check() {
    echo "--- :rust: Checking generated shim environment contract"
    rustfmt --edition 2024 --check scripts/generate-env-contract.rs
    bash scripts/generate-env-contract.sh --check
    echo "--- :rust: Checking Rust error policy"
    bash scripts/check-rust-error-policy.sh
    echo "--- :rust: Running cargo fmt --check"
    cargo fmt --all -- --check
}

# The direct dependents of package $1 in the normal dependency graph of
# sbx-mgr's server and service, built with the feature arguments that follow
# up to `--`, must all be among the packages after `--`. `dependency_fence`
# checks only sbx-mgr's own manifests; this catches a crate elsewhere in the
# graph that starts depending on the package and re-exports it.
sbxmgr_assert_direct_dependents() {
    local package="$1"
    shift
    local feature_args=()
    while [ "$1" != "--" ]; do
        feature_args+=("$1")
        shift
    done
    shift
    local allowed=" $* "
    local graph dependent
    local unexpected=()
    graph="$(cargo tree --locked -p pplx-space-sbx-mgr -p pplx-space-sbxmgr-svc \
        "${feature_args[@]}" -e normal -i "${package}" --depth 1 --prefix none)"
    while read -r dependent; do
        if [[ "${allowed}" != *" ${dependent} "* ]]; then
            unexpected+=("${dependent}")
        fi
    done < <(awk 'NR > 1 { print $1 }' <<<"${graph}" | sort -u)
    if [ "${#unexpected[@]}" -gt 0 ]; then
        echo "${package} enters sbx-mgr's production graph (${feature_args[*]:-no features}) through ${unexpected[*]}; only $* may depend on it" >&2
        return 1
    fi
}

# sbx-mgr's SQLite store makes its raw connection and statement methods
# public only under its `test-support` feature, which the workspace pass's
# `--all-features` turns on. Lint sbx-mgr's production libraries and
# binaries without dev targets in every feature configuration its images
# are built with, so production code that calls a gated store method fails
# before merge, even behind a feature such as `otel`: no features, the
# release images' set (docker/Dockerfile.rust-build-common) and the Kind
# images' set (docker/Dockerfile.rust-build-kind). The migrator has no
# features of its own.
run_sbxmgr_production_checks() {
    local target_dir="$1"
    local sbxmgr_feature_sets=(
        ""
        "pplx-space-sbxmgr-svc/otel,pplx-space-sbxmgr-svc/datadog"
        "pplx-space-sbxmgr-svc/otel,pplx-space-sbxmgr-svc/datadog,pplx-space-sbxmgr-svc/test-hooks"
    )
    local sbxmgr_features sbxmgr_feature_args sbxmgr_graph
    for sbxmgr_features in "${sbxmgr_feature_sets[@]}"; do
        sbxmgr_feature_args=()
        if [ -n "${sbxmgr_features}" ]; then
            sbxmgr_feature_args=(--features "${sbxmgr_features}")
        fi
        echo "--- :rust: Running sbx-mgr production clippy without test-support (features: ${sbxmgr_features:-none})"
        CARGO_TARGET_DIR="${target_dir}" cargo clippy \
            --locked \
            -p pplx-space-sbx-mgr-store \
            -p pplx-space-sbx-mgr \
            -p pplx-space-sbxmgr-svc \
            -p pplx-space-db-migrate \
            -p pplx-space-db-migrator \
            "${sbxmgr_feature_args[@]}" \
            --lib --bins \
            -- \
            -D warnings
        sbxmgr_graph="$(cargo tree --locked \
            -p pplx-space-sbx-mgr -p pplx-space-sbxmgr-svc \
            -p pplx-space-db-migrate -p pplx-space-db-migrator \
            "${sbxmgr_feature_args[@]}" \
            -e normal,features --prefix none)"
        if grep -q '^pplx-space-sbx-mgr-store feature "test-support"' <<<"${sbxmgr_graph}"; then
            echo "sbx-mgr's production graph (features: ${sbxmgr_features:-none}) must not enable the store's test-support" >&2
            return 1
        fi
        # The shared SQLite helpers open, migrate and delete a database by
        # path; only the store may bring them into the server's graph.
        sbxmgr_assert_direct_dependents pplx-space-sqlite "${sbxmgr_feature_args[@]}" -- \
            pplx-space-sbx-mgr-store
        # rusqlite enters only through the store, the helpers it depends on,
        # and three shared crates whose rusqlite use hands no connection out:
        # common-jobs' functions take a connection in, and common-core and
        # accounts only implement rusqlite's value conversion traits for
        # their types.
        sbxmgr_assert_direct_dependents rusqlite "${sbxmgr_feature_args[@]}" -- \
            pplx-space-sbx-mgr-store pplx-space-sqlite pplx-space-common-jobs pplx-space-common-core \
            pplx-space-accounts
    done
}

run_clippy_checks() {
    local target_dir="target/buildkite-lint"

    # Enable every declared feature automatically so newly added production
    # feature paths and test targets cannot become invisible to CI.
    echo "--- :rust: Running workspace clippy"
    CARGO_TARGET_DIR="${target_dir}" cargo clippy \
        --workspace \
        --locked \
        --all-targets \
        --all-features \
        -- \
        -D warnings

    echo "--- :rust: Checking neutral observability without native Datadog"
    CARGO_TARGET_DIR="${target_dir}" cargo clippy \
        --locked -p pplx-space-observability --no-default-features \
        --features otel --all-targets -- -D warnings
    local otel_graph
    otel_graph="$(cargo tree --locked -p pplx-space-observability \
        --no-default-features --features otel -e normal,build --prefix none)"
    if grep -Eq '^(datadog|datadog-|libdd-|opentelemetry-datadog|dogstatsd|cadence|statsd)' <<<"${otel_graph}"; then
        echo "Neutral observability must not link a Datadog SDK" >&2
        return 1
    fi

    # Preserve the feature-isolation check without a redundant cargo check;
    # clippy compiles this same graph before running its lint passes.
    echo "--- :rust: Running common-svc no-default-features clippy"
    CARGO_TARGET_DIR="${target_dir}" cargo clippy \
        --locked \
        -p pplx-space-common-svc \
        --no-default-features \
        -- \
        -D warnings

    # Built alone, so a sqlx feature common-svc's migration-checksums feature
    # needs but only gets through unification with the rest of the workspace
    # fails here.
    echo "--- :rust: Running common-svc migration-checksums clippy"
    CARGO_TARGET_DIR="${target_dir}" cargo clippy \
        --locked \
        -p pplx-space-common-svc \
        --no-default-features \
        --features migration-checksums \
        -- \
        -D warnings

    run_sbxmgr_production_checks "${target_dir}"

    # feature-flags must lint clean with no vendor SDK compiled in; the matching
    # dependency-graph assertion lives in run_feature_isolation_checks.
    # `--all-targets` so the SDK-free test targets lint too; run_feature_isolation_checks
    # is what actually executes them.
    echo "--- :rust: Running feature-flags no-default-features clippy"
    CARGO_TARGET_DIR="${target_dir}" cargo clippy \
        --locked \
        -p pplx-space-feature-flags \
        --no-default-features \
        --all-targets \
        -- \
        -D warnings

    # Each of the account identity crate's features by itself, so one that
    # relies on another being enabled fails here; the isolated-crates mode
    # builds it only with no features and with all of them.
    echo "--- :rust: Running account identity clippy with each feature alone"
    local accounts_feature
    for accounts_feature in serde sqlx rusqlite; do
        CARGO_TARGET_DIR="${target_dir}" cargo clippy \
            --locked \
            -p pplx-space-accounts \
            --no-default-features \
            --features "${accounts_feature}" \
            --all-targets \
            -- \
            -D warnings
    done

    # The crates split out of common-core and common-svc, and the other
    # libraries beside them, are linted alone by run_isolated_crate_checks in
    # lib/rust-isolated-crates.sh (the isolated-crates mode), which finds them
    # in the workspace metadata. A new crate needs no step here.
}

nextest_test_threads() {
    local test_threads
    if [ -n "${NEXTEST_TEST_THREADS:-}" ]; then
        echo "${NEXTEST_TEST_THREADS}"
        return
    fi
    test_threads="$(nproc)"
    if [ "${test_threads}" -gt 12 ]; then
        test_threads=12
    fi
    echo "${test_threads}"
}

cleanup_nextest_containers_by_label() {
    local label_key="$1"
    local cleanup_id="$2"
    local container_ids=()
    mapfile -t container_ids < <(
        docker ps --all --quiet \
            --filter "label=${label_key}=${cleanup_id}"
    )
    if [ "${#container_ids[@]}" -gt 0 ]; then
        # Tolerated, like every other `docker rm --force` in the repo: the
        # daemon may already be removing these, and under `set -e` a lost race
        # would abort before run_test_checks returns the nextest status it
        # carefully captured — failing a job whose tests all passed.
        docker rm --force "${container_ids[@]}" >/dev/null 2>&1 || true
    fi
}

cleanup_nextest_postgres() {
    cleanup_nextest_containers_by_label "space.nextest.cleanup-id" "$1"
}

# The Redis setup script is fail-open (see setup-nextest-redis.sh), so it may
# have started no container at all; the filter then matches nothing and this
# is a no-op, same as the postgres path when nothing needed cleanup.
cleanup_nextest_redis() {
    cleanup_nextest_containers_by_label "space.nextest.redis-cleanup-id" "$1"
}

# Surface the test-dependency skip markers collected during the nextest run.
# Passing tests' captured stderr is hidden by nextest, so the log file is the
# only place the markers survive; a Buildkite annotation makes the count
# visible per build while the soak decides whether strictness can be flipped.
report_test_dep_skips() {
    local log="${SPACE_TEST_DEP_SKIP_LOG:-}"
    [ -n "${log}" ] || return 0
    local count=0
    if [ -s "${log}" ]; then
        count="$(wc -l < "${log}" | tr -d '[:space:]')"
    fi
    echo "--- :mag: test-dependency skip markers: ${count}"
    if [ "${count}" -gt 0 ]; then
        cat "${log}"
        if command -v buildkite-agent >/dev/null 2>&1; then
            {
                echo "**${count} test(s) skipped for missing test dependencies** (SPACE-TEST-DEP-SKIP)"
                echo '```'
                cat "${log}"
                echo '```'
            } | buildkite-agent annotate --style warning --context space-test-dep-skips || true
        fi
    elif command -v buildkite-agent >/dev/null 2>&1; then
        # A retry shares build-scoped annotation contexts with earlier job
        # attempts. Remove an old warning when the retry is clean so the
        # per-build soak signal reflects the latest result.
        buildkite-agent annotation remove --context space-test-dep-skips || true
    fi
}

prepare_test_dep_skip_log() {
    local target_dir="$1"
    local log="$2"
    # `-p` is portable across GNU and BSD mkdir. Keep it outside
    # run_test_checks because the completeness guard rejects `-p` there as a
    # possible Cargo package selector.
    mkdir -p "${target_dir}"
    rm -f "${log}"
}

# The test build's binary list and Cargo metadata, which let the privileged
# step run the same test binaries without invoking a build.
test_reuse_metadata() {
    local target_dir="$1"
    printf '%s\n' \
        --binaries-metadata "${target_dir}/nextest-binaries.json" \
        --cargo-metadata "${target_dir}/nextest-cargo-metadata.json"
}

write_test_reuse_metadata() {
    local target_dir="$1"
    CARGO_TARGET_DIR="${target_dir}" cargo nextest list \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --workspace \
        --locked \
        --all-targets \
        --all-features \
        --list-type binaries-only \
        --message-format json >"${target_dir}/nextest-binaries.json"
    cargo metadata --format-version 1 --locked --all-features \
        >"${target_dir}/nextest-cargo-metadata.json"
}

# Every test in the inventory, one per line as `<binary-id> <test-name>`,
# narrowed by any filterset arguments given.
list_inventory() {
    local target_dir="$1"
    shift
    local reuse=()
    mapfile -t reuse < <(test_reuse_metadata "${target_dir}")
    # nextest's notices go to the log only when the listing fails.
    local errors="${target_dir}/nextest-list.err"
    if ! CARGO_TARGET_DIR="${target_dir}" cargo nextest list "${reuse[@]}" \
        --run-ignored all \
        --ignore-default-filter \
        --message-format oneline \
        "$@" 2>"${errors}" | LC_ALL=C sort; then
        cat "${errors}" >&2
        return 1
    fi
}

# The privileged group's filter clauses from .config/nextest.toml, one per
# line, as written there.
privileged_group_clauses() {
    python3 - "${REPO_ROOT}/.config/nextest.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as config_file:
    config = tomllib.load(config_file)
overrides = config.get("profile", {}).get("default", {}).get("overrides", [])
filters = [o["filter"] for o in overrides if o.get("test-group") == "privileged"]
if len(filters) != 1:
    sys.exit(f"expected one override assigning test-group privileged, found {len(filters)}")
for line in filters[0].splitlines():
    clause = line.strip()
    if clause.startswith("or "):
        clause = clause[3:]
    if clause:
        print(clause)
PY
}

# `not group(privileged)` is the complement of the group by definition, so a
# mistake in the group's filter never drops a test: it moves the test to the
# unprivileged half. Catch the mistakes that do that silently. Every clause
# must match at least one test, so a renamed test or a typo fails. And every
# test a clause matches must be in the group, so an earlier override that
# assigns another group fails. A privileged test that no clause names is not
# detectable here; it fails the unprivileged half only if it asserts what it
# needs. The group's list is kept for the privileged step to compare against.
check_privileged_group() {
    local target_dir="$1"
    local privileged="${target_dir}/nextest-privileged.txt"
    local total
    echo "--- :scales: Checking the privileged test group"
    total="$(list_inventory "${target_dir}" | wc -l | tr -d '[:space:]')"
    list_inventory "${target_dir}" -E "${PRIVILEGED_TESTS_FILTERSET}" >"${privileged}"
    local clauses=()
    mapfile -t clauses < <(privileged_group_clauses)
    if [ "${#clauses[@]}" -eq 0 ]; then
        echo "::error::The privileged test group has no filter clauses." >&2
        return 1
    fi
    local failures=0
    local clause matched outside
    for clause in "${clauses[@]}"; do
        if ! matched="$(list_inventory "${target_dir}" -E "${clause}")"; then
            echo "::error::Privileged test clause does not parse on its own: ${clause}" >&2
            failures=$((failures + 1))
            continue
        fi
        if [ -z "${matched}" ]; then
            echo "::error::Privileged test clause matches no test: ${clause}" >&2
            failures=$((failures + 1))
            continue
        fi
        outside="$(LC_ALL=C comm -23 <(printf '%s\n' "${matched}") "${privileged}")"
        if [ -n "${outside}" ]; then
            echo "::error::Privileged test clause matches tests outside the group, which an earlier override assigns elsewhere: ${clause}" >&2
            printf '  %s\n' "${outside}" >&2
            failures=$((failures + 1))
        fi
    done
    echo "${total} tests; ${#clauses[@]} privileged clauses select $(wc -l <"${privileged}" | tr -d '[:space:]') of them:"
    sed 's/^/  /' "${privileged}"
    if [ "${failures}" -ne 0 ]; then
        echo "${failures} privileged test clause(s) are wrong; see .config/nextest.toml." >&2
        return 1
    fi
}

# tests-unprivileged must not run as root: a test needing privilege that is
# missing from the privileged group would then pass here instead of failing.
# The test user needs a passwd entry, since the spaced tests resolve the
# current user's name and groups.
require_unprivileged_user() {
    if [ "$(id -u)" -eq 0 ]; then
        echo "tests-unprivileged must not run as root; use tests or tests-privileged." >&2
        return 1
    fi
    if ! getent passwd "$(id -u)" >/dev/null; then
        echo "uid $(id -u) has no passwd entry; run as the CI image's space-ci user." >&2
        return 1
    fi
}

# The privileged step runs as root in the runner's workspace. Hand anything it
# created back to the workspace owner so later steps and caches can use it.
# Its caller runs it on the left of `||`, where errexit is off, so each step
# returns its own failure.
restore_workspace_ownership() {
    local owner
    owner="$(stat -c '%u:%g' .)" || return $?
    local count
    count="$(find . -xdev ! -user "${owner%%:*}" -print | wc -l | tr -d '[:space:]')" || return $?
    if [ "${count}" -gt 0 ]; then
        echo "Returning ${count} file(s) created as root to ${owner}"
        find . -xdev ! -user "${owner%%:*}" -exec chown -h "${owner}" {} +
    fi
}

run_test_checks() {
    local target_dir="target/buildkite-tests"
    local test_threads
    local nextest_status=0
    test_threads="$(nextest_test_threads)"
    SPACE_NEXTEST_POSTGRES_CLEANUP_ID="${BUILDKITE_JOB_ID:-local-$$}"
    export SPACE_NEXTEST_POSTGRES_CLEANUP_ID
    SPACE_NEXTEST_REDIS_CLEANUP_ID="${BUILDKITE_JOB_ID:-local-$$}"
    export SPACE_NEXTEST_REDIS_CLEANUP_ID

    # The fixtures append one SPACE-TEST-DEP-SKIP line here per skip a run
    # opted into with SPACE_TEST_DEPS_OPTIONAL. CI is strict, so a dependency
    # outage fails its test; the zero-count report remains useful evidence that
    # no fail-open path was taken.
    SPACE_TEST_DEP_SKIP_LOG="$(pwd)/${target_dir}/space-test-dep-skips.log"
    export SPACE_TEST_DEP_SKIP_LOG
    prepare_test_dep_skip_log "${target_dir}" "${SPACE_TEST_DEP_SKIP_LOG}"

    # The nextest setup scripts pull these images before any test starts.
    # Pulling now overlaps the downloads with the test build; the setup scripts
    # skip an image that is already present and retry one that failed here.
    # Without Docker (tests-unprivileged), the runner already started both.
    local image_pulls=()
    local image
    if [ "${test_partition}" = all ]; then
        for image in "$(space_test_dependency_postgres_image)" "$(space_test_dependency_redis_image)"; do
            space_pull_docker_image "${image}" >/dev/null 2>&1 &
            image_pulls+=("$!")
        done
    fi

    # Build with nextest's Cargo arguments first so the doctests, the M1 gate
    # and the feature-isolation checks can run beside the test run; nextest's
    # own build is then a no-op.
    # Cargo holds the build-directory lock only while compiling, so the side
    # checks' small builds do not wait for the tests to finish.
    echo "--- :rust: Building every workspace test"
    CARGO_TARGET_DIR="${target_dir}" cargo test \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --workspace \
        --locked \
        --all-targets \
        --all-features \
        --timings \
        --no-run

    if [ "${test_partition}" = unprivileged ]; then
        write_test_reuse_metadata "${target_dir}"
        check_privileged_group "${target_dir}"
    fi

    local side_log="${target_dir}/side-checks.log"
    local side_status=0
    run_side_checks "${target_dir}" >"${side_log}" 2>&1 &
    local side_checks="$!"

    echo "--- :rust: Running every workspace test"
    CARGO_TARGET_DIR="${target_dir}" cargo nextest run \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --workspace \
        --locked \
        --all-targets \
        --all-features \
        --run-ignored all \
        --ignore-default-filter \
        --retries 1 \
        --no-fail-fast \
        --test-threads "${test_threads}" \
        ${NEXTEST_PARTITION_ARGS[@]+"${NEXTEST_PARTITION_ARGS[@]}"} || nextest_status=$?

    if [ "${test_partition}" = all ]; then
        wait "${image_pulls[@]}" || true
        cleanup_nextest_postgres "${SPACE_NEXTEST_POSTGRES_CLEANUP_ID}"
        cleanup_nextest_redis "${SPACE_NEXTEST_REDIS_CLEANUP_ID}"
    fi
    report_test_dep_skips

    wait "${side_checks}" || side_status=$?
    cat "${side_log}"
    if [ "${nextest_status}" -ne 0 ]; then
        return "${nextest_status}"
    fi
    return "${side_status}"
}

# The privileged half, from the build tests-unprivileged left in the target
# directory: nextest runs those binaries without invoking Cargo's build, so this
# root container compiles nothing into the runner's target directory.
run_privileged_test_checks() {
    local target_dir="target/buildkite-tests"
    local test_threads
    local nextest_status=0
    local m1_status=0
    test_threads="$(nextest_test_threads)"
    local file
    for file in nextest-binaries.json nextest-cargo-metadata.json nextest-privileged.txt; do
        if [ ! -s "${target_dir}/${file}" ]; then
            echo "${target_dir}/${file} is missing; tests-unprivileged writes it after the test build." >&2
            return 1
        fi
    done
    SPACE_NEXTEST_POSTGRES_CLEANUP_ID="${BUILDKITE_JOB_ID:-local-$$}-privileged"
    export SPACE_NEXTEST_POSTGRES_CLEANUP_ID
    SPACE_NEXTEST_REDIS_CLEANUP_ID="${SPACE_NEXTEST_POSTGRES_CLEANUP_ID}"
    export SPACE_NEXTEST_REDIS_CLEANUP_ID
    SPACE_TEST_DEP_SKIP_LOG="$(pwd)/${target_dir}/space-test-dep-skips-privileged.log"
    export SPACE_TEST_DEP_SKIP_LOG
    prepare_test_dep_skip_log "${target_dir}" "${SPACE_TEST_DEP_SKIP_LOG}"

    # Run exactly the tests the unprivileged step's group check listed.
    list_inventory "${target_dir}" -E "${PRIVILEGED_TESTS_FILTERSET}" \
        >"${target_dir}/nextest-privileged-here.txt"
    if ! cmp -s "${target_dir}/nextest-privileged.txt" "${target_dir}/nextest-privileged-here.txt"; then
        echo "The privileged tests differ from the ones the unprivileged step checked:" >&2
        diff "${target_dir}/nextest-privileged.txt" "${target_dir}/nextest-privileged-here.txt" >&2 || true
        return 1
    fi

    local reuse=()
    mapfile -t reuse < <(test_reuse_metadata "${target_dir}")
    echo "--- :rust: Running the privileged workspace tests"
    CARGO_TARGET_DIR="${target_dir}" cargo nextest run "${reuse[@]}" \
        --run-ignored all \
        --ignore-default-filter \
        --retries 1 \
        --no-fail-fast \
        --test-threads "${test_threads}" \
        -E "${PRIVILEGED_TESTS_FILTERSET}" || nextest_status=$?

    cleanup_nextest_postgres "${SPACE_NEXTEST_POSTGRES_CLEANUP_ID}"
    cleanup_nextest_redis "${SPACE_NEXTEST_REDIS_CLEANUP_ID}"
    report_test_dep_skips

    check_m1_document "${target_dir}" || m1_status=$?
    if [ "${nextest_status}" -ne 0 ]; then
        return "${nextest_status}"
    fi
    return "${m1_status}"
}

# nextest executes Cargo's binary test targets but intentionally does not
# execute rustdoc tests. Cargo discovers and runs that complementary workspace
# inventory without package or doctest-name selectors.
run_workspace_doctests() {
    local target_dir="$1"
    echo "--- :rust: Running every workspace doctest"
    CARGO_TARGET_DIR="${target_dir}" cargo test \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --workspace \
        --locked \
        --all-features \
        --doc \
        --no-fail-fast \
        -- \
        --include-ignored
}

# The M1 document is generated by this workspace, so its SDK gate runs here
# rather than in a step that would compile the workspace a second time to print
# it once. The test profile reuses the test build; the default dev profile lacks
# the test profile's package overrides and would rebuild the API crate.
# `--examples` only adds the stable example path, which `--all-targets` builds
# but does not link. `tests` mode also already requires the Docker daemon the
# pinned generator needs; tests-unprivileged has none, so it only writes the
# document and tests-privileged checks it. Stay workspace-wide, without package
# selectors.
run_m1_sdk_check() {
    local target_dir="$1"
    generate_m1_document "${target_dir}"
    if [ "${m1_generator_check}" = deferred ]; then
        echo "The M1 generator check runs in tests-privileged, which has Docker."
        return 0
    fi
    check_m1_document "${target_dir}"
}

generate_m1_document() {
    local target_dir="$1"
    echo "--- :snake: Generating the M1 OpenAPI document"
    CARGO_TARGET_DIR="${target_dir}" cargo build \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --profile test \
        --workspace \
        --locked \
        --all-features \
        --examples
    "${target_dir}/debug/examples/m1_openapi" >"${target_dir}/m1-openapi.json"
}

check_m1_document() {
    local target_dir="$1"
    echo "--- :snake: Checking M1 generated-SDK conformance"
    bash "${REPO_ROOT}/scripts/check-m1-sdk-generation.sh" "${target_dir}/m1-openapi.json"
}

# ── Main ────────────────────────────────────────────────────────────────────

main() {
cd "$REPO_ROOT"

validation_mode="${1:-all}"
usage="Usage: $0 [fmt|clippy|isolated-crates|tests|tests-unprivileged|tests-privileged|all]"
if [ "$#" -gt 1 ]; then
    echo "${usage}" >&2
    exit 2
fi
case "${validation_mode}" in
    fmt | clippy | isolated-crates | tests | tests-unprivileged | tests-privileged | all) ;;
    *)
        echo "${usage}" >&2
        exit 2
        ;;
esac

needs_build_dependencies=false
needs_nextest=false
needs_docker=false
restore_ownership_on_exit=false
sccache_active=false
sccache_cleanup=false
# Registered before any preflight can exit: the privileged step's root-owned
# files must go back to the workspace owner however it ends, or the runner's
# cache-save steps cannot read them.
on_exit() {
    local status=$?
    if [ "${sccache_cleanup}" = true ]; then
        cleanup
    fi
    if [ "${restore_ownership_on_exit}" = true ]; then
        restore_workspace_ownership || {
            local restore_status=$?
            echo "Could not return root-owned workspace files to their owner (status ${restore_status})." >&2
            if [ "${status}" -eq 0 ]; then
                status="${restore_status}"
            fi
        }
    fi
    exit "${status}"
}
trap on_exit EXIT
case "${validation_mode}" in
    clippy | isolated-crates)
        needs_build_dependencies=true
        ;;
    tests | all)
        needs_build_dependencies=true
        needs_nextest=true
        needs_docker=true
        ;;
    tests-unprivileged)
        require_unprivileged_user
        needs_build_dependencies=true
        needs_nextest=true
        test_partition="unprivileged"
        NEXTEST_PARTITION_ARGS=(-E "${UNPRIVILEGED_TESTS_FILTERSET}")
        m1_generator_check="deferred"
        ;;
    tests-privileged)
        if [ "$(id -u)" -ne 0 ]; then
            echo "tests-privileged runs the tests that need root; run it as root." >&2
            exit 1
        fi
        needs_nextest=true
        needs_docker=true
        restore_ownership_on_exit=true
        ;;
esac

echo "--- :gear: Preparing Rust toolchain"
install_rust
assert_msrv_floor

tools=()
if [ "${needs_build_dependencies}" = true ]; then
    echo "--- :gear: Installing build dependencies"
    install_protoc
    install_c_toolchain
    tools+=(sccache wild)
fi
if [ "${needs_nextest}" = true ]; then
    install_cargo_nextest
    install_test_runtime_dependencies
fi
if [ "${needs_docker}" = true ]; then
    tools+=(docker-cli)
fi
if [ "${#tools[@]}" -gt 0 ]; then
    run_install_tools "${tools[@]}"
    export PATH="${LOCAL_BIN_DIR}:${PATH}"
fi

# Ensure `cc` is discoverable for C build scripts. Rust's native links use gcc
# as the compiler driver, with a stable -B search directory that resolves `ld`
# to Wild. GCC versions before 16 do not support `-fuse-ld=wild` directly.
if [ "${needs_build_dependencies}" = true ] && ! command -v cc &>/dev/null; then
    if command -v gcc &>/dev/null; then
        echo "cc not found — creating symlink to gcc"
        gcc_path="$(command -v gcc)"
        ln -sf "$gcc_path" "$(dirname "$gcc_path")/cc"
    elif command -v clang &>/dev/null; then
        echo "cc not found — creating symlink to clang"
        clang_path="$(command -v clang)"
        ln -sf "$clang_path" "$(dirname "$clang_path")/cc"
    else
        echo "Error: no C compiler found (need cc, gcc, or clang)" >&2
        exit 1
    fi
fi
if [ "${needs_build_dependencies}" = true ]; then
    wild_linker_dir="/tmp/space-wild-linker"
    mkdir -p "${wild_linker_dir}"
    ln -sf "$(command -v wild)" "${wild_linker_dir}/ld"

    # Keep C compilation on gcc while routing every native Rust link through Wild.
    export CC="${CC:-gcc}"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="gcc"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-Clink-arg=-B${wild_linker_dir}"
    # rustdoc reads RUSTDOCFLAGS rather than the target's RUSTFLAGS, so doctest
    # binaries would otherwise link through the system linker.
    export RUSTDOCFLAGS="${RUSTDOCFLAGS:+${RUSTDOCFLAGS} }-Clink-arg=-B${wild_linker_dir}"
    resolved_ld="$(gcc -B"${wild_linker_dir}" -print-prog-name=ld)"
    echo "Using CC=$CC, Cargo linker=gcc, ld=${resolved_ld}"
    wild --version
fi

# Test builds keep file and line tables for backtraces, not full debugger
# information. Build dependencies are pinned to none because Cargo's default
# ("none when possible") depends on whether the same crate is also needed at
# runtime in that invocation. Otherwise the doctest run would rebuild the
# workspace's build-script helper, rerun the protobuf build scripts, and
# recompile everything generated from them. The test profile inherits both.
if [ "${needs_nextest}" = true ]; then
    export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-line-tables-only}"
    export CARGO_PROFILE_DEV_BUILD_OVERRIDE_DEBUG="${CARGO_PROFILE_DEV_BUILD_OVERRIDE_DEBUG:-false}"
    # These package overrides in Cargo.toml work around Mach-O unwind limits.
    # Linux has no such limit; share the same unoptimized graph across nextest,
    # doctests and the generated-SDK build so none recompiles the API package.
    if [ "$(uname -s)" = Linux ]; then
        TEST_PROFILE_ARGS=(
            --config 'profile.test.package.pplx-space-api.opt-level=0'
            --config 'profile.test.package.pplx-space-sbx-mgr.opt-level=0'
        )
    fi
fi

if [ "${needs_docker}" = true ]; then
    export TESTCONTAINERS_POSTGRES_IMAGE="$(space_test_dependency_postgres_image)"
    export TESTCONTAINERS_RYUK_DISABLED="${TESTCONTAINERS_RYUK_DISABLED:-true}"

    echo "--- :docker: Checking Docker availability"
    if ! wait_for_docker; then
        echo "Docker daemon is not available on this agent." >&2
        exit 1
    fi
    echo "✅ Docker daemon is available"
fi

cleanup() {
    if [ "${sccache_active}" = true ]; then
        echo "--- :bar_chart: sccache statistics"
        sccache --show-stats || true
        # GitHub Actions reads the Rust misses from this file to decide whether
        # main refreshes the compiler-cache snapshot.
        if [ -n "${SPACE_CI_SCCACHE_STATS:-}" ]; then
            sccache --show-stats --stats-format=json >"${SPACE_CI_SCCACHE_STATS}" || true
        fi
        sccache --stop-server >/dev/null 2>&1 || true
    elif [ "${needs_build_dependencies}" = true ]; then
        echo "--- :warning: sccache was unavailable; statistics skipped"
    fi
}
sccache_cleanup=true
if [ "${needs_build_dependencies}" != true ]; then
    unset RUSTC_WRAPPER
fi
if [ "${validation_mode}" = "fmt" ]; then
    run_fmt_check
    echo "--- :rust: Rust ${validation_mode} validation passed"
    exit 0
fi

# Buildkite checkouts live under build-specific directories. Normalize the
# repository prefix so otherwise-identical compiler inputs can share cache keys.
export SCCACHE_BASEDIRS="${SCCACHE_BASEDIRS:-${REPO_ROOT}}"
if [ "${needs_build_dependencies}" = true ]; then
    if sccache --zero-stats; then
        sccache_active=true
    else
        echo "WARNING: sccache initialization failed; continuing without compiler caching." >&2
        unset RUSTC_WRAPPER
    fi
fi

if [ "${needs_build_dependencies}" = true ]; then
    echo "--- :rust: Fetching Cargo dependencies"
    cargo fetch --locked
fi

case "${validation_mode}" in
    clippy)
        run_clippy_checks
        ;;
    isolated-crates)
        run_isolated_crate_checks
        ;;
    tests | tests-unprivileged)
        run_test_checks
        ;;
    tests-privileged)
        run_privileged_test_checks
        ;;
    all)
        run_fmt_check
        run_clippy_checks
        run_isolated_crate_checks
        run_test_checks
        ;;
esac

echo "--- :rust: Rust ${validation_mode} validation passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
