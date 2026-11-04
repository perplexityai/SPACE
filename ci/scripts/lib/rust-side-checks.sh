#!/usr/bin/env bash
# The Rust tests' side checks, which run beside nextest after the test build
# (run_side_checks, called from run_test_checks in run-rust-checks.sh).
#
# They live apart from run-rust-checks.sh so that changing them keeps main's
# reusable Cargo target directory: its cache key hashes run-rust-checks.sh,
# which decides how target/buildkite-tests is built (see rust-target-cache.sh
# and "Name the Cargo target directory" in .github/workflows/common-ci.yml).
# Nothing here may build in that directory except through a function defined
# there, as the doctests and the M1 gate do. The feature-isolation checks build
# in their own directory, which is never cached.

# Non-default feature configurations, each pinned to the one package whose
# dependency boundary it proves. Additional to the workspace suite in
# run_test_checks, never a narrowing of it: that suite stays workspace-wide and
# selector-free. Concretely, this re-runs the crate's own tests under a second
# feature set, picks up the ones gated on `cfg(not(feature = "eppo"))` that exist
# only there, and then asserts dependency-graph properties that no amount of
# compiling or testing can establish. Deliberately uncounted: the exact split
# has already drifted twice as tests were added on either side, and a number
# here has nothing enforcing it.
#
# Their own target directory: this graph shares few units with the workspace
# test build, and building it in that one would hold the build-directory lock
# that nextest's own build waits for while the checks run beside it.
run_feature_isolation_checks() {
    local target_dir="target/buildkite-feature-isolation"
    local test_threads
    test_threads="$(nextest_test_threads)"

    # Separate Cargo invocations keep workspace feature unification from hiding
    # selector mistakes in builds without the native adapter. The deprecated
    # `datadog-otel` is not among them: it enables exactly `datadog`'s features,
    # and no code tests for it. The runs start together. Cargo's build-directory
    # lock still builds them one at a time, but each run's tests then overlap
    # the next one's build; three of them wait out a 3 s export timeout. Their
    # logs print in order once all have ended; the first failure in that order
    # is returned.
    local telemetry_features log index run_status status=0
    local runs=() logs=()
    mkdir -p "${target_dir}"
    for telemetry_features in "" otlp-metrics otel datadog; do
        log="${target_dir}/observability-${telemetry_features:-none}.log"
        logs+=("${log}")
        (
            echo "--- :rust: Testing observability features: ${telemetry_features:-none}"
            CARGO_TARGET_DIR="${target_dir}" cargo nextest run \
                --locked -p pplx-space-observability --no-default-features \
                --features "${telemetry_features}" --all-targets \
                --no-fail-fast --test-threads "${test_threads}"
        ) >"${log}" 2>&1 &
        runs+=("$!")
    done
    for index in "${!runs[@]}"; do
        run_status=0
        wait "${runs[index]}" || run_status=$?
        cat "${logs[index]}"
        if [ "${status}" -eq 0 ]; then
            status="${run_status}"
        fi
    done
    if [ "${status}" -ne 0 ]; then
        return "${status}"
    fi

    # An SDK-free feature-flags build has to be tested, not just linted:
    # compiling proves no code path still reaches for the vendor SDK, while only
    # running the tests proves the client still resolves each caller's default
    # and that `from_env` still answers in a build with no provider compiled in.
    echo "--- :rust: Running feature-flags no-default-features tests"
    CARGO_TARGET_DIR="${target_dir}" cargo nextest run \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --locked \
        -p pplx-space-feature-flags \
        --no-default-features \
        --all-targets \
        --no-fail-fast \
        --test-threads "${test_threads}"

    # The crate's runnable provider example has to compile AND execute with no
    # SDK present, so it is evidence rather than prose. `cargo test --doc` exits
    # 0 when it runs nothing, so this asserts the identity of that doctest
    # rather than a pass count. A count is not the claim, and two mutations get
    # through one — both verified:
    #
    #   - `no_run` compile-checks and reports `1 passed` while never running the
    #     body, so the example's assertions stop executing. It is distinguishable
    #     only by the line it prints: `- (line N) - compile ... ok`.
    #   - an `ignore`d example plus any newly added method-level doctest also
    #     yields a non-zero aggregate count, and the crate has plenty of public
    #     methods with no examples yet, so adding one is ordinary work.
    #
    # Matching `- (line N) ... ok` with nothing between `)` and `...` admits only
    # a doctest that actually ran, in this file.
    #
    # The other three steps already have a floor: nextest exits 4 on an empty
    # selection (and 105 when a setup script fails), and the two graph
    # assertions fail closed.
    echo "--- :rust: Running feature-flags no-default-features doctests"
    local doctest_log="${target_dir}/feature-flags-no-default-doctests.log"
    if ! CARGO_TARGET_DIR="${target_dir}" cargo test \
        ${TEST_PROFILE_ARGS[@]+"${TEST_PROFILE_ARGS[@]}"} \
        --locked \
        -p pplx-space-feature-flags \
        --no-default-features \
        --doc \
        --no-fail-fast \
        >"${doctest_log}" 2>&1; then
        cat "${doctest_log}"
        return 1
    fi
    cat "${doctest_log}"
    if ! grep -qE \
        '^test crates/l10_foundation/feature/flags/src/lib\.rs - \(line [0-9]+\) \.\.\. ok$' \
        "${doctest_log}"; then
        echo "no crate-level doctest in crates/l10_foundation/feature/flags/src/lib.rs ran to" \
            "completion without the eppo feature; one is missing, ignored, or" \
            "fenced no_run" >&2
        return 1
    fi
    # The check above is satisfied by *any* crate-level doctest that ran, so on
    # its own it degrades to "some example executed" the moment a second one is
    # added — fence the provider example `no_run`, add an example elsewhere in
    # the crate docs, and it would pass. Verified. So also reject any
    # crate-level example that is attributed or ignored, which holds however
    # many there are.
    if grep -qE \
        '^test crates/l10_foundation/feature/flags/src/lib\.rs - \(line [0-9]+\)( - (compile|should panic) \.\.\. ok| \.\.\. ignored)$' \
        "${doctest_log}"; then
        echo "a crate-level doctest in crates/l10_foundation/feature/flags/src/lib.rs is" \
            "ignored or fenced so its body never executes; the SDK-free build's" \
            "runnable evidence has to actually run" >&2
        return 1
    fi

    # Compiling clean does NOT prove the vendor SDK left the graph, which is the
    # property the crate docs actually promise a consumer. Make `eppo` a plain
    # dependency and drop `dep:eppo` from the feature list: cargo accepts it,
    # every check above stays green, and every consumer silently regains the SDK.
    # Checked by mutation. `--prefix none` plus a whole-line match keeps this
    # from tripping over a checkout path that happens to contain "eppo".
    echo "--- :rust: Asserting eppo is absent from the SDK-free normal dep graph"
    # Resolve first, into a variable. Piping `cargo tree` straight into `if`
    # would fail OPEN: the pipeline's non-zero status gets consumed by the `if`,
    # so a cargo error would take the false branch and the check would "pass"
    # having inspected nothing. Assigning under `set -e` makes that a hard fail.
    local sdk_free_graph sdk_free_packages
    sdk_free_graph="$(
        CARGO_TARGET_DIR="${target_dir}" cargo tree \
            --locked \
            -p pplx-space-feature-flags \
            --no-default-features \
            -e normal \
            --prefix none
    )"
    # Separate assignment, not `local x="$(...)"`: with the declaration and the
    # assignment combined, `local` is the status-bearing command and a cargo or
    # awk failure would be masked.
    sdk_free_packages="$(awk '{print $1}' <<<"${sdk_free_graph}")"
    # Positive control before the negative one: "no eppo in this list" is only
    # evidence if the list is a real dependency graph. Name the crate's
    # unconditional dependencies rather than its own root package — the root is
    # the one line `cargo tree` prints no matter what, so matching it would only
    # distinguish "cargo printed something" from "cargo printed nothing", and a
    # root-only graph would sail through the eppo check below and get certified
    # on zero dependency edges. Verified: it did.
    #
    # Listing them here also states the invariant. The day one of these becomes
    # feature-gated — `crates/l10_foundation/feature/flags/Cargo.toml` discusses exactly
    # that — this breaks loudly, which is the moment the check would otherwise
    # lose its evidence base silently. `pplx-space-metrics` is on the list for
    # that reason and not only as a control: an SDK-free deployment is the one
    # with no vendor console, so putting the flag counters back behind the
    # `eppo` feature would take away the only evidence it has that its flags
    # are inert on purpose.
    local required
    for required in pplx-space-feature-flags pplx-space-metrics serde_json tracing; do
        if ! grep -qxF "${required}" <<<"${sdk_free_packages}"; then
            echo "the --no-default-features dependency graph for" \
                "pplx-space-feature-flags is missing ${required}; it is not a" \
                "graph this check can draw a conclusion from" >&2
            return 1
        fi
    done
    if grep -qxE 'eppo|eppo_core' <<<"${sdk_free_packages}"; then
        echo "pplx-space-feature-flags still depends on eppo with --no-default-features" >&2
        return 1
    fi

    # And the direction that actually ships. The assertion above proves the SDK
    # *can* leave the graph; nothing else in CI proves it is *there* by default,
    # and default features are what a release image resolves —
    # `docker/Dockerfile.rust-build-common` builds with `--bin` and never
    # `--all-features`. `default = []` in the crate manifest is valid TOML that
    # every other gate passes: clippy both ways, both nextest runs, and the
    # assertion above (which sees no eppo and is satisfied). It would put every
    # SPACE flag at its compiled-in default in every shipped binary, with the
    # `from_env` warn as the only signal anywhere. Verified by mutation.
    #
    # Both blocks resolve `-e normal`, the normal plus proc-macro graph, so
    # build- and dev-dependency channels are out of scope. That is the right
    # scope for what a consumer links, and the crate docs make the same bounded
    # claim — both stop at what a consumer links. A build- or dev-dependency on
    # the SDK would not be caught here; it also would not reach a shipped
    # binary. (It happens to be absent from `-e dev` today too.)
    #
    # This one needs no separate positive control: `grep -qxF eppo` IS the
    # positive assertion, so an unreadable graph or a broken PATH fails it
    # closed. That asymmetry is the whole reason the block above needs one and
    # this block doesn't — an assertion of *absence* is satisfied vacuously by a
    # degraded graph, an assertion of *presence* is not.
    #
    # Do not delete either block as redundant: they are mutually protective. The
    # absence check matches the literal names `eppo`/`eppo_core`, so an SDK
    # vendored or re-published under some other name would evade it — but such a
    # crate would also stop the default graph containing a package named `eppo`,
    # so this block fires instead. Neither is sufficient alone.
    echo "--- :rust: Asserting eppo IS present in the default normal dep graph"
    local default_graph default_packages
    default_graph="$(
        CARGO_TARGET_DIR="${target_dir}" cargo tree \
            --locked \
            -p pplx-space-feature-flags \
            -e normal \
            --prefix none
    )"
    default_packages="$(awk '{print $1}' <<<"${default_graph}")"
    if ! grep -qxF 'eppo' <<<"${default_packages}"; then
        echo "pplx-space-feature-flags no longer pulls eppo with default features;" \
            "every shipped binary would resolve flags to their compiled-in defaults" >&2
        return 1
    fi
}

# Run one side check and say when it started and ended. The check runs as its
# own job rather than as `check || status=$?`: the checks stop at their first
# failure through errexit, which a `||` would switch off inside them.
run_timed_side_check() {
    local started="${SECONDS}"
    local status=0
    echo "--- :stopwatch: $1 started at $(date -u +%T)"
    "$@" &
    wait "$!" || status=$?
    echo "--- :stopwatch: $1 ended at $(date -u +%T) after $((SECONDS - started)) s with status ${status}"
    return "${status}"
}

# Runs beside nextest, after the test build, with its output captured by the
# caller. Two lanes run at the same time: the doctests followed by the M1 gate,
# and the feature-isolation checks. The M1 gate waits because it builds in the
# test build's directory: started at once, its build and the doctests' would
# hold that directory's lock for seconds while nextest's own build waits for
# it. The feature-isolation checks build in their own. Each lane takes about as
# long as nextest, so neither outlasts it. Once both lanes have ended, each
# check's log is printed whole, doctests first. Every check runs even when
# another fails; the first failure in that order is returned.
run_side_checks() {
    local target_dir="$1"
    local isolation_job
    local doctest_status=0
    local m1_status=0
    local isolation_status=0
    run_timed_side_check run_feature_isolation_checks \
        >"${target_dir}/side-feature-isolation.log" 2>&1 &
    isolation_job="$!"
    run_timed_side_check run_workspace_doctests "${target_dir}" \
        >"${target_dir}/side-doctests.log" 2>&1 &
    wait "$!" || doctest_status=$?
    run_timed_side_check run_m1_sdk_check "${target_dir}" \
        >"${target_dir}/side-m1.log" 2>&1 &
    wait "$!" || m1_status=$?
    wait "${isolation_job}" || isolation_status=$?
    cat "${target_dir}/side-doctests.log" "${target_dir}/side-m1.log" \
        "${target_dir}/side-feature-isolation.log"
    local status
    for status in "${doctest_status}" "${m1_status}" "${isolation_status}"; do
        if [ "${status}" -ne 0 ]; then
            return "${status}"
        fi
    done
    return 0
}
