#!/usr/bin/env bash
# Cargo's RUSTC_WRAPPER for CI's Rust checks: compiles the workspace's own
# crates with rustc's parallel front end, and hands every invocation to the
# wrapper it replaces (sccache), if any.
#
# The front end of one large crate runs on one thread, so most of the
# runner's cores sit idle while pplx-space-api and pplx-space-sbx-mgr compile.
# `-Zthreads` spreads it. It is unstable, so the stable compiler accepts it
# only under RUSTC_BOOTSTRAP. Set to a crate name, RUSTC_BOOTSTRAP unlocks that
# crate alone, and `-Zallow-features=` keeps it from using any unstable
# language feature. A dependency compiles exactly as it does without this
# wrapper: set to 1, RUSTC_BOOTSTRAP would also reach the build scripts that
# probe for nightly features, and anyhow, thiserror and proc-macro2 would
# compile different code.
#
# Cargo runs: <this> <rustc or clippy-driver> [rustc] ARGS...
# SPACE_RUST_FRONTEND_THREADS: the thread count; 0 or 1 turns this off.
# SPACE_RUSTC_INNER_WRAPPER: the wrapper this one runs in front of.
# SPACE_RUSTC_WORKSPACE_ROOT: crates whose manifest is under it get the flags.
set -euo pipefail

inner=()
if [ -n "${SPACE_RUSTC_INNER_WRAPPER:-}" ]; then
    inner=("${SPACE_RUSTC_INNER_WRAPPER}")
fi
threads="${SPACE_RUST_FRONTEND_THREADS:-0}"
root="${SPACE_RUSTC_WORKSPACE_ROOT:-}"
manifest="${CARGO_MANIFEST_DIR:-}"

crate=""
previous=""
for argument in "$@"; do
    if [ "${previous}" = --crate-name ]; then
        crate="${argument}"
        break
    fi
    previous="${argument}"
done

if [[ -z "${crate}" || -z "${root}" || ! "${threads}" =~ ^[0-9]+$ || "${threads}" -le 1 ]] ||
    [[ "${manifest}" != "${root}" && "${manifest}" != "${root}/"* ]]; then
    exec ${inner[@]+"${inner[@]}"} "$@"
fi

RUSTC_BOOTSTRAP="${crate}" exec ${inner[@]+"${inner[@]}"} "$@" \
    "-Zthreads=${threads}" -Zallow-features=
