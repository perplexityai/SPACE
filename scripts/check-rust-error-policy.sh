#!/usr/bin/env bash
# Keep Rust error APIs typed. This deliberately uses find/grep rather than rg:
# the minimal formatting CI image does not guarantee ripgrep is installed.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

status=0

production_rust_files() {
    find "$@" -name '*.rs' \
        ! -name build.rs \
        ! -name '*tests.rs' \
        ! -path '*/tests/*' \
        ! -path '*/benches/*' \
        ! -path '*/examples/*' \
        -print0
}

report_matches() {
    local heading="$1"
    local pattern="$2"
    shift 2
    local matches
    matches="$(production_rust_files "$@" | xargs -0 grep -nE "${pattern}" || true)"
    if [ -n "${matches}" ]; then
        echo "Error: ${heading}" >&2
        echo "${matches}" >&2
        status=1
    fi
}

report_matches \
    'production library code must return typed errors, not anyhow' \
    'anyhow::|use[[:space:]]+anyhow' \
    crates/libs

report_matches \
    'derive thiserror::Error instead of implementing std::error::Error manually' \
    'impl[[:space:]]+(std::error::Error|Error)[[:space:]]+for[[:space:]]' \
    crates/apps crates/libs crates/tools

derive_without_debug="$(
    production_rust_files crates/apps crates/libs crates/tools \
        | xargs -0 grep -nE '#\[derive\([^]]*(thiserror::Error|Error)' \
        | grep -v 'Debug' \
        || true
)"
if [ -n "${derive_without_debug}" ]; then
    echo 'Error: every thiserror-derived error must also derive Debug' >&2
    echo "${derive_without_debug}" >&2
    status=1
fi

exit "${status}"
