#!/usr/bin/env bash
# Keep Rust error APIs typed. This deliberately uses find/grep rather than rg:
# the minimal formatting CI image does not guarantee ripgrep is installed.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

status=0

# Library crates, and every crate root. The layer directories under crates/
# fill as crates move into them, so a root that doesn't exist yet is skipped.
library_roots=(crates/libs crates/l10_foundation crates/l20_domain crates/l30_storage crates/l40_api)
all_roots=("${library_roots[@]}" crates/apps crates/l50_app crates/test_support crates/tools)

production_rust_files() {
    local roots=()
    local root
    for root in "$@"; do
        if [ -d "${root}" ]; then
            roots+=("${root}")
        fi
    done
    if [ "${#roots[@]}" -eq 0 ]; then
        return 0
    fi
    find "${roots[@]}" -name '*.rs' \
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
    "${library_roots[@]}"

report_matches \
    'derive thiserror::Error instead of implementing std::error::Error manually' \
    'impl[[:space:]]+(std::error::Error|Error)[[:space:]]+for[[:space:]]' \
    "${all_roots[@]}"

derive_without_debug="$(
    production_rust_files "${all_roots[@]}" \
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
