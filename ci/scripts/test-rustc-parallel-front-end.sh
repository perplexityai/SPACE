#!/usr/bin/env bash
# Regression test for ci/scripts/rustc-parallel-front-end.sh: a workspace
# crate gets the parallel front end, unlocked for that crate alone; a
# dependency, a crate outside the workspace root, a compiler query and a
# disabled thread count pass through unchanged; and the wrapper it replaces
# still runs.
set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/rustc-parallel-front-end.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

# Stands in for sccache and rustc alike: prints what it was given.
cat >"${scratch}/record" <<'EOF'
#!/usr/bin/env bash
echo "RUSTC_BOOTSTRAP=${RUSTC_BOOTSTRAP-unset} $(basename "$0") $*"
EOF
chmod +x "${scratch}/record"
ln -s record "${scratch}/rustc"
ln -s record "${scratch}/clippy-driver"
ln -s record "${scratch}/sccache"

FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# check NAME EXPECTED MANIFEST THREADS INNER ARGS...
check() {
    local name="$1" expected="$2" manifest="$3" threads="$4" inner="$5"
    shift 5
    local actual
    actual="$(env -u RUSTC_BOOTSTRAP CARGO_MANIFEST_DIR="${manifest}" \
        SPACE_RUST_FRONTEND_THREADS="${threads}" SPACE_RUSTC_INNER_WRAPPER="${inner}" \
        SPACE_RUSTC_WORKSPACE_ROOT=/work/space bash "${SCRIPT}" "$@")" ||
        fail "${name}: the wrapper failed"
    [ "${actual}" = "${expected}" ] || fail "${name}: ran '${actual}', expected '${expected}'"
}

rustc="${scratch}/rustc"
sccache="${scratch}/sccache"
check "workspace crate" \
    "RUSTC_BOOTSTRAP=pplx_space_api sccache ${rustc} --crate-name pplx_space_api src/lib.rs -Zthreads=8 -Zallow-features=" \
    /work/space/crates/api 8 "${sccache}" "${rustc}" --crate-name pplx_space_api src/lib.rs
check "workspace crate under clippy" \
    "RUSTC_BOOTSTRAP=pplx_space_api sccache ${scratch}/clippy-driver ${rustc} --crate-name pplx_space_api -Zthreads=8 -Zallow-features=" \
    /work/space/crates/api 8 "${sccache}" "${scratch}/clippy-driver" "${rustc}" --crate-name pplx_space_api
check "no inner wrapper" \
    "RUSTC_BOOTSTRAP=pplx_space_api rustc --crate-name pplx_space_api -Zthreads=4 -Zallow-features=" \
    /work/space/crates/api 4 "" "${rustc}" --crate-name pplx_space_api
check "dependency" \
    "RUSTC_BOOTSTRAP=unset sccache ${rustc} --crate-name anyhow src/lib.rs" \
    /cargo/registry/src/anyhow-1.0.0 8 "${sccache}" "${rustc}" --crate-name anyhow src/lib.rs
check "sibling of the workspace root" \
    "RUSTC_BOOTSTRAP=unset sccache ${rustc} --crate-name other" \
    /work/space-other 8 "${sccache}" "${rustc}" --crate-name other
check "compiler query" \
    "RUSTC_BOOTSTRAP=unset sccache ${rustc} -vV" \
    /work/space/crates/api 8 "${sccache}" "${rustc}" -vV
for threads in 0 1 "" eight; do
    check "thread count '${threads}'" \
        "RUSTC_BOOTSTRAP=unset sccache ${rustc} --crate-name pplx_space_api" \
        /work/space/crates/api "${threads}" "${sccache}" "${rustc}" --crate-name pplx_space_api
done

[ "${FAILURES}" -eq 0 ] || exit 1
echo "rustc-parallel-front-end tests passed"
