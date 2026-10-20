#!/usr/bin/env bash
# infra/space/kind/scripts/build-rust-artifacts.sh skips a run whose source sync
# wrote nothing when a record of the last assembly still vouches for the synced
# copy. That record must never outlive a sync that touched the copy: the sync
# rewrites files one at a time, so one that rewrote an edited file and then died
# on a later input would otherwise leave a record that a retry — finding nothing
# left to write — honours, reusing an image built from the old sources. A normal
# bring-up never exercises that path, so these cases pin it against the real
# script on a small fake checkout, with docker and the platform probes stubbed:
#
#   1. Unchanged sources and a valid record: the run reuses the image and runs no
#      docker build or container.
#   2. A sync that rewrites an edited file and then fails, retried after the
#      failing input is restored: the retry builds; it does not reuse.
#   3. A record naming a different image than the one tagged: the run builds.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script_src="$repo_root/infra/space/kind/scripts/build-rust-artifacts.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
status=0
fail() {
    echo "FAIL: $1" >&2
    [[ -z "${2:-}" ]] || printf '%s\n' "$2" | sed 's/^/    /' >&2
    status=1
}

# The fake checkout: the real script, its cache, shared-input and Rust build libraries and sync helper, and a build stage whose
# COPY list ends with an input the cases can take away.
repo="$work/repo"
mkdir -p "$repo/infra/space/kind/scripts" "$repo/infra/space/kind/lib" "$repo/docker" "$repo/crates/a/src"
cp "$script_src" "$repo/infra/space/kind/scripts/"
cp "$repo_root/infra/space/kind/lib/build-cache.sh" "$repo_root/infra/space/kind/lib/shared-build-inputs.sh" \
    "$repo/infra/space/kind/lib/"
cp "$repo_root/infra/space/kind/lib/rust-build.sh" "$repo/infra/space/kind/lib/"
cp "$repo_root/infra/space/kind/scripts/sync-rust-sources.py" "$repo/infra/space/kind/scripts/"
cat >"$repo/docker/Dockerfile.rust-build-kind" <<'EOF'
FROM chef AS cook
FROM cook AS build
COPY Cargo.toml ./
COPY crates ./crates
COPY late-input late-input
RUN cargo build --locked --release \
    -F feat \
    --bin bin1

RUN bash scripts/build-runtime-supervisor.sh a b \
    -F supfeat

FROM scratch AS build-artifacts-kind
COPY --from=build /workspace/target/release/bin1 /artifacts/bin/bin1
EOF
for f in Dockerfile.rust-build-common Dockerfile.rust-artifacts-host run-cargo-with-sccache.sh; do
    echo "# $f" >"$repo/docker/$f"
done
printf '[workspace]\n' >"$repo/Cargo.toml"
printf 'fn a() {}\n' >"$repo/crates/a/src/lib.rs"
printf 'late\n' >"$repo/late-input"

# Stubs: a Linux/x86_64 host, a flock that always grants (the script's own lock
# is not under test), and a docker that answers `image inspect` with
# $TEST_IMAGE_ID and records every other call.
stubs="$work/stubs"
mkdir -p "$stubs"
# shellcheck disable=SC2016 # the $1 belongs to the stub
printf '#!/usr/bin/env bash\ncase "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) echo Linux ;; esac\n' >"$stubs/uname"
printf '#!/usr/bin/env bash\nexit 0\n' >"$stubs/flock"
cat >"$stubs/docker" <<'EOF'
#!/usr/bin/env bash
if [[ "$1 $2" == "image inspect" ]]; then
    echo "${TEST_IMAGE_ID:-sha256:feedfacefeedfacefeedfacefeedface}"
    exit 0
fi
echo "$*" >>"$TEST_DOCKER_LOG"
exit 97
EOF
chmod +x "$stubs/"*

state="$work/state"
mkdir -p "$state/rust-artifacts"
run() { # log -> runs the script; stdout+stderr to $1
    (cd "$repo" && PATH="$stubs:$PATH" SPACE_KIND_RUST_ARTIFACTS=incremental SPACE_TILT_STATE_DIR="$state" \
        TEST_DOCKER_LOG="$work/docker.log" bash infra/space/kind/scripts/build-rust-artifacts.sh) >"$1" 2>&1
}
inputs=(Cargo.toml crates late-input)

# The record a completed assembly of this checkout would have written, computed
# by the script's own variable block and key function.
key="$(cd "$repo" && bash -c '
    RUST_BUILD=test-hooks
    eval "$(sed -n "/^ARTIFACT_IMAGE=/,/^RUNTIME_LIBC_IMAGE=/p" "$1")"
    eval "$(sed -n "/^assembly_key()/,/^}/p" "$1")"
    assembly_key' _ infra/space/kind/scripts/build-rust-artifacts.sh)"
seed() {
    python3 "$repo/infra/space/kind/scripts/sync-rust-sources.py" "$repo" "$state/rust-src" "${inputs[@]}" >/dev/null
    printf '%s sha256:feedfacefeedfacefeedfacefeedface\n' "$key" >"$state/rust-artifacts/.assembled"
    rm -f "$work/docker.log"
}

# Case 1.
seed
if run "$work/one"; then
    grep -q "sources unchanged since the last assembly" "$work/one" \
        || fail "unchanged sources with a valid record must reuse the image" "$(cat "$work/one")"
    [[ ! -s "$work/docker.log" ]] || fail "a reuse must not build or run anything" "$(cat "$work/docker.log")"
else
    fail "a reuse run must succeed" "$(cat "$work/one")"
fi

# Case 2.
seed
printf 'fn a() { 1; }\n' >"$repo/crates/a/src/lib.rs"
mv "$repo/late-input" "$work/late-input.aside"
if run "$work/two-a"; then
    fail "a sync with a missing input must fail the run" "$(cat "$work/two-a")"
fi
grep -q "build input missing from the checkout: late-input" "$work/two-a" \
    || fail "the failing sync must name the missing input" "$(cat "$work/two-a")"
cmp -s "$repo/crates/a/src/lib.rs" "$state/rust-src/crates/a/src/lib.rs" \
    || fail "the case needs the failed sync to have rewritten the edited file first"
mv "$work/late-input.aside" "$repo/late-input"
run "$work/two-b" || true
grep -q "source sync: 0 written, 0 removed" "$work/two-b" \
    || fail "the retry's sync must find nothing left to write (the case's premise)" "$(cat "$work/two-b")"
grep -q "sources unchanged since the last assembly" "$work/two-b" \
    && fail "a retry after a failed sync reused an image built from the old sources" "$(cat "$work/two-b")"
[[ -s "$work/docker.log" ]] || fail "the retry must go on to build" "$(cat "$work/two-b")"
printf 'fn a() {}\n' >"$repo/crates/a/src/lib.rs"

# Case 3.
seed
(cd "$repo" && PATH="$stubs:$PATH" SPACE_KIND_RUST_ARTIFACTS=incremental SPACE_TILT_STATE_DIR="$state" \
    TEST_DOCKER_LOG="$work/docker.log" TEST_IMAGE_ID=sha256:0123456789abcdef0123456789abcdef \
    bash infra/space/kind/scripts/build-rust-artifacts.sh) >"$work/three" 2>&1 || true
grep -q "sources unchanged since the last assembly" "$work/three" \
    && fail "a record naming another image must not be honoured" "$(cat "$work/three")"
[[ -s "$work/docker.log" ]] || fail "a stale record must lead to a build" "$(cat "$work/three")"

if [ "$status" -eq 0 ]; then
    echo "ok: the artifact build reuses an image only while its record vouches for the synced sources," \
        "and a sync that failed partway never leaves one behind"
fi
exit "$status"
