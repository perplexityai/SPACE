#!/usr/bin/env bash
# build-rust-artifacts.sh decides where the kind stack's Rust binaries come
# from — an incremental cargo build inside the builder image with a host
# target dir, or the in-Docker BuildKit union build — and both paths must
# produce the same /artifacts layout. A green bring-up proves whichever path it
# took; it cannot prove the other decisions, which these cases pin against the
# real script with only the platform probes stubbed on PATH:
#
#   1. `docker` never consults the platform probes.
#   2. `incremental` fails closed when Docker is absent — never a silent
#      fallback to a five-minute BuildKit compile — naming the requirement.
#   3. `auto` falls back to docker off Linux/x86_64, saying why.
#   4. `auto` picks incremental when Linux/x86_64, docker, flock and python3 are present.
#   5. The bin list the build stage compiles and the /artifacts layout the
#      scratch stage copies describe the same set: every compiled bin is
#      copied, and nothing is copied that is not compiled (the supervisor and
#      its compatibility file are the one indirect pair). An extra `--bin`
#      with no COPY would compile silently for nothing; a COPY with no `--bin`
#      would fail only an incremental build, and only at staging time.
#   6. The glibc guard: a binary requiring a newer GLIBC symbol version than
#      the runtime image provides fails the assembly with the binary named
#      (the 2026-09-22 bench lost 30 min to exactly this surfacing as an
#      api-server healthz timeout); binaries at or below it pass, static ones
#      are ignored. objdump and ldd are stubbed, so the guard's arithmetic is
#      what is tested, not the tools.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/infra/space/kind/scripts/build-rust-artifacts.sh"
dockerfile="$repo_root/docker/Dockerfile.rust-build-kind"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
status=0

fail() {
    echo "FAIL: $1" >&2
    [[ -z "${2:-}" ]] || printf '%s\n' "$2" | sed 's/^/    /' >&2
    status=1
}

# A PATH whose platform probes are the stubs in $1 and nothing else. The
# system dirs are NOT on it — on a CI agent /usr/bin holds a real docker, and
# the first version of this file let case 2 find it and pass for the wrong
# reason. The text tools the script and the guard need are symlinked into one
# allowlisted dir instead.
tools="$work/tools"
mkdir -p "$tools"
for tool in bash sh dirname basename sed grep sort head tail awk cat tr env mktemp; do
    real="$(command -v "$tool" 2>/dev/null || true)"
    [[ -n "$real" ]] || { echo "FAIL: test needs $tool on PATH" >&2; exit 1; }
    ln -s "$real" "$tools/$tool"
done
stub_path() {
    printf '%s:%s' "$1" "$tools"
}

make_stub() { # dir name body
    printf '#!/usr/bin/env bash\n%s\n' "$3" >"$1/$2"
    chmod +x "$1/$2"
}

linux_uname='case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) /usr/bin/uname "$@" ;; esac'

mode_with() { # stub-dir request -> prints "rc\nmode\nstderr"
    local stubs=$1 request=$2 out rc=0
    out="$(SPACE_KIND_RUST_ARTIFACTS="$request" PATH="$(stub_path "$stubs")" \
        bash "$script" --mode 2>"$work/err")" || rc=$?
    printf '%s\n%s\n%s' "$rc" "$out" "$(cat "$work/err")"
}

# Case 1: docker never touches the probes.
stubs="$work/case1"; mkdir -p "$stubs"
make_stub "$stubs" uname 'echo "uname must not run in docker mode" >&2; exit 97'
make_stub "$stubs" docker 'echo "docker must not run in docker mode" >&2; exit 97'
result="$(mode_with "$stubs" docker)"
[[ "$(sed -n 1p <<<"$result")" == 0 && "$(sed -n 2p <<<"$result")" == docker ]] \
    || fail "docker mode must be chosen without consulting the platform" "$result"
grep -q "must not run" <<<"$result" && fail "docker mode ran a platform probe" "$result"

# Case 2: incremental without docker fails closed and names it.
stubs="$work/case2"; mkdir -p "$stubs"
make_stub "$stubs" uname "$linux_uname"
make_stub "$stubs" flock 'exit 0'
result="$(mode_with "$stubs" incremental)"
[[ "$(sed -n 1p <<<"$result")" != 0 ]] \
    || fail "incremental mode without docker must fail closed, not fall back" "$result"
grep -q "docker is not on PATH" <<<"$result" \
    || fail "the incremental-mode refusal must name the missing requirement" "$result"
[[ "$(sed -n 2p <<<"$result")" == docker ]] \
    && fail "incremental mode must never print docker as the chosen mode" "$result"

# Case 3: auto off Linux/x86_64 falls back to docker and says so.
stubs="$work/case3"; mkdir -p "$stubs"
make_stub "$stubs" uname 'case "$1" in -s) echo Darwin ;; -m) echo arm64 ;; *) /usr/bin/uname "$@" ;; esac'
make_stub "$stubs" docker 'exit 0'
make_stub "$stubs" flock 'exit 0'
result="$(mode_with "$stubs" auto)"
[[ "$(sed -n 1p <<<"$result")" == 0 && "$(sed -n 2p <<<"$result")" == docker ]] \
    || fail "auto off Linux/x86_64 must choose docker" "$result"
grep -q "need Linux/x86_64" <<<"$result" \
    || fail "auto's fallback must state the platform requirement" "$result"

# Case 4: auto with every requirement satisfied picks incremental.
stubs="$work/case4"; mkdir -p "$stubs"
make_stub "$stubs" uname "$linux_uname"
make_stub "$stubs" docker 'exit 0'
make_stub "$stubs" flock 'exit 0'
make_stub "$stubs" python3 'exit 0'
result="$(mode_with "$stubs" auto)"
[[ "$(sed -n 1p <<<"$result")" == 0 && "$(sed -n 2p <<<"$result")" == incremental ]] \
    || fail "auto on Linux/x86_64 with docker must choose incremental" "$result"

# Case 5: the compiled set and the copied set agree.
build_stage() {
    awk '/^FROM cook AS build$/ { s = 1; next } /^FROM / { s = 0 } s' "$dockerfile"
}
compiled="$( { build_stage | grep -oE -- '--bin [^ \\]+' | awk '{print $2}'; } | sort -u)"
copied="$(awk '/^FROM scratch AS build-artifacts-kind$/ { s = 1; next } s && /^COPY --from=build / {
             sub("/workspace/target/release/", "", $3); print $3 }' "$dockerfile" \
          | grep -vx space-runtime-supervisor-compatibility | sort -u)"
[[ -n "$compiled" && -n "$copied" ]] || fail "could not read the bin lists from $dockerfile"
diff_out="$(diff <(printf '%s\n' "$compiled") <(printf '%s\n' "$copied") || true)"
[[ -z "$diff_out" ]] \
    || fail "the build stage's --bin list and the scratch stage's COPY list differ (< compiled, > copied)" "$diff_out"
grep -qx space-runtime-supervisor-svc <<<"$copied" \
    || fail "the supervisor binary must be copied into the artifact layout"

# Case 6: the glibc guard. `docker run` is stubbed: an `--entrypoint sh`
# invocation is the runtime image's `ldd --version` (glibc 2.36); anything else
# is the builder running the objdump loop, answered from a table keyed by
# binary name. The guard's functions are sourced from the script itself.
stubs="$work/case6"; mkdir -p "$stubs"
cat >"$stubs/docker" <<'EOF'
#!/usr/bin/env bash
if [[ " $* " == *" --entrypoint sh "* ]]; then
    echo "ldd (Debian GLIBC 2.36-9+deb12u10) 2.36"
    exit 0
fi
# builder: the last arguments after the script's `_` are the binary paths
seen=0
for arg in "$@"; do
    if (( seen )); then
        case "$(basename "$arg")" in
            old-bin)    echo "old-bin 2.34" ;;
            edge-bin)   echo "edge-bin 2.36" ;;
            new-bin)    echo "new-bin 2.39" ;;
            static-bin) echo "static-bin none" ;;
            *)          echo "$(basename "$arg") none" ;;
        esac
    fi
    [[ "$arg" == "_" ]] && seen=1
done
EOF
chmod +x "$stubs/docker"
guard_env() { # extra assignments...
    PATH="$(stub_path "$stubs")" REPO_ROOT="$repo_root" CHEF_IMAGE=x TARGET_DIR="$work" CARGO_HOME_DIR="$work" SRC_DIR="$work" \
    RUNTIME_LIBC_IMAGE=runtime:test RELEASE_LTO=false "$@"
}
guard_body='
set -euo pipefail
log() { printf "[rust-artifacts] %s\n" "$*"; }
die() { printf "[rust-artifacts] ERROR: %s\n" "$*" >&2; exit 1; }
eval "$(sed -n "/^in_builder()/,/^build_incremental()/p" "$1" | sed "\$d")"
shift
assert_glibc_compatible "$@"'
if ! out="$(guard_env bash -c "$guard_body" _ "$script" /w/old-bin /w/edge-bin /w/static-bin 2>&1)"; then
    fail "binaries at or below the runtime glibc must pass the guard" "$out"
fi
grep -q "require ≤ GLIBC_2.36; runtime image runtime:test provides 2.36" <<<"$out" \
    || fail "the guard must report the highest requirement and the runtime version" "$out"
if out="$(guard_env bash -c "$guard_body" _ "$script" /w/old-bin /w/new-bin 2>&1)"; then
    fail "a binary needing GLIBC_2.39 must fail the guard against a 2.36 runtime" "$out"
fi
grep -q "new-bin needs GLIBC_2.39" <<<"$out" \
    || fail "the guard's refusal must name the offending binary and version" "$out"
grep -q "old-bin" <<<"$(grep needs <<<"$out" || true)" \
    && fail "the guard must not blame a compatible binary" "$out"

if [ "$status" -eq 0 ]; then
    echo "ok: the artifact build chooses incremental only on Linux/x86_64 with docker, fails closed when" \
        "asked for it without, falls back from auto with a reason, compiles exactly the set it copies," \
        "and refuses binaries the runtime image's glibc cannot load"
fi
exit "$status"
