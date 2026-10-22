#!/usr/bin/env bash
# The Kind registry layer cache (infra/space/kind/lib/build-cache.sh) and the
# runtime images' use of it (infra/space/kind/scripts/runtime-image-cache.sh).
# A bring-up shows only that its builds succeeded, not which layers they could
# have read or written, so these cases run the real scripts with docker
# stubbed on PATH and check the options they pass:
#
#   1. Without a repository nothing is imported, and a write is refused.
#   2. A repository adds one registry import per scope; PRs never export.
#   3. Only perplexityai/SPACE main push CI may export, and its export never
#      fails a build (ignore-error).
#   4. The repository must be a GHCR package or a loopback registry.
#   5. An export can be made to fail on its errors, for a caller that exists
#      only to export.
#   6. `build` adds the image's Dockerfile, its mirrored bases and an import
#      per stage, never an export, even with writes enabled; an image without
#      stages imports nothing, and an unknown image is an error.
#   7. A build that fails reading a cached layer or a mirrored base, or whose
#      error names the cache repository or a mirrored image, runs once more
#      without the imports and the mirrored bases, keeping its other build
#      contexts; any other failure stands, after one attempt.
#   8. publish builds each cached stage by itself into the cache only, with
#      that stage's scope, then the seed builder's stages, with export errors
#      failing; a failed build fails publish without stopping the others. Its
#      retry drops a broken import but keeps the mirrored bases, and a
#      Dockerfile whose listed base the mirror could not provide is not
#      exported at all.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lib="$repo_root/infra/space/kind/lib/build-cache.sh"
script="$repo_root/infra/space/kind/scripts/runtime-image-cache.sh"
repository=ghcr.io/example-owner/example-cache

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
status=0

fail() {
    echo "FAIL: $1" >&2
    [[ -z "${2:-}" ]] || printf '%s\n' "$2" | sed 's/^/    /' >&2
    status=1
}

# Run the library function with only the given environment; print its exit
# status and then CACHE_FLAGS, one element per line.
flags() { # scope env...
    local scope=$1
    shift
    env -i PATH="$PATH" "$@" bash -c '
        source "$1"
        rc=0
        space_kind_cache_flags "$2" 2>/dev/null || rc=$?
        echo "$rc"
        printf "%s\n" "${CACHE_FLAGS[@]}"' _ "$lib" "$scope"
}

main_push=(GITHUB_ACTIONS=true GITHUB_EVENT_NAME=push GITHUB_REF=refs/heads/main
    GITHUB_REPOSITORY=perplexityai/SPACE)

# Case 1: no repository.
result="$(flags kind-x)"
[[ "$result" == 0 ]] || fail "no repository must give no options" "$result"
result="$(flags kind-x SPACE_KIND_BUILD_CACHE_WRITE=1)"
[[ "$(head -1 <<<"$result")" != 0 ]] || fail "a write without a repository must be refused" "$result"

# Case 2: a repository imports its scope; a PR does not export.
expected="0
--cache-from
type=registry,ref=$repository:kind-x"
result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository")"
[[ "$result" == "$expected" ]] || fail "a repository must import exactly its scope" "$result"
result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 \
    GITHUB_ACTIONS=true GITHUB_EVENT_NAME=pull_request GITHUB_REF=refs/pull/1/merge \
    GITHUB_REPOSITORY=perplexityai/SPACE)"
[[ "$(head -1 <<<"$result")" != 0 ]] || fail "a pull request must not be allowed to export" "$result"
grep -q -- --cache-to <<<"$result" && fail "a refused write must not leave an export option" "$result"

# Case 3: main push CI exports, and an export failure does not fail the build.
result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}")"
[[ "$(head -1 <<<"$result")" == 0 ]] || fail "main push CI must be allowed to export" "$result"
grep -qx -- "type=registry,ref=$repository:kind-x,mode=max,image-manifest=true,oci-mediatypes=true,ignore-error=true" <<<"$result" \
    || fail "main's export must write the scope and ignore export errors" "$result"
result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 \
    "${main_push[@]}" GITHUB_REPOSITORY=example-owner/SPACE)"
[[ "$(head -1 <<<"$result")" != 0 ]] || fail "a fork's main push must not export" "$result"
result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=yes)"
[[ "$(head -1 <<<"$result")" != 0 ]] || fail "a write setting other than 0 or 1 must be refused" "$result"

# Case 4: accepted and refused repositories.
for accepted in "$repository" localhost:5000/space-kind-build-cache 127.0.0.1:5000/cache; do
    result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$accepted")"
    [[ "$(head -1 <<<"$result")" == 0 ]] || fail "$accepted must be accepted" "$result"
done
for refused in docker.io/example/cache ghcr.io/Example/cache ghcr.io/example/cache:tag \
    registry.example.com:5000/cache ghcr.io/example; do
    result="$(flags kind-x SPACE_KIND_BUILD_CACHE_REPOSITORY="$refused")"
    [[ "$(head -1 <<<"$result")" != 0 ]] || fail "$refused must be refused" "$result"
done


# Case 5: --fail-on-export-error.
result="$(env -i PATH="$PATH" SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 \
    "${main_push[@]}" bash -c 'source "$1"; space_kind_cache_flags kind-x --fail-on-export-error; printf "%s\n" "${CACHE_TO_FLAGS[@]}"' _ "$lib")"
grep -q -- ",ignore-error=false$" <<<"$result" || fail "--fail-on-export-error must keep export errors" "$result"

# A docker stub that records each call's arguments. STUB_MODE=read-fail fails
# a build that imports a cache the way an unreadable cached layer does;
# STUB_MODE=fail fails every build the way a broken RUN step does.
stubs="$work/stubs"
mkdir -p "$stubs"
cat >"$stubs/docker" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_LOG"
if [[ "$1" == buildx && "${STUB_MODE:-}" == mirror-down ]]; then exit 1; fi
[[ "$1" == build ]] || exit 0
case "${STUB_MODE:-ok}" in
    read-fail)
        if [[ "$*" == *--cache-from* ]]; then
            echo "ERROR: failed to solve: failed to copy: httpReadSeeker: failed open: content not found" >&2
            exit 1
        fi ;;
    mirror-read-fail)
        if [[ "$*" == *=docker-image://* ]]; then
            echo "ERROR: failed to solve: failed to copy: httpReadSeeker: failed open: could not fetch content descriptor from remote: not found" >&2
            exit 1
        fi ;;
    mirror-named-fail)
        if [[ "$*" == *=docker-image://* ]]; then
            echo "ERROR: failed to solve: ghcr.io/example-owner/mirror@sha256:0: failed to resolve source metadata: unexpected status 500" >&2
            exit 1
        fi ;;
    fail)
        echo 'ERROR: failed to solve: process "/bin/sh -c false" did not complete successfully: exit code: 1' >&2
        exit 1 ;;
esac
STUB
chmod +x "$stubs/docker"
log="$work/docker.log"
mirrors="$work/mirrored-images.txt"
digest="sha256:$(printf '%064d' 1)"
echo "public.ecr.aws/docker/library/debian:bookworm-slim $digest ghcr.io/example-owner/mirror:debian" >"$mirrors"
run_build() { # env... -- image args...
    local -a vars=()
    while [[ "$1" != -- ]]; do vars+=("$1"); shift; done
    shift
    : >"$log"
    env -i PATH="$stubs:$PATH" HOME="$work" DOCKER_LOG="$log" SPACE_MIRRORED_IMAGES="$mirrors" \
        "${vars[@]}" bash "$script" build "$@" >/dev/null 2>"$work/err"
}
builds() { grep '^build ' "$log" || true; }

# Case 6: build.
mirror_context="--build-context=public.ecr.aws/docker/library/debian:bookworm-slim=docker-image://ghcr.io/example-owner/mirror@$digest"
run_build SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_CI_IMAGE_MIRROR=ghcr -- space-api -t img . \
    || fail "build space-api failed" "$(cat "$work/err")"
expected="build --cache-from type=registry,ref=$repository:kind-api-smoke-build-e1 --cache-from type=registry,ref=$repository:kind-api-runtime-deps-e1 $mirror_context -f docker/Dockerfile.api -t img ."
[[ "$(builds)" == "$expected" ]] || fail "space-api must import both stages and take its mirrored bases" "$(builds)"
run_build SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}" -- space-worker . \
    || fail "build must not refuse a build with writes enabled" "$(cat "$work/err")"
[[ "$(builds)" == "build --cache-from type=registry,ref=$repository:kind-worker-runtime-deps-e1 -f docker/Dockerfile.worker ." ]] \
    || fail "build must import and never export" "$(builds)"
run_build -- space-api . || fail "build without a repository failed" "$(cat "$work/err")"
[[ "$(builds)" == "build -f docker/Dockerfile.api ." ]] || fail "build without a repository must import nothing" "$(builds)"
run_build SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_CI_IMAGE_MIRROR=ghcr -- space-kind-key-broker . \
    || fail "build of an image without stages failed" "$(cat "$work/err")"
[[ "$(builds)" == "build $mirror_context -f docker/Dockerfile.kind-key-broker ." ]] \
    || fail "an image without cached stages must import nothing and still take its mirrored bases" "$(builds)"
run_build SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" -- space-unknown . \
    && fail "an image the table does not declare must be an error"

# Case 7: a layer the cache cannot provide, and a genuine failure.
run_build SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" STUB_MODE=read-fail -- space-data-plane . \
    || fail "a build that could not read a cached layer must succeed without the cache" "$(cat "$work/err")"
expected="build --cache-from type=registry,ref=$repository:kind-data-plane-runtime-deps-e1 -f docker/Dockerfile.data-plane .
build -f docker/Dockerfile.data-plane ."
[[ "$(builds)" == "$expected" ]] || fail "the retry must drop exactly the imports" "$(builds)"
grep -q "building again from the sources" "$work/err" || fail "the retry must say why" "$(cat "$work/err")"
run_build SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" STUB_MODE=fail -- space-data-plane . \
    && fail "a genuine build failure must fail the build"
[[ "$(builds | wc -l)" -eq 1 ]] || fail "a genuine build failure must not be retried" "$(builds)"

for mode in mirror-read-fail mirror-named-fail; do
    run_build SPACE_CI_IMAGE_MIRROR=ghcr STUB_MODE=$mode -- space-seed-publish --build-context seed=/tmp/seed . \
        || fail "$mode: a build whose mirrored base fails must succeed from the sources" "$(cat "$work/err")"
    expected="build $mirror_context -f docker/Dockerfile.seed-publish --build-context seed=/tmp/seed .
build -f docker/Dockerfile.seed-publish --build-context seed=/tmp/seed ."
    [[ "$(builds)" == "$expected" ]] \
        || fail "$mode: the retry must drop the mirrored bases and keep the local context" "$(builds)"
done

# Case 8: publish.
publish() { # env...
    : >"$log"
    env -i PATH="$stubs:$PATH" HOME="$work" DOCKER_LOG="$log" \
        SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}" "$@" \
        bash "$script" publish >/dev/null 2>"$work/err"
}
publish || fail "publish failed" "$(cat "$work/err")"
export_of() { # scope
    printf -- '--cache-to type=registry,ref=%s:%s,mode=max,image-manifest=true,oci-mediatypes=true,ignore-error=false' \
        "$repository" "$1"
}
for build in \
    "docker/Dockerfile.api smoke-build kind-api-smoke-build-e1" \
    "docker/Dockerfile.api runtime-deps kind-api-runtime-deps-e1" \
    "docker/Dockerfile.data-plane runtime-deps kind-data-plane-runtime-deps-e1" \
    "docker/Dockerfile.cluster-controller runtime-deps kind-cluster-controller-runtime-deps-e1" \
    "docker/Dockerfile.worker runtime-deps kind-worker-runtime-deps-e1" \
    "docker/Dockerfile.seed-publish runtime-deps kind-seed-publish-runtime-deps-e1" \
    "docker/Dockerfile.ui deps kind-ui-deps-e1"; do
    read -r dockerfile stage scope <<<"$build"
    line="build --cache-from type=registry,ref=$repository:$scope -f $dockerfile --target $stage --output type=cacheonly $(export_of "$scope") ."
    grep -qxF -- "$line" "$log" || fail "publish must export $stage of $dockerfile to $scope" "$(builds)"
done
grep -- "--target cloud-image --output type=cacheonly" "$log" | grep -qF -- "$(export_of kind-seed-builder-e1)" \
    || fail "publish must export the seed builder's cloud-image stage to its own scope" "$(builds)"
grep -q "Dockerfile.kind-key-broker" "$log" && fail "an image without cached stages must not be built"
[[ "$(builds | wc -l)" -eq 8 ]] || fail "publish must run exactly one build per cached stage" "$(builds)"
publish STUB_MODE=fail && fail "publish must fail when its builds or exports fail"
[[ "$(builds | wc -l)" -eq 8 ]] || fail "a failed build must not stop the others" "$(builds)"
grep -q "keeps main's previous layers" "$work/err" || fail "a failed export must say what it leaves" "$(cat "$work/err")"
: >"$log"
env -i PATH="$stubs:$PATH" HOME="$work" DOCKER_LOG="$log" SPACE_MIRRORED_IMAGES="$mirrors" SPACE_CI_IMAGE_MIRROR=ghcr \
    SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}" STUB_MODE=read-fail \
    bash "$script" publish >/dev/null 2>"$work/err" || fail "publish must survive a broken import" "$(cat "$work/err")"
retry="$(builds | grep -- "-f docker/Dockerfile.data-plane " | sed -n 2p)"
[[ "$retry" == "build -f docker/Dockerfile.data-plane --target runtime-deps --output type=cacheonly $mirror_context $(export_of kind-data-plane-runtime-deps-e1) ." ]] \
    || fail "publish's retry must drop the import and keep the mirrored base" "$(builds)"

# A listed base whose mirror copy is unreachable: its Dockerfiles' stages keep
# main's tags; the others are still exported.
: >"$log"
env -i PATH="$stubs:$PATH" HOME="$work" DOCKER_LOG="$log" SPACE_MIRRORED_IMAGES="$mirrors" SPACE_CI_IMAGE_MIRROR=ghcr \
    SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}" STUB_MODE=mirror-down \
    bash "$script" publish >/dev/null 2>"$work/err" && fail "publish must fail when a mirrored base is unavailable"
expected="docker/Dockerfile.worker
docker/Dockerfile.ui
/seed"
[[ "$(builds | grep -oE 'docker/Dockerfile\.[a-z0-9-]+|/seed$' | uniq)" == "$expected" ]] \
    || fail "only Dockerfiles whose mirrored bases were all available may be exported" "$(builds)"
grep -q "a mirrored base of docker/Dockerfile.api was unavailable" "$work/err" \
    || fail "a skipped export must say why" "$(cat "$work/err")"

exit "$status"
