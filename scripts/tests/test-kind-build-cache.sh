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
#   5. cache-from prints each of an image's stages as one word per option and
#      never an export, even with writes enabled; an image without stages
#      prints nothing, and an unknown image is an error.
#   6. publish builds each cached stage by itself into the cache only, with
#      that stage's scope, and then the seed builder's stages.
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

# Case 5: cache-from.
cache_from() { # image env...
    local image=$1
    shift
    env -i PATH="$PATH" HOME="$work" "$@" bash "$script" cache-from "$image" 2>"$work/err"
}
expected="--cache-from=type=registry,ref=$repository:kind-api-smoke-build
--cache-from=type=registry,ref=$repository:kind-api-runtime-deps"
result="$(cache_from space-api SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository")" \
    || fail "cache-from space-api failed" "$(cat "$work/err")"
[[ "$result" == "$expected" ]] || fail "space-api must import both of its stages" "$result"
result="$(cache_from space-worker SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" \
    SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}")" \
    || fail "cache-from must not refuse a build with writes enabled" "$(cat "$work/err")"
[[ "$result" == "--cache-from=type=registry,ref=$repository:kind-worker-runtime-deps" ]] \
    || fail "cache-from must import and never export" "$result"
result="$(cache_from space-api)" || fail "cache-from without a repository failed" "$(cat "$work/err")"
[[ -z "$result" ]] || fail "cache-from without a repository must print nothing" "$result"
result="$(cache_from space-kind-key-broker SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository")" \
    || fail "cache-from an image without stages failed" "$(cat "$work/err")"
[[ -z "$result" ]] || fail "an image without cached stages must import nothing" "$result"
cache_from space-unknown SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" >/dev/null \
    && fail "an image the table does not declare must be an error"

# Case 6: publish, against a docker stub that records each build's arguments.
stubs="$work/stubs"
mkdir -p "$stubs"
cat >"$stubs/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_LOG"
EOF
chmod +x "$stubs/docker"
log="$work/docker.log"
env -i PATH="$stubs:$PATH" HOME="$work" DOCKER_LOG="$log" \
    SPACE_KIND_BUILD_CACHE_REPOSITORY="$repository" SPACE_KIND_BUILD_CACHE_WRITE=1 "${main_push[@]}" \
    bash "$script" publish >/dev/null 2>"$work/err" || fail "publish failed" "$(cat "$work/err")"
export_of() { # scope
    printf -- '--cache-to type=registry,ref=%s:%s,mode=max,image-manifest=true,oci-mediatypes=true,ignore-error=true' \
        "$repository" "$1"
}
for build in \
    "docker/Dockerfile.api smoke-build kind-api-smoke-build" \
    "docker/Dockerfile.api runtime-deps kind-api-runtime-deps" \
    "docker/Dockerfile.data-plane runtime-deps kind-data-plane-runtime-deps" \
    "docker/Dockerfile.cluster-controller runtime-deps kind-cluster-controller-runtime-deps" \
    "docker/Dockerfile.worker runtime-deps kind-worker-runtime-deps" \
    "docker/Dockerfile.seed-publish runtime-deps kind-seed-publish-runtime-deps" \
    "docker/Dockerfile.ui deps kind-ui-deps"; do
    read -r dockerfile stage scope <<<"$build"
    line="build -f $dockerfile --target $stage --output type=cacheonly --cache-from type=registry,ref=$repository:$scope $(export_of "$scope") ."
    grep -qxF -- "$line" "$log" || fail "publish must export $stage of $dockerfile to $scope" "$(cat "$log")"
done
grep -q -- "--target cloud-image --output type=cacheonly" "$log" \
    || fail "publish must export the seed builder's cloud-image stage" "$(cat "$log")"
grep -qF -- "$(export_of kind-seed-builder)" "$log" \
    || fail "the seed builder's stages must be exported to its own scope" "$(cat "$log")"
grep -q "Dockerfile.kind-key-broker" "$log" && fail "an image without cached stages must not be built"
[[ "$(wc -l <"$log")" -eq 8 ]] || fail "publish must run exactly one build per cached stage" "$(cat "$log")"

exit "$status"
