# shellcheck shell=bash
# The Kind image builds' registry layer cache: one repository, one tag
# ("scope") per cached build, written only by trusted main CI.
#
# Every Kind build that keeps layers there takes its options from
# space_kind_cache_flags: the Rust artifact builder
# (infra/space/kind/scripts/build-rust-artifacts.sh) and the runtime images and
# seed builder (infra/space/kind/scripts/runtime-image-cache.sh).
#
# Env:
#   SPACE_KIND_BUILD_CACHE_REPOSITORY  optional cache repository: a lowercase
#                                      ghcr.io/owner/package without a tag, or a
#                                      registry on the loopback interface
#                                      (localhost:<port>/<name>) to try the cache
#                                      locally; unset leaves builds unchanged
#   SPACE_KIND_BUILD_CACHE_WRITE       1 exports layers (trusted main CI only);
#                                      default 0 imports without exporting
#
# Imports are optional performance hints: a missing or unreachable cache makes
# a cold build, which fetches from upstream as before.

# The generation of the runtime images' and the seed builder's tags. A layer
# main exported keeps being reused for as long as its inputs are unchanged,
# even one built while a package archive served a broken package. Bumping this
# number moves every reader and main's next export to new, empty tags, so that
# export rebuilds each layer from upstream. Readers miss until it has run.
SPACE_KIND_LAYER_CACHE_EPOCH=1

# The tag of one cached stage: kind-<name>-e<epoch>.
space_kind_layer_scope() {
  printf 'kind-%s-e%s\n' "$1" "$SPACE_KIND_LAYER_CACHE_EPOCH"
}

# Set CACHE_FLAGS to the docker build options that import <scope> and, when
# writes are enabled, export it; CACHE_FROM_FLAGS and CACHE_TO_FLAGS hold the
# two parts. Populates arrays rather than word-splitting registry
# configuration. CI enables the containerd image store for the default Docker
# build driver, which registry exports need. Token permissions are enforced by
# workflow callers, not by these environment checks.
#
# An export ignores its errors, so a failed upload never fails the build that
# produced the layers. A caller whose only purpose is the export passes
# --fail-on-export-error to see the failure instead.
space_kind_cache_flags() {
  local scope=$1 repository="${SPACE_KIND_BUILD_CACHE_REPOSITORY:-}"
  local write="${SPACE_KIND_BUILD_CACHE_WRITE:-0}" ignore_error=true
  CACHE_FLAGS=()
  CACHE_FROM_FLAGS=()
  CACHE_TO_FLAGS=()
  case "${2:-}" in
    "") ;;
    --fail-on-export-error) ignore_error=false ;;
    *) printf 'build cache: unknown option %s\n' "$2" >&2; return 1 ;;
  esac
  if [[ "$write" != 0 && "$write" != 1 ]]; then
    printf 'build cache: SPACE_KIND_BUILD_CACHE_WRITE must be 0 or 1\n' >&2
    return 1
  fi
  if [[ -z "$repository" ]]; then
    if [[ "$write" == 1 ]]; then
      printf 'build cache: cache writes require SPACE_KIND_BUILD_CACHE_REPOSITORY\n' >&2
      return 1
    fi
    return 0
  fi
  if [[ ! "$repository" =~ ^ghcr\.io/[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$ \
        && ! "$repository" =~ ^(localhost|127\.0\.0\.1):[0-9]+/[a-z0-9][a-z0-9._-]*$ ]]; then
    printf 'build cache: the repository must be a lowercase ghcr.io/owner/package or localhost:<port>/<name>, without a tag\n' >&2
    return 1
  fi
  CACHE_FROM_FLAGS=(--cache-from "type=registry,ref=$repository:$scope")
  # Read the old scopes during migration, without consuming more shared quota
  # or making PRs wait for an export. Bound an unavailable service's lookup.
  if [[ "${GITHUB_ACTIONS:-}" == true && -n "${ACTIONS_RUNTIME_TOKEN:-}" ]]; then
    CACHE_FROM_FLAGS+=(--cache-from "type=gha,scope=$scope,timeout=60s")
  fi
  if [[ "$write" == 1 ]]; then
    if [[ "${GITHUB_ACTIONS:-}" != true || "${GITHUB_EVENT_NAME:-}" != push \
          || "${GITHUB_REF:-}" != refs/heads/main || "${GITHUB_REPOSITORY:-}" != perplexityai/SPACE ]]; then
      printf 'build cache: cache writes are restricted to perplexityai/SPACE main push CI\n' >&2
      return 1
    fi
    CACHE_TO_FLAGS=(--cache-to "type=registry,ref=$repository:$scope,mode=max,image-manifest=true,oci-mediatypes=true,ignore-error=$ignore_error")
  fi
  # shellcheck disable=SC2034 # read by the callers
  CACHE_FLAGS=("${CACHE_FROM_FLAGS[@]}" "${CACHE_TO_FLAGS[@]}")
}

# A build that could not read a layer blob a manifest named: missing,
# unreadable or corrupt. BuildKit reports it the same way for an imported
# cache's layers and for a mirrored base image's.
SPACE_KIND_LAYER_READ_FAILURE='^ERROR:.*(httpReadSeeker|could not fetch content descriptor|unexpected commit digest|failed size validation)'

# Run `docker build <import options> <args>`, where the first argument counts
# the import options that follow it. Reading the cache can fail after its
# manifest loaded: a layer it names is missing, corrupt or unreachable, and
# the build stops at the first step that needs it. Such a build runs once more
# without the imports, so a broken cache entry costs a cold build and never
# the build. Every other failure, and a repeat of this one, stands. The
# build's stdout and stderr are passed through unchanged.
space_kind_build_reading_cache() {
  local - count=$1 log status=0
  set -o pipefail
  shift
  local -a imports=("${@:1:count}")
  shift "$count"
  if (( count == 0 )); then
    docker build "$@"
    return
  fi
  log="$(mktemp)"
  { docker build "${imports[@]}" "$@" 2>&1 1>&3 3>&- | tee "$log" >&2 3>&-; } 3>&1 || status=$?
  if (( status != 0 )) && grep -qE "$SPACE_KIND_LAYER_READ_FAILURE" "$log"; then
    printf 'build cache: could not read a cached layer; building again without the cache\n' >&2
    [[ "${GITHUB_ACTIONS:-}" != true ]] \
      || printf '::warning::A Kind build could not read a cached layer and was rebuilt without the cache\n' >&2
    rm -f "$log"
    docker build "$@"
    return
  fi
  rm -f "$log"
  return "$status"
}
