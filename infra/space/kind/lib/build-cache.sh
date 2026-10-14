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

# Set CACHE_FLAGS to the docker build options that import <scope> and, when
# writes are enabled, export it. Populates an array rather than word-splitting
# registry configuration. CI enables the containerd image store for the default
# Docker build driver, which registry exports need. Token permissions are
# enforced by workflow callers, not by these environment checks.
space_kind_cache_flags() {
  local scope=$1 repository="${SPACE_KIND_BUILD_CACHE_REPOSITORY:-}"
  local write="${SPACE_KIND_BUILD_CACHE_WRITE:-0}"
  CACHE_FLAGS=()
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
  CACHE_FLAGS=(--cache-from "type=registry,ref=$repository:$scope")
  # Read the old scopes during migration, without consuming more shared quota
  # or making PRs wait for an export. Bound an unavailable service's lookup.
  if [[ "${GITHUB_ACTIONS:-}" == true && -n "${ACTIONS_RUNTIME_TOKEN:-}" ]]; then
    CACHE_FLAGS+=(--cache-from "type=gha,scope=$scope,timeout=60s")
  fi
  if [[ "$write" == 1 ]]; then
    if [[ "${GITHUB_ACTIONS:-}" != true || "${GITHUB_EVENT_NAME:-}" != push \
          || "${GITHUB_REF:-}" != refs/heads/main || "${GITHUB_REPOSITORY:-}" != perplexityai/SPACE ]]; then
      printf 'build cache: cache writes are restricted to perplexityai/SPACE main push CI\n' >&2
      return 1
    fi
    CACHE_FLAGS+=(--cache-to "type=registry,ref=$repository:$scope,mode=max,image-manifest=true,oci-mediatypes=true,ignore-error=true")
  fi
}
