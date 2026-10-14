#!/usr/bin/env bash
# Prepare and report a package cache that a CI job restored from GitHub's
# Actions cache: Cargo's registry, Go's module download cache, uv's cache, or
# npm's cache.
#
# Main saves each one under the hash of its lockfile, and pull requests and the
# merge queue restore it, so a job downloads only the packages its lockfile
# changed. A missing or unusable copy only makes the job slower: the tools fetch
# whatever is absent from upstream as before.
#
# restored <kind> <dir> <matched-key>
#   Run after the restore, before anything reads <dir>. When <matched-key> is
#   empty nothing was restored, or the restore failed, possibly after
#   unpacking part of an archive; a truncated archive would fail the job where
#   a missing one only costs a download, so <dir> is emptied. Then marks the
#   time, which `report` compares against.
#
# report <kind> <dir>
#   Prints the cache's size and how many packages the job added since
#   `restored`: downloaded, or for uv also built from source. Never fails.
#
# <kind> is cargo (<dir> is the registry: CARGO_HOME/registry), go (GOMODCACHE),
# uv (UV_CACHE_DIR) or npm (npm's cache). The marker sits beside <dir>, outside
# what is cached.
set -euo pipefail

usage() {
    echo "usage: $0 restored <cargo|go|uv|npm> <dir> <matched-key> | report <cargo|go|uv|npm> <dir>" >&2
    exit 2
}

# The paths the cache saves under <dir>, and the files a fetch adds there.
cached_paths() {
    case "$1" in
        cargo) printf '%s\n' "$2/cache" "$2/index" ;;
        go) printf '%s\n' "$2/cache/download" ;;
        uv | npm) printf '%s\n' "$2" ;;
    esac
}

fetched() {
    local kind="$1" dir="$2" marker="$3"
    case "${kind}" in
        cargo) find "${dir}/cache" -name '*.crate' -newer "${marker}" ;;
        go) find "${dir}/cache/download" -name '*.zip' -newer "${marker}" ;;
        # uv unpacks every wheel it downloads or builds into one new directory.
        uv) find "${dir}/archive-v0" -mindepth 1 -maxdepth 1 -newer "${marker}" ;;
        npm) find "${dir}/_cacache/content-v2" -type f -newer "${marker}" ;;
    esac 2>/dev/null | wc -l
}

unit() {
    case "$1" in
        cargo) echo crates ;;
        go) echo "module archives" ;;
        uv) echo "distributions, downloaded or built," ;;
        npm) echo "cache entries" ;;
    esac
}

[[ $# -ge 3 ]] || usage
command="$1" kind="$2" dir="${3%/}"
case "${kind}" in
    cargo | go | uv | npm) ;;
    *) usage ;;
esac
[[ -n "${dir}" ]] || usage
marker="${dir}.restored"

case "${command}" in
    restored)
        [[ $# -eq 4 ]] || usage
        matched_key="$4"
        if [[ -n "${matched_key}" ]]; then
            echo "Restored the ${kind} packages from ${matched_key}."
        else
            echo "No ${kind} package cache was restored; this job fetches its packages from upstream."
            rm -rf "${dir}"
        fi
        mkdir -p "${dir}"
        # A restore keeps the mtimes of the run that saved the cache, so
        # backdating the mark by a second never counts a restored file, and
        # it does count a file the filesystem's coarse clock stamps with the
        # mark's own time.
        touch -d '1 second ago' "${marker}"
        ;;
    report)
        [[ $# -eq 3 ]] || usage
        if [[ ! -e "${marker}" ]]; then
            echo "The ${kind} package cache in ${dir} was never prepared."
            exit 0
        fi
        mapfile -t paths < <(cached_paths "${kind}" "${dir}")
        size="$(du -csh "${paths[@]}" 2>/dev/null | tail -n 1 | cut -f 1 || true)"
        echo "The ${kind} package cache holds ${size:-nothing}; this job added $(fetched "${kind}" "${dir}" "${marker}") $(unit "${kind}") to it."
        ;;
    *)
        usage
        ;;
esac
