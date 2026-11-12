#!/usr/bin/env bash
# Reuse a Cargo target directory built by an earlier commit, safely.
#
# sccache serves library compiles but cannot cache the units that link: test
# harnesses, binaries, build scripts and proc-macros. A restored target
# directory skips those too. Cargo decides freshness by mtime, and a checkout
# stamps every file with the checkout time, so a restored directory is only
# usable once the sources' mtimes say exactly what changed.
#
# hydrate <target-dir> [<parts-dir>]
#   Unpacks <parts-dir> (see prepare) into <target-dir> when it exists. Then
#   stamps every tracked file and directory with one fixed time older than any
#   build output, so unchanged sources read as fresh, compares the tree the
#   directory was built from with HEAD, and stamps every differing path, and
#   every directory above it, newer than anything in the directory. Cargo then
#   rebuilds exactly those paths' units and everything depending on them.
#   Directories matter because a build script's `rerun-if-changed=<dir>` reads
#   the newest mtime in the walk, directories included, and a deletion changes
#   only its parent. The built-from tree is a full `git ls-tree` listing, so the
#   comparison needs no history and works for pull-request and merge-queue
#   merge commits. A directory that fails to unpack or has no readable record
#   is deleted, and the job builds cold. The fixed stamp applies to cold builds
#   too, so a saved directory agrees with the stamps the next job applies.
#
#   It also deletes what a later step could read by path instead of through
#   Cargo: the uplifted copies of binaries, examples and libraries at the top of
#   the profile directory, and integration-test scratch space. Cargo recreates
#   an uplifted copy for every target it still builds, fresh or not, so a
#   removed or renamed binary cannot survive as a stale, runnable file.
#
# prepare <target-dir> <parts-dir>
#   After a successful build: keeps only Cargo's own layout, records HEAD's
#   tree, and packs the directory into PARTS zstd tar archives in <parts-dir>
#   (save=true). Refuses (save=false) when the build changed a tracked file,
#   because the directory would then match neither tree.
#
#   The archives are what gets cached. Files go in path order, so each crate's
#   test binaries sit next to each other, and zstd's 2 GiB long-distance window
#   finds the code those statically linked binaries share: 17.7 GB packs to
#   1.6 GB, where the cache action's own compression gives 3.3 GB. Several
#   archives unpack in parallel; one zstd stream decompresses on one core. A
#   small archive of the directories unpacks before them, so that no two tars
#   ever race to create the same directory.
#
# Every subcommand writes GitHub step outputs when GITHUB_OUTPUT is set.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RECORD_NAME=".space-target-source"
# 2000-01-01T00:00:00Z: older than any target directory this job can restore.
SOURCE_EPOCH=946684800
PARTS="${SPACE_TARGET_CACHE_PARTS:-4}"
ZSTD_LEVEL="${SPACE_TARGET_CACHE_ZSTD_LEVEL:-12}"
ZSTD_WINDOW_LOG=31

output() {
    echo "$1=$2"
    if [ -n "${GITHUB_OUTPUT:-}" ]; then
        echo "$1=$2" >>"${GITHUB_OUTPUT}"
    fi
}

stamp_paths() {
    local stamp="$1"
    xargs -0 -r touch -h -c -d "@${stamp}" --
}

stamp_every_source_path() {
    git ls-files -z | stamp_paths "${SOURCE_EPOCH}"
    git ls-tree -r -d -z --name-only HEAD | stamp_paths "${SOURCE_EPOCH}"
}

# Every path whose mode or content differs between the recorded tree and
# HEAD, including paths present in only one of them.
changed_paths() {
    local record="$1"
    local scratch="$2"
    LC_ALL=C sort -z "${record}/tree" >"${scratch}/previous"
    git ls-tree -r -z --full-tree HEAD | LC_ALL=C sort -z >"${scratch}/current"
    {
        LC_ALL=C comm -z -23 "${scratch}/previous" "${scratch}/current"
        LC_ALL=C comm -z -13 "${scratch}/previous" "${scratch}/current"
    } | cut -z -f2- | LC_ALL=C sort -zu
}

# Each changed path that still exists, plus every existing directory above it.
paths_to_restamp() {
    local path parent
    while IFS= read -r -d '' path; do
        if [ -e "${path}" ] || [ -L "${path}" ]; then
            printf '%s\0' "${path}"
        fi
        parent="${path}"
        while [[ "${parent}" == */* ]]; do
            parent="${parent%/*}"
            if [ -d "${parent}" ]; then
                printf '%s\0' "${parent}"
            fi
        done
    done
}

# Cargo 1.100 keeps intermediates and fingerprints under build/<crate>/<hash>.
# Top-level outputs and examples are uplifted copies it recreates on every build.
delete_path_readable_outputs() {
    local target_dir="$1"
    local profile
    rm -rf "${target_dir}/tmp"
    for profile in "${target_dir}"/*/; do
        [ -d "${profile}build" ] || continue
        find "${profile}" -maxdepth 1 \( -type f -o -type l \) -delete
        if [ -d "${profile}examples" ]; then
            find "${profile}examples" -maxdepth 1 \( -type f -o -type l \) -delete
        fi
    done
}

extract() {
    zstd -q -d --long="${ZSTD_WINDOW_LOG}" -c "$1" | tar -xf - -C "$2"
}

# The directories first, then the parts in parallel. A tar that finds a
# file's directory missing creates it, but GNU tar before 1.35 then gives up
# on the file when another tar created that directory in the meantime.
unpack() {
    local parts_dir="$1" target_dir="$2"
    local part pids=() status=0
    if [ ! -f "${parts_dir}/dirs.tar.zst" ] || ! compgen -G "${parts_dir}/part-*.tar.zst" >/dev/null; then
        echo "No directory archive or no file parts in ${parts_dir}." >&2
        return 1
    fi
    mkdir -p "${target_dir}"
    extract "${parts_dir}/dirs.tar.zst" "${target_dir}"
    for part in "${parts_dir}"/part-*.tar.zst; do
        extract "${part}" "${target_dir}" &
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do
        wait "${pid}" || status=$?
    done
    return "${status}"
}

# Runs in its own process (see hydrate) so that errexit applies throughout;
# any failure there discards the restored directory.
hydrate_unguarded() {
    local target_dir="$1" parts_dir="${2:-}"
    local record="${target_dir}/${RECORD_NAME}"
    local scratch newest now stamp changed_count start

    if [ -n "${parts_dir}" ] && [ -d "${parts_dir}" ]; then
        start="${SECONDS}"
        rm -rf "${target_dir}"
        unpack "${parts_dir}" "${target_dir}"
        rm -rf "${parts_dir}"
        echo "Unpacked the target directory in $((SECONDS - start)) s."
    fi
    stamp_every_source_path
    if [ ! -d "${target_dir}" ]; then
        echo "No target directory was restored; building cold."
        output restored false
        return 0
    fi
    if [ ! -s "${record}/tree" ]; then
        echo "::warning::The restored target directory has no source record; building cold."
        rm -rf "${target_dir}"
        output restored false
        return 0
    fi

    scratch="$(mktemp -d)"
    changed_paths "${record}" "${scratch}" >"${scratch}/changed"
    changed_count="$(tr -cd '\0' <"${scratch}/changed" | wc -c)"
    echo "The restored target directory was built from $(cat "${record}/commit");" \
        "${changed_count} paths differ from HEAD:"
    tr '\0' '\n' <"${scratch}/changed" | sed -n '1,50p'

    delete_path_readable_outputs "${target_dir}"

    # A changed path must read as newer than every restored output, even when
    # the saving runner's clock ran ahead of this one. Outputs are files; the
    # unpacked directories carry this job's clock.
    newest="$(find "${target_dir}" -type f -printf '%T@\n' | sort -n | tail -n 1)"
    newest="${newest%.*}"
    now="$(date +%s)"
    if [ "$((newest + 2 - now))" -gt 30 ]; then
        echo "The restored target directory is $((newest - now)) s in the future." >&2
        return 1
    elif [ "$((newest + 2))" -gt "${now}" ]; then
        sleep "$((newest + 2 - now))"
    fi
    stamp="$(date +%s)"
    paths_to_restamp <"${scratch}/changed" | stamp_paths "${stamp}"
    rm -rf "${scratch}"

    output changed_paths "${changed_count}"
    output restored true
}

hydrate() {
    local target_dir="$1" parts_dir="${2:-}"
    if bash "${BASH_SOURCE[0]}" hydrate-unguarded "${target_dir}" "${parts_dir}"; then
        return 0
    fi
    echo "::warning::Hydrating the Cargo target directory failed; building cold."
    rm -rf "${target_dir}"
    if [ -n "${parts_dir}" ]; then
        rm -rf "${parts_dir}"
    fi
    output restored false
}

# Every directory, empty ones included, in one archive, then contiguous runs
# of the path-sorted file list, of about equal size, as parts. Hard links are
# stored as regular files so that no part refers to a file in another. A
# failed tar or zstd in any part fails prepare, so nothing is saved: waiting
# on a background pipeline's pid returns its pipefail status.
pack() {
    local target_dir="$1" parts_dir="$2"
    local threads list pid pids=() status=0
    threads=$(( ($(nproc) + PARTS - 1) / PARTS ))
    rm -rf "${parts_dir}"
    mkdir -p "${parts_dir}"
    (cd "${target_dir}" && find . -mindepth 1 -type d -printf '%P\n') | LC_ALL=C sort \
        >"${parts_dir}/dirs.list"
    (cd "${target_dir}" && find . \( -type f -o -type l \) -printf '%s\t%P\n') |
        LC_ALL=C sort -t $'\t' -k2 |
        awk -F '\t' -v parts="${PARTS}" -v dir="${parts_dir}" '
            { size[NR] = $1; path[NR] = $2; total += $1 }
            END {
                part = 0
                for (i = 1; i <= NR; i++) {
                    list = sprintf("%s/part-%02d.list", dir, part)
                    print path[i] >> list
                    done += size[i]
                    if (part < parts - 1 && done >= total * (part + 1) / parts) {
                        close(list)
                        part++
                    }
                }
            }'
    for list in "${parts_dir}"/*.list; do
        tar --format=posix --hard-dereference --no-recursion -C "${target_dir}" -cf - -T "${list}" |
            zstd -q -T"${threads}" -"${ZSTD_LEVEL}" --long="${ZSTD_WINDOW_LOG}" \
                -o "${list%.list}.tar.zst" &
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do
        wait "${pid}" || status=$?
    done
    rm -f "${parts_dir}"/*.list
    return "${status}"
}

prepare() {
    local target_dir="$1" parts_dir="$2"
    local record="${target_dir}/${RECORD_NAME}" start
    # The CI container builds as root; the cache action runs as the runner user.
    if [ -n "$(find "${target_dir}" ! -user "$(id -u)" -print -quit)" ]; then
        sudo chown -R "$(id -u):$(id -g)" "${target_dir}"
    fi
    if ! git diff --quiet HEAD --; then
        echo "::warning::The build changed tracked files; not saving the target directory."
        git diff --stat HEAD -- | tail -n 20
        output save false
        return 0
    fi
    # Only Cargo's own layout is reused; reports, logs and scratch space are not.
    find "${target_dir}" -mindepth 1 -maxdepth 1 ! -name debug ! -name .rustc_info.json \
        ! -name CACHEDIR.TAG -exec rm -rf {} +
    delete_path_readable_outputs "${target_dir}"
    mkdir -p "${record}"
    git ls-tree -r -z --full-tree HEAD >"${record}/tree"
    git rev-parse HEAD >"${record}/commit"
    start="${SECONDS}"
    pack "${target_dir}" "${parts_dir}"
    echo "Packed $(du -sh "${target_dir}" | cut -f1) into $(du -sh "${parts_dir}" | cut -f1)" \
        "in $((SECONDS - start)) s."
    output save true
}

usage() {
    echo "Usage: $0 hydrate <target-dir> [<parts-dir>] | prepare <target-dir> <parts-dir>" >&2
    exit 2
}

main() {
    [ "$#" -ge 2 ] || usage
    cd "${REPO_ROOT}"
    case "$1" in
        hydrate) hydrate "$2" "${3:-}" ;;
        hydrate-unguarded) hydrate_unguarded "$2" "${3:-}" ;;
        prepare)
            [ "$#" -eq 3 ] || usage
            prepare "$2" "$3"
            ;;
        *) usage ;;
    esac
}

main "$@"
