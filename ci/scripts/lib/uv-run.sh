# shellcheck shell=bash
# `uv run` from uv's cache when the cache holds the whole environment, and
# against the package index otherwise.
#
# Online, uv revalidates each cached index page once its freshness lapses (PyPI
# allows ten minutes), even when every distribution it needs is cached, so an
# index outage fails a run whose packages are all on disk. CI restores uv's
# cache from main (see common-ci.yml), so the environment is first prepared
# offline, by a command that runs nothing. If that works the run is offline;
# otherwise it goes to the index as before. The command itself runs once, so a
# failing test never runs twice. Each call logs which way it ran to stderr.
#
# space_uv_run <uv> [uv run options...] -- <command> [args...]
#   With --script among the options, <command> is the script, and its
#   environment is prepared with `uv sync --script`. Only this uv call is
#   offline: one the command makes itself is not.
space_uv_run() {
    local uv_bin="$1"
    shift
    local options=() script=false prepared=false
    while [[ $# -gt 0 && "$1" != -- ]]; do
        [[ "$1" == --script ]] && script=true
        options+=("$1")
        shift
    done
    if [[ $# -lt 2 ]]; then
        echo "usage: space_uv_run <uv> [uv run options...] -- <command> [args...]" >&2
        return 2
    fi
    shift

    if [[ "${script}" == true ]]; then
        if "${uv_bin}" sync --offline --quiet --script "$1" </dev/null >/dev/null 2>&1; then
            prepared=true
        fi
    elif "${uv_bin}" run --offline --quiet ${options[@]+"${options[@]}"} python -c '' </dev/null >/dev/null 2>&1; then
        prepared=true
    fi
    if [[ "${prepared}" == true ]]; then
        echo "uv: running $1 offline, from uv's cache" >&2
        "${uv_bin}" run --offline ${options[@]+"${options[@]}"} "$@"
    else
        echo "uv: uv's cache lacks part of $1's environment; resolving against the package index" >&2
        "${uv_bin}" run ${options[@]+"${options[@]}"} "$@"
    fi
}
