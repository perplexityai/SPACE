#!/usr/bin/env bash
# Put a pull request's own archcheck findings first, for annotation.
#
# order-archcheck-findings.sh [--own=<filter-mode>] <rdjson> <pull-request-files>
#   Prints the rdjson report <rdjson> with the diagnostics this pull request
#   is responsible for first, then the rest. With --own=<filter-mode>, prints
#   only the first group's diagnostics that reviewdog keeps with that
#   -filter-mode (file or nofilter). Run from the repository root; needs jq.
#
#   <pull-request-files> is the output of
#   `gh api --paginate repos/<owner>/<repo>/pulls/<number>/files`: one or
#   more JSON arrays of files (gh may merge pages).
#
# GitHub shows the first 10 warning annotations of a step, and archcheck
# reports every file_size finding before the pattern_ratchet ones, so without
# this a pull request's own findings can be pushed out by the tree's.
#
# The pull request's own findings, in this order, each in report order:
# - no_new_entries and no_raised_caps findings. archcheck reports these only
#   for entries that differ from the base branch's copy of
#   architecture-baseline.toml, by table and key, so they include the
#   entries of a renamed or split table.
# - Findings in a file the pull request changes, under its old or new name.
#   archcheck's only other findings in architecture-baseline.toml are stale
#   entries for files that are gone, so one there is the pull request's when
#   its entry names a file it changes. An entry's key may be indented and a
#   basic or literal string; a basic string with an escape JSON lacks (\e,
#   \U) names no file.
#
# reviewdog's -filter-mode=file keeps findings in files whose diff has a
# +++ line: those with changes, under their new name. A pure rename, a mode
# change or a binary file has none.
set -euo pipefail

own=""
if [[ "${1:-}" == --own=* ]]; then
    own="${1#--own=}"
    shift
fi
if [[ -n "${own}" && "${own}" != file && "${own}" != nofilter ]] || (($# != 2)); then
    echo "usage: $0 [--own=file|--own=nofilter] <rdjson> <pull-request-files>" >&2
    exit 2
fi
rdjson="$1"
pull_request_files="$2"
baseline=architecture-baseline.toml

head_baseline=/dev/null
[[ -f "${baseline}" ]] && head_baseline="${baseline}"

jq --arg baseline "${baseline}" --arg own "${own}" --arg q "'" \
    --slurpfile pages "${pull_request_files}" --rawfile head_baseline "${head_baseline}" '
    [$pages[][]] as $listed
    | ([$listed[] | .filename, .previous_filename // empty] | INDEX(.)) as $files
    | ([$listed[] | select(.changes > 0) | .filename] | INDEX(.)) as $diffed
    | def entry_path:
        capture("^\\s*(?:\"(?<basic>(?:[^\"\\\\]|\\\\.)*)\"|\($q)(?<literal>[^\($q)]*)\($q))\\s*=")
        | if .basic != null then (try ("\"" + .basic + "\"" | fromjson) catch null) else .literal end;
      ([$head_baseline | split("\n") | to_entries[]
          | select([.value | entry_path | select(. != null and $files[.] != null)] | length > 0)
          | .key + 1 | tostring]
        | INDEX(.)) as $named_lines
    | def compares_with_base: (.code.value // "") | test("\\.(no_new_entries|no_raised_caps)$");
      def own:
        compares_with_base
        or if .location.path == $baseline then $named_lines[.location.range.start.line // 0 | tostring] != null
        else $files[.location.path // ""] != null end;
      def shown: $own == "nofilter" or $diffed[.location.path // ""] != null;
      .diagnostics |= (. // []
        | (map(select(compares_with_base)) + map(select(own and (compares_with_base | not)))) as $own_group
        | if $own != "" then $own_group | map(select(shown)) else $own_group + map(select(own | not)) end)
    ' "${rdjson}"
