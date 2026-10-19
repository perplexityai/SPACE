#!/usr/bin/env bash
# Regression test for ci/scripts/order-archcheck-findings.sh: a pull request's
# own findings come first, those comparing with the base's baseline or
# ARCHITECTURE.toml leading, each group in report order; a stale baseline
# entry is its own when it names a changed file, or a [crate_names] entry the
# changed Cargo.toml of its member; and --own counts only what reviewdog shows.
set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/order-archcheck-findings.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
cd "${WORK}"

FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# Prints "<path>:<line>" for each diagnostic of the ordered report.
order() {
    bash "${SCRIPT}" "$@" | jq -r '.diagnostics[] | "\(.location.path):\(.location.range.start.line)"' | paste -sd ' ' -
}

expect() {
    local name="$1" expected="$2"
    shift 2
    local actual
    actual="$(order "$@")" || { fail "${name}: the script failed"; return; }
    [[ "${actual}" == "${expected}" ]] || fail "${name}: got '${actual}', expected '${expected}'"
}

diagnostic() {
    jq -nc --arg path "$1" --argjson line "$2" --arg code "$3" --arg severity "${4:-WARNING}" \
        '{message: "m", location: {path: $path, range: {start: {line: $line}}}, severity: $severity, code: {value: $code}}'
}

report() {
    jq -sc '{source: {name: "archcheck"}, diagnostics: .}'
}

# One page of the pull request's files, as the GitHub API lists them, each
# with changes.
pages() {
    jq -nc '$ARGS.positional | map({filename: ., changes: 1})' --args "$@"
}

# Prints "<prefix><i>.rs:1" for i from 1 to <n>.
numbered() {
    seq 1 "$2" | sed "s/.*/$1&.rs:1/" | paste -sd ' ' -
}

tree_findings() {
    for i in $(seq 1 11); do diagnostic "tree${i}.rs" 1 file_size.listed_no_growth; done
}

B=architecture-baseline.toml

# A pull request touching only source files (-filter-mode=file): its files'
# findings come first in report order, which is all reviewdog shows.
cat >"${B}" <<'EOF'
[file_size]
"a.rs" = 1100
"b.rs" = 1200
"gone.rs" = 1300
EOF
{
    diagnostic a.rs 1 file_size.listed_no_growth
    diagnostic big.rs 1 file_size.no_crossing
    diagnostic "${B}" 4 file_size.stale_entries
    diagnostic c.rs 1 file_size.listed_no_growth
    diagnostic b.rs 7 pattern_ratchet.tables.listed_no_growth
    diagnostic c.rs 9 pattern_ratchet.tables.no_crossing ERROR
} | report >source.rdjson
pages c.rs big.rs >sources.json
expect "source files" "big.rs:1 c.rs:1 c.rs:9 a.rs:1 ${B}:4 b.rs:7" source.rdjson sources.json
expect "source files, own only" "big.rs:1 c.rs:1 c.rs:9" --own=file source.rdjson sources.json

# Nothing changed that has findings: the report is unchanged.
pages README.md >unrelated.json
[[ "$(bash "${SCRIPT}" source.rdjson unrelated.json | jq -c .)" == "$(jq -c . source.rdjson)" ]] ||
    fail "unrelated files: the report changed"

# A pull request changing the baseline (-filter-mode=nofilter) adds an entry
# and raises a cap: those come first, not the stale entry it left alone.
{
    diagnostic a.rs 1 file_size.listed_no_growth
    diagnostic "${B}" 3 file_size.no_raised_caps
    diagnostic "${B}" 4 file_size.stale_entries
    diagnostic "${B}" 5 file_size.no_new_entries
    diagnostic c.rs 1 file_size.listed_no_growth
} | report >baseline.rdjson
pages "${B}" >baseline.json
expect "baseline" "${B}:3 ${B}:5 a.rs:1 ${B}:4 c.rs:1" baseline.rdjson baseline.json
# With no file list, as when the API call fails, they still come first.
echo '[]' >none.json
expect "no file list" "${B}:3 ${B}:5 a.rs:1 ${B}:4 c.rs:1" baseline.rdjson none.json

# Renaming a table, or splitting one, leaves entry lines as they were, but
# archcheck compares by table and key, so their no_new_entries findings are
# the pull request's.
printf '[pattern_ratchet.new]\n"x.rs" = 1\n"y.rs" = 2\n"z.rs" = 3\n' >"${B}"
{
    tree_findings
    for line in 2 3 4; do diagnostic "${B}" "${line}" pattern_ratchet.new.no_new_entries; done
} | report >renamed.rdjson
pages ARCHITECTURE.toml "${B}" >rules.json
expect "renamed table" "${B}:2 ${B}:3 ${B}:4" --own=nofilter renamed.rdjson rules.json
printf '[pattern_ratchet.old]\n"x.rs" = 1\n\n[pattern_ratchet.more]\n"y.rs" = 2\n"z.rs" = 3\n' >"${B}"
{
    tree_findings
    for line in 5 6; do diagnostic "${B}" "${line}" pattern_ratchet.more.no_new_entries; done
} | report >split.rdjson
expect "split table" "${B}:5 ${B}:6" --own=nofilter split.rdjson rules.json

# Base-comparison findings lead the pull request's own, so 10 or more own
# findings elsewhere can't push them out.
printf '[file_size]\n"own.rs" = 1200\n' >"${B}"
{
    for i in $(seq 1 5); do diagnostic "tree${i}.rs" 1 file_size.listed_no_growth; done
    for i in $(seq 1 12); do diagnostic "own${i}.rs" 1 pattern_ratchet.tables.no_crossing; done
    diagnostic "${B}" 2 file_size.no_raised_caps
} | report >many.rdjson
many=()
for i in $(seq 1 12); do many+=("own${i}.rs"); done
pages ARCHITECTURE.toml "${many[@]}" >many.json
expect "more than 10 own" "${B}:2 $(numbered own 12) $(numbered tree 5)" many.rdjson many.json
count="$(bash "${SCRIPT}" --own=nofilter many.rdjson many.json | jq '.diagnostics | length')"
[[ "${count}" == 13 ]] || fail "more than 10 own: --own=nofilter printed ${count} diagnostics, expected 13"

# Deleting listed files makes their stale entries the pull request's, however
# the key is written. Without editing the baseline (-filter-mode=file),
# reviewdog shows none of them.
cat >"${B}" <<'EOF'
[file_size]
  "indented.rs" = 1300
'literal.rs' = 1300
"quote\"d.rs"=1300
EOF
# A \u escape, written so that nothing on the way here decodes it.
printf '"uni%su0063ode.rs" = 1300\n"kept.rs" = 1300\n' "\\" >>"${B}"
{
    tree_findings
    for line in 2 3 4 5 6; do diagnostic "${B}" "${line}" file_size.stale_entries; done
} | report >deleted.rdjson
pages indented.rs literal.rs 'quote"d.rs' unicode.rs >deleted.json
expect "deleted files" "${B}:2 ${B}:3 ${B}:4 ${B}:5" --own=nofilter deleted.rdjson deleted.json
expect "deleted files, file mode" "" --own=file deleted.rdjson deleted.json

# The API's file list: renamed files under both names, names with spaces,
# and one array per page, with or without a separator.
printf '[file_size]\n"old name.rs" = 1300\n' >"${B}"
{
    tree_findings
    diagnostic "new name.rs" 1 file_size.listed_no_growth
    diagnostic "${B}" 2 file_size.stale_entries
    diagnostic second.rs 1 file_size.listed_no_growth
    diagnostic third.rs 1 file_size.listed_no_growth
} | report >listed.rdjson
{
    printf '[{"filename": "new name.rs", "previous_filename": "old name.rs", "status": "renamed", "changes": 2}]'
    pages second.rs
    pages third.rs | tr -d '\n'
} >listed.json
expect "file list" "new name.rs:1 ${B}:2 second.rs:1 third.rs:1 $(numbered tree 11)" listed.rdjson listed.json

# A pure rename has no diff for reviewdog's file mode to keep: its findings
# are the pull request's, but not shown.
{
    tree_findings
    diagnostic moved.rs 1 file_size.listed_no_growth
    diagnostic edited.rs 1 file_size.listed_no_growth
} | report >moved.rdjson
{
    printf '[{"filename": "moved.rs", "previous_filename": "kept.rs", "status": "renamed", "changes": 0}]'
    pages edited.rs
} >moved.json
expect "pure rename" "moved.rs:1 edited.rs:1 $(numbered tree 11)" moved.rdjson moved.json
expect "pure rename, file mode" "edited.rs:1" --own=file moved.rdjson moved.json

# crate_names.policy_changed compares ARCHITECTURE.toml with the base's, so
# it leads with the other base comparisons; a required no_new finding on a new
# crate's Cargo.toml is the pull request's, and kept.
printf '[file_size]\n' >"${B}"
{
    tree_findings
    diagnostic own.rs 1 file_size.listed_no_growth
    diagnostic crates/new-utils/Cargo.toml 2 crate_names.no_new ERROR
    diagnostic ARCHITECTURE.toml 80 crate_names.policy_changed
    diagnostic ARCHITECTURE.toml 81 crate_names.stale_entries
} | report >policy.rdjson
pages ARCHITECTURE.toml own.rs crates/new-utils/Cargo.toml >policy.json
expect "crate_names policy" \
    "ARCHITECTURE.toml:80 own.rs:1 crates/new-utils/Cargo.toml:2 ARCHITECTURE.toml:81" \
    --own=nofilter policy.rdjson policy.json

# A stale [crate_names] entry is the pull request's when it changes the
# Cargo.toml of the member path the entry maps to (old or new name), however
# the key and path are written; an empty path is the root package.
cat >"${B}" <<'EOF'
[file_size]
"crates/z/src/lib.rs" = 1300

[crate_names]
pplx-space-x-common = "crates/x"
"pplx-space-y-utils" = 'crates/y'  # moved
pplx-space-z-core = "crates/z"
pplx-space-root-misc = ""
EOF
{
    tree_findings
    for line in 5 6 7 8; do diagnostic "${B}" "${line}" crate_names.stale_entries; done
} | report >crates.rdjson
{
    printf '[{"filename": "crates/y2/Cargo.toml", "previous_filename": "crates/y/Cargo.toml", "status": "renamed", "changes": 0}]'
    pages crates/x/Cargo.toml Cargo.toml crates/z/src/lib.rs
} >crates.json
expect "crate_names entries" "${B}:5 ${B}:6 ${B}:8 $(numbered tree 11) ${B}:7" crates.rdjson crates.json

if bash "${SCRIPT}" source.rdjson 2>/dev/null; then
    fail "the script accepted a missing <pull-request-files> argument"
fi
if bash "${SCRIPT}" --own=diff source.rdjson sources.json 2>/dev/null; then
    fail "the script accepted an unknown filter mode"
fi

[ "${FAILURES}" -eq 0 ] || exit 1
echo "order-archcheck-findings tests passed"
