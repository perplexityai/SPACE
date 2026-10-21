#!/usr/bin/env bash
# Fail when a release build in docker/Dockerfile.rust-build-common resolves a
# test-only Cargo feature: `test-hooks` or `test-support` on any package, or
# any `test-*` feature on a package built from this checkout.
#
# Why this reads the Dockerfile instead of checking a `-p` selection, as
# run_sbxmgr_production_checks in run-rust-checks.sh does: the release stages
# run `cargo build` and `cargo chef cook` from the workspace root, most of them
# with only `--bin` and `-F` arguments. `--bin` filters targets, not packages,
# so Cargo resolves features across every workspace member, and a feature any
# member enables in its normal dependencies reaches the binaries those stages
# link. A `-p` graph cannot show that. So every `cargo build` and
# `cargo chef cook` invocation in the file is read back with its own package
# and feature selection, and `cargo tree` lists the features each package in
# that selection resolves to, `-F` ones included. A new release stage is
# covered without editing this script.
#
# Third-party `test-*` features are out of scope. aws-config turns on
# `test-util` in aws-credential-types and, through it, the aws-smithy crates,
# and libdd-data-pipeline turns it on in tokio, all in their normal
# dependencies upstream. So this check does not claim that a release build
# carries no test-only feature at all.
#
# `cargo chef cook` passes its selection to `cargo build` on a skeleton of the
# same manifests and lockfile, so it resolves the same features. `--bin` and
# `--release` do not change feature resolution and are dropped. Without a
# `--target`, the graph is resolved for every target, a superset of the host
# the image builds on.
#
# It checks the `cargo` invocations written in the Dockerfile itself, read
# through the small grammar below; anything in the file outside the grammar
# fails rather than being skipped. A build inside a script that a RUN runs is
# out of its scope: the runtime-supervisor stage's script is checked only
# through the cargo command the Dockerfile passes it.
#   - Instructions are joined across backslash continuations the way Docker
#     joins them, with nothing inserted, so a word split across a continuation
#     is read whole. Comment lines and blank lines inside one are dropped, as
#     Docker drops them. A parser directive that changes the escape
#     character, and a SHELL instruction, fail.
#   - A RUN is in shell form without a heredoc. A RUN in exec form, or with
#     `<<` anywhere in it, fails, and so does `cargo` in any other instruction.
#   - In a RUN, `cargo` is a word of its own. `cargo build` and
#     `cargo chef cook` are checked. `cargo chef prepare`, `cargo --version`
#     and a `cargo install` of a registry crate build nothing from the
#     workspace and pass. Any other subcommand fails. So does any `cargo` the
#     word scan did not reach: one inside `$(...)`, backticks or an `sh -c`
#     string, or after a separator with no space, such as `true;cargo`. So
#     does any word that needs the shell to expand it and contains `cargo` in
#     any case, such as `${CARGO:-cargo}`.
#   - Every argument of a checked invocation, including the values of options
#     it drops such as `--bin`, is a literal word: a quoted literal is read as
#     its contents, and an expansion, glob or redirection fails.
#   - A `cargo install` argument is a literal word too, read the same way, and
#     `--path` in any form fails, since it builds local code. The one
#     expansion allowed is a double-quoted variable as the value of
#     `--version`, such as `--version "${CARGO_CHEF_VERSION}"`, which stays
#     one word.
#
# Usage: check-release-build-features.sh [DOCKERFILE [WORKSPACE_DIR]]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
dockerfile="${1:-${REPO_ROOT}/docker/Dockerfile.rust-build-common}"
workspace="${2:-${REPO_ROOT}}"

if [ ! -f "${dockerfile}" ]; then
    echo "${dockerfile}: no such file" >&2
    exit 1
fi
shown="${dockerfile#"${REPO_ROOT}/"}"

# A literal word an invocation may carry: no quote, expansion, glob,
# redirection or separator character.
LITERAL='^[A-Za-z0-9_./,:=+@-]+$'

# One line per instruction, as "<first line number><TAB><stage><TAB><text>",
# with backslash continuations joined. The stage is `-` before the first named
# one: an empty field would collapse into the next under `read`.
instructions() {
    awk '
        /^[[:space:]]*#/ { next }
        continued && /^[[:space:]]*$/ { next }
        {
            line = $0
            if (!start) start = NR
            if (sub(/\\[[:space:]]*$/, "", line)) { text = text line; continued = 1; next }
            text = text line
            continued = 0
            if (toupper(text) ~ /^[[:space:]]*FROM[[:space:]]/) {
                n = split(text, words, /[[:space:]]+/)
                for (i = 1; i < n; i++) if (toupper(words[i]) == "AS") stage = words[i + 1]
            }
            if (text ~ /[^[:space:]]/) print start "\t" (stage == "" ? "-" : stage) "\t" text
            text = ""; start = 0
        }
        END { if (text ~ /[^[:space:]]/) print start "\t" (stage == "" ? "-" : stage) "\t" text }
    ' "${dockerfile}"
}

# The number of times `cargo` appears in $1 as a word of its own, wherever it
# is: after a space, a separator, `$(`, a backtick or a quote.
count_cargo() {
    local rest="$1" count=0
    while [[ "${rest}" =~ (^|[^A-Za-z0-9_.-])cargo([^A-Za-z0-9_.-]|$)(.*) ]]; do
        count=$((count + 1))
        rest="${BASH_REMATCH[3]}"
    done
    echo "${count}"
}

failures=0
checked=0

fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

# Resolve one invocation's selection and report any test-only feature in it.
# $1 names the invocation; the rest are `cargo tree` selection arguments.
check_selection() {
    local where="$1"
    shift
    local leaked
    echo "${where}:"
    echo "  cargo tree --locked -e normal,build $*"
    # `{f}` prints each package with the features it resolved to, one per
    # line. Not the `-e features` graph: it leaves out the features a selected
    # package's own `[features]` table turns on, so `-p x -F x/test-hooks`
    # would read clean. One checked pipeline, with no temporary file: awk
    # rejects a line without the format's tab and an empty graph, and a cargo,
    # awk or sort failure fails the selection instead of reading as clean. A
    # blank line separates the trees of several roots. A path package is one
    # built from this checkout.
    if ! leaked="$(cd "${workspace}" && cargo tree --locked -e normal,build --prefix none \
        --format $'{p}\t{f}' "$@" </dev/null | awk -F '\t' '
            NF == 0 { next }
            NF == 1 { bad = 1; exit }
            {
                packages++
                sub(/ \(\*\)$/, "", $2)
                split($1, package, " ")
                ours = $1 ~ / \(\//
                n = split($2, features, ",")
                for (i = 1; i <= n; i++)
                    if (features[i] == "test-hooks" || features[i] == "test-support" \
                        || (ours && features[i] ~ /^test-/))
                        print package[1] " feature \"" features[i] "\""
            }
            END { if (bad) exit 3; if (!packages) exit 4 }
        ' | sort -u)"; then
        fail "${where}: could not resolve the selection or read its features"
        return
    fi
    checked=$((checked + 1))
    if [ -n "${leaked}" ]; then
        fail "${where} enables test-only features:"
        printf '    %s\n' "${leaked//$'\n'/$'\n'    }" >&2
    else
        echo "  ok: no test-only features"
    fi
}

# Read a literal word: a whole quoted literal as its contents. Prints nothing
# and returns 1 for a word that needs the shell to expand it.
literal() {
    local word="$1"
    if [[ "${word}" =~ ^\"([^\"]*)\"$ || "${word}" =~ ^\'([^\']*)\'$ ]]; then
        word="${BASH_REMATCH[1]}"
    fi
    [[ "${word}" =~ ${LITERAL} ]] || return 1
    printf '%s\n' "${word}"
}

# Read the arguments of one `cargo build` or `cargo chef cook` invocation into
# `cargo tree` selection arguments, or fail naming what it could not read.
check_invocation() {
    local where="$1"
    shift
    local selection=() has_target=false arg value
    while [ "$#" -gt 0 ]; do
        if ! arg="$(literal "$1")"; then
            fail "${where}: cannot read argument $1"
            return
        fi
        shift
        case "${arg}" in
            --locked | --frozen | --offline | --release | -v | --verbose | --timings | --bins | --lib) ;;
            --workspace | --all | --all-features | --no-default-features)
                selection+=("${arg}")
                ;;
            --bin | --profile | --recipe-path | -j | --jobs | -p | --package | -F | --features | --exclude | --target)
                if [ "$#" -eq 0 ] || ! value="$(literal "$1")" || [[ "${value}" == -* ]]; then
                    fail "${where}: cannot read ${arg} ${1:-without a value}"
                    return
                fi
                shift
                case "${arg}" in
                    --bin | --profile | --recipe-path | -j | --jobs) ;;
                    --target)
                        has_target=true
                        selection+=("${arg}" "${value}")
                        ;;
                    *) selection+=("${arg}" "${value}") ;;
                esac
                ;;
            --bin=* | --profile=* | --recipe-path=* | --jobs=*) ;;
            --target=*)
                has_target=true
                selection+=("${arg}")
                ;;
            --package=* | --features=* | --exclude=*)
                selection+=("${arg}")
                ;;
            *)
                # Includes --tests, --all-targets and --examples: they add
                # dev-dependencies, which is not a release selection.
                fail "${where}: unsupported argument ${arg}"
                return
                ;;
        esac
    done
    if [ "${has_target}" = false ]; then
        selection+=(--target all)
    fi
    check_selection "${where}" "${selection[@]}"
}

# Read the arguments of a `cargo install` at words[index] in the caller's
# scope, up to the end of its command, and fail one that builds local code or
# cannot be read. Marks the one expansion it allows in `allowed`.
check_install() {
    local arg value end=false
    while [ "${index}" -lt "${#words[@]}" ] && [ "${end}" = false ]; do
        arg="${words[index]}"
        case "${arg}" in
            '&&' | '||' | ';;' | ';' | '|' | ')') break ;;
            *';')
                arg="${arg%;}"
                end=true
                ;;
        esac
        index=$((index + 1))
        if [[ "${arg}" == --version || "${arg}" == --vers ]] && [ "${end}" = false ] \
            && [[ "${words[index]:-}" =~ ^\"\$(\{[A-Za-z_][A-Za-z0-9_]*\}|[A-Za-z_][A-Za-z0-9_]*)\"\;?$ ]]; then
            allowed[index]=1
            [[ "${words[index]}" != *';' ]] || end=true
            index=$((index + 1))
            continue
        fi
        if ! value="$(literal "${arg}")"; then
            fail "${where}: cannot read cargo install argument ${arg}"
            return
        fi
        case "${value}" in
            --path | --path=*)
                fail "${where}: cargo install ${value} builds local code, which this check does not read"
                return
                ;;
        esac
    done
}

# Check one RUN instruction's shell-form body.
check_run() {
    local at="$1" body="$2"
    local words word index=0 handled=0 expected branch="" where subcommand kind
    local args=() allowed=()
    expected="$(count_cargo "${body}")"
    # Word splitting without globbing: the words are only compared below.
    set -f
    # shellcheck disable=SC2206
    words=(${body})
    set +f
    while [ "${expected}" -gt 0 ] && [ "${index}" -lt "${#words[@]}" ]; do
        word="${words[index]}"
        index=$((index + 1))
        # The `case` branch an invocation sits in, such as the cook's `worker)`.
        if [[ "${word}" =~ ^([A-Za-z0-9_.-]+)\)$ ]]; then
            branch="${BASH_REMATCH[1]}"
            continue
        fi
        if [ "${word}" != cargo ] && [[ "${word}" != */cargo ]]; then
            continue
        fi
        handled=$((handled + 1))
        where="${at}${branch:+, ${branch}})"
        subcommand="${words[index]:-}"
        case "${subcommand}" in
            build)
                kind="cargo build"
                index=$((index + 1))
                ;;
            chef)
                case "${words[index + 1]:-}" in
                    cook)
                        kind="cargo chef cook"
                        index=$((index + 2))
                        ;;
                    # Writes the recipe; compiles nothing.
                    prepare) continue ;;
                    *)
                        fail "${where}: unrecognized cargo chef ${words[index + 1]:-}"
                        continue
                        ;;
                esac
                ;;
            # Installs a tool from its registry, outside the workspace graph.
            install)
                index=$((index + 1))
                check_install
                continue
                ;;
            --version | -V) continue ;;
            *)
                fail "${where}: unrecognized cargo ${subcommand}"
                continue
                ;;
        esac
        args=()
        while [ "${index}" -lt "${#words[@]}" ]; do
            word="${words[index]}"
            case "${word}" in
                '&&' | '||' | ';;' | ';' | '|' | ')') break ;;
                *';')
                    args+=("${word%;}")
                    break
                    ;;
            esac
            args+=("${word}")
            index=$((index + 1))
        done
        builds=$((builds + 1))
        check_invocation "${where} ${kind}" ${args[@]+"${args[@]}"}
    done
    # A word the shell expands that mentions cargo may run it, as in
    # `${CARGO:-cargo} build`, whatever the scan above made of it.
    for index in "${!words[@]}"; do
        word="${words[index]}"
        if [[ "${word,,}" == *cargo* ]] && [ -z "${allowed[index]:-}" ] \
            && ! literal "${word}" >/dev/null; then
            fail "${at}): cannot read ${word}, which may run cargo"
        fi
    done
    if [ "${handled}" -ne "${expected}" ]; then
        fail "${at}): cargo appears where this check cannot read it, such as inside" \
            "\$(...), backticks, a quoted command or after a separator with no space"
    fi
}

if grep -qiE '^[[:space:]]*#[[:space:]]*escape[[:space:]]*=' "${dockerfile}"; then
    fail "${shown}: cannot read a file with an escape parser directive"
fi

# Collected first, so a failure to read the file fails here under errexit.
parsed="$(instructions)"
builds=0
while IFS=$'\t' read -r line_number stage text; do
    [ -n "${line_number}" ] || continue
    [ "${stage}" != - ] || stage="no stage"
    at="${shown}:${line_number} (${stage}"
    # Leading whitespace, then the instruction keyword.
    text="${text#"${text%%[![:space:]]*}"}"
    keyword="${text%%[[:space:]]*}"
    keyword="${keyword^^}"
    if [ "${keyword}" = SHELL ]; then
        fail "${at}): cannot read RUN instructions under a SHELL instruction"
        continue
    fi
    if [ "${keyword}" != RUN ]; then
        if [ "$(count_cargo "${text}")" -gt 0 ]; then
            fail "${at}): cannot read cargo in a ${keyword} instruction"
        fi
        continue
    fi
    if [[ "${text}" == *"<<"* ]]; then
        fail "${at}): cannot read a RUN with a heredoc"
        continue
    fi
    # The RUN keyword and its options, such as --mount.
    body="${text:3}"
    while [[ "${body}" =~ ^[[:space:]]*--[^[:space:]]*(.*)$ ]]; do
        body="${BASH_REMATCH[1]}"
    done
    if [[ "${body}" =~ ^[[:space:]]*\[ ]]; then
        fail "${at}): cannot read a RUN in exec form"
        continue
    fi
    check_run "${at}" "${body}"
done < <(printf '%s\n' "${parsed}")

if [ "${builds}" -eq 0 ]; then
    fail "${shown}: found no cargo build or cargo chef cook invocation to check"
fi
if [ "${failures}" -ne 0 ]; then
    echo "${failures} failure(s) in ${shown}; ${checked} release build selection(s) resolved." >&2
    exit 1
fi
echo "${checked} release build selection(s) in ${shown} enable no test-only features."
