#!/usr/bin/env bash
# Fail when a release build in docker/Dockerfile.rust-build-common resolves a
# test-only Cargo feature (`test-hooks` or `test-support`) on any crate.
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
# covered without editing this script, and an invocation it cannot read fails
# the check instead of being skipped.
#
# `cargo chef cook` runs `cargo build` with the same selection on a skeleton of
# the same manifests and lockfile, so it resolves the same features. `--bin`
# and `--release` do not change feature resolution and are dropped. Without a
# `--target`, the graph is resolved for every target, a superset of the host
# the image builds on.
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

# One line per instruction, as "<first line number><TAB><stage><TAB><text>",
# with backslash continuations joined. Docker drops comment lines, including
# those inside a continued instruction, so they are dropped here too.
instructions() {
    awk '
        /^[[:space:]]*#/ { next }
        {
            line = $0
            if (!start) start = NR
            if (sub(/\\[[:space:]]*$/, "", line)) { text = text line " "; next }
            text = text line
            if (toupper(text) ~ /^[[:space:]]*FROM[[:space:]]/) {
                n = split(text, words, /[[:space:]]+/)
                for (i = 1; i < n; i++) if (toupper(words[i]) == "AS") stage = words[i + 1]
            }
            if (text ~ /[^[:space:]]/) print start "\t" stage "\t" text
            text = ""; start = 0
        }
    ' "${dockerfile}"
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
    local graph
    local leaked=()
    echo "${where}:"
    echo "  cargo tree --locked -e normal,build $*"
    # Each package with the features it resolved to, one per line. Not the
    # `-e features` graph: it leaves out the features a selected package's own
    # `[features]` table turns on, so `-p x -F x/test-hooks` would read clean.
    # Assigned under errexit so a cargo error fails the check rather than
    # reading as an empty, clean graph.
    graph="$(cd "${workspace}" && cargo tree --locked -e normal,build --prefix none \
        --format $'{p}\t{f}' "$@")"
    # A blank line separates the trees of several roots.
    if [ -z "${graph}" ] || awk -F '\t' 'NF == 1 { bad = 1 } END { exit !bad }' <<<"${graph}"; then
        fail "${where}: cargo tree did not list each package's features"
        return
    fi
    mapfile -t leaked < <(awk -F '\t' '{
        sub(/ \(\*\)$/, "", $2)
        split($1, package, " ")
        n = split($2, features, ",")
        for (i = 1; i <= n; i++)
            if (features[i] == "test-hooks" || features[i] == "test-support")
                print package[1] " feature \"" features[i] "\""
    }' <<<"${graph}" | sort -u)
    checked=$((checked + 1))
    if [ "${#leaked[@]}" -gt 0 ]; then
        fail "${where} enables test-only features:"
        printf '    %s\n' "${leaked[@]}" >&2
    else
        echo "  ok: no test-only features"
    fi
}

# Read the arguments of one `cargo build` or `cargo chef cook` invocation into
# `cargo tree` selection arguments, or fail naming what it could not read.
check_invocation() {
    local where="$1"
    shift
    local selection=() has_target=false arg value
    while [ "$#" -gt 0 ]; do
        arg="$1"
        shift
        # A whole quoted word, such as a quoted feature list, is read as its
        # contents. Anything else that needs the shell to expand it is not
        # something this check can resolve.
        if [[ "${arg}" =~ ^\"([^\"\'\$\`\\]*)\"$ || "${arg}" =~ ^\'([^\']*)\'$ ]]; then
            arg="${BASH_REMATCH[1]}"
        fi
        if [[ "${arg}" == *[\"\'\$\`\\\*\?]* ]]; then
            fail "${where}: cannot read argument ${arg}"
            return
        fi
        case "${arg}" in
            --locked | --frozen | --offline | --release | -v | --verbose | --timings) ;;
            --bin | --profile | --recipe-path | -j | --jobs)
                [ "$#" -gt 0 ] || { fail "${where}: ${arg} has no value"; return; }
                shift
                ;;
            --bin=* | --profile=* | --recipe-path=* | --jobs=*) ;;
            --bins | --lib) ;;
            --workspace | --all | --all-features | --no-default-features)
                selection+=("${arg}")
                ;;
            -p | --package | -F | --features | --exclude | --target)
                [ "$#" -gt 0 ] || { fail "${where}: ${arg} has no value"; return; }
                value="$1"
                shift
                if [[ "${value}" =~ ^\"([^\"\'\$\`\\]*)\"$ ]]; then
                    value="${BASH_REMATCH[1]}"
                fi
                if [[ -z "${value}" || "${value}" == -* || "${value}" == *[\"\'\$\`\\\*\?]* ]]; then
                    fail "${where}: cannot read ${arg} ${value}"
                    return
                fi
                [ "${arg}" != --target ] || has_target=true
                selection+=("${arg}" "${value}")
                ;;
            --package=* | --features=* | --exclude=* | --target=*)
                [[ "${arg}" != --target=* ]] || has_target=true
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

builds=0
while IFS=$'\t' read -r line_number stage text; do
    if ! [[ "$(tr '[:lower:]' '[:upper:]' <<<"${text}")" =~ ^[[:space:]]*RUN[[:space:]] ]]; then
        continue
    fi
    if [[ "${text}" != *cargo* ]]; then
        continue
    fi
    if [[ "${text}" == *"<<"* ]]; then
        fail "${shown}:${line_number} (${stage:-no stage}): cannot read a RUN heredoc that mentions cargo"
        continue
    fi
    read -ra words <<<"${text}"
    index=0
    # The `case` branch an invocation sits in, such as the cook's `worker)`.
    branch=""
    while [ "${index}" -lt "${#words[@]}" ]; do
        word="${words[index]}"
        index=$((index + 1))
        if [[ "${word}" =~ ^([A-Za-z0-9_.-]+)\)$ ]]; then
            branch="${BASH_REMATCH[1]}"
            continue
        fi
        if [ "${word}" != cargo ] && [[ "${word}" != */cargo ]]; then
            continue
        fi
        where="${shown}:${line_number} (${stage:-no stage}${branch:+, ${branch}})"
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
                    prepare)
                        continue
                        ;;
                    *)
                        fail "${where}: unrecognized cargo chef ${words[index + 1]:-}"
                        continue
                        ;;
                esac
                ;;
            # Installs a tool from its registry, outside the workspace graph.
            install)
                if [[ " ${words[*]:index} " == *" --path "* ]]; then
                    fail "${where}: cannot read cargo install --path"
                fi
                continue
                ;;
            --version | -V)
                continue
                ;;
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
done < <(instructions)

if [ "${builds}" -eq 0 ]; then
    fail "${shown}: found no cargo build or cargo chef cook invocation to check"
fi
if [ "${failures}" -ne 0 ]; then
    echo "${failures} failure(s) in ${shown}; ${checked} release build selection(s) resolved." >&2
    exit 1
fi
echo "${checked} release build selection(s) in ${shown} enable no test-only features."
