#!/usr/bin/env bash
# Run archcheck on HEAD against a base commit, as CI does.
#
# run-archcheck.sh <base> <pull-request-head> [<rdjson>]
#   resolve-migration-base.sh resolves the base: for a pull request, pass an
#   empty <base> and the pull request's head, and the base is the first
#   parent of the synthetic merge once its second is that head; for a merge
#   group, pass its base_sha and an empty head. archcheck compares with the
#   base's architecture-baseline.toml and ARCHITECTURE.toml, an empty file
#   standing in for one the base lacks, and with the files git detects as
#   renamed since the base, so that a baseline entry moved with its file is
#   not a new one. With <rdjson>, the report goes there in reviewdog's
#   format; otherwise to stdout as text. Exits as cargo arch does: 1 for
#   required findings, 2 for an error. Run from the repository root.
#
# If the base can't be resolved, or a copy can't be read, a workflow warning
# names the comparisons skipped, and archcheck runs without them.
set -uo pipefail

candidate="${1-}"
pull_request_head="${2-}"
rdjson="${3-}"

mkdir -p target
base_args=() missing=()
# Passes the base copy of a file, or an empty file if the base has none.
base_copy() {
    local file="target/archcheck-base-$2"
    if ! git cat-file -e "${base}:$2" 2>/dev/null; then
        echo "archcheck: ${base} has no $2; comparing with an empty one"
        : >"${file}" && base_args+=("--$1" "${file}")
    elif git show "${base}:$2" >"${file}"; then
        echo "archcheck: comparing with $2 from ${base}"
        base_args+=("--$1" "${file}")
    else
        missing+=("$2")
    fi
}
if base="$(bash ci/scripts/resolve-migration-base.sh "${candidate}" "${pull_request_head}")"; then
    base_copy base-baseline architecture-baseline.toml
    # --renames needs --base-baseline, which a missing copy leaves out.
    if ((${#missing[@]} == 0)); then
        # Past -l files (diff.renameLimit by default), git skips inexact
        # rename detection with only a warning on stderr: allow 20000, about
        # 20 s for as many unrelated small files added and deleted, and make
        # a skip visible.
        if git diff -z --name-status --find-renames -l20000 --diff-filter=R "${base}" HEAD \
            >target/archcheck-renames 2>target/archcheck-renames.stderr; then
            cat target/archcheck-renames.stderr >&2
            base_args+=(--renames target/archcheck-renames)
            if grep -q "rename detection was skipped" target/archcheck-renames.stderr; then
                echo "::warning title=archcheck::git skipped rename detection since ${base} for too many files; no_new_entries counts an entry moved with its file as new."
            fi
        else
            cat target/archcheck-renames.stderr >&2
            echo "::warning title=archcheck::Could not list the renames since ${base}; no_new_entries counts an entry moved with its file as new."
        fi
    fi
    base_copy base-architecture ARCHITECTURE.toml
else
    missing=(architecture-baseline.toml ARCHITECTURE.toml)
fi
# Expanded as ${a[@]+"${a[@]}"}: before bash 4.4, set -u rejects an empty
# array's "${a[@]}".
for file in ${missing[@]+"${missing[@]}"}; do
    case "${file}" in
        architecture-baseline.toml) skipped="no_new_entries and no_raised_caps are skipped, and crate_names.no_new allows only the crates listed when it was added" ;;
        ARCHITECTURE.toml) skipped="crate_names.policy_changed, layers.policy_changed and the layers.placement check for new or changed unmoved entries are skipped, and a pattern_ratchet instance the base baseline has no table for counts as new" ;;
    esac
    echo "::warning title=archcheck::Could not read the base ${file}; ${skipped}."
done
if [[ -n "${rdjson}" ]]; then
    exec cargo arch --format rdjson ${base_args[@]+"${base_args[@]}"} >"${rdjson}"
fi
exec cargo arch ${base_args[@]+"${base_args[@]}"}
