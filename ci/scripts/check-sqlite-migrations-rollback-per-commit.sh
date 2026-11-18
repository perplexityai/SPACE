#!/usr/bin/env bash
# Run the SQLite rollback guard once per first-parent commit.
#
# One push to main can land a whole merge-queue batch, as
# check-migration-isolation-per-commit.sh explains. The rollback guard judges
# a change against the release before it: across a batch, a table one PR
# creates and the next drops with a marker shows no drop, and that marker
# then fails as stale. So each landed commit is checked against its first
# parent, with that commit's copy of the guard, in its own detached worktree.
# A commit from before the guard existed is skipped.
set -euo pipefail

if (($# != 1)); then
    echo "usage: $0 <base-commit>" >&2
    exit 2
fi

cd "$(git rev-parse --show-toplevel)"

guard=scripts/check-sqlite-migrations-rollback.sh
base="$(git rev-parse --verify "$1^{commit}")"
# Collected first: a walk that fails must fail the check, not read as fewer
# commits.
walk="$(git rev-list --first-parent --reverse "${base}..HEAD")"
mapfile -t commits < <(printf '%s' "${walk}" | sed '/^$/d')

if ((${#commits[@]} == 0)); then
    echo "No commits after ${base}; SQLite rollback check passed."
    exit 0
fi

worktree_root="$(mktemp -d)"
cleanup() {
    rm -rf -- "${worktree_root}"
    git worktree prune
}
trap cleanup EXIT

failed=()
for commit in "${commits[@]}"; do
    if ! parent="$(git rev-parse --verify --quiet "${commit}^1")"; then
        echo "ERROR: ${commit} has no parent to compare against." >&2
        exit 1
    fi

    echo "Checking $(git log -1 --format='%h %s' "${commit}")"
    # Skipped only when the commit's tree reads and lacks the guard.
    if ! entry="$(git ls-tree "${commit}" -- "${guard}")"; then
        echo "ERROR: can't read the tree of ${commit} to find ${guard}." >&2
        exit 1
    fi
    if [[ -z "${entry}" ]]; then
        echo "  ${guard} doesn't exist at this commit; skipped."
        continue
    fi
    worktree="${worktree_root}/${commit}"
    git worktree add --quiet --detach "${worktree}" "${commit}"
    if ! MIGRATION_SQLITE_ROLLBACK_BASE_REF="${parent}" bash "${worktree}/${guard}"; then
        failed+=("${commit}")
    fi
    git worktree remove --force "${worktree}"
done

if ((${#failed[@]} > 0)); then
    echo "ERROR: the SQLite rollback guard failed for ${#failed[@]} of ${#commits[@]} commit(s):" >&2
    for commit in "${failed[@]}"; do
        echo "  $(git log -1 --format='%h %s' "${commit}")" >&2
    done
    exit 1
fi

echo "The SQLite rollback guard passed for each of ${#commits[@]} commit(s) after ${base}."
