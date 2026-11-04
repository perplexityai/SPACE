# Check scopes

Pull CI and the merge queue run a check only when the change touches
something it reads. `ci/scripts/check-needed.sh` decides inside each job,
right after the checkout, so no job waits for a separate selection job; a
check it does not need skips its remaining steps and passes, and its log
says why. Main CI passes no base and runs every check, except archcheck,
which runs inside Rust clippy only for pull requests and merge groups.

What counts as the change:

- **Pull CI:** GitHub's synthetic merge against its first parent, the base
  the run tests. The Kind selector, the migration guards, archcheck and the
  reuse record (`ci/scripts/pull-ci-reuse.py`) use the same base.
- **Merge queue:** the entry's merge group against its `base_sha`. Each
  entry's group is based on the entry ahead of it, so a check skips when this
  entry's own change touches none of its inputs. That is safe while the merge
  queue requires every entry's group to pass, as it does today; requiring
  only the head of a group would let an entry ahead merge without its checks.

## Files

- `<scope>.skip` lists the paths the check never reads. The check skips only
  when every changed path matches one of them. A path missing from the list
  makes the check run, so an omission costs time, never coverage.
- `<scope>.run` lists the only paths the check reads, and the check runs when
  a changed path matches one of them. Use it only for a check whose inputs
  are self-contained: today, the Go shim.
- `run-all.txt` lists paths that make every check run: the workflows, the CI
  scripts, these files and the CI image's inputs.
- The scope `always`, used by the two documentation jobs, has no file: those
  checks run for every change.

A scope file without patterns, or a missing `run-all.txt`, fails the check
instead of skipping it, and anything else the script cannot decide runs it.

Patterns are shell globs matched against the whole repository path, where
`*` also matches `/`. A line starting with `!` re-includes paths an earlier
line matched; the last matching line wins. In `ci/kind-pull-paths.txt`, `*`
stays within a directory instead, so write directory entries as `dir/**`,
which means the same in both lists.

## Adding an entry

Add a `.skip` entry only for a path the check provably does not read: name
the code or test that would read it, or confirm that none does. For
`rust.skip`, also confirm that archcheck does not read the path, because
Main CI never runs archcheck to catch a wrong entry.

## Wiring a new check

- In a matrix job of `rust-static-ci.yml` or `common-ci.yml`, give the entry
  a `scope:`; the job's decision step reads it.
- In a job of its own, add a step with `id: scope` right after the checkout,
  as the other jobs do, naming its scope.
- Gate every later step on `steps.scope.outputs.needed == 'true'`, inside any
  existing `always()`, `failure()` or `!cancelled()` condition. The steps that
  `common-ci.yml` shares through YAML anchors already carry that gate, so a
  job that uses one needs its `scope` step.
