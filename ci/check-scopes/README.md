# Check scopes

Pull CI and the merge queue run a check only when the change touches
something it reads. `ci/scripts/check-needed.sh` decides, inside each job, so
no job waits for a separate selection job; a check it does not need skips its
remaining steps and passes. Main CI runs every check.

Each check names one scope here:

- `<scope>.skip` lists the paths the check never reads. The check skips only
  when every changed path matches one of them. A path missing from the list
  makes the check run, so an omission costs time, never coverage.
- `<scope>.run` lists the only paths the check reads. The check runs when a
  changed path matches one of them. Use it only for a check whose inputs are
  self-contained.
- `always.txt` lists paths that make every check run: the workflows, CI
  scripts and the CI image, and these files.

Patterns are shell globs matched against the whole repository path, where
`*` also matches `/`. A line starting with `!` re-includes paths an earlier
line matched; the last matching line wins.

Add an entry only for a path the check provably does not read: name the
code or test that would read it, or confirm that none does.
