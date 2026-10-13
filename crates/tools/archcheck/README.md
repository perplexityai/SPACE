# archcheck

`archcheck` checks the repository structure rules in
[`ARCHITECTURE.toml`](../../../ARCHITECTURE.toml). Existing violations a rule
tolerates for now are listed in
[`architecture-baseline.toml`](../../../architecture-baseline.toml).

```sh
cargo arch                         # human-readable report
cargo arch --format rdjson         # reviewdog Diagnostic Format, for CI
cargo arch --base-baseline <path>  # also compare with the base branch's baseline
cargo arch --write-baseline        # update the baseline's entries; never adds one
cargo arch --root <path>           # check another checkout
```

Without `--root`, it checks the Cargo workspace containing the current
directory. Exit status: 0 without required findings, 1 with one, 2 for a
configuration, usage or I/O error.

## What is checked

- **Production Rust file:** a `.rs` file under `<member>/src/` of a workspace
  member in the root `Cargo.toml`, except `tests.rs`, `*_tests.rs` and
  anything below a `tests/` directory. Symlinks under `src/` are skipped; a
  member directory or `src` that is itself a symlink is followed. Other files
  may have non-UTF-8 names; a `.rs` file may not (exit 2).
- **Code line:** a line that is not blank and does not start with `//` after
  trimming. Lines end at LF or CRLF, as they do for rustc's `//` comments and
  line numbers, so a file with only CR line breaks is one line (`rustfmt
  --check` rejects such files). A leading byte order mark is ignored. Block
  comments and attributes count.

`file_size` limits production files to `limit` code lines, with five
behaviours:

- `no_crossing`: a file outside the baseline is over the limit;
- `no_new_entries`: the baseline has an entry that the base branch's copy
  lacks (a renamed file counts as added);
- `no_raised_caps`: an entry's cap is higher than in the base branch's copy;
- `listed_no_growth`: a listed file grew past its cap;
- `stale_entries`: an entry can be lowered or removed, because its file shrank,
  is at or under the limit, or no longer exists (or the key names no
  production file).

`no_new_entries` and `no_raised_caps` compare with the base branch's copy of
the baseline, given with `--base-baseline <path>`, so that the baseline is no
way around the limit.
Lowered caps and removed entries are silent. Only that copy's `[file_size]`
table is read, and other tables and keys are ignored, so a pull request can
change the baseline's schema. If the copy can't be read, archcheck exits 2.
If it isn't TOML, or its `[file_size]` isn't a table of integers, both
behaviours are skipped with a warning. Without the flag (the local default)
they are skipped too. Either way, one line after the summary line says so: on
stdout for `human`, on stderr for `rdjson`.

## Configuration and modes

Each behaviour has a `mode`: `advisory` findings are reported, `required` ones
also fail the check, so behaviours can graduate one at a time. Unknown keys,
sections or modes, a missing file, bad TOML, and `members` with globs or with
empty, `.` or `..` components exit 2. So does a `--base-baseline` that can't
be read, or one combined with `--write-baseline`.

## Baseline

`cargo arch --write-baseline` rewrites `architecture-baseline.toml` from its
entries and the tree: an entry whose file is still over the limit gets the
file's current count as its cap, and the other entries are removed. It never
adds an entry. It prints `not added: <path> (N code lines); …` for each file
over the limit without one instead: split the file, or, for a deliberate
exception, add the entry by hand, which `no_new_entries` then reports. Raised
caps are written too, and `no_raised_caps` reports each of them. It reads the
existing file with the strict parser, so a missing or invalid baseline exits 2:
it can't create one from scratch. Entries should only be removed or lowered.
The inventory walks
the filesystem, so untracked `.rs` files under `src/` are counted too: check
`git status` before `--write-baseline`.

## In CI

On pull requests, the Rust clippy job in
[`rust-static-ci.yml`](../../../.github/workflows/rust-static-ci.yml) runs
`cargo arch --format rdjson --base-baseline <file>` and passes the findings to
[reviewdog](https://github.com/reviewdog/reviewdog) with
`-reporter=github-pr-annotations`. The file is the baseline of the synthetic
merge's first parent, the base branch, or an empty file if the base has none.
If the base can't be resolved, the step warns and runs without the flag. Each
finding is a diagnostic with code `<rule>.<behaviour>` on line 1 of its file,
with the path exactly as it is; a new entry, a raised cap, and a stale entry
for a deleted file point at the entry's line in the baseline. Required findings are
`ERROR`, advisory ones `WARNING`.

- `-filter-mode=file` reports findings in the files the pull request changes.
- `-filter-mode=nofilter` reports every finding when the pull request changes
  `ARCHITECTURE.toml`, the baseline, the root `Cargo.toml` or anything under
  `crates/tools/archcheck/` (by old or new name), or when its file list can't
  be read in full.
- reviewdog fails once it has written 10 annotations, whatever their severity.
  So advisory findings get one run that never fails the step, and required
  findings a second run with `-fail-level=error` that does.

Only annotations are filtered. Once a behaviour is `required`,
`tests/repository.rs` fails on any required finding anywhere in the tree.

## Adding a rule

1. Add a module under `src/rules/` with a type that implements `Rule`.
2. Add its configuration to `RulesConfig` in `src/config.rs`, with one
   `BehaviourConfig` per behaviour, and its table to `ARCHITECTURE.toml`.
3. If it tolerates existing violations, add a table to `Baseline` and
   implement `Rule::update_baseline`, which never adds entries.
4. Register it in `rules::registered()` and add test cases to `tests/cli.rs`.
