# archcheck

`archcheck` checks the repository structure rules in
[`ARCHITECTURE.toml`](../../../ARCHITECTURE.toml). Existing violations a rule
tolerates for now are listed in
[`architecture-baseline.toml`](../../../architecture-baseline.toml).

```sh
cargo arch                    # human-readable report
cargo arch --format rdjson    # reviewdog Diagnostic Format, for CI
cargo arch --write-baseline   # regenerate the baseline
cargo arch --root <path>      # check another checkout
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

`file_size` limits production files to `limit` code lines, with three
behaviours:

- `no_crossing`: a file outside the baseline is over the limit;
- `listed_no_growth`: a listed file grew past its cap;
- `stale_entries`: an entry can be lowered or removed, because its file shrank,
  is at or under the limit, or no longer exists (or the key names no
  production file).

## Configuration and modes

Each behaviour has a `mode`: `advisory` findings are reported, `required` ones
also fail the check, so behaviours can graduate one at a time. Unknown keys,
sections or modes, a missing file, bad TOML, and `members` with globs or with
empty, `.` or `..` components exit 2.

## Baseline

`cargo arch --write-baseline` rewrites `architecture-baseline.toml` from the
tree, without reading the old file: each production file over the limit, with
its current count as its cap. Entries should only be removed or lowered. The
inventory walks the filesystem, so untracked `.rs` files under `src/` are
counted too: check `git status` before `--write-baseline`.

## In CI

On pull requests, the Rust clippy job in
[`rust-static-ci.yml`](../../../.github/workflows/rust-static-ci.yml) runs
`cargo arch --format rdjson` and passes the findings to
[reviewdog](https://github.com/reviewdog/reviewdog) with
`-reporter=github-pr-annotations`. Each finding is a diagnostic with code
`<rule>.<behaviour>` on line 1 of its file, with the path exactly as it is; a
stale entry for a deleted file points at its line in the baseline. Required
findings are `ERROR`, advisory ones `WARNING`.

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
   implement `Rule::update_baseline`.
4. Register it in `rules::registered()` and add test cases to `tests/cli.rs`.
