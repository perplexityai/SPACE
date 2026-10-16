# archcheck

`archcheck` checks the repository structure rules in
[`ARCHITECTURE.toml`](../../../ARCHITECTURE.toml). Existing violations a rule
tolerates for now are listed in
[`architecture-baseline.toml`](../../../architecture-baseline.toml).

```sh
cargo arch                             # human-readable report
cargo arch --format rdjson             # reviewdog Diagnostic Format, for CI
cargo arch --base-baseline <path>      # also compare with the base branch's baseline
cargo arch --base-architecture <path>  # ... and with its ARCHITECTURE.toml
cargo arch --write-baseline            # update the baseline's entries; never adds one
cargo arch --write-baseline=<table>    # update one table's entries, keeping the others
cargo arch --root <path>               # check another checkout
```

Without `--root`, it checks the Cargo workspace containing the current
directory. The workspace members are the ones `cargo metadata --no-deps
--offline` reports, run from the root: the root `Cargo.toml`'s `members`, and
every path dependency inside the workspace, which Cargo makes a member too. If
`cargo metadata` fails, archcheck exits 2. Exit status: 0 without required
findings, 1 with one, 2 for a configuration, usage or I/O error.

## What is checked

- **Production Rust file:** a `.rs` file under `<member>/src/` of a workspace
  member, except `tests.rs`, `*_tests.rs` and
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
table is read here, and other tables and keys are ignored, so a pull request
can change the baseline's schema. If the copy can't be read, archcheck exits 2.
If it isn't TOML, or its `[file_size]` isn't a table of integers, both
behaviours are skipped with a warning. Without the flag (the local default)
they are skipped too. Either way, one line after the summary line says so: on
stdout for `human`, on stderr for `rdjson`.

`pattern_ratchet` is a lint: it counts, per production file, the code lines
that mention a name, and ratchets those counts. It has any number of named
instances, each with its own patterns, allowed paths, modes and baseline
table:

```toml
[rules.pattern_ratchet.<instance>]
patterns = ['\bsome_name\b']     # regexes, matched against each code line
allow = ["crates/x/src/owner.rs", "crates/x/src/owner/**"]
paths = ["crates/x/**"]          # optional: the files checked; all when absent

[rules.pattern_ratchet.<instance>.no_crossing]
mode = "advisory"
# ...and no_new_entries, no_raised_caps, listed_no_growth and stale_entries,
# as for file_size
```

It counts mentions, not uses. A code line matches if any pattern matches
anywhere in it, so a string literal, an error message, a log line or a
trailing comment that only names an identifier counts the same as code that
uses it; several matches on one line count once. A name built at run time,
such as with `format!`, does not match, and only production `.rs` files are
read, so text in other files under `src/`, such as SQL loaded with
`include_str!`, is not counted either; moving a match into such a file shows
up as a `stale_entries` finding on the file it left, not a `no_crossing` one.
Nor are files outside workspace members' `src/` read: for example,
`scripts/admin/prune_cluster_artifacts.py` and
`scripts/benchmarks/admin-node-inventory.sql` name `direct_tables`' tables
today, and no behaviour sees them. Use `\b` around a
name so that longer identifiers containing it do not match. Patterns are
case-sensitive unless they use an inline `(?i)` flag. `allow` globs match
repository-relative paths: `*` and `?` stay within one path component and
`**` spans any number. Files they match are never counted.
`paths`, when given, takes globs of the same form and limits the instance to
the production files they match, for a name that other crates use for their
own purposes; files outside them are never counted, and an entry for one is
stale. An empty `paths` would check nothing and exits 2; omit it to check
every production file. An empty `allow` is fine.

- `no_crossing`: a file outside the allowed paths and the instance's baseline
  table has a matching line;
- `no_new_entries`: the instance's table has an entry that the base branch's
  copy of that table lacks (a renamed file counts as added);
- `no_raised_caps`: an entry's cap is higher than in the base branch's copy
  of the table;
- `listed_no_growth`: a listed file has more matching lines than its cap;
- `stale_entries`: an entry can be lowered or removed, because its file has
  fewer matching lines, none, is now allowed, or no longer exists.

`no_new_entries` and `no_raised_caps` compare with the base branch's copy as
`file_size`'s do. Each instance reads only its own `[pattern_ratchet.<instance>]`
table from that copy, leniently: a table for an instance that is not
configured, such as one this pull request renamed or removed, is ignored, with
a line after the summary (`base baseline has [pattern_ratchet.<instance>],
which is not configured; skipped`) so that the removal shows in the log; a
missing table has no entries, so every entry of the instance's own table is
new. If the instance's table, or `pattern_ratchet` itself, isn't a table of
integers, that instance's two behaviours are skipped with a warning, and the
other tables are still compared. If the copy isn't TOML at all, one warning
line names every comparison skipped, `file_size`'s and each instance's.
Without `--base-baseline`, they are skipped, one line per instance after the
summary line. Loosening an instance in
`ARCHITECTURE.toml`, by widening `allow` or narrowing `patterns`, is not
reported by any behaviour: review such changes.

An instance name is lowercase ASCII letters, digits and underscores, starting
with a letter. Adding an instance takes configuration and a baseline table, no
code changes:

1. Add its tables to `ARCHITECTURE.toml`, with a comment saying why.
2. Run `cargo arch --write-baseline=pattern_ratchet.<instance>`. The baseline
   has no table for the instance, so it is left alone, and the command lists
   every file outside the allowed paths that matches today as `not added`,
   followed by those entries as TOML.
3. Add the table to `architecture-baseline.toml` by hand: paste the listed
   entries, or, if no file matches, add an empty `[pattern_ratchet.<instance>]`
   or none at all. Review each entry.
4. Commit both files. On the pull request, `no_new_entries` reports each
   entry, since the base branch's copy has no table for the instance: that is
   expected for a deliberate new instance.

## Crate names

`crate_names` keeps new crates from getting catch-all names. For each
workspace member, it lowercases the package name and the library target's
name (from `[lib] name`, or Cargo's default from the package name) and splits
them on `-` and `_`. The crate matches if a part of either is in
`banned_tokens`: `common`, `shared`, `util`, `utils`, `helper`, `helpers`,
`misc` and `core`. Only whole parts match, so `pplx-space-common-io`,
`foo_core` and a library named `utils` do, and `database`, `coreutils` and
`encore` don't. An empty token list, or a token that is empty, uppercase or
contains `-` or `_`, can't match anything and exits 2.

`allow` lists exact package names exempt from the rule, such as a
`pplx-space-shared-memory` whose `shared` is not a catch-all. It starts empty.
Its names must be lowercase and listed once, and a name both allowed and in the
baseline exits 2.

The baseline's `[crate_names]` table maps each tolerated crate's package name
to its member path. An entry covers only the crate at that path: a crate with a
listed name at another path is reported, until the entry's path is updated,
so a listed crate that moves needs a one-line baseline edit, and a removed
crate's name can't be reused elsewhere.

- `no_new` (required): a matching crate the baseline doesn't list at its path,
  shown on the `name =` line of its `Cargo.toml` (or the `[lib] name` line, if
  only the library's name matches); and an entry a pull request may not list,
  shown on the entry's line, so it can't list its own new crate. With
  `--base-baseline`, entries are compared with the base branch's
  `[crate_names]`: an entry whose name it lacks is reported, and updating the
  path of an existing entry is not. Without a copy of that table, because the
  base predates the rule or wasn't given, only the 16 crates that matched when
  the rule was added (`BOOTSTRAP` in `src/rules/crate_names.rs`) may be
  listed. A table that doesn't parse counts as empty, with a warning, as does
  a copy that isn't TOML at all, under the one warning for that copy.
- `stale_entries` (advisory): an entry whose crate is gone or renamed, or no
  longer matches; an `allow` name that no workspace member has, or that
  doesn't match.
- `policy_changed` (advisory): every difference in `[rules.crate_names]` from
  the base branch's `ARCHITECTURE.toml`, given with `--base-architecture`: a
  token added or removed, an `allow` name added or removed, a mode changed,
  or the whole table new. Each is shown on its line in `ARCHITECTURE.toml`, so
  a loosened policy is visible in review. That copy is read leniently: other
  tables and unknown keys are ignored, and one that can't be parsed skips the
  behaviour with a warning. Without the flag, it is skipped too, and one line
  after the summary says so.

`--write-baseline` keeps each entry whose crate still matches at the entry's
path, removes the other entries, and never adds one: it prints `not added: …`
for each matching crate without an entry, and `not updated: …` for an entry
whose crate is at another path, which it leaves for a deliberate edit. Unlike
for the other tables, it lists no entries to paste: a hand-added entry fails
`no_new`. It always writes the `[crate_names]` table, even when empty, after
the other tables; `--write-baseline=crate_names` updates it alone.

Module names aren't checked. Catch-all modules haven't been appearing in the
history, a module's reach is limited to its crate, `file_size` already bounds
how much one can hold, and a check on module names is easy to evade (`util.rs`
becomes `support.rs`) without changing what the module holds.

## Dependency fences

`dependency_fence` keeps packages out of workspace members that must not
depend on them. Each `[rules.dependency_fence.<instance>]` lists Cargo
`packages`, the `members` it checks and the members it `allow`s, as globs over
member directories:

```toml
[rules.dependency_fence.sbxmgr_rusqlite]
packages = ["libsqlite3-sys", "rusqlite"]
members = ["crates/libs/sbx/mgr/**", "crates/apps/sbx/**"]
allow = ["crates/libs/sbx/mgr/store"]

[rules.dependency_fence.sbxmgr_rusqlite.no_crossing]
mode = "required"
```

`no_crossing` reports each dependency a checked member outside `allow`
declares on a listed package: normal, dev or build, for any target, and under
any name. The records come from `cargo metadata`, which names the depended-on
package even when `package = "…"` renames it, so a fence does not depend on
how a manifest is written. Each finding points at the manifest line that
declares the dependency. There is no baseline: an existing dependency is
removed or its member allowed. An empty `packages` or `members`, or a
`members` or `allow` glob that matches no workspace member, exits 2.

## Configuration and modes

Each behaviour has a `mode`: `advisory` findings are reported, `required` ones
also fail the check, so behaviours can graduate one at a time. Unknown keys,
sections or modes, a missing file, bad TOML, an invalid regex, glob or instance
name, an empty `patterns`, `paths`, `packages` or `members` list, a baseline
table for an instance that is not configured, and a failing `cargo metadata`
exit 2. So does a `--base-baseline`
or `--base-architecture` that can't be read, or one combined with
`--write-baseline`.

## Baseline

Each table maps a repository-relative path to its cap, the count it had when
listed: under `[file_size]`, each production file over the limit, with its
code-line count; under `[pattern_ratchet.<instance>]`, each production file
outside the instance's allowed paths with a matching line, with its count of
matching lines. `[crate_names]`, written last, maps a package name to its
member path instead (see [Crate names](#crate-names)). Entries should only be
removed or lowered.

`cargo arch --write-baseline` rewrites `architecture-baseline.toml` from its
entries and the tree. An entry still in violation gets its current count as
its cap: under `[file_size]`, a file still over the limit; under
`[pattern_ratchet.<instance>]`, a file outside the allowed paths that still has
a matching line. The other entries are removed. It never adds an entry, or a
`[pattern_ratchet.<instance>]` table the file lacks (`[file_size]` and
`[crate_names]` are always written, empty if the file had none; for
`[crate_names]`, see [Crate names](#crate-names)). It prints `not added: <path> (N code lines); …` for each
file over the limit without an entry instead, and `not added: <path> (N code
lines matching the <instance> patterns); …` for each matching file without
one, then, after its summary line, the entries they would need, as TOML, one
block per table, each after a comment saying where it goes. For a table the
file has, the block has no header and its comment says to add the entries
inside that table, below its header: pasted at the end of the file, they would
land in whichever table comes last. For a table the file lacks, the block
starts with the table's header and goes at the end of the file. Fix the file,
or, for a deliberate exception, add the entry by hand, inside its table, which
`no_new_entries` then reports. Raised
caps are written too, and `no_raised_caps` reports each of them. It reads the
existing file with the strict parser, so a missing or invalid baseline exits 2:
it can't create one from scratch.

`cargo arch --write-baseline=<table>`, with `<table>` `file_size`,
`crate_names` or `pattern_ratchet.<instance>` for a configured instance, does
the same for that
table alone and keeps every other table as the existing baseline has it,
printing only that table's `not added` lines and entries. Use it when adding
or changing one rule, so that other tables are left alone. If the file has no
`[pattern_ratchet.<instance>]` table for it, the table is not created and the
file is left alone (see adding an
instance, above). Other tables come out byte-identical if the file is in
generated form (as written by archcheck); hand-added comments, quoting or key
order are normalized. As for the unscoped form, the existing baseline must
exist and pass the checks `cargo arch` makes, including that every
`[pattern_ratchet.<instance>]` table names a configured instance; otherwise, as
for an unknown table, it exits 2 and leaves the file alone. The table is only
taken after `=`: `--write-baseline` alone is always the unscoped write, and an
argument after it that is not a flag is a usage error.

The inventory walks the filesystem, so untracked `.rs` files under `src/` are
counted too: check `git status` before `--write-baseline`.

## In CI

On pull requests, the Rust clippy job in
[`rust-static-ci.yml`](../../../.github/workflows/rust-static-ci.yml) runs
`cargo arch --format rdjson --base-baseline <file> --base-architecture <file>`
and passes the findings to [reviewdog](https://github.com/reviewdog/reviewdog)
with `-reporter=github-pr-annotations`. The files are the baseline and
`ARCHITECTURE.toml` of the synthetic merge's first parent, the base branch, or
an empty file for one the base lacks. If the base can't be resolved, the step
warns and runs without either flag: the `no_new_entries` and `no_raised_caps`
behaviours and `crate_names.policy_changed` are skipped, and
`crate_names.no_new` allows only the bootstrap entries. Each finding is a
diagnostic with code `<rule>.<behaviour>`, or `<rule>.<instance>.<behaviour>`
for a rule with instances, on line 1 of its file, with the path exactly as it
is. A new catch-all crate points at the `name =` line of its `Cargo.toml`
instead, and a new entry, a raised cap, a stale entry for a deleted file and a
stale `[crate_names]` entry at the entry's line in the baseline.
`pattern_ratchet`'s `no_crossing`, `listed_no_growth` and shrank
`stale_entries` findings point at the file's first matching line; one for a
file with no matches left, at its line 1. Required findings are `ERROR`,
advisory ones `WARNING`.

- `-filter-mode=file` reports findings in the files the pull request changes,
  so one on a line of the baseline shows only when the baseline changes.
- `-filter-mode=nofilter` reports every finding when the pull request changes
  `ARCHITECTURE.toml`, the baseline, the root `Cargo.toml` or anything under
  `crates/tools/archcheck/` (by old or new name), or when its file list can't
  be read in full.
- reviewdog fails once it has written 10 annotations, whatever their severity.
  So advisory findings get one run that never fails the step, and required
  findings a second run with `-fail-level=error` that does.
- GitHub shows a step's first 10 warning annotations, and the report lists
  every `file_size` finding first. So
  [`order-archcheck-findings.sh`](../../../ci/scripts/order-archcheck-findings.sh)
  hands the advisory run the pull request's own findings first, then the
  rest. First come all `no_new_entries` and `no_raised_caps` findings: they
  compare with the base's copy by table and key, so they include the entries
  of a renamed or split pattern table even though those lines are unchanged.
  `crate_names.policy_changed` findings, which compare with the base's
  `ARCHITECTURE.toml`, come with them, although the report lists them last.
  Then come findings in the files the pull request changes (old or new name),
  and stale entries in the baseline for files it changes, such as files it
  deletes, or, in `[crate_names]`, for a crate whose `Cargo.toml` it changes,
  at the entry's member path. Each group keeps report order. With `file`, this
  changes nothing that is shown. Required findings are annotated from the
  report as it is, in their own run. The step logs how many own advisory
  findings reviewdog will show under the step's filter mode (with `file`,
  none in a file renamed without edits); with more than 10, it says in the
  step summary that at most 10 are annotated. Annotations are not the full
  inventory: every finding is in the step's log and its rdjson output, or run
  `cargo arch`.

A new catch-all crate's finding is on its own `Cargo.toml`, a file the pull
request adds, so it is annotated in either mode and its `ERROR` fails the job.
So is a renamed library's, whose manifest the pull request edits. Moving a
listed crate changes the root `Cargo.toml`'s `members`, where every listed
crate is today, and a self-listed entry or a changed `allow` changes the
baseline or `ARCHITECTURE.toml`: each switches to `nofilter`. A listed crate
that is only a path dependency, moved without editing its manifest, would not
be annotated with `file`; `tests/repository.rs` still fails on it.

Only annotations are filtered. Once a behaviour is `required`,
`tests/repository.rs` fails on any required finding anywhere in the tree.

## Adding a rule

A new instance of `pattern_ratchet` is configuration, not a new rule (see
above). For a new kind of check:

1. Add a module under `src/rules/`. A rule that caps a count per file in a
   baseline table, as both rules do, implements `CountRule`
   (`src/rules/ratchet.rs`): one `CountRatchet` per table, whose `measure`
   gives a violating file's count and the line findings about it point at,
   and whose wording the findings and `not added` lines are built from. The
   five behaviours, finding locations, the comparisons with the base
   branch's copy, the skip lines, and `--write-baseline` (update or remove,
   never add) come with it. Any other check implements `Rule`; if it has a
   baseline table, it also implements `Rule::tables`, without which the
   baseline's table is rejected as naming no instance, and
   `Rule::update_baseline`, which never adds an entry; if it compares with
   the base branch's copy, `Rule::baseline_rule`, `Rule::skipped` and
   `Rule::base_comparisons` too: without `Rule::baseline_rule`, a table
   `Baseline::load_base` does not read gets skip lines, not exit 2.
2. Add its configuration to `RulesConfig` in `src/config.rs`, with one
   `BehaviourConfig` per behaviour, and its table to `ARCHITECTURE.toml`.
3. For a rule with a baseline table, in `src/baseline.rs`:
   - add a `#[serde(default)]` field to `BaselineFile`, read into the tables
     in `Baseline::from_file` under the rule's name (`BaselineFile` denies
     unknown fields, so a table it does not know makes the next check exit
     2);
   - read the rule's own key from the parsed document in
     `Baseline::load_base`, on its own, as for `pattern_ratchet`, and record
     the rule as read; a registered rule that is not read makes every
     `--base-baseline` run whose base copy is TOML exit 2. Do not add a
     field to `BaseBaselineFile`, which reads `[file_size]` alone: a second
     field there would let one table that does not parse skip both rules'
     comparisons;
   - describe the table in the generated header, `HEADER`.
4. Register it in `rules::registered()` and add test cases to `tests/cli.rs`.
