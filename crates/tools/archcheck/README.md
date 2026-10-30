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
cargo arch --base-baseline <path> --renames <path>  # ... pairing renamed files with their entries
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
  lacks (a renamed file counts as added, unless `--renames` pairs it with
  its old path; see [Renamed files](#renamed-files));
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
include = ["crates/x/src/**"]    # optional: the files checked; all when absent
allow = ["crates/x/src/owner.rs", "crates/x/src/owner/**"]

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
`include`, when given, takes globs of the same form and limits the instance to
the production files they match, for a name that other crates use for their
own purposes; files outside them are never counted, and an entry for one is
stale. `allow` still exempts files inside that scope. An empty `include` would
check nothing and exits 2; omit it to check every production file. An empty
`allow` is fine.

- `no_crossing`: a file outside the allowed paths and the instance's baseline
  table has a matching line;
- `no_new_entries`: the instance's table has an entry that the base branch's
  copy of that table lacks (a renamed file counts as added, unless
  `--renames` pairs it with its old path);
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

### Renamed files

A file the pull request renames keeps its entry: under `[file_size]` and in
every `[pattern_ratchet.<instance>]` table, an entry whose path the base
branch's copy lacks is compared with the entry of the path it was renamed
from, so moving the entry to the new path is not a new entry, and at or
below the old cap not a raised one. Above it, `no_raised_caps` reports
`<new>: renamed from <old>, with cap raised from <old cap> to <cap> …`.

The renames are git's, given with `--renames <path>`: the output of `git
diff -z --name-status --find-renames <base> HEAD`, which CI passes (see
[In CI](#in-ci)). Only renames (`R` records) count: a copy keeps its source,
and other records, and renames of paths that are not UTF-8, are ignored.
Input that is not that output, such as the same diff without `-z`, output cut
off before its last NUL, or a path in two renames, exits 2, as does
`--renames` without `--base-baseline` or with `--write-baseline`. Without the
flag, a renamed file's entry counts as added.

To check a branch as CI does, before or after committing, compare the base
with the working tree, which includes renames staged with `git mv` (a file
moved without `git mv` or `git add` is untracked, and not seen):

```sh
git show main:architecture-baseline.toml >/tmp/base-baseline.toml
git diff -z --name-status --find-renames -l20000 main >/tmp/renames
cargo arch --base-baseline /tmp/base-baseline.toml --renames /tmp/renames
```

`-l20000` raises git's limit on inexact rename detection from
`diff.renameLimit`; past it, git skips the detection with only a warning on
stderr, and renamed entries count as new.

archcheck does not pair a gone entry with a new one whose cap fits, which
would let an unrelated new file over the limit take over a deleted file's
entry: git pairs a deleted file with an added one only when their contents
are similar (by default, half), so a file deleted and an unrelated one added
are still a new entry. A rename git does not detect, such as one that also
rewrites most of the file, is reported as new too.

A renamed file without an entry is reported as a `no_crossing` on its new
path, and an old entry left behind as stale. With `--renames`, the
`no_crossing` finding says so: `It was renamed from <old>: move that entry in
<table> to the new path, at or below <old>'s cap of <cap> in the base
branch's copy.` while the baseline lists the old path, or, once the entry is
gone, as after `--write-baseline`, `It was renamed from <old>: give it an
entry in <table> at or below <old>'s cap of <cap> in the base branch's
copy.` The cap is the one the new entry would be compared with: the new
path's own (`its cap`), if the base copy lists it, or the old path's. A file
that was not renamed gets no rename advice, nor does one renamed from a path
the baseline does not list when the base copy has no cap for it. Without
`--renames`, every `no_crossing` finding ends with `If the file was renamed,
move its entry in <table> to the new path; with --renames, no_new_entries
accepts the moved entry at or below its old cap.`

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
packages = ["libsqlite3-sys", "pplx-space-sqlite", "rusqlite"]
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

A fence reads only the checked members' own dependencies, not what those
dependencies depend on. For sbx-mgr, `run_sbxmgr_production_checks` in
`ci/scripts/run-rust-checks.sh` covers the transitive side: with `cargo
tree -i`, it asserts which crates may bring rusqlite and `pplx-space-sqlite`
into the server's production graph.

## Layers

`layers` keeps every workspace crate in a layer and its dependencies pointing
down the stack. The layers are directories under `crates/`, bottom-up:

| Layer | Holds |
|---|---|
| `l10_foundation` | generic building blocks that name nothing SPACE-specific |
| `l20_domain` | models (types and the rules that keep them valid) and protocols (wire schemas and conversions), without I/O |
| `l30_storage` | stores: the only crates that reach a database, private to the crates that own their state |
| `l40_api` | use cases and their entry points (handlers, loops, sweepers, jobs), and each service's `*-client` |
| `l50_app` | thin mains that compose API crates |
| `test_support` | test doubles and fixtures, reachable only through `[dev-dependencies]` |
| `tools` | repository tools, never shipped; nothing depends on them |

A crate in `crates/<layer>/` takes its layer from the directory. Below it, the
crate's path is its package name without `pplx-space-`, split on `-`:
`pplx-space-sbx-mgr-client` in `l40_api` is at `crates/l40_api/sbx/mgr/client`.
A crate may sit one directory below its name's path (a leaf without `-`); use
one where another crate's name in the layer would nest, such as `server` for a
service: `sbx-mgr` beside `sbx-mgr-client` is at
`crates/l40_api/sbx/mgr/server`. The crates that predate the layer
directories are listed by layer in `[rules.layers.unmoved]` until they move;
a new crate goes in a layer directory.

Normal and build dependencies follow this matrix; the checks below say what
each finding means and how to fix it.

| From | May depend on |
|---|---|
| Foundation | Foundation |
| Domain | Domain, Foundation |
| Storage | Domain, Foundation, and another store that lists it as a user |
| API | API, Domain, Foundation, and the stores that list it as a user; a `*-client` only Domain, Foundation and other clients |
| App | API, Domain, Foundation; never another app or a store |
| test_support, tools | anything but tools |

```toml
[rules.layers.unmoved]                 # crates outside the layer directories
l10_foundation = ["pplx-space-byte-size"]

[rules.layers.store_users]             # store → the crates that own its state
pplx-space-sandbox-store = ["pplx-space-api"]

[rules.layers.allowed_edges]           # edges the matrix forbids, allowed by name
pplx-space-db-migrator = ["pplx-space-db-migrate"]

[rules.layers.placement]
mode = "required"
# ...and layout, test_only, clients, store_edges, stale_entries and
# policy_changed, each with a mode, and direction and dev_direction, each with
# no_crossing, no_new_entries and stale_entries.
```

The checks, each with its own mode:

- `placement`: a crate outside the layer directories without an `unmoved`
  entry, shown on the `name =` line of its `Cargo.toml`; and, with
  `--base-architecture`, an `unmoved` entry the base branch's copy lacks,
  shown on the entry's line, since the list only shrinks. If the base
  branch has no `[rules.layers]`, the whole list is new and only
  `policy_changed` reports it.
- `layout`: a crate in a layer directory at another path than its name's,
  with or without a leaf, or with another crate inside its directory (shown
  on the outer crate, which takes a leaf; if it already has one, one of the
  two moves). Crates outside the layer directories aren't checked.
- `direction`: a normal or build dependency the matrix forbids, shown on the
  manifest line that declares it. The finding names the items the crate's
  production files use from the other one, and the fixes: move those items
  down; move the crate up, with its dependents whose layer would then point
  the wrong way, by a normal, build or dev edge (not checked against the
  client and store rules), unless the other crate is an app (the advice to
  move up to `l30_storage` adds that the stores the crate uses must then list
  it, and to move a `*-client` up to `l40_api`, that it may then depend only
  on Domain, Foundation and clients); or invert the edge through a trait. An
  app depending on an app moves the shared code into an API crate; one
  depending on a store goes through the API crate that owns it.
- `dev_direction`: a dev-dependency on a higher layer. Tests may use their
  own and lower layers and test_support, including another service's real
  server or store; the client and store rules don't apply to them, and
  test_support and tools crates may dev-depend on any layer but tools. A
  dev-dependency that duplicates a normal or build one is checked only as
  that. Fix by moving the fixtures into test_support or down, or the tests
  up.
- `test_only`: a normal or build dependency on a test_support crate from a
  layer crate, or any dependency on a tools crate. Tools may use test_support
  as a normal dependency. An `allowed_edges` entry, which exempts an edge
  from `direction`, `clients` and `store_edges`, never exempts one from
  `test_only`.
- `clients`: a `*-client` crate in `l40_api` depending on anything but
  Domain, Foundation and other clients, such as its own service or a store.
  The fix is a protocol crate in Domain that both depend on.
- `store_edges`: an API or Storage crate depending on a store whose
  `store_users` doesn't list it. The fix is the owner's API, or for a store,
  which can't depend on one, moving the read into the owning store; or, if
  the crate owns the state, listing it.
- `stale_entries`: an `unmoved` entry for a crate that is gone or now in a
  layer directory; a `store_users` entry for something that isn't a store, or
  listing a crate that is gone, a client, or one outside API and Storage,
  which allows nothing; an `allowed_edges` entry whose normal or build
  dependency is gone, that needs no exception, or that is for a `test_only`
  edge, which it can't exempt. An entry naming a crate without a layer isn't
  stale: the crate's `placement` finding covers it. Each is shown on its line
  in `ARCHITECTURE.toml`.
- `policy_changed`: every difference in `[rules.layers]` from the base
  branch's `ARCHITECTURE.toml`, given with `--base-architecture`: an
  `unmoved` entry added or moved to another layer, a store user or allowed
  edge added or removed, a mode changed, or the whole table new. A removed
  `unmoved` entry is one only if its crate moved into another layer's
  directory than the entry's (shown on the `unmoved` header, or the first
  `[rules.layers…]` one); otherwise its crate moved where the entry said, was
  deleted, or is a `placement` finding. A base with `[rules.layers]` and a
  branch without it gets one advisory finding on line 1 of
  `ARCHITECTURE.toml`: the whole rule was removed and no longer runs. The
  copy is read as for `crate_names`; a
  `[rules.layers]` of another shape skips this behaviour and `placement`'s
  comparison with a warning (on a branch without the rule, the check for its
  removal), and `crate_names.policy_changed` still runs.

`direction` and `dev_direction` have baseline tables, `[layers.direction]`
and `[layers.dev_direction]`, which map each package to the packages it
depends on against the check today, one per line. Package names, not paths,
so moving a crate into its layer directory leaves its entries alone. Each
check has three behaviours:

- `no_crossing`: an edge with the check's problem that its table doesn't
  list;
- `no_new_entries`: an entry the base branch's copy of the table lacks,
  given with `--base-baseline`. A copy with a `[layers]` key but without the
  table, or whose `ARCHITECTURE.toml` has a `[rules.layers]`, even one this
  branch can't read, tolerates nothing. If the base branch has no layers rule
  yet (neither a `[layers]` key in its baseline nor, with
  `--base-architecture`, a `[rules.layers]`), every entry is reported as
  advisory instead, as the pull request seeding the table. Without the flag,
  or with a `[layers]` key or table that doesn't parse, it is skipped with a
  line after the summary;
- `stale_entries`: an entry whose edge is gone, whose package is gone or has
  no layer, or that the check no longer reports, such as a dev entry whose
  dependency is also a normal one.

`--write-baseline` keeps the entries that are still violations, removes the
rest, and adds none: it prints `not added: …` for each edge without one,
saying to fix the dependency, since an entry added by hand fails
`no_new_entries`; a reviewed, permanent exception to `direction` goes in
`[rules.layers.allowed_edges]`, which `policy_changed` reports, and
`dev_direction` has no exception path. As for `crate_names`, it lists no TOML
to paste. It always writes both tables, after
the tables of caps and before `[crate_names]`;
`--write-baseline=layers.<check>` updates one, creating it if the file lacks
it.

### Limits

- The checks see direct dependencies only. An edge through a re-export, such
  as `common-svc` re-exporting a store, is not seen until the legacy crates
  that re-export go.
- The package-keyed tables have no rename map. Renaming a package that a
  `[layers.*]` baseline entry names fails `no_new_entries` until its edges
  are fixed. Renaming one that an `unmoved` entry names fails `placement`,
  since its entry under the new name is new, until the crate moves into its
  layer directory; fixing its edges doesn't help.

### Adding a crate, a store user or an exception

- **A crate:** create it in its layer's directory, at the path its package
  name gives. If no layer fits, the crate probably holds two things; split it.
- **A store user:** if an API or Storage crate owns state a store keeps, add it
  to the store's list in `[rules.layers.store_users]`. `policy_changed`
  reports the change for review. A crate that only needs the data uses the
  owner's API instead.
- **An exception:** an edge the matrix forbids for good reason, such as the
  database migrator applying every store's migrations, goes in
  `[rules.layers.allowed_edges]` with a comment saying why; `policy_changed`
  reports it. A violation to fix later doesn't: it fails `no_crossing`, and
  a hand-added baseline entry fails `no_new_entries`.
- **Moving a crate:** `git mv` it into its layer directory and remove its
  `unmoved` entry, which `stale_entries` asks for. Its layers baseline
  entries keep matching, but path-keyed configuration has to follow the
  crate: ratchet `include` and `allow`, fence `members` and `allow`, the
  `[file_size]` and ratchet baseline keys, and the member paths of
  `[crate_names]` baseline entries.

## Configuration and modes

Each behaviour has a `mode`: `advisory` findings are reported, `required` ones
also fail the check, so behaviours can graduate one at a time. Unknown keys,
sections or modes, a missing file, bad TOML, an invalid regex, glob or instance
name, an empty `patterns`, `include`, `packages` or `members` list, a layer
name that is not one of the seven, a package listed twice in `unmoved` or in
one `store_users` or `allowed_edges` list, a baseline table for an instance or
layers check that is not configured, and a failing `cargo metadata` exit 2. So
does a `--base-baseline`, `--renames` or `--base-architecture` that can't be
read, or one combined with `--write-baseline`, and `--renames` input that is
not git's (see [Renamed files](#renamed-files)).

## Baseline

Each table maps a repository-relative path to its cap, the count it had when
listed: under `[file_size]`, each production file over the limit, with its
code-line count; under `[pattern_ratchet.<instance>]`, each production file
outside the instance's allowed paths with a matching line, with its count of
matching lines. `[layers.<check>]` maps a package name to the packages it
depends on against the check instead (see [Layers](#layers)), and
`[crate_names]`, written last, a package name to its member path (see
[Crate names](#crate-names)). Entries should only be
removed or lowered.

`cargo arch --write-baseline` rewrites `architecture-baseline.toml` from its
entries and the tree. An entry still in violation gets its current count as
its cap: under `[file_size]`, a file still over the limit; under
`[pattern_ratchet.<instance>]`, a file outside the allowed paths that still has
a matching line. The other entries are removed. It never adds an entry, or a
`[pattern_ratchet.<instance>]` table the file lacks (`[file_size]`,
`[crate_names]` and, with the layers rule configured, both `[layers.<check>]`
tables are always written, empty if the file had none; for `[crate_names]`
and the layers tables, which list no entries to paste, see [Crate
names](#crate-names) and [Layers](#layers)). It prints `not added: <path> (N code lines); …` for each
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
`crate_names`, `pattern_ratchet.<instance>` for a configured instance or
`layers.<check>` with the layers rule configured, does
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
`cargo arch --format rdjson --base-baseline <file> --renames <file>
--base-architecture <file>` and passes the findings to
[reviewdog](https://github.com/reviewdog/reviewdog) with
`-reporter=github-pr-annotations`. The files are the baseline and
`ARCHITECTURE.toml` of the synthetic merge's first parent, the base branch, or
an empty file for one the base lacks, and the renames git detects from that
base to the merge (`git diff -z --name-status --find-renames -l20000
--diff-filter=R`). If git can't list them, the step warns and runs without
`--renames`, so a moved entry counts as new; so does a rename git does not
detect. If git skips rename detection because more files were added and
deleted than `-l` allows, the step turns git's warning into a workflow
warning, and moved entries count as new too. If the base can't be resolved,
the step warns and runs without any of the flags: the `no_new_entries` and
`no_raised_caps` behaviours, `crate_names.policy_changed`,
`layers.policy_changed` and `layers.placement`'s check for new `unmoved`
entries are skipped, and `crate_names.no_new` allows only the bootstrap
entries. Each finding is a
diagnostic with code `<rule>.<behaviour>`, or `<rule>.<instance>.<behaviour>`
for a rule with instances, on line 1 of its file, with the path exactly as it
is. A new catch-all crate points at the `name =` line of its `Cargo.toml`
instead, and a new entry, a raised cap, a stale entry for a deleted file and a
stale `[crate_names]` entry at the entry's line in the baseline. `layers`
findings point at a crate's `name =` line (`placement`, `layout`), the
manifest line declaring a dependency (an edge), the entry's line in
`ARCHITECTURE.toml` (a new `unmoved` entry, `stale_entries`,
`policy_changed`) or the edge's line in the baseline.
`pattern_ratchet`'s `no_crossing`, `listed_no_growth` and shrank
`stale_entries` findings point at the file's first matching line; one for a
file with no matches left, at its line 1. Required findings are `ERROR`,
advisory ones `WARNING`.

- `-filter-mode=file` reports advisory findings in the files the pull request
  changes, so one on a line of the baseline shows only when the baseline
  changes.
- `-filter-mode=nofilter` reports every advisory finding when the pull request
  changes `ARCHITECTURE.toml`, the baseline, the root `Cargo.toml` or anything
  under `crates/tools/archcheck/` (by old or new name), or when its file list
  can't be read in full.
- reviewdog fails once it has written 10 annotations, whatever their severity.
  So advisory findings get one run that never fails the step, and required
  findings a second run with `-fail-level=error` that does. That run always
  uses `nofilter`: with `file`, a required finding on a file renamed without
  edits, or on a baseline line the pull request leaves alone, such as a
  deleted file's stale entry, would be dropped and the job would pass. A
  required finding already on the base branch fails it too, as
  `tests/repository.rs` fails every pull request's tests then.
- GitHub shows a step's first 10 warning annotations, and the report lists
  every `file_size` finding first. So
  [`order-archcheck-findings.sh`](../../../ci/scripts/order-archcheck-findings.sh)
  hands the advisory run the pull request's own findings first, then the rest.
  First come all `no_new_entries` and `no_raised_caps` findings: they compare
  with the base's copy by table and key, so they include the entries of a
  renamed or split pattern table even though those lines are unchanged.
  `crate_names.policy_changed` and `layers.policy_changed` findings, which
  compare with the base's `ARCHITECTURE.toml`, come with them, although the
  report lists them last.
  Then come findings in the files the pull request changes (old or new name),
  and stale entries in the baseline for files it changes, such as files it
  deletes, or, in `[crate_names]`, for a crate whose `Cargo.toml` it changes,
  at the entry's member path. Each group keeps report order. With `file`, this
  changes nothing that is shown. Required findings are annotated from the
  report as it is, in their own run, in every file. The step logs how many own
  advisory findings reviewdog will show under the step's filter mode (with
  `file`, none in a file renamed without edits); with more than 10, it says in
  the step summary that at most 10 are annotated. Annotations are not the full
  inventory: every finding is in the step's log and its rdjson output, or run
  `cargo arch`.

A required finding, such as a new catch-all crate's on its own `Cargo.toml`,
is annotated wherever it is, and its `ERROR` fails the job, including for a
listed crate that is only a path dependency, moved without editing its
manifest.

Only advisory annotations are filtered. Once a behaviour is `required`,
`tests/repository.rs` also fails on any required finding anywhere in the
tree.

## Adding a rule

A new instance of `pattern_ratchet` is configuration, not a new rule (see
above). For a new kind of check:

1. Add a module under `src/rules/`. A rule that caps a count per file in a
   baseline table, as both rules do, implements `CountRule`
   (`src/rules/ratchet.rs`): one `CountRatchet` per table, whose `measure`
   gives a violating file's count and the line findings about it point at, and
   whose wording the findings and `not added` lines are built from. The five
   behaviours, finding locations, the comparisons with the base branch's copy,
   renamed files, the skip lines, and `--write-baseline` (update or remove,
   never add) come with it. Any other check implements `Rule`; if it has a
   baseline table, it also implements `Rule::tables`, without which the
   baseline's table is rejected as naming no instance, and
   `Rule::update_baseline`, which never adds an entry; if it compares with the
   base branch's copy, `Rule::baseline_rule`, `Rule::skipped` and
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
