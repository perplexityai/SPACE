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
include_layers = ["l40_api"]     # optional: the layers checked; all when absent
allow = ["crates/x/src/owner.rs", "crates/x/src/owner/**"]
allow_layers = ["l10_foundation"]  # optional: layers whose files are never counted

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

`include_layers` and `allow_layers` do the same by layer: they list
[layers](#layers) by name (`l10_foundation`, `l20_domain`, `l30_storage`,
`l40_api`, `l50_app`, `test_support` or `tools`), and a file is in the layer
of the workspace member whose `src/` holds it, the innermost one if members
nest. The layers rule gives each crate its layer, from its layer directory or
else its `[rules.layers] unmoved` entry, so an instance reads the same before
and after a crate moves into its directory, without editing its globs. A file
must be inside both `include` and `include_layers` when both are given, and
either `allow` or `allow_layers` exempts it. A crate without a layer, which
`layers.placement` reports, is in none: `include_layers` leaves its files out
and `allow_layers` does not exempt them. Findings then speak of allowed paths
and layers, and an entry for a file in an allowed layer is stale (`… is in a
crate in <layer>, which [rules.pattern_ratchet.<instance>] allow_layers …
allows`). Either key without `[rules.layers]`, an unknown layer name, a layer
listed twice and an empty `include_layers` exit 2; an empty `allow_layers` is
fine.

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

[rules.layers.storage]                 # see Storage access below
drivers = ["sqlx", "rusqlite"]         # packages that reach a database or object storage
helpers = ["pplx-space-s3-http-client"]  # storage helpers without a driver dependency

[rules.layers]
legacy = ["pplx-space-common-svc"]     # crates being dissolved; see Storage access below
protocols = ["pplx-space-events"]      # protocols without a *-protocol or *-proto name; see Domain

[rules.layers.domain_fences]           # packages no Domain crate depends on, by family
runtime = ["tokio"]
http = ["hyper", "reqwest"]

[rules.layers.placement]
mode = "required"
# ...and layout, test_only, clients, store_edges, single_user, stale_entries
# and policy_changed, each with a mode, and direction, dev_direction,
# storage_access, codec_features, domain_dependencies and model_protocol, each
# with no_crossing, no_new_entries and stale_entries.
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
- `single_user`: a hint, advisory, for a Domain crate that may belong in its
  one user (see [Domain](#domain)).
- `stale_entries`: an `unmoved` entry for a crate that is gone or now in a
  layer directory; a `store_users` entry for something that isn't a store, or
  listing a crate that is gone, a client, or one outside API and Storage,
  which allows nothing; an `allowed_edges` entry whose normal or build
  dependency is gone, that needs no exception, or that is for a `test_only`
  edge, which it can't exempt; a `legacy` entry for a crate that is gone;
  a `[rules.layers.storage]` `helpers` entry that is gone, outside
  `l10_foundation`, also `legacy`, or a helper by its own driver dependency
  already; a `protocols` entry that is gone, outside `l20_domain` or a
  protocol by its name already, and a `[rules.layers.domain_fences]` package
  that is a driver or a storage helper, which `storage_access` checks
  instead, or a Foundation crate that its own dependencies fence already.
  An entry naming a crate without a layer isn't stale: the crate's
  `placement` finding covers it. Each is shown on its line in
  `ARCHITECTURE.toml`.
- `policy_changed`: every difference in `[rules.layers]` from the base
  branch's `ARCHITECTURE.toml`, given with `--base-architecture`: an
  `unmoved` entry added or moved to another layer, a store user or allowed
  edge added or removed, a legacy crate or a protocol added or removed, a
  driver or helper added to or removed from `[rules.layers.storage]`, a
  package added to, removed from or moved between
  `[rules.layers.domain_fences]` families, a mode changed, or the whole
  table new. A removed `unmoved` entry is one only if its crate moved into
  another layer's directory than the entry's (shown on the `unmoved` header,
  or the first `[rules.layers…]` one); otherwise its crate moved where the
  entry said, was deleted, or is a `placement` finding. A base with
  `[rules.layers]` and a branch without it gets one advisory finding on line
  1 of `ARCHITECTURE.toml`: the whole rule was removed and no longer runs.
  The copy is read as for `crate_names`; a
  `[rules.layers]` of another shape skips this behaviour and `placement`'s
  comparison with a warning (on a branch without the rule, the check for its
  removal), and `crate_names.policy_changed` still runs.

`direction` and `dev_direction` have baseline tables, `[layers.direction]`
and `[layers.dev_direction]`, which map each package to the packages it
depends on against the check today, one per line; so do `storage_access`,
`codec_features` (see [Storage access](#storage-access)),
`domain_dependencies` and `model_protocol` (see [Domain](#domain)). Package names, not
paths, so moving a crate into its layer directory leaves its entries alone.
Each check has three behaviours:

- `no_crossing`: an edge with the check's problem that its table doesn't
  list;
- `no_new_entries`: an entry the base branch's copy of the table lacks,
  given with `--base-baseline`. If the base branch has no layers rule yet
  (neither a `[layers]` key in its baseline nor, with `--base-architecture`,
  a `[rules.layers]`), or its `[rules.layers]` has none of the check's modes,
  as before the check existed, every entry is reported as advisory instead,
  as the pull request seeding the table. Otherwise a copy without the table,
  such as one with a `[layers]` key but not the table, or one whose
  `ARCHITECTURE.toml` has a `[rules.layers]` this branch can't read,
  tolerates nothing. Without the flag, or with a `[layers]` key or table
  that doesn't parse, it is skipped with a line after the summary;
- `stale_entries`: an entry whose edge is gone, whose package is gone or has
  no layer, or that the check no longer reports, such as a dev entry whose
  dependency is also a normal one.

`--write-baseline` keeps the entries that are still violations, removes the
rest, and adds none: it prints `not added: …` for each edge without one,
saying to fix the dependency, since an entry added by hand fails
`no_new_entries`; a reviewed, permanent exception to `direction` goes in
`[rules.layers.allowed_edges]`, which `policy_changed` reports,
`dev_direction` and `model_protocol` have no exception path, a crate that
has to reach storage for good becomes a store or a storage helper (see
[Storage access](#storage-access)), and a Domain crate that needs a fenced
package is not Domain code and moves to its layer (see [Domain](#domain)).
As for `crate_names`, it lists no TOML to paste. It always writes all six
tables, after the tables of caps and before `[crate_names]`;
`--write-baseline=layers.<check>` updates one, creating it if the file lacks
it.

### Storage access

Nothing above Storage talks to a database or object storage directly,
Domain never does, and Foundation does only in a storage helper, which names
nothing SPACE-specific. `[rules.layers.storage]` says what that means:

- `drivers`: the packages outside the workspace that reach a database or
  object storage, such as `sqlx`, `rusqlite`, `redis`, `deadpool-redis` and
  `aws-sdk-s3`, and their sub-crates, so that depending on `sqlx-core`
  instead of `sqlx` is no way around the checks. At least one.
- A **storage helper** is derived, not listed: an `l10_foundation` crate
  with a required (normal or build, not optional) dependency on a driver,
  such as `pplx-space-sqlite`. `helpers` adds the few without one, such as
  `pplx-space-s3-http-client`, which plugs an HTTP connector into an
  `aws-sdk-s3` client without depending on it; `policy_changed` reports each
  change to the list. A derived helper is no policy change: the manifest
  diff that adds its driver dependency is where it is reviewed.
- `[rules.layers] legacy`, beside the storage table: the crates being
  dissolved, which predate the layers and mix SPACE-specific code in, such
  as `pplx-space-common-core` and `pplx-space-common-svc`. The storage
  checks cover them like every other crate, with today's edges baselined;
  what the list changes is that a legacy `l10_foundation` crate is no
  helper even with a required driver dependency, so a crate that depends on
  one does not reach storage through it. Test support and tools are left
  out of both checks.

Two checks, each with the three baselined behaviours above and a table
keyed by package:

- `storage_access`: only stores and helpers may have a normal or build
  dependency on a driver or helper. An optional dependency counts too, since
  a feature turns it on, except from an `l10_foundation` crate: there it is a
  codec behind a feature, such as `sqlx` impls for an identifier type, and
  `codec_features` covers whoever turns it on. The finding is shown on the
  manifest line that declares the dependency, names the items the crate
  uses from it, and gives the fixes for its kind: an API crate moves that
  code into a store that lists it as a user and calls the store, or, for a
  generic mechanism, into a storage helper that such a store calls; an app,
  which never depends on a store, uses the API crate that owns the state;
  a client, which never depends on one either, moves the storage work into
  its server, behind its protocol; from Domain, a codec's impls move into
  the store that persists the types; from Foundation, a crate that names
  nothing SPACE-specific can be listed as a helper. Moving the code into a
  new helper never lets the original crate depend on that helper:
  `storage_access` checks that edge too. `[layers.storage_access]` maps each
  package to the drivers and helpers it depends on, such as
  `pplx-space-api = ["sqlx"]`.
- `codec_features`: a **driver feature** is a feature of a crate outside
  Storage that turns on an optional dependency on a driver or helper, with
  `dep:<name>`, `<name>/<feature>` or Cargo's implicit feature for the
  dependency, that turns on an optional dependency whose `features` or
  default features include another crate's driver feature, or that
  forwards to another driver feature, its own or another such crate's
  (`<name>/<feature>` or `<name>?/<feature>`). Only stores and
  helpers may enable one: with `features = […]` on a normal or build
  dependency, through its default features when `default` is a driver
  feature, or by forwarding from their own `[features]`. As an optional
  driver dependency of an `l10_foundation` crate is exempt from
  `storage_access`, so is its enabling behind a feature of its own, on an
  optional dependency or by forwarding: that feature is a driver feature
  itself, and whoever turns it on is checked. The finding is shown
  on that dependency's or `[features]` item's line and says which driver the
  feature reaches. `[layers.codec_features]` maps each package to the
  `<package>/<feature>` entries it enables, such as `pplx-space-api =
  ["pplx-space-common-core/sqlx"]`. A store's own driver features are not
  checked: depending on a store is how an API crate reaches storage, and a
  feature that reaches a driver only through a store's is no driver feature.

Dev-dependencies are not checked: tests may open a database. A finding that
is stale names the reason: the crate or the dependency is gone, the crate is
now a store, a helper or left out, the target is no longer a driver or
helper, an optional Foundation dependency or a Foundation crate's enabling
behind its own feature is a codec, or the feature no longer reaches a
driver.

There are no per-edge exceptions, as `allowed_edges` gives the matrix: a
crate that has to reach storage for good becomes a store, in `l30_storage`,
or, if it names nothing SPACE-specific, a storage helper (see below). Moving
it into `l30_storage` is reviewed as a change to its layer, and listing it in
`[rules.layers.storage] helpers` is a policy change, which `policy_changed`
reports; a Foundation crate that becomes a helper by gaining a required
driver dependency is derived, and reviewed in that manifest diff only. A
violation to fix later gets no entry: a hand-added one fails
`no_new_entries`.

### Domain

`l20_domain` holds two kinds of crate. A **protocol** holds wire schemas and
the conversions between them and the models: a Domain crate whose package
name ends in `-protocol` or `-proto`, or that `[rules.layers] protocols`
lists, such as `pplx-space-events`. Every other Domain crate is a **model**:
types and the rules that keep them valid. A crate that holds both, until it
is split, counts as a model. Domain does no I/O, except generating IDs in ID
constructors and logging. Two checks, each with the three baselined
behaviours above and a table keyed by package, and a hint:

- `domain_dependencies`: no Domain crate has a normal or build dependency,
  optional or not, for any target, on a fenced package. Each family of
  `[rules.layers.domain_fences]`, such as `runtime`, `http`, `cloud` or
  `kube`, is an exact list of package names, so a new package is added by
  hand. tonic and prost are not listed: protocols build on them. The
  workspace's own I/O crates are derived, as storage helpers are: an
  `l10_foundation` crate, legacy or not, with a required (normal or build,
  not optional) dependency on a fenced package is fenced under that
  package's family, to a fixpoint, so a crate wrapping such a crate is fenced
  too, and the finding names the dependency that fenced it. Features are
  not followed, neither in that derivation nor in the features a Domain
  crate turns on, so a crate that does I/O only behind a feature is listed,
  such as `pplx-space-observability` under `runtime`, whose `otel` feature
  brings tokio in, unless a required dependency fences it already, as
  `pplx-space-metrics` fences `pplx-space-feature-flags`. A listed package
  that is a driver or a storage helper is left to `storage_access`, which
  reports it from Domain already, and is a `stale_entries` finding, as is a
  listed Foundation crate that its own dependencies fence already. The
  finding is shown on the
  manifest line that declares the dependency, names the family and the
  items the crate uses from it, and gives the
  fixes: move that code into the `l40_api` crate that does the I/O, or into
  an `l10_foundation` mechanism that names nothing SPACE-specific, and pass
  the Domain crate plain values or a trait it defines; or move a crate that
  is not Domain code to its layer. `[layers.domain_dependencies]` maps each
  package to the fenced packages it depends on, such as
  `pplx-space-spaced-common = ["tokio"]`. Dev-dependencies are not checked,
  and only declared dependencies are: a protocol's tonic brings tokio and
  hyper into its build regardless.
- `model_protocol`: no model has a normal or build dependency on a protocol.
  Protocols convert between the wire and the models, so they depend on the
  models, and on other protocols, never the other way. The finding names the
  items the model uses from the protocol and the fixes: move them into a
  model and convert in the protocol, or split the model's wire code into a
  protocol crate of its own. `[layers.model_protocol]` maps each model to the
  protocols it depends on.
- `single_user` (advisory): a Domain crate that exactly one crate uses, in
  `l30_storage`, `l40_api`, `l50_app`, `test_support` or `tools`, may belong
  in it; the hint is shown on the Domain crate's `name =` line. Users are
  the crates with a normal or build dependency on it, and for one whose
  `src/lib.rs` re-exports it at the top level (`pub use <crate>`, `pub use
  <crate>::*` or `pub use <crate>::<item>`, under the name it imports the
  crate by, outside any `mod`, function or other braces, comment or literal,
  and not `pub(crate)`), as a client that re-exports its protocol does, that
  crate's users too: moving the crate into one of them would break the
  others. The
  crates `[rules.layers] legacy` lists are not counted, and neither are
  their users: what they re-export, often by glob, can't be told apart by
  path, so a hint for a crate a legacy crate depends on says that those
  re-exports can hide real users. Nor is the hint given for a Foundation
  user, whose edge `direction` reports, or with a second user in Domain,
  which could not follow the crate up; a legacy crate gets none itself.
  There is no baseline. Today no crate qualifies: each Domain crate with one
  direct user outside Domain is re-exported by that user, which has users of
  its own, or by a legacy crate, or that user is in Foundation.

Two `pattern_ratchet` instances complete the picture by line rather than by
dependency (`ARCHITECTURE.toml` lists their patterns and what they miss):
`domain_io`, with `include_layers = ["l20_domain"]`, counts environment
reads, filesystem calls, sockets and connections, process and task
spawning, clock reads and sleeps in Domain's production files; `foundation_space_names`, with
`include_layers = ["l10_foundation"]`, counts `SPACE_*` names in
Foundation's, which should name nothing SPACE-specific. Both count the
legacy crates in their layer, with baseline entries, so that new matches do
not land there.

### Limits

- The checks see direct dependencies only. An edge through a re-export, such
  as `common-svc` re-exporting a store, is not seen until the legacy crates
  that re-export go.
- A Foundation crate that gains a required driver dependency becomes a
  storage helper with no finding of its own: only listed `helpers` changes
  are policy changes, so a derived helper is reviewed in the manifest diff.
  Its existing non-exempt dependents then get `storage_access` findings;
  stores, other helpers, test support and tools, and `l10_foundation` crates
  whose dependency on it is optional are exempt.
- `codec_features` reads the feature requests in each crate's own manifest,
  not the features Cargo unifies for a build. `common-svc` enables
  `common-core/sqlx`, so its dependents build against `common-core`'s `sqlx`
  impls whatever they request; one that drops `"sqlx"` from its own
  manifest loses its baseline entry, which `stale_entries` reports, but
  keeps the impls.
- The drivers are a hand-kept list, and `cargo metadata` runs with
  `--no-deps`, so a crate outside the workspace that wraps or re-exports a
  driver is not seen unless it is listed. A workspace crate that does is
  seen as a storage helper only once it is in `l10_foundation` with a
  required driver dependency.
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
- **A storage helper:** an `l10_foundation` crate with a required driver
  dependency is one already. One that wraps a driver's client without
  depending on the driver goes in `[rules.layers.storage] helpers` with a
  comment saying why, and must name nothing SPACE-specific.
- **A protocol:** name it `*-protocol` (or `*-proto`); a protocol with
  another name, such as one that predates the convention, goes in
  `[rules.layers] protocols`, which `policy_changed` reports.
- **A Domain dependency:** a package that does I/O, such as a new HTTP
  client or `aws-sdk-*` crate, goes in its `[rules.layers.domain_fences]`
  family; a Foundation crate built on one needs no entry. A Domain crate that needs one is not Domain code; there is no
  per-edge exception, and a hand-added baseline entry fails `no_new_entries`.
  For a line that `domain_io` matches but that is not I/O, narrow the
  instance's patterns in a reviewed change.
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
one `store_users` or `allowed_edges` list or in a `[rules.layers.storage]` list,
a package listed twice in `protocols` or in two `[rules.layers.domain_fences]`
families, an empty `drivers`, an empty `include_layers` or a layer
listed twice in it or in `allow_layers`, either of them without
`[rules.layers]`, a baseline table for an instance or
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
depends on against the check instead, or for `codec_features` the
`<package>/<feature>` entries it enables (see [Layers](#layers)), and
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
