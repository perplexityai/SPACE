# Database Migrations

Migrations under `db/` are immutable once they have been applied by any shared
environment. Do not edit an applied migration to change behavior or fix a bug:
add a new migration in the same store directory instead.

The first public PostgreSQL migration is `20261002070625_baseline`; SQLite
stores start at `20260930000000_baseline`. These preserve the final historical
schemas and are rollback floors. Startup supports fresh databases and compatible
public migration histories. Historical catalogs predating these baselines are
unsupported and rejected without rewriting history or recreating the database.
This also applies to retained databases returning from offline storage. Images
with historical catalogs cannot reopen public histories; do not roll back across
that boundary.

Before removing history-conversion support from an existing deployment, deploy
a release that still supports the conversion and verify that PostgreSQL and all
service and shared SQLite histories use the public baselines. Account for
retained and offline databases too; successful startup of active services alone
does not establish that the transition is complete.

The PostgreSQL migrator ignores `goose_db_version`, whether present or absent.

SQLx records migration checksums. Editing an applied migration makes the bundled
checksum diverge from the checksum stored in the database, which correctly stops
Postgres migration on startup and requires operator intervention. Node-local
SQLite stores can hold authoritative state. New changes must remain additive
migrations for every store.

Postgres migrations under `db/postgres/` use SQLx numeric versions. Retired
legacy migrations used `NNN_name.up.sql` / `NNN_name.down.sql` names through
`093`. New Postgres migrations must use a UTC creation timestamp:
`YYYYMMDDHHMMSS_name.up.sql` and `YYYYMMDDHHMMSS_name.down.sql`. For Postgres,
the migration version guard evaluates only newly added files, including files
moved or renamed into `db/postgres/`, requires their numeric prefix to be
greater than the fixed legacy cutoff, and rejects a new timestamp that is not
newer than the max Postgres migration version already on the base branch.

Generate the version prefix with:

```bash
date -u +%Y%m%d%H%M%S
```

A new table needs an owner: the store that writes it declares it in its
`Cargo.toml` (`[package.metadata.space.state]`) before the migration that
creates it lands, and `cargo arch` checks it. The shared catalogs, such as
`db/common`, and the databases they are applied to are listed in
`[rules.layers.state.shared_catalogs]` in `ARCHITECTURE.toml`. See
[State ownership](../crates/tools/archcheck/README.md#state-ownership).

## Migration runners

Each database's `up` command runs its embedded SQLx migrations. Applied files
must retain their exact checksums unless an equivalent revision is explicitly
registered as described below. There are no automatic history repairs.

SQLite consumers of the common jobs schema run the shared catalog first, using
`_space_common_schema_migrations`, then the service catalog using
`_sqlx_migrations`. Both existing histories are validated before either catalog
can apply SQL. Existing history rows and migration versions are preserved.

## Equivalent historical revisions

A reviewed edit that preserves executable SQL (for example, removing a private
name from a comment) can retain upgrade compatibility through
`db/accepted-checksums.toml`. Each entry identifies a store by its directory name
(`postgres`, `template-manager`, `common`, etc.), a numeric migration version,
and the SHA-384 digests of **all** accepted up-migration revisions, including the
currently bundled file. Compute a digest with `shasum -a 384 <migration.up.sql>`.
Commit the registry entry together with the corresponding SQL edit.

PostgreSQL and SQLite runners embed the registry. They accept a recorded digest
only when both it and the bundled digest are listed for the same store/version.
They do not rewrite migration history or rerun already applied SQL. New databases
record the bundled digest. Unlisted digests remain errors; public SQLite baselines
fail closed without recreating the database. The registry does not cover down
migrations, whose checksums SQLx does not record.

Keep accepted digests while their migration catalog is supported so deployments
can upgrade directly from older releases.
This is not a mechanism for changing schema or data behavior: those
changes still require a new migration. Rollbacks to versions predating a registry
entry can reject a database created with the revised SQL.

## Rolling back past a newer SQLite migration

SQLite rollbacks are image-only, as Postgres rollbacks are: the older release
starts on the schema the newer one left. A release that accepts newer
migrations by default finds, in its service history and in the shared `common`
history, each recorded migration that succeeded, that it doesn't bundle, that
records a SHA-384 checksum, and that is newer than every migration that catalog
bundles. It leaves each one applied and logs a warning per row with the
`store`, `migration_version` and `recorded_checksum` (the recorded SHA-384, in
hex). Accepting a migration only leaves it in place; it doesn't verify that the
older release can still read and write the data that migration left. The
allowlist and markers in 'Rollback-safe SQLite migrations' are what make that
safe. It still refuses, before any write:

- a failed row;
- a bundled row whose recorded checksum it doesn't accept (see 'Equivalent
  historical revisions');
- an unbundled row whose checksum isn't a 48-byte SHA-384 digest;
- an unbundled row older than the newest migration its catalog bundles: one
  withdrawn after a newer migration was bundled, one merged out of timestamp
  order, or a newer row that a later release's migration has overtaken;
- a newer row in either history while either history is missing a migration
  the release bundles, since the newer release that recorded the row applied
  both catalogs in full, the shared one first.

The overtaken case means that after a rollback, deploying a release that
bundles a newer migration in that store, but not the newer rows, refuses the
database: for example, a hotfix of the rollback target that adds its own
migration, or a release built after a revert deleted a migration's file. Undo
the newer rows with the newer image's `down`, as below, before deploying such a
release. A withdrawn migration stays bundled once any environment has applied
it, and a forward migration undoes its change, as
`20261113190000_restore_sandbox_legacy_state` does for
`20261109120000_drop_sandbox_legacy_state` in sandbox-manager.

Rolling forward again applies nothing new: the newer release finds its
migrations recorded. Its code then runs on rows the older release wrote in the
meantime, which is why the allowlist admits only nullable or defaulted columns,
and a `data` marker's reason says how those rows are handled.

Image-only rollback past a migration that a `space-allow-rollback-unsafe`
marker exempts is safe only to a release its reason allows. A reason such as
"release N stopped reading it" means a release older than N still uses what
the migration drops or changes; a reason such as "the new code reads both
forms" allows any target. Before a rollback that skips releases, list the
marked migrations between the two:

```bash
bash scripts/list-rollback-unsafe-migrations.sh <target-ref> <current-ref>
```

It prints one line per marker in the SQLite up migrations `<current-ref>` has
and `<target-ref>` doesn't, with the store, version, kind, object and reason,
newest first per store. A `db/common` marker is listed once for each consumer
store in `ARCHITECTURE.toml`, under that store: each consumer's database has
its own common history. It exits 0 when there are none, 1 when it lists some,
and 2 when it can't read the refs or files. The list marks rollback
boundaries, not the commands to run. Read each reason against the target. For
each marker whose reason names a release newer than the target, stop that
store and run the newer image's `space-db-migrator <store> … down` until the
marked version is gone from its histories, as below; for a `common` line that
is the consumer store's migrator, until `_space_common_schema_migrations` no
longer records it. Each run reverts the newest migration across the store's
history and `common`'s, so every migration newer than the marked one is
reverted first.

A release that doesn't accept newer migrations by default refuses every
recorded migration it doesn't bundle, unless the setting below lists it.
Rolling back to one needs, on every node, the newer image's `space-db-migrator
<store> … down`: first stop the store's daemon on the node and keep the newer
release from restarting it or re-running its migrations until the older release
starts, since a daemon still serving would read and write a schema being
reverted under it. Each run reverts only the newest applied migration across
the store's history and `common`'s, so run it once per migration, newest first.
Some down migrations are rollback floors that refuse by design, such as
runtime-manager's `20261012064915_artifact_operations`, so read each down
script first, and where one refuses, follow its recovery instructions instead.
For template-manager, read 'Node dirty-ref revision migration' and 'Template
cache partition migration' first: some earlier releases reverse certain newer
migrations themselves at startup, with data checks that a manual `down` skips,
and those sections say which targets need which steps.

Such releases from #1818 on accept a newer migration only when
`SPACE_SQLITE_ACCEPT_NEWER_MIGRATIONS` lists it as a `<store>:<version>` entry
(`common` for the shared catalog), set on the containers that run the store's
migrations at startup, in the same deploy as the image revert. That setting
still works for a rollback to one of them, after checking each entry:

1. Find the entries without starting the older release on the node: take a
   consistent copy of the store's database with
   `sqlite3 -readonly <db> ".backup <copy>"`, and run the target image's
   `space-db-migrator <store> --database-path <copy> up` against the copy with
   the setting unset. Its refusal names each entry, unless another invalid row,
   one that failed, was modified or isn't bundled, stops the check first: it
   then names only that row, and the list is incomplete. Resolve that row as
   its recovery guidance says, and run it again.
2. For each one, read the migration in the newer release and check it against
   the target's code: whether the target uses anything it drops or renames,
   whether the target's writes can violate a constraint, index or trigger it
   adds, a new table's foreign key into a table the target deletes rows of
   included, whether it rewrites rows the target reads, and whether the newer
   release writes values the target doesn't handle. A migration with no
   marker passes the first three. If a node may have run a
   pre-release build, compare the recorded checksum
   (`SELECT lower(hex(checksum)) FROM _sqlx_migrations WHERE version = …`, or
   `_space_common_schema_migrations` for `common`) with `shasum -a 384` of the
   file you checked.
3. After rolling forward again to a release that accepts newer migrations by
   default, remove the setting. That release ignores it, but a release from
   #1818 up to the one that accepts newer migrations by default still reads it
   on a later rollback, and an entry left over would accept its migration
   unchecked.

A release that accepts newer migrations by default no longer reads the setting:
if it is set, the release logs a warning at startup and ignores it. While a
database records a newer migration, an older image's `down` refuses too,
because SQLx validates the applied history, so undo newer migrations with the
newer image.

The `sqlite3` commands in this section need the `sqlite3` command-line tool,
which a store's own image may not include. Run them where it is available,
against the database file on the node. In the charts in this repository, a
store's pods mount the node's `/space/run` directory, so one way is an
ephemeral debug container on the node:
`kubectl debug node/<node> -it --image=<an image with sqlite3>` mounts the
node's root filesystem at `/host`, which puts sandbox-manager's database at
`/host/space/run/sandbox-manager/sbx-manager.db`. Run `space-db-migrator`
itself from the store's image.

## Rollback-safe SQLite migrations

Because the release before a SQLite migration runs on the schema the migration
leaves, the [SQLite rollback guard](../scripts/check-sqlite-migrations-rollback.sh)
classifies every statement a change adds to a `db/<store>/*.up.sql` outside
`db/postgres/`, `db/common` included. It runs in CI's migration guards after the
version guard. It applies the store's base-branch migrations to a database, after
`db/common`'s for the stores `[rules.layers.state.shared_catalogs]` in
`ARCHITECTURE.toml` lists as its consumers, and then runs the change's
statements one at a time under SQLite's authorizer. An object that database
has before the change's statements run is existing; what the change creates is
new, `db/common`'s new objects included for its consumers. Names compare as
SQLite compares them, folding ASCII case only. A statement that fails to apply
fails the check with SQLite's message.

A statement needs no marker only when it is one of these:

1. `CREATE TABLE` of a new name with no foreign key into an existing table.
   Keys into other new tables are fine.
2. `CREATE INDEX` that isn't `UNIQUE`, on any table, partial ones included, or
   `CREATE UNIQUE INDEX` on a new table.
3. `CREATE VIEW` of a new name.
4. `ALTER TABLE <existing> ADD COLUMN` that adds one trailing column and
   changes nothing else, with no `CHECK` and no `REFERENCES`: nullable with or
   without a constant default, `NOT NULL` with a non-NULL constant default, or
   `GENERATED ALWAYS AS (…) VIRTUAL` without `NOT NULL`.
5. Any statement on new tables, indexes and views, but for a copy of an
   existing table's rows into a new one.
6. `INSERT … VALUES` or `INSERT … DEFAULT VALUES` into an existing table that
   doesn't declare `ON CONFLICT REPLACE`, with no `REPLACE`, `OR REPLACE` or
   upsert clause. A seeded value must be one the previous release can read.

Objects in `temp` or attached databases are ignored, and so are the writes of
a trigger the base already has, which the previous release makes too.

An `ADD COLUMN` is safe only while the previous release never inserts without
a column list (`INSERT INTO t VALUES (…)`) and never reads `SELECT *` by
position. The stores' code does neither; keep it that way.

Everything else needs a marker of one of two kinds. `schema` covers any other
`ALTER` of an existing table, such as a rename or a dropped column; a `DROP` of
an existing table, index, view or trigger; `CREATE UNIQUE INDEX` on an existing
table; any `CREATE TRIGGER`, since its body can write existing tables; a
foreign key from a new table into an existing one, whatever its `ON DELETE`,
since the previous release deletes parent rows without knowing the new table;
`PRAGMA`; and any other kind of statement. `data` covers an `UPDATE` or
`DELETE` of an existing table's rows, cascaded deletes included; an `INSERT …
SELECT`, `REPLACE`, `INSERT OR REPLACE` or upsert into an existing table; any
`INSERT` into one that declares `ON CONFLICT REPLACE`; and an `INSERT` or
`CREATE TABLE … AS` that copies an existing table's rows into a new one, which
freezes data the previous release keeps writing after a rollback. What a
trigger the change creates writes to an existing table while the change's own
statements run counts as `data` too.

The marker sits in a SQL comment of the migration that holds the statement,
and exempts only that kind on that object in that file:

```sql
ALTER TABLE sandboxes DROP COLUMN legacy_state;
-- space-allow-rollback-unsafe: sandboxes.legacy_state: schema: release N stopped reading it
```

The object is the one the guard reports: a table, index, view or trigger's own
name, `<table>.<column>`, or for a foreign key `<new_table>.<column>`, or
`<new_table>` when the key is composite. The guard prints each finding it
exempts with its marker. A marker fails when no finding in its file matches
it, when it names an unknown kind, when its reason is empty or still the
placeholder the guard prints, and when it is added to a landed migration, one
whose version the base has: a marker there never exempts anything. In a landed
migration only the statements its base revision lacks are classified, so a
comment edit passes. Deleting or renaming a landed migration fails with no
marker to exempt it: a merged migration must be assumed already applied
wherever main is deployed, and a release that doesn't bundle an applied
migration refuses the database. Keep the file and undo its change with a
forward migration, as `20261113190000_restore_sandbox_legacy_state` does. A
migration that is a symlink or another non-regular file fails too, and so does
`ATTACH`, `DETACH` or `VACUUM`, which open or create other database files. On
main, CI runs the guard once per first-parent commit, so the PRs a push lands
together are judged as they merged.

A destructive change takes two releases. Release N stops reading and writing
the object, and stops relying on any rule it enforces; release N+1 drops it,
with a `schema` marker whose reason names release N.

A `data` marker's reason says why the rows the previous release writes after a
rollback, in the old form, are fine: that release never writes the old form;
the new code reads both forms; or an idempotent startup or reconciler fixup
converts the rows left over. It must also hold for the rollback target: the
target has to read every row the migration converted, not only the newer
code. A copy into a new table needs the same: the previous release keeps
writing the source, so the copy goes stale.

A statement the guard flags wrongly takes a marker too, with a reason that
starts `false positive: …`, and the guard is then fixed.

Limits: the guard judges statements, not the code that uses the schema. The
newer release must handle rows the older release writes while rolled back,
and must not write values, such as a new state, that the older release can't
read.

A new or changed `db/common` migration always needs one marker that names the
shared catalog, `-- space-allow-rollback-unsafe: common: schema: <reason>`,
whose reason covers every consumer store `ARCHITECTURE.toml` lists, and the
guard names them when it is missing. The guard doesn't classify the migration
against each consumer's own schema: a shared table with a foreign key into a
consumer's table, for one, passes the common catalog's check. So the marker
exempts every finding in that file, and the reason is where its effect on
each consumer is argued. The migration must also apply on each consumer's
base schema and on a new node, which runs the whole common catalog before the
consumer's own.

## Node dirty-ref revision migration

Template-manager bundles `20261031050126_node_dirty_ref_revisions`. Some
earlier template-manager releases don't bundle it but list it among the
migrations they reverse on rollback, with a copy of its SQL under their own
`db/template-manager/history/`. Their reversal compares the recorded checksum
only with that copy and ignores `db/accepted-checksums.toml`, so it reverses
the migration only while the bundled file keeps that checksum:
`template_manager_migrations_keep_the_checksums_releases_reverse` pins it.
Change the schema with a new migration, never by editing this one.

Rolling back to one of those releases reverses the migration with the image
alone, unless that release also lists the partition migration below without
bundling it. Such a release checks every migration it would reverse before
reversing any, so a row that blocks the partition reversal blocks this one
too; follow the restrictions under 'Template cache partition migration'.
Those releases' `space-db-migrator template-manager up` and template-manager's
startup reverse it before validating the rest of the history, so a database
that also records a later migration they don't bundle is reversed and then
refused.
Where a rollback runs that migrator in an init container beside a serving pod
of a release that bundles the migration, the serving pod still reads
`node_dirty_refs.revision`: its outbox reads and acknowledgements fail and its
rows stay queued until it exits or a migrator re-applies the migration. The
older release acknowledges rows by key, so a re-enqueue made while a send is
in flight can be lost again, and re-applying the migration restarts revisions
at 0. A send the serving pod started before the reversal and acknowledges
after a re-apply can then match a restarted revision and delete a re-enqueue
made during that send.

A release that neither bundles nor lists `20261031050126` refuses a database
that records it. That covers every release from before #1277, including the
withdrawn-interval ones that list only the partition migration below, and they
all predate `SPACE_SQLITE_ACCEPT_NEWER_MIGRATIONS`. Rolling back to one needs,
first, with template-manager stopped and the newer one kept from restarting
until the older one starts, `20261031050126` reverted with
`space-db-migrator template-manager --database-path <db> down`, run with a
migrator that bundles every migration the database records, such as the newer
release's. Each run reverts only the newest applied migration across
template-manager's history and the shared `common` catalog's, so check both
first: `sqlite3 -readonly <db> "SELECT max(version) FROM _sqlx_migrations"`
and `sqlite3 -readonly <db> "SELECT max(version) FROM _space_common_schema_migrations"`.
If either prints a version later than `20261031050126`, revert those later
migrations first, newest first across both, reading each down script as
'Rolling back past a newer SQLite migration' describes. Then run it once more
to revert `20261031050126`, and check that `_sqlx_migrations`' `max(version)`
no longer prints it. 'Template cache partition migration' and
`docs/execution-environments.md` say when this applies to a partition rollback.

## Template cache partition migration

Template-manager's `20261026220935_template_cache_partitions` was withdrawn
after some nodes had applied it, and is bundled again with the same SQL.
Releases from the withdrawn interval don't bundle it. Their writes succeed on either
schema, so one can keep serving while a migrator applies the migration.
When one finds the migration recorded with its original checksum, it applies
the down SQL and removes the history row at startup. Whether rolling back to
it needs more than the image depends on how it treats
`20261031050126_node_dirty_ref_revisions`, as 'Node dirty-ref revision
migration' describes. One from before #1277 doesn't list that version and
refuses a database that records it. First stop template-manager and keep the
newer one from restarting until the older one starts: a restart can apply a
migration the database doesn't yet record. Then check the database's history:
`sqlite3 -readonly <db> "SELECT max(version) FROM _sqlx_migrations"` must
print `20261031050126`, or `20261026220935` if the node doesn't record the
dirty-ref migration, and
`sqlite3 -readonly <db> "SELECT version FROM _space_common_schema_migrations WHERE version > 20260930000000"`
must print nothing, since `down` reverts the newest migration across both
histories. If either check fails, stop and follow 'Node dirty-ref revision
migration' instead. If the first printed `20261026220935`, no `down` is
needed. If it printed `20261031050126`, run
`space-db-migrator template-manager --database-path <db> down` once, with a
migrator from #1235 on such as the newer release's, before starting the older
one; it reverts only that migration. `docs/execution-environments.md`
describes the steps. A release that bundles it applies it normally and never
reverses it.

The reversal counts rows only. A `{hash}@{environment}` build or snapshot
directory left without a row stays on disk, and that release's startup
recovery can register such a snapshot directory as a row. Remove those
directories before rolling back, as `docs/execution-environments.md`
describes.

That reversal refuses, before any write, a database holding an
environment-partition build, tag pin or unfinished environment
`template_stamp` job, since reversing would drop that row, and template-manager
then fails to start. Keep running a release that bundles the migration on such
a node. To check a node before rolling back, this read-only query counts the
rows that block the reversal:

```sql
SELECT (SELECT COUNT(*) FROM template_builds WHERE cache_partition != '')
     + (SELECT COUNT(*) FROM template_pinned_tags WHERE cache_partition != '')
     + (SELECT COUNT(*) FROM jobs
        WHERE kind = 'template_stamp'
          AND status IN ('pending', 'running', 'cancelling')
          AND CASE
                WHEN json_valid(payload) THEN json_extract(payload, '$.partition_environment')
              END IS NOT NULL);
```

Where a rollback runs `space-db-migrator` in an init container beside the
serving pod, that pod still names `cache_partition` after the reversal, so its reads and writes fail
until it exits.

Their reversal compares the recorded checksum only with its own copy and
ignores `db/accepted-checksums.toml`, so the migration's SQL must stay
byte-identical: `template_manager_migrations_keep_the_checksums_releases_reverse`
pins its checksum.
Change the schema with a new migration, never by editing this one.

## Sandbox-manager runtime upgrade attempts

Sandbox-manager's `20261114030001_sandbox_runtime_upgrade_attempt_id` adds a
nullable `attempt_id` column to `sandbox_runtime_upgrades` and
`sandbox_runtime_upgrade_outcomes`.
`20261114030002_sandbox_runtime_upgrade_attempt_outcomes` adds
`sandbox_runtime_upgrade_attempt_outcomes`, which holds at most one final
outcome per operation and attempt id. That table has no foreign key to
`sandboxes`, so deleting a sandbox's row leaves its attempt outcomes in place.

Each down refuses, before it changes the schema, while a row it guards is
recorded:

- `20261114030002`'s while the attempt-outcome table holds any row. Dropping
  the table would lose it, and `sandbox_runtime_upgrade_outcomes` keeps only
  one outcome per operation.
- `20261114030001`'s while an operation's outcome in
  `sandbox_runtime_upgrade_outcomes` is anything but `rolled_back` or
  `already_on_target`, the two a release without these migrations parses.
  It guards outcomes only, not the other columns an older release parses.

A release that applies these migrations without writing either leaves the
column NULL and the table empty, so both downs go through. A non-NULL
`attempt_id` doesn't hold back `20261114030001`'s down: it drops the ids, which
a release without the column never reads. Once a node has recorded a guarded
row, the down that guards it refuses while the row stays. No store code deletes
attempt outcomes, so `20261114030002`'s down refuses on that node until an
operator deletes them. An image-only rollback runs no down, so after a node
records an outcome the older release can't parse, such as `refused`, roll it
back only to a release whose store parses that outcome. Bundling these
migrations isn't enough: at the commit that adds them, the store's
`RecordedRuntimeUpgradeOutcome` parses only `rolled_back` and
`already_on_target`. While both migrations are applied, this read-only query
counts the rows that block the reversal:

```sql
SELECT (SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes)
     + (SELECT COUNT(*) FROM sandbox_runtime_upgrade_outcomes
         WHERE outcome NOT IN ('rolled_back', 'already_on_target'));
```

Once `20261114030002`'s down has run, its table is gone and that query fails.
Only `20261114030001`'s guard is left then, and this read-only query counts
the rows that hold it back:

```sql
SELECT COUNT(*) FROM sandbox_runtime_upgrade_outcomes
 WHERE outcome NOT IN ('rolled_back', 'already_on_target');
```

`20261120180000_sandbox_runtime_upgrade_attempt_identity` adds nullable
`target_environment`, `target_state` and `authorization_id` columns to
`sandbox_runtime_upgrade_attempt_outcomes`: what the operation an attempt
outcome belongs to asked for, kept after the sandbox's row is deleted. A row
that has them is final for its attempt; one without them is final for no
attempt. Its down refuses, before it changes the schema, while any row has
them, since dropping the columns would leave every final outcome final for no
attempt and let a retry of an attempt its caller gave up run again. It goes
through over rows without them, and keeps those. This read-only query counts
the rows that hold it back:

```sql
SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes
 WHERE target_environment IS NOT NULL;
```

Those rows also need `20261114030002`'s down to wait, which it does while the
table holds any row. An image-only rollback runs no down, so once that count
is nonzero, roll the binary back only to a release that honors recorded
finality (see "Upgrading a sandbox's runtime" in
`docs/execution-environments.md`).

## Console password provisioning

The password hash marker `!` locks console password login and rejects existing
console sessions. It does not disable independently issued API keys. Operators
can set a password for an existing user without changing that user's role or
organization:

```bash
read -rsp 'New console password: ' SPACE_USER_PASSWORD; echo
export SPACE_USER_PASSWORD
space-db-migrator set-user-password --database-url "$DATABASE_URL" --email "$USER_EMAIL"
unset SPACE_USER_PASSWORD
```

The command requires database access, accepts no password argument, and reads
`SPACE_USER_PASSWORD` without a default. It atomically updates the password and
revokes that user's console sessions. `--if-locked` only provisions a locked
account, preserving passwords and sessions on subsequent bootstrap runs. The
Kind bootstrap script and Tilt provision their local console fixture after
migrations; that development password must not be used in shared environments.

## UUID identifier generation

New SPACE-owned durable identifiers use UUIDv7. Generate application-owned IDs
in application code with `Uuid::now_v7()`. When Postgres owns generation, use
`space_uuid_v7()` in defaults, backfills, triggers, and SQL expressions. Do not
use `gen_random_uuid()` or `uuid_generate_v4()` for an identifier, and do not
make a v4 value look like v7 by changing only its version nibble.

UUIDv4 remains valid when a concrete privacy or protocol requirement forbids an
identifier from revealing its creation time. Put the justification on the same
line as the Postgres generator so the migration guard and reviewer see it:

```sql
DEFAULT gen_random_uuid() /* space-allow-uuid-v4: tenant-visible id must not disclose creation time */
```

Tokens, nonces, UUIDv5 namespaces, stable fixtures, and externally owned UUIDs
are not durable SPACE-owned identifiers. Choose their representation and
generator from their own security or protocol requirements. UUIDv4 by itself is
not a substitute for a cryptographic secret.

When converting an existing default, add a new migration that changes future
generation only. Do not edit applied migrations or rewrite existing primary and
foreign keys solely to change their UUID version. Mixed-version tables must use
an explicit timestamp column when exact age or event ordering matters: UUIDv7
provides millisecond locality, not authoritative ordering across generators,
hosts, clock skew, or multiple values created in one millisecond.

`ALTER COLUMN ... SET DEFAULT` does not rewrite table rows, but `ALTER TABLE`
still needs a brief `ACCESS EXCLUSIVE` lock. Review table traffic, use a short
`lock_timeout`, and split unrelated or hot table groups so a wait on one table
does not hold locks across the entire schema.

Keep that `lock_timeout` short, set it in the file, and pair it with a
`statement_timeout` that is comfortably larger. Those three together are what
buy the migration its retries: `space-db-migrator control-plane up` (and the
re-apply after `reset`) retries a migration that loses a lock race, up to five
attempts spaced 1s, 2s, 4s, 8s, within a five-minute window measured from the
first failure.

Each part matters. **In-file** — a migration that sets no `lock_timeout`
inherits the session's, which is `space_migrator`'s role-level five minutes, so
its second attempt exhausts the window and the migration gets one retry rather
than four. That is about right for a lock nobody won in five minutes, and it is
why the window exists: five attempts at that bound would run one migration to
about 1,515s — 1,215s of that spent retrying — which is most of the migration
Job's 30-minute `activeDeadlineSeconds`, and a Job killed by its deadline
reports nothing about why. **Larger `statement_timeout`** — whichever bound
is shorter fires first, and `statement_timeout` raises `57014`
(`query_canceled`), not `55P03`. Set them equal and the retry silently never
fires for the one migration class it exists for.

A `-- no-transaction` migration cannot follow this advice: it must be a single
statement, so it carries no in-file bounds and always inherits the session's.
It is also never retried, for the reason below. Those two facts travel
together — the migrations that cannot set their own bounds are the ones the
retry does not cover.

One more gap to know about: a deadlock raises `40P01`, not `55P03`, and is not
retried either. `deadlock_timeout` is 1s, at or below every in-file bound the
corpus uses (`1s` and `5s`), so a file that holds `ACCESS EXCLUSIVE` from an
earlier statement while a later one waits can lose that way rather than to its
own `lock_timeout`. A single DDL statement is mostly safe from this, but not
inherently — one statement can still lock several objects, as
`ALTER TABLE a ADD CONSTRAINT ... REFERENCES b` does, and that can cycle with a
session taking the same objects in the other order.

The inherited five minutes assumes `space_migrator` exists. When it does not,
`assume_migrator_role` warns and leaves the session on the connecting role,
which normally means `lock_timeout = 0`. A migration that sets its own bound —
`SET LOCAL` or not — is unaffected and still retries as usual. One relying on
the session's waits unbounded instead, never raising `55P03`, so it neither
retries nor times out. `-- no-transaction` files are stuck in that branch —
they cannot carry a bound of their own — except on a replay run, where any that
are ordered after the leaks described below inherit those instead.

Do not read that as local-only. The migration Job runs `bootstrap-roles` only
when the `space-db-bootstrap-roles` secret already exists in the cluster;
`run-space-db-migrate.sh` degrades to migrate-only when it does not, and logs
that it did. A new cluster's first deploys, before that ExternalSecret lands,
run migrations with no `space_migrator` and no bound. Check for that log line
before assuming a hung migration is contention.

Do not assume the role-level bounds apply mid-run. Eight up migrations open
with a non-`LOCAL` `SET lock_timeout = '1s'` *and* `SET statement_timeout =
'5s'`, and a non-`LOCAL` `SET` that commits persists for the rest of the
session — while SQLx runs every migration in one run on one session. So on any
run that replays them (`reset`, a fresh cluster, a devbox bootstrap) every
later migration without its own bounds inherits 1s and 5s. The
`statement_timeout` leak is the one that bites: a later index build or table
rewrite needing more than 5s dies with `57014`, which is deliberately not
retried. Prefer `SET LOCAL`, which reverts with the transaction.

The retry fires only on SQLSTATE `55P03` (`lock_not_available`), and only for a
transaction-wrapped migration failing its own statement, so an invalid
statement still fails on the first attempt. Retrying is what absorbs a
transient lock holder, not a longer per-attempt wait. Two more consequences for
a migration author. A transaction-wrapped migration is re-run from the top,
which is safe because SQLx rolls the file and its `_sqlx_migrations` row back
together. A `-- no-transaction` migration is not retried at all, because a
failed `CREATE INDEX CONCURRENTLY` can leave an INVALID index behind with no
history row, and recovering that needs a `DROP INDEX CONCURRENTLY` and a rerun.

[The migration lint guard](../scripts/check-postgres-migrations-squawk.sh)
checks changed `db/postgres/*.up.sql` files with Squawk. Squawk catches generic
Postgres migration hazards such as unsafe constraints, non-concurrent indexes,
and statements that conflict with transaction-wrapped migration tools. Treat it
as a baseline check, not a full production-risk review.

The same guard runs SPACE-specific checks before Squawk. It rejects UUIDv4
generation in changed Postgres up migrations unless the inline exception above
has a non-empty reason. Down migrations may restore the previous default for a
rollback.

The repository guard rejects partial indexes in both forward and rollback
migrations: any `CREATE INDEX` statement with a `WHERE` predicate. Prefer a
non-partial index or redesign the access path. There is no rollback exception,
so a down migration must not recreate a partial index removed by its up
migration. The initial public baseline preserves inherited indexes and uniqueness
semantics; only its exact reviewed content is exempt from the new-migration lint.

The same guard lane also rejects PRs that combine `db/postgres/*.sql`
migrations with runtime/deployable app changes. Land Postgres migrations in a
separate PR from application behavior changes so lock risk, rollout, and
rollback are reviewed independently. Docs, tests, and migration-guard
maintenance can travel with migration-only PRs.

The sole production-code exception is the credential fence required by a
credential-table migration. That PR may change the canonical table-name entries
inside the multiline `CREDENTIAL_TABLES` declaration in
`crates/l30_storage/db/migrate/src/bootstrap_roles.rs`; formatting-only changes and
changes outside that list remain blocked. Removing an entry also requires a
matching standalone `DROP TABLE` statement in a changed Postgres up migration.

The same PR may also change entries inside `RO_COLUMN_DENYLIST`, because the
bootstrap-role test requires every hash/bearer column to be fenced from
`space_ro` and every fenced relation to exist, so the fence and the migration
that creates, adds, or renames such a column cannot land separately. Adding a
table entry or adding a column to an existing entry is allowed. Dropping a
fenced column from an entry is not, and removing a whole entry requires a
matching standalone `DROP TABLE` or `DROP VIEW` statement in a changed Postgres
up migration. Each entry must stay on one line in the canonical
`    ("table", &["column", ...]),` form.

The exact Postgres bootstrap-role test file,
`crates/l30_storage/db/migrate/src/bootstrap_roles/tests/postgres_bootstrap_tests.rs`,
may append regression coverage in the same PR, but its existing cross-checks
must remain byte-for-byte unchanged and appended code cannot shadow the fence
constants. This is not a general exception for the bootstrap-role test tree;
that tree and unrelated role-provisioning or application changes remain
separate.

`sandboxes` is a hot production table. Do not put whole-table data movement such
as `UPDATE sandboxes ...`, `DELETE FROM sandboxes ...`, or `INSERT INTO
sandboxes ... SELECT ...` in SQLx startup migrations. Backfills on hot tables
belong in bounded jobs or maintenance runbooks, with production row count,
write-QPS, lock behavior, and rollback reviewed before deploy.

Ordinary SQLx Postgres migrations run inside a transaction, so Squawk is invoked
with `--assume-in-transaction`. A migration that must run outside a transaction,
such as `CREATE INDEX CONCURRENTLY`, must be a single-statement migration and
must include Squawk's transaction override comment:

```sql
-- squawk-disable-assume-in-transaction
CREATE INDEX CONCURRENTLY ...
```

Common SQLite DDL lives under `db/common/`. Its public catalog starts at
`20260930000000_baseline`; later common and service migrations use timestamped
up/down pairs above that floor. The shared catalog is consumed by sandbox,
volume, template, build, and runtime-manager stores.

Public-catalog startup validates both service and shared histories before
writes, then applies common schema before service schema. Failed, modified,
missing, or unbundled applied versions are rejected without recreating the
database, except a successful version newer than everything the catalog
bundles, which a binary rollback leaves applied as described under 'Rolling
back past a newer SQLite migration'. Some earlier template-manager releases
reverse `20261026220935` and `20261031050126` themselves instead, as described
under 'Node dirty-ref revision migration' and 'Template cache partition
migration'. A template-manager migration must also leave the database readable
and writable by the release before it: a rollout can run
`space-db-migrator template-manager up` in an init container while the
previous release's pod still serves from the same node-local database. Use
expand/contract sequencing and retain verified backups.

The migrator never adopts untracked schemas, repairs rejected history, or
recreates a database after a migration failure. Baseline history conversion is
unsupported. Preserve rejected databases for a reviewed state-preserving recovery
plan; never edit history rows to bypass validation.

Image-only rollback to a historical catalog is unsupported for every public
SQLite database, including ones created on new nodes. Older binaries can insert
legacy migration rows; older template-manager binaries can recreate the store
and lose authoritative local policies and generations. Before any new writes,
stop all writers and restore the verified original file set, including its
matching WAL/SHM state, before starting the old binary. After new writes or an
accidental old-image startup, retain the affected files and use a reviewed
state-preserving recovery plan. Never edit history rows to bypass validation.

For stores with shared jobs, `down` removes the greatest applied timestamp across
both catalogs, choosing the service migration first when timestamps match.
Migration timestamps must reflect release/dependency order. The migration
version guard enforces this for SQLite catalogs too. An up migration added or
moved directly into a service catalog `db/<store>/` must be newer than the
newest version on the base branch in both that catalog and `db/common/`. One
added or moved into `db/common/` must be newer than the newest version on the
base branch in every SQLite catalog, not only the consumers. `history/`
subdirectories are not part of a catalog, and a catalog absent from the base
branch is not compared. A branch overtaken by a newer migration renumbers on
rebase, as Postgres branches already do. Baseline down migrations reject
rollback below the supported schema floor.

The `jobs` table belongs to `db/common/`. Add shared tables, columns, and indexes
there instead of copying jobs DDL into service directories. Service-owned
objects that depend on common tables, such as a consumer-only index, stay in
the service catalog. Daemons and the CLI use the shared migration startup path;
new migrations must not rely on the retired service-to-common history stamping.

## Control-plane Postgres roles (`bootstrap-roles`)

Control-plane role provisioning is owned by `space-db-migrator
bootstrap-roles`, not by SQL migrations. The command is convergent and safe to
re-run; provisioning flows run it **before and after** `control-plane up` —
before so the roles exist for migrations to connect as, after so tables the
migrations just created get their read grants and the credential fence:

```bash
SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD=... \
SPACE_DB_BOOTSTRAP_USER_PASSWORD=... \
SPACE_DB_BOOTSTRAP_RO_PASSWORD=... \
SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD=... \
SPACE_DB_BOOTSTRAP_CREDS_PASSWORD=... \
  space-db-migrator bootstrap-roles --database-url "$ADMIN_URL"
space-db-migrator control-plane --database-url "$MIGRATOR_URL" up
space-db-migrator bootstrap-roles --database-url "$ADMIN_URL"
```

`$ADMIN_URL` must authenticate as a role that can create roles (the RDS
master user or a local superuser). `$MIGRATOR_URL` authenticates as
`space_migrator`; migrations are expected to run as that role.

Provisioned roles:

| Role | Purpose |
| --- | --- |
| `space_superadmin` | Break-glass operator login. `CREATEDB CREATEROLE` plus `rds_superuser` on RDS (a true `SUPERUSER` only on self-managed Postgres) plus `space_migrator` membership. Distinct from the unmanaged RDS master user. |
| `space_migrator` | DDL login for migrations. Owns every application relation in `public`; pre-existing relations are adopted on each run. Its new tables are auto-granted to the other logins via default privileges. Minutes-scale role-level `statement_timeout` (default `10min`) and `lock_timeout` (default `5min`), tunable via `--migrator-statement-timeout` / `--migrator-lock-timeout`. |
| `space_user` | Application login: read/write DML on every table except credential tables. |
| `space_ro` | Read-only query login for humans and dashboards; same credential fence as `space_user`. |
| `space_creds` | Credential-manager login: read/write DML on every table including credential tables; also a member of the `space_credmgr` capability role whose table grants live in migrations. |

`space_user`, `space_ro`, and `space_creds` all run under short role-level
session timeouts: `statement_timeout` (default `10s`) and
`idle_in_transaction_session_timeout` (default `15s`), tunable via
`--app-statement-timeout` / `--app-idle-transaction-timeout`.

### Bounding application lock waits

`space_user`, `space_ro`, and `space_creds` all carry an optional role-level
`lock_timeout`, configured through `bootstrap-roles --app-lock-timeout` or
`SPACE_DB_APP_LOCK_TIMEOUT`.

**Omitting the value is not the same as `0`.** An omitted value leaves each
role's existing `lock_timeout` alone; an explicit `0` disables the bound. This
lets bootstrap commands that carry no timeout configuration preserve the
cluster's setting. To disable an existing bound, pass `--app-lock-timeout 0`.

**Enable the bound before deploying migrations.** `ALTER ROLE … SET` applies
at login, so existing pooled connections retain their previous setting.
Configure the bound, reconnect the application pools, and only then deploy DDL.
Running role bootstrap immediately before migrations does not protect those
migrations from unbounded waits in existing application sessions.

Without a lock timeout, a statement blocked on a lock waits out
`statement_timeout`, holding its pooled connection for the whole wait. During
DDL on a hot table, enough blocked queries can occupy the entire pool while
the database itself is idle; unrelated queries then fail on acquire timeout.
Bounding the wait at the role covers every call site, including plain
`SELECT`s that only block because of DDL.

DDL is not the only source of contention. `node_status_snapshots` references
`nodes` with `ON DELETE CASCADE`, so deleting a node also deletes its status
snapshots and holds locks on the parent rows until the transaction completes.

The wait that reaches the application is row-level, not table-level: the
`RowExclusive` the cascade takes on `node_status_snapshots` does not conflict
with an ordinary `SELECT` or with another writer's `RowExclusive`. What blocks
is a statement wanting a conflicting lock on the *same row* — an
`INSERT INTO node_status_snapshots` takes `FOR KEY SHARE` on the parent `nodes`
row the terminator is deleting, and waits for it. Size the bound against that,
not against a table-wide stall.

`space_ro` gets the same bound: it cannot take row locks at all — `FOR UPDATE`
and `FOR SHARE` need `UPDATE`/`DELETE`/`TRUNCATE` privilege — so in practice it
waits on a DDL conflict, which is exactly the wait worth failing fast. (An
advisory lock is the exception: `pg_advisory_lock` is executable by `PUBLIC`, so
a read-only session can wait on one if someone writes a query that does.) `space_migrator` is
deliberately excluded and keeps its minutes-scale `lock_timeout` — DDL
legitimately needs to wait.

The bound converts lock waits into `55P03` (`lock_not_available`) errors, so
every path that can block needs a disposition for it.
`pplx_space_postgres_pool::contention` is where the codes live for the
services: callers use `is_lock_contention` (`40001` / `40P01` / `55P03`),
`is_lock_not_available` (`55P03` only) or `is_query_canceled` (`57014`) rather
than open-coding a SQLSTATE list, because a hand-written
`matches!(code, "40001" | "40P01")` silently omits the one code this bound
introduces.

`space-db-migrator` is the exception and keeps its own literals: its DDL
recovery keys off `55P03` alongside `42501`/`55000`, which are not contention,
and `migration_retry` deliberately narrows to `55P03` on a single statement
kind. Those are different questions about the same code, not a second copy of
this one.

Three dispositions cover every path, and which one applies is a property of
the caller, not of the statement:

- **Retryable** — the caller has a retry loop or the client is told to retry.
  This covers the api crate's substrate store, its controller
  boundaries, the ECW and public surfaces, and the credmgr authority (tonic
  `Unavailable`) — but *not* every `sqlx` boundary in the crate; see the
  pitfalls under **Fatal**. The ones with
  their own reasoning worth keeping: the finalize-delete path (bounded retries
  *and* a 503 when the budget is exhausted, since exhausting it does not make
  contention permanent), the auto-lifecycle receipt consumer (defer the
  receipt), and the Direct route reservation, which classifies
  `Io`/`PoolTimedOut`/`57014` as retryable-but-not-contention so they do not
  get the contention `Retry-After`. A guard that runs before any teardown is
  always safely retryable.

  The admin handlers that route through `support::db_error` — node groups,
  template visibility, node maintenance, nodes, warm pools — split the two
  classes: `40001`/`40P01` answer 409, because those mean this request's own
  writes raced another interleaved write and "retry the request" describes it,
  while `55P03` answers 503, because any statement can raise it including a
  plain read on a GET that blocked behind DDL. Handlers carrying their own
  error envelope reach that split through `support::contention_disposition`
  rather than restating it, passing the `ContentionScope` their statement sat
  at: `Writing` earns the 409, `ReadOnly` withholds it and answers every
  contention SQLSTATE with the 503. `identity.rs` passes `ReadOnly` for a
  blanket conversion that covers both its master-key write and its plain reads
  — under-claiming, because telling a read that its own write lost a race is
  the one answer that is false rather than merely imprecise. Any future admin
  handler that neither returns `db_error` nor goes through that helper is not
  covered; check before assuming.
- **Degradable** — a periodic reconciler or a node-driven RPC. The tick
  fails, nothing is written, and the next tick retries; the failure is a
  latency blip, not lost state. The internal-api node RPCs and the
  cluster-controller reconcilers are here, as is `template_visibility()`, which
  degrades to `UNSPECIFIED` on any error and never fails release resolution —
  `55P03` reaches it as `template.visibility_lookup.degraded{reason:query_failed}`,
  which is the cheap confirmation after enabling that the lookup fails fast
  instead of hanging. No reconciler classifies `55P03`,
  and none needs to *if* it takes its guard lock in the same transaction as the
  write it guards — then a `55P03` aborts the transaction before the write, and
  the sweep skips that row rather than acting on a read it could not lock. That
  was checked on the orphaned-sandbox sweep, not proven for every reconciler; a
  reconciler that reads unlocked and writes afterwards has no such property, so
  check the transaction boundary before assuming this bucket applies.
- **Fatal** — transient contention must not become a permanent-looking
  `Internal`/500 response. Check each error boundary, including handlers with
  custom error envelopes: even a statement that takes no explicit lock in SQL
  can block behind `ACCESS EXCLUSIVE`.

  Two shapes are easy to miss: a handler that maps through a local
  `|error: &dyn fmt::Display|` closure erases the `sqlx::Error` at the map site,
  and an error enum that gains a `sqlx`-wrapping variant can inherit an
  inappropriate catch-all conversion.

Inside the substrate the classification is reached through an extension trait,
`DbContext::db_context`, rather than a bare `map_err` at each site. That is
partly ergonomics — one line per call site instead of a five-line closure — but
mostly a type check: the trait is implemented only for `Result<_, sqlx::Error>`
and for the warm-pool store's error, so reaching for it on a `kube` or `tonic`
result does not compile, and the compiler rather than review is what keeps the
classification on database errors. A `SubstrateError::Failed` built directly
from a `sqlx::Error` is the regression to look for. `direct.rs` keeps one
hand-written mapper on top of it, because its route reservation classifies
`Io`/`PoolTimedOut`/`57014` as retryable-but-not-contention.

Finding a gap like this is a matter of reading the error mapping, not the SQL:
the statement looks ordinary and the disposition lives at the boundary, often
two crates away. When adding a call site, check what its error type becomes at
the HTTP or gRPC edge. Look for error enums that wrap `sqlx::Error` but do
not classify contention, including session-auth login, admin identity,
credential routes, ECW template builds, federated-grant upserts, and credmgr
status mappers.

`space_creds` carries the bound too. Credential upserts use
`INSERT ... ON CONFLICT`, which can wait on a concurrent unfinished insert of
the same key. Their callers need a retryable response (`Unavailable` from
credmgr, 503/409 from the admin handlers, or 503 from credential routes);
otherwise the timeout trades a slow success for a fast permanent-looking
failure.
`space_migrator` is bounded too, at its own longer value (`migrator_lock`,
default 5min) because migrations hold DDL locks and rewrite tables. The one
managed login role that carries neither `statement_timeout` nor `lock_timeout`
is `space_superadmin`.

Two shapes need care when adding code:

- **Advisory locks are bounded too.** `pg_advisory_xact_lock` goes through
  the same heavyweight lock manager as a table or row lock. The public-API
  idempotency guards and the fork-source lifecycle guard are built on it and
  *want* to wait for a peer transaction, so their callers must treat `55P03`
  as retryable — the serialization is still correct, it just fails fast.
- **A transaction-local raise applies to the whole transaction.**
  `set_config('lock_timeout', …, TRUE)` is transaction-scoped, not
  statement-scoped. Raising the bound to wait out one acquisition also raises
  it for every later statement in the same transaction, which is why the
  guards above classify rather than raise. The one deliberate local override
  is the auto-lifecycle receipt consumer: it sets `2s` for the remainder of
  the node publication transaction, which was a *lower* bound before this knob
  existed and becomes a *raise* once the role bound is sub-second. That is
  intended — the statements it covers already hold a batch of row locks, so
  giving up on one contended row is more expensive there than waiting — but it
  means the effective bound on that path is its own constant, not the role's.

### Application pools

Long-running services connect only as the managed logins, with no selector
and no fallback to the connecting admin. `pplx-space-postgres-pool`
exposes three constructors: `connect_space_user_pool` (the service's
general-purpose pool, as `space_user`), `CredentialsPool::connect` (the
credential-fenced lane, as `space_creds`), and
`connect_space_user_sideline_pool_lazy` (a *sideline* pool, also `space_user`,
with an explicit connection cap). A sideline pool exists for one reason: a
query that shares the database with load-bearing traffic but must never be
able to starve it. On a shared pool a sideline that blocks — a relation lock
taken by DDL is the usual way — holds a connection until `statement_timeout`,
and once the general pool is drained the load-bearing queries start failing
their own acquire. A capped pool turns that cross-workload failure into a
self-contained one. Sideline pools deliberately emit no `db.pool.*` metrics,
because those are tagged by `database_identity` alone and a small pool's
`utilization` ratio would pollute the general pool's saturation signal;
observe them through their caller's degradation metric instead. Both read their required password
from `SPACE_DB_USER_PASSWORD` / `SPACE_DB_CREDS_PASSWORD` and fail startup
when it is missing, so a misconfigured deployment stops at pod start rather
than at the first query. `DATABASE_URL` supplies only the endpoint (host,
port, database, TLS); its embedded admin credentials are never used as a
pool login.

Per service: space-api and space-cluster-controller hold both pools (their
credential and snapshot-encrypted-fields statements run on the
`CredentialsPool`); space-cluster-api holds the general `space_user` pool plus
a capped `space_user` sideline pool for the best-effort template-visibility
lookup (that lookup reads `templates`, which its release queries do not, so
sharing one pool would let DDL on that table starve release resolution); the
credmgr authority holds only the `space_creds` pool. The `CredentialsPool`
newtype keeps the lanes distinct in signatures — fenced statements, and any
transaction that mixes fenced and unfenced ones, must begin and stay on it.
Do not derive a database identity from request data, pass role-name strings
through business logic, or use `space_admin`/`space_superadmin` to bypass a
missing grant. `space_ro` remains provisioned for humans and dashboards; no
service connects as it.

Rules that follow from this model:

- **The connecting admin is never managed.** The RDS master user (named
  `space_admin` in SPACE deployments) is the credential live services
  authenticate with; `bootstrap-roles` refuses to run if any managed role
  name matches the session user, and no password is minted for it.
- **Migrations always run as `space_migrator`.** `control-plane up`/`down`
  assume the role via `SET ROLE` on every connection when it exists, even if
  the connection authenticates as the admin, so migration-created objects
  are always migrator-owned and its default privileges fire. If the role
  exists but cannot be assumed, migration fails loudly instead of silently
  running as the admin; only databases where `bootstrap-roles` has never run
  fall back to the connecting identity (with a warning).
- **New roles belong in `bootstrap-roles`, not in migrations.** Migrations run
  as `space_migrator`, which has no `CREATEROLE`; a `CREATE ROLE` statement in
  a migration will fail on fresh databases. (`bootstrap-roles` pre-creates
  `space_credmgr`, so the guarded legacy migration that introduced it no-ops.)
- **New credential-bearing tables must be added to `CREDENTIAL_TABLES`** in
  `crates/l30_storage/db/migrate/src/bootstrap_roles.rs`, in the same PR as the
  migration that creates them. Until the next `bootstrap-roles` run, a new
  `space_migrator`-created table is accessible to `space_user` and `space_ro`
  through default privileges — the trailing bootstrap run is what enforces
  the fence.
- Role passwords come only from the `SPACE_DB_BOOTSTRAP_*_PASSWORD`
  environment variables. In AWS they are minted by the postgres Terraform
  stack into the `${prefix}postgres-bootstrap-role-credentials` Secrets
  Manager secret (JSON keyed by these exact env-var names);
  local operators must supply the role passwords for their environment.
  `--allow-missing-passwords` converges roles and grants without touching
  LOGIN state, for grants-only runs.
- Long-running services receive only the managed passwords their pools use:
  API/admin/controller get the `space_user` and `space_creds` passwords;
  cluster API gets `space_user`; the credmgr authority gets `space_creds`.
  The Helm charts project them from the `space-db-bootstrap-roles` secret
  with `optional: false`. Direct-node systemd mirrors this with
  per-component env files; migrator, operator/bastion, and Datadog
  credentials remain separate.
- External database inventories do not provision SPACE roles or own their credentials.
