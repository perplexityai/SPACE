# pplx-space-db-test-fixtures

Shared Postgres and Redis fixtures for SPACE tests. `docs/rust/space-conventions.md`
("Service-dependency tests") describes how tests reach them and what happens
when a dependency is unavailable.

## Schema variants

A schema variant is a candidate schema change that the migrated test template
takes on after the migrations, so the existing Postgres suites can show
whether today's code works unchanged against it before anyone writes it as a
migration. Each variant's SQL lives in [`schema-variants/`](schema-variants/).
It is a test candidate, not a migration: nothing applies it to a real
database.

A run selects a variant with `SPACE_TEST_POSTGRES_SCHEMA_VARIANT`. Unset or
empty leaves the template exactly as the migrations make it. An unknown name,
or a value that is not UTF-8, fails template creation before the migrations
run, so a misspelled variant cannot pass as a run against it.

| Variant | SQL | What it changes |
| --- | --- | --- |
| `execution-record-views` | [`execution_record_views.sql`](schema-variants/execution_record_views.sql) | Renames `sandbox_k8s_executions` to `sandbox_execution_records` and `node_k8s` to `node_identities`, renames their columns `pod_materialized_at` to `materialized_at` and `k8s_node_name`/`k8s_node_uid` to `name`/`incarnation_uid`, adds their substrate columns, makes `pod_name` and `pod_namespace` nullable, and leaves invoker compatibility views with `WITH CASCADED CHECK OPTION` under the old names. Grants `space_credmgr` base `SELECT` on every column the views read. |

### Run the suites against a variant

`cargo nextest run` builds the template once per run in its Postgres setup
script, which reads the variable from the environment nextest runs in:

```bash
SPACE_TEST_POSTGRES_SCHEMA_VARIANT=execution-record-views \
  cargo nextest run --workspace --run-ignored all \
    -E 'rdeps(pplx-space-db-test-fixtures) and not group(privileged)'
```

`rdeps(pplx-space-db-test-fixtures)` selects the tests of every package that
uses these fixtures, so a package that starts using a variant's tables later is
included without editing this command.

Run the same command without the variable for the default schema, and compare
the failures. Plain `cargo test` reads the variable when its process-local
container builds the template, unless `SPACE_TEST_POSTGRES_ADMIN_URL` names an
existing server, whose template it uses as it is.
`TESTCONTAINERS_POSTGRES_IMAGE` picks the server version as usual.

### Write variant-aware tests

The fixtures connect as the superuser, so a variant run checks privileges only
where a test switches role, as the credential-injection store's resolution test
does with `SET LOCAL ROLE space_credmgr`. The migrator pins below cover the
bootstrap-roles logins. Test DDL that only a table accepts, such as a row
trigger, `TRUNCATE` or `ALTER TABLE`, names the relation through
`pplx_space_db_test_fixtures::physical_table("sandbox_k8s_executions")?`,
which returns the renamed table under a variant that turned the name into a
view and the name itself otherwise. Statements the application issues stay as
they are: a production statement that fails against a variant is the finding
the variant exists to surface.

Once the real migration makes a variant's change, the default `physical_table`
result names a view and the pins' candidate no longer applies. The
migration-isolation guard rejects changes to `physical_table`, the api test
helpers and the pins in a migration PR, so before that migration a crates
change must make them work on either schema.

### Pins under the application roles

`crates/l30_storage/db/migrate/src/bootstrap_roles/tests/execution_record_view_tests.rs`
applies `execution-record-views` as `space_migrator` on a server that
bootstrap-roles and `control-plane up` prepared, and runs the statement shapes
today's code and its tests depend on as the real logins: conflict handling, the
check option, row and table locks, statements prepared before the rename, and
the invoker views' privilege checks. Their server comes from Docker: under
nextest, one the `bare-postgres` setup script starts for all of them, each
test on a database of its own; under `cargo test`, a container per test. So
they are in the nextest `privileged` group. Run them on each supported server
version:

```bash
for image in \
  public.ecr.aws/docker/library/postgres:16-alpine \
  public.ecr.aws/docker/library/postgres:17-alpine; do
  TESTCONTAINERS_POSTGRES_IMAGE="${image}" cargo nextest run \
    -p pplx-space-db-migrate -E 'test(/^bootstrap_roles::tests::execution_record_view_tests::/)'
done
```

No CI job runs the variant: it would repeat the whole Rust test job. Run it by
hand when changing a variant or the code paths it covers.
