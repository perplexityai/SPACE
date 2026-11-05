//! Pins for the execution-record compatibility views, the test candidate in
//! `pplx-space-db-test-fixtures`' `schema-variants/execution_record_views.sql`.
//!
//! The shared fixtures run every Postgres suite against that candidate as the
//! superuser. These run the statement shapes today's code and its tests depend
//! on as the application logins bootstrap-roles creates, on a server that
//! migrated the way a deployment does, so they also cover privileges: the views
//! run with the caller's (`security_invoker`). Run them on each supported
//! server version through `TESTCONTAINERS_POSTGRES_IMAGE`.
//!
//! Under nextest the pins share one bare server (`.config/nextest.toml`). The
//! first to arrive bootstraps and migrates a template database on it, and
//! each pin clones that into a database of its own. What they still share is
//! the cluster-global state: the roles bootstrap provisions, and
//! [`CREDMGR_PROBE_ROLE`], which the template build creates. A pin must not
//! change cluster-global state; the invoker pin's second bootstrap is safe
//! only because it converges the shared roles to the values the template
//! build gave them.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, SchemaVariant};
use sqlx::Row as _;

use super::super::*;
use super::postgres_bootstrap_tests::run_bootstrap;
use crate::pg_test_support::{PostgresFixture, PostgresServer, TestResult, bare_postgres_server};

/// A login holding only the credential manager's capability role, so its reads
/// see exactly the column grants migrations give `space_credmgr`. `space_creds`
/// also holds every service privilege on these tables, which would hide them.
/// The template build creates it, not bootstrap, once per server, since roles
/// are cluster-wide.
const CREDMGR_PROBE_ROLE: &str = "credmgr_probe";

/// The database every pin clones; see [`migrated_fixture`].
const MIGRATED_TEMPLATE: &str = "execution_record_pins_template";

/// Session advisory lock that serializes building [`MIGRATED_TEMPLATE`]
/// among the pins sharing a server.
const TEMPLATE_LOCK: i64 = 0x7069_6e73;

/// The node inventory's upsert (`store/node_inventory.rs` in the api crate).
const NODE_INVENTORY_UPSERT: &str = sql!(
    "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
     VALUES ($1::uuid, $2, $3)
     ON CONFLICT (node_id) DO UPDATE
     SET k8s_node_name = EXCLUDED.k8s_node_name,
         k8s_node_uid = EXCLUDED.k8s_node_uid,
         updated_at = NOW()
     WHERE node_k8s.k8s_node_name IS DISTINCT FROM EXCLUDED.k8s_node_name
        OR node_k8s.k8s_node_uid IS DISTINCT FROM EXCLUDED.k8s_node_uid"
);

/// The execution claim's shape: a targetless `ON CONFLICT DO NOTHING`, which
/// arbitrates every unique index on the base table: one active execution per
/// sandbox, `(sandbox_id, generation)` and `pod_name`.
const EXECUTION_CLAIM_INSERT: &str = sql!(
    "INSERT INTO sandbox_k8s_executions (
         sandbox_id, generation, pod_name, pod_namespace, state, active
     )
     VALUES ($1::uuid, $2, $3, 'sandboxes', 'creating', TRUE)
     ON CONFLICT DO NOTHING
     RETURNING generation, pod_name, state, pod_materialized_at IS NULL AS unmaterialized"
);

/// The credential manager's storage-vend node lookup
/// (`storage_vend_store.rs` in the credmgr crate), without its target-node and
/// snapshot legs, which read only `sandboxes` and `snapshots`.
const STORAGE_VEND_NODE_LOOKUP: &str = sql!(
    "WITH latest_execution AS (
         SELECT k8s_node_name
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1::uuid AND k8s_node_name IS NOT NULL
         ORDER BY generation DESC
         LIMIT 1
     ), candidate_nodes AS (
         SELECT active_node_id AS node_id FROM sandboxes WHERE id = $1::uuid
         UNION
         SELECT k.node_id
         FROM latest_execution e
         JOIN node_k8s k ON k.k8s_node_name = e.k8s_node_name
     )
     SELECT k.k8s_node_name, k.k8s_node_uid
     FROM candidate_nodes candidates
     JOIN node_k8s k ON k.node_id = candidates.node_id
     WHERE k.k8s_node_uid IS NOT NULL"
);

/// Both execution claims' generation allocation (`execution_rows.rs` in the api
/// crate, `postgres.rs` in the placement crate), which reads through the
/// view.
const EXECUTION_GENERATION_ALLOCATION: &str = sql!(
    "SELECT COALESCE(MAX(generation), 0) + 1
     FROM sandbox_k8s_executions
     WHERE sandbox_id = $1::uuid"
);

/// The credential-injection resolution's identity join
/// (`resolution.rs` in the credential-injection store).
const INJECTION_IDENTITY_JOIN: &str = sql!(
    "SELECT e.generation
     FROM sandboxes s
     JOIN node_k8s k ON k.node_id = s.active_node_id AND k.k8s_node_uid IS NOT NULL
     JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         AND e.k8s_node_name = k.k8s_node_name
     WHERE s.id = $1::uuid"
);

/// A database of this test's own, cloned from a bootstrapped database migrated
/// through `control-plane up` and converged by a second bootstrap, as a
/// deployment leaves it before the candidate migrates.
async fn migrated_fixture() -> TestResult<Option<PostgresFixture>> {
    let Some(server) = bare_postgres_server().await? else {
        return Ok(None);
    };
    let mut admin = server.connect_admin().await?;
    sqlx::query(sql!("SELECT pg_advisory_lock($1)"))
        .bind(TEMPLATE_LOCK)
        .execute(&mut admin)
        .await?;
    // Marked a template only once complete, so a pin that failed while
    // building it leaves a database the next one drops and builds again.
    let built: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT FROM pg_database WHERE datname = $1 AND datistemplate)"
    ))
    .bind(MIGRATED_TEMPLATE)
    .fetch_one(&mut admin)
    .await?;
    if !built {
        build_migrated_template(&server, &mut admin).await?;
    }
    let database = format!("execution_record_pin_{}", uuid::Uuid::now_v7().simple());
    execute(
        &mut admin,
        &format!("CREATE DATABASE {database} TEMPLATE {MIGRATED_TEMPLATE} STRATEGY FILE_COPY"),
    )
    .await?;
    admin.close().await?;
    Ok(Some(server.database(&database).await?))
}

async fn build_migrated_template(server: &PostgresServer, admin: &mut PgConnection) -> TestResult {
    execute(
        admin,
        &format!("DROP DATABASE IF EXISTS {MIGRATED_TEMPLATE} WITH (FORCE)"),
    )
    .await?;
    execute(admin, &format!("CREATE DATABASE {MIGRATED_TEMPLATE}")).await?;
    let template = server.database(MIGRATED_TEMPLATE).await?;
    run_bootstrap(&template).await?;
    crate::run_control_plane(crate::ControlPlaneArgs {
        job_deadline_seconds: crate::migration_retry::MIGRATION_JOB_ACTIVE_DEADLINE.as_secs(),
        database_url: template.database_url("postgres"),
        confirm_data_loss: false,
        command: crate::Command::Up,
    })
    .await?;
    run_bootstrap(&template).await?;
    template.pool.close().await;
    if !role_exists(admin, CREDMGR_PROBE_ROLE).await? {
        execute(
            admin,
            &format!("CREATE ROLE {CREDMGR_PROBE_ROLE} LOGIN IN ROLE {CREDMGR_CAPABILITY_ROLE}"),
        )
        .await?;
    }
    execute(
        admin,
        &format!(
            "ALTER DATABASE {MIGRATED_TEMPLATE} WITH IS_TEMPLATE true ALLOW_CONNECTIONS false"
        ),
    )
    .await?;
    Ok(())
}

/// Apply the candidate as `space_migrator`, the role a migration runs as, so the
/// views get the owner and default privileges a real migration would give them.
async fn apply_candidate(fixture: &PostgresFixture) -> TestResult {
    let mut conn = fixture.connect_as("postgres").await?;
    assume_migrator_role(&mut conn).await?;
    sqlx::raw_sql(SchemaVariant::ExecutionRecordViews.sql())
        .execute(&mut conn)
        .await?;
    Ok(())
}

async fn relkind(fixture: &PostgresFixture, relation: &str) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT relkind::text FROM pg_class WHERE oid = $1::regclass"
    ))
    .bind(relation)
    .fetch_one(&fixture.pool)
    .await?)
}

/// `(name, type)` of each column in order: the row type a statement written
/// against the relation reads.
async fn row_type(fixture: &PostgresFixture, relation: &str) -> TestResult<Vec<(String, String)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT attname::text, format_type(atttypid, atttypmod)
         FROM pg_attribute
         WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped
         ORDER BY attnum"
    ))
    .bind(relation)
    .fetch_all(&fixture.pool)
    .await?)
}

/// A `ready` worker node and a `started` Kubernetes sandbox placed on it,
/// written as `space_user`. Returns `(node_id, sandbox_id)`.
async fn seed_node_and_sandbox(conn: &mut PgConnection) -> TestResult<(String, String)> {
    let node_id: String = sqlx::query_scalar(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES (space_uuid_v7(), '127.0.0.1', 'worker', 'k8s', 'ready')
         RETURNING id::text"
    ))
    .fetch_one(&mut *conn)
    .await?;
    let sandbox_id = seed_sandbox(conn, &node_id).await?;
    Ok((node_id, sandbox_id))
}

async fn seed_sandbox(conn: &mut PgConnection, node_id: &str) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation, active_node_id
         )
         VALUES (space_uuid_v7(), $1::uuid, $2::uuid, 'k8s', 'started', 'on_node', 1, $3::uuid)
         RETURNING id::text"
    ))
    .bind(SEED_USER_ID.to_string())
    .bind(SEED_ORG_ID.to_string())
    .bind(node_id)
    .fetch_one(&mut *conn)
    .await?)
}

async fn claim_execution(
    conn: &mut PgConnection,
    sandbox_id: &str,
    generation: i64,
) -> sqlx::Result<Option<(i64, String, String, bool)>> {
    sqlx::query_as(EXECUTION_CLAIM_INSERT)
        .bind(sandbox_id)
        .bind(generation)
        .bind(format!("space-sbx-pin-{generation}-{sandbox_id}"))
        .fetch_optional(&mut *conn)
        .await
}

async fn upsert_node_identity(
    conn: &mut PgConnection,
    node_id: &str,
    name: &str,
    uid: &str,
) -> sqlx::Result<u64> {
    Ok(sqlx::query(NODE_INVENTORY_UPSERT)
        .bind(node_id)
        .bind(name)
        .bind(uid)
        .execute(&mut *conn)
        .await?
        .rows_affected())
}

fn assert_sqlstate<T: std::fmt::Debug>(result: sqlx::Result<T>, expected: &str) {
    let error = result.expect_err("the statement must fail");
    assert_eq!(
        sqlstate(&error).as_deref(),
        Some(expected),
        "unexpected error: {error}"
    );
}

/// Run `statement` with `id` inside a savepoint of the caller's transaction,
/// assert it fails with `expected`, and roll back to the savepoint so the
/// transaction can go on.
async fn assert_sqlstate_in_savepoint(
    conn: &mut PgConnection,
    statement: &'static str,
    id: &str,
    expected: &str,
) -> TestResult {
    sqlx::query(sql!("SAVEPOINT probe"))
        .execute(&mut *conn)
        .await?;
    assert_sqlstate(
        sqlx::query(statement)
            .bind(id)
            .fetch_optional(&mut *conn)
            .await,
        expected,
    );
    sqlx::query(sql!("ROLLBACK TO SAVEPOINT probe"))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[tokio::test]
async fn views_keep_the_old_row_types_and_writes_reach_the_base_tables() -> TestResult {
    let Some(fixture) = migrated_fixture().await? else {
        return Ok(());
    };
    let executions_before = row_type(&fixture, "sandbox_k8s_executions").await?;
    let nodes_before = row_type(&fixture, "node_k8s").await?;
    apply_candidate(&fixture).await?;

    for (view, base, before) in [
        (
            "sandbox_k8s_executions",
            "sandbox_execution_records",
            &executions_before,
        ),
        ("node_k8s", "node_identities", &nodes_before),
    ] {
        assert_eq!(relkind(&fixture, view).await?, "v", "{view} is a view");
        assert_eq!(relkind(&fixture, base).await?, "r", "{base} is a table");
        assert_eq!(&row_type(&fixture, view).await?, before, "{view} row type");
    }

    // The candidate grants the service logins nothing on the views:
    // `space_migrator`'s default privileges give them their privileges, as for
    // any new relation, before any bootstrap runs again. One privilege per
    // call, since `has_table_privilege` is true if any listed one is held.
    for view in ["sandbox_k8s_executions", "node_k8s"] {
        for (role, held) in [
            (USER_ROLE, [true, true, true, true]),
            (CREDS_ROLE, [true, true, true, true]),
            (RO_ROLE, [true, false, false, false]),
        ] {
            for (privilege, expected) in ["SELECT", "INSERT", "UPDATE", "DELETE"]
                .into_iter()
                .zip(held)
            {
                let actual: bool = sqlx::query_scalar(sql!(
                    "SELECT has_table_privilege($1::name, $2::text, $3::text)"
                ))
                .bind(role)
                .bind(view)
                .bind(privilege)
                .fetch_one(&fixture.pool)
                .await?;
                assert_eq!(actual, expected, "{role} {privilege} on {view}");
            }
        }
    }

    let mut conn = fixture.connect_as(USER_ROLE).await?;
    let (node_id, sandbox_id) = seed_node_and_sandbox(&mut conn).await?;

    // A targetless ON CONFLICT DO NOTHING claims through the view, and a
    // second active claim conflicts on the base table's partial unique index.
    // The pin's RETURNING reads an aliased view column, which production's
    // claim does not.
    let claim = claim_execution(&mut conn, &sandbox_id, 1).await?;
    assert_eq!(
        claim,
        Some((
            1,
            format!("space-sbx-pin-1-{sandbox_id}"),
            "creating".to_string(),
            true
        ))
    );
    assert_eq!(claim_execution(&mut conn, &sandbox_id, 2).await?, None);
    // The base default fills the column the view does not expose.
    let base_row: (String, i64) = sqlx::query_as(sql!(
        "SELECT managed_by, count(*) OVER ()
         FROM sandbox_execution_records WHERE sandbox_id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(base_row, ("k8s".to_string(), 1));

    // An UPDATE through the view writes the renamed base column.
    let materialized: bool = sqlx::query_scalar(sql!(
        "UPDATE sandbox_k8s_executions
         SET pod_uid = 'pod-uid-1',
             pod_materialized_at = COALESCE(pod_materialized_at, NOW())
         WHERE sandbox_id = $1::uuid AND active
         RETURNING pod_materialized_at IS NOT NULL"
    ))
    .bind(&sandbox_id)
    .fetch_one(&mut conn)
    .await?;
    assert!(materialized);
    let base_materialized: bool = sqlx::query_scalar(sql!(
        "SELECT materialized_at IS NOT NULL
         FROM sandbox_execution_records WHERE sandbox_id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .fetch_one(&mut conn)
    .await?;
    assert!(base_materialized);

    // The node inventory's aliased upsert: inserts, skips an unchanged
    // identity through its WHERE on the view's names, updates a changed one.
    assert_eq!(
        upsert_node_identity(&mut conn, &node_id, "node-a", "uid-1").await?,
        1
    );
    assert_eq!(
        upsert_node_identity(&mut conn, &node_id, "node-a", "uid-1").await?,
        0
    );
    assert_eq!(
        upsert_node_identity(&mut conn, &node_id, "node-a", "uid-2").await?,
        1
    );
    let base_identity: (String, String, String) = sqlx::query_as(sql!(
        "SELECT name, incarnation_uid, managed_by FROM node_identities WHERE node_id = $1::uuid"
    ))
    .bind(&node_id)
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(
        base_identity,
        ("node-a".to_string(), "uid-2".to_string(), "k8s".to_string())
    );

    // UPDATE ... FROM with a view on each side.
    let placed = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions e
         SET k8s_node_name = k.k8s_node_name
         FROM node_k8s k, sandboxes s
         WHERE k.node_id = s.active_node_id
           AND s.id = e.sandbox_id
           AND e.sandbox_id = $1::uuid
           AND e.active"
    ))
    .bind(&sandbox_id)
    .execute(&mut conn)
    .await?
    .rows_affected();
    assert_eq!(placed, 1);

    // The retired-generation insert's `INSERT ... SELECT ... WHERE NOT EXISTS
    // ... ON CONFLICT DO NOTHING` (`execution_rows.rs` in the api crate).
    let other_sandbox = seed_sandbox(&mut conn, &node_id).await?;
    let retired = sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, state, active,
             terminated_at
         )
         SELECT $1::uuid, 1, $2, 'sandboxes', 'replaced', FALSE, NOW()
         WHERE NOT EXISTS (
             SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1::uuid
         )
         ON CONFLICT DO NOTHING"
    ))
    .bind(&other_sandbox)
    .bind(format!("space-sbx-pin-retired-{other_sandbox}"))
    .execute(&mut conn)
    .await?
    .rows_affected();
    assert_eq!(retired, 1);
    Ok(())
}

#[tokio::test]
async fn views_hide_other_substrates_rows_but_their_unique_indexes_still_arbitrate() -> TestResult {
    let Some(fixture) = migrated_fixture().await? else {
        return Ok(());
    };
    apply_candidate(&fixture).await?;
    let mut conn = fixture.connect_as(USER_ROLE).await?;
    let (node_id, sandbox_id) = seed_node_and_sandbox(&mut conn).await?;

    // Another substrate's active execution, written to the base table. It has
    // no pod, which the base table now allows.
    sqlx::query(sql!(
        "INSERT INTO sandbox_execution_records (sandbox_id, generation, state, managed_by)
         VALUES ($1::uuid, 1, 'running', 'space')"
    ))
    .bind(&sandbox_id)
    .execute(&mut conn)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_identities (node_id, name, managed_by)
         VALUES ($1::uuid, 'space-node', 'space')"
    ))
    .bind(&node_id)
    .execute(&mut conn)
    .await?;

    // Neither view reads, updates or deletes them.
    let visible: i64 = sqlx::query_scalar(sql!(
        "SELECT (SELECT count(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1::uuid)
              + (SELECT count(*) FROM node_k8s WHERE node_id = $2::uuid)"
    ))
    .bind(&sandbox_id)
    .bind(&node_id)
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(visible, 0);
    let touched = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET state = 'failed' WHERE sandbox_id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .execute(&mut conn)
    .await?
    .rows_affected()
        + sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1::uuid"))
            .bind(&node_id)
            .execute(&mut conn)
            .await?
            .rows_affected();
    assert_eq!(touched, 0);

    // The base table's unique indexes span every substrate, so the hidden row
    // still arbitrates. A hidden row also collides on `(sandbox_id,
    // generation)`, whatever its state, because the claim allocates its
    // generation through the view. A DO NOTHING claim reports no row as if
    // another claim had won, though the view shows no active execution for the
    // sandbox...
    assert_eq!(claim_execution(&mut conn, &sandbox_id, 2).await?, None);
    // ...and the inventory's DO UPDATE reaches the hidden row, whose updated
    // version the check option refuses.
    assert_sqlstate(
        upsert_node_identity(&mut conn, &node_id, "node-a", "uid-1").await,
        "44000",
    );

    // A retired hidden row at generation 1 on a sandbox with no Kubernetes
    // execution: the view's allocation picks 1 again on every attempt, the
    // claim skips, and the claims' follow-up read finds no active execution.
    let fresh_sandbox = seed_sandbox(&mut conn, &node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_execution_records (sandbox_id, generation, state, active, managed_by)
         VALUES ($1::uuid, 1, 'replaced', FALSE, 'space')"
    ))
    .bind(&fresh_sandbox)
    .execute(&mut conn)
    .await?;
    let allocated: i64 = sqlx::query_scalar(EXECUTION_GENERATION_ALLOCATION)
        .bind(&fresh_sandbox)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(allocated, 1);
    assert_eq!(claim_execution(&mut conn, &fresh_sandbox, 1).await?, None);
    let active: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1::uuid AND active"
    ))
    .bind(&fresh_sandbox)
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(active, 0);

    // The check option refuses a write that would leave a row the view cannot
    // see: here, a base default that no longer fills in `k8s`.
    let mut admin = fixture.connect_as("postgres").await?;
    let mut tx = admin.begin().await?;
    sqlx::query(sql!(
        "ALTER TABLE sandbox_execution_records ALTER COLUMN managed_by SET DEFAULT 'space'"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!("SET LOCAL ROLE space_user"))
        .execute(&mut *tx)
        .await?;
    let other_sandbox = seed_sandbox(&mut tx, &node_id).await?;
    assert_sqlstate(claim_execution(&mut tx, &other_sandbox, 1).await, "44000");
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn row_and_table_locks_through_the_views_lock_the_base_tables() -> TestResult {
    let Some(fixture) = migrated_fixture().await? else {
        return Ok(());
    };
    apply_candidate(&fixture).await?;
    let mut conn = fixture.connect_as(USER_ROLE).await?;
    let (node_id, sandbox_id) = seed_node_and_sandbox(&mut conn).await?;
    claim_execution(&mut conn, &sandbox_id, 1).await?;
    upsert_node_identity(&mut conn, &node_id, "node-a", "uid-1").await?;

    // The auto-lifecycle completion's lock on the latest execution, and the
    // queue GC's lock on a node and its identity, held by one transaction.
    let mut holder = fixture.connect_as(USER_ROLE).await?;
    let mut held = holder.begin().await?;
    let locked: i64 = sqlx::query_scalar(sql!(
        "SELECT execution.generation
         FROM sandbox_k8s_executions execution
         WHERE execution.sandbox_id = $1::uuid
         ORDER BY execution.generation DESC
         LIMIT 1
         FOR UPDATE OF execution NOWAIT"
    ))
    .bind(&sandbox_id)
    .fetch_one(&mut *held)
    .await?;
    assert_eq!(locked, 1);
    sqlx::query(sql!(
        "SELECT n.id FROM nodes n JOIN node_k8s k ON k.node_id = n.id
         WHERE n.id = $1::uuid FOR UPDATE OF n, k"
    ))
    .bind(&node_id)
    .fetch_one(&mut *held)
    .await?;

    // NOWAIT and SKIP LOCKED on a view join see the base rows' locks.
    assert_sqlstate(
        sqlx::query(sql!(
            "SELECT s.id FROM sandboxes s
             JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
             WHERE s.id = $1::uuid
             FOR UPDATE OF e NOWAIT"
        ))
        .bind(&sandbox_id)
        .fetch_optional(&mut conn)
        .await,
        "55P03",
    );
    let skipped: Option<String> = sqlx::query_scalar(sql!(
        "SELECT s.id::text FROM sandboxes s
         JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         WHERE s.id = $1::uuid
         FOR UPDATE OF e SKIP LOCKED"
    ))
    .bind(&sandbox_id)
    .fetch_optional(&mut conn)
    .await?;
    assert_eq!(skipped, None);
    for (base_lock, id) in [
        (
            sql!(
                "SELECT 1 FROM sandbox_execution_records
                 WHERE sandbox_id = $1::uuid FOR UPDATE NOWAIT"
            ),
            &sandbox_id,
        ),
        (
            sql!("SELECT 1 FROM node_identities WHERE node_id = $1::uuid FOR UPDATE NOWAIT"),
            &node_id,
        ),
    ] {
        assert_sqlstate(
            sqlx::query(base_lock)
                .bind(id)
                .fetch_optional(&mut conn)
                .await,
            "55P03",
        );
    }
    held.rollback().await?;

    // LOCK TABLE on a view locks the view and its base table, so writers to
    // either wait behind it. Only a test takes this lock on these relations
    // (the api crate's create-readiness characterization test); this leg keeps
    // that contention test meaningful under the views.
    let mut held = holder.begin().await?;
    sqlx::query(sql!(
        "LOCK TABLE sandbox_k8s_executions, node_k8s IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *held)
    .await?;
    let locked_relations: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT relation::regclass::text FROM pg_locks
         WHERE pid = pg_backend_pid() AND locktype = 'relation'
           AND mode = 'AccessExclusiveLock'
         ORDER BY 1"
    ))
    .fetch_all(&mut *held)
    .await?;
    assert_eq!(
        locked_relations,
        [
            "node_identities",
            "node_k8s",
            "sandbox_execution_records",
            "sandbox_k8s_executions"
        ]
    );
    // Any bound proves the write waited on the lock: an unblocked one succeeds.
    sqlx::query(sql!("SET lock_timeout = '20ms'"))
        .execute(&mut conn)
        .await?;
    for (blocked_write, id) in [
        (
            sql!(
                "UPDATE sandbox_execution_records SET updated_at = NOW() WHERE sandbox_id = $1::uuid"
            ),
            &sandbox_id,
        ),
        (
            sql!(
                "UPDATE sandbox_k8s_executions SET updated_at = NOW() WHERE sandbox_id = $1::uuid"
            ),
            &sandbox_id,
        ),
        (
            sql!("UPDATE node_identities SET updated_at = NOW() WHERE node_id = $1::uuid"),
            &node_id,
        ),
    ] {
        assert_sqlstate(
            sqlx::query(blocked_write).bind(id).execute(&mut conn).await,
            "55P03",
        );
    }
    held.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn statements_prepared_before_the_rename_run_through_the_views() -> TestResult {
    let Some(fixture) = migrated_fixture().await? else {
        return Ok(());
    };
    let mut conn = fixture.connect_as(USER_ROLE).await?;
    let (node_id, sandbox_id) = seed_node_and_sandbox(&mut conn).await?;
    let select_all = sql!("SELECT * FROM sandbox_k8s_executions WHERE sandbox_id = $1::uuid");
    let select_named = sql!(
        "SELECT generation, pod_name, pod_materialized_at IS NULL
         FROM sandbox_k8s_executions WHERE sandbox_id = $1::uuid ORDER BY generation"
    );
    let select_node =
        sql!("SELECT k8s_node_name, k8s_node_uid FROM node_k8s WHERE node_id = $1::uuid");

    // Prepare every statement on this connection before the rename.
    assert!(claim_execution(&mut conn, &sandbox_id, 1).await?.is_some());
    upsert_node_identity(&mut conn, &node_id, "node-a", "uid-1").await?;
    sqlx::query(select_all)
        .bind(&sandbox_id)
        .fetch_one(&mut conn)
        .await?;
    sqlx::query(select_named)
        .bind(&sandbox_id)
        .fetch_all(&mut conn)
        .await?;
    sqlx::query(select_node)
        .bind(&node_id)
        .fetch_one(&mut conn)
        .await?;
    // Each statement's server-side name and prepare time: a statement prepared
    // again, under its old name or a new one, changes the list.
    let prepared_statements = sql!(
        "SELECT name, prepare_time::text, statement FROM pg_prepared_statements ORDER BY name"
    );
    let prepared_before: Vec<(String, String, String)> = sqlx::query_as(prepared_statements)
        .fetch_all(&mut conn)
        .await?;
    for statement in [
        EXECUTION_CLAIM_INSERT,
        NODE_INVENTORY_UPSERT,
        select_all,
        select_named,
        select_node,
    ] {
        assert!(
            prepared_before
                .iter()
                .any(|(_, _, prepared)| prepared == statement),
            "prepared before the rename: {statement}"
        );
    }

    apply_candidate(&fixture).await?;
    // A hidden row for the same sandbox: a statement still bound to the old
    // table would return it; one re-resolved to the view does not.
    let mut admin = fixture.connect_as("postgres").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_execution_records (sandbox_id, generation, state, active, managed_by)
         VALUES ($1::uuid, 9, 'replaced', FALSE, 'space')"
    ))
    .bind(&sandbox_id)
    .execute(&mut admin)
    .await?;

    // The same connection re-runs the statements it prepared. Postgres
    // re-analyzes them against the new catalog: they resolve the old names to
    // the views and keep their result types, so none fails with 0A000.
    let rows = sqlx::query(select_all)
        .bind(&sandbox_id)
        .fetch_all(&mut conn)
        .await?;
    assert_eq!(rows.len(), 1, "the hidden row stays hidden");
    assert_eq!(rows[0].try_get::<i64, _>("generation")?, 1);
    assert!(claim_execution(&mut conn, &sandbox_id, 2).await?.is_none());
    let unchanged = upsert_node_identity(&mut conn, &node_id, "node-a", "uid-1").await?;
    let changed = upsert_node_identity(&mut conn, &node_id, "node-a", "uid-2").await?;
    assert_eq!((unchanged, changed), (0, 1));
    let named: Vec<(i64, String, bool)> = sqlx::query_as(select_named)
        .bind(&sandbox_id)
        .fetch_all(&mut conn)
        .await?;
    assert_eq!(
        named,
        vec![(1, format!("space-sbx-pin-1-{sandbox_id}"), true)]
    );
    let node: (String, String) = sqlx::query_as(select_node)
        .bind(&node_id)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(node, ("node-a".to_string(), "uid-2".to_string()));
    let prepared_after: Vec<(String, String, String)> = sqlx::query_as(prepared_statements)
        .fetch_all(&mut conn)
        .await?;
    let names_and_times = |prepared: &[(String, String, String)]| {
        prepared
            .iter()
            .map(|(name, prepare_time, _)| (name.clone(), prepare_time.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names_and_times(&prepared_after),
        names_and_times(&prepared_before),
        "the connection reused its prepared statements rather than preparing them again"
    );
    Ok(())
}

#[tokio::test]
async fn invoker_views_check_the_callers_privileges_on_view_and_base() -> TestResult {
    let Some(fixture) = migrated_fixture().await? else {
        return Ok(());
    };
    apply_candidate(&fixture).await?;
    let mut admin = fixture.connect_as("postgres").await?;

    let mut user = fixture.connect_as(USER_ROLE).await?;
    let (node_id, sandbox_id) = seed_node_and_sandbox(&mut user).await?;
    claim_execution(&mut user, &sandbox_id, 1).await?;
    upsert_node_identity(&mut user, &node_id, "node-a", "uid-1").await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = 'node-a'
         WHERE sandbox_id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .execute(&mut user)
    .await?;
    let expected_identity = vec![("node-a".to_string(), "uid-1".to_string())];

    // The credential manager's reads work through the views both as the
    // service login and as a login holding only the capability role's grants.
    for role in [CREDS_ROLE, CREDMGR_PROBE_ROLE] {
        let mut conn = fixture.connect_as(role).await?;
        let identity: Vec<(String, String)> = sqlx::query_as(STORAGE_VEND_NODE_LOOKUP)
            .bind(&sandbox_id)
            .fetch_all(&mut conn)
            .await?;
        assert_eq!(identity, expected_identity, "{role}");
        let generation: i64 = sqlx::query_scalar(INJECTION_IDENTITY_JOIN)
            .bind(&sandbox_id)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(generation, 1, "{role}");
    }

    // The view's column grants still fence the capability role...
    let mut probe = fixture.connect_as(CREDMGR_PROBE_ROLE).await?;
    assert_sqlstate(
        sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT pod_uid FROM sandbox_k8s_executions LIMIT 1"
        ))
        .fetch_optional(&mut probe)
        .await,
        "42501",
    );
    // ...but only through the view: the invoker view needed the role to read
    // every base column its query reads, so the base table no longer fences
    // that column.
    let base_pod_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_execution_records WHERE sandbox_id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .fetch_one(&mut probe)
    .await?;
    assert_eq!(base_pod_uid, None);
    drop(probe);

    // The old table's column grants alone are not enough through an invoker
    // view: without the base grants on the view's other columns, its reads
    // fail. Each view's check is proved on its own, with the other view's
    // grants intact and its read as the control.
    let set_role = format!("SET LOCAL ROLE {CREDMGR_PROBE_ROLE}");
    let select_execution =
        sql!("SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1::uuid");
    let select_node = sql!("SELECT k8s_node_name FROM node_k8s WHERE node_id = $1::uuid");

    // Without the execution base columns, the execution view and the
    // injection join fail; the node view still reads.
    let mut tx = admin.begin().await?;
    sqlx::query(sql!(
        "REVOKE SELECT (
             id, pod_name, pod_namespace, pod_uid, pod_ip, state, created_at,
             updated_at, terminated_at, activation_attempt_id,
             activation_attempt_expires_at, activation_attempt_target_state,
             route_activation_id, materialized_at, runtime_upgrade_operation_id,
             runtime_upgrade_target_environment_ref, runtime_upgrade_authorization_id
         ) ON sandbox_execution_records FROM space_credmgr"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(sql!(set_role.as_str())))
        .execute(&mut *tx)
        .await?;
    assert_sqlstate_in_savepoint(&mut tx, select_execution, &sandbox_id, "42501").await?;
    assert_sqlstate_in_savepoint(&mut tx, INJECTION_IDENTITY_JOIN, &sandbox_id, "42501").await?;
    let node_name: String = sqlx::query_scalar(select_node)
        .bind(&node_id)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(node_name, "node-a");
    tx.rollback().await?;

    // Without `updated_at` on the node base table, the node view fails; the
    // execution view still reads.
    let mut tx = admin.begin().await?;
    sqlx::query(sql!(
        "REVOKE SELECT (updated_at) ON node_identities FROM space_credmgr"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(sql!(set_role.as_str())))
        .execute(&mut *tx)
        .await?;
    assert_sqlstate_in_savepoint(&mut tx, select_node, &node_id, "42501").await?;
    let generation: i64 = sqlx::query_scalar(select_execution)
        .bind(&sandbox_id)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(generation, 1);
    tx.rollback().await?;

    // The read-only login reads through the views and writes through neither.
    let mut ro = fixture.connect_as(RO_ROLE).await?;
    let readable: i64 = sqlx::query_scalar(sql!(
        "SELECT (SELECT count(*) FROM sandbox_k8s_executions)
              + (SELECT count(*) FROM node_k8s)"
    ))
    .fetch_one(&mut ro)
    .await?;
    assert_eq!(readable, 2);
    assert_sqlstate(
        sqlx::query(sql!("DELETE FROM node_k8s"))
            .execute(&mut ro)
            .await,
        "42501",
    );
    drop(ro);

    // The next bootstrap converges grants over the new relations without
    // taking away what the credential manager's reads need. It also rewrites
    // the cluster-global roles every pin on the server uses, which is safe only
    // because it converges them to the values the template build gave them.
    run_bootstrap(&fixture).await?;
    let mut probe = fixture.connect_as(CREDMGR_PROBE_ROLE).await?;
    let identity: Vec<(String, String)> = sqlx::query_as(STORAGE_VEND_NODE_LOOKUP)
        .bind(&sandbox_id)
        .fetch_all(&mut probe)
        .await?;
    assert_eq!(identity, expected_identity);
    Ok(())
}
