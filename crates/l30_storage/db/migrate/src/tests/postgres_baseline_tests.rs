use pplx_space_db_test_fixtures::{try_raw_pg_test_db, try_raw_pg_test_db_with_max_connections};
use sqlx::SqlSafeStr as _;

use super::super::*;
use crate::pg_test_support::TestResult;

#[tokio::test]
async fn postgres_rejects_legacy_history_without_changing_data() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db().await? else {
        return Ok(());
    };
    Migrator::with_migrations(vec![sqlx::migrate::Migration::new(
        44,
        Cow::Borrowed("legacy schema"),
        MigrationType::Simple,
        "CREATE TABLE retained_state (id integer PRIMARY KEY); INSERT INTO retained_state VALUES (7);"
            .into_sql_str(),
        false,
    )])
    .run(&fixture.pool)
    .await?;
    let history: String = sqlx::query_scalar(sql!(
        "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
    ))
    .fetch_one(&fixture.pool)
    .await?;

    let result = run_control_plane(ControlPlaneArgs {
        database_url: fixture.database_url.clone(),
        job_deadline_seconds: 60,
        confirm_data_loss: false,
        command: Command::Up,
    })
    .await;

    assert!(matches!(result, Err(RunDbMigrateError::ApplyMigrations(_))));
    assert_eq!(
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
        ))
        .fetch_one(&fixture.pool)
        .await?,
        history
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>(sql!("SELECT id FROM retained_state"))
            .fetch_one(&fixture.pool)
            .await?,
        7
    );
    assert!(
        sqlx::query_scalar::<_, bool>(sql!("SELECT to_regclass('public.organizations') IS NULL"))
            .fetch_one(&fixture.pool)
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn all_postgres_migrations_apply_to_fresh_database() -> TestResult {
    let Some(fixture) = try_raw_pg_test_db().await? else {
        return Ok(());
    };

    POSTGRES_MIGRATOR.run(&fixture.pool).await?;
    let history: String = sqlx::query_scalar(sql!(
        "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
    ))
    .fetch_one(&fixture.pool)
    .await?;

    // Retired metadata is opaque: startup must not depend on its columns.
    sqlx::raw_sql(sql!(
        "CREATE TABLE goose_db_version (retained_marker integer);
         INSERT INTO goose_db_version VALUES (7);"
    ))
    .execute(&fixture.pool)
    .await?;

    for goose_present in [true, false] {
        if !goose_present {
            sqlx::query(sql!("DROP TABLE goose_db_version"))
                .execute(&fixture.pool)
                .await?;
        }

        apply_control_plane_migrations(
            &fixture.pool,
            migration_retry::LockRetryPolicy::DEFAULT,
            RunDbMigrateError::ApplyMigrations,
        )
        .await?;

        assert_eq!(
            sqlx::query_scalar::<_, String>(sql!(
                "SELECT json_agg(history ORDER BY version)::text FROM _sqlx_migrations AS history"
            ))
            .fetch_one(&fixture.pool)
            .await?,
            history
        );
        if goose_present {
            assert_eq!(
                sqlx::query_scalar::<_, i32>(sql!("SELECT retained_marker FROM goose_db_version"))
                    .fetch_one(&fixture.pool)
                    .await?,
                7
            );
        }
    }

    let latest: i64 = sqlx::query_scalar(sql!(
        "SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    let expected_latest = POSTGRES_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .max()
        .expect("Postgres migrator must bundle at least one migration");
    assert_eq!(latest, expected_latest);

    let mut migrations: Vec<_> = POSTGRES_MIGRATOR.iter().cloned().collect();
    migrations.push(sqlx::migrate::Migration::new(
        expected_latest + 1,
        Cow::Borrowed("next migration"),
        MigrationType::Simple,
        "ALTER TABLE organizations ADD COLUMN upgrade_probe boolean NOT NULL DEFAULT true;"
            .into_sql_str(),
        false,
    ));
    Migrator::with_migrations(migrations)
        .run(&fixture.pool)
        .await?;

    let upgraded: bool = sqlx::query_scalar(sql!(
        "SELECT upgrade_probe FROM organizations WHERE slug = 'local'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert!(upgraded);
    Ok(())
}

/// The firewall_rules_changed down keeps its enum label, so applying the up
/// again must succeed without adding a second one.
#[tokio::test]
async fn firewall_rules_changed_reapplies_after_its_down_with_one_label() -> TestResult {
    const VERSION: i64 = 20261118160040;
    let Some(fixture) = try_raw_pg_test_db().await? else {
        return Ok(());
    };
    let migrator = Migrator::with_migrations(
        POSTGRES_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= VERSION)
            .cloned()
            .collect(),
    );
    let previous = migrator
        .iter()
        .map(|migration| migration.version)
        .filter(|version| *version < VERSION)
        .max()
        .expect("a migration precedes firewall_rules_changed");
    let labels = async || {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*) FROM pg_enum
             WHERE enumtypid = 'public.state_reconciliation_reason'::regtype
               AND enumlabel = 'firewall_rules_changed'"
        ))
        .fetch_one(&fixture.pool)
        .await
    };

    migrator.run(&fixture.pool).await?;
    assert_eq!(labels().await?, 1);
    migrator.undo(&fixture.pool, previous).await?;
    assert_eq!(labels().await?, 1);
    assert!(
        !sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1)"
        ))
        .bind(VERSION)
        .fetch_one(&fixture.pool)
        .await?
    );
    migrator.run(&fixture.pool).await?;
    assert_eq!(labels().await?, 1);
    Ok(())
}

/// Reapplying the campaign generation up re-adds the column at 0, which would
/// let an If-Match read before the rollback match a campaign that changed
/// since. So the down refuses while any campaign exists, whatever its
/// generation and finished ones included, leaving the column, its trigger and
/// the migration's history row in place, and runs once the table is empty.
#[tokio::test]
async fn campaign_generation_down_refuses_while_campaigns_exist() -> TestResult {
    const VERSION: i64 = 20261118041622;
    // A refused undo returns before sqlx's unlock, so its session keeps the
    // migrator's advisory lock. One connection keeps the later undo and run
    // on that session, where the lock is reentrant, instead of blocking on it
    // from another.
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let migrator = Migrator::with_migrations(
        POSTGRES_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= VERSION)
            .cloned()
            .collect(),
    );
    let previous = migrator
        .iter()
        .map(|migration| migration.version)
        .filter(|version| *version < VERSION)
        .max()
        .expect("a migration precedes runtime_upgrade_campaigns_generation");
    let schema = async || {
        sqlx::query_as::<_, (bool, bool, bool)>(sql!(
            "SELECT
                 EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_name = 'runtime_upgrade_campaigns'
                            AND column_name = 'generation'),
                 EXISTS (SELECT 1 FROM pg_trigger
                          WHERE tgname = 'runtime_upgrade_campaigns_raise_generation'),
                 EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1)"
        ))
        .bind(VERSION)
        .fetch_one(&fixture.pool)
        .await
    };

    let generation = async || {
        sqlx::query_scalar::<_, i64>(sql!("SELECT generation FROM runtime_upgrade_campaigns"))
            .fetch_one(&fixture.pool)
            .await
    };
    let assert_refused = async |expected_generation: i64| -> TestResult {
        let refusal = migrator
            .undo(&fixture.pool, previous)
            .await
            .expect_err("the down must refuse while a campaign exists");
        assert!(
            matches!(
                &refusal,
                MigrateError::ExecuteMigration(_, version) if *version == VERSION
            ),
            "unexpected error: {refusal:?}"
        );
        assert!(
            refusal
                .to_string()
                .contains("runtime_upgrade_campaigns has campaigns"),
            "unexpected error: {refusal}"
        );
        assert_eq!(schema().await?, (true, true, true));
        assert_eq!(generation().await?, expected_generation);
        Ok(())
    };

    migrator.run(&fixture.pool).await?;
    // A campaign still at generation 0 blocks the down too: once the column is
    // gone, a change made before the reapply would also land back at 0.
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_campaigns (
             from_environment_ref, to_environment_ref, kind, phase,
             max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
             include_running, reason)
         VALUES ('sha256:' || repeat('a', 64), 'sha256:' || repeat('b', 64),
                 'templates', 'canary', 1, 1, 1, false, 'test')"
    ))
    .execute(&fixture.pool)
    .await?;
    assert_refused(0).await?;

    // So does a finished campaign that has changed once, which a reapply
    // would put back at 0.
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'completed', finished_at = now()"
    ))
    .execute(&fixture.pool)
    .await?;
    assert_eq!(generation().await?, 1);
    assert_refused(1).await?;

    sqlx::query(sql!("DELETE FROM runtime_upgrade_campaigns"))
        .execute(&fixture.pool)
        .await?;
    migrator.undo(&fixture.pool, previous).await?;
    assert_eq!(schema().await?, (false, false, false));
    migrator.run(&fixture.pool).await?;
    assert_eq!(schema().await?, (true, true, true));
    Ok(())
}

/// The flag is the only record that the control plane, not a node, wrote a
/// disk, as a memory drop does. So the down refuses while an unplaced sandbox
/// is still pinned to one, leaving the column and the migration's history row
/// in place, and runs once no pinned disk carries the flag. The fixture is
/// the row a memory drop leaves: `suspended`, pinned to that disk.
#[tokio::test]
async fn written_off_node_down_refuses_while_a_sandbox_is_pinned_to_one() -> TestResult {
    const VERSION: i64 = 20261124180000;
    // One connection, as in `campaign_generation_down_refuses_while_campaigns_exist`:
    // a refused undo keeps the migrator's advisory lock on its session.
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let migrator = Migrator::with_migrations(
        POSTGRES_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= VERSION)
            .cloned()
            .collect(),
    );
    let previous = migrator
        .iter()
        .map(|migration| migration.version)
        .filter(|version| *version < VERSION)
        .max()
        .expect("a migration precedes snapshot_written_off_node");
    let schema = async || {
        sqlx::query_as::<_, (bool, bool)>(sql!(
            "SELECT
                 EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_name = 'snapshots'
                            AND column_name = 'written_off_node'),
                 EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1)"
        ))
        .bind(VERSION)
        .fetch_one(&fixture.pool)
        .await
    };

    migrator.run(&fixture.pool).await?;
    let artifact_id: String = sqlx::query_scalar(sql!("SELECT gen_random_uuid()::text"))
        .fetch_one(&fixture.pool)
        .await?;
    let sandbox_id: String = sqlx::query_scalar(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation, current_local_artifact_id,
             current_local_artifact_kind, current_local_artifact_origin
         )
         SELECT space_uuid_v7(), id, org_id, 'k8s', 'suspended', 'unplaced', 1,
                $1::uuid, 'disk', 'stop'
           FROM users
          ORDER BY id
          LIMIT 1
         RETURNING id::text"
    ))
    .bind(&artifact_id)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, org_id, type, state, local_artifact_id,
             artifact_kind, artifact_origin, written_off_node
         )
         SELECT gen_random_uuid(), id, org_id, 'disk', 'ready',
                current_local_artifact_id, 'disk', 'stop', true
           FROM sandboxes
          WHERE id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .execute(&fixture.pool)
    .await?;

    let refusal = migrator
        .undo(&fixture.pool, previous)
        .await
        .expect_err("the down must refuse while a sandbox is pinned to an off-node disk");
    assert!(
        matches!(
            &refusal,
            MigrateError::ExecuteMigration(_, version) if *version == VERSION
        ),
        "unexpected error: {refusal:?}"
    );
    assert!(
        refusal
            .to_string()
            .contains("pinned to a disk the off-node stop wrote"),
        "unexpected error: {refusal}"
    );
    assert_eq!(schema().await?, (true, true));

    // The same pin on a disk a node wrote guards nothing, so the down runs.
    sqlx::query(sql!(
        "UPDATE snapshots SET written_off_node = false WHERE sandbox_id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .execute(&fixture.pool)
    .await?;
    migrator.undo(&fixture.pool, previous).await?;
    assert_eq!(schema().await?, (false, false));
    migrator.run(&fixture.pool).await?;
    assert_eq!(schema().await?, (true, true));
    Ok(())
}

/// `sandboxes_keep_suspended_restore_lease` keeps a restore lease across a
/// write that keeps a row `suspended`, which is how a memory drop seals the
/// row for its copy. A write that moves the row anywhere else but
/// `restoring` still clears it, and the restore-only preserve flag is reset
/// either way. The down restores the old function, under which any write to a
/// non-`restoring` row clears the lease.
#[tokio::test]
async fn keep_suspended_restore_lease_keeps_a_seal_only_while_suspended() -> TestResult {
    const VERSION: i64 = 20261124184807;
    let Some(fixture) = try_raw_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let migrator = Migrator::with_migrations(
        POSTGRES_MIGRATOR
            .iter()
            .filter(|migration| migration.version <= VERSION)
            .cloned()
            .collect(),
    );
    let previous = migrator
        .iter()
        .map(|migration| migration.version)
        .filter(|version| *version < VERSION)
        .max()
        .expect("a migration precedes sandboxes_keep_suspended_restore_lease");
    migrator.run(&fixture.pool).await?;
    let sandbox_id: String = sqlx::query_scalar(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         )
         SELECT space_uuid_v7(), id, org_id, 'k8s', 'suspended', 'unplaced', 1
           FROM users
          ORDER BY id
          LIMIT 1
         RETURNING id::text"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    let seal = async || {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'suspended',
                    restore_lease_token = gen_random_uuid(),
                    restore_lease_expires_at = NOW() + INTERVAL '1 minute',
                    preserve_local_artifact_on_restore_failure = true
              WHERE id = $1::uuid"
        ))
        .bind(&sandbox_id)
        .execute(&fixture.pool)
        .await
    };
    let write = async |lifecycle: &str| {
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = $2 WHERE id = $1::uuid"
        ))
        .bind(&sandbox_id)
        .bind(lifecycle)
        .execute(&fixture.pool)
        .await
    };
    // (lease token set, lease expiry set, preserve flag)
    let lease = async || {
        sqlx::query_as::<_, (bool, bool, bool)>(sql!(
            "SELECT restore_lease_token IS NOT NULL,
                    restore_lease_expires_at IS NOT NULL,
                    preserve_local_artifact_on_restore_failure
               FROM sandboxes WHERE id = $1::uuid"
        ))
        .bind(&sandbox_id)
        .fetch_one(&fixture.pool)
        .await
    };

    seal().await?;
    assert_eq!(lease().await?, (true, true, false), "the seal's own write");
    write("suspended").await?;
    assert_eq!(
        lease().await?,
        (true, true, false),
        "suspended to suspended"
    );
    write("started").await?;
    assert_eq!(
        lease().await?,
        (false, false, false),
        "suspended to started"
    );

    // A lease taken while `restoring` still clears once the row leaves it,
    // including back to `suspended`.
    write("restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = gen_random_uuid(),
                restore_lease_expires_at = NOW() + INTERVAL '1 minute'
          WHERE id = $1::uuid"
    ))
    .bind(&sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        lease().await?,
        (true, true, false),
        "restoring keeps its lease"
    );
    write("suspended").await?;
    assert_eq!(
        lease().await?,
        (false, false, false),
        "restoring to suspended"
    );

    migrator.undo(&fixture.pool, previous).await?;
    seal().await?;
    assert_eq!(
        lease().await?,
        (false, false, false),
        "the old function clears the lease on every non-restoring write"
    );
    migrator.run(&fixture.pool).await?;
    seal().await?;
    assert_eq!(lease().await?, (true, true, false));
    Ok(())
}
