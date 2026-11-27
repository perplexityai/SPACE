use super::*;
use crate::db::{
    AbandonRuntimeUpgrade, ExecutionMetadata, ObservedSandbox, RuntimeUpgradeAuthorization,
    RuntimeUpgradeRequestIdentity, RuntimeUpgradeTargetState, SandboxState,
    abandon_runtime_upgrade, delete_sandbox, fixture::insert_test_sandbox, read,
    record_runtime_upgrade_refusal, test_clock, upsert_sandbox_execution_metadata,
};
use pplx_space_execution_spec::EnvironmentReference;

const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

fn op(index: u128) -> RuntimeUpgradeOperationId {
    RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::from_u128(
        0x0190_0000_0000_7000_8000_0000_0000_0000 | index,
    ))
}

fn sbx(index: u128) -> String {
    uuid::Uuid::from_u128(index).to_string()
}

fn environment_pair() -> (ExecutionMetadata, EnvironmentReference) {
    use pplx_space_cpu_arch::Arch;
    use pplx_space_execution_spec::{
        ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion,
        GuestOperatingSystem, GuestPlatform, RuntimeIdentity, RuntimeKind, StateFormatIdentity,
    };
    let reference: EnvironmentReference = format!("sha256:{}", "b".repeat(64)).parse().unwrap();
    let execution = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: "1.17.0".parse().unwrap(),
        },
        GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: Arch::X86_64,
        },
        Some(StateFormatIdentity {
            format: "firecracker-snapshot".parse().unwrap(),
            version: "12.0.0".parse().unwrap(),
        }),
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: Some("boot".parse().unwrap()),
            version: Some(ArtifactVersion::parse("6.18.44").unwrap()),
            digest: Some(format!("sha256:{}", "d".repeat(64)).parse().unwrap()),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    )
    .unwrap()
    .with_environment(reference);
    (execution, reference)
}

fn insert_attempt_outcome(
    conn: &Connection,
    operation_id: &str,
    attempt_id: &str,
    sandbox_id: &str,
    recorded_at: &str,
    last_seen_at: Option<&str>,
) {
    conn.execute(
        "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes
             (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at,
              target_environment, target_state, authorization_id, last_seen_at)
         VALUES (?1, ?2, ?3, 'rolled_back', 'boot failed', ?4, 'env', 'started', 'auth', ?5)",
        params![
            operation_id,
            attempt_id,
            sandbox_id,
            recorded_at,
            last_seen_at
        ],
    )
    .unwrap();
}

fn insert_abandoned(
    conn: &Connection,
    operation_id: &str,
    sandbox_id: &str,
    recorded_at: &str,
    last_seen_at: Option<&str>,
) {
    conn.execute(
        "INSERT INTO sandbox_runtime_upgrade_abandoned
             (operation_id, sandbox_id, recorded_at, last_seen_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![operation_id, sandbox_id, recorded_at, last_seen_at],
    )
    .unwrap();
}

fn attempt_exists(conn: &Connection, operation_id: &str, attempt_id: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sandbox_runtime_upgrade_attempt_outcomes
              WHERE operation_id = ?1 AND attempt_id = ?2
         )",
        params![operation_id, attempt_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn abandoned_exists(conn: &Connection, operation_id: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sandbox_runtime_upgrade_abandoned
              WHERE operation_id = ?1
         )",
        [operation_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn attempt_timestamps(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
) -> (String, Option<String>) {
    conn.query_row(
        "SELECT recorded_at, last_seen_at
           FROM sandbox_runtime_upgrade_attempt_outcomes
          WHERE operation_id = ?1 AND attempt_id = ?2",
        params![operation_id.to_string(), attempt_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

fn abandoned_timestamps(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> (String, Option<String>) {
    conn.query_row(
        "SELECT recorded_at, last_seen_at
           FROM sandbox_runtime_upgrade_abandoned
          WHERE operation_id = ?1",
        [operation_id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

#[test]
fn cursor_sweeps_bound_batch_size_and_preserve_null_boundary_and_malformed_rows() {
    let conn = crate::db::test_conn();
    let sandbox_id = sbx(1);
    let now = RETENTION.as_secs() + 100;
    let cutoff = now - RETENTION.as_secs();

    // Seed rows at the front of each table that must survive (exact cutoff
    // boundary, `NULL` `last_seen_at` still fresh via `recorded_at`,
    // `max(recorded_at, last_seen_at)` when `last_seen_at < recorded_at`,
    // malformed timestamps, and non-canonical IDs) so the cursor must advance
    // past retained rows without starving older rows behind them.
    let kept_attempts = [
        (
            op(1).to_string(),
            cutoff.to_string(),
            Some(cutoff.to_string()),
        ),
        (op(2).to_string(), cutoff.to_string(), None),
        (op(3).to_string(), cutoff.to_string(), Some("1".to_string())),
        (op(4).to_string(), "bad".to_string(), Some("1".to_string())),
        (op(5).to_string(), "1".to_string(), Some("bad".to_string())),
        (
            "not-a-uuid".to_string(),
            "1".to_string(),
            Some("1".to_string()),
        ),
    ];
    for (operation_id, recorded_at, last_seen_at) in &kept_attempts {
        insert_attempt_outcome(
            &conn,
            operation_id,
            "a",
            &sandbox_id,
            recorded_at,
            last_seen_at.as_deref(),
        );
    }
    let kept_abandoned = [
        (
            op(11).to_string(),
            cutoff.to_string(),
            Some(cutoff.to_string()),
        ),
        (op(12).to_string(), cutoff.to_string(), None),
        (
            op(13).to_string(),
            cutoff.to_string(),
            Some("1".to_string()),
        ),
        (op(14).to_string(), "bad".to_string(), Some("1".to_string())),
        (op(15).to_string(), "1".to_string(), Some("bad".to_string())),
        (
            "not-a-uuid".to_string(),
            "1".to_string(),
            Some("1".to_string()),
        ),
    ];
    for (operation_id, recorded_at, last_seen_at) in &kept_abandoned {
        insert_abandoned(
            &conn,
            operation_id,
            &sandbox_id,
            recorded_at,
            last_seen_at.as_deref(),
        );
    }

    // Add 130 aged rows per table (including `NULL` `last_seen_at` falling back
    // to aged `recorded_at`, and `cutoff - 1` boundary) so the total exceeds
    // `ROWS_PER_BATCH` (128).
    let expired_cutoff = (cutoff - 1).to_string();
    for i in 0..130_u128 {
        let seen = (i % 2 == 0).then_some(expired_cutoff.as_str());
        insert_attempt_outcome(
            &conn,
            &op(100 + i).to_string(),
            "a",
            &sandbox_id,
            &expired_cutoff,
            seen,
        );
        insert_abandoned(
            &conn,
            &op(300 + i).to_string(),
            &sandbox_id,
            &expired_cutoff,
            seen,
        );
    }

    let mut pruner = RuntimeUpgradeRetentionPruner::default();

    // `now < retention` never underflows or deletes anything.
    assert_eq!(
        pruner
            .next_batch(&conn, RETENTION.as_secs() - 1, RETENTION)
            .unwrap()
            .total_deleted(),
        0
    );

    // Page 1 scans 128 rows (6 kept + 122 expired) and snapshots `through`.
    let first = pruner.next_batch(&conn, now, RETENTION).unwrap();
    assert_eq!(first.examined_attempt_outcomes, ROWS_PER_BATCH);
    assert_eq!(
        first.deleted_attempt_outcomes,
        ROWS_PER_BATCH - kept_attempts.len()
    );
    assert_eq!(first.examined_abandoned, ROWS_PER_BATCH);
    assert_eq!(
        first.deleted_abandoned,
        ROWS_PER_BATCH - kept_abandoned.len()
    );

    // Rows inserted while a sweep is in progress land past `through` and wait
    // for the next sweep rather than extending the current one indefinitely.
    let mid_sweep_attempt = op(500).to_string();
    let mid_sweep_abandoned = op(501).to_string();
    insert_attempt_outcome(&conn, &mid_sweep_attempt, "a", &sandbox_id, "1", Some("1"));
    insert_abandoned(&conn, &mid_sweep_abandoned, &sandbox_id, "1", Some("1"));

    // Page 2 finishes the 8 remaining rows of the snapshotted sweep and resets.
    let second = pruner.next_batch(&conn, now, RETENTION).unwrap();
    assert_eq!(second.examined_attempt_outcomes, 8);
    assert_eq!(second.deleted_attempt_outcomes, 8);
    assert_eq!(second.examined_abandoned, 8);
    assert_eq!(second.deleted_abandoned, 8);
    assert!(attempt_exists(&conn, &mid_sweep_attempt, "a"));
    assert!(abandoned_exists(&conn, &mid_sweep_abandoned));

    // Fresh sweep picks up the mid-sweep arrivals while keeping all 6 boundary/malformed rows.
    let third = pruner.next_batch(&conn, now, RETENTION).unwrap();
    assert_eq!(third.deleted_attempt_outcomes, 1);
    assert_eq!(third.deleted_abandoned, 1);
    for (operation_id, _, _) in &kept_attempts {
        assert!(attempt_exists(&conn, operation_id, "a"), "{operation_id}");
    }
    for (operation_id, _, _) in &kept_abandoned {
        assert!(abandoned_exists(&conn, operation_id), "{operation_id}");
    }
}

#[test]
fn touches_and_repeat_abandonment_refresh_last_seen_without_altering_recorded_at() {
    let conn = crate::db::test_conn();
    let (execution, reference) = environment_pair();
    let sandbox_id = sbx(2);
    insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopped);
    upsert_sandbox_execution_metadata(&conn, &sandbox_id, &execution).unwrap();

    let _clock = test_clock::ticking_from(1_000);
    let row = read::sandbox(&conn, &sandbox_id).unwrap().unwrap();
    let observed = ObservedSandbox::of(&row);
    let operation_id = op(50);
    let request = RuntimeUpgradeRequestIdentity {
        operation_id,
        sandbox_id: sandbox_id.clone(),
        target_environment: reference,
        target_state: RuntimeUpgradeTargetState::Started,
        authorization_id: RuntimeUpgradeAuthorization {
            id: "auth-1".to_owned(),
            reason: "rollout".to_owned(),
            memory_loss: false,
        }
        .id,
    };

    assert!(
        record_runtime_upgrade_refusal(
            &conn,
            &request,
            "attempt-1",
            "uncertified edge",
            Some(observed),
        )
        .unwrap()
    );
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "attempt-1"),
        ("1000".to_owned(), Some("1000".to_owned()))
    );
    assert_eq!(
        read::runtime_upgrade_report(&conn, operation_id)
            .unwrap()
            .unwrap()
            .attempt_id
            .as_deref(),
        Some("attempt-1")
    );

    assert_eq!(
        abandon_runtime_upgrade(&conn, &sandbox_id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert_eq!(
        abandoned_timestamps(&conn, operation_id),
        ("1001".to_owned(), Some("1001".to_owned()))
    );
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "attempt-1"),
        ("1000".to_owned(), Some("1001".to_owned())),
        "abandoning an operation also refreshes sibling attempt outcomes"
    );

    // Individual attempt touch and operation-wide touch advance `last_seen_at`
    // without altering `recorded_at`, and ignore backward clock jumps.
    let _later = test_clock::ticking_from(5_000);
    touch_runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-1").unwrap();
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "attempt-1"),
        ("1000".to_owned(), Some("5000".to_owned()))
    );
    assert_eq!(
        read::runtime_upgrade_report(&conn, operation_id)
            .unwrap()
            .unwrap()
            .attempt_id
            .as_deref(),
        Some("attempt-1")
    );
    assert_eq!(
        abandoned_timestamps(&conn, operation_id),
        ("1001".to_owned(), Some("1001".to_owned()))
    );

    touch_retained_runtime_upgrade_operation(&conn, operation_id, &sandbox_id).unwrap();
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "attempt-1"),
        ("1000".to_owned(), Some("5001".to_owned()))
    );
    assert_eq!(
        abandoned_timestamps(&conn, operation_id),
        ("1001".to_owned(), Some("5001".to_owned()))
    );

    let _backward = test_clock::ticking_from(2_000);
    touch_runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-1").unwrap();
    touch_retained_runtime_upgrade_operation(&conn, operation_id, &sandbox_id).unwrap();
    assert_eq!(
        abandon_runtime_upgrade(&conn, &sandbox_id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "attempt-1"),
        ("1000".to_owned(), Some("5001".to_owned()))
    );
    assert_eq!(
        abandoned_timestamps(&conn, operation_id),
        ("1001".to_owned(), Some("5001".to_owned()))
    );

    let _forward = test_clock::ticking_from(9_000);
    assert_eq!(
        abandon_runtime_upgrade(&conn, &sandbox_id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert_eq!(
        abandoned_timestamps(&conn, operation_id),
        ("1001".to_owned(), Some("9000".to_owned()))
    );
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "attempt-1"),
        ("1000".to_owned(), Some("9000".to_owned()))
    );

    // Malformed `last_seen_at` stays fail-closed across touch and repeat
    // abandon; cross-sandbox legacy attempt rows aren't refreshed; and an
    // attempt-outcome update error doesn't roll back a committed abandonment.
    let other_sandbox = sbx(21);
    conn.execute(
        "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes (
             operation_id, attempt_id, sandbox_id, outcome, failure,
             recorded_at, target_environment, target_state, authorization_id, last_seen_at
         ) VALUES (?1, 'cross-sbx-legacy', ?2, 'rolled_back', 'legacy', '1000', NULL, NULL, NULL, '1000')",
        params![operation_id.to_string(), other_sandbox],
    )
    .unwrap();
    for malformed in ["bad", "1\0x"] {
        conn.execute(
            "UPDATE sandbox_runtime_upgrade_attempt_outcomes SET last_seen_at = ?2
              WHERE operation_id = ?1 AND attempt_id = 'attempt-1'",
            params![operation_id.to_string(), malformed],
        )
        .unwrap();
        conn.execute(
            "UPDATE sandbox_runtime_upgrade_abandoned SET last_seen_at = ?2
              WHERE operation_id = ?1",
            params![operation_id.to_string(), malformed],
        )
        .unwrap();
        touch_runtime_upgrade_attempt_outcome(&conn, operation_id, "attempt-1").unwrap();
        touch_retained_runtime_upgrade_operation(&conn, operation_id, &sandbox_id).unwrap();
        assert_eq!(
            abandon_runtime_upgrade(&conn, &sandbox_id, operation_id).unwrap(),
            AbandonRuntimeUpgrade::Abandoned
        );
        assert_eq!(
            attempt_timestamps(&conn, operation_id, "attempt-1")
                .1
                .as_deref(),
            Some(malformed)
        );
        assert_eq!(
            abandoned_timestamps(&conn, operation_id).1.as_deref(),
            Some(malformed)
        );
    }
    assert_eq!(
        attempt_timestamps(&conn, operation_id, "cross-sbx-legacy")
            .1
            .as_deref(),
        Some("1000")
    );
    conn.execute(
        "DELETE FROM sandbox_runtime_upgrade_attempt_outcomes
          WHERE operation_id = ?1 AND attempt_id = 'cross-sbx-legacy'",
        [operation_id.to_string()],
    )
    .unwrap();
    backdate_retained_runtime_upgrade_for_test(&conn, operation_id, 1_000, Some(9_000)).unwrap();

    let isolated_abandon = op(301);
    insert_attempt_outcome(
        &conn,
        &isolated_abandon.to_string(),
        "attempt-1",
        &sandbox_id,
        "1000",
        Some("1000"),
    );
    conn.execute_batch(
        "CREATE TEMP TRIGGER fail_attempt_touch
         BEFORE UPDATE ON sandbox_runtime_upgrade_attempt_outcomes
         BEGIN SELECT RAISE(ABORT, 'attempt touch failure'); END;",
    )
    .unwrap();
    assert_eq!(
        abandon_runtime_upgrade(&conn, &sandbox_id, isolated_abandon).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    conn.execute_batch("DROP TRIGGER fail_attempt_touch;")
        .unwrap();
    assert!(abandoned_exists(&conn, &isolated_abandon.to_string()));
    assert_eq!(
        attempt_timestamps(&conn, isolated_abandon, "attempt-1")
            .1
            .as_deref(),
        Some("1000")
    );
    conn.execute(
        "DELETE FROM sandbox_runtime_upgrade_attempt_outcomes WHERE operation_id = ?1",
        [isolated_abandon.to_string()],
    )
    .unwrap();
    conn.execute(
        "DELETE FROM sandbox_runtime_upgrade_abandoned WHERE operation_id = ?1",
        [isolated_abandon.to_string()],
    )
    .unwrap();

    delete_sandbox(&conn, &sandbox_id).unwrap();
    assert!(abandoned_exists(&conn, &operation_id.to_string()));

    let mut pruner = RuntimeUpgradeRetentionPruner::default();
    let guard_acquired = std::cell::Cell::new(false);
    assert_eq!(
        pruner
            .next_batch_with_clock(
                &conn,
                RETENTION,
                |candidate| {
                    assert_eq!(candidate, sandbox_id);
                    guard_acquired.set(true);
                    Some(())
                },
                || {
                    assert!(
                        guard_acquired.get(),
                        "cutoff clock must be sampled after candidate idle guards are held"
                    );
                    assert!(
                        !conn.is_autocommit(),
                        "cutoff clock must be sampled inside the IMMEDIATE transaction"
                    );
                    Ok(Some(1_001 + RETENTION.as_secs() + 100))
                },
            )
            .unwrap()
            .unwrap()
            .total_deleted(),
        0,
        "refreshed last_seen_at must keep rows alive past recorded_at + retention"
    );
    let pruned = pruner
        .next_batch(&conn, 9_000 + RETENTION.as_secs() + 1, RETENTION)
        .unwrap();
    assert_eq!(pruned.deleted_attempt_outcomes, 1);
    assert_eq!(pruned.deleted_abandoned, 1);
}

#[test]
fn active_local_state_and_aliases_block_pruning_and_errors_reset_cursors() {
    let conn = crate::db::test_conn();
    let [
        sbx_live,
        sbx_upgrading,
        sbx_outcome,
        sbx_transfer,
        sbx_outbox,
        sbx_artifact,
        sbx_op_alias,
        sbx_fk_1,
        sbx_fk_2,
    ] = [20, 21, 22, 24, 25, 26, 30, 91, 92].map(sbx);

    insert_test_sandbox(&conn, &sbx_live, SandboxState::Stopped);
    insert_test_sandbox(&conn, &sbx_fk_1, SandboxState::Stopped);
    insert_test_sandbox(&conn, &sbx_fk_2, SandboxState::Stopped);

    let [
        op_live,
        op_upgrading,
        op_outcome,
        op_transfer,
        op_outbox,
        op_artifact,
        op_sbx_alias,
    ] = [700, 701, 702, 704, 705, 706, 708].map(|i| op(i).to_string());
    let [
        ab_op_live,
        ab_op_upgrading,
        ab_op_outcome,
        ab_op_transfer,
        ab_op_outbox,
        ab_op_artifact,
        ab_op_sbx_alias,
    ] = [750, 751, 752, 754, 755, 756, 758].map(|i| op(i).to_string());

    conn.execute_batch(&format!(
        "INSERT INTO sandbox_runtime_upgrades
            (operation_id, sandbox_id, target_environment, target_state,
             authorization_id, authorization_reason, memory_loss_authorized,
             source_state, previous_execution_metadata_json, phase, created_at, updated_at)
         VALUES ('{op_upgrading}', '{sbx_fk_1}', 'env', 'started',
                 'auth', 'reason', 0, 'stopped', '{{}}', 'fenced', '1', '1'),
                ('{ab_op_upgrading}', '{sbx_fk_1}', 'env', 'started',
                 'auth', 'reason', 0, 'stopped', '{{}}', 'fenced', '1', '1');
         INSERT INTO sandbox_runtime_upgrade_outcomes
            (operation_id, sandbox_id, outcome, target_environment, target_state,
             authorization_id, failure, recorded_at)
         VALUES ('{op_outcome}', '{sbx_fk_2}', 'rolled_back', 'env', 'started',
                 'auth', NULL, '1'),
                ('{ab_op_outcome}', '{sbx_fk_2}', 'rolled_back', 'env', 'started',
                 'auth', NULL, '1');
         INSERT INTO sandbox_restore_transfer_scopes (sandbox_id, transfer_scope_id, staging_dir)
         VALUES ('{sbx_transfer}', 'scope-1', '/var/lib/space/staging');
         INSERT INTO auto_lifecycle_delete_outbox
            (receipt_id, sandbox_id, execution_generation, route_activation_id,
             auto_lifecycle_policy_fingerprint, state, created_at, completed_at)
         VALUES ('receipt-1', '{sbx_outbox}', 1, 'activation-1', 'fp-1', 'completed', '1', '1');
         INSERT INTO local_artifacts
            (local_artifact_id, sandbox_id, artifact_kind, artifact_origin, local_path, created_at)
         VALUES ('artifact-1', '{sbx_artifact}', 'full', 'suspend',
                 '/var/lib/space/artifacts/1', '1');"
    ))
    .unwrap();

    for (attempt_op, abandoned_op, sandbox_id) in [
        (&op_live, &ab_op_live, &sbx_live),
        (&op_upgrading, &ab_op_upgrading, &sbx_upgrading),
        (&op_outcome, &ab_op_outcome, &sbx_outcome),
        (&op_transfer, &ab_op_transfer, &sbx_transfer),
        (&op_outbox, &ab_op_outbox, &sbx_outbox),
        (&op_artifact, &ab_op_artifact, &sbx_artifact),
    ] {
        insert_attempt_outcome(&conn, attempt_op, "a", sandbox_id, "1", Some("1"));
        insert_abandoned(&conn, abandoned_op, sandbox_id, "1", Some("1"));
    }

    // Non-canonical UUID spellings (hyphenless) must be skipped rather than
    // bypassing case-sensitive SQLite joins.
    let non_canonical_sbx = sbx_live.replace('-', "");
    insert_attempt_outcome(
        &conn,
        &op_sbx_alias,
        "a",
        &non_canonical_sbx,
        "1",
        Some("1"),
    );
    insert_abandoned(&conn, &ab_op_sbx_alias, &non_canonical_sbx, "1", Some("1"));
    let non_canonical_op = op(711).to_string().replace('-', "");
    let non_canonical_ab_op = op(761).to_string().replace('-', "");
    insert_attempt_outcome(&conn, &non_canonical_op, "a", &sbx_op_alias, "1", Some("1"));
    insert_abandoned(&conn, &non_canonical_ab_op, &sbx_op_alias, "1", Some("1"));

    let now = RETENTION.as_secs() + 10;
    let mut pruner = RuntimeUpgradeRetentionPruner::default();
    assert_eq!(
        pruner
            .next_batch(&conn, now, RETENTION)
            .unwrap()
            .total_deleted(),
        0
    );

    // Clear active state and verify a mid-batch SQLite abort rolls back the
    // transaction and resets cursors so page 1 is retried once cleared.
    conn.execute_batch(
        "DELETE FROM sandbox_restore_transfer_scopes;
         DELETE FROM auto_lifecycle_delete_outbox;
         DELETE FROM local_artifacts;
         DELETE FROM sandboxes;",
    )
    .unwrap();
    for i in 0..130 {
        insert_attempt_outcome(
            &conn,
            &op(800 + i).to_string(),
            "a",
            &sbx(40),
            "1",
            Some("1"),
        );
    }
    conn.execute_batch(&format!(
        "CREATE TEMP TRIGGER fail_abandoned_delete
         BEFORE DELETE ON sandbox_runtime_upgrade_abandoned
         WHEN OLD.operation_id = '{ab_op_artifact}'
         BEGIN SELECT RAISE(ABORT, 'simulated disk error'); END;"
    ))
    .unwrap();

    pruner.next_batch(&conn, now, RETENTION).unwrap_err();
    assert!(attempt_exists(&conn, &op_live, "a"));

    conn.execute_batch("DROP TRIGGER fail_abandoned_delete;")
        .unwrap();
    let page1 = pruner.next_batch(&conn, now, RETENTION).unwrap();
    assert!(
        !attempt_exists(&conn, &op_live, "a"),
        "error rollback resets the attempt cursor so page 1 revisits earlier rowids"
    );
    let page2 = pruner.next_batch(&conn, now, RETENTION).unwrap();
    assert_eq!(
        page1.deleted_attempt_outcomes + page2.deleted_attempt_outcomes,
        136
    );
    assert_eq!(page1.deleted_abandoned + page2.deleted_abandoned, 6);
    assert!(attempt_exists(&conn, &op_sbx_alias, "a"));
    assert!(attempt_exists(&conn, &non_canonical_op, "a"));
    assert!(abandoned_exists(&conn, &ab_op_sbx_alias));
    assert!(abandoned_exists(&conn, &non_canonical_ab_op));
}

#[test]
fn cross_table_operation_coupling_holds_across_attempts_and_pages() {
    let conn = crate::db::test_conn();
    let sandbox_id = sbx(50);
    let fresh = (RETENTION.as_secs() + 5).to_string();
    let now = RETENTION.as_secs() + 10;

    // `coupled_by_abandoned`: >128 aged attempt outcomes share an operation
    // whose abandonment row is still fresh, so neither table may prune the
    // operation until the abandonment row ages out too.
    let coupled_by_abandoned = op(900).to_string();
    for i in 0..130 {
        insert_attempt_outcome(
            &conn,
            &coupled_by_abandoned,
            &format!("attempt-{i}"),
            &sandbox_id,
            "1",
            Some("1"),
        );
    }
    insert_abandoned(&conn, &coupled_by_abandoned, &sandbox_id, "1", Some(&fresh));

    // `coupled_by_attempt`: an aged abandonment row shares an operation with
    // one aged attempt and one fresh attempt, so the abandonment marker stays
    // until every attempt outcome of the operation has been pruned.
    let coupled_by_attempt = op(901).to_string();
    insert_attempt_outcome(
        &conn,
        &coupled_by_attempt,
        "old",
        &sandbox_id,
        "1",
        Some("1"),
    );
    insert_attempt_outcome(
        &conn,
        &coupled_by_attempt,
        "fresh",
        &sandbox_id,
        "1",
        Some(&fresh),
    );
    insert_abandoned(&conn, &coupled_by_attempt, &sandbox_id, "1", Some("1"));

    // `coupled_by_resident_abandoned`: a legacy cross-sandbox attempt row on an
    // idle sandbox stays coupled while the operation's abandonment row belongs
    // to a sandbox that still has local node state.
    let resident_sandbox = sbx(51);
    insert_test_sandbox(&conn, &resident_sandbox, SandboxState::Stopped);
    let coupled_by_resident_abandoned = op(902).to_string();
    insert_attempt_outcome(
        &conn,
        &coupled_by_resident_abandoned,
        "legacy",
        &sandbox_id,
        "1",
        Some("1"),
    );
    insert_abandoned(
        &conn,
        &coupled_by_resident_abandoned,
        &resident_sandbox,
        "1",
        Some("1"),
    );

    let mut pruner = RuntimeUpgradeRetentionPruner::default();
    let first = pruner.next_batch(&conn, now, RETENTION).unwrap();
    let second = pruner.next_batch(&conn, now, RETENTION).unwrap();
    assert_eq!(
        first.deleted_attempt_outcomes + second.deleted_attempt_outcomes,
        1,
        "only coupled_by_attempt's aged attempt may be deleted while sibling state blocks the rest"
    );
    assert_eq!(first.deleted_abandoned + second.deleted_abandoned, 0);
    assert!(attempt_exists(&conn, &coupled_by_abandoned, "attempt-0"));
    assert!(attempt_exists(&conn, &coupled_by_attempt, "fresh"));
    assert!(attempt_exists(
        &conn,
        &coupled_by_resident_abandoned,
        "legacy"
    ));
    assert!(abandoned_exists(&conn, &coupled_by_abandoned));
    assert!(abandoned_exists(&conn, &coupled_by_attempt));
    assert!(abandoned_exists(&conn, &coupled_by_resident_abandoned));

    // Once `resident_sandbox` is deleted and `now` advances past `fresh + RETENTION`,
    // bounded sweeps drain all remaining attempts and then delete all three
    // abandonment rows.
    delete_sandbox(&conn, &resident_sandbox).unwrap();
    let later = RETENTION.as_secs() * 2 + 10;
    let mut deleted_attempts = 0;
    let mut deleted_abandoned = 0;
    for _ in 0..3 {
        let batch = pruner.next_batch(&conn, later, RETENTION).unwrap();
        deleted_attempts += batch.deleted_attempt_outcomes;
        deleted_abandoned += batch.deleted_abandoned;
    }
    assert_eq!(deleted_attempts, 132);
    assert_eq!(deleted_abandoned, 3);
}
