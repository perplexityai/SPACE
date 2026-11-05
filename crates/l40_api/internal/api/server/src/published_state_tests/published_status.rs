//! How node publication reads `PublishedSandboxState.status`, through
//! [`publish_sandbox_states`] against the real Postgres fixture.
//!
//! The status is parsed once where the publication enters: trimmed and
//! compared without regard to ASCII case against the stored lifecycle
//! vocabulary. Each sweep below covers one ingest statement that decides on
//! the published status, and publishes ASCII spellings of the lifecycle it
//! acts on: every spelling the parse accepts must reach the decision the
//! canonical one does, and every near miss the decision an unrelated status
//! does. A status whose trimmed text is outside ASCII is unrecognized,
//! whatever the database collation would lower-case it to.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxQuarantineReasonCode};
use pplx_space_common_svc::types::{
    FACET_SANDBOX_DESIRED_STATE, SANDBOX_FAILURE_EPISODE_KEY, SANDBOX_LOCAL_SUSPEND_KEY,
};
use pplx_space_db_test_fixtures::{insert_ready_worker_node, pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    insert_active_sandbox, insert_k8s_execution, insert_ready_suspend_snapshot,
    published_sandbox_state,
};
use crate::proto::PublishedSandboxState;
use crate::publish_sandbox_states;

/// ASCII spellings of `canonical`, each with whether the parse reads it as
/// `canonical`. Padding covers every ASCII whitespace the parse trims.
fn ascii_spellings(canonical: &str) -> Vec<(String, bool)> {
    let upper = canonical.to_ascii_uppercase();
    let (head, tail) = canonical.split_at(1);
    let title = format!("{}{tail}", head.to_ascii_uppercase());
    vec![
        (canonical.to_owned(), true),
        (upper.clone(), true),
        (format!("  {title}\t"), true),
        (format!("\n\x0b{upper}\r\x0c"), true),
        (format!("{canonical}x"), false),
        (format!("{head} {tail}"), false),
        ("bogus".to_owned(), false),
    ]
}

async fn ready_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

/// A `k8s` row in `lifecycle` on `node_id` with one running execution at
/// generation 7.
async fn insert_running(pool: &PgPool, node_id: NodeId, lifecycle: &str) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    insert_active_sandbox(pool, sandbox_id, node_id, lifecycle).await?;
    insert_k8s_execution(pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
    Ok(sandbox_id)
}

/// Publish `state` from `node_id` in one transaction and commit it.
async fn publish(pool: &PgPool, node_id: NodeId, state: PublishedSandboxState) -> TestResult {
    let mut tx = pool.begin().await?;
    Box::pin(publish_sandbox_states(&mut tx, node_id, &[state])).await?;
    tx.commit().await?;
    Ok(())
}

async fn lifecycle(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn observed_status(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT status FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Whether the node's desired-state facet observation of `sandbox_id`
/// carries `key`.
async fn facet_has(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    key: &str,
) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM node_facet_observed_status
              WHERE node_id = $1 AND facet = $2 AND element = $3
                AND observed_payload ? $4
         )"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(key)
    .fetch_one(pool)
    .await?)
}

async fn activation_attempt(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn set_activation_attempt(
    pool: &PgPool,
    sandbox_id: SandboxId,
    attempt: Uuid,
    target_state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                route_activation_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = $3
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(attempt)
    .bind(target_state)
    .execute(pool)
    .await?;
    Ok(())
}

fn with_receipt(
    mut state: PublishedSandboxState,
    kind: &str,
    target_state: &str,
    attempt: Uuid,
    reason_code: &str,
) -> PublishedSandboxState {
    state.transition_receipt = Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
        id: Uuid::now_v7().to_string(),
        kind: kind.to_owned(),
        target_state: target_state.to_owned(),
        execution_generation: 7,
        reason_code: reason_code.to_owned(),
        activation_attempt_id: Some(attempt.to_string()),
    });
    state
}

fn failed_report(sandbox_id: SandboxId, status: &str) -> PublishedSandboxState {
    let mut report = published_sandbox_state(sandbox_id, status);
    report.execution_generation = Some(7);
    report.pod_uid = Some("pod-uid-7".to_owned());
    report.failure_reason = Some("synthetic guest failure".to_owned());
    report
}

/// A `failed` report spelled outside ASCII is unrecognized: it neither moves
/// a `k8s` row to `failed` nor opens a failure episode, though a database
/// collation that lower-cases `İ` to `i` would read it as `failed`. The
/// observed state still records the node's spelling.
#[tokio::test(flavor = "current_thread")]
async fn non_ascii_failed_spelling_is_unrecognized() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;
    let sandbox_id = insert_running(pool, node_id, "started").await?;

    publish(pool, node_id, failed_report(sandbox_id, "FAİLED")).await?;

    assert_eq!(lifecycle(pool, sandbox_id).await?, "started");
    assert!(!facet_has(pool, node_id, sandbox_id, SANDBOX_FAILURE_EPISODE_KEY).await?);
    assert_eq!(
        observed_status(pool, sandbox_id).await?.as_deref(),
        Some("FAİLED")
    );
    Ok(())
}

/// A `failed` report spelled outside ASCII closes a failure episode an earlier
/// `failed` report opened, as any other unrecognized status does. A database
/// collation that lower-cases `İ` to `i` would keep it open.
#[tokio::test(flavor = "current_thread")]
async fn non_ascii_failed_spelling_closes_an_open_failure_episode() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;
    let sandbox_id = insert_running(pool, node_id, "started").await?;

    publish(pool, node_id, failed_report(sandbox_id, "failed")).await?;
    assert_eq!(lifecycle(pool, sandbox_id).await?, "failed");
    assert!(facet_has(pool, node_id, sandbox_id, SANDBOX_FAILURE_EPISODE_KEY).await?);

    publish(pool, node_id, failed_report(sandbox_id, "FAİLED")).await?;

    assert!(!facet_has(pool, node_id, sandbox_id, SANDBOX_FAILURE_EPISODE_KEY).await?);
    assert_eq!(lifecycle(pool, sandbox_id).await?, "failed");
    assert_eq!(
        observed_status(pool, sandbox_id).await?.as_deref(),
        Some("FAİLED")
    );
    Ok(())
}

/// `mark_published_failed`: an accepted spelling of `failed` moves a
/// `started` `k8s` row to `failed`. The observed state records each spelling
/// as published, trimmed.
#[tokio::test(flavor = "current_thread")]
async fn failed_projection_reads_every_ascii_spelling_of_failed() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("failed") {
        let sandbox_id = insert_running(pool, node_id, "started").await?;
        publish(pool, node_id, failed_report(sandbox_id, &status)).await?;
        let actual = (
            lifecycle(pool, sandbox_id).await?,
            observed_status(pool, sandbox_id).await?,
        );
        let expected = (
            if accepted { "failed" } else { "started" }.to_owned(),
            Some(status.trim().to_owned()),
        );
        if actual != expected {
            mismatches.push(format!("{status:?}: {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The failure-episode write: an accepted spelling of `failed` with an
/// execution generation records the episode.
#[tokio::test(flavor = "current_thread")]
async fn failure_episode_records_every_ascii_spelling_of_failed() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("failed") {
        let sandbox_id = insert_running(pool, node_id, "failed").await?;
        publish(pool, node_id, failed_report(sandbox_id, &status)).await?;
        let recorded = facet_has(pool, node_id, sandbox_id, SANDBOX_FAILURE_EPISODE_KEY).await?;
        if recorded != accepted {
            mismatches.push(format!("{status:?}: recorded {recorded}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The failure-episode clear: once an episode is open, an accepted spelling
/// of `failed` keeps it and any other status clears it.
#[tokio::test(flavor = "current_thread")]
async fn failure_episode_clear_keeps_every_ascii_spelling_of_failed() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("failed") {
        let sandbox_id = insert_running(pool, node_id, "failed").await?;
        publish(pool, node_id, failed_report(sandbox_id, "failed")).await?;
        assert!(facet_has(pool, node_id, sandbox_id, SANDBOX_FAILURE_EPISODE_KEY).await?);
        publish(pool, node_id, failed_report(sandbox_id, &status)).await?;
        let kept = facet_has(pool, node_id, sandbox_id, SANDBOX_FAILURE_EPISODE_KEY).await?;
        if kept != accepted {
            mismatches.push(format!("{status:?}: kept {kept}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The local suspend handoff: an accepted spelling of `suspended` with a
/// full suspend artifact records the handoff for a `k8s` row.
#[tokio::test(flavor = "current_thread")]
async fn local_suspend_handoff_records_every_ascii_spelling_of_suspended() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("suspended") {
        let sandbox_id = SandboxId::new();
        insert_active_sandbox(pool, sandbox_id, node_id, "started").await?;
        insert_k8s_execution(pool, sandbox_id, 1, "running", None).await?;
        let mut report = published_sandbox_state(sandbox_id, &status);
        report.execution_generation = Some(1);
        report.pod_uid = Some("pod-uid-1".to_owned());
        report.current_local_artifact_id = Some(Uuid::now_v7().to_string());
        report.current_local_artifact_kind =
            Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
        report.current_local_artifact_origin =
            Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);
        publish(pool, node_id, report).await?;
        let recorded = facet_has(pool, node_id, sandbox_id, SANDBOX_LOCAL_SUSPEND_KEY).await?;
        if recorded != accepted {
            mismatches.push(format!("{status:?}: recorded {recorded}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// `converge_published_restoring_to_started`: an accepted spelling of
/// `started` converges a `restoring` row with a routable execution.
#[tokio::test(flavor = "current_thread")]
async fn restored_convergence_reads_every_ascii_spelling_of_started() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("started") {
        let sandbox_id = insert_running(pool, node_id, "restoring").await?;
        publish(pool, node_id, published_sandbox_state(sandbox_id, &status)).await?;
        let actual = lifecycle(pool, sandbox_id).await?;
        if actual != if accepted { "started" } else { "restoring" } {
            mismatches.push(format!("{status:?}: {actual}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// `converge_published_stopped`: an accepted spelling of `stopped` converges
/// a `started` row.
#[tokio::test(flavor = "current_thread")]
async fn stopped_convergence_reads_every_ascii_spelling_of_stopped() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("stopped") {
        let sandbox_id = insert_running(pool, node_id, "started").await?;
        publish(pool, node_id, published_sandbox_state(sandbox_id, &status)).await?;
        let actual = lifecycle(pool, sandbox_id).await?;
        if actual != if accepted { "stopped" } else { "started" } {
            mismatches.push(format!("{status:?}: {actual}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// `release_completed_activations`: an activation-completed receipt for
/// `started` whose status is an accepted spelling of `started` releases the
/// attempt and projects `started`. Any other status disagrees with the
/// receipt, which is then ignored.
#[tokio::test(flavor = "current_thread")]
async fn activation_completion_reads_every_ascii_spelling_of_its_target() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("started") {
        let sandbox_id = insert_running(pool, node_id, "stopped").await?;
        let attempt = Uuid::now_v7();
        set_activation_attempt(pool, sandbox_id, attempt, "started").await?;
        let report = with_receipt(
            published_sandbox_state(sandbox_id, &status),
            "activation_completed",
            "started",
            attempt,
            "",
        );
        publish(pool, node_id, report).await?;
        let actual = (
            lifecycle(pool, sandbox_id).await?,
            activation_attempt(pool, sandbox_id).await?,
        );
        let expected = if accepted {
            ("started".to_owned(), None)
        } else {
            ("stopped".to_owned(), Some(attempt))
        };
        if actual != expected {
            mismatches.push(format!("{status:?}: {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// `select_activation_rollback_candidates`: an activation-rollback receipt
/// for `stopped` whose status is an accepted spelling of `stopped` moves a
/// `starting` row to `stopped`. Any other status disagrees with the receipt,
/// which is then ignored.
#[tokio::test(flavor = "current_thread")]
async fn activation_rollback_reads_every_ascii_spelling_of_its_target() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("stopped") {
        let sandbox_id = insert_running(pool, node_id, "starting").await?;
        let attempt = Uuid::now_v7();
        set_activation_attempt(pool, sandbox_id, attempt, "stopped").await?;
        let report = with_receipt(
            published_sandbox_state(sandbox_id, &status),
            "activation_rollback",
            "stopped",
            attempt,
            SandboxQuarantineReasonCode::ColdActivationInterrupted.as_str(),
        );
        publish(pool, node_id, report).await?;
        let actual = lifecycle(pool, sandbox_id).await?;
        if actual != if accepted { "stopped" } else { "starting" } {
            mismatches.push(format!("{status:?}: {actual}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// `recover_pause_failed_from_inventory`: an accepted spelling of `started`
/// or `paused` recovers an sbx-manager pause-failed quarantine to that
/// lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn pause_failed_recovery_reads_every_ascii_spelling_of_a_healthy_state() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for target in ["started", "paused"] {
        for (status, accepted) in ascii_spellings(target) {
            let sandbox_id = insert_running(pool, node_id, "quarantined").await?;
            sqlx::query(sql!(
                "INSERT INTO sandbox_quarantine_state (
                     sandbox_id, quarantine_id, entered_from_lifecycle, source,
                     reason_code, next_action, last_observed_node_id
                 )
                 VALUES ($1, $2, 'started', 'sbx_manager', 'pause_failed',
                         'requires_operator_attention', $3)"
            ))
            .bind(sandbox_id)
            .bind(Uuid::now_v7())
            .bind(node_id)
            .execute(pool)
            .await?;
            publish(pool, node_id, published_sandbox_state(sandbox_id, &status)).await?;
            let actual = lifecycle(pool, sandbox_id).await?;
            if actual != if accepted { target } else { "quarantined" } {
                mismatches.push(format!("{status:?}: {actual}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// `select_suspended_projection_candidates`: an accepted spelling of
/// `suspended` with a durable snapshot of the current cycle moves a `started`
/// row to `suspended`.
#[tokio::test(flavor = "current_thread")]
async fn suspended_projection_reads_every_ascii_spelling_of_suspended() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (status, accepted) in ascii_spellings("suspended") {
        let sandbox_id = insert_running(pool, node_id, "started").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET target_node_id = NULL WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        insert_ready_suspend_snapshot(pool, sandbox_id, node_id).await?;
        publish(pool, node_id, published_sandbox_state(sandbox_id, &status)).await?;
        let actual = lifecycle(pool, sandbox_id).await?;
        if actual != if accepted { "suspended" } else { "started" } {
            mismatches.push(format!("{status:?}: {actual}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
