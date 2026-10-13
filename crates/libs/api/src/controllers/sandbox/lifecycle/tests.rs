use pplx_space_common_core::types::OrgId;
use pplx_space_common_core::types::{NodeId, SandboxId};

use crate::reconcile_queue::finalize_sandbox_db_delete_tx;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db_with_max_connections, raise_sqlstate};

use super::*;

use pplx_space_test_env::TestResult;

#[test]
fn cluster_delete_miss_reason_classifies_by_substrate_error() {
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::Unavailable {
            message: "sandbox is not assigned to an active worker node".into(),
        }),
        "node_unavailable",
    );
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::Failed {
            message: "node-api delete error".into(),
        }),
        "node_delete_failed",
    );
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::RetainedFilesystemOnlyCopy {
            message: "retained filesystem is the only copy".into(),
        }),
        "retained_filesystem_only_copy",
    );
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::Timeout {
            message: "deadline".into(),
            phase: None,
        }),
        "node_delete_timeout",
    );
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::Cancelled {
            message: "cancelled".into(),
        }),
        "node_delete_cancelled",
    );
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::NotFound {
            message: "absent".into(),
        }),
        "node_already_absent",
    );
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::Conflict {
            message: "conflict".into(),
        }),
        "other",
    );
    // Its own bucket, not `other`: a lost lock race on a control-plane
    // statement is one of the outcomes the `lock_timeout` rollout watches, and
    // `other` is where a newly-added variant silently lands.
    assert_eq!(
        cluster_delete_miss_reason(&SubstrateError::LockContention {
            message: "lost race".into(),
        }),
        "lock_contention",
    );
}

#[test]
fn protected_node_delete_defers_control_plane_delete() {
    assert!(cluster_delete_must_defer_control_plane(
        &SubstrateError::SuspendInFlight {
            message: "suspend publication pending".into(),
        }
    ));
    assert!(cluster_delete_must_defer_control_plane(
        &SubstrateError::RetainedFilesystemOnlyCopy {
            message: "retained filesystem is the only copy".into(),
        }
    ));
    assert!(!cluster_delete_must_defer_control_plane(
        &SubstrateError::Unavailable {
            message: "node is unreachable".into(),
        }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_sandbox_db_delete_removes_live_sandbox() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();

    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '192.0.2.111', 'worker', 'space')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, active_node_id, target_node_id,
             managed_by, lifecycle, placement, network_policy_override_json,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2,
             $2,
             'space',
             'started',
             'on_node',
             $3,
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(sqlx::types::Json(NetworkPolicyV1::default()))
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    finalize_sandbox_db_delete_tx(&mut tx, sandbox_id).await?;
    tx.commit().await?;

    let row = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT id
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(&db.pool)
    .await?;

    assert_eq!(row, None);
    Ok(())
}

/// Captures sandbox events (`event_stream = "space.sandbox"`) emitted on the
/// current thread, keyed by `event_type`, so a test can assert which
/// lifecycle events a controller call did (or did not) emit.
#[derive(Clone, Default)]
struct SandboxEventCapture {
    types: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl<S> tracing_subscriber::Layer<S> for SandboxEventCapture
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        event.record(&mut visitor);
        let fields = visitor.into_fields();
        if pplx_space_observability::test_capture::field_value(&fields, "event_stream")
            == Some("space.sandbox")
            && let Some(event_type) =
                pplx_space_observability::test_capture::field_value(&fields, "event_type")
        {
            self.types.lock().unwrap().push(event_type.to_string());
        }
    }
}

/// A rejected cluster activation must not leave an orphaned `*_requested`
/// sandbox event behind. The async operation dispatcher re-drives the
/// action on every requeue, so a `start_requested` emitted *before* the
/// admission gate — with no terminal `start_completed`/`start_failed` —
/// repeats on each backpressured retry and corrupts the event timeline.
/// Admission has to gate before the requested event is emitted.
#[tokio::test(flavor = "current_thread")]
async fn rejected_activation_emits_no_orphaned_requested_event() -> TestResult {
    use tracing_subscriber::layer::SubscriberExt as _;

    // Empty node inventory → the direct substrate reports zero eligible
    // start nodes → cluster admission rejects the Start with empty_cluster
    // before any runtime work.
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space',
             'created',
             'unplaced',
             $2,
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(NetworkPolicyV1::default()))
    .execute(&db.pool)
    .await?;

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");

    // Direct is only a placeholder capacity source: it reports zero eligible nodes,
    // as a synced, empty Kubernetes node cache would.
    let make_substrate = || {
        crate::controllers::substrate::Substrate::Direct(Box::new(
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        ))
    };
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        make_substrate(),
        crate::cluster_admission::ClusterSandboxAdmissionConfig {
            enabled: true,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    let controller = SandboxController::with_cluster_admission(make_substrate(), admission);

    let capture = SandboxEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    let result = controller
        .action(
            &db.pool,
            sandbox_id,
            Some(&sandbox),
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::ecw("start"),
            SandboxAction::Start,
            None,
        )
        .await;
    drop(guard);

    assert!(
        result.is_err(),
        "an empty cluster must reject the start activation",
    );
    let emitted = capture.types.lock().unwrap().clone();
    assert!(
        !emitted
            .iter()
            .any(|event_type| event_type == "start_requested"),
        "a rejected activation must not emit an orphaned start_requested event; saw {emitted:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_start_restore_spends_deferred_admission_at_entry() -> TestResult {
    // Arrange.
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space',
             'suspended',
             'on_node',
             $2,
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(NetworkPolicyV1::default()))
    .execute(&db.pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");
    let make_substrate = || {
        crate::controllers::substrate::Substrate::Direct(Box::new(
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        ))
    };
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        make_substrate(),
        crate::cluster_admission::ClusterSandboxAdmissionConfig {
            enabled: true,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    let controller = SandboxController::with_cluster_admission(make_substrate(), admission);

    // Act.
    let error = controller
        .action(
            &db.pool,
            sandbox_id,
            Some(&sandbox),
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::ecw("start"),
            SandboxAction::Start,
            None,
        )
        .await
        .expect_err("direct restore-shaped Start must pay admission");

    // Assert.
    assert_eq!(error.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
    Ok(())
}

/// Invalid activation options must be rejected before admission and before
/// the requested event: the malformed request returns a 400 (not the
/// admission 429) — proving validation ran first — and emits no orphaned
/// `start_requested` event, so no token is spent on a request that can't run.
#[tokio::test(flavor = "current_thread")]
async fn delete_fork_guard_persists_delete_intent_before_runtime_teardown() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             ) VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'space', 'started', 'on_node', 1
             )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    begin_delete_against_active_fork(&db.pool, sandbox_id).await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, "deleting");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_delete_restores_the_previous_lifecycle() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space', 'paused', 'on_node', 1
         )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let previous = begin_delete_against_active_fork(&db.pool, sandbox_id).await?;
    restore_lifecycle_after_failed_delete(
        &db.pool,
        sandbox_id,
        previous.expect("persisted sandbox has a previous lifecycle"),
    )
    .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, "paused");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_failure_restores_the_previous_lifecycle() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space', 'paused', 'on_node', 1
         )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let previous = begin_delete_against_active_fork(&db.pool, sandbox_id).await?;
    restore_lifecycle_after_failed_finalize_delete(&db.pool, sandbox_id, previous).await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, "paused");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_failure_accepts_an_already_deleted_row() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;

    restore_lifecycle_after_failed_finalize_delete(
        &db.pool,
        SandboxId::new(),
        Some(SandboxLifecycle::Paused),
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_fork_guard_allows_observed_only_cleanup_without_a_sandbox_row() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;

    let previous = begin_delete_against_active_fork(&db.pool, SandboxId::new()).await?;

    assert_eq!(previous, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn persisted_delete_intent_denies_later_fork_admission() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    let org_id = SEED_ORG_ID;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             ) VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 $2,
                 'space', 'started', 'on_node', 1
             )"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;

    begin_delete_against_active_fork(&db.pool, sandbox_id).await?;

    let source_sandbox_id = sandbox_id;
    let request = crate::fork_operations::ForkRequest::from_wire(Some(300), Some(1))?;
    let key = pplx_space_common_core::types::IdempotencyKey::parse("fork-after-delete-intent")?;
    let store = crate::public_operations::PublicOperationStore::new(db.pool.clone());
    let admission = crate::fork_operations::begin_fork_operation_admission(
        &store,
        OrgId::from_uuid(org_id),
        source_sandbox_id,
        request,
        None,
        &key,
    )
    .await?;
    let outcome = crate::fork_operations::admit_locked_fork_operation(
        admission,
        crate::fork_operations::AdmitForkOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            source_sandbox_id,
            request,
            ecw_host_domain: "e2b.example.test",
            hard_deadline: std::time::Duration::from_secs(240),
            federated_owner: None,
            federated_service_grant: None,
            idempotency_key: &key,
        },
    )
    .await?;
    assert_eq!(
        outcome,
        crate::public_operations::CreatePublicOperationResult::AdmissionDenied
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_activation_options_reject_before_admission_and_requested_event() -> TestResult {
    use tracing_subscriber::layer::SubscriberExt as _;

    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space',
             'created',
             'unplaced',
             $2,
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(NetworkPolicyV1::default()))
    .execute(&db.pool)
    .await?;

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");

    // Admission that would reject (empty cluster) — the invalid request must
    // still fail with a 400 before this ever runs.
    // Direct is only a placeholder capacity source: it reports zero eligible nodes,
    // as a synced, empty Kubernetes node cache would.
    let make_substrate = || {
        crate::controllers::substrate::Substrate::Direct(Box::new(
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        ))
    };
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        make_substrate(),
        crate::cluster_admission::ClusterSandboxAdmissionConfig {
            enabled: true,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    let controller = SandboxController::with_cluster_admission(make_substrate(), admission);

    let capture = SandboxEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    let result = controller
        .action(
            &db.pool,
            sandbox_id,
            Some(&sandbox),
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::ecw("start"),
            SandboxAction::Start,
            Some(crate::models::SandboxActivationRequest {
                blocking: None,
                ready_timeout_secs: Some(0),
            }),
        )
        .await;
    drop(guard);

    let err = result.expect_err("ready_timeout_secs=0 must be rejected");
    assert_eq!(
        err.status(),
        axum::http::StatusCode::BAD_REQUEST,
        "invalid options must fail validation (400), not admission (429)",
    );
    let emitted = capture.types.lock().unwrap().clone();
    assert!(
        !emitted
            .iter()
            .any(|event_type| event_type == "start_requested"),
        "an invalid request must not emit an orphaned start_requested event; saw {emitted:?}",
    );
    Ok(())
}

/// With a `lock_timeout` on the application role, the finalize transaction's
/// two lock acquisitions — the fork-source advisory lock and the sandbox row
/// `FOR UPDATE` — abort with `55P03` where they previously blocked until the
/// peer committed. The retry loop exists for exactly that: a bounded wait is
/// not a refusal. Before the bound, `55P03` could only reach this predicate
/// from a `NOWAIT` acquisition, which the finalize path does not use, so the
/// omission was invisible.
#[tokio::test(flavor = "current_thread")]
async fn finalize_delete_retries_bounded_lock_waits() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;

    for sqlstate in ["55P03", "40001", "40P01"] {
        let error = raise_sqlstate(&db.pool, sqlstate).await;
        assert!(
            is_retryable_finalize_sandbox_db_delete_error(&error),
            "{sqlstate} must be retryable"
        );
    }

    // A unique violation is a real conflict: retrying reproduces it, and
    // burning the retry budget on it only delays the caller's error.
    let unique_violation = raise_sqlstate(&db.pool, "23505").await;
    assert!(!is_retryable_finalize_sandbox_db_delete_error(
        &unique_violation
    ));
    Ok(())
}

/// The delete guard runs before any runtime teardown, so a contention abort
/// leaves nothing half-done and the caller can just retry. It has no retry
/// loop of its own, so the disposition has to be in the status code: a 503,
/// not the 500 every other guard failure maps to. Exercises the guard
/// end-to-end rather than `db_contention_error` in isolation, so a call site
/// that reverts to `internal_error` fails here.
#[tokio::test(flavor = "current_thread")]
async fn delete_guard_contention_is_retryable_not_internal() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             ) VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'space', 'started', 'on_node', 1
             )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    // Inject the abort at the guard's delete-intent UPDATE. A real
    // `lock_timeout` expiry raises the same SQLSTATE from the same statement;
    // the trigger just makes it deterministic without a second connection
    // racing for the row.
    sqlx::query(sql!(
        "CREATE FUNCTION fail_delete_intent_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION USING
                 ERRCODE = '55P03',
                 MESSAGE = 'injected delete-intent lock timeout';
         END;
         $$"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER fail_delete_intent_for_test
         BEFORE UPDATE ON sandboxes
         FOR EACH ROW
         EXECUTE FUNCTION fail_delete_intent_for_test()"
    ))
    .execute(&db.pool)
    .await?;

    let error = begin_delete_against_active_fork(&db.pool, sandbox_id)
        .await
        .expect_err("an injected lock timeout must fail the guard");
    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);

    // The sandbox keeps its lifecycle: the guard's transaction rolled back, so
    // a retry sees the same starting state rather than a row stranded in
    // `deleting`.
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    Ok(())
}

/// Exhausting the finalize retry budget is still contention, not a permanent
/// failure.
///
/// The retry loop treats `55P03` as retryable, but the exit *after* four failed
/// attempts converted the error to a flat 500 — so a caller that hit sustained
/// contention (a peer holding the row longer than four bounded waits) was told
/// the delete was unrecoverable, while the identical race resolved on attempt
/// three returned a retryable 503.
///
/// Drives `delete_control_plane` rather than re-deriving the disposition from
/// `db_contention_error`: an earlier version of this test asserted the helper's
/// answer directly, which stayed green when the production `match` arm was
/// reverted to `ControllerError::internal`. Injected via a trigger so all four
/// attempts fail deterministically; a real lock would have to be held across
/// the whole backoff to reproduce it.
#[tokio::test(flavor = "current_thread")]
async fn exhausting_the_finalize_retry_budget_stays_retryable() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             ) VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'space', 'stopped', 'on_node', 1
             )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    sqlx::query(sql!(
        "CREATE FUNCTION fail_finalize_with_contention_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION USING
                 ERRCODE = '55P03',
                 MESSAGE = 'injected sustained finalize contention';
         END;
         $$"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER fail_finalize_with_contention_for_test
         BEFORE DELETE ON sandboxes
         FOR EACH ROW
         EXECUTE FUNCTION fail_finalize_with_contention_for_test()"
    ))
    .execute(&db.pool)
    .await?;

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");
    let make_substrate = || {
        crate::controllers::substrate::Substrate::Direct(Box::new(
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        ))
    };
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        make_substrate(),
        crate::cluster_admission::ClusterSandboxAdmissionConfig {
            enabled: false,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    let controller = SandboxController::with_cluster_admission(make_substrate(), admission);
    let error = controller
        .delete_control_plane(&db.pool, sandbox_id, Some(&sandbox))
        .await
        .expect_err("every finalize attempt must fail");

    assert_eq!(
        error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "an exhausted retry budget is not a 500"
    );
    assert!(
        error.is_lock_contention(),
        "the exhausted error must keep its contention identity so the public \
         and ECW translators do not re-label it as a fleet condition"
    );
    Ok(())
}

/// The delete pre-flight's `55P03` is retryable, not an unrecoverable delete.
///
/// `any_creating_snapshot_for_sandbox` is the first database statement
/// `delete_control_plane` runs, and it is a plain `SELECT` on `snapshots` — so
/// under the role-level `lock_timeout` it aborts with `55P03` whenever DDL
/// holds `ACCESS EXCLUSIVE` on that table. It answered a flat 500, telling the
/// caller the delete had failed for good before anything at all had been
/// written; nothing had, so the identical retry succeeds.
///
/// Drives `delete_control_plane` rather than asserting `db_contention_error`'s
/// answer, for the reason recorded on
/// `exhausting_the_finalize_retry_budget_stays_retryable`: a helper-level
/// assertion stays green when the production arm is reverted to
/// `ControllerError::internal`. The contention is a real held lock rather than
/// an injected `RAISE`, because a `SELECT` has no trigger to hang one on — so
/// the pre-check is handed a pool whose sessions carry the role-level bound.
#[tokio::test(flavor = "current_thread")]
async fn a_contended_delete_precheck_stays_retryable() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             ) VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 $2,
                 'space', 'stopped', 'on_node', 1
             )"
    ))
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");

    // The role-level `lock_timeout`, applied here per session so the
    // pre-check aborts instead of blocking until the lock is released.
    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            db.database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("lock_timeout", "250ms")]),
        )
        .await?;

    // `ACCESS EXCLUSIVE` is what DDL on `snapshots` takes; it conflicts with
    // the pre-check's `ACCESS SHARE`, so the `SELECT` blocks and then aborts.
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE snapshots IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;

    let controller =
        SandboxController::new(crate::controllers::substrate::Substrate::Direct(Box::new(
            crate::controllers::substrate::direct::DirectSubstrate::new(bounded.clone()),
        )));
    let error = controller
        .delete_control_plane(&bounded, sandbox_id, Some(&sandbox))
        .await
        .expect_err("the pre-check cannot read snapshots while DDL holds the table");

    blocker.rollback().await?;

    assert_eq!(
        error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a pre-flight that wrote nothing is retryable, not an internal failure"
    );
    assert!(
        error.is_lock_contention(),
        "the error must carry the contention identity: `translate::controller_error` \
         reads it to answer the ECW `DELETE` with `lock_contention` rather than a \
         fleet 503, and the status alone would pass whether the arm classified or \
         just happened to answer 503"
    );

    // The retry the 503 advertises has to actually get somewhere: with the lock
    // released, the identical call must clear the pre-check. Asserted as "not
    // contention" rather than `Ok` because this fixture has no substrate to
    // complete the delete against — what is being proved is that the abort was
    // transient, which a status assertion alone cannot show.
    let retried = controller
        .delete_control_plane(&bounded, sandbox_id, Some(&sandbox))
        .await;
    if let Err(error) = &retried {
        assert!(
            !error.is_lock_contention(),
            "the pre-check must clear once the lock is released; still contended: {error}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn resume_uses_resolved_runtime_without_async_observation() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    for (generation, supported) in [(Some(1), true), (None, true), (Some(1), false)] {
        let id = SandboxId::new();
        let node = NodeId::new();
        let builder = pplx_space_node_api_sim::SimWorkerHarness::builder(node.to_string())
            .with_internal_api();
        let worker = if supported {
            builder
        } else {
            builder.without_activation_policy_support()
        }
        .start()
        .await?;
        sqlx::query(sql!("INSERT INTO nodes (id, address, role, managed_by, state) VALUES ($1, '127.0.0.1', 'worker', 'space', 'ready')"))
        .bind(node).execute(&db.pool).await?;
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
        ))
        .bind(node)
        .bind(worker.endpoint())
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement, active_node_id, template_id, content_hash, network_policy_generation) VALUES ($1, '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'space', 'paused', 'on_node', $2, 'seed', 'hash-seed', 1)"))
        .bind(id).bind(node).execute(&db.pool).await?;
        let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
            id.to_string(),
            "seed".into(),
            "fc".into(),
            "test".into(),
            None,
            false,
        );
        record.status = "paused".into();
        record.execution_generation = generation;
        worker.state().insert_new(record).await?;
        let controller =
            SandboxController::new(crate::controllers::substrate::Substrate::Direct(Box::new(
                crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
            )));
        let mut client = controller
            .substrate
            .runtime_client(id, crate::controllers::substrate::RecoveryCharge::Exempt)
            .await?;
        let request = controller
            .resume_request_with_current_policy(
                &db.pool,
                id,
                &mut client,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: std::time::Duration::from_secs(5),
                },
            )
            .await;
        if !supported {
            assert_eq!(
                request.unwrap_err().status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(worker.state().get(&id.to_string()).await?.status, "paused");
            continue;
        }
        let request = request?;
        assert_eq!(request.expected_execution_generation, generation);
        client.resume_sandbox(request).await?;
        assert_eq!(worker.state().get(&id.to_string()).await?.status, "running");
    }
    Ok(())
}
