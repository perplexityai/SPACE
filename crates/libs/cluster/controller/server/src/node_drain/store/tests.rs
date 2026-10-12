use pplx_space_common_core::types::{NodeId, SandboxId};
use uuid::Uuid;

use super::*;

#[test]
fn suspend_in_flight_restore_defers_to_the_next_sweep() {
    let error = SubstrateError::SuspendInFlight {
        message: "suspend finalization owns the execution".to_string(),
    };

    let outcome = converging_restore_outcome(&error)
        .expect("suspend ownership must remain a retryable restore outcome");
    let RestoreOutcome::Converging { detail, wait_phase } = outcome else {
        panic!("suspend ownership must classify as converging");
    };
    assert_eq!(detail, "suspend finalization owns the execution");
    assert_eq!(wait_phase, None);
}

#[test]
fn retained_only_copy_is_not_a_converging_suspend() {
    let error = SubstrateError::RetainedFilesystemOnlyCopy {
        message: "retained filesystem is the only copy".to_string(),
    };

    assert!(
        converging_restore_outcome(&error).is_none(),
        "a retained-only-copy refusal requires recovery and must remain visible as a stuck evacuation"
    );
}

#[test]
fn pending_restore_source_defers_to_the_next_sweep() {
    let error = SubstrateError::RestoreSourcePending {
        message: "restore source is not yet portable".to_string(),
    };

    let outcome = converging_restore_outcome(&error)
        .expect("pending restore source must remain a retryable restore outcome");
    let RestoreOutcome::Converging { detail, wait_phase } = outcome else {
        panic!("pending restore source must classify as converging");
    };
    assert_eq!(detail, "restore source is not yet portable");
    assert_eq!(wait_phase, None);
}

/// The snapshot-ready `LISTEN` connection must carry TCP keepalive +
/// `tcp_user_timeout` GUCs so Postgres reaps a dead or unreachable
/// cluster-controller within ~1 minute instead of letting its backend pin
/// the xmin horizon for hours — a pinned xmin starves autovacuum on the
/// high-churn `node_reconcile_*` tables and drives RDS CPU up as sequential
/// scans wade through the accumulated dead tuples. This asserts that
/// `connect_restore_listener` applies those GUCs to the session it returns
/// (it fails if the hardening `SET`s are dropped).
#[tokio::test(flavor = "current_thread")]
async fn restore_listener_hardens_connection_against_xmin_pinning()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    // connect_restore_listener applies the keepalive + tcp_user_timeout
    // GUCs on the session it returns.
    let mut listener = connect_restore_listener(&db.pool).await?;
    let hardened_timeout: String =
        sqlx::query_scalar(sql!("SELECT current_setting('tcp_user_timeout')"))
            .fetch_one(&mut listener)
            .await?;
    assert_eq!(
        hardened_timeout, "30000",
        "connect_restore_listener must set tcp_user_timeout (ms) on the session"
    );
    let keepalive_idle: String =
        sqlx::query_scalar(sql!("SELECT current_setting('tcp_keepalives_idle')"))
            .fetch_one(&mut listener)
            .await?;
    assert_ne!(
        keepalive_idle, "0",
        "connect_restore_listener must enable TCP keepalive probing"
    );

    Ok(())
}

/// A dropped backing connection must surface promptly to the caller
/// (`try_recv() -> Ok(None)` or `Err`) rather than silently healing onto a
/// fresh sqlx-reconnected — and therefore *unhardened* — backend. `recv()`
/// would instead loop back onto that reconnected backend and block, so this
/// test would time out. It pins the `try_recv` + `eager_reconnect(false)`
/// contract that forces every reconnect back through
/// `connect_restore_listener` (which re-applies the keepalive hardening).
#[tokio::test(flavor = "current_thread")]
async fn restore_listener_surfaces_dropped_connection()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    let mut listener = connect_restore_listener(&db.pool).await?;
    let pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut listener)
        .await?;

    // Kill the listener's backend out from under it.
    sqlx::query(sql!("SELECT pg_terminate_backend($1)"))
        .bind(pid)
        .execute(&db.pool)
        .await?;

    // The drop must surface within the timeout as Ok(None) or Err — not a
    // hang (which is what recv() would do after silently reconnecting).
    match tokio::time::timeout(Duration::from_secs(10), listener.try_recv()).await {
        Ok(Ok(None)) | Ok(Err(_)) => {}
        Ok(Ok(Some(_))) => panic!("unexpected notification after backend kill"),
        Err(_) => panic!("try_recv did not surface the dropped connection (hung)"),
    }

    Ok(())
}

/// The drain-completion edge drives two things — the promotion-client eviction
/// and the phase UPDATE's residency guard — so pinning it pins both.
#[test]
fn only_the_drain_completion_edge_evicts_the_client_and_guards_residency() {
    assert!(is_drain_completion_edge(PHASE_DRAINING, PHASE_DRAINED));
    assert!(!is_drain_completion_edge(PHASE_DRAINING, PHASE_TERMINATING));
    assert!(!is_drain_completion_edge(PHASE_DRAINED, PHASE_TERMINATING));

    // The guard rides on that edge and only that edge: a node leaving the drain
    // must not be held back by a sandbox that is still on it.
    assert!(
        set_drain_phase_query(ResidencyEvidence::LifecycleOnly, GuardScope::Wide)
            .contains("NOT EXISTS"),
        "the drain-completion UPDATE must re-check residency atomically"
    );
    assert!(
        !set_drain_phase_query(ResidencyEvidence::LifecycleOnly, GuardScope::None)
            .contains("NOT EXISTS"),
        "no other edge may inherit the guard"
    );

    // Gate off still guards, but on the narrower population. Asserting the
    // *lifecycle sets* rather than the presence of `NOT EXISTS` is what makes
    // this catch the defect the wide-always shape had: both scopes contain
    // `NOT EXISTS`, so that substring cannot tell them apart.
    let resident = set_drain_phase_query(ResidencyEvidence::LifecycleOnly, GuardScope::Resident);
    assert!(
        resident.contains("NOT EXISTS"),
        "gate off must still close the resident TOCTOU race"
    );
    for lifecycle in ["'stopped'", "'created'", "'deleting'", "'quarantined'"] {
        assert!(
            !resident.contains(lifecycle),
            "gate off must not enforce {lifecycle}: that is the term the flag withholds"
        );
    }
    let wide = set_drain_phase_query(ResidencyEvidence::LifecycleOnly, GuardScope::Wide);
    assert!(
        wide.contains("NOT IN"),
        "gate on must use the terminator's exclusion list, which covers those four"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn evacuation_redrives_pod_retirement_after_suspend_commit()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    // A real forced drain: the evacuation claim re-asserts the node's phase and
    // mode, so a node that isn't `draining` in `forced` mode cannot be evacuated —
    // which is the point of that guard, and makes this fixture match production
    // rather than relying on the claim not looking at the node.
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode)
         VALUES ($1, '10.0.0.99'::inet, 'worker', 'k8s', 'registered', 'draining', 'forced')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'on_node', 'k8s', $2, $2, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active
         ) VALUES ($1, 1, $2, 'space', 'pod-uid', 'running', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{sandbox_id}-g1"))
    .execute(&db.pool)
    .await?;

    let committed_artifact_id = Uuid::now_v7();
    let suspend_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let suspend_calls_for_fake = Arc::clone(&suspend_calls);
    let suspend_pool = db.pool.clone();
    let sink = SubstrateSink::with_suspend_execution(db.pool.clone(), move |requested_id| {
        let suspend_pool = suspend_pool.clone();
        let suspend_calls = Arc::clone(&suspend_calls_for_fake);
        async move {
            suspend_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            sqlx::query(sql!(
                "WITH committed AS (
                    UPDATE sandbox_k8s_executions
                       SET state = 'suspended'
                     WHERE sandbox_id = $1 AND active
                     RETURNING sandbox_id
                 )
                 UPDATE sandboxes
                    SET current_local_artifact_id = $2,
                        current_local_artifact_kind = 'full',
                        current_local_artifact_origin = 'suspend'
                  WHERE id = $1
                    AND lifecycle = 'suspending'
                    AND EXISTS (SELECT 1 FROM committed)"
            ))
            .bind(requested_id)
            .bind(committed_artifact_id)
            .execute(&suspend_pool)
            .await
            .map_err(|error| SubstrateError::Failed {
                message: error.to_string(),
            })?;
            Err(SubstrateError::Unavailable {
                message: "pod deletion remains unconfirmed after suspend commit".to_string(),
            })
        }
    });

    NodeDrainSink::evacuate_sandbox(&sink, sandbox_id, node_id, SandboxLifecycle::Started)
        .await
        .expect_err("pod deletion timeout after artifact commit must remain retryable");
    let (lifecycle, marker, artifact_id): (String, Option<String>, Option<Uuid>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, evacuation_restore_target, current_local_artifact_id
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(lifecycle, "suspending");
    assert_eq!(marker.as_deref(), Some("started"));
    assert_eq!(artifact_id, Some(committed_artifact_id));
    assert_eq!(
        NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?,
        vec![(sandbox_id, SandboxLifecycle::Started)],
        "the next drain tick must select the retained claim for pod-retirement redrive"
    );

    NodeDrainSink::evacuate_sandbox(&sink, sandbox_id, node_id, SandboxLifecycle::Started)
        .await
        .expect_err("pod retirement redrive must remain retryable");
    assert_eq!(
        suspend_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the second evacuation attempt must resume the committed suspend claim"
    );
    let (lifecycle, marker): (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT lifecycle, evacuation_restore_target
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(lifecycle, "suspending");
    assert_eq!(marker.as_deref(), Some("started"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn evacuation_reverts_claim_when_suspend_fails_before_commit()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    // Forced draining, so the claim lands and the suspend failure reaches the
    // revert; any other node state refuses the claim before the suspend.
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode)
         VALUES ($1, '10.0.0.99'::inet, 'worker', 'k8s', 'registered', 'draining', 'forced')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'paused', 'on_node', 'k8s', $2, $2, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active
         ) VALUES ($1, 1, $2, 'space', 'pod-uid', 'running', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{sandbox_id}-g1"))
    .execute(&db.pool)
    .await?;
    let suspend_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let suspend_calls_for_fake = Arc::clone(&suspend_calls);
    let sink = SubstrateSink::with_suspend_execution(db.pool.clone(), move |_requested_id| {
        suspend_calls_for_fake.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async move {
            Err(SubstrateError::Unavailable {
                message: "worker response was lost before artifact commit".to_string(),
            })
        }
    });

    NodeDrainSink::evacuate_sandbox(&sink, sandbox_id, node_id, SandboxLifecycle::Paused)
        .await
        .expect_err("a pre-commit suspend failure must remain retryable");
    assert_eq!(
        suspend_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the claim must land, so the failure reaches the revert"
    );
    let (lifecycle, marker, artifact_id): (String, Option<String>, Option<Uuid>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, evacuation_restore_target, current_local_artifact_id
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(lifecycle, "paused");
    assert_eq!(marker, None);
    assert_eq!(artifact_id, None);
    Ok(())
}

/// The failed-on-node classifier keys on `SUSPEND_SANDBOX_FAILED_MARKER`
/// being a substring of the suspend error. Pin both ends: the message
/// sbx-mgr renders for the `failed` state (and the adapter-wrapped form
/// that reaches `evacuate_sandbox`) must match, while a suspend rejection
/// for any *other* node-local state must NOT — flipping a merely-`stopped`
/// sandbox to `failed` would be wrong.
#[test]
fn suspend_failed_marker_classifies_failed_state_only() {
    // What sbx-mgr renders for the `failed` state (mirrors grpc_lifecycle).
    let failed_msg = format!("cannot suspend: {SUSPEND_SANDBOX_FAILED_MARKER}");
    assert!(is_sandbox_failed_on_node_rejection(&failed_msg));

    // The form that reaches `evacuate_sandbox`: the kubernetes adapter
    // wraps the node-api Status into `SubstrateError::Failed`, embedding the
    // status Display (code + message), so the marker survives as a substring.
    let adapter_wrapped =
        format!("sandbox suspend failed: status: FailedPrecondition, message: \"{failed_msg}\"");
    assert!(is_sandbox_failed_on_node_rejection(&adapter_wrapped));

    // A suspend rejection for a non-failed state must not be classified
    // terminal — those keep retrying (the divergence may yet self-heal).
    // `stopped` shares the same `FailedPrecondition` code, so this is the
    // case the marker (not the code) is what separates.
    let stopped_msg = "cannot suspend: sandbox is stopped, must be started or paused";
    assert!(!is_sandbox_failed_on_node_rejection(stopped_msg));

    // A generic transport/infra failure must not be classified terminal.
    assert!(!is_sandbox_failed_on_node_rejection(
        "sandbox suspend failed: status: Unavailable, message: \"node-api not reachable\""
    ));
}

/// The reupload give-up classifier keys on
/// `REUPLOAD_SANDBOX_RESTORING_MARKER` being a substring of the reupload
/// error, scoped to the `restoring` state only. Pin both ends: the message
/// sbx-mgr renders for the `restoring` state (and the adapter-wrapped
/// `Conflict` form that reaches `promote_local_snapshot`) must match, while
/// a refusal from a **running** state (`started`/`paused`) or a transient
/// `suspending` must NOT — abandoning their potentially-newer state for an
/// older snapshot would lose work — and terminal artifact-missing /
/// sandbox-unknown refusals and generic infra failures must NOT either.
#[test]
fn reupload_restoring_marker_classifies_restoring_refusals_only() {
    // What sbx-mgr renders for the stuck-`restoring` case (mirrors
    // grpc_lifecycle's producer), and the adapter-wrapped form that reaches
    // the promote classifier.
    let restoring_msg = "cannot reupload snapshot: sandbox is restoring, must be suspended";
    assert!(is_reupload_sandbox_restoring_rejection(restoring_msg));
    let adapter_wrapped =
        format!("reupload failed: status: FailedPrecondition, message: \"{restoring_msg}\"");
    assert!(is_reupload_sandbox_restoring_rejection(&adapter_wrapped));

    // Running / non-restoring states must NOT be classified as give-up
    // eligible: a started/paused VM holds live state newer than the
    // snapshot, and abandoning it would lose work. This is the data-loss
    // guard — the give-up is `restoring`-only by design.
    for state in ["started", "paused", "stopped", "suspending"] {
        assert!(
            !is_reupload_sandbox_restoring_rejection(&format!(
                "cannot reupload snapshot: sandbox is {state}, must be suspended"
            )),
            "a {state} reupload refusal must NOT be classified as give-up eligible"
        );
    }

    // The terminal reupload refusals (handled by the artifact-missing path)
    // and a generic infra failure must NOT be classified either.
    assert!(!is_reupload_sandbox_restoring_rejection(&format!(
        "cannot reupload snapshot: {REUPLOAD_ARTIFACT_MISSING_MARKER} at /space/... — pruned"
    )));
    assert!(!is_reupload_sandbox_restoring_rejection(&format!(
        "cannot reupload snapshot: {REUPLOAD_SANDBOX_UNKNOWN_MARKER} for sandbox_id=..."
    )));
    assert!(!is_reupload_sandbox_restoring_rejection(
        "reupload failed: status: Unavailable, message: \"node-api not reachable\""
    ));
}
