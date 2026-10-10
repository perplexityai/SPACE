//! Characterization of the lifecycle controller's refusal outcomes: which
//! prior lifecycles the `lifecycle = 'started'` writers in this module and in
//! `wake_completion` refuse, what `action` and `untracked_action` return when
//! they refuse, and what the `sandboxes` row looks like afterwards.
//!
//! Every case runs against the direct substrate. Paths that only the
//! Kubernetes substrate can reach (the redrive's hydration probe, the
//! drain-path `reachable_runtime_client == None` refusal) are left to that
//! substrate's own tests.

use axum::http::StatusCode;
use pplx_space_common_core::types::{NodeId, NodeManagedBy, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db_with_max_connections;
use pplx_space_metrics::sandbox::api::{SandboxApiMetricIdentity, WakeCompletionAction};
use pplx_space_node_api_sim::{SandboxRecord, SimState, SimWorkerHarness};
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::*;
use crate::controllers::sandbox::wake_completion::wake_completion_redrive;
use crate::controllers::substrate::Substrate;
use crate::controllers::substrate::direct::DirectSubstrate;

/// Values `sandboxes.lifecycle` can hold that are not `SandboxLifecycle`
/// variants (see `SandboxLifecycle::all`).
const NON_VARIANT_LIFECYCLES: [&str; 2] = ["deleted", "cleanup_required"];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    lifecycle: String,
    placement: String,
}

impl Row {
    fn new(lifecycle: &str, placement: &str) -> Self {
        Self {
            lifecycle: lifecycle.to_owned(),
            placement: placement.to_owned(),
        }
    }
}

async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    managed_by: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             template_id, content_hash, network_policy_override_json,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, $3, 'unplaced', 'seed', 'hash-seed', $4, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(managed_by)
    .bind(lifecycle)
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn place_on_node(pool: &PgPool, sandbox_id: SandboxId, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node', active_node_id = $2, auto_lifecycle_json = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(sqlx::types::Json(crate::e2b::auto_lifecycle_policy(
        60, true, true,
    )))
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_lifecycle(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<Row>> {
    Ok(sqlx::query_as::<_, (String, String)>(sql!(
        "SELECT lifecycle, placement FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?
    .map(|(lifecycle, placement)| Row {
        lifecycle,
        placement,
    }))
}

async fn tracked(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Sandbox> {
    Ok(crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .expect("sandbox row exists"))
}

fn direct_substrate(pool: &PgPool) -> Substrate {
    Substrate::Direct(Box::new(DirectSubstrate::new(pool.clone())))
}

fn direct_controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(direct_substrate(pool))
}

/// Cluster admission enabled over an empty direct inventory: every charged
/// activation is refused with a 429, so a refusal that is *not* a 429 proves
/// the refusing check ran before admission.
fn controller_with_empty_cluster_admission(pool: &PgPool) -> SandboxController {
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        direct_substrate(pool),
        crate::cluster_admission::ClusterSandboxAdmissionConfig {
            enabled: true,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    SandboxController::with_cluster_admission(direct_substrate(pool), admission)
}

fn identity(action: SandboxAction) -> SandboxApiMetricIdentity {
    SandboxApiMetricIdentity::native(action.as_str())
}

async fn run_action(
    controller: &SandboxController,
    pool: &PgPool,
    sandbox_id: SandboxId,
    sandbox: Option<&Sandbox>,
    action: SandboxAction,
    activation: Option<SandboxActivationRequest>,
) -> Result<serde_json::Value, ControllerError> {
    controller
        .action(
            pool,
            sandbox_id,
            sandbox,
            identity(action),
            action,
            activation,
        )
        .await
}

/// A registered direct worker backed by the in-process simulator. Callers
/// must have entered `enter_sim_worker_process` first.
async fn start_sim_worker(pool: &PgPool) -> TestResult<(SimWorkerHarness, NodeId)> {
    let node_id = NodeId::new();
    let harness = SimWorkerHarness::builder(node_id.to_string())
        .with_internal_api()
        .start()
        .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(harness.endpoint())
    .execute(pool)
    .await?;
    Ok((harness, node_id))
}

async fn insert_resident(state: &SimState, sandbox_id: SandboxId, status: &str) -> TestResult {
    let mut record = SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "seed".to_owned(),
        "fc".to_owned(),
        "root".to_owned(),
        Some("hash-seed".to_owned()),
        false,
    )
    .with_route_identity_provenance(1, 1, Some(NodeManagedBy::Space));
    record.status = status.to_owned();
    state.insert_new(record).await?;
    Ok(())
}

async fn set_drain_phase(pool: &PgPool, node_id: NodeId, phase: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = $2, drain_mode = 'forced' WHERE id = $1"
    ))
    .bind(node_id)
    .bind(phase)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// `complete_wake_lifecycle`: the post-wake / post-restore completion CAS.
// ---------------------------------------------------------------------------

/// Every stored lifecycle against both published-suspend fences. Refusals
/// leave both `lifecycle` and `placement` untouched; a completion writes
/// `started` / `on_node`.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_cas_outcome_for_every_prior_lifecycle() -> TestResult {
    use WakeLifecycleCompletion::{Completed, Lost, SupersededBySuspend};

    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;

    let mut cases: Vec<(String, PublishedSuspendFence, WakeLifecycleCompletion)> = Vec::new();
    for fence in [
        PublishedSuspendFence::Enforced,
        PublishedSuspendFence::DirectRestoreBaseline,
    ] {
        for lifecycle in SandboxLifecycle::all() {
            let expected = match lifecycle {
                SandboxLifecycle::Created
                | SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Started
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Checkpointing => Completed,
                SandboxLifecycle::Suspending => SupersededBySuspend,
                SandboxLifecycle::Suspended => match fence {
                    PublishedSuspendFence::Enforced => SupersededBySuspend,
                    PublishedSuspendFence::DirectRestoreBaseline => Completed,
                },
                SandboxLifecycle::Stopping
                | SandboxLifecycle::Stopped
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed => Lost,
            };
            cases.push((lifecycle.as_str().to_owned(), fence, expected));
        }
        for lifecycle in NON_VARIANT_LIFECYCLES {
            cases.push((lifecycle.to_owned(), fence, Lost));
        }
        // The guard compares a trimmed, lower-cased lifecycle.
        cases.push((" Failed ".to_owned(), fence, Lost));
        cases.push(("SUSPENDING".to_owned(), fence, SupersededBySuspend));
    }

    for (lifecycle, fence, expected) in cases {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, &lifecycle, "space").await?;

        let outcome = complete_wake_lifecycle(pool, sandbox_id, fence).await?;

        assert_eq!(
            outcome, expected,
            "prior lifecycle {lifecycle:?}, {fence:?}"
        );
        let expected_row = if expected == Completed {
            Row::new("started", "on_node")
        } else {
            Row::new(&lifecycle, "unplaced")
        };
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(expected_row),
            "prior lifecycle {lifecycle:?}, {fence:?}"
        );
    }

    // A row deleted while the wake was in flight is a lost CAS, not a
    // completion.
    for fence in [
        PublishedSuspendFence::Enforced,
        PublishedSuspendFence::DirectRestoreBaseline,
    ] {
        let missing = SandboxId::new();
        assert_eq!(complete_wake_lifecycle(pool, missing, fence).await?, Lost);
        assert_eq!(row(pool, missing).await?, None);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `action` refusal arms that return before any substrate call.
// ---------------------------------------------------------------------------

/// Wake (explicit or implicit) of a teardown or terminal lifecycle is a 409
/// naming the lifecycle. It is refused before cluster admission, so it is
/// never a 429 even with an exhausted limiter, and it writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn wake_refuses_teardown_and_terminal_lifecycles_before_admission() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = controller_with_empty_cluster_admission(pool);

    let refused = [
        SandboxLifecycle::Stopping.as_str(),
        SandboxLifecycle::Stopped.as_str(),
        SandboxLifecycle::Deleting.as_str(),
        SandboxLifecycle::Quarantined.as_str(),
        SandboxLifecycle::Failed.as_str(),
    ]
    .into_iter()
    .chain(NON_VARIANT_LIFECYCLES);
    for lifecycle in refused {
        for action in [
            SandboxAction::Wake,
            SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
        ] {
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, lifecycle, "space").await?;
            let sandbox = tracked(pool, sandbox_id).await?;

            let error = run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None)
                .await
                .expect_err("a teardown or terminal lifecycle cannot be woken");

            assert_eq!(
                error.status(),
                StatusCode::CONFLICT,
                "{action:?} on {lifecycle}"
            );
            assert_eq!(
                error.to_string(),
                format!("sandbox cannot be woken while lifecycle is {lifecycle}"),
            );
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(Row::new(lifecycle, "unplaced"))
            );
        }
    }
    Ok(())
}

/// The internal access-path labels are not lifecycle verbs: `action` answers
/// 400 for them, and `untracked_action` additionally refuses the DB-only
/// snapshot-policy verbs, which need a tracked row to update.
#[tokio::test(flavor = "current_thread")]
async fn non_lifecycle_actions_are_bad_requests_and_write_nothing() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let internal_labels = [
        SandboxAction::Access,
        SandboxAction::Stats,
        SandboxAction::OpenPort,
        SandboxAction::Rollback,
        SandboxAction::Spaced,
        SandboxAction::Update,
    ];

    for action in internal_labels {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "started", "space").await?;
        let sandbox = tracked(pool, sandbox_id).await?;

        let error = run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None)
            .await
            .expect_err("internal labels are not lifecycle actions");

        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{action:?}");
        assert_eq!(error.to_string(), format!("Unknown action: {action}"));
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new("started", "unplaced"))
        );
    }

    for action in internal_labels.into_iter().chain([
        SandboxAction::EnableSnapshots,
        SandboxAction::DisableSnapshots,
    ]) {
        let sandbox_id = SandboxId::new();

        let error = run_action(&controller, pool, sandbox_id, None, action, None)
            .await
            .expect_err("untracked sandboxes accept only runtime actions");

        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{action:?}");
        assert_eq!(error.to_string(), format!("Unknown action: {action}"));
    }
    Ok(())
}

/// Placement-affecting actions and the in-place `pause` / `checkpoint` are
/// refused with a 409 while the resolved node drains, before any substrate
/// call, and leave the row as it was.
#[tokio::test(flavor = "current_thread")]
async fn drain_gated_actions_conflict_and_leave_the_row() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'space', 'registered', 'draining', 'forced')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;

    for (action, lifecycle) in [
        (SandboxAction::Start, "stopped"),
        (SandboxAction::Wake, "paused"),
        (
            SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
            "paused",
        ),
        (SandboxAction::Resume, "paused"),
        (SandboxAction::Restore, "suspended"),
        (SandboxAction::Pause, "started"),
        (SandboxAction::Checkpoint, "started"),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, lifecycle, "space").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        let sandbox = tracked(pool, sandbox_id).await?;

        let error = run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None)
            .await
            .expect_err("a draining node refuses placement and in-place snapshot work");

        assert_eq!(error.status(), StatusCode::CONFLICT, "{action:?}");
        assert!(
            error.to_string().contains("draining"),
            "{action:?}: {error}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new(lifecycle, "on_node")),
            "{action:?}"
        );
    }
    Ok(())
}

/// When the substrate cannot reach a worker, every runtime action fails as
/// unavailable and none of them writes the lifecycle it would have reached.
#[tokio::test(flavor = "current_thread")]
async fn unreachable_runtime_fails_actions_without_writing_lifecycle() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);

    for (action, lifecycle) in [
        (SandboxAction::Start, "stopped"),
        (SandboxAction::Wake, "paused"),
        (SandboxAction::Suspend, "started"),
        (SandboxAction::Stop, "started"),
        (SandboxAction::Kill, "started"),
        (SandboxAction::Pause, "started"),
        (SandboxAction::Resume, "paused"),
        (SandboxAction::Checkpoint, "started"),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, lifecycle, "space").await?;
        let sandbox = tracked(pool, sandbox_id).await?;

        let error = run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None)
            .await
            .expect_err("no worker is assigned");

        assert_eq!(
            error.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{action:?}: {error}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new(lifecycle, "unplaced")),
            "{action:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `action` completion writes, against a simulated direct worker.
// ---------------------------------------------------------------------------

/// The wake handler maps the completion CAS to its response. The row is
/// moved after the handler read it, which is the race the CAS exists for:
/// a newer suspend claim is a silent success that keeps the claim, and a
/// teardown or terminal lifecycle is a 500 that keeps that lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_outcome_maps_to_the_wake_response() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;

    for moved_to in [
        None,
        Some("suspending"),
        Some("stopping"),
        Some("stopped"),
        Some("deleting"),
        Some("quarantined"),
        Some("failed"),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "paused", "space").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        insert_resident(worker.state(), sandbox_id, "paused").await?;
        let sandbox = tracked(pool, sandbox_id).await?;
        if let Some(lifecycle) = moved_to {
            set_lifecycle(pool, sandbox_id, lifecycle).await?;
        }

        let result = run_action(
            &controller,
            pool,
            sandbox_id,
            Some(&sandbox),
            SandboxAction::Wake,
            None,
        )
        .await;

        let ok_body = serde_json::json!({
            "ok": true,
            "action": "wake",
            "sandbox_id": sandbox_id.to_string(),
        });
        match moved_to {
            None => {
                assert_eq!(result?, ok_body);
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new("started", "on_node"))
                );
            }
            Some("suspending") => {
                assert_eq!(
                    result?, ok_body,
                    "a newer suspend claim is not a wake failure"
                );
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new("suspending", "on_node"))
                );
            }
            Some(lifecycle) => {
                let error = result.expect_err("a lost completion CAS fails the wake");
                assert_eq!(
                    error.status(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "{lifecycle}"
                );
                assert!(
                    error
                        .to_string()
                        .starts_with("sandbox wake lost the lifecycle CAS"),
                    "{lifecycle}: {error}"
                );
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new(lifecycle, "on_node"))
                );
            }
        }
    }
    Ok(())
}

/// Every runtime action that reports a resulting lifecycle (start, resume,
/// pause, stop, kill, suspend) writes it through the terminal-state guard:
/// a terminal lifecycle that landed while the runtime call was in flight is
/// kept, while the action itself still reports success.
#[tokio::test(flavor = "current_thread")]
async fn completed_actions_keep_a_terminal_lifecycle_that_landed_mid_flight() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;

    for (action, prior, resident, completed) in [
        (SandboxAction::Start, "stopped", "stopped", "started"),
        (SandboxAction::Resume, "paused", "paused", "started"),
        (SandboxAction::Pause, "started", "running", "paused"),
        (SandboxAction::Stop, "started", "running", "stopped"),
        (SandboxAction::Kill, "started", "running", "stopped"),
        (SandboxAction::Suspend, "started", "running", "suspended"),
    ] {
        let terminal = ["failed", "quarantined"]
            .into_iter()
            .chain(NON_VARIANT_LIFECYCLES)
            .map(Some);
        for landed in [None].into_iter().chain(terminal) {
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, prior, "space").await?;
            place_on_node(pool, sandbox_id, node_id).await?;
            insert_resident(worker.state(), sandbox_id, resident).await?;
            let sandbox = tracked(pool, sandbox_id).await?;
            if let Some(lifecycle) = landed {
                set_lifecycle(pool, sandbox_id, lifecycle).await?;
            }

            let body =
                run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None).await?;

            assert_eq!(
                body,
                serde_json::json!({
                    "ok": true,
                    "action": action.as_str(),
                    "sandbox_id": sandbox_id.to_string(),
                }),
            );
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(Row::new(landed.unwrap_or(completed), "on_node")),
                "{action:?} with {landed:?}"
            );
        }
    }
    Ok(())
}

/// Stop and kill are not drain-gated: on a draining node they still reach
/// the worker and mark the sandbox stopped.
#[tokio::test(flavor = "current_thread")]
async fn stop_and_kill_on_a_draining_node_still_reach_the_worker() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;
    set_drain_phase(pool, node_id, "draining").await?;

    for action in [SandboxAction::Stop, SandboxAction::Kill] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "started", "space").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        insert_resident(worker.state(), sandbox_id, "running").await?;
        let sandbox = tracked(pool, sandbox_id).await?;

        run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None).await?;

        assert_eq!(
            worker.state().get(&sandbox_id.to_string()).await?.status,
            "stopped",
            "{action:?}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new("stopped", "on_node")),
            "{action:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `untracked_action`: no Postgres row was supplied by the caller.
// ---------------------------------------------------------------------------

/// Without lifecycle context, untracked start and restore validate their
/// activation options first and then charge cluster admission
/// conservatively (cold / restore).
#[tokio::test(flavor = "current_thread")]
async fn untracked_start_and_restore_validate_then_charge_admission() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = controller_with_empty_cluster_admission(pool);

    for action in [SandboxAction::Start, SandboxAction::Restore] {
        let invalid = SandboxActivationRequest {
            blocking: None,
            ready_timeout_secs: Some(0),
        };
        let error = run_action(
            &controller,
            pool,
            SandboxId::new(),
            None,
            action,
            Some(invalid),
        )
        .await
        .expect_err("invalid activation options are rejected");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{action:?}");

        let error = run_action(&controller, pool, SandboxId::new(), None, action, None)
            .await
            .expect_err("an empty cluster refuses the activation");
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS, "{action:?}");
    }
    Ok(())
}

/// The direct substrate resolves a worker only through a placed `sandboxes`
/// row, so an untracked runtime action for an unknown id is unavailable.
#[tokio::test(flavor = "current_thread")]
async fn untracked_runtime_actions_without_placement_are_unavailable() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);

    for action in [
        SandboxAction::Start,
        SandboxAction::Wake,
        SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
        SandboxAction::Suspend,
        SandboxAction::Stop,
        SandboxAction::Pause,
        SandboxAction::Resume,
        SandboxAction::Kill,
        SandboxAction::Checkpoint,
    ] {
        let sandbox_id = SandboxId::new();
        let error = run_action(&controller, pool, sandbox_id, None, action, None)
            .await
            .expect_err("no worker can be resolved");
        assert_eq!(
            error.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{action:?}: {error}"
        );
        assert_eq!(row(pool, sandbox_id).await?, None);
    }
    Ok(())
}

/// Untracked actions report the status they reached in the body and never
/// write `sandboxes.lifecycle`, even when a row for the id exists (the
/// caller passed no tracked sandbox, so the controller does not own it).
#[tokio::test(flavor = "current_thread")]
async fn untracked_actions_report_status_and_leave_the_row() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;

    for (action, resident, status, resident_after) in [
        (SandboxAction::Start, "stopped", "started", "running"),
        (SandboxAction::Wake, "paused", "started", "running"),
        (SandboxAction::Stop, "running", "stopped", "stopped"),
        (SandboxAction::Kill, "running", "stopped", "stopped"),
        (SandboxAction::Pause, "running", "paused", "paused"),
        (SandboxAction::Resume, "paused", "started", "running"),
        (SandboxAction::Suspend, "running", "suspended", "suspended"),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "created", "space").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        insert_resident(worker.state(), sandbox_id, resident).await?;

        let body = run_action(&controller, pool, sandbox_id, None, action, None).await?;

        assert_eq!(
            body,
            serde_json::json!({
                "ok": true,
                "action": action.as_str(),
                "sandbox_id": sandbox_id.to_string(),
                "status": status,
            }),
        );
        assert_eq!(
            worker.state().get(&sandbox_id.to_string()).await?.status,
            resident_after,
            "{action:?}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new("created", "on_node")),
            "{action:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `delete` of an observed-only sandbox (`delete_observed_sandbox`).
// ---------------------------------------------------------------------------

async fn insert_observed_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         VALUES ($1, $2, 'running')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// With no tracked row, `delete` targets the node that last observed the
/// sandbox. An id no node observed is a 404.
#[tokio::test(flavor = "current_thread")]
async fn observed_only_delete_without_an_observation_is_not_found() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);

    let error = controller
        .delete(pool, SandboxId::new(), None)
        .await
        .expect_err("nothing observed the sandbox");

    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    Ok(())
}

/// The worker on the node that observed the sandbox deletes it, and only it.
#[tokio::test(flavor = "current_thread")]
async fn observed_only_delete_removes_the_resident_on_the_observing_node() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;
    let sandbox_id = SandboxId::new();
    let bystander = SandboxId::new();
    insert_resident(worker.state(), sandbox_id, "running").await?;
    insert_resident(worker.state(), bystander, "running").await?;
    insert_observed_state(pool, sandbox_id, node_id).await?;

    controller.delete(pool, sandbox_id, None).await?;

    assert!(
        worker.state().get(&sandbox_id.to_string()).await.is_err(),
        "the observed resident is deleted on its node"
    );
    assert_eq!(
        worker.state().get(&bystander.to_string()).await?.status,
        "running"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// `wake_completion_redrive`: the cancelled-wake completion on the direct
// substrate, whose hydration probe always answers routable.
// ---------------------------------------------------------------------------

async fn redrive(pool: &PgPool, sandbox_id: SandboxId) {
    // A zero budget makes exactly one attempt: read, probe, fenced CAS.
    wake_completion_redrive(
        pool.clone(),
        direct_substrate(pool),
        sandbox_id,
        WakeCompletionAction::Wake,
        std::time::Duration::ZERO,
    )
    .await;
}

/// The redrive claims only a `restoring` row; any other prior lifecycle
/// (including the non-variant values) is left exactly as found.
#[tokio::test(flavor = "current_thread")]
async fn redrive_completes_only_a_restoring_row() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;

    let lifecycles = SandboxLifecycle::all()
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .chain(NON_VARIANT_LIFECYCLES);
    for lifecycle in lifecycles {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, lifecycle, "space").await?;

        redrive(pool, sandbox_id).await;

        let expected = if lifecycle == SandboxLifecycle::Restoring.as_str() {
            Row::new("started", "on_node")
        } else {
            Row::new(lifecycle, "unplaced")
        };
        assert_eq!(row(pool, sandbox_id).await?, Some(expected), "{lifecycle}");
    }

    // A deleted row stays deleted.
    let missing = SandboxId::new();
    redrive(pool, missing).await;
    assert_eq!(row(pool, missing).await?, None);
    Ok(())
}

/// Even with a routable answer from the probe, the fenced CAS refuses a
/// `restoring` row under an unexpired restore lease, and a Kubernetes row
/// with no routable execution; an expired lease does not block it.
#[tokio::test(flavor = "current_thread")]
async fn redrive_cas_is_fenced_by_lease_and_routability() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;

    for (managed_by, lease, expected) in [
        ("space", Some("1 hour"), Row::new("restoring", "unplaced")),
        ("space", Some("-1 second"), Row::new("started", "on_node")),
        ("k8s", None, Row::new("restoring", "unplaced")),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "restoring", managed_by).await?;
        if let Some(lease) = lease {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET restore_lease_expires_at = NOW() + $2::INTERVAL,
                        restore_lease_token = $3
                  WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(lease)
            .bind(Uuid::now_v7())
            .execute(pool)
            .await?;
        }

        redrive(pool, sandbox_id).await;

        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(expected),
            "{managed_by} with lease {lease:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Delete-intent compensation.
// ---------------------------------------------------------------------------

/// Restoring the pre-delete lifecycle is a CAS on `deleting`: if anything
/// moved the row meanwhile it refuses with a 500 and leaves the row alone.
#[tokio::test(flavor = "current_thread")]
async fn failed_delete_compensation_refuses_a_row_that_left_deleting() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;

    for current in ["deleting", "started", "failed", "stopped"] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, current, "space").await?;

        let result =
            restore_lifecycle_after_failed_delete(pool, sandbox_id, SandboxLifecycle::Paused).await;

        if current == "deleting" {
            result?;
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(Row::new("paused", "unplaced"))
            );
        } else {
            let error = result.expect_err("the row left `deleting`");
            assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(Row::new(current, "unplaced")),
                "{current}"
            );
        }
    }
    Ok(())
}
