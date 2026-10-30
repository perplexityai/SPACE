//! Characterization of the lifecycle controller's refusal outcomes: which
//! prior lifecycles the lifecycle writers in this module and in
//! `wake_completion` refuse, what `action` and `untracked_action` return when
//! they refuse, and what the `sandboxes` row looks like afterwards. The
//! completion CAS itself (`complete_wake_lifecycle`) is swept in
//! `wake_completion_postgres_tests`.
//!
//! Every case here runs against the direct substrate. The cases whose path
//! differs by substrate also run against the Kubernetes substrate in the
//! `kubernetes` submodule, whose doc names the three twins it renames; each
//! twin's doc says whether its outcome differs. That module also covers the
//! drain path's `reachable_runtime_client == None` refusal of stop and kill,
//! which only Kubernetes reaches.

use axum::http::StatusCode;
use pplx_space_common_core::types::{NodeId, NodeManagedBy, SandboxId, UserId};
use pplx_space_db_test_fixtures::pg_test_db_with_max_connections;
use pplx_space_metrics::sandbox::api::{SandboxApiMetricIdentity, WakeCompletionAction};
use pplx_space_node_api_sim::{SandboxRecord, SimState, SimWorkerHarness};
use pplx_space_placement::StoredSandboxLifecycle;
use pplx_space_placement::test_support::{known_defects, stored_lifecycle_matrix};
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::sandbox::wake_completion::wake_completion_redrive;
use crate::test_support::direct_substrate;

#[cfg(feature = "kubernetes")]
mod kubernetes;

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

async fn set_placement(pool: &PgPool, sandbox_id: SandboxId, placement: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET placement = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(placement)
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
// `action` refusal arms that return before any substrate call.
// ---------------------------------------------------------------------------

/// Wake (explicit or implicit) of a teardown or terminal lifecycle is a 409
/// naming the lifecycle. It is refused before cluster admission, so it is
/// never a 429 even with an exhausted limiter, and it writes nothing. Every
/// other stored lifecycle passes the entry guard and is refused later (by the
/// empty cluster's admission, or by the substrate for a restore-shaped wake)
/// with something other than a 409.
///
/// The entry guard is deliberately narrower than the completion CAS
/// (`complete_wake_lifecycle`): `suspending` passes here and the CAS defers
/// to it (as it does to `suspended` on Kubernetes); on direct, `suspended` is
/// the restore baseline the CAS advances from.
#[tokio::test(flavor = "current_thread")]
async fn wake_refuses_teardown_and_terminal_lifecycles_before_admission() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = controller_with_empty_cluster_admission(pool);

    let stored = StoredSandboxLifecycle::all().iter().map(|stored| {
        let refused = match stored {
            StoredSandboxLifecycle::Live(
                SandboxLifecycle::Stopping
                | SandboxLifecycle::Stopped
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed,
            )
            | StoredSandboxLifecycle::Deleted
            | StoredSandboxLifecycle::CleanupRequired => true,
            StoredSandboxLifecycle::Live(
                SandboxLifecycle::Created
                | SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Started
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Checkpointing
                | SandboxLifecycle::Suspending
                | SandboxLifecycle::Suspended,
            ) => false,
        };
        (stored.as_str(), refused)
    });
    for (lifecycle, refused) in stored {
        for action in [
            SandboxAction::Wake,
            SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
        ] {
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, lifecycle, "space").await?;
            let sandbox = tracked(pool, sandbox_id).await?;

            let error = run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None)
                .await
                .expect_err("the empty cluster admits no wake");

            if refused {
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
            } else {
                assert_ne!(
                    error.status(),
                    StatusCode::CONFLICT,
                    "{action:?} on {lifecycle} passes the entry guard: {error}"
                );
            }
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
    // Direct is only a placeholder here: this test never reaches the substrate.
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
        // No action writes `created`, so any lifecycle write is visible.
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "created", "space").await?;
        let sandbox = tracked(pool, sandbox_id).await?;

        let error = run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None)
            .await
            .expect_err("internal labels are not lifecycle actions");

        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{action:?}");
        assert_eq!(error.to_string(), format!("Unknown action: {action}"));
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new("created", "unplaced")),
            "{action:?}"
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
    // Direct is only a placeholder here: this test never reaches the substrate.
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
///
/// A wake from `suspended` is a worker-local restore that leaves the row
/// `suspended` for the CAS to advance, so it reaches `started` only because
/// the handler takes its fence from Direct's capabilities, which do not fence
/// `suspended`.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_outcome_maps_to_the_wake_response() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;

    for (prior, moved_to) in ["paused", "suspended"].into_iter().flat_map(|prior| {
        [
            None,
            Some("suspending"),
            Some("stopping"),
            Some("stopped"),
            Some("deleting"),
            Some("quarantined"),
            Some("failed"),
        ]
        .map(|moved_to| (prior, moved_to))
    }) {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, prior, "space").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        if prior == "suspended" {
            insert_resident(worker.state(), sandbox_id, "running").await?;
            worker
                .state()
                .suspend_to_shared_dir(&sandbox_id.to_string())
                .await?;
        } else {
            insert_resident(worker.state(), sandbox_id, prior).await?;
        }
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
                assert_eq!(result?, ok_body, "woken from {prior}");
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new("started", "on_node")),
                    "woken from {prior}"
                );
            }
            Some("suspending") => {
                assert_eq!(
                    result?, ok_body,
                    "woken from {prior}: a newer suspend claim is not a wake failure"
                );
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new("suspending", "on_node")),
                    "woken from {prior}"
                );
            }
            Some(lifecycle) => {
                let error = result.expect_err("a lost completion CAS fails the wake");
                assert_eq!(
                    error.status(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "woken from {prior}, moved to {lifecycle}"
                );
                assert!(
                    error
                        .to_string()
                        .starts_with("sandbox wake lost the lifecycle CAS"),
                    "woken from {prior}, moved to {lifecycle}: {error}"
                );
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new(lifecycle, "on_node")),
                    "woken from {prior}, moved to {lifecycle}"
                );
            }
        }
    }
    Ok(())
}

/// Every runtime action that reports a resulting lifecycle (start, resume,
/// pause, stop, kill, suspend) writes it through the terminal-state guard:
/// a terminal lifecycle that landed while the runtime call was in flight is
/// kept, together with the placement, while the action itself still reports
/// success. Only start also writes the placement (`on_node`), so the row is
/// seeded `unplaced` with its active node still assigned.
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
        let completed_placement = if action == SandboxAction::Start {
            "on_node"
        } else {
            "unplaced"
        };
        // Only the lifecycles both guard families treat as terminal. The
        // cross-intent lifecycles `deleting`, `stopping`, `suspending` and
        // `suspended` are deliberately excluded: this post-result write
        // currently replaces them with the action's own lifecycle, which is
        // an open bug and is not encoded here.
        let terminal = ["failed", "quarantined"]
            .into_iter()
            .chain(StoredSandboxLifecycle::tombstones_as_str())
            .map(Some);
        for landed in [None].into_iter().chain(terminal) {
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, prior, "space").await?;
            place_on_node(pool, sandbox_id, node_id).await?;
            set_placement(pool, sandbox_id, "unplaced").await?;
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
            let expected = match landed {
                None => Row::new(completed, completed_placement),
                Some(lifecycle) => Row::new(lifecycle, "unplaced"),
            };
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(expected),
                "{action:?} with {landed:?}"
            );
        }
    }
    Ok(())
}

/// The two post-action stamps and the pooled post-wake update share one
/// guard: they write unless the row is `failed`, `quarantined` or a
/// tombstone. Swept here over every stored value with today's outcome, so a
/// change to that guard fails this table. Columns: `stamp` is
/// `maybe_update_sandbox_lifecycle` with the pause stamp `paused`;
/// `state_stamp` is `maybe_update_sandbox_state` with start's
/// `started`/`on_node`; `post_wake` is `update_and_fetch_post_wake_sandbox`.
///
/// `!` marks the known defect `completed_actions_keep_a_terminal_lifecycle_that_landed_mid_flight`
/// leaves unencoded: a write that lands after a cross-intent transition
/// (`deleting`, `stopping`, `suspending`, `suspended`, however spelled)
/// replaces it with the action's own lifecycle. Fixing it means turning
/// those marks into `.` and lowering `KNOWN_DEFECTS`. Whether a row is
/// written is read from `xmin`, so a stamp that rewrites the value the row
/// already holds still counts as applied.
#[tokio::test(flavor = "current_thread")]
async fn post_action_stamps_write_every_stored_lifecycle_but_the_terminal_ones() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              stamp  state_stamp  post_wake
        "created           A        A           A",
        "creating          A        A           A",
        "starting          A        A           A",
        "started           A        A           A",
        "restoring         A        A           A",
        "pausing           A        A           A",
        "paused            A        A           A",
        "resuming          A        A           A",
        "suspending        !        !           !",
        "suspended         !        !           !",
        "stopping          !        !           !",
        "stopped           A        A           A",
        "deleting          !        !           !",
        "checkpointing     A        A           A",
        "quarantined       .        .           .",
        "failed            .        .           .",
        "deleted           .        .           .",
        "cleanup_required  .        .           .",
        "bogus             A        A           A",
        "_Suspended_       !        !           !",
    ];
    /// Five cross-intent rows in each of three columns.
    const KNOWN_DEFECTS: usize = 15;

    async fn written_row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(String, Row)> {
        let (xmin, lifecycle, placement): (String, String, String) = sqlx::query_as(sql!(
            "SELECT xmin::text, lifecycle, placement FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        Ok((xmin, Row::new(&lifecycle, &placement)))
    }

    #[derive(Debug, Clone, Copy)]
    enum Writer {
        Stamp,
        StateStamp,
        PostWake,
    }

    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;
    let writers = [Writer::Stamp, Writer::StateStamp, Writer::PostWake];
    let matrix = stored_lifecycle_matrix(writers, EXPECTED);
    assert_eq!(known_defects(&matrix), KNOWN_DEFECTS);

    let mut mismatches = Vec::new();
    for (stored, cells) in matrix {
        for (writer, expect) in cells {
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, stored, "space").await?;
            let (xmin_before, before) = written_row(pool, sandbox_id).await?;

            // The stamps are best-effort and report nothing; the post-wake
            // update reports a refusal as an error.
            let (moved_to, reported) = match writer {
                Writer::Stamp => {
                    maybe_update_sandbox_lifecycle(pool, sandbox_id, "paused").await;
                    (Row::new("paused", "unplaced"), None)
                }
                Writer::StateStamp => {
                    maybe_update_sandbox_state(pool, sandbox_id, "started", "on_node").await;
                    (Row::new("started", "on_node"), None)
                }
                Writer::PostWake => {
                    let updated =
                        update_and_fetch_post_wake_sandbox(pool, sandbox_id, Some(NodeId::new()))
                            .await;
                    (Row::new("started", "on_node"), Some(updated.is_ok()))
                }
            };

            let (xmin_after, after) = written_row(pool, sandbox_id).await?;
            let written = xmin_after != xmin_before;
            let expected = if expect.applies() { moved_to } else { before };
            if written != expect.applies()
                || reported.is_some_and(|updated| updated != expect.applies())
                || after != expected
            {
                mismatches.push(format!(
                    "{writer:?} from {stored:?} ({expect:?}): written {written}, row now {after:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// A native start operation whose post-result writes find a `stopped` written
/// while its runtime call was in flight write `started` over it: first
/// `maybe_update_sandbox_state(.., "started", "on_node")`, then the shared
/// `maybe_update_sandbox_lifecycle`. The lifecycle operation executor reports
/// success, without a retry, with the sandbox running. This pins today's
/// start write, which has no CAS on the lifecycle it read (only the terminal
/// guard, pinned by `completed_actions_keep_a_terminal_lifecycle_that_landed_mid_flight`),
/// not an invariant: the wake path makes the opposite choice for the same
/// race, and its completion CAS refuses `started` over a mid-flight `stopped`
/// (`wake_completion_outcome_maps_to_the_wake_response`). Unifying the two is
/// a decision, not a refactor. What the overwrite relies on is node
/// publication: it converges a row the node reports stopped back to
/// `stopped`, and a node report of `started` never moves a `stopped` row
/// (`node_publication_converges_only_from_its_source_lifecycles` in the
/// internal API). So outside the Kubernetes activation-receipt path, a
/// refused write over a running sandbox would stay wrong; that is the cost of
/// wake's opposite choice. On that path, an activation-completion receipt
/// whose attempt fence still matches sets the receipt's target lifecycle over
/// any row its excluded list omits, `stopped` included
/// (`consume_activation_completion_receipts` in the internal API).
///
/// This drives `SandboxLifecycleOperationExecutor::execute` directly. The
/// `stopped` is written by a trigger on the start's route-identity receipt,
/// which the direct substrate persists after the node start returns and
/// before the lifecycle write; it changes only the database row and fires for
/// this test's sandbox only. The trigger counts its firings so the test can
/// prove the injected `stopped` landed; it does not count node starts.
#[tokio::test(flavor = "current_thread")]
async fn native_start_overwrites_a_stopped_written_mid_flight_without_a_retry() -> TestResult {
    use crate::public_operations::{ApiProduct, ClaimedPublicOperation};
    use crate::public_sandbox_operations::{
        SandboxLifecycleOperationExecutor, SandboxLifecycleOperationPayload,
    };
    use pplx_space_public_api::{
        OperationId, OperationKind, OperationState, PublicOperationResult, PublicSandboxId,
        SandboxLifecycleAction, SandboxLifecycleState,
    };

    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_sim_worker(pool).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "created", "space").await?;
    place_on_node(pool, sandbox_id, node_id).await?;
    set_placement(pool, sandbox_id, "unplaced").await?;
    insert_resident(worker.state(), sandbox_id, "creating").await?;
    sqlx::query(sql!(
        "CREATE TABLE test_route_receipts (sandbox_id UUID PRIMARY KEY, receipts INT NOT NULL)"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO test_route_receipts (sandbox_id, receipts) VALUES ($1, 0)"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION test_stop_after_first_route_receipt() RETURNS trigger
         LANGUAGE plpgsql AS $$
         DECLARE receipts INT;
         BEGIN
             UPDATE test_route_receipts r
                SET receipts = r.receipts + 1
              WHERE r.sandbox_id = NEW.sandbox_id
             RETURNING r.receipts INTO receipts;
             IF receipts = 1 THEN
                 UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = NEW.sandbox_id;
             END IF;
             RETURN NEW;
         END $$"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER test_stop_after_first_route_receipt
         AFTER INSERT OR UPDATE ON sandbox_direct_route_identities
         FOR EACH ROW EXECUTE FUNCTION test_stop_after_first_route_receipt()"
    ))
    .execute(pool)
    .await?;

    let operation = ClaimedPublicOperation {
        id: OperationId::new(),
        org_id: "00000000-0000-0000-0000-000000000001".parse()?,
        actor_user_id: Some("00000000-0000-0000-0000-000000000010".parse()?),
        api_product: ApiProduct::Native,
        kind: OperationKind::StartSandbox,
        target_type: None,
        target_id: Some(PublicSandboxId::from(sandbox_id).to_string()),
        payload: SandboxLifecycleOperationPayload {
            sandbox_id: PublicSandboxId::from(sandbox_id),
            action: SandboxLifecycleAction::Start,
            rollback: None,
            wake_activity: None,
        }
        .into_json(),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
        age_at_first_claim_secs: None,
    };
    let executor = SandboxLifecycleOperationExecutor::new(pool.clone(), direct_controller(pool));

    let result = executor.execute(&operation).await;

    let receipts: i32 = sqlx::query_scalar(sql!(
        "SELECT receipts FROM test_route_receipts WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert!(receipts >= 1, "the mid-flight stopped was never written");
    assert_eq!(result.state, OperationState::Succeeded, "{result:?}");
    assert!(result.retry_after.is_none(), "{result:?}");
    let reported = result
        .result_json
        .clone()
        .ok_or("a succeeded operation reports a result")?;
    let PublicOperationResult::Sandbox(reported) = serde_json::from_value(reported)? else {
        return Err(format!("not a sandbox operation result: {result:?}").into());
    };
    assert_eq!(reported.state, SandboxLifecycleState::Running);
    assert_eq!(
        row(pool, sandbox_id).await?,
        Some(Row::new("started", "on_node"))
    );
    Ok(())
}

/// A completed action does not wait behind the `FOR KEY SHARE` lock every
/// foreign-key insert into a child table takes on the sandbox row; a node
/// publication transaction can hold one for its whole duration. The open
/// transaction stands in for that insert and holds its lock across the whole
/// action. Start is seeded unplaced so its lifecycle-and-placement write is
/// pinned under the lock as well.
///
/// Direct suspend is the exception: it persists its suspend artifact under
/// `SELECT ... FOR UPDATE` on the sandbox row
/// (`persist_optional_current_local_artifact`), which conflicts with
/// `FOR KEY SHARE`. It waits on that lock while the child insert is open and
/// completes once the insert ends.
///
/// This holds on direct only: on Kubernetes, start and wake wait too
/// (`kubernetes::activations_and_suspend_wait_for_a_key_share_lock`).
#[tokio::test(flavor = "current_thread")]
async fn completed_actions_do_not_wait_for_a_key_share_lock() -> TestResult {
    use std::time::Duration;

    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    // The open child-insert transaction holds one connection, and the
    // lock-wait probe below takes another while suspend waits on its own.
    let db = pg_test_db_with_max_connections(5).await?;
    let pool = &db.pool;
    let controller = direct_controller(pool);
    let (worker, node_id) = start_sim_worker(pool).await?;
    let seed = async |sandbox_id: SandboxId, prior: &str, placement: &str, resident: &str| {
        insert_sandbox(pool, sandbox_id, prior, "space").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        set_placement(pool, sandbox_id, placement).await?;
        insert_resident(worker.state(), sandbox_id, resident).await?;
        tracked(pool, sandbox_id).await
    };
    let hold_key_share = async |sandbox_id: SandboxId| -> TestResult<_> {
        let mut child_insert = pool.begin().await?;
        sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE"))
            .bind(sandbox_id)
            .execute(&mut *child_insert)
            .await?;
        let holder: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *child_insert)
            .await?;
        Ok((child_insert, holder))
    };

    for (action, prior, resident, completed) in [
        (SandboxAction::Start, "stopped", "stopped", "started"),
        (SandboxAction::Wake, "paused", "paused", "started"),
        (SandboxAction::Resume, "paused", "paused", "started"),
        (SandboxAction::Pause, "started", "running", "paused"),
        (SandboxAction::Stop, "started", "running", "stopped"),
        (SandboxAction::Kill, "started", "running", "stopped"),
    ] {
        let placement = if action == SandboxAction::Start {
            "unplaced"
        } else {
            "on_node"
        };
        let sandbox_id = SandboxId::new();
        let sandbox = seed(sandbox_id, prior, placement, resident).await?;
        let (child_insert, _) = hold_key_share(sandbox_id).await?;

        // The timeout only bounds the failure: an action that waits on the
        // lock cannot finish while the transaction holds it.
        let body = tokio::time::timeout(
            Duration::from_secs(10),
            run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None),
        )
        .await
        .map_err(|_| format!("{action:?} waited behind a FOR KEY SHARE lock"))??;
        child_insert.rollback().await?;

        assert_eq!(body["ok"], true, "{action:?}");
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new(completed, "on_node")),
            "{action:?}"
        );
    }

    let sandbox_id = SandboxId::new();
    let sandbox = seed(sandbox_id, "started", "on_node", "running").await?;
    let (child_insert, holder) = hold_key_share(sandbox_id).await?;
    let suspend = run_action(
        &controller,
        pool,
        sandbox_id,
        Some(&sandbox),
        SandboxAction::Suspend,
        None,
    );
    tokio::pin!(suspend);
    // Only the artifact lock itself, located by its `sql!` module, and only
    // while this test's child insert is what blocks it: other api statements
    // end in the same `FOR UPDATE` text.
    let artifact_lock_wait = async {
        loop {
            let waiting: bool = sqlx::query_scalar(sql!(
                r"SELECT EXISTS (
                     SELECT 1
                       FROM pg_stat_activity
                      WHERE datname = current_database()
                        AND wait_event_type = 'Lock'
                        AND $1 = ANY(pg_blocking_pids(pid))
                        AND query ~ '^/\*pplx_space_api::controllers::substrate:[0-9]+\*/ SELECT id FROM sandboxes WHERE id = \$1 FOR UPDATE$'
                 )"
            ))
            .bind(holder)
            .fetch_one(pool)
            .await?;
            if waiting {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    // Both arms are bounded: suspend must neither finish under the lock nor
    // fail to reach it.
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut suspend => {
                Err(format!("suspend finished under a FOR KEY SHARE lock: {result:?}"))
            }
            waited = artifact_lock_wait => waited.map_err(|error| error.to_string()),
        }
    })
    .await
    .map_err(|_| "suspend never waited on its artifact lock")??;
    child_insert.rollback().await?;
    let body = tokio::time::timeout(Duration::from_secs(10), suspend)
        .await
        .map_err(|_| "suspend did not complete after the lock was released")??;
    assert_eq!(body["ok"], true);
    assert_eq!(
        row(pool, sandbox_id).await?,
        Some(Row::new("suspended", "on_node"))
    );
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
    // Direct is only a placeholder capacity source: it reports zero eligible nodes,
    // as a synced, empty Kubernetes node cache would.
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
    }
    Ok(())
}

/// The wake a session queues (a native wake with no actor) runs through the
/// lifecycle executor like a `:wake`, but only while the sandbox's wake
/// policy, read when it executes, still lets the session's activity wake it:
/// allowed, it wakes the paused resident; not allowed, it fails as a conflict
/// and leaves the sandbox paused. A wake recording access, as for a
/// reattach to a running process, needs `access`. One recording no activity,
/// as an older API or one with the record switched off queues every wake, is
/// judged as a command, as an older executor judges it. A caller's `:wake`
/// is not policy-gated.
#[tokio::test(flavor = "current_thread")]
async fn a_system_wake_operation_runs_only_while_the_policy_allows_its_activity() -> TestResult {
    use crate::public_operations::{ApiProduct, ClaimedPublicOperation};
    use crate::public_sandbox_operations::{
        ImplicitWakeActivity::{Access, Command},
        SandboxLifecycleOperationExecutor, SandboxLifecycleOperationPayload,
    };
    use pplx_space_public_api::{
        OperationId, OperationKind, OperationState, PublicErrorCode, PublicSandboxId,
        SandboxLifecycleAction,
    };

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_sim_worker(pool).await?;
    let executor = SandboxLifecycleOperationExecutor::new(pool.clone(), direct_controller(pool));

    let caller: UserId = "00000000-0000-0000-0000-000000000010".parse()?;
    let org_id = "00000000-0000-0000-0000-000000000001".parse()?;
    let access = Some(Access);
    let command = Some(Command);
    // (actor, wake.on, if not the access policy placing it sets, recorded
    // activity, wakes)
    for (actor, wake_on, recorded, wakes) in [
        (None, None, None, true),
        (None, Some(&[][..]), None, false),
        (Some(caller), Some(&[]), None, true),
        (Some(caller), Some(&["command"]), access, true),
        (None, None, access, true),
        (None, Some(&["command"]), command, true),
        (None, Some(&["command"]), access, false),
        // Recording none: queued by an older API, or with the record off.
        (None, Some(&["command"]), None, true),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "paused", "space").await?;
        // Places it with an access-wake policy.
        place_on_node(pool, sandbox_id, node_id).await?;
        insert_resident(worker.state(), sandbox_id, "paused").await?;
        if let Some(wake_on) = wake_on {
            sqlx::query(sql!(
                "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(serde_json::json!({ "wake": { "on": wake_on } }))
            .execute(pool)
            .await?;
        }
        let payload = SandboxLifecycleOperationPayload {
            sandbox_id: PublicSandboxId::from(sandbox_id),
            action: SandboxLifecycleAction::Wake,
            rollback: None,
            wake_activity: recorded,
        }
        .into_json();
        let case = format!("{actor:?} {wake_on:?} {recorded:?}");
        let operation = ClaimedPublicOperation {
            id: OperationId::new(),
            org_id,
            actor_user_id: actor,
            api_product: ApiProduct::Native,
            kind: OperationKind::WakeSandbox,
            target_type: None,
            target_id: Some(PublicSandboxId::from(sandbox_id).to_string()),
            payload,
            attempts: 1,
            claim_generation: 1,
            traceparent: None,
            age_at_first_claim_secs: None,
        };

        let result = executor.execute(&operation).await;

        let resident = worker.state().get(&sandbox_id.to_string()).await?.status;
        if wakes {
            assert_eq!(
                result.state,
                OperationState::Succeeded,
                "{case}: {result:?}"
            );
            assert_eq!(resident, "running");
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(Row::new("started", "on_node"))
            );
        } else {
            assert_eq!(result.state, OperationState::Failed, "{case}: {result:?}");
            assert_eq!(result.terminal_error_code, Some(PublicErrorCode::Conflict));
            // Refused by the executor before any node call: a sandbox with no
            // node to ask (a suspended Kubernetes one) has no other check.
            assert_eq!(
                result.progress_message, "sandbox wake policy no longer allows this wake",
                "{result:?}"
            );
            assert_eq!(resident, "paused");
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(Row::new("paused", "on_node"))
            );
        }
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
    // Direct is only a placeholder here: this test never reaches the substrate.
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

    for lifecycle in StoredSandboxLifecycle::all_as_str() {
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
    Ok(())
}

// ---------------------------------------------------------------------------
// Delete-intent compensations.
// ---------------------------------------------------------------------------

/// Persisting the delete intent and the two compensations that undo it.
///
/// The intent write moves every live row to `deleting` and refuses a
/// tombstone, or any value that is not exactly a live one, with a 500: its
/// lifecycle read cannot parse them. Restoring the
/// pre-delete lifecycle, after a failed delete or a failed finalize, is a CAS
/// on `deleting`: if anything moved the row meanwhile it refuses with a 500
/// and leaves the row alone. Columns are the intent write and the
/// failed-delete and failed-finalize compensations; `A`: the writer moves the
/// row; `.`: it refuses. The finalize compensation's missing-row success is
/// pinned by `finalize_failure_accepts_an_already_deleted_row`.
#[tokio::test(flavor = "current_thread")]
async fn delete_intent_and_its_compensations_over_every_stored_lifecycle() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              intent  delete  finalize
        "created           A       .       .",
        "creating          A       .       .",
        "starting          A       .       .",
        "started           A       .       .",
        "restoring         A       .       .",
        "pausing           A       .       .",
        "paused            A       .       .",
        "resuming          A       .       .",
        "suspending        A       .       .",
        "suspended         A       .       .",
        "stopping          A       .       .",
        "stopped           A       .       .",
        "deleting          A       A       A",
        "checkpointing     A       .       .",
        "quarantined       A       .       .",
        "failed            A       .       .",
        "deleted           .       .       .",
        "cleanup_required  .       .       .",
        "bogus             .       .       .",
        "_Suspended_       .       .       .",
    ];
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;

    #[derive(Debug, Clone, Copy)]
    enum Writer {
        Intent,
        FailedDelete,
        FailedFinalize,
    }

    let writers = [Writer::Intent, Writer::FailedDelete, Writer::FailedFinalize];
    for (current, cells) in stored_lifecycle_matrix(writers, EXPECTED) {
        // A restoration target that differs from the current lifecycle, so a
        // write that slipped past the CAS would be visible.
        let target = if current == SandboxLifecycle::Paused.as_str() {
            SandboxLifecycle::Started
        } else {
            SandboxLifecycle::Paused
        };
        for (writer, applies) in cells {
            let applies = applies.applies();
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, current, "space").await?;

            let (result, moved_to) = match writer {
                Writer::Intent => (
                    begin_delete_against_active_fork(pool, sandbox_id)
                        .await
                        .map(|_| ()),
                    SandboxLifecycle::Deleting,
                ),
                Writer::FailedDelete => (
                    restore_lifecycle_after_failed_delete(pool, sandbox_id, target).await,
                    target,
                ),
                Writer::FailedFinalize => (
                    restore_lifecycle_after_failed_finalize_delete(pool, sandbox_id, Some(target))
                        .await,
                    target,
                ),
            };

            let case = format!("{writer:?} from {current:?}");
            if applies {
                assert!(result.is_ok(), "{case}: {result:?}");
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new(moved_to.as_str(), "unplaced")),
                    "{case}"
                );
            } else {
                let Err(error) = result else {
                    panic!("{case}: moved the row, expected a refusal");
                };
                assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR, "{case}");
                assert_eq!(
                    row(pool, sandbox_id).await?,
                    Some(Row::new(current, "unplaced")),
                    "{case}"
                );
            }
        }
    }
    Ok(())
}
