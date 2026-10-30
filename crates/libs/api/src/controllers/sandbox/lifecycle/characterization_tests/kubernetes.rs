//! The characterization cases whose path differs by substrate, run against a
//! `KubernetesSubstrate` built with the `kubernetes::tests` fixtures. Each
//! test is the twin of the direct test of the same name in the parent module
//! and keeps its setup and assertions. Where Kubernetes' outcome differs, the
//! test pins Kubernetes' outcome and its doc says why. Three twins are
//! renamed because the direct name's claim does not hold here, and their
//! docs name the direct test: `untracked_runtime_actions_without_a_row_are_not_found`,
//! `untracked_actions_report_status_and_leave_the_row_unless_suspend_claims_it`
//! and `activations_and_suspend_wait_for_a_key_share_lock`.
//!
//! Rows are seeded `managed_by = 'k8s'`; live sandboxes come from the
//! `test_support::kubernetes` fixtures.

use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;

use super::*;
use crate::controllers::substrate::kubernetes::KubernetesSubstrate;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::KubeLog;
use crate::controllers::substrate::kubernetes::tests::{
    substrate_with_store, substrate_with_store_and_client,
};
use crate::test_support::kubernetes::{
    LiveExecution, RouteIdentity, insert_kubernetes_resident, insert_live_execution, kubernetes,
    pod_creates, start_kubernetes_sim_worker, substrate_over_pods, unavailable_apiserver,
};

/// Bounds every action a test drives, so a regression that waits fails
/// instead of hanging the test.
const ACTION_BUDGET: Duration = Duration::from_secs(10);

fn kubernetes_controller(substrate: KubernetesSubstrate) -> SandboxController {
    SandboxController::new(kubernetes(substrate))
}

/// The twin of `controller_with_empty_cluster_admission`: the fixture's node
/// cache is synced and empty, so admission counts zero eligible start nodes
/// and refuses every charged activation with a 429. The controller's
/// apiserver is [`unavailable_apiserver`].
fn kubernetes_controller_with_empty_cluster_admission(pool: &PgPool) -> SandboxController {
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        kubernetes(substrate_with_store(pool.clone())),
        crate::test_support::enabled_config(),
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    let (client, _) = unavailable_apiserver();
    SandboxController::with_cluster_admission(
        kubernetes(substrate_with_store_and_client(pool.clone(), client)),
        admission,
    )
}

/// A controller whose substrate holds `pods` and the worker's node-api pod,
/// and reaches `worker` through them.
fn controller_over_pods(
    pool: &PgPool,
    worker: &SimWorkerHarness,
    pods: Vec<Pod>,
) -> TestResult<(SandboxController, KubeLog)> {
    let (substrate, log) = substrate_over_pods(pool, worker.port(), pods)?;
    Ok((kubernetes_controller(substrate), log))
}

async fn bounded<T>(
    what: impl std::fmt::Debug,
    action: impl std::future::Future<Output = T>,
) -> TestResult<T> {
    tokio::time::timeout(ACTION_BUDGET, action)
        .await
        .map_err(|_| format!("{what:?} did not finish within {ACTION_BUDGET:?}").into())
}

/// Installs the Kubernetes counterpart of the direct tests' route-receipt
/// hook. An execution seeded with [`RouteIdentity::Legacy`] carries no
/// `route_activation_id`, so an activation's Wake publishes it once the node
/// acknowledged that Wake
/// (`publish_legacy_route_identity_after_ack`), after the node call returned
/// and before the controller's lifecycle write. On the first publication for
/// a sandbox listed in `test_route_publications`, the trigger writes that
/// row's `lifecycle` over the sandbox. It changes only the database row and
/// counts its firings so a test can prove the write landed.
async fn install_lifecycle_write_on_route_publication(pool: &PgPool) -> TestResult {
    sqlx::query(sql!(
        "CREATE TABLE test_route_publications (
             sandbox_id UUID PRIMARY KEY,
             lifecycle TEXT NOT NULL,
             publications INT NOT NULL DEFAULT 0
         )"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION test_lifecycle_on_route_publication() RETURNS trigger
         LANGUAGE plpgsql AS $$
         DECLARE
             publications INT;
             target TEXT;
         BEGIN
             UPDATE test_route_publications p
                SET publications = p.publications + 1
              WHERE p.sandbox_id = NEW.sandbox_id
             RETURNING p.publications, p.lifecycle INTO publications, target;
             IF publications = 1 THEN
                 UPDATE sandboxes SET lifecycle = target WHERE id = NEW.sandbox_id;
             END IF;
             RETURN NEW;
         END $$"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER test_lifecycle_on_route_publication
         AFTER UPDATE OF route_activation_id ON sandbox_k8s_executions
         FOR EACH ROW
         WHEN (OLD.route_activation_id IS NULL AND NEW.route_activation_id IS NOT NULL)
         EXECUTE FUNCTION test_lifecycle_on_route_publication()"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn write_lifecycle_on_route_publication(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO test_route_publications (sandbox_id, lifecycle) VALUES ($1, $2)"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

async fn route_publications(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<i32> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT publications FROM test_route_publications WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Lands a lifecycle with Kubernetes suspend's last write. On the first
/// `suspending` → `suspended` write for a sandbox listed in
/// `test_suspend_finalizations` (`finish_suspended_execution`, after the
/// node's suspend became durable and the pod was retired), the trigger writes
/// that row's `lifecycle` over the sandbox in the same transaction. The
/// finalization holds the row lock until it commits, so this is the write a
/// concurrent writer would make right after it, before the controller's
/// post-result write. It counts its firings so a test can prove the write
/// landed.
async fn install_lifecycle_write_on_suspend_finalization(pool: &PgPool) -> TestResult {
    sqlx::query(sql!(
        "CREATE TABLE test_suspend_finalizations (
             sandbox_id UUID PRIMARY KEY,
             lifecycle TEXT NOT NULL,
             finalizations INT NOT NULL DEFAULT 0
         )"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION test_lifecycle_on_suspend_finalization() RETURNS trigger
         LANGUAGE plpgsql AS $$
         DECLARE
             finalizations INT;
             target TEXT;
         BEGIN
             UPDATE test_suspend_finalizations f
                SET finalizations = f.finalizations + 1
              WHERE f.sandbox_id = NEW.id
             RETURNING f.finalizations, f.lifecycle INTO finalizations, target;
             IF finalizations = 1 THEN
                 UPDATE sandboxes SET lifecycle = target WHERE id = NEW.id;
             END IF;
             RETURN NEW;
         END $$"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER test_lifecycle_on_suspend_finalization
         AFTER UPDATE ON sandboxes
         FOR EACH ROW
         WHEN (OLD.lifecycle = 'suspending' AND NEW.lifecycle = 'suspended')
         EXECUTE FUNCTION test_lifecycle_on_suspend_finalization()"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn write_lifecycle_on_suspend_finalization(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO test_suspend_finalizations (sandbox_id, lifecycle) VALUES ($1, $2)"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

async fn suspend_finalizations(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<i32> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT finalizations FROM test_suspend_finalizations WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

// ---------------------------------------------------------------------------
// `action` refusal arms.
// ---------------------------------------------------------------------------

/// Which check refuses a wake on the empty cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WakeRefusal {
    /// The shared entry guard: a 409 naming the lifecycle.
    EntryGuard,
    /// Cluster admission's 429, at entry or in the restore-lease claim.
    Admission,
    /// The substrate's refusal of a `suspending` lifecycle.
    SuspendInFlight,
    /// The substrate's pod create, which the apiserver refuses.
    PodCreate,
}

/// The entry guard is shared, so the same lifecycles are refused with the
/// same 409 before admission and write nothing, as on direct. The direct test
/// asserts only that every other lifecycle is refused with something else;
/// here each is pinned. Entry admission refuses `created` with a 429.
/// `restoring` and `suspended` defer their charge to the restore-lease
/// claim, which refuses with the same 429 and releases the claim, leaving
/// the row as it was. `suspending` reaches the substrate, which refuses a
/// `suspending` lifecycle (suspend finalization in flight) with a 503. The
/// others are not charged at entry, reach the substrate, and fail in its pod
/// create: the
/// [`unavailable_apiserver`] refuses the create with a 503 and its read then
/// finds no pod, which is a 500.
#[tokio::test(flavor = "current_thread")]
async fn wake_refuses_teardown_and_terminal_lifecycles_before_admission() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let controller = kubernetes_controller_with_empty_cluster_admission(pool);

    let stored = StoredSandboxLifecycle::all().iter().map(|stored| {
        let refusal = match stored {
            StoredSandboxLifecycle::Live(
                SandboxLifecycle::Stopping
                | SandboxLifecycle::Stopped
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed,
            )
            | StoredSandboxLifecycle::Deleted
            | StoredSandboxLifecycle::CleanupRequired => WakeRefusal::EntryGuard,
            StoredSandboxLifecycle::Live(
                SandboxLifecycle::Created
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Suspended,
            ) => WakeRefusal::Admission,
            StoredSandboxLifecycle::Live(SandboxLifecycle::Suspending) => {
                WakeRefusal::SuspendInFlight
            }
            StoredSandboxLifecycle::Live(
                SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Started
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Checkpointing,
            ) => WakeRefusal::PodCreate,
        };
        (stored.as_str(), refusal)
    });
    for (lifecycle, refusal) in stored {
        for action in [
            SandboxAction::Wake,
            SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
        ] {
            let case = format!("{action:?} on {lifecycle}");
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, lifecycle, "k8s").await?;
            let sandbox = tracked(pool, sandbox_id).await?;

            let error = bounded(
                &case,
                run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None),
            )
            .await?
            .expect_err("the empty cluster admits no wake");

            let message = error.to_string();
            match refusal {
                WakeRefusal::EntryGuard => {
                    assert_eq!(error.status(), StatusCode::CONFLICT, "{case}");
                    assert_eq!(
                        message,
                        format!("sandbox cannot be woken while lifecycle is {lifecycle}"),
                    );
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new(lifecycle, "unplaced"))
                    );
                }
                WakeRefusal::Admission => {
                    assert_eq!(
                        error.status(),
                        StatusCode::TOO_MANY_REQUESTS,
                        "{case}: {message}"
                    );
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new(lifecycle, "unplaced")),
                        "{case}"
                    );
                }
                WakeRefusal::SuspendInFlight => {
                    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{case}");
                    assert_eq!(
                        message,
                        format!(
                            "sandbox {sandbox_id} suspend finalization is in flight; retry \
                             activation after the current execution is retired"
                        ),
                    );
                }
                WakeRefusal::PodCreate => {
                    assert_eq!(
                        error.status(),
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "{case}: {message}"
                    );
                    assert!(
                        message.starts_with("failed to create sandbox pod"),
                        "{case}: {message}"
                    );
                }
            }
        }
    }
    Ok(())
}

/// No action writes the lifecycle it would have reached, as on direct, but
/// three of them fail differently. On direct every action is a 503 because
/// no worker is placed. Here the [`unavailable_apiserver`] has no pod and
/// refuses creates with a 503. Wake, stop and kill take the recovering
/// runtime path, which tries to recreate the pod. After the refused create,
/// `ensure_pod_exists` reads the pod back uncached; this double answers 404,
/// so the create fails as a 500 (`failed to create sandbox pod`). An
/// apiserver that also failed that read would make it a 503 with a retry
/// hint, so the 500 is the outcome for this double, not for every outage.
/// The other actions are 503s without a pod create. Start of a `stopped` row
/// claims a new execution, finds no restore source, and abandons the claim.
/// Suspend finds no active execution. Pause and checkpoint use the
/// non-recovering client. Resume uses the recovering client, but `paused` is
/// dormant, so it does not recover.
#[tokio::test(flavor = "current_thread")]
async fn unreachable_runtime_fails_actions_without_writing_lifecycle() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let (client, log) = unavailable_apiserver();
    let controller = kubernetes_controller(substrate_with_store_and_client(pool.clone(), client));

    for (action, lifecycle, status, creates_pod) in [
        (
            SandboxAction::Start,
            "stopped",
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            SandboxAction::Wake,
            "paused",
            StatusCode::INTERNAL_SERVER_ERROR,
            true,
        ),
        (
            SandboxAction::Suspend,
            "started",
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            SandboxAction::Stop,
            "started",
            StatusCode::INTERNAL_SERVER_ERROR,
            true,
        ),
        (
            SandboxAction::Kill,
            "started",
            StatusCode::INTERNAL_SERVER_ERROR,
            true,
        ),
        (
            SandboxAction::Pause,
            "started",
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            SandboxAction::Resume,
            "paused",
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            SandboxAction::Checkpoint,
            "started",
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, lifecycle, "k8s").await?;
        let sandbox = tracked(pool, sandbox_id).await?;
        let creates_before = pod_creates(&log);

        let error = bounded(
            action,
            run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None),
        )
        .await?
        .expect_err("no worker is assigned");

        assert_eq!(error.status(), status, "{action:?}: {error}");
        assert_eq!(
            pod_creates(&log) > creates_before,
            creates_pod,
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
// `action` completion writes, against a simulated worker behind a pod.
// ---------------------------------------------------------------------------

/// The wake handler's mapping of the completion CAS is the same as on
/// direct, but only a row that moves after the node's Wake returned reaches
/// it. Kubernetes re-reads the row before it materializes the execution
/// (`ensure_pod_exists`), so a row that moved after the handler read it but
/// before the substrate call (the direct test's injection point) is refused
/// there and the CAS never runs: a newer suspend claim is a retryable 503
/// (suspend finalization in flight) and a teardown or terminal lifecycle a
/// 409, where direct answers success and a 500. Both keep the row as moved.
/// The second pass moves the row from the route-publication hook instead,
/// after the node acknowledged the Wake, and pins the CAS's own mapping.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_outcome_maps_to_the_wake_response() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    install_lifecycle_write_on_route_publication(pool).await?;

    for after_node_wake in [false, true] {
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
            insert_sandbox(pool, sandbox_id, "paused", "k8s").await?;
            place_on_node(pool, sandbox_id, node_id).await?;
            let LiveExecution {
                pod,
                route_activation_id,
            } = insert_live_execution(pool, sandbox_id, RouteIdentity::Legacy).await?;
            insert_kubernetes_resident(worker.state(), sandbox_id, "paused", route_activation_id)
                .await?;
            let (controller, _) = controller_over_pods(pool, &worker, vec![pod])?;
            let sandbox = tracked(pool, sandbox_id).await?;
            if let Some(lifecycle) = moved_to {
                if after_node_wake {
                    write_lifecycle_on_route_publication(pool, sandbox_id, lifecycle).await?;
                } else {
                    set_lifecycle(pool, sandbox_id, lifecycle).await?;
                }
            }

            let result = bounded(
                (after_node_wake, moved_to),
                run_action(
                    &controller,
                    pool,
                    sandbox_id,
                    Some(&sandbox),
                    SandboxAction::Wake,
                    None,
                ),
            )
            .await?;

            let ok_body = serde_json::json!({
                "ok": true,
                "action": "wake",
                "sandbox_id": sandbox_id.to_string(),
            });
            let case = format!("{moved_to:?} (after node wake: {after_node_wake})");
            if moved_to.is_some() && after_node_wake {
                assert_eq!(
                    route_publications(pool, sandbox_id).await?,
                    1,
                    "{case}: the hook's write never landed"
                );
            }
            match (moved_to, after_node_wake) {
                (None, _) => {
                    assert_eq!(result?, ok_body, "{case}");
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new("started", "on_node"))
                    );
                }
                (Some("suspending"), true) => {
                    assert_eq!(
                        result?, ok_body,
                        "a newer suspend claim is not a wake failure"
                    );
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new("suspending", "on_node"))
                    );
                }
                (Some("suspending"), false) => {
                    let error = result.expect_err("a suspend claim refuses materialization");
                    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{case}");
                    assert_eq!(
                        error.to_string(),
                        format!(
                            "sandbox {sandbox_id} suspend finalization is in flight; retry \
                             activation after the current execution is retired"
                        ),
                    );
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new("suspending", "on_node"))
                    );
                }
                (Some(lifecycle), true) => {
                    let error = result.expect_err("a lost completion CAS fails the wake");
                    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR, "{case}");
                    assert!(
                        error
                            .to_string()
                            .starts_with("sandbox wake lost the lifecycle CAS"),
                        "{case}: {error}"
                    );
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new(lifecycle, "on_node"))
                    );
                }
                (Some(lifecycle), false) => {
                    let error = result.expect_err("a moved row refuses materialization");
                    assert_eq!(error.status(), StatusCode::CONFLICT, "{case}");
                    assert_eq!(
                        error.to_string(),
                        format!(
                            "sandbox cannot materialize an execution after lifecycle changed \
                             from paused to {lifecycle}"
                        ),
                    );
                    assert_eq!(
                        row(pool, sandbox_id).await?,
                        Some(Row::new(lifecycle, "on_node"))
                    );
                }
            }
        }
    }
    Ok(())
}

/// Where `completed_actions_keep_a_terminal_lifecycle_that_landed_mid_flight`
/// lands the terminal lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Landing {
    /// Before the action runs, as in the direct test.
    BeforeAction,
    /// After the node acknowledged start's Wake, from the route-publication
    /// hook ([`install_lifecycle_write_on_route_publication`]).
    AfterWakeAck,
    /// With suspend's own `suspended` finalization
    /// ([`install_lifecycle_write_on_suspend_finalization`]).
    WithSuspendFinalization,
}

/// Resume, pause, stop and kill keep a terminal lifecycle that landed before
/// the action ran and report success, as on direct: they re-read nothing
/// that refuses it, so they reach the controller's guarded post-result
/// write. Start and suspend re-read the row before calling the node, so a
/// terminal lifecycle that landed before them is refused up front and the
/// guarded write is never reached. Start refuses with a 409
/// (`start_execution` accepts only recoverable live lifecycles). Suspend's
/// ownership claim, a CAS from `started` or `paused` to `suspending`
/// (`claim_suspend_execution`), fails with a 503.
///
/// So start and suspend also land the terminal lifecycle after their node
/// call. Start's lands from the route-publication hook, after the node
/// acknowledged its Wake (a [`RouteIdentity::Legacy`] execution). Suspend's
/// lands with its own `suspended` finalization, its last write before the
/// controller's. Each action then reports success, as on direct, and the
/// controller's guarded write keeps the terminal lifecycle. Between suspend's
/// claim and its finalization, the substrate's own writes accept only a
/// `suspending` row; this test does not land a lifecycle there.
///
/// The row keeps the terminal lifecycle in every case. Its placement is kept
/// too, except after start's Wake: the substrate itself records the pod's
/// node placement (`persist_pod_node_placement`), so that row ends `on_node`.
#[tokio::test(flavor = "current_thread")]
async fn completed_actions_keep_a_terminal_lifecycle_that_landed_mid_flight() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    install_lifecycle_write_on_route_publication(pool).await?;
    install_lifecycle_write_on_suspend_finalization(pool).await?;

    for (action, prior, resident, completed, landing) in [
        (
            SandboxAction::Start,
            "stopped",
            "stopped",
            "started",
            Landing::BeforeAction,
        ),
        (
            SandboxAction::Start,
            "stopped",
            "stopped",
            "started",
            Landing::AfterWakeAck,
        ),
        (
            SandboxAction::Resume,
            "paused",
            "paused",
            "started",
            Landing::BeforeAction,
        ),
        (
            SandboxAction::Pause,
            "started",
            "running",
            "paused",
            Landing::BeforeAction,
        ),
        (
            SandboxAction::Stop,
            "started",
            "running",
            "stopped",
            Landing::BeforeAction,
        ),
        (
            SandboxAction::Kill,
            "started",
            "running",
            "stopped",
            Landing::BeforeAction,
        ),
        (
            SandboxAction::Suspend,
            "started",
            "running",
            "suspended",
            Landing::BeforeAction,
        ),
        (
            SandboxAction::Suspend,
            "started",
            "running",
            "suspended",
            Landing::WithSuspendFinalization,
        ),
    ] {
        let completed_placement = if action == SandboxAction::Start {
            "on_node"
        } else {
            "unplaced"
        };
        let route = if landing == Landing::AfterWakeAck {
            RouteIdentity::Legacy
        } else {
            RouteIdentity::Published
        };
        // The same exclusion as the direct test: only the lifecycles both
        // guard families treat as terminal.
        let terminal = ["failed", "quarantined"]
            .into_iter()
            .chain(StoredSandboxLifecycle::tombstones_as_str())
            .map(Some);
        for landed in [None].into_iter().chain(terminal) {
            let case = format!("{action:?} with {landed:?} landed {landing:?}");
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, prior, "k8s").await?;
            place_on_node(pool, sandbox_id, node_id).await?;
            set_placement(pool, sandbox_id, "unplaced").await?;
            let LiveExecution {
                pod,
                route_activation_id,
            } = insert_live_execution(pool, sandbox_id, route).await?;
            insert_kubernetes_resident(worker.state(), sandbox_id, resident, route_activation_id)
                .await?;
            let (controller, _) = controller_over_pods(pool, &worker, vec![pod])?;
            let sandbox = tracked(pool, sandbox_id).await?;
            if let Some(lifecycle) = landed {
                match landing {
                    Landing::BeforeAction => set_lifecycle(pool, sandbox_id, lifecycle).await?,
                    Landing::AfterWakeAck => {
                        write_lifecycle_on_route_publication(pool, sandbox_id, lifecycle).await?;
                    }
                    Landing::WithSuspendFinalization => {
                        write_lifecycle_on_suspend_finalization(pool, sandbox_id, lifecycle)
                            .await?;
                    }
                }
            }

            // Boxed for stack margin, as in the untracked-actions twin: at
            // opt-level 0, as CI builds it, this test needs 1.375-1.5 MiB of
            // libtest's 2 MiB stack.
            let run = Box::pin(run_action(
                &controller,
                pool,
                sandbox_id,
                Some(&sandbox),
                action,
                None,
            ));
            let result = bounded(&case, run).await?;

            let ok_body = serde_json::json!({
                "ok": true,
                "action": action.as_str(),
                "sandbox_id": sandbox_id.to_string(),
            });
            if landed.is_some() {
                let landed_writes = match landing {
                    Landing::BeforeAction => 1,
                    Landing::AfterWakeAck => route_publications(pool, sandbox_id).await?,
                    Landing::WithSuspendFinalization => {
                        suspend_finalizations(pool, sandbox_id).await?
                    }
                };
                assert_eq!(landed_writes, 1, "{case}: the hook's write never landed");
            }
            match (landed, action, landing) {
                (Some(lifecycle), SandboxAction::Start, Landing::BeforeAction) => {
                    let error = result.expect_err("start refuses a terminal lifecycle");
                    assert_eq!(error.status(), StatusCode::CONFLICT, "{case}");
                    assert_eq!(
                        error.to_string(),
                        format!("sandbox cannot be started while lifecycle is {lifecycle}"),
                    );
                }
                (Some(_), SandboxAction::Suspend, Landing::BeforeAction) => {
                    let error = result.expect_err("suspend cannot claim a terminal lifecycle");
                    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{case}");
                    assert_eq!(
                        error.to_string(),
                        "sandbox execution changed before suspend ownership was claimed"
                    );
                }
                _ => assert_eq!(result?, ok_body, "{case}"),
            }
            let expected = match (landed, landing) {
                (None, _) => Row::new(completed, completed_placement),
                (Some(lifecycle), Landing::AfterWakeAck) => Row::new(lifecycle, "on_node"),
                (Some(lifecycle), _) => Row::new(lifecycle, "unplaced"),
            };
            assert_eq!(row(pool, sandbox_id).await?, Some(expected), "{case}");
        }
    }
    Ok(())
}

/// Same outcome as on direct, with the `stopped` written by the
/// route-publication hook, the Kubernetes counterpart of the direct test's
/// route-receipt trigger: the native start operation writes `started` over
/// it and succeeds without a retry. A Kubernetes start of a `created` row
/// publishes the route once the node acknowledged its Wake, then the shared
/// `maybe_update_sandbox_state(.., "started", "on_node")` runs unguarded, so
/// the activation-receipt path the direct test's doc mentions is not what
/// writes `started` here.
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
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "created", "k8s").await?;
    place_on_node(pool, sandbox_id, node_id).await?;
    set_placement(pool, sandbox_id, "unplaced").await?;
    let LiveExecution {
        pod,
        route_activation_id,
    } = insert_live_execution(pool, sandbox_id, RouteIdentity::Legacy).await?;
    insert_kubernetes_resident(worker.state(), sandbox_id, "creating", route_activation_id).await?;
    install_lifecycle_write_on_route_publication(pool).await?;
    write_lifecycle_on_route_publication(pool, sandbox_id, "stopped").await?;
    let (controller, _) = controller_over_pods(pool, &worker, vec![pod])?;

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
    let executor = SandboxLifecycleOperationExecutor::new(pool.clone(), controller);

    // Boxed for stack margin, as in the untracked-actions twin: at opt-level
    // 0, as CI builds it, this test needs 1.5-1.625 MiB of libtest's 2 MiB.
    let result = bounded("start", Box::pin(executor.execute(&operation))).await?;

    assert!(
        route_publications(pool, sandbox_id).await? >= 1,
        "the mid-flight stopped was never written"
    );
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

/// Whether the substrate holds the sandbox's pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxPod {
    /// In the reflector and the apiserver, as for a live sandbox.
    Cached,
    /// In neither: the action's runtime client must recover a pod.
    Missing,
}

/// Twin of `completed_actions_do_not_wait_for_a_key_share_lock`, renamed
/// because its claim does not hold here. With the pod cached, resume, pause,
/// stop and kill finish while a transaction holds `FOR KEY SHARE` on the
/// sandbox row, as on direct. Start and wake block on it in `SELECT lifecycle
/// FROM sandboxes WHERE id = $1 FOR UPDATE`, the statement
/// `lock_sandbox_execution_lifecycle_in` runs, whose row lock conflicts with
/// `FOR KEY SHARE`. The `sql!` locator names where that function is defined,
/// not which caller ran it: it has three (`persist_materialized_pod_metadata`,
/// `claim_started_recovery` and `lock_sandbox_for_execution_lifecycle_in`,
/// which the execution claim and `claim_active_execution_pod_materialization`
/// both call). That the first wait is the execution claim `ensure_pod_exists`
/// takes before it materializes the pod is an inference from the code.
///
/// Stop and kill of a sandbox whose pod is gone block too, before any pod is
/// recreated, on `SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE`
/// (`lock_sandbox_for_execution_mutation_in`, also in
/// `store::execution_rows`). That statement has many callers; reading the
/// code, the one waiting is the pod-tracking cleanup that resolving the
/// missing pod runs (`resolve_pod_instance` → `forget_pod_tracking_if_generation`).
/// Only after it does their recovering runtime client recreate the pod.
///
/// Suspend blocks too, on the `SELECT id ... FOR UPDATE` that persists its
/// commit (`persist_or_accept_suspend_commit`, the only statement of that
/// shape in `store::suspend`), the counterpart of direct suspend's artifact
/// lock. Kubernetes suspend then ends `unplaced`, where direct suspend leaves
/// the row `on_node`.
///
/// In this interleaving the blocking transaction ends promptly, and each
/// waiter then completes. A longer blocker is not waited out indefinitely.
/// The api sets neither `lock_timeout` nor `statement_timeout` on its own
/// sessions, but it connects as `space_user`, whose role-level
/// `statement_timeout` `bootstrap-roles` sets (`--app-statement-timeout`,
/// 10s by default); that ends any of these waits with an error. The role's
/// `lock_timeout` is opt-in (`SPACE_DB_APP_LOCK_TIMEOUT`). Within the api:
///
/// - start and wake: the activation wait budget (`ready_wait_bounds`) around
///   `ensure_pod_exists`, in `start_execution` and `drive_prepared_wake`,
///   can end the wait first;
/// - stop and kill with the pod gone: nothing. The wait is in pod
///   resolution, before `ensure_execution_ready_for_access` takes its wait
///   budget;
/// - suspend: nothing. Its `suspend_durability` deadline bounds the node-api
///   resolve, connect and suspend poll, not `finish_suspend_handoff`, which
///   persists the commit. The suspend also runs in a tracked task that
///   outlives its request.
///
/// Each wait is tied to the holder's backend pid through `pg_blocking_pids`
/// and to the waiting statement's `sql!` locator, and every wait is bounded.
#[tokio::test(flavor = "current_thread")]
async fn activations_and_suspend_wait_for_a_key_share_lock() -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    // The open child-insert transaction holds one connection, and the
    // lock-wait probe takes another while the action waits on its own.
    let db = pg_test_db_with_max_connections(5).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    let materialization_lock = r"^/\*pplx_space_api::controllers::substrate::kubernetes::store::execution_rows:[0-9]+\*/ SELECT lifecycle FROM sandboxes WHERE id = \$1 FOR UPDATE$";
    let pod_tracking_lock = r"^/\*pplx_space_api::controllers::substrate::kubernetes::store::execution_rows:[0-9]+\*/ SELECT id FROM sandboxes WHERE id = \$1 FOR UPDATE$";
    let suspend_commit_lock = r"^/\*pplx_space_api::controllers::substrate::kubernetes::store::suspend:[0-9]+\*/ SELECT id\s+FROM sandboxes\s+WHERE id = \$1\s+FOR UPDATE$";

    for (action, prior, resident, completed, pod_seed, waits_on) in [
        (
            SandboxAction::Start,
            "stopped",
            "stopped",
            "started",
            SandboxPod::Cached,
            Some(materialization_lock),
        ),
        (
            SandboxAction::Wake,
            "paused",
            "paused",
            "started",
            SandboxPod::Cached,
            Some(materialization_lock),
        ),
        (
            SandboxAction::Resume,
            "paused",
            "paused",
            "started",
            SandboxPod::Cached,
            None,
        ),
        (
            SandboxAction::Pause,
            "started",
            "running",
            "paused",
            SandboxPod::Cached,
            None,
        ),
        (
            SandboxAction::Stop,
            "started",
            "running",
            "stopped",
            SandboxPod::Cached,
            None,
        ),
        (
            SandboxAction::Kill,
            "started",
            "running",
            "stopped",
            SandboxPod::Cached,
            None,
        ),
        (
            SandboxAction::Stop,
            "started",
            "running",
            "stopped",
            SandboxPod::Missing,
            Some(pod_tracking_lock),
        ),
        (
            SandboxAction::Kill,
            "started",
            "running",
            "stopped",
            SandboxPod::Missing,
            Some(pod_tracking_lock),
        ),
        (
            SandboxAction::Suspend,
            "started",
            "running",
            "suspended",
            SandboxPod::Cached,
            Some(suspend_commit_lock),
        ),
    ] {
        let case = format!("{action:?} with the pod {pod_seed:?}");
        let (placement, completed_placement) = match (action, pod_seed) {
            (SandboxAction::Start, _) => ("unplaced", "on_node"),
            (SandboxAction::Suspend, _) => ("on_node", "unplaced"),
            // Resolving the missing pod unplaces the row. The double never
            // binds the recreated pod to a node, so no placement is recorded
            // again.
            (_, SandboxPod::Missing) => ("on_node", "unplaced"),
            _ => ("on_node", "on_node"),
        };
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, prior, "k8s").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        set_placement(pool, sandbox_id, placement).await?;
        let LiveExecution {
            pod,
            route_activation_id,
        } = insert_live_execution(pool, sandbox_id, RouteIdentity::Published).await?;
        insert_kubernetes_resident(worker.state(), sandbox_id, resident, route_activation_id)
            .await?;
        let pods = match pod_seed {
            SandboxPod::Cached => vec![pod],
            SandboxPod::Missing => Vec::new(),
        };
        let (controller, log) = controller_over_pods(pool, &worker, pods)?;
        let sandbox = tracked(pool, sandbox_id).await?;
        let mut child_insert = pool.begin().await?;
        sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE"))
            .bind(sandbox_id)
            .execute(&mut *child_insert)
            .await?;
        let holder: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *child_insert)
            .await?;
        // Boxed for stack margin, as in the untracked-actions twin: at
        // opt-level 0, as CI builds it, this test needs 1.5-1.625 MiB of
        // libtest's 2 MiB stack.
        let mut run = Box::pin(run_action(
            &controller,
            pool,
            sandbox_id,
            Some(&sandbox),
            action,
            None,
        ));

        let body = match waits_on {
            None => {
                // The timeout only bounds the failure: an action that waits
                // on the lock cannot finish while the transaction holds it.
                let body = bounded(&case, &mut run)
                    .await
                    .map_err(|_| format!("{case} waited behind a FOR KEY SHARE lock"))??;
                child_insert.rollback().await?;
                body
            }
            Some(statement) => {
                let lock_wait = async {
                    loop {
                        let waiting: bool = sqlx::query_scalar(sql!(
                            "SELECT EXISTS (
                                 SELECT 1
                                   FROM pg_stat_activity
                                  WHERE datname = current_database()
                                    AND wait_event_type = 'Lock'
                                    AND $1 = ANY(pg_blocking_pids(pid))
                                    AND query ~ $2
                             )"
                        ))
                        .bind(holder)
                        .bind(statement)
                        .fetch_one(pool)
                        .await?;
                        if waiting {
                            return Ok::<_, sqlx::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                };
                // Both arms are bounded: the action must neither finish
                // under the lock nor fail to reach it.
                bounded(&case, async {
                    tokio::select! {
                        result = &mut run => Err(format!(
                            "{case} finished under a FOR KEY SHARE lock: {result:?}"
                        )),
                        waited = lock_wait => waited.map_err(|error| error.to_string()),
                    }
                })
                .await
                .map_err(|_| format!("{case} never waited on {statement}"))??;
                child_insert.rollback().await?;
                bounded(&case, run)
                    .await
                    .map_err(|_| format!("{case} did not complete after the lock ended"))??
            }
        };

        assert_eq!(body["ok"], true, "{case}");
        assert_eq!(
            pod_creates(&log),
            usize::from(pod_seed == SandboxPod::Missing),
            "{case}: only a missing pod is recreated"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Some(Row::new(completed, completed_placement)),
            "{case}"
        );
    }
    Ok(())
}

/// Same outcome as on direct: stop and kill are not drain-gated, and on a
/// draining node the non-recovering `reachable_runtime_client` still finds
/// the live pod and reaches the worker. Only Kubernetes can also refuse
/// there: for a sandbox on the draining node whose pod is gone, that client
/// finds no runtime and the action is a 503, instead of recovering a pod onto
/// the cordoned node. The lifecycle is kept, but resolving the pod the
/// apiserver no longer has forgets its tracking
/// (`forget_pod_tracking_if_generation`), which unplaces the row.
#[tokio::test(flavor = "current_thread")]
async fn stop_and_kill_on_a_draining_node_still_reach_the_worker() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    set_drain_phase(pool, node_id, "draining").await?;

    for action in [SandboxAction::Stop, SandboxAction::Kill] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "started", "k8s").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        let LiveExecution {
            pod,
            route_activation_id,
        } = insert_live_execution(pool, sandbox_id, RouteIdentity::Published).await?;
        insert_kubernetes_resident(worker.state(), sandbox_id, "running", route_activation_id)
            .await?;
        let (controller, _) = controller_over_pods(pool, &worker, vec![pod])?;
        let sandbox = tracked(pool, sandbox_id).await?;

        bounded(
            action,
            run_action(&controller, pool, sandbox_id, Some(&sandbox), action, None),
        )
        .await??;

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

        let gone = SandboxId::new();
        insert_sandbox(pool, gone, "started", "k8s").await?;
        place_on_node(pool, gone, node_id).await?;
        let gone_execution = insert_live_execution(pool, gone, RouteIdentity::Published).await?;
        insert_kubernetes_resident(
            worker.state(),
            gone,
            "running",
            gone_execution.route_activation_id,
        )
        .await?;
        let (controller, log) = controller_over_pods(pool, &worker, Vec::new())?;
        let sandbox = tracked(pool, gone).await?;

        let error = bounded(
            action,
            run_action(&controller, pool, gone, Some(&sandbox), action, None),
        )
        .await?
        .expect_err("a draining node's missing pod is not recovered");

        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error}");
        assert!(
            error.to_string().starts_with(&format!(
                "sandbox runtime is currently unreachable for {action}"
            )),
            "{action:?}: {error}"
        );
        assert_eq!(pod_creates(&log), 0, "{action:?}");
        assert_eq!(
            worker.state().get(&gone.to_string()).await?.status,
            "running",
            "{action:?}"
        );
        assert_eq!(
            row(pool, gone).await?,
            Some(Row::new("started", "unplaced")),
            "{action:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `untracked_action`: no Postgres row was supplied by the caller.
// ---------------------------------------------------------------------------

/// Twin of `untracked_runtime_actions_without_placement_are_unavailable`,
/// renamed because the outcome differs. On direct every action is a 503
/// because no placed row names a worker; the Kubernetes substrate looks the
/// sandbox up itself
/// (`require_sandbox`), so an id with no row is a 404. Suspend reads the
/// active execution first and is a 503 because none exists.
#[tokio::test(flavor = "current_thread")]
async fn untracked_runtime_actions_without_a_row_are_not_found() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let (client, log) = unavailable_apiserver();
    let controller = kubernetes_controller(substrate_with_store_and_client(pool.clone(), client));

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
        let error = bounded(
            action,
            run_action(&controller, pool, sandbox_id, None, action, None),
        )
        .await?
        .expect_err("no worker can be resolved");
        if action == SandboxAction::Suspend {
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error}");
            assert_eq!(error.to_string(), "sandbox is not assigned to a pod");
        } else {
            assert_eq!(error.status(), StatusCode::NOT_FOUND, "{action:?}: {error}");
            assert_eq!(
                error.to_string(),
                format!("sandbox not found: {sandbox_id}")
            );
        }
    }
    assert!(
        log.lock().expect("kube log mutex poisoned").is_empty(),
        "an unknown id never reaches the apiserver"
    );
    Ok(())
}

/// Twin of `untracked_actions_report_status_and_leave_the_row`, renamed
/// because the outcome differs for suspend. Every other action reports the
/// status it reached and leaves the row alone, as on direct. Kubernetes
/// suspend takes the same path whether or not the caller passed a tracked
/// row (`suspend_execution`): it claims the row itself with a CAS from
/// `started` or `paused` to `suspending` (`claim_suspend_execution`), and its
/// handoff then writes the row `suspended` and `unplaced`. So on a `created`
/// row it is refused with a 503 before reaching the worker, while on a
/// `started` or `paused` row it suspends the resident and writes the row.
#[tokio::test(flavor = "current_thread")]
async fn untracked_actions_report_status_and_leave_the_row_unless_suspend_claims_it() -> TestResult
{
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    let untouched = Row::new("created", "on_node");
    let suspended = Row::new("suspended", "unplaced");

    for (action, prior, resident, status, resident_after, row_after) in [
        (
            SandboxAction::Start,
            "created",
            "stopped",
            Some("started"),
            "running",
            &untouched,
        ),
        (
            SandboxAction::Wake,
            "created",
            "paused",
            Some("started"),
            "running",
            &untouched,
        ),
        (
            SandboxAction::Stop,
            "created",
            "running",
            Some("stopped"),
            "stopped",
            &untouched,
        ),
        (
            SandboxAction::Kill,
            "created",
            "running",
            Some("stopped"),
            "stopped",
            &untouched,
        ),
        (
            SandboxAction::Pause,
            "created",
            "running",
            Some("paused"),
            "paused",
            &untouched,
        ),
        (
            SandboxAction::Resume,
            "created",
            "paused",
            Some("started"),
            "running",
            &untouched,
        ),
        (
            SandboxAction::Suspend,
            "created",
            "running",
            None,
            "running",
            &untouched,
        ),
        (
            SandboxAction::Suspend,
            "started",
            "running",
            Some("suspended"),
            "suspended",
            &suspended,
        ),
        (
            SandboxAction::Suspend,
            "paused",
            "paused",
            Some("suspended"),
            "suspended",
            &suspended,
        ),
    ] {
        let case = format!("{action:?} on {prior}");
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, prior, "k8s").await?;
        place_on_node(pool, sandbox_id, node_id).await?;
        let LiveExecution {
            pod,
            route_activation_id,
        } = insert_live_execution(pool, sandbox_id, RouteIdentity::Published).await?;
        insert_kubernetes_resident(worker.state(), sandbox_id, resident, route_activation_id)
            .await?;
        let (controller, _) = controller_over_pods(pool, &worker, vec![pod])?;

        // Boxed: `run_action` is a large future, and holding it inline in
        // this test's future overflows libtest's default stack when the api
        // crate is built at opt-level 0, as CI builds it on Linux.
        // Boxed, it still needs 1.75-1.875 MiB of the 2 MiB: little margin.
        let run = Box::pin(run_action(
            &controller,
            pool,
            sandbox_id,
            None,
            action,
            None,
        ));
        let result = bounded(&case, run).await?;

        match status {
            Some(status) => assert_eq!(
                result?,
                serde_json::json!({
                    "ok": true,
                    "action": action.as_str(),
                    "sandbox_id": sandbox_id.to_string(),
                    "status": status,
                }),
                "{case}"
            ),
            None => {
                let error = result.expect_err("suspend cannot claim a created row");
                assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error}");
                assert_eq!(
                    error.to_string(),
                    "sandbox execution changed before suspend ownership was claimed"
                );
            }
        }
        assert_eq!(
            worker.state().get(&sandbox_id.to_string()).await?.status,
            resident_after,
            "{case}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?.as_ref(),
            Some(row_after),
            "{case}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `delete` of an observed-only sandbox (`delete_observed_sandbox`).
// ---------------------------------------------------------------------------

/// Same outcome as on direct. No warm-pool slot is reserved for the id, so
/// `delete_observed_execution` resolves the observing node's node-api pod
/// from the node cache and deletes through it.
#[tokio::test(flavor = "current_thread")]
async fn observed_only_delete_removes_the_resident_on_the_observing_node() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let (worker, node_id) = start_kubernetes_sim_worker(pool).await?;
    let (controller, _) = controller_over_pods(pool, &worker, Vec::new())?;
    let sandbox_id = SandboxId::new();
    let bystander = SandboxId::new();
    insert_kubernetes_resident(worker.state(), sandbox_id, "running", None).await?;
    insert_kubernetes_resident(worker.state(), bystander, "running", None).await?;
    insert_observed_state(pool, sandbox_id, node_id).await?;

    bounded("delete", controller.delete(pool, sandbox_id, None)).await??;

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
// `wake_completion_redrive` on the Kubernetes substrate, whose hydration
// probe answers routable only for a running active execution with a pod IP,
// or for one the apiserver shows a routable pod for.
// ---------------------------------------------------------------------------

/// With a routable active execution the outcome is the same as on direct:
/// only a `restoring` row is completed. Unlike direct's probe, which always
/// answers routable, Kubernetes' probe gates the CAS: a `restoring` row
/// whose execution has no pod IP, and whose pod the apiserver does not have,
/// is left `restoring`.
#[tokio::test(flavor = "current_thread")]
async fn redrive_completes_only_a_restoring_row() -> TestResult {
    let db = pg_test_db_with_max_connections(2).await?;
    let pool = &db.pool;

    for lifecycle in StoredSandboxLifecycle::all_as_str() {
        for routable in [true, false] {
            let sandbox_id = SandboxId::new();
            insert_sandbox(pool, sandbox_id, lifecycle, "k8s").await?;
            insert_live_execution(pool, sandbox_id, RouteIdentity::Published).await?;
            if !routable {
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET state = 'creating', pod_ip = NULL, pod_uid = ''
                      WHERE sandbox_id = $1"
                ))
                .bind(sandbox_id)
                .execute(pool)
                .await?;
            }
            let (client, _) = unavailable_apiserver();
            let substrate = kubernetes(substrate_with_store_and_client(pool.clone(), client));

            // A zero budget makes exactly one attempt: read, probe, fenced CAS.
            bounded(
                lifecycle,
                wake_completion_redrive(
                    pool.clone(),
                    substrate,
                    sandbox_id,
                    WakeCompletionAction::Wake,
                    Duration::ZERO,
                ),
            )
            .await?;

            let expected = if routable && lifecycle == SandboxLifecycle::Restoring.as_str() {
                Row::new("started", "on_node")
            } else {
                Row::new(lifecycle, "unplaced")
            };
            assert_eq!(
                row(pool, sandbox_id).await?,
                Some(expected),
                "{lifecycle} (routable: {routable})"
            );
        }
    }
    Ok(())
}
