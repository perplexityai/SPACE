//! The suspend action's lifecycle write on the Kubernetes substrate, whose
//! `suspend_execution` commits `suspended` itself, and a queued suspend's
//! fence on the execution generation it was queued for.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_common_core::types::{ExecutionGeneration, LocalArtifactId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::substrate::kubernetes::tests::{
    deleted_pod_kubernetes_client, insert_k8s_execution, insert_test_sandbox_row,
    substrate_with_store_and_client,
};

const POD_NAME: &str = "space-sbx-suspend-race-g1";
const POD_UID: &str = "suspend-race-uid";

/// A wake that claims the sandbox right after the substrate commits its
/// suspend keeps the row. The suspend action writes no lifecycle of its own
/// afterwards: a trailing `suspended` would put the woken, routed sandbox
/// back to `suspended` while its new execution stays active, the shape whose
/// pod holds the only copy of the sandbox's state.
///
/// A deferred trigger stands in for the wake: it moves the row to
/// `restoring` when the transaction that committed `suspended` commits,
/// which is after the substrate's write and before anything the controller
/// does next.
#[tokio::test(flavor = "current_thread")]
async fn suspend_keeps_a_wake_that_claimed_after_the_suspend_committed() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(pool, sandbox_id, "suspending").await?;
    // The node already committed the suspend, so the substrate only retires
    // the pod and finalizes the row; it never reaches the node.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(LocalArtifactId::new())
    .execute(pool)
    .await?;
    insert_k8s_execution(pool, sandbox_id, 1, POD_NAME, POD_UID, None, true).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET state = 'suspended' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION wake_claims_after_suspend() RETURNS trigger AS $$
         BEGIN
             UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = NEW.id;
             RETURN NULL;
         END;
         $$ LANGUAGE plpgsql"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE CONSTRAINT TRIGGER wake_claims_after_suspend
             AFTER UPDATE OF lifecycle ON sandboxes
             DEFERRABLE INITIALLY DEFERRED
             FOR EACH ROW
             WHEN (OLD.lifecycle = 'suspending' AND NEW.lifecycle = 'suspended')
             EXECUTE FUNCTION wake_claims_after_suspend()"
    ))
    .execute(pool)
    .await?;

    let mut substrate = substrate_with_store_and_client(
        pool.clone(),
        deleted_pod_kubernetes_client(POD_NAME, POD_UID, Arc::new(AtomicUsize::new(0))),
    );
    substrate.k8s.k8s_blocking_delete_timeout = std::time::Duration::from_millis(500);
    let controller = SandboxController::new(
        crate::controllers::substrate::Substrate::from_kubernetes(substrate),
    );
    let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .expect("sandbox row exists");

    controller
        .action(
            pool,
            sandbox_id,
            Some(&sandbox),
            SandboxApiMetricIdentity::native(SandboxAction::Suspend.as_str()),
            SandboxAction::Suspend,
            None,
            crate::controllers::substrate::ActivationClient::Native,
        )
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(lifecycle, "restoring");
    Ok(())
}

// ── A queued suspend's execution fence ──────────────────────────────────────

const WOKEN_POD_NAME: &str = "space-sbx-suspend-race-g2";
const WOKEN_POD_UID: &str = "suspend-race-woken-uid";

/// A native suspend operation of `sandbox_id`, as the dispatcher claims it on
/// a requeue, queued for `fence` or, with `None`, by an API that records none.
fn queued_suspend(
    sandbox_id: SandboxId,
    fence: Option<i64>,
) -> TestResult<crate::public_operations::ClaimedPublicOperation> {
    use pplx_space_public_api::{
        OperationId, OperationKind, PublicSandboxId, SandboxLifecycleAction,
    };
    let public_sandbox_id = PublicSandboxId::from(sandbox_id);
    Ok(crate::public_operations::ClaimedPublicOperation {
        id: OperationId::new(),
        org_id: "00000000-0000-0000-0000-000000000001".parse()?,
        actor_user_id: Some("00000000-0000-0000-0000-000000000010".parse()?),
        api_product: crate::public_operations::ApiProduct::Native,
        kind: OperationKind::SuspendSandbox,
        target_type: None,
        target_id: Some(public_sandbox_id.to_string()),
        payload: crate::public_sandbox_operations::SandboxLifecycleOperationPayload {
            sandbox_id: public_sandbox_id,
            action: SandboxLifecycleAction::Suspend,
            rollback: None,
            wake_activity: None,
            queued_execution_generation: fence.map(ExecutionGeneration::new).transpose()?,
        }
        .into_json(),
        attempts: 2,
        claim_generation: 2,
        traceparent: None,
        age_at_first_claim_secs: None,
    })
}

/// A suspend executor over a Kubernetes substrate whose apiserver answers
/// nothing, with the log of every request the substrate made.
fn suspend_executor(
    pool: &PgPool,
) -> (
    crate::public_sandbox_operations::SandboxLifecycleOperationExecutor,
    crate::controllers::substrate::kubernetes::scripted_test_doubles::KubeLog,
) {
    let (client, log) = crate::test_support::kubernetes::unavailable_apiserver();
    let controller =
        SandboxController::new(crate::controllers::substrate::Substrate::from_kubernetes(
            substrate_with_store_and_client(pool.clone(), client),
        ));
    (
        crate::public_sandbox_operations::SandboxLifecycleOperationExecutor::new(
            pool.clone(),
            controller,
        ),
        log,
    )
}

/// The shape a requeued suspend finds after a wake: its first attempt
/// suspended generation 1, retiring it, and the user's wake then restored the
/// sandbox as generation 2, which runs. The row went `suspended`,
/// `restoring`, then `started`, after generation 1 was retired.
async fn insert_woken_after_suspend(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    insert_test_sandbox_row(pool, sandbox_id, "suspended").await?;
    insert_k8s_execution(pool, sandbox_id, 1, POD_NAME, POD_UID, None, false).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'suspended', terminated_at = NOW() - INTERVAL '1 minute'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    for lifecycle in ["restoring", "started"] {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox_id)
            .bind(lifecycle)
            .execute(pool)
            .await?;
    }
    insert_k8s_execution(
        pool,
        sandbox_id,
        2,
        WOKEN_POD_NAME,
        WOKEN_POD_UID,
        Some("10.0.0.2"),
        true,
    )
    .await?;
    Ok(())
}

/// `(lifecycle, the active execution's generation and state)`.
async fn suspend_target(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(String, Option<(i64, String)>)> {
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
    let active: Option<(i64, String)> = sqlx::query_as(sql!(
        "SELECT generation, state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?;
    Ok((lifecycle, active))
}

/// A suspend requeued after its first attempt suspended the sandbox and the
/// user then woke it ends `canceled`, its execution superseded by the woken
/// one, and does not suspend the woken sandbox: the row stays `started`,
/// generation 2 stays the active running execution, and the substrate makes
/// no request.
#[tokio::test(flavor = "current_thread")]
async fn a_requeued_suspend_after_a_wake_ends_superseded_without_suspending() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_woken_after_suspend(pool, sandbox_id).await?;
    let (executor, log) = suspend_executor(pool);

    let result = executor
        .execute(&queued_suspend(sandbox_id, Some(1))?)
        .await;

    assert_eq!(
        result.state,
        pplx_space_public_api::OperationState::Canceled,
        "{result:?}"
    );
    assert_eq!(
        result.terminal_error_code,
        Some(pplx_space_public_api::PublicErrorCode::OperationFailed),
        "{result:?}"
    );
    let error = result
        .terminal_error_json
        .clone()
        .ok_or("a superseded suspend records why")?;
    assert_eq!(
        error["reason"],
        crate::public_sandbox_operations::SUSPEND_SUPERSEDED_BY_NEWER_EXECUTION_REASON,
        "{error}"
    );
    assert_eq!(error["queued_execution_generation"], 1, "{error}");
    assert_eq!(error["current_execution_generation"], 2, "{error}");
    assert_eq!(
        result.progress_message,
        "Execution generation 1, which this suspend was requested for, is no longer active; \
         generation 2 now runs, and this suspend is not applied to it. Request a new suspend \
         to suspend the sandbox as it runs now."
    );
    assert_eq!(
        suspend_target(pool, sandbox_id).await?,
        ("started".to_owned(), Some((2, "running".to_owned()))),
        "the woken sandbox is not suspended again"
    );
    assert!(
        log.lock().expect("kube log").is_empty(),
        "the superseded suspend reaches no pod"
    );
    Ok(())
}

/// The same requeue from an API that records no generation runs as every
/// suspend did before the fence: it claims the woken execution, generation 2,
/// moving the row to `suspending` (here it then finds no node and is retried).
#[tokio::test(flavor = "current_thread")]
async fn a_requeued_suspend_without_a_fence_acts_on_the_woken_execution_as_before() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_woken_after_suspend(pool, sandbox_id).await?;
    let (executor, _) = suspend_executor(pool);

    let result = executor.execute(&queued_suspend(sandbox_id, None)?).await;

    assert_eq!(
        result.state,
        pplx_space_public_api::OperationState::Queued,
        "{result:?}"
    );
    assert_eq!(
        suspend_target(pool, sandbox_id).await?,
        ("suspending".to_owned(), Some((2, "running".to_owned()))),
        "an unfenced suspend claims whatever execution is active"
    );
    Ok(())
}

/// A requeued suspend whose execution is still the one it was queued for
/// goes on: here its first attempt claimed generation 1 and the node
/// committed the suspend, so this attempt retires the pod and finalizes the
/// row `suspended`.
#[tokio::test(flavor = "current_thread")]
async fn a_requeued_suspend_of_its_own_execution_completes() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(pool, sandbox_id, "suspending").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(LocalArtifactId::new())
    .execute(pool)
    .await?;
    insert_k8s_execution(pool, sandbox_id, 1, POD_NAME, POD_UID, None, true).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET state = 'suspended' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let deletes = Arc::new(AtomicUsize::new(0));
    let mut substrate = substrate_with_store_and_client(
        pool.clone(),
        deleted_pod_kubernetes_client(POD_NAME, POD_UID, Arc::clone(&deletes)),
    );
    substrate.k8s.k8s_blocking_delete_timeout = std::time::Duration::from_millis(500);
    let executor = crate::public_sandbox_operations::SandboxLifecycleOperationExecutor::new(
        pool.clone(),
        SandboxController::new(crate::controllers::substrate::Substrate::from_kubernetes(
            substrate,
        )),
    );

    let result = executor
        .execute(&queued_suspend(sandbox_id, Some(1))?)
        .await;

    assert_eq!(
        result.state,
        pplx_space_public_api::OperationState::Succeeded,
        "{result:?}"
    );
    assert_eq!(
        suspend_target(pool, sandbox_id).await?,
        ("suspended".to_owned(), None),
        "the suspend it was queued for completes"
    );
    assert_eq!(
        deletes.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "its pod is retired"
    );
    Ok(())
}

/// A suspend whose sandbox is already `suspended` completes as already done
/// before the fence is read: here another execution, generation 2, is
/// active, which the fence alone would cancel.
#[tokio::test(flavor = "current_thread")]
async fn a_requeued_suspend_already_suspended_completes() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(pool, sandbox_id, "suspended").await?;
    insert_k8s_execution(pool, sandbox_id, 1, POD_NAME, POD_UID, None, false).await?;
    insert_k8s_execution(
        pool,
        sandbox_id,
        2,
        WOKEN_POD_NAME,
        WOKEN_POD_UID,
        None,
        true,
    )
    .await?;
    let (executor, log) = suspend_executor(pool);

    let result = executor
        .execute(&queued_suspend(sandbox_id, Some(1))?)
        .await;

    assert_eq!(
        result.state,
        pplx_space_public_api::OperationState::Succeeded,
        "{result:?}"
    );
    let reported = result
        .result_json
        .ok_or("a succeeded suspend reports a result")?;
    assert_eq!(
        reported["controller_response"]["already_completed"], true,
        "{reported}"
    );
    assert!(log.lock().expect("kube log").is_empty());
    Ok(())
}

/// The Kubernetes suspend claim is conditional on the fence: with another
/// generation active, the substrate refuses before it records its
/// `suspend_requested` or claims anything, with the typed refusal that names
/// both generations. The controller passes it through as such, without a
/// `suspend_failed` event, so a wake that lands between the executor's check
/// and the claim is neither suspended nor reported as a failed suspend.
#[tokio::test(flavor = "current_thread")]
async fn kubernetes_suspend_refuses_a_fence_the_active_execution_does_not_match() -> TestResult {
    use tracing_subscriber::layer::SubscriberExt as _;

    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_woken_after_suspend(pool, sandbox_id).await?;
    let (client, log) = crate::test_support::kubernetes::unavailable_apiserver();
    let substrate = substrate_with_store_and_client(pool.clone(), client);
    let fenced = ExecutionGeneration::new(1)?;
    let current = ExecutionGeneration::new(2)?;

    let capture = super::tests::SandboxEventCapture::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let refused = crate::controllers::substrate::SubstrateAdapter::suspend_execution(
        &substrate,
        sandbox_id,
        Some(fenced),
    )
    .await;
    let controller = SandboxController::new(
        crate::controllers::substrate::Substrate::from_kubernetes(substrate),
    );
    let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .ok_or("the sandbox exists")?;
    let acted = controller
        .action_on_execution(
            pool,
            sandbox_id,
            Some(&sandbox),
            SandboxApiMetricIdentity::native("suspend"),
            SandboxAction::Suspend,
            None,
            crate::controllers::substrate::ActivationClient::Native,
            Some(fenced),
        )
        .await;
    drop(guard);

    assert!(
        matches!(
            refused,
            Err(SubstrateError::ExecutionReplaced { fenced: f, current: c })
                if f == fenced && c == current
        ),
        "{refused:?}"
    );
    let error = acted.expect_err("the controller refuses the replaced execution");
    assert_eq!(
        error
            .execution_replaced_details()
            .map(crate::controllers::ExecutionReplaced::current),
        Some(current),
        "{error:?}"
    );
    // The controller's own `suspend_requested`, which it records for every
    // action before the substrate call; the substrate records none.
    let emitted = capture.types.lock().expect("event capture").clone();
    assert_eq!(
        emitted
            .iter()
            .filter(|event| event.starts_with("suspend_"))
            .collect::<Vec<_>>(),
        ["suspend_requested"],
        "{emitted:?}"
    );
    assert_eq!(
        suspend_target(pool, sandbox_id).await?,
        ("started".to_owned(), Some((2, "running".to_owned()))),
        "nothing was claimed"
    );
    assert!(log.lock().expect("kube log").is_empty());
    Ok(())
}

/// Runs its statements once, on a connection of its own, when the
/// controller records a suspend's `suspend_requested`: after the executor
/// checked the fence and before the substrate reads the active execution.
/// Nothing reads the database in between, so the event is the one point at
/// which a test can land a replacement there.
struct ReplaceOnSuspendRequested {
    options: sqlx::postgres::PgConnectOptions,
    statements: std::sync::Mutex<Option<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ReplaceOnSuspendRequested {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        event.record(&mut visitor);
        let fields = visitor.into_fields();
        if pplx_space_observability::test_capture::field_value(&fields, "event_stream")
            != Some("space.sandbox")
            || pplx_space_observability::test_capture::field_value(&fields, "event_type")
                != Some("suspend_requested")
        {
            return;
        }
        let Some(statements) = self.statements.lock().expect("statements").take() else {
            return;
        };
        let options = self.options.clone();
        // The test's runtime is blocked in this callback, so the statements
        // run on a runtime and connection of their own.
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("replacement runtime")
                .block_on(async move {
                    use sqlx::Connection as _;
                    let mut connection = sqlx::PgConnection::connect_with(&options)
                        .await
                        .expect("replacement connection");
                    for statement in statements {
                        sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                            .execute(&mut connection)
                            .await
                            .expect("replacement statement");
                    }
                });
        })
        .join()
        .expect("replacement thread");
    }
}

/// Run a fenced suspend of `sandbox_id` for generation 1 that finds its
/// fence still active, and replace generation 1 with generation 2, running,
/// before the substrate's claim.
async fn suspend_racing(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(
    crate::public_sandbox_operations::PublicOperationExecutionResult,
    crate::controllers::substrate::kubernetes::scripted_test_doubles::KubeLog,
)> {
    use tracing_subscriber::layer::SubscriberExt as _;

    let (executor, log) = suspend_executor(pool);
    let operation = queued_suspend(sandbox_id, Some(1))?;
    let statements = vec![
        format!(
            "UPDATE sandbox_k8s_executions
                SET active = FALSE, state = 'replaced',
                    terminated_at = NOW() - INTERVAL '1 minute'
              WHERE sandbox_id = '{sandbox_id}' AND generation = 1"
        ),
        format!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
             SELECT sandbox_id, 2, '{WOKEN_POD_NAME}', pod_namespace, '{WOKEN_POD_UID}',
                    '10.0.0.2', 'running', TRUE
               FROM sandbox_k8s_executions
              WHERE sandbox_id = '{sandbox_id}' AND generation = 1"
        ),
    ];
    let replace = ReplaceOnSuspendRequested {
        options: (*pool.connect_options()).clone(),
        statements: std::sync::Mutex::new(Some(statements)),
    };
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(replace));
    let result = executor.execute(&operation).await;
    drop(guard);
    Ok((result, log))
}

/// A started sandbox at generation 1.
async fn insert_started_at_generation_one(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    insert_test_sandbox_row(pool, sandbox_id, "started").await?;
    insert_k8s_execution(
        pool,
        sandbox_id,
        1,
        POD_NAME,
        POD_UID,
        Some("10.0.0.1"),
        true,
    )
    .await?;
    Ok(())
}

/// Another execution that replaces the fenced one after the executor
/// checked the fence and before the substrate's claim, here with the row
/// `started` throughout, as a recovery or start that rebuilt the pod leaves
/// it: the substrate refuses the new execution, and the executor reads that
/// refusal as the supersession it is. The operation ends `canceled`, the
/// sandbox is not suspended, and no pod is touched.
#[tokio::test(flavor = "current_thread")]
async fn a_replacement_between_the_fence_check_and_the_claim_supersedes_the_suspend() -> TestResult
{
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_started_at_generation_one(pool, sandbox_id).await?;

    let (result, log) = suspend_racing(pool, sandbox_id).await?;

    assert_eq!(
        result.state,
        pplx_space_public_api::OperationState::Canceled,
        "{result:?}"
    );
    let error = result
        .terminal_error_json
        .clone()
        .ok_or("a superseded suspend records why")?;
    assert_eq!(
        error["reason"],
        crate::public_sandbox_operations::SUSPEND_SUPERSEDED_BY_NEWER_EXECUTION_REASON,
        "{error}"
    );
    assert_eq!(error["queued_execution_generation"], 1, "{error}");
    assert_eq!(error["current_execution_generation"], 2, "{error}");
    assert_eq!(
        suspend_target(pool, sandbox_id).await?,
        ("started".to_owned(), Some((2, "running".to_owned()))),
        "nothing was claimed"
    );
    assert!(log.lock().expect("kube log").is_empty());
    Ok(())
}
