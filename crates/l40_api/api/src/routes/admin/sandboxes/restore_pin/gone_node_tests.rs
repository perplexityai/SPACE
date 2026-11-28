//! The drained-node terminator's give-up and the pin agree on a worker that's
//! gone. The give-up keeps a suspended sandbox whose history can't be ordered,
//! because its captures are durable, and holds the node for it until an
//! operator pins one. The pin asks the sandbox's node what it holds, and a
//! node that's gone never answers. So the pin reads a node Kubernetes
//! confirms gone as holding no copy, and only that node: one that's still
//! there, or one the API server can't speak for, keeps the `503`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::{Request, Response, StatusCode};
use kube::client::{Body, ClientBuilder};
use pplx_space_accounts::NodeId;
use pplx_space_k8s_node_id::space_k8s_node_id;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::restore_pin::restore_pin_state;
use pplx_space_sandbox_store::{drain_gate_blockers, lost};
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{RestorePinRequest, pin_checked};
use crate::controllers::sandbox::SandboxController;
use crate::controllers::substrate::Substrate;
use crate::controllers::substrate::kubernetes::tests::substrate_with_store_and_client;

const NODE_NAME: &str = "space-test-gone-node";
const NODE_UID: &str = "uid-1";

/// How long each case waits for the node to say what it holds. No node-api pod
/// serves the node in any case, so the ask never answers and every case waits
/// this out.
const ASK_BUDGET: Duration = Duration::from_millis(200);

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What the Kubernetes API server says about the sandbox's node.
#[derive(Clone, Copy, Debug)]
enum NodeAnswer {
    /// No Node under the name: the worker is gone.
    Absent,
    /// The Node the worker registered as.
    Present,
    /// The API server fails the read.
    Fails,
    /// No Node under the name, and a replacement registers under it before
    /// the answer arrives.
    AbsentWhileAReplacementRegisters,
}

fn json(status: StatusCode, body: &serde_json::Value) -> Result<Response<Body>, BoxError> {
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body)?))?)
}

/// A Kubernetes client whose only Node read answers as `answer` says.
/// `replaced_before_absent` is set once the replacement's registration has
/// been written and the read is about to answer that the Node is absent.
fn kube_client(
    answer: NodeAnswer,
    pool: PgPool,
    node_id: NodeId,
    replaced_before_absent: Arc<AtomicBool>,
) -> kube::Client {
    let service = tower::service_fn(move |request: Request<Body>| {
        let pool = pool.clone();
        let replaced_before_absent = Arc::clone(&replaced_before_absent);
        async move {
            assert_eq!(request.uri().path(), format!("/api/v1/nodes/{NODE_NAME}"));
            let absent = serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "NotFound",
                "code": 404
            });
            match answer {
                NodeAnswer::Absent => json(StatusCode::NOT_FOUND, &absent),
                NodeAnswer::Present => json(
                    StatusCode::OK,
                    &serde_json::json!({
                        "kind": "Node",
                        "apiVersion": "v1",
                        "metadata": {"name": NODE_NAME, "uid": NODE_UID}
                    }),
                ),
                NodeAnswer::Fails => json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "reason": "InternalError",
                        "code": 500
                    }),
                ),
                NodeAnswer::AbsentWhileAReplacementRegisters => {
                    let registered = sqlx::query(sql!(
                        "UPDATE node_k8s SET k8s_node_uid = 'uid-2' WHERE node_id = $1"
                    ))
                    .bind(node_id)
                    .execute(&pool)
                    .await?;
                    let answer = json(StatusCode::NOT_FOUND, &absent)?;
                    replaced_before_absent.store(registered.rows_affected() == 1, Ordering::SeqCst);
                    Ok(answer)
                }
            }
        }
    });
    ClientBuilder::new(service, "space").build()
}

/// A drained Kubernetes worker registered as [`NODE_NAME`] under [`NODE_UID`].
async fn insert_drained_worker(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = space_k8s_node_id(NODE_NAME, "worker");
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase)
         VALUES ($1, '10.0.0.71'::INET, 'worker', 'k8s', 'ready', 'drained')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(NODE_NAME)
    .bind(NODE_UID)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A sandbox suspended on `node_id` five minutes ago, with no pinned capture.
async fn insert_suspended_sandbox(pool: &PgPool, node_id: NodeId) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by, target_node_id,
             lifecycle_changed_at, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s', $2,
             NOW() - INTERVAL '5 minutes', NOW() - INTERVAL '30 days', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// A ready suspend capture of `sandbox_id` with a stored copy, uploaded from
/// `node_id` `minutes_ago`, recording `generation`.
async fn insert_suspend_capture(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    generation: Option<i64>,
    minutes_ago: i32,
) -> TestResult<LocalArtifactId> {
    let artifact = LocalArtifactId::new();
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, timestamp, org_id,
             local_artifact_id, artifact_kind, artifact_origin, generation
         )
         VALUES (
             $1, $2, 'full', 'ready', $3, NOW() - make_interval(mins => $4),
             '00000000-0000-0000-0000-000000000001', $5, 'full', 'suspend', $6
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .bind(minutes_ago)
    .bind(artifact.as_uuid())
    .bind(generation)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://test-bucket/' || $1::TEXT, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .execute(pool)
    .await?;
    Ok(artifact)
}

/// The sandbox's `(lifecycle, current_local_artifact_id)`.
async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(String, Option<Uuid>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The terminator's give-up on the node: claim, then flip what it claimed.
async fn give_up(pool: &PgPool, node_id: NodeId) -> TestResult<Vec<SandboxId>> {
    let mut tx = pool.begin().await?;
    let claimed = lost::lock_unrecoverable_candidates(&mut *tx, node_id).await?;
    let lost = lost::mark_unrecoverable_sandboxes_lost(
        &mut *tx,
        node_id,
        Some(NODE_UID),
        &claimed,
        Duration::ZERO,
    )
    .await?;
    tx.commit().await?;
    Ok(lost)
}

/// Each answer, and whether the pin lands. One case per test, so each
/// runs on its own database within the per-test time budget.
#[rstest::rstest]
#[case::absent(NodeAnswer::Absent, true)]
#[case::present(NodeAnswer::Present, false)]
#[case::fails(NodeAnswer::Fails, false)]
#[case::absent_while_a_replacement_registers(NodeAnswer::AbsentWhileAReplacementRegisters, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_pin_reads_a_confirmed_gone_node_as_holding_no_copy(
    #[case] answer: NodeAnswer,
    #[case] lands: bool,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let node_id = insert_drained_worker(pool).await?;
    // Its only copies are two suspend captures that can't be ordered: the
    // one with no generation registered last.
    let sandbox_id = insert_suspended_sandbox(pool, node_id).await?;
    let with_generation = insert_suspend_capture(pool, sandbox_id, node_id, Some(5), 20).await?;
    insert_suspend_capture(pool, sandbox_id, node_id, None, 10).await?;
    // A sandbox with no copy off the node, which the give-up does fail.
    let casualty = insert_suspended_sandbox(pool, node_id).await?;

    assert_eq!(give_up(pool, node_id).await?, [casualty], "{answer:?}");
    assert_eq!(
        row(pool, sandbox_id).await?,
        ("suspended".to_owned(), None),
        "{answer:?}"
    );
    assert_eq!(
        drain_gate_blockers::count_on_node(pool, node_id).await?,
        1,
        "{answer:?}: the unordered history holds the node"
    );

    let read = restore_pin_state(pool, sandbox_id).await?.expect("sandbox");
    assert!(read.unordered, "{answer:?}");
    assert_eq!(read.restore_node_id, Some(node_id), "{answer:?}");
    let replaced_before_absent = Arc::new(AtomicBool::new(false));
    let controller =
        SandboxController::new(Substrate::from_kubernetes(substrate_with_store_and_client(
            pool.clone(),
            kube_client(
                answer,
                pool.clone(),
                node_id,
                Arc::clone(&replaced_before_absent),
            ),
        )));
    let request = RestorePinRequest {
        local_artifact_id: with_generation,
        lifecycle_changed_at: read.lifecycle_changed_at.expect("lifecycle_changed_at"),
    };

    let pinned = pin_checked(pool, &controller, sandbox_id, &request, ASK_BUDGET).await;

    // Only the replacement case is refused by the registration read that
    // follows the absent answer, so it must have got that far.
    assert_eq!(
        replaced_before_absent.load(Ordering::SeqCst),
        matches!(answer, NodeAnswer::AbsentWhileAReplacementRegisters),
        "{answer:?}"
    );
    if lands {
        let pinned = pinned.map_err(|error| format!("{answer:?}: {error:?}"))?;
        assert_eq!(
            pinned.pinned_artifact_id,
            Some(with_generation),
            "{answer:?}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            ("suspended".to_owned(), Some(with_generation.as_uuid())),
            "{answer:?}"
        );
        assert_eq!(
            drain_gate_blockers::count_on_node(pool, node_id).await?,
            0,
            "{answer:?}: the pinned sandbox releases the node"
        );
    } else {
        let refused = pinned.expect_err("the node may still hold a copy");
        assert_eq!(
            refused.classification().0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{answer:?}: {refused:?}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            ("suspended".to_owned(), None),
            "{answer:?}"
        );
    }
    Ok(())
}
