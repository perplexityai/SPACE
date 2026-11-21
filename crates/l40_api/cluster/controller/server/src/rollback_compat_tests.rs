//! A rolled-back control plane, or an old cluster-controller leader during a
//! rolling deploy, reading rows this change holds: main's source-release and
//! give-up statements, frozen in [`main_statements`], against an unordered
//! history, a capture without a generation registered after one with a
//! generation. This change refuses to release such a sandbox's source and
//! keeps its row; main does neither.

mod main_statements;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::lost::{DEAD_UPLOAD_SETTLE, mark_unrecoverable_sandboxes_lost_query};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use crate::drained_node_terminator::PHASE_DRAINED;
use crate::lifecycle_writer_test_support::{
    SandboxSeed, insert_node, insert_sandbox, insert_snapshot,
};

/// The kube uid the drained node registers and the terminator binds.
const NODE_UID: &str = "uid-synthetic";

/// A `suspended` row resident on `node`, evacuation-marked when `marked`,
/// whose history is unordered: a ready capture with generation 5, then a
/// capture without one in `newest_state`.
async fn unordered_history(
    pool: &PgPool,
    node: NodeId,
    newest_state: &str,
    marked: bool,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let seed = SandboxSeed::new("suspended", "k8s").on_node(node);
    let seed = if marked { seed.marked("started") } else { seed };
    insert_sandbox(pool, sandbox_id, &seed).await?;
    insert_snapshot(pool, sandbox_id, "ready", -20).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET generation = 5 WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    insert_snapshot(pool, sandbox_id, newest_state, 0).await?;
    Ok(sandbox_id)
}

async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Main answers a node's `DurablyRestorable` with yes for an unpinned
/// unordered history, so a new node's eviction against main's control plane
/// deletes the local copy, and an old leader's drain gate doesn't hold the
/// node for it. This change answers no and holds the node.
#[tokio::test(flavor = "current_thread")]
async fn main_releases_the_source_of_an_unordered_history_this_change_holds() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node = insert_node(pool, None, None).await?;
    let sandbox_id = unordered_history(pool, node, "ready", false).await?;

    let main_restorable: bool =
        sqlx::query_scalar(sqlx::AssertSqlSafe(main_statements::DURABLY_RESTORABLE))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
    assert!(main_restorable, "main's DurablyRestorable");
    assert!(
        !pplx_space_sandbox_store::durably_restorable(pool, sandbox_id).await?,
        "this change's DurablyRestorable"
    );

    let main_blockers: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(main_statements::DRAIN_GATE))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert_eq!(main_blockers, 0, "main's drain gate");
    let query = crate::node_drain::store::drain_blocking_sandboxes_query();
    let blockers: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert_eq!(blockers, 1, "this change's drain gate");
    Ok(())
}

/// Main's give-ups fail an evacuation-marked unordered history whose newest
/// capture never uploaded, though its older capture's backup is ready. This
/// change keeps the row `suspended` with that copy.
#[tokio::test(flavor = "current_thread")]
async fn main_fails_an_unordered_history_this_change_keeps() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node = insert_node(pool, Some(PHASE_DRAINED), None).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'ip-10-0-0-98.synthetic.internal', $2)"
    ))
    .bind(node)
    .bind(NODE_UID)
    .execute(pool)
    .await?;

    // The drain's give-up after `ArtifactMissing`.
    let failed = unordered_history(pool, node, "failed", true).await?;
    let flipped = sqlx::query(sqlx::AssertSqlSafe(main_statements::MARK_SANDBOX_LOST))
        .bind(failed)
        .bind(node)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        (flipped, lifecycle_of(pool, failed).await?.as_str()),
        (1, "failed"),
        "main's drain give-up"
    );
    let kept = unordered_history(pool, node, "failed", true).await?;
    assert_eq!(
        pplx_space_sandbox_store::drain::mark_sandbox_lost(pool, kept, node).await?,
        0,
        "this change's drain give-up"
    );
    assert_eq!(lifecycle_of(pool, kept).await?, "suspended");

    // The drained-node terminator's give-up.
    let terminate = |query: String, sandbox_id: SandboxId| async move {
        let marked: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .bind(node)
            .bind(Some(NODE_UID))
            .bind(vec![sandbox_id])
            .bind(DEAD_UPLOAD_SETTLE.as_secs().cast_signed())
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(pool)
            .await?;
        TestResult::Ok(marked)
    };
    let failed = unordered_history(pool, node, "failed", true).await?;
    assert_eq!(
        terminate(
            main_statements::MARK_UNRECOVERABLE_SANDBOXES_LOST.to_owned(),
            failed
        )
        .await?,
        [failed],
        "main's terminator give-up"
    );
    let kept = unordered_history(pool, node, "failed", true).await?;
    assert_eq!(
        terminate(mark_unrecoverable_sandboxes_lost_query(), kept).await?,
        Vec::<SandboxId>::new(),
        "this change's terminator give-up"
    );
    assert_eq!(lifecycle_of(pool, kept).await?, "suspended");
    Ok(())
}
