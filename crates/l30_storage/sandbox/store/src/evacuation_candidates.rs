//! Which sandboxes on a draining node node drain may evacuate next, in the
//! order it dispatches them.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_residency::{ResidencyEvidence, occupies_node_predicate};
use pplx_space_sql_locator::sql;

/// One evacuation candidate: the sandbox and the lifecycle its evacuation
/// restores it to. A `suspending` row reports its `evacuation_restore_target`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct EvacuationCandidate {
    pub id: SandboxId,
    pub lifecycle: SandboxLifecycle,
}

/// `$1` = node id, `$2` = the evacuable lifecycles as `text[]`.
///
/// The third disjunct is the in-flight clause and doesn't consult `$2`: a
/// stopped evacuation already carrying its marker stays listed after the
/// lifecycle is disabled, so it can finish. An encrypted `stopped` row is never
/// listed, on either disjunct, because its disk artifact can't be re-placed.
/// The `ORDER BY` decides who is dispatched within the caller's slot budget:
/// the suspend population first, then the stopped rows, oldest first within
/// each. `{occupies}` is [`occupies_node_predicate`] under `residency`, so
/// census-absent rows never take a slot.
///
/// Why the order and the encrypted exclusion are load-bearing is argued on
/// `list_evacuatable_sandbox_rows` in `pplx-space-cluster-controller-server`'s
/// `node_drain/store.rs`; read it before changing either.
fn list_on_node_query(residency: ResidencyEvidence) -> String {
    let occupies = occupies_node_predicate("s", "$1", residency);
    sql!(format!(
        "SELECT s.id,
                CASE
                    WHEN s.lifecycle = 'suspending' THEN s.evacuation_restore_target
                    ELSE s.lifecycle
                END AS lifecycle
           FROM sandboxes s
          WHERE s.active_node_id = $1
            AND (
                s.lifecycle = ANY($2)
                OR (
                    s.lifecycle = 'suspending'
                    AND s.evacuation_restore_target = ANY($2)
                )
                OR (
                    s.lifecycle = 'stopped'
                    AND s.evacuation_restore_target = 'stopped'
                )
            )
            AND NOT (
                s.lifecycle = 'stopped'
                AND COALESCE(s.volume_encryption, FALSE)
            )
            AND {occupies}
          ORDER BY CASE WHEN s.lifecycle = 'stopped' THEN 1 ELSE 0 END,
                   s.lifecycle_changed_at,
                   s.id"
    ))
}

static LIST_ON_NODE_LIFECYCLE_ONLY: LazyLock<String> =
    LazyLock::new(|| list_on_node_query(ResidencyEvidence::LifecycleOnly));
static LIST_ON_NODE_NODE_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| list_on_node_query(ResidencyEvidence::NodeCensusAware));

/// The rows on `node_id` in one of `evacuable_lifecycles`, mid-suspend toward
/// one, or carrying a stopped evacuation's marker, that still occupy the node
/// under `residency`, in dispatch order. A read; it takes no lock, and in the
/// census-aware mode it compares against the executor's `NOW()`.
pub async fn list_on_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    evacuable_lifecycles: &[&str],
    residency: ResidencyEvidence,
) -> Result<Vec<EvacuationCandidate>, DbError> {
    let statement = match residency {
        ResidencyEvidence::LifecycleOnly => LIST_ON_NODE_LIFECYCLE_ONLY.as_str(),
        ResidencyEvidence::NodeCensusAware => LIST_ON_NODE_NODE_CENSUS_AWARE.as_str(),
    };
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .bind(evacuable_lifecycles)
        .fetch_all(executor.into_sqlx())
        .await?)
}
