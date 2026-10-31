//! Production [`SandboxStore`] backed by Postgres.

use std::collections::HashMap;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SubstrateKind};
use pplx_space_sandbox_portability::execution_supersession::ExecutionSupersessionRow;
use sqlx::PgPool;
use uuid::Uuid;

use super::backend::SandboxStore;
use super::classify::{RowLifecycle, SandboxRestoreState};

/// One row of the classification query, decoded by column name so a
/// reordered `SELECT` can't swap two of the flags. `managed_by` and
/// `lifecycle` are read as text and parsed per row so one unrecognized value
/// can't fail the batch. The last four are one [`ExecutionSupersessionRow`].
/// When the LEFT JOIN found no execution to return, the first three are `NULL`
/// and the materialized expression reads `FALSE`, so the row is skipped on
/// `generation`.
#[derive(sqlx::FromRow)]
struct ClassifyRow {
    id: Uuid,
    managed_by: String,
    lifecycle: String,
    generation: Option<i64>,
    active: Option<bool>,
    replaced: Option<bool>,
    materialized: Option<bool>,
}

/// Map a raw row's `managed_by` + `lifecycle` to what the classifier may act
/// on: anything but a row with a known lifecycle whose substrate
/// [restores through pods](SubstrateKind::restores_through_pods) is
/// [`RowLifecycle::Unrecognized`], which the classifier skips. An unparseable
/// `managed_by` does not restore through pods.
fn row_lifecycle(managed_by: &str, lifecycle: &str) -> RowLifecycle {
    if !managed_by
        .parse::<SubstrateKind>()
        .is_ok_and(SubstrateKind::restores_through_pods)
    {
        return RowLifecycle::Unrecognized;
    }
    lifecycle
        .parse::<SandboxLifecycle>()
        .map_or(RowLifecycle::Unrecognized, RowLifecycle::Known)
}

pub struct SandboxDbStore {
    pool: PgPool,
}

impl SandboxDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl SandboxStore for SandboxDbStore {
    /// One lookup over the owning `sandboxes` rows, with their
    /// active execution (the adoption fence) and the execution rows the
    /// backup fence reads for each pod's generation. Read-only: the
    /// reaper's only mutation is the pod delete, and it only ever deletes
    /// provably-superseded pods, so no lock is needed here.
    ///
    /// Every row but a tombstone is returned — a non-Kubernetes or
    /// unrecognized-lifecycle row as [`RowLifecycle::Unrecognized`], so it is
    /// skipped rather than mistaken for a missing row.
    async fn classify_sandboxes(
        &self,
        floors: &[(SandboxId, i64)],
    ) -> Result<HashMap<SandboxId, SandboxRestoreState>, sqlx::Error> {
        if floors.is_empty() {
            return Ok(HashMap::new());
        }
        let (raw_ids, raw_floors): (Vec<Uuid>, Vec<i64>) = floors
            .iter()
            .map(|(id, floor)| (id.as_uuid(), *floor))
            .unzip();
        // `$1` = sandbox ids, `$2` = each id's lowest scanned pod generation
        // (the floor). One row per sandbox and execution that is active or at
        // or above its floor, or one with NULL execution columns when there is
        // none. A single statement, so the active execution and the rows above
        // the floor come from one snapshot.
        //
        // `lifecycle <> 'deleted'` excludes soft-delete **tombstone** rows,
        // which the control plane retains (with `lifecycle = 'deleted'`) until
        // tombstone-retention hard-deletes them. Excluding them makes a deleted
        // sandbox's restore pod fall to the row-absent (`SandboxMissing`) path
        // and be reaped, which is correct: a deleted sandbox has no legitimate
        // owner for the pod.
        let rows: Vec<ClassifyRow> = sqlx::query_as(sql!(
            "SELECT s.id, s.managed_by, s.lifecycle,
                    e.generation, e.active, e.state = 'replaced' AS replaced,
                    e.pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(e.pod_uid), '') IS NOT NULL
                        AS materialized
               FROM UNNEST($1::uuid[], $2::bigint[]) AS p(sandbox_id, floor_generation)
               JOIN sandboxes s ON s.id = p.sandbox_id
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND (e.active OR e.generation >= p.floor_generation)
              WHERE s.lifecycle <> 'deleted'"
        ))
        .bind(&raw_ids)
        .bind(&raw_floors)
        .fetch_all(&self.pool)
        .await?;
        let mut states: HashMap<SandboxId, SandboxRestoreState> = HashMap::new();
        for row in rows {
            let state = states
                .entry(SandboxId::from_uuid(row.id))
                .or_insert_with(|| SandboxRestoreState {
                    lifecycle: row_lifecycle(&row.managed_by, &row.lifecycle),
                    executions: Vec::new(),
                });
            if let (Some(generation), Some(active), Some(replaced), Some(materialized)) =
                (row.generation, row.active, row.replaced, row.materialized)
            {
                state.executions.push(ExecutionSupersessionRow {
                    generation,
                    active,
                    replaced,
                    materialized,
                });
            }
        }
        Ok(states)
    }
}
