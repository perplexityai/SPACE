//! Production [`SandboxStore`] backed by Postgres.

use std::collections::HashMap;

use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_portability::execution_supersession::ExecutionSupersessionRow;
use sqlx::PgPool;
use uuid::Uuid;

use super::backend::SandboxStore;
use super::classify::{RowLifecycle, SandboxRestoreState};

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
        // One statement, so the active execution and the rows above each
        // floor come from one snapshot; tombstone rows read as missing, so a
        // deleted sandbox's restore pod falls to the `SandboxMissing` path.
        let rows =
            pplx_space_sandbox_store::restore_pod_owners::list(&self.pool, &raw_ids, &raw_floors)
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
