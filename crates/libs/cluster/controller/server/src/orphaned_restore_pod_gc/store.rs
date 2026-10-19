//! Production [`SandboxStore`] backed by Postgres.

use std::collections::HashMap;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SubstrateKind};
use sqlx::PgPool;
use uuid::Uuid;

use super::backend::SandboxStore;
use super::classify::{RowLifecycle, SandboxRestoreState};

/// Row shape of the classification query: `(sandbox_id, managed_by, lifecycle,
/// active_generation)`. `managed_by` and `lifecycle` are read as text and
/// parsed per row so one unrecognized value can't fail the batch.
/// `active_generation` is `NULL` when the LEFT JOIN found no active execution
/// row.
type ClassifyRow = (Uuid, String, String, Option<i64>);

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
    /// One `ANY($1)` lookup over the owning `sandboxes` rows, LEFT JOINed to
    /// their single active execution so the classifier has the active
    /// execution's generation — the supersession fence. Read-only: the reaper's
    /// only mutation is the pod delete, and it only ever deletes
    /// provably-superseded pods, so no lock is needed here.
    ///
    /// `lifecycle <> 'deleted'` excludes soft-delete **tombstone** rows, which
    /// the control plane retains (with `lifecycle = 'deleted'`) until
    /// tombstone-retention hard-deletes them. Excluding them makes a deleted
    /// sandbox's restore pod fall to the row-absent (`SandboxMissing`) path and
    /// be reaped, which is correct: a deleted sandbox has no legitimate owner
    /// for the pod. Every other row is returned — a non-Kubernetes or
    /// unrecognized-lifecycle row as [`RowLifecycle::Unrecognized`], so it is
    /// skipped rather than mistaken for a missing row.
    async fn classify_sandboxes(
        &self,
        ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, SandboxRestoreState>, sqlx::Error> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let raw_ids: Vec<Uuid> = ids.iter().map(|id| id.as_uuid()).collect();
        let rows: Vec<ClassifyRow> = sqlx::query_as(sql!(
            "SELECT s.id, s.managed_by, s.lifecycle, e.generation
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.lifecycle <> 'deleted'
                AND s.id = ANY($1)"
        ))
        .bind(&raw_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, managed_by, lifecycle, active_generation)| {
                (
                    SandboxId::from_uuid(id),
                    SandboxRestoreState {
                        lifecycle: row_lifecycle(&managed_by, &lifecycle),
                        active_generation,
                    },
                )
            })
            .collect())
    }
}
