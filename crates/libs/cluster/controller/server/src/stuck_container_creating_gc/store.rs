//! The Postgres reads behind the GC's two pod-marker exclusions.
//!
//! Both markers the GC excludes on — the restore annotations and the
//! `pool-role=warm` label — are stamped at pod-build time and never cleared.
//! Read literally they answer "was this pod ever a restore / a pool slot?",
//! while the exclusions mean "is it *still* owned by the restore path / the
//! warm-pool reconciler?". Those diverge the moment a restore completes or a
//! slot is claimed: the pod keeps the marker, ownership has moved on, and the
//! pod becomes permanently un-collectable however long it stays wedged. The
//! live row is what closes that gap; [`super::classify::MarkerOwnership`] holds
//! the decision these lookups feed.
//!
//! Every answer is per requested key, so a key the caller asked about is always
//! present in the result: `Absent` when no row exists, `Unknown` when the row
//! holds a value this build can't interpret. A failed query yields an empty map
//! and therefore `Unknown` for everything — the pre-existing, over-excluding
//! behavior.
//!
//! The GC reads Postgres here; it still never writes it. Retiring the sandbox
//! row of a deleted pod remains the convergence reconcilers' job, which is what
//! the startup convergence gate checks.

use std::collections::HashMap;
use std::str::FromStr;

use pplx_space_cluster_pool::{SlotOwnership, WarmPoolStore};
use pplx_space_common_core::restore_ownership::NODE_RPC_TIMEOUT;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SubstrateKind};
use sqlx::PgPool;

use super::classify::{Lookup, Owner};

/// Maximum keys bound into one `= ANY($1)` statement. The candidate list is
/// bounded only by "stuck pods past the threshold", which in the multi-thousand
/// stuck-create storm this GC exists to relieve can be far larger than the
/// per-minute delete budget. Chunking keeps any single statement's array small
/// enough that the planner stays on the index scan.
const LOOKUP_CHUNK: usize = 1_000;

/// How long a started recovery's `recovering` execution row stays
/// authoritative: the node-RPC timeout, which is what the API's restore claims
/// bind as their lease too.
pub(super) const STARTED_RECOVERY_LEASE: std::time::Duration = NODE_RPC_TIMEOUT;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("query failed: {0}")]
    Query(#[from] sqlx::Error),
    #[error("warm-pool slot lookup failed: {0}")]
    Pool(#[from] pplx_space_cluster_pool::PoolError),
}

/// Dependency-injection seam for the per-tick ownership lookups, mirroring
/// [`super::backend::GcBackend`] so the reconciler core stays testable without
/// a live Postgres.
pub trait GcStore: Send + Sync {
    /// One entry per requested sandbox id.
    fn restore_owners(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> impl std::future::Future<Output = Result<HashMap<SandboxId, Lookup<Owner>>, StoreError>> + Send;

    /// One entry per requested pod name.
    fn warm_pool_owners(
        &self,
        pod_names: &[String],
    ) -> impl std::future::Future<Output = Result<HashMap<String, Lookup<Owner>>, StoreError>> + Send;
}

/// Live `GcStore` over the cluster Postgres. Warm-pool slots are read through
/// [`WarmPoolStore`], which owns that table.
pub struct PostgresGcStore {
    pool: PgPool,
    slots: WarmPoolStore,
}

impl PostgresGcStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            slots: WarmPoolStore::new(pool.clone()),
            pool,
        }
    }
}

impl GcStore for PostgresGcStore {
    async fn restore_owners(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, Lookup<Owner>>, StoreError> {
        // Seeded `Absent`: a requested id with no row is a sandbox that is gone,
        // and every row read below overwrites its own key. Only a
        // Kubernetes-managed row can own a pod in this cluster, so another
        // substrate's row reads as no row.
        let mut lookup: HashMap<SandboxId, Lookup<Owner>> =
            sandbox_ids.iter().map(|id| (*id, Lookup::Absent)).collect();

        let lease_secs = i64::try_from(STARTED_RECOVERY_LEASE.as_secs())
            .expect("started recovery lease seconds fit i64");
        for chunk in sandbox_ids.chunks(LOOKUP_CHUNK) {
            let ids: Vec<uuid::Uuid> = chunk.iter().map(|id| id.as_uuid()).collect();
            // A started recovery keeps the row in `started` and records
            // ownership on its execution instead, so the lifecycle alone would
            // report the pod's marker stale while a recovery is mid-flight.
            let rows = sqlx::query_as::<_, (uuid::Uuid, String, bool)>(sql!(
                "SELECT s.id,
                        s.lifecycle,
                        EXISTS (
                            SELECT 1
                            FROM sandbox_k8s_executions e
                            WHERE e.sandbox_id = s.id
                              AND e.active
                              AND e.state = 'recovering'
                              AND e.updated_at > NOW() - make_interval(secs => $2)
                        ) AS recovering
                 FROM sandboxes s
                 WHERE s.id = ANY($1) AND s.managed_by = $3"
            ))
            .bind(&ids)
            .bind(lease_secs)
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(&self.pool)
            .await?;
            for (id, lifecycle, recovering) in rows {
                let id = SandboxId::from_uuid(id);
                if recovering {
                    lookup.insert(id, Lookup::Known(Owner::InFlight));
                    continue;
                }
                // The column is wider than the enum in principle; an
                // unrecognized value must not stay `Absent`, which would
                // *release* the pod.
                let resolved = match SandboxLifecycle::from_str(&lifecycle) {
                    Ok(SandboxLifecycle::Restoring) => Lookup::Known(Owner::InFlight),
                    Ok(_) => Lookup::Known(Owner::Released),
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id = %id,
                            %error,
                            "stuck_container_creating_gc: unrecognized sandbox lifecycle; \
                             keeping the pod's marker exclusion"
                        );
                        Lookup::Unknown
                    }
                };
                lookup.insert(id, resolved);
            }
        }
        Ok(lookup)
    }

    async fn warm_pool_owners(
        &self,
        pod_names: &[String],
    ) -> Result<HashMap<String, Lookup<Owner>>, StoreError> {
        let mut lookup: HashMap<String, Lookup<Owner>> = pod_names
            .iter()
            .map(|name| (name.clone(), Lookup::Absent))
            .collect();

        for chunk in pod_names.chunks(LOOKUP_CHUNK) {
            for row in self.slots.slot_ownership_by_pod_name(chunk).await? {
                let owner = warm_pool_owner(&row);
                lookup.insert(row.pod_name, owner);
            }
        }
        Ok(lookup)
    }
}

/// Which side of a warm-pool handoff owns the pod behind one slot row.
///
/// The pod's `pool-role=warm` label is stamped at creation and never removed, so
/// the label alone can't answer this — only the slot row can.
pub(super) fn warm_pool_owner(row: &SlotOwnership) -> Lookup<Owner> {
    match row.pool_owns_pod() {
        Ok(true) => Lookup::Known(Owner::InFlight),
        Ok(false) => Lookup::Known(Owner::Released),
        // A state this build can't interpret must not stay `Absent`, which
        // would *release* the pod.
        Err(error) => {
            tracing::warn!(
                pod_name = row.pod_name,
                %error,
                "stuck_container_creating_gc: unrecognized warm-pool slot state; \
                 keeping the pod's marker exclusion"
            );
            Lookup::Unknown
        }
    }
}
