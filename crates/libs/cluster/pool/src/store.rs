use pplx_space_common_core::sql;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use pplx_space_common_core::types::{
    DEFAULT_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC, DEFAULT_DISK_IO_MAX_OPERATIONS_PER_SEC, MemorySize,
    SandboxId, VcpuCount,
};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    ClaimedPoolSlot, ClaimedSlotBindSource, ManualPoolConfigSpec, NewPoolSlot,
    OpenPoolSlotDeleteLock, PoolConfig, PoolConfigOwner, PoolConfigWithCounts, PoolError, PoolSlot,
    PoolSlotCounts, PoolSlotState, ProtectedTemplateRelease, ReleasePoolConfigWithCounts,
    ReservedPoolSlot, Result,
    models::{
        PoolAutoscaleCandidate, PoolObservedDemand, PoolPolicyRelease, memory_to_mib_i32,
        vcpu_to_i32,
    },
    rows::{
        ClaimedPoolSlotRow, PoolConfigRow, PoolObservedDemandRow, PoolPolicyReleaseRow,
        PoolSlotCountsRow, PoolSlotRow, ReservedPoolSlotRow,
    },
};

const AUTOSCALE_LOOKBACK: Duration = Duration::from_secs(24 * 60 * 60);
const AUTOSCALE_DEFAULT_VCPU: i32 = 2;
const AUTOSCALE_DEFAULT_MEMORY_MIB: i32 = 1024;
const MAX_ROOTFS_MIB: i64 =
    (pplx_space_common_core::sandbox_storage::MAX_SANDBOX_ROOTFS_BYTES / (1024 * 1024)) as i64;
// A full sizing bucket is also the admission threshold for a release outside
// the proactive retained window. That keeps one-off exact releases from each
// allocating the policy floor while preserving the existing sizing ratio once
// sustained demand admits the release.
const AUTOSCALE_SANDBOXES_PER_READY_SLOT: i64 = 10;
const AUTOSCALE_MIN_DEMAND_SANDBOXES: i64 = AUTOSCALE_SANDBOXES_PER_READY_SLOT;

/// Shared predicate for "this specific slot is a stale `claimed` row whose
/// handoff died": still `claimed`, older than the caller's timeout, and not
/// owned by a `sandboxes` row. The bind transaction inserts the sandbox row
/// and stamps the slot `bound` atomically, so a dead handoff leaves neither;
/// a `claimed` row that *does* have a sandbox row is a receipt from a bind
/// that predates the stamp (or from a rolled-back pre-stamp binary) and must
/// never be reaped — its pod is alive under that sandbox. The sandbox guard
/// is what makes this predicate safe on every environment regardless of
/// deploy ordering or rollbacks; once no pre-stamp receipts can exist it
/// never matches and is defense-in-depth. Kept as one macro so the
/// lock-free recheck and the `FOR UPDATE` locking variant cannot drift
/// apart while both stay compile-time literals for sqlx. Binds: `$1` = slot
/// id, `$2` = staleness threshold in seconds.
macro_rules! stale_unbound_claimed_slot_by_id_sql {
    ($suffix:literal) => {
        // Locator comment matching the `sql!` macro; expands at the const
        // definitions below, where the SQL text lives.
        concat!(
            "/*",
            module_path!(),
            ":",
            line!(),
            "*/ ",
            "SELECT s.id
             FROM kubernetes_pool_slots s
             WHERE s.id = $1
               AND s.state = 'claimed'
               AND s.claimed_at IS NOT NULL
               AND s.claimed_at <= NOW() - ($2 * INTERVAL '1 second')
               AND NOT EXISTS (
                   SELECT 1
                   FROM sandboxes sb
                   WHERE sb.id = s.reserved_sandbox_id
               )",
            $suffix
        )
    };
}

const STALE_UNBOUND_CLAIMED_SLOT_BY_ID_SQL: &str = stale_unbound_claimed_slot_by_id_sql!("");
const STALE_UNBOUND_CLAIMED_SLOT_BY_ID_FOR_UPDATE_SQL: &str =
    stale_unbound_claimed_slot_by_id_sql!(" FOR UPDATE SKIP LOCKED");

/// Result of one bounded backstop sweep window over `kubernetes_pool_slots`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackstopSweepOutcome {
    /// Receipts deleted inside the examined window.
    pub deleted: u64,
    /// Where the next sweep should resume: `Some(id)` continues after the
    /// last examined slot; `None` means this window reached the lap's
    /// high-water id and the next sweep should start a fresh lap.
    pub next_cursor: Option<Uuid>,
}

impl BackstopSweepOutcome {
    fn wrapped() -> Self {
        Self {
            deleted: 0,
            next_cursor: None,
        }
    }
}

/// One measured sweep window: the id closing the half-open range
/// `(cursor, last_id]` plus whether the window ran out of table before
/// filling.
struct BackstopWindow {
    last_id: Uuid,
    exhausted: bool,
}

impl BackstopWindow {
    fn into_outcome(self, deleted: u64) -> BackstopSweepOutcome {
        BackstopSweepOutcome {
            deleted,
            next_cursor: if self.exhausted {
                None
            } else {
                Some(self.last_id)
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct WarmPoolStore {
    pool: PgPool,
}

impl WarmPoolStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn enabled_configs(&self) -> Result<Vec<PoolConfig>> {
        let rows = sqlx::query_as::<_, PoolConfigRow>(sql!(
            "SELECT c.id, c.template_release_id, r.template_name, r.hash AS content_hash,
                    c.vcpu, c.memory_mib, c.disk_mib, c.disk_io_max_operations_per_sec,
                    c.disk_io_max_bandwidth_mib_per_sec, c.target_ready_slots, c.refill_per_tick,
                    c.max_warming_slots, c.namespace, c.enabled, c.encryptable,
                    c.managed_by, c.created_at, c.updated_at
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             WHERE c.enabled
             ORDER BY c.created_at"
        ))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn reconcilable_configs(&self) -> Result<Vec<PoolConfig>> {
        let rows = sqlx::query_as::<_, PoolConfigRow>(sql!(
            "SELECT c.id, c.template_release_id, r.template_name, r.hash AS content_hash,
                    c.vcpu, c.memory_mib, c.disk_mib, c.disk_io_max_operations_per_sec,
                    c.disk_io_max_bandwidth_mib_per_sec, c.target_ready_slots, c.refill_per_tick,
                    c.max_warming_slots, c.namespace, c.enabled, c.encryptable,
                    c.managed_by, c.created_at, c.updated_at
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             WHERE c.enabled
                OR EXISTS (
                    SELECT 1
                    FROM kubernetes_pool_slots s
                    WHERE s.config_id = c.id
                      AND s.state IN ('warming', 'ready')
                )
             ORDER BY c.created_at"
        ))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Disabled configs whose retirement state still needs publication.
    ///
    /// The controller emits explicit zero-valued active/success state during
    /// this grace period. That bounded tombstone prevents a stale per-config
    /// metric group from looking active after a manual disable, including when
    /// an already-empty config no longer qualifies for normal reconciliation.
    pub async fn recently_disabled_configs(&self, grace: Duration) -> Result<Vec<PoolConfig>> {
        let grace_secs = i64::try_from(grace.as_secs()).unwrap_or(i64::MAX);
        let rows = sqlx::query_as::<_, PoolConfigRow>(sql!(
            "SELECT c.id, c.template_release_id, r.template_name, r.hash AS content_hash,
                    c.vcpu, c.memory_mib, c.disk_mib, c.disk_io_max_operations_per_sec,
                    c.disk_io_max_bandwidth_mib_per_sec, c.target_ready_slots, c.refill_per_tick,
                    c.max_warming_slots, c.namespace, c.enabled, c.encryptable,
                    c.managed_by, c.created_at, c.updated_at
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             WHERE NOT c.enabled
               AND c.updated_at >= NOW() - ($1 * INTERVAL '1 second')
             ORDER BY c.created_at"
        ))
        .bind(grace_secs)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_configs_with_counts(&self) -> Result<Vec<PoolConfigWithCounts>> {
        let rows = sqlx::query_as::<_, PoolConfigWithCounts>(sql!(
            "SELECT c.id, c.template_release_id, r.template_name, r.hash AS content_hash,
                    c.vcpu, c.memory_mib, c.disk_mib, c.disk_io_max_operations_per_sec,
                    c.disk_io_max_bandwidth_mib_per_sec, c.target_ready_slots, c.refill_per_tick,
                    c.max_warming_slots, c.namespace, c.enabled, c.encryptable,
                    c.managed_by, c.created_at, c.updated_at,
                    COUNT(s.id) FILTER (WHERE s.state = 'warming') AS warming_slots,
                    COUNT(s.id) FILTER (WHERE s.state = 'ready') AS ready_slots,
                    COUNT(s.id) FILTER (WHERE s.state IN ('claimed', 'bound')) AS claimed_slots
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             LEFT JOIN kubernetes_pool_slots s ON s.config_id = c.id
             GROUP BY c.id, r.template_name, r.hash
             ORDER BY r.template_name, c.created_at"
        ))
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn config_with_counts(&self, id: Uuid) -> Result<Option<PoolConfigWithCounts>> {
        let row = sqlx::query_as::<_, PoolConfigWithCounts>(sql!(
            "SELECT c.id, c.template_release_id, r.template_name, r.hash AS content_hash,
                    c.vcpu, c.memory_mib, c.disk_mib, c.disk_io_max_operations_per_sec,
                    c.disk_io_max_bandwidth_mib_per_sec, c.target_ready_slots, c.refill_per_tick,
                    c.max_warming_slots, c.namespace, c.enabled, c.encryptable,
                    c.managed_by, c.created_at, c.updated_at,
                    COUNT(s.id) FILTER (WHERE s.state = 'warming') AS warming_slots,
                    COUNT(s.id) FILTER (WHERE s.state = 'ready') AS ready_slots,
                    COUNT(s.id) FILTER (WHERE s.state IN ('claimed', 'bound')) AS claimed_slots
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             LEFT JOIN kubernetes_pool_slots s ON s.config_id = c.id
             WHERE c.id = $1
             GROUP BY c.id, r.template_name, r.hash"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    pub async fn release_configs_with_counts(
        &self,
        release_ids: &[Uuid],
    ) -> Result<Vec<ReleasePoolConfigWithCounts>> {
        let rows = sqlx::query_as::<_, ReleasePoolConfigWithCounts>(sql!(
            "SELECT c.template_release_id, c.id, c.vcpu, c.memory_mib, c.disk_mib,
                    c.disk_io_max_operations_per_sec, c.disk_io_max_bandwidth_mib_per_sec,
                    c.target_ready_slots, c.refill_per_tick, c.max_warming_slots,
                    c.namespace, c.enabled, c.encryptable, c.managed_by,
                    COUNT(s.id) FILTER (WHERE s.state = 'warming') AS warming_slots,
                    COUNT(s.id) FILTER (WHERE s.state = 'ready') AS ready_slots,
                    COUNT(s.id) FILTER (WHERE s.state IN ('claimed', 'bound')) AS claimed_slots
             FROM kubernetes_pool_configs c
             LEFT JOIN kubernetes_pool_slots s ON s.config_id = c.id
             WHERE c.template_release_id = ANY($1)
             GROUP BY c.id
             ORDER BY c.created_at"
        ))
        .bind(release_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Create or update the operator-owned pool config for the given release
    /// and shape, taking `managed_by` ownership away from the autoscaler.
    /// Runs inside the caller's transaction: the caller resolves the
    /// template release (template-domain logic that lives with the API) and
    /// both commit atomically.
    pub async fn upsert_manual_config(
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
        spec: &ManualPoolConfigSpec<'_>,
    ) -> Result<Uuid> {
        // Fenced in the store, not just the API: any persisted encryptable
        // row aborts the autoscaler's partition-scoped ON CONFLICT while the
        // legacy 3-column index is live. Lifted with the index-drop stack.
        if spec.encryptable {
            return Err(PoolError::EncryptableConfigFenced);
        }
        let existing = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
             FROM kubernetes_pool_configs
             WHERE template_release_id = $1
               AND vcpu = $2
               AND memory_mib = $3
               AND encryptable = $4
             ORDER BY created_at
             LIMIT 1"
        ))
        .bind(template_release_id)
        .bind(spec.vcpu)
        .bind(spec.memory_mib)
        .bind(spec.encryptable)
        .fetch_optional(&mut **tx)
        .await?;

        let id = if let Some(existing) = existing {
            sqlx::query(sql!(
                "UPDATE kubernetes_pool_configs
                 SET disk_mib = CASE WHEN $2 THEN NULL ELSE COALESCE($3, disk_mib) END,
                     target_ready_slots = $4,
                     refill_per_tick = $5,
                     max_warming_slots = $6,
                     namespace = $7,
                     enabled = $8,
                     managed_by = $9,
                     updated_at = NOW()
                 WHERE id = $1"
            ))
            .bind(existing)
            .bind(spec.reset_disk_to_template_default)
            .bind(spec.disk_mib)
            .bind(spec.target_ready_slots)
            .bind(spec.refill_per_tick)
            .bind(spec.max_warming_slots)
            .bind(spec.namespace)
            .bind(spec.enabled)
            .bind(PoolConfigOwner::Manual.as_str())
            .execute(&mut **tx)
            .await?;
            existing
        } else {
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO kubernetes_pool_configs (
                     template_release_id, vcpu, memory_mib, disk_mib, encryptable,
                     target_ready_slots, refill_per_tick, max_warming_slots,
                     namespace, enabled, managed_by,
                     disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                 RETURNING id"
            ))
            .bind(template_release_id)
            .bind(spec.vcpu)
            .bind(spec.memory_mib)
            .bind(spec.disk_mib)
            .bind(spec.encryptable)
            .bind(spec.target_ready_slots)
            .bind(spec.refill_per_tick)
            .bind(spec.max_warming_slots)
            .bind(spec.namespace)
            .bind(spec.enabled)
            .bind(PoolConfigOwner::Manual.as_str())
            .bind(
                i64::try_from(DEFAULT_DISK_IO_MAX_OPERATIONS_PER_SEC)
                    .expect("default disk IOPS fits in PostgreSQL BIGINT"),
            )
            .bind(
                i64::try_from(DEFAULT_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC)
                    .expect("default disk bandwidth fits in PostgreSQL BIGINT"),
            )
            .fetch_one(&mut **tx)
            .await?
        };

        Ok(id)
    }

    /// Disable a pool config and zero its target so the reconciler drains
    /// its open slots. `managed_by` is deliberately untouched: a disabled
    /// autoscaler-owned config stays autoscaler-owned. Returns `false` when
    /// no config has this id.
    pub async fn disable_config(&self, id: Uuid) -> Result<bool> {
        let result = sqlx::query(sql!(
            "UPDATE kubernetes_pool_configs
             SET enabled = FALSE,
                 target_ready_slots = 0,
                 updated_at = NOW()
             WHERE id = $1"
        ))
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Reconcile policy-owned release pools from retained-release floors and
    /// bounded sustained demand.
    ///
    /// Returns configs deleted after they finished draining so callers can
    /// publish a final inactive state from the returned identity metadata.
    ///
    /// This sequence intentionally is not transactional: it relies on
    /// `space-cluster-controller` running as a singleton `Recreate` deployment.
    /// Add serialization or a transaction before calling it concurrently.
    pub async fn autoscale_configs(&self, namespace: &str) -> Result<Vec<PoolConfig>> {
        let candidates = self.autoscale_candidates().await?;

        for candidate in &candidates {
            self.upsert_autoscale_config(candidate, namespace).await?;
        }

        self.disable_stale_autoscale_configs(&candidates).await?;
        let retired_configs = self.delete_empty_disabled_autoscale_configs().await?;

        Ok(retired_configs)
    }

    async fn autoscale_candidates(&self) -> Result<Vec<PoolAutoscaleCandidate>> {
        let demand = self.observed_demand().await?;
        let demanded_release_ids = demand
            .iter()
            .filter(|observed| observed.observed_sandboxes >= AUTOSCALE_MIN_DEMAND_SANDBOXES)
            .map(|observed| observed.template_release_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let releases = self.policy_releases(&demanded_release_ids).await?;

        Ok(select_autoscale_candidates(releases, demand))
    }

    async fn policy_releases(
        &self,
        demanded_release_ids: &[Uuid],
    ) -> Result<Vec<PoolPolicyRelease>> {
        let mut releases = self.retained_policy_releases().await?;
        let retained = releases
            .iter()
            .map(|release| (release.template_release_id, release.vcpu, release.memory))
            .collect::<HashSet<_>>();
        releases.extend(
            self.demanded_policy_releases(demanded_release_ids)
                .await?
                .into_iter()
                .filter(|release| {
                    !retained.contains(&(release.template_release_id, release.vcpu, release.memory))
                }),
        );

        Ok(releases)
    }

    /// Releases whose exact artifacts must remain available for pool
    /// convergence. The retained-policy selection is shared with autoscaling
    /// so artifact GC cannot drift to a different proactive release window.
    pub async fn protected_template_releases(&self) -> Result<Vec<ProtectedTemplateRelease>> {
        let enabled = sqlx::query_as::<_, (String, String)>(sql!(
            "SELECT r.template_name, r.hash
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             WHERE c.enabled"
        ))
        .fetch_all(&self.pool)
        .await?;
        let retained = self.retained_policy_releases().await?;

        Ok(enabled
            .into_iter()
            .map(|(template_name, content_hash)| ProtectedTemplateRelease {
                template_name,
                content_hash,
            })
            .chain(
                retained
                    .into_iter()
                    .map(|release| ProtectedTemplateRelease {
                        template_name: release.template_name.to_string(),
                        content_hash: release.content_hash,
                    }),
            )
            .collect::<HashSet<_>>()
            .into_iter()
            .collect())
    }

    async fn retained_policy_releases(&self) -> Result<Vec<PoolPolicyRelease>> {
        let rows = sqlx::query_as::<_, PoolPolicyReleaseRow>(sql!(
            "SELECT
                 retained.id AS template_release_id,
                 p.template_name,
                 retained.hash AS content_hash,
                 p.vcpu,
                 p.memory_mib,
                 p.disk_mib,
                 COALESCE(p.disk_io_max_operations_per_sec, $1)
                     AS disk_io_max_operations_per_sec,
                 COALESCE(p.disk_io_max_bandwidth_mib_per_sec, $2)
                     AS disk_io_max_bandwidth_mib_per_sec,
                 p.min_ready_slots,
                 p.max_ready_slots,
                 p.refill_per_tick,
                 p.max_warming_slots,
                 p.retain_releases,
                 TRUE AS retained
             FROM warm_pool_policies p
             JOIN LATERAL (
                 SELECT release.id,
                      release.hash,
                      COALESCE(
                          (
                              SELECT MAX(published.modified_at)
                              FROM template_release_tags published
                              WHERE published.template_release_id = release.id
                          ),
                          release.created_at
                      ) AS published_at
                  FROM template_releases release
                  WHERE release.template_name = p.template_name
                    AND release.type = p.runtime
                  ORDER BY published_at DESC, release.id DESC
                  LIMIT p.retain_releases
             ) retained ON TRUE
             WHERE p.enabled
             ORDER BY p.template_name, p.vcpu, p.memory_mib,
                      retained.published_at DESC, retained.id DESC"
        ))
        .bind(
            i64::try_from(DEFAULT_DISK_IO_MAX_OPERATIONS_PER_SEC)
                .expect("disk I/O default fits in PostgreSQL BIGINT"),
        )
        .bind(
            i64::try_from(DEFAULT_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC)
                .expect("disk I/O default fits in PostgreSQL BIGINT"),
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn demanded_policy_releases(
        &self,
        demanded_release_ids: &[Uuid],
    ) -> Result<Vec<PoolPolicyRelease>> {
        let rows = sqlx::query_as::<_, PoolPolicyReleaseRow>(sql!(
            "SELECT
                 demanded.id AS template_release_id,
                 p.template_name,
                 demanded.hash AS content_hash,
                 p.vcpu,
                 p.memory_mib,
                 p.disk_mib,
                 COALESCE(p.disk_io_max_operations_per_sec, $2)
                     AS disk_io_max_operations_per_sec,
                 COALESCE(p.disk_io_max_bandwidth_mib_per_sec, $3)
                     AS disk_io_max_bandwidth_mib_per_sec,
                 p.min_ready_slots,
                 p.max_ready_slots,
                 p.refill_per_tick,
                 p.max_warming_slots,
                 p.retain_releases,
                 FALSE AS retained
             FROM warm_pool_policies p
             JOIN LATERAL (
                 SELECT release.id,
                        release.hash,
                        COALESCE(
                            (
                                SELECT MAX(published.modified_at)
                                FROM template_release_tags published
                                WHERE published.template_release_id = release.id
                            ),
                            release.created_at
                        ) AS published_at
                 FROM template_releases release
                 WHERE release.template_name = p.template_name
                   AND release.type = p.runtime
                   AND release.id = ANY($1::UUID[])
             ) demanded ON TRUE
             WHERE p.enabled
             ORDER BY p.template_name, p.vcpu, p.memory_mib,
                      demanded.published_at DESC, demanded.id DESC"
        ))
        .bind(demanded_release_ids)
        .bind(
            i64::try_from(DEFAULT_DISK_IO_MAX_OPERATIONS_PER_SEC)
                .expect("disk I/O default fits in PostgreSQL BIGINT"),
        )
        .bind(
            i64::try_from(DEFAULT_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC)
                .expect("disk I/O default fits in PostgreSQL BIGINT"),
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn observed_demand(&self) -> Result<Vec<PoolObservedDemand>> {
        let lookback_secs =
            i64::try_from(AUTOSCALE_LOOKBACK.as_secs()).expect("autoscale lookback fits i64");
        let rows = sqlx::query_as::<_, PoolObservedDemandRow>(sql!(
            // Aggregate BEFORE the release lateral. The lateral's inputs
            // (template_id, content_hash and its bare form) are constant
            // within a demand group, so resolving the release per distinct
            // group instead of per sandbox row cuts the probe count from one
            // per recent create to one per distinct (template, hash, shape)
            // combination. Groups whose hash resolves to no release drop
            // uniformly, and distinct hashes resolving to the same release
            // re-merge in the outer GROUP BY — output is identical to the
            // per-row form. Runs per tick by design: this pass also
            // re-points pools at freshly published releases, so its
            // frequency is deliberately untouched.
            "WITH recent_sandboxes AS (
                 SELECT
                     template_id,
                     content_hash,
                     CASE
                         WHEN content_hash LIKE 'sha256:%' THEN SUBSTRING(content_hash FROM 8)
                         ELSE content_hash
                     END AS bare_content_hash,
                     COALESCE(vcpu, $2)::INT AS vcpu,
                     COALESCE(memory_mib, $3)::INT AS memory_mib,
                     COUNT(*)::BIGINT AS observed_sandboxes
                 FROM sandboxes
                 WHERE created_at >= NOW() - ($1 * INTERVAL '1 second')
                   AND template_id IS NOT NULL
                   AND content_hash IS NOT NULL
                 GROUP BY 1, 2, 3, 4, 5
             )
             SELECT
                 release_by_hash.id AS template_release_id,
                 s.vcpu,
                 s.memory_mib,
                 SUM(s.observed_sandboxes)::BIGINT AS observed_sandboxes
             FROM recent_sandboxes s
             JOIN LATERAL (
                 SELECT r.id
                 FROM template_releases r
                 WHERE r.template_name = s.template_id
                   AND (
                       r.hash = s.content_hash
                       OR r.hash = s.bare_content_hash
                       OR r.hash = 'sha256:' || s.bare_content_hash
                   )
                 ORDER BY (r.hash = s.content_hash) DESC, r.created_at DESC, r.id DESC
                 LIMIT 1
             ) release_by_hash ON TRUE
             GROUP BY 1, 2, 3"
        ))
        .bind(lookback_secs)
        .bind(AUTOSCALE_DEFAULT_VCPU)
        .bind(AUTOSCALE_DEFAULT_MEMORY_MIB)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn upsert_autoscale_config(
        &self,
        candidate: &PoolAutoscaleCandidate,
        namespace: &str,
    ) -> Result<()> {
        // The autoscaler owns only plaintext pools: encryptable = FALSE,
        // upserting within that partition only.
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_configs (
                 template_release_id, vcpu, memory_mib, disk_mib, encryptable,
                 disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec,
                 target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled, managed_by
             )
             VALUES ($1, $2, $3, $4, FALSE, $5, $6, $7, $8, $9, $10, TRUE, 'autoscaler')
             ON CONFLICT (template_release_id, vcpu, memory_mib, encryptable) DO UPDATE
             SET target_ready_slots = EXCLUDED.target_ready_slots,
                 disk_mib = EXCLUDED.disk_mib,
                 disk_io_max_operations_per_sec = EXCLUDED.disk_io_max_operations_per_sec,
                 disk_io_max_bandwidth_mib_per_sec = EXCLUDED.disk_io_max_bandwidth_mib_per_sec,
                 refill_per_tick = EXCLUDED.refill_per_tick,
                 max_warming_slots = EXCLUDED.max_warming_slots,
                 namespace = EXCLUDED.namespace,
                 -- A desired release may re-enter after its prior pool began draining.
                 enabled = TRUE,
                 updated_at = NOW()
             WHERE kubernetes_pool_configs.managed_by = 'autoscaler'
             "
        ))
        .bind(candidate.template_release_id)
        .bind(vcpu_to_i32(candidate.vcpu))
        .bind(memory_to_mib_i32(candidate.memory))
        .bind(
            candidate
                .disk_mib
                .map(i32::try_from)
                .transpose()
                .map_err(|_| PoolError::InvalidResource {
                    field: "disk_mib",
                    value: i32::MAX,
                })?,
        )
        .bind(
            i64::try_from(candidate.disk_io_limits.max_operations_per_sec()).map_err(|_| {
                PoolError::InvalidDiskIo {
                    field: "disk_io_max_operations_per_sec",
                    value: i64::MAX,
                }
            })?,
        )
        .bind(
            i64::try_from(candidate.disk_io_limits.max_bandwidth_mib_per_sec()).map_err(|_| {
                PoolError::InvalidDiskIo {
                    field: "disk_io_max_bandwidth_mib_per_sec",
                    value: i64::MAX,
                }
            })?,
        )
        .bind(candidate.target_ready_slots)
        .bind(candidate.refill_per_tick)
        .bind(candidate.max_warming_slots)
        .bind(namespace)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn disable_stale_autoscale_configs(
        &self,
        candidates: &[PoolAutoscaleCandidate],
    ) -> Result<()> {
        let release_ids = candidates
            .iter()
            .map(|candidate| candidate.template_release_id)
            .collect::<Vec<_>>();
        let vcpus = candidates
            .iter()
            .map(|candidate| vcpu_to_i32(candidate.vcpu))
            .collect::<Vec<_>>();
        let memory_mib = candidates
            .iter()
            .map(|candidate| memory_to_mib_i32(candidate.memory))
            .collect::<Vec<_>>();
        sqlx::query(sql!(
            "UPDATE kubernetes_pool_configs c
             SET enabled = FALSE,
                 target_ready_slots = 0,
                 updated_at = NOW()
             -- An empty desired set intentionally drains every autoscaler-owned config.
             WHERE c.managed_by = 'autoscaler'
               AND (c.enabled OR c.target_ready_slots != 0)
               AND NOT EXISTS (
                   SELECT 1
                   FROM unnest($1::UUID[], $2::INT[], $3::INT[])
                        AS desired(template_release_id, vcpu, memory_mib)
                   WHERE desired.template_release_id = c.template_release_id
                     AND desired.vcpu = c.vcpu
                     AND desired.memory_mib = c.memory_mib
               )"
        ))
        .bind(release_ids)
        .bind(vcpus)
        .bind(memory_mib)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete_empty_disabled_autoscale_configs(&self) -> Result<Vec<PoolConfig>> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query_as::<_, PoolConfigRow>(sql!(
            "SELECT c.id, c.template_release_id, r.template_name, r.hash AS content_hash,
                    c.vcpu, c.memory_mib, c.disk_mib, c.disk_io_max_operations_per_sec,
                    c.disk_io_max_bandwidth_mib_per_sec, c.target_ready_slots, c.refill_per_tick,
                    c.max_warming_slots, c.namespace, c.enabled, c.encryptable,
                    c.managed_by, c.created_at, c.updated_at
             FROM kubernetes_pool_configs c
             JOIN template_releases r ON r.id = c.template_release_id
             WHERE c.managed_by = 'autoscaler'
               AND NOT c.enabled
               AND NOT EXISTS (
                   SELECT 1
                   FROM kubernetes_pool_slots s
                   WHERE s.config_id = c.id
               )
             FOR UPDATE OF c"
        ))
        .fetch_all(&mut *tx)
        .await?;
        let configs = rows
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<PoolConfig>>>()?;
        let config_ids = configs.iter().map(|config| config.id).collect::<Vec<_>>();
        sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_configs
             WHERE id = ANY($1::UUID[])"
        ))
        .bind(&config_ids)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(configs)
    }

    pub async fn slot_counts(&self, config_id: Uuid) -> Result<PoolSlotCounts> {
        let counts = sqlx::query_as::<_, PoolSlotCountsRow>(sql!(
            "SELECT
                 COUNT(*) FILTER (WHERE state = 'warming') AS warming,
                 COUNT(*) FILTER (WHERE state = 'ready') AS ready
             FROM kubernetes_pool_slots
             WHERE config_id = $1
               AND state IN ('warming', 'ready')"
        ))
        .bind(config_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(counts.into())
    }

    pub async fn warming_slots(&self, config_id: Uuid) -> Result<Vec<PoolSlot>> {
        let rows = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "SELECT id, config_id, template_release_id, reserved_sandbox_id, template_name,
                    content_hash, vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                    disk_io_max_bandwidth_mib_per_sec, pod_name, pod_namespace, pod_uid,
                    host(pod_ip) AS pod_ip, k8s_node_name, state, claim_id, ready_at,
                    claimed_at, created_at, updated_at
             FROM kubernetes_pool_slots
             WHERE config_id = $1
               AND state = 'warming'
             ORDER BY created_at"
        ))
        .bind(config_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn open_slots(&self, config_id: Uuid) -> Result<Vec<PoolSlot>> {
        let rows = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "SELECT id, config_id, template_release_id, reserved_sandbox_id, template_name,
                    content_hash, vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                    disk_io_max_bandwidth_mib_per_sec, pod_name, pod_namespace, pod_uid,
                    host(pod_ip) AS pod_ip, k8s_node_name, state, claim_id, ready_at,
                    claimed_at, created_at, updated_at
             FROM kubernetes_pool_slots
             WHERE config_id = $1
               AND state IN ('warming', 'ready')
             ORDER BY ready_at NULLS LAST, created_at"
        ))
        .bind(config_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn insert_warming_slot(&self, slot: NewPoolSlot<'_>) -> Result<PoolSlot> {
        let slot_id = Uuid::now_v7();
        let row = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "INSERT INTO kubernetes_pool_slots (
                 id, config_id, template_release_id, reserved_sandbox_id, template_name,
                 content_hash,
                 vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                 disk_io_max_bandwidth_mib_per_sec, pod_name, pod_namespace, state
             )
             SELECT $1, $2, $3, $4, $5, $6, $7, $8, c.disk_mib,
                     c.disk_io_max_operations_per_sec,
                     c.disk_io_max_bandwidth_mib_per_sec,
                     $9, $10, 'warming'
             FROM kubernetes_pool_configs c
             WHERE c.id = $2
             RETURNING id, config_id, template_release_id, reserved_sandbox_id, template_name,
                       content_hash, vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                       disk_io_max_bandwidth_mib_per_sec, pod_name, pod_namespace, pod_uid,
                       host(pod_ip) AS pod_ip, k8s_node_name, state, claim_id, ready_at,
                       claimed_at, created_at, updated_at"
        ))
        .bind(slot_id)
        .bind(slot.config_id)
        .bind(slot.template_release_id)
        .bind(slot.reserved_sandbox_id)
        .bind(slot.template_name.as_str())
        .bind(slot.content_hash.to_string())
        .bind(vcpu_to_i32(slot.vcpu))
        .bind(memory_to_mib_i32(slot.memory))
        .bind(slot.pod_name)
        .bind(slot.pod_namespace)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn mark_slot_ready(
        &self,
        slot_id: Uuid,
        pod_uid: &str,
        pod_ip: &str,
        k8s_node_name: &str,
    ) -> Result<PoolSlot> {
        let ready_state = PoolSlotState::Warming.mark_ready()?;
        let row = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "UPDATE kubernetes_pool_slots
             SET state = $2,
                 pod_uid = $3,
                 pod_ip = $4::inet,
                 k8s_node_name = $5,
                 ready_at = COALESCE(ready_at, NOW()),
                 updated_at = NOW()
             WHERE id = $1
               AND state = 'warming'
             RETURNING id, config_id, template_release_id, reserved_sandbox_id, template_name,
                       content_hash, vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                       disk_io_max_bandwidth_mib_per_sec, pod_name, pod_namespace, pod_uid,
                       host(pod_ip) AS pod_ip, k8s_node_name, state, claim_id, ready_at,
                       claimed_at, created_at, updated_at"
        ))
        .bind(slot_id)
        .bind(ready_state.as_str())
        .bind(pod_uid)
        .bind(pod_ip)
        .bind(k8s_node_name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PoolError::SlotNotWarming { slot_id })?;

        row.try_into()
    }

    pub async fn delete_open_slot(&self, slot_id: Uuid) -> Result<bool> {
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE id = $1
               AND state IN ('warming', 'ready')"
        ))
        .bind(slot_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn lock_open_slot_for_delete(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
    ) -> Result<bool> {
        let row = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
             FROM kubernetes_pool_slots
             WHERE id = $1
               AND state IN ('warming', 'ready')
             FOR UPDATE SKIP LOCKED"
        ))
        .bind(slot_id)
        .fetch_optional(&mut **tx)
        .await?;

        Ok(row.is_some())
    }

    pub async fn lock_open_slot_for_delete_by_reserved_sandbox_id(
        tx: &mut Transaction<'_, Postgres>,
        reserved_sandbox_id: SandboxId,
    ) -> Result<OpenPoolSlotDeleteLock> {
        let row = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "SELECT id, config_id, template_release_id, reserved_sandbox_id, template_name,
                    content_hash, vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                    disk_io_max_bandwidth_mib_per_sec, pod_name, pod_namespace, pod_uid,
                    host(pod_ip) AS pod_ip, k8s_node_name, state, claim_id, ready_at,
                    claimed_at, created_at, updated_at
             FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = $1
               AND state IN ('warming', 'ready')
             FOR UPDATE SKIP LOCKED"
        ))
        .bind(reserved_sandbox_id)
        .fetch_optional(&mut **tx)
        .await?;

        if let Some(row) = row {
            return Ok(OpenPoolSlotDeleteLock::Locked(Box::new(row.try_into()?)));
        }

        let state = sqlx::query_scalar::<_, String>(sql!(
            "SELECT state
             FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = $1"
        ))
        .bind(reserved_sandbox_id)
        .fetch_optional(&mut **tx)
        .await?;

        match state {
            None => Ok(OpenPoolSlotDeleteLock::NotFound),
            Some(state) => {
                let state = state.parse::<PoolSlotState>()?;
                if matches!(state, PoolSlotState::Warming | PoolSlotState::Ready) {
                    Ok(OpenPoolSlotDeleteLock::Busy)
                } else {
                    Ok(OpenPoolSlotDeleteLock::NotOpen { state })
                }
            }
        }
    }

    pub async fn delete_locked_open_slot(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
    ) -> Result<bool> {
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE id = $1
               AND state IN ('warming', 'ready')"
        ))
        .bind(slot_id)
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn claim_ready_slot(
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
        vcpu: VcpuCount,
        memory: MemorySize,
        namespace: &str,
        claim_id: Uuid,
        encryptable: bool,
    ) -> Result<Option<ClaimedPoolSlot>> {
        let Some(slot) = sqlx::query_as::<_, ClaimedPoolSlotRow>(sql!(
            "SELECT s.id, s.reserved_sandbox_id, s.pod_name, s.pod_namespace, s.pod_uid,
                    host(s.pod_ip) AS pod_ip, s.k8s_node_name, s.vcpu, s.memory_mib, s.disk_mib,
                    s.disk_io_max_operations_per_sec, s.disk_io_max_bandwidth_mib_per_sec
             FROM kubernetes_pool_slots s
             JOIN kubernetes_pool_configs c ON c.id = s.config_id
             WHERE s.state = 'ready'
               AND c.enabled
               AND c.encryptable = $5
               AND s.template_release_id = $1
               AND s.vcpu = $2
               AND s.memory_mib = $3
               AND s.disk_mib IS NOT DISTINCT FROM c.disk_mib
               AND (s.disk_mib IS NULL OR s.disk_mib BETWEEN 1 AND $6)
               AND s.disk_io_max_operations_per_sec = c.disk_io_max_operations_per_sec
               AND s.disk_io_max_bandwidth_mib_per_sec = c.disk_io_max_bandwidth_mib_per_sec
               AND s.pod_ip IS NOT NULL
               AND s.pod_namespace = $4
             ORDER BY s.ready_at ASC
             FOR UPDATE OF s SKIP LOCKED
             LIMIT 1"
        ))
        .bind(template_release_id)
        .bind(vcpu_to_i32(vcpu))
        .bind(memory_to_mib_i32(memory))
        .bind(namespace)
        .bind(encryptable)
        .bind(MAX_ROOTFS_MIB)
        .fetch_optional(&mut **tx)
        .await?
        else {
            return Ok(None);
        };

        Self::mark_slot_claimed(tx, slot.id, claim_id).await?;

        Ok(Some(slot.try_into()?))
    }

    /// Claim the oldest ready slot across every enabled shape for a release.
    /// Default-shape creates use this to preserve their any-shape acquisition
    /// behavior; callers report the returned slot's exact shape on a hit.
    /// Oversized historical slots remain visible to operators but cannot acquire
    /// a new owner. Filter before LIMIT so they cannot starve compatible siblings.
    pub async fn claim_ready_slot_for_release(
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
        namespace: &str,
        claim_id: Uuid,
        encryptable: bool,
    ) -> Result<Option<ClaimedPoolSlot>> {
        let Some(slot) = sqlx::query_as::<_, ClaimedPoolSlotRow>(sql!(
            "SELECT s.id, s.reserved_sandbox_id, s.pod_name, s.pod_namespace, s.pod_uid,
                    host(s.pod_ip) AS pod_ip, s.k8s_node_name, s.vcpu, s.memory_mib, s.disk_mib,
                    s.disk_io_max_operations_per_sec, s.disk_io_max_bandwidth_mib_per_sec
             FROM kubernetes_pool_slots s
             JOIN kubernetes_pool_configs c ON c.id = s.config_id
             WHERE s.state = 'ready'
               AND c.enabled
               AND c.encryptable = $3
               AND s.template_release_id = $1
               AND s.disk_mib IS NOT DISTINCT FROM c.disk_mib
               AND (s.disk_mib IS NULL OR s.disk_mib BETWEEN 1 AND $4)
               AND s.disk_io_max_operations_per_sec = c.disk_io_max_operations_per_sec
               AND s.disk_io_max_bandwidth_mib_per_sec = c.disk_io_max_bandwidth_mib_per_sec
               AND s.pod_ip IS NOT NULL
               AND s.pod_namespace = $2
             ORDER BY s.ready_at ASC
             FOR UPDATE OF s SKIP LOCKED
             LIMIT 1"
        ))
        .bind(template_release_id)
        .bind(namespace)
        .bind(encryptable)
        .bind(MAX_ROOTFS_MIB)
        .fetch_optional(&mut **tx)
        .await?
        else {
            return Ok(None);
        };

        Self::mark_slot_claimed(tx, slot.id, claim_id).await?;

        Ok(Some(slot.try_into()?))
    }

    /// Resolve the release-and-shape pool represented by a claim miss. An
    /// explicit request resolves only its exact enabled config; a default
    /// request resolves the preferred configured pool after the any-shape
    /// ready-slot claim path has exhausted available warm capacity.
    pub async fn preferred_enabled_shape_for_release(
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
        namespace: &str,
        requested_shape: Option<(VcpuCount, MemorySize)>,
        encryptable: bool,
    ) -> Result<Option<crate::PoolAssignment>> {
        if let Some((vcpu, memory)) = requested_shape {
            let row = sqlx::query_as::<_, (Option<i32>, i64, i64)>(sql!(
                "SELECT disk_mib, disk_io_max_operations_per_sec,
                        disk_io_max_bandwidth_mib_per_sec
                 FROM kubernetes_pool_configs
                 WHERE template_release_id = $1
                   AND namespace = $2
                   AND enabled
                   AND encryptable = $5
                   AND vcpu = $3
                   AND memory_mib = $4
                 LIMIT 1"
            ))
            .bind(template_release_id)
            .bind(namespace)
            .bind(vcpu_to_i32(vcpu))
            .bind(memory_to_mib_i32(memory))
            .bind(encryptable)
            .fetch_optional(&mut **tx)
            .await?;
            return row
                .map(|(disk_mib, operations, bandwidth)| {
                    Ok(crate::PoolAssignment {
                        vcpu,
                        memory,
                        disk_mib: crate::models::parse_disk_mib(disk_mib)?,
                        disk_io_limits: crate::models::parse_disk_io_limits(operations, bandwidth)?,
                    })
                })
                .transpose();
        }

        let row = sqlx::query_as::<_, (i32, i32, Option<i32>, i64, i64)>(sql!(
            "SELECT vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                    disk_io_max_bandwidth_mib_per_sec
             FROM kubernetes_pool_configs
             WHERE template_release_id = $1
               AND namespace = $2
               AND enabled
               AND encryptable = $3
             ORDER BY target_ready_slots DESC, updated_at DESC, id DESC
             LIMIT 1"
        ))
        .bind(template_release_id)
        .bind(namespace)
        .bind(encryptable)
        .fetch_optional(&mut **tx)
        .await?;

        row.map(pool_assignment_from_shape_row).transpose()
    }

    /// Preferred enabled plaintext pool assignment for a release, the
    /// shape/disk-limit source for cold-fallback resource resolution.
    /// Encryptable twins are excluded so they can never stamp their shape
    /// onto plaintext creates.
    pub async fn preferred_enabled_plaintext_assignment(
        &self,
        template_release_id: Uuid,
    ) -> Result<Option<crate::PoolAssignment>> {
        let row = sqlx::query_as::<_, (i32, i32, Option<i32>, i64, i64)>(sql!(
            "SELECT vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                    disk_io_max_bandwidth_mib_per_sec
             FROM kubernetes_pool_configs
             WHERE template_release_id = $1
               AND enabled
               AND NOT encryptable
             ORDER BY target_ready_slots DESC, updated_at DESC, id DESC
             LIMIT 1"
        ))
        .bind(template_release_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(pool_assignment_from_shape_row).transpose()
    }

    /// Preferred enabled Firecracker policy assignment for a template. This
    /// is the fallback shape for every cold create when no release-specific
    /// pool config has been materialized, including releases outside the
    /// policy's `retain_releases` window. Persisting the policy shape on the
    /// sandbox row lets that demand match the policy when the autoscaler
    /// decides whether to re-admit a pool for the release.
    ///
    /// Ordering mirrors zero-demand policy materialization: the policy with
    /// the most `min_ready_slots` wins, then the most recently updated one.
    pub async fn preferred_enabled_policy_assignment(
        &self,
        template_name: &str,
    ) -> Result<Option<crate::PoolAssignment>> {
        let row = sqlx::query_as::<_, (i32, i32, Option<i32>, i64, i64)>(sql!(
            "SELECT vcpu, memory_mib, disk_mib,
                    COALESCE(disk_io_max_operations_per_sec, $2),
                    COALESCE(disk_io_max_bandwidth_mib_per_sec, $3)
             FROM warm_pool_policies
             WHERE template_name = $1
               AND runtime = 'fc'
               AND enabled
             ORDER BY min_ready_slots DESC, updated_at DESC
             LIMIT 1"
        ))
        .bind(template_name)
        .bind(
            i64::try_from(DEFAULT_DISK_IO_MAX_OPERATIONS_PER_SEC)
                .expect("default disk IOPS fits in PostgreSQL BIGINT"),
        )
        .bind(
            i64::try_from(DEFAULT_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC)
                .expect("default disk bandwidth fits in PostgreSQL BIGINT"),
        )
        .fetch_optional(&self.pool)
        .await?;

        row.map(pool_assignment_from_shape_row).transpose()
    }

    async fn mark_slot_claimed(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
        claim_id: Uuid,
    ) -> Result<()> {
        let claimed_state = PoolSlotState::Ready.claim()?;
        sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
             SET state = $1, claim_id = $2, claimed_at = NOW(), updated_at = NOW()
             WHERE id = $3"
        ))
        .bind(claimed_state.as_str())
        .bind(claim_id)
        .bind(slot_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Stamp a claimed slot `bound` inside the caller's bind transaction —
    /// the transaction that copies the pod identity onto the sandbox's
    /// execution rows. Committing the stamp atomically with the bind is what
    /// lets the stale-claimed sweep treat any sufficiently old `claimed` row
    /// as a dead handoff. The claim-id and state guards make this a CAS:
    /// `false` means the slot was already reaped, invalidated, or re-claimed,
    /// and the caller must abort the bind.
    pub async fn bind_claimed_slot(
        tx: &mut Transaction<'_, Postgres>,
        claim_id: Uuid,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let bound_state = PoolSlotState::Claimed.bind()?;
        let result = sqlx::query(
            "UPDATE kubernetes_pool_slots
             SET state = $1,
                 updated_at = NOW()
             WHERE claim_id = $2
               AND reserved_sandbox_id = $3
               AND state = $4",
        )
        .bind(bound_state.as_str())
        .bind(claim_id)
        .bind(reserved_sandbox_id)
        .bind(PoolSlotState::Claimed.as_str())
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Read the claimed slot's pod identity for the bind transaction. This is
    /// a column read only: the bind's writes go to sandbox-owned tables and
    /// live with the API substrate that owns them, keeping each domain's
    /// tables written from one home. `None` means the claim vanished between
    /// the claim transaction and the bind (reaped or invalidated), and the
    /// caller must abort the bind. Runs after [`Self::bind_claimed_slot`],
    /// whose CAS already row-locks the slot; the `FOR UPDATE` here keeps the
    /// read self-sufficiently locked if callers ever reorder.
    pub async fn bind_source(
        tx: &mut Transaction<'_, Postgres>,
        claim_id: Uuid,
        reserved_sandbox_id: SandboxId,
    ) -> Result<Option<ClaimedSlotBindSource>> {
        let source = sqlx::query_as::<_, ClaimedSlotBindSource>(sql!(
            "SELECT pod_name, pod_namespace, NULLIF(BTRIM(pod_uid), '') AS pod_uid,
                    host(pod_ip) AS pod_ip, k8s_node_name
             FROM kubernetes_pool_slots
             WHERE claim_id = $1
               AND reserved_sandbox_id = $2
             FOR UPDATE"
        ))
        .bind(claim_id)
        .bind(reserved_sandbox_id)
        .fetch_optional(&mut **tx)
        .await?;

        Ok(source)
    }

    /// Lock and read the slot reserving `reserved_sandbox_id`, in any state.
    ///
    /// Used by a create that was admitted against a specific slot (the
    /// create's public ID is that slot's reserved ID). The caller adopts only
    /// a `claimed` slot whose claim id matches the one recorded at admission;
    /// every other answer tells it whether the pool still owns a pod under
    /// this ID. `reserved_sandbox_id` is unique, so this is at most one row.
    /// The `FOR UPDATE` lock (no `SKIP LOCKED`) waits out a concurrent sweep
    /// or bind instead of misreading a locked row as absent.
    pub async fn lock_reserved_slot(
        tx: &mut Transaction<'_, Postgres>,
        reserved_sandbox_id: SandboxId,
    ) -> Result<Option<ReservedPoolSlot>> {
        let row = sqlx::query_as::<_, ReservedPoolSlotRow>(sql!(
            "SELECT id, reserved_sandbox_id, vcpu, memory_mib, disk_mib,
                    disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec,
                    pod_name, pod_namespace, pod_uid, host(pod_ip) AS pod_ip, k8s_node_name,
                    template_release_id, content_hash, state, claim_id
             FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = $1
             FOR UPDATE"
        ))
        .bind(reserved_sandbox_id)
        .fetch_optional(&mut **tx)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    /// Take a `ready` slot out of the claimable inventory without handing it
    /// to anyone: it becomes a `claimed` row with no claim id, which no bind
    /// can match and the stale-claimed sweep collects (pod and row) once the
    /// claim timeout passes. Used when a create owns the slot's reserved ID
    /// but will not adopt the slot, so no other create may bind that ID.
    /// `false` means the slot was not `ready`.
    pub async fn fence_ready_slot(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
    ) -> Result<bool> {
        let claimed_state = PoolSlotState::Ready.claim()?;
        let result = sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
             SET state = $1, claim_id = NULL, claimed_at = NOW(), updated_at = NOW()
             WHERE id = $2
               AND state = $3"
        ))
        .bind(claimed_state.as_str())
        .bind(slot_id)
        .bind(PoolSlotState::Ready.as_str())
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Whether the slot reserving `reserved_sandbox_id` is a `cleanup_pending`
    /// work item, i.e. the warm-pool reconciler still owes that sandbox a pod
    /// delete and a sandbox finalize.
    pub async fn reserved_slot_cleanup_pending(
        &self,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let pending = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM kubernetes_pool_slots
                 WHERE reserved_sandbox_id = $1
                   AND state = $2
             )"
        ))
        .bind(reserved_sandbox_id)
        .bind(PoolSlotState::CleanupPending.as_str())
        .fetch_one(&self.pool)
        .await?;

        Ok(pending)
    }

    /// Which of the given sandbox ids are reserved by a pool slot, in any
    /// state. Deliberately state-free: a slot row's existence means the pool
    /// created that reserved sandbox id (or still holds its receipt), which
    /// is what drain-time published state needs to avoid misclassifying a
    /// pool-reserved id as deleted.
    pub async fn reserved_sandbox_ids(&self, sandbox_ids: &[SandboxId]) -> Result<Vec<SandboxId>> {
        let rows = sqlx::query_scalar::<_, SandboxId>(sql!(
            "SELECT reserved_sandbox_id
             FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = ANY($1)"
        ))
        .bind(sandbox_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Return an unbound claimed slot to the ready inventory. This is the
    /// pre-bind abandon path: the claimed pod failed validation before any
    /// sandbox rows referenced it, so the still-warm slot goes back to the
    /// pool. The claim-id and state guards make a lost race (slot already
    /// reaped or re-claimed) a no-op.
    pub async fn release_claimed_slot(
        &self,
        claim_id: Uuid,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let released_state = PoolSlotState::Claimed.release()?;
        let result = sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
             SET state = $1,
                 claim_id = NULL,
                 claimed_at = NULL,
                 updated_at = NOW()
             WHERE claim_id = $2
               AND reserved_sandbox_id = $3
               AND state = $4"
        ))
        .bind(released_state.as_str())
        .bind(claim_id)
        .bind(reserved_sandbox_id)
        .bind(PoolSlotState::Claimed.as_str())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Strip the claim id from a claimed slot without changing its state, so
    /// the slot can never satisfy another claim and the stale-claimed sweep
    /// collects its pod after the claim timeout. Used when the claimed pod
    /// is unusable (stale identity) or when the API-side bind transaction
    /// failed after the claim committed — cases where the pod's fitness is
    /// unknown, so it must not be handed to another caller.
    pub async fn invalidate_claimed_slot(
        &self,
        claim_id: Uuid,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let result = sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
             SET claim_id = NULL,
                 updated_at = NOW()
             WHERE claim_id = $1
               AND reserved_sandbox_id = $2
               AND state = $3"
        ))
        .bind(claim_id)
        .bind(reserved_sandbox_id)
        .bind(PoolSlotState::Claimed.as_str())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Transactional form of [`Self::invalidate_claimed_slot`], for a caller
    /// that already holds the slot's row lock.
    pub async fn invalidate_claimed_slot_in(
        tx: &mut Transaction<'_, Postgres>,
        claim_id: Uuid,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let result = sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
             SET claim_id = NULL,
                 updated_at = NOW()
             WHERE claim_id = $1
               AND reserved_sandbox_id = $2
               AND state = $3"
        ))
        .bind(claim_id)
        .bind(reserved_sandbox_id)
        .bind(PoolSlotState::Claimed.as_str())
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn stale_unbound_claimed_slots(
        &self,
        older_than: Duration,
        limit: i64,
    ) -> Result<Vec<PoolSlot>> {
        let older_than_secs = i64::try_from(older_than.as_secs()).unwrap_or(i64::MAX);
        let rows = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "SELECT s.id, s.config_id, s.template_release_id, s.reserved_sandbox_id,
                    s.template_name, s.content_hash, s.vcpu, s.memory_mib, s.disk_mib,
                    s.disk_io_max_operations_per_sec, s.disk_io_max_bandwidth_mib_per_sec,
                    s.pod_name,
                    s.pod_namespace, s.pod_uid, host(s.pod_ip) AS pod_ip, s.k8s_node_name,
                    s.state, s.claim_id, s.ready_at, s.claimed_at, s.created_at, s.updated_at
             FROM kubernetes_pool_slots s
             WHERE s.state = 'claimed'
               AND s.claimed_at IS NOT NULL
               AND s.claimed_at <= NOW() - ($1 * INTERVAL '1 second')
               AND NOT EXISTS (
                   SELECT 1
                   FROM sandboxes sb
                   WHERE sb.id = s.reserved_sandbox_id
               )
             ORDER BY s.claimed_at ASC
             LIMIT $2"
        ))
        .bind(older_than_secs)
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn cleanup_pending_slots(&self, limit: i64) -> Result<Vec<PoolSlot>> {
        let rows = sqlx::query_as::<_, PoolSlotRow>(sql!(
            "SELECT s.id, s.config_id, s.template_release_id, s.reserved_sandbox_id,
                    s.template_name, s.content_hash, s.vcpu, s.memory_mib, s.disk_mib,
                    s.disk_io_max_operations_per_sec, s.disk_io_max_bandwidth_mib_per_sec,
                    s.pod_name,
                    s.pod_namespace, s.pod_uid, host(s.pod_ip) AS pod_ip, s.k8s_node_name,
                    s.state, s.claim_id, s.ready_at, s.claimed_at, s.created_at, s.updated_at
             FROM kubernetes_pool_slots s
             WHERE s.state = 'cleanup_pending'
             ORDER BY s.updated_at ASC
             LIMIT $1"
        ))
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn mark_claimed_slot_cleanup_pending(
        tx: &mut Transaction<'_, Postgres>,
        claim_id: Uuid,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let cleanup_pending_state = PoolSlotState::Bound.cleanup_pending()?;
        let result = sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
             SET state = $1,
                 updated_at = NOW()
             WHERE claim_id = $2
               AND reserved_sandbox_id = $3
               AND state IN ($4, $5)"
        ))
        .bind(cleanup_pending_state.as_str())
        .bind(claim_id)
        .bind(reserved_sandbox_id)
        .bind(PoolSlotState::Bound.as_str())
        .bind(PoolSlotState::Claimed.as_str())
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Lock-free variant of [`Self::lock_stale_unbound_claimed_slot_for_delete`]:
    /// re-checks that the slot is still a stale, unbound `claimed` row without
    /// opening a transaction or taking a row lock. The stale-claimed reaper
    /// runs this immediately before the irreversible Kubernetes pod delete so
    /// that no transaction (row lock + pooled connection) is held across the
    /// external call; the locked variant then re-verifies inside the short
    /// bookkeeping transaction that removes the row.
    pub async fn stale_unbound_claimed_slot_exists(
        &self,
        slot_id: Uuid,
        older_than: Duration,
    ) -> Result<bool> {
        let older_than_secs = i64::try_from(older_than.as_secs()).unwrap_or(i64::MAX);
        let row = sqlx::query_scalar::<_, Uuid>(STALE_UNBOUND_CLAIMED_SLOT_BY_ID_SQL)
            .bind(slot_id)
            .bind(older_than_secs)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }

    /// Return the subset of `slot_ids` that still have a `kubernetes_pool_slots`
    /// row (in any state). The warm-pool orphan GC uses this to skip a pod whose
    /// slot the pool still tracks — such a pod is the desired→actual reconcile's
    /// (`observe_ready_slots` / `observe_warming_slots`) or the claimed-slot
    /// reapers' to delete under a row lock, never the orphan sweep's. Only pods
    /// whose slot row is *gone* are orphans the sweep owns. An empty input
    /// short-circuits without a query.
    pub async fn live_slot_ids(&self, slot_ids: &[Uuid]) -> Result<HashSet<Uuid>> {
        if slot_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let rows = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id FROM kubernetes_pool_slots WHERE id = ANY($1)"
        ))
        .bind(slot_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().collect())
    }

    /// Return the subset of the candidate `(sandbox_id, pod_name)` pairs that
    /// back a **live sandbox execution** — an `active` `sandbox_k8s_executions`
    /// row for the exact pair. The warm-pool orphan GC uses this as its
    /// load-bearing guard: a warm-pool pod keeps its `pool-role=warm` label
    /// after adoption and its slot row is deleted row-only, so "warm pod with no
    /// slot row" alone would match a live user sandbox — the active execution
    /// here is what excludes it.
    ///
    /// Keyed on the `active` flag, not mere execution existence: suspend, delete,
    /// and replace retire the execution (`active = FALSE`), so a suspended
    /// sandbox's leaked terminal pod is collectable rather than pinned live
    /// forever. `pod_name` is uniquely indexed on `sandbox_k8s_executions`, so
    /// the pair match identifies the exact live pod, and every wake-in-place
    /// state (`stopped`/`paused`) keeps its pod both non-terminal *and*
    /// `active = TRUE` (so it is invisible to the terminal-only GC and protected
    /// here), while suspend/delete/replace make a fresh generation on wake.
    ///
    /// `sandbox_ids` and `pod_names` are parallel slices (paired by index); the
    /// caller supplies both from the same candidate list. An empty input
    /// short-circuits without a query.
    pub async fn live_execution_pod_names(
        &self,
        sandbox_ids: &[SandboxId],
        pod_names: &[String],
    ) -> Result<HashSet<String>> {
        debug_assert_eq!(sandbox_ids.len(), pod_names.len());
        if pod_names.is_empty() {
            return Ok(HashSet::new());
        }
        let rows = sqlx::query_scalar::<_, String>(sql!(
            "SELECT e.pod_name
             FROM sandbox_k8s_executions e
             JOIN UNNEST($1::uuid[], $2::text[]) AS candidate(sandbox_id, pod_name)
               ON e.sandbox_id = candidate.sandbox_id
              AND e.pod_name = candidate.pod_name
             WHERE e.active"
        ))
        .bind(sandbox_ids)
        .bind(pod_names)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().collect())
    }

    /// Authoritative point-in-time recheck that a single orphan candidate is
    /// still unowned: `true` iff the pool tracks **no** `kubernetes_pool_slots`
    /// row for `slot_id` **and** there is **no** active `sandbox_k8s_executions`
    /// row for the exact `(sandbox_id, pod_name)` pair.
    ///
    /// The warm-pool orphan GC's batch scan ([`Self::live_slot_ids`] /
    /// [`Self::live_execution_pod_names`]) is a cheap pre-filter, but it
    /// snapshots ownership seconds before a candidate is acted on and reads the
    /// two tables at different moments. This is the destructive-action fence: it
    /// runs as a **single statement** (so slot-row absence and live-execution
    /// absence are observed at one instant, not two) immediately before the pod
    /// DELETE, and the caller fails closed unless it returns `true`. A
    /// claim/bind that established ownership after the scan is caught here
    /// rather than after the irreversible delete.
    pub async fn orphan_candidate_still_unowned(
        &self,
        slot_id: Uuid,
        sandbox_id: SandboxId,
        pod_name: &str,
    ) -> Result<bool> {
        let unowned = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT NOT EXISTS (SELECT 1 FROM kubernetes_pool_slots WHERE id = $1)
                AND NOT EXISTS (
                    SELECT 1 FROM sandbox_k8s_executions
                    WHERE sandbox_id = $2 AND pod_name = $3 AND active
                )"
        ))
        .bind(slot_id)
        .bind(sandbox_id)
        .bind(pod_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(unowned)
    }

    /// Delete the claimed slot whose pod's execution is being retired, inside
    /// the caller's teardown transaction.
    ///
    /// After a successful claim-bind the slot row is only a receipt: the pod
    /// identity was copied onto the sandbox's execution rows at bind time and
    /// pod teardown is owned by the sandbox lifecycle from then on. The
    /// sandbox lifecycle calls this from every transaction that retires the
    /// claimed pod — the pod-tracking clear on delete-shaped teardowns and
    /// the API-driven suspend finalization; the node-published suspend
    /// finalization retires whole batches through
    /// [`Self::delete_claimed_slots_for_retired_executions`]. Bound receipts
    /// are invisible to the
    /// stale-claimed sweep by state, so this inline delete (with the
    /// suspended-fossil drain as its backstop) is what keeps receipts from
    /// outliving their sandbox. The state guard is what lets this
    /// compose with the other cleanup paths: `warming`/`ready` slots still
    /// own their pod (deleting the row would leak it), and a
    /// `cleanup_pending` slot is a durable work item that still owes a pod
    /// kill and sandbox finalize. `claimed` stays accepted alongside `bound`
    /// while receipts from binds that predate the bind stamp still exist.
    pub async fn delete_claimed_slot_for_retired_execution(
        tx: &mut Transaction<'_, Postgres>,
        reserved_sandbox_id: SandboxId,
    ) -> Result<bool> {
        let deleted =
            Self::delete_claimed_slots_for_retired_executions(tx, &[reserved_sandbox_id]).await?;

        Ok(deleted > 0)
    }

    /// Batch form of [`Self::delete_claimed_slot_for_retired_execution`] for
    /// the transactions that retire many claimed pods (or delete many
    /// sandboxes) at once. `reserved_sandbox_id` is unique per slot and ids
    /// without a receipt row are no-ops, so callers pass every finalized
    /// sandbox id without checking whether the sandbox was pool-claimed.
    /// Returns the number of receipts deleted.
    ///
    /// Returns the raw [`sqlx::Error`] (this verb performs exactly one
    /// statement) so `sqlx::Error`-boundary callers such as the shared
    /// sandbox-delete transaction compose without an error-type detour.
    pub async fn delete_claimed_slots_for_retired_executions(
        conn: &mut PgConnection,
        reserved_sandbox_ids: &[SandboxId],
    ) -> std::result::Result<u64, sqlx::Error> {
        if reserved_sandbox_ids.is_empty() {
            return Ok(0);
        }
        let ids = reserved_sandbox_ids
            .iter()
            .map(|id| id.as_uuid())
            .collect::<Vec<_>>();
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = ANY($1)
               AND state IN ($2, $3)"
        ))
        .bind(ids)
        .bind(PoolSlotState::Bound.as_str())
        .bind(PoolSlotState::Claimed.as_str())
        .execute(conn)
        .await?;

        Ok(result.rows_affected())
    }

    /// Sweep one keyset window of the dead-claimed-slot backstop: `bound`
    /// (and legacy pre-stamp `claimed`) receipts whose sandbox is suspended.
    /// The suspend finalization deletes the receipt inline; this backstop
    /// only catches receipts a crashed or buggy finalization missed, so its
    /// steady-state match rate is ~zero. That is exactly why it is windowed:
    /// a predicate-driven `LIMIT` cannot short-circuit when nothing matches,
    /// so the pre-window form re-scanned every receipt on every call. Each
    /// call now examines at most `window` rows starting after `cursor` (a
    /// cheap primary-key range), deletes the matches inside that range, and
    /// reports where the next sweep should resume. Per-call cost is bounded
    /// by `window` regardless of table size; what grows with the table is
    /// the lap time until a given fossil is revisited.
    ///
    /// A `suspended` lifecycle means the suspend finalization committed,
    /// which happens strictly after the claimed pod was torn down — no pod
    /// to kill, a pure DB delete. Slots whose sandbox row is *absent* are
    /// deliberately excluded: pre-bind `claimed` crash orphans may still own
    /// a live pod (the stale-unbound sweep's job), and orphaned `bound`
    /// receipts fall to [`Self::sweep_orphaned_bound_receipts_window`]. The
    /// `older_than` grace guards hypothetical claim/`reserved_sandbox_id`
    /// reuse. `SKIP LOCKED` keeps the delete off claim-path and
    /// cleanup-pending row locks.
    pub async fn sweep_dead_claimed_slots_window(
        &self,
        older_than: Duration,
        cursor: Uuid,
        through: Uuid,
        window: i64,
    ) -> Result<BackstopSweepOutcome> {
        let Some(range) = self.backstop_window_range(cursor, through, window).await? else {
            return Ok(BackstopSweepOutcome::wrapped());
        };
        let older_than_secs = i64::try_from(older_than.as_secs()).unwrap_or(i64::MAX);
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE id IN (
                 SELECT s.id
                 FROM kubernetes_pool_slots s
                 JOIN sandboxes sb ON sb.id = s.reserved_sandbox_id
                 WHERE s.id > $2
                   AND s.id <= $3
                   AND s.state IN ($4, $5)
                   AND s.claimed_at IS NOT NULL
                   AND s.claimed_at <= NOW() - ($1 * INTERVAL '1 second')
                   AND sb.lifecycle = 'suspended'
                 FOR UPDATE OF s SKIP LOCKED
             )"
        ))
        .bind(older_than_secs)
        .bind(cursor)
        .bind(range.last_id)
        .bind(PoolSlotState::Bound.as_str())
        .bind(PoolSlotState::Claimed.as_str())
        .execute(&self.pool)
        .await?;

        Ok(range.into_outcome(result.rows_affected()))
    }

    /// Sweep one keyset window of the orphaned-receipt backstop: `bound`
    /// receipts whose sandbox row no longer exists. The sandbox-delete
    /// transaction retires the receipt inline; this backstop only catches
    /// receipts a missed delete path orphaned, and is windowed for the same
    /// empty-scan reason as [`Self::sweep_dead_claimed_slots_window`] — its
    /// anti-join predicate is invisible to any index, so an unwindowed call
    /// probed `sandboxes` once per receipt in the table to find nothing.
    ///
    /// `bound` only, never pre-bind `claimed`: a `bound` stamp means the
    /// bind transaction committed, so a `sandboxes` row existed and its
    /// absence now means the sandbox was hard-deleted — pod teardown was
    /// owned by that delete flow (a leaked terminal pod falls to the
    /// warm-pool orphan sweep). A pre-bind `claimed` row with no sandbox row
    /// is the opposite: a crash orphan that may still own a live pod, and
    /// the row itself is how the stale-unbound sweep finds that pod to kill —
    /// deleting it would strand the pod. The `older_than` grace keeps the
    /// sweep clear of in-flight delete transactions.
    pub async fn sweep_orphaned_bound_receipts_window(
        &self,
        older_than: Duration,
        cursor: Uuid,
        through: Uuid,
        window: i64,
    ) -> Result<BackstopSweepOutcome> {
        let Some(range) = self.backstop_window_range(cursor, through, window).await? else {
            return Ok(BackstopSweepOutcome::wrapped());
        };
        let older_than_secs = i64::try_from(older_than.as_secs()).unwrap_or(i64::MAX);
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE id IN (
                 SELECT s.id
                 FROM kubernetes_pool_slots s
                 WHERE s.id > $2
                   AND s.id <= $3
                   AND s.state = $4
                   AND s.claimed_at IS NOT NULL
                   AND s.claimed_at <= NOW() - ($1 * INTERVAL '1 second')
                   AND NOT EXISTS (
                       SELECT 1
                       FROM sandboxes sb
                       WHERE sb.id = s.reserved_sandbox_id
                   )
                 FOR UPDATE OF s SKIP LOCKED
             )"
        ))
        .bind(older_than_secs)
        .bind(cursor)
        .bind(range.last_id)
        .bind(PoolSlotState::Bound.as_str())
        .execute(&self.pool)
        .await?;

        Ok(range.into_outcome(result.rows_affected()))
    }

    /// Measure the next backstop sweep window: the id closing the half-open
    /// range `(cursor, last_id]` that contains at most `window` slots inside
    /// `(cursor, through]`, from an index-only primary-key range scan (the
    /// one place ordering is load-bearing, because the cursor is a resume
    /// position, not a fairness policy). The delete then bounds itself with
    /// the same two-parameter range instead of shipping the id list back.
    /// `None` means the lap reached `through` and the caller should start a
    /// fresh lap.
    ///
    /// `through` is the lap's high-water id, captured by the caller when the
    /// lap started ([`Self::newest_slot_id`]). Slot ids are UUIDv7, so new
    /// inserts always sort after the cursor — without the high-water bound a
    /// lap over a busy table would chase its own tail and its length would
    /// depend on the insert rate. Rows born mid-lap wait for the next lap,
    /// which is inside every backstop's eligibility grace anyway.
    ///
    /// Measured separately from the guarded delete on purpose: the window is
    /// only a scan budget, so a slot claimed or deleted between the two
    /// statements at worst moves work to a later lap (the delete re-checks
    /// the full predicate).
    async fn backstop_window_range(
        &self,
        cursor: Uuid,
        through: Uuid,
        window: i64,
    ) -> Result<Option<BackstopWindow>> {
        let ids = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
             FROM kubernetes_pool_slots
             WHERE id > $1
               AND id <= $2
             ORDER BY id
             LIMIT $3"
        ))
        .bind(cursor)
        .bind(through)
        .bind(window.max(0))
        .fetch_all(&self.pool)
        .await?;

        let Some(last_id) = ids.last().copied() else {
            return Ok(None);
        };
        Ok(Some(BackstopWindow {
            last_id,
            exhausted: (ids.len() as i64) < window,
        }))
    }

    /// The newest slot id, used as a backstop lap's high-water bound. `None`
    /// means the table is empty and there is nothing to sweep.
    pub async fn newest_slot_id(&self) -> Result<Option<Uuid>> {
        Ok(sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
             FROM kubernetes_pool_slots
             ORDER BY id DESC
             LIMIT 1"
        ))
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn lock_stale_unbound_claimed_slot_for_delete(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
        older_than: Duration,
    ) -> Result<bool> {
        let older_than_secs = i64::try_from(older_than.as_secs()).unwrap_or(i64::MAX);
        let row = sqlx::query_scalar::<_, Uuid>(STALE_UNBOUND_CLAIMED_SLOT_BY_ID_FOR_UPDATE_SQL)
            .bind(slot_id)
            .bind(older_than_secs)
            .fetch_optional(&mut **tx)
            .await?;

        Ok(row.is_some())
    }

    pub async fn lock_cleanup_pending_slot_for_delete(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
    ) -> Result<bool> {
        let row = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
             FROM kubernetes_pool_slots
             WHERE id = $1
               AND state = 'cleanup_pending'
             FOR UPDATE SKIP LOCKED"
        ))
        .bind(slot_id)
        .fetch_optional(&mut **tx)
        .await?;

        Ok(row.is_some())
    }

    pub async fn delete_locked_claimed_slot(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
    ) -> Result<bool> {
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE id = $1
               AND state = 'claimed'"
        ))
        .bind(slot_id)
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_locked_cleanup_pending_slot(
        tx: &mut Transaction<'_, Postgres>,
        slot_id: Uuid,
    ) -> Result<bool> {
        let result = sqlx::query(sql!(
            "DELETE FROM kubernetes_pool_slots
             WHERE id = $1
               AND state = 'cleanup_pending'"
        ))
        .bind(slot_id)
        .execute(&mut **tx)
        .await?;

        Ok(result.rows_affected() > 0)
    }
}

fn pool_assignment_from_shape_row(
    (vcpu, memory_mib, disk_mib, operations, bandwidth): (i32, i32, Option<i32>, i64, i64),
) -> Result<crate::PoolAssignment> {
    let vcpu = u32::try_from(vcpu).map_err(|_| PoolError::InvalidResource {
        field: "vcpu",
        value: vcpu,
    })?;
    let memory_mib = u64::try_from(memory_mib).map_err(|_| PoolError::InvalidResource {
        field: "memory_mib",
        value: memory_mib,
    })?;
    Ok(crate::PoolAssignment {
        vcpu: VcpuCount::try_from_u32(vcpu)?,
        memory: MemorySize::mib(memory_mib)?,
        disk_mib: crate::models::parse_disk_mib(disk_mib)?,
        disk_io_limits: crate::models::parse_disk_io_limits(operations, bandwidth)?,
    })
}

fn select_autoscale_candidates(
    releases: Vec<PoolPolicyRelease>,
    demand: Vec<PoolObservedDemand>,
) -> Vec<PoolAutoscaleCandidate> {
    let observed_by_release = demand
        .into_iter()
        .map(|demand| {
            (
                (demand.template_release_id, demand.vcpu, demand.memory),
                demand.observed_sandboxes,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut ranked_releases = Vec::with_capacity(releases.len());

    for release in releases {
        let observed_sandboxes = observed_by_release
            .get(&(release.template_release_id, release.vcpu, release.memory))
            .copied()
            .unwrap_or_default();

        ranked_releases.push((release, observed_sandboxes));
    }

    let mut selected = vec![false; ranked_releases.len()];
    let mut demanded_by_policy = HashMap::<_, Vec<(usize, i64)>>::new();
    for (index, (release, observed_sandboxes)) in ranked_releases.iter().enumerate() {
        if release.retained {
            if let Some(selected) = selected.get_mut(index) {
                *selected = true;
            }
            continue;
        }
        if *observed_sandboxes < AUTOSCALE_MIN_DEMAND_SANDBOXES {
            continue;
        }

        demanded_by_policy
            .entry((release.template_name.clone(), release.vcpu, release.memory))
            .or_default()
            .push((index, *observed_sandboxes));
    }

    // `retain_releases` also bounds the additional demand-selected set for
    // each policy. Prefer the busiest exact releases so many caller-controlled
    // hashes cannot grow warm-pool inventory without bound.
    for demanded in demanded_by_policy.values_mut() {
        demanded.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        let limit = demanded
            .first()
            .and_then(|(index, _)| ranked_releases.get(*index))
            .and_then(|(release, _)| usize::try_from(release.retain_releases).ok())
            .unwrap_or_default();
        for (index, _) in demanded.iter().take(limit) {
            if let Some(selected) = selected.get_mut(*index) {
                *selected = true;
            }
        }
    }

    ranked_releases
        .into_iter()
        .zip(selected)
        .filter(|(_, selected)| *selected)
        .map(
            |((release, observed_sandboxes), _)| PoolAutoscaleCandidate {
                template_release_id: release.template_release_id,
                vcpu: release.vcpu,
                memory: release.memory,
                disk_mib: release.disk_mib,
                disk_io_limits: release.disk_io_limits,
                target_ready_slots: target_ready_slots(
                    observed_sandboxes,
                    release.min_ready_slots,
                    release.max_ready_slots,
                ),
                refill_per_tick: release.refill_per_tick,
                max_warming_slots: release.max_warming_slots,
            },
        )
        .collect()
}

fn target_ready_slots(observed_sandboxes: i64, min_ready_slots: i32, max_ready_slots: i32) -> i32 {
    debug_assert!(observed_sandboxes >= 0);
    let divisor = AUTOSCALE_SANDBOXES_PER_READY_SLOT;
    let demand_target = observed_sandboxes / divisor + i64::from(observed_sandboxes % divisor != 0);
    let target = demand_target.clamp(i64::from(min_ready_slots), i64::from(max_ready_slots));
    i32::try_from(target).expect("policy ready-slot bounds fit i32")
}
