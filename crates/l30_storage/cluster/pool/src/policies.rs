//! The admin surface over `warm_pool_policies`, the per-template warm-pool
//! shapes.

use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

/// The admin columns of one `warm_pool_policies` row: every column but
/// `retain_shape_on_miss`.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct WarmPoolPolicyRecord {
    pub template_name: String,
    pub runtime: String,
    pub vcpu: i32,
    pub memory_mib: i32,
    pub disk_mib: Option<i32>,
    pub min_ready_slots: i32,
    pub max_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub retain_releases: i32,
    pub disk_io_max_operations_per_sec: Option<i64>,
    pub disk_io_max_bandwidth_mib_per_sec: Option<i64>,
    pub enabled: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Every Firecracker policy, ordered by template, runtime and shape.
pub async fn fc_warm_pool_policies(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<WarmPoolPolicyRecord>, sqlx::Error> {
    sqlx::query_as::<_, WarmPoolPolicyRecord>(sql!(
        "SELECT template_name, runtime, vcpu, memory_mib, disk_mib, min_ready_slots,
                max_ready_slots, refill_per_tick, max_warming_slots,
                retain_releases, disk_io_max_operations_per_sec,
                disk_io_max_bandwidth_mib_per_sec, enabled, created_at, updated_at
         FROM warm_pool_policies
         WHERE runtime = 'fc'
         ORDER BY template_name, runtime, vcpu, memory_mib"
    ))
    .fetch_all(executor)
    .await
}

/// A Firecracker policy to write with [`upsert_fc_warm_pool_policy`].
#[derive(Debug, Clone, Copy)]
pub struct WarmPoolPolicyUpsert<'a> {
    pub template_name: &'a str,
    pub vcpu: i32,
    pub memory_mib: i32,
    /// `None` keeps a stored override unless `reset_disk_to_template_default`.
    pub disk_mib: Option<i32>,
    pub min_ready_slots: i32,
    pub max_ready_slots: i32,
    pub refill_per_tick: i32,
    pub max_warming_slots: i32,
    pub retain_releases: i32,
    /// `None` keeps a stored limit unless `reset_disk_io_to_default`.
    pub disk_io_max_operations_per_sec: Option<i64>,
    /// `None` keeps a stored limit unless `reset_disk_io_to_default`.
    pub disk_io_max_bandwidth_mib_per_sec: Option<i64>,
    pub enabled: bool,
    /// Clears a stored disk override, so the template's default applies.
    pub reset_disk_to_template_default: bool,
    /// Clears both stored disk-I/O limits, so the defaults apply.
    pub reset_disk_io_to_default: bool,
}

/// Insert the policy for its template and shape, or update the one there,
/// returning the row as written.
pub async fn upsert_fc_warm_pool_policy(
    executor: impl sqlx::PgExecutor<'_>,
    policy: &WarmPoolPolicyUpsert<'_>,
) -> Result<WarmPoolPolicyRecord, sqlx::Error> {
    sqlx::query_as::<_, WarmPoolPolicyRecord>(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, disk_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots,
             retain_releases, disk_io_max_operations_per_sec,
             disk_io_max_bandwidth_mib_per_sec, enabled
         )
         VALUES ($1, 'fc', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (template_name, runtime, vcpu, memory_mib) DO UPDATE
         SET disk_mib = CASE WHEN $13 THEN NULL
                             ELSE COALESCE(EXCLUDED.disk_mib, warm_pool_policies.disk_mib)
                        END,
             min_ready_slots = EXCLUDED.min_ready_slots,
             max_ready_slots = EXCLUDED.max_ready_slots,
             refill_per_tick = EXCLUDED.refill_per_tick,
             max_warming_slots = EXCLUDED.max_warming_slots,
             retain_releases = EXCLUDED.retain_releases,
             disk_io_max_operations_per_sec =
                 CASE WHEN $14 THEN NULL
                      ELSE COALESCE(EXCLUDED.disk_io_max_operations_per_sec,
                                    warm_pool_policies.disk_io_max_operations_per_sec)
                 END,
             disk_io_max_bandwidth_mib_per_sec =
                 CASE WHEN $14 THEN NULL
                      ELSE COALESCE(EXCLUDED.disk_io_max_bandwidth_mib_per_sec,
                                    warm_pool_policies.disk_io_max_bandwidth_mib_per_sec)
                 END,
             enabled = EXCLUDED.enabled,
             updated_at = NOW()
         RETURNING template_name, runtime, vcpu, memory_mib, disk_mib, min_ready_slots,
                   max_ready_slots, refill_per_tick, max_warming_slots,
                   retain_releases, disk_io_max_operations_per_sec,
                   disk_io_max_bandwidth_mib_per_sec, enabled, created_at, updated_at"
    ))
    .bind(policy.template_name)
    .bind(policy.vcpu)
    .bind(policy.memory_mib)
    .bind(policy.disk_mib)
    .bind(policy.min_ready_slots)
    .bind(policy.max_ready_slots)
    .bind(policy.refill_per_tick)
    .bind(policy.max_warming_slots)
    .bind(policy.retain_releases)
    .bind(policy.disk_io_max_operations_per_sec)
    .bind(policy.disk_io_max_bandwidth_mib_per_sec)
    .bind(policy.enabled)
    .bind(policy.reset_disk_to_template_default)
    .bind(policy.reset_disk_io_to_default)
    .fetch_one(executor)
    .await
}
