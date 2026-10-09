use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::DiskIoLimits;
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

use crate::controllers::ControllerError;
use crate::middleware::auth::AuthUser;
use crate::models::{ClusterPoolConfigRequest, WarmPoolPolicyRequest, WarmPoolPolicyResponse};
use crate::routes::AppState;
use crate::routes::admin::support::{self, ContentionScope, db_error, require_managing_org_admin};

pub async fn list_warm_pools(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    match state.template_controller.list_cluster_pool_configs().await {
        Ok(configs) => Json(configs).into_response(),
        Err(error) => controller_error(error).into_response(),
    }
}

pub async fn upsert_warm_pool(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(request): Json<ClusterPoolConfigRequest>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    match state
        .template_controller
        .upsert_cluster_pool_config(request)
        .await
    {
        Ok(config) => Json(config).into_response(),
        Err(error) => controller_error(error).into_response(),
    }
}

pub async fn disable_warm_pool(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(pool_id): Path<Uuid>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    match state
        .template_controller
        .disable_cluster_pool_config(pool_id)
        .await
    {
        Ok(config) => Json(config).into_response(),
        Err(error) => controller_error(error).into_response(),
    }
}

#[derive(Debug, FromRow)]
struct WarmPoolPolicyRow {
    template_name: String,
    runtime: String,
    vcpu: i32,
    memory_mib: i32,
    disk_mib: Option<i32>,
    min_ready_slots: i32,
    max_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
    retain_releases: i32,
    disk_io_max_operations_per_sec: Option<i64>,
    disk_io_max_bandwidth_mib_per_sec: Option<i64>,
    enabled: bool,
    created_at: time::OffsetDateTime,
    updated_at: time::OffsetDateTime,
}

#[derive(Debug, Serialize)]
pub struct WarmPoolPolicyCapabilitiesResponse {
    disk_capacity: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    disk_io: Option<WarmPoolDiskIoCapability>,
}

#[derive(Debug, Serialize)]
struct WarmPoolDiskIoCapability {
    max_operations_per_sec: u64,
    max_bandwidth_mib_per_sec: u64,
    default_operations_per_sec: u64,
    default_bandwidth_mib_per_sec: u64,
}

pub async fn list_warm_pool_policies(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    match fetch_policies(&state.pool).await {
        Ok(policies) => Json(
            policies
                .into_iter()
                .map(WarmPoolPolicyResponse::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        // `ReadOnly`: the gate above already classifies its own preflight, but
        // this `SELECT` is a second statement that can block behind DDL on
        // `warm_pool_policies` on its own. It wrote nothing, so every
        // contention SQLSTATE answers a retryable 503 and none takes the
        // write-specific 409 — see `support::contention_disposition`.
        Err(error) => match support::contention_disposition(ContentionScope::ReadOnly, &error) {
            Some((status, message)) => api_error(status, message).into_response(),
            None => {
                tracing::warn!(%error, "failed to load warm-pool policies");
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to load warm-pool policies",
                )
                .into_response()
            }
        },
    }
}

pub async fn get_warm_pool_policy_capabilities(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }

    match configured_disk_io_policy_capability() {
        Ok(capabilities) => Json(capabilities).into_response(),
        Err(err) => err.into_response(),
    }
}

pub async fn upsert_warm_pool_policy(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(request): Json<WarmPoolPolicyRequest>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let disk_io_ceiling = match configured_disk_io_ceiling() {
        Ok(ceiling) => ceiling,
        Err(err) => return err.into_response(),
    };
    if let Err(err) = validate_policy_request(&request, disk_io_ceiling) {
        return err.into_response();
    }

    match store_policy(&state.pool, &request).await {
        Ok(policy) => Json(WarmPoolPolicyResponse::from(policy)).into_response(),
        // Through `db_error` like the other guarded admin writes: this is an
        // `INSERT ... ON CONFLICT`, so it blocks on a concurrent unfinished
        // upsert of the same key and on DDL, and answering that with a 500
        // while a template-visibility flip answers 503 for the same SQLSTATE
        // on the same surface is the inconsistency a reader copies.
        Err(error) => db_error(error).into_response(),
    }
}

fn controller_error(error: ControllerError) -> (StatusCode, Json<serde_json::Value>) {
    let status = error.status();
    if status.is_server_error() {
        tracing::warn!(error = %error, "admin warm-pool request failed");
    }
    api_error(status, error.to_string())
}

fn api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": message.into()})))
}

async fn fetch_policies(pool: &sqlx::PgPool) -> Result<Vec<WarmPoolPolicyRow>, sqlx::Error> {
    sqlx::query_as::<_, WarmPoolPolicyRow>(sql!(
        "SELECT template_name, runtime, vcpu, memory_mib, disk_mib, min_ready_slots,
                max_ready_slots, refill_per_tick, max_warming_slots,
                retain_releases, disk_io_max_operations_per_sec,
                disk_io_max_bandwidth_mib_per_sec, enabled, created_at, updated_at
         FROM warm_pool_policies
         WHERE runtime = 'fc'
         ORDER BY template_name, runtime, vcpu, memory_mib"
    ))
    .fetch_all(pool)
    .await
}

async fn store_policy(
    pool: &sqlx::PgPool,
    request: &WarmPoolPolicyRequest,
) -> Result<WarmPoolPolicyRow, sqlx::Error> {
    sqlx::query_as::<_, WarmPoolPolicyRow>(sql!(
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
    .bind(request.template_name.trim())
    .bind(request.vcpu as i32)
    .bind(request.memory_mib as i32)
    .bind(request.disk_mib.map(|value| value as i32))
    .bind(request.min_ready_slots as i32)
    .bind(request.max_ready_slots as i32)
    .bind(request.refill_per_tick as i32)
    .bind(request.max_warming_slots as i32)
    .bind(request.retain_releases as i32)
    .bind(request.disk_io_max_operations_per_sec.map(u64_to_i64))
    .bind(request.disk_io_max_bandwidth_mib_per_sec.map(u64_to_i64))
    .bind(request.enabled)
    .bind(request.reset_disk_to_template_default)
    .bind(request.reset_disk_io_to_default)
    .fetch_one(pool)
    .await
}

fn validate_policy_request(
    request: &WarmPoolPolicyRequest,
    disk_io_ceiling: DiskIoLimits,
) -> Result<(), ControllerError> {
    if request.template_name.trim().is_empty() {
        return Err(ControllerError::bad_request("template_name is required"));
    }
    if request.vcpu == 0 || request.memory_mib == 0 {
        return Err(ControllerError::bad_request(
            "vcpu and memory_mib must be greater than 0",
        ));
    }
    if let Some(disk_mib) = request.disk_mib {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(disk_mib)
            .map_err(|error| ControllerError::bad_request(format!("disk_mib: {error}")))?;
    }
    if request.reset_disk_to_template_default && request.disk_mib.is_some() {
        return Err(ControllerError::bad_request(
            "reset_disk_to_template_default cannot be combined with disk_mib",
        ));
    }
    if request.max_ready_slots < request.min_ready_slots {
        return Err(ControllerError::bad_request(
            "max_ready_slots must be greater than or equal to min_ready_slots",
        ));
    }
    if request.retain_releases == 0 {
        return Err(ControllerError::bad_request(
            "retain_releases must be greater than 0",
        ));
    }
    if request.enabled && request.max_ready_slots > 0 && request.refill_per_tick == 0 {
        return Err(ControllerError::bad_request(
            "refill_per_tick must be greater than 0 for an enabled non-empty policy",
        ));
    }
    if request.enabled && request.max_ready_slots > 0 && request.max_warming_slots == 0 {
        return Err(ControllerError::bad_request(
            "max_warming_slots must be greater than 0 for an enabled non-empty policy",
        ));
    }
    if request.refill_per_tick > request.max_warming_slots {
        return Err(ControllerError::bad_request(
            "refill_per_tick must be less than or equal to max_warming_slots",
        ));
    }
    if request.reset_disk_io_to_default
        && (request.disk_io_max_operations_per_sec.is_some()
            || request.disk_io_max_bandwidth_mib_per_sec.is_some())
    {
        return Err(ControllerError::bad_request(
            "reset_disk_io_to_default cannot be combined with disk I/O limits",
        ));
    }
    match (
        request.disk_io_max_operations_per_sec,
        request.disk_io_max_bandwidth_mib_per_sec,
    ) {
        (None, None) => {}
        (Some(operations), Some(bandwidth)) => {
            if operations == 0 || bandwidth == 0 {
                return Err(ControllerError::bad_request(
                    "disk I/O limits must be greater than 0",
                ));
            }
            if operations > disk_io_ceiling.max_operations_per_sec()
                || bandwidth > disk_io_ceiling.max_bandwidth_mib_per_sec()
            {
                return Err(ControllerError::bad_request(format!(
                    "disk I/O limits exceed this cluster's safety ceiling of {} IOPS and {} MiB/s",
                    disk_io_ceiling.max_operations_per_sec(),
                    disk_io_ceiling.max_bandwidth_mib_per_sec(),
                )));
            }
            if i64::try_from(operations).is_err() || i64::try_from(bandwidth).is_err() {
                return Err(ControllerError::bad_request(
                    "disk I/O limits exceed the supported range",
                ));
            }
        }
        _ => {
            return Err(ControllerError::bad_request(
                "disk_io_max_operations_per_sec and disk_io_max_bandwidth_mib_per_sec must be set together",
            ));
        }
    }
    if [
        request.vcpu,
        request.memory_mib,
        request.min_ready_slots,
        request.max_ready_slots,
        request.refill_per_tick,
        request.max_warming_slots,
        request.retain_releases,
    ]
    .into_iter()
    .any(|value| i32::try_from(value).is_err())
    {
        return Err(ControllerError::bad_request(
            "warm-pool policy values exceed the supported range",
        ));
    }
    if request
        .disk_mib
        .is_some_and(|value| i32::try_from(value).is_err())
    {
        return Err(ControllerError::bad_request(
            "disk_mib exceeds the supported range",
        ));
    }
    Ok(())
}

fn configured_disk_io_ceiling() -> Result<DiskIoLimits, ControllerError> {
    configured_disk_io_policy_capability()?
        .disk_io
        .map(|capability| {
            DiskIoLimits::try_new(
                capability.max_operations_per_sec,
                capability.max_bandwidth_mib_per_sec,
            )
            .map_err(|error| {
                ControllerError::internal(format!(
                    "invalid configured disk I/O safety ceiling: {error}"
                ))
            })
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn configured_disk_io_policy_capability()
-> Result<WarmPoolPolicyCapabilitiesResponse, ControllerError> {
    let operations = std::env::var("SPACE_DISK_IO_SAFETY_MAX_OPERATIONS_PER_SEC").ok();
    let bandwidth = std::env::var("SPACE_DISK_IO_SAFETY_MAX_BANDWIDTH_MIB_PER_SEC").ok();
    disk_io_policy_capability_from_values(operations.as_deref(), bandwidth.as_deref())
}

fn disk_io_policy_capability_from_values(
    operations: Option<&str>,
    bandwidth: Option<&str>,
) -> Result<WarmPoolPolicyCapabilitiesResponse, ControllerError> {
    let disk_io = match (operations, bandwidth) {
        (None, None) => None,
        (Some(operations), Some(bandwidth)) => {
            let defaults = DiskIoLimits::default();
            let limits = DiskIoLimits::try_new(
                parse_positive_u64("SPACE_DISK_IO_SAFETY_MAX_OPERATIONS_PER_SEC", operations)?,
                parse_positive_u64("SPACE_DISK_IO_SAFETY_MAX_BANDWIDTH_MIB_PER_SEC", bandwidth)?,
            )
            .map_err(|error| {
                ControllerError::internal(format!(
                    "invalid configured disk I/O safety ceiling: {error}"
                ))
            })?;
            Some(WarmPoolDiskIoCapability {
                max_operations_per_sec: limits.max_operations_per_sec(),
                max_bandwidth_mib_per_sec: limits.max_bandwidth_mib_per_sec(),
                default_operations_per_sec: defaults.max_operations_per_sec(),
                default_bandwidth_mib_per_sec: defaults.max_bandwidth_mib_per_sec(),
            })
        }
        _ => {
            return Err(ControllerError::internal(
                "disk I/O safety ceiling must configure both operations and bandwidth",
            ));
        }
    };
    Ok(WarmPoolPolicyCapabilitiesResponse {
        disk_capacity: true,
        disk_io,
    })
}

fn parse_positive_u64(name: &str, raw: &str) -> Result<u64, ControllerError> {
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| ControllerError::internal(format!("{name} must be a positive integer")))?;
    if parsed == 0 {
        return Err(ControllerError::internal(format!(
            "{name} must be a positive integer"
        )));
    }
    Ok(parsed)
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).expect("disk I/O request was range validated")
}

impl From<WarmPoolPolicyRow> for WarmPoolPolicyResponse {
    fn from(row: WarmPoolPolicyRow) -> Self {
        let disk_io_limits = row
            .disk_io_max_operations_per_sec
            .zip(row.disk_io_max_bandwidth_mib_per_sec)
            .and_then(|(operations, bandwidth)| {
                Some((
                    u64::try_from(operations).ok()?,
                    u64::try_from(bandwidth).ok()?,
                ))
            });
        Self {
            template_name: row.template_name,
            runtime: row.runtime,
            vcpu: row.vcpu.cast_unsigned(),
            memory_mib: row.memory_mib.cast_unsigned(),
            disk_mib: row.disk_mib.and_then(|value| u32::try_from(value).ok()),
            min_ready_slots: row.min_ready_slots.cast_unsigned(),
            max_ready_slots: row.max_ready_slots.cast_unsigned(),
            refill_per_tick: row.refill_per_tick.cast_unsigned(),
            max_warming_slots: row.max_warming_slots.cast_unsigned(),
            retain_releases: row.retain_releases.cast_unsigned(),
            retain_shape_on_miss: true,
            disk_io_max_operations_per_sec: disk_io_limits.map(|limits| limits.0),
            disk_io_max_bandwidth_mib_per_sec: disk_io_limits.map(|limits| limits.1),
            disk_io_source: disk_io_limits.map(|_| "warm_pool"),
            enabled: row.enabled,
            created_at: row
                .created_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            updated_at: row
                .updated_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests;
