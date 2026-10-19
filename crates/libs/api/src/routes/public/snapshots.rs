use pplx_space_common_core::types::{OrgId, SandboxId, SnapshotId};

use pplx_space_common_core::sql;
use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
};
use pplx_space_public_api::{
    PageCursor, PublicPage, PublicSandboxId, PublicSnapshot, PublicSnapshotId, PublicSnapshotState,
    PublicSnapshotType, SandboxResources,
};
use serde::Deserialize;
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;

use crate::{middleware::auth::AuthOrg, routes::AppState};

use super::{
    cursors::{CursorKind, decode_cursor, encode_cursor},
    error::PublicApiError,
    sandboxes::parse_public_sandbox_id,
};

const DEFAULT_SNAPSHOT_LIST_LIMIT: u16 = 100;
const MAX_SNAPSHOT_LIST_LIMIT: u16 = 1000;

pub async fn list_snapshots(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(sandbox_id): Path<String>,
    Query(query): Query<ListSnapshotsQuery>,
) -> Result<Json<PublicPage<PublicSnapshot>>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let limit = query.limit();
    let cursor = query.cursor_parts()?;
    let rows = fetch_snapshot_rows(&state.pool, org.id, sandbox_id, cursor, limit).await?;
    let (rows, next_cursor) = snapshot_page_rows(rows, limit)?;
    let data = rows
        .into_iter()
        .map(public_snapshot_from_row)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(PublicPage { data, next_cursor }))
}

pub async fn get_snapshot(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path((sandbox_id, snapshot_id)): Path<(String, String)>,
) -> Result<Json<PublicSnapshot>, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    let snapshot_id = parse_public_snapshot_id(&snapshot_id)?;
    let Some(row) = fetch_snapshot_row(&state.pool, org.id, sandbox_id, snapshot_id.into()).await?
    else {
        return Err(PublicApiError::not_found("snapshot not found"));
    };

    Ok(Json(public_snapshot_from_row(row)?))
}

#[derive(Debug, Deserialize)]
pub struct ListSnapshotsQuery {
    cursor: Option<String>,
    limit: Option<u16>,
}

impl ListSnapshotsQuery {
    fn cursor_parts(&self) -> Result<Option<(OffsetDateTime, SnapshotId)>, PublicApiError> {
        let Some(cursor) = self.cursor.as_deref() else {
            return Ok(None);
        };
        let cursor = decode_cursor(CursorKind::Snapshot, cursor)?;
        parse_snapshot_cursor(&cursor).map(Some)
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_SNAPSHOT_LIST_LIMIT,
            Some(limit) if limit > MAX_SNAPSHOT_LIST_LIMIT => MAX_SNAPSHOT_LIST_LIMIT,
            Some(limit) => limit,
        }
    }
}

#[derive(Debug, FromRow)]
struct SnapshotRow {
    id: SnapshotId,
    sandbox_id: SandboxId,
    snapshot_type: String,
    state: String,
    created_at: OffsetDateTime,
    label: Option<String>,
    retention_tier: Option<String>,
    size_bytes: Option<i64>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    template_id: Option<String>,
    content_hash: Option<String>,
    failure_reason: Option<String>,
}

async fn fetch_snapshot_rows(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
    cursor: Option<(OffsetDateTime, SnapshotId)>,
    limit: u16,
) -> Result<Vec<SnapshotRow>, sqlx::Error> {
    let (cursor_created_at, cursor_id) = cursor
        .map(|(created_at, id)| (Some(created_at), Some(id)))
        .unwrap_or((None, None));
    // `deleting` rows are claimed for prune and de-advertised: they must
    // read as gone on this surface, and `PublicSnapshotState` deliberately
    // has no such wire variant.
    sqlx::query_as::<_, SnapshotRow>(sql!(
        "SELECT sn.id,
                sn.sandbox_id,
                sn.type AS snapshot_type,
                sn.state,
                sn.timestamp AS created_at,
                sn.label,
                sn.retention_tier,
                sn.size_bytes,
                sn.vcpu,
                sn.memory_mib,
                sn.template_id,
                sn.content_hash,
                sn.failure_reason
         FROM snapshots sn
         JOIN sandboxes s ON s.id = sn.sandbox_id
         WHERE sn.sandbox_id = $1
           AND s.org_id = $2
           AND sn.artifact_origin IS DISTINCT FROM 'fork'
           AND sn.state <> 'deleting'
           AND (
               $3::TIMESTAMPTZ IS NULL
               OR (sn.timestamp, sn.id) < ($3::TIMESTAMPTZ, $4::UUID)
           )
         ORDER BY sn.timestamp DESC, sn.id DESC
         LIMIT $5"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(org_id)
    .bind(cursor_created_at)
    .bind(cursor_id)
    .bind(i64::from(limit) + 1)
    .fetch_all(pool)
    .await
}

async fn fetch_snapshot_row(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
    snapshot_id: SnapshotId,
) -> Result<Option<SnapshotRow>, sqlx::Error> {
    // Excluding `deleting` turns a get on a claimed row into a 404, matching
    // the list surface above.
    sqlx::query_as::<_, SnapshotRow>(sql!(
        "SELECT sn.id,
                sn.sandbox_id,
                sn.type AS snapshot_type,
                sn.state,
                sn.timestamp AS created_at,
                sn.label,
                sn.retention_tier,
                sn.size_bytes,
                sn.vcpu,
                sn.memory_mib,
                sn.template_id,
                sn.content_hash,
                sn.failure_reason
         FROM snapshots sn
         JOIN sandboxes s ON s.id = sn.sandbox_id
         WHERE sn.sandbox_id = $1
           AND sn.id = $2
           AND s.org_id = $3
           AND sn.artifact_origin IS DISTINCT FROM 'fork'
           AND sn.state <> 'deleting'"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(snapshot_id)
    .bind(org_id)
    .fetch_optional(pool)
    .await
}

fn public_snapshot_from_row(row: SnapshotRow) -> Result<PublicSnapshot, PublicApiError> {
    let snapshot_type = row
        .snapshot_type
        .parse::<PublicSnapshotType>()
        .map_err(|_| PublicApiError::internal())?;
    let state = row
        .state
        .parse::<PublicSnapshotState>()
        .map_err(|_| PublicApiError::internal())?;
    Ok(PublicSnapshot {
        id: PublicSnapshotId::from(row.id),
        sandbox_id: PublicSandboxId::from(row.sandbox_id),
        snapshot_type,
        state,
        label: row.label,
        retention_tier: row.retention_tier,
        size_bytes: non_negative_u64(row.size_bytes),
        resources: SandboxResources {
            vcpu: positive_u32(row.vcpu),
            memory_mib: positive_u32(row.memory_mib),
            disk_mib: None,
        },
        template: row.template_id,
        template_digest: row.content_hash,
        failure_reason: row.failure_reason,
        created_at: row.created_at,
    })
}

fn snapshot_page_rows(
    mut rows: Vec<SnapshotRow>,
    limit: u16,
) -> Result<(Vec<SnapshotRow>, Option<PageCursor>), PublicApiError> {
    let has_more = rows.len() > usize::from(limit);
    if has_more {
        rows.truncate(usize::from(limit));
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|snapshot| snapshot_cursor(snapshot.created_at, snapshot.id))
            .transpose()?
    } else {
        None
    };

    Ok((rows, next_cursor))
}

fn snapshot_cursor(
    created_at: OffsetDateTime,
    snapshot_id: SnapshotId,
) -> Result<PageCursor, PublicApiError> {
    encode_cursor(
        CursorKind::Snapshot,
        format!("{}:{snapshot_id}", created_at.unix_timestamp_nanos()),
    )
}

fn parse_snapshot_cursor(value: &str) -> Result<(OffsetDateTime, SnapshotId), PublicApiError> {
    let Some((created_at, snapshot_id)) = value.split_once(':') else {
        return Err(PublicApiError::bad_request("invalid cursor"));
    };
    let created_at = created_at
        .parse::<i128>()
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))
        .and_then(|created_at| {
            OffsetDateTime::from_unix_timestamp_nanos(created_at)
                .map_err(|_| PublicApiError::bad_request("invalid cursor"))
        })?;
    let snapshot_id = snapshot_id
        .parse::<SnapshotId>()
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    Ok((created_at, snapshot_id))
}

fn parse_public_snapshot_id(value: &str) -> Result<PublicSnapshotId, PublicApiError> {
    PublicSnapshotId::from_str(value)
        .map_err(|_| PublicApiError::bad_request("invalid snapshot id"))
}

fn non_negative_u64(value: Option<i64>) -> Option<u64> {
    value.and_then(|value| u64::try_from(value).ok())
}

fn positive_u32(value: Option<i32>) -> Option<u32> {
    value
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

#[cfg(test)]
mod tests;
