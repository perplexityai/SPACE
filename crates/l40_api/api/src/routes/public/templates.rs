use crate::routes::public::error::database_error;
use pplx_space_common_core::types::OrgId;

use pplx_space_common_core::sql;
use std::collections::BTreeMap;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
};
use pplx_space_common_core::types::{RuntimeKind, TemplateName, TemplateVisibility};
use pplx_space_public_api::{
    PageCursor, PublicPage, PublicTemplate, PublicTemplateSnapshot, PublicTemplateVariant,
    SandboxResources,
};
use serde::Deserialize;
use sqlx::{FromRow, PgPool, types::Json as SqlJson};
use time::OffsetDateTime;

use crate::{middleware::auth::AuthOrg, routes::AppState};

use super::{
    cursors::{CursorKind, decode_cursor, encode_cursor},
    error::PublicApiError,
};

const DEFAULT_TEMPLATE_LIST_LIMIT: u16 = 100;
const MAX_TEMPLATE_LIST_LIMIT: u16 = 1000;

pub async fn list_templates(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Query(query): Query<ListTemplatesQuery>,
) -> Result<Json<PublicPage<PublicTemplate>>, PublicApiError> {
    let limit = query.limit();
    let cursor = query.cursor()?;
    let rows = fetch_template_rows(&state.pool, org.id, cursor.as_deref(), limit)
        .await
        .map_err(database_error)?;
    let (rows, next_cursor) = template_page_rows(rows, limit)?;
    let names = rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>();
    let variants = fetch_template_variants(&state.pool, &names)
        .await
        .map_err(database_error)?;
    let data = public_templates(rows, variants);

    Ok(Json(PublicPage { data, next_cursor }))
}

pub async fn get_template(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(template_name): Path<String>,
) -> Result<Json<PublicTemplate>, PublicApiError> {
    let template_name = TemplateName::parse(&template_name)
        .map_err(|error| PublicApiError::bad_request(format!("invalid template name: {error}")))?
        .as_str()
        .to_string();
    let Some(row) = fetch_template_row(&state.pool, org.id, &template_name)
        .await
        .map_err(database_error)?
    else {
        return Err(PublicApiError::not_found("template not found"));
    };
    let variants = fetch_template_variants(&state.pool, std::slice::from_ref(&template_name))
        .await
        .map_err(database_error)?;
    let mut data = public_templates(vec![row], variants);

    Ok(Json(data.pop().ok_or_else(PublicApiError::internal)?))
}

#[derive(Debug, Deserialize)]
pub struct ListTemplatesQuery {
    cursor: Option<String>,
    limit: Option<u16>,
}

impl ListTemplatesQuery {
    fn cursor(&self) -> Result<Option<String>, PublicApiError> {
        self.cursor
            .as_deref()
            .map(|cursor| decode_cursor(CursorKind::Template, cursor))
            .transpose()
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_TEMPLATE_LIST_LIMIT,
            Some(limit) if limit > MAX_TEMPLATE_LIST_LIMIT => MAX_TEMPLATE_LIST_LIMIT,
            Some(limit) => limit,
        }
    }
}

#[derive(Debug, FromRow)]
struct TemplateRow {
    name: String,
    description: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct TemplateVariantRow {
    template_name: String,
    content_hash: String,
    runtime: Option<String>,
    arch: Option<String>,
    built_at: Option<String>,
    artifact_size_bytes: Option<i64>,
    has_base_snapshot: bool,
    base_snapshot_size_bytes: Option<i64>,
    snapshots: SqlJson<Vec<PublicTemplateSnapshot>>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    disk_size_mb: Option<i32>,
}

async fn fetch_template_rows(
    pool: &PgPool,
    org_id: OrgId,
    cursor: Option<&str>,
    limit: u16,
) -> Result<Vec<TemplateRow>, sqlx::Error> {
    sqlx::query_as::<_, TemplateRow>(sql!(
        "SELECT name, description, created_at, updated_at
         FROM templates
         WHERE (visibility = $1 OR org_id = $2)
           AND desired_state = 'present'
           AND ($3::TEXT IS NULL OR name > $3)
         ORDER BY name ASC
         LIMIT $4"
    ))
    .bind(TemplateVisibility::Generic.as_str())
    .bind(org_id)
    .bind(cursor)
    .bind(i64::from(limit) + 1)
    .fetch_all(pool)
    .await
}

async fn fetch_template_row(
    pool: &PgPool,
    org_id: OrgId,
    template_name: &str,
) -> Result<Option<TemplateRow>, sqlx::Error> {
    sqlx::query_as::<_, TemplateRow>(sql!(
        "SELECT name, description, created_at, updated_at
         FROM templates
         WHERE name = $1
           AND desired_state = 'present'
           AND (visibility = $2 OR org_id = $3)"
    ))
    .bind(template_name)
    .bind(TemplateVisibility::Generic.as_str())
    .bind(org_id)
    .fetch_optional(pool)
    .await
}

async fn fetch_template_variants(
    pool: &PgPool,
    template_names: &[String],
) -> Result<Vec<TemplateVariantRow>, sqlx::Error> {
    if template_names.is_empty() {
        return Ok(Vec::new());
    }

    sqlx::query_as::<_, TemplateVariantRow>(sql!(
        "SELECT *
         FROM (
             SELECT DISTINCT ON (template_name, content_hash, COALESCE(runtime, ''))
                    template_name,
                    content_hash,
                    runtime,
                    arch,
                    built_at,
                    artifact_size_bytes,
                    has_base_snapshot,
                    base_snapshot_size_bytes,
                    snapshots,
                    vcpu,
                    memory_mib,
                    disk_size_mb
             FROM node_template_inventory
             WHERE template_name = ANY($1)
               AND content_hash <> ''
             ORDER BY template_name,
                      content_hash,
                      COALESCE(runtime, ''),
                      observed_at DESC
         ) variants
         ORDER BY template_name ASC, built_at DESC NULLS LAST, content_hash ASC"
    ))
    .bind(template_names)
    .fetch_all(pool)
    .await
}

fn public_templates(
    rows: Vec<TemplateRow>,
    variant_rows: Vec<TemplateVariantRow>,
) -> Vec<PublicTemplate> {
    let mut variants_by_template: BTreeMap<String, Vec<PublicTemplateVariant>> = BTreeMap::new();
    for row in variant_rows {
        variants_by_template
            .entry(row.template_name.clone())
            .or_default()
            .push(public_template_variant(row));
    }

    rows.into_iter()
        .map(|row| PublicTemplate {
            variants: variants_by_template.remove(&row.name).unwrap_or_default(),
            name: row.name,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .collect()
}

fn template_page_rows(
    mut rows: Vec<TemplateRow>,
    limit: u16,
) -> Result<(Vec<TemplateRow>, Option<PageCursor>), PublicApiError> {
    let has_more = rows.len() > usize::from(limit);
    if has_more {
        rows.truncate(usize::from(limit));
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|row| encode_cursor(CursorKind::Template, row.name.clone()))
            .transpose()?
    } else {
        None
    };

    Ok((rows, next_cursor))
}

fn public_template_variant(row: TemplateVariantRow) -> PublicTemplateVariant {
    let runtime = row
        .runtime
        .as_deref()
        .and_then(|runtime| RuntimeKind::parse(runtime).ok());
    PublicTemplateVariant {
        reference: format!("{}@{}", row.template_name, row.content_hash),
        content_hash: row.content_hash,
        runtime,
        arch: row.arch,
        built_at: row.built_at,
        artifact_size_bytes: non_negative_u64(row.artifact_size_bytes),
        has_base_snapshot: row.has_base_snapshot,
        base_snapshot_size_bytes: non_negative_u64(row.base_snapshot_size_bytes),
        snapshots: row.snapshots.0,
        resources: SandboxResources {
            vcpu: positive_u32(row.vcpu),
            memory_mib: positive_u32(row.memory_mib),
            disk_mib: positive_u32(row.disk_size_mb),
        },
    }
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
