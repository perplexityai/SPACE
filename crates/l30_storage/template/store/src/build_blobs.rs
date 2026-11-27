//! The uploaded file blobs an ECW template build reads, keyed by template
//! and files hash (`template_build_blobs`).

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;

/// Record `blob_sha256` as `org_id`'s blob for `files_hash` of `template`.
/// `false` when another organization recorded that mapping.
///
/// The SDK's file hash excludes archive metadata, so same-owner reuploads can
/// have different blob digests even when the file contents are unchanged.
/// Mappings recorded before uploads carried an owner have a NULL org_id; the
/// first owned upload claims them so later writers meet the owner check.
pub async fn record(
    executor: impl Executor<'_>,
    template: &TemplateName,
    files_hash: &str,
    blob_sha256: &str,
    org_id: OrgId,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "INSERT INTO template_build_blobs (template_id, files_hash, blob_sha256, org_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (template_id, files_hash) DO UPDATE
             SET blob_sha256 = EXCLUDED.blob_sha256,
                 org_id = EXCLUDED.org_id
             WHERE template_build_blobs.org_id IS NULL
                OR template_build_blobs.org_id = EXCLUDED.org_id"
    ))
    .bind(template.as_str())
    .bind(files_hash)
    .bind(blob_sha256)
    .bind(org_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() != 0)
}

/// The blob recorded for `files_hash` of `template`, when the row is
/// `org_id`'s or no organization's.
pub async fn blob_sha256(
    executor: impl Executor<'_>,
    template: &TemplateName,
    files_hash: &str,
    org_id: OrgId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        r#"
            SELECT blob_sha256
            FROM template_build_blobs
            WHERE template_id = $1
              AND files_hash = $2
              AND (org_id = $3 OR org_id IS NULL)
            "#
    ))
    .bind(template.as_str())
    .bind(files_hash)
    .bind(org_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?)
}
