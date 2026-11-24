//! `public_idempotency_records`: what a request recorded under its
//! idempotency key, for its retries to replay.
//!
//! The reads that lock a record or compare its expiry with the database
//! clock, and the writes guarded on a record's recorded response, stay at
//! their callers.

use pplx_space_accounts::OrgId;
use pplx_space_sql_locator::sql;
use serde::Serialize;
use sqlx::{FromRow, PgExecutor, types::Json};
use uuid::Uuid;

/// An idempotency key: the record's identity among an org's records.
#[derive(Clone, Copy, Debug)]
pub struct IdempotencyKeyRef<'a> {
    pub org_id: OrgId,
    pub method: &'a str,
    pub path_template: &'a str,
    pub idempotency_key: &'a str,
}

/// A recorded response and the fingerprint of the request that recorded it.
#[derive(Debug, FromRow)]
pub struct RecordedReply<T> {
    pub fingerprint: String,
    pub response: Json<T>,
}

/// The reply recorded under the native API's `key`, expired or not.
pub async fn find_native_reply<T>(
    executor: impl PgExecutor<'_>,
    key: IdempotencyKeyRef<'_>,
) -> Result<Option<RecordedReply<T>>, sqlx::Error>
where
    T: serde::de::DeserializeOwned + Send + Unpin + 'static,
{
    sqlx::query_as(sql!(
        "SELECT request_fingerprint AS fingerprint, response_json AS response
         FROM public_idempotency_records
         WHERE org_id = $1 AND api_product = 'native' AND method = $4
           AND path_template = $2 AND idempotency_key = $3"
    ))
    .bind(key.org_id.as_uuid())
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(key.method)
    .fetch_optional(executor)
    .await
}

/// Record `response` under the native API's `key` as record `id`, never to
/// expire, unless a record already holds the key. `false` means one did, and
/// nothing was recorded.
pub async fn insert_permanent_native_reply<T: Serialize + Sync>(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    response: &T,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO public_idempotency_records (id, org_id, api_product, method, path_template, idempotency_key, request_fingerprint, response_json, expires_at)
         VALUES ($1, $2, 'native', $7, $3, $4, $5, $6, 'infinity')
         ON CONFLICT (org_id, api_product, method, path_template, idempotency_key) DO NOTHING"
    ))
    .bind(id)
    .bind(key.org_id.as_uuid())
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(request_fingerprint)
    .bind(Json(response))
    .bind(key.method)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Record `response_json` under `api_product`'s `key` as record `id`, with
/// the default expiry. Fails on a record that already holds the key; the
/// caller deletes an expired one first under the key's lock.
pub async fn insert_reply<T: Serialize + Sync>(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    response_json: &T,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO public_idempotency_records
         (id, org_id, api_product, method, path_template, idempotency_key,
          request_fingerprint, response_json)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
    ))
    .bind(id)
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(request_fingerprint)
    .bind(Json(response_json))
    .execute(executor)
    .await?;
    Ok(())
}

/// Record `response_json` under the native API's `POST` key at
/// `path_template` as record `id`, with the default expiry, unless a record
/// already holds the key. `false` means one did, and nothing was recorded.
pub async fn insert_native_post_reply_if_absent(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    org_id: OrgId,
    path_template: &str,
    idempotency_key: &str,
    request_fingerprint: &str,
    response_json: serde_json::Value,
) -> Result<bool, sqlx::Error> {
    let inserted = sqlx::query(sql!(
        "INSERT INTO public_idempotency_records (
             id,
             org_id,
             api_product,
             method,
             path_template,
             idempotency_key,
             request_fingerprint,
             response_json
         )
         VALUES ($1, $2, 'native', 'POST', $3, $4, $5, $6)
         ON CONFLICT (org_id, api_product, method, path_template, idempotency_key) DO NOTHING"
    ))
    .bind(id)
    .bind(org_id)
    .bind(path_template)
    .bind(idempotency_key)
    .bind(request_fingerprint)
    .bind(Json(response_json))
    .execute(executor)
    .await?;
    Ok(inserted.rows_affected() == 1)
}

/// Record that `operation_id` was created under `api_product`'s `key` as
/// record `id`, expiring `ttl_hours` after now by the database clock.
pub async fn insert_for_operation(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    operation_id: Uuid,
    ttl_hours: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO public_idempotency_records (
             id,
             org_id,
             api_product,
             method,
             path_template,
             idempotency_key,
             request_fingerprint,
             operation_id,
             expires_at
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8,
             NOW() + ($9::TEXT || ' hours')::INTERVAL
         )"
    ))
    .bind(id)
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(request_fingerprint)
    .bind(operation_id)
    .bind(ttl_hours.to_string())
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete the records of the operations `operation_ids`, returning the ids
/// of the records deleted.
pub async fn delete_for_operations(
    executor: impl PgExecutor<'_>,
    operation_ids: &[Uuid],
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_idempotency_records
         WHERE operation_id = ANY($1::UUID[])
         RETURNING id"
    ))
    .bind(operation_ids)
    .fetch_all(executor)
    .await
}
