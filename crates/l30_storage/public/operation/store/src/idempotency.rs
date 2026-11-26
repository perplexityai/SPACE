//! `public_idempotency_records`: what a request recorded under its
//! idempotency key, for its retries to replay.
//!
//! Besides the plain reads and inserts, this owns the reads that compare a
//! record's expiry with the database clock, the ones that lock the live
//! record until the caller's transaction ends, the expiry deletes, a key
//! binding's renewal, the writes guarded on a record's recorded response,
//! and the advisory lock ([`lock_key`]) that serializes a key's writers,
//! whose key the caller derives. Each runs in the executor its caller
//! passes.

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use serde::Serialize;
use sqlx::{FromRow, types::Json};
use time::OffsetDateTime;
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
    executor: impl Executor<'_>,
    key: IdempotencyKeyRef<'_>,
) -> Result<Option<RecordedReply<T>>, DbError>
where
    T: serde::de::DeserializeOwned + Send + Unpin + 'static,
{
    Ok(sqlx::query_as(sql!(
        "SELECT request_fingerprint AS fingerprint, response_json AS response
         FROM public_idempotency_records
         WHERE org_id = $1 AND api_product = 'native' AND method = $4
           AND path_template = $2 AND idempotency_key = $3"
    ))
    .bind(key.org_id.as_uuid())
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(key.method)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Record `response` under the native API's `key` as record `id`, never to
/// expire, unless a record already holds the key. `false` means one did, and
/// nothing was recorded.
pub async fn insert_permanent_native_reply<T: Serialize + Sync>(
    executor: impl Executor<'_>,
    id: Uuid,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    response: &T,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Record `response_json` under `api_product`'s `key` as record `id`, with
/// the default expiry. Fails on a record that already holds the key; the
/// caller deletes an expired one first under the key's lock.
pub async fn insert_reply<T: Serialize + Sync>(
    executor: impl Executor<'_>,
    id: Uuid,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    response_json: &T,
) -> Result<(), DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Record `response_json` under the native API's `POST` key at
/// `path_template` as record `id`, with the default expiry, unless a record
/// already holds the key. `false` means one did, and nothing was recorded.
pub async fn insert_native_post_reply_if_absent(
    executor: impl Executor<'_>,
    id: Uuid,
    org_id: OrgId,
    path_template: &str,
    idempotency_key: &str,
    request_fingerprint: &str,
    response_json: serde_json::Value,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(inserted.rows_affected() == 1)
}

/// Record that `operation_id` was created under `api_product`'s `key` as
/// record `id`, expiring `ttl_hours` after now by the database clock.
pub async fn insert_for_operation(
    executor: impl Executor<'_>,
    id: Uuid,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    operation_id: Uuid,
    ttl_hours: i64,
) -> Result<(), DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete the records of the operations `operation_ids`, returning the ids
/// of the records deleted.
pub async fn delete_for_operations(
    executor: impl Executor<'_>,
    operation_ids: &[Uuid],
) -> Result<Vec<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_idempotency_records
         WHERE operation_id = ANY($1::UUID[])
         RETURNING id"
    ))
    .bind(operation_ids)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The operation a live record names, if any, and the fingerprint of the
/// request that recorded it.
#[derive(Debug, FromRow)]
pub struct OperationKeyRow {
    pub operation_id: Option<Uuid>,
    pub request_fingerprint: String,
}

/// The live record under `api_product`'s `key`: one that has not expired by
/// the database clock.
pub async fn find_live(
    executor: impl Executor<'_>,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
) -> Result<Option<OperationKeyRow>, DbError> {
    Ok(sqlx::query_as::<_, OperationKeyRow>(sql!(
        "SELECT operation_id, request_fingerprint
             FROM public_idempotency_records
             WHERE org_id = $1
               AND api_product = $2
               AND method = $3
               AND path_template = $4
               AND idempotency_key = $5
               AND expires_at > NOW()"
    ))
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// [`find_live`], locking the record it finds until the caller's
/// transaction ends.
/// The caller must pass its open transaction.
pub async fn lock_live<C>(
    conn: &mut C,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
) -> Result<Option<OperationKeyRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, OperationKeyRow>(sql!(
        "SELECT operation_id, request_fingerprint
         FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = $2
           AND method = $3
           AND path_template = $4
           AND idempotency_key = $5
           AND expires_at > NOW()
         FOR UPDATE"
    ))
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// Delete the record under `api_product`'s `key` if it has expired by the
/// database clock, and lock the live one, if any, until the caller's
/// transaction ends. The key is unique, so the two never coexist: the
/// delete happens only when no live record comes back. `NOW()` is the
/// transaction's start, so the two predicates partition the record exactly.
/// The caller must pass its open transaction.
pub async fn delete_expired_and_lock_live<C>(
    conn: &mut C,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
) -> Result<Option<OperationKeyRow>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, OperationKeyRow>(sql!(
        "WITH expired AS (
             DELETE FROM public_idempotency_records
             WHERE org_id = $1
               AND api_product = $2
               AND method = $3
               AND path_template = $4
               AND idempotency_key = $5
               AND expires_at <= NOW()
         )
         SELECT operation_id, request_fingerprint
         FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = $2
           AND method = $3
           AND path_template = $4
           AND idempotency_key = $5
           AND expires_at > NOW()
         FOR UPDATE"
    ))
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// Delete the record under `api_product`'s `key` if it has expired by the
/// database clock.
pub async fn delete_expired(
    executor: impl Executor<'_>,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = $2
           AND method = $3
           AND path_template = $4
           AND idempotency_key = $5
           AND expires_at <= NOW()"
    ))
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete up to `limit` records expired by the database clock, oldest
/// first, returning the ids deleted.
///
/// The batch's ids are collected first, as an array, so the delete finds
/// them through the primary key rather than joining the whole table. The
/// outer `expires_at` check is what a selected row is held to once its
/// lock is free: a record renewed while this waits on it is rechecked as
/// renewed and kept.
pub async fn delete_expired_batch(
    executor: impl Executor<'_>,
    limit: i64,
) -> Result<Vec<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_idempotency_records
         WHERE id = ANY(ARRAY(
             SELECT id
             FROM public_idempotency_records
             WHERE expires_at <= NOW()
             ORDER BY expires_at ASC, id ASC
             LIMIT $1
         ))
           AND expires_at <= NOW()
         RETURNING id"
    ))
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Bind the native API's `key` to `request_fingerprint` as record `id`
/// until `ttl_minutes` after now by the database clock. With no record
/// under the key, this inserts one whose response is `{}`. A record
/// already under the key is taken over when it has expired or holds the
/// same fingerprint: it takes `id`, `request_fingerprint` and the new
/// expiry. `false` means a live record holds another fingerprint, and
/// nothing changed.
pub async fn bind_native_key(
    executor: impl Executor<'_>,
    id: Uuid,
    key: IdempotencyKeyRef<'_>,
    request_fingerprint: &str,
    ttl_minutes: i32,
) -> Result<bool, DbError> {
    let bound = sqlx::query_scalar::<_, Uuid>(sql!(
        "INSERT INTO public_idempotency_records (
             id, org_id, api_product, method, path_template, idempotency_key,
             request_fingerprint, response_json, expires_at
         )
         VALUES (
             $1, $2, 'native', $3, $4, $5, $6, '{}'::jsonb,
             NOW() + make_interval(mins => $7)
         )
         ON CONFLICT (org_id, api_product, method, path_template, idempotency_key) DO UPDATE
         SET id = EXCLUDED.id,
             request_fingerprint = EXCLUDED.request_fingerprint,
             expires_at = EXCLUDED.expires_at
         WHERE public_idempotency_records.expires_at <= NOW()
            OR public_idempotency_records.request_fingerprint = EXCLUDED.request_fingerprint
         RETURNING id"
    ))
    .bind(id)
    .bind(key.org_id)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(request_fingerprint)
    .bind(ttl_minutes)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(bound.is_some())
}

/// A live record's fingerprint and response, if it has one.
#[derive(Debug, FromRow)]
pub struct LiveReplyRow {
    pub request_fingerprint: String,
    pub response_json: Option<Json<serde_json::Value>>,
}

/// The live record under the native API's `key`, by the database clock.
pub async fn find_live_native_reply(
    executor: impl Executor<'_>,
    key: IdempotencyKeyRef<'_>,
) -> Result<Option<LiveReplyRow>, DbError> {
    Ok(sqlx::query_as::<_, LiveReplyRow>(sql!(
        "SELECT request_fingerprint, response_json FROM public_idempotency_records
             WHERE org_id = $1 AND api_product = 'native' AND method = $2
               AND path_template = $3 AND idempotency_key = $4 AND expires_at > NOW()"
    ))
    .bind(key.org_id)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The live record under the native API's `POST` key `idempotency_key` at
/// `path_template` of `org_id`, by the database clock.
pub async fn find_live_native_post_reply(
    executor: impl Executor<'_>,
    org_id: OrgId,
    path_template: &str,
    idempotency_key: &str,
) -> Result<Option<LiveReplyRow>, DbError> {
    Ok(sqlx::query_as::<_, LiveReplyRow>(sql!(
        "SELECT request_fingerprint, response_json
         FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = 'native'
           AND method = 'POST'
           AND path_template = $2
           AND idempotency_key = $3
           AND expires_at > NOW()"
    ))
    .bind(org_id)
    .bind(path_template)
    .bind(idempotency_key)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// A live record as a response reservation reads it.
#[derive(Debug, FromRow)]
pub struct ReservationRow {
    pub request_fingerprint: String,
    pub response_json: Option<Json<serde_json::Value>>,
    pub created_at: OffsetDateTime,
    /// Whether the record is older than the reservation lifetime, by the
    /// database clock.
    pub reservation_expired: bool,
}

/// The live record under `api_product`'s `key`, by the database clock, and
/// whether it was created more than `lifetime_secs` ago.
pub async fn find_live_reservation(
    executor: impl Executor<'_>,
    api_product: &str,
    key: IdempotencyKeyRef<'_>,
    lifetime_secs: f64,
) -> Result<Option<ReservationRow>, DbError> {
    Ok(sqlx::query_as::<_, ReservationRow>(sql!(
        "SELECT request_fingerprint, response_json, created_at,
                    created_at + make_interval(secs => $6) <= NOW() AS reservation_expired
             FROM public_idempotency_records
             WHERE org_id = $1 AND api_product = $2 AND method = $3
               AND path_template = $4 AND idempotency_key = $5 AND expires_at > NOW()"
    ))
    .bind(key.org_id)
    .bind(api_product)
    .bind(key.method)
    .bind(key.path_template)
    .bind(key.idempotency_key)
    .bind(lifetime_secs)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Replace record `id`'s response with `replacement` while it still holds
/// `recorded`. `false` means it no longer did, and nothing changed.
pub async fn replace_recorded_reply<R: Serialize + Sync, N: Serialize + Sync>(
    executor: impl Executor<'_>,
    id: Uuid,
    recorded: &R,
    replacement: &N,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "UPDATE public_idempotency_records SET response_json = $2
             WHERE id = $1 AND response_json = $3"
    ))
    .bind(id)
    .bind(Json(replacement))
    .bind(Json(recorded))
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Delete record `id` while it still holds `recorded`. `false` means it no
/// longer did, and nothing changed.
pub async fn delete_recorded_reply<R: Serialize + Sync>(
    executor: impl Executor<'_>,
    id: Uuid,
    recorded: &R,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "DELETE FROM public_idempotency_records WHERE id = $1 AND response_json = $2"
    ))
    .bind(id)
    .bind(Json(recorded))
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Take the transaction-level advisory lock `key`, which the caller derives
/// from an idempotency key to serialize that key's writers, waiting for it.
/// The lock lasts until the caller's transaction ends.
/// The caller must pass its open transaction.
pub async fn lock_key<C>(conn: &mut C, key: i64) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!("SELECT pg_advisory_xact_lock($1)"))
        .bind(key)
        .execute(conn.into_sqlx())
        .await?;
    Ok(())
}
