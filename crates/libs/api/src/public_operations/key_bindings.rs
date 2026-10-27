use super::*;

/// Bind an `Idempotency-Key` to one request on a route whose result the
/// sandbox, not this store, replays, and report whether the key belongs to
/// that request. A key reused with a different fingerprint answers `false`
/// until its binding has gone unused for ten minutes, which outlasts the
/// sandbox's own replay of a completed operation.
///
/// The record carries no response: the sandbox keeps the result of its own
/// keyed operations, and this binding only stops a different request from
/// reaching it under the same key. A file route may take one binding per
/// request, which outpaces the retention sweep, so each call also deletes a
/// few of the route's own expired bindings, skipping any another call holds.
pub(crate) async fn bind_idempotency_key(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
    idempotency: &CreatePublicOperationIdempotency<'_>,
) -> Result<bool, sqlx::Error> {
    // The prune leaves this key's own row to the upsert: one statement
    // cannot both delete a row and resolve an insert conflict against it.
    let bound = sqlx::query_scalar::<_, Uuid>(sql!(
        "WITH pruned AS (
             DELETE FROM public_idempotency_records
             WHERE id IN (
                 SELECT id
                 FROM public_idempotency_records
                 WHERE expires_at <= NOW()
                   AND api_product = 'native'
                   AND method = $3
                   AND path_template = $4
                   AND NOT (org_id = $2 AND idempotency_key = $5)
                 ORDER BY expires_at ASC
                 LIMIT 4
                 FOR UPDATE SKIP LOCKED
             )
         )
         INSERT INTO public_idempotency_records (
             id, org_id, api_product, method, path_template, idempotency_key,
             request_fingerprint, response_json, expires_at
         )
         VALUES ($1, $2, 'native', $3, $4, $5, $6, '{}'::jsonb, NOW() + INTERVAL '10 minutes')
         ON CONFLICT (org_id, api_product, method, path_template, idempotency_key) DO UPDATE
         SET request_fingerprint = EXCLUDED.request_fingerprint,
             expires_at = EXCLUDED.expires_at
         WHERE public_idempotency_records.expires_at <= NOW()
            OR public_idempotency_records.request_fingerprint = EXCLUDED.request_fingerprint
         RETURNING id"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .bind(idempotency.method.as_str())
    .bind(idempotency.path_template.as_str())
    .bind(idempotency.key.as_str())
    .bind(idempotency.request_fingerprint.as_str())
    .fetch_optional(executor)
    .await?;
    Ok(bound.is_some())
}
