use super::*;

/// How long a file route's key binding lasts after its last use. It outlasts
/// the sandbox's own replay of a completed operation.
pub(crate) const KEY_BINDING_TTL_MINUTES: i32 = 10;

/// Bind an `Idempotency-Key` to one request on a route whose result the
/// sandbox, not this store, replays, and report whether the key belongs to
/// that request. A key reused with a different fingerprint answers `false`
/// until its binding has gone unused for [`KEY_BINDING_TTL_MINUTES`].
///
/// The record carries no response: the sandbox keeps the result of its own
/// keyed operations, and this binding only stops a different request from
/// reaching it under the same key. Expired bindings are left to the retention
/// sweep.
///
/// Every successful bind gives the row a fresh `id`, as replacing a record
/// does elsewhere in this store. A retention sweep selects expired rows by
/// `id` and, if a bind holds the row, waits for it; when it resumes it finds
/// no row with the `id` it selected, so it cannot delete a binding renewed
/// under it, whatever version of the sweep is running.
pub(crate) async fn bind_idempotency_key(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
    idempotency: &CreatePublicOperationIdempotency<'_>,
) -> Result<bool, sqlx::Error> {
    operation_store::idempotency::bind_native_key(
        executor,
        Uuid::now_v7(),
        idempotency.store_key(org_id),
        idempotency.request_fingerprint.as_str(),
        KEY_BINDING_TTL_MINUTES,
    )
    .await
}
