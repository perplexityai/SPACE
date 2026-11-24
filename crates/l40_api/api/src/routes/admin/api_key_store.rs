//! Revoking an organization's API key, a guarded write that stays with its
//! caller. The key lookups, listing and creates are
//! `pplx_space_accounts_store::api_keys` verbs; recording a key's use stays in
//! `crate::auth`.

use pplx_space_accounts::{ApiKeyId, OrgId};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// What [`revoke`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RevokeOutcome {
    Revoked,
    AlreadyRevoked,
    NotFound,
}

/// Revoke an organization's key, keeping its row so it lists as revoked.
///
/// Revocation replaces `key_hash` with `revoked:<id>`, which no SHA-256 hex
/// digest equals, rather than relying on readers to filter on `revoked_at`:
/// every lookup by `key_hash`, including those of releases that predate the
/// column, stops matching the key in the same statement. `revoked_at` is kept
/// from the first revocation.
///
/// One statement, so it composes into the caller's transaction. Its `EXISTS`
/// reads the snapshot from before the `UPDATE`, which revokes without
/// removing the row, so a key this statement revokes still counts as found.
pub(super) async fn revoke(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    api_key_id: ApiKeyId,
) -> Result<RevokeOutcome, sqlx::Error> {
    let (revoked, found) = sqlx::query_as::<_, (bool, bool)>(sql!(
        "WITH revoked AS (
             UPDATE api_keys
                SET revoked_at = NOW(),
                    updated_at = NOW(),
                    key_hash = 'revoked:' || id::text
              WHERE id = $1
                AND org_id = $2
                AND revoked_at IS NULL
          RETURNING id
         )
         SELECT EXISTS(SELECT 1 FROM revoked),
                EXISTS(SELECT 1 FROM api_keys WHERE id = $1 AND org_id = $2)"
    ))
    .bind(api_key_id)
    .bind(org_id)
    .fetch_one(executor)
    .await?;
    Ok(match (revoked, found) {
        (true, _) => RevokeOutcome::Revoked,
        (false, true) => RevokeOutcome::AlreadyRevoked,
        (false, false) => RevokeOutcome::NotFound,
    })
}

#[cfg(test)]
mod tests;
