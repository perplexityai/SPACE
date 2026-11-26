//! Reading the adopted default, the base an operator reviews a candidate
//! against. It takes no lock, so it may be stale at once: a start compares
//! its precondition under the control lock.

use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

use crate::RevisionId;

#[cfg(test)]
mod tests;

/// Stored state that can't be read.
#[derive(Debug, thiserror::Error)]
pub enum StoredStateError {
    #[error("stored firewall revision {id} doesn't decode: {source}")]
    StoredRevision { id: RevisionId, source: DecodeError },
    #[error("the firewall_rules_control row is missing")]
    MissingControlRow,
    #[error(transparent)]
    Database(#[from] DbError),
}

/// The adopted default and its document; `None` when no revision is adopted.
/// An unfinished rollout's candidate is never returned. Two statements, which
/// agree because revisions are immutable. It takes the pool, `&Db` (or the
/// driver's pool during the transition), which runs each statement on
/// whichever connection it hands out.
pub async fn read_adopted(
    pool: impl Executor<'_> + Copy,
) -> Result<Option<(RevisionId, FirewallRuleRevision)>, StoredStateError> {
    let id = sqlx::query_scalar::<_, Option<RevisionId>>(sql!(
        "SELECT enrollment_default_revision_id FROM firewall_rules_control WHERE singleton"
    ))
    .fetch_optional(pool.into_sqlx())
    .await
    .map_err(DbError::from)?
    .ok_or(StoredStateError::MissingControlRow)?;
    let Some(id) = id else {
        return Ok(None);
    };
    Ok(Some((id, read_revision(pool.into_sqlx(), id).await?)))
}

/// Read revision `id` back through the protocol's strict decoder.
pub(crate) async fn read_revision(
    executor: impl sqlx::PgExecutor<'_>,
    id: RevisionId,
) -> Result<FirewallRuleRevision, StoredStateError> {
    let document = sqlx::query_scalar::<_, String>(sql!(
        "SELECT jsonb_build_object(
                    'schema_version', ruleset_format_version, 'rules', ruleset)::text
         FROM firewall_rules_revisions
         WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)?;
    decode_revision(&document).map_err(|source| StoredStateError::StoredRevision { id, source })
}
