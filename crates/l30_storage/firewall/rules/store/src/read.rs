//! Reading the adopted default, the base an operator reviews a candidate
//! against. It takes no lock, so it may be stale at once: a start compares
//! its precondition under the control lock.

use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision};
use pplx_space_sql_locator::sql;

use crate::RevisionId;

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum ReadAdoptedError {
    #[error("stored firewall revision {id} doesn't decode: {source}")]
    StoredRevision { id: RevisionId, source: DecodeError },
    #[error("the firewall_rules_control row is missing")]
    MissingControlRow,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// The adopted default and its document, in one statement; `None` when no
/// revision is adopted. An unfinished rollout's candidate is never returned.
pub async fn read_adopted(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<(RevisionId, FirewallRuleRevision)>, ReadAdoptedError> {
    let (id, document) = sqlx::query_as::<_, (Option<RevisionId>, String)>(sql!(
        "SELECT c.enrollment_default_revision_id,
                jsonb_build_object(
                    'schema_version', r.ruleset_format_version, 'rules', r.ruleset)::text
         FROM firewall_rules_control c
         LEFT JOIN firewall_rules_revisions r ON r.id = c.enrollment_default_revision_id
         WHERE c.singleton"
    ))
    .fetch_optional(executor)
    .await?
    .ok_or(ReadAdoptedError::MissingControlRow)?;
    let Some(id) = id else {
        return Ok(None);
    };
    decode_revision(&document)
        .map(|revision| Some((id, revision)))
        .map_err(|source| ReadAdoptedError::StoredRevision { id, source })
}
