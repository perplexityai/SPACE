//! Reading one node's instructions: its assignment, with the revision it
//! names, and the round it's asked to verify, in one statement so they come
//! from one snapshot. It writes nothing and decides no authorization.

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision};
use pplx_space_sql_locator::sql;
use time::OffsetDateTime;

use crate::RevisionId;

#[cfg(test)]
mod tests;

/// What a node is to enforce and verify. Each part may be absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeInstructions {
    pub assignment: Option<Assignment>,
    pub pending_round: Option<PendingRound>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Assignment {
    pub generation: u64,
    pub revision_id: RevisionId,
    pub revision: FirewallRuleRevision,
}

/// A round the node hasn't answered yet, as the control plane issued it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingRound {
    pub round: u64,
    pub issued_at: OffsetDateTime,
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("stored firewall revision {id} doesn't decode: {source}")]
    StoredRevision { id: RevisionId, source: DecodeError },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Read `node`'s assignment and pending round together.
pub async fn fetch_instructions(
    executor: impl sqlx::PgExecutor<'_>,
    node: NodeId,
) -> Result<NodeInstructions, FetchError> {
    // Anchored on the node, so a round without an assignment still returns.
    // The document is rebuilt as `start::read_revision` rebuilds it.
    let (generation, revision_id, document, round, issued_at) = sqlx::query_as::<
        _,
        (
            Option<i64>,
            Option<RevisionId>,
            Option<String>,
            Option<i64>,
            Option<OffsetDateTime>,
        ),
    >(sql!(
        "SELECT a.assignment_generation,
                a.revision_id,
                jsonb_build_object(
                    'schema_version', r.ruleset_format_version, 'rules', r.ruleset)::text,
                o.pending_round,
                o.pending_round_issued_at
         FROM (SELECT $1::uuid AS node_id) n
         LEFT JOIN (firewall_rules_node_assignments a
                    JOIN firewall_rules_revisions r ON r.id = a.revision_id)
             ON a.node_id = n.node_id
         LEFT JOIN firewall_rules_node_observations o ON o.node_id = n.node_id"
    ))
    .bind(node)
    .fetch_one(executor)
    .await?;
    let assignment = match (generation, revision_id, document) {
        (Some(generation), Some(id), Some(document)) => Some(Assignment {
            generation: non_negative(generation)?,
            revision_id: id,
            revision: decode_revision(&document)
                .map_err(|source| FetchError::StoredRevision { id, source })?,
        }),
        _ => None,
    };
    // The schema sets the round and its issue time together.
    let pending_round = match (round, issued_at) {
        (Some(round), Some(issued_at)) => Some(PendingRound {
            round: non_negative(round)?,
            issued_at,
        }),
        _ => None,
    };
    Ok(NodeInstructions {
        assignment,
        pending_round,
    })
}

/// The schema keeps both counters non-negative; report a violation rather
/// than guess.
fn non_negative(value: i64) -> Result<u64, sqlx::Error> {
    u64::try_from(value).map_err(|error| sqlx::Error::Decode(Box::new(error)))
}
