//! Reading one node's instructions: its assignment, with the revision it
//! names, and the round it's asked to verify. Run it in one `REPEATABLE READ`
//! transaction so they come from one snapshot. It writes nothing and decides
//! no authorization.

use jiff::Timestamp;
use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;
use time::OffsetDateTime;

use crate::RevisionId;
use crate::read::{StoredStateError, read_revision};

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
    pub issued_at: Timestamp,
    /// The assignment generation the round asks about, read from the round
    /// rather than the assignment.
    pub assignment_generation: u64,
}

/// Read `node`'s assignment and pending round.
pub async fn fetch_instructions(
    conn: &mut PgConnection,
    node: NodeId,
) -> Result<NodeInstructions, StoredStateError> {
    // Anchored on the node, so a round without an assignment still returns.
    let (generation, revision_id, round, issued_at, round_generation) = sqlx::query_as::<
        _,
        (
            Option<i64>,
            Option<RevisionId>,
            Option<i64>,
            Option<OffsetDateTime>,
            Option<i64>,
        ),
    >(sql!(
        "SELECT a.assignment_generation,
                a.revision_id,
                o.pending_round,
                o.pending_round_issued_at,
                o.pending_assignment_generation
         FROM (SELECT $1::uuid AS node_id) n
         LEFT JOIN firewall_rules_node_assignments a ON a.node_id = n.node_id
         LEFT JOIN firewall_rules_node_observations o ON o.node_id = n.node_id"
    ))
    .bind(node)
    .fetch_one(&mut *conn)
    .await?;
    let assignment = match (generation, revision_id) {
        (Some(generation), Some(id)) => Some(Assignment {
            generation: non_negative(generation)?,
            revision_id: id,
            revision: read_revision(&mut *conn, id).await?,
        }),
        _ => None,
    };
    // The schema sets the round and its issue time together.
    let pending_round = match (round, issued_at) {
        (Some(round), Some(issued_at)) => Some(pending_round(
            round,
            issued_at,
            bound_generation(round_generation)?,
        )?),
        _ => None,
    };
    Ok(NodeInstructions {
        assignment,
        pending_round,
    })
}

/// A pending round as stored, with the generation it asks about, decoded
/// alike for Fetch and the round opener.
pub(crate) fn pending_round(
    round: i64,
    issued_at: OffsetDateTime,
    assignment_generation: u64,
) -> Result<PendingRound, sqlx::Error> {
    Ok(PendingRound {
        round: non_negative(round)?,
        issued_at: Timestamp::from_nanosecond(issued_at.unix_timestamp_nanos())
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
        assignment_generation,
    })
}

/// The generation a pending round names; the schema never stores one without.
pub(crate) fn bound_generation(stored: Option<i64>) -> Result<u64, sqlx::Error> {
    let stored = stored.ok_or_else(|| {
        sqlx::Error::Decode("pending round names no assignment generation".into())
    })?;
    non_negative(stored)
}

/// The schema keeps every counter non-negative; report a violation rather
/// than guess.
fn non_negative(value: i64) -> Result<u64, sqlx::Error> {
    u64::try_from(value).map_err(|error| sqlx::Error::Decode(Box::new(error)))
}
