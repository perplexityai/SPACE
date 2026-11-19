//! Starting a rollout: saving the exact reviewed revision and the fixed set
//! of nodes that should receive it. A start adopts nothing and delivers
//! nothing: the enrollment default stays as it was, and no assignment,
//! observation, lease or wakeup is written. A completion that proves the
//! rollout adopts its revision with [`adopt_rollout_revision`]; an abort
//! leaves the default alone.
//!
//! Firewall writers serialize on the `firewall_rules_control` row, locked
//! first. A start then checks the slot, freezes its targets, and inserts the
//! revision, the header and the items. An enrollment writer must lock the
//! row `FOR SHARE` or stronger before it enrolls a node, so a node is either
//! in a start's targets or enrolled after that start commits. A start locks
//! no existing header or item, and the lease, renewal and operator verbs
//! never take the control row, so none of them waits on a start in a cycle.
//!
//! A lost response is an unknown outcome. Nothing records a receipt or
//! retries; repeating a start while the first is unfinished is refused by
//! the slot.

use std::fmt;

use pplx_space_accounts::{NodeId, UserId};
use pplx_space_firewall_rules_model::{FirewallRuleRevision, RuleRevisionDiff, diff};
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision, encode_revision};
use pplx_space_rollout_queue::HeaderPhase;
use pplx_space_sql_locator::sql;
use sqlx::{Connection, PgConnection, Postgres, Transaction};

use crate::{ItemPhase, RevisionId, RolloutId, end_transaction, rollout_phase};

#[cfg(test)]
mod tests;

/// The unique key that admits one unfinished rollout per database.
const UNFINISHED_SLOT_KEY: &str = "firewall_rules_rollouts_unfinished_slot_key";

/// The enrollment default a start expects, or the one it found. Neither is a
/// wildcard: [`Self::NoRevision`] matches only an unset default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdoptedRevision {
    NoRevision,
    Revision(RevisionId),
}

impl fmt::Display for AdoptedRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRevision => f.write_str("no revision"),
            Self::Revision(id) => write!(f, "revision {id}"),
        }
    }
}

impl From<Option<RevisionId>> for AdoptedRevision {
    fn from(id: Option<RevisionId>) -> Self {
        id.map_or(Self::NoRevision, Self::Revision)
    }
}

/// An operator's request to roll a revision out.
#[derive(Clone, Debug)]
pub struct StartRollout {
    /// The trusted operator, recorded as the header's creator and updater.
    pub actor: UserId,
    /// Why: 1 to 1024 characters, which the schema checks.
    pub reason: String,
    /// The complete candidate. It must pass the protocol's strict decoder.
    pub revision: FirewallRuleRevision,
    /// The default the operator reviewed the candidate against.
    pub expected_adopted: AdoptedRevision,
}

/// A committed start. Its rollout proves nothing yet, and one with no
/// targets never will: completing it isn't evidence for adoption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartedRollout {
    pub revision_id: RevisionId,
    pub rollout_id: RolloutId,
    /// The header's generation as committed, for later `If-Match`.
    pub generation: u64,
    pub review: Review,
}

/// The change the start makes once adopted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Review {
    /// Nothing is adopted, so every candidate rule is new. There's no base,
    /// not even an empty one.
    FirstPush,
    /// The candidate against the default read under the start's lock.
    Against {
        base: RevisionId,
        diff: RuleRevisionDiff,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Invalid(DecodeError),
    /// The default isn't the one expected; nothing was written.
    #[error("the adopted firewall default is {adopted}, not the expected one")]
    Stale { adopted: AdoptedRevision },
    #[error("a firewall rules rollout is already in progress")]
    RolloutInProgress,
    #[error("node {0} can't enforce the candidate firewall revision")]
    Unqualified(NodeId),
    #[error("starting a firewall rules rollout is disabled: {0}")]
    NotEnabled(&'static str),
    #[error("stored firewall revision {id} doesn't decode: {source}")]
    StoredRevision { id: RevisionId, source: DecodeError },
    #[error("the firewall_rules_control row is missing")]
    MissingControlRow,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

impl StartError {
    /// Only the slot key means another rollout holds the slot.
    fn from_header_insert(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &error
            && db.is_unique_violation()
            && db.constraint() == Some(UNFINISHED_SLOT_KEY)
        {
            return Self::RolloutInProgress;
        }
        Self::Database(error)
    }
}

/// Who a start targets and whether they can enforce its candidate.
pub(crate) trait Targets {
    /// Every enrolled node, offline ones included, read once under the
    /// control lock.
    async fn enrolled(&mut self, tx: &mut PgConnection) -> Result<Vec<NodeId>, StartError>;

    /// Refuse unless every node can enforce the whole candidate, rules
    /// unchanged from the default included. No I/O: the control lock is held.
    fn qualify(&self, candidate: &FirewallRuleRevision, nodes: &[NodeId])
    -> Result<(), StartError>;
}

const UNAPPROVED: &str = "no firewall enrollment predicate or target qualification is approved";

/// Production's targets until an enrollment predicate and a qualification
/// contract are approved: refused, with no fallback, before anything is
/// written.
struct Unapproved;

impl Targets for Unapproved {
    async fn enrolled(&mut self, _tx: &mut PgConnection) -> Result<Vec<NodeId>, StartError> {
        Err(StartError::NotEnabled(UNAPPROVED))
    }

    fn qualify(&self, _: &FirewallRuleRevision, _: &[NodeId]) -> Result<(), StartError> {
        Err(StartError::NotEnabled(UNAPPROVED))
    }
}

/// Start rolling `start.revision` out as a canary, if the adopted default is
/// `start.expected_adopted` and no rollout is unfinished. Commits before it
/// returns; on any error nothing is written.
pub async fn start_rollout(
    conn: &mut PgConnection,
    start: &StartRollout,
) -> Result<StartedRollout, StartError> {
    start_with(conn, start, &mut Unapproved).await
}

pub(crate) async fn start_with(
    conn: &mut PgConnection,
    start: &StartRollout,
    targets: &mut impl Targets,
) -> Result<StartedRollout, StartError> {
    // A revision built in code may break nft's limits, which only the
    // decoder checks.
    let document = encode_revision(&start.revision);
    let candidate = decode_revision(&document).map_err(StartError::Invalid)?;
    // Its own transaction at READ COMMITTED, so each read after the control
    // lock sees what the lock's previous holder committed.
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    let started = start_in(&mut tx, start, &candidate, &document, targets).await;
    end_transaction(tx, started).await
}

async fn start_in(
    tx: &mut PgConnection,
    start: &StartRollout,
    candidate: &FirewallRuleRevision,
    document: &str,
    targets: &mut impl Targets,
) -> Result<StartedRollout, StartError> {
    let adopted = lock_control(&mut *tx)
        .await?
        .ok_or(StartError::MissingControlRow)?;
    let adopted = AdoptedRevision::from(adopted);
    if adopted != start.expected_adopted {
        return Err(StartError::Stale { adopted });
    }
    // The schema keeps the slot exactly while canary, rolling or paused.
    let in_progress = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM firewall_rules_rollouts WHERE unfinished_slot IS NOT NULL)"
    ))
    .fetch_one(&mut *tx)
    .await?;
    if in_progress {
        return Err(StartError::RolloutInProgress);
    }
    let review = match adopted {
        AdoptedRevision::NoRevision => Review::FirstPush,
        AdoptedRevision::Revision(base) => Review::Against {
            base,
            diff: diff(&read_revision(&mut *tx, base).await?, candidate),
        },
    };

    let nodes = targets.enrolled(&mut *tx).await?;
    targets.qualify(candidate, &nodes)?;

    let revision_id = RevisionId(uuid::Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, ($2::jsonb ->> 'schema_version')::bigint, $2::jsonb -> 'rules')"
    ))
    .bind(revision_id)
    .bind(document)
    .execute(&mut *tx)
    .await?;
    let rollout_id = RolloutId(uuid::Uuid::now_v7());
    let generation = sqlx::query_scalar::<_, i64>(sql!(
        "INSERT INTO firewall_rules_rollouts (
             id, to_revision_id, phase, unfinished_slot, reason,
             created_by_user_id, updated_by_user_id
         )
         VALUES ($1, $2, $3, 1, $4, $5, $5)
         RETURNING generation"
    ))
    .bind(rollout_id)
    .bind(revision_id)
    .bind(rollout_phase(HeaderPhase::Canary))
    .bind(&start.reason)
    .bind(start.actor)
    .fetch_one(&mut *tx)
    .await
    .map_err(StartError::from_header_insert)?;
    // Attempts, lease, issuance and schedule keep their empty defaults.
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_rollout_items (rollout_id, node_id, phase)
         SELECT $1, node, $3 FROM unnest($2::uuid[]) AS node"
    ))
    .bind(rollout_id)
    .bind(nodes.iter().map(|node| node.as_uuid()).collect::<Vec<_>>())
    .bind(ItemPhase::Pending.as_str())
    .execute(&mut *tx)
    .await?;
    Ok(StartedRollout {
        revision_id,
        rollout_id,
        generation: u64::try_from(generation).unwrap_or(0),
        review,
    })
}

/// Lock the control row and read the default. `None` when the row is
/// missing, an invariant break, never seeded here.
async fn lock_control(tx: &mut PgConnection) -> Result<Option<Option<RevisionId>>, sqlx::Error> {
    sqlx::query_scalar::<_, Option<RevisionId>>(sql!(
        "SELECT enrollment_default_revision_id
         FROM firewall_rules_control
         WHERE singleton
         FOR UPDATE"
    ))
    .fetch_optional(tx)
    .await
}

/// Read revision `id` back through the protocol's strict decoder.
pub(crate) async fn read_revision(
    executor: impl sqlx::PgExecutor<'_>,
    id: RevisionId,
) -> Result<FirewallRuleRevision, StartError> {
    let document = sqlx::query_scalar::<_, String>(sql!(
        "SELECT jsonb_build_object(
                    'schema_version', ruleset_format_version, 'rules', ruleset)::text
         FROM firewall_rules_revisions
         WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(executor)
    .await?;
    decode_revision(&document).map_err(|source| StartError::StoredRevision { id, source })
}

#[derive(Debug, thiserror::Error)]
pub enum AdoptError {
    #[error("the firewall_rules_control row is missing")]
    MissingControlRow,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Make `rollout`'s revision the enrollment default within the caller's
/// transaction, which this neither commits nor rolls back. For the
/// completion that has proved the rollout: it must also finish the header
/// in the same transaction, after this, since a finished rollout is never
/// adopted and so an aborted or superseded one can't roll the default back.
/// Locks the control row, then the header, as a start does, so call it
/// before locking the header. `None`, writing nothing, when the rollout is
/// missing or finished: then don't complete.
pub async fn adopt_rollout_revision(
    tx: &mut Transaction<'_, Postgres>,
    rollout: RolloutId,
) -> Result<Option<RevisionId>, AdoptError> {
    lock_control(tx)
        .await?
        .ok_or(AdoptError::MissingControlRow)?;
    let Some(revision) = sqlx::query_scalar::<_, RevisionId>(sql!(
        "SELECT to_revision_id
         FROM firewall_rules_rollouts
         WHERE id = $1
           AND finished_at IS NULL
         FOR UPDATE"
    ))
    .bind(rollout)
    .fetch_optional(&mut **tx)
    .await?
    else {
        return Ok(None);
    };
    sqlx::query(sql!(
        "UPDATE firewall_rules_control SET enrollment_default_revision_id = $1 WHERE singleton"
    ))
    .bind(revision)
    .execute(&mut **tx)
    .await?;
    Ok(Some(revision))
}
