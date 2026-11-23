//! Accepting a worker's report on its pending round: the round's evidence
//! moves from pending to accepted in one transaction, or nothing is written.
//! A duplicate or a refusal rolls back before any update, so the row and its
//! `updated_at` stay as they were.
//!
//! Locks, in order: the node's assignment row (shared), then its observation
//! row, as [`open_round`](crate::open_round) takes them. Freshness is judged
//! on the clock reading of the write, after both locks.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use serde_json::{Map, Value};
use sqlx::{Connection, PgConnection};

use crate::{RevisionId, roll_back};

/// Most slots one report may describe: one past node-mgr's largest pool,
/// `MAX_IPV4_NETWORK_SLOTS`.
const MAX_REPORT_SLOTS: usize = 16_384;
/// Most census gaps one report may list: a slot gap per slot, the listing's
/// gap, and as many stray namespaces. A worker with more can't report.
const MAX_CENSUS_GAPS: usize = 2 * MAX_REPORT_SLOTS;
/// The accepted document's only schema version. Any change to the document
/// needs a new one: a retained document would otherwise compare as a conflict.
const DOCUMENT_VERSION: u64 = 1;

/// Why a report isn't well formed.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidReport {
    #[error("{field} {value} is larger than the largest stored integer")]
    OutOfRange { field: &'static str, value: u64 },
    #[error("{0} slots is more than {MAX_REPORT_SLOTS}")]
    TooManySlots(usize),
    #[error("{0} census gaps is more than {MAX_CENSUS_GAPS}")]
    TooManyGaps(usize),
    #[error("slot {0} is reported more than once")]
    DuplicateSlot(u32),
    #[error("an incomplete census lists no gap")]
    NoGaps,
}

/// Whether the worker accounted for every managed namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Census {
    Complete,
    /// What couldn't be accounted for, whatever the slots say.
    Incomplete(Vec<CensusGap>),
}

/// Why a census is incomplete. Ordered as the document lists them.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CensusGap {
    InventoryFailed,
    /// The host names no directory only managed namespaces occupy.
    Unowned,
    ListingFailed,
    /// An eligible slot has no namespace.
    Missing(u32),
    /// An eligible slot wasn't read at its final lifecycle.
    Unread(u32),
    /// A managed namespace no slot accounts for.
    Extra,
}

impl CensusGap {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InventoryFailed => "inventory_failed",
            Self::Unowned => "unowned",
            Self::ListingFailed => "listing_failed",
            Self::Missing(_) => "missing",
            Self::Unread(_) => "unread",
            Self::Extra => "extra",
        }
    }
}

/// What the worker observed in one slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotOutcome {
    Verified,
    /// Mismatched while apply work for the slot was pending.
    Applying,
    Failed(Failure),
    Unknown(UnknownReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    RulesMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownReason {
    /// Not a slot the apply takes.
    Ineligible,
    NamespaceMissing,
    NamespaceUnreadable,
    TableUnreadable,
    /// Read, but not shown unchanged at the end.
    Unconfirmed,
}

impl SlotOutcome {
    /// The outcome's name and its reason, if it has one.
    const fn names(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::Verified => ("verified", None),
            Self::Applying => ("applying", None),
            Self::Failed(Failure::RulesMismatch) => ("failed", Some("rules_mismatch")),
            Self::Unknown(reason) => (
                "unknown",
                Some(match reason {
                    UnknownReason::Ineligible => "ineligible",
                    UnknownReason::NamespaceMissing => "namespace_missing",
                    UnknownReason::NamespaceUnreadable => "namespace_unreadable",
                    UnknownReason::TableUnreadable => "table_unreadable",
                    UnknownReason::Unconfirmed => "unconfirmed",
                }),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotEvidence {
    pub slot_id: u32,
    /// The slot's lifecycle generation when observed.
    pub slot_generation: u64,
    pub outcome: SlotOutcome,
}

/// A well-formed answer to one round, with its canonical document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    round: i64,
    assignment_generation: i64,
    revision_id: RevisionId,
    manager_epoch: i64,
    document: String,
}

impl Report {
    /// Slots and gaps may come in any order; the document sorts both.
    pub fn new(
        round: u64,
        assignment_generation: u64,
        revision_id: RevisionId,
        manager_epoch: u64,
        census: Census,
        mut slots: Vec<SlotEvidence>,
    ) -> Result<Self, InvalidReport> {
        if slots.len() > MAX_REPORT_SLOTS {
            return Err(InvalidReport::TooManySlots(slots.len()));
        }
        slots.sort_by_key(|slot| slot.slot_id);
        if let Some(slot) = slots.windows(2).find_map(|pair| match pair {
            [first, second] if first.slot_id == second.slot_id => Some(first.slot_id),
            _ => None,
        }) {
            return Err(InvalidReport::DuplicateSlot(slot));
        }
        let slots = slots
            .iter()
            .map(|slot| {
                let (outcome, reason) = slot.outcome.names();
                let mut entry = Map::new();
                entry.insert("slot_id".into(), slot.slot_id.into());
                entry.insert(
                    "slot_generation".into(),
                    stored("slot_generation", slot.slot_generation)?.into(),
                );
                entry.insert("outcome".into(), outcome.into());
                if let Some(reason) = reason {
                    entry.insert("reason".into(), reason.into());
                }
                Ok(Value::Object(entry))
            })
            .collect::<Result<Vec<_>, InvalidReport>>()?;
        let (round, assignment_generation, manager_epoch) = (
            stored("round", round)?,
            stored("assignment_generation", assignment_generation)?,
            stored("manager_epoch", manager_epoch)?,
        );
        let mut document = Map::new();
        document.insert("schema_version".into(), DOCUMENT_VERSION.into());
        document.insert("round".into(), round.into());
        document.insert("assignment_generation".into(), assignment_generation.into());
        document.insert("revision_id".into(), revision_id.to_string().into());
        document.insert("manager_epoch".into(), manager_epoch.into());
        match census {
            Census::Complete => {
                document.insert("census".into(), "complete".into());
            }
            Census::Incomplete(mut gaps) => {
                if gaps.is_empty() {
                    return Err(InvalidReport::NoGaps);
                }
                if gaps.len() > MAX_CENSUS_GAPS {
                    return Err(InvalidReport::TooManyGaps(gaps.len()));
                }
                gaps.sort_unstable();
                let gaps = gaps.into_iter().map(|gap| {
                    let mut entry = Map::new();
                    entry.insert("gap".into(), gap.as_str().into());
                    if let CensusGap::Missing(slot_id) | CensusGap::Unread(slot_id) = gap {
                        entry.insert("slot_id".into(), slot_id.into());
                    }
                    Value::Object(entry)
                });
                document.insert("census".into(), "incomplete".into());
                document.insert("census_gaps".into(), gaps.collect());
            }
        }
        document.insert("slots".into(), slots.into());
        Ok(Self {
            round,
            assignment_generation,
            revision_id,
            manager_epoch,
            document: Value::Object(document).to_string(),
        })
    }
}

/// Every stored integer is a `BIGINT`.
fn stored(field: &'static str, value: u64) -> Result<i64, InvalidReport> {
    i64::try_from(value).map_err(|_| InvalidReport::OutOfRange { field, value })
}

/// What [`accept_report`] did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Acceptance {
    /// This call stored the report and committed.
    Accepted,
    /// The accepted report is this one: an earlier call stored it. It needn't
    /// be current. Nothing changed.
    Duplicate,
    /// Nothing changed.
    Refused(ReportRefusal),
}

/// Why a report wasn't accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportRefusal {
    /// The report's round isn't the node's pending round.
    NotPending,
    /// A different report for this round was already accepted.
    Conflict,
    /// The node's observations belong to another manager epoch.
    EpochMismatch,
    /// The pending round asks about another assignment generation.
    GenerationMismatch,
    /// The node's assignment is no longer the generation and revision reported.
    Superseded,
    /// The round was issued `max_age` or longer ago.
    Expired,
}

/// Accept `report` as `node`'s answer to its pending round. Commits only when
/// it returns [`Acceptance::Accepted`].
///
/// Accepted under the locks: the observations' manager epoch, pending round
/// and the generation it asks about all match the report, and the node is
/// still assigned the reported generation and revision. The round must have
/// been issued less than `max_age` before the write's clock reading. A missing
/// observation row is never created.
pub async fn accept_report(
    conn: &mut PgConnection,
    node: NodeId,
    report: &Report,
    max_age: Duration,
) -> Result<Acceptance, sqlx::Error> {
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    match accept(&mut tx, node, report, max_age).await {
        Ok(Acceptance::Accepted) => {
            tx.commit().await?;
            Ok(Acceptance::Accepted)
        }
        other => {
            roll_back(tx).await;
            other
        }
    }
}

async fn accept(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node: NodeId,
    report: &Report,
    max_age: Duration,
) -> Result<Acceptance, sqlx::Error> {
    // Shared, as the opener takes it: no supersede commits while this waits.
    let current = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT assignment_generation = $2 AND revision_id = $3
         FROM firewall_rules_node_assignments
         WHERE node_id = $1
         FOR SHARE"
    ))
    .bind(node)
    .bind(report.assignment_generation)
    .bind(report.revision_id)
    .fetch_optional(&mut **tx)
    .await?;
    // Read after any wait for the lock, so a racing acceptance is seen.
    let observed = sqlx::query_as::<_, (i64, Option<i64>, Option<i64>, Option<i64>, bool)>(sql!(
        "SELECT manager_epoch, pending_round, pending_assignment_generation, accepted_round,
                coalesce(accepted_report = $2::jsonb, false)
         FROM firewall_rules_node_observations
         WHERE node_id = $1
         FOR UPDATE"
    ))
    .bind(node)
    .bind(&report.document)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((epoch, pending, asked, accepted, duplicate)) = observed else {
        return Ok(Acceptance::Refused(ReportRefusal::NotPending));
    };
    // jsonb equality ignores formatting and key order.
    if duplicate {
        return Ok(Acceptance::Duplicate);
    }
    let refusal = if accepted == Some(report.round) {
        Some(ReportRefusal::Conflict)
    } else if epoch != report.manager_epoch {
        Some(ReportRefusal::EpochMismatch)
    } else if pending != Some(report.round) {
        Some(ReportRefusal::NotPending)
    } else if asked != Some(report.assignment_generation) {
        Some(ReportRefusal::GenerationMismatch)
    } else if current != Some(true) {
        Some(ReportRefusal::Superseded)
    } else {
        None
    };
    if let Some(refusal) = refusal {
        return Ok(Acceptance::Refused(refusal));
    }
    // clock_timestamp(), not now(): now() is from before the lock waits.
    let written = sqlx::query(sql!(
        "UPDATE firewall_rules_node_observations
         SET accepted_round = pending_round,
             accepted_round_issued_at = pending_round_issued_at,
             accepted_report = $2::jsonb,
             pending_round = NULL,
             pending_round_issued_at = NULL,
             pending_assignment_generation = NULL
         WHERE node_id = $1
           AND clock_timestamp() < pending_round_issued_at + make_interval(secs => $3)"
    ))
    .bind(node)
    .bind(&report.document)
    .bind(max_age.as_secs_f64())
    .execute(&mut **tx)
    .await?;
    Ok(if written.rows_affected() == 1 {
        Acceptance::Accepted
    } else {
        Acceptance::Refused(ReportRefusal::Expired)
    })
}
