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
use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgConnection};

use crate::{RevisionId, roll_back};

/// How long after its round was issued, on the database clock, a report may
/// be accepted. It covers wake delivery, fetch, observation and report. A
/// round this old can't be answered, so [`open_round`](crate::open_round)
/// replaces it rather than return it.
pub const REPORT_MAX_AGE: Duration = Duration::from_secs(300);

/// Most slots one report may describe: one past node-mgr's largest pool,
/// `MAX_IPV4_NETWORK_SLOTS`.
const MAX_REPORT_SLOTS: usize = 16_384;
/// Census gaps one report may list beyond one per slot: each host-wide gap,
/// `InventoryFailed`, `Unowned` and `ListingFailed`, once.
const HOST_GAPS: usize = 3;
/// Most managed namespaces no slot accounts for one report may count, 64
/// times `MAX_REPORT_SLOTS`. A larger count is malformed, not stored.
const MAX_EXTRA_NAMESPACES: u64 = 64 * MAX_REPORT_SLOTS as u64;
/// The accepted document's only schema version. Any change to the document
/// needs a new one: a retained document would otherwise compare as a conflict.
/// No caller stores a report yet, so version 1 may still change until one
/// does.
const DOCUMENT_VERSION: u64 = 1;

/// Why a report isn't well formed.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidReport {
    #[error("{field} {value} is larger than the largest stored integer")]
    OutOfRange { field: &'static str, value: u64 },
    #[error("{0} slots is more than {MAX_REPORT_SLOTS}")]
    TooManySlots(usize),
    #[error("{gaps} census gaps is more than {HOST_GAPS} past the {slots} slots")]
    TooManyGaps { gaps: usize, slots: usize },
    #[error("{0} extra namespaces is more than {MAX_EXTRA_NAMESPACES}")]
    TooManyExtraNamespaces(u64),
    #[error("slot {0} is reported more than once")]
    DuplicateSlot(u32),
    #[error("slot {0} has more than one census gap")]
    RepeatedSlotGap(u32),
    #[error("census gap {0:?} is listed more than once")]
    RepeatedGap(CensusGap),
    #[error("an incomplete census lists no gap and counts no extra namespace")]
    NoGaps,
}

/// Whether the worker accounted for every managed namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Census {
    /// No gap and no extra namespace.
    Complete,
    /// What couldn't be accounted for, whatever the slots say: at least one
    /// gap or extra namespace.
    Incomplete {
        gaps: Vec<CensusGap>,
        /// Managed namespaces no slot accounts for.
        extra_namespaces: u64,
    },
}

/// Why a census is incomplete. Each slot has at most one gap, and each other
/// gap appears at most once. Its serde form is the document's.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "gap", rename_all = "snake_case", deny_unknown_fields)]
pub enum CensusGap {
    InventoryFailed,
    /// The host names no directory only managed namespaces occupy.
    Unowned,
    ListingFailed,
    /// An eligible slot has no namespace.
    Missing {
        slot_id: u32,
    },
    /// An eligible slot wasn't read at its final lifecycle.
    Unread {
        slot_id: u32,
    },
}

impl CensusGap {
    const fn slot_id(self) -> Option<u32> {
        match self {
            Self::Missing { slot_id } | Self::Unread { slot_id } => Some(slot_id),
            Self::InventoryFailed | Self::Unowned | Self::ListingFailed => None,
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

/// Its serde form is the document's.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    RulesMismatch,
}

/// Its serde form is the document's.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownReason {
    /// Not a slot the apply takes.
    Ineligible,
    NamespaceMissing,
    NamespaceUnreadable,
    TableUnreadable,
    /// Read, but not shown unchanged at the end.
    Unconfirmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotEvidence {
    pub slot_id: u32,
    /// The slot's lifecycle generation when observed.
    pub slot_generation: u64,
    pub outcome: SlotOutcome,
}

/// The accepted document, schema version 1. Derived field order is the
/// canonical key order; [`Report::new`] sorts both lists.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReportV1 {
    pub(crate) schema_version: u64,
    pub(crate) round: u64,
    pub(crate) assignment_generation: u64,
    pub(crate) revision_id: uuid::Uuid,
    pub(crate) manager_epoch: u64,
    pub(crate) census: CensusV1,
    /// Empty, like `extra_namespaces` is 0, when the census is complete.
    pub(crate) census_gaps: Vec<CensusGap>,
    pub(crate) extra_namespaces: u64,
    pub(crate) slots: Vec<SlotV1>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CensusV1 {
    Complete,
    Incomplete,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SlotV1 {
    Verified {
        slot_id: u32,
        slot_generation: u64,
    },
    Applying {
        slot_id: u32,
        slot_generation: u64,
    },
    Failed {
        slot_id: u32,
        slot_generation: u64,
        reason: Failure,
    },
    Unknown {
        slot_id: u32,
        slot_generation: u64,
        reason: UnknownReason,
    },
}

impl From<SlotEvidence> for SlotV1 {
    fn from(slot: SlotEvidence) -> Self {
        let SlotEvidence {
            slot_id,
            slot_generation,
            outcome,
        } = slot;
        match outcome {
            SlotOutcome::Verified => Self::Verified {
                slot_id,
                slot_generation,
            },
            SlotOutcome::Applying => Self::Applying {
                slot_id,
                slot_generation,
            },
            SlotOutcome::Failed(reason) => Self::Failed {
                slot_id,
                slot_generation,
                reason,
            },
            SlotOutcome::Unknown(reason) => Self::Unknown {
                slot_id,
                slot_generation,
                reason,
            },
        }
    }
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
    /// Slots and gaps may come in any order; the document sorts both, slots
    /// by ID and gaps host-wide first, then by slot.
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
        for slot in &slots {
            stored("slot_generation", slot.slot_generation)?;
        }
        let (stored_round, stored_generation, stored_epoch) = (
            stored("round", round)?,
            stored("assignment_generation", assignment_generation)?,
            stored("manager_epoch", manager_epoch)?,
        );
        let (census, census_gaps, extra_namespaces) = match census {
            Census::Complete => (CensusV1::Complete, Vec::new(), 0),
            Census::Incomplete {
                mut gaps,
                extra_namespaces,
            } => {
                if gaps.is_empty() && extra_namespaces == 0 {
                    return Err(InvalidReport::NoGaps);
                }
                if extra_namespaces > MAX_EXTRA_NAMESPACES {
                    return Err(InvalidReport::TooManyExtraNamespaces(extra_namespaces));
                }
                if gaps.len() > slots.len() + HOST_GAPS {
                    return Err(InvalidReport::TooManyGaps {
                        gaps: gaps.len(),
                        slots: slots.len(),
                    });
                }
                // A slot's gaps sort together, so a second one is adjacent.
                gaps.sort_unstable_by_key(|&gap| (gap.slot_id(), gap));
                if let Some(first) = gaps.windows(2).find_map(|pair| match pair {
                    [first, second]
                        if first == second
                            || (first.slot_id().is_some()
                                && first.slot_id() == second.slot_id()) =>
                    {
                        Some(*first)
                    }
                    _ => None,
                }) {
                    return Err(match first.slot_id() {
                        Some(slot) => InvalidReport::RepeatedSlotGap(slot),
                        None => InvalidReport::RepeatedGap(first),
                    });
                }
                (CensusV1::Incomplete, gaps, extra_namespaces)
            }
        };
        let document = ReportV1 {
            schema_version: DOCUMENT_VERSION,
            round,
            assignment_generation,
            revision_id: revision_id.0,
            manager_epoch,
            census,
            census_gaps,
            extra_namespaces,
            slots: slots.into_iter().map(SlotV1::from).collect(),
        };
        let document = match serde_json::to_string(&document) {
            Ok(text) => text,
            // Every map key is a string and no type here returns an error, so
            // writing to a String cannot fail.
            Err(error) => unreachable!("report serialization failed: {error}"),
        };
        Ok(Self {
            round: stored_round,
            assignment_generation: stored_generation,
            revision_id,
            manager_epoch: stored_epoch,
            document,
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
    /// The round was issued [`REPORT_MAX_AGE`] or longer ago.
    Expired,
}

/// Accept `report` as `node`'s answer to its pending round. Commits only when
/// it returns [`Acceptance::Accepted`].
///
/// Accepted under the locks: the observations' manager epoch, pending round
/// and the generation it asks about all match the report, and the node is
/// still assigned the reported generation and revision. The round must have
/// been issued less than [`REPORT_MAX_AGE`] before the write's clock reading.
/// A missing observation row is never created.
pub async fn accept_report(
    conn: &mut PgConnection,
    node: NodeId,
    report: &Report,
) -> Result<Acceptance, sqlx::Error> {
    let mut tx = conn
        .begin_with(sql!("BEGIN ISOLATION LEVEL READ COMMITTED"))
        .await?;
    match accept(&mut tx, node, report).await {
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
    .bind(REPORT_MAX_AGE.as_secs_f64())
    .execute(&mut **tx)
    .await?;
    Ok(if written.rows_affected() == 1 {
        Acceptance::Accepted
    } else {
        Acceptance::Refused(ReportRefusal::Expired)
    })
}

#[cfg(test)]
mod tests;
