//! The firewall assignment this node durably accepted: [`FirewallAssignments`]
//! accepts the control plane's assignments in generation order and publishes
//! each one only after it commits. Accepted is not installed: nothing here
//! touches nftables.

use std::path::Path;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision, encode_revision};
use pplx_space_node_mgr_store::firewall_assignment::{self as store, AssignmentRow};
use rusqlite::{Connection, TransactionBehavior};
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// An assignment as delivered: the node it names is checked, not trusted.
#[derive(Clone, Copy, Debug)]
pub struct IncomingAssignment<'a> {
    pub node_id: NodeId,
    pub generation: u64,
    pub revision_id: Uuid,
    pub document: &'a str,
}

/// One coherent accepted assignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredAssignment {
    pub node_id: NodeId,
    pub generation: u64,
    pub revision_id: Uuid,
    pub revision: FirewallRuleRevision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acceptance {
    /// Committed and published.
    Committed,
    /// Already the accepted assignment; nothing was written.
    Replayed,
}

#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    #[error("assignment is for node {incoming}, not this node {expected}")]
    WrongNode { expected: NodeId, incoming: NodeId },
    #[error("accepted assignments are bound to node {bound}, not {expected}")]
    BoundToOtherNode { bound: NodeId, expected: NodeId },
    #[error("assignment generation {0} is too large to store")]
    GenerationOutOfRange(u64),
    #[error(transparent)]
    InvalidRevision(#[from] DecodeError),
    #[error("assignment generation {incoming} is older than accepted generation {accepted}")]
    Stale { accepted: u64, incoming: u64 },
    #[error("generation {generation} was already accepted with a different revision")]
    Conflict { generation: u64 },
    #[error("failed to store the firewall assignment: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("the firewall assignment writer is poisoned")]
    Poisoned,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error(transparent)]
    Writer(#[from] store::OpenError),
    #[error("failed to read the accepted firewall assignment: {0}")]
    Read(#[from] rusqlite::Error),
    #[error("the accepted firewall assignment does not decode: {0}")]
    Record(#[from] DecodeError),
}

pub struct FirewallAssignments {
    writer: Mutex<Connection>,
    current: ArcSwapOption<DesiredAssignment>,
}

impl FirewallAssignments {
    /// Opens the store in node-manager's migrated database with its own
    /// durable writer, and loads the accepted assignment. Open one per
    /// database: the mutex serializes acceptance only within an instance.
    pub fn open(db_path: &Path) -> Result<Self, OpenError> {
        let writer = store::open_writer(db_path)?;
        let current = store::read(&writer)?
            .map(|row| {
                Ok::<_, DecodeError>(Arc::new(DesiredAssignment {
                    node_id: row.node_id,
                    generation: row.generation,
                    revision_id: row.revision_id,
                    revision: decode_revision(&row.document)?,
                }))
            })
            .transpose()?;
        Ok(Self {
            writer: Mutex::new(writer),
            current: ArcSwapOption::new(current),
        })
    }

    /// The accepted assignment, or `None` before the first one.
    pub fn current(&self) -> Option<Arc<DesiredAssignment>> {
        self.current.load_full()
    }

    /// Durably accepts `incoming` for `expected_node`, which the trusted local
    /// caller supplies. Blocks on SQLite; call it from a blocking thread.
    pub fn accept(
        &self,
        expected_node: NodeId,
        incoming: IncomingAssignment<'_>,
    ) -> Result<Acceptance, AcceptError> {
        if incoming.node_id != expected_node {
            return Err(AcceptError::WrongNode {
                expected: expected_node,
                incoming: incoming.node_id,
            });
        }
        if i64::try_from(incoming.generation).is_err() {
            return Err(AcceptError::GenerationOutOfRange(incoming.generation));
        }
        let revision = decode_revision(incoming.document)?;
        let row = AssignmentRow {
            node_id: expected_node,
            generation: incoming.generation,
            revision_id: incoming.revision_id,
            document: encode_revision(&revision),
        };

        let mut writer = self.writer.lock().map_err(|_| AcceptError::Poisoned)?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(saved) = store::read(&tx)? {
            if saved.node_id != expected_node {
                return Err(AcceptError::BoundToOtherNode {
                    bound: saved.node_id,
                    expected: expected_node,
                });
            }
            if row.generation < saved.generation {
                return Err(AcceptError::Stale {
                    accepted: saved.generation,
                    incoming: row.generation,
                });
            }
            if row.generation == saved.generation {
                return if saved == row {
                    Ok(Acceptance::Replayed)
                } else {
                    Err(AcceptError::Conflict {
                        generation: row.generation,
                    })
                };
            }
        }
        store::replace(&tx, &row)?;
        tx.commit()?;
        self.publish(
            &writer,
            DesiredAssignment {
                node_id: row.node_id,
                generation: row.generation,
                revision_id: row.revision_id,
                revision,
            },
        );
        Ok(Acceptance::Committed)
    }

    /// Takes the locked writer so that publication stays serialized with the
    /// commit: otherwise an older generation could publish over a newer one.
    fn publish(&self, _locked_writer: &Connection, assignment: DesiredAssignment) {
        self.current.store(Some(Arc::new(assignment)));
    }
}
