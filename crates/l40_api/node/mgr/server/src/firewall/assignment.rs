//! The firewall assignment this node durably accepted: [`FirewallAssignments`]
//! accepts the control plane's assignments in generation order and publishes
//! each one only after its record is durable. Accepted is not installed:
//! nothing here touches nftables.
//!
//! The record is one file, in production
//! [`NODE_MANAGER_FIREWALL_ASSIGNMENT_FILE`](pplx_space_host_layout::paths::NODE_MANAGER_FIREWALL_ASSIGNMENT_FILE)
//! beside node-manager's database. A node-manager that predates it ignores it.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision, encode_revision};
use tonic::Status;
use uuid::Uuid;

use crate::service::artifact_fs::{self, AtomicWriteError, BoundedReadError, MAX_RECORD_BYTES};

#[cfg(test)]
mod tests;

const RECORD_FORMAT_VERSION: u32 = 1;

/// The record file; `revision` is the canonical document.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format_version: u32,
    node_id: Uuid,
    generation: u64,
    revision_id: Uuid,
    revision: String,
}

/// Read first, so a newer format is reported as one.
#[derive(serde::Deserialize)]
struct Format {
    format_version: u32,
}

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
    /// Durable and published.
    Committed,
    /// Already the accepted assignment; nothing was written.
    Replayed,
}

/// A replacement may be on disk without being durable, so neither acceptance
/// nor [`FirewallAssignments::current`] is authoritative until reopened.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("the firewall assignment store is unavailable until reopened")]
pub struct Unavailable;

#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    #[error("assignment is for node {incoming}, not this node {expected}")]
    WrongNode { expected: NodeId, incoming: NodeId },
    /// Permanent for this record: a node that keeps its disk under a new
    /// `NodeId` recovers only by deleting the record file, which the caller owns.
    #[error("accepted assignments are bound to node {bound}, not {expected}")]
    BoundToOtherNode { bound: NodeId, expected: NodeId },
    #[error(transparent)]
    InvalidRevision(#[from] DecodeError),
    #[error("firewall assignment record would exceed {MAX_RECORD_BYTES} bytes")]
    TooLarge,
    #[error("assignment generation {incoming} is older than accepted generation {accepted}")]
    Stale { accepted: u64, incoming: u64 },
    #[error("generation {generation} was already accepted with a different revision")]
    Conflict { generation: u64 },
    /// The record wasn't replaced; the accepted assignment is unchanged.
    #[error("failed to write the firewall assignment record: {0}")]
    Write(Status),
    /// The record was replaced but not made durable; the store is now
    /// [`Unavailable`].
    #[error("the firewall assignment record may not be durable: {0}")]
    Unsynced(Status),
    #[error(transparent)]
    Unavailable(#[from] Unavailable),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("read the firewall assignment record: {0}")]
    Read(#[source] std::io::Error),
    #[error("open the firewall assignment record: {0}")]
    Open(Status),
    #[error("sync the firewall assignment record's directory: {0}")]
    Sync(#[source] std::io::Error),
    #[error("firewall assignment record exceeds {MAX_RECORD_BYTES} bytes")]
    TooLarge,
    #[error("decode the firewall assignment record: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("unsupported firewall assignment record format {0}")]
    Format(u32),
    #[error("the accepted firewall assignment does not decode: {0}")]
    Revision(#[from] DecodeError),
}

pub struct FirewallAssignments {
    path: PathBuf,
    /// Spans compare, durable replace and publication.
    writer: Mutex<()>,
    current: ArcSwapOption<DesiredAssignment>,
    unavailable: AtomicBool,
    #[cfg(test)]
    write: fn(&Path, &[u8]) -> Result<(), AtomicWriteError>,
}

impl FirewallAssignments {
    /// Loads the record at `path`; a missing file is no accepted assignment.
    /// Any other unreadable record, such as a newer format or `schema_version`
    /// after a rollback, fails and is left in place.
    ///
    /// Open one store per file, only while holding node-manager's service
    /// mutex, which keeps a second process from writing; the store's own mutex
    /// serializes acceptance within the process.
    pub fn open(path: &Path) -> Result<Self, OpenError> {
        let current = read_record(path)?
            .map(|record| {
                Ok::<_, OpenError>(Arc::new(DesiredAssignment {
                    node_id: NodeId::from_uuid(record.node_id),
                    generation: record.generation,
                    revision_id: record.revision_id,
                    revision: decode_revision(&record.revision)?,
                }))
            })
            .transpose()?;
        Ok(Self {
            path: path.to_owned(),
            writer: Mutex::new(()),
            current: ArcSwapOption::new(current),
            unavailable: AtomicBool::new(false),
            #[cfg(test)]
            write: write_record,
        })
    }

    /// The accepted assignment, or `None` before the first one.
    pub fn current(&self) -> Result<Option<Arc<DesiredAssignment>>, Unavailable> {
        if self.unavailable.load(Ordering::Acquire) {
            return Err(Unavailable);
        }
        Ok(self.current.load_full())
    }

    /// Durably accepts `incoming` for `expected_node`, which the trusted local
    /// caller supplies. Blocks on file I/O: async callers run it through
    /// `NodeManagerService::off_runtime`, whose task node-manager drains before
    /// releasing its service mutex, so a write can't outlive ownership.
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
        let assignment = DesiredAssignment {
            node_id: expected_node,
            generation: incoming.generation,
            revision_id: incoming.revision_id,
            revision: decode_revision(incoming.document)?,
        };

        let _writer = self.writer.lock().map_err(|_| Unavailable)?;
        if let Some(saved) = self.current()? {
            if saved.node_id != expected_node {
                return Err(AcceptError::BoundToOtherNode {
                    bound: saved.node_id,
                    expected: expected_node,
                });
            }
            if assignment.generation < saved.generation {
                return Err(AcceptError::Stale {
                    accepted: saved.generation,
                    incoming: assignment.generation,
                });
            }
            if assignment.generation == saved.generation {
                return if *saved == assignment {
                    Ok(Acceptance::Replayed)
                } else {
                    Err(AcceptError::Conflict {
                        generation: assignment.generation,
                    })
                };
            }
        }
        let bytes = encode_record(&assignment);
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(AcceptError::TooLarge);
        }
        #[cfg(test)]
        let write = self.write;
        #[cfg(not(test))]
        let write = write_record;
        write(&self.path, &bytes).map_err(|error| match error {
            AtomicWriteError::NotReplaced(status) => AcceptError::Write(status),
            AtomicWriteError::NotSynced(status) => {
                self.unavailable.store(true, Ordering::Release);
                AcceptError::Unsynced(status)
            }
        })?;
        self.publish(assignment);
        Ok(Acceptance::Committed)
    }

    /// Moves the published assignment only to a higher generation, so an older
    /// one can never replace a newer one.
    fn publish(&self, assignment: DesiredAssignment) {
        let assignment = Arc::new(assignment);
        self.current.rcu(|current| match current {
            Some(current) if current.generation >= assignment.generation => {
                Some(Arc::clone(current))
            }
            _ => Some(Arc::clone(&assignment)),
        });
    }
}

fn encode_record(assignment: &DesiredAssignment) -> Vec<u8> {
    let record = Record {
        format_version: RECORD_FORMAT_VERSION,
        node_id: assignment.node_id.as_uuid(),
        generation: assignment.generation,
        revision_id: assignment.revision_id,
        revision: encode_revision(&assignment.revision),
    };
    match serde_json::to_vec(&record) {
        Ok(bytes) => bytes,
        // Integers, UUIDs and a string always serialize.
        Err(error) => unreachable!("firewall assignment record serialization failed: {error}"),
    }
}

/// Absence is checked by name, then the record is opened beneath its pinned
/// parent without following links, as the write publishes it. The parent is
/// synced first: a rename whose directory sync failed, or never ran, is only
/// trusted once it is durable.
fn read_record(path: &Path) -> Result<Option<Record>, OpenError> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(OpenError::Read(source)),
        Ok(_) => {}
    }
    let bytes =
        artifact_fs::read_bounded(path, MAX_RECORD_BYTES, true).map_err(|error| match error {
            BoundedReadError::Open(status) => OpenError::Open(status),
            BoundedReadError::Sync(source) => OpenError::Sync(source),
            BoundedReadError::Read(source) => OpenError::Read(source),
            BoundedReadError::TooLarge => OpenError::TooLarge,
        })?;
    let Format { format_version } = serde_json::from_slice(&bytes)?;
    if format_version != RECORD_FORMAT_VERSION {
        return Err(OpenError::Format(format_version));
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Temp file, `sync_all`, rename, parent `sync_all`.
fn write_record(path: &Path, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    artifact_fs::write_durable_atomic(path, |file| {
        (&*file).write_all(bytes).map_err(|error| {
            Status::unavailable(format!("write firewall assignment record: {error}"))
        })
    })
    .map(drop)
}
