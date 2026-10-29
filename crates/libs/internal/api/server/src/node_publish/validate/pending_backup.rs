use std::{collections::BTreeMap, str::FromStr};

use pplx_space_common_core::types::{
    ArtifactKind, LocalArtifactId, LocalArtifactIdParseError, SandboxId, SandboxIdParseError,
};
use pplx_space_sbx_mgr_client::proto::PendingLocalBackup;

use crate::metadata::artifact_kind_from_proto;
use crate::proto::PendingLocalBackupReport;

/// The reported artifact; its sandbox identity is the map key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingLocalBackupEntry {
    artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
}

/// A node's parsed answer to which local artifacts have no durable copy.
///
/// Duplicate sandbox identities are rejected rather than collapsed: choosing
/// one artifact would make the other look absent during set replacement.
/// Construction checks wire evidence, not permission to replace the mirror;
/// census and current artifact-ownership guards remain transactional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedPendingLocalBackups {
    entries: BTreeMap<SandboxId, PendingLocalBackupEntry>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PendingLocalBackupParseError {
    #[error("invalid pending-backup sandbox_id {value:?}: {source}")]
    SandboxId {
        value: String,
        #[source]
        source: SandboxIdParseError,
    },
    #[error("invalid pending-backup local_artifact_id {value:?}: {source}")]
    LocalArtifactId {
        value: String,
        #[source]
        source: LocalArtifactIdParseError,
    },
    #[error("pending-backup artifact_kind must be DISK or FULL, got {0}")]
    ArtifactKind(i32),
    #[error("sandbox {0} appears more than once in the pending-backup report")]
    DuplicateSandbox(SandboxId),
}

impl ParsedPendingLocalBackups {
    fn parse(entries: &[PendingLocalBackup]) -> Result<Self, PendingLocalBackupParseError> {
        let mut parsed = BTreeMap::new();
        for entry in entries {
            let sandbox_id = SandboxId::from_str(entry.sandbox_id.trim()).map_err(|source| {
                PendingLocalBackupParseError::SandboxId {
                    value: entry.sandbox_id.trim().to_owned(),
                    source,
                }
            })?;
            let artifact_id =
                LocalArtifactId::from_str(entry.local_artifact_id.trim()).map_err(|source| {
                    PendingLocalBackupParseError::LocalArtifactId {
                        value: entry.local_artifact_id.trim().to_owned(),
                        source,
                    }
                })?;
            let artifact_kind = artifact_kind_from_proto(entry.artifact_kind).ok_or(
                PendingLocalBackupParseError::ArtifactKind(entry.artifact_kind),
            )?;
            if parsed
                .insert(
                    sandbox_id,
                    PendingLocalBackupEntry {
                        artifact_id,
                        artifact_kind,
                    },
                )
                .is_some()
            {
                return Err(PendingLocalBackupParseError::DuplicateSandbox(sandbox_id));
            }
        }
        Ok(Self { entries: parsed })
    }

    pub(crate) fn names(&self, sandbox_id: SandboxId) -> bool {
        self.entries.contains_key(&sandbox_id)
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Build all parallel arrays in one pass to preserve index alignment.
    pub(crate) fn unnest_columns(
        &self,
    ) -> (Vec<SandboxId>, Vec<LocalArtifactId>, Vec<&'static str>) {
        self.entries
            .iter()
            .map(|(sandbox_id, entry)| {
                (*sandbox_id, entry.artifact_id, entry.artifact_kind.as_str())
            })
            .collect()
    }
}

/// Malformed or absent evidence is not an empty census. Parsing retains the
/// distinction without deciding whether the database may trust the report.
#[derive(Debug)]
pub(crate) enum PendingBackupReport {
    Absent,
    Present {
        reported_count: usize,
        parsed: Result<ParsedPendingLocalBackups, PendingLocalBackupParseError>,
    },
}

impl PendingBackupReport {
    pub(crate) fn from_proto(report: Option<&PendingLocalBackupReport>) -> Self {
        match report {
            None => Self::Absent,
            Some(report) => Self::Present {
                reported_count: report.pending.len(),
                parsed: ParsedPendingLocalBackups::parse(&report.pending),
            },
        }
    }

    pub(crate) fn reported_count(&self) -> Option<usize> {
        match self {
            Self::Absent => None,
            Self::Present { reported_count, .. } => Some(*reported_count),
        }
    }

    pub(crate) fn parsed(
        &self,
    ) -> Option<Result<&ParsedPendingLocalBackups, &PendingLocalBackupParseError>> {
        match self {
            Self::Absent => None,
            Self::Present { parsed, .. } => Some(parsed.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests;
