//! Cold suspended sandboxes eviction may consider, by evidence lane. Both
//! lanes share the idle-witness and valid-`updated_at` SQL fragments, so a
//! row is cold, or malformed, the same way in each.

use crate::db::{Connection, Result};

/// Only the store defines these; callers pick one, they do not add their own.
mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Proofable<'_> {}
    impl Sealed for super::ProoflessLegacy {}
}

/// A lane [`eviction_candidates`] can read, and what it returns.
pub trait EvictionCandidates: sealed::Sealed {
    type Output;

    fn read(self, conn: &Connection) -> Result<Self::Output>;
}

/// Read the candidates of `lane`.
pub fn eviction_candidates<E: EvictionCandidates>(conn: &Connection, lane: E) -> Result<E::Output> {
    lane.read(conn)
}

/// Rows with remote-copy evidence (a pinned artifact or an upload job with
/// a snapshot id) and no active upload, after the keyset position, as
/// `(sandbox_id, idle witness)` in witness then id order.
#[derive(Clone, Copy, Debug)]
pub struct Proofable<'a> {
    pub cutoff: i64,
    pub cursor_witness: Option<i64>,
    pub cursor_sandbox_id: Option<&'a str>,
    pub limit: usize,
}

impl EvictionCandidates for Proofable<'_> {
    type Output = Vec<(String, u64)>;

    fn read(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::eviction_scan::idle_suspended_candidates(
            conn,
            self.cutoff,
            self.cursor_witness,
            self.cursor_sandbox_id,
            self.limit,
        )
    }
}

/// Ids of rows with neither a pinned artifact nor an upload job carrying a
/// snapshot id, oldest witness first: legacy rows that need a re-upload.
#[derive(Clone, Copy, Debug)]
pub struct ProoflessLegacy {
    pub idle_cutoff_epoch_secs: u64,
    pub limit: usize,
}

impl EvictionCandidates for ProoflessLegacy {
    type Output = Vec<String>;

    fn read(self, conn: &Connection) -> Result<Self::Output> {
        crate::db::eviction_scan::legacy_reupload_candidates(
            conn,
            self.idle_cutoff_epoch_secs,
            self.limit,
        )
    }
}
