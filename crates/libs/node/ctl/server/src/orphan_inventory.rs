//! One read-only page per reconciliation tick. No candidate is a cleanup job.
//!
//! A walk spans several ticks, so its pages are summed and one summary is
//! logged when it ends. A walk is a live read, not a snapshot: a sandbox
//! created or torn down while it runs can show up once. A candidate the
//! previous complete walk also found is not that race, so the summary warns
//! when such a persistent candidate first appears, and when a walk could not
//! read every source. Candidates only one walk found, and persistent ones
//! already warned about, are logged at INFO.

use std::collections::HashSet;

use pplx_space_node_mgr_client::{
    NodeManagerGrpcClient,
    proto::{ListOrphanCandidatesRequest, ListOrphanCandidatesResponse},
};
use std::time::Duration;

/// Candidate names remembered per walk. Any past this are counted as
/// `untracked` and never judged persistent.
const MAX_TRACKED: usize = 4096;
const MAX_SAMPLES: usize = 10;
const MAX_WARNINGS: usize = 8;

#[derive(Default)]
pub(super) struct Observer {
    client: Option<NodeManagerGrpcClient>,
    cursor: String,
    walk: Walk,
    /// Every tracked candidate the last complete walk found.
    previous: HashSet<String>,
    /// The persistent candidates the last complete walk reported.
    reported: HashSet<String>,
}

#[derive(Default)]
struct Walk {
    candidates: HashSet<String>,
    total: usize,
    untracked: usize,
    warnings: Vec<String>,
}

/// What a finished walk found, as one summary.
#[derive(Debug)]
struct WalkSummary {
    candidates: usize,
    /// Candidates past [`MAX_TRACKED`], counted but not judged.
    untracked: usize,
    /// Tracked candidates the previous complete walk also found.
    persistent: usize,
    /// Persistent candidates no earlier walk reported.
    new_persistent: usize,
    /// Newly persistent candidates first, then the others.
    samples: Vec<String>,
    warnings: Vec<String>,
    /// Every source was read.
    complete: bool,
    scan_started_at_unix_ms: i64,
    observed_at_unix_ms: i64,
}

impl Observer {
    pub(super) async fn observe(&mut self) {
        if self.cursor.is_empty() {
            self.walk = Walk::default();
        }
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            if self.client.is_none() {
                self.client = Some(
                    pplx_space_node_mgr_client::connect()
                        .await
                        .map_err(|error| tonic::Status::unavailable(error.to_string()))?,
                );
            }
            self.client
                .as_mut()
                .expect("client connected above")
                .list_orphan_candidates(ListOrphanCandidatesRequest {
                    page_token: self.cursor.clone(),
                })
                .await
                .map(tonic::Response::into_inner)
        })
        .await;
        match result {
            Ok(Ok(page)) => {
                tracing::debug!(
                    complete = page.complete,
                    last_page = page.next_page_token.is_empty(),
                    observed_at_unix_ms = page.observed_at_unix_ms,
                    "read-only orphan inventory page observed"
                );
                self.cursor = page.next_page_token.clone();
                if let Some(summary) = self.record_page(page) {
                    summary.log();
                }
            }
            Ok(Err(status)) if status.code() == tonic::Code::ResourceExhausted => {
                // Admission/registry contention rejects before advancing a
                // page. Keep the cursor and retry on the next existing tick.
                tracing::debug!(%status, "orphan inventory busy; retaining cursor for retry");
            }
            Ok(Err(status)) if status.code() == tonic::Code::Unimplemented => {
                self.cursor.clear();
                tracing::debug!(
                    "node manager does not implement orphan inventory (local simulator); retrying next tick"
                );
            }
            error => {
                // A lost response may have consumed the cursor. Start another
                // walk next tick; the server bounds and expires abandoned walks.
                self.cursor.clear();
                self.client = None;
                tracing::warn!(
                    ?error,
                    "orphan inventory unavailable; retrying next node reconciliation tick"
                );
            }
        }
    }

    /// Adds a page to the current walk, and summarizes the walk if the page
    /// is its last.
    fn record_page(&mut self, page: ListOrphanCandidatesResponse) -> Option<WalkSummary> {
        let last_page = page.next_page_token.is_empty();
        let walk = &mut self.walk;
        for candidate in page
            .orphaned_namespaces
            .into_iter()
            .chain(page.orphaned_cgroups)
            .chain(page.orphaned_run_dirs)
            .chain(page.orphaned_cache_dirs)
            .chain(page.orphaned_nft_rules)
            .chain(page.orphaned_processes)
        {
            walk.total += 1;
            if walk.candidates.len() < MAX_TRACKED {
                walk.candidates.insert(candidate);
            } else if !walk.candidates.contains(&candidate) {
                walk.untracked += 1;
            }
        }
        let room = MAX_WARNINGS.saturating_sub(walk.warnings.len());
        walk.warnings.extend(page.warnings.into_iter().take(room));
        if !last_page {
            return None;
        }

        let walk = std::mem::take(&mut self.walk);
        let mut persistent: Vec<_> = walk
            .candidates
            .intersection(&self.previous)
            .cloned()
            .collect();
        persistent.sort();
        let mut samples: Vec<_> = persistent
            .iter()
            .filter(|candidate| !self.reported.contains(*candidate))
            .cloned()
            .collect();
        let new_persistent = samples.len();
        // Newly persistent candidates are the ones to inspect, so they lead.
        let mut rest: Vec<_> = walk
            .candidates
            .iter()
            .filter(|candidate| !samples.contains(*candidate))
            .cloned()
            .collect();
        rest.sort();
        samples.extend(rest);
        samples.truncate(MAX_SAMPLES);
        // An incomplete walk can miss a candidate that is still there, so it
        // neither starts nor ends persistence.
        if page.complete {
            self.previous = walk.candidates;
            self.reported = persistent.iter().cloned().collect();
        }
        Some(WalkSummary {
            candidates: walk.total,
            untracked: walk.untracked,
            persistent: persistent.len(),
            new_persistent,
            samples,
            warnings: walk.warnings,
            complete: page.complete,
            scan_started_at_unix_ms: page.scan_started_at_unix_ms,
            observed_at_unix_ms: page.observed_at_unix_ms,
        })
    }
}

impl WalkSummary {
    /// WARN for what needs inspecting: a candidate newly persistent, or a
    /// walk that could not read every source. INFO for other candidates, and
    /// DEBUG for a clean node.
    fn log(&self) {
        macro_rules! summary {
            ($level:ident) => {
                tracing::$level!(
                    candidates = self.candidates,
                    untracked = self.untracked,
                    persistent = self.persistent,
                    new_persistent = self.new_persistent,
                    samples = ?self.samples,
                    warnings = ?self.warnings,
                    complete = self.complete,
                    scan_started_at_unix_ms = self.scan_started_at_unix_ms,
                    observed_at_unix_ms = self.observed_at_unix_ms,
                    "read-only orphan inventory; inspect candidates before any manual cleanup"
                )
            };
        }
        if !self.complete || self.new_persistent > 0 {
            summary!(warn);
        } else if self.candidates > 0 {
            summary!(info);
        } else {
            summary!(debug);
        }
    }
}

#[cfg(test)]
mod tests;
