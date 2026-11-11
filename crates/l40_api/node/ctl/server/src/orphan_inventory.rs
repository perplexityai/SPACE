//! One read-only page per reconciliation tick. No candidate is a cleanup job.

use pplx_space_node_mgr_client::{NodeManagerGrpcClient, proto::ListOrphanCandidatesRequest};
use std::time::Duration;

#[derive(Default)]
pub(super) struct Observer {
    client: Option<NodeManagerGrpcClient>,
    cursor: String,
}

impl Observer {
    pub(super) async fn observe(&mut self) {
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
                let candidates = page.orphaned_namespaces.len()
                    + page.orphaned_cgroups.len()
                    + page.orphaned_run_dirs.len()
                    + page.orphaned_cache_dirs.len()
                    + page.orphaned_nft_rules.len()
                    + page.orphaned_processes.len();
                let samples: Vec<_> = page
                    .orphaned_namespaces
                    .iter()
                    .chain(&page.orphaned_cgroups)
                    .chain(&page.orphaned_run_dirs)
                    .chain(&page.orphaned_cache_dirs)
                    .chain(&page.orphaned_nft_rules)
                    .chain(&page.orphaned_processes)
                    .take(10)
                    .collect();
                if candidates > 0
                    || !page.warnings.is_empty()
                    || (page.next_page_token.is_empty() && !page.complete)
                {
                    tracing::warn!(candidates, ?samples, warnings = ?page.warnings,
                        complete = page.complete,
                        scan_started_at_unix_ms = page.scan_started_at_unix_ms,
                        observed_at_unix_ms = page.observed_at_unix_ms,
                        "read-only orphan inventory; inspect candidates before any manual cleanup");
                } else {
                    tracing::debug!(
                        complete = page.complete,
                        observed_at_unix_ms = page.observed_at_unix_ms,
                        "read-only orphan inventory page observed"
                    );
                }
                self.cursor = page.next_page_token;
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
}
