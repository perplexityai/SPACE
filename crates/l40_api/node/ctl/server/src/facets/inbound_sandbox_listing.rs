//! The node's sandbox listing, shared by the inbound facets of one reconcile.

use std::sync::Arc;

use super::*;

/// The sandbox IDs one inbound reconcile pass reads from sandbox-manager.
///
/// The inbound facets of a pass run concurrently, and a Full scope of each
/// would otherwise page through every sandbox row on the node on its own.
/// A failed listing is not kept, so the next facet to ask lists again, as
/// it would have alone. The outbound inventory lists separately, because it
/// must observe what this pass changed.
#[derive(Clone, Default)]
pub(crate) struct InboundSandboxListing(Arc<tokio::sync::OnceCell<Vec<String>>>);

impl InboundSandboxListing {
    pub(super) async fn sandbox_ids(&self) -> Result<Vec<String>, String> {
        self.sandbox_ids_from(list_local_sandbox_ids).await
    }

    async fn sandbox_ids_from<Fut>(&self, list: impl FnOnce() -> Fut) -> Result<Vec<String>, String>
    where
        Fut: Future<Output = Result<Vec<String>, String>>,
    {
        self.0.get_or_try_init(list).await.cloned()
    }
}

#[cfg(test)]
mod tests;
