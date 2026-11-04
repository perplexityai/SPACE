//! One bounded observation per node-status tick. A failed collection is evidence
//! of unavailability, never a complete empty inventory or an omitted sample.

use std::{future::Future, time::Duration};

use pplx_space_execution_common::proto::NodeExecutionInventory;
use pplx_space_node_api_client::proto::{
    ExecutionInventoryObservation, ExecutionInventoryUnavailable, NodeRole,
    execution_inventory_observation::Outcome,
};
use pplx_space_node_mgr_client::proto::GetExecutionInventoryRequest;

use crate::local_manager_request;

// This includes connection and RPC time. A slow discovery must not prevent the
// node-status facet from reporting failure; its next periodic tick resamples.
const COLLECTION_BUDGET: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) enum CollectedExecutionInventory {
    Complete(NodeExecutionInventory),
    Unavailable,
}

impl CollectedExecutionInventory {
    pub(super) fn into_publication(
        self,
    ) -> (
        Option<NodeExecutionInventory>,
        ExecutionInventoryObservation,
    ) {
        let (legacy, outcome) = match self {
            Self::Complete(inventory) => (Some(inventory.clone()), Outcome::Complete(inventory)),
            Self::Unavailable => (None, Outcome::Unavailable(ExecutionInventoryUnavailable {})),
        };
        (
            legacy,
            ExecutionInventoryObservation {
                outcome: Some(outcome),
            },
        )
    }
}

#[derive(Debug, thiserror::Error)]
enum CollectionError {
    #[error("read node-manager execution inventory: {0}")]
    Read(#[from] tonic::Status),
}

pub(super) async fn collect(node_role: Option<NodeRole>) -> Option<CollectedExecutionInventory> {
    bounded_collection(node_role, async {
        let mut manager = pplx_space_node_mgr_client::lazy();
        let mut request = local_manager_request(GetExecutionInventoryRequest {}, false);
        request.set_timeout(COLLECTION_BUDGET);
        Ok(manager.get_execution_inventory(request).await?.into_inner())
    })
    .await
}

async fn bounded_collection(
    node_role: Option<NodeRole>,
    fetch: impl Future<Output = Result<NodeExecutionInventory, CollectionError>>,
) -> Option<CollectedExecutionInventory> {
    if node_role == Some(NodeRole::Cluster) {
        return None;
    }
    let outcome = match tokio::time::timeout(COLLECTION_BUDGET, fetch).await {
        Ok(Ok(inventory)) => CollectedExecutionInventory::Complete(inventory),
        Ok(Err(CollectionError::Read(error))) if error.code() == tonic::Code::DeadlineExceeded => {
            warn_collection_timeout();
            CollectedExecutionInventory::Unavailable
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "execution inventory collection unavailable");
            CollectedExecutionInventory::Unavailable
        }
        Err(_elapsed) => {
            warn_collection_timeout();
            CollectedExecutionInventory::Unavailable
        }
    };
    Some(outcome)
}

fn warn_collection_timeout() {
    tracing::warn!(
        budget = ?COLLECTION_BUDGET,
        "execution inventory collection timed out"
    );
}

#[cfg(test)]
mod tests;
