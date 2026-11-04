use pplx_space_node_api_client::proto::{
    FacetSyncOverride, GetReconciliationStatusRequest, NodeCtlStatus, ReconciliationStatus,
    TriggerStateReconciliationRequest,
};
use tonic::{Request, Status};

async fn client() -> Result<pplx_space_node_api_client::NodeCtlLazyClient, Status> {
    Ok(pplx_space_node_api_client::node_ctl_lazy())
}

pub async fn reconciliation_status()
-> Result<(Vec<ReconciliationStatus>, Vec<FacetSyncOverride>), Status> {
    let mut client = client().await?;
    let response = client
        .get_reconciliation_status(Request::new(GetReconciliationStatusRequest {}))
        .await?
        .into_inner();
    Ok((response.reconciliations, response.sync_overrides))
}

pub async fn node_ctl_status() -> NodeCtlStatus {
    match reconciliation_status().await {
        Ok((reconciliations, sync_overrides)) => NodeCtlStatus {
            reachable: true,
            error: None,
            reconciliations,
            sync_overrides,
        },
        Err(error) => NodeCtlStatus {
            reachable: false,
            error: Some(error.message().to_string()),
            reconciliations: Vec::new(),
            sync_overrides: Vec::new(),
        },
    }
}

pub async fn node_ctl_status_if_present() -> Option<NodeCtlStatus> {
    if !std::path::Path::new(pplx_space_common_core::host::linux::paths::NODE_CTL_SOCKET).exists() {
        return None;
    }
    Some(node_ctl_status().await)
}

pub async fn trigger_state_reconciliation(
    request: TriggerStateReconciliationRequest,
) -> Result<(), Status> {
    let mut client = client().await?;
    client
        .trigger_state_reconciliation(Request::new(request))
        .await?;
    Ok(())
}
