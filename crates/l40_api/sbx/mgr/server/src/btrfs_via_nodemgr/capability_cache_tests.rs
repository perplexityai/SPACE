use super::*;
use crate::node_manager_transport::fake::{FakeNodeManager, ServedNodeManager};
use pplx_space_node_mgr_client::proto::GetBtrfsSendCapabilitiesResponse;

fn answering(inspect_send_source: bool) -> ServedNodeManager {
    FakeNodeManager::default()
        .unary(
            "GetBtrfsSendCapabilities",
            move |_: GetBtrfsSendCapabilitiesRequest| {
                Ok(GetBtrfsSendCapabilitiesResponse {
                    supported: true,
                    inspect_send_source,
                })
            },
        )
        .serve()
}

/// A capability answer belongs to the node-manager that gave it: another
/// connector asks its own node-manager, and repeat questions within the cache
/// lifetime are answered without a second probe.
#[tokio::test]
async fn send_source_inspection_is_cached_per_connector() {
    let inspecting = answering(true);
    let refusing = answering(false);

    for _ in 0..2 {
        assert!(node_manager_inspects_send_sources(&inspecting.connector()).await);
        assert!(!node_manager_inspects_send_sources(&refusing.connector()).await);
    }

    assert_eq!(inspecting.calls(), ["GetBtrfsSendCapabilities"]);
    assert_eq!(refusing.calls(), ["GetBtrfsSendCapabilities"]);
}
