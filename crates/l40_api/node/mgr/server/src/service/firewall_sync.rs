//! `SyncFirewallRules`: node-ctl delivers this node's firewall assignment,
//! which is durably accepted and then applied to one slot or every eligible
//! slot; a node-wide binding also gates each runtime launch on it. Only a
//! test fixture binds the node, slots and record (`fixture`); without one, as
//! on every production node, the RPC refuses after authorization and launches
//! are unchanged.

use pplx_space_host_layout::identities::user_names;
use pplx_space_uds_peer_auth::UdsPeerPrincipal;
use tonic::{Request, Response, Status};

use super::NodeManagerService;
use crate::net::SlotInfo;
use crate::proto::{SyncFirewallRulesRequest, SyncFirewallRulesResponse};

#[cfg(all(test, target_os = "linux"))]
mod fixture;

#[cfg(all(test, target_os = "linux"))]
pub(super) use fixture::FirewallSync;

/// Node-ctl alone delivers assignments; root and the admin roles don't.
const ALLOW_FIREWALL_SYNC: &[UdsPeerPrincipal] =
    &[UdsPeerPrincipal::Service(user_names::SPACE_NODECTL)];

impl NodeManagerService {
    pub async fn sync_firewall_rules(
        &self,
        request: Request<SyncFirewallRulesRequest>,
    ) -> Result<Response<SyncFirewallRulesResponse>, Status> {
        self.authorize_peer(&request, "SyncFirewallRules", ALLOW_FIREWALL_SYNC)?;
        #[cfg(all(test, target_os = "linux"))]
        if let Some(sync) = self.firewall_sync.clone() {
            return sync
                .deliver(self, request.into_inner())
                .await
                .map(Response::new);
        }
        Err(Status::failed_precondition(
            "this node has no firewall rule sync binding",
        ))
    }

    /// Runs `launch`, the dispatch that lets `lease`'s guest send, once any
    /// binding admits it; `namespace` is the entry the launcher opens.
    pub(super) async fn admit_launch<T>(
        &self,
        lease: &SlotInfo,
        namespace: &str,
        launch: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        #[cfg(all(test, target_os = "linux"))]
        if let Some(sync) = &self.firewall_sync {
            return sync
                .start(&self.pool, lease, namespace.as_ref(), launch)
                .await;
        }
        let _ = (lease, namespace);
        launch.await
    }
}
