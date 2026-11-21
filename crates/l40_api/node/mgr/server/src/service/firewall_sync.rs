//! `SyncFirewallRules`: node-ctl delivers this node's firewall assignment,
//! which is durably accepted and then applied to one slot. Only a test fixture
//! binds the node, slot and record (`fixture`); without one, as on every
//! production node, the RPC refuses after authorization.

use pplx_space_host_layout::identities::user_names;
use pplx_space_uds_peer_auth::UdsPeerPrincipal;
use tonic::{Request, Response, Status};

use super::NodeManagerService;
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
}
