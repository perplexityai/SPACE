//! `FetchFirewallRules`: node-ctl reads its own worker's firewall assignment
//! and pending round. The worker is the one live registration matching the
//! caller's reviewed Kubernetes node name and UID, never its address, and it
//! must be enrolled. Neither the node-ctl principal nor an enrollment
//! predicate is approved yet, so production refuses every caller.

use std::time::SystemTime;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_protocol::encode_revision;
use pplx_space_rpc_auth::{
    AuthenticatedNodeIdentity, FirewallRulesFetchAuthority, NodeApiAuthenticatedCaller, NodeRole,
};
use pplx_space_sql_locator::sql;
use tonic::{Request, Response, Status};

use crate::ClusterApiService;
use crate::proto::{
    FetchFirewallRulesRequest, FetchFirewallRulesResponse, FirewallRulesAssignment,
    FirewallRulesPendingRound,
};
use crate::template_releases_store::TemplateReleasesStore;

const DENIED: &str = "the caller may not fetch firewall rules";

/// The workers that may fetch. Production enrolls none until an enrollment
/// predicate is approved; tests name their fixture workers.
#[derive(Clone, Debug, Default)]
pub(crate) enum Enrollment {
    #[default]
    Unapproved,
    #[cfg(all(test, feature = "kubernetes"))]
    Fixture(Vec<NodeId>),
}

impl Enrollment {
    fn admit(&self, node: NodeId) -> Result<(), Status> {
        match self {
            Self::Unapproved => Err(Status::failed_precondition(format!(
                "firewall rules aren't enabled for node {node}: no enrollment predicate is approved"
            ))),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Fixture(enrolled) if enrolled.contains(&node) => Ok(()),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Fixture(_) => Err(Status::permission_denied(DENIED)),
        }
    }
}

impl ClusterApiService {
    pub(crate) async fn fetch_firewall_rules(
        &self,
        request: Request<FetchFirewallRulesRequest>,
    ) -> Result<Response<FetchFirewallRulesResponse>, Status> {
        let denied = || Status::permission_denied(DENIED);
        let caller = request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .ok_or_else(denied)?;
        if caller.firewall_rules_fetch_authority() != FirewallRulesFetchAuthority::Granted {
            return Err(denied());
        }
        // Missing or malformed claims never fall back to the address.
        let AuthenticatedNodeIdentity::Kubernetes(identity) = caller.node_identity() else {
            return Err(denied());
        };
        // The binding and the instructions come from one snapshot.
        let mut tx = self
            .pool
            .begin_with(sql!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            .await
            .map_err(|error| Status::internal(format!("begin firewall rules fetch: {error}")))?;
        let node = match TemplateReleasesStore::nodes_for_identity(
            &mut *tx,
            identity,
            &[NodeRole::Worker],
        )
        .await
        .map_err(|error| Status::internal(format!("resolve caller node identity: {error}")))?
        .as_slice()
        {
            [(node, Some(true))] => *node,
            _ => return Err(denied()),
        };
        self.firewall_enrollment.admit(node)?;
        let instructions = pplx_space_firewall_rules_store::fetch_instructions(&mut *tx, node)
            .await
            .map_err(|error| Status::internal(format!("fetch firewall instructions: {error}")))?;
        tx.commit()
            .await
            .map_err(|error| Status::internal(format!("end firewall rules fetch: {error}")))?;
        Ok(Response::new(FetchFirewallRulesResponse {
            node_id: node.to_string(),
            assignment: instructions
                .assignment
                .map(|assignment| FirewallRulesAssignment {
                    generation: assignment.generation,
                    revision_id: assignment.revision_id.to_string(),
                    revision: encode_revision(&assignment.revision),
                }),
            pending_round: instructions
                .pending_round
                .map(|pending| FirewallRulesPendingRound {
                    round: pending.round,
                    issued_at: Some(SystemTime::from(pending.issued_at).into()),
                }),
        }))
    }
}
