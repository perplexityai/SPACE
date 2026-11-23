//! `FetchFirewallRules`: node-ctl reads its own worker's firewall assignment
//! and pending round. The caller's reviewed Kubernetes node name must match
//! exactly one live worker registration, whose UID must match too; the
//! address is never used, and the worker must be enrolled. Neither the
//! node-ctl principal nor an enrollment predicate is approved yet, so
//! production refuses every caller. Direct workers can't fetch: static-token
//! callers are denied and the lookup requires `managed_by = k8s`, so an
//! enrollment predicate must exclude them until a Direct binding exists.

use std::time::SystemTime;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_protocol::encode_revision;
use pplx_space_node_registration_store::nodes_for_identity;
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

const DENIED: &str = "the caller may not fetch firewall rules";

/// PERMISSION_DENIED for every caller; only the log has the reason.
fn denied(reason: &'static str) -> Status {
    tracing::warn!(reason, "refused firewall rules fetch");
    Status::permission_denied(DENIED)
}

fn not_enabled(reason: &'static str, node: NodeId, why: &str) -> Status {
    tracing::warn!(reason, "refused firewall rules fetch");
    Status::failed_precondition(format!(
        "firewall rules aren't enabled for node {node}: {why}"
    ))
}

/// The workers that may fetch. Production enrolls none until an enrollment
/// predicate is approved; tests name their fixture workers.
#[derive(Clone, Debug)]
pub(crate) enum Enrollment {
    Unapproved,
    #[cfg(all(test, feature = "kubernetes"))]
    Fixture(Vec<NodeId>),
}

impl Enrollment {
    fn admit(&self, node: NodeId) -> Result<(), Status> {
        match self {
            Self::Unapproved => Err(not_enabled(
                "no_enrollment_predicate",
                node,
                "no enrollment predicate is approved",
            )),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Fixture(enrolled) if enrolled.contains(&node) => Ok(()),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Fixture(_) => Err(not_enabled("unenrolled", node, "it isn't enrolled")),
        }
    }
}

impl ClusterApiService {
    pub(crate) async fn fetch_firewall_rules(
        &self,
        request: Request<FetchFirewallRulesRequest>,
    ) -> Result<Response<FetchFirewallRulesResponse>, Status> {
        let caller = request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .ok_or_else(|| denied("no_fetch_authority"))?;
        if caller.firewall_rules_fetch_authority() != FirewallRulesFetchAuthority::Granted {
            return Err(denied("no_fetch_authority"));
        }
        // Missing or malformed claims never fall back to the address.
        let identity = match caller.node_identity() {
            AuthenticatedNodeIdentity::Kubernetes(identity) => identity,
            AuthenticatedNodeIdentity::MalformedKubernetesClaims => {
                return Err(denied("malformed_node_claims"));
            }
            AuthenticatedNodeIdentity::RemoteAddress
            | AuthenticatedNodeIdentity::MissingKubernetesClaims => {
                return Err(denied("missing_node_claims"));
            }
        };
        // The binding and the instructions come from one snapshot.
        let mut tx = self
            .pool
            .begin_with(sql!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            .await
            .map_err(|error| Status::internal(format!("begin firewall rules fetch: {error}")))?;
        let node = match nodes_for_identity(
            &mut *tx,
            identity.name(),
            identity.uid(),
            &[NodeRole::Worker],
        )
        .await
        .map_err(|error| Status::internal(format!("resolve caller node identity: {error}")))?
        .as_slice()
        {
            [(node, Some(true))] => *node,
            [] => return Err(denied("no_live_worker")),
            [(_, Some(false))] => return Err(denied("uid_mismatch")),
            [(_, None)] => return Err(denied("uid_unsynced")),
            _ => return Err(denied("ambiguous_name")),
        };
        self.firewall_enrollment.admit(node)?;
        let instructions = pplx_space_firewall_rules_store::fetch_instructions(&mut tx, node)
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
                    assignment_generation: pending.assignment_generation,
                }),
        }))
    }
}
