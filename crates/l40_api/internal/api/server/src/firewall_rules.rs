//! `FetchFirewallRules` and `ReportFirewallRules`: node-ctl reads its own
//! worker's firewall assignment and pending round, and records that worker's
//! answer. The caller's reviewed Kubernetes node name must match exactly one
//! live worker registration, whose UID must match too; the address is never
//! used, and the worker must be enrolled. Each RPC needs its own authority.
//! Neither the node-ctl principal nor an enrollment predicate is approved yet,
//! so production refuses every caller. Direct workers can't call them:
//! static-token callers are denied and the lookup requires `managed_by = k8s`,
//! so an enrollment predicate must exclude them until a Direct binding exists.

use std::time::SystemTime;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_protocol::encode_revision;
use pplx_space_node_registration_store::nodes_for_identity;
use pplx_space_rpc_auth::{
    AuthenticatedKubernetesNode, AuthenticatedNodeIdentity, FirewallRulesFetchAuthority,
    NodeApiAuthenticatedCaller, NodeRole,
};
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;
use tonic::{Request, Response, Status};

use crate::ClusterApiService;
use crate::proto::{
    FetchFirewallRulesRequest, FetchFirewallRulesResponse, FirewallRulesAssignment,
    FirewallRulesPendingRound,
};

mod report;

/// The RPC a refusal answers, which words it.
#[derive(Clone, Copy, Debug)]
enum Call {
    Fetch,
    Report,
}

impl Call {
    /// PERMISSION_DENIED for every caller; only the log has the reason.
    fn denied(self, reason: &'static str) -> Status {
        match self {
            Self::Fetch => {
                tracing::warn!(reason, "refused firewall rules fetch");
                Status::permission_denied("the caller may not fetch firewall rules")
            }
            Self::Report => {
                tracing::warn!(reason, "refused firewall rules report");
                Status::permission_denied("the caller may not report firewall rules")
            }
        }
    }

    fn not_enabled(self, reason: &'static str, node: NodeId, why: &str) -> Status {
        match self {
            Self::Fetch => tracing::warn!(reason, "refused firewall rules fetch"),
            Self::Report => tracing::warn!(reason, "refused firewall rules report"),
        }
        Status::failed_precondition(format!(
            "firewall rules aren't enabled for node {node}: {why}"
        ))
    }
}

/// The workers that may fetch and report. Production enrolls none until an
/// enrollment predicate is approved; tests name their fixture workers.
#[derive(Clone, Debug)]
pub(crate) enum Enrollment {
    Unapproved,
    #[cfg(any(all(test, feature = "kubernetes"), feature = "test-support"))]
    Fixture(Vec<NodeId>),
}

impl Enrollment {
    fn admit(&self, node: NodeId, call: Call) -> Result<(), Status> {
        match self {
            Self::Unapproved => Err(call.not_enabled(
                "no_enrollment_predicate",
                node,
                "no enrollment predicate is approved",
            )),
            #[cfg(any(all(test, feature = "kubernetes"), feature = "test-support"))]
            Self::Fixture(enrolled) if enrolled.contains(&node) => Ok(()),
            #[cfg(any(all(test, feature = "kubernetes"), feature = "test-support"))]
            Self::Fixture(_) => Err(call.not_enabled("unenrolled", node, "it isn't enrolled")),
        }
    }
}

/// The caller's reviewed node claims. Missing or malformed claims never fall
/// back to the address.
fn reviewed_node(
    caller: &NodeApiAuthenticatedCaller,
    call: Call,
) -> Result<&AuthenticatedKubernetesNode, Status> {
    match caller.node_identity() {
        AuthenticatedNodeIdentity::Kubernetes(identity) => Ok(identity),
        AuthenticatedNodeIdentity::MalformedKubernetesClaims => {
            Err(call.denied("malformed_node_claims"))
        }
        AuthenticatedNodeIdentity::RemoteAddress
        | AuthenticatedNodeIdentity::MissingKubernetesClaims => {
            Err(call.denied("missing_node_claims"))
        }
    }
}

/// The one live worker registration `identity` names by name and UID.
async fn resolve_worker(
    conn: &mut PgConnection,
    identity: &AuthenticatedKubernetesNode,
    call: Call,
) -> Result<NodeId, Status> {
    match nodes_for_identity(
        &mut *conn,
        identity.name(),
        identity.uid(),
        &[NodeRole::Worker],
    )
    .await
    .map_err(|error| Status::internal(format!("resolve caller node identity: {error}")))?
    .as_slice()
    {
        [(node, Some(true))] => Ok(*node),
        [] => Err(call.denied("no_live_worker")),
        [(_, Some(false))] => Err(call.denied("uid_mismatch")),
        [(_, None)] => Err(call.denied("uid_unsynced")),
        _ => Err(call.denied("ambiguous_name")),
    }
}

impl ClusterApiService {
    pub(crate) async fn fetch_firewall_rules(
        &self,
        request: Request<FetchFirewallRulesRequest>,
    ) -> Result<Response<FetchFirewallRulesResponse>, Status> {
        let call = Call::Fetch;
        let caller = request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .ok_or_else(|| call.denied("no_fetch_authority"))?;
        if caller.firewall_rules_fetch_authority() != FirewallRulesFetchAuthority::Granted {
            return Err(call.denied("no_fetch_authority"));
        }
        let identity = reviewed_node(caller, call)?;
        // The binding and the instructions come from one snapshot.
        let mut tx = self
            .pool
            .begin_with(sql!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            .await
            .map_err(|error| Status::internal(format!("begin firewall rules fetch: {error}")))?;
        let node = resolve_worker(&mut tx, identity, call).await?;
        self.firewall_enrollment.admit(node, call)?;
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
                    assignment_generation: Some(pending.assignment_generation),
                }),
        }))
    }
}
