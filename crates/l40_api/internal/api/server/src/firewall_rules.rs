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

/// The workers that may fetch. Production enrolls none until an enrollment
/// predicate is approved; tests name their fixture workers.
#[derive(Clone, Debug)]
pub(crate) enum Enrollment {
    Unapproved,
    #[cfg(all(test, feature = "kubernetes"))]
    Fixture(Vec<NodeId>),
}

impl Enrollment {
    fn admit(&self, node: NodeId) -> Result<(), Refusal> {
        match self {
            Self::Unapproved => Err(Refusal::NoEnrollmentPredicate(node)),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Fixture(enrolled) if enrolled.contains(&node) => Ok(()),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Fixture(_) => Err(Refusal::Unenrolled(node)),
        }
    }
}

/// Why a fetch was refused. Only the server log names it; callers see
/// [`Refusal::status`].
#[derive(Clone, Copy, Debug)]
enum Refusal {
    NoFetchAuthority,
    /// A static-token caller or a token without pod/node claims.
    MissingNodeClaims,
    MalformedNodeClaims,
    /// No live Kubernetes worker carries the name: unregistered, deleted, or
    /// another role.
    NoLiveWorker,
    AmbiguousName,
    UidMismatch,
    /// The registration has no inventory UID until node sync supplies it.
    UidUnsynced,
    #[cfg(all(test, feature = "kubernetes"))]
    Unenrolled(NodeId),
    NoEnrollmentPredicate(NodeId),
}

impl Refusal {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NoFetchAuthority => "no_fetch_authority",
            Self::MissingNodeClaims => "missing_node_claims",
            Self::MalformedNodeClaims => "malformed_node_claims",
            Self::NoLiveWorker => "no_live_worker",
            Self::AmbiguousName => "ambiguous_name",
            Self::UidMismatch => "uid_mismatch",
            Self::UidUnsynced => "uid_unsynced",
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Unenrolled(_) => "unenrolled",
            Self::NoEnrollmentPredicate(_) => "no_enrollment_predicate",
        }
    }

    fn status(self) -> Status {
        match self {
            Self::NoEnrollmentPredicate(node) => Status::failed_precondition(format!(
                "firewall rules aren't enabled for node {node}: no enrollment predicate is approved"
            )),
            #[cfg(all(test, feature = "kubernetes"))]
            Self::Unenrolled(node) => Status::failed_precondition(format!(
                "firewall rules aren't enabled for node {node}: it isn't enrolled"
            )),
            _ => Status::permission_denied(DENIED),
        }
    }

    /// Log the reason with the reviewed identity, which carries no token data.
    fn log(self, caller: Option<&NodeApiAuthenticatedCaller>) -> Status {
        let node = caller.and_then(|caller| match caller.node_identity() {
            AuthenticatedNodeIdentity::Kubernetes(node) => Some(node),
            _ => None,
        });
        tracing::warn!(
            reason = self.as_str(),
            service_account = caller.and_then(NodeApiAuthenticatedCaller::service_account),
            node_name = node.map(|node| node.name().as_str()),
            node_uid = node.map(|node| node.uid().as_str()),
            "refused firewall rules fetch"
        );
        self.status()
    }
}

impl ClusterApiService {
    pub(crate) async fn fetch_firewall_rules(
        &self,
        request: Request<FetchFirewallRulesRequest>,
    ) -> Result<Response<FetchFirewallRulesResponse>, Status> {
        let caller = request.extensions().get::<NodeApiAuthenticatedCaller>();
        let refuse = |reason: Refusal| reason.log(caller);
        let Some(caller) = caller else {
            return Err(refuse(Refusal::NoFetchAuthority));
        };
        if caller.firewall_rules_fetch_authority() != FirewallRulesFetchAuthority::Granted {
            return Err(refuse(Refusal::NoFetchAuthority));
        }
        // Missing or malformed claims never fall back to the address.
        let identity = match caller.node_identity() {
            AuthenticatedNodeIdentity::Kubernetes(identity) => identity,
            AuthenticatedNodeIdentity::MalformedKubernetesClaims => {
                return Err(refuse(Refusal::MalformedNodeClaims));
            }
            AuthenticatedNodeIdentity::RemoteAddress
            | AuthenticatedNodeIdentity::MissingKubernetesClaims => {
                return Err(refuse(Refusal::MissingNodeClaims));
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
            [] => return Err(refuse(Refusal::NoLiveWorker)),
            [(_, Some(false))] => return Err(refuse(Refusal::UidMismatch)),
            [(_, None)] => return Err(refuse(Refusal::UidUnsynced)),
            _ => return Err(refuse(Refusal::AmbiguousName)),
        };
        self.firewall_enrollment.admit(node).map_err(refuse)?;
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
                }),
        }))
    }
}
