//! Persistence of open Kubernetes route activation claims
//! (`sandbox_open_route_activations`).

use crate::db::Connection;
use crate::db::db_private_outside_tests;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxTransitionReceiptKind, SpaceApiRouteIdentity,
};
use pplx_space_sqlite::SqliteU64;
use rusqlite::params;

use crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance;

#[derive(Clone, Debug)]
/// Durable idempotency record for one exact Kubernetes activation.
///
/// This is not an exclusive lease: the matching pod and attempt may continue
/// after a manager restart. A later activation is fenced by the retained
/// generation recorded when that activation actually begins.
pub struct OpenRouteActivationClaim {
    route_generation: RouteGeneration,
    node_id: NodeId,
    execution_generation: ExecutionGeneration,
    pod_uid: PodUid,
    activation_attempt_id: SandboxActivationAttemptId,
}

impl OpenRouteActivationClaim {
    pub fn for_cluster_fenced_identity(
        identity: &SpaceApiRouteIdentity,
        provenance: RouteIdentityProvenance,
    ) -> Option<Self> {
        if provenance != RouteIdentityProvenance::ClusterFenced
            || identity.substrate_kind != Some(NodeManagedBy::Kubernetes)
        {
            return None;
        }
        Some(Self {
            route_generation: identity.route_generation?,
            node_id: identity.current_node_id?,
            execution_generation: identity.execution_generation?,
            pod_uid: identity.pod_uid.clone()?,
            activation_attempt_id: identity.activation_attempt_id?,
        })
    }

    /// The execution the claimed activation runs.
    pub fn execution_generation(&self) -> ExecutionGeneration {
        self.execution_generation
    }

    pub fn requested_generation(&self) -> u64 {
        // ExecutionGeneration admits only positive i64 values.
        self.route_generation
            .get()
            .min(self.execution_generation.get().unsigned_abs())
    }
}

fn write_claim(
    conn: &Connection,
    sandbox_id: &str,
    claim: &OpenRouteActivationClaim,
) -> crate::db::Result<()> {
    conn.execute(
        "INSERT INTO sandbox_open_route_activations (
             sandbox_id, route_generation, node_id, execution_generation,
             pod_uid, substrate_kind, activation_attempt_id,
             identity_provenance
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(sandbox_id) DO UPDATE SET
             route_generation = excluded.route_generation,
             node_id = excluded.node_id,
             execution_generation = excluded.execution_generation,
             pod_uid = excluded.pod_uid,
             substrate_kind = excluded.substrate_kind,
             activation_attempt_id = excluded.activation_attempt_id,
             identity_provenance = excluded.identity_provenance",
        params![
            sandbox_id,
            SqliteU64(claim.route_generation.get()),
            claim.node_id.to_string(),
            claim.execution_generation.get(),
            claim.pod_uid.as_str(),
            NodeManagedBy::Kubernetes.to_string(),
            claim.activation_attempt_id.to_string(),
            RouteIdentityProvenance::ClusterFenced.as_str(),
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_create_claim(
    conn: &Connection,
    sandbox_id: &str,
    claim: &OpenRouteActivationClaim,
) -> crate::db::Result<()> {
    write_claim(conn, sandbox_id, claim)
}

db_private_outside_tests! {
    fn claim_matches(
        conn: &Connection,
        sandbox_id: &str,
        claim: &OpenRouteActivationClaim,
    ) -> crate::db::Result<bool> {
        conn.query_row(
            "SELECT EXISTS (
             SELECT 1
              FROM sandbox_open_route_activations
              WHERE sandbox_id = ?1
                AND route_generation = ?2
                AND node_id = ?3
                AND execution_generation = ?4
                AND pod_uid = ?5
                AND substrate_kind = 'k8s'
                AND activation_attempt_id = ?6
                AND identity_provenance = ?7
         )",
            params![
                sandbox_id,
                SqliteU64(claim.route_generation.get()),
                claim.node_id.to_string(),
                claim.execution_generation.get(),
                claim.pod_uid.as_str(),
                claim.activation_attempt_id.to_string(),
                RouteIdentityProvenance::ClusterFenced.as_str(),
            ],
            |row| row.get(0),
        )
    }
}

db_private_outside_tests! {
    /// Whether this node rolled back the activation of `claim`, which the caller
    /// has matched as the open claim: the row carries an activation-rollback
    /// receipt for the claim's execution generation. Every activation clears the
    /// receipt when it begins and writes the claim, so while the claim still
    /// matches, such a receipt can only come from that activation's own rollback
    /// cleanup, which kept the claim open. The receipt names the operation's
    /// attempt, which an API-driven wake may carry apart from the route attempt
    /// the claim holds, so the attempt isn't compared. Some later state writes
    /// clear the receipt and some keep it; either way the activation that
    /// follows has to claim the row from the state the rollback left.
    fn claim_rolled_back(
        conn: &Connection,
        sandbox_id: &str,
        claim: &OpenRouteActivationClaim,
    ) -> crate::db::Result<bool> {
        conn.query_row(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes
                  WHERE id = ?1
                    AND transition_receipt_kind = ?2
                    AND transition_receipt_execution_generation = ?3
             )",
            params![
                sandbox_id,
                SandboxTransitionReceiptKind::ActivationRollback.as_str(),
                claim.execution_generation.get(),
            ],
            |row| row.get(0),
        )
    }
}

pub fn close_claim(conn: &Connection, sandbox_id: &str) -> crate::db::Result<bool> {
    Ok(conn.execute(
        "DELETE FROM sandbox_open_route_activations WHERE sandbox_id = ?1",
        [sandbox_id],
    )? == 1)
}

#[cfg(any(test, feature = "test-support"))]
pub fn claim_exists(conn: &Connection, sandbox_id: &str) -> crate::db::Result<bool> {
    conn.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM sandbox_open_route_activations WHERE sandbox_id = ?1
         )",
        [sandbox_id],
        |row| row.get(0),
    )
}
