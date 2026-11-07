//! Kubernetes node inventory: role upserts, demotion of unobserved nodes, and
//! tombstone checks.

use super::*;
use pplx_space_common_core::types::SubstrateKind;
use pplx_space_node_registration_store::{
    K8sNodeRegistration, K8sNodeRegistrationError, K8sNodeRegistrationStep, register_k8s_node,
};

impl K8sStore {
    /// `instance_id` is the EC2 instance the Node's `providerID` names;
    /// `None` leaves a recorded instance alone.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::controllers::substrate::kubernetes) async fn persist_k8s_node_inventory(
        &self,
        node_name: &str,
        node_uid: &str,
        node_id: NodeId,
        role: NodeRole,
        address: Option<&str>,
        state: NodeStateWrite,
        instance_id: Option<&str>,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes node inventory transaction")?;

        let changed = self
            .upsert_k8s_node_role(
                &mut tx,
                K8sNodeRoleUpsert {
                    node_name,
                    node_uid,
                    node_id,
                    role,
                    address,
                    state,
                    instance_id,
                },
            )
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes node inventory transaction")?;
        Ok(changed)
    }

    /// Demote stale ready rows missing from a credible full-cluster Kubernetes
    /// membership snapshot without deleting node or sandbox placement records.
    pub(in crate::controllers::substrate::kubernetes) async fn demote_unobserved_k8s_nodes(
        &self,
        live_node_names: &[String],
        missing_grace: Duration,
        max_per_tick: u32,
    ) -> Result<Vec<String>, SubstrateError> {
        if live_node_names.is_empty() {
            return Ok(Vec::new());
        }

        let missing_grace_secs = i64::try_from(missing_grace.as_secs()).unwrap_or(i64::MAX);
        let mut changed = sqlx::query_scalar::<_, String>(sql!(
            "WITH candidates AS MATERIALIZED (
                 SELECT candidate.id,
                        candidate.last_heartbeat,
                        candidate_k8s.updated_at AS k8s_updated_at
                 FROM nodes AS candidate
                 JOIN node_k8s AS candidate_k8s
                   ON candidate_k8s.node_id = candidate.id
                 WHERE candidate.managed_by = $4
                   AND candidate.state = 'ready'
                   AND candidate.drain_phase IS NULL
                   AND NOT (candidate_k8s.k8s_node_name = ANY($1::TEXT[]))
                   AND (
                        candidate.last_heartbeat IS NULL
                        OR candidate.last_heartbeat
                           < NOW() - ($2 * INTERVAL '1 second')
                   )
                   AND candidate_k8s.updated_at
                       < NOW() - ($2 * INTERVAL '1 second')
                 ORDER BY candidate_k8s.updated_at, candidate.id
                 LIMIT $3
             )
             UPDATE nodes AS n
             SET state = 'registered'
             FROM node_k8s AS nk
             JOIN candidates AS candidate
               ON candidate.id = nk.node_id
             WHERE n.id = candidate.id
               AND n.managed_by = $4
               AND n.state = 'ready'
               AND n.drain_phase IS NULL
               AND n.last_heartbeat IS NOT DISTINCT FROM candidate.last_heartbeat
               AND nk.updated_at = candidate.k8s_updated_at
             RETURNING nk.k8s_node_name"
        ))
        .bind(live_node_names)
        .bind(missing_grace_secs)
        .bind(i64::from(max_per_tick))
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to demote unobserved Kubernetes nodes")?;
        changed.sort_unstable();
        Ok(changed)
    }

    /// Register a Kubernetes node through the registration store, in the
    /// caller's transaction, with the errors and warning this path reports.
    pub(super) async fn upsert_k8s_node_role(
        &self,
        conn: &mut PgConnection,
        upsert: K8sNodeRoleUpsert<'_>,
    ) -> Result<bool, SubstrateError> {
        let K8sNodeRoleUpsert {
            node_name,
            node_uid,
            node_id,
            role,
            address,
            state,
            instance_id,
        } = upsert;

        // `manage_state` decides whether a conflict-update writes `state`.
        // The lifecycle owner writes it on every upsert; insert-only writers
        // (pod placement, the admin inventory listing) supply an initial state
        // for a new row only and must leave an existing node's state alone.
        let (state, manage_state) = match state {
            NodeStateWrite::Manage(value) => (value.as_str(), true),
            NodeStateWrite::InsertOnly(value) => (value.as_str(), false),
        };

        register_k8s_node(
            conn,
            K8sNodeRegistration {
                node_id,
                name: node_name,
                uid: node_uid,
                role,
                address,
                state,
                manage_state,
                instance_id,
            },
            || {
                tracing::warn!(
                    node_id = %node_id,
                    node_name = %node_name,
                    new_k8s_node_uid = %node_uid,
                    "k8s Node UID changed for existing node row; cleared drain_phase/drain_mode/drain_started_at/drain_last_error — \
                     any in-progress drain was abandoned"
                );
            },
        )
        .await
        .map_err(|error| match error {
            K8sNodeRegistrationError::Tombstoned => SubstrateError::NodeTombstoned {
                message: format!(
                    "kube node {node_name} role {role} is tombstoned (admin-terminated); \
                     refusing to re-create the SPACE node row"
                ),
            },
            K8sNodeRegistrationError::TombstonedDuringRegistration => {
                SubstrateError::NodeTombstoned {
                    message: format!(
                        "kube node {node_name} role {role} was tombstoned during upsert; \
                         rolling back to keep the SPACE node row deleted"
                    ),
                }
            }
            K8sNodeRegistrationError::NotKubernetes => SubstrateError::Failed {
                message: format!(
                    "Kubernetes node uid {node_uid} role {role} for {node_name} conflicts with a non-Kubernetes node"
                ),
            },
            K8sNodeRegistrationError::Query { step, source } => {
                let what = match step {
                    K8sNodeRegistrationStep::CheckTombstone => "failed to check node tombstone",
                    K8sNodeRegistrationStep::UpsertNode => {
                        "failed to upsert Kubernetes node registry"
                    }
                    K8sNodeRegistrationStep::RecheckOwnership => {
                        "failed to re-check Kubernetes node ownership"
                    }
                    K8sNodeRegistrationStep::ClearDrain => {
                        "failed to clear drain state on UID change"
                    }
                    K8sNodeRegistrationStep::RecordInstance => {
                        "failed to record Kubernetes node instance"
                    }
                    K8sNodeRegistrationStep::UpsertNodeK8s => {
                        "failed to upsert Kubernetes node metadata"
                    }
                    K8sNodeRegistrationStep::RecheckTombstone => {
                        "failed to re-check node tombstone"
                    }
                };
                crate::controllers::substrate::substrate_db_error(what, &source)
            }
        })
    }
}
