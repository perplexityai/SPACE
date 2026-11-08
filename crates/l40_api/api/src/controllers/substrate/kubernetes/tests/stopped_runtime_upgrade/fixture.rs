//! The stopped sandbox the stay-stopped runtime upgrade tests run against,
//! on the shared scripted node and Kubernetes API
//! (`super::super::runtime_upgrade_fixture`).

use std::sync::atomic::Ordering;
use std::time::Duration;

use pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};

use super::super::runtime_upgrade_dispatch::{E1, publish_environment};
pub(in crate::controllers::substrate::kubernetes) use super::super::runtime_upgrade_fixture::*;
use crate::controllers::substrate::kubernetes::KubernetesSubstrate;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{BoxError, ScriptedReply};
use crate::controllers::substrate::runtime_upgrade::StoppedRuntimeUpgrade;
use crate::controllers::substrate::{SubstrateAdapter, SubstrateError};

pub(in crate::controllers::substrate::kubernetes) const UPGRADE_TIMEOUT: Duration =
    Duration::from_secs(3);
/// The budget of an attempt the test expects to end unsettled: it runs to its
/// deadline, so a short one keeps the test fast.
const UNSETTLED_TIMEOUT: Duration = Duration::from_millis(800);

pub(super) struct Subject {
    pub(super) db: pplx_space_db_test_fixtures::TestDb,
    pub(super) sandbox_id: SandboxId,
    pub(super) node_id: NodeId,
    pub(super) substrate: KubernetesSubstrate,
    pub(super) cluster: Cluster,
}

/// A stopped sandbox whose disk is on `node-a` and whose last execution E0
/// ran, with E0 blocked in favor of the default E1 along a certified edge.
pub(super) async fn subject() -> Result<Subject, BoxError> {
    subject_whose_backups(backup_enqueued()).await
}

/// [`subject`] whose node answers backup enqueues with `enqueue`.
pub(super) async fn subject_whose_backups(enqueue: ScriptedReply) -> Result<Subject, BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let (sandbox_id, node_id) = seed_sandbox_on_e0(&pool, "stopped").await?;
    let cluster = Cluster::start(&pool, node_id, enqueue).await?;
    let substrate = cluster.substrate(&pool);
    Ok(Subject {
        db,
        sandbox_id,
        node_id,
        substrate,
        cluster,
    })
}

impl Subject {
    pub(super) fn node(&self) -> &Node {
        &self.cluster.node
    }

    pub(super) fn pods(&self) -> &Pods {
        &self.cluster.pods
    }

    /// A substrate over the same database, node and pods, whose policy cache
    /// is empty, as another replica's would be.
    pub(super) fn forget_cached_policy(&mut self) {
        self.substrate = self.cluster.substrate(&self.db.pool);
    }

    /// A substrate like [`Self::forget_cached_policy`]'s whose pod reflector
    /// has seen the sandbox's current pod, as a running replica's has.
    pub(super) fn cache_the_pod(&mut self) {
        let pod = self.pods().current().into_iter().collect();
        self.substrate = self
            .cluster
            .substrate_caching(&self.db.pool, pod, vec![worker_node()]);
    }

    /// [`Self::cache_the_pod`] whose [`worker_node`] advertises only
    /// `capacity` network slots.
    pub(super) fn cache_the_pod_with_network_slots(&mut self, capacity: u64) {
        let pod = self.pods().current().into_iter().collect();
        let mut node = worker_node();
        if let Some(status) = node.status.as_mut() {
            let slots = std::collections::BTreeMap::from([(
                SANDBOX_NETWORK_SLOTS_RESOURCE.to_string(),
                k8s_openapi::apimachinery::pkg::api::resource::Quantity(capacity.to_string()),
            )]);
            status.capacity = Some(slots.clone());
            status.allocatable = Some(slots);
        }
        self.substrate = self
            .cluster
            .substrate_caching(&self.db.pool, pod, vec![node]);
    }

    pub(super) async fn upgrade(&self) -> Result<StoppedRuntimeUpgrade, SubstrateError> {
        self.substrate
            .upgrade_stopped_runtime(self.sandbox_id, UPGRADE_TIMEOUT)
            .await
    }

    /// An attempt the test expects to end unsettled, under a short deadline.
    pub(super) async fn upgrade_unsettled(&self) -> SubstrateError {
        self.substrate
            .upgrade_stopped_runtime(self.sandbox_id, UNSETTLED_TIMEOUT)
            .await
            .expect_err("the upgrade settled")
    }

    pub(super) async fn lifecycle(&self) -> Result<String, BoxError> {
        Ok(
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(self.sandbox_id)
                .fetch_one(&self.db.pool)
                .await?,
        )
    }

    /// The active execution's generation, attempt target and whether its
    /// lease still runs.
    pub(super) async fn active_execution(
        &self,
    ) -> Result<Option<(i64, Option<String>, bool)>, BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT generation, activation_attempt_target_state,
                    COALESCE(activation_attempt_expires_at > NOW(), FALSE)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .fetch_optional(&self.db.pool)
        .await?)
    }

    /// The upgrade the active execution records: operation, target and
    /// authorization.
    pub(super) async fn recorded_upgrade(
        &self,
    ) -> Result<(Option<uuid::Uuid>, Option<String>, Option<String>), BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT runtime_upgrade_operation_id, runtime_upgrade_target_environment_ref,
                    runtime_upgrade_authorization_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .fetch_one(&self.db.pool)
        .await?)
    }

    /// The backup enqueues the node received, in order.
    pub(super) fn enqueued_backups(
        &self,
    ) -> Vec<pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest> {
        self.cluster
            .worker
            .received(ENQUEUE_BACKUP)
            .into_iter()
            .map(|message| prost::Message::decode(message).unwrap())
            .collect()
    }

    /// No pod was created and nothing was asked of the node.
    pub(super) fn assert_nothing_placed_or_sent(&self, context: &str) {
        assert_eq!(self.pods().creates.load(Ordering::SeqCst), 0, "{context}");
        for method in [GET_RUNTIME_UPGRADE, WAKE, ENQUEUE_BACKUP] {
            assert!(
                self.cluster.worker.first_call(method).is_none(),
                "{context}: the upgrade called {method}"
            );
        }
    }

    /// The row is stopped, and no execution or pod is left.
    pub(super) async fn assert_stopped_without_execution(
        &self,
        context: &str,
    ) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert_eq!(self.active_execution().await?, None, "{context}");
        assert!(
            !self.pods().exists(),
            "{context}: a pod outlived the upgrade"
        );
        Ok(())
    }

    /// The upgrade kept its pod and its execution, whose claim's lease is
    /// over, for a retry.
    pub(super) async fn assert_kept_for_a_retry(&self, context: &str) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert!(self.pods().exists(), "{context}: the pod is gone");
        let (_, target, live) = self
            .active_execution()
            .await?
            .unwrap_or_else(|| panic!("{context}: no execution"));
        assert_eq!(
            (target.as_deref(), live),
            (Some("stopped"), false),
            "{context}"
        );
        assert!(self.recorded_upgrade().await?.0.is_some(), "{context}");
        Ok(())
    }

    /// An attempt that ended unsettled kept its pod and its execution, whose
    /// claim's lease still runs, so nothing takes the execution mid-upgrade.
    pub(super) async fn assert_kept_under_its_lease(&self, context: &str) -> Result<(), BoxError> {
        assert_eq!(self.lifecycle().await?, "stopped", "{context}");
        assert!(self.pods().exists(), "{context}: the pod is gone");
        let (_, target, live) = self
            .active_execution()
            .await?
            .unwrap_or_else(|| panic!("{context}: no execution"));
        assert_eq!(
            (target.as_deref(), live),
            (Some("stopped"), true),
            "{context}"
        );
        assert!(self.recorded_upgrade().await?.0.is_some(), "{context}");
        Ok(())
    }

    /// The active execution's claim's lease runs out, as it does once its
    /// time is up.
    pub(super) async fn end_the_lease(&self) -> Result<(), BoxError> {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(self.sandbox_id)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    /// The node published the sandbox stopped under E1.
    pub(super) async fn publish_upgraded(&self) -> Result<(), BoxError> {
        publish_environment(&self.db.pool, self.sandbox_id, self.node_id, E1).await
    }
}

pub(super) fn upgraded(outcome: &StoppedRuntimeUpgrade) -> uuid::Uuid {
    match outcome {
        StoppedRuntimeUpgrade::Upgraded {
            target,
            operation_id,
        } => {
            assert_eq!(target.to_string(), E1);
            *operation_id
        }
        StoppedRuntimeUpgrade::AsRecorded => panic!("expected an upgrade, got {outcome:?}"),
    }
}
