//! The sandbox's record of whose activation created its runtime
//! (`sandboxes.activating_client_id`), kept in step with the pod the active
//! execution row tracks.
//!
//! Whenever a statement moves the active row to a different pod, the same
//! transaction sets the record to that pod's `space.pplx.net/client-id`
//! label, or to `NULL` when the pod carries none. The label comes from the
//! pod itself, never from the request. An unlabeled view of the pod the row
//! already tracks writes nothing: the label is missing only because an older
//! image created the pod, or because a cached view predates the adoption
//! patch, and neither means the record is wrong.
//!
//! The pod-tracking statements lock only the execution row, while multi-row
//! mutations lock `sandboxes` first. So the write is conditional: under the
//! execution-row lock, a caller checks whether the record has to change, and
//! only then restarts in the established order (`sandboxes`, then the
//! execution row) to write it. That happens about once per new pod.

use super::*;
use pplx_space_common_core::types::ClientId;
use pplx_space_sandbox_store::attribute_runtime::{UNATTRIBUTED_LIFECYCLES, set_activating_client};

/// The active execution row's tracked pod, read under its row lock, with the
/// sandbox's lifecycle and record read beside it when attribution is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TrackedPod {
    pub(super) pod_uid: Option<String>,
    /// `None` with attribution off, when nothing reads or writes the record.
    pub(super) sandbox: Option<TrackedSandbox>,
}

/// The sandbox's lifecycle and record, read after the execution row's lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TrackedSandbox {
    pub(super) lifecycle: String,
    pub(super) record: Option<ClientId>,
}

impl TrackedPod {
    /// What the record becomes when the row moves to `pod_uid`, observed with
    /// `pod_client` as its label, or `None` to leave it alone. Reached only
    /// through [`K8sStore::record_after`], which applies the switch; the UID
    /// fill (`backfill_missing_pod_uid`) applies it itself and calls
    /// [`attribution_update`] directly, since its row tracked no pod before.
    fn record_after(
        &self,
        pod_uid: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Option<Option<ClientId>> {
        let sandbox = self.sandbox.as_ref()?;
        attribution_update(
            &sandbox.lifecycle,
            self.pod_uid.as_deref(),
            pod_uid,
            pod_client,
            sandbox.record.as_ref(),
        )
    }
}

/// What the record becomes when the tracked pod moves from `previous_uid` to
/// `pod_uid`, observed with `pod_client` as its label, or `None` to leave the
/// record as it is:
///
/// - a sandbox in one of the [`UNATTRIBUTED_LIFECYCLES`]: nothing;
/// - no pod tracked after the write: nothing;
/// - a different pod: its label, or `NULL` when it has none;
/// - the same pod, labeled: its label;
/// - the same pod, unlabeled: nothing.
///
/// A value equal to the current `record` is also `None`.
pub(in crate::controllers::substrate::kubernetes) fn attribution_update(
    lifecycle: &str,
    previous_uid: Option<&str>,
    pod_uid: Option<&str>,
    pod_client: Option<&ClientId>,
    record: Option<&ClientId>,
) -> Option<Option<ClientId>> {
    if UNATTRIBUTED_LIFECYCLES.contains(&lifecycle) {
        return None;
    }
    let pod_uid = pod_uid?;
    let target = if previous_uid == Some(pod_uid) {
        Some(pod_client?.clone())
    } else {
        pod_client.cloned()
    };
    (target.as_ref() != record).then_some(target)
}

impl K8sStore {
    /// [`TrackedPod::record_after`], or `None` when attribution is off: then
    /// pod tracking never writes the record.
    pub(super) fn record_after(
        &self,
        tracked: &TrackedPod,
        pod_uid: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Option<Option<ClientId>> {
        if !self.attribution.is_enabled() {
            return None;
        }
        tracked.record_after(pod_uid, pod_client)
    }

    /// Lock the active execution row of `generation`, and read its tracked
    /// pod with the sandbox's lifecycle and record. The `sandboxes` row isn't
    /// locked. `None` when no such active row exists.
    ///
    /// The sandbox is read by a second statement, after the lock is held. A
    /// row a locking read waited on is re-read at its newest version, but a
    /// joined row isn't, and every record write runs under this same lock: so
    /// a separate read is what sees one that committed while this waited.
    /// With attribution off the sandbox isn't read at all.
    pub(super) async fn lock_tracked_pod_in(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Option<TrackedPod>, SubstrateError> {
        let Some(pod_uid) = sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT NULLIF(BTRIM(pod_uid), '')
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2 AND active
              FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to lock the tracked Kubernetes pod")?
        else {
            return Ok(None);
        };
        if !self.attribution.is_enabled() {
            return Ok(Some(TrackedPod {
                pod_uid,
                sandbox: None,
            }));
        }
        let sandbox = sqlx::query_as::<_, (String, Option<ClientId>)>(sql!(
            "SELECT lifecycle, activating_client_id FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to read the sandbox's activating client")?;
        Ok(sandbox.map(|(lifecycle, record)| TrackedPod {
            pod_uid,
            sandbox: Some(TrackedSandbox { lifecycle, record }),
        }))
    }

    /// Repair a record that lags the label of the pod the active execution row
    /// of `generation` already tracks, writing nothing else: the pod metadata
    /// write would also re-derive the row's state, which a record-only repair
    /// must not touch. A no-op unless the row still tracks `pod_uid`.
    pub(in crate::controllers::substrate::kubernetes) async fn repair_runtime_attribution(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: &str,
        pod_client: Option<&ClientId>,
    ) -> Result<(), SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start runtime attribution transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(());
        }
        if let Some(tracked) = self
            .lock_tracked_pod_in(&mut tx, sandbox_id, generation)
            .await?
            && tracked.pod_uid.as_deref() == Some(pod_uid)
            && let Some(record) = self.record_after(&tracked, Some(pod_uid), pod_client)
        {
            Self::attribute_runtime_in(&mut tx, sandbox_id, record.as_ref()).await?;
        }
        tx.commit()
            .await
            .db_context("failed to commit runtime attribution transaction")?;
        Ok(())
    }

    /// Set the record to `client_id` inside the caller's transaction, which
    /// already holds the `sandboxes` row lock.
    pub(super) async fn attribute_runtime_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        client_id: Option<&ClientId>,
    ) -> Result<(), SubstrateError> {
        set_activating_client(&mut *conn, sandbox_id, client_id)
            .await
            .db_context("failed to record the sandbox's activating client")?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "runtime_attribution/tests.rs"]
mod tests;
