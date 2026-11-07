//! Registering a Kubernetes node: its `nodes` row and its `node_k8s` row, in
//! the caller's transaction.

use pplx_space_common_core::types::{NodeId, NodeRole, SubstrateKind};
use pplx_space_sql_locator::sql;
use sqlx::PgConnection;

/// A Kubernetes node to register under its deterministic SPACE node id.
#[derive(Debug, Clone, Copy)]
pub struct K8sNodeRegistration<'a> {
    pub node_id: NodeId,
    pub name: &'a str,
    pub uid: &'a str,
    pub role: NodeRole,
    /// `None` keeps the address an existing row has.
    pub address: Option<&'a str>,
    /// The `nodes.state` a new row starts in.
    pub state: &'a str,
    /// Whether registering an existing row also writes `state`. Only the
    /// node's lifecycle owner does; every other writer leaves it alone.
    pub manage_state: bool,
}

/// The statement of [`register_k8s_node`] that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum K8sNodeRegistrationStep {
    /// The tombstone check before anything is written.
    CheckTombstone,
    /// The `nodes` upsert.
    UpsertNode,
    /// The ownership re-check after an upsert that returned no row.
    RecheckOwnership,
    /// Clearing drain state when the Kubernetes Node UID changed.
    ClearDrain,
    /// The `node_k8s` upsert.
    UpsertNodeK8s,
    /// The tombstone re-check after the upserts.
    RecheckTombstone,
}

impl K8sNodeRegistrationStep {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CheckTombstone => "check tombstone",
            Self::UpsertNode => "upsert node",
            Self::RecheckOwnership => "re-check ownership",
            Self::ClearDrain => "clear drain state",
            Self::UpsertNodeK8s => "upsert node_k8s",
            Self::RecheckTombstone => "re-check tombstone",
        }
    }
}

impl std::fmt::Display for K8sNodeRegistrationStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why [`register_k8s_node`] refused or failed. Roll back the caller's
/// transaction on any of these: a failed statement aborts it, and a refusal
/// after the upserts can leave their writes in it.
#[derive(Debug, thiserror::Error)]
pub enum K8sNodeRegistrationError {
    /// The node was tombstoned before the registration wrote anything.
    #[error("node is tombstoned")]
    Tombstoned,
    /// A tombstone committed while the registration waited on the node's row.
    #[error("node was tombstoned during registration")]
    TombstonedDuringRegistration,
    /// The node id belongs to a node another substrate manages.
    #[error("node id belongs to a non-Kubernetes node")]
    NotKubernetes,
    #[error("Kubernetes node registration failed to {step}: {source}")]
    Query {
        step: K8sNodeRegistrationStep,
        #[source]
        source: sqlx::Error,
    },
}

trait Step<T> {
    fn step(self, step: K8sNodeRegistrationStep) -> Result<T, K8sNodeRegistrationError>;
}

impl<T> Step<T> for Result<T, sqlx::Error> {
    fn step(self, step: K8sNodeRegistrationStep) -> Result<T, K8sNodeRegistrationError> {
        self.map_err(|source| K8sNodeRegistrationError::Query { step, source })
    }
}

/// Register a Kubernetes node: refuse a tombstoned node, upsert its `nodes`
/// row, clear its drain state when its Kubernetes Node UID changed, upsert its
/// `node_k8s` row, and refuse it again if a tombstone committed meanwhile.
/// Returns whether any of those writes changed a row.
///
/// Runs on the caller's connection, inside the caller's transaction: on an error,
/// roll back the caller's transaction. `on_drain_cleared` runs as soon as
/// the drain state is cleared, before the statements after it run.
pub async fn register_k8s_node(
    conn: &mut PgConnection,
    registration: K8sNodeRegistration<'_>,
    on_drain_cleared: impl FnOnce(),
) -> Result<bool, K8sNodeRegistrationError> {
    let K8sNodeRegistration {
        node_id,
        name: node_name,
        uid: node_uid,
        role,
        address,
        state,
        manage_state,
    } = registration;

    // Tombstone gate. Inventory upserts (kube_node_sync, late pod placement
    // on a not-yet-cordoned terminated node) reach this function. Without
    // this check they re-create the `nodes` row under the same deterministic
    // id after an admin terminate deleted it, so the ghost row returns. The
    // caller's whole transaction is rolled back.
    if is_node_k8s_tombstoned(conn, node_id)
        .await
        .step(K8sNodeRegistrationStep::CheckTombstone)?
    {
        return Err(K8sNodeRegistrationError::Tombstoned);
    }

    let node_changed = sqlx::query_scalar::<_, bool>(sql!(
        "WITH upsert AS (
                 INSERT INTO nodes (id, address, role, managed_by, state)
                 VALUES ($1, $2::INET, $3, $6, $4)
                 ON CONFLICT (id) DO UPDATE
                 SET address = COALESCE(EXCLUDED.address, nodes.address),
                     role = EXCLUDED.role,
                     managed_by = EXCLUDED.managed_by,
                     state = CASE WHEN $5 THEN EXCLUDED.state ELSE nodes.state END
                 WHERE nodes.managed_by = $6
                   AND (
                     nodes.address IS DISTINCT FROM COALESCE(EXCLUDED.address, nodes.address)
                     OR nodes.role IS DISTINCT FROM EXCLUDED.role
                     OR ($5 AND nodes.state IS DISTINCT FROM EXCLUDED.state)
                   )
                 RETURNING TRUE AS changed
             )
             SELECT changed FROM upsert
             UNION ALL
             SELECT FALSE FROM nodes
             WHERE id = $1
               AND managed_by = $6
               AND NOT EXISTS (SELECT 1 FROM upsert)"
    ))
    .bind(node_id)
    .bind(address)
    .bind(role.as_str())
    .bind(state)
    .bind(manage_state)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(&mut *conn)
    .await
    .step(K8sNodeRegistrationStep::UpsertNode)?;

    let node_changed = match node_changed {
        Some(changed) => changed,
        None => {
            // The upsert returned no row. Under READ COMMITTED a
            // concurrent insert of this same node id can commit while our
            // `INSERT ... ON CONFLICT` waits on the unique index: the
            // ON CONFLICT arm then locks and reads that row, the
            // change-detection WHERE skips the now-redundant update
            // (address and role already match, and placement does not own
            // `state`), and the fallback SELECT — bound to our stale
            // statement snapshot — cannot see the row, so the whole query
            // returns nothing. Only a genuinely non-Kubernetes conflict
            // should abort the caller, so re-check node ownership with a
            // fresh statement snapshot before erroring.
            let existing_is_k8s = sqlx::query_scalar::<_, bool>(sql!(
                "SELECT TRUE FROM nodes WHERE id = $1 AND managed_by = $2"
            ))
            .bind(node_id)
            .bind(SubstrateKind::Kubernetes)
            .fetch_optional(&mut *conn)
            .await
            .step(K8sNodeRegistrationStep::RecheckOwnership)?
            .is_some();

            if !existing_is_k8s {
                return Err(K8sNodeRegistrationError::NotKubernetes);
            }
            // The node already exists as a Kubernetes row; the concurrent
            // writer established it and this upsert changed nothing.
            false
        }
    };

    // Detect same-name kube Node UID replacement BEFORE the upsert
    // below overwrites `node_k8s.k8s_node_uid`. If a replacement
    // node arrives and we leave `nodes.drain_phase` intact, the
    // node-drain reconciler will resolve the providerID of the
    // REPLACEMENT instance and ASG-terminate it on the operator's
    // behalf — the original (now-gone) instance was the intended
    // target. Clear drain state atomically with the UID change so
    // in-progress drains are abandoned (operator can re-drain
    // against the new identity if they still want to).
    //
    // `drain_mode` is cleared with them. It records *how* a drain is being
    // serviced, so it means nothing once the phase is gone — and leaving it
    // set is worse than meaningless: the admin listing and `space-admin-cli get-node`
    // read the column independently of the phase, so a surviving mode makes
    // a node that is not draining report `drain_phase: null` next to
    // `drain_mode: gentle`.
    let drain_cleared = sqlx::query(sql!(
        "UPDATE nodes
             SET drain_phase = NULL,
                 drain_mode = NULL,
                 drain_started_at = NULL,
                 drain_last_error = NULL
             WHERE id = $1
               AND EXISTS (
                 SELECT 1 FROM node_k8s
                 WHERE node_id = $1 AND k8s_node_uid IS DISTINCT FROM $2
               )"
    ))
    .bind(node_id)
    .bind(node_uid)
    .execute(&mut *conn)
    .await
    .step(K8sNodeRegistrationStep::ClearDrain)?
    .rows_affected();
    if drain_cleared > 0 {
        on_drain_cleared();
    }

    let node_k8s_changed = sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, $3)
             ON CONFLICT (node_id) DO UPDATE
             SET k8s_node_name = EXCLUDED.k8s_node_name,
                 k8s_node_uid = EXCLUDED.k8s_node_uid,
                 updated_at = NOW()
             WHERE node_k8s.k8s_node_name IS DISTINCT FROM EXCLUDED.k8s_node_name
                OR node_k8s.k8s_node_uid IS DISTINCT FROM EXCLUDED.k8s_node_uid"
    ))
    .bind(node_id)
    .bind(node_name)
    .bind(node_uid)
    .execute(&mut *conn)
    .await
    .step(K8sNodeRegistrationStep::UpsertNodeK8s)?
    .rows_affected()
        > 0;

    // Race-close re-check, paired with the pre-check above. A
    // concurrent `:terminate` that commits during our row-lock wait
    // would not have been visible to the pre-check but is now
    // visible — returning Err here causes the caller's transaction
    // to roll back, undoing the resurrection insert.
    if is_node_k8s_tombstoned(conn, node_id)
        .await
        .step(K8sNodeRegistrationStep::RecheckTombstone)?
    {
        return Err(K8sNodeRegistrationError::TombstonedDuringRegistration);
    }
    Ok(node_changed || drain_cleared > 0 || node_k8s_changed)
}

async fn is_node_k8s_tombstoned(
    conn: &mut PgConnection,
    node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
    ))
    .bind(node_id)
    .fetch_one(conn)
    .await
}
