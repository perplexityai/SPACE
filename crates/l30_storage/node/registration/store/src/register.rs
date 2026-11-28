//! Registering a Kubernetes node: its `nodes` row and its `node_k8s` row, in
//! the caller's transaction.

use pplx_space_accounts::NodeId;
use pplx_space_k8s_node_id::Ec2InstanceId;
use pplx_space_node_model::{NodeRole, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

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
    /// The EC2 instance the Node's `providerID` names. `None` keeps the
    /// recorded instance while the Node's UID is unchanged; on a UID change
    /// it clears it, since a new Node may be another machine.
    pub instance_id: Option<&'a Ec2InstanceId>,
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
    /// The check of a v2 drain's row before a new Node UID that reports no
    /// instance takes it.
    CheckUidChange,
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
            Self::CheckUidChange => "check the v2 drain's Node UID",
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
    /// A Node under a new UID that reports no instance would take the row of
    /// a v2 drain. Deferred, as a tombstoned node is refused: the next
    /// registration retries, once the Node reports its `providerID`, and
    /// then keeps the drain for the same instance or clears it for another.
    #[error("node has a v2 drain, and the Node under a new UID reports no instance yet")]
    AwaitingInstance,
    #[error("Kubernetes node registration failed to {step}: {source}")]
    Query {
        step: K8sNodeRegistrationStep,
        #[source]
        source: DbError,
    },
}

trait Step<T> {
    fn step(self, step: K8sNodeRegistrationStep) -> Result<T, K8sNodeRegistrationError>;
}

impl<T> Step<T> for Result<T, sqlx::Error> {
    fn step(self, step: K8sNodeRegistrationStep) -> Result<T, K8sNodeRegistrationError> {
        self.map_err(|source| K8sNodeRegistrationError::Query {
            step,
            source: DbError::from(source),
        })
    }
}

/// Register a Kubernetes node: refuse a tombstoned node, upsert its `nodes`
/// row, clear its drain state when its Kubernetes Node UID changed, upsert its
/// `node_k8s` row with the instance its Node names, and refuse it again if a
/// tombstone committed meanwhile. A Node under a new UID that reports no
/// instance yet is deferred ([`K8sNodeRegistrationError::AwaitingInstance`])
/// while the row has a drain under an engine (v2, or one this build doesn't
/// know), and such a drain survives a UID change whose Node reports the
/// drain's recorded instance: the same machine re-created its Node.
/// Returns whether any of those writes changed a row.
///
/// Runs on the caller's connection, inside the caller's transaction: on an error,
/// roll back the caller's transaction. `on_drain_cleared` runs as soon as
/// the drain state is cleared, before the statements after it run, told
/// whether the drain it abandoned was a v2 one.
pub async fn register_k8s_node<C>(
    conn: &mut C,
    registration: K8sNodeRegistration<'_>,
    on_drain_cleared: impl FnOnce(bool),
) -> Result<bool, K8sNodeRegistrationError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let K8sNodeRegistration {
        node_id,
        name: node_name,
        uid: node_uid,
        role,
        address,
        state,
        manage_state,
        instance_id,
    } = registration;

    // Tombstone gate. Inventory upserts (kube_node_sync, late pod placement
    // on a not-yet-cordoned terminated node) reach this function. Without
    // this check they re-create the `nodes` row under the same deterministic
    // id after an admin terminate deleted it, so the ghost row returns. The
    // caller's whole transaction is rolled back.
    if is_node_k8s_tombstoned(&mut *conn, node_id)
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
    .fetch_optional((&mut *conn).into_sqlx())
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
            .fetch_optional((&mut *conn).into_sqlx())
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

    // A v2 drain's node is its recorded instance: a Node under a new UID is
    // that machine again only if it reports the same instance, and a
    // replacement only if it reports another. A drain under an engine this
    // build doesn't know (`drain_engine` set, not 'v2') is held the same way:
    // this build can't tell whether it is v2-like, so it drops it only for
    // another machine. One that reports none yet is
    // deferred, under the row lock the upsert above took, so the next sync
    // decides once the Node's `providerID` names its instance. A Node's
    // `providerID` never changes once set, so without a UID change the
    // instance can't change either. The check is its own statement, so it
    // reads the row as committed after the upsert waited on its lock: the
    // upsert's fallback read sees its statement's snapshot, from before a v2
    // drain that committed meanwhile. Every registration that reports no
    // instance runs it (one primary-key join: every Kind node sync, and the
    // seconds before a `providerID` appears on EKS); for a row without an
    // engine it reads false. It reads the registration's own columns, never
    // the drain's status.
    let awaiting_instance = instance_id.is_none()
        && sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM nodes n
                   JOIN node_k8s k ON k.node_id = n.id
                  WHERE n.id = $1
                    AND n.drain_engine IS NOT NULL
                    AND k.k8s_node_uid IS DISTINCT FROM $2
             )"
        ))
        .bind(node_id)
        .bind(node_uid)
        .fetch_one((&mut *conn).into_sqlx())
        .await
        .step(K8sNodeRegistrationStep::CheckUidChange)?;
    if awaiting_instance {
        return Err(K8sNodeRegistrationError::AwaitingInstance);
    }

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
    // `drain_mode: gentle`. A v2 drain's engine and status go with them for
    // the same reason: its epoch and report are the replaced machine's.
    //
    // A v2 drain is the exception when the instance is unchanged: the same
    // machine re-created its Node (a `kubectl delete node`, a kubelet
    // restart), and its sandbox-manager is still sealed under the drain's
    // generation, which only that drain's undrain or terminate settles. The
    // registering Node's instance must be known and equal to the recorded
    // one, `node_k8s.instance_id`; an unreported one was deferred above. A
    // drain under an engine this build doesn't know is kept the same way.
    //
    // Only a row with something to clear matches: any drain column set, so
    // a stray `drain_mode` alone is still cleared. A Node re-created on a
    // node outside any drain changes nothing here and reports no abandoned
    // drain.
    //
    // `prior` is the row as this statement reads it before its update (the
    // upsert above holds its lock), so the clear says whether the drain it
    // abandoned was a v2 one.
    let drain_cleared = sqlx::query_scalar::<_, bool>(sql!(
        "UPDATE nodes n
             SET drain_phase = NULL,
                 drain_mode = NULL,
                 drain_started_at = NULL,
                 drain_last_error = NULL,
                 drain_engine = NULL,
                 drain_status = NULL
             FROM node_k8s k, nodes prior
             WHERE n.id = $1
               AND k.node_id = n.id
               AND prior.id = n.id
               AND k.k8s_node_uid IS DISTINCT FROM $2
               AND (
                 n.drain_phase IS NOT NULL
                 OR n.drain_mode IS NOT NULL
                 OR n.drain_started_at IS NOT NULL
                 OR n.drain_last_error IS NOT NULL
                 OR n.drain_engine IS NOT NULL
                 OR n.drain_status IS NOT NULL
               )
               AND NOT COALESCE(
                 n.drain_engine IS NOT NULL AND k.instance_id = $3::TEXT,
                 FALSE
               )
             RETURNING COALESCE(prior.drain_engine = 'v2', FALSE)"
    ))
    .bind(node_id)
    .bind(node_uid)
    .bind(instance_id.map(Ec2InstanceId::as_str))
    .fetch_optional((&mut *conn).into_sqlx())
    .await
    .step(K8sNodeRegistrationStep::ClearDrain)?;
    if let Some(abandoned_v2) = drain_cleared {
        on_drain_cleared(abandoned_v2);
    }

    // A registration with no instance keeps the recorded one, unless the
    // Node's uid changed: a new Node may be a replacement on another machine,
    // so it takes no instance until one is reported. An instance alone
    // doesn't date the row: `updated_at` dates its Kubernetes metadata,
    // which absence demotion reads. The `nodes` upsert above locked the
    // node's row, so concurrent registrations of this node id apply in turn.
    let node_k8s_changed = sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (node_id) DO UPDATE
             SET k8s_node_name = EXCLUDED.k8s_node_name,
                 k8s_node_uid = EXCLUDED.k8s_node_uid,
                 instance_id = CASE
                     WHEN node_k8s.k8s_node_uid IS DISTINCT FROM EXCLUDED.k8s_node_uid
                     THEN EXCLUDED.instance_id
                     ELSE COALESCE(EXCLUDED.instance_id, node_k8s.instance_id)
                 END,
                 updated_at = CASE
                     WHEN node_k8s.k8s_node_name IS DISTINCT FROM EXCLUDED.k8s_node_name
                       OR node_k8s.k8s_node_uid IS DISTINCT FROM EXCLUDED.k8s_node_uid
                     THEN NOW()
                     ELSE node_k8s.updated_at
                 END
             WHERE node_k8s.k8s_node_name IS DISTINCT FROM EXCLUDED.k8s_node_name
                OR node_k8s.k8s_node_uid IS DISTINCT FROM EXCLUDED.k8s_node_uid
                OR (EXCLUDED.instance_id IS NOT NULL
                    AND node_k8s.instance_id IS DISTINCT FROM EXCLUDED.instance_id)"
    ))
    .bind(node_id)
    .bind(node_name)
    .bind(node_uid)
    .bind(instance_id.map(Ec2InstanceId::as_str))
    .execute((&mut *conn).into_sqlx())
    .await
    .step(K8sNodeRegistrationStep::UpsertNodeK8s)?
    .rows_affected()
        > 0;

    // Race-close re-check, paired with the pre-check above. A
    // concurrent `:terminate` that commits during our row-lock wait
    // would not have been visible to the pre-check but is now
    // visible — returning Err here causes the caller's transaction
    // to roll back, undoing the resurrection insert.
    if is_node_k8s_tombstoned(&mut *conn, node_id)
        .await
        .step(K8sNodeRegistrationStep::RecheckTombstone)?
    {
        return Err(K8sNodeRegistrationError::TombstonedDuringRegistration);
    }
    Ok(node_changed || drain_cleared.is_some() || node_k8s_changed)
}

async fn is_node_k8s_tombstoned(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
    ))
    .bind(node_id)
    .fetch_one(executor.into_sqlx())
    .await
}
