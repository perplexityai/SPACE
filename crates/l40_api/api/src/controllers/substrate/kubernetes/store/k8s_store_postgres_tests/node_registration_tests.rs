//! Kubernetes node registration through `upsert_k8s_node_role`, reached
//! through each production caller: who owns `nodes.state`, and how a
//! tombstone that commits mid-upsert rolls back every caller's transaction.

use pplx_space_common_core::types::NodeId;
use tokio_util::task::AbortOnDropHandle;

use super::*;

/// The production paths that register a Kubernetes node, each with the
/// state write it makes.
#[derive(Clone, Copy, Debug)]
pub(super) enum RegistrationCaller {
    /// `kube_node_sync`, the lifecycle owner.
    NodeSync,
    /// The admin inventory listing.
    InventoryListing,
    /// Pod placement, inside its own transaction.
    PodPlacement,
}

impl RegistrationCaller {
    pub(super) const ALL: [Self; 3] = [Self::NodeSync, Self::InventoryListing, Self::PodPlacement];

    /// Registers `node_id` as the caller would. `sandbox_id` names the
    /// sandbox pod placement records; the inventory callers ignore it.
    pub(super) async fn register(
        self,
        store: &K8sStore,
        sandbox_id: SandboxId,
        node_name: &str,
        node_id: NodeId,
    ) -> Result<(), SubstrateError> {
        let address = Some("10.0.0.70");
        let inventory = |state| {
            store.persist_k8s_node_inventory(
                node_name,
                "uid-registration",
                node_id,
                NodeRole::Worker,
                address,
                state,
                None,
            )
        };
        match self {
            Self::NodeSync => inventory(NodeStateWrite::Manage(NodeLifecycleState::Ready))
                .await
                .map(drop),
            Self::InventoryListing => {
                inventory(NodeStateWrite::InsertOnly(NodeLifecycleState::Ready))
                    .await
                    .map(drop)
            }
            Self::PodPlacement => {
                store
                    .persist_pod_node_placement(
                        sandbox_id,
                        node_name,
                        "uid-registration",
                        node_id,
                        NodeRole::Worker,
                        address,
                        None,
                    )
                    .await
            }
        }
    }
}

/// What any registration leaves behind for a node and the sandbox placed on
/// it: the `nodes` and `node_k8s` rows, the sandbox's active node, and the
/// active execution's Kubernetes node name.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct RegistrationFootprint {
    nodes: i64,
    node_k8s: i64,
    active_node_id: Option<NodeId>,
    execution_node_name: Option<String>,
}

impl RegistrationFootprint {
    pub(super) const NONE: Self = Self {
        nodes: 0,
        node_k8s: 0,
        active_node_id: None,
        execution_node_name: None,
    };
}

/// A started sandbox with an active execution, so pod placement has rows it
/// would write if registration let it continue.
pub(super) async fn insert_placeable_sandbox(fixture: &PostgresStore) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    Ok(sandbox_id)
}

pub(super) async fn registration_footprint(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> TestResult<RegistrationFootprint> {
    Ok(sqlx::query_as(sql!(
        "SELECT (SELECT COUNT(*) FROM nodes WHERE id = $1) AS nodes,
                (SELECT COUNT(*) FROM node_k8s WHERE node_id = $1) AS node_k8s,
                s.active_node_id,
                e.k8s_node_name AS execution_node_name
         FROM sandboxes s
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         WHERE s.id = $2"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Every `NodeLifecycleState`, so a sweep over it covers the whole enum.
fn every_node_lifecycle_state() -> [NodeLifecycleState; 2] {
    let all = [NodeLifecycleState::Ready, NodeLifecycleState::Registered];
    for state in all {
        // Adding a state fails this match until `all` lists it.
        match state {
            NodeLifecycleState::Ready | NodeLifecycleState::Registered => {}
        }
    }
    all
}

/// `Manage` writes `state` on insert and on conflict; `InsertOnly` writes it
/// only on insert and never overwrites an existing node's state, whichever
/// direction the write would move it. Every existing state (or none) meets
/// every write with the existing row's address and UID, so only `state`
/// differs. Each `InsertOnly` write over an existing row runs again with a
/// new address, so the conflict update fires: it must refresh the address and
/// keep the existing state.
///
/// `changed` is asserted for `Manage` only: `kube_node_sync` collects it, and
/// both `InsertOnly` callers discard it.
#[tokio::test(flavor = "current_thread")]
async fn only_a_managed_state_write_overwrites_an_existing_node_state() -> TestResult {
    let fixture = postgres_store().await?;
    let mut existing_states = vec![None];
    existing_states.extend(every_node_lifecycle_state().map(Some));
    let mut writes = Vec::new();
    for state in every_node_lifecycle_state() {
        writes.extend([
            NodeStateWrite::Manage(state),
            NodeStateWrite::InsertOnly(state),
        ]);
    }

    let mut cases = Vec::new();
    for existing in existing_states {
        for write in writes.iter().copied() {
            let node_id = NodeId::new();
            let node_name = format!("space-test-state-write-{}", node_id.as_uuid().simple());
            let persist = |address, state| {
                fixture.store.persist_k8s_node_inventory(
                    &node_name,
                    "uid-state-write",
                    node_id,
                    NodeRole::Worker,
                    Some(address),
                    state,
                    None,
                )
            };
            if let Some(existing) = existing {
                persist("10.0.0.71", NodeStateWrite::Manage(existing)).await?;
            }

            let changed = persist("10.0.0.71", write).await?;

            let expected = match (existing, write) {
                (None, NodeStateWrite::Manage(written) | NodeStateWrite::InsertOnly(written)) => {
                    written
                }
                (Some(_), NodeStateWrite::Manage(written)) => written,
                (Some(existing), NodeStateWrite::InsertOnly(_)) => existing,
            };
            let case = format!("existing {existing:?}, write {write:?}");
            if let NodeStateWrite::Manage(written) = write {
                assert_eq!(changed, existing != Some(written), "{case}: changed");
            }
            cases.push((node_id, expected, "10.0.0.71", case));

            // The same insert-only write over the existing row, with a new
            // address so the conflict update runs.
            if let (Some(existing), NodeStateWrite::InsertOnly(_)) = (existing, write) {
                let node_id = NodeId::new();
                let node_name = format!("space-test-state-write-{}", node_id.as_uuid().simple());
                let persist = |address, state| {
                    fixture.store.persist_k8s_node_inventory(
                        &node_name,
                        "uid-state-write",
                        node_id,
                        NodeRole::Worker,
                        Some(address),
                        state,
                        None,
                    )
                };
                persist("10.0.0.71", NodeStateWrite::Manage(existing)).await?;
                persist("10.0.0.72", write).await?;
                cases.push((
                    node_id,
                    existing,
                    "10.0.0.72",
                    format!("existing {existing:?}, write {write:?}, new address"),
                ));
            }
        }
    }

    // Read back after every case has run, so a write that strayed onto
    // another case's row shows up there too.
    for (node_id, expected, expected_address, case) in cases {
        let (state, address): (String, String) =
            sqlx::query_as(sql!("SELECT state, HOST(address) FROM nodes WHERE id = $1"))
                .bind(node_id)
                .fetch_one(&fixture.pool)
                .await?;
        assert_eq!(state, expected.as_str(), "{case}");
        assert_eq!(address, expected_address, "{case}: address");
    }
    Ok(())
}

/// An admin terminate (delete the `nodes` row, record the tombstone) that
/// commits while a registration waits on that row passes the tombstone
/// pre-check unseen. The registration then re-creates both rows, and only the
/// re-check after the upsert keeps them deleted: it fails the caller's
/// transaction, which rolls back every row the registration wrote.
/// Swept over every caller, so pod placement must also return the refusal and
/// commit none of its own writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_committed_mid_registration_rolls_back_every_caller() -> TestResult {
    // The held terminate, the blocked registration, and the probe that sees it
    // wait.
    let fixture = postgres_store_with_max_connections(3).await?;
    for caller in RegistrationCaller::ALL {
        let node_id = NodeId::new();
        let node_name = format!("space-test-terminated-{}", node_id.as_uuid().simple());
        let sandbox_id = insert_placeable_sandbox(&fixture).await?;
        RegistrationCaller::NodeSync
            .register(&fixture.store, sandbox_id, &node_name, node_id)
            .await?;

        let mut terminate = fixture.pool.begin().await?;
        let terminate_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *terminate)
            .await?;
        sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
            .bind(node_id)
            .execute(&mut *terminate)
            .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role, reason)
             VALUES ($1, $2, 'worker', 'admin-terminate')"
        ))
        .bind(node_id)
        .bind(&node_name)
        .execute(&mut *terminate)
        .await?;

        let store = fixture.store.clone();
        let name = node_name.clone();
        let registration = AbortOnDropHandle::new(tokio::spawn(async move {
            caller.register(&store, sandbox_id, &name, node_id).await
        }));
        wait_until_blocked_by(&fixture.pool, terminate_pid).await?;

        terminate.commit().await?;
        let result = tokio::time::timeout(Duration::from_secs(5), registration).await??;

        assert!(
            matches!(result, Err(SubstrateError::NodeTombstoned { .. })),
            "{caller:?}: {result:?}"
        );
        if let Err(err) = &result {
            assert!(
                err.to_string().contains("tombstoned"),
                "{caller:?}: the error must mention the tombstone: {err}"
            );
        }
        assert_eq!(
            registration_footprint(&fixture.pool, node_id, sandbox_id).await?,
            RegistrationFootprint::NONE,
            "{caller:?}: the terminated node must stay deleted"
        );
    }
    Ok(())
}
