use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};
use uuid::Uuid;

use super::*;

use crate::ClusterApiService;
use crate::proto::{
    NodeRole as ProtoNodeRole, PendingLocalBackupReport, PublishNodeStateRequest,
    PublishedSandboxState,
};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDb, insert_ready_worker_node,
    try_pg_test_db_with_max_connections,
};
use pplx_space_sbx_mgr_client::proto::{ArtifactKind as ProtoArtifactKind, PendingLocalBackup};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use std::time::Duration;
use time::OffsetDateTime;
use tonic::Request;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

struct Mirror {
    db: TestDb,
    service: ClusterApiService,
    node_id: NodeId,
}

async fn mirror() -> TestResult<Option<Mirror>> {
    mirror_with_max_connections(2).await
}

async fn mirror_with_max_connections(max_connections: u32) -> TestResult<Option<Mirror>> {
    let Some(db) = try_pg_test_db_with_max_connections(max_connections).await? else {
        return Ok(None);
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let service = ClusterApiService::new(db.pool.clone());
    Ok(Some(Mirror {
        db,
        service,
        node_id,
    }))
}

impl Mirror {
    fn pool(&self) -> &PgPool {
        &self.db.pool
    }

    /// A suspended sandbox with **no** artifact marker.
    ///
    /// Markerless is the point: the delete guard's departed-marker arm asks
    /// whether the sandbox's *current* artifact is durable, so a sandbox with
    /// no marker can only ever leave the queue through the own-bytes arm.
    /// Every test that seeds a snapshot to exercise artifact identity
    /// therefore exercises it alone, and the two tests that want the second
    /// disjunct call [`Mirror::pin_marker`] to opt in.
    async fn suspended_sandbox(&self) -> TestResult<SandboxId> {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, placement, active_node_id,
                 target_node_id, managed_by, network_policy_override_json,
                 network_policy_generation, placement_changed_at
             )
             VALUES ($1, $2, $3, 'suspended', 'on_node', NULL, $4, 'k8s', $5, 1, NOW())"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(self.node_id)
        .bind(sqlx::types::Json(
            &pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(self.pool())
        .await?;
        Ok(sandbox_id)
    }

    /// A `snapshots` row for one exact artifact, plus the storage location
    /// every real snapshot row carries from the moment it is inserted.
    ///
    /// `age` back-dates `snapshots.timestamp`. The delete guard reads no
    /// timestamp — both its arms are keyed on artifact identity — so this
    /// only keeps rows distinguishable in a fixture that seeds several, and
    /// matches production, where a snapshot predates the census that notices
    /// it.
    async fn snapshot(
        &self,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        kind: ArtifactKind,
        state: &str,
        age: Duration,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, source_node_id, org_id,
                 local_artifact_id, artifact_kind, artifact_origin, timestamp
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $3, 'suspend',
                     NOW() - make_interval(secs => $8))"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(kind.as_str())
        .bind(state)
        .bind(self.node_id)
        .bind(SEED_ORG_ID)
        .bind(artifact_id)
        .bind(age.as_secs_f64())
        .execute(self.pool())
        .await?;
        // `snapshot_store::insert` writes the storage location at
        // `BeginArtifactBackup`, before a byte is uploaded, so a `creating`
        // row has one too. Seeding it only for `ready` rows would make the
        // durability probe pass for the right answer by the wrong mechanism —
        // a missing join partner rather than a non-terminal state.
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations
                 (snapshot_id, storage_connection_string, org_id)
             VALUES ($1, 's3://space-test/pending-backup', $2)"
        ))
        .bind(snapshot_id)
        .bind(SEED_ORG_ID)
        .execute(self.pool())
        .await?;
        Ok(snapshot_id)
    }

    /// Pin `sandboxes.current_local_artifact_*`, which production writes on
    /// every suspend and stop, and which the delete guard's departed-marker arm
    /// reads: a queue row may go once the *marker's* artifact is durable, even
    /// when the queued artifact never was.
    async fn pin_marker(
        &self,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        kind: ArtifactKind,
    ) -> TestResult<()> {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(kind.as_str())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Move the sandbox off this mirror's node, the way a restore elsewhere
    /// does: placement writes `target_node_id = placed.node_id`, so both
    /// attachment columns name the new node.
    ///
    /// The delete guard's departed-marker arm requires this. A durable marker does
    /// not by itself order the marker against the queued artifact — the marker
    /// can lag behind what the node is reporting — so the disjunct fires only
    /// once the sandbox is attached somewhere else. Seeding a marker without
    /// calling this leaves the sandbox on the node, which is the *retain* case,
    /// not the release one.
    async fn relocate_to_other_node(&self, sandbox_id: SandboxId) -> TestResult<NodeId> {
        let other_node_id = NodeId::new();
        insert_ready_worker_node(self.pool(), other_node_id).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET active_node_id = $2,
                    target_node_id = $2,
                    placement = 'on_node'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(other_node_id)
        .execute(self.pool())
        .await?;
        Ok(other_node_id)
    }

    /// Give the sandbox a direct route naming a *different* node, without
    /// touching either `sandboxes` attachment column.
    ///
    /// `publish_sandbox_states` reads a direct-route sandbox's node from
    /// `current_node_id` in preference to `active_node_id`, so this is the
    /// residency signal the guard would otherwise miss entirely.
    async fn route_to_other_node(&self, sandbox_id: SandboxId) -> TestResult<NodeId> {
        let other_node_id = NodeId::new();
        insert_ready_worker_node(self.pool(), other_node_id).await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_identities (
                 sandbox_id, route_generation, current_node_id, substrate_kind,
                 execution_generation, activation_attempt_id
             )
             VALUES ($1, 1, $2, 'space', 1, $3)"
        ))
        .bind(sandbox_id)
        .bind(other_node_id)
        .bind(Uuid::now_v7())
        .execute(self.pool())
        .await?;
        Ok(other_node_id)
    }

    /// Mark the sandbox `deleted` without removing the row, the way the census
    /// prune does when a node stops reporting a space-managed sandbox.
    ///
    /// The queue row survives: the foreign key cascades on a row *delete*, and
    /// a tombstone is not one.
    async fn tombstone(&self, sandbox_id: SandboxId) -> TestResult {
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Detach the sandbox from every node in `sandboxes`, leaving any direct
    /// route as the only signal that names one.
    async fn unplace(&self, sandbox_id: SandboxId) -> TestResult {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET active_node_id = NULL,
                    target_node_id = NULL,
                    placement = 'unplaced'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Attach the sandbox to this mirror's node through `active_node_id`
    /// alone, clearing `target_node_id`.
    ///
    /// The distinction matters to the residency union: with the target column
    /// also naming this node, a predicate that consulted only the direct
    /// route and `active_node_id` would still be caught by the target
    /// disjunct, and the fixture would not discriminate.
    async fn attach_active_only(&self, sandbox_id: SandboxId) -> TestResult {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET active_node_id = $2,
                    target_node_id = NULL,
                    placement = 'on_node'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(self.node_id)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// A queue row as a prior census would have left it. `created_at` takes
    /// its default: no predicate reads it, so back-dating it would only
    /// suggest one does.
    async fn seed_queue_row(
        &self,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        kind: ArtifactKind,
    ) -> TestResult<()> {
        sqlx::query(sql!(
            "INSERT INTO node_local_backup_pending
                 (node_id, sandbox_id, artifact_id, artifact_kind)
             VALUES ($1, $2, $3, $4)"
        ))
        .bind(self.node_id)
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(kind.as_str())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn queue(&self) -> TestResult<Vec<(SandboxId, Uuid, String)>> {
        Ok(sqlx::query_as(sql!(
            "SELECT sandbox_id, artifact_id, artifact_kind
               FROM node_local_backup_pending
              WHERE node_id = $1
              ORDER BY sandbox_id"
        ))
        .bind(self.node_id)
        .fetch_all(self.pool())
        .await?)
    }

    async fn gate(&self) -> TestResult<Option<OffsetDateTime>> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT local_backup_inventory_reported_at
               FROM node_status_snapshots
              WHERE node_id = $1"
        ))
        .bind(self.node_id)
        .fetch_one(self.pool())
        .await?)
    }

    async fn backdate_gate(&self) -> TestResult<OffsetDateTime> {
        sqlx::query(sql!(
            "UPDATE node_status_snapshots
                SET local_backup_inventory_reported_at =
                        local_backup_inventory_reported_at - make_interval(hours => 1)
              WHERE node_id = $1"
        ))
        .bind(self.node_id)
        .execute(self.pool())
        .await?;
        Ok(self
            .gate()
            .await?
            .expect("back-dating requires an already-stamped gate"))
    }

    async fn publish(&self, census: Census) -> Result<(), tonic::Status> {
        self.service
            .publish_node_state(publish_request(self.node_id, census))
            .await
            .map(|_| ())
    }

    /// The shape most of the delete-guard tests need: the node still has the
    /// sandbox and **states** that nothing is pending for it.
    ///
    /// Not [`Census::census_only`], which is the node saying nothing at all —
    /// the difference between the two is the whole subject of
    /// [`an_absent_report_and_an_empty_one_take_different_branches`], and
    /// getting it wrong here would make every delete-guard test below assert
    /// against a publication the mirror declines outright.
    async fn reports_nothing_pending(&self, sandbox_id: SandboxId) -> Result<(), tonic::Status> {
        self.publish(Census::full(vec![suspended(sandbox_id)], Vec::new()))
            .await
    }
}

/// A free function rather than a `Mirror` method because the interleaving test
/// drives a publish from a spawned task, which cannot borrow the fixture. The
/// `TcpConnectInfo` extension is the part worth having in one place: without
/// it the publish fails origin authentication, and a second copy that drifted
/// would fail in a way that looks like the change under test.
fn publish_request(node_id: NodeId, census: Census) -> Request<PublishNodeStateRequest> {
    let mut request = Request::new(PublishNodeStateRequest {
        node_id: node_id.to_string(),
        role: Some(ProtoNodeRole::Worker as i32),
        sandbox_inventory_complete: census.inventory_complete,
        sandboxes: census.sandboxes,
        pending_local_backups: census.pending,
        ..PublishNodeStateRequest::default()
    });
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
        });
    request
}

/// One publication, built by the shape the test needs rather than by five
/// positional booleans at every call site.
struct Census {
    inventory_complete: bool,
    sandboxes: Vec<PublishedSandboxState>,
    /// `None` is the node saying nothing; `Some` with an empty `pending` is
    /// the node saying nothing is pending. The wire type keeps those apart.
    pending: Option<PendingLocalBackupReport>,
}

impl Census {
    /// The shape a healthy worker publishes every 60 seconds: a complete
    /// sandbox census and a complete pending-backup report. An empty `pending`
    /// here is the node stating that nothing is pending — contrast
    /// [`Census::census_only`], which is the node not answering.
    fn full(sandboxes: Vec<PublishedSandboxState>, pending: Vec<PendingLocalBackup>) -> Self {
        Self {
            inventory_complete: true,
            sandboxes,
            pending: Some(PendingLocalBackupReport { pending }),
        }
    }

    /// What a node image that predates the report field sends, and what an
    /// `Items` delta publish sends: the same sandbox census, no report.
    fn census_only(sandboxes: Vec<PublishedSandboxState>) -> Self {
        Self {
            inventory_complete: true,
            sandboxes,
            pending: None,
        }
    }

    fn incomplete_inventory(mut self) -> Self {
        self.inventory_complete = false;
        self
    }
}

fn suspended(sandbox_id: SandboxId) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some("suspended".to_string()),
        ..PublishedSandboxState::default()
    }
}

fn pending_backup(
    sandbox_id: SandboxId,
    artifact_id: Uuid,
    kind: ArtifactKind,
) -> PendingLocalBackup {
    PendingLocalBackup {
        sandbox_id: sandbox_id.to_string(),
        local_artifact_id: artifact_id.to_string(),
        artifact_kind: crate::metadata::artifact_kind_to_proto(kind),
    }
}

// ---------------------------------------------------------------------------
// The decision, without a database
// ---------------------------------------------------------------------------

async fn decide_for(
    mirror: &Mirror,
    pending: Option<Vec<PendingLocalBackup>>,
    prune: SandboxInventoryPrune,
    state_only: BTreeSet<SandboxId>,
) -> TestResult<PendingLocalBackupCensus> {
    let reported = pending.map(|pending| PendingLocalBackupReport { pending });
    let mut conn = mirror.pool().acquire().await?;
    Ok(decide(
        &mut conn,
        reported.as_ref(),
        prune,
        &state_only,
        mirror.node_id,
    )
    .await?)
}

fn declined(census: &PendingLocalBackupCensus) -> Option<LocalBackupQueueOutcome> {
    match census {
        PendingLocalBackupCensus::Declined(outcome) => Some(*outcome),
        PendingLocalBackupCensus::Report(_) => None,
    }
}

/// An absent report is "this node said nothing"; a present but empty one is
/// "this node has nothing pending". They must take different branches, and
/// only the second may advance the gate.
///
/// This is the seam between the two slices, and neither side can test it
/// alone. The node collapses every enumeration failure — sandbox-manager
/// unreachable, an RPC error, an `Unimplemented` from an older manager, any
/// delta publish — to "send no message", and that is a claim about what goes
/// on the wire. Whether the cluster then treats it as "no report" rather than
/// as an empty set is decided here, and no node-side test can observe that
/// branch. If the two ever converged, an older node's gate would be stamped
/// and its whole queue deleted on the following census, with the failure
/// visible on neither side alone.
#[tokio::test(flavor = "current_thread")]
async fn an_absent_report_and_an_empty_one_take_different_branches() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let census = decide_for(
        &mirror,
        None,
        SandboxInventoryPrune::Applied,
        BTreeSet::new(),
    )
    .await?;
    assert_eq!(
        declined(&census),
        Some(LocalBackupQueueOutcome::NoReport),
        "an absent report must not reconcile and must not stamp the gate"
    );

    // ...and a present, empty report is a usable statement that nothing is
    // pending, or the assertion above would hold for the wrong reason.
    let census = decide_for(
        &mirror,
        Some(Vec::new()),
        SandboxInventoryPrune::Applied,
        BTreeSet::new(),
    )
    .await?;
    assert!(matches!(census, PendingLocalBackupCensus::Report(_)));
    Ok(())
}

/// The report and the sandbox census come from one node-side enumeration, so
/// the server cannot certify one while refusing the other.
#[tokio::test(flavor = "current_thread")]
async fn a_census_the_server_refused_to_prune_carries_no_usable_report() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let census = decide_for(
        &mirror,
        Some(Vec::new()),
        SandboxInventoryPrune::Skipped,
        BTreeSet::new(),
    )
    .await?;
    assert_eq!(
        declined(&census),
        Some(LocalBackupQueueOutcome::CensusRejected)
    );
    Ok(())
}

/// Condition 4 in full: a publication is declined exactly when the control
/// plane believes a sandbox is holding a local artifact, this publication
/// could take that sandbox only as lifecycle state, **and** the report does
/// not name it.
///
/// All three conjuncts are load-bearing, and each drop fails differently:
///
/// * without the marker test, a degraded sandbox with nothing on disk — a
///   running one, say — would decline every census, and the node's mirror
///   would never advance;
/// * without the state-only test the guard fires on healthy publications;
/// * without the omission test, a degraded sandbox that *did* report its
///   artifact declines anyway, so one chronically degraded sandbox keeps the
///   node dark.
///
/// The marker is what makes this closed. A status list would have to name
/// `suspended`, `stopped`, the teardown-suspend transition and
/// `cleanup_required`, and would silently pass whatever it had not been told
/// about.
#[tokio::test(flavor = "current_thread")]
async fn only_an_undescribed_artifact_holder_the_report_omits_withdraws_it() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    #[derive(Debug)]
    struct Case {
        holds_artifact: bool,
        state_only: bool,
        reported: bool,
        declined: bool,
    }
    let cases = [
        Case {
            holds_artifact: true,
            state_only: true,
            reported: false,
            declined: true,
        },
        // Nothing on disk, so the report saying nothing about it is expected.
        Case {
            holds_artifact: false,
            state_only: true,
            reported: false,
            declined: false,
        },
        // The entry the degradation might have dropped is demonstrably there.
        Case {
            holds_artifact: true,
            state_only: true,
            reported: true,
            declined: false,
        },
        // Fully described: no reason to doubt the report.
        Case {
            holds_artifact: true,
            state_only: false,
            reported: false,
            declined: false,
        },
    ];
    for case in cases {
        let sandbox_id = mirror.suspended_sandbox().await?;
        if case.holds_artifact {
            mirror
                .pin_marker(sandbox_id, Uuid::now_v7(), ArtifactKind::Full)
                .await?;
        }
        let report = if case.reported {
            vec![pending_backup(
                sandbox_id,
                Uuid::now_v7(),
                ArtifactKind::Full,
            )]
        } else {
            Vec::new()
        };
        let state_only = if case.state_only {
            BTreeSet::from([sandbox_id])
        } else {
            BTreeSet::new()
        };
        let census = decide_for(
            &mirror,
            Some(report),
            SandboxInventoryPrune::Applied,
            state_only,
        )
        .await?;
        assert_eq!(
            declined(&census).is_some(),
            case.declined,
            "{case:?} must {} the report",
            if case.declined {
                "withdraw"
            } else {
                "leave standing"
            }
        );
        if case.declined {
            assert_eq!(
                declined(&census),
                Some(LocalBackupQueueOutcome::UndescribedSandbox)
            );
        }
    }
    Ok(())
}

/// Two entries for one sandbox contradict the one-row-per-`(node, sandbox)`
/// contract. Collapsing them would pick an artifact arbitrarily and leave the
/// loser looking like a row the node stopped reporting.
#[tokio::test(flavor = "current_thread")]
async fn two_entries_for_one_sandbox_are_refused_rather_than_collapsed() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let census = decide_for(
        &mirror,
        Some(vec![
            pending_backup(sandbox_id, Uuid::now_v7(), ArtifactKind::Full),
            pending_backup(sandbox_id, Uuid::now_v7(), ArtifactKind::Disk),
        ]),
        SandboxInventoryPrune::Applied,
        BTreeSet::new(),
    )
    .await?;
    assert_eq!(
        declined(&census),
        Some(LocalBackupQueueOutcome::MalformedReport)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unparseable_entry_refuses_the_whole_report() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let good = pending_backup(SandboxId::new(), Uuid::now_v7(), ArtifactKind::Full);

    let mut bad_sandbox = good.clone();
    bad_sandbox.sandbox_id = "not-a-uuid".to_string();
    let mut bad_artifact = good.clone();
    bad_artifact.local_artifact_id = "not-a-uuid".to_string();
    let mut bad_kind = good.clone();
    bad_kind.artifact_kind = ProtoArtifactKind::Unspecified as i32;

    for entry in [bad_sandbox, bad_artifact, bad_kind] {
        let census = decide_for(
            &mirror,
            Some(vec![good.clone(), entry.clone()]),
            SandboxInventoryPrune::Applied,
            BTreeSet::new(),
        )
        .await?;
        assert_eq!(
            declined(&census),
            Some(LocalBackupQueueOutcome::MalformedReport),
            "a report containing {entry:?} must be refused whole: a partially \
             parsed set is not the node's answer, and the set-replace would \
             read the dropped entries as no longer pending"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The set-replace, against Postgres
// ---------------------------------------------------------------------------

/// Insert-missing plus set equality: after a clean census the node's rows are
/// exactly what it reported.
#[tokio::test(flavor = "current_thread")]
async fn a_clean_census_mirrors_exactly_the_reported_set() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let pending_sandbox = mirror.suspended_sandbox().await?;
    let quiet_sandbox = mirror.suspended_sandbox().await?;
    let pending_artifact = Uuid::now_v7();

    mirror
        .publish(Census::full(
            vec![suspended(pending_sandbox), suspended(quiet_sandbox)],
            vec![pending_backup(
                pending_sandbox,
                pending_artifact,
                ArtifactKind::Full,
            )],
        ))
        .await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(pending_sandbox, pending_artifact, "full".to_string())],
        "the queue holds the reported set and nothing else: a suspended \
         sandbox the node did not name as pending gets no row at all"
    );
    assert!(
        mirror.gate().await?.is_some(),
        "a mirrored census must stamp the gate that permits believing an \
         empty queue"
    );
    Ok(())
}

/// The gate is re-stamped by every accepted census rather than latched once,
/// which is what lets a reader's freshness window notice a writer that
/// *stops* mirroring — a rollback to a build without this path, or a node
/// whose every census is now declined — instead of trusting one old success
/// forever.
///
/// Aging is deliberately **not** what covers a same-name node replacement:
/// the successor's first accepted census re-stamps the gate immediately, so
/// it never goes stale for a window to catch. That case is covered by the
/// guarded set-replace instead, which keeps any predecessor row it cannot
/// prove recoverable. This test pins the advancement, not that guarantee.
#[tokio::test(flavor = "current_thread")]
async fn the_gate_advances_on_every_accepted_census() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    let first = mirror.backdate_gate().await?;

    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    let second = mirror.gate().await?.expect("a mirrored census must stamp");
    assert!(
        second > first,
        "the gate must move with each accepted census: {second} is not after \
         {first}"
    );
    Ok(())
}

/// Upsert-identity: a sandbox that woke, changed and re-suspended onto a new
/// artifact keeps one row, now naming the new artifact.
///
/// The replacement is the only write that stops tracking an artifact without
/// the delete guard's proof, which is safe for the reason the guard's second
/// disjunct is safe: the node has told us the pin moved, so the displaced
/// artifact is state something already restored from.
#[tokio::test(flavor = "current_thread")]
async fn re_reporting_a_sandbox_replaces_its_artifact_identity() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let first_artifact = Uuid::now_v7();
    let second_artifact = Uuid::now_v7();

    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(
                sandbox_id,
                first_artifact,
                ArtifactKind::Disk,
            )],
        ))
        .await?;
    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, first_artifact, "disk".to_string())],
        "a stopped sandbox's disk artifact is queued the same way a suspended \
         sandbox's full artifact is"
    );

    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(
                sandbox_id,
                second_artifact,
                ArtifactKind::Full,
            )],
        ))
        .await?;
    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, second_artifact, "full".to_string())],
        "one row per (node, sandbox), carrying the current artifact"
    );
    Ok(())
}

/// Delete-durable-absent: the node stopped reporting the artifact and the
/// control plane can see a ready, non-orphaned copy of that exact artifact.
#[tokio::test(flavor = "current_thread")]
async fn an_absent_artifact_with_a_durable_copy_leaves_the_queue() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, artifact_id, ArtifactKind::Full)
        .await?;
    mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(300),
        )
        .await?;

    mirror.reports_nothing_pending(sandbox_id).await?;

    assert!(mirror.queue().await?.is_empty());
    Ok(())
}

/// Keep-and-alert: the same absence, with no durable copy anywhere. This is
/// the row that answers "what did that disk take with it", so the census must
/// not remove it on the node's silence alone.
#[tokio::test(flavor = "current_thread")]
async fn an_absent_artifact_with_no_durable_copy_is_kept_and_counted() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, artifact_id, ArtifactKind::Full)
        .await?;
    // An upload that started and never finished is not a durable copy, and it
    // is the shape most likely to be mistaken for one.
    mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(300),
        )
        .await?;

    mirror.reports_nothing_pending(sandbox_id).await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact_id, "full".to_string())],
        "an artifact the node stopped reporting, with nothing durable to fall \
         back on, stays in the queue"
    );
    Ok(())
}

/// The reconcile's derived counts must agree with the rows it leaves behind.
///
/// `unknown_sandboxes` is `reported - upserted`; `depth` and `retained_absent`
/// are a post-delete `COUNT` and a filtered `COUNT` over the same scan. None of
/// the three is recoverable from any single row, so every row assertion in this
/// file can hold while all three are wrong. An earlier revision of this module
/// proved it: a lifecycle guard on the upsert made `unknown_sandboxes` count
/// skipped tombstones, and the queue contents were correct throughout while the
/// count reported an inventory disagreement that had not happened.
///
/// These are the values `reconcile` **returns**. What `emit_metrics` does with
/// them afterwards is telemetry and is asserted nowhere.
#[tokio::test(flavor = "current_thread")]
async fn the_reconcile_counts_agree_with_the_rows_it_leaves() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };

    // Reported again: the upsert affects it, it stays, and it is not absent.
    let reported_again = mirror.suspended_sandbox().await?;
    let reported_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(reported_again, reported_artifact, ArtifactKind::Full)
        .await?;

    // Absent with a durable copy: the delete releases it.
    let released = mirror.suspended_sandbox().await?;
    let released_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(released, released_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .snapshot(
            released,
            released_artifact,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(300),
        )
        .await?;

    // Absent with only an unfinished upload: kept, and counted as a casualty.
    let casualty = mirror.suspended_sandbox().await?;
    let casualty_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(casualty, casualty_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .snapshot(
            casualty,
            casualty_artifact,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(300),
        )
        .await?;

    // Reported *and* tombstoned. The census prune mints `deleted` from absence
    // earlier in the same transaction, so this is an ordinary steady state, and
    // the upsert must still affect the row: an earlier revision skipped
    // tombstones, which silently moved them into the reported-minus-upserted
    // gap and alerted an inventory disagreement that had not happened. Without
    // this row in the fixture, restoring that guard leaves every assertion
    // below green.
    let reported_tombstone = mirror.suspended_sandbox().await?;
    let tombstone_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(reported_tombstone, tombstone_artifact, ArtifactKind::Full)
        .await?;
    mirror.tombstone(reported_tombstone).await?;

    // Reported, but the control plane has no row for it: the upsert's `EXISTS`
    // filters it out, so it is the whole of the reported-minus-upserted gap.
    let unknown = SandboxId::new();

    let census = decide_for(
        &mirror,
        Some(vec![
            pending_backup(reported_again, reported_artifact, ArtifactKind::Full),
            pending_backup(reported_tombstone, tombstone_artifact, ArtifactKind::Full),
            pending_backup(unknown, Uuid::now_v7(), ArtifactKind::Full),
        ]),
        SandboxInventoryPrune::Applied,
        BTreeSet::new(),
    )
    .await?;
    let PendingLocalBackupCensus::Report(report) = census else {
        panic!("a well-formed report on an accepted census must mirror");
    };

    let mut conn = mirror.pool().acquire().await?;
    let counts = reconcile(&mut conn, mirror.node_id, &report).await?;

    // Re-derive from the rows rather than restating the expected numbers, so a
    // wrong derivation fails here even if the fixture drifts.
    let (rows, absent_rows): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE NOT (sandbox_id = ANY($2)))
           FROM node_local_backup_pending
          WHERE node_id = $1"
    ))
    .bind(mirror.node_id)
    .bind(vec![reported_again, reported_tombstone])
    .fetch_one(&mut *conn)
    .await?;

    assert_eq!(
        (counts.depth as i64, counts.retained_absent as i64),
        (rows, absent_rows),
        "depth and retained_absent must equal the rows actually left for this \
         node, and the casualty count must be the absent subset of them"
    );
    assert_eq!(
        (rows, absent_rows),
        (3, 1),
        "the fixture must leave both reported rows and the casualty, of which \
         only the casualty is absent \u{2014} otherwise the check above is vacuous"
    );
    assert_eq!(
        counts.unknown_sandboxes, 1,
        "only the entry with no `sandboxes` row is unknown: the upsert affects \
         the tombstoned row too, so a guard that skipped tombstones would \
         report two here"
    );
    assert_eq!(
        counts.deleted_durable, 1,
        "only the absent artifact with a durable copy is released"
    );
    Ok(())
}

/// A sandbox whose pin has moved on: the queued artifact was never backed up,
/// but the sandbox's current artifact is durable, so the stale bytes on this
/// node are not the only copy of anything a restore would load.
///
/// This is the disjunct that stops the queue filling with false casualties —
/// a sandbox that suspends here, restores elsewhere and re-suspends there
/// leaves a row behind whose artifact will never gain a snapshot of its own.
///
/// It is deliberately *not* a timestamp comparison. `snapshots.timestamp` is
/// the row's insert time, so a deferred backup of an **older** artifact lands
/// after a **newer** artifact was queued, and any "newer than X" test would
/// read that as the newer artifact being superseded by older bytes. The
/// predicate is keyed on the sandbox's marker identity instead.
#[tokio::test(flavor = "current_thread")]
async fn an_absent_artifact_whose_sandbox_moved_on_leaves_the_queue() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let queued_artifact = Uuid::now_v7();
    let marker_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, queued_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .pin_marker(sandbox_id, marker_artifact, ArtifactKind::Full)
        .await?;
    // Stamped *older* than the queue row, so no timestamp ordering could
    // justify the delete — only the marker can.
    mirror
        .snapshot(
            sandbox_id,
            marker_artifact,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(900),
        )
        .await?;
    // The relocation is what makes the name true and the delete correct. A
    // durable marker on a sandbox still attached to this node is the retain
    // case, covered by the test below; without this line the two are the same
    // fixture and neither would be testing what it claims.
    mirror.relocate_to_other_node(sandbox_id).await?;

    // Absent from this node's census, not merely reported with nothing
    // pending: a sandbox that moved to another node is not in its old node's
    // inventory at all, and seeding it as present would describe a state
    // production does not produce.
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;

    assert!(
        mirror.queue().await?.is_empty(),
        "the sandbox left this node and its current artifact is durable, so \
         the artifact this row tracked is superseded rather than lost"
    );
    Ok(())
}

/// A census that stops naming a space-managed sandbox must not erase its
/// casualty row, even though the prune tombstones the sandbox first.
///
/// This is the same-name node replacement, which is the case the queue most
/// exists for. A replacement machine reuses `nodes.id`, so its very first
/// complete census omits every predecessor sandbox.
/// `prune_absent_node_sandboxes` marks those `deleted` — in the *same
/// transaction*, before the mirror runs — so `lifecycle` cannot be used to
/// release a row here: it would release exactly the rows recording what the
/// predecessor's disk took with it, without testing durability at all, and
/// the successor's census would then stamp a gate certifying the empty
/// queue.
///
/// The cost of getting this right is a deleted sandbox's row staying counted
/// into `absent_not_durable` until its `sandboxes` row is hard-deleted and
/// the foreign key cascades. That is metric noise; this is a record.
#[tokio::test(flavor = "current_thread")]
async fn a_census_that_stops_naming_a_sandbox_keeps_its_casualty_row() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_override_json,
             network_policy_generation, placement_changed_at
         )
         VALUES ($1, $2, $3, 'started', 'on_node', $4, $4, 'space', $5, 1, NOW())"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(mirror.node_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(mirror.pool())
    .await?;
    let artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, artifact, ArtifactKind::Full)
        .await?;

    // The successor's first census: complete, and naming nothing.
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(mirror.pool())
            .await?;
    assert_eq!(
        lifecycle, "deleted",
        "precondition: the prune must have tombstoned this sandbox, or the \
         test is not exercising the path it describes"
    );
    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact, "full".to_string())],
        "the artifact had no durable copy and the node that held it is gone, \
         which is precisely a casualty — the tombstone the prune just wrote \
         must not release the record of it"
    );
    Ok(())
}

/// The converse, and the one that matters for data loss: a durable marker on a
/// sandbox that is **still attached to this node** does not release the row.
///
/// Durability of the marker says nothing about whether the marker is newer
/// than the queued artifact, and two same-node paths leave it older.
/// `collect_sandbox_states` runs before `collect_pending_local_backups`, so a
/// suspend landing between the two round trips is reported while the census
/// still carries the pre-suspend row; and a `state_degraded` sandbox skips the
/// marker assignment for as long as it stays degraded. In both, the queue row
/// holds the *newer* artifact and the marker the older one, so deleting on
/// marker durability alone discards the only copy of the newer state.
///
/// Seeded in the shape that makes the distinction load-bearing: the queued
/// artifact has no snapshot at all, so the own-bytes arm cannot fire and the
/// row's fate rests entirely on the second.
#[tokio::test(flavor = "current_thread")]
async fn a_durable_marker_does_not_release_a_row_while_the_sandbox_stays_on_the_node() -> TestResult
{
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let queued_artifact = Uuid::now_v7();
    let marker_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, queued_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .pin_marker(sandbox_id, marker_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .snapshot(
            sandbox_id,
            marker_artifact,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(900),
        )
        .await?;
    // `suspended_sandbox` seeds `active_node_id = NULL`, which is already the
    // post-reaper shape the test below covers. Attach it for real, so this
    // test exercises `active_node_id = <node>` and the two are not the same
    // fixture under different names.
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(mirror.node_id)
    .execute(mirror.pool())
    .await?;

    mirror.reports_nothing_pending(sandbox_id).await?;

    let queue = mirror.queue().await?;
    assert_eq!(
        queue,
        vec![(
            sandbox_id,
            queued_artifact,
            ArtifactKind::Full.as_str().to_owned()
        )],
        "the sandbox is still attached to this node, so the marker may yet \
         catch up to the queued artifact and the row is the only record that \
         this node holds it"
    );
    Ok(())
}

/// The reaper's unplace must not read as "the sandbox left".
///
/// `suspended_execution_reaper` sets `active_node_id = NULL` but carries the
/// node forward with `target_node_id = COALESCE(target_node_id,
/// active_node_id)`, precisely so downstream predicates can still attribute
/// the sandbox to its node. A residency test on `active_node_id` alone would
/// read that as a relocation and release the row; requiring both columns is
/// what stops it.
#[tokio::test(flavor = "current_thread")]
async fn an_unplaced_sandbox_still_targeting_this_node_keeps_its_row() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let queued_artifact = Uuid::now_v7();
    let marker_artifact = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, queued_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .pin_marker(sandbox_id, marker_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .snapshot(
            sandbox_id,
            marker_artifact,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(900),
        )
        .await?;
    // The post-reaper shape: unplaced, no active node, target still this one.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'unplaced', active_node_id = NULL, target_node_id = $2
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(mirror.node_id)
    .execute(mirror.pool())
    .await?;

    mirror.reports_nothing_pending(sandbox_id).await?;

    assert_eq!(
        mirror.queue().await?.len(),
        1,
        "an unplaced sandbox whose target is still this node has not left it"
    );
    Ok(())
}

/// The same identity rule one column over: `artifact_kind` is part of the key,
/// not decoration.
#[tokio::test(flavor = "current_thread")]
async fn a_durable_copy_of_the_other_kind_does_not_clear_the_row() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, artifact_id, ArtifactKind::Full)
        .await?;
    // Same artifact id, `disk` rather than `full`. The departed-marker arm cannot
    // fire either: this sandbox has no marker.
    mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Disk,
            "ready",
            Duration::from_secs(900),
        )
        .await?;

    mirror.reports_nothing_pending(sandbox_id).await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact_id, "full".to_string())]
    );
    Ok(())
}

/// A reported sandbox with no `sandboxes` row cannot be mirrored: the queue's
/// foreign key would reject it. Skipping the entry keeps one unknown identity
/// from aborting a whole node's census.
#[tokio::test(flavor = "current_thread")]
async fn an_unknown_sandbox_is_skipped_rather_than_failing_the_publication() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let known = mirror.suspended_sandbox().await?;
    let known_artifact = Uuid::now_v7();
    let unknown = SandboxId::new();

    mirror
        .publish(Census::full(
            vec![suspended(known)],
            vec![
                pending_backup(known, known_artifact, ArtifactKind::Full),
                pending_backup(unknown, Uuid::now_v7(), ArtifactKind::Full),
            ],
        ))
        .await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(known, known_artifact, "full".to_string())]
    );
    assert!(
        mirror.gate().await?.is_some(),
        "the gate still advances: a sandbox with no `sandboxes` row has nothing \
         to recover and nothing a drain could strand, so an entry naming one is \
         a reporting gap rather than a reason to distrust the rest of the report"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

/// Walk one node through every publication shape that must not advance the
/// gate, and check *which* of the two non-advancing outcomes each gets.
///
/// The gate is three-way, and the distinction is the point. A publication
/// that could not have mirrored — a facet tick carrying no sandbox inventory
/// — must leave a good gate alone. A publication that **could** have
/// mirrored and did not must clear it: the queue is then known to be
/// un-refreshed, and a preserved timestamp would let a reader's freshness
/// window keep certifying an empty queue the server knows it did not
/// refresh. Preserving both alike is the defect this ordering exists to
/// prevent, and a stamp taken from the wire flag would certify a mirror that
/// never ran.
#[tokio::test(flavor = "current_thread")]
async fn only_a_mirrored_census_advances_the_gate() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;

    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    let stamped = mirror.backdate_gate().await?;

    // 1. An `Items` delta tick: no report, and no complete inventory either,
    //    so it never could have mirrored. Preserve.
    mirror
        .publish(Census::census_only(vec![suspended(sandbox_id)]).incomplete_inventory())
        .await?;
    assert_eq!(
        mirror.gate().await?,
        Some(stamped),
        "a tick that could not have mirrored must neither stamp nor blank the \
         gate"
    );

    // 2. A report whose sandbox census the server did not accept — also not
    //    prunable, so also a preserve.
    mirror
        .publish(Census::full(vec![suspended(sandbox_id)], Vec::new()).incomplete_inventory())
        .await?;
    assert_eq!(
        mirror.gate().await?,
        Some(stamped),
        "a census the server did not prune against cannot certify the report \
         published with it, and could not have mirrored either"
    );

    // 2a. A Full census the server could not prune against because one
    //     sandbox id did not parse. The node still *claimed* a complete
    //     inventory, so it meant to give a full answer and could not — the
    //     gate is withdrawn. This is the arm that distinguishes keying
    //     withdrawal on the node's claim from keying it on the server's
    //     prune verdict, which a single bad id downgrades.
    let mut unparseable = suspended(sandbox_id);
    unparseable.sandbox_id = "not-a-uuid".to_string();
    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id), unparseable],
            Vec::new(),
        ))
        .await?;
    assert_eq!(
        mirror.gate().await?,
        None,
        "a node that claimed a complete inventory and shipped one it could \
         not be pruned against has not refreshed the queue, so the gate is \
         withdrawn rather than left to age"
    );
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    mirror.backdate_gate().await?;

    // 2b. A *Full* census carrying no report at all — sandbox-manager timed
    //     out, or the writer rolled back to a build without this path. This
    //     one could have mirrored and did not, so the gate is cleared rather
    //     than left to age out: a new pending artifact since the last mirror
    //     is absent from a queue that still looks certified.
    mirror
        .publish(Census::census_only(vec![suspended(sandbox_id)]))
        .await?;
    assert_eq!(
        mirror.gate().await?,
        None,
        "a Full census that could not mirror must clear the gate, not leave a \
         recent timestamp certifying a queue it did not refresh"
    );
    // Re-arm the gate so the next arm's clear is observable rather than a
    // no-op against an already-NULL column. The timestamp is not compared
    // again until the final arm, which re-arms for itself.
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    mirror.backdate_gate().await?;

    // 3. A degraded row in the same publication, which the report then says
    //    nothing about, for a sandbox the control plane knows is holding a
    //    local artifact. This arm is also what pins the coupling to
    //    `publish_sandbox_states`: it drives a real `state_degraded` flag
    //    through the real producer, so the mirror reading
    //    `state_only_sandbox_ids` rather than the wire flag is asserted end to
    //    end rather than argued.
    mirror
        .pin_marker(sandbox_id, Uuid::now_v7(), ArtifactKind::Full)
        .await?;
    let mut degraded = suspended(sandbox_id);
    degraded.state_degraded = true;
    mirror
        .publish(Census::full(vec![degraded], Vec::new()))
        .await?;
    assert_eq!(
        mirror.gate().await?,
        None,
        "a node that could not fully describe a sandbox it is holding an \
         artifact for, and named no pending artifact for it, has not earned \
         permission to have its empty queue believed — and since this census \
         could have mirrored, the stale permission is withdrawn rather than \
         left to expire"
    );
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    let stamped = mirror.backdate_gate().await?;

    // 4. A malformed report.
    let mut malformed = pending_backup(sandbox_id, Uuid::now_v7(), ArtifactKind::Full);
    malformed.local_artifact_id = "not-a-uuid".to_string();
    mirror
        .publish(Census::full(vec![suspended(sandbox_id)], vec![malformed]))
        .await?;
    assert_eq!(
        mirror.gate().await?,
        None,
        "an unusable report on a census that could have mirrored withdraws the \
         gate rather than leaving it where the last usable one put it"
    );

    // ...and the clean shape still moves it, so the assertions above are not
    // simply a column that never changes.
    mirror.reports_nothing_pending(sandbox_id).await?;
    let advanced = mirror.gate().await?.expect("a mirrored census must stamp");
    assert!(
        advanced > stamped,
        "an accepted census must advance the gate: {advanced} is not after \
         {stamped}"
    );
    Ok(())
}

/// A degraded sandbox whose marker is already backed up must not withdraw
/// this node's report either.
///
/// `list_pending_backups` filters on `backup_completed_at IS NULL`, so an
/// artifact whose backup finished is one the node is *right* to omit. Reading
/// that omission as a gap declines the census — and since a declined census
/// withdraws the gate, one chronically degraded sandbox with an old, durable
/// marker would hold its node on the history predicate indefinitely.
///
/// Contrast the gate walk above, whose degraded arm pins a marker with no
/// snapshot at all: that one is still pending, so declining is correct. The
/// difference between the two is the whole conjunct.
#[tokio::test(flavor = "current_thread")]
async fn a_degraded_sandbox_whose_marker_is_backed_up_does_not_withdraw_the_report() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let marker_artifact = Uuid::now_v7();
    mirror
        .pin_marker(sandbox_id, marker_artifact, ArtifactKind::Full)
        .await?;
    mirror
        .snapshot(
            sandbox_id,
            marker_artifact,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(900),
        )
        .await?;

    let mut degraded = suspended(sandbox_id);
    degraded.state_degraded = true;
    mirror
        .publish(Census::full(vec![degraded], Vec::new()))
        .await?;

    assert!(
        mirror.gate().await?.is_some(),
        "a marker with a durable copy is not pending, so a report that omits \
         it has no gap and the census must be accepted"
    );
    Ok(())
}

/// A degraded observation for a sandbox that has moved away must not
/// withdraw this node's report.
///
/// `state_only_sandbox_ids` is built in the Rust loop before ownership is
/// consulted, so a stale foreign id reaches the guard even though
/// `publish_sandbox_states` drops that row from its writes. The sandbox's
/// marker then names the *other* node's artifact — which this node is right
/// not to describe.
///
/// Without the residency scoping this is not a one-tick nuisance: the report
/// is declined on every census, and since a declined census withdraws the
/// gate, the node is held on the expensive history predicate for as long as
/// it keeps reporting the stale row.
#[tokio::test(flavor = "current_thread")]
async fn a_degraded_sandbox_that_moved_away_does_not_withdraw_the_report() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    // Holding an artifact, and now living on a different node.
    mirror
        .pin_marker(sandbox_id, Uuid::now_v7(), ArtifactKind::Full)
        .await?;
    mirror.relocate_to_other_node(sandbox_id).await?;

    // This node still reports it, degraded, and names no pending artifact.
    let mut degraded = suspended(sandbox_id);
    degraded.state_degraded = true;
    mirror
        .publish(Census::full(vec![degraded], Vec::new()))
        .await?;

    assert!(
        mirror.gate().await?.is_some(),
        "a sandbox that has moved to another node is not this node's to \
         describe, so omitting it from the report is correct and must not \
         withdraw the gate"
    );
    Ok(())
}

/// The same exclusion, reached through the direct route rather than through
/// the `sandboxes` attachment columns.
///
/// A direct-route sandbox can be unplaced in `sandboxes` while
/// `sandbox_direct_route_identities.current_node_id` still names the node
/// running it — that column is what `publish_sandbox_states` scopes its
/// observed-state write by. Reading only `active_node_id` / `target_node_id`
/// sees a sandbox attached nowhere, keeps it in scope, and declines this
/// node's census on every tick for a sandbox it does not own.
#[tokio::test(flavor = "current_thread")]
async fn a_degraded_sandbox_routed_elsewhere_does_not_withdraw_the_report() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    mirror
        .pin_marker(sandbox_id, Uuid::now_v7(), ArtifactKind::Full)
        .await?;
    // Attached to no node in `sandboxes`; the direct route is the only signal
    // that names one, and it names someone else.
    mirror.unplace(sandbox_id).await?;
    mirror.route_to_other_node(sandbox_id).await?;

    let mut degraded = suspended(sandbox_id);
    degraded.state_degraded = true;
    mirror
        .publish(Census::full(vec![degraded], Vec::new()))
        .await?;

    assert!(
        mirror.gate().await?.is_some(),
        "the direct route names another node, so this node is right not to \
         describe the artifact and the omission must not withdraw the gate"
    );
    Ok(())
}

/// The counterpart, and the reason residency is a union rather than a
/// preference: a sandbox this node still holds must keep declining even when
/// a direct-route row names somewhere else.
///
/// `publish_sandbox_states` resolves this disagreement by preferring
/// `current_node_id` (`published_state.rs:2259`), which is right for choosing
/// whose observation to record. Importing that preference here would read
/// this sandbox as departed and certify a census that omits an artifact with
/// no durable copy — on the node whose disk is holding it.
///
/// The fixture is built to discriminate: `target_node_id` is cleared, so
/// `active_node_id` is the only column naming this node and a predicate that
/// preferred the route over it has nothing else to catch the sandbox.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_route_elsewhere_does_not_certify_a_sandbox_still_held_here() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    mirror
        .pin_marker(sandbox_id, Uuid::now_v7(), ArtifactKind::Full)
        .await?;
    mirror.attach_active_only(sandbox_id).await?;
    mirror.route_to_other_node(sandbox_id).await?;

    let mut degraded = suspended(sandbox_id);
    degraded.state_degraded = true;
    mirror
        .publish(Census::full(vec![degraded], Vec::new()))
        .await?;

    assert!(
        mirror.gate().await?.is_none(),
        "`active_node_id` still names this node, so the artifact may be on \
         this disk; a route naming elsewhere must not release the census"
    );
    Ok(())
}

/// A declined census must not touch the rows either, not just the gate.
///
/// The gate assertion above cannot catch this: a set-replace that ran and then
/// failed to stamp would look identical from the gate's side and would have
/// already deleted the rows the decline exists to protect.
#[tokio::test(flavor = "current_thread")]
async fn a_declined_census_leaves_the_rows_alone() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    mirror
        .seed_queue_row(sandbox_id, artifact_id, ArtifactKind::Full)
        .await?;
    // Durable, so a set-replace that ran would certainly delete it.
    mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Full,
            "ready",
            Duration::from_secs(300),
        )
        .await?;

    mirror
        .publish(Census::census_only(vec![suspended(sandbox_id)]))
        .await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact_id, "full".to_string())],
        "a node that reported nothing has not said the artifact is gone"
    );
    assert_eq!(
        mirror.gate().await?,
        None,
        "and its gate was never stamped in the first place"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The finalize fast path
// ---------------------------------------------------------------------------

/// Finalize as the worker node the fixtures register at `127.0.0.1`.
async fn finalize(
    service: &ClusterApiService,
    snapshot_id: SnapshotId,
    state: crate::proto::SuspendSnapshotTerminalState,
) -> Result<(), tonic::Status> {
    let mut request = Request::new(crate::proto::FinalizeArtifactBackupRequest {
        backup_attempt_id: snapshot_id.to_string(),
        target_state: state as i32,
        bytes_uploaded: 1,
        failure_reason: None,
        artifact_digests: Vec::new(),
        suspend_uncommitted: false,
    });
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 49000))),
        });
    service.finalize_artifact_backup(request).await.map(|_| ())
}

/// The fast path removes the row for exactly the artifact it made durable, and
/// finalizing an older artifact leaves a newer artifact's row standing.
///
/// This is why `artifact_id` and `artifact_kind` are in the queue's payload
/// rather than being treated as annotations: a delete keyed on
/// `(node, sandbox)` alone would pass every other test in this file and lose
/// the newer artifact here.
#[tokio::test(flavor = "current_thread")]
async fn finalizing_an_old_artifact_leaves_the_new_artifacts_row() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let old_artifact = Uuid::now_v7();
    let new_artifact = Uuid::now_v7();
    let old_snapshot = mirror
        .snapshot(
            sandbox_id,
            old_artifact,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(600),
        )
        .await?;

    // The node has already re-suspended onto a newer artifact, so the census
    // upserted the row forward while the old upload was still in flight.
    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(sandbox_id, new_artifact, ArtifactKind::Full)],
        ))
        .await?;

    finalize(
        &mirror.service,
        old_snapshot,
        crate::proto::SuspendSnapshotTerminalState::Ready,
    )
    .await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, new_artifact, "full".to_string())],
        "finalizing the old artifact must not remove the row tracking the new \
         one, whose bytes are still only on this node"
    );
    Ok(())
}

/// A `ready` finalize whose only storage location was already orphaned leaves
/// the row, because no live copy exists to have made the artifact durable.
///
/// The fast path has no census backstop, which is what makes this load-bearing
/// rather than belt-and-braces. The node stamps
/// `local_artifacts.backup_completed_at` immediately after the finalize
/// returns `ready`, and `list_pending_backups` filters on that column being
/// NULL, so the next census no longer reports the artifact and a row deleted
/// here would never come back. Deleting on the `ready` state alone would
/// discard the casualty record for bytes that exist nowhere but this node.
#[tokio::test(flavor = "current_thread")]
async fn a_finalize_onto_an_orphaned_location_leaves_the_row() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact = Uuid::now_v7();
    let snapshot_id = mirror
        .snapshot(
            sandbox_id,
            artifact,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(600),
        )
        .await?;
    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(sandbox_id, artifact, ArtifactKind::Full)],
        ))
        .await?;
    // Orphaned before the finalize lands — the sweeper and the sandbox-delete
    // path both reach this state while an upload is still `creating`.
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id)
    .execute(mirror.pool())
    .await?;

    finalize(
        &mirror.service,
        snapshot_id,
        crate::proto::SuspendSnapshotTerminalState::Ready,
    )
    .await?;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact, "full".to_string())],
        "a ready snapshot whose only storage location is orphaned is not a \
         durable copy, so the row must survive the finalize"
    );
    Ok(())
}

/// The positive control for the test above, plus the `failed` arm: a finalize
/// that did not make bytes durable removes nothing.
#[tokio::test(flavor = "current_thread")]
async fn only_a_ready_finalize_of_the_queued_artifact_clears_its_row() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    let failed_attempt = mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(600),
        )
        .await?;
    let ready_attempt = mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(300),
        )
        .await?;
    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(sandbox_id, artifact_id, ArtifactKind::Full)],
        ))
        .await?;

    finalize(
        &mirror.service,
        failed_attempt,
        crate::proto::SuspendSnapshotTerminalState::Failed,
    )
    .await?;
    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact_id, "full".to_string())],
        "a failed upload leaves the bytes on this node only; the row is still \
         the truth"
    );

    finalize(
        &mirror.service,
        ready_attempt,
        crate::proto::SuspendSnapshotTerminalState::Ready,
    )
    .await?;

    assert!(mirror.queue().await?.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// Lock footprint and interleaving
// ---------------------------------------------------------------------------

/// A steady-state publish must take no row lock on `nodes`.
///
/// `nodes` is the row `persist_pod_node_placement` holds through commit, and
/// whose contention under a slow drain query is what this queue exists to
/// relieve; it is the reason the freshness gate lives on the
/// `node_status_snapshots` satellite and the reason
/// `node_local_backup_pending` carries no `node_id` foreign key.
///
/// **Steady state, not every publish.** A node's first census inserts its
/// `node_status_snapshots` row, and that table does have a `nodes` foreign
/// key, so that one insert takes `FOR KEY SHARE` on the node row. Every census
/// after it takes the `ON CONFLICT DO UPDATE` arm, where the referential check
/// is skipped because the key is unchanged. The fixture therefore publishes
/// once before holding the lock, which is also the state a node being drained
/// is in — it has been publishing for hours.
///
/// Asserted by contention rather than by counting `pg_locks` rows, because a
/// `FOR UPDATE` row lock lives in the tuple header and never appears there — a
/// counting test would read zero whatever the statement locked. Both
/// directions are covered: the control proves the held lock really does block
/// a `nodes` writer, so a publish that completes anyway is evidence rather
/// than a fixture that locked nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_publish_transaction_takes_no_nodes_row_lock() -> TestResult {
    let Some(mirror) = mirror_with_max_connections(6).await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    // Steady state: every satellite row this publication touches already
    // exists, so the second publish exercises updates rather than the inserts
    // of a node's very first census.
    let census = || {
        Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(sandbox_id, artifact_id, ArtifactKind::Full)],
        )
    };
    mirror.publish(census()).await?;

    let mut holder = mirror.pool().begin().await?;
    let held: Vec<NodeId> =
        sqlx::query_scalar(sql!("SELECT id FROM nodes WHERE id = $1 FOR UPDATE"))
            .bind(mirror.node_id)
            .fetch_all(&mut *holder)
            .await?;
    assert_eq!(held, vec![mirror.node_id]);

    // Control: a genuine `nodes` row writer must not get through. Bounded by
    // `lock_timeout` rather than by a dropped future, so the arm that proves
    // the fixture works is as deterministic as the arm it exists to support.
    // `SET LOCAL` because a plain `SET` outlives the statement on a *pooled*
    // connection, and the publish below would then inherit a one-second lock
    // budget from this control — turning the real assertion into an accident
    // of which connection the pool handed it.
    let mut writer = mirror.pool().begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '1s'"))
        .execute(&mut *writer)
        .await?;
    let control = sqlx::query(sql!(
        "UPDATE nodes SET last_heartbeat = NOW() WHERE id = $1"
    ))
    .bind(mirror.node_id)
    .execute(&mut *writer)
    .await;
    assert_eq!(
        control
            .as_ref()
            .err()
            .and_then(|error| error.as_database_error())
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03"),
        "the fixture's FOR UPDATE must actually block a nodes writer, or the \
         assertion below proves nothing; got {control:?}"
    );
    writer.rollback().await?;

    let published = tokio::time::timeout(Duration::from_secs(20), mirror.publish(census())).await;
    holder.rollback().await?;
    published.map_err(|_| {
        "the publish transaction blocked on the node row: something in it now \
         takes a nodes lock"
    })??;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact_id, "full".to_string())]
    );
    Ok(())
}

/// A finalize racing a census, driven as a real Postgres interleaving rather
/// than a sleep.
///
/// The node's census is a snapshot of a moment that has already passed by the
/// time it arrives, so a backup can finalize while a census that still calls
/// it pending is mid-transaction. The design's answer is that neither writer
/// is authoritative on its own: the finalize takes the fast path, the census
/// re-asserts what the node reported, and the *next* census — by which point
/// the node knows the backup finished — converges on the truth through the
/// durable-absent delete. This walks all three steps.
#[tokio::test(flavor = "current_thread")]
async fn a_finalize_racing_a_census_converges_within_one_census() -> TestResult {
    let Some(mirror) = mirror_with_max_connections(6).await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    let artifact_id = Uuid::now_v7();
    let snapshot_id = mirror
        .snapshot(
            sandbox_id,
            artifact_id,
            ArtifactKind::Full,
            "creating",
            Duration::from_secs(600),
        )
        .await?;
    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id)],
            vec![pending_backup(sandbox_id, artifact_id, ArtifactKind::Full)],
        ))
        .await?;

    // Park the census inside its transaction, at the mirror's insert.
    sqlx::query(
        "CREATE OR REPLACE FUNCTION park_backup_queue_insert_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             PERFORM pg_advisory_lock(6184, 4816);
             PERFORM pg_advisory_unlock(6184, 4816);
             RETURN NEW;
         END;
         $$",
    )
    .execute(mirror.pool())
    .await?;
    sqlx::query(
        "CREATE TRIGGER park_backup_queue_insert_for_test
         BEFORE INSERT ON node_local_backup_pending
         FOR EACH ROW
         EXECUTE FUNCTION park_backup_queue_insert_for_test()",
    )
    .execute(mirror.pool())
    .await?;

    let mut gate_conn = mirror.pool().acquire().await?;
    sqlx::query("SELECT pg_advisory_lock(6184, 4816)")
        .execute(&mut *gate_conn)
        .await?;

    // The node's snapshot predates the upload finishing, so it still reports
    // the artifact as pending.
    let census_pool = mirror.pool().clone();
    let census_node = mirror.node_id;
    let census = tokio::spawn(async move {
        ClusterApiService::new(census_pool)
            .publish_node_state(publish_request(
                census_node,
                Census::full(
                    vec![suspended(sandbox_id)],
                    vec![pending_backup(sandbox_id, artifact_id, ArtifactKind::Full)],
                ),
            ))
            .await
            .map(|_| ())
    });

    let mut parked = false;
    for _ in 0..200 {
        let waiting: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*)
               FROM pg_locks
              WHERE locktype = 'advisory'
                AND classid = 6184
                AND objid = 4816
                AND NOT granted"
        ))
        .fetch_one(mirror.pool())
        .await?;
        if waiting >= 1 {
            parked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(parked, "the census never reached the mirror's insert");

    // The upload finishes while that census is still open.
    finalize(
        &mirror.service,
        snapshot_id,
        crate::proto::SuspendSnapshotTerminalState::Ready,
    )
    .await?;

    sqlx::query("SELECT pg_advisory_unlock(6184, 4816)")
        .execute(&mut *gate_conn)
        .await?;
    census.await??;

    assert_eq!(
        mirror.queue().await?,
        vec![(sandbox_id, artifact_id, "full".to_string())],
        "the census re-asserts what the node reported; the fast path is an \
         optimization, not an authority that outranks the node's own account"
    );

    // One census later the node knows the backup finished and stops reporting
    // it, and the durable-absent delete converges.
    mirror.reports_nothing_pending(sandbox_id).await?;
    assert!(
        mirror.queue().await?.is_empty(),
        "the queue must match the node's truth within one census of the race"
    );
    Ok(())
}

/// An inventory the server refused to prune against carries no usable report,
/// even when the report itself is well-formed and names a sandbox the cluster
/// knows.
///
/// The unparseable identity here is in the *sandbox census*, not in the
/// pending list: it is what makes the server skip the prune, which is the
/// condition under test. `an_unknown_sandbox_is_skipped_rather_than_failing_the_publication`
/// is the other direction — a bad identity in the report itself.
#[tokio::test(flavor = "current_thread")]
async fn a_report_without_a_census_prune_never_reaches_the_queue() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    let sandbox_id = mirror.suspended_sandbox().await?;
    // Arm the gate first, so the assertion below distinguishes "cleared" from
    // "never set". Without this it holds whichever branch runs.
    mirror.publish(Census::full(Vec::new(), Vec::new())).await?;
    assert!(
        mirror.gate().await?.is_some(),
        "precondition: the gate must be armed for its later absence to mean \
         anything"
    );

    // Flagged complete, but one identity the server cannot parse: the prune is
    // skipped, so the report that came with it is not certifiable either.
    let mut unparseable = suspended(sandbox_id);
    unparseable.sandbox_id = "not-a-sandbox-uuid".to_string();
    mirror
        .publish(Census::full(
            vec![suspended(sandbox_id), unparseable],
            vec![pending_backup(
                sandbox_id,
                Uuid::now_v7(),
                ArtifactKind::Full,
            )],
        ))
        .await?;

    assert!(mirror.queue().await?.is_empty());
    assert_eq!(
        mirror.gate().await?,
        None,
        "the node claimed a complete inventory and shipped one the server \
         could not prune against, so the permission it had earned is \
         withdrawn rather than left standing"
    );
    Ok(())
}

/// Neither disjunct of the delete guard may fall back to reading a sandbox's
/// whole snapshot history per queue row. Asserted on the index the plan names
/// rather than on a buffer count, because counts move with the machine and the
/// statistics while the index is the property.
///
/// An earlier revision nested the disjunction inside one correlated `EXISTS`.
/// It selected exactly the same rows and cost 20,403 shared buffers against
/// 1,203, because no index covered an `OR` spanning two different key sets —
/// invisible to every test that checks only which rows come back, which is why
/// this one reads the plan.
///
/// It EXPLAINs [`DELETE_DURABLE_ABSENT`] itself, so the `USING` join and the
/// node filter around the shared predicate are pinned here too.
#[tokio::test(flavor = "current_thread")]
async fn the_delete_guard_resolves_through_indexes_not_a_history_scan() -> TestResult {
    let Some(mirror) = mirror().await? else {
        return Ok(());
    };
    seed_at_scale(mirror.pool(), mirror.node_id).await?;

    let plan = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(sql!(
        format!("EXPLAIN (COSTS OFF) {}", DELETE_DURABLE_ABSENT.as_str()).as_str()
    )))
    .bind(mirror.node_id)
    .bind(Vec::<Uuid>::new())
    .fetch_all(mirror.pool())
    .await?
    .into_iter()
    .map(|(line,)| line)
    .collect::<Vec<_>>()
    .join("\n");

    // Both disjuncts, counted rather than merely present. They are the same
    // fragment over different artifact expressions, so a single hit would be
    // satisfied by the own-bytes arm alone and the assertion would say nothing
    // about whether the departed-marker arm still had an index.
    let index_hits = plan.matches("idx_snapshots_ready_local_artifact").count();
    assert_eq!(
        index_hits, 2,
        "both artifact-identity probes must resolve through \
         idx_snapshots_ready_local_artifact; found {index_hits}, plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on snapshots"),
        "a sequential scan over snapshots means a probe lost its index; \
         plan was:\n{plan}"
    );
    Ok(())
}

/// Enough rows that the planner prefers an index, and enough snapshots per
/// sandbox that a lost index shows up as a history scan rather than as noise.
///
/// **The row count has a floor.** At roughly 40k snapshots (2,000 sandboxes
/// x 20) or fewer, the planner costs a sequential scan of `snapshots` below
/// the index probes and the plan assertion fails on a fixture that is
/// perfectly correct, so a smaller fixture would be testing the planner's
/// costing thresholds rather than whether the predicate is sargable. 2,000
/// sandboxes x 50 snapshots (100k) clears that floor with margin.
///
/// Every seeded id is `gen_random_uuid()` rather than `space_uuid_v7()`,
/// including the `snapshot_storage_locations.id` that would otherwise come
/// from its column default. Both `snapshots.id` and
/// `snapshot_storage_locations.id` default to the PL/pgSQL `space_uuid_v7()`,
/// which costs about 8x a `gen_random_uuid()` call, and at 100k rows per table
/// that dominates the whole test. Nothing here depends on the ids being
/// time-ordered: the plan is keyed on `(sandbox_id, local_artifact_id,
/// artifact_kind)` and the fixture never reads an id back.
///
/// The two 100k-row inserts run with `session_replication_role = replica`,
/// which turns off their foreign-key checks. Postgres checks those row by row,
/// two or three lookups per row, and they were most of the fixture's cost.
/// Every seeded reference is taken from a row that already exists, so there is
/// nothing for a check to catch, and neither table has a trigger of its own
/// that the setting would also skip. The sandboxes insert keeps its triggers.
async fn seed_at_scale(pool: &PgPool, node: NodeId) -> TestResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, lifecycle_changed_at,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin, network_policy_generation
         )
         SELECT gen_random_uuid(), $1, $2, 'suspended', 'unplaced', 'k8s', $3,
                NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
                gen_random_uuid(), 'full', 'suspend', 1
           FROM generate_series(1, 2000)"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node)
    .execute(pool)
    .await?;
    let mut bulk = pool.begin().await?;
    sqlx::query(sql!("SET LOCAL session_replication_role = replica"))
        .execute(&mut *bulk)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         )
         SELECT gen_random_uuid(), s.id, 'full', 'ready', $1, $2,
                NOW() - (g * INTERVAL '1 minute'), gen_random_uuid(), 'full', 'suspend'
           FROM sandboxes s CROSS JOIN generate_series(1, 50) g"
    ))
    .bind(node)
    .bind(SEED_ORG_ID)
    .execute(&mut *bulk)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (id, snapshot_id, storage_connection_string, org_id)
         SELECT gen_random_uuid(), id, 's3://space-test/plan-probe', $1 FROM snapshots"
    ))
    .bind(SEED_ORG_ID)
    .execute(&mut *bulk)
    .await?;
    bulk.commit().await?;
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending (
             node_id, sandbox_id, artifact_id, artifact_kind
         )
         SELECT $1, s.id, gen_random_uuid(), 'full' FROM sandboxes s LIMIT 200"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    // Only the four tables the plan reads. A database-wide ANALYZE also walks
    // every table the migrations create, none of which this statement touches.
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations, node_local_backup_pending"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
