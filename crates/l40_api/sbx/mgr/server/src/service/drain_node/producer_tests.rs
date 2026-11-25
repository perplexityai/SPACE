//! Node-local artifact producers on a drained node: a snapshot of a sandbox
//! without a VM is refused while the node is sealed, and one of a running or
//! paused sandbox is admitted and keeps the drain pending until it ends. A
//! rollback that doesn't restart is a producer too: admitted for a running
//! sandbox, it holds its token until it returns, and the stopped sandbox it
//! leaves keeps the drain pending, since a v2 drain doesn't move one. So is
//! an idle auto-stop, which holds its token until its state write commits.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_node_mgr_client::proto::{CreateBtrfsSnapshotRequest, CreateBtrfsSnapshotResponse};
use pplx_space_sandbox_model::SandboxId;
use tokio::sync::Semaphore;

use super::planner::{DrainScans, PlannedAction, SNAPSHOT_REASON, STOPPED_REASON};
use crate::activation_seal::{ACTIVATIONS_SEALED_MARKER, ActivationSealStatus};
use crate::node_manager_transport::fake::{FakeNodeManager, ServedNodeManager};
use crate::proto::{CreateSnapshotRequest, DrainReport, DrainWait as WireWait, SnapshotType};
use crate::service::activation_support::internal_request;
use crate::service::*;

/// A sandbox in `state` on a new connection. An encrypted volume skips the
/// snapshot's plaintext-COW probe of the host cache directory.
fn sandbox_in(state: SandboxState) -> (crate::db::Connection, String) {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, state);
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [&id],
    )
    .unwrap();
    (conn, id)
}

/// A node-manager whose disk snapshots each wait for a permit of `gate`.
fn gated_node_manager(gate: &Arc<Semaphore>) -> ServedNodeManager {
    let gate = Arc::clone(gate);
    FakeNodeManager::default()
        .unary_async(
            "CreateBtrfsSnapshot",
            move |_: CreateBtrfsSnapshotRequest| {
                let gate = Arc::clone(&gate);
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(CreateBtrfsSnapshotResponse {})
                }
            },
        )
        .serve()
}

fn service_on(
    conn: crate::db::Connection,
    node_manager: &ServedNodeManager,
) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        conn,
        crate::service::SandboxManagerServiceOptions {
            node_manager: node_manager.connector(),
            ..crate::service::SandboxManagerServiceOptions::for_test()
        },
    )
}

fn snapshot_request(sandbox_id: &str, kind: SnapshotType) -> Request<CreateSnapshotRequest> {
    internal_request(CreateSnapshotRequest {
        sandbox_id: sandbox_id.to_owned(),
        r#type: kind as i32,
        ..Default::default()
    })
}

/// A drain call that suspends running sandboxes too.
const EVACUATE: bool = true;
/// A drain call that keeps running sandboxes running.
const HOLD: bool = false;

/// Whether the drain `report` describes is finished: nothing pending or
/// blocked.
fn finished(report: &DrainReport) -> bool {
    report.pending.is_empty() && report.blocked.is_empty()
}

/// Each pending entry of `report` as `(sandbox, kind, running)`.
fn kinds(report: &DrainReport) -> Vec<(String, WireWait, bool)> {
    report
        .pending
        .iter()
        .map(|entry| (entry.sandbox_id.clone(), entry.wait(), entry.running))
        .collect()
}

fn artifact_count(service: &SandboxManagerService, sandbox_id: &str) -> i64 {
    service
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE sandbox_id = ?1",
            [sandbox_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn assert_sealed_refusal(refused: &Status) {
    assert_eq!(
        refused.code(),
        tonic::Code::FailedPrecondition,
        "{refused:?}"
    );
    assert!(
        refused.message().starts_with(ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
}

/// Wait until `sandbox_id` has a producer in flight.
async fn producer_in_flight(service: &SandboxManagerService, sandbox_id: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !service
            .activation_seal
            .status()
            .producers_in_flight
            .iter()
            .any(|id| id == sandbox_id)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the snapshot is admitted");
}

/// The entry a producer in flight adds to a drain's report, for a sandbox
/// whose VM is `running` or not: work the drain doesn't drive.
fn snapshot_entry(sandbox_id: &str, running: bool) -> (String, WireWait, bool) {
    (sandbox_id.into(), WireWait::External, running)
}

/// On a sealed node, a disk or full snapshot of a stopped or suspended
/// sandbox, which has no VM, is refused with the sealed refusal: node-manager
/// isn't asked for a snapshot, no artifact is published and no producer is
/// left in flight.
#[tokio::test]
async fn a_sealed_node_refuses_a_snapshot_of_a_sandbox_without_a_vm() {
    for state in [SandboxState::Stopped, SandboxState::Suspended] {
        for kind in [SnapshotType::Disk, SnapshotType::Full] {
            let node_manager = gated_node_manager(&Arc::new(Semaphore::new(1)));
            let (conn, id) = sandbox_in(state);
            let service = service_on(conn, &node_manager);
            service.begin_drain(7, HOLD).unwrap();
            let refused = service
                .create_snapshot(snapshot_request(&id, kind))
                .await
                .unwrap_err();
            assert_sealed_refusal(&refused);
            assert_eq!(
                node_manager.calls(),
                Vec::<String>::new(),
                "{state} {kind:?}"
            );
            assert_eq!(artifact_count(&service, &id), 0, "{state} {kind:?}");
            assert_eq!(
                service.lookup_sandbox(&id).unwrap().state,
                SandboxStatus::from(state)
            );
            service.activation_seal.assert_quiesced();
        }
    }
}

/// A node never sealed, or one whose seal row couldn't be read, takes a
/// stopped sandbox's disk snapshot as before: node-manager snapshots it and
/// the artifact is published.
#[tokio::test]
async fn an_unsealed_or_unknown_seal_admits_a_snapshot_as_before() {
    for unknown in [false, true] {
        let node_manager = gated_node_manager(&Arc::new(Semaphore::new(1)));
        let (conn, id) = sandbox_in(SandboxState::Stopped);
        if unknown {
            conn.execute_batch("ALTER TABLE activation_seal RENAME TO activation_seal_hidden")
                .unwrap();
        }
        let service = service_on(conn, &node_manager);
        assert_eq!(service.activation_seal.read_error().is_some(), unknown);
        let snapshot = service
            .create_snapshot(snapshot_request(&id, SnapshotType::Disk))
            .await
            .unwrap()
            .into_inner();
        assert!(snapshot.published_artifact.is_some(), "{snapshot:?}");
        assert_eq!(node_manager.calls(), ["CreateBtrfsSnapshot"]);
        assert_eq!(artifact_count(&service, &id), 1);
        service.activation_seal.assert_quiesced();
    }
}

/// On a sealed node, a disk snapshot of a running or paused sandbox is
/// admitted. While it is in flight the drain reports the sandbox pending,
/// waiting on work the drain doesn't drive, and isn't finished; once the
/// snapshot is published, the drain uploads it before it is finished.
#[tokio::test]
async fn a_snapshot_in_flight_on_a_sealed_node_keeps_the_drain_pending() {
    for state in [SandboxState::Started, SandboxState::Paused] {
        let gate = Arc::new(Semaphore::new(0));
        let node_manager = gated_node_manager(&gate);
        let (conn, id) = sandbox_in(state);
        let service = service_on(conn, &node_manager);
        // Holding, so the drain suspends no started VM; a paused VM's
        // suspend waits for the wake lock until the sandbox has moved on.
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        service.begin_drain(7, HOLD).unwrap();
        let snapshot = tokio::spawn({
            let service = service.clone();
            let id = id.clone();
            async move {
                service
                    .create_snapshot(snapshot_request(&id, SnapshotType::Disk))
                    .await
            }
        });
        producer_in_flight(&service, &id).await;

        let first = service.activation_seal.status();
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert!(!finished(&report), "{state}: {report:?}");
        let pending: Vec<_> = kinds(&report)
            .into_iter()
            .filter(|(_, wait, _)| *wait == WireWait::External)
            .collect();
        assert_eq!(
            pending,
            [snapshot_entry(&id, state == SandboxState::Started)],
            "{state}: {report:?}"
        );

        gate.add_permits(1);
        let published = snapshot
            .await
            .unwrap()
            .unwrap()
            .into_inner()
            .published_artifact
            .expect("the snapshot is published");
        assert_eq!(
            service.activation_seal.status().producers_in_flight,
            Vec::<String>::new()
        );

        // The sandbox moves on, suspended with a durable snapshot pinned, so
        // only the new artifact is left.
        let pinned = pplx_space_snapshot_model::LocalArtifactId::new();
        {
            let db = service.db.lock().unwrap();
            db.execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     local_path, created_at, backup_completed_at, state
                 ) VALUES (?1, ?2, 'full', 'suspend', '/tmp/a', '0', '1', 'ready')",
                crate::db::fixture::params![pinned.to_string(), id],
            )
            .unwrap();
            db.execute(
                "UPDATE sandboxes SET state = 'suspended', current_local_artifact_id = ?2,
                     current_local_artifact_kind = 'full',
                     current_local_artifact_origin = 'suspend'
                 WHERE id = ?1",
                crate::db::fixture::params![id, pinned.to_string()],
            )
            .unwrap();
        }
        drop(wake);
        service.drain_actions.wait_idle().await;
        let first = service.activation_seal.status();
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert!(!finished(&report), "{state}: {report:?}");
        assert_eq!(
            report
                .pending
                .iter()
                .map(|entry| (entry.sandbox_id.as_str(), entry.wait()))
                .collect::<Vec<_>>(),
            [(id.as_str(), WireWait::Moving)],
            "{state}: {report:?}"
        );
        service.drain_actions.wait_idle().await;

        service
            .local_artifacts()
            .mark_backup_completed(published.local_artifact_id.parse().unwrap())
            .unwrap();
        let first = service.activation_seal.status();
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert!(finished(&report), "{state}: {report:?}");
        service.drain_actions.wait_idle().await;
        service.activation_seal.assert_quiesced();
    }
}

/// Pin a durable full snapshot to suspended `sandbox_id`, so a drain finds
/// it evacuated but for anything else it holds.
fn pin_durable_snapshot(service: &SandboxManagerService, sandbox_id: &str) {
    let pinned = pplx_space_snapshot_model::LocalArtifactId::new();
    let db = service.db.lock().unwrap();
    db.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             local_path, created_at, backup_completed_at, state
         ) VALUES (?1, ?2, 'full', 'suspend', '/tmp/a', '0', '1', 'ready')",
        crate::db::fixture::params![pinned.to_string(), sandbox_id],
    )
    .unwrap();
    db.execute(
        "UPDATE sandboxes SET current_local_artifact_id = ?2,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend'
         WHERE id = ?1",
        crate::db::fixture::params![sandbox_id, pinned.to_string()],
    )
    .unwrap();
}

/// The write gate's invariant, through a drain's pass: a producer admitted
/// before the seal holds the sandbox's artifact lock until its artifact is
/// registered, so a pass that would find the sandbox evacuated checks it
/// again instead, even with a first read that missed the producer; once its
/// write commits, the new artifact is pending upload, and the drain is
/// finished only once that artifact is durable.
#[tokio::test]
async fn a_producer_admitted_before_the_seal_keeps_its_row_pending_until_its_artifact_is_durable() {
    let gate = Arc::new(Semaphore::new(0));
    let node_manager = gated_node_manager(&gate);
    let (conn, id) = sandbox_in(SandboxState::Suspended);
    let service = service_on(conn, &node_manager);
    pin_durable_snapshot(&service, &id);
    // A suspended sandbox's disk snapshot, which a node known sealed would
    // refuse, admitted before the seal.
    let snapshot = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move {
            service
                .create_snapshot(snapshot_request(&id, SnapshotType::Disk))
                .await
        }
    });
    producer_in_flight(&service, &id).await;
    let first = service.begin_drain(7, HOLD).unwrap();
    let missed = ActivationSealStatus {
        producers_in_flight: Vec::new(),
        ..first
    };

    let report = service.finish_drain(7, &missed, DrainScans::default());
    assert!(report.evacuated.is_empty(), "{report:?}");
    assert_eq!(kinds(&report), [(id.clone(), WireWait::External, false)]);
    assert!(
        report.pending[0]
            .reason
            .contains("another writer holds its artifacts"),
        "{report:?}"
    );

    gate.add_permits(1);
    let published = snapshot
        .await
        .unwrap()
        .unwrap()
        .into_inner()
        .published_artifact
        .expect("the snapshot is published");
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert_eq!(kinds(&report), [(id.clone(), WireWait::Moving, false)]);
    service.drain_actions.wait_idle().await;

    service
        .local_artifacts()
        .mark_backup_completed(published.local_artifact_id.parse().unwrap())
        .unwrap();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(finished(&report), "{report:?}");
    assert_eq!(
        report
            .evacuated
            .iter()
            .map(|entry| entry.sandbox_id.as_str())
            .collect::<Vec<_>>(),
        [id.as_str()]
    );
    service.drain_actions.wait_idle().await;
    service.activation_seal.assert_quiesced();
}

/// A snapshot that waits for the artifact lock a drain's suspend holds reads
/// the row again once it has the lock: after the suspend it finds no VM and
/// is refused, writing nothing. Had the suspend failed, leaving the VM, it
/// is admitted.
#[tokio::test]
async fn a_snapshot_queued_behind_the_drains_suspend_is_refused_once_suspended() {
    for suspended in [true, false] {
        let gate = Arc::new(Semaphore::new(1));
        let node_manager = gated_node_manager(&gate);
        let (conn, id) = sandbox_in(SandboxState::Started);
        let service = service_on(conn, &node_manager);
        service.begin_drain(7, EVACUATE).unwrap();
        // The locks a drain suspend holds while it suspends the VM.
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        let capture = service
            .write_gate
            .lock_for_capture(
                &id,
                crate::service::write_gate::DrainCompatible::DrainSuspend,
            )
            .await;
        let unwaited = service.write_gate.artifact_lock_handles(&id);
        let snapshot = tokio::spawn({
            let service = service.clone();
            let id = id.clone();
            async move {
                service
                    .create_snapshot(snapshot_request(&id, SnapshotType::Disk))
                    .await
            }
        });
        // The snapshot read the row once it waits for the artifact lock,
        // holding the lock's handle.
        tokio::time::timeout(Duration::from_secs(2), async {
            while service.write_gate.artifact_lock_handles(&id) == unwaited {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the snapshot waits for the artifact lock");
        if suspended {
            crate::db::test_fixtures::state_write_fixtures::write_status(
                &service.db.lock().unwrap(),
                &id,
                SandboxState::Suspended,
            );
        }
        drop(capture);
        drop(wake);
        let answered = snapshot.await.unwrap();
        if suspended {
            assert_sealed_refusal(&answered.unwrap_err());
            assert_eq!(node_manager.calls(), Vec::<String>::new());
            assert_eq!(artifact_count(&service, &id), 0);
        } else {
            answered.unwrap();
            assert_eq!(node_manager.calls(), ["CreateBtrfsSnapshot"]);
            assert_eq!(artifact_count(&service, &id), 1);
        }
        service.cancel_drain(7).unwrap();
        service.drain_actions.wait_idle().await;
        service.activation_seal.assert_quiesced();
    }
}

/// A producer admitted under a drain that a cancel ends stays in flight until
/// it ends, and then leaves no entry: a later drain doesn't report it.
#[tokio::test]
async fn a_producer_ending_after_a_cancel_clears_its_entry() {
    let gate = Arc::new(Semaphore::new(0));
    let node_manager = gated_node_manager(&gate);
    let (conn, id) = sandbox_in(SandboxState::Started);
    let service = service_on(conn, &node_manager);
    service.begin_drain(7, HOLD).unwrap();
    let snapshot = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move {
            service
                .create_snapshot(snapshot_request(&id, SnapshotType::Disk))
                .await
        }
    });
    producer_in_flight(&service, &id).await;
    service.cancel_drain(7).unwrap();
    assert_eq!(
        service.activation_seal.status().producers_in_flight,
        std::slice::from_ref(&id)
    );

    gate.add_permits(1);
    snapshot.await.unwrap().unwrap();
    service.activation_seal.assert_quiesced();
    let first = service.begin_drain(8, HOLD).unwrap();
    let report = service.finish_drain(8, &first, DrainScans::default());
    assert!(
        report
            .pending
            .iter()
            .all(|entry| entry.wait() != WireWait::External),
        "{report:?}"
    );
    service.cancel_drain(8).unwrap();
}

/// The backup RPC's publication of a stopped sandbox's current instance is a
/// producer the seal refuses, before node-manager is asked for the disk
/// snapshot; the drain's own publication of an unpinned suspended sandbox's
/// snapshot is the drain's work, which the producer check leaves alone.
#[tokio::test]
async fn the_backup_rpc_publication_is_refused_on_a_sealed_node_but_not_the_drains() {
    // A backup needs the cluster-api wiring its upload registers with.
    if !crate::service::tests::enter_route_test_process(&[
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(std::ffi::OsStr::new("http://127.0.0.1:9")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("drain-producer-test")),
        ),
    ]) {
        return;
    }
    let node_manager = gated_node_manager(&Arc::new(Semaphore::new(1)));
    let (conn, id) = sandbox_in(SandboxState::Stopped);
    let service = service_on(conn, &node_manager);
    service.begin_drain(7, HOLD).unwrap();
    let refused = service
        .enqueue_artifact_backup(internal_request(
            crate::proto::EnqueueArtifactBackupRequest {
                sandbox_id: id.clone(),
                expected_artifact_kind: crate::proto::ArtifactKind::Disk as i32,
                source: Some(
                    crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(
                        crate::proto::CurrentInstanceSource::default(),
                    ),
                ),
            },
        ))
        .await
        .unwrap_err();
    assert_sealed_refusal(&refused);
    assert_eq!(node_manager.calls(), Vec::<String>::new());
    assert_eq!(artifact_count(&service, &id), 0);

    // The fixture has no suspend snapshot on disk, so the drain's
    // publication stops past the producer check, at its local-file check.
    let suspended = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &service.db.lock().unwrap(),
        &suspended,
        SandboxState::Suspended,
    );
    let published = service
        .run_drain_action(
            7,
            PlannedAction::PublishSnapshot {
                sandbox_id: suspended,
            },
        )
        .await
        .unwrap_err();
    assert!(
        !published.message().contains(ACTIVATIONS_SEALED_MARKER),
        "{published:?}"
    );
    service.activation_seal.assert_quiesced();
}

/// A sandbox root owns, so a rollback needs no route identity, in `state`
/// on a new connection.
fn root_owned_in(state: SandboxState) -> (crate::db::Connection, String) {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, state);
    conn.execute(
        "UPDATE sandboxes SET user = ?1 WHERE id = ?2",
        crate::db::fixture::params![pplx_space_accounts::ROOT_SANDBOX_OWNER.as_str(), id],
    )
    .unwrap();
    (conn, id)
}

fn rollback_request(sandbox_id: &str) -> Request<crate::proto::RollbackRequest> {
    internal_request(crate::proto::RollbackRequest {
        sandbox_id: sandbox_id.to_owned(),
        snapshot_name: "d-snap".to_owned(),
        auto_restart: false,
        ..Default::default()
    })
}

/// On a sealed node, a rollback that doesn't restart a running sandbox is
/// admitted, as before: the fixture has no snapshot files, so it stops at its
/// input checks, with the sandbox still running and no producer left in
/// flight. A rollback that goes through leaves the sandbox stopped, which a
/// v2 drain doesn't move: the drain reports it pending, waiting on what the
/// drain doesn't drive, starts nothing for it and is never finished.
#[tokio::test]
async fn a_rollback_of_a_running_sandbox_leaves_the_drain_pending() {
    let node_manager = gated_node_manager(&Arc::new(Semaphore::new(1)));
    let (conn, id) = root_owned_in(SandboxState::Started);
    let service = service_on(conn, &node_manager);
    service.begin_drain(7, HOLD).unwrap();
    let admitted = service.rollback(rollback_request(&id)).await.unwrap_err();
    assert!(
        admitted.message().contains("rootfs.ext4")
            && !admitted.message().contains(ACTIVATIONS_SEALED_MARKER),
        "{admitted:?}"
    );
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Started)
    );
    service.activation_seal.assert_quiesced();

    // What an applied rollback leaves: the sandbox stopped, no disk pinned.
    crate::db::test_fixtures::state_write_fixtures::write_status(
        &service.db.lock().unwrap(),
        &id,
        SandboxState::Stopped,
    );
    for _ in 0..2 {
        let first = service.activation_seal.status();
        let report = service.finish_drain(7, &first, DrainScans::default());
        assert!(!finished(&report), "{report:?}");
        assert_eq!(kinds(&report), [(id.clone(), WireWait::External, false)]);
        assert_eq!(report.pending[0].reason, STOPPED_REASON);
        assert!(report.evacuated.is_empty(), "{report:?}");
        service.drain_actions.wait_idle().await;
    }
    assert_eq!(node_manager.calls(), Vec::<String>::new());
    service.activation_seal.assert_quiesced();
}

/// A rollback that doesn't restart holds its producer token for the whole
/// rollback: admitted on a node not yet sealed, it is still in flight when a
/// drain seals the node, and the drain reports the sandbox pending, waiting
/// on what the drain doesn't drive, until the rollback returns. A rollback
/// asked once the node is sealed is refused for a sandbox without a VM.
#[tokio::test]
async fn a_rollback_holds_its_producer_token_until_it_returns() {
    let node_manager = gated_node_manager(&Arc::new(Semaphore::new(1)));
    let (conn, id) = root_owned_in(SandboxState::Started);
    let service = service_on(conn, &node_manager);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    *service
        .auto_lifecycle
        .rollback_admission_barrier
        .lock()
        .unwrap() = Some(Arc::clone(&barrier));
    let rollback = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move { service.rollback(rollback_request(&id)).await }
    });
    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("the rollback is admitted");
    *service
        .auto_lifecycle
        .rollback_admission_barrier
        .lock()
        .unwrap() = None;
    assert_eq!(
        service.activation_seal.status().producers_in_flight,
        std::slice::from_ref(&id)
    );

    // The seal lands mid-rollback: the drain reports the rollback pending.
    service.begin_drain(7, HOLD).unwrap();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert!(
        report.pending.iter().any(|entry| entry.sandbox_id == id
            && entry.wait() == WireWait::External
            && entry.reason == SNAPSHOT_REASON),
        "{report:?}"
    );

    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("the rollback goes on");
    let ended = rollback.await.unwrap().unwrap_err();
    assert!(ended.message().contains("rootfs.ext4"), "{ended:?}");
    service.activation_seal.assert_quiesced();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(
        !report
            .pending
            .iter()
            .any(|entry| entry.reason == SNAPSHOT_REASON),
        "the rollback ended: {report:?}"
    );

    // Sealed now, a rollback of a sandbox without a VM is refused.
    crate::db::test_fixtures::state_write_fixtures::write_status(
        &service.db.lock().unwrap(),
        &id,
        SandboxState::Stopped,
    );
    assert_sealed_refusal(&service.rollback(rollback_request(&id)).await.unwrap_err());
    service.activation_seal.assert_quiesced();
    service.drain_actions.wait_idle().await;
}

/// An unknown seal admits a producer, as a node never sealed does, so a
/// drain can't have trusted a report from before the seal was read: the next
/// drain call fails while the seal row still can't be read, sealing
/// nothing, and once the row reads it seals and reports the producer
/// pending, then the artifact it published, so the drain isn't finished
/// until that artifact is durable. A suspended sandbox's disk snapshot stands
/// for the producer: it has no VM, so a node known sealed would refuse it.
#[tokio::test]
async fn an_unreadable_seal_admits_a_producer_the_next_call_reports() {
    let gate = Arc::new(Semaphore::new(0));
    let node_manager = FakeNodeManager::default()
        .unary_async("CreateBtrfsSnapshot", {
            let gate = Arc::clone(&gate);
            move |_: CreateBtrfsSnapshotRequest| {
                let gate = Arc::clone(&gate);
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(CreateBtrfsSnapshotResponse {})
                }
            }
        })
        .unary(
            "ListOrphanCandidates",
            |_: pplx_space_node_mgr_client::proto::ListOrphanCandidatesRequest| {
                Ok(pplx_space_node_mgr_client::proto::ListOrphanCandidatesResponse::default())
            },
        )
        .serve();
    let (conn, id) = sandbox_in(SandboxState::Suspended);
    conn.execute_batch("ALTER TABLE activation_seal RENAME TO activation_seal_hidden")
        .unwrap();
    let service = service_on(conn, &node_manager);
    assert!(service.activation_seal.read_error().is_some());
    let snapshot = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move {
            service
                .create_snapshot(snapshot_request(&id, SnapshotType::Disk))
                .await
        }
    });
    producer_in_flight(&service, &id).await;

    let drain = |generation| {
        internal_request(crate::proto::DrainNodeRequest {
            generation,
            op: Some(crate::proto::drain_node_request::Op::Drain(
                crate::proto::drain_node_request::Drain { evacuate: false },
            )),
        })
    };
    let failed = service.drain_node(drain(7)).await.unwrap_err();
    assert_eq!(failed.code(), tonic::Code::Internal, "{failed:?}");
    assert!(
        failed.message().contains("activation seal unreadable"),
        "{failed:?}"
    );
    assert_eq!(service.activation_seal.status().sealed_generation, None);

    service
        .db
        .lock()
        .unwrap()
        .execute_batch("ALTER TABLE activation_seal_hidden RENAME TO activation_seal")
        .unwrap();
    let report =
        |response: Response<crate::proto::DrainNodeResponse>| match response.into_inner().result {
            Some(crate::proto::drain_node_response::Result::Drain(report)) => report,
            other => panic!("not a drain's answer: {other:?}"),
        };
    let sealed = report(service.drain_node(drain(7)).await.unwrap());
    assert_eq!(service.activation_seal.status().sealed_generation, Some(7));
    assert!(!finished(&sealed), "{sealed:?}");
    assert!(
        kinds(&sealed).contains(&snapshot_entry(&id, false)),
        "the producer is pending: {sealed:?}"
    );

    gate.add_permits(1);
    snapshot
        .await
        .unwrap()
        .unwrap()
        .into_inner()
        .published_artifact
        .expect("the admitted snapshot is published");
    service.activation_seal.assert_quiesced();
    let after = report(service.drain_node(drain(7)).await.unwrap());
    assert!(!finished(&after), "its artifact is still local: {after:?}");
    assert!(
        kinds(&after)
            .iter()
            .any(|(sandbox, wait, _)| *sandbox == id && *wait == WireWait::Moving),
        "{after:?}"
    );
    service.drain_actions.wait_idle().await;
}

/// The durable full snapshot a suspended sandbox pins.
const DURABLE_SNAPSHOT: &str = "01939e3e-9001-7000-8000-000000000001";

/// An idle auto-stop of a suspended sandbox holds its producer token until
/// its state write commits. It takes only the wake lock, so a drain can seal
/// the node between the stop's admission and its write, and read the row
/// still suspended on its durable snapshot: on its own, evacuated, and the
/// node finished. The stop in flight keeps the sandbox pending instead. Once
/// the write commits the token is released and the row is stopped, which a
/// v2 drain doesn't move, so the sandbox is still pending, now as stopped,
/// and nothing is evacuated.
#[tokio::test]
async fn an_idle_stop_holds_its_producer_token_until_its_write_commits() {
    let node_manager = gated_node_manager(&Arc::new(Semaphore::new(0)));
    let service = service_on(crate::db::test_conn(), &node_manager);
    let id = SandboxId::new().to_string();
    crate::service::reconcile::tests::insert_due_sandbox(
        &service,
        &id,
        SandboxState::Suspended,
        AutoLifecycleIdleAction::Stop,
        10,
    );
    {
        let db = service.db.lock().unwrap();
        db.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = ?1,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?2",
            crate::db::fixture::params![DURABLE_SNAPSHOT, id],
        )
        .unwrap();
        db.execute(
            "INSERT INTO local_artifacts (
                 local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                 snapshot_name, local_path, created_at, backup_completed_at
             ) VALUES (?1, ?2, 'full', 'suspend', NULL, '/tmp/a', '0', '1')",
            crate::db::fixture::params![DURABLE_SNAPSHOT, id],
        )
        .unwrap();
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    *service
        .auto_lifecycle
        .suspended_stop_barrier
        .lock()
        .unwrap() = Some(Arc::clone(&barrier));
    let stop = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move { service.run_due_auto_lifecycle(&id).await }
    });
    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("the idle stop is admitted and reaches its write");
    assert_eq!(
        service.activation_seal.status().producers_in_flight,
        std::slice::from_ref(&id)
    );

    // The seal lands between the stop's admission and its write: the drain
    // reports the stop pending, though the row reads evacuated.
    service.begin_drain(7, HOLD).unwrap();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert_eq!(kinds(&report), [snapshot_entry(&id, false)]);
    assert_eq!(report.pending[0].reason, SNAPSHOT_REASON);

    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("the idle stop writes");
    tokio::time::timeout(Duration::from_secs(5), stop)
        .await
        .expect("the idle stop finishes")
        .unwrap()
        .unwrap();
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxStatus::from(SandboxState::Stopped)
    );
    service.activation_seal.assert_quiesced();
    let first = service.activation_seal.status();
    let report = service.finish_drain(7, &first, DrainScans::default());
    assert!(!finished(&report), "{report:?}");
    assert_eq!(kinds(&report), [(id.clone(), WireWait::External, false)]);
    assert_eq!(report.pending[0].reason, STOPPED_REASON);
    assert!(report.evacuated.is_empty(), "{report:?}");
    service.drain_actions.wait_idle().await;
}
