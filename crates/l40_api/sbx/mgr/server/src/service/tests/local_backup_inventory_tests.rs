use super::service_tests::{insert_test_sandbox, node_ctl_request};
use super::*;
use pplx_space_common_core::host::linux::identities::users;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};

fn pin_artifact(
    service: &SandboxManagerService,
    sandbox_id: &str,
    artifact_id: LocalArtifactId,
    kind: &str,
    origin: &str,
    backup_completed_at: Option<&str>,
) {
    let db = service.db.lock().expect("mutex poisoned");
    db.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at
         ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, '0', ?6)",
        crate::db::fixture::params![
            artifact_id.to_string(),
            sandbox_id,
            kind,
            origin,
            format!("/space/artifacts/{artifact_id}"),
            backup_completed_at
        ],
    )
    .unwrap();
    db.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = ?2,
                current_local_artifact_origin = ?3
          WHERE id = ?4",
        crate::db::fixture::params![artifact_id.to_string(), kind, origin, sandbox_id],
    )
    .unwrap();
}

async fn list_pending(service: &SandboxManagerService) -> Vec<crate::proto::PendingLocalBackup> {
    <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::
        list_pending_local_backups(
            service,
            node_ctl_request(crate::proto::ListPendingLocalBackupsRequest {}),
        )
        .await
        .expect("node-ctl may read the pending-backup inventory")
        .into_inner()
        .pending
}

/// Both artifact kinds cross the wire as their own enum value, and never
/// `ARTIFACT_KIND_UNSPECIFIED`.
///
/// The zero value is the one an `i32` field takes by default, so a mapping
/// that silently failed would produce it rather than an error. The queue
/// this feeds constrains the column to exactly those two values
/// (`CHECK (artifact_kind IN ('disk', 'full'))` in
/// `db/postgres/20261002070625_baseline.up.sql`), so an
/// unspecified kind reaching it would be rejected by the database. No
/// Rust reader exists yet — this pins the producer against the stored
/// contract, not against a live consumer.
#[tokio::test]
async fn pending_local_backups_carry_a_concrete_artifact_kind_for_both_origins() {
    let conn = crate::db::test_conn();
    let suspended = SandboxId::new().to_string();
    let stopped = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &suspended, SandboxState::Suspended);
    insert_test_sandbox(&conn, &stopped, SandboxState::Stopped);
    let service = SandboxManagerService::for_test(conn);
    let suspend_artifact = LocalArtifactId::new();
    let stop_artifact = LocalArtifactId::new();
    pin_artifact(
        &service,
        &suspended,
        suspend_artifact,
        "full",
        "suspend",
        None,
    );
    pin_artifact(&service, &stopped, stop_artifact, "disk", "stop", None);

    let pending = list_pending(&service).await;

    let by_sandbox = pending
        .iter()
        .map(|entry| (entry.sandbox_id.as_str(), entry))
        .collect::<std::collections::BTreeMap<_, _>>();
    let suspend_entry = by_sandbox
        .get(suspended.as_str())
        .expect("a suspended sandbox's unbacked artifact is pending");
    assert_eq!(
        suspend_entry.local_artifact_id,
        suspend_artifact.to_string()
    );
    assert_eq!(
        suspend_entry.artifact_kind,
        crate::proto::ArtifactKind::Full as i32
    );
    let stop_entry = by_sandbox
        .get(stopped.as_str())
        .expect("a stopped sandbox's unbacked disk artifact is pending");
    assert_eq!(stop_entry.local_artifact_id, stop_artifact.to_string());
    assert_eq!(
        stop_entry.artifact_kind,
        crate::proto::ArtifactKind::Disk as i32
    );
    for entry in &pending {
        assert_ne!(
            entry.artifact_kind,
            crate::proto::ArtifactKind::Unspecified as i32,
            "node_local_backup_pending's CHECK admits only 'disk' and 'full'"
        );
    }
}

/// A sandbox row this binary cannot fully materialize still publishes its
/// pending artifact. Whether a consumer *trusts* a census carrying
/// degraded rows is its decision, not the node's; the node's job is to
/// report the bytes on its disk either way, and a degraded row is exactly
/// the one whose loss nobody would otherwise notice.
///
/// The two mechanisms are asserted separately: that the fixture really is
/// degraded (via `State`, which is what sets the flag), and that the
/// pending list is unaffected. Asserting only the second would pass just as
/// well if the corruption had failed to degrade anything.
#[tokio::test]
async fn a_degraded_sandbox_still_reports_its_pending_artifact() {
    let conn = crate::db::test_conn();
    let degraded = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &degraded, SandboxState::Suspended);
    let service = SandboxManagerService::for_test(conn);
    let degraded_artifact = LocalArtifactId::new();
    pin_artifact(
        &service,
        &degraded,
        degraded_artifact,
        "full",
        "suspend",
        None,
    );
    {
        let db = service.db.lock().expect("mutex poisoned");
        crate::db::seed_historical_lifecycle_rows(&db, |db| {
            db.execute(
                "UPDATE sandboxes SET state = 'state_from_newer_worker' WHERE id = ?1",
                [&degraded],
            )
            .unwrap();
        });
    }

    let state =
        <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::state(
            &service,
            node_ctl_request(crate::proto::StateRequest {
                sandbox_ids: vec![degraded.clone()],
            }),
        )
        .await
        .expect("an unreadable row must not reject the publication")
        .into_inner();
    let published = state
        .sandboxes
        .iter()
        .find(|sandbox| sandbox.sandbox_id == degraded)
        .expect("a degraded row still publishes its lifecycle");
    assert!(
        published.state_degraded,
        "fixture must actually produce a degraded row"
    );

    let pending = list_pending(&service).await;

    assert_eq!(pending.len(), 1, "got {pending:?}");
    assert_eq!(pending[0].sandbox_id, degraded);
    assert_eq!(pending[0].local_artifact_id, degraded_artifact.to_string());
}

/// The inventory is node-ctl's to read, not the general observer set's.
///
/// `space-nodeapi` is the principal that matters: it is in
/// `ALLOW_SANDBOX_OBSERVER`, so it can already read sandbox state, and
/// `ALLOW_LOCAL_BACKUP_INVENTORY`'s comment says the narrowing away from
/// that set is deliberate. Asserting a principal that no allowlist admits
/// would pass whichever set this RPC had been given.
#[tokio::test]
async fn pending_local_backups_are_not_readable_by_the_general_observer_set() {
    let conn = crate::db::test_conn();
    let service = SandboxManagerService::for_test(conn);
    let mut request = Request::new(crate::proto::ListPendingLocalBackupsRequest {});
    request.extensions_mut().insert(PeerIdentity::new(
        users::SPACE_NODEAPI.uid,
        users::SPACE_NODEAPI.primary_group.gid,
        None,
    ));

    let error =
        <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::
            list_pending_local_backups(&service, request)
            .await
            .expect_err("only node-ctl and root read the pending-backup inventory");

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}
