use super::*;
use pplx_space_common_core::{
    sql,
    types::{LocalArtifactId, NodeId, SnapshotId},
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db_with_max_connections};
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use crate::lifecycle_writer_test_support::relabel_keeping_lifecycle_age;

async fn fixture(pool: &PgPool) -> TestResult<store::Candidate> {
    let id = SandboxId::new();
    let node = NodeId::new();
    let snapshot = SnapshotId::new();
    let artifact = LocalArtifactId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id,address,role,managed_by) VALUES ($1,'192.0.2.10','worker','k8s')"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id,k8s_node_name) VALUES ($1,'worker-example')"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id,user_id,org_id,managed_by,lifecycle,placement,active_node_id,target_node_id,
            network_policy_generation,command_ready_pending)
         VALUES ($1,$2::UUID,$3::UUID,'k8s','quarantined','on_node',$4,$4,1,TRUE)"
    )).bind(id).bind(SEED_USER_ID).bind(SEED_ORG_ID).bind(node).execute(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (sandbox_id,quarantine_id,entered_at,source,reason_code,next_action)
         VALUES ($1,$2,NOW()-INTERVAL '20 minutes','sbx_manager','suspend_snapshot_publication_failed','requires_operator_attention')"
    )).bind(id).bind(Uuid::now_v7()).execute(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id,generation,pod_name,pod_namespace,pod_uid,pod_ip,state,active,created_at)
         VALUES ($1,1,$2,'sandboxes','example-uid','192.0.2.11','running',TRUE,NOW()-INTERVAL '1 hour')"
    )).bind(id).bind(format!("sandbox-{id}")).execute(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id,sandbox_id,org_id,type,state,timestamp,local_artifact_id,artifact_kind,artifact_origin,size_bytes)
         VALUES ($1,$2,$3::UUID,'full','ready',NOW()-INTERVAL '21 minutes',$4,'full','suspend',1024)"
    )).bind(snapshot).bind(id).bind(SEED_ORG_ID).bind(artifact).execute(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id,storage_connection_string) VALUES ($1,'s3://example-bucket/snapshot')"
    )).bind(snapshot).execute(pool).await?;
    Ok(store::candidates(pool, None, Some(id), 2).await?.remove(0))
}

#[tokio::test]
async fn publication_quarantine_repair_preserves_identity_and_retires_stale_ownership() -> TestResult
{
    let db = pg_test_db_with_max_connections(3).await?;
    let c = fixture(&db.pool).await?;
    // The old orphan scan excludes this reachable shape entirely.
    assert!(
        super::super::list_candidates_after(&db.pool, None, c.sandbox_id, 100)
            .await?
            .is_empty()
    );
    assert!(store::recover(&db.pool, &c).await?);
    assert!(
        !store::recover(&db.pool, &c).await?,
        "idempotent after episode retirement"
    );
    let row: (String,String,Option<Uuid>,Option<LocalArtifactId>,bool) = sqlx::query_as(sql!(
        "SELECT lifecycle,placement,active_node_id,current_local_artifact_id,command_ready_pending FROM sandboxes WHERE id=$1"
    )).bind(c.sandbox_id).fetch_one(&db.pool).await?;
    assert_eq!(
        row,
        (
            "suspended".into(),
            "unplaced".into(),
            None,
            Some(c.artifact_id),
            false
        )
    );
    let active: bool = sqlx::query_scalar(sql!(
        "SELECT active OR pod_ip IS NOT NULL FROM sandbox_k8s_executions WHERE id=$1"
    ))
    .bind(c.execution_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(!active);
    let archived: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_quarantine_history WHERE sandbox_id=$1 AND quarantine_id=$2 AND action='recovered_to_suspended'"
    )).bind(c.sandbox_id).bind(c.quarantine_id).fetch_one(&db.pool).await?;
    assert_eq!(archived, 1);
    Ok(())
}

#[tokio::test]
async fn publication_quarantine_repair_rechecks_evidence_after_probes() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    for (label, mutation) in [
        (
            "new activity",
            "UPDATE sandboxes SET last_command_at=NOW() WHERE id=$1",
        ),
        (
            "new access",
            "UPDATE sandboxes SET last_access_at=NOW() WHERE id=$1",
        ),
        (
            "new lifecycle",
            "UPDATE sandboxes SET lifecycle='deleting' WHERE id=$1",
        ),
        (
            "new episode",
            "UPDATE sandbox_quarantine_state SET quarantine_id=space_uuid_v7() WHERE sandbox_id=$1",
        ),
        (
            "operator ownership",
            "UPDATE sandbox_quarantine_state SET next_action_owner='operator' WHERE sandbox_id=$1",
        ),
        (
            "recovery claim",
            "UPDATE sandbox_quarantine_state SET claim_token=space_uuid_v7() WHERE sandbox_id=$1",
        ),
        (
            "wrong failure",
            "UPDATE sandbox_quarantine_state SET reason_code='pause_failed' WHERE sandbox_id=$1",
        ),
        (
            "renewed restore",
            "UPDATE sandboxes SET lifecycle='restoring', restore_lease_token=space_uuid_v7(), restore_lease_expires_at=NOW()+INTERVAL '1 minute' WHERE id=$1",
        ),
        (
            "new pod identity",
            "UPDATE sandbox_k8s_executions SET pod_uid='replacement' WHERE sandbox_id=$1",
        ),
        (
            "new activation",
            "UPDATE sandbox_k8s_executions SET state='recovering' WHERE sandbox_id=$1",
        ),
        (
            "unfinished upload",
            "UPDATE snapshots SET state='creating' WHERE sandbox_id=$1",
        ),
        (
            "later snapshot",
            "UPDATE snapshots SET timestamp=NOW() WHERE sandbox_id=$1",
        ),
        (
            "wrong origin",
            "UPDATE snapshots SET artifact_origin='stop' WHERE sandbox_id=$1",
        ),
        (
            "missing storage",
            "DELETE FROM snapshot_storage_locations WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id=$1)",
        ),
        (
            "multiple executions",
            "INSERT INTO sandbox_k8s_executions (sandbox_id,generation,pod_name,pod_namespace,state,active) VALUES ($1,2,'replacement-'||$1::TEXT,'sandboxes','running',FALSE)",
        ),
    ] {
        let c = fixture(&db.pool).await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(mutation)))
            .bind(c.sandbox_id)
            .execute(&db.pool)
            .await?;
        assert!(!store::recover(&db.pool, &c).await?, "{label}");
        let present: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS(SELECT 1 FROM sandbox_quarantine_state WHERE sandbox_id=$1)"
        ))
        .bind(c.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert!(present, "{label}: retain the quarantine");
    }
    Ok(())
}

/// The recovery moves a row to `suspended` only while it is still
/// `quarantined`: a candidate listed while `quarantined` whose row now stores
/// any other lifecycle, with every other piece of evidence and its age
/// unchanged, is refused and keeps that lifecycle. `A`: recovered; `.`:
/// refused.
#[tokio::test]
async fn publication_quarantine_repair_recovers_only_a_row_still_quarantined() -> TestResult {
    const EXPECTED: &[&str] = &[
        "created           .",
        "creating          .",
        "starting          .",
        "started           .",
        "restoring         .",
        "pausing           .",
        "paused            .",
        "resuming          .",
        "suspending        .",
        "suspended         .",
        "stopping          .",
        "stopped           .",
        "deleting          .",
        "checkpointing     .",
        "quarantined       A",
        "failed            .",
        "deleted           .",
        "cleanup_required  .",
        "bogus             .",
        "_Suspended_       .",
    ];
    let db = pg_test_db_with_max_connections(3).await?;
    let mut mismatches = Vec::new();
    for (stored, [(_, recovers)]) in stored_lifecycle_matrix(["recover"], EXPECTED) {
        let recovers = recovers.applies();
        let c = fixture(&db.pool).await?;
        relabel_keeping_lifecycle_age(&db.pool, c.sandbox_id, stored).await?;

        let recovered = store::recover(&db.pool, &c).await?;

        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id=$1"))
                .bind(c.sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        let expected_lifecycle = if recovers { "suspended" } else { stored };
        if recovered != recovers || lifecycle != expected_lifecycle {
            mismatches.push(format!(
                "from {stored}: recovered {recovered}, row now {lifecycle}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[tokio::test]
async fn publication_quarantine_repair_requires_positive_absence_from_both_owners() {
    use ObservedSandboxState::*;
    for (first_pod_absent, worker, second_pod_absent, expected) in [
        (true, Some(Missing), true, true),
        (false, Some(Missing), true, false),
        (true, Some(Missing), false, false),
        (true, None, true, false),
        (true, Some(VmAlive { node_status: None }), true, false),
        (
            true,
            Some(DormantWithArtifact { node_status: None }),
            true,
            false,
        ),
        (
            true,
            Some(DormantNoArtifact { node_status: None }),
            true,
            false,
        ),
    ] {
        let mut pods = [first_pod_absent, second_pod_absent].into_iter();
        let actual = confirm_absence(
            || std::future::ready(Ok(pods.next().expect("at most two probes"))),
            async { Ok(worker) },
        )
        .await
        .unwrap();
        assert_eq!(
            actual, expected,
            "pod absence={first_pod_absent}/{second_pod_absent}, worker={worker:?}"
        );
    }
    let unavailable = || {
        RecoveryError::Worker(
            pplx_space_api::controllers::substrate::SubstrateError::Unavailable {
                message: "synthetic unavailable probe".into(),
            },
        )
    };
    confirm_absence(|| async { Err(unavailable()) }, async { Ok(Some(Missing)) })
        .await
        .expect_err("failed pod probe must defer recovery");
    confirm_absence(|| async { Ok(true) }, async { Err(unavailable()) })
        .await
        .expect_err("failed worker probe must defer recovery");
}

/// The candidate scan binds the Kubernetes substrate: the recoverable fixture
/// is no candidate once its row is direct-managed.
#[tokio::test]
async fn direct_rows_are_not_recovery_candidates() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let c = fixture(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1"
    ))
    .bind(c.sandbox_id)
    .execute(&db.pool)
    .await?;
    assert!(
        store::candidates(&db.pool, None, Some(c.sandbox_id), 2)
            .await?
            .is_empty()
    );
    Ok(())
}
