//! Writer × stored-lifecycle matrix for the Kubernetes suspend claim
//! (`claim_suspend_execution`) and the suspend commit that records the
//! node's published artifact (`persist_or_accept_suspend_commit`).
//!
//! Each case seeds a `k8s` row with an active `running` execution whose
//! generation and pod UID the writer is handed, then stores one lifecycle on
//! the row, so the lifecycle is the only variable. The expected table is
//! written out rather than derived from the statements.

use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId, SandboxId};
use pplx_space_placement::test_support::stored_lifecycle_matrix;

use super::*;

/// Columns are the claim and the commit. `A`: the claim moves the row to
/// `suspending`, or the commit records the artifact on it; `.`: the writer
/// refuses and the row keeps its lifecycle and artifact marker.
const EXPECTED: &[&str] = &[
    //              claim  commit
    "created          .      .",
    "creating         .      .",
    "starting         .      .",
    "started          A      .",
    "restoring        .      .",
    "pausing          .      .",
    "paused           A      .",
    "resuming         .      .",
    "suspending       .      A",
    "suspended        .      .",
    "stopping         .      .",
    "stopped          .      .",
    "deleting         .      .",
    "checkpointing    .      .",
    "quarantined      .      .",
    "failed           .      .",
    "deleted          .      .",
    "cleanup_required .      .",
    "bogus            .      .",
    "_Suspended_      .      .",
];

/// A `k8s` row stored in `lifecycle` whose active execution is `running`
/// under a pod UID, with that execution's generation and UID.
async fn seed(
    fixture: &PostgresStore,
    lifecycle: &str,
) -> TestResult<(SandboxId, ExecutionGeneration, PodUid)> {
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let claim = fixture
        .store
        .ensure_active_execution_row(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = $3, state = 'running'
          WHERE sandbox_id = $1
            AND generation = $2
            AND active"
    ))
    .bind(sandbox_id)
    .bind(claim.generation)
    .bind(Uuid::now_v7().to_string())
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(&fixture.pool)
        .await?;
    let execution = fixture
        .store
        .active_execution_status(sandbox_id)
        .await?
        .expect("the seeded execution is active");
    let generation = ExecutionGeneration::new(execution.generation)?;
    let pod_uid = execution
        .pod_uid
        .expect("the seeded execution has a pod UID");
    Ok((sandbox_id, generation, pod_uid))
}

async fn lifecycle_and_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(String, Option<LocalArtifactId>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn suspend_claim_and_commit_move_only_their_stored_lifecycles() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;

    let mut mismatches = Vec::new();
    for (stored, [(_, claims), (_, commits)]) in
        stored_lifecycle_matrix(["claim", "commit"], EXPECTED)
    {
        let (claims, commits) = (claims.applies(), commits.applies());
        let (sandbox_id, generation, pod_uid) = seed(&fixture, stored).await?;
        let claimed = matches!(
            fixture
                .store
                .claim_suspend_execution(sandbox_id, generation, &pod_uid)
                .await,
            Ok(SuspendExecutionClaim::Claimed(_))
        );
        let (lifecycle, _) = lifecycle_and_artifact(pool, sandbox_id).await?;
        let expected = if claims { "suspending" } else { stored };
        if claimed != claims || lifecycle != expected {
            mismatches.push(format!(
                "claim from {stored}: claimed {claimed}, row now {lifecycle}"
            ));
        }

        let (sandbox_id, generation, pod_uid) = seed(&fixture, stored).await?;
        let artifact = super::super::super::super::PublishedLocalArtifact {
            id: LocalArtifactId::new(),
            kind: ArtifactKind::Full,
        };
        let committed = fixture
            .store
            .persist_or_accept_suspend_commit(sandbox_id, generation, &pod_uid, artifact)
            .await
            .is_ok();
        let after = lifecycle_and_artifact(pool, sandbox_id).await?;
        let expected = (stored.to_owned(), commits.then_some(artifact.id));
        if committed != commits || after != expected {
            mismatches.push(format!(
                "commit from {stored}: committed {committed}, row now {after:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
