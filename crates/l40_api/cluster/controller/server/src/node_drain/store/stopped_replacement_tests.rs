//! Postgres-backed coverage for the two guards around re-placing an evacuated
//! `stopped` sandbox: the evacuation listing never offers an encrypted stopped
//! resident, whose disk can't be re-placed, and the restore sweep opens a
//! re-placement only where `SPACE_NODE_DRAIN_REPLACE_STOPPED_EVACUATIONS` is on.
//!
//! The re-placement itself, run end to end through the Kubernetes substrate,
//! is `lifecycle_writer_tests::restore_finalize`'s
//! `a_released_stopped_evacuation_is_re_placed_stopped`.

use super::stopped_evacuation_tests::{
    TestResult, forced_draining_node, insert_sandbox, marker_of,
};
use super::*;

/// A sink over `pool` with every drain flag on and residency read from the
/// lifecycle alone, so no case depends on census rows it didn't seed. None of
/// these cases reaches a substrate call.
fn sink(pool: PgPool) -> SubstrateSink {
    let mut sink = SubstrateSink::with_suspend_execution(pool, |_| async move {
        unreachable!("these cases never suspend");
    });
    sink.residency_evidence = ResidencyEvidence::LifecycleOnly;
    sink
}

async fn encrypt(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET volume_encryption = TRUE WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $2, evacuation_restore_target = 'stopped'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar::<_, String>(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// **An encrypted stopped resident is never evacuated, and it holds its node.**
///
/// sandbox-manager can't materialize an encrypted capture as a stopped
/// sandbox, so evacuating one would release the node holding the only usable
/// copy and leave the sandbox on no node with nothing able to place it.
///
/// One fixture carries every population the exclusion has to tell apart, so a
/// filter that dropped too much or too little fails here: encrypted stopped
/// rows, fresh and already marked, are withheld; the same two shapes in
/// plaintext, and an encrypted *started* resident (suspend evacuation handles
/// encryption), are still offered.
#[tokio::test(flavor = "current_thread")]
async fn an_encrypted_stopped_resident_is_not_offered_and_holds_its_node() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = sink(pool.clone());
    let node = forced_draining_node(pool).await?;

    let plaintext_fresh = insert_sandbox(pool, Some(node), "stopped").await?;
    let plaintext_in_flight = insert_sandbox(pool, Some(node), "stopped").await?;
    mark(pool, plaintext_in_flight, "stopped").await?;
    let encrypted_fresh = insert_sandbox(pool, Some(node), "stopped").await?;
    encrypt(pool, encrypted_fresh).await?;
    let encrypted_in_flight = insert_sandbox(pool, Some(node), "stopped").await?;
    encrypt(pool, encrypted_in_flight).await?;
    mark(pool, encrypted_in_flight, "stopped").await?;
    let encrypted_started = insert_sandbox(pool, Some(node), "started").await?;
    encrypt(pool, encrypted_started).await?;

    let mut listed: Vec<SandboxId> = NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node)
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    listed.sort();
    let mut want = vec![plaintext_fresh, plaintext_in_flight, encrypted_started];
    want.sort();
    assert_eq!(
        listed, want,
        "encrypted stopped residents, fresh or marked, must be withheld; everything else stays \
         offered"
    );

    // Held: a node whose only resident is an encrypted stopped sandbox doesn't
    // complete its drain, while an empty node beside it does.
    let held = forced_draining_node(pool).await?;
    let resident = insert_sandbox(pool, Some(held), "stopped").await?;
    encrypt(pool, resident).await?;
    let empty = forced_draining_node(pool).await?;
    assert!(
        !NodeDrainSink::set_drain_phase(&sink, held, PHASE_DRAINING, PHASE_DRAINED, None).await?,
        "an unevacuated encrypted stopped resident must hold its node in draining"
    );
    assert!(
        NodeDrainSink::set_drain_phase(&sink, empty, PHASE_DRAINING, PHASE_DRAINED, None).await?,
        "the same write on an empty node must complete, or the refusal above proves nothing"
    );
    Ok(())
}

/// **With re-placement off, the sweep opens no stopped re-placement, but one
/// already claimed finishes.**
///
/// A released stopped evacuation is refused as a restore failure, so it backs
/// off and escalates by id, and nothing about the row changes. A row the claim
/// has already moved to `restoring` passes the gate: here it reaches the
/// eligibility check and defers, because the fixture gives it no durable disk
/// artifact, which a refused row could never reach. The same released shape
/// is re-placed with the flag on in
/// `a_released_stopped_evacuation_is_re_placed_stopped`.
#[tokio::test(flavor = "current_thread")]
async fn with_re_placement_off_only_a_claimed_re_placement_proceeds() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let mut sink = sink(pool.clone());
    sink.replace_stopped_evacuations = false;
    let timeout = PositiveDuration::new(Duration::from_secs(5)).expect("non-zero");

    let released = insert_sandbox(pool, None, "stopped").await?;
    mark(pool, released, "stopped").await?;
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(
        pool,
        released,
        LocalArtifactId::new(),
        "s3://test-bucket/released-disk?region=us-east-1",
    )
    .await?;
    let Err(error) = sink
        .restore_evacuated_sandbox(released, SandboxLifecycle::Stopped, timeout, None)
        .await
    else {
        panic!("an unopened re-placement must be refused with the flag off");
    };
    assert!(
        matches!(&error, EvacuateError::Restore(message)
            if message.contains("SPACE_NODE_DRAIN_REPLACE_STOPPED_EVACUATIONS")),
        "the refusal must be a restore failure naming the flag, got {error:?}"
    );
    assert_eq!(lifecycle_of(pool, released).await?, "stopped");
    assert_eq!(marker_of(pool, released).await?.as_deref(), Some("stopped"));

    let claimed = insert_sandbox(pool, None, "stopped").await?;
    mark(pool, claimed, "restoring").await?;
    let outcome = sink
        .restore_evacuated_sandbox(claimed, SandboxLifecycle::Stopped, timeout, None)
        .await?;
    assert!(
        matches!(outcome, RestoreOutcome::Deferred),
        "a claimed re-placement must pass the gate and defer on the missing artifact"
    );
    Ok(())
}
