//! What a Direct restore spends, and when: admission is charged by the
//! restore that claims the lease, so a restore the claim refuses spends
//! none, one admission refuses or that is dropped while it waits for
//! admission, or while it gives a refused claim back, gives its claim back (a
//! retry's lease ends at once), and a wake
//! that paid before asking its node doesn't pay again when it falls back to
//! a restore.
//! A wake that finds its unplaced sandbox still running pays before it
//! places the row on that node.
//! Also the pin's view of a source node whose registration is deleted, which
//! the restore doesn't load a local copy from.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pplx_space_sandbox_store::restore_pin::{pin_restore_source, restore_pin_state};

use super::*;
use crate::controllers::substrate::{
    ActivationAdmission, ActivationAdmissionClass, ActivationAdmissionFuture,
    ActivationAdmissionRejection, ActivationAttribution, ActivationRejectionKind, CapPolicy,
    LiveCapFuture, RecoveryCharge,
};

/// Counts the restores it's asked to admit, and refuses them when `refuse`.
#[derive(Default)]
struct CountingAdmission {
    asked: AtomicUsize,
    refuse: bool,
}

impl ActivationAdmission for CountingAdmission {
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_> {
        assert_eq!(class, ActivationAdmissionClass::Restore);
        self.asked.fetch_add(1, Ordering::SeqCst);
        let refuse = self.refuse;
        Box::pin(async move {
            if refuse {
                return Err(ActivationAdmissionRejection::new(
                    "no token".to_owned(),
                    Some(Duration::from_secs(1)),
                    ActivationRejectionKind::RateLimited,
                ));
            }
            Ok(())
        })
    }

    fn reserve_live(
        &self,
        _client: pplx_space_accounts::ClientId,
        _sandbox: SandboxId,
    ) -> LiveCapFuture<'_> {
        Box::pin(async { Ok(None) })
    }

    fn precheck_live(
        &self,
        _client: pplx_space_accounts::ClientId,
        _sandbox: SandboxId,
    ) -> ActivationAdmissionFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn live_cap_configured(&self) -> bool {
        false
    }
}

/// Tells the test when a restore asks it for admission, and doesn't answer
/// until the test tells it to refuse.
#[derive(Default)]
struct BlockedAdmission {
    asked: tokio::sync::Notify,
    refuse: tokio::sync::Notify,
}

impl ActivationAdmission for BlockedAdmission {
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_> {
        assert_eq!(class, ActivationAdmissionClass::Restore);
        self.asked.notify_one();
        Box::pin(async move {
            self.refuse.notified().await;
            Err(ActivationAdmissionRejection::new(
                "no token".to_owned(),
                Some(Duration::from_secs(1)),
                ActivationRejectionKind::RateLimited,
            ))
        })
    }

    fn reserve_live(
        &self,
        _client: pplx_space_accounts::ClientId,
        _sandbox: SandboxId,
    ) -> LiveCapFuture<'_> {
        Box::pin(async { Ok(None) })
    }

    fn precheck_live(
        &self,
        _client: pplx_space_accounts::ClientId,
        _sandbox: SandboxId,
    ) -> ActivationAdmissionFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn live_cap_configured(&self) -> bool {
        false
    }
}

/// A suspended sandbox with one ready backup of generation 5, restorable on a
/// worker other than its source node. Nothing listens on either endpoint.
/// Returns the sandbox and the backup.
async fn insert_remote_restorable(pool: &PgPool) -> TestResult<(SandboxId, SnapshotId)> {
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    insert_sandbox(pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(pool, source_node_id, "127.0.0.1:1", "9.0.0").await?;
    insert_ready_worker(pool, NodeId::new(), "127.0.0.1:2", "9.0.0").await?;
    let snapshot_id = insert_ready_snapshot(
        pool,
        sandbox_id,
        source_node_id,
        "9.0.0",
        Some("s3://bucket/snapshots/sandbox/charge"),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET local_artifact_id = $2,
                artifact_kind = 'full',
                artifact_origin = 'suspend',
                generation = 5
          WHERE id = $1"
    ))
    .bind(snapshot_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok((sandbox_id, snapshot_id))
}

/// The row's lifecycle and whether it holds a restore lease.
async fn lifecycle_and_lease(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<(String, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, restore_lease_token IS NOT NULL FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn restore<A: ActivationAdmission + 'static>(
    fixture: &PostgresFixture,
    sandbox_id: SandboxId,
    admission: &Arc<A>,
) -> Result<ActivationCompletion, SubstrateError> {
    fixture
        .substrate
        .restore_execution(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("restore"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(2),
            },
            RecoveryCharge::enforce(
                Some(admission),
                CapPolicy::ReserveAtClaim,
                ActivationAttribution::native(),
            ),
        )
        .await
}

/// A restore whose claim is refused because a capture is stamped ahead of
/// the database clock answers the retryable `Unavailable` (a 503), leaves the
/// sandbox suspended and unleased, and spends no admission. Once the clock
/// has passed the capture, the restore claims and pays once.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_refused_at_its_claim_spends_no_admission() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (sandbox_id, snapshot_id) = insert_remote_restorable(&fixture.pool).await?;
    let stamp = |ahead: bool| {
        let pool = fixture.pool.clone();
        async move {
            sqlx::query(sql!(
                "UPDATE snapshots
                    SET \"timestamp\" = clock_timestamp()
                        + CASE WHEN $2 THEN INTERVAL '1 hour' ELSE INTERVAL '0' END
                  WHERE id = $1"
            ))
            .bind(snapshot_id)
            .bind(ahead)
            .execute(&pool)
            .await
        }
    };
    stamp(true).await?;
    let admission = Arc::new(CountingAdmission::default());

    let refused = restore(&fixture, sandbox_id, &admission)
        .await
        .expect_err("a claim must not land below a registered capture");
    assert!(
        matches!(
            &refused,
            SubstrateError::Unavailable { message }
                if message.contains("ahead of the database clock")
        ),
        "{refused:?}"
    );
    assert_eq!(admission.asked.load(Ordering::SeqCst), 0);
    assert_eq!(
        lifecycle_and_lease(&fixture.pool, sandbox_id).await?,
        ("suspended".to_owned(), false)
    );

    // The clock passes the capture: the restore claims, pays, and only then
    // fails, on a worker nothing listens on.
    stamp(false).await?;
    let failed = restore(&fixture, sandbox_id, &admission)
        .await
        .expect_err("no worker is listening");
    assert!(
        !failed.to_string().contains("ahead of the database clock"),
        "{failed:?}"
    );
    assert_eq!(admission.asked.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A restore that admission refuses after its claim gives the claim back:
/// the sandbox is suspended and unleased again, so the next restore can
/// claim it.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_admission_refuses_gives_its_claim_back() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (sandbox_id, _) = insert_remote_restorable(&fixture.pool).await?;
    let admission = Arc::new(CountingAdmission {
        refuse: true,
        ..CountingAdmission::default()
    });

    let refused = restore(&fixture, sandbox_id, &admission)
        .await
        .expect_err("admission refuses");
    assert!(
        matches!(
            &refused,
            SubstrateError::AdmissionRejected {
                kind: ActivationRejectionKind::RateLimited,
                ..
            }
        ),
        "{refused:?}"
    );
    assert_eq!(admission.asked.load(Ordering::SeqCst), 1);
    assert_eq!(
        lifecycle_and_lease(&fixture.pool, sandbox_id).await?,
        ("suspended".to_owned(), false)
    );
    Ok(())
}

/// A retry that admission refuses after taking over an expired lease asked
/// no worker to start the sandbox, so its lease ends at once rather than on
/// its own: the next restore takes the row over without waiting it out. The
/// row stays `restoring` and its claim time doesn't move, so that restore
/// reads the same source window.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_admission_refuses_does_not_hold_its_lease() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (sandbox_id, _) = insert_remote_restorable(&fixture.pool).await?;
    // An earlier restore claimed the sandbox and its lease lapsed.
    let claimed_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring',
                lifecycle_changed_at = clock_timestamp(),
                restore_lease_expires_at = NOW() - INTERVAL '1 second',
                restore_lease_token = $2
          WHERE id = $1
      RETURNING lifecycle_changed_at"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .fetch_one(&fixture.pool)
    .await?;
    let refusing = Arc::new(CountingAdmission {
        refuse: true,
        ..CountingAdmission::default()
    });

    let refused = restore(&fixture, sandbox_id, &refusing)
        .await
        .expect_err("admission refuses");
    assert!(
        matches!(
            &refused,
            SubstrateError::AdmissionRejected {
                kind: ActivationRejectionKind::RateLimited,
                ..
            }
        ),
        "{refused:?}"
    );
    assert_eq!(refusing.asked.load(Ordering::SeqCst), 1);
    let (lifecycle, lease_ended, still_claimed_at): (String, bool, time::OffsetDateTime) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, restore_lease_expires_at <= NOW(), lifecycle_changed_at
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        (lifecycle.as_str(), lease_ended, still_claimed_at),
        ("restoring", true, claimed_at)
    );

    // The next restore takes the row over and pays; it fails only on a
    // worker nothing listens on.
    let admitting = Arc::new(CountingAdmission::default());
    let failed = restore(&fixture, sandbox_id, &admitting)
        .await
        .expect_err("no worker is listening");
    assert!(
        !matches!(&failed, SubstrateError::RestoreInFlight { .. }),
        "{failed:?}"
    );
    assert_eq!(admitting.asked.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A restore dropped while it waits for admission asked no worker to start
/// the sandbox, so it gives its claim back as a refused one does: a first
/// attempt's row goes back to `suspended`, a retry's lease ends at once, and
/// the next restore claims without waiting out a lease nothing runs under.
/// So does one dropped after admission `refused` it, while its own release
/// of the claim still waits for a database connection.
#[rstest::rstest]
#[tokio::test(flavor = "current_thread")]
async fn a_restore_cancelled_at_admission_does_not_hold_its_lease(
    #[values(false, true)] retry: bool,
    #[values(false, true)] refused: bool,
) -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let (sandbox_id, _) = insert_remote_restorable(&fixture.pool).await?;
    if retry {
        // An earlier restore claimed the sandbox and its lease lapsed.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'restoring',
                    lifecycle_changed_at = clock_timestamp(),
                    restore_lease_expires_at = NOW() - INTERVAL '1 second',
                    restore_lease_token = $2
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .execute(&fixture.pool)
        .await?;
    }
    let lease_is_live = || async {
        let live: bool = sqlx::query_scalar(sql!(
            "SELECT restore_lease_token IS NOT NULL AND restore_lease_expires_at > NOW()
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
        TestResult::Ok(live)
    };

    let blocked = Arc::new(BlockedAdmission::default());
    {
        // Declared first, so the restore is dropped before the connections
        // go back to the pool.
        let mut every_connection = Vec::new();
        let mut waiting = std::pin::pin!(restore(&fixture, sandbox_id, &blocked));
        tokio::select! {
            answered = &mut waiting => panic!("admission hasn't answered: {}", answered.is_ok()),
            () = blocked.asked.notified() => {}
        }
        assert!(lease_is_live().await?);
        if refused {
            // With every connection taken, the release the refusal makes
            // waits for one.
            for _ in 0..fixture.pool.options().get_max_connections() {
                every_connection.push(fixture.pool.acquire().await?);
            }
            blocked.refuse.notify_one();
            tokio::time::timeout(Duration::from_millis(200), &mut waiting)
                .await
                .expect_err("the refused restore's release has no connection to run on");
        }
        // The caller goes away here: the restore is dropped mid-wait.
    }
    // The dropped restore gives the claim back on a task of its own.
    tokio::time::timeout(Duration::from_secs(10), async {
        while lease_is_live().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        TestResult::Ok(())
    })
    .await
    .map_err(|_| "the cancelled restore kept its lease")??;
    assert_eq!(
        lifecycle_and_lease(&fixture.pool, sandbox_id).await?.0,
        if retry { "restoring" } else { "suspended" }
    );

    // The next restore claims and pays; it fails only on a worker nothing
    // listens on.
    let admitting = Arc::new(CountingAdmission::default());
    let failed = restore(&fixture, sandbox_id, &admitting)
        .await
        .expect_err("no worker is listening");
    assert!(
        !matches!(&failed, SubstrateError::RestoreInFlight { .. }),
        "{failed:?}"
    );
    assert_eq!(admitting.asked.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A wake of a placed sandbox pays before it asks its node. When that node
/// holds no record of the sandbox and the wake falls back to a restore, the
/// restore claims its lease and doesn't pay again.
#[tokio::test(flavor = "current_thread")]
async fn a_wake_that_falls_back_to_a_restore_pays_once() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    super::idle_suspended::seed_idle_suspended(
        &fixture.pool,
        &state,
        sandbox_id,
        node_id,
        server.endpoint(),
    )
    .await?;
    let policy = crate::e2b::auto_lifecycle_policy(60, true, true);
    place_suspended_sandbox(&fixture.pool, sandbox_id, node_id, &policy).await?;
    // The node kept the suspended copy and holds no record: it answers an
    // implicit Wake `NotFound`.
    state.remove(&sandbox_id.to_string()).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");
    let admission = Arc::new(CountingAdmission::default());

    let failed = Box::pin(fixture.substrate.wake_execution(
        crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
        &sandbox,
        Duration::from_secs(5),
        Some(ExecutionActivityKind::Access),
        RecoveryCharge::enforce(
            Some(&admission),
            CapPolicy::ReserveAtClaim,
            ActivationAttribution::native(),
        ),
    ))
    .await
    .expect_err("the simulated node restores only a sandbox it has a record of");

    // The restore got past its claim, where it would pay, to the node.
    assert!(
        matches!(
            &failed,
            SubstrateError::Failed { message } if message.contains("node-api restore error")
        ),
        "{failed:?}"
    );
    assert_eq!(admission.asked.load(Ordering::SeqCst), 1);
    drop(server);
    Ok(())
}

/// A wake of an unplaced sandbox whose node still runs it claims no restore
/// lease: it places the row on that node and wakes it there. It pays before
/// it places the row, so a wake admission refuses leaves the row suspended
/// and unplaced. An admitted wake places it and pays once.
#[tokio::test(flavor = "current_thread")]
async fn a_wake_admission_refuses_does_not_place_a_running_sandbox() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    super::idle_suspended::seed_idle_suspended(
        &fixture.pool,
        &state,
        sandbox_id,
        node_id,
        server.endpoint(),
    )
    .await?;
    // The node runs the sandbox its publication left suspended and unplaced.
    state.set_status(&sandbox_id.to_string(), "running").await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");
    let placed = || async {
        sqlx::query_as::<_, (String, Option<NodeId>)>(sql!(
            "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await
    };
    let wake = |admission: Arc<CountingAdmission>| {
        Box::pin(fixture.substrate.wake_execution(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("wake"),
            &sandbox,
            Duration::from_secs(5),
            None,
            RecoveryCharge::enforce(
                Some(&admission),
                CapPolicy::ReserveAtClaim,
                ActivationAttribution::native(),
            ),
        ))
    };

    let refusing = Arc::new(CountingAdmission {
        refuse: true,
        ..CountingAdmission::default()
    });
    let refused = wake(Arc::clone(&refusing))
        .await
        .expect_err("admission refuses");
    assert!(
        matches!(
            &refused,
            SubstrateError::AdmissionRejected {
                kind: ActivationRejectionKind::RateLimited,
                ..
            }
        ),
        "{refused:?}"
    );
    assert_eq!(refusing.asked.load(Ordering::SeqCst), 1);
    assert_eq!(placed().await?, ("suspended".to_owned(), None));

    let admission = Arc::new(CountingAdmission::default());
    wake(Arc::clone(&admission)).await?;
    assert_eq!(admission.asked.load(Ordering::SeqCst), 1);
    assert_eq!(placed().await?, ("started".to_owned(), Some(node_id)));
    drop(server);
    Ok(())
}

/// A source node whose Direct registration is deleted can't take a restore,
/// so an unordered history there is refused for good and the operator is told
/// to pin a capture. The pin doesn't ask that node what it holds: it lands,
/// and the restore then loads the pinned capture on another worker.
#[tokio::test(flavor = "current_thread")]
async fn a_pin_does_not_ask_a_deregistered_source_node() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let source_node_id = NodeId::new();
    let alternate_node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(source_node_id)).await?;
    insert_ready_worker(&fixture.pool, source_node_id, "127.0.0.1:1", "9.0.0").await?;
    insert_ready_worker(&fixture.pool, alternate_node_id, "127.0.0.1:2", "9.0.0").await?;
    let pinned = Uuid::now_v7();
    let pinned_uri = "s3://bucket/snapshots/sandbox/with-generation";
    // (URI, artifact, capture generation, seconds ago): the capture with no
    // generation registered last, so capture order can't choose.
    for (uri, artifact, generation, seconds_ago) in [
        (pinned_uri, pinned, Some(5_i64), 20),
        (
            "s3://bucket/snapshots/sandbox/no-generation",
            Uuid::now_v7(),
            None,
            10,
        ),
    ] {
        let snapshot_id = insert_ready_snapshot(
            &fixture.pool,
            sandbox_id,
            source_node_id,
            "9.0.0",
            Some(uri),
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE snapshots
                SET local_artifact_id = $2,
                    artifact_kind = 'full',
                    artifact_origin = 'suspend',
                    generation = $3,
                    timestamp = NOW() - make_interval(secs => $4::INT)
              WHERE id = $1"
        ))
        .bind(snapshot_id)
        .bind(artifact)
        .bind(generation)
        .bind(seconds_ago)
        .execute(&fixture.pool)
        .await?;
    }
    sqlx::query(sql!(
        "UPDATE node_direct SET deleted_at = NOW() WHERE node_id = $1"
    ))
    .bind(source_node_id)
    .execute(&fixture.pool)
    .await?;

    let refused = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await
        .expect_err("no source node and no orderable capture");
    assert!(
        matches!(&refused, SubstrateError::RestoreContextUnavailable { .. }),
        "{refused:?}"
    );

    let read = restore_pin_state(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox");
    assert!(read.unordered);
    assert_eq!(read.restore_node_id, Some(source_node_id));
    let controller = crate::controllers::sandbox::SandboxController::new(
        crate::controllers::substrate::Substrate::from_direct(DirectSubstrate::new(
            fixture.pool.clone(),
        )),
    );
    assert_eq!(
        controller
            .node_reported_artifact(source_node_id, sandbox_id)
            .await?,
        None
    );
    assert!(
        pin_restore_source(
            &fixture.pool,
            sandbox_id,
            LocalArtifactId::from_uuid(pinned),
            read.lifecycle_changed_at.expect("lifecycle_changed_at"),
            read.restore_node_id,
        )
        .await?
    );

    let candidates = fixture
        .substrate
        .direct_restore_candidates(sandbox_id)
        .await?;
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (
                candidate.kind,
                candidate.node.id,
                candidate.storage_uri.as_deref()
            ))
            .collect::<Vec<_>>(),
        [(
            DirectRestoreCandidateKind::RemoteSnapshot,
            alternate_node_id,
            Some(pinned_uri)
        )]
    );
    Ok(())
}
