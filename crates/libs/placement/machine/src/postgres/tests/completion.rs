//! `complete_placement` over the real migrated schema: the fenced CAS, the
//! lapsed-claim adoption, the blocking and superseding guard sets, and the
//! node-assignment stamp.

use super::*;
use pplx_space_common_core::types::NodeId;

#[tokio::test(flavor = "current_thread")]
async fn complete_placement_flips_the_claimed_row_and_clears_the_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    mark_execution_routable(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::Completed
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("started", "on_node")
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "leaving the restore window clears the lease"
    );
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost,
        "a consumed fence must not complete twice"
    );
    Ok(())
}

/// Completion is also the placement write: the CAS stamps the node
/// assignment derived from the fenced execution's pod, mirroring the
/// sandbox-side write of the legacy bind leg (`persist_pod_node_placement`).
/// Live routing, drain, and orphan repair all read `active_node_id`; a
/// serving row without it is the phantom shape they choke on.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_stamps_the_node_assignment_from_the_fenced_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    let node_id = mark_execution_routable(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::Completed
    );

    let (active_node, target_node, placement_stamped): (Option<NodeId>, Option<NodeId>, bool) =
        sqlx::query_as(sql!(
            "SELECT active_node_id, target_node_id, placement_changed_at IS NOT NULL
             FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        (active_node, target_node),
        (Some(node_id), Some(node_id)),
        "the completion must record the fenced execution's node assignment"
    );
    assert!(
        placement_stamped,
        "the completion must stamp placement_changed_at"
    );
    Ok(())
}

/// SPACE node identity is role-scoped — the same Kubernetes host registers
/// one SPACE node per role, so worker and builder rows share a
/// `k8s_node_name`. The completion must stamp the worker row, never a role
/// sibling: routing and drain read `active_node_id` as "the worker serving
/// this sandbox".
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_stamps_the_worker_row_when_roles_share_the_node_name() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    let shared_name = format!("shared-host-{}", Uuid::now_v7().simple());
    // Builder first, so a name-only lookup that takes "any matching row"
    // has the wrong answer available before the right one.
    register_k8s_node(pool, "builder", &shared_name).await?;
    let worker = register_k8s_node(pool, "worker", &shared_name).await?;
    set_execution_routable_on(pool, sandbox, &shared_name).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::Completed
    );

    let active_node: Option<NodeId> =
        sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id = $1"))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        active_node,
        Some(worker),
        "the completion must stamp the worker row, not a role sibling"
    );
    Ok(())
}

/// A Kubernetes node registered only under a non-worker role cannot back a
/// serving sandbox: sandboxes run on workers, and stamping the builder
/// sibling would publish a placement that routing and drain act on
/// incorrectly.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_refuses_a_node_registered_only_under_a_non_worker_role() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    let node_name = format!("builder-only-{}", Uuid::now_v7().simple());
    register_k8s_node(pool, "builder", &node_name).await?;
    set_execution_routable_on(pool, sandbox, &node_name).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost,
        "a builder-only node must not back a serving sandbox"
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring", "nothing was written");
    Ok(())
}

/// A fenced execution whose Kubernetes node has no registered SPACE node
/// cannot complete: the join that derives the node assignment is part of
/// the CAS, so "serving" always implies a resolvable `active_node_id`.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_refuses_a_fence_whose_node_is_unregistered() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    // Routable evidence, but the node name maps to no node_k8s row.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running', pod_ip = '10.244.0.7', k8s_node_name = 'ghost-node'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost,
        "an unregistered node must not back a serving sandbox"
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring", "nothing was written");
    Ok(())
}

/// Ownership is not evidence: a fence holder whose restore never produced a
/// routable execution (still `creating`, no pod IP) must not expose the row
/// as serving — that is the connect-wedge shape, a `started`/`on_node` row
/// with nothing routable behind it. The completion CAS demands the fenced
/// execution be routable, the same `started`-convergence invariant the
/// legacy redrive CAS, the transient-lifecycle sweeper's converge CAS, and
/// the node published-state convergence all enforce.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_refuses_a_fence_whose_execution_is_not_routable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    // The claim mints the execution as `creating` with no pod IP — exactly
    // the shape a timed-out or cancelled restore leaves behind.
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost,
        "an unroutable execution must not be exposed as serving"
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("restoring", "unplaced"),
        "the refused completion writes nothing"
    );
    Ok(())
}

/// The single blocking set: a teardown or terminal lifecycle that landed
/// while substrate work was in flight must win against the completion — the
/// row is not resurrected. The lease trigger is bypassed in the arrange step
/// so the CAS loss is attributable to the blocking list alone, not to a
/// cleared token.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_cas_loses_to_every_blocking_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);

    for blocking in PLACEMENT_COMPLETION_BLOCKING_LIFECYCLES {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        let attempt = expect_claimed(
            store
                .claim_placement(sandbox, ClaimKind::UserRestore)
                .await?,
        );
        mark_execution_routable(pool, sandbox).await?;
        set_lifecycle_bypassing_lease_trigger(pool, sandbox, blocking.as_str()).await?;
        assert_eq!(
            lease_token(pool, sandbox).await?,
            Some(attempt.fence.lease.as_uuid()),
            "arrange must keep the token so the CAS loss isolates the blocking list"
        );

        assert_eq!(
            store
                .complete_placement(PlacementProof::Fence(&attempt.fence))
                .await?,
            PlacementOutcome::CasLost,
            "completion must lose to {blocking}"
        );
        let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
        assert_eq!(
            lifecycle,
            blocking.as_str(),
            "the blocked row must not be resurrected"
        );
        assert_ne!(placement, "on_node");
    }
    Ok(())
}

/// An immediate pause can claim `suspending` after the node finishes the
/// wake but before this control-plane completion writes back. The stale
/// completion must not project `started` over the newer suspend claim — that
/// breaks the generation-bound suspend commit — but it is a handoff, not a
/// loss: the node-side wake succeeded and the caller may continue. Guard and
/// disposition ported from the api's post-wake CAS hardening (defer stale
/// wake completion to the suspend owner).
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_is_superseded_by_a_newer_suspend_claim() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);

    for superseding in PLACEMENT_SUPERSEDING_SUSPEND_LIFECYCLES {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        let attempt = expect_claimed(
            store
                .claim_placement(sandbox, ClaimKind::UserRestore)
                .await?,
        );
        mark_execution_routable(pool, sandbox).await?;
        set_lifecycle_bypassing_lease_trigger(pool, sandbox, superseding.as_str()).await?;
        assert_eq!(
            lease_token(pool, sandbox).await?,
            Some(attempt.fence.lease.as_uuid()),
            "arrange must keep the token so the supersession isolates the lifecycle guard"
        );

        assert_eq!(
            store
                .complete_placement(PlacementProof::Fence(&attempt.fence))
                .await?,
            PlacementOutcome::SupersededBySuspend,
            "completion must defer to the {superseding} owner"
        );
        let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
        assert_eq!(
            lifecycle,
            superseding.as_str(),
            "the suspend claim must not be overwritten with started"
        );
        assert_ne!(placement, "on_node");
    }
    Ok(())
}

/// The lapsed-attempt adoption proof reports the same handoff: its CAS
/// already refuses non-`restoring` rows, so a newer suspend owner makes the
/// adoption miss — the classification must call that supersession, not a
/// generic loss, so the publish convergence treats it as converged rather
/// than failed.
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_is_superseded_by_a_newer_suspend_claim() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);

    for superseding in PLACEMENT_SUPERSEDING_SUSPEND_LIFECYCLES {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        expect_claimed(
            store
                .claim_placement(sandbox, ClaimKind::UserRestore)
                .await?,
        );
        mark_execution_routable(pool, sandbox).await?;
        expire_lease(pool, sandbox).await?;
        set_lifecycle_bypassing_lease_trigger(pool, sandbox, superseding.as_str()).await?;

        assert_eq!(
            store
                .complete_placement(PlacementProof::LapsedClaim { sandbox })
                .await?,
            PlacementOutcome::SupersededBySuspend,
            "adoption must defer to the {superseding} owner"
        );
        let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
        assert_eq!(lifecycle, superseding.as_str(), "nothing was written");
        assert_ne!(placement, "on_node");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn complete_placement_cas_loses_when_the_fence_generation_is_retired() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );

    mark_execution_routable(pool, sandbox).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost,
        "a fence whose generation is no longer the active execution is stale"
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring", "nothing was written");
    Ok(())
}

/// Node-drain evacuation is lifecycle-preserving: the marker records the
/// pre-evacuation lifecycle, and the completion must finalize back to it — a
/// `paused` sandbox comes back paused, not started.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_completion_preserves_the_markers_paused_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "paused").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );

    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::Completed
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("paused", "on_node"),
        "the completion must finalize to the marker's preserved lifecycle"
    );
    assert_eq!(
        evacuation_marker(pool, sandbox).await?,
        None,
        "the finalize consumes the marker"
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "leaving the restore window clears the lease"
    );
    Ok(())
}

/// A fence whose lease token is not the row's current token is stale proof:
/// the completion CAS refuses it and writes nothing, even though the
/// lifecycle and generation still match.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_refuses_a_fence_with_a_foreign_lease_token() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    let forged = PlacementFence {
        sandbox,
        generation: attempt.fence.generation,
        lease: LeaseToken::mint(),
    };
    mark_execution_routable(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&forged))
            .await?,
        PlacementOutcome::CasLost
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        lifecycle, "restoring",
        "the refused completion wrote nothing"
    );
    assert_ne!(placement, "on_node");
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(attempt.fence.lease.as_uuid()),
        "the row keeps the real owner's token"
    );
    Ok(())
}

/// A fence from a superseded generation must not complete even while the
/// row still carries its lease token: ownership is the whole fence, not the
/// token alone.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_refuses_a_fence_from_a_superseded_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    // A newer generation replaced the fence's execution row; the replacement
    // is routable so the CAS loss isolates the generation mismatch.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    insert_execution(pool, sandbox, 2, "creating", 0).await?;
    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(attempt.fence.lease.as_uuid()),
        "arrange keeps the token so the CAS loss isolates the generation"
    );

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost
    );

    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring", "nothing was written");
    Ok(())
}

/// Completion is once-only: leaving `restoring` cleared the lease, so a
/// second completion with the same fence is a CAS loss that leaves the
/// completed row exactly as the first completion wrote it.
#[tokio::test(flavor = "current_thread")]
async fn second_completion_with_a_consumed_fence_leaves_the_completed_row_intact() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    mark_execution_routable(pool, sandbox).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::Completed
    );

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&attempt.fence))
            .await?,
        PlacementOutcome::CasLost,
        "a consumed fence must not complete twice"
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("started", "on_node"),
        "the completed row is untouched by the refused second completion"
    );
    assert_eq!(lease_token(pool, sandbox).await?, None);
    Ok(())
}

/// A wake completion for a row that no longer exists is a CAS loss, not an
/// error: a hard delete is one more supervisor write that wins the race.
#[tokio::test(flavor = "current_thread")]
async fn complete_placement_on_a_missing_sandbox_is_a_cas_loss() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let store = store_for(&db);
    let sandbox = SandboxId::new();
    let fence = PlacementFence {
        sandbox,
        generation: ExecutionGeneration::new(1).expect("1 is a valid generation"),
        lease: LeaseToken::mint(),
    };

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&fence))
            .await?,
        PlacementOutcome::CasLost
    );
    Ok(())
}

/// The fenceless completion: once the owning attempt's lease has lapsed,
/// `complete_placement(PlacementProof::LapsedClaim { sandbox })` adopts the row's
/// own fence inside its CAS and finalizes exactly like the owner would
/// have.
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_adopts_an_expired_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    mark_execution_routable(pool, sandbox).await?;
    expire_lease(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::Completed
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("started", "on_node")
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "the adopted completion consumes the lapsed lease pair"
    );
    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::CasLost,
        "a consumed adoption must not complete twice"
    );
    Ok(())
}

/// A cross-node re-drive can replace the active execution generation under
/// the same lease before the owning attempt lapses. The adoption reads the
/// row's *active* execution — whatever generation it now carries — so the
/// lapsed-claim completion converges a row whose claim-time generation was
/// replaced, which a fence captured at claim time could never complete.
/// (Ported from the deleted api cancellation-redrive suite; the publish
/// ingest is the production caller of this CAS.)
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_adopts_a_replaced_active_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    // The lapsed attempt's own claim: its fence pairs generation 1.
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    // A cross-node re-drive replaces the active execution under the same
    // lease.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    insert_execution(pool, sandbox, 2, "running", 0).await?;
    mark_execution_routable(pool, sandbox).await?;
    expire_lease(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::Completed,
        "the adoption must complete through the replaced active generation"
    );
    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("started", "on_node")
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "the adopted completion consumes the lapsed lease pair"
    );
    Ok(())
}

/// An unexpired lease belongs to a live attempt that will run its own
/// fenced completion; the adoption CAS must refuse it even though the
/// execution is already routable.
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_defers_to_a_live_lease() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    mark_execution_routable(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::CasLost
    );

    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring", "the refused adoption wrote nothing");
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(attempt.fence.lease.as_uuid()),
        "the live owner's token survives the refused adoption"
    );
    Ok(())
}

/// A tokenless transitional row is the transient-lifecycle sweeper's to
/// converge: with no lease pair there is nothing to adopt, and the CAS
/// matches zero rows.
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_refuses_a_tokenless_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "restoring").await?;
    mark_execution_routable(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::CasLost
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(lifecycle, "restoring");
    Ok(())
}

/// Adoption is not a shortcut around evidence: the active execution must be
/// routable at flip time, the same demand the fenced CAS makes.
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_refuses_an_unroutable_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    expire_lease(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::CasLost
    );
    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        lifecycle, "restoring",
        "no routable evidence, nothing written"
    );
    Ok(())
}

/// The adopted completion runs the same lifecycle-preserving finalize as
/// the fenced one: an evacuation-marked row lands on the marker's target
/// with the marker consumed and the lease pair cleared in the CAS's own
/// SET (finalizing into `paused` cannot rely on the lease-clearing
/// trigger).
#[tokio::test(flavor = "current_thread")]
async fn lapsed_claim_completion_preserves_the_evacuation_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    set_evacuation_marker(pool, sandbox, "paused").await?;
    expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::EvacuationRestore)
            .await?,
    );
    mark_execution_routable(pool, sandbox).await?;
    expire_lease(pool, sandbox).await?;

    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox })
            .await?,
        PlacementOutcome::Completed
    );

    let (lifecycle, placement) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("paused", "on_node"),
        "the adoption must finalize to the marker's preserved lifecycle"
    );
    assert_eq!(
        evacuation_marker(pool, sandbox).await?,
        None,
        "the finalize consumes the marker"
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        None,
        "the adopted completion consumes the lapsed lease pair"
    );
    Ok(())
}

/// The legacy fence bridge assembles proof, not ownership: a
/// `PlacementFence::from_held_lease` fence whose token is not the row's
/// current lease CAS-loses the completion exactly like any other stale
/// fence, even with the lifecycle, generation, and routable evidence all in
/// place.
#[tokio::test(flavor = "current_thread")]
async fn from_held_lease_fence_with_a_foreign_token_cas_loses_completion() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let attempt = expect_claimed(
        store
            .claim_placement(sandbox, ClaimKind::UserRestore)
            .await?,
    );
    mark_execution_routable(pool, sandbox).await?;
    let bridged =
        PlacementFence::from_held_lease(sandbox, attempt.fence.generation(), LeaseToken::mint());

    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&bridged))
            .await?,
        PlacementOutcome::CasLost,
        "a bridged fence with a foreign token must not complete"
    );

    let (lifecycle, _) = lifecycle_and_placement(pool, sandbox).await?;
    assert_eq!(
        lifecycle, "restoring",
        "the refused completion wrote nothing"
    );
    assert_eq!(
        lease_token(pool, sandbox).await?,
        Some(attempt.fence.lease().as_uuid()),
        "the row keeps the real owner's token"
    );
    Ok(())
}

/// A stopped evacuation's re-placement has no finalization yet, so its
/// attempt can't complete under either proof, even with a routable execution
/// that would satisfy the rest of the CAS. A `paused` evacuation with the
/// same evidence completes beside it, so the refusal is the marker's.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_evacuation_attempt_cannot_complete_under_either_proof() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = store_for(&db);
    let claim = |target: &'static str| {
        let store = store.clone();
        async move {
            let lifecycle = if target == "stopped" {
                "stopped"
            } else {
                "suspended"
            };
            let sandbox = insert_sandbox(pool, lifecycle).await?;
            set_evacuation_marker(pool, sandbox, target).await?;
            let attempt = expect_claimed(
                store
                    .claim_placement(sandbox, ClaimKind::EvacuationRestore)
                    .await?,
            );
            mark_execution_routable(pool, sandbox).await?;
            TestResult::Ok((sandbox, attempt))
        }
    };

    let (control, control_attempt) = claim("paused").await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&control_attempt.fence))
            .await?,
        PlacementOutcome::Completed
    );
    assert_eq!(lifecycle_and_placement(pool, control).await?.0, "paused");

    let (fenced, fenced_attempt) = claim("stopped").await?;
    assert_eq!(fenced_attempt.origin, ClaimOrigin::FreshFromStopped);
    assert_eq!(
        store
            .complete_placement(PlacementProof::Fence(&fenced_attempt.fence))
            .await?,
        PlacementOutcome::CasLost
    );

    let (lapsed, _) = claim("stopped").await?;
    expire_lease(pool, lapsed).await?;
    assert_eq!(
        store
            .complete_placement(PlacementProof::LapsedClaim { sandbox: lapsed })
            .await?,
        PlacementOutcome::CasLost
    );

    for sandbox in [fenced, lapsed] {
        assert_eq!(
            lifecycle_and_placement(pool, sandbox).await?.0,
            "restoring",
            "nothing was written"
        );
        assert_eq!(
            evacuation_marker(pool, sandbox).await?.as_deref(),
            Some("stopped")
        );
    }
    Ok(())
}
