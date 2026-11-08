//! The upgrade's claim on the sandbox's execution: what it refuses, what it
//! records, how a takeover reads an earlier attempt's record, and how it
//! excludes starts, drains and re-placements.

use super::*;

/// An upgrade whose claim's lease still runs may own the execution, so
/// another is refused whatever it would decide now: a publication that makes
/// it "as recorded" doesn't let it retire that execution.
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_in_flight_refuses_another() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;

    for publish in [false, true] {
        if publish {
            s.publish_upgraded().await?;
        }
        let error = s.upgrade().await.unwrap_err();

        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        assert!(s.pods().exists());
        assert_eq!(
            s.active_execution().await?,
            Some((7, Some("stopped".to_string()), true))
        );
    }
    Ok(())
}

/// An earlier attempt placed an upgrade the node may have begun. The retry
/// keeps that upgrade although the policy no longer asks for it; it never
/// decides afresh over it.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_keeps_the_upgrade_an_earlier_attempt_recorded() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    s.end_the_lease().await?;
    let recorded = s.recorded_upgrade().await?;
    {
        use pplx_space_runtime_environment_policy_store::{
            PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_environment_state,
        };
        let mut tx = s.db.pool.begin().await?;
        set_environment_state(
            &mut tx,
            E0.parse()?,
            RuntimeEnvironmentState::Allowed,
            &PolicyChange {
                actor: None,
                reason: PolicyChangeReason::new("test allow")?,
            },
        )
        .await?;
        tx.commit().await?;
    }
    s.forget_cached_policy();
    s.node().report(Report::Upgraded { publish: true });

    let operation = upgraded(&s.upgrade().await?);

    assert_eq!(Some(operation), recorded.0);
    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    s.assert_stopped_without_execution("after the retry").await
}

/// A claim targeting `stopped` with nothing recorded isn't an upgrade's: a
/// drain's re-placement leaves one when its best-effort release fails, over
/// the pod holding the sandbox's re-placed copy. While its lease runs, the
/// operation answers a retryable refusal and leaves the execution and its pod
/// to their owner. Once the lease has ended, the operation claims the
/// execution as any activation may claim one whose lease ended, replaces the
/// pod, which doesn't carry the upgrade, and converges.
#[rstest::rstest]
#[case::live(3600)]
#[case::expired(-3600)]
#[tokio::test(flavor = "current_thread")]
async fn a_drains_stopped_claim_is_reclaimed_only_once_its_lease_ended(
    #[case] lease_secs: i64,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "stopped", lease_secs).await?;
    s.cache_the_pod();
    let before = (s.active_execution().await?, left_by(&s).await?);

    let outcome = s.upgrade().await;

    if lease_secs > 0 {
        let error = outcome.unwrap_err();
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        s.assert_nothing_placed_or_sent("refused");
        assert!(s.pods().exists());
        assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 0);
        assert_eq!((s.active_execution().await?, left_by(&s).await?), before);
    } else {
        let operation = upgraded(&outcome?);
        s.pods()
            .assert_every_create_upgrades(&operation.to_string(), "stopped");
        assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);
        assert_eq!(s.enqueued_backups().len(), 1);
        s.assert_stopped_without_execution("after the reclaimed upgrade")
            .await?;
    }
    Ok(())
}

/// A retry that fails to record the earlier attempt's upgrade again ends
/// its claim and keeps the execution and pod, with what it recorded, for the
/// next retry, and places and reads nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_resend_failing_to_record_keeps_the_execution() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    s.end_the_lease().await?;
    let recorded = s.recorded_upgrade().await?;
    let creates = s.pods().creates.load(Ordering::SeqCst);
    let reads = s.cluster.worker.received(GET_RUNTIME_UPGRADE).len();
    // The store refuses to record the upgrade again.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_runtime_upgrade_record() RETURNS trigger AS $$
         BEGIN RAISE EXCEPTION 'recording refused'; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER refuse_runtime_upgrade_record
             BEFORE UPDATE OF runtime_upgrade_authorization_id ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION refuse_runtime_upgrade_record();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(error.to_string().contains("record"), "{error:?}");
    assert_eq!(
        s.active_execution()
            .await?
            .map(|(_, target, live)| (target, live)),
        Some((Some("stopped".to_string()), false))
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    // Nothing new was placed or read.
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), creates);
    assert_eq!(s.cluster.worker.received(GET_RUNTIME_UPGRADE).len(), reads);
    assert!(s.pods().exists());
    Ok(())
}

/// The claim targeting `stopped` and the upgrade's record commit together or
/// not at all. Here the record fails and the attempt ends without ending its
/// claim, as one whose process exits right after the claim would: nothing of
/// the claim is left, which a retry would otherwise refuse as another
/// activation's for good once its lease ran out. The next attempt claims the
/// execution afresh and converges in one call.
#[tokio::test(flavor = "current_thread")]
async fn an_attempt_ending_at_its_claim_leaves_no_unrecorded_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    // The record fails, and so does anything that would end an attempt's
    // claim afterwards.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_runtime_upgrade_record() RETURNS trigger AS $$
         BEGIN RAISE EXCEPTION 'recording refused'; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER refuse_runtime_upgrade_record
             BEFORE UPDATE OF runtime_upgrade_authorization_id ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION refuse_runtime_upgrade_record();
         CREATE FUNCTION keep_activation_claims() RETURNS trigger AS $$
         BEGIN
             IF OLD.activation_attempt_id IS NOT NULL
                AND NEW.activation_attempt_id IS DISTINCT FROM OLD.activation_attempt_id
             THEN
                 RETURN NULL;
             END IF;
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER keep_activation_claims
             BEFORE UPDATE ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION keep_activation_claims();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(error.to_string().contains("record"), "{error:?}");
    assert!(left_by(&s).await.is_err(), "a claim was left unrecorded");
    assert_eq!(s.active_execution().await?, None);
    s.assert_nothing_placed_or_sent("refused");

    sqlx::raw_sql(sql!(
        "DROP TRIGGER refuse_runtime_upgrade_record ON sandbox_k8s_executions;
         DROP TRIGGER keep_activation_claims ON sandbox_k8s_executions;"
    ))
    .execute(&s.db.pool)
    .await?;
    // Any claim left would have run out its lease by now.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    upgraded(&s.upgrade().await?);
    s.assert_stopped_without_execution("after the retry").await
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_in_flight_keeps_its_execution() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "started", 300).await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    assert!(s.pods().exists());
    assert_eq!(
        s.active_execution().await?,
        Some((7, Some("started".to_string()), true))
    );
    Ok(())
}

/// A start that crashed leaves an expired claim targeting `started`, maybe
/// over a running guest. It isn't an upgrade's leftover, so when the decision
/// keeps the sandbox's environment, nothing claims it: it stays for the
/// start's own recovery.
#[tokio::test(flavor = "current_thread")]
async fn a_crashed_start_is_left_alone_when_the_decision_keeps_its_environment()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "started", -3600).await?;
    s.publish_upgraded().await?;

    assert_eq!(s.upgrade().await?, StoppedRuntimeUpgrade::AsRecorded);

    assert!(s.pods().exists());
    assert_eq!(
        s.active_execution().await?,
        Some((7, Some("started".to_string()), false))
    );
    Ok(())
}

/// The disk node is checked before the claim, and a drain can move the disk
/// in between while the row stays `stopped`: it releases the node (the
/// evacuation marker set, the placement cleared) or finishes re-placing the
/// sandbox on another node. The claim checks the disk node again under the
/// sandbox row's lock, so either answers a retryable 503 with nothing claimed
/// or placed, rather than a pod pinned to the old node's released or stale
/// copy. Here the change lands inside the claim's transaction, after its lock
/// and before the record, and rolls back with the refused claim; a retry once
/// nothing moves converges.
#[rstest::rstest]
#[case::released(true)]
#[case::re_placed(false)]
#[tokio::test(flavor = "current_thread")]
async fn a_disk_that_moves_before_the_claim_is_refused_for_now(
    #[case] released: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let node_b = pplx_space_k8s_node_id::space_k8s_node_id("node-b", "worker");
    pplx_space_db_test_fixtures::insert_ready_worker_node(&s.db.pool, node_b).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'node-b')"
    ))
    .bind(node_b)
    .execute(&s.db.pool)
    .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name
            .as_deref(),
        Some(WORKER_NODE)
    );
    // The drain's write, run by the claim's own execution insert.
    let drain = if released {
        sql!(
            "CREATE FUNCTION drain_moves_the_disk() RETURNS trigger AS $$
             BEGIN
                 UPDATE sandboxes
                    SET evacuation_restore_target = 'stopped', active_node_id = NULL
                  WHERE id = NEW.sandbox_id;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql;
             CREATE TRIGGER drain_moves_the_disk
                 AFTER INSERT ON sandbox_k8s_executions
                 FOR EACH ROW EXECUTE FUNCTION drain_moves_the_disk();"
        )
    } else {
        sql!(
            "CREATE FUNCTION drain_moves_the_disk() RETURNS trigger AS $$
             BEGIN
                 UPDATE sandboxes
                    SET active_node_id =
                        (SELECT node_id FROM node_k8s WHERE k8s_node_name = 'node-b')
                  WHERE id = NEW.sandbox_id;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql;
             CREATE TRIGGER drain_moves_the_disk
                 AFTER INSERT ON sandbox_k8s_executions
                 FOR EACH ROW EXECUTE FUNCTION drain_moves_the_disk();"
        )
    };
    sqlx::raw_sql(drain).execute(&s.db.pool).await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    // A release leaves the evacuation's marker, which refuses first.
    let reason = if released { "evacuating" } else { "disk moved" };
    assert!(message.contains(reason), "{message}");
    s.assert_nothing_placed_or_sent("refused");
    assert_eq!(s.active_execution().await?, None);

    sqlx::raw_sql(sql!(
        "DROP TRIGGER drain_moves_the_disk ON sandbox_k8s_executions"
    ))
    .execute(&s.db.pool)
    .await?;
    upgraded(&s.upgrade().await?);
    s.assert_stopped_without_execution("after the retry").await
}

/// A start that moved the sandbox on owns the execution an earlier upgrade
/// left: the takeover, which checks the lifecycle under the sandbox's lock,
/// refuses it with a retryable error and leaves its attempt alone.
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_refuses_a_sandbox_a_start_moved_on() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let before = s.active_execution().await?;

    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            left_by(&s).await?,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(s.active_execution().await?, before);
    Ok(())
}

/// A drain's stopped evacuation that opened on the sandbox, its marker set
/// while the disk is still on its node, refuses the upgrade retryably under
/// the sandbox's lock, both a new claim and a takeover of the execution an
/// earlier attempt left: a boot after the evacuation minted its backup would
/// drop the node's pin on it, and the teardown would never release the node.
/// Nothing is claimed, placed or changed; the same upgrade succeeds once the
/// marker is gone.
#[tokio::test(flavor = "current_thread")]
async fn an_open_stopped_evacuation_refuses_the_upgrade() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let mark = |marker: Option<&'static str>| {
        sqlx::query(sql!(
            "UPDATE sandboxes SET evacuation_restore_target = $2 WHERE id = $1"
        ))
        .bind(s.sandbox_id)
        .bind(marker)
        .execute(&s.db.pool)
    };
    mark(Some("stopped")).await?;

    let error = s.upgrade().await.unwrap_err();
    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("evacuating"), "{message}");
    s.assert_nothing_placed_or_sent("refused");
    assert_eq!(s.active_execution().await?, None);

    held_execution(&s, "stopped", -3600).await?;
    record_upgrade_on_held_execution(&s).await?;
    let before = s.active_execution().await?;
    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            left_by(&s).await?,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();
    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("evacuating"), "{message}");
    assert_eq!(s.active_execution().await?, before);

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    *s.pods().pod.lock().unwrap() = None;
    mark(None).await?;
    upgraded(&s.upgrade().await?);
    Ok(())
}

/// An attempt that replaced the claim the retry read owns the execution, and
/// so does a replacement execution that carried the read attempt to a new
/// generation: the takeover, which checks the attempt and the generation
/// under the sandbox's lock, refuses either with a retryable error and leaves
/// the execution alone.
#[rstest::rstest]
#[case::another_attempt(
    "UPDATE sandbox_k8s_executions SET activation_attempt_id = gen_random_uuid()
      WHERE sandbox_id = $1 AND active"
)]
#[case::another_generation(
    "UPDATE sandbox_k8s_executions SET generation = 8 WHERE sandbox_id = $1 AND active"
)]
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_refuses_a_claim_another_attempt_replaced(
    #[case] replace: &str,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let read = left_by(&s).await?;
    sqlx::query(sqlx::AssertSqlSafe(sql!(replace)))
        .bind(s.sandbox_id)
        .execute(&s.db.pool)
        .await?;
    let before = s.active_execution().await?;
    let replacement = left_by(&s).await?;

    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            read,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(left_by(&s).await?, replacement);
    Ok(())
}

/// The recorded upgrade is what makes a `stopped` claim the upgrade's. Only
/// the upgrade claims that target on a `stopped` row, and it keeps the record
/// across its own claims. Any other activation that claims the execution
/// clears it: a start, including one that reuses an execution without a pod,
/// and a drain's re-placement, which claims `stopped` on a `restoring` row.
#[rstest::rstest]
#[case::the_upgrade(SandboxTransitionTargetState::Stopped, "stopped", true)]
#[case::a_start(SandboxTransitionTargetState::Started, "stopped", false)]
#[case::a_re_placement(SandboxTransitionTargetState::Stopped, "restoring", false)]
#[tokio::test(flavor = "current_thread")]
async fn only_the_upgrades_claim_keeps_its_recorded_upgrade(
    #[case] target: SandboxTransitionTargetState,
    #[case] lifecycle: &str,
    #[case] kept: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    // The execution never got its pod.
    *s.pods().pod.lock().unwrap() = None;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = NULL WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(s.sandbox_id)
        .bind(lifecycle)
        .execute(&s.db.pool)
        .await?;
    let recorded = s.recorded_upgrade().await?;
    let claim = |owed_backup_enqueued| {
        s.substrate
            .store
            .ensure_active_execution_claim_for_lifecycle(
                s.sandbox_id,
                lifecycle.parse().expect("a lifecycle"),
                None,
                Some((
                    pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                    target,
                )),
                None,
                owed_backup_enqueued,
            )
    };

    if kept {
        claim(None).await?;
        assert_eq!(s.recorded_upgrade().await?, recorded);
        return Ok(());
    }
    // Another activation clears the record only once the backup the upgrade
    // owes is enqueued for its operation.
    let error = claim(None).await.map(drop).unwrap_err();
    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    claim(Some(operation)).await?;
    assert_eq!(s.recorded_upgrade().await?, (None, None, None));
    Ok(())
}

/// A drain's re-placement moving the row to `restoring` while the upgrade
/// claims the execution and records itself either waits for the claim or
/// makes it fail, record and all: here the drain's write is uncommitted when
/// the claim runs, and commits before the claim could. That holds for the
/// first claim and for a replacement generation's. A record that read the
/// lifecycle without locking the row would see `stopped` and succeed, and
/// the drain, which claims after its write commits, would join an attempt
/// whose record it never saw.
#[rstest::rstest]
#[case::first_claim(false)]
#[case::replacement(true)]
#[tokio::test(flavor = "current_thread")]
async fn a_claim_racing_a_drains_restoring_write_fails(
    #[case] replacement: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let before = s.active_execution().await?;
    let mut drain = s.db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&mut *drain)
    .await?;
    let upgrade = super::super::store::StoppedUpgradeRecord {
        upgrade: super::super::store::RecordedRuntimeUpgrade {
            operation_id: uuid::Uuid::now_v7(),
            target: E1.parse()?,
            authorization_id: format!("runtime-environment-blocked:{E0}"),
        },
        disk_node: WORKER_NODE.to_string(),
    };
    let attempt = Some((
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        SandboxTransitionTargetState::Stopped,
    ));
    let store = &s.substrate.store;
    let claim = async {
        if replacement {
            store
                .replace_active_execution_claim_if_generation(
                    s.sandbox_id,
                    7,
                    "replaced",
                    attempt,
                    Some(&upgrade),
                )
                .await
                .map(drop)
        } else {
            store
                .ensure_active_execution_claim_for_lifecycle(
                    s.sandbox_id,
                    pplx_space_common_core::types::SandboxLifecycle::Stopped,
                    None,
                    attempt,
                    Some(&upgrade),
                    None,
                )
                .await
                .map(drop)
        }
    };
    tokio::pin!(claim);
    // Give the claim its chance to run against the uncommitted write.
    let early = tokio::time::timeout(Duration::from_millis(200), &mut claim).await;

    drain.commit().await?;
    let claimed = match early {
        Ok(claimed) => claimed,
        Err(_blocked) => claim.await,
    };

    assert!(
        claimed.is_err(),
        "the claim recorded the upgrade under a drain's restoring row"
    );
    // Rolled back whole: no replacement, no claim, nothing recorded.
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(s.recorded_upgrade().await?.0, None);
    Ok(())
}

/// The claim checks the disk pin again under the sandbox row's lock: a pin
/// that stopped naming the checked node since the precheck (here its node's
/// Kubernetes registration is gone, while execution history still names it)
/// refuses the claim retryably, and nothing is claimed or recorded.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_whose_disk_pin_moved_since_the_check_fails() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let before = s.active_execution().await?;
    sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
        .bind(s.node_id)
        .execute(&s.db.pool)
        .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name
            .as_deref(),
        Some(WORKER_NODE),
        "only the pin should have moved"
    );
    let upgrade = super::super::store::StoppedUpgradeRecord {
        upgrade: super::super::store::RecordedRuntimeUpgrade {
            operation_id: uuid::Uuid::now_v7(),
            target: E1.parse()?,
            authorization_id: format!("runtime-environment-blocked:{E0}"),
        },
        disk_node: WORKER_NODE.to_string(),
    };

    let claimed = s
        .substrate
        .store
        .ensure_active_execution_claim_for_lifecycle(
            s.sandbox_id,
            pplx_space_common_core::types::SandboxLifecycle::Stopped,
            None,
            Some((
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Stopped,
            )),
            Some(&upgrade),
            None,
        )
        .await;

    let Err(SubstrateError::Unavailable { message }) = &claimed else {
        panic!("{:?}", claimed.map(drop));
    };
    assert!(message.contains("moved off node"), "{message}");
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(s.recorded_upgrade().await?.0, None);
    Ok(())
}

/// An execution whose claim expired still carries an earlier upgrade's
/// record, which may owe a disk backup. A claim recording a different
/// operation never overwrites it: the claim fails retryably, claims nothing,
/// and the earlier record stays for its own retry to take over.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_never_overwrites_another_operations_record() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let earlier = record_upgrade_on_held_execution(&s).await?;
    let recorded = s.recorded_upgrade().await?;
    let upgrade = super::super::store::StoppedUpgradeRecord {
        upgrade: super::super::store::RecordedRuntimeUpgrade {
            operation_id: uuid::Uuid::now_v7(),
            target: E1.parse()?,
            authorization_id: format!("runtime-environment-blocked:{E0}"),
        },
        disk_node: WORKER_NODE.to_string(),
    };

    let claimed = s
        .substrate
        .store
        .ensure_active_execution_claim_for_lifecycle(
            s.sandbox_id,
            pplx_space_common_core::types::SandboxLifecycle::Stopped,
            None,
            Some((
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Stopped,
            )),
            Some(&upgrade),
            None,
        )
        .await;

    assert!(
        matches!(&claimed, Err(SubstrateError::Unavailable { .. })),
        "{:?}",
        claimed.map(drop)
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    assert_eq!(recorded.0, Some(earlier));
    Ok(())
}

/// A drain's re-placement that finds a runtime upgrade's live claim never
/// joins it, though both target `stopped`: the upgrade's pod is a
/// placeholder booting its upgrade, not the re-placement, and joining would
/// release the upgrade's claim as the drain's own. It answers a retryable
/// refusal and leaves the claim alone.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placement_never_joins_a_runtime_upgrades_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;
    record_upgrade_on_held_execution(&s).await?;
    // The shape a stopped evacuation's teardown leaves for the drain's
    // re-placement.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', placement = 'unplaced',
                active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(
        &s.db.pool,
        s.sandbox_id,
        pplx_space_common_core::types::LocalArtifactId::new(),
        "s3://space-test/evacuated-disk?region=us-east-1",
    )
    .await?;
    let before = (s.active_execution().await?, left_by(&s).await?);

    let error = s
        .substrate
        .restore_execution_inner(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("node_drain.evacuate"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::kubernetes::restore_intent::RestoreIntent::drain(
                None,
                pplx_space_common_core::types::SandboxLifecycle::Stopped,
            ),
        )
        .await
        .unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("runtime upgrade owns"), "{message}");
    assert_eq!((s.active_execution().await?, left_by(&s).await?), before);
    assert!(s.node().wakes.lock().unwrap().is_empty());
    Ok(())
}

/// A retry that took over an earlier attempt's execution ends that claim
/// however the read after the takeover goes: a claim left live would refuse
/// starts and retries until its lease ran out. Here that read finds nothing
/// recorded, so the claim is given up rather than expired with its target.
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_whose_read_finds_no_upgrade_gives_its_claim_up() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    record_upgrade_on_held_execution(&s).await?;
    // Only an update that assigns another attempt, the takeover's, clears
    // the record, so only the read after the takeover misses it.
    sqlx::query(sql!(
        "CREATE FUNCTION clear_upgrade_record() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.activation_attempt_id IS DISTINCT FROM OLD.activation_attempt_id THEN
                 NEW.runtime_upgrade_operation_id := NULL;
                 NEW.runtime_upgrade_target_environment_ref := NULL;
                 NEW.runtime_upgrade_authorization_id := NULL;
             END IF;
             RETURN NEW;
         END $$"
    ))
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER clear_upgrade_record BEFORE UPDATE ON sandbox_k8s_executions
         FOR EACH ROW EXECUTE FUNCTION clear_upgrade_record()"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("takeover"), "{message}");
    assert_eq!(s.active_execution().await?, Some((7, None, false)));
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// Before a drain checks the node for a fresh stopped evacuation, it settles
/// a stay-stopped upgrade whose lease ended: a boot the upgrade verified but
/// never stopped would leave the node reporting the sandbox running, and the
/// node check would defer on it forever. The settle reserves the expired
/// claim, deletes the pod (deferring until it's gone) and abandons the
/// operation on the node, which stops a verified boot; then it ends the
/// reservation's lease again, leaving the execution and its record for the
/// evacuation to take over. A claim whose lease still runs is left alone.
#[tokio::test(flavor = "current_thread")]
async fn a_drain_settles_an_expired_upgrade_before_it_checks_the_node() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    let (operation, _, _) = s.recorded_upgrade().await?;
    let operation = operation.expect("the recorded upgrade");

    assert!(
        s.substrate
            .settle_expired_stopped_upgrade(s.sandbox_id)
            .await?,
        "a live lease isn't the drain's to settle"
    );
    assert!(s.node().abandons().is_empty());

    s.end_the_lease().await?;
    assert!(
        !s.substrate
            .settle_expired_stopped_upgrade(s.sandbox_id)
            .await?,
        "the drain went on while the upgrade's pod was still there"
    );
    assert!(!s.pods().exists(), "the upgrade's pod wasn't deleted");
    assert!(
        s.node().abandons().is_empty(),
        "abandoned before the pod was gone"
    );
    s.cache_the_pod();

    assert!(
        s.substrate
            .settle_expired_stopped_upgrade(s.sandbox_id)
            .await?
    );
    assert_eq!(s.node().abandons(), [(operation.to_string(), true)]);
    let (_, target, live) = s
        .active_execution()
        .await?
        .expect("the upgrade's execution");
    assert_eq!(target.as_deref(), Some("stopped"));
    assert!(!live, "the settle kept its reservation");
    assert_eq!(s.recorded_upgrade().await?.0, Some(operation));
    Ok(())
}
