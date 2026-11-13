//! What the upgrade refuses before it places anything: a sandbox it doesn't
//! apply to, one whose disk isn't on a node that can take the upgrade, and an
//! encrypted one.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_on_an_allowed_environment_is_left_alone() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.publish_upgraded().await?;

    assert_eq!(s.upgrade().await?, StoppedRuntimeUpgrade::AsRecorded);

    s.assert_nothing_placed_or_sent("refused");
    s.assert_stopped_without_execution("as recorded").await
}

#[tokio::test(flavor = "current_thread")]
async fn a_stranded_sandbox_is_refused_before_anything_is_placed() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = '{}' WHERE environment_ref = $1"
    ))
    .bind(E0)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::FailedPrecondition { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    s.assert_stopped_without_execution("stranded").await
}

/// Only a stopped sandbox is upgraded without a start; any other is refused
/// for good (409). A drain's re-placement, which holds a stopped sandbox
/// `restoring` while it moves the disk and leaves it `stopped` again, is
/// refused retryably (503), as a disk still moving off a draining node is. A
/// user restore's `restoring` ends started.
#[rstest::rstest]
#[case::started("started", false)]
#[case::suspended("suspended", false)]
#[case::paused("paused", false)]
#[case::restored_by_a_user("restoring", false)]
#[case::re_placed_by_a_drain("restoring", true)]
#[tokio::test(flavor = "current_thread")]
async fn only_a_stopped_sandbox_is_upgraded_without_a_start(
    #[case] lifecycle: &str,
    #[case] re_placement: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $2,
                evacuation_restore_target = CASE WHEN $3 THEN 'stopped' END
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .bind(lifecycle)
    .bind(re_placement)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    if re_placement {
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
    } else {
        assert!(
            matches!(error, SubstrateError::Conflict { .. }),
            "{error:?}"
        );
    }
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_is_on_no_node_is_refused() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = NULL, target_node_id = NULL WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = NULL WHERE sandbox_id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    // Evacuated: the disk is durable in object storage and on no node.
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(
        &s.db.pool,
        s.sandbox_id,
        pplx_space_common_core::types::LocalArtifactId::new(),
        "s3://space-test/evacuated-disk?region=us-east-1",
    )
    .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name,
        None
    );

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::FailedPrecondition { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// The node the disk is named on has to be a registered node able to take
/// work: a pod pinned to a node that's gone, or can't run it, never runs, and
/// no other node holds the disk. Refused before anything is placed: for good
/// (412) when the node is gone or outside the sandbox node selector, which no
/// retry changes, and retryably (503) when it can't take work now.
#[rstest::rstest]
#[case::gone("gone", true)]
#[case::outside_the_selector("outside the selector", true)]
#[case::not_ready("not ready", false)]
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_node_cant_take_work_is_refused(
    #[case] node: &str,
    #[case] for_good: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    let mut worker = worker_node();
    match node {
        "outside the selector" => worker.metadata.labels = None,
        "not ready" => {
            if let Some(conditions) = worker
                .status
                .as_mut()
                .and_then(|status| status.conditions.as_mut())
            {
                conditions[0].status = "False".to_string();
            }
        }
        _ => {}
    }
    let nodes = if node == "gone" { vec![] } else { vec![worker] };
    s.substrate = s.cluster.substrate_with_nodes(&s.db.pool, nodes);

    let error = s.upgrade().await.unwrap_err();

    if for_good {
        assert!(
            matches!(error, SubstrateError::FailedPrecondition { .. }),
            "{error:?}"
        );
    } else {
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
    }
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// An encrypted sandbox's stopped disk has no artifact the platform can use,
/// so the upgrade's owed backup could never be a real one. It's refused for
/// good (412) before anything is claimed or placed.
#[tokio::test(flavor = "current_thread")]
async fn an_encrypted_sandbox_is_refused_before_anything_is_placed() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET volume_encryption = TRUE WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let before = s.active_execution().await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::FailedPrecondition { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("encrypted"), "{message}");
    assert_eq!(s.active_execution().await?, before);
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// The upgrade's pod goes only where the placement pins the disk to a
/// registered node, the one node an attempt that ends before persisting its
/// pod can still be backed up on. A disk node named only by execution
/// history, an unregistered placement node, or a re-placement's chosen
/// target is refused for good (412) before anything is claimed or placed.
#[rstest::rstest]
#[case::unregistered("unregistered")]
#[case::history_only("history only")]
#[case::replacing("replacing")]
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_node_isnt_its_placements_pin_is_refused(
    #[case] placement: &str,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let pool = &s.db.pool;
    match placement {
        "unregistered" => {
            sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
                .bind(s.node_id)
                .execute(pool)
                .await?;
        }
        "history only" => {
            sqlx::query(sql!(
                "UPDATE sandboxes SET active_node_id = NULL WHERE id = $1"
            ))
            .bind(s.sandbox_id)
            .execute(pool)
            .await?;
        }
        _ => {
            let target = pplx_space_k8s_node_id::space_k8s_node_id("node-target", "worker");
            pplx_space_db_test_fixtures::insert_ready_worker_node(pool, target).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
            ))
            .bind(s.sandbox_id)
            .bind(target)
            .execute(pool)
            .await?;
        }
    }
    let before = s.active_execution().await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::FailedPrecondition { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("pins the disk to"), "{message}");
    assert_eq!(s.active_execution().await?, before);
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// A retry that will reuse the pod an earlier attempt left on the disk's
/// node, the one carrying the recorded operation, needs no new network slot,
/// so a node full only with that pod's own isn't refused: the retry reuses
/// the pod and converges. A pod that doesn't carry the operation is replaced,
/// which takes a slot the full node doesn't have, so that retry is refused,
/// retryably, before anything is placed.
#[rstest::rstest]
#[case::carrying_the_operation(true)]
#[case::without_the_operation(false)]
#[tokio::test(flavor = "current_thread")]
async fn a_retry_reusing_its_pod_on_a_full_node_needs_no_new_slot(
    #[case] carried: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    if carried {
        s.pods().annotate(&[
            (
                SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
                &operation.to_string(),
            ),
            (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, "stopped"),
        ]);
    }
    s.pods().request_network_slots(1);
    s.cache_the_pod_with_network_slots(1);

    let outcome = s.upgrade().await;

    if carried {
        assert_eq!(upgraded(&outcome?), operation);
        assert_eq!(s.pods().creates.load(Ordering::SeqCst), 0);
        s.assert_stopped_without_execution("after the upgrade")
            .await
    } else {
        let error = outcome.unwrap_err();
        let SubstrateError::Unavailable { message } = &error else {
            panic!("{error:?}");
        };
        assert!(message.contains("network slot"), "{message}");
        s.assert_nothing_placed_or_sent("refused");
        assert!(s.pods().exists());
        Ok(())
    }
}

/// A drain is moving the stopped sandbox's disk off its node: a retryable
/// refusal until the disk is placed again, and nothing is placed meanwhile.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_moving_off_a_draining_node_is_refused_for_now() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}
