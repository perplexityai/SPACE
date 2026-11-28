//! Regression coverage for `sandbox_force_restarts` stop ownership across
//! `node_drain` stopped-disk evacuations, stopped re-placements, and implicit
//! wakes (`sessions`/`exec`/`pty`/port-exposure/files) that emit no
//! `public_operations` row.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType, PublicSandboxId};
use pplx_space_sandbox_model::{SandboxId, SandboxTransitionReceiptKind};

use super::*;

pub(super) async fn drain_stopped_sandbox(
    pool: &PgPool,
    sandbox: Uuid,
    replacement_node: Option<NodeId>,
    artifact_id: Uuid,
    snapshot_metadata: Option<serde_json::Value>,
    re_placed_at: time::OffsetDateTime,
) -> TestResult<()> {
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, org_id, type, state, local_artifact_id,
             artifact_kind, artifact_origin, runtime, runtime_version,
             state_format_name, state_format_version, execution_metadata, timestamp
         )
         VALUES (
             $1, $2, $3, 'disk', 'ready', $4, 'disk', 'stop',
             'firecracker', NULL, 'firecracker-snapshot', NULL, $5,
             GREATEST(
                 NOW(),
                 COALESCE(
                     (SELECT MAX(terminated_at) + INTERVAL '1 microsecond'
                        FROM sandbox_k8s_executions
                       WHERE sandbox_id = $2),
                     '-infinity'::timestamptz
                 )
             )
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(SEED_ORG_ID)
    .bind(artifact_id)
    .bind(snapshot_metadata)
    .execute(pool)
    .await?;

    // Matches `node_drain` -> `retire_executions_predating_current_stop`:
    // happens after the durable `'disk'`/`'stop'` backup row is recorded.
    assert!(
        pplx_space_sandbox_store::k8s_executions::retire::finish_active(
            pool,
            SandboxId::from_uuid(sandbox),
            "stopped",
        )
        .await?
    );

    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;

    if let Some(node_id) = replacement_node {
        let next_gen: i64 = sqlx::query_scalar(sql!(
            "SELECT COALESCE(MAX(generation), 0) + 1
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox)
        .fetch_one(pool)
        .await?;
        let re_placed_pod_uid = format!("pod-replaced-{sandbox}-{next_gen}");
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                 pod_ip, state, active, route_activation_id
             )
             VALUES ($1, $2, $3, 'space', $4, '10.0.0.20'::inet, 'running', TRUE, $5)"
        ))
        .bind(sandbox)
        .bind(next_gen)
        .bind(format!("sbx-{sandbox}-g{next_gen}"))
        .bind(re_placed_pod_uid)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
             SET placement = 'on_node',
                 active_node_id = $2,
                 evacuation_restore_target = NULL,
                 current_local_artifact_id = $3,
                 current_local_artifact_kind = 'disk',
                 current_local_artifact_origin = 'stop',
                 lifecycle_changed_at = $4
             WHERE id = $1"
        ))
        .bind(sandbox)
        .bind(node_id)
        .bind(artifact_id)
        .bind(re_placed_at)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(sql!(
            "UPDATE sandboxes
             SET placement = 'off_node',
                 active_node_id = NULL,
                 current_local_artifact_id = $2,
                 current_local_artifact_kind = 'disk',
                 current_local_artifact_origin = 'stop',
                 lifecycle_changed_at = $3
             WHERE id = $1"
        ))
        .bind(sandbox)
        .bind(artifact_id)
        .bind(re_placed_at)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn implicit_wake_via_activation_receipt(
    pool: &PgPool,
    node_id: NodeId,
    sandbox: Uuid,
) -> TestResult<()> {
    let generation: i64 = sqlx::query_scalar(sql!(
        "SELECT generation
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    let attempt_id = Uuid::now_v7();
    let route_id = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'running',
                pod_ip = '10.0.0.30'::inet,
                activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = 'started',
                route_activation_id = $3,
                updated_at = NOW()
          WHERE sandbox_id = $1 AND generation = $4 AND active"
    ))
    .bind(sandbox)
    .bind(attempt_id)
    .bind(route_id)
    .bind(generation)
    .execute(pool)
    .await?;

    let rows = serde_json::json!([{
        "sandbox_id": sandbox,
        "published_lifecycle": "started",
        "transition_receipt_id": Uuid::now_v7(),
        "transition_receipt_kind": SandboxTransitionReceiptKind::ActivationCompleted.as_str(),
        "transition_receipt_target_state": "started",
        "transition_receipt_execution_generation": generation,
        "transition_receipt_activation_attempt_id": attempt_id,
    }]);
    let released =
        pplx_space_sandbox_store::ingest::release_completed_activations(pool, node_id, rows)
            .await?;
    assert_eq!(released, vec![SandboxId::from_uuid(sandbox)]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn node_drain_evacuated_and_replaced_stopped_sandbox_finishes_cold_boot_unless_user_activated()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let drained_node = node(&db.pool).await?;
    let replacement_node = node(&db.pool).await?;
    let third_node = node(&db.pool).await?;

    let evacuated_off_node =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;
    let replaced_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;
    let multi_hop_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;
    let user_powered_off_then_drained =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;

    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (evacuated_off_node, replacement_node),
        (replaced_stopped, replacement_node),
        (multi_hop_stopped, third_node),
        (user_powered_off_then_drained, replacement_node),
    ]))
    .persisting(db.pool.clone());
    for id in [
        evacuated_off_node,
        replaced_stopped,
        multi_hop_stopped,
        user_powered_off_then_drained,
    ] {
        dispatcher.script_start(
            id,
            vec![Err(DispatchError::Retryable("node draining".to_owned()))],
        );
    }
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    tick(&reconciler).await;
    assert_eq!(counts(&db.pool, campaign).await?.pending, 4);

    let source_meta = Some(serde_json::json!({ "environment": source().to_string() }));
    for (sandbox, dest_node, user_activated, snapshot_metadata) in [
        (evacuated_off_node, None, false, None),
        (
            replaced_stopped,
            Some(replacement_node),
            false,
            source_meta.clone(),
        ),
        (
            multi_hop_stopped,
            Some(replacement_node),
            false,
            source_meta.clone(),
        ),
        (
            user_powered_off_then_drained,
            Some(replacement_node),
            true,
            source_meta.clone(),
        ),
    ] {
        let stop_fence = dispatcher
            .recorded_stop(campaign, sandbox)
            .and_then(RecordedStop::into_fence)
            .expect("stop recorded");
        if user_activated {
            let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
            let activated_at =
                to_offset_date_time(stop_fence.stopped_at) + time::Duration::milliseconds(50);
            sqlx::query(sql!(
                "INSERT INTO public_operations (
                     operation_id, org_id, actor_user_id, kind, target_type, target_id,
                     state, requested_at, completed_at, gc_after
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, NOW() - INTERVAL '1 hour')"
            ))
            .bind(Uuid::now_v7())
            .bind(SEED_ORG_ID)
            .bind(SEED_USER_ID)
            .bind(OperationKind::StartSandbox.as_str())
            .bind(OperationTargetType::Sandbox.as_str())
            .bind(target_id)
            .bind(OperationState::Succeeded.as_str())
            .bind(activated_at)
            .execute(&db.pool)
            .await?;
            set_lifecycle(&db.pool, sandbox, SandboxLifecycle::Started.as_str()).await?;
            let due_started =
                pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
            pplx_space_public_operation_store::operations::delete_by_ids(&db.pool, &due_started)
                .await?;
            set_lifecycle(&db.pool, sandbox, SandboxLifecycle::Stopped.as_str()).await?;
            let due_unpinned =
                pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
            pplx_space_public_operation_store::operations::delete_by_ids(&db.pool, &due_unpinned)
                .await?;
        }
        let artifact_id = Uuid::now_v7();
        let changed_at = to_offset_date_time(stop_fence.stopped_at)
            + if dest_node.is_some() {
                time::Duration::milliseconds(250)
            } else {
                time::Duration::ZERO
            };
        drain_stopped_sandbox(
            &db.pool,
            sandbox,
            dest_node,
            artifact_id,
            snapshot_metadata,
            changed_at,
        )
        .await?;
        if sandbox == multi_hop_stopped {
            // Second uninterrupted stopped drain re-registers the same
            // `artifact_id` at a newer snapshot timestamp after `G0` retired.
            drain_stopped_sandbox(
                &db.pool,
                sandbox,
                Some(third_node),
                artifact_id,
                source_meta.clone(),
                changed_at + time::Duration::milliseconds(250),
            )
            .await?;
        }
    }
    let due_pinned =
        pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
    pplx_space_public_operation_store::operations::delete_by_ids(&db.pool, &due_pinned).await?;

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    for (upgraded, expected_node) in [
        (evacuated_off_node, replacement_node),
        (replaced_stopped, replacement_node),
        (multi_hop_stopped, third_node),
    ] {
        assert_eq!(item(&db.pool, campaign, upgraded).await?.0, "done");
        let (lifecycle, artifact_kind, active_node_id, observed_node_id): (
            String,
            Option<String>,
            Option<NodeId>,
            NodeId,
        ) = sqlx::query_as(sql!(
            "SELECT s.lifecycle,
                    s.current_local_artifact_kind,
                    s.active_node_id,
                    o.node_id
             FROM sandboxes AS s
             JOIN sandbox_observed_states AS o ON o.sandbox_id = s.id
             WHERE s.id = $1"
        ))
        .bind(upgraded)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "started");
        assert_eq!(artifact_kind, None);
        assert_eq!(active_node_id, Some(expected_node));
        assert_eq!(observed_node_id, expected_node);
    }
    assert_eq!(
        item(&db.pool, campaign, user_powered_off_then_drained)
            .await?
            .0,
        "skipped"
    );
    assert_eq!(
        dispatcher.recorded_stopped_at(campaign, user_powered_off_then_drained),
        None
    );
    assert_eq!(
        dispatcher
            .calls()
            .iter()
            .filter(|&call| *call == Call::Start(user_powered_off_then_drained))
            .count(),
        1
    );
    let still_stopped: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(user_powered_off_then_drained)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(still_stopped, "stopped");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn implicit_wake_and_guest_stop_across_stopped_node_drain_skips_instead_of_cold_booting()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let drained_node = node(&db.pool).await?;
    let replacement_node = node(&db.pool).await?;
    let third_node = node(&db.pool).await?;

    // All four sandboxes experience an implicit wake (`sessions`/`exec`/`pty`)
    // with ZERO `public_operations` rows, then guest power-off to `'stopped'`:
    // 1. `seq_a_higher_gen`: implicit wake replaces `G0` with `G1` (`replaced`),
    //    guest stops, then `node_drain` pins & re-places the stopped disk.
    // 2. `seq_a_reused_gen_new_pod`: implicit wake materializes a new `pod_uid`
    //    on `G0`'s generation, guest stops, then `node_drain` pins & re-places.
    // 3. `seq_b_wake_after_drain`: `node_drain` pins & re-places `G0`'s disk
    //    onto `replacement_node`, then `release_completed_activations` wakes it
    //    and the guest stops without a second drain.
    // 4. `seq_b_wake_then_second_drain`: same as (3), followed by a second
    //    `node_drain` that snapshots and pins a newer disk `A_2` after `G0`
    //    was already retired.
    let seq_a_higher_gen =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;
    let seq_a_reused_gen_new_pod =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;
    let seq_b_wake_after_drain =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;
    let seq_b_wake_then_second_drain =
        resident_on_source(&db.pool, SandboxLifecycle::Started, drained_node).await?;

    let all = [
        seq_a_higher_gen,
        seq_a_reused_gen_new_pod,
        seq_b_wake_after_drain,
        seq_b_wake_then_second_drain,
    ];
    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (seq_a_higher_gen, replacement_node),
        (seq_a_reused_gen_new_pod, replacement_node),
        (seq_b_wake_after_drain, replacement_node),
        (seq_b_wake_then_second_drain, third_node),
    ]))
    .persisting(db.pool.clone());
    for id in all {
        dispatcher.script_start(
            id,
            vec![Err(DispatchError::Retryable("node draining".to_owned()))],
        );
    }
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    tick(&reconciler).await;
    assert_eq!(counts(&db.pool, campaign).await?.pending, 4);

    let source_meta = Some(serde_json::json!({ "environment": source().to_string() }));

    // 3 & 4. `seq_b_wake_after_drain` and `seq_b_wake_then_second_drain`:
    //        `node_drain` evacuates & re-places `G0`'s stop disk onto
    //        `replacement_node`, then implicit wake runs
    //        `release_completed_activations`, then guest stops.
    for (sandbox, second_drain) in [
        (seq_b_wake_after_drain, false),
        (seq_b_wake_then_second_drain, true),
    ] {
        let fence = dispatcher
            .recorded_stop(campaign, sandbox)
            .and_then(RecordedStop::into_fence)
            .expect("stop recorded");
        drain_stopped_sandbox(
            &db.pool,
            sandbox,
            Some(replacement_node),
            Uuid::now_v7(),
            source_meta.clone(),
            to_offset_date_time(fence.stopped_at) + time::Duration::milliseconds(200),
        )
        .await?;
        upsert_snapshot(&db.pool, replacement_node, SnapshotSeed::default()).await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, runtime, state_format_name, execution_metadata
             )
             VALUES ($1, $2, 'firecracker', 'firecracker-snapshot', $3)"
        ))
        .bind(sandbox)
        .bind(replacement_node)
        .bind(source_meta.clone())
        .execute(&db.pool)
        .await?;
        implicit_wake_via_activation_receipt(&db.pool, replacement_node, sandbox).await?;
        set_lifecycle(&db.pool, sandbox, SandboxLifecycle::Stopped.as_str()).await?;
        if second_drain {
            drain_stopped_sandbox(
                &db.pool,
                sandbox,
                Some(third_node),
                Uuid::now_v7(),
                source_meta.clone(),
                to_offset_date_time(fence.stopped_at) + time::Duration::milliseconds(500),
            )
            .await?;
        }
    }

    // 1 & 2. Exercise in-flight fences before finishing both `seq_a_*` cases:
    //        - `seq_a_higher_gen`: `G0` retired as `'replaced'`, `G1` holds a live
    //          `activation_attempt_id`.
    //        - `seq_a_reused_gen_new_pod`: `claim_pod_materialization` cleared
    //          `G0.pod_uid` to `NULL` and set `activation_attempt_id` at
    //          `lifecycle_changed_at == stopped_at`.
    //        - `seq_b_wake_after_drain`: already `Superseded`, with an additional
    //          in-flight `public_operations` `StartSandbox` row.
    {
        let fence_higher = dispatcher
            .recorded_stop(campaign, seq_a_higher_gen)
            .and_then(RecordedStop::into_fence)
            .expect("stop recorded");
        let fence_reused = dispatcher
            .recorded_stop(campaign, seq_a_reused_gen_new_pod)
            .and_then(RecordedStop::into_fence)
            .expect("stop recorded");
        assert!(
            pplx_space_sandbox_store::k8s_executions::retire::finish_active(
                &db.pool,
                SandboxId::from_uuid(seq_a_higher_gen),
                "replaced",
            )
            .await?
        );
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                 pod_ip, state, active, activation_attempt_id,
                 activation_attempt_expires_at, route_activation_id
             )
             VALUES ($1, 2, $2, 'space', $3, '10.0.0.21'::inet, 'running', TRUE, $4,
                     NOW() + INTERVAL '5 minutes', $5)"
        ))
        .bind(seq_a_higher_gen)
        .bind(format!("sbx-{seq_a_higher_gen}-g2"))
        .bind(format!("pod-wake-{seq_a_higher_gen}"))
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;

        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET pod_uid = NULL,
                    pod_materialized_at = NOW(),
                    state = 'creating',
                    activation_attempt_id = $2,
                    activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                    updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = 1 AND active"
        ))
        .bind(seq_a_reused_gen_new_pod)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;

        let in_flight_op_id = Uuid::now_v7();
        let seq_b_target_id =
            PublicSandboxId::from(SandboxId::from_uuid(seq_b_wake_after_drain)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())"
        ))
        .bind(in_flight_op_id)
        .bind(SEED_ORG_ID)
        .bind(SEED_USER_ID)
        .bind(OperationKind::StartSandbox.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(seq_b_target_id)
        .bind(OperationState::Running.as_str())
        .execute(&db.pool)
        .await?;

        tick(&reconciler).await;
        for in_flight_sbx in [
            seq_a_higher_gen,
            seq_a_reused_gen_new_pod,
            seq_b_wake_after_drain,
        ] {
            let (state_during_wake, err_during_wake, attempts_during_wake) =
                item(&db.pool, campaign, in_flight_sbx).await?;
            assert_eq!(
                state_during_wake, "pending",
                "in-flight wake on {in_flight_sbx} must keep item pending"
            );
            assert_eq!(
                attempts_during_wake, 1,
                "in-flight wake on {in_flight_sbx} must release without burning attempts"
            );
            assert_eq!(
                err_during_wake.as_deref(),
                Some(super::super::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT)
            );
            assert!(
                dispatcher
                    .recorded_stopped_at(campaign, in_flight_sbx)
                    .is_some(),
                "stop ownership stays recorded on {in_flight_sbx} while wake is in flight"
            );
            assert_eq!(
                dispatcher
                    .calls()
                    .iter()
                    .filter(|&call| *call == Call::Start(in_flight_sbx))
                    .count(),
                1,
                "driver must release before calling start on {in_flight_sbx} while wake is in flight"
            );
        }

        sqlx::query(sql!(
            "DELETE FROM public_operations WHERE operation_id = $1"
        ))
        .bind(in_flight_op_id)
        .execute(&db.pool)
        .await?;

        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET pod_uid = $2,
                    state = 'running',
                    updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = 1 AND active"
        ))
        .bind(seq_a_reused_gen_new_pod)
        .bind(format!("pod-reused-gen-{seq_a_reused_gen_new_pod}"))
        .execute(&db.pool)
        .await?;
        implicit_wake_via_activation_receipt(&db.pool, drained_node, seq_a_reused_gen_new_pod)
            .await?;
        set_lifecycle(
            &db.pool,
            seq_a_reused_gen_new_pod,
            SandboxLifecycle::Stopped.as_str(),
        )
        .await?;
        drain_stopped_sandbox(
            &db.pool,
            seq_a_reused_gen_new_pod,
            Some(replacement_node),
            Uuid::now_v7(),
            source_meta.clone(),
            to_offset_date_time(fence_reused.stopped_at) + time::Duration::milliseconds(300),
        )
        .await?;

        implicit_wake_via_activation_receipt(&db.pool, drained_node, seq_a_higher_gen).await?;
        set_lifecycle(
            &db.pool,
            seq_a_higher_gen,
            SandboxLifecycle::Stopped.as_str(),
        )
        .await?;
        drain_stopped_sandbox(
            &db.pool,
            seq_a_higher_gen,
            Some(replacement_node),
            Uuid::now_v7(),
            source_meta.clone(),
            to_offset_date_time(fence_higher.stopped_at) + time::Duration::milliseconds(300),
        )
        .await?;
    }

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");

    for sandbox in all {
        assert_eq!(
            item(&db.pool, campaign, sandbox).await?.0,
            "skipped",
            "sandbox {sandbox} must skip after implicit wake + guest stop instead of cold-booting"
        );
        assert_eq!(dispatcher.recorded_stopped_at(campaign, sandbox), None);
        assert_eq!(
            dispatcher
                .calls()
                .iter()
                .filter(|&call| *call == Call::Start(sandbox))
                .count(),
            1,
            "sandbox {sandbox} must relinquish stop ownership before dispatching a second start"
        );
        let still_stopped: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(still_stopped, "stopped");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn converged_node_stop_during_or_after_timed_out_stop_adopts_only_admitted_campaign_stops()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            failure_budget: 5,
            ..force_restart_seed()
        },
    )
    .await?;
    let host_node = node(&db.pool).await?;
    let admitted_timeout_converged =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;
    let unadmitted_timeout_poweroff =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;
    let in_flight_converged =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;
    let pre_admission_refused_poweroff =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;

    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (admitted_timeout_converged, host_node),
        (unadmitted_timeout_poweroff, host_node),
        (in_flight_converged, host_node),
        (pre_admission_refused_poweroff, host_node),
    ]))
    .persisting(db.pool.clone());
    {
        let mut state = dispatcher.state();
        state
            .admitted_stop_pre_commit_deadline_advances
            .insert(admitted_timeout_converged, Duration::from_secs(31));
        state
            .unadmitted_stop_pre_commit_deadline_advances
            .insert(unadmitted_timeout_poweroff, Duration::from_secs(31));
        state
            .converge_stopped_during_in_flight_stop
            .insert(in_flight_converged, host_node);
        state.pre_admission_stop_errors.insert(
            pre_admission_refused_poweroff,
            VecDeque::from([DispatchError::Retryable(
                "stop_not_admitted: transition slot busy".to_owned(),
            )]),
        );
    }
    let memory = FakeMemoryRetirement::retired(source());

    // First, isolate each `item_deadline`-cancelled `stop` call on its own 30s
    // `item_deadline` controller tick so `tokio::time::pause` runs on one
    // in-flight task at a time: `admitted_timeout_converged` times out after
    // the node admits the campaign stop (before post-RPC commit), while
    // `unadmitted_timeout_poweroff` times out before node admission.
    for timed_out in [admitted_timeout_converged, unadmitted_timeout_poweroff] {
        set_lifecycle(&db.pool, timed_out, SandboxLifecycle::Started.as_str()).await?;
        let deadline_controller = Reconciler::new(
            db.pool.clone(),
            Drivers::default().with(SandboxForceRestartDriver::new(
                db.pool.clone(),
                K8S,
                dispatcher.clone(),
                memory.clone(),
            )),
            ReconcilerConfig {
                enabled: true,
                lease: Duration::from_secs(300),
                item_deadline: Duration::from_secs(30),
                ..ReconcilerConfig::default()
            },
        );
        tick(&deadline_controller).await;
        assert_eq!(item(&db.pool, campaign, timed_out).await?.0, "failed");
        assert!(
            matches!(
                dispatcher.recorded_stop(campaign, timed_out),
                Some(RecordedStop::Initiated(_))
            ),
            "mid-stop deadline timeout on {timed_out} must leave RecordedStop::Initiated"
        );
    }
    assert_eq!(
        dispatcher
            .state()
            .node_admitted_campaign_stops
            .keys()
            .copied()
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([(campaign, admitted_timeout_converged)]),
        "only admitted_timeout_converged should carry node-admitted campaign stop evidence"
    );

    // Run `in_flight_converged` (where `converge_published_stopped` races the
    // in-flight `stop` RPC before post-RPC commit) and
    // `pre_admission_refused_poweroff` (where `sbx-mgr` refuses before admission)
    // while the two timed-out items sit in their 5s retry backoff.
    set_lifecycle(
        &db.pool,
        in_flight_converged,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;
    set_lifecycle(
        &db.pool,
        pre_admission_refused_poweroff,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;
    let first_controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());
    tick(&first_controller).await;
    drop(first_controller);

    assert_eq!(
        item(&db.pool, campaign, in_flight_converged).await?,
        ("done".to_owned(), None, 1),
        "in-flight converge_published_stopped during admitted campaign stop must adopt and cold-boot"
    );
    assert_eq!(
        item(&db.pool, campaign, pre_admission_refused_poweroff)
            .await?
            .0,
        "pending"
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, pre_admission_refused_poweroff),
        None,
        "pre-admission stop refusal must clear RecordedStop::Initiated immediately"
    );
    assert!(
        matches!(
            dispatcher.recorded_stop(campaign, unadmitted_timeout_poweroff),
            Some(RecordedStop::Initiated(_))
        ),
        "unadmitted deadline timeout keeps RecordedStop::Initiated until retry inspects node evidence"
    );

    // `PublishNodeState`'s `converge_published_stopped` now converges `'stopped'`
    // for `admitted_timeout_converged` (from the campaign's admitted node stop)
    // and for `pre_admission_refused_poweroff` + `unadmitted_timeout_poweroff`
    // (from guest self-poweroff / idle auto-stop, with no `public_operations` row).
    let converged = pplx_space_sandbox_store::ingest::converge_published_stopped(
        &db.pool,
        host_node,
        serde_json::json!([
            { "sandbox_id": admitted_timeout_converged, "published_lifecycle": "stopped" },
            { "sandbox_id": pre_admission_refused_poweroff, "published_lifecycle": "stopped" },
            { "sandbox_id": unadmitted_timeout_poweroff, "published_lifecycle": "stopped" },
        ]),
    )
    .await?;
    assert_eq!(converged.len(), 3);

    // Reset failed items (`retry_failed = true`) so every remaining item is
    // eligible on the next tick.
    store::pause_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    let resumed = store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        true,
    )
    .await?;
    assert_eq!(resumed.phase, CampaignPhase::Rolling);

    // Hold memory retirement for one tick on a replacement controller:
    // `admitted_timeout_converged` must hold (`Release`) rather than skipping
    // while stopped under an initiated campaign stop.
    let second_controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());
    memory.unretire(source());
    let held_driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == campaign)
        .expect("campaign exists");
    let leased = store::lease_items(&db.pool, campaign, Duration::from_secs(60), 10).await?;
    let held_lease = leased
        .iter()
        .find(|l| l.subject_id == admitted_timeout_converged)
        .expect("admitted_timeout_converged leased");
    assert!(matches!(
        held_driver.upgrade(&campaign_row, held_lease).await,
        ItemOutcome::Release(_)
    ));
    for l in &leased {
        store::finish_item(
            &db.pool,
            l,
            &ItemOutcome::Release(ItemError::new("hold check")),
        )
        .await?;
    }

    // Re-retire memory and drive the campaign to completion.
    memory.retire(source());
    tick_until_finished(&second_controller, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");

    for upgraded in [in_flight_converged, admitted_timeout_converged] {
        assert_eq!(
            item(&db.pool, campaign, upgraded).await?.0,
            "done",
            "campaign-admitted stop on {upgraded} must cold-boot onto target"
        );
        let snap = store::subject_sandbox(&db.pool, upgraded)
            .await?
            .expect("sandbox exists");
        assert_eq!(snap.lifecycle, "started");
        assert_eq!(
            snap.execution
                .and_then(|e| e.execution_metadata)
                .and_then(|m| m
                    .get("environment")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)),
            Some(target().to_string())
        );
    }

    for untouched in [pre_admission_refused_poweroff, unadmitted_timeout_poweroff] {
        assert_eq!(
            item(&db.pool, campaign, untouched).await?.0,
            "skipped",
            "unadmitted stop followed by guest/idle poweroff on {untouched} must skip"
        );
        assert_eq!(dispatcher.recorded_stop(campaign, untouched), None);
        assert!(
            !dispatcher.started().contains(&untouched),
            "must never cold-boot {untouched} after unadmitted stop + poweroff"
        );
        let snap = store::subject_sandbox(&db.pool, untouched)
            .await?
            .expect("sandbox exists");
        assert_eq!(snap.lifecycle, "stopped");
    }

    // Cross-store lock agreement: `public_operation_store::share_lock_campaign_retention`
    // must block `campaign_store::create_campaign(..., SandboxForceRestarts)` until
    // the GC transaction commits, AND `FakeDispatcher::start` must return
    // `HeldStopped` without mutating Postgres if an item lease is no longer active.
    let mut tx_gc = db.pool.begin().await?;
    let gc_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *tx_gc)
        .await?;
    pplx_space_public_operation_store::operations::share_lock_campaign_retention(&mut *tx_gc)
        .await?;
    let mut create_conn = db.pool.acquire().await?;
    let create_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *create_conn)
        .await?;
    let create_handle = tokio::spawn(async move {
        store::create_campaign(
            &mut *create_conn,
            &store::NewCampaign {
                kind: CampaignKind::SandboxForceRestarts,
                from: source(),
                to: target(),
                canary: false,
                max_in_flight_per_node: 1,
                max_in_flight_per_cluster: 2,
                failure_budget: 2,
                include_running: false,
                reason: "cross-store retention lock check".to_owned(),
                created_by: None,
            },
        )
        .await
    });
    let lock_deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let blocked: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks
                 WHERE pid = $1
                   AND locktype = 'advisory'
                   AND NOT granted
                   AND $2 = ANY(pg_blocking_pids($1))
             )"
        ))
        .bind(create_pid)
        .bind(gc_pid)
        .fetch_one(&mut *tx_gc)
        .await?;
        if blocked {
            break;
        }
        assert!(
            std::time::Instant::now() < lock_deadline,
            "create_campaign must block on share_lock_campaign_retention"
        );
        tokio::task::yield_now().await;
    }
    assert!(!create_handle.is_finished());
    tx_gc.commit().await?;
    let follow_up = create_handle.await??;

    let inactive_lease_sbx =
        resident_on_source(&db.pool, SandboxLifecycle::Stopped, host_node).await?;
    record(&db.pool, follow_up, &[inactive_lease_sbx]).await?;
    let leased = store::lease_items(&db.pool, follow_up, Duration::from_secs(30), 4)
        .await?
        .pop()
        .expect("leased item");
    store::pause_campaign(
        &db_handle(&db.pool),
        follow_up,
        &store::PhaseChange::default(),
    )
    .await?;
    let snap_before = store::subject_sandbox(&db.pool, inactive_lease_sbx)
        .await?
        .expect("sandbox exists");
    let held = dispatcher
        .start(
            &leased,
            SandboxId::from_uuid(inactive_lease_sbx),
            StopFence::at(to_jiff(snap_before.lifecycle_changed_at)),
            source(),
            target(),
        )
        .await?;
    assert!(
        matches!(held, TargetStart::HeldStopped(_)),
        "start under a paused/inactive campaign lease must return HeldStopped, got {held:?}"
    );
    assert_eq!(
        store::subject_sandbox(&db.pool, inactive_lease_sbx)
            .await?
            .expect("sandbox exists")
            .lifecycle,
        "stopped"
    );

    Ok(())
}

#[tokio::test]
async fn post_claim_start_deadline_cancellation_declined_stop_cleanup_and_stranded_pagination()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let host_node = node(&db.pool).await?;
    let campaign = store::create_campaign(
        &db.pool,
        &store::NewCampaign {
            kind: CampaignKind::SandboxForceRestarts,
            from: source(),
            to: target(),
            canary: false,
            max_in_flight_per_node: 4,
            max_in_flight_per_cluster: 4,
            failure_budget: 4,
            include_running: false,
            reason: "post-claim start deadline cancellation check".to_owned(),
            created_by: None,
        },
    )
    .await?;

    let canary_done = resident_on_source(&db.pool, SandboxLifecycle::Started, host_node).await?;
    let cancelled_start_converged =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;
    let cancelled_start_external_stop =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;
    let declined_stop_moved_to_target =
        resident_on_source(&db.pool, SandboxLifecycle::Created, host_node).await?;

    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (canary_done, host_node),
        (cancelled_start_converged, host_node),
        (cancelled_start_external_stop, host_node),
        (declined_stop_moved_to_target, host_node),
    ]))
    .persisting(db.pool.clone());
    {
        let mut state = dispatcher.state();
        state
            .post_claim_start_deadline_advances
            .insert(cancelled_start_converged, Duration::from_secs(31));
        state
            .post_claim_start_deadline_advances
            .insert(cancelled_start_external_stop, Duration::from_secs(31));
        state
            .unadmitted_stop_pre_commit_deadline_advances
            .insert(declined_stop_moved_to_target, Duration::from_secs(31));
    }
    let memory = FakeMemoryRetirement::retired(source());
    let controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());
    tick(&controller).await;
    assert_eq!(item(&db.pool, campaign, canary_done).await?.0, "done");

    // 1. Real `worker.rs` `item_deadline` cancellation after `dispatcher.start`
    //    claims `'stopped' -> 'starting'` in Postgres (`StartingOnTarget` recorded
    //    before the node RPC completes), and after `dispatcher.stop` records
    //    `RecordedStop::Initiated` on `declined_stop_moved_to_target`.
    let make_deadline_controller = || {
        Reconciler::new(
            db.pool.clone(),
            Drivers::default().with(SandboxForceRestartDriver::new(
                db.pool.clone(),
                K8S,
                dispatcher.clone(),
                memory.clone(),
            )),
            ReconcilerConfig {
                enabled: true,
                lease: Duration::from_secs(300),
                item_deadline: Duration::from_secs(30),
                ..ReconcilerConfig::default()
            },
        )
    };
    for cancelled in [cancelled_start_converged, cancelled_start_external_stop] {
        set_lifecycle(&db.pool, cancelled, SandboxLifecycle::Started.as_str()).await?;
        tick(&make_deadline_controller()).await;
        let (phase, _, attempts) = item(&db.pool, campaign, cancelled).await?;
        assert_eq!((phase.as_str(), attempts), ("failed", 1));
        assert!(
            matches!(
                dispatcher.recorded_stop(campaign, cancelled),
                Some(RecordedStop::StartingOnTarget(_))
            ),
            "post-claim start deadline cancellation on {cancelled} must leave StartingOnTarget"
        );
        let snap = store::subject_sandbox(&db.pool, cancelled)
            .await?
            .expect("sandbox exists");
        assert_eq!(snap.lifecycle, "starting");
    }
    set_lifecycle(
        &db.pool,
        declined_stop_moved_to_target,
        SandboxLifecycle::Started.as_str(),
    )
    .await?;
    tick(&make_deadline_controller()).await;
    let (declined_failed_phase, _, declined_failed_attempts) =
        item(&db.pool, campaign, declined_stop_moved_to_target).await?;
    assert_eq!(
        (declined_failed_phase.as_str(), declined_failed_attempts),
        ("failed", 1)
    );
    assert!(matches!(
        dispatcher.recorded_stop(campaign, declined_stop_moved_to_target),
        Some(RecordedStop::Initiated(_))
    ));

    // 2. Re-enqueue the deadline-failed items (`retry_failed = true`) while both
    //    `StartingOnTarget` sandboxes are still `'starting'`: a reconciler tick
    //    releases them (`pending`, `attempts = 0`) without burning attempts or
    //    clearing `StartingOnTarget`, while `declined_stop_moved_to_target`
    //    holds with `RecordedStop::Initiated(_)` still intact.
    store::pause_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    let resumed = store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        true,
    )
    .await?;
    assert_eq!(resumed.phase, CampaignPhase::Rolling);
    dispatcher.state().stop_script.insert(
        declined_stop_moved_to_target,
        GuardedStop::Held("node stop RPC still settling".to_owned()),
    );
    tick(&controller).await;

    for cancelled in [cancelled_start_converged, cancelled_start_external_stop] {
        let (phase, _, attempts) = item(&db.pool, campaign, cancelled).await?;
        assert_eq!(
            (phase.as_str(), attempts),
            ("pending", 0),
            "lingering StartingOnTarget item {cancelled} must release without burning attempts"
        );
        assert!(matches!(
            dispatcher.recorded_stop(campaign, cancelled),
            Some(RecordedStop::StartingOnTarget(_))
        ));
    }
    let (declined_phase, _, declined_attempts) =
        item(&db.pool, campaign, declined_stop_moved_to_target).await?;
    assert_eq!((declined_phase.as_str(), declined_attempts), ("pending", 0));
    assert!(matches!(
        dispatcher.recorded_stop(campaign, declined_stop_moved_to_target),
        Some(RecordedStop::Initiated(_))
    ));

    // 3. Node settles both lingering starts via `internal-api` (updating only
    //    `sandboxes` + `sandbox_observed_states`), an external `sandbox.stop`
    //    supersedes `cancelled_start_external_stop`, and `declined_stop_moved_to_target`'s
    //    retry hits `GuardedStop::Declined` in `recover_after_declined_stop` while
    //    `RecordedStop::Initiated(_)` is still recorded.
    let external_stop_fence = dispatcher
        .recorded_stop(campaign, cancelled_start_external_stop)
        .and_then(RecordedStop::into_fence)
        .expect("StartingOnTarget fence");
    dispatcher
        .settle_lingering_start(&db.pool, cancelled_start_converged, target())
        .await?;
    dispatcher
        .settle_lingering_start(&db.pool, cancelled_start_external_stop, target())
        .await?;
    let target_id =
        PublicSandboxId::from(SandboxId::from_uuid(cancelled_start_external_stop)).to_string();
    sqlx::query(sql!(
        "INSERT INTO public_operations (
             operation_id, org_id, actor_user_id, kind, target_type, target_id,
             state, requested_at, completed_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(SEED_USER_ID)
    .bind(OperationKind::StopSandbox.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(OperationState::Succeeded.as_str())
    .bind(to_offset_date_time(external_stop_fence.stopped_at))
    .execute(&db.pool)
    .await?;
    dispatcher
        .state()
        .move_to_target_after_initiated_stop
        .insert(declined_stop_moved_to_target);
    tick(&controller).await;
    assert_eq!(
        item(&db.pool, campaign, declined_stop_moved_to_target)
            .await?
            .0,
        "skipped"
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, declined_stop_moved_to_target),
        None
    );
    assert!(
        !dispatcher
            .state()
            .calls
            .contains(&Call::Start(declined_stop_moved_to_target)),
        "declined stop that saw sandbox move to target under Initiated must never call Start"
    );

    tick_until_finished(&controller, &db.pool, campaign, 4).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign, cancelled_start_converged).await?.0,
        "done"
    );
    assert_eq!(
        item(&db.pool, campaign, cancelled_start_external_stop)
            .await?
            .0,
        "skipped"
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, cancelled_start_external_stop),
        None
    );
    assert_eq!(
        item(&db.pool, campaign, declined_stop_moved_to_target)
            .await?
            .0,
        "skipped"
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, declined_stop_moved_to_target),
        None,
        "recover_after_declined_stop must clear RecordedStop::Initiated on Skipped"
    );

    // 4. `lease_when_stranded` keyset pagination and fairness: when `moves == false`
    //    (no certified upgrade edge on `source()`) and `scan_limit = 2`, neither
    //    unbound source items, target-attributed items on an unreachable node, nor
    //    target-attributed items that repeatedly return `Release` starve a trailing
    //    target-ready item behind them.
    sqlx::query(sql!(
        "UPDATE runtime_environments
         SET upgrade_edges = '{}'
         WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    let unreachable_node = node(&db.pool).await?;
    upsert_snapshot(
        &db.pool,
        unreachable_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    let stranded_campaign = store::create_campaign(
        &db.pool,
        &store::NewCampaign {
            kind: CampaignKind::SandboxForceRestarts,
            from: source(),
            to: target(),
            canary: false,
            max_in_flight_per_node: 4,
            max_in_flight_per_cluster: 4,
            failure_budget: 4,
            include_running: false,
            reason: "stranded keyset pagination and fairness check".to_owned(),
            created_by: None,
        },
    )
    .await?;
    let ordered = unattributable_in_order(&db.pool, host_node, "1.9.0", 8).await?;
    for &sbx in &ordered {
        set_lifecycle(&db.pool, sbx, SandboxLifecycle::Started.as_str()).await?;
    }
    // `ordered[0..2]`: unbound source items; `ordered[2..4]`: target-bound on an
    // unreachable node; `ordered[4..6]`: target-bound with an in-flight external
    // stop (`Release`); `ordered[6]`: unbound; `ordered[7]`: `trailing_target`.
    for &unreachable_sbx in &ordered[2..4] {
        set_bound(&db.pool, unreachable_sbx, target()).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_observed_states
             SET node_id = $2
             WHERE sandbox_id = $1"
        ))
        .bind(unreachable_sbx)
        .bind(unreachable_node)
        .execute(&db.pool)
        .await?;
    }
    for &releasing_sbx in &ordered[4..6] {
        set_bound(&db.pool, releasing_sbx, target()).await?;
        let releasing_target_id =
            PublicSandboxId::from(SandboxId::from_uuid(releasing_sbx)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(SEED_USER_ID)
        .bind(OperationKind::StopSandbox.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(releasing_target_id)
        .bind(OperationState::Running.as_str())
        .execute(&db.pool)
        .await?;
    }
    let trailing_target = ordered[7];
    set_bound(&db.pool, trailing_target, target()).await?;
    record(&db.pool, stranded_campaign, &ordered).await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET attempts = 1,
             last_error = 'lingering start'
         WHERE campaign_id = $1"
    ))
    .bind(stranded_campaign)
    .execute(&db.pool)
    .await?;
    for &target_sbx in ordered[2..6]
        .iter()
        .chain(std::iter::once(&trailing_target))
    {
        let snap = store::subject_sandbox(&db.pool, target_sbx)
            .await?
            .expect("target sandbox exists");
        dispatcher.state().item_stopped_at.insert(
            (stranded_campaign, target_sbx),
            RecordedStop::StartingOnTarget(StartingClaim::from_stop(StopFence::at(to_jiff(
                snap.lifecycle_changed_at,
            )))),
        );
    }
    let stranded_driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    let stranded_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == stranded_campaign)
        .expect("stranded campaign");

    // Pass 1 (`scan_limit = 2`): skips `ordered[0..2]` (unbound) and `ordered[2..4]`
    // (unreachable node), leasing `ordered[4..6]` which release with updated
    // `updated_at`.
    let candidates_1 = stranded_driver
        .stranded_lease_candidates(&stranded_row, 2)
        .await
        .expect("stranded candidates pass 1");
    let leased_pass_1 = store::lease_selected_items(
        &db.pool,
        stranded_campaign,
        &candidates_1,
        Duration::from_secs(300),
        2,
    )
    .await
    .expect("stranded pass 1");
    let leased_ids_1: Vec<Uuid> = leased_pass_1.iter().map(|item| item.subject_id).collect();
    assert_eq!(leased_ids_1, ordered[4..6].to_vec());
    for leased_item in &leased_pass_1 {
        let outcome = stranded_driver.upgrade(&stranded_row, leased_item).await;
        assert!(matches!(outcome, ItemOutcome::Release(_)));
        assert!(store::finish_item(&db.pool, leased_item, &outcome).await?);
    }

    // Pass 2 (`scan_limit = 2`): `ordered[4..6]` rotated behind `trailing_target`
    // by `updated_at`, so `trailing_target` is selected, leased, and finished `done`.
    let candidates_2 = stranded_driver
        .stranded_lease_candidates(&stranded_row, 2)
        .await
        .expect("stranded candidates pass 2");
    let leased_pass_2 = store::lease_selected_items(
        &db.pool,
        stranded_campaign,
        &candidates_2,
        Duration::from_secs(300),
        2,
    )
    .await
    .expect("stranded pass 2");
    assert!(
        leased_pass_2
            .iter()
            .any(|item| item.subject_id == trailing_target),
        "trailing_target must rotate ahead of released items on pass 2"
    );
    for leased_item in &leased_pass_2 {
        let outcome = stranded_driver.upgrade(&stranded_row, leased_item).await;
        assert!(store::finish_item(&db.pool, leased_item, &outcome).await?);
    }
    assert_eq!(
        item(&db.pool, stranded_campaign, trailing_target).await?.0,
        "done",
        "lease_when_stranded must skip unreachable nodes and rotate past released items"
    );

    Ok(())
}
