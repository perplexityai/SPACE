//! Guarded `stop` simulation for [`FakeDispatcher`].

use std::collections::VecDeque;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};
use pplx_space_sql_locator::sql;

use super::helpers::{
    active_k8s_stopped_execution, check_stop_external_fences, has_active_item_lease,
    pause_and_hang_past_deadline, stop_claim_refusal, verify_converged_initiated_stop_execution,
};
use super::{Call, FakeDispatcher};
use crate::runtime_upgrade::sandboxes::tests::*;

impl FakeDispatcher {
    pub(super) async fn dispatch_stop(
        &self,
        item: &LeasedItem,
        record_stop: bool,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> Result<GuardedStop, DispatchError> {
        let recorded_item = record_stop.then_some(item);
        let sandbox = sandbox_id.as_uuid();
        let (scripted, lingering_settled, lingering_stopping, move_to_target_declined) = {
            let mut state = self.state();
            state.calls.push(Call::Stop(sandbox));
            state.stops.push((source, target));
            if let Some(error) = state
                .stop_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front)
            {
                return Err(error);
            }
            (
                state.stop_script.remove(&sandbox),
                state.lingering_stops_on_stop_call.remove(&sandbox),
                state.lingering_stopping_on_stop_call.remove(&sandbox),
                state.move_to_target_after_initiated_stop.remove(&sandbox),
            )
        };
        if move_to_target_declined {
            if let Some(pool) = &self.persist_pool {
                if record_stop && self.recorded_stop(item.campaign_id, sandbox).is_none() {
                    let (raw_initiated_at, raw_dispatched_at): (
                        time::OffsetDateTime,
                        time::OffsetDateTime,
                    ) = sqlx::query_as(sql!("SELECT NOW(), NOW()"))
                        .fetch_one(pool)
                        .await
                        .expect("capturing initiated stop timestamps before simulated target move");
                    let k8s_stopped_execution = active_k8s_stopped_execution(pool, sandbox).await;
                    self.record_item_initiated_stop(
                        recorded_item,
                        sandbox,
                        InitiatedStop {
                            initiated_at: to_jiff(raw_initiated_at),
                            dispatched_at: to_jiff(raw_dispatched_at),
                            k8s_stopped_execution,
                        },
                    )
                    .await;
                }
                set_bound(pool, sandbox, target)
                    .await
                    .expect("moving sandbox to target during declined stop");
            }
            return Ok(GuardedStop::Declined(
                "concurrent node transition moved sandbox to target".to_owned(),
            ));
        }
        if lingering_stopping {
            if let Some(pool) = &self.persist_pool {
                set_lifecycle(pool, sandbox, SandboxLifecycle::Stopping.as_str())
                    .await
                    .expect("persisting simulated lingering stopping transition");
            }
            return Ok(GuardedStop::Declined(
                "sandbox is already stopping".to_owned(),
            ));
        }
        if lingering_settled {
            if let Some(pool) = &self.persist_pool {
                let k8s_stopped_execution = active_k8s_stopped_execution(pool, sandbox).await;
                let changed_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
                    "UPDATE sandboxes
                     SET lifecycle = $2
                     WHERE id = $1
                     RETURNING lifecycle_changed_at"
                ))
                .bind(sandbox)
                .bind(SandboxLifecycle::Stopped.as_str())
                .fetch_one(pool)
                .await
                .expect("persisting simulated lingering stop");
                let settled = to_jiff(changed_at);
                self.record_item_stopped_at(
                    recorded_item,
                    sandbox,
                    None,
                    StopFence::new(settled, settled, settled, k8s_stopped_execution),
                )
                .await;
            }
            return Ok(GuardedStop::Declined(
                "sandbox already stopped by earlier attempt".to_owned(),
            ));
        }
        if let Some(scripted) = scripted {
            if let GuardedStop::Stopped(stop) = &scripted {
                self.record_item_stopped_at(recorded_item, sandbox, None, stop.clone())
                    .await;
            }
            return Ok(scripted);
        }
        let (
            in_flight_external_kill,
            allow_before_stop,
            unretire_before_stop,
            episode_replacement,
            pod_replacement,
            pod_uid_in_place,
        ) = {
            let mut state = self.state();
            (
                state.external_kill_during_in_flight_stop.remove(&sandbox),
                state.allow_source_before_stop.remove(&sandbox),
                state.unretire_source_before_stop.remove(&sandbox),
                state.in_flight_stop_episode_replacements.remove(&sandbox),
                state.replace_k8s_pod_during_in_flight_stop.remove(&sandbox),
                state
                    .update_active_k8s_pod_uid_during_in_flight_stop
                    .remove(&sandbox),
            )
        };
        if unretire_before_stop && let Some(retirement) = &self.memory_retirement {
            retirement.unretire(source);
        }
        let (expected_initiated, stop) = if let Some(pool) = &self.persist_pool {
            if allow_before_stop {
                let mut tx = pool
                    .begin()
                    .await
                    .expect("opening transaction for simulated pre-stop source allow");
                pplx_space_runtime_environment_policy_store::set_environment_state(
                    &mut *tx,
                    source,
                    RuntimeEnvironmentState::Allowed,
                    &change(),
                )
                .await
                .expect("allowing source before fake stop");
                tx.commit()
                    .await
                    .expect("committing simulated pre-stop source allow");
            }
            if !has_active_item_lease(pool, item).await {
                return Ok(GuardedStop::Held(
                    "the campaign item lease is no longer active".to_owned(),
                ));
            }
            let Some(subject) = store::subject_sandbox(pool, sandbox)
                .await
                .expect("reading subject sandbox before fake stop")
            else {
                self.clear_initiated_stop(recorded_item, sandbox, None)
                    .await;
                return Ok(GuardedStop::Declined("the sandbox was deleted".to_owned()));
            };
            let inputs = Inputs::read(pool)
                .await
                .expect("reading policy and catalog before fake stop");
            let effective = subject.effective_execution();
            let attributed = effective.and_then(|exec| inputs.attributed(exec));

            if record_stop && subject.lifecycle == SandboxLifecycle::Stopped.as_str() {
                match self.recorded_stop(item.campaign_id, sandbox) {
                    Some(RecordedStop::Stopped(_) | RecordedStop::StartingOnTarget(_)) => {
                        return Ok(GuardedStop::Declined(
                            "sandbox already stopped by earlier attempt".to_owned(),
                        ));
                    }
                    Some(RecordedStop::Initiated(initiated)) => {
                        if effective.is_some()
                            && attributed != Some(source)
                            && attributed != Some(target)
                        {
                            if effective.is_some_and(Inputs::is_unbound) {
                                return Ok(GuardedStop::Held(
                                    "the sandbox's unbound execution isn't currently attributed"
                                        .to_owned(),
                                ));
                            }
                            self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                .await;
                            return Ok(GuardedStop::Declined(
                                "the sandbox moved to neither source nor target".to_owned(),
                            ));
                        }
                        if let Err((should_clear, refusal)) = check_stop_external_fences(
                            pool,
                            sandbox,
                            to_offset_date_time(initiated.initiated_at),
                            to_offset_date_time(initiated.dispatched_at),
                        )
                        .await
                        {
                            if should_clear {
                                self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                    .await;
                            }
                            return Ok(refusal);
                        }
                        let settled_stopped_at = to_jiff(subject.lifecycle_changed_at);
                        if settled_stopped_at < initiated.initiated_at {
                            self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                .await;
                            return Ok(GuardedStop::Declined(
                                "the sandbox stopped before this campaign initiated its stop"
                                    .to_owned(),
                            ));
                        }
                        if let Err((should_clear, refusal)) =
                            verify_converged_initiated_stop_execution(
                                pool,
                                sandbox,
                                &initiated,
                                &subject,
                                "the sandbox left the execution where this campaign initiated its stop",
                            )
                            .await
                        {
                            if should_clear {
                                self.clear_initiated_stop(
                                    recorded_item,
                                    sandbox,
                                    Some(&initiated),
                                )
                                .await;
                            }
                            return Ok(refusal);
                        }
                        if self
                            .state()
                            .node_admitted_campaign_stops
                            .get(&(item.campaign_id, sandbox))
                            .copied()
                            != Some(initiated.dispatched_at)
                        {
                            self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                .await;
                            return Ok(GuardedStop::Declined(
                                "the sandbox runtime stopped without an admitted campaign stop"
                                    .to_owned(),
                            ));
                        }
                        let stopped_at = if subject.pinned_stop_disk {
                            initiated.dispatched_at.min(settled_stopped_at)
                        } else {
                            settled_stopped_at
                        };
                        let expected_initiated = initiated.clone();
                        let fence = StopFence {
                            initiated_at: initiated.initiated_at,
                            dispatched_at: initiated.dispatched_at,
                            stopped_at,
                            settled_stopped_at,
                            k8s_stopped_execution: initiated.k8s_stopped_execution,
                        };
                        (Some(expected_initiated), fence)
                    }
                    _ => {
                        return Ok(GuardedStop::Declined(
                            "the sandbox is no longer started or paused".to_owned(),
                        ));
                    }
                }
            } else {
                if attributed != Some(source) {
                    if record_stop && effective.is_some_and(Inputs::is_unbound) {
                        return Ok(GuardedStop::Held(
                            crate::runtime_upgrade::sandboxes::force_restarts::POLICY_STOPPED_FORCE_RESTARTING
                                .to_owned(),
                        ));
                    }
                    self.clear_initiated_stop(recorded_item, sandbox, None)
                        .await;
                    return Ok(GuardedStop::Declined(
                        "the sandbox is no longer on the campaign's source".to_owned(),
                    ));
                }
                if !matches!(subject.lifecycle.as_str(), "started" | "paused") {
                    self.clear_initiated_stop(recorded_item, sandbox, None)
                        .await;
                    return Ok(GuardedStop::Declined(
                        "the sandbox is no longer started or paused".to_owned(),
                    ));
                }
                if let Some(refusal) = stop_claim_refusal(
                    &inputs,
                    record_stop,
                    source,
                    target,
                    self.memory_is_retired(source),
                ) {
                    return Ok(refusal);
                }
                let (raw_initiated_at, raw_dispatched_at): (
                    time::OffsetDateTime,
                    time::OffsetDateTime,
                ) = sqlx::query_as(sql!("SELECT LEAST(NOW(), $1), NOW()"))
                    .bind(subject.lifecycle_changed_at)
                    .fetch_one(pool)
                    .await
                    .expect("capturing stop initiation and dispatch timestamps");
                if let Err((should_clear, refusal)) =
                    check_stop_external_fences(pool, sandbox, raw_initiated_at, raw_dispatched_at)
                        .await
                {
                    if should_clear {
                        self.clear_initiated_stop(recorded_item, sandbox, None)
                            .await;
                    }
                    return Ok(refusal);
                }
                let pre_stop_k8s_execution = active_k8s_stopped_execution(pool, sandbox).await;
                let initiated = self
                    .record_item_initiated_stop(
                        recorded_item,
                        sandbox,
                        InitiatedStop {
                            initiated_at: to_jiff(raw_initiated_at),
                            dispatched_at: to_jiff(raw_dispatched_at),
                            k8s_stopped_execution: pre_stop_k8s_execution,
                        },
                    )
                    .await;
                let initiated_at = to_offset_date_time(initiated.initiated_at);
                let dispatched_at = to_offset_date_time(initiated.dispatched_at);
                let episode_key = (item.campaign_id, sandbox);
                let episode_admitted = self
                    .state()
                    .node_admitted_campaign_stops
                    .get(&episode_key)
                    .copied()
                    == Some(initiated.dispatched_at);
                if self.state().already_stopped_on_node.contains(&sandbox) && !episode_admitted {
                    self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                        .await;
                    return Ok(GuardedStop::Declined(
                        "the sandbox runtime was already stopped on the node".to_owned(),
                    ));
                }
                let (pre_admission_error, unadmitted_deadline, admitted_deadline, converge_node) = {
                    let mut state = self.state();
                    let pre_admission_error = state
                        .pre_admission_stop_errors
                        .get_mut(&sandbox)
                        .and_then(VecDeque::pop_front);
                    let unadmitted_deadline = state
                        .unadmitted_stop_pre_commit_deadline_advances
                        .remove(&sandbox);
                    let admitted_deadline = state
                        .admitted_stop_pre_commit_deadline_advances
                        .remove(&sandbox);
                    let converge_node = state
                        .converge_stopped_during_in_flight_stop
                        .remove(&sandbox);
                    (
                        pre_admission_error,
                        unadmitted_deadline,
                        admitted_deadline,
                        converge_node,
                    )
                };
                if let Some(error) = pre_admission_error {
                    if self
                        .state()
                        .node_admitted_campaign_stops
                        .get(&episode_key)
                        .copied()
                        != Some(initiated.dispatched_at)
                    {
                        self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                            .await;
                    }
                    return Err(error);
                }
                if let Some(advance) = unadmitted_deadline {
                    pause_and_hang_past_deadline(advance).await;
                }
                self.state()
                    .node_admitted_campaign_stops
                    .insert(episode_key, initiated.dispatched_at);
                if let Some(advance) = admitted_deadline {
                    pause_and_hang_past_deadline(advance).await;
                }
                if episode_replacement {
                    sqlx::query(sql!(
                        "UPDATE sandboxes
                         SET lifecycle_changed_at = $2 + INTERVAL '50 milliseconds'
                         WHERE id = $1"
                    ))
                    .bind(sandbox)
                    .bind(subject.lifecycle_changed_at.max(dispatched_at))
                    .execute(pool)
                    .await
                    .expect("advancing lifecycle_changed_at for simulated replacement episode");
                }
                if let Some(replacement_exec) = pod_replacement {
                    sqlx::query(sql!(
                        "UPDATE sandbox_k8s_executions
                         SET active = FALSE
                         WHERE sandbox_id = $1 AND active"
                    ))
                    .bind(sandbox)
                    .execute(pool)
                    .await
                    .expect("retiring old k8s execution during in-flight fake stop");
                    sqlx::query(sql!(
                        "INSERT INTO sandbox_k8s_executions (
                             sandbox_id, generation, pod_name, pod_namespace,
                             pod_uid, pod_ip, state, active
                         )
                         VALUES ($1, $2, $3, 'space', $4, '10.0.0.10'::inet, 'running', TRUE)"
                    ))
                    .bind(sandbox)
                    .bind(replacement_exec.generation)
                    .bind(format!("sbx-repl-{}", replacement_exec.generation))
                    .bind(replacement_exec.pod_uid.as_str())
                    .execute(pool)
                    .await
                    .expect("inserting replacement k8s execution during in-flight fake stop");
                }
                if let Some(updated_pod_uid) = pod_uid_in_place {
                    sqlx::query(sql!(
                        "UPDATE sandbox_k8s_executions
                         SET pod_uid = $2
                         WHERE sandbox_id = $1 AND active"
                    ))
                    .bind(sandbox)
                    .bind(updated_pod_uid.as_str())
                    .execute(pool)
                    .await
                    .expect("updating active k8s pod_uid in place during in-flight fake stop");
                }
                if let Some(converge_node) = converge_node {
                    let rows = serde_json::json!([{
                        "sandbox_id": sandbox,
                        "published_lifecycle": "stopped",
                    }]);
                    pplx_space_sandbox_store::ingest::converge_published_stopped(
                        pool,
                        converge_node,
                        rows,
                    )
                    .await
                    .expect("converging published stopped during in-flight fake stop");
                }
                if in_flight_external_kill {
                    use pplx_space_public_api::{
                        OperationKind, OperationState, OperationTargetType, PublicSandboxId,
                    };
                    let target_id =
                        PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
                    sqlx::query(sql!(
                        "INSERT INTO public_operations (
                             operation_id, org_id, actor_user_id, kind, target_type, target_id,
                             state, requested_at, completed_at
                         )
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
                    ))
                    .bind(Uuid::now_v7())
                    .bind(SEED_ORG_ID)
                    .bind(SEED_USER_ID)
                    .bind(OperationKind::KillSandbox.as_str())
                    .bind(OperationTargetType::Sandbox.as_str())
                    .bind(target_id)
                    .bind(OperationState::Succeeded.as_str())
                    .bind(dispatched_at)
                    .bind(dispatched_at)
                    .execute(pool)
                    .await
                    .expect("inserting simulated in-flight external kill operation");
                }
                let stoppable = [
                    SandboxLifecycle::Started.as_str(),
                    SandboxLifecycle::Paused.as_str(),
                ];
                let direct_settled = if let Some(exec) = &initiated.k8s_stopped_execution {
                    sqlx::query_scalar::<_, time::OffsetDateTime>(sql!(
                        "UPDATE sandboxes
                         SET lifecycle = $2
                         WHERE id = $1
                           AND lifecycle = ANY($3)
                           AND lifecycle_changed_at = $4
                           AND EXISTS (
                               SELECT 1
                               FROM sandbox_k8s_executions AS ske
                               WHERE ske.sandbox_id = $1
                                 AND ske.active
                                 AND ske.generation = $5
                                 AND BTRIM(ske.pod_uid) = $6
                           )
                         RETURNING lifecycle_changed_at"
                    ))
                    .bind(sandbox)
                    .bind(SandboxLifecycle::Stopped.as_str())
                    .bind(&stoppable[..])
                    .bind(subject.lifecycle_changed_at)
                    .bind(exec.generation)
                    .bind(exec.pod_uid.as_str())
                    .fetch_optional(pool)
                    .await
                    .expect("persisting fake k8s stop")
                } else {
                    sqlx::query_scalar::<_, time::OffsetDateTime>(sql!(
                        "UPDATE sandboxes
                         SET lifecycle = $2
                         WHERE id = $1
                           AND lifecycle = ANY($3)
                           AND lifecycle_changed_at = $4
                           AND NOT EXISTS (
                               SELECT 1
                               FROM sandbox_k8s_executions AS ske
                               WHERE ske.sandbox_id = $1
                                 AND ske.active
                                 AND NULLIF(BTRIM(ske.pod_uid), '') IS NOT NULL
                           )
                         RETURNING lifecycle_changed_at"
                    ))
                    .bind(sandbox)
                    .bind(SandboxLifecycle::Stopped.as_str())
                    .bind(&stoppable[..])
                    .bind(subject.lifecycle_changed_at)
                    .fetch_optional(pool)
                    .await
                    .expect("persisting fake direct stop")
                };
                let (mut changed_at, converged_pinned_stop_disk) = match direct_settled {
                    Some(changed_at) => (changed_at, false),
                    None => {
                        let Some(current) = store::subject_sandbox(pool, sandbox)
                            .await
                            .expect("re-reading subject sandbox after converged fake stop")
                        else {
                            self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                .await;
                            return Ok(GuardedStop::Declined("the sandbox was deleted".to_owned()));
                        };
                        if current.lifecycle != SandboxLifecycle::Stopped.as_str()
                            || current.lifecycle_changed_at < initiated_at
                        {
                            self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                .await;
                            return Ok(GuardedStop::Declined(
                                "the sandbox is no longer started or paused".to_owned(),
                            ));
                        }
                        if let Err((should_clear, refusal)) =
                            check_stop_external_fences(pool, sandbox, initiated_at, dispatched_at)
                                .await
                        {
                            if should_clear {
                                self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                    .await;
                            }
                            return Ok(refusal);
                        }
                        if let Err((should_clear, refusal)) =
                            verify_converged_initiated_stop_execution(
                                pool,
                                sandbox,
                                &initiated,
                                &current,
                                "the sandbox left the execution where this campaign stopped it",
                            )
                            .await
                        {
                            if should_clear {
                                self.clear_initiated_stop(recorded_item, sandbox, Some(&initiated))
                                    .await;
                            }
                            return Ok(refusal);
                        }
                        (current.lifecycle_changed_at, current.pinned_stop_disk)
                    }
                };
                if in_flight_external_kill {
                    // Ensure `stopped_at` settles strictly after the in-flight kill's
                    // `completed_at` without sleeping (`sandboxes_touch_lifecycle_changed_at`
                    // leaves `lifecycle_changed_at` intact when `lifecycle` stays `'stopped'`).
                    changed_at = sqlx::query_scalar(sql!(
                        "UPDATE sandboxes
                         SET lifecycle_changed_at = $2 + INTERVAL '50 milliseconds'
                         WHERE id = $1
                         RETURNING lifecycle_changed_at"
                    ))
                    .bind(sandbox)
                    .bind(dispatched_at)
                    .fetch_one(pool)
                    .await
                    .expect("advancing stopped_at past in-flight external kill");
                }
                let expected_initiated = initiated.clone();
                let settled_stopped_at = to_jiff(changed_at);
                let stopped_at = if converged_pinned_stop_disk {
                    initiated.dispatched_at.min(settled_stopped_at)
                } else {
                    settled_stopped_at
                };
                (
                    Some(expected_initiated),
                    StopFence {
                        initiated_at: initiated.initiated_at,
                        dispatched_at: initiated.dispatched_at,
                        stopped_at,
                        settled_stopped_at,
                        k8s_stopped_execution: initiated.k8s_stopped_execution,
                    },
                )
            }
        } else {
            (None, StopFence::at(jiff::Timestamp::UNIX_EPOCH))
        };
        self.record_item_stopped_at(
            recorded_item,
            sandbox,
            expected_initiated.as_ref(),
            stop.clone(),
        )
        .await;
        let (on_commit, post_commit_starting, post_deadline_advance, post_error) = {
            let mut state = self.state();
            let on_commit = state.on_stop_commit.remove(&sandbox);
            let post_commit_starting = state.post_stop_commit_starting.remove(&sandbox);
            let advance = state.post_commit_stop_deadline_advances.remove(&sandbox);
            let error = state
                .post_commit_stop_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front);
            (on_commit, post_commit_starting, advance, error)
        };
        if let Some(hook) = on_commit {
            hook();
        }
        if post_commit_starting && let Some(pool) = &self.persist_pool {
            sqlx::query(sql!(
                "UPDATE sandboxes
                 SET lifecycle_changed_at = lifecycle_changed_at - INTERVAL '1 second'
                 WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await
            .expect("aging stopped timestamp before post-stop starting transition");
            set_lifecycle(pool, sandbox, SandboxLifecycle::Starting.as_str())
                .await
                .expect("simulating post-stop starting transition");
        }
        if let Some(advance) = post_deadline_advance {
            pause_and_hang_past_deadline(advance).await;
        }
        if let Some(error) = post_error {
            return Err(error);
        }
        Ok(GuardedStop::Stopped(stop))
    }
}
