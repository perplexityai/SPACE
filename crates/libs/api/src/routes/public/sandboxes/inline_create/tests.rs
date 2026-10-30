use super::*;

#[test]
fn the_inline_wait_defaults_and_is_capped() {
    assert_eq!(inline_wait(None), DEFAULT_NATIVE_CREATE_INLINE_WAIT);
    assert_eq!(inline_wait(Some("nope")), DEFAULT_NATIVE_CREATE_INLINE_WAIT);
    assert_eq!(inline_wait(Some("250")), Duration::from_millis(250));
    assert_eq!(inline_wait(Some("0")), Duration::ZERO);
    assert_eq!(
        inline_wait(Some("999999999")),
        MAX_NATIVE_CREATE_INLINE_WAIT
    );
}

#[cfg(feature = "kubernetes")]
mod postgres {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pplx_space_common_core::sql;
    use pplx_space_db_test_fixtures::TestDbResult;
    use pplx_space_public_api::{OperationId, OperationKind, OperationState, PublicErrorCode};
    use tokio::sync::Notify;

    use pplx_space_common_svc::DrainSignal;
    use tokio_util::task::TaskTracker;

    use super::*;
    use crate::controllers::sandbox::SandboxController;
    use crate::inline_operations::InlineOperationsConfig;
    use crate::public_operation_driver::{OperationExecutionConfig, OperationExecutor};
    use crate::public_operation_driver::{OperationExecutionResult, drive_claimed_operation};
    use crate::public_operations::{
        ClaimLimits, ClaimPublicOperations, PublicOperationRequeueReason,
    };
    use crate::routes::public::sandboxes::NativeCreatePoolClaims;
    use crate::routes::public::sandboxes::admission_test_fixture::AdmissionPoolFixture;

    /// What a scripted inline execution does once it starts.
    #[derive(Clone, Copy)]
    enum Script {
        Succeed,
        /// A terminal failure, recorded on the first attempt.
        Fail,
        /// A pool-slot-release wait: workflow progress, retried later.
        Retry,
        /// A transient retry after handing its wait back, as a fenced
        /// restore does under the dispatcher.
        DeferredRetry,
        Panic,
    }

    struct ScriptState {
        script: Script,
        calls: AtomicUsize,
        started: Notify,
        /// When set, the execution waits here after starting.
        gate: Option<Notify>,
    }

    #[derive(Clone)]
    struct ScriptedCreate(Arc<ScriptState>);

    impl ScriptedCreate {
        fn new(script: Script) -> Self {
            Self::build(script, false)
        }

        fn gated(script: Script) -> Self {
            Self::build(script, true)
        }

        fn build(script: Script, gated: bool) -> Self {
            Self(Arc::new(ScriptState {
                script,
                calls: AtomicUsize::new(0),
                started: Notify::new(),
                gate: gated.then(Notify::new),
            }))
        }

        fn calls(&self) -> usize {
            self.0.calls.load(Ordering::SeqCst)
        }

        fn open_gate(&self) {
            self.0.gate.as_ref().expect("gated script").notify_one();
        }

        fn inline(&self, wait: Duration) -> InlineCreate {
            self.inline_on(
                wait,
                runner(
                    TaskTracker::new(),
                    DrainSignal::new(),
                    OperationExecutionConfig::default(),
                    16,
                ),
            )
        }

        fn inline_on(&self, wait: Duration, runner: InlineOperations) -> InlineCreate {
            InlineCreate {
                wait,
                executor: Arc::new(self.clone()),
                runner,
            }
        }
    }

    /// A process's inline execution with `max_concurrent` permits, as
    /// `AppState` builds it for the API.
    fn runner(
        tasks: TaskTracker,
        drain_signal: DrainSignal,
        execution: OperationExecutionConfig,
        max_concurrent: usize,
    ) -> InlineOperations {
        InlineOperations::new(
            InlineOperationsConfig { max_concurrent },
            tasks,
            drain_signal,
            execution,
            true,
        )
    }

    impl InlineCreateExecutor for ScriptedCreate {
        fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution {
            let state = Arc::clone(&self.0);
            Box::pin(async move {
                state.calls.fetch_add(1, Ordering::SeqCst);
                state.started.notify_one();
                if let Some(gate) = &state.gate {
                    gate.notified().await;
                }
                match state.script {
                    Script::Succeed => crate::public_sandbox_operations::create_success_result(
                        operation
                            .target_id
                            .as_deref()
                            .expect("create has a target")
                            .parse()
                            .expect("target is a sandbox ID"),
                        pplx_space_common_core::types::SandboxLifecycle::Started,
                        "sandbox created",
                    )
                    .into(),
                    Script::Fail => OperationExecutionResult {
                        state: OperationState::Failed,
                        progress_message: "sandbox create failed".to_string(),
                        result_json: None,
                        terminal_error_code: Some(PublicErrorCode::CapacityUnavailable),
                        terminal_error_json: Some(serde_json::json!({ "message": "no capacity" })),
                        retry_after: None,
                        requeue_reason: PublicOperationRequeueReason::TransientFailure,
                    },
                    Script::Retry => OperationExecutionResult {
                        state: OperationState::Queued,
                        progress_message: "waiting for the pool to collect a slot".to_string(),
                        result_json: None,
                        terminal_error_code: None,
                        terminal_error_json: None,
                        retry_after: Some(Duration::from_secs(2)),
                        requeue_reason: PublicOperationRequeueReason::WorkflowProgress,
                    },
                    Script::DeferredRetry => {
                        // Records the deferral only under a deferral scope.
                        crate::retry_deferral::defer_retry_wait("test");
                        OperationExecutionResult {
                            state: OperationState::Queued,
                            progress_message: "deferring restore".to_string(),
                            result_json: None,
                            terminal_error_code: None,
                            terminal_error_json: None,
                            retry_after: Some(Duration::from_secs(5)),
                            requeue_reason: PublicOperationRequeueReason::TransientFailure,
                        }
                    }
                    Script::Panic => panic!("inline execution task dies"),
                }
            })
        }
    }

    /// The durable state the dispatcher and a poller see.
    #[derive(Debug, PartialEq, Eq)]
    struct OperationRow {
        state: String,
        attempts: i32,
        claim_generation: i64,
    }

    async fn operation_row(
        fixture: &AdmissionPoolFixture,
        operation_id: OperationId,
    ) -> TestDbResult<OperationRow> {
        let (state, attempts, claim_generation): (String, i32, i64) = sqlx::query_as(sql!(
            "SELECT state, attempts, claim_generation
                 FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&fixture.db.pool)
        .await?;
        Ok(OperationRow {
            state,
            attempts,
            claim_generation,
        })
    }

    async fn event_types(
        fixture: &AdmissionPoolFixture,
        operation_id: OperationId,
    ) -> TestDbResult<Vec<String>> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT event_type FROM public_operation_events
             WHERE operation_id = $1 ORDER BY sequence"
        ))
        .bind(operation_id.as_uuid())
        .fetch_all(&fixture.db.pool)
        .await?)
    }

    /// One dispatcher claim pass at its default staleness.
    async fn dispatcher_claim(
        fixture: &AdmissionPoolFixture,
    ) -> TestDbResult<crate::public_operations::PublicOperationClaim> {
        Ok(PublicOperationStore::new(fixture.db.pool.clone())
            .claim_operations(ClaimPublicOperations {
                limits: ClaimLimits::any(16),
                claim_stale_after: OperationExecutionConfig::default().claim_stale_after,
                claimable_kinds: &[OperationKind::CreateSandbox],
            })
            .await?)
    }

    /// Age the operation's claim past the dispatcher's staleness window, as
    /// a dead claimant leaves it.
    async fn age_claim(
        fixture: &AdmissionPoolFixture,
        operation_id: OperationId,
    ) -> TestDbResult<()> {
        sqlx::query(sql!(
            "UPDATE public_operations
             SET claimed_at = NOW() - INTERVAL '10 minutes'
             WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .execute(&fixture.db.pool)
        .await?;
        Ok(())
    }

    async fn wait_for_state(
        fixture: &AdmissionPoolFixture,
        operation_id: OperationId,
        state: &str,
    ) -> TestDbResult<OperationRow> {
        for _ in 0..200 {
            let row = operation_row(fixture, operation_id).await?;
            if row.state == state {
                return Ok(row);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("operation never reached {state}");
    }

    fn claims<'a>(
        controller: &'a SandboxController,
        inline: Option<InlineCreate>,
    ) -> Option<NativeCreatePoolClaims<'a>> {
        Some(NativeCreatePoolClaims { controller, inline })
    }

    fn executor_tags(
        counters: &[pplx_space_metrics::test_capture::CapturedCounter],
        name: &str,
    ) -> Vec<String> {
        counters
            .iter()
            .filter(|counter| counter.name == name)
            .flat_map(|counter| counter.tags.iter())
            .filter(|tag| tag.starts_with("executor:"))
            .cloned()
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_hit_runs_inline_and_is_terminal_on_the_first_read() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Succeed);
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-hit",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        let counters = capture.finish();

        assert_eq!(script.calls(), 1);
        // The response carries the finished operation, so the client needs
        // no poll; it matches what a poll would read.
        let finished = response
            .operation
            .as_ref()
            .expect("a finished create answers with its record");
        assert_eq!(finished.id, response.operation_id);
        assert_eq!(finished.state, OperationState::Succeeded);
        assert_eq!(
            Some(finished),
            PublicOperationStore::new(fixture.db.pool.clone())
                .fetch_operation(fixture.org_id, response.operation_id)
                .await?
                .as_ref()
        );
        let row = operation_row(&fixture, response.operation_id).await?;
        assert_eq!(
            row,
            OperationRow {
                state: "succeeded".to_string(),
                attempts: 1,
                claim_generation: 1,
            }
        );
        assert_eq!(
            event_types(&fixture, response.operation_id).await?,
            vec![
                "operation.created",
                "operation.claimed",
                "operation.completed"
            ]
        );
        // The executor saw the create the request recorded, under the
        // slot's reserved ID and with the claim to adopt.
        let payload = fixture.payload(response.operation_id).await?;
        assert_eq!(
            payload.sandbox_id,
            pplx_space_public_api::PublicSandboxId::from(fixture.slots[0].reserved_sandbox_id)
        );
        assert!(payload.pool_claim.is_some());
        // Nothing is left for the dispatcher, even once it would call a
        // claim stale.
        age_claim(&fixture, response.operation_id).await?;
        assert!(dispatcher_claim(&fixture).await?.operations.is_empty());
        assert_eq!(
            executor_tags(&counters, "operations.completed"),
            vec!["executor:inline"]
        );
        assert_eq!(
            executor_tags(&counters, "operations.claimed"),
            vec!["executor:inline"]
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_terminal_failure_inline_answers_with_the_failed_record() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Fail);
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-fail",
                fixture.request(),
            )
            .await
            .expect("a failed create is still accepted");

        let finished = response
            .operation
            .expect("a failed create answers with its record");
        assert_eq!(finished.id, response.operation_id);
        assert_eq!(finished.state, OperationState::Failed);
        assert_eq!(
            finished.terminal_error_code,
            Some(PublicErrorCode::CapacityUnavailable)
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn past_the_deadline_the_create_answers_and_finishes_in_the_background()
    -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::gated(Script::Succeed);
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_millis(20)))),
                "inline-deadline",
                fixture.request(),
            )
            .await
            .expect("create answers at the deadline");

        // Answered while the execution still holds its claim, without a
        // record: the client polls as before.
        assert_eq!(response.operation, None);
        let row = operation_row(&fixture, response.operation_id).await?;
        assert_eq!(row.state, "running");
        assert!(
            dispatcher_claim(&fixture).await?.operations.is_empty(),
            "a live inline claim is not the dispatcher's"
        );

        script.open_gate();
        let row = wait_for_state(&fixture, response.operation_id, "succeeded").await?;
        assert_eq!((row.attempts, row.claim_generation), (1, 1));
        assert_eq!(script.calls(), 1, "a single execution");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_client_disconnect_does_not_stop_the_execution() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::gated(Script::Succeed);
        let inline = script.inline(Duration::from_secs(5));
        let request = fixture.create_with(
            claims(&controller, Some(inline)),
            "inline-disconnect",
            fixture.request(),
        );
        // Drop the request future once the execution has started, as axum
        // does when the client goes away.
        tokio::select! {
            _ = request => panic!("the request should still be waiting"),
            () = script.0.started.notified() => {}
        }
        let operation_id: uuid::Uuid = sqlx::query_scalar(sql!(
            "SELECT operation_id FROM public_operations WHERE org_id = $1"
        ))
        .bind(fixture.org_id)
        .fetch_one(&fixture.db.pool)
        .await?;
        let operation_id = OperationId::from_uuid(operation_id);

        script.open_gate();
        wait_for_state(&fixture, operation_id, "succeeded").await?;
        assert_eq!(script.calls(), 1);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_retryable_outcome_is_handed_to_the_dispatcher() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Retry);
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-retry",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        let counters = capture.finish();

        assert_eq!(script.calls(), 1, "no inline retry loop");
        assert_eq!(response.operation, None, "a released retry is not finished");
        let row = operation_row(&fixture, response.operation_id).await?;
        assert_eq!(row.state, "queued");
        assert!(
            counters
                .iter()
                .any(|counter| counter.name == "operations.requeued"
                    && counter
                        .tags
                        .iter()
                        .any(|tag| tag == "reason:workflow_progress")),
            "released as the dispatcher releases its own retries"
        );
        // Not before its retry time; then the dispatcher takes it, with the
        // same payload and claim.
        assert!(dispatcher_claim(&fixture).await?.operations.is_empty());
        sqlx::query(sql!(
            "UPDATE public_operations SET next_attempt_at = NOW() WHERE operation_id = $1"
        ))
        .bind(response.operation_id.as_uuid())
        .execute(&fixture.db.pool)
        .await?;
        let claim = dispatcher_claim(&fixture).await?;
        assert_eq!(claim.operations.len(), 1);
        assert_eq!(claim.operations[0].id, response.operation_id);
        assert_eq!(claim.operations[0].claim_generation, 2);
        Ok(())
    }

    /// Drive a dispatcher re-claim of `operation_id` through the real create
    /// executor, as the dispatcher's own executor does.
    async fn redrive(
        fixture: &AdmissionPoolFixture,
        controller: &SandboxController,
        operation_id: OperationId,
    ) -> TestDbResult<OperationExecutionResult> {
        let claim = dispatcher_claim(fixture).await?;
        assert_eq!(claim.operations.len(), 1, "the dispatcher reclaims it");
        let operation = claim.operations[0].clone();
        assert_eq!(operation.id, operation_id);
        let executor =
            CreateSandboxOperationExecutor::new(fixture.db.pool.clone(), controller.clone());
        let result: OperationExecutionResult = executor.execute(&operation).await.into();
        drive_claimed_operation(
            PublicOperationStore::new(fixture.db.pool.clone()),
            claim.claim_token,
            OperationExecutionConfig::default(),
            operation,
            OperationExecutor::Dispatcher,
            std::future::ready(result.clone()),
        )
        .await?;
        Ok(result)
    }

    /// The inline task dies before it adopts the slot (here, as soon as it
    /// starts): the claim goes stale and the dispatcher reclaims the same
    /// operation with the same claim, so the admitted-claim path adopts the
    /// slot that is still claimed for it.
    #[tokio::test(flavor = "current_thread")]
    async fn an_inline_task_that_dies_before_adoption_is_reclaimed_with_its_slot()
    -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Panic);
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-dies",
                fixture.request(),
            )
            .await
            .expect("a dead inline task still answers");
        assert_eq!(script.calls(), 1);
        assert_eq!(response.operation, None);

        let row = operation_row(&fixture, response.operation_id).await?;
        assert_eq!(row.state, "running");
        assert!(
            dispatcher_claim(&fixture).await?.operations.is_empty(),
            "not until the claim is stale"
        );
        age_claim(&fixture, response.operation_id).await?;
        let claim = dispatcher_claim(&fixture).await?;
        assert_eq!(claim.operations.len(), 1);
        let reclaimed = &claim.operations[0];
        assert_eq!(reclaimed.id, response.operation_id);
        assert_eq!(reclaimed.claim_generation, 2);
        let payload = fixture.payload(response.operation_id).await?;
        let pool_claim = payload.pool_claim.expect("the claim survives");
        assert_eq!(
            fixture.slot_state(fixture.slots[0].id).await?,
            ("claimed".to_string(), Some(pool_claim.claim_id)),
            "the slot is still claimed for this create, for the dispatcher to adopt"
        );
        Ok(())
    }

    /// An inline task stopped after adopting the slot but before recording
    /// the result (the API process going away): its claim goes stale, the
    /// dispatcher's re-drive confirms the existing sandbox, and the stopped
    /// execution can no longer record anything. Once recorded, nothing is
    /// reclaimable.
    #[tokio::test(flavor = "current_thread")]
    async fn an_inline_task_stopped_after_adoption_converges_through_the_dispatcher()
    -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::gated(Script::Succeed);
        // Record the create claimed, without running it from the route.
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::ZERO))),
                "inline-abort",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        // The zero wait answered while the gated task was still running.
        // Hold it there: aging the claim below stands in for the renewals a
        // stopped process no longer sends.
        script.0.started.notified().await;
        let operation_id = response.operation_id;
        let row = operation_row(&fixture, operation_id).await?;
        assert_eq!(row.state, "running");

        // After an adoption the task did not record: the sandbox row is
        // there under the claimed ID, and the re-drive confirms it without
        // building a second one.
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, network_policy_generation
             )
             VALUES ($1, $2, $3, 'k8s', 'started', 'unplaced', NULL, 1)"
        ))
        .bind(fixture.slots[0].reserved_sandbox_id)
        .bind(fixture.user_id)
        .bind(fixture.org_id)
        .execute(&fixture.db.pool)
        .await?;
        age_claim(&fixture, operation_id).await?;
        let result = redrive(&fixture, &controller, operation_id).await?;
        assert_eq!(result.state, OperationState::Succeeded, "{result:?}");
        assert_eq!(
            result.progress_message,
            "sandbox already exists for create operation"
        );
        let row = operation_row(&fixture, operation_id).await?;
        assert_eq!(row.state, "succeeded");
        let rows: i64 =
            sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes WHERE org_id = $1"))
                .bind(fixture.org_id)
                .fetch_one(&fixture.db.pool)
                .await?;
        assert_eq!(rows, 1);

        // The old inline execution finishing late cannot record over it.
        script.open_gate();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let late = operation_row(&fixture, operation_id).await?;
        assert_eq!(late.claim_generation, 2);
        assert_eq!(late.state, "succeeded");

        // After the record, nothing is reclaimable.
        age_claim(&fixture, operation_id).await?;
        assert!(dispatcher_claim(&fixture).await?.operations.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn misses_and_the_kill_switch_stay_queued_for_the_dispatcher() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Succeed);

        // Kill switch off: claimed at admission, but recorded queued.
        let off = fixture
            .create_with(claims(&controller, None), "inline-off", fixture.request())
            .await
            .expect("create is recorded");
        assert_eq!(off.operation, None);
        let row = operation_row(&fixture, off.operation_id).await?;
        assert_eq!((row.state.as_str(), row.attempts), ("queued", 0));
        assert!(
            fixture
                .payload(off.operation_id)
                .await?
                .pool_claim
                .is_some()
        );

        // A miss (the only slot is taken) never runs inline.
        let miss = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-miss",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        assert_eq!(miss.operation, None);
        let row = operation_row(&fixture, miss.operation_id).await?;
        assert_eq!((row.state.as_str(), row.attempts), ("queued", 0));
        assert!(
            fixture
                .payload(miss.operation_id)
                .await?
                .pool_claim
                .is_none()
        );

        // Disabled claiming never runs inline either.
        let disabled = fixture
            .create_with(None, "inline-disabled", fixture.request())
            .await
            .expect("create is recorded");
        assert_eq!(disabled.operation, None);
        let row = operation_row(&fixture, disabled.operation_id).await?;
        assert_eq!(row.state, "queued");
        assert_eq!(script.calls(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_ineligible_create_never_runs_inline() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Succeed);
        // An org that requires volume encryption never takes a warm slot.
        fixture.enable_org_encryption().await?;
        let request = fixture.request();
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-ineligible",
                request,
            )
            .await
            .expect("create is recorded");
        let row = operation_row(&fixture, response.operation_id).await?;
        assert_eq!(row.state, "queued");
        assert_eq!(script.calls(), 0);
        assert_eq!(fixture.slot_count("ready").await?, 1);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_idempotent_replay_neither_claims_nor_runs_again() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(2).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::Succeed);
        let inline = script.inline(Duration::from_secs(5));
        let first = fixture
            .create_with(
                claims(&controller, Some(inline.clone())),
                "inline-replay",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        let replay = fixture
            .create_with(
                claims(&controller, Some(inline)),
                "inline-replay",
                fixture.request(),
            )
            .await
            .expect("a replay answers");

        assert!(first.operation.is_some());
        // A replay answers as it always has.
        assert_eq!(replay.operation, None);
        assert_eq!(replay.operation_id, first.operation_id);
        assert_eq!(replay.sandbox_id, first.sandbox_id);
        assert_eq!(script.calls(), 1);
        assert_eq!(fixture.create_operations().await?, 1);
        assert_eq!(fixture.slot_count("ready").await?, 1);

        // A conflicting body under the same key is still a conflict.
        let mut other = fixture.request();
        other.name = Some("different".parse().expect("valid name"));
        let conflict = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-replay",
                other,
            )
            .await;
        conflict.expect_err("a different body under the same key conflicts");
        assert_eq!(script.calls(), 1);
        Ok(())
    }

    /// Shutdown waits for a create already running inline, and once the
    /// process drains a create that wins a slot is left to the dispatcher.
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_waits_for_a_running_inline_create_and_queues_new_ones() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(2).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::gated(Script::Succeed);
        let tasks = TaskTracker::new();
        let drain_signal = DrainSignal::new();
        let inline = script.inline_on(
            Duration::ZERO,
            runner(
                tasks.clone(),
                drain_signal.clone(),
                OperationExecutionConfig::default(),
                16,
            ),
        );
        let running = fixture
            .create_with(
                claims(&controller, Some(inline.clone())),
                "inline-before-drain",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        script.0.started.notified().await;

        // As `run` shuts the API down: drain, then close the tracker.
        drain_signal.begin();
        tasks.close();
        let queued = fixture
            .create_with(
                claims(&controller, Some(inline)),
                "inline-after-drain",
                fixture.request(),
            )
            .await
            .expect("a create while draining is recorded");
        assert_eq!(queued.operation, None);
        let row = operation_row(&fixture, queued.operation_id).await?;
        assert_eq!((row.state.as_str(), row.attempts), ("queued", 0));
        assert!(
            fixture
                .payload(queued.operation_id)
                .await?
                .pool_claim
                .is_some(),
            "it keeps its slot for the dispatcher to adopt"
        );
        assert_eq!(script.calls(), 1);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), tasks.wait())
                .await
                .is_err(),
            "shutdown waits for the inline create"
        );
        script.open_gate();
        tokio::time::timeout(Duration::from_secs(5), tasks.wait())
            .await
            .expect("the inline create finishes within the grace");
        let row = operation_row(&fixture, running.operation_id).await?;
        assert_eq!(row.state, "succeeded");
        Ok(())
    }

    /// An inline create renews its claim as often as the configured
    /// staleness asks, as the dispatcher's executions do.
    #[tokio::test(flavor = "current_thread")]
    async fn an_inline_create_renews_its_claim_at_the_configured_staleness() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::gated(Script::Succeed);
        let inline = script.inline_on(
            Duration::ZERO,
            runner(
                TaskTracker::new(),
                DrainSignal::new(),
                OperationExecutionConfig {
                    claim_stale_after: Duration::from_millis(60),
                    ..OperationExecutionConfig::default()
                },
                16,
            ),
        );
        let response = fixture
            .create_with(
                claims(&controller, Some(inline)),
                "inline-renewal",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        script.0.started.notified().await;
        let claimed_at = || async {
            sqlx::query_scalar::<_, time::OffsetDateTime>(sql!(
                "SELECT claimed_at FROM public_operations WHERE operation_id = $1"
            ))
            .bind(response.operation_id.as_uuid())
            .fetch_one(&fixture.db.pool)
            .await
        };
        let first = claimed_at().await?;
        let mut renewed = false;
        for _ in 0..200 {
            if claimed_at().await? > first {
                renewed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(renewed, "the claim is renewed while the create runs");
        script.open_gate();
        wait_for_state(&fixture, response.operation_id, "succeeded").await?;
        Ok(())
    }

    /// A step that hands its retry wait back requeues the create without
    /// spending an attempt, as under the dispatcher.
    #[tokio::test(flavor = "current_thread")]
    async fn an_inline_create_hands_a_deferred_retry_wait_back() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::new(Script::DeferredRetry);
        let response = fixture
            .create_with(
                claims(&controller, Some(script.inline(Duration::from_secs(5)))),
                "inline-deferral",
                fixture.request(),
            )
            .await
            .expect("create is recorded");

        assert_eq!(script.calls(), 1);
        let row = operation_row(&fixture, response.operation_id).await?;
        assert_eq!(
            (row.state.as_str(), row.attempts),
            ("queued", 0),
            "a deferred retry gives its attempt back"
        );
        Ok(())
    }

    /// A create that wins a slot while every inline permit is held is
    /// recorded queued, with its slot, for the dispatcher; once a permit
    /// frees, the next one runs inline again.
    #[tokio::test(flavor = "current_thread")]
    async fn a_create_that_finds_no_free_permit_is_queued_for_the_dispatcher() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(3).await?;
        let controller = fixture.controller();
        let script = ScriptedCreate::gated(Script::Succeed);
        let one_permit = runner(
            TaskTracker::new(),
            DrainSignal::new(),
            OperationExecutionConfig::default(),
            1,
        );
        let inline = script.inline_on(Duration::ZERO, one_permit);
        let holding = fixture
            .create_with(
                claims(&controller, Some(inline.clone())),
                "inline-holds-the-permit",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        script.0.started.notified().await;

        let saturated = fixture
            .create_with(
                claims(&controller, Some(inline.clone())),
                "inline-saturated",
                fixture.request(),
            )
            .await
            .expect("a create on a saturated pod is recorded");
        let row = operation_row(&fixture, saturated.operation_id).await?;
        assert_eq!((row.state.as_str(), row.attempts), ("queued", 0));
        assert!(
            fixture
                .payload(saturated.operation_id)
                .await?
                .pool_claim
                .is_some(),
            "it keeps its slot for the dispatcher to adopt"
        );
        assert_eq!(script.calls(), 1);

        script.open_gate();
        wait_for_state(&fixture, holding.operation_id, "succeeded").await?;
        let freed = fixture
            .create_with(
                claims(&controller, Some(inline)),
                "inline-after-the-permit-frees",
                fixture.request(),
            )
            .await
            .expect("create is recorded");
        script.0.started.notified().await;
        script.open_gate();
        wait_for_state(&fixture, freed.operation_id, "succeeded").await?;
        assert_eq!(script.calls(), 2);
        Ok(())
    }
}
