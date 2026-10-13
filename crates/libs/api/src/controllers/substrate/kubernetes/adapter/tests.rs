use pplx_space_common_core::types::LocalArtifactId;

use super::*;
use pplx_space_sbx_mgr_client::proto::{PrepareTeardownSuspendResponse, TeardownSuspendOutcome};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::SubscriberExt as _;

const PENDING_REASON: &str = "sandbox record is not present on this node";

#[derive(Clone, Default)]
struct SuspendDiagnosticCapture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SuspendDiagnosticCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if !event
            .metadata()
            .fields()
            .iter()
            .any(|field| field.name() == "poll_elapsed_ms")
        {
            return;
        }
        struct Fields(BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().to_owned(), format!("{value:?}"));
            }
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

async fn poll_suspend_responses(
    responses: Vec<Result<PrepareTeardownSuspendResponse, tonic::Status>>,
    budget: Duration,
) -> (
    Result<super::suspend::GenerationSuspendOutcome, SubstrateError>,
    Vec<BTreeMap<String, String>>,
) {
    let sandbox_id = SandboxId::new();
    let generation = ExecutionGeneration::new(1).unwrap();
    let mut responses = std::collections::VecDeque::from(responses);
    let capture = SuspendDiagnosticCapture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let result = poll_generation_suspend(
        move |request| {
            assert_eq!(request.get_ref().sandbox_id, sandbox_id.to_string());
            assert_eq!(request.get_ref().execution_generation, generation.get());
            let response = responses.pop_front();
            async move {
                match response {
                    Some(response) => response.map(tonic::Response::new),
                    None => std::future::pending().await,
                }
            }
        },
        sandbox_id,
        generation,
        "http://node-api",
        Instant::now() + budget,
        budget,
    )
    .await;
    let summaries: Vec<_> = capture
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|fields| fields.contains_key("poll_budget_ms"))
        .cloned()
        .collect();
    for summary in &summaries {
        assert_eq!(summary["sandbox_id"], sandbox_id.to_string());
        assert_eq!(summary["execution_generation"], "1");
        assert_eq!(summary["node_api_endpoint"], "\"http://node-api\"");
    }
    (result, summaries)
}

fn pending_suspend(reason: Option<&str>) -> Result<PrepareTeardownSuspendResponse, tonic::Status> {
    Ok(PrepareTeardownSuspendResponse {
        outcome: TeardownSuspendOutcome::Pending as i32,
        reason: reason.map(str::to_owned),
        published_artifact: None,
    })
}

#[tokio::test(start_paused = true)]
async fn generation_suspend_timeout_distinguishes_pending_transport_and_no_response() {
    for (responses, pending, reasonless, retries, reason, previous_reason, code) in [
        (vec![], 0, 0, 0, None, None, "None"),
        (
            vec![pending_suspend(Some(PENDING_REASON))],
            1,
            0,
            0,
            Some(PENDING_REASON),
            Some(PENDING_REASON),
            "None",
        ),
        (
            vec![Err(tonic::Status::unavailable("node restarting"))],
            0,
            0,
            1,
            None,
            None,
            "Some(Unavailable)",
        ),
        (
            vec![
                pending_suspend(Some(PENDING_REASON)),
                Err(tonic::Status::deadline_exceeded("RPC deadline")),
            ],
            1,
            0,
            1,
            Some(PENDING_REASON),
            Some(PENDING_REASON),
            "Some(DeadlineExceeded)",
        ),
        // A reasonless tail remains distinguishable from a wholly reasonless wait.
        (
            vec![pending_suspend(Some(PENDING_REASON)), pending_suspend(None)],
            2,
            1,
            0,
            None,
            Some(PENDING_REASON),
            "None",
        ),
        (vec![pending_suspend(None)], 1, 1, 0, None, None, "None"),
    ] {
        let (result, summaries) = poll_suspend_responses(responses, Duration::from_secs(2)).await;
        let error = result.unwrap_err();
        assert!(matches!(error, SubstrateError::Timeout { .. }));
        // Operator wire text and counters must not leak into client errors/events.
        assert!(!error.to_string().contains(PENDING_REASON));
        assert!(!error.to_string().contains("pending_responses"));
        assert_eq!(summaries.len(), 1);
        let summary = &summaries[0];
        assert_eq!(summary["pending_responses"], pending.to_string());
        assert_eq!(
            summary["reasonless_pending_responses"],
            reasonless.to_string()
        );
        assert_eq!(summary["retryable_errors"], retries.to_string());
        assert_eq!(summary["last_pending_reason"], format!("{reason:?}"));
        assert_eq!(
            summary["last_reasoned_pending_reason"],
            format!("{previous_reason:?}")
        );
        assert_eq!(summary["last_retryable_code"], code);
        assert_eq!(summary["poll_elapsed_ms"], "2000");
        assert_eq!(summary["poll_budget_ms"], "2000");
    }
    let (result, summaries) = poll_suspend_responses(vec![], Duration::from_millis(1)).await;
    assert!(matches!(result, Err(SubstrateError::Timeout { .. })));
    assert_eq!(summaries[0]["poll_budget_ms"], "1");
    assert_eq!(summaries[0]["poll_elapsed_ms"], "1");
    assert_eq!(summaries[0]["pending_responses"], "0");
}

#[tokio::test(start_paused = true)]
async fn generation_suspend_bounds_retained_reason_on_a_utf8_boundary() {
    let reason = "界".repeat(512);
    let (result, summaries) =
        poll_suspend_responses(vec![pending_suspend(Some(&reason))], Duration::from_secs(2)).await;
    assert!(matches!(result, Err(SubstrateError::Timeout { .. })));
    let diagnostic = &summaries[0]["last_pending_reason"];
    assert!(diagnostic.contains(&"界".repeat(170)));
    assert!(!diagnostic.contains(&"界".repeat(171)));
    assert!(diagnostic.contains("[truncated, original_bytes=1536]"));
    assert!(!result.unwrap_err().to_string().contains('界'));
}

// The budget-derived timeout messages print the budget with `Duration`'s
// `Debug`. For each production default that is byte-identical to the earlier
// `{}s` of `as_secs()`; a test's sub-second budget no longer reads `0s`.
#[test]
fn create_ready_timeout_message_names_the_budget() {
    let default = SubstrateBudgets::DEFAULT.create_ready;
    for (budget, expected) in [
        (
            default,
            format!(
                "sandbox create did not reach readiness within {}s",
                default.as_secs()
            ),
        ),
        (
            Duration::from_millis(500),
            "sandbox create did not reach readiness within 500ms".to_string(),
        ),
    ] {
        match create_ready_timeout_error(budget) {
            SubstrateError::Timeout {
                message,
                phase: None,
            } => assert_eq!(message, expected),
            other => panic!("unexpected create timeout error {other:?}"),
        }
    }
    assert_eq!(
        format!("{default:?}"),
        "135s",
        "the production create budget's rendering"
    );
}

#[tokio::test(start_paused = true)]
async fn generation_suspend_timeout_message_names_the_budget() {
    let default = SubstrateBudgets::DEFAULT.suspend_durability;
    let sandbox_id = SandboxId::new();
    let prefix = format!(
        "generation-bound suspend for sandbox {sandbox_id} generation 1 did not become durable within"
    );
    for (budget, expected) in [
        (default, format!("{prefix} {}s", default.as_secs())),
        (Duration::from_millis(600), format!("{prefix} 600ms")),
    ] {
        let result = poll_generation_suspend(
            |_request| std::future::pending(),
            sandbox_id,
            ExecutionGeneration::new(1).unwrap(),
            "http://node-api",
            Instant::now() + budget,
            budget,
        )
        .await;
        match result {
            Err(SubstrateError::Timeout {
                message,
                phase: None,
            }) => assert_eq!(message, expected),
            other => panic!("unexpected suspend timeout result {other:?}"),
        }
    }
    assert_eq!(
        format!("{default:?}"),
        "300s",
        "the production suspend budget's rendering"
    );
}

#[tokio::test(start_paused = true)]
async fn generation_suspend_polling_preserves_success_and_terminal_failure() {
    let artifact_id = LocalArtifactId::new();
    let durable = PrepareTeardownSuspendResponse {
        outcome: TeardownSuspendOutcome::Durable as i32,
        reason: None,
        published_artifact: Some(pplx_space_sbx_mgr_client::proto::PublishedArtifact {
            local_artifact_id: artifact_id.to_string(),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
            origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
            snapshot_name: None,
        }),
    };
    let (result, summaries) = poll_suspend_responses(
        vec![
            pending_suspend(Some(PENDING_REASON)),
            Err(tonic::Status::unavailable("node restarting")),
            Ok(durable),
        ],
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        result.unwrap(),
        super::suspend::GenerationSuspendOutcome::Durable(PublishedLocalArtifact {
            id: artifact_id,
            kind: ArtifactKind::Full,
        })
    );
    assert!(summaries.is_empty());

    // A generation-fenced node delete is an outcome for the caller to resolve
    // against the control plane's snapshot record, not a poll failure.
    let (result, summaries) = poll_suspend_responses(
        vec![
            pending_suspend(Some(PENDING_REASON)),
            Ok(PrepareTeardownSuspendResponse {
                outcome: TeardownSuspendOutcome::Deleted as i32,
                reason: None,
                published_artifact: None,
            }),
        ],
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        result.unwrap(),
        super::suspend::GenerationSuspendOutcome::NodeDeleted
    );
    assert!(summaries.is_empty());

    let mut terminal_responses = vec![
        Err(tonic::Status::permission_denied("denied")),
        Err(tonic::Status::not_found("sandbox record no longer exists")),
    ];
    terminal_responses.extend(
        [
            TeardownSuspendOutcome::Superseded,
            TeardownSuspendOutcome::Unspecified,
            TeardownSuspendOutcome::Durable, // Missing artifact must still fail.
        ]
        .map(|outcome| {
            Ok(PrepareTeardownSuspendResponse {
                outcome: outcome as i32,
                reason: None,
                published_artifact: None,
            })
        }),
    );
    for (index, response) in terminal_responses.into_iter().enumerate() {
        let (result, summaries) = poll_suspend_responses(
            vec![
                pending_suspend(Some(PENDING_REASON)),
                Err(tonic::Status::unavailable("node restarting")),
                response,
            ],
            Duration::from_secs(2),
        )
        .await;
        if index < 3 {
            assert!(matches!(result, Err(SubstrateError::Unavailable { .. })));
        } else {
            assert!(matches!(result, Err(SubstrateError::Failed { .. })));
        }
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0]["pending_responses"], "1");
        assert_eq!(summaries[0]["retryable_errors"], "1");
        assert_eq!(
            summaries[0]["last_pending_reason"],
            format!("{:?}", Some(PENDING_REASON))
        );
        assert_eq!(summaries[0]["last_retryable_code"], "Some(Unavailable)");
    }
}

#[tokio::test(start_paused = true)]
async fn generation_suspend_reason_change_logs_are_rate_limited_and_capped() {
    for reasoned in [false, true] {
        let capture = SuspendDiagnosticCapture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        let mut attempts = 0;
        let result = poll_generation_suspend(
            |_| {
                attempts += 1;
                // Alternation continues well beyond the cap; an ordinary
                // reasonless wait should remain silent until the final summary.
                let reason = if reasoned && attempts % 2 == 1 {
                    Some(PENDING_REASON)
                } else {
                    None
                };
                let response = (attempts <= 480).then(|| pending_suspend(reason).unwrap());
                async move {
                    match response {
                        Some(response) => Ok(tonic::Response::new(response)),
                        None => std::future::pending().await,
                    }
                }
            },
            SandboxId::new(),
            ExecutionGeneration::new(1).unwrap(),
            "http://node-api",
            Instant::now() + Duration::from_secs(120),
            Duration::from_secs(120),
        )
        .await;
        assert!(matches!(result, Err(SubstrateError::Timeout { .. })));
        let events = capture.0.lock().unwrap();
        let changes: Vec<_> = events
            .iter()
            .filter(|fields| !fields.contains_key("poll_budget_ms"))
            .collect();
        assert_eq!(changes.len(), if reasoned { 3 } else { 0 });
        for pair in changes.windows(2) {
            let before: u64 = pair[0]["poll_elapsed_ms"].parse().unwrap();
            let after: u64 = pair[1]["poll_elapsed_ms"].parse().unwrap();
            assert!(after - before >= 30_000);
            assert_ne!(
                pair[0]["last_pending_reason"],
                pair[1]["last_pending_reason"]
            );
        }
        assert_eq!(events.len(), changes.len() + 1, "one final failure summary");
    }
}

#[test]
fn pooled_metadata_update_deadline_is_reclaimable() {
    let status = tonic::Status::deadline_exceeded(
        "worker-node-api/update_sandbox deadline exceeded while running RPC",
    );

    assert!(pooled_metadata_update_error_is_reclaimable(&status));
}

#[test]
fn generation_suspend_retries_only_transient_transport_failures() {
    assert!(generation_suspend_rpc_should_retry(
        &tonic::Status::unavailable("node restarting")
    ));
    assert!(generation_suspend_rpc_should_retry(
        &tonic::Status::deadline_exceeded("request timed out")
    ));
    assert!(generation_suspend_rpc_should_retry(
        &tonic::Status::unknown("transport error")
    ));
    assert!(generation_suspend_rpc_should_retry(
        &tonic::Status::cancelled("transport is closing")
    ));
    assert!(!generation_suspend_rpc_should_retry(
        &tonic::Status::cancelled("server rejected operation")
    ));
    assert!(!generation_suspend_rpc_should_retry(
        &tonic::Status::failed_precondition("generation superseded")
    ));
}

#[tokio::test]
async fn disconnected_suspend_waiter_does_not_cancel_continuation() {
    let task_tracker = TaskTracker::new();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_in_task = Arc::clone(&completed);
    let tracker_for_waiter = task_tracker.clone();

    let waiter = tokio::spawn(async move {
        await_suspend_continuation(&tracker_for_waiter, async move {
            started_tx
                .send(())
                .expect("the request waiter must observe continuation startup");
            release_rx
                .await
                .expect("the test must release the suspend continuation");
            completed_in_task.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
    });
    started_rx
        .await
        .expect("suspend continuation must start before disconnect");

    waiter.abort();
    assert!(
        waiter
            .await
            .expect_err("the request waiter must be cancelled")
            .is_cancelled()
    );
    release_tx
        .send(())
        .expect("the suspend continuation must outlive its request waiter");
    task_tracker.close();
    tokio::time::timeout(Duration::from_secs(1), task_tracker.wait())
        .await
        .expect("detached suspend continuation must finish");

    assert!(
        completed.load(std::sync::atomic::Ordering::SeqCst),
        "request cancellation must not cancel suspend finalization"
    );
}

#[test]
fn teardown_suspend_budget_covers_remote_durability_work() {
    assert!(
        TEARDOWN_SUSPEND_DURABILITY_BUDGET > crate::controllers::substrate::NODE_RPC_TIMEOUT,
        "remote snapshot durability can outlive a single node-RPC timeout"
    );
    assert!(
        TEARDOWN_SUSPEND_DURABILITY_BUDGET
            < Duration::from_secs(crate::controllers::substrate::ALB_IDLE_TIMEOUT_SECS),
        "the API must answer before the outer load-balancer timeout"
    );
}

#[test]
fn generation_suspend_requires_the_exact_suspend_artifact() {
    let artifact_id = LocalArtifactId::new();
    let artifact = pplx_space_sbx_mgr_client::proto::PublishedArtifact {
        local_artifact_id: artifact_id.to_string(),
        artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
        origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
        snapshot_name: None,
    };
    let parsed = published_generation_suspend_artifact(
        pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse {
            outcome: pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Durable as i32,
            reason: None,
            published_artifact: Some(artifact),
        },
        "http://node-api",
    )
    .unwrap();
    assert_eq!(parsed.id, artifact_id);
    assert_eq!(parsed.kind, ArtifactKind::Full);

    let error = published_generation_suspend_artifact(
        pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse {
            outcome: pplx_space_sbx_mgr_client::proto::TeardownSuspendOutcome::Durable as i32,
            reason: None,
            published_artifact: None,
        },
        "http://node-api",
    )
    .expect_err("durable without an exact artifact must be rejected");
    assert!(matches!(error, SubstrateError::Failed { .. }));
}

#[test]
fn unconfirmed_suspend_delete_never_authorizes_retirement() {
    for outcome in [
        K8sBlockingDeleteOutcome::TimedOut,
        K8sBlockingDeleteOutcome::ObserveError,
        K8sBlockingDeleteOutcome::PreconditionFailed,
    ] {
        assert!(
            suspend_pod_deletion_confirmation(outcome, "sandbox-pod", 30).is_err(),
            "{outcome:?} must keep the active execution"
        );
    }
    suspend_pod_deletion_confirmation(K8sBlockingDeleteOutcome::Deleted, "sandbox-pod", 30)
        .unwrap();
    suspend_pod_deletion_confirmation(K8sBlockingDeleteOutcome::NotFound, "sandbox-pod", 30)
        .unwrap();
}
