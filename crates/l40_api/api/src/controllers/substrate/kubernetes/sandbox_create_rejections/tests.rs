use k8s_openapi::api::core::v1::{Event, EventSource, ObjectReference};

use super::*;

/// Kubelet's Event message for a node-mgr memory admission rejection.
const MEMORY_REJECTION: &str = "Failed to create pod sandbox: rpc error: code = ResourceExhausted \
desc = failed to start sandbox \"b054b625699f\": failed to create containerd task: failed to \
create shim task: sbx-manager Wake [space-err=ResourceExhausted]: network allocate failed: node \
memory admission refused: node uses 90000 MiB plus 9000 MiB existing Firecracker growth reserve plus new sandbox demand 8320 \
MiB plus reserve 1024 MiB, requiring 108344 MiB above total memory 100000 MiB";

/// Transient node-mgr backpressure shares the gRPC code but clears on retry.
const QUEUE_FULL: &str = "Failed to create pod sandbox: rpc error: code = ResourceExhausted \
desc = failed to create shim task: sbx-manager Wake [space-err=ResourceExhausted]: network \
allocate failed: lifecycle queue full";

const JAILER_TIMEOUT: &str = "Failed to create pod sandbox: rpc error: code = Unknown desc = \
failed to start sandbox \"abc\": failed to create shim task: sbx-manager Wake \
[space-err=Internal]: LaunchSandbox failed: jailer did not start within 5s";

fn event(reason: &str, kind: &str, message: &str) -> Event {
    Event {
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
        involved_object: ObjectReference {
            kind: Some(kind.to_string()),
            name: Some("space-sbx-01a1-g1".to_string()),
            uid: Some("61a90a93".to_string()),
            ..Default::default()
        },
        source: Some(EventSource {
            host: Some("node-a".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn memory_admission_rejection_is_recorded_with_node_mgr_reason() {
    let (pod, rejection) = rejection_from_event(&event(
        FAILED_CREATE_POD_SANDBOX_REASON,
        "Pod",
        MEMORY_REJECTION,
    ))
    .expect("rejection");
    assert_eq!(pod, "space-sbx-01a1-g1");
    assert_eq!(rejection.pod_uid.as_deref(), Some("61a90a93"));
    assert_eq!(rejection.node_name.as_deref(), Some("node-a"));
    assert!(
        rejection.message.starts_with(
            "network allocate failed: node memory admission refused: node uses 90000 MiB"
        ),
        "{}",
        rejection.message
    );
    assert!(rejection.message.ends_with("total memory 100000 MiB"));
}

#[test]
fn other_sandbox_create_failures_are_left_to_kubelet_retry() {
    assert!(
        rejection_from_event(&event(
            FAILED_CREATE_POD_SANDBOX_REASON,
            "Pod",
            JAILER_TIMEOUT
        ))
        .is_none()
    );
    assert!(rejection_from_event(&event("BackOff", "Pod", MEMORY_REJECTION)).is_none());
    assert!(
        rejection_from_event(&event(FAILED_CREATE_POD_SANDBOX_REASON, "Pod", QUEUE_FULL)).is_none()
    );
    assert!(
        rejection_from_event(&event(
            FAILED_CREATE_POD_SANDBOX_REASON,
            "Node",
            MEMORY_REJECTION
        ))
        .is_none()
    );
}

#[test]
fn summary_is_bounded_on_a_char_boundary() {
    let long = format!("[space-err=ResourceExhausted]: {}", "é".repeat(600));
    let summary = summarize_rejection(&long);
    assert!(summary.len() <= 512);
    assert!(summary.starts_with('é'));
}

#[test]
fn lookup_matches_name_and_rejects_a_different_uid() {
    let index = SandboxCreateRejections::empty();
    index.observe_event(&event(
        FAILED_CREATE_POD_SANDBOX_REASON,
        "Pod",
        MEMORY_REJECTION,
    ));
    assert!(
        index
            .rejection_for("space-sbx-01a1-g1", Some("61a90a93"))
            .is_some()
    );
    assert!(index.rejection_for("space-sbx-01a1-g1", None).is_some());
    assert!(
        index
            .rejection_for("space-sbx-01a1-g1", Some("other-uid"))
            .is_none()
    );
    assert!(index.rejection_for("space-sbx-other-g1", None).is_none());
}

#[tokio::test(start_paused = true)]
async fn waiter_resolves_once_the_rejection_is_observed() {
    let index = SandboxCreateRejections::empty();
    let writer = index.clone();
    let waiter = tokio::spawn(async move {
        index
            .wait_for_rejection("space-sbx-01a1-g1", Some("61a90a93"))
            .await
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!waiter.is_finished());
    writer.observe_event(&event(
        FAILED_CREATE_POD_SANDBOX_REASON,
        "Pod",
        MEMORY_REJECTION,
    ));
    let rejection = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter resolves")
        .expect("join");
    assert_eq!(rejection.node_name.as_deref(), Some("node-a"));
}

#[test]
fn rejection_error_is_retryable_and_not_a_node_mgr_outage() {
    let (_, rejection) = rejection_from_event(&event(
        FAILED_CREATE_POD_SANDBOX_REASON,
        "Pod",
        MEMORY_REJECTION,
    ))
    .expect("rejection");
    let error = super::super::wait_ready::node_admission_rejected_error(&rejection);
    assert!(matches!(
        error,
        crate::controllers::substrate::SubstrateError::Unavailable { .. }
    ));
    // A node-mgr outage defers pod cleanup; a refused pod must be deleted now.
    assert!(!super::super::support::is_nodemgr_unreachable(&error));
}
