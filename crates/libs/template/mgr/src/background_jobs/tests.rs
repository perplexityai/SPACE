use pplx_space_common_core::types::JobId;

use super::*;
use uuid::Uuid;

fn payload() -> StampPayload {
    StampPayload {
        name: "seed".to_string(),
        content_hash: "sha256:deadbeef".to_string(),
    }
}

fn job(kind: &str, payload_json: &str) -> Job {
    Job {
        job_id: JobId::new(),
        kind: kind.to_string(),
        status: "running".to_string(),
        payload: payload_json.to_string(),
        subject_id: None,
        dedup_key: None,
        failure: None,
        attempts: 1,
        max_attempts: MAX_ATTEMPTS,
        bytes_processed: 0,
        error: None,
        created_at: "2026-05-25T00:00:00Z".to_string(),
        started_at: Some("2026-05-25T00:00:01Z".to_string()),
        completed_at: None,
        traceparent: None,
        lease_owner: None,
        priority: Default::default(),
    }
}

#[test]
fn classify_nodemgr_connect_loss_is_retriable() {
    // Models nodemgr-loss during received_uuid stamping: the gRPC
    // client can't reach the socket (nodemgr restarting, K8s pod
    // rescheduled, transient network blip). This MUST classify as
    // retriable — the durable worker's exponential backoff is the
    // only thing that converges the stamp once nodemgr comes back.
    // Misclassifying as permanent would silently strand the build
    // on the Full codec forever even though the underlying issue
    // resolves on its own.
    let err = BtrfsViaNodemgrError::Connect("connect to /space/run/...: ENOENT".to_string());
    let result = stamp::classify_finalize_result(Err(err), &payload());
    assert!(
        matches!(result, Err(JobError::Retriable(_))),
        "connect-loss must be retriable, got {result:?}",
    );
}

#[test]
fn classify_failed_precondition_is_permanent() {
    // The digest dir genuinely isn't a btrfs subvolume (legacy
    // plain-dir install, non-btrfs host). No amount of retrying
    // changes that — surface as permanent so the metric makes the
    // operational state visible.
    let err = BtrfsViaNodemgrError::Rpc {
        rpc: "BtrfsFinalizeTemplateSubvolume",
        status: tonic::Status::failed_precondition("digest dir is not a subvolume"),
    };
    let result = stamp::classify_finalize_result(Err(err), &payload());
    assert!(
        matches!(result, Err(JobError::Permanent(_))),
        "FailedPrecondition must be permanent, got {result:?}",
    );
}

#[test]
fn classify_internal_rpc_error_is_retriable() {
    // Anything other than FailedPrecondition — Internal, Unavailable,
    // DeadlineExceeded, the works — could be transient (nodemgr
    // overloaded, kernel hiccup, btrfs-progs binary missing during
    // a rollout window). The worker retries and converges.
    let err = BtrfsViaNodemgrError::Rpc {
        rpc: "BtrfsFinalizeTemplateSubvolume",
        status: tonic::Status::internal("btrfs property set timed out"),
    };
    let result = stamp::classify_finalize_result(Err(err), &payload());
    assert!(
        matches!(result, Err(JobError::Retriable(_))),
        "non-FailedPrecondition RPC errors must be retriable, got {result:?}",
    );
}

#[test]
fn classify_invalid_uuid_is_retriable() {
    // The privileged path returned a value that isn't a UUID. This
    // shouldn't happen on a well-behaved nodemgr, but if it does the
    // safe default is retry — a subsequent stamp may succeed once
    // whatever produced the malformed response (binary mismatch,
    // truncated read) resolves.
    let err = BtrfsViaNodemgrError::InvalidUuid {
        value: "not-a-uuid".to_string(),
        error: "invalid length".to_string(),
    };
    let result = stamp::classify_finalize_result(Err(err), &payload());
    assert!(
        matches!(result, Err(JobError::Retriable(_))),
        "InvalidUuid must be retriable, got {result:?}",
    );
}

#[test]
fn classify_success_returns_zero_bytes() {
    let result = stamp::classify_finalize_result(Ok(Uuid::nil()), &payload());
    assert!(
        matches!(result, Ok(0)),
        "success must report 0 bytes, got {result:?}"
    );
}

#[tokio::test]
async fn unknown_job_kind_is_permanent() {
    // The dispatcher must not "succeed" on unknown kinds; in the
    // future-codec scenario where an older binary picks up a job
    // kind it doesn't recognize, marking the job Permanent surfaces
    // the situation as a metric instead of looping forever.
    let executor = TemplateBackgroundExecutor;
    let result = executor
        .execute(
            &job(
                "completely-unknown-kind",
                &serde_json::to_string(&payload()).unwrap(),
            ),
            tokio_util::sync::CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(result, Err(JobError::Permanent(_))),
        "unknown kind must be permanent, got {result:?}",
    );
}
