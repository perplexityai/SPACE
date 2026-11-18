use super::DrainRestoreRetry;

mod executor;
mod supersession;

/// The backoff doubles from its base to its cap.
#[test]
fn failed_attempts_back_off_to_the_cap() {
    let retry = DrainRestoreRetry::with_max_attempts(16);
    let waits = (1..=7)
        .map(|attempt| retry.backoff(attempt).as_secs())
        .collect::<Vec<_>>();
    assert_eq!(waits, [60, 120, 240, 480, 960, 1800, 1800]);
}

/// The payload is read by every version that claims it, so a field a newer
/// writer adds reads as the payload without it, and the payload this
/// version writes reads back as itself.
#[test]
fn a_payload_with_a_field_this_version_does_not_know_reads_as_without_it() {
    let payload = super::DrainRestoreOperationPayload::new(
        pplx_space_sandbox_model::SandboxId::new(),
        uuid::Uuid::now_v7(),
    );
    let mut newer = payload.clone().into_json();
    newer["field_from_a_newer_writer"] = serde_json::json!(["any", "value"]);
    assert_eq!(
        serde_json::from_value::<super::DrainRestoreOperationPayload>(newer).expect("parses"),
        payload
    );
    assert_eq!(
        serde_json::from_value::<super::DrainRestoreOperationPayload>(payload.clone().into_json())
            .expect("parses"),
        payload
    );
}
