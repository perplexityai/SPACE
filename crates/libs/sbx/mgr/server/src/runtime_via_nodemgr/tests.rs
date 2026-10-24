use super::*;

#[tokio::test(start_paused = true)]
async fn node_manager_state_read_respects_its_deadline() {
    let read = std::future::pending::<Result<VmState, RuntimeStateReadError>>();

    let error = bounded_vm_state_read(read, Duration::from_millis(10))
        .await
        .expect_err("a wedged node-manager read must time out");

    assert!(matches!(
        error,
        RuntimeStateReadError::Timeout { timeout_ms: 10 }
    ));
}

#[test]
fn state_read_errors_preserve_rpc_code_without_peer_payloads() {
    for code in [tonic::Code::Unimplemented, tonic::Code::Unavailable] {
        let error = RuntimeStateReadError::from(tonic::Status::new(code, "untrusted-peer-body"));
        assert!(!error.to_string().contains("untrusted-peer-body"));
        assert!(matches!(error, RuntimeStateReadError::Rpc(status) if status.code() == code));
    }
}
