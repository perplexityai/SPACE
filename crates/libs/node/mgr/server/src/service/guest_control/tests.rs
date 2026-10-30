use super::*;

#[test]
fn operator_sessions_cannot_exhaust_readiness_capacity() {
    let budgets = GuestAccessBudgets::default();
    let sessions = (0..32)
        .map(|_| budgets.channels.clone().try_acquire_owned().unwrap())
        .collect::<Vec<_>>();
    budgets.channels.clone().try_acquire_owned().unwrap_err();
    let probe = budgets.probes.clone().try_acquire_owned().unwrap();
    drop(sessions);
    drop(probe);
    assert_eq!(budgets.channels.available_permits(), 32);
    assert_eq!(budgets.probes.available_permits(), 32);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn serial_authenticates_the_connected_peer() {
    let (client, _server) = tokio::net::UnixStream::pair().unwrap();
    let owner = pplx_space_node_runtime_core::launch::ProcessObserver::current().unwrap();
    let credentials = client.peer_cred().unwrap();
    // Production slots use equal UID/GID; the test accepts only the same case.
    if credentials.uid() == credentials.gid() {
        validate_serial_peer(&client, credentials.uid(), &owner).unwrap();
    }
    let wrong_uid = credentials.uid().wrapping_add(1);
    assert_eq!(
        validate_serial_peer(&client, wrong_uid, &owner)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}
