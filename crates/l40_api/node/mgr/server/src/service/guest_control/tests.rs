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

#[tokio::test]
async fn terminal_error_survives_a_full_data_queue() {
    use tokio_stream::StreamExt;
    for outcome in [Ok(()), Err(Status::aborted("execution replaced"))] {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        for _ in 0..4 {
            tx.try_send(Ok(OpenGuestChannelResponse { data: vec![1] }))
                .unwrap();
        }
        let (terminal_tx, terminal) = tokio::sync::oneshot::channel();
        let expected = outcome.as_ref().err().map(Status::code);
        let task = tokio::spawn(async move {
            terminal_tx.send(outcome).unwrap();
            drop(tx);
        });
        let mut stream = ChannelStream {
            receiver: ReceiverStream::new(rx),
            task,
            terminal,
            terminal_delivered: false,
        };
        for _ in 0..4 {
            assert_eq!(stream.next().await.unwrap().unwrap().data, vec![1]);
        }
        assert_eq!(stream.next().await.map(|v| v.unwrap_err().code()), expected);
        assert!(stream.next().await.is_none());
    }
}
