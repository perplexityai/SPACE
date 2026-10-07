use super::*;

const LONG: Duration = Duration::from_secs(60);

fn far_deadline() -> Instant {
    Instant::now() + LONG
}

#[tokio::test(start_paused = true)]
async fn free_capacity_admits_immediately() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);

    let permit = admission
        .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
        .await
        .unwrap();

    assert_eq!(permit.admission(), Admission::Immediate);
    assert_eq!(admission.in_flight(TlsHandshakeLeg::ClientToNetgw), 1);
    drop(permit);
    assert_eq!(admission.in_flight(TlsHandshakeLeg::ClientToNetgw), 0);
}

#[tokio::test(start_paused = true)]
async fn legs_share_one_pool_and_drop_restores_capacity() {
    let admission = HandshakeAdmission::new(2, Duration::ZERO);
    let downstream = admission
        .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
        .await
        .unwrap();
    let _upstream = admission
        .admit(TlsHandshakeLeg::NetgwToOrigin, far_deadline())
        .await
        .unwrap();

    assert!(
        admission
            .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
            .await
            .is_err(),
        "both legs must consume the same pool"
    );
    drop(downstream);
    assert_eq!(
        admission
            .admit(TlsHandshakeLeg::NetgwToOrigin, far_deadline())
            .await
            .unwrap()
            .admission(),
        Admission::Immediate,
        "a completed handshake should immediately restore capacity"
    );
}

#[tokio::test(start_paused = true)]
async fn full_pool_queues_until_a_permit_is_released() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission
        .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
        .await
        .unwrap();

    let waiter = tokio::spawn({
        let admission = admission.clone();
        async move {
            admission
                .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(admission.queued(TlsHandshakeLeg::ClientToNetgw), 1);

    tokio::time::sleep(Duration::from_millis(749)).await;
    drop(held);
    let permit = waiter.await.unwrap().unwrap();

    assert_eq!(
        permit.admission(),
        Admission::Queued {
            waited: Duration::from_millis(750)
        }
    );
    assert_eq!(admission.queued(TlsHandshakeLeg::ClientToNetgw), 0);
    assert_eq!(admission.in_flight(TlsHandshakeLeg::ClientToNetgw), 1);
}

#[tokio::test(start_paused = true)]
async fn queue_wait_stops_at_the_queue_limit() {
    let admission = HandshakeAdmission::new(1, Duration::from_secs(2));
    let _held = admission
        .admit(TlsHandshakeLeg::NetgwToOrigin, far_deadline())
        .await
        .unwrap();

    let error = admission
        .admit(TlsHandshakeLeg::NetgwToOrigin, far_deadline())
        .await
        .unwrap_err();

    assert_eq!(
        error,
        Saturated {
            waited: Duration::from_secs(2)
        }
    );
    assert_eq!(admission.queued(TlsHandshakeLeg::NetgwToOrigin), 0);
}

#[tokio::test(start_paused = true)]
async fn queue_wait_stops_at_an_earlier_caller_deadline() {
    let admission = HandshakeAdmission::new(1, Duration::from_secs(5));
    let _held = admission
        .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
        .await
        .unwrap();

    let error = admission
        .admit(
            TlsHandshakeLeg::ClientToNetgw,
            Instant::now() + Duration::from_millis(300),
        )
        .await
        .unwrap_err();

    assert_eq!(error.waited, Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn waiters_are_admitted_in_arrival_order() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission
        .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
        .await
        .unwrap();
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut waiters = Vec::new();
    for (index, leg) in [
        TlsHandshakeLeg::ClientToNetgw,
        TlsHandshakeLeg::NetgwToOrigin,
        TlsHandshakeLeg::ClientToNetgw,
    ]
    .into_iter()
    .enumerate()
    {
        let admission = admission.clone();
        let order_tx = order_tx.clone();
        waiters.push(tokio::spawn(async move {
            let permit = admission.admit(leg, far_deadline()).await.unwrap();
            order_tx.send(index).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(permit);
        }));
        // Let this waiter join the queue before the next one starts.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    drop(order_tx);
    drop(held);
    for waiter in waiters {
        waiter.await.unwrap();
    }

    let mut order = Vec::new();
    while let Some(index) = order_rx.recv().await {
        order.push(index);
    }
    assert_eq!(order, [0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_waiter_leaves_the_queue() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission
        .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
        .await
        .unwrap();

    let waiter = tokio::spawn({
        let admission = admission.clone();
        async move {
            admission
                .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(admission.queued(TlsHandshakeLeg::ClientToNetgw), 1);

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(admission.queued(TlsHandshakeLeg::ClientToNetgw), 0);

    drop(held);
    assert_eq!(
        admission
            .admit(TlsHandshakeLeg::ClientToNetgw, far_deadline())
            .await
            .unwrap()
            .admission(),
        Admission::Immediate,
        "a cancelled waiter must not keep a permit"
    );
}
