use super::*;

const SANDBOX_A: &str = "019d84bd-280d-7331-99c6-b63377f3a3a2";
const SANDBOX_B: &str = "019d84bd-280d-7331-99c6-b63377f3a3a3";
const SANDBOX_C: &str = "019d84bd-280d-7331-99c6-b63377f3a3a4";

fn limits(permits: usize) -> SlotLimits {
    SlotLimits {
        permits,
        waiters: permits,
    }
}

fn limits_with_waiters(permits: usize, waiters: usize) -> SlotLimits {
    SlotLimits { permits, waiters }
}

#[test]
fn service_port_ingress_per_sandbox_override_cannot_exceed_gateway_capacity() {
    assert_eq!(
        service_port_ingress_per_sandbox_bounds(NODE_PERMITS),
        (
            DEFAULT_PER_SANDBOX_PERMITS,
            pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS,
        )
    );
    assert_eq!(service_port_ingress_per_sandbox_bounds(32), (32, 32));
}

async fn wait_for_sandbox_waiters(
    limiter: &UdsPerSandboxLimiter,
    sandbox_id: &str,
    expected: usize,
) {
    for _ in 0..100 {
        if limiter.sandbox_waiters_snapshot(sandbox_id) == Some(expected) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "sandbox {sandbox_id} waiters did not reach {expected}; got {:?}",
        limiter.sandbox_waiters_snapshot(sandbox_id)
    );
}

async fn wait_for_node_waiters(limiter: &UdsPerSandboxLimiter, expected: usize) {
    for _ in 0..100 {
        if limiter.node_waiters_snapshot() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "node waiters did not reach {expected}; got {}",
        limiter.node_waiters_snapshot()
    );
}

#[tokio::test]
async fn saturated_sandbox_does_not_block_another_sandbox() {
    let limiter = UdsPerSandboxLimiter::for_tests(limits(1), limits(2));

    let _first = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();
    let rejected = limiter.acquire("SpacedMkdir", SANDBOX_A).await;
    let error = rejected.err().expect("second sandbox-a request rejected");
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        pplx_space_common_grpc::UdsPerSandboxLimitExceeded::from_status(&error),
        Some(pplx_space_common_grpc::UdsPerSandboxLimitExceeded)
    );

    let _other = limiter.acquire("SpacedMkdir", SANDBOX_B).await.unwrap();
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), Some(1));
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_B), Some(1));
    assert_eq!(limiter.node_snapshot(), 2);
}

#[tokio::test]
async fn node_limit_still_protects_collective_load() {
    let limiter = UdsPerSandboxLimiter::for_tests(limits(2), limits(1));

    let _first = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();
    let rejected = limiter.acquire("SpacedMkdir", SANDBOX_B).await;

    let error = rejected.err().expect("second node-wide request rejected");
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_B), None);
    assert_eq!(limiter.node_snapshot(), 1);
}

#[tokio::test]
async fn dropping_permit_releases_accounting() {
    let limiter = UdsPerSandboxLimiter::for_tests(limits(1), limits(1));

    let permit = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), Some(1));
    assert_eq!(limiter.node_snapshot(), 1);

    drop(permit);
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), None);
    assert_eq!(limiter.node_snapshot(), 0);
}

#[tokio::test]
async fn empty_sandbox_id_is_invalid() {
    let limiter = UdsPerSandboxLimiter::for_tests(limits(1), limits(1));

    let error = limiter
        .acquire("SpacedMkdir", "")
        .await
        .err()
        .expect("empty sandbox id rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn bounded_wait_admits_when_permit_is_released() {
    let limiter =
        UdsPerSandboxLimiter::for_tests_with_timeout(limits(1), limits(2), Duration::from_secs(1));
    let first = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();

    let waiting_limiter = limiter.clone();
    let waiting =
        tokio::spawn(async move { waiting_limiter.acquire("SpacedMkdir", SANDBOX_A).await });
    wait_for_sandbox_waiters(&limiter, SANDBOX_A, 1).await;
    drop(first);

    let second = timeout(Duration::from_secs(1), waiting)
        .await
        .expect("waiting acquire should finish")
        .expect("task should not panic")
        .expect("released permit should admit waiter");
    drop(second);
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), None);
}

#[tokio::test]
async fn bounded_waiter_budget_rejects_excess_sandbox_waiters_immediately() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_timeout(
        limits_with_waiters(1, 1),
        limits(3),
        Duration::from_secs(1),
    );
    let first = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();

    let waiting_limiter = limiter.clone();
    let waiting =
        tokio::spawn(async move { waiting_limiter.acquire("SpacedMkdir", SANDBOX_A).await });
    wait_for_sandbox_waiters(&limiter, SANDBOX_A, 1).await;

    let rejected = timeout(
        Duration::from_millis(50),
        limiter.acquire("SpacedMkdir", SANDBOX_A),
    )
    .await
    .expect("excess waiter should reject without waiting for acquire timeout");
    let error = match rejected {
        Ok(_) => panic!("waiter budget should reject third request"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("reason=wait_queue_full"));
    assert!(error.message().contains("waiter_limit=1"));

    drop(first);
    let second = waiting
        .await
        .expect("task should not panic")
        .expect("released permit should admit bounded waiter");
    drop(second);
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), None);
}

#[tokio::test]
async fn bounded_wait_still_rejects_when_timeout_expires() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_timeout(
        limits(1),
        limits(2),
        Duration::from_millis(1),
    );
    let _first = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();

    let error = limiter
        .acquire("SpacedMkdir", SANDBOX_A)
        .await
        .err()
        .expect("timeout should reject saturated sandbox");
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("reason=saturated"));
    assert_eq!(limiter.sandbox_waiters_snapshot(SANDBOX_A), Some(0));
}

#[tokio::test]
async fn node_waiter_budget_rejects_collective_excess_waiters_immediately() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_timeout(
        limits(2),
        limits_with_waiters(1, 1),
        Duration::from_secs(1),
    );
    let first = limiter.acquire("SpacedMkdir", SANDBOX_A).await.unwrap();

    let waiting_limiter = limiter.clone();
    let waiting =
        tokio::spawn(async move { waiting_limiter.acquire("SpacedMkdir", SANDBOX_B).await });
    wait_for_node_waiters(&limiter, 1).await;

    let rejected = timeout(
        Duration::from_millis(50),
        limiter.acquire("SpacedMkdir", SANDBOX_C),
    )
    .await
    .expect("excess node waiter should reject without waiting for acquire timeout");
    let error = match rejected {
        Ok(_) => panic!("node waiter budget should reject third request"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("scope=node"));
    assert!(error.message().contains("reason=wait_queue_full"));
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_C), None);

    drop(first);
    let second = waiting
        .await
        .expect("task should not panic")
        .expect("released node permit should admit bounded waiter");
    drop(second);
}

#[tokio::test]
async fn equivalent_uuid_spellings_share_quota_and_selectors_are_rejected() {
    let limiter = UdsPerSandboxLimiter::for_tests(limits(1), limits(8));
    let canonical = SANDBOX_A;
    let permit = limiter.acquire("SpacedWrite", canonical).await.unwrap();
    for alias in [canonical.to_uppercase(), canonical.replace('-', "")] {
        let error = limiter
            .acquire("SpacedWrite", &alias)
            .await
            .err()
            .expect("same sandbox quota");
        assert_eq!(error.code(), Code::ResourceExhausted);
    }
    for selector in ["77f3", "019d84bd-280d", "garbage"] {
        let error = limiter
            .acquire("SpacedRead", selector)
            .await
            .err()
            .expect("full UUID required");
        assert_eq!(error.code(), Code::InvalidArgument);
    }
    drop(permit);
    assert_eq!(limiter.node_snapshot(), 0);
    assert_eq!(limiter.sandbox_snapshot(canonical), None);
}

#[tokio::test]
async fn pre_frame_node_permit_converts_without_double_acquisition() {
    let limiter = UdsPerSandboxLimiter::for_tests(limits(1), limits(1));
    let node = limiter.acquire_node("SpacedWrite").await.unwrap();
    assert_eq!(
        limiter
            .acquire_node("SpacedWrite")
            .await
            .err()
            .unwrap()
            .code(),
        Code::ResourceExhausted
    );
    let permit = limiter
        .acquire_with_node("SpacedWrite", SANDBOX_A, node)
        .await
        .unwrap();
    assert_eq!(limiter.node_snapshot(), 1);
    drop(permit);
    assert_eq!(limiter.node_snapshot(), 0);
    let node = limiter.acquire_node("SpacedWrite").await.unwrap();
    assert!(
        limiter
            .acquire_with_node("SpacedWrite", "invalid", node)
            .await
            .is_err()
    );
    assert_eq!(limiter.node_snapshot(), 0);
}

fn expect_limited(result: Result<UdsPerSandboxPermit, Status>, scope: &str) {
    let error = result.err().expect("request over its budget is rejected");
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        pplx_space_common_grpc::UdsPerSandboxLimitExceeded::from_status(&error),
        Some(pplx_space_common_grpc::UdsPerSandboxLimitExceeded)
    );
    assert!(
        error.message().contains(&format!("scope={scope},")),
        "{}",
        error.message()
    );
}

#[tokio::test]
async fn reserved_calls_keep_their_permits_when_the_shared_budget_is_full() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_reserved(2, 1, 8);
    let shared = [
        limiter
            .acquire("SpacedConnectProcess", SANDBOX_A)
            .await
            .unwrap(),
        limiter.acquire("SpacedWaitReady", SANDBOX_A).await.unwrap(),
    ];
    expect_limited(
        limiter.acquire("SpacedWaitReady", SANDBOX_A).await,
        "sandbox",
    );

    let kill = limiter
        .acquire_in(SandboxBudget::Reserved, "SpacedKillProcess", SANDBOX_A)
        .await
        .expect("a reserved call is admitted while the shared budget is full");
    expect_limited(
        limiter
            .acquire_in(SandboxBudget::Reserved, "SpacedListProcesses", SANDBOX_A)
            .await,
        "sandbox_reserved",
    );
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), Some(2));
    assert_eq!(limiter.reserved_snapshot(SANDBOX_A), Some(1));

    drop(kill);
    drop(shared);
    assert_eq!(limiter.sandbox_snapshot(SANDBOX_A), None);
    assert_eq!(limiter.reserved_snapshot(SANDBOX_A), None);
    assert_eq!(limiter.node_snapshot(), 0);
}

#[tokio::test(start_paused = true)]
async fn default_budget_admits_reserved_calls_with_streams_and_long_calls_holding_every_shared_permit()
 {
    let limiter = UdsPerSandboxLimiter::control_with_shared_permits(UDS_PER_SANDBOX_PERMITS);
    let mut shared = Vec::new();
    for method in ["SpacedConnectProcess", "SpacedWaitReady"]
        .into_iter()
        .cycle()
        .take(UDS_PER_SANDBOX_PERMITS)
    {
        shared.push(limiter.acquire(method, SANDBOX_A).await.unwrap());
    }
    expect_limited(
        limiter.acquire("SpacedWaitReady", SANDBOX_A).await,
        "sandbox",
    );

    let mut reserved = Vec::new();
    for method in ["SpacedKillProcess", "SpacedSendSignal"]
        .into_iter()
        .cycle()
        .take(UDS_PER_SANDBOX_RESERVED_PERMITS)
    {
        reserved.push(
            limiter
                .acquire_in(SandboxBudget::Reserved, method, SANDBOX_A)
                .await
                .unwrap_or_else(|error| panic!("{method} rejected: {error}")),
        );
    }
    expect_limited(
        limiter
            .acquire_in(SandboxBudget::Reserved, "SpacedKillProcess", SANDBOX_A)
            .await,
        "sandbox_reserved",
    );
}
