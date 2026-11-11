use super::*;
use pplx_space_common_core::types::CredentialReferenceId;
use pplx_space_net_common::EffectiveNetworkPolicyRefScope;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot;

fn tls_on_loader(pulls: Arc<AtomicU64>) -> PolicyLoader {
    Arc::new(move || {
        pulls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
            policy.tls_interception = pplx_space_net_common::TlsInterception::On;
            Ok(PulledNetworkPolicy {
                policy,
                policy_ref: test_policy_ref(),
                network_credentials: Default::default(),
            })
        })
    })
}

/// An installed TLS-On policy is `Ready` even while the CA/credential
/// gate is closed. Readiness of the interception dependencies is reported on
/// the side, never by demoting the policy lifecycle state that sandbox-manager
/// keys sandbox start on.
#[tokio::test(start_paused = true)]
async fn interception_dependency_gate_blocks_traffic_without_blocking_policy_hydration() {
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = open.clone();
    let state = GatewayNetworkState::pending_policy()
        .with_readiness_metrics()
        .with_traffic_gate(Arc::new(move || gate.load(Ordering::SeqCst)));
    let observe_dependencies = state.interception_readiness_observer();
    assert!(matches!(
        state.udp_policy().await,
        TrafficPolicy::NotReady(PolicyNotReady::PendingPolicy)
    ));
    assert_eq!(
        state.snapshot().await.interception_readiness,
        InterceptionReadiness::NotRequired
    );
    let pulls = Arc::new(AtomicU64::new(0));
    let loader = tls_on_loader(pulls.clone());
    let snapshot = state
        .refresh(loader.clone(), None, NetworkStateReason::Startup)
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(snapshot.installed_policy_ref, test_policy_ref().to_string());
    assert_eq!(
        snapshot.interception_readiness,
        InterceptionReadiness::DependenciesNotReady
    );
    assert!(snapshot.interception_readiness.blocks_admission());
    assert!(snapshot.policy.is_some());
    // UDP has no connection to hold: it fails closed immediately.
    assert!(matches!(
        state.udp_policy().await,
        TrafficPolicy::NotReady(PolicyNotReady::InterceptionNotReady)
    ));
    let denied = tokio::time::timeout(Duration::from_millis(1), state.wait_for_tcp_policy())
        .await
        .expect("missing interception dependencies must deny TCP immediately");
    assert!(matches!(
        denied,
        TrafficPolicy::NotReady(PolicyNotReady::InterceptionNotReady)
    ));
    assert_eq!(state.snapshot().await.pending_tcp_connections, 0);
    // Interception dependency availability is not a policy-loader failure:
    // waiting must neither exhaust the hydration budget nor re-install an
    // already current policy.
    let shutdown = CancellationToken::new();
    let hydration = state.spawn_hydration(loader.clone(), shutdown.clone(), None);
    for _ in 0..60 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(pulls.load(Ordering::SeqCst), 1);
    let snapshot = state.snapshot().await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(
        snapshot.interception_readiness,
        InterceptionReadiness::DependenciesNotReady
    );
    open.store(true, Ordering::SeqCst);
    observe_dependencies(true);
    let snapshot = state.snapshot().await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(
        snapshot.interception_readiness,
        InterceptionReadiness::Ready
    );
    assert_eq!(
        state
            .refresh(loader, None, NetworkStateReason::ControlRefresh)
            .await
            .mode,
        NetworkState::Ready
    );
    assert_eq!(pulls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        state.wait_for_tcp_policy().await,
        TrafficPolicy::Ready(_)
    ));
    assert!(matches!(state.udp_policy().await, TrafficPolicy::Ready(_)));
    open.store(false, Ordering::SeqCst);
    observe_dependencies(false);
    let snapshot = state.snapshot().await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(
        snapshot.interception_readiness,
        InterceptionReadiness::DependenciesNotReady
    );
    assert!(!state.is_current_tcp_policy(snapshot.policy.as_ref().unwrap()));
    state.set_no_identity(NetworkStateReason::NoIdentity).await;
    open.store(true, Ordering::SeqCst);
    let unavailable = state.snapshot().await;
    assert_eq!(unavailable.mode, NetworkState::NoIdentity);
    assert_eq!(
        unavailable.interception_readiness,
        InterceptionReadiness::NotRequired
    );
    shutdown.cancel();
    hydration.await.unwrap();
}

/// TLS-Off sandboxes never consult the interception gate: a closed gate does
/// not hold or deny anything, and readiness reports `NotRequired`.
#[tokio::test]
async fn tls_off_policy_ignores_closed_interception_gate() {
    let state = GatewayNetworkState::pending_policy().with_traffic_gate(Arc::new(|| false));
    let snapshot = state
        .refresh(
            immediate_success_loader(test_policy_ref()),
            None,
            NetworkStateReason::Startup,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(
        snapshot.interception_readiness,
        InterceptionReadiness::NotRequired
    );
    assert!(!snapshot.interception_readiness.blocks_admission());
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), state.wait_for_tcp_policy())
            .await
            .expect("TLS-Off admission must not wait on the interception gate"),
        TrafficPolicy::Ready(_)
    ));
    assert!(matches!(state.udp_policy().await, TrafficPolicy::Ready(_)));
}

fn test_policy_ref() -> EffectiveNetworkPolicyRef {
    EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap()
}

#[tokio::test]
async fn changed_tls_policy_applies_to_subsequent_admissions() {
    let state = GatewayNetworkState::pending_policy().with_traffic_gate(Arc::new(|| true));
    let off_ref = test_policy_ref();
    state
        .refresh(
            immediate_success_loader(off_ref.clone()),
            None,
            NetworkStateReason::Startup,
        )
        .await;
    let TrafficPolicy::Ready(off_policy) = state.wait_for_tcp_policy().await else {
        panic!("off policy must be ready")
    };
    assert_eq!(
        off_policy.tls_interception,
        pplx_space_net_common::TlsInterception::Off
    );

    let on_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    let loader: PolicyLoader = Arc::new({
        let on_ref = on_ref.clone();
        move || {
            let on_ref = on_ref.clone();
            Box::pin(async move {
                let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
                policy.tls_interception = pplx_space_net_common::TlsInterception::On;
                Ok(PulledNetworkPolicy {
                    policy,
                    policy_ref: on_ref,
                    network_credentials: Default::default(),
                })
            })
        }
    });
    state
        .refresh(loader, Some(on_ref), NetworkStateReason::ControlRefresh)
        .await;
    let TrafficPolicy::Ready(on_policy) = state.wait_for_tcp_policy().await else {
        panic!("on policy must be ready")
    };
    assert_eq!(
        on_policy.tls_interception,
        pplx_space_net_common::TlsInterception::On
    );
    assert!(!Arc::ptr_eq(&off_policy, &on_policy));
    assert!(!state.is_current_tcp_policy(&off_policy));
}

#[tokio::test]
async fn network_credentials_are_bound_to_the_exact_installed_policy_generation() {
    use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
    use pplx_space_net_common::{
        CredentialHeaderBindingV1, NetworkCredentialDestinationV1, NetworkCredentialHttpMethodV1,
        NetworkCredentialRuleV1, TlsInterception,
    };

    let reference_id: CredentialReferenceId =
        "018f1e9d-7b7a-7d3a-8000-000000000001".parse().unwrap();
    let rule = NetworkCredentialRuleV1 {
        rule_id: "api".into(),
        destination: NetworkCredentialDestinationV1::new(
            "api.example.com".parse().unwrap(),
            None,
            Some("/v1".into()),
        )
        .unwrap(),
        credential_header_bindings: vec![CredentialHeaderBindingV1 {
            header_name: "authorization".into(),
            credential_reference_id: reference_id.as_uuid(),
        }],
        supported_http_methods: vec![NetworkCredentialHttpMethodV1::Get],
    };
    let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
    policy.tls_interception = TlsInterception::On;
    policy.network_credential_rules = vec![rule];
    let state = GatewayNetworkState::pending_policy().with_traffic_gate(Arc::new(|| true));
    let first_ref = test_policy_ref();
    let loader: PolicyLoader = Arc::new(move || {
        let policy = policy.clone();
        let policy_ref = first_ref.clone();
        Box::pin(async move {
            let network_credentials = NetworkCredentialSnapshot::from_wire(
                vec![ResolvedNetworkCredential {
                    credential_reference_id: reference_id.to_string(),
                    value: b"Bearer generation-one".to_vec(),
                }],
                &policy,
            )
            .unwrap();
            Ok(PulledNetworkPolicy {
                policy,
                policy_ref,
                network_credentials,
            })
        })
    });
    state
        .refresh(loader, None, NetworkStateReason::Startup)
        .await;
    let TrafficPolicy::Ready(installed) = state.wait_for_tcp_policy().await else {
        panic!("credential policy should be ready")
    };
    let value = state
        .with_network_credentials(&installed, |credentials| {
            credentials.value(reference_id).unwrap().to_vec()
        })
        .unwrap();
    assert_eq!(value, b"Bearer generation-one");

    let next_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    state
        .refresh(
            immediate_success_loader(next_ref.clone()),
            Some(next_ref),
            NetworkStateReason::ControlRefresh,
        )
        .await;
    assert!(state.with_network_credentials(&installed, |_| ()).is_err());
}

fn immediate_success_loader(policy_ref: EffectiveNetworkPolicyRef) -> PolicyLoader {
    Arc::new(move || {
        let policy_ref = policy_ref.clone();
        Box::pin(async move {
            Ok(PulledNetworkPolicy {
                policy: pplx_space_net_common::NetworkPolicyV1::allow_all(),
                policy_ref,
                network_credentials: Default::default(),
            })
        })
    })
}

fn v1_domain_allow_loader(policy_ref: EffectiveNetworkPolicyRef) -> PolicyLoader {
    Arc::new(move || {
        let policy_ref = policy_ref.clone();
        Box::pin(async move {
            // V1 allow-domain policy: enforced via the DNS allow cache, no
            // domain denies possible. Must NOT be gated by SNI sniffing.
            let egress = pplx_space_net_common::EgressPolicyV1::new(
                pplx_space_net_common::EgressDefaultAction::Deny,
                vec![pplx_space_net_common::EgressDestinationV1::Domain(
                    "example.com".parse().unwrap(),
                )],
                vec![],
            )
            .unwrap();
            Ok(PulledNetworkPolicy {
                policy: pplx_space_net_common::NetworkPolicyV1 {
                    version: pplx_space_net_common::NetworkPolicyVersion::V1,
                    tls_interception: Default::default(),
                    tls_interception_exemptions: Vec::new(),
                    network_credential_rules: vec![],
                    egress,
                },
                policy_ref,
                network_credentials: Default::default(),
            })
        })
    })
}

/// V1 domain-allow policies install `Ready` and are enforced through the DNS
/// allow cache without being gated by SNI sniffing.
#[tokio::test]
async fn domain_policy_installs_ready() {
    let policy_ref = test_policy_ref();
    let state = GatewayNetworkState::pending_policy();
    let snapshot = state
        .refresh(
            v1_domain_allow_loader(policy_ref.clone()),
            Some(policy_ref),
            NetworkStateReason::Startup,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert!(snapshot.policy.is_some());
}

#[tokio::test]
async fn policy_install_reconciles_dns_and_connections_on_self_hydration() {
    let state = GatewayNetworkState::pending_policy();
    let cancellation = state.reserved_dns_cancellation.operation_token();
    let shutdown = CancellationToken::new();
    let _connection = state.tcp_connections.register(
        "192.0.2.10:443".parse().unwrap(),
        crate::tls_sni::ClientHelloScan::NotTls,
        &NetworkPolicyV1::allow_all(),
        &state.dns_allow_cache,
        shutdown.clone(),
    );
    let policy_ref = test_policy_ref();
    let snapshot = state
        .refresh(
            v1_domain_allow_loader(policy_ref.clone()),
            Some(policy_ref.clone()),
            NetworkStateReason::SelfRetry,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert!(cancellation.is_cancelled());
    assert!(
        shutdown.is_cancelled(),
        "the installed policy must close the old IP-only connection"
    );
    let next_operation = state.reserved_dns_cancellation.operation_token();
    state
        .refresh(
            v1_domain_allow_loader(policy_ref.clone()),
            Some(policy_ref),
            NetworkStateReason::SelfRetry,
        )
        .await;
    assert!(
        !next_operation.is_cancelled(),
        "an unchanged policy must not cancel current DNS work"
    );
}

/// A refresh out of `NoIdentity` *does* install a policy — that is the
/// legitimate pool-adoption path (an explicit `RefreshNetworkPolicy` with a
/// real expected ref installs a policy on a previously identity-less
/// gateway).
#[tokio::test]
async fn refresh_out_of_no_identity_is_the_adoption_path() {
    let policy_ref = test_policy_ref();
    let state = GatewayNetworkState::no_identity();

    let snapshot = state
        .refresh(
            immediate_success_loader(policy_ref.clone()),
            Some(policy_ref.clone()),
            NetworkStateReason::from_external("pool-adoption"),
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(snapshot.installed_policy_ref, policy_ref.to_string());
}

/// A policy hydrates through the state machine with the expected ref, a
/// matching re-refresh short-circuits, and a ref mismatch fails closed.
#[tokio::test]
async fn policy_hydrates_ready_with_stable_ref_and_fails_closed_on_mismatch() {
    let state = GatewayNetworkState::pending_policy();
    let policy_ref = test_policy_ref();
    let loader = immediate_success_loader(policy_ref.clone());

    let snapshot = state
        .refresh(
            loader.clone(),
            Some(policy_ref.clone()),
            NetworkStateReason::Startup,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(snapshot.installed_policy_ref, policy_ref.to_string());
    assert!(snapshot.policy.is_some());

    // Same expected ref again (e.g. reconcile after gateway restart):
    // short-circuits Ready with the identical installed ref.
    let snapshot = state
        .refresh(
            loader.clone(),
            Some(policy_ref.clone()),
            NetworkStateReason::Reconcile,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(snapshot.installed_policy_ref, policy_ref.to_string());

    // A pull that disagrees with the expected ref must fail closed, not
    // install the mismatched document.
    let hash = "f".repeat(64);
    let other_ref = EffectiveNetworkPolicyRef::cluster(
        pplx_space_net_common::EffectiveNetworkPolicyRefScope::Sandbox,
        "sbx-other",
        9,
        hash,
    )
    .unwrap();
    let snapshot = state
        .refresh(loader, Some(other_ref), NetworkStateReason::Reconcile)
        .await;
    assert_eq!(snapshot.mode, NetworkState::PolicyError);
    assert_eq!(
        snapshot.last_error_kind,
        crate::proto::NetworkPolicyErrorKind::PolicyRefMismatch
    );
    assert!(snapshot.policy.is_none());
}

fn immediate_error_loader(error: &'static str) -> PolicyLoader {
    Arc::new(move || {
        Box::pin(async move {
            Err(HydrateNetworkStateError::GetNetworkPolicy {
                sandbox_id: pplx_space_common_core::types::SandboxId::new(),
                source: tonic::Status::unavailable(error),
            })
        })
    })
}

#[tokio::test]
async fn typed_load_failures_clear_policy_and_preserve_diagnostics() {
    type FailureCase = (fn() -> HydrateNetworkStateError, &'static str);
    let failures: [FailureCase; 2] = [
        (
            || HydrateNetworkStateError::InvalidReference {
                reference: "invalid".to_owned(),
                source: pplx_space_net_common::EffectiveNetworkPolicyRefParseError::InvalidFormat,
            },
            "policy ref is invalid: effective network policy ref has invalid format: invalid",
        ),
        (
            || HydrateNetworkStateError::Policy(NetworkPolicyFromHostControlError::MissingV1Policy),
            "host-control returned no V1 policy",
        ),
    ];
    for (failure, message) in failures {
        let state = GatewayNetworkState::pending_policy();
        let installed_ref = test_policy_ref();
        state
            .refresh(
                immediate_success_loader(installed_ref.clone()),
                None,
                NetworkStateReason::Startup,
            )
            .await;
        let next_ref = EffectiveNetworkPolicyRef::local(
            EffectiveNetworkPolicyRefScope::Sandbox,
            "sandbox-1",
            "f".repeat(64),
        )
        .unwrap();
        let loader: PolicyLoader = Arc::new(move || Box::pin(async move { Err(failure()) }));
        let snapshot = state
            .refresh(loader, Some(next_ref), NetworkStateReason::ControlRefresh)
            .await;

        assert_eq!(snapshot.mode, NetworkState::PolicyError);
        assert!(snapshot.policy.is_none());
        assert_eq!(snapshot.installed_policy_ref, installed_ref.to_string());
        assert_eq!(
            snapshot.last_error_kind,
            NetworkPolicyErrorKind::LoaderError
        );
        assert_eq!(snapshot.last_error, message);
        assert_eq!(snapshot.consecutive_hydration_failures, 1);
        assert!(matches!(
            state.wait_for_tcp_policy().await,
            TrafficPolicy::NotReady(PolicyNotReady::PolicyError)
        ));
    }
}

fn counting_success_loader(
    policy_ref: EffectiveNetworkPolicyRef,
) -> (PolicyLoader, Arc<std::sync::atomic::AtomicUsize>) {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_for_loader = Arc::clone(&calls);
    let loader: PolicyLoader = Arc::new(move || {
        let policy_ref = policy_ref.clone();
        let calls = Arc::clone(&calls_for_loader);
        Box::pin(async move {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(PulledNetworkPolicy {
                policy: pplx_space_net_common::NetworkPolicyV1::allow_all(),
                policy_ref,
                network_credentials: Default::default(),
            })
        })
    });
    (loader, calls)
}

/// `refresh` must keep short-circuiting when the gateway is already Ready
/// with the expected v0 ref installed — the Ready short-circuit is what
/// keeps steady-state reconciles from re-running the loader.
#[tokio::test]
async fn refresh_short_circuits_when_ready_with_matching_ref() {
    use std::sync::atomic::Ordering::SeqCst;

    let state = GatewayNetworkState::pending_policy();
    let policy_ref = test_policy_ref();
    let (loader, calls) = counting_success_loader(policy_ref.clone());

    let snapshot = state
        .refresh(
            loader.clone(),
            Some(policy_ref.clone()),
            NetworkStateReason::ControlRefresh,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert_eq!(calls.load(SeqCst), 1, "first refresh runs the loader");

    // Already Ready with the same v0 ref: refresh must short-circuit.
    state
        .refresh(
            loader.clone(),
            Some(policy_ref.clone()),
            NetworkStateReason::ControlRefresh,
        )
        .await;
    assert_eq!(
        calls.load(SeqCst),
        1,
        "refresh short-circuits when Ready and the v0 ref matches"
    );
}

#[tokio::test]
async fn matching_policy_ref_reloads_the_complete_network_credential_snapshot() {
    use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
    use pplx_space_net_common::{
        CredentialHeaderBindingV1, NetworkCredentialDestinationV1, NetworkCredentialHttpMethodV1,
        NetworkCredentialRuleV1,
    };
    use std::sync::atomic::Ordering::SeqCst;

    let reference_id: CredentialReferenceId =
        "018f1e9d-7b7a-7d3a-8000-000000000001".parse().unwrap();
    let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
    policy.network_credential_rules = vec![NetworkCredentialRuleV1 {
        rule_id: "api".into(),
        destination: NetworkCredentialDestinationV1::new(
            "api.example.com".parse().unwrap(),
            None,
            None,
        )
        .unwrap(),
        credential_header_bindings: vec![CredentialHeaderBindingV1 {
            header_name: "authorization".into(),
            credential_reference_id: reference_id.as_uuid(),
        }],
        supported_http_methods: vec![NetworkCredentialHttpMethodV1::Get],
    }];
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let loader: PolicyLoader = Arc::new({
        let calls = calls.clone();
        let policy = policy.clone();
        let policy_ref = test_policy_ref();
        move || {
            let generation = calls.fetch_add(1, SeqCst) + 1;
            let policy = policy.clone();
            let policy_ref = policy_ref.clone();
            Box::pin(async move {
                let network_credentials = NetworkCredentialSnapshot::from_wire(
                    vec![ResolvedNetworkCredential {
                        credential_reference_id: reference_id.to_string(),
                        value: format!("Bearer generation-{generation}").into_bytes(),
                    }],
                    &policy,
                )
                .unwrap();
                Ok(PulledNetworkPolicy {
                    policy,
                    policy_ref,
                    network_credentials,
                })
            })
        }
    });
    let state = GatewayNetworkState::pending_policy().with_traffic_gate(Arc::new(|| true));
    let policy_ref = test_policy_ref();
    state
        .refresh(
            loader.clone(),
            Some(policy_ref.clone()),
            NetworkStateReason::ControlRefresh,
        )
        .await;
    let TrafficPolicy::Ready(first_policy) = state.wait_for_tcp_policy().await else {
        panic!("first credential policy should be ready")
    };
    assert_eq!(
        state
            .with_network_credentials(&first_policy, |credentials| {
                credentials.value(reference_id).unwrap().to_vec()
            })
            .unwrap(),
        b"Bearer generation-1"
    );

    state
        .refresh(loader, Some(policy_ref), NetworkStateReason::ControlRefresh)
        .await;
    assert_eq!(calls.load(SeqCst), 2);
    assert!(
        state
            .with_network_credentials(&first_policy, |_| ())
            .is_err(),
        "the first policy/material snapshot must be fenced after replacement"
    );
    let TrafficPolicy::Ready(second_policy) = state.wait_for_tcp_policy().await else {
        panic!("replacement credential policy should be ready")
    };
    assert_eq!(
        state
            .with_network_credentials(&second_policy, |credentials| {
                credentials.value(reference_id).unwrap().to_vec()
            })
            .unwrap(),
        b"Bearer generation-2"
    );
}

fn controlled_success_loader(
    policy_ref: EffectiveNetworkPolicyRef,
) -> (PolicyLoader, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(Some(release_rx)));

    let loader = Arc::new(move || {
        let started_tx = Arc::clone(&started_tx);
        let release_rx = Arc::clone(&release_rx);
        let policy_ref = policy_ref.clone();
        Box::pin(async move {
            if let Some(started_tx) = started_tx.lock().unwrap().take() {
                assert!(started_tx.send(()).is_ok());
            }
            let release_rx = release_rx
                .lock()
                .unwrap()
                .take()
                .expect("test loader should only be called once");
            release_rx
                .await
                .expect("test loader release sender should stay alive");
            Ok(PulledNetworkPolicy {
                policy: pplx_space_net_common::NetworkPolicyV1::allow_all(),
                policy_ref,
                network_credentials: Default::default(),
            })
        })
            as Pin<
                Box<
                    dyn Future<Output = Result<PulledNetworkPolicy, HydrateNetworkStateError>>
                        + Send,
                >,
            >
    });

    (loader, started_rx, release_tx)
}

async fn assert_stale_refresh_success_cannot_overwrite(terminal_mode: NetworkState) {
    let state = GatewayNetworkState::pending_policy();
    let policy_ref = test_policy_ref();
    let (loader, started_rx, release_tx) = controlled_success_loader(policy_ref.clone());
    let refresh_state = state.clone();
    let refresh_policy_ref = policy_ref.clone();
    let refresh_task = tokio::spawn(async move {
        refresh_state
            .refresh(
                loader,
                Some(refresh_policy_ref),
                NetworkStateReason::ControlRefresh,
            )
            .await
    });

    started_rx.await.unwrap();
    match terminal_mode {
        NetworkState::NoIdentity => {
            state.set_no_identity(NetworkStateReason::NoIdentity).await;
        }
        NetworkState::Draining => {
            state.set_draining(NetworkStateReason::Shutdown).await;
        }
        NetworkState::Stopped => {
            state.set_stopped(NetworkStateReason::Shutdown).await;
        }
        _ => panic!("unsupported terminal mode for stale refresh test"),
    }
    release_tx.send(()).unwrap();

    let snapshot = refresh_task.await.unwrap();
    assert_eq!(snapshot.mode, terminal_mode);
    assert!(snapshot.policy.is_none());
    assert!(snapshot.last_error.is_empty());
    assert_eq!(snapshot.last_error_kind, NetworkPolicyErrorKind::None);
}

async fn wait_for_pending_tcp_count(state: &GatewayNetworkState, expected: u64) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if state.snapshot().await.pending_tcp_connections == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn state_change_wait_ignores_existing_watch_version() {
    let state = GatewayNetworkState::pending_policy();
    let policy_ref = test_policy_ref();
    let snapshot = state
        .refresh(
            immediate_success_loader(policy_ref),
            None,
            NetworkStateReason::Startup,
        )
        .await;
    assert_eq!(snapshot.mode, NetworkState::Ready);

    state.set_retry_backoff(Duration::ZERO).await;
    let shutdown = CancellationToken::new();
    let wait_state = state.clone();
    let wait_shutdown = shutdown.clone();
    let wait_task = tokio::spawn(async move {
        wait_state
            .wait_for_state_change_or_shutdown(Duration::from_secs(60), &wait_shutdown)
            .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!wait_task.is_finished());

    shutdown.cancel();
    assert!(wait_task.await.unwrap());
}

#[tokio::test]
async fn state_change_wait_observes_future_watch_version() {
    let state = GatewayNetworkState::pending_policy();
    let shutdown = CancellationToken::new();
    let wait_state = state.clone();
    let wait_shutdown = shutdown.clone();
    let wait_task = tokio::spawn(async move {
        wait_state
            .wait_for_state_change_or_shutdown(Duration::from_secs(60), &wait_shutdown)
            .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!wait_task.is_finished());

    state.set_no_identity(NetworkStateReason::NoIdentity).await;
    let result = tokio::time::timeout(Duration::from_secs(1), wait_task)
        .await
        .expect("wait should wake on a future state change")
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn cancelled_tcp_policy_wait_releases_pending_counter() {
    let state = GatewayNetworkState::pending_policy();
    let wait_state = state.clone();
    let wait_task = tokio::spawn(async move { wait_state.wait_for_tcp_policy().await });

    wait_for_pending_tcp_count(&state, 1).await;
    wait_task.abort();
    assert!(wait_task.await.unwrap_err().is_cancelled());
    wait_for_pending_tcp_count(&state, 0).await;
}

#[tokio::test]
async fn stale_refresh_success_cannot_overwrite_draining() {
    assert_stale_refresh_success_cannot_overwrite(NetworkState::Draining).await;
}

#[tokio::test]
async fn stale_refresh_success_cannot_overwrite_stopped() {
    assert_stale_refresh_success_cannot_overwrite(NetworkState::Stopped).await;
}

#[tokio::test]
async fn stale_refresh_success_cannot_overwrite_no_identity() {
    assert_stale_refresh_success_cannot_overwrite(NetworkState::NoIdentity).await;
}

#[tokio::test]
async fn explicit_refresh_from_no_identity_can_install_policy() {
    let state = GatewayNetworkState::no_identity();
    let policy_ref = test_policy_ref();

    let snapshot = state
        .refresh(
            immediate_success_loader(policy_ref.clone()),
            Some(policy_ref.clone()),
            NetworkStateReason::PoolAdoption,
        )
        .await;

    assert_eq!(snapshot.mode, NetworkState::Ready);
    assert!(snapshot.policy.is_some());
    assert_eq!(snapshot.installed_policy_ref, policy_ref.to_string());
    assert!(snapshot.last_error.is_empty());
    assert_eq!(snapshot.last_error_kind, NetworkPolicyErrorKind::None);
}

#[test]
fn external_pool_adoption_rollback_reason_is_preserved() {
    assert_eq!(
        NetworkStateReason::from_external("pool-adoption-rollback"),
        NetworkStateReason::PoolAdoptionRollback
    );
    assert_eq!(
        NetworkStateReason::from_external("pool_adoption_rollback").as_str(),
        "pool_adoption_rollback"
    );
}

#[tokio::test]
async fn terminal_non_error_states_report_empty_last_error() {
    let state = GatewayNetworkState::pending_policy();
    let error_snapshot = state
        .refresh(
            immediate_error_loader("loader failed"),
            None,
            NetworkStateReason::ControlRefresh,
        )
        .await;
    assert_eq!(error_snapshot.mode, NetworkState::PolicyError);
    assert!(!error_snapshot.last_error.is_empty());

    let no_identity = state.set_no_identity(NetworkStateReason::NoIdentity).await;
    assert_eq!(no_identity.mode, NetworkState::NoIdentity);
    assert!(no_identity.last_error.is_empty());
    assert_eq!(no_identity.last_error_kind, NetworkPolicyErrorKind::None);

    let state = GatewayNetworkState::pending_policy();
    state
        .refresh(
            immediate_error_loader("loader failed"),
            None,
            NetworkStateReason::ControlRefresh,
        )
        .await;
    state.set_draining(NetworkStateReason::Shutdown).await;
    let draining = state.snapshot().await;
    assert_eq!(draining.mode, NetworkState::Draining);
    assert!(draining.last_error.is_empty());
    assert_eq!(draining.last_error_kind, NetworkPolicyErrorKind::None);

    let state = GatewayNetworkState::pending_policy();
    state
        .refresh(
            immediate_error_loader("loader failed"),
            None,
            NetworkStateReason::ControlRefresh,
        )
        .await;
    state.set_stopped(NetworkStateReason::Shutdown).await;
    let stopped = state.snapshot().await;
    assert_eq!(stopped.mode, NetworkState::Stopped);
    assert!(stopped.last_error.is_empty());
    assert_eq!(stopped.last_error_kind, NetworkPolicyErrorKind::None);

    let policy_ref = test_policy_ref();
    let refresh_after_stopped = state
        .refresh(
            immediate_success_loader(policy_ref.clone()),
            Some(policy_ref),
            NetworkStateReason::ControlRefresh,
        )
        .await;
    assert_eq!(refresh_after_stopped.mode, NetworkState::Stopped);
    assert!(refresh_after_stopped.last_error.is_empty());
    assert_eq!(
        refresh_after_stopped.last_error_kind,
        NetworkPolicyErrorKind::None
    );
}
