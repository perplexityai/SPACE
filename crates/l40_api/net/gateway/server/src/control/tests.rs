// Both existing readiness replies use these tags. A peer that only understands
// this pre-TLS contract must still distinguish ready from pending.
#[derive(Clone, PartialEq, prost::Message)]
struct LegacyReadiness {
    #[prost(int32, tag = "1")]
    state: i32,
    #[prost(string, tag = "2")]
    installed_policy_ref: String,
}

fn root_request<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(
            0,
            0,
            Some(std::process::id().try_into().unwrap()),
        ));
    request
}

#[tokio::test]
async fn control_rpcs_preserve_readiness_and_report_installed_tls_policy() {
    use super::*;
    use crate::state::PulledNetworkPolicy;
    use pplx_space_net_common::{EffectiveNetworkPolicyRefScope, NetworkPolicyV1, TlsInterception};
    use prost::Message;
    use std::sync::atomic::{AtomicBool, Ordering};

    let policy_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sandbox-1",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    for mode in [TlsInterception::Off, TlsInterception::On] {
        let ca_ready = Arc::new(AtomicBool::new(false));
        let state = GatewayNetworkState::pending_policy()
            .with_traffic_gate({
                let ca_ready = ca_ready.clone();
                Arc::new(move || ca_ready.load(Ordering::SeqCst))
            })
            .with_readiness_metrics();
        let loader: PolicyLoader = Arc::new({
            let policy_ref = policy_ref.clone();
            move || {
                let policy_ref = policy_ref.clone();
                Box::pin(async move {
                    let mut policy = NetworkPolicyV1::allow_all();
                    policy.tls_interception = mode;
                    Ok(PulledNetworkPolicy {
                        policy,
                        policy_ref,
                        network_credentials: Default::default(),
                    })
                })
            }
        });
        let service = NetGatewayControlService::new(
            Arc::default(),
            state,
            loader,
            GatewayDebugState::default(),
            ServicePortIngressState::default(),
            CancellationToken::new(),
        );
        let reported_mode = service
            .get_stats(root_request(GetStatsRequest {}))
            .await
            .unwrap()
            .into_inner()
            .tls_interception_mode;
        assert_eq!(
            pplx_space_net_common::proto::TlsInterception::try_from(reported_mode).unwrap(),
            pplx_space_net_common::proto::TlsInterception::Off
        );
        let pending = service
            .get_network_state(root_request(GetNetworkStateRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            LegacyReadiness::decode(pending.encode_to_vec().as_slice())
                .unwrap()
                .state,
            2
        );

        for installed in [false, true, false, true] {
            ca_ready.store(installed, Ordering::SeqCst);
            let refreshed = service
                .refresh_network_policy(root_request(RefreshNetworkPolicyRequest {
                    expected_policy_ref: policy_ref.to_string(),
                    reason: "start".to_owned(),
                }))
                .await
                .unwrap()
                .into_inner();
            let observed = service
                .get_network_state(root_request(GetNetworkStateRequest {}))
                .await
                .unwrap()
                .into_inner();
            let reported_mode = service
                .get_stats(root_request(GetStatsRequest {}))
                .await
                .unwrap()
                .into_inner()
                .tls_interception_mode;
            assert_eq!(
                pplx_space_net_common::proto::TlsInterception::try_from(reported_mode).unwrap(),
                match mode {
                    TlsInterception::Off => pplx_space_net_common::proto::TlsInterception::Off,
                    TlsInterception::On => pplx_space_net_common::proto::TlsInterception::On,
                }
            );
            // An installed policy is Ready on the legacy readiness
            // contract regardless of the CA gate. Old sandbox-managers that only
            // decode `state` must never see a CA outage as a failed boot.
            for bytes in [refreshed.encode_to_vec(), observed.encode_to_vec()] {
                let legacy = LegacyReadiness::decode(bytes.as_slice()).unwrap();
                assert_eq!(legacy.state, 3, "{mode:?}, CA installed={installed}");
                assert_eq!(legacy.installed_policy_ref, policy_ref.to_string());
            }
            assert!(observed.last_error.is_empty());
            // The gate is reported on the side, on every readiness reply.
            let expected_readiness = match (mode, installed) {
                (TlsInterception::Off, _) => crate::proto::InterceptionReadiness::NotRequired,
                (TlsInterception::On, true) => crate::proto::InterceptionReadiness::Ready,
                (TlsInterception::On, false) => {
                    crate::proto::InterceptionReadiness::DependenciesNotReady
                }
            };
            let stats = service
                .get_stats(root_request(GetStatsRequest {}))
                .await
                .unwrap()
                .into_inner();
            for (label, reported) in [
                ("refresh", refreshed.interception_readiness),
                ("get_network_state", observed.interception_readiness),
                ("get_stats", stats.interception_readiness),
            ] {
                assert_eq!(
                    crate::proto::InterceptionReadiness::try_from(reported).unwrap(),
                    expected_readiness,
                    "{label}: {mode:?}, CA installed={installed}"
                );
            }
        }
    }
}
