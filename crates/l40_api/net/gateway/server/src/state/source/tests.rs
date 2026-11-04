use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn network_policy_loader_requires_v1_and_a_valid_reference() {
    let v1 = NetworkPolicyV1::deny_all();
    let v1_proto = NetworkPolicyProto::from(&v1);
    let response = GetNetworkPolicyResponse {
        effective_policy: Some(v1_proto),
        effective_policy_ref: EffectiveNetworkPolicyRef::no_identity().to_string(),
        ..Default::default()
    };

    assert_eq!(
        InitialNetworkState::from_host_control(response.clone())
            .unwrap()
            .into_policy()
            .policy,
        v1,
        "the V1 host-control field hydrates the gateway"
    );

    assert!(matches!(
        InitialNetworkState::from_host_control(GetNetworkPolicyResponse {
            effective_policy: None,
            ..response.clone()
        }),
        Err(HydrateNetworkStateError::Policy(
            NetworkPolicyFromHostControlError::MissingV1Policy
        ))
    ));

    for (reference, expected) in [
        ("", EffectiveNetworkPolicyRefParseError::Empty),
        (
            "invalid",
            EffectiveNetworkPolicyRefParseError::InvalidFormat,
        ),
    ] {
        let error = InitialNetworkState::from_host_control(GetNetworkPolicyResponse {
            effective_policy_ref: reference.to_owned(),
            ..response.clone()
        })
        .err()
        .expect("host-control responses must reject invalid references before hydration succeeds");
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<EffectiveNetworkPolicyRefParseError>(),
            Some(&expected),
        );
        assert!(matches!(
            error,
            HydrateNetworkStateError::InvalidReference { reference: raw, source }
                if raw == reference && source == expected
        ));
    }
}

/// sandbox-manager registers a gateway only after its launch returns, so the
/// first pulls are refused. Within the registration window that refusal is
/// pending, not a failure: no failure event, retries that back off rather than
/// storm host-control on one reused connection, and a pull that completes as
/// soon as the owner's first control call, which follows registration, arrives.
/// A gateway that is never registered fails once, within bounded attempts,
/// including when a pull sent before the window closes is refused after it.
#[tokio::test(start_paused = true)]
async fn initial_hydration_treats_refusal_before_registration_as_pending() {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt as _;

    // With `crosses_window`, every pull takes `SLOW_REPLY`, which keeps the
    // attempt budget from running out before the window does, and the first
    // pull sent within `IN_FLIGHT` of the window's end is answered just after
    // it. `IN_FLIGHT` exceeds the gap between pulls, so one such pull is sent.
    const SLOW_REPLY: Duration = Duration::from_millis(100);
    const IN_FLIGHT: Duration = Duration::from_millis(400);
    let window_closed = REGISTRATION_WINDOW + Duration::from_millis(1);
    let sandbox_id = pplx_space_common_core::types::SandboxId::new();
    for (registered_after, crosses_window) in [
        (Some(Duration::from_millis(300)), false),
        (None, false),
        (None, true),
    ] {
        let layer = CapturedEventsLayer::new();
        let events = layer.events();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        let launched_at = tokio::time::Instant::now();
        let (owner_contact, owner_contacted) = tokio::sync::watch::channel(false);
        if let Some(after) = registered_after {
            tokio::spawn(async move {
                tokio::time::sleep(after).await;
                owner_contact.send_replace(true);
            });
        }
        let connects = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connect = {
            let connects = Arc::clone(&connects);
            async move {
                connects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        let pull = {
            let attempts = Arc::clone(&attempts);
            move |(), pending_refusal: Option<ExpectedGrpcStatus>| {
                let elapsed = launched_at.elapsed();
                attempts
                    .lock()
                    .unwrap()
                    .push((elapsed, pending_refusal.is_some()));
                let registered = registered_after.is_some_and(|after| elapsed >= after);
                let reply_after = if !crosses_window {
                    Duration::ZERO
                } else if elapsed < REGISTRATION_WINDOW
                    && elapsed + IN_FLIGHT >= REGISTRATION_WINDOW
                {
                    window_closed - elapsed
                } else {
                    SLOW_REPLY
                };
                async move {
                    tokio::time::sleep(reply_after).await;
                    if registered {
                        Ok(InitialNetworkState {
                            policy: NetworkPolicyV1::allow_all(),
                            network_credentials: Default::default(),
                            policy_ref: EffectiveNetworkPolicyRef::no_identity(),
                            network_tx_bytes: 0,
                            network_rx_bytes: 0,
                        })
                    } else {
                        Err(HydrateNetworkStateError::GetNetworkPolicy {
                            sandbox_id,
                            source: tonic::Status::permission_denied("unknown caller"),
                        })
                    }
                }
            }
        };
        // Every wait its full bound, so the attempt count is exact.
        let wait = RegistrationWait {
            launched_at,
            owner_contacted,
            jitter: |bound| bound,
        };

        let result = hydrate_network_state(sandbox_id, wait, connect, pull).await;
        let finished = launched_at.elapsed();
        let (attempts, marked): (Vec<_>, Vec<_>) = attempts.lock().unwrap().iter().copied().unzip();
        let failed = events
            .events()
            .iter()
            .filter(|event| {
                field_value(event, "event_type") == Some("network_policy_enforcement_failed")
            })
            .count();
        assert_eq!(
            attempts.first(),
            Some(&Duration::ZERO),
            "the first pull is immediate"
        );
        assert_eq!(
            connects.load(Ordering::SeqCst),
            1,
            "one connection for every pull"
        );
        match registered_after {
            Some(after) => {
                assert!(result.is_ok());
                assert_eq!(failed, 0, "a refusal before registration is not a failure");
                assert_eq!(finished, after, "the owner's call ends the wait at once");
                // Waits of 10, 20, 40, 80 and 160 ms: pulls at 0, 10, 30, 70
                // and 150 ms, then the owner's call ends the next wait.
                assert_eq!(attempts.len(), 6, "{attempts:?}");
                // Refusals before the owner's call are pending registration;
                // the pull after it must succeed, so its refusal stays an error.
                assert_eq!(marked, [true, true, true, true, true, false]);
            }
            None if crosses_window => {
                assert!(result.is_err());
                assert_eq!(failed, 1);
                // The pull sent before the window closed was marked, and its
                // refusal, answered after the window, was retried rather than
                // ending the wait. The pull sent after the window is the last,
                // and the refusal that ends the wait is never marked.
                let [.., in_flight, last] = attempts[..] else {
                    panic!("{attempts:?}");
                };
                assert!(in_flight < REGISTRATION_WINDOW, "{attempts:?}");
                assert!(last >= window_closed, "{attempts:?}");
                assert_eq!(marked.last(), Some(&false), "{marked:?}");
                assert!(marked[..marked.len() - 1].iter().all(|marked| *marked));
                assert!(finished <= window_closed + PENDING_RETRY_CAP + SLOW_REPLY);
            }
            None => {
                assert!(result.is_err());
                assert_eq!(failed, 1);
                assert!(attempts.len() <= usize::try_from(MAX_PENDING_ATTEMPTS).unwrap());
                assert!(finished <= REGISTRATION_WINDOW + PENDING_RETRY_CAP);
                // The refusal that ends the wait is a failure, so its span is
                // an error: the last pull is never marked expected.
                assert_eq!(marked.last(), Some(&false), "{marked:?}");
                assert!(marked[..marked.len() - 1].iter().all(|marked| *marked));
                // Past the doubling, retries are a full cap apart, not a storm.
                assert!(
                    attempts
                        .windows(2)
                        .skip(6)
                        .all(|pair| pair[1] - pair[0] == PENDING_RETRY_CAP),
                    "{attempts:?}"
                );
            }
        }
    }
}

/// A pull hands its pending-registration mark to the client trace layer as an
/// extension of the request it sends; an unmarked pull sends none.
#[tokio::test]
async fn policy_pull_attaches_its_pending_refusal_mark_to_the_request() {
    #[derive(Clone, Default)]
    struct CaptureMark(Arc<std::sync::Mutex<Vec<Option<ExpectedGrpcStatus>>>>);

    impl tonic::codegen::Service<http::Request<tonic::body::Body>> for CaptureMark {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
            self.0
                .lock()
                .unwrap()
                .push(request.extensions().get::<ExpectedGrpcStatus>().copied());
            std::future::ready(Ok(
                tonic::Status::permission_denied("unknown caller").into_http()
            ))
        }
    }

    let capture = CaptureMark::default();
    let sandbox_id = pplx_space_common_core::types::SandboxId::new();
    let pending = ExpectedGrpcStatus::new(tonic::Code::PermissionDenied);
    for mark in [Some(pending), None] {
        let pulled =
            pull_network_state(sandbox_id, HostControlClient::new(capture.clone()), mark).await;
        assert!(matches!(pulled, Err(error) if error.is_unregistered_caller()));
    }
    assert_eq!(*capture.0.lock().unwrap(), [Some(pending), None]);
}
