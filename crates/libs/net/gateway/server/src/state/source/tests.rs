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
/// A gateway that is never registered fails once, within bounded attempts.
#[tokio::test(start_paused = true)]
async fn initial_hydration_treats_refusal_before_registration_as_pending() {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt as _;

    let sandbox_id = pplx_space_common_core::types::SandboxId::new();
    for registered_after in [Some(Duration::from_millis(300)), None] {
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
            move |()| {
                let elapsed = launched_at.elapsed();
                attempts.lock().unwrap().push(elapsed);
                let registered = registered_after.is_some_and(|after| elapsed >= after);
                async move {
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
        let attempts = attempts.lock().unwrap().clone();
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
            }
            None => {
                assert!(result.is_err());
                assert_eq!(failed, 1);
                assert!(attempts.len() <= usize::try_from(MAX_PENDING_ATTEMPTS).unwrap());
                assert!(finished <= REGISTRATION_WINDOW + PENDING_RETRY_CAP);
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
