use super::{
    KnownEgressAssignmentError, KnownEgressConfig, KnownEgressLocalError, MAX_KNOWN_EGRESS_DOMAINS,
    NetGatewayLaunchConfig, NetGatewayRouteIdentityPushError, NetGatewayRouteIdentityPushOutcome,
    NetGatewayRouteVerificationKeyPushError, NetGatewayRouteVerificationKeyPushOutcome, Operation,
    Outcome, RefreshAbandonReason, classify_route_identity_push,
    classify_route_verification_key_response, known_egress_domains_from_assignment,
    local_known_egress_domains, net_gateway_client_span, net_gateway_launch_config_with,
    netgtw_launch_args, network_policy_refresh_error_to_status, push_route_identity,
    record_route_security_push_span, require_local_authority_peer,
    validate_route_verification_profile,
};
use crate::db::PolicyAuthority;
use crate::state::SandboxState;
use pplx_space_common_core::host::linux::identities::user_names;
use pplx_space_common_svc::{LocalRole, UdsPeerPrincipal};
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG};

// `authority=local` pins a node-local, cluster-immune policy (network or
// auto-lifecycle), so only an on-node operator may set it. The forwarding
// service principals (`space-nodeapi`, `space-nodectl`) — the reach any
// node-api-allowlisted ServiceAccount has into these RPCs — must be refused for
// `Local`, while `Cluster` is always allowed.
#[test]
fn require_local_authority_peer_gates_local_to_on_node_operators() {
    let operators = [
        UdsPeerPrincipal::Root,
        UdsPeerPrincipal::Role(LocalRole::Operator),
        UdsPeerPrincipal::Role(LocalRole::Admin),
    ];
    let refused = [
        UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
        UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
        UdsPeerPrincipal::Role(LocalRole::Observer),
        UdsPeerPrincipal::Other,
    ];

    for principal in operators {
        require_local_authority_peer(principal, PolicyAuthority::Local)
            .expect("on-node operator may set authority=local");
        require_local_authority_peer(principal, PolicyAuthority::Cluster)
            .expect("authority=cluster is always allowed");
    }
    for principal in refused {
        assert_eq!(
            require_local_authority_peer(principal, PolicyAuthority::Local)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        require_local_authority_peer(principal, PolicyAuthority::Cluster)
            .expect("authority=cluster is always allowed");
    }
}

// The abandon reason's Display is the single source of truth for the
// log/Status message; pin both strings so a rename can't silently change
// the operator-visible message.
#[test]
fn refresh_abandon_reason_display_is_stable() {
    assert_eq!(
        RefreshAbandonReason::SandboxRowGone.to_string(),
        "refresh abandoned: sandbox row no longer exists"
    );
    assert_eq!(
        RefreshAbandonReason::StateCannotAuthorizeGateway(SandboxState::Deleting).to_string(),
        "refresh abandoned: sandbox state deleting cannot have a live net-gateway"
    );
}

#[test]
fn verification_profile_accepts_unknown_or_supported_but_rejects_known_old_gateways() {
    for profile in [None, Some(1), Some(u32::MAX)] {
        validate_route_verification_profile(profile).unwrap();
    }
    assert_eq!(
        validate_route_verification_profile(Some(0))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[test]
fn invalid_argument_on_a_route_identity_is_a_producer_bug() {
    for version in [0, 1, u32::MAX] {
        let result = classify_route_identity_push(Ok(version));
        if version == 0 {
            let error = result.unwrap_err();
            assert!(matches!(
                error,
                NetGatewayRouteIdentityPushError::UnsupportedProfile(0)
            ));
            assert_eq!(
                super::route_identity_push_status(error).code(),
                tonic::Code::Unimplemented
            );
        } else {
            assert_eq!(result.unwrap().identity_profile_version(), Some(version));
        }
    }
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::Unimplemented,
        tonic::Code::Unavailable,
    ] {
        let error = classify_route_identity_push(Err(NetGatewayRouteIdentityPushError::Rpc(
            tonic::Status::new(code, "route identity rejected"),
        )))
        .unwrap_err();
        assert!(
            matches!(error, NetGatewayRouteIdentityPushError::Rpc(status) if status.code() == code)
        );
    }
}

// Divergence is carried by the outcome, not re-encoded as a gRPC status
// for the reconciler to re-parse; it contributes no identity profile
// version, so the verification-key push probes rather than skips.
#[test]
fn failed_precondition_is_a_divergence_outcome() {
    let outcome = classify_route_identity_push(Err(NetGatewayRouteIdentityPushError::Rpc(
        tonic::Status::failed_precondition("stale route identity generation"),
    )))
    .expect("persistent divergence must not starve verification-key push");
    assert_eq!(outcome, NetGatewayRouteIdentityPushOutcome::Diverged);
    assert_eq!(outcome.identity_profile_version(), None);
}

#[derive(Clone, Default)]
struct SpanFieldCapture {
    fields: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    names: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl SpanFieldCapture {
    fn get(&self, name: &str) -> Option<String> {
        self.fields
            .lock()
            .expect("capture mutex poisoned")
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.clone())
    }

    fn contains_span(&self, name: &str) -> bool {
        self.names
            .lock()
            .expect("capture mutex poisoned")
            .iter()
            .any(|span_name| span_name == name)
    }
}

impl<S> tracing_subscriber::Layer<S> for SpanFieldCapture
where
    S: tracing::Subscriber,
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.names
            .lock()
            .expect("capture mutex poisoned")
            .push(attributes.metadata().name().to_string());
    }

    fn on_record(
        &self,
        _id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = SpanFieldGrabber::default();
        values.record(&mut visitor);
        self.fields
            .lock()
            .expect("capture mutex poisoned")
            .extend(visitor.fields);
    }
}

#[derive(Default)]
struct SpanFieldGrabber {
    fields: Vec<(String, String)>,
}

impl tracing::field::Visit for SpanFieldGrabber {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

fn captured_route_security_push_span(
    operation: Operation,
    outcome: Result<Outcome, &NetGatewayRouteVerificationKeyPushError>,
    failure: Option<&str>,
) -> SpanFieldCapture {
    use tracing_subscriber::layer::SubscriberExt as _;

    let capture = SpanFieldCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        let span = net_gateway_client_span("SetRouteVerificationKeys", "sandbox-id");
        record_route_security_push_span(&span, operation, outcome, failure);
    });
    capture
}

#[test]
fn unsupported_verification_key_push_records_an_error_span() {
    let error = NetGatewayRouteVerificationKeyPushError::Rpc(tonic::Status::unimplemented(
        "gateway does not implement verification-key synchronization",
    ));
    let failure = error.to_string();
    let capture =
        captured_route_security_push_span(Operation::VerificationKeys, Err(&error), Some(&failure));

    assert_eq!(capture.get("otel.status_code").as_deref(), Some("ERROR"));
    assert_eq!(
        capture.get("route_security.outcome").as_deref(),
        Some("failed")
    );
}

// The same recorder serves the identity push, so acknowledged transport
// outcomes must not produce error spans either.
#[test]
fn tolerated_identity_push_outcomes_record_ok_spans() {
    for outcome in [Outcome::Applied, Outcome::Absent, Outcome::Diverged] {
        let capture =
            captured_route_security_push_span(Operation::Identity, Ok(outcome), Some("ignored"));
        assert_eq!(
            capture.get("otel.status_code").as_deref(),
            Some("OK"),
            "{outcome} is a tolerated push outcome"
        );
        assert_eq!(
            capture.get("route_security.outcome").as_deref(),
            Some(outcome.as_str())
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn absent_route_identity_does_not_create_a_client_rpc_span() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let capture = SpanFieldCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);
    let identity = SpaceApiRouteIdentity::default();

    assert_eq!(
        push_route_identity("sandbox-id", &identity)
            .await
            .expect("an identity-free local gateway needs no identity RPC"),
        NetGatewayRouteIdentityPushOutcome::Absent
    );
    assert!(
        !capture.contains_span("net_gateway.route_security_push"),
        "no SetRouteIdentity RPC was attempted"
    );
}

#[test]
fn failed_verification_key_push_records_an_error_span() {
    let error = NetGatewayRouteVerificationKeyPushError::GenerationMismatch {
        expected: 2,
        applied: 1,
    };
    let failure = error.to_string();
    let capture = captured_route_security_push_span(
        Operation::VerificationKeys,
        Err(&error),
        Some(failure.as_str()),
    );

    assert_eq!(capture.get("otel.status_code").as_deref(), Some("ERROR"));
    assert_eq!(
        capture.get("route_security.outcome").as_deref(),
        Some("failed")
    );
    assert!(
        capture
            .get("otel.status_description")
            .is_some_and(|description| description.contains("generation mismatch")),
        "the span must describe why the push failed"
    );
}

#[test]
fn verification_key_response_requires_matching_generation_and_supported_rpc() {
    let generation = 3_u64.try_into().unwrap();
    for code in [tonic::Code::Unimplemented, tonic::Code::Unavailable] {
        let error = classify_route_verification_key_response(
            Err(tonic::Status::new(code, "gateway failed")),
            generation,
        )
        .unwrap_err();
        assert!(
            matches!(error, NetGatewayRouteVerificationKeyPushError::Rpc(status) if status.code() == code)
        );
    }
    for applied in [2, 3, 4] {
        let result = classify_route_verification_key_response(
            Ok(
                pplx_space_net_gateway_client::proto::SetRouteVerificationKeysResponse {
                    generation: applied,
                    identity_profile_version: 1,
                },
            ),
            generation,
        );
        if applied == 3 {
            assert_eq!(
                result.unwrap(),
                NetGatewayRouteVerificationKeyPushOutcome::Applied
            );
        } else {
            assert!(matches!(
                result,
                Err(NetGatewayRouteVerificationKeyPushError::GenerationMismatch { .. })
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn refresh_waits_for_delayed_readiness_but_bounds_pending_and_hung_requests() {
    use super::super::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::SandboxId;
    use pplx_space_net_gateway_client::proto::{NetworkState, RefreshNetworkPolicyResponse};

    use std::time::Duration;

    for (ready_after_secs, request_delay_secs, matching_ref, expect_ready) in [
        (Some(0), 0, true, true),
        (Some(45), 0, true, true),
        (Some(55), 0, true, true),
        (Some(60), 0, true, false),
        (Some(75), 0, true, false),
        (None, 0, true, false),
        (Some(0), 60, true, false),
        (Some(0), 120, true, false),
        (Some(0), 0, false, false),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let db = crate::db::SharedConnection::new(conn);
        let expected_ref = format!(
            "cluster:sandbox:{sandbox_id}:generation:1:sha256:{}",
            "a".repeat(64)
        )
        .parse::<pplx_space_net_common::EffectiveNetworkPolicyRef>()
        .unwrap();
        let started = tokio::time::Instant::now();
        let result = super::refresh_network_policy_with_request(
            &db,
            &sandbox_id,
            Some(&expected_ref),
            "snapshot-restore",
            |request| {
                assert_eq!(request.expected_policy_ref, expected_ref.to_string());
                let ready = ready_after_secs
                    .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds));
                async move {
                    tokio::time::sleep(Duration::from_secs(request_delay_secs)).await;
                    Ok(RefreshNetworkPolicyResponse {
                        state: if ready {
                            NetworkState::Ready
                        } else {
                            NetworkState::PendingPolicy
                        } as i32,
                        // A hydrated policy alone does not satisfy readiness.
                        installed_policy_ref: if matching_ref {
                            request.expected_policy_ref
                        } else {
                            request
                                .expected_policy_ref
                                .replace("generation:1", "generation:2")
                        },
                        // Leaving interception_readiness at its zero default
                        // makes this table double as an old gateway that omits
                        // the field; setting it here drops that coverage.
                        ..Default::default()
                    })
                }
            },
        )
        .await;
        if expect_ready {
            let seconds = ready_after_secs.expect("successful case must become ready");
            result.expect("delayed network readiness must not exhaust the initial retry schedule");
            assert!(started.elapsed() >= Duration::from_secs(seconds));
            assert!(started.elapsed() < Duration::from_secs(60));
            if seconds == 0 {
                assert!(started.elapsed() < Duration::from_secs(1));
            }
        } else {
            let error = result.expect_err("readiness must fail closed by its deadline");
            assert_eq!(
                network_policy_refresh_error_to_status(error).code(),
                tonic::Code::Unavailable
            );
            assert_eq!(started.elapsed(), Duration::from_secs(60));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn refresh_rechecks_deletion_after_rpc_and_honors_a_shorter_caller_deadline() {
    use super::super::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::SandboxId;
    use pplx_space_net_gateway_client::proto::{NetworkState, RefreshNetworkPolicyResponse};

    use std::time::Duration;

    for delete_during_rpc in [false, true] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let db = crate::db::SharedConnection::new(conn);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            super::NETWORK_POLICY_SYNC_REFRESH_TIMEOUT,
            super::refresh_network_policy_with_request(&db, &sandbox_id, None, "test", |_| async {
                if delete_during_rpc {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    crate::db::delete_sandbox(&db.lock().unwrap(), &sandbox_id).unwrap();
                    Ok(RefreshNetworkPolicyResponse {
                        state: NetworkState::Ready as i32,
                        ..Default::default()
                    })
                } else {
                    std::future::pending().await
                }
            }),
        )
        .await;
        if delete_during_rpc {
            assert!(result.unwrap().unwrap_err().is_abandoned());
            assert_eq!(started.elapsed(), Duration::from_secs(1));
        } else {
            assert!(
                result.is_err(),
                "caller deadline must bound connection and RPC waiting"
            );
            assert_eq!(
                started.elapsed(),
                super::NETWORK_POLICY_SYNC_REFRESH_TIMEOUT
            );
        }
    }
}

// A deleted sandbox must not spend retries connecting to its removed gateway.
#[tokio::test(start_paused = true)]
async fn refresh_abandons_schedule_when_sandbox_row_is_gone() {
    let conn = crate::db::test_conn();
    let db = crate::db::SharedConnection::new(conn);

    let started = tokio::time::Instant::now();
    let error = super::refresh_network_policy(
        &db,
        "019f0000-0000-7000-8000-000000000000",
        None,
        "test-abandon",
    )
    .await
    .expect_err("refresh against a missing sandbox row must fail");

    assert!(error.is_abandoned());
    let error = network_policy_refresh_error_to_status(error);
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("abandoned"),
        "error should name the abandon, got: {}",
        error.message()
    );
    // The abandon must land before the first connect attempt: a single
    // failed connect advances (paused) time by its ~10 s budget, and the
    // full schedule by ~2 virtual minutes.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "refresh burned the retry schedule instead of abandoning: {:?}",
        started.elapsed()
    );
}

use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, PodUid, RouteGeneration, SpaceApiRouteIdentity,
};

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|position| args.get(position + 1))
        .map(String::as_str)
}

fn flag_values<'a>(args: &'a [String], flag: &str) -> Vec<&'a str> {
    args.windows(2)
        .filter(|pair| pair[0] == flag)
        .map(|pair| pair[1].as_str())
        .collect()
}

fn test_route_identity() -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        route_generation: Some(RouteGeneration::new(42).expect("route generation parses")),
        current_node_id: Some(NodeId::new()),
        substrate_kind: Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes),
        execution_generation: Some(ExecutionGeneration::new(7).expect("generation parses")),
        pod_uid: Some(PodUid::parse("pod-uid-42").expect("pod uid parses")),
        ..Default::default()
    }
}

#[test]
fn launch_args_always_carry_socket_and_sandbox_id() {
    let args = netgtw_launch_args("sbx-1", false, None, &NetGatewayLaunchConfig::default());
    let sandbox_id_pos = args
        .iter()
        .position(|arg| arg == "--sandbox-id")
        .expect("--sandbox-id present");
    assert_eq!(args[sandbox_id_pos + 1], "sbx-1");
    assert!(has_flag(&args, "--socket"));
    assert!(!has_flag(&args, "--tls-interception-mode"));
    assert!(
        !has_flag(&args, "--tcp-domain-authorization-mode"),
        "omitting the retired mode also selects the older gateway's default DNS-attribution path"
    );
}

#[test]
fn no_identity_flag_is_present_only_when_enabled() {
    let pool_args = netgtw_launch_args("sbx-1", true, None, &NetGatewayLaunchConfig::default());
    assert!(has_flag(&pool_args, "--no-identity"));
    assert!(!has_flag(
        &netgtw_launch_args("sbx-1", false, None, &NetGatewayLaunchConfig::default()),
        "--no-identity"
    ));
}

#[test]
fn launch_args_include_route_identity_for_tenant_gateway() {
    let identity = test_route_identity();
    let args = netgtw_launch_args(
        "sbx-1",
        false,
        Some(&identity),
        &NetGatewayLaunchConfig::default(),
    );
    assert_eq!(flag_value(&args, "--route-generation"), Some("42"));
    assert_eq!(
        flag_value(&args, "--current-node-id"),
        identity
            .current_node_id
            .map(|node_id| node_id.to_string())
            .as_deref()
    );
    assert_eq!(flag_value(&args, "--execution-generation"), Some("7"));
    assert_eq!(flag_value(&args, "--pod-uid"), Some("pod-uid-42"));
}

#[test]
fn launch_args_omit_absent_route_identity_fields() {
    let identity = SpaceApiRouteIdentity {
        route_generation: Some(RouteGeneration::new(42).expect("route generation parses")),
        ..Default::default()
    };
    let args = netgtw_launch_args(
        "sbx-1",
        false,
        Some(&identity),
        &NetGatewayLaunchConfig::default(),
    );
    assert_eq!(flag_value(&args, "--route-generation"), Some("42"));
    assert!(!has_flag(&args, "--current-node-id"));
    assert!(!has_flag(&args, "--execution-generation"));
    assert!(!has_flag(&args, "--pod-uid"));
}

#[test]
fn launch_args_do_not_include_route_identity_for_no_identity_gateway() {
    let identity = test_route_identity();
    let args = netgtw_launch_args(
        "sbx-1",
        true,
        Some(&identity),
        &NetGatewayLaunchConfig::default(),
    );
    for flag in [
        "--route-generation",
        "--current-node-id",
        "--execution-generation",
        "--pod-uid",
    ] {
        assert!(!has_flag(&args, flag));
    }
}

#[test]
fn known_egress_assignment_becomes_gateway_launch_arguments() {
    let domains = known_egress_domains_from_assignment(serde_json::json!([
        "Egress-Proxy.Example.Test.",
        "git-proxy.example.test"
    ]))
    .unwrap();
    let args = netgtw_launch_args(
        "sbx-1",
        false,
        None,
        &NetGatewayLaunchConfig {
            known_egress_domains: domains,
        },
    );

    assert_eq!(
        flag_values(&args, "--known-egress-domain"),
        ["egress-proxy.example.test", "git-proxy.example.test"]
    );
    assert!(matches!(
        known_egress_domains_from_assignment(serde_json::json!(["not a domain"])),
        Err(KnownEgressAssignmentError::InvalidDomains(_))
    ));
    assert!(matches!(
        known_egress_domains_from_assignment(serde_json::json!(vec![
            "example.com";
            MAX_KNOWN_EGRESS_DOMAINS + 1
        ])),
        Err(KnownEgressAssignmentError::TooManyDomains(_))
    ));
}

/// The object form carries the same domains as the bare array.
///
/// It exists for OpenFeature backends, whose JSON resolution returns a
/// structure and so cannot express a top-level array at all. Both forms are
/// compared against the same expected output rather than each being asserted
/// separately, because the thing that matters is that a deployment reading
/// this flag gets identical gateway arguments whichever backend serves it.
#[test]
fn a_known_egress_assignment_reads_the_same_as_an_array_or_as_a_domains_object() {
    let expected = ["egress-proxy.example.test", "git-proxy.example.test"];
    let names = ["Egress-Proxy.Example.Test.", "git-proxy.example.test"];

    let from_array = known_egress_domains_from_assignment(serde_json::json!(names)).unwrap();
    let from_object =
        known_egress_domains_from_assignment(serde_json::json!({"domains": names})).unwrap();

    assert_eq!(from_array, from_object);
    assert_eq!(
        flag_values(
            &netgtw_launch_args(
                "sbx-1",
                false,
                None,
                &NetGatewayLaunchConfig {
                    known_egress_domains: from_object,
                },
            ),
            "--known-egress-domain"
        ),
        expected
    );
}

/// flagd's stringified array reads the same as a real one.
///
/// flagd 0.2.2 hands back a nested array as its JSON text, so without this the
/// flag resolves to no domains at all whenever a deployment serves it through
/// flagd — silently, since an empty list is also what "unset" means. All three
/// shapes are compared against one expected output rather than asserted
/// separately, because the contract is that a deployment gets identical
/// gateway arguments whichever backend and encoding it ends up with.
#[test]
fn a_known_egress_assignment_reads_the_same_when_flagd_stringifies_the_array() {
    let names = ["Egress-Proxy.Example.Test.", "git-proxy.example.test"];

    let from_array = known_egress_domains_from_assignment(serde_json::json!(names)).unwrap();
    let from_object =
        known_egress_domains_from_assignment(serde_json::json!({"domains": names})).unwrap();
    let from_stringified = known_egress_domains_from_assignment(serde_json::json!({
        "domains": serde_json::to_string(&names).unwrap(),
    }))
    .unwrap();

    assert_eq!(from_array, from_stringified);
    assert_eq!(from_object, from_stringified);
    assert_eq!(
        flag_values(
            &netgtw_launch_args(
                "sbx-1",
                false,
                None,
                &NetGatewayLaunchConfig {
                    known_egress_domains: from_stringified,
                },
            ),
            "--known-egress-domain"
        ),
        ["egress-proxy.example.test", "git-proxy.example.test"]
    );
}

/// A string under `domains` that is not an encoded array is still rejected.
///
/// Decoding is confined to "this member must be a list", so a string that does
/// not decode to one has to fail rather than silently disable diagnostics —
/// otherwise the workaround for flagd would become a hole that swallows every
/// malformed assignment.
#[test]
fn a_stringified_known_egress_value_that_is_not_an_array_is_rejected() {
    for encoded in [
        "egress-proxy.example.test",
        "[\"not a domain\"]",
        "{\"domains\": []}",
        "[",
    ] {
        assert!(
            matches!(
                known_egress_domains_from_assignment(serde_json::json!({ "domains": encoded })),
                Err(KnownEgressAssignmentError::InvalidDomains(_))
            ),
            "{encoded:?} should not parse as a domain list"
        );
    }
}

/// An object carrying the wrong keys is rejected. Unwrapping the envelope must
/// not turn into "accept any object": a misspelled key, or the quota flag's
/// JSON pointed at this flag by mistake, has to fail rather than read as "no
/// domains" — because a rejected assignment falls back to the local list, and
/// a typo silently adopting whatever that holds is worse than a loud failure.
#[test]
fn a_known_egress_object_with_the_wrong_keys_is_rejected() {
    for assignment in [
        serde_json::json!({"domain": ["example.com"]}),
        serde_json::json!({"domains": "example.com"}),
        serde_json::json!({"unspecified": 8, "overrides": {}}),
    ] {
        assert!(
            matches!(
                known_egress_domains_from_assignment(assignment.clone()),
                Err(KnownEgressAssignmentError::InvalidDomains(_))
            ),
            "{assignment} should not parse as known-egress domains"
        );
    }
}

// A gateway's interception readiness must not gate sandbox startup.
#[tokio::test(start_paused = true)]
async fn unavailable_interception_dependencies_still_converge_the_start_gate() {
    use super::super::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::SandboxId;
    use pplx_space_net_gateway_client::proto::{
        InterceptionReadiness, NetworkState, RefreshNetworkPolicyResponse,
    };

    use std::time::Duration;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
    let db = crate::db::SharedConnection::new(conn);
    let expected_ref = format!(
        "cluster:sandbox:{sandbox_id}:generation:1:sha256:{}",
        "a".repeat(64)
    )
    .parse::<pplx_space_net_common::EffectiveNetworkPolicyRef>()
    .unwrap();

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let started = tokio::time::Instant::now();
    let result = super::refresh_network_policy_with_request(
        &db,
        &sandbox_id,
        Some(&expected_ref),
        "start",
        |request| async move {
            Ok(RefreshNetworkPolicyResponse {
                state: NetworkState::Ready as i32,
                installed_policy_ref: request.expected_policy_ref,
                interception_readiness: InterceptionReadiness::DependenciesNotReady as i32,
                ..Default::default()
            })
        },
    )
    .await;

    result.expect("unavailable interception dependencies must not fail the start gate");
    // Not merely "eventually Ok": a gate that retried the refresh would still
    // converge, and would still have held the sandbox past its deadline.
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "converging must not spend the readiness retry schedule"
    );
    assert!(
        counters
            .finish()
            .contains(&pplx_space_metrics::test_capture::CapturedCounter {
                name: "reconcile.network_policy.interception_not_ready".to_string(),
                value: 1,
                tags: vec!["reason:start".to_string()],
            }),
        "denied egress is invisible unless this fires"
    );
}

/// An empty envelope is an answer — "no domains" — not a malformed document.
///
/// An OpenFeature backend resolves a JSON flag as a *structure*, so `{}` is how
/// an operator clearing the list in flagd spells it; there is no absent value
/// available. Reading it as an error would send the resolver to its local-list
/// fallback and reinstate exactly what the operator just cleared.
///
/// Asserted alongside the shapes that must still fail, because the distinction
/// is the whole point: no keys is empty, wrong keys is an error.
#[test]
fn an_empty_known_egress_envelope_reads_as_no_domains() {
    for assignment in [
        serde_json::json!({}),
        serde_json::json!({"domains": null}),
        serde_json::json!({"domains": []}),
        serde_json::json!([]),
        // Eppo's `get_json_assignment` can return a top-level null. Without its
        // own arm it fails array parsing and incorrectly reinstates the local list.
        serde_json::json!(null),
        // And an empty string under the key, which is an operator clearing the
        // value rather than a stringified array that failed to decode.
        serde_json::json!({"domains": ""}),
        serde_json::json!({"domains": "  "}),
    ] {
        assert_eq!(
            known_egress_domains_from_assignment(assignment.clone()).unwrap(),
            Vec::<pplx_space_net_common::DomainName>::new(),
            "{assignment} should read as no domains"
        );
    }

    // And it reaches the gateway as "no domains" rather than the local list,
    // which is the behaviour an operator clearing the flag is asking for.
    for cleared in [
        serde_json::json!({}),
        serde_json::json!(null),
        serde_json::json!({"domains": ""}),
    ] {
        let serving_empty = FeatureFlagClient::fixed_json([(
            SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG.key(),
            cleared.clone(),
        )]);
        assert_eq!(
            launched_domains(&serving_empty, &deploy_config("local.example.test")),
            Vec::<String>::new(),
            "{cleared} should clear the list, not reinstate the local one"
        );
    }
}

// ---------------------------------------------------------------------------
// Known-egress configuration
// ---------------------------------------------------------------------------

/// A client that serves `names` for the known-egress flag, as Eppo does.
fn client_serving(names: &[&str]) -> FeatureFlagClient {
    FeatureFlagClient::fixed_json([(
        SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG.key(),
        serde_json::json!(names),
    )])
}

fn deploy_config(domains: &str) -> KnownEgressConfig {
    KnownEgressConfig::from_values(Some(domains))
}

/// The domains a launch actually carries, which is the only thing the gateway
/// sees. Asserted through the launch arguments rather than off the resolver,
/// because a resolver that is right and a launch path that drops the value is
/// the failure this plumbing exists to prevent.
fn launched_domains(
    feature_flags: &FeatureFlagClient,
    known_egress: &KnownEgressConfig,
) -> Vec<String> {
    let launch_config = net_gateway_launch_config_with(feature_flags, "sbx-1", known_egress);
    let args = netgtw_launch_args("sbx-1", false, None, &launch_config);
    flag_values(&args, "--known-egress-domain")
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// A malformed assignment leaves the local list standing.
///
/// `JsonFlag` declares that a read the caller *rejects* lands on local
/// configuration, exactly like a read that resolved nothing — otherwise a
/// garbage document in a console this deployment may not own silently kills
/// diagnostics the deployment configured for itself.
///
/// Asserted with a non-empty local list and a provider that serves something
/// unparseable, so a resolver that returned an empty vector here fails rather
/// than looking like the master was off.
#[test]
fn a_rejected_assignment_falls_back_to_the_local_list() {
    let serving_garbage = FeatureFlagClient::fixed_json([(
        SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG.key(),
        serde_json::json!({"domain": ["typo.example.test"]}),
    )]);

    assert_eq!(
        launched_domains(&serving_garbage, &deploy_config("local.example.test")),
        ["local.example.test"]
    );
    // With no local list there is nothing to fall back to, so the answer is
    // still empty — the fallback is to local configuration, not to a default.
    assert_eq!(
        launched_domains(&serving_garbage, &deploy_config("")),
        Vec::<String>::new()
    );
}

/// Nothing resolved: the local list is the answer.
///
/// This is the whole point of the change — before it, a deployment that
/// configures no flag provider could not populate the list by any means.
#[test]
fn no_provider_launches_the_local_list() {
    assert_eq!(
        launched_domains(
            &FeatureFlagClient::disabled(),
            &deploy_config("Local.Example.Test., other.example.test")
        ),
        ["local.example.test", "other.example.test"]
    );
    assert_eq!(
        launched_domains(&FeatureFlagClient::disabled(), &deploy_config("")),
        Vec::<String>::new()
    );
}

/// An assignment replaces the local list rather than adding to it.
///
/// Asserted with both sides non-empty and disjoint, so a resolver that
/// concatenated them would fail here rather than read as a pass.
#[test]
fn an_assignment_replaces_the_local_list_rather_than_extending_it() {
    assert_eq!(
        launched_domains(
            &client_serving(&["from-provider.example.test"]),
            &deploy_config("local.example.test")
        ),
        ["from-provider.example.test"]
    );
}

/// An assignment that resolves to an empty list still wins over the local one.
///
/// "The provider said no domains" and "the provider said nothing" are
/// different answers, and only the second falls through to local configuration.
#[test]
fn an_empty_assignment_is_an_answer_and_not_a_fallthrough() {
    assert_eq!(
        launched_domains(&client_serving(&[]), &deploy_config("local.example.test")),
        Vec::<String>::new()
    );
}

/// The local list goes through the same `DomainName` validation an assignment
/// does, so a name Eppo would reject is rejected here too.
#[test]
fn the_local_list_is_normalized_and_validated_like_an_assignment() {
    assert_eq!(
        local_known_egress_domains(Some(
            " Egress-Proxy.Example.Test. , git-proxy.example.test "
        ))
        .unwrap(),
        known_egress_domains_from_assignment(serde_json::json!([
            "Egress-Proxy.Example.Test.",
            "git-proxy.example.test"
        ]))
        .unwrap()
    );
    // Empty members are separators, not domains, so a trailing comma is not an
    // error and does not become an empty argument.
    assert_eq!(
        local_known_egress_domains(Some("example.test,,")).unwrap(),
        local_known_egress_domains(Some("example.test")).unwrap()
    );
    assert!(local_known_egress_domains(None).unwrap().is_empty());
    assert!(local_known_egress_domains(Some("   ")).unwrap().is_empty());
}

/// One bad entry rejects the whole local list, matching the assignment path.
///
/// Dropping just the bad entry would launch a list nobody configured, and the
/// diagnostics would look healthy while quietly missing a domain.
#[test]
fn one_invalid_local_entry_rejects_the_whole_list() {
    assert!(matches!(
        local_known_egress_domains(Some("example.test,not a domain")),
        Err(KnownEgressLocalError::InvalidDomain { .. })
    ));
    assert!(matches!(
        local_known_egress_domains(Some(
            &["example.test"; MAX_KNOWN_EGRESS_DOMAINS + 1].join(",")
        )),
        Err(KnownEgressLocalError::TooManyDomains(_))
    ));
    // Exactly at the cap is accepted, on both paths. Without this, tightening
    // `>` to `>=` would reject a legal list and no test would notice.
    assert_eq!(
        local_known_egress_domains(Some(&["example.test"; MAX_KNOWN_EGRESS_DOMAINS].join(",")))
            .unwrap()
            .len(),
        MAX_KNOWN_EGRESS_DOMAINS
    );
    assert_eq!(
        known_egress_domains_from_assignment(serde_json::json!(vec![
            "example.test";
            MAX_KNOWN_EGRESS_DOMAINS
        ]))
        .unwrap()
        .len(),
        MAX_KNOWN_EGRESS_DOMAINS
    );
    // A rejected list leaves no local domains.
    let parsed = KnownEgressConfig::from_values(Some("not a domain"));
    assert!(parsed.domains.is_empty());
}

/// A rejected local list is distinguishable from one that was never set.
///
/// Both leave `domains` empty, so without a separate flag the two look
/// identical from outside the process — and "no domains" is also the default,
/// which makes a typo read as a deliberate choice. Asserted against an unset
/// list *and* an empty-but-present one, so the flag tracks "was rejected"
/// rather than merely "is empty".
#[test]
fn a_rejected_local_list_is_not_mistaken_for_an_unset_one() {
    let rejected = KnownEgressConfig::from_values(Some("not a domain"));
    assert!(rejected.local_list_rejected);
    assert!(rejected.domains.is_empty());

    for absent in [None, Some(""), Some("  "), Some(",")] {
        let parsed = KnownEgressConfig::from_values(absent);
        assert!(
            !parsed.local_list_rejected,
            "{absent:?} is an unset list, not a rejected one"
        );
        assert!(parsed.domains.is_empty());
    }

    // A list that parses leaves the flag clear even though it is non-empty,
    // so the flag means "rejected" and not "we looked at it".
    let accepted = KnownEgressConfig::from_values(Some("example.test"));
    assert!(!accepted.local_list_rejected);
    assert_eq!(accepted.domains.len(), 1);
}
