use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId, SandboxRouteIdentity, SpaceApiRouteIdentity,
};
use tonic::metadata::{MetadataMap, MetadataValue};

use super::*;

fn complete_identity() -> SandboxRouteIdentity {
    SandboxRouteIdentity::try_new(
        SandboxId::new(),
        RouteGeneration::new(7).unwrap(),
        NodeId::new(),
        NodeManagedBy::Kubernetes,
        ExecutionGeneration::new(11).unwrap(),
        SandboxActivationAttemptId::new(),
        Some(PodUid::parse("pod-uid").unwrap()),
    )
    .unwrap()
}

#[test]
fn route_capability_presence_recognizes_only_the_ed25519_header() {
    let mut metadata = MetadataMap::new();
    assert_eq!(
        space_api_route_capability_presence(&metadata),
        SpaceApiRouteCapabilityPresence::Absent
    );

    metadata.insert(
        SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY,
        MetadataValue::from_static("ed25519.test.payload.signature"),
    );
    assert_eq!(
        space_api_route_capability_presence(&metadata),
        SpaceApiRouteCapabilityPresence::Present
    );

    metadata.clear();
    metadata.insert(
        "x-space-api-route-token",
        MetadataValue::from_static("obsolete"),
    );
    assert_eq!(
        space_api_route_capability_presence(&metadata),
        SpaceApiRouteCapabilityPresence::Absent
    );
}

#[test]
fn route_token_metadata_forwards_only_the_ed25519_capability() {
    let mut metadata = MetadataMap::new();
    metadata.insert(
        SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY,
        MetadataValue::from_static("ed25519.test.payload.signature"),
    );
    metadata.insert("authorization", MetadataValue::from_static("secret"));

    let filtered = space_api_route_token_metadata(&metadata);

    assert_eq!(
        filtered
            .get(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY)
            .unwrap(),
        "ed25519.test.payload.signature"
    );
    assert!(!filtered.contains_key("authorization"));
}

#[test]
fn route_identity_remediation_refreshes_only_rotating_dimensions() {
    for dimension in [
        SpaceApiRouteIdentityDimension::RouteGeneration,
        SpaceApiRouteIdentityDimension::CurrentNodeId,
        SpaceApiRouteIdentityDimension::SubstrateKind,
        SpaceApiRouteIdentityDimension::ExecutionGeneration,
        SpaceApiRouteIdentityDimension::ActivationAttemptId,
        SpaceApiRouteIdentityDimension::PodUid,
    ] {
        assert_eq!(
            SpaceApiRouteIdentityMatchError::Mismatch(dimension).remediation(),
            RouteIdentityRemediation::RefreshRoute
        );
    }
    assert_eq!(
        SpaceApiRouteIdentityMatchError::Mismatch(SpaceApiRouteIdentityDimension::SandboxId)
            .remediation(),
        RouteIdentityRemediation::Deny
    );
}

#[test]
fn rejection_metadata_round_trips_as_typed_values() {
    let mut status = Status::permission_denied("stale route");
    set_route_identity_rejection_metadata(
        &mut status,
        RouteIdentityRemediation::RefreshRoute,
        Some(SpaceApiRouteIdentityDimension::ActivationAttemptId),
    );

    assert_eq!(
        route_identity_remediation_from_status(&status),
        Some(RouteIdentityRemediation::RefreshRoute)
    );
    assert_eq!(
        route_identity_dimension_from_status(&status),
        Some(SpaceApiRouteIdentityDimension::ActivationAttemptId)
    );
    assert_eq!(
        route_identity_remediation_from_status(&Status::permission_denied("missing")),
        None
    );
}

#[test]
fn complete_route_identity_match_names_every_dimension() {
    let expected = complete_identity();
    let cases = [
        (
            SpaceApiRouteIdentityDimension::SandboxId,
            SandboxRouteIdentity::try_new(
                SandboxId::new(),
                expected.route_generation(),
                expected.current_node_id(),
                expected.substrate_kind(),
                expected.execution_generation(),
                expected.activation_attempt_id(),
                expected.pod_uid().cloned(),
            )
            .unwrap(),
        ),
        (
            SpaceApiRouteIdentityDimension::RouteGeneration,
            SandboxRouteIdentity::try_new(
                expected.sandbox_id(),
                RouteGeneration::new(expected.route_generation().get() + 1).unwrap(),
                expected.current_node_id(),
                expected.substrate_kind(),
                expected.execution_generation(),
                expected.activation_attempt_id(),
                expected.pod_uid().cloned(),
            )
            .unwrap(),
        ),
        (
            SpaceApiRouteIdentityDimension::CurrentNodeId,
            SandboxRouteIdentity::try_new(
                expected.sandbox_id(),
                expected.route_generation(),
                NodeId::new(),
                expected.substrate_kind(),
                expected.execution_generation(),
                expected.activation_attempt_id(),
                expected.pod_uid().cloned(),
            )
            .unwrap(),
        ),
        (
            SpaceApiRouteIdentityDimension::SubstrateKind,
            SandboxRouteIdentity::try_new(
                expected.sandbox_id(),
                expected.route_generation(),
                expected.current_node_id(),
                NodeManagedBy::Space,
                expected.execution_generation(),
                expected.activation_attempt_id(),
                None,
            )
            .unwrap(),
        ),
        (
            SpaceApiRouteIdentityDimension::ExecutionGeneration,
            SandboxRouteIdentity::try_new(
                expected.sandbox_id(),
                expected.route_generation(),
                expected.current_node_id(),
                expected.substrate_kind(),
                ExecutionGeneration::new(expected.execution_generation().get() + 1).unwrap(),
                expected.activation_attempt_id(),
                expected.pod_uid().cloned(),
            )
            .unwrap(),
        ),
        (
            SpaceApiRouteIdentityDimension::ActivationAttemptId,
            SandboxRouteIdentity::try_new(
                expected.sandbox_id(),
                expected.route_generation(),
                expected.current_node_id(),
                expected.substrate_kind(),
                expected.execution_generation(),
                SandboxActivationAttemptId::new(),
                expected.pod_uid().cloned(),
            )
            .unwrap(),
        ),
        (
            SpaceApiRouteIdentityDimension::PodUid,
            SandboxRouteIdentity::try_new(
                expected.sandbox_id(),
                expected.route_generation(),
                expected.current_node_id(),
                expected.substrate_kind(),
                expected.execution_generation(),
                expected.activation_attempt_id(),
                Some(PodUid::parse("different-pod").unwrap()),
            )
            .unwrap(),
        ),
    ];

    assert_eq!(match_complete_route_identity(&expected, &expected), Ok(()));
    for (dimension, actual) in cases {
        assert_eq!(
            match_complete_route_identity(&actual, &expected),
            Err(SpaceApiRouteIdentityMatchError::Mismatch(dimension))
        );
    }
}

#[test]
fn present_route_identity_match_ignores_absent_dimensions_but_rejects_conflicts() {
    let expected = complete_identity();
    assert_eq!(
        match_present_route_identity_dimensions(&SpaceApiRouteIdentity::default(), &expected),
        Ok(())
    );

    let local = SpaceApiRouteIdentity {
        route_generation: Some(
            RouteGeneration::new(expected.route_generation().get() + 1).unwrap(),
        ),
        ..Default::default()
    };
    assert_eq!(
        match_present_route_identity_dimensions(&local, &expected),
        Err(SpaceApiRouteIdentityMatchError::Mismatch(
            SpaceApiRouteIdentityDimension::RouteGeneration
        ))
    );
}

#[test]
fn bridge_caller_policy_is_typed_and_rejects_unknown_values() {
    let mut metadata = MetadataMap::new();
    set_space_api_bridge_caller_policy(&mut metadata, SpaceApiBridgeCallerPolicy::TrustedLifecycle);
    assert_eq!(
        space_api_bridge_caller_policy(&metadata).unwrap(),
        Some(SpaceApiBridgeCallerPolicy::TrustedLifecycle)
    );

    metadata.insert(
        SPACE_API_BRIDGE_CALLER_POLICY_METADATA_KEY,
        MetadataValue::from_static("unknown"),
    );
    assert_eq!(
        space_api_bridge_caller_policy(&metadata)
            .expect_err("unknown policy rejects")
            .code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn wait_ready_lifecycle_marker_is_consumed() {
    let mut request = wait_ready_lifecycle_request(());
    assert_eq!(
        take_wait_ready_call_purpose(request.metadata_mut()),
        WaitReadyCallPurpose::LifecycleProbe
    );
    assert_eq!(
        wait_ready_call_purpose(request.metadata()),
        WaitReadyCallPurpose::PublicBridge
    );
}
