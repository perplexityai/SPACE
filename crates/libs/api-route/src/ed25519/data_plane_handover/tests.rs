use aws_lc_rs::rand::SystemRandom;

use super::*;

fn authority(issuer: &str, cluster: &str) -> SpaceApiRouteEd25519Authority {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    SpaceApiRouteEd25519Authority::new(
        SpaceApiRouteEd25519Issuer::parse(issuer).unwrap(),
        ClusterId::parse(cluster).unwrap(),
        SpaceApiRouteEd25519Signer::from_pkcs8(
            SpaceApiRouteEd25519KeyId::parse("test-key").unwrap(),
            pkcs8.as_ref(),
        )
        .unwrap(),
    )
}

fn verifier_for(authority: &SpaceApiRouteEd25519Authority) -> SpaceApiRouteEd25519Verifier {
    SpaceApiRouteEd25519Verifier::new(
        authority.issuer().clone(),
        authority.cluster_id().clone(),
        [authority.verification_key()],
    )
    .unwrap()
}

fn route() -> DataPlaneRouteHandoverRoute {
    DataPlaneRouteHandoverRoute {
        session_id: "proc_1".to_string(),
        token_sha256: "a".repeat(64),
        org_id: OrgId::new(),
        sandbox_id: SandboxId::new(),
        kind: "process".to_string(),
        route_generation: 2,
        current_node_id: NodeId::new(),
        backend_endpoint: "https://10.0.0.1:9800".to_string(),
        execution_generation: ExecutionGeneration::new(3).unwrap(),
        revocation_epoch: 0,
        org_revocation_epoch: 4,
        adapter_state_json: r#"{"command":["true"]}"#.to_string(),
        space_api_route_token: "ed25519.test-key.payload.signature".to_string(),
    }
}

fn sign_claims(
    authority: &SpaceApiRouteEd25519Authority,
    edit: impl FnOnce(&mut DataPlaneRouteHandoverClaims),
) -> String {
    let now = unix_now_secs();
    let mut claims = DataPlaneRouteHandoverClaims {
        schema_version: DATA_PLANE_ROUTE_HANDOVER_SCHEMA_VERSION,
        issuer: authority.issuer().clone(),
        audience: DATA_PLANE_ROUTE_HANDOVER_AUDIENCE.to_string(),
        cluster_id: authority.cluster_id().clone(),
        route: route(),
        issued_at_unix_secs: now,
        expires_at_unix_secs: now + DATA_PLANE_ROUTE_HANDOVER_TTL_SECS,
    };
    edit(&mut claims);
    authority
        .signer
        .mint_data_plane_route_handover(&claims)
        .unwrap()
}

#[test]
fn handover_round_trips_through_its_verifier() {
    let authority = authority("space-api", "testing");
    let route = route();
    let handover = authority
        .mint_data_plane_route_handover(route.clone())
        .unwrap();
    assert_eq!(
        verifier_for(&authority)
            .verify_data_plane_route_handover(&handover)
            .unwrap(),
        route
    );
}

#[test]
fn handover_and_route_tokens_do_not_verify_as_each_other() {
    let authority = authority("space-api", "testing");
    let verifier = verifier_for(&authority);
    let handover = authority.mint_data_plane_route_handover(route()).unwrap();
    let as_route_token = SpaceApiRouteEd25519Token::parse(handover.clone());
    assert!(
        as_route_token.is_err() || verifier.verify(&as_route_token.unwrap()).is_err(),
        "a node must not accept a handover as a route token"
    );

    let route_token = authority
        .mint_space_api_bridge(
            pplx_space_common_core::types::SandboxRouteIdentity::try_new(
                SandboxId::new(),
                pplx_space_common_core::types::RouteGeneration::new(3).unwrap(),
                NodeId::new(),
                pplx_space_common_core::types::NodeManagedBy::Space,
                ExecutionGeneration::new(3).unwrap(),
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                None,
            )
            .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        verifier.verify_data_plane_route_handover(route_token.secret()),
        Err(DataPlaneRouteHandoverError::Malformed)
    ));
}

#[test]
fn handover_rejects_forgery_and_tampering() {
    let authority = authority("space-api", "testing");
    let verifier = verifier_for(&authority);

    let other = authority_with_same_identity(&authority);
    let forged = other.mint_data_plane_route_handover(route()).unwrap();
    assert!(matches!(
        verifier.verify_data_plane_route_handover(&forged),
        Err(DataPlaneRouteHandoverError::InvalidSignature)
    ));

    let handover = authority.mint_data_plane_route_handover(route()).unwrap();
    let mut parts = handover.split('.').map(str::to_string).collect::<Vec<_>>();
    let mut claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&parts[2]).unwrap()).unwrap();
    claims["backend_endpoint"] = "https://203.0.113.9:9800".into();
    parts[2] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    assert!(matches!(
        verifier.verify_data_plane_route_handover(&parts.join(".")),
        Err(DataPlaneRouteHandoverError::InvalidSignature)
    ));

    let mut unknown_key = handover.split('.').map(str::to_string).collect::<Vec<_>>();
    unknown_key[1] = "other-key".to_string();
    assert!(matches!(
        verifier.verify_data_plane_route_handover(&unknown_key.join(".")),
        Err(DataPlaneRouteHandoverError::UnknownKeyId)
    ));
}

fn authority_with_same_identity(
    authority: &SpaceApiRouteEd25519Authority,
) -> SpaceApiRouteEd25519Authority {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    SpaceApiRouteEd25519Authority::new(
        authority.issuer().clone(),
        authority.cluster_id().clone(),
        SpaceApiRouteEd25519Signer::from_pkcs8(
            SpaceApiRouteEd25519KeyId::parse("test-key").unwrap(),
            pkcs8.as_ref(),
        )
        .unwrap(),
    )
}

#[test]
fn handover_rejects_expiry_long_lifetimes_and_foreign_issuers() {
    let authority = authority("space-api", "testing");
    let verifier = verifier_for(&authority);
    let now = unix_now_secs();

    let expired = sign_claims(&authority, |claims| {
        claims.issued_at_unix_secs = now - 60;
        claims.expires_at_unix_secs = now - 50;
    });
    assert!(matches!(
        verifier.verify_data_plane_route_handover(&expired),
        Err(DataPlaneRouteHandoverError::Expired)
    ));

    let long_lived = sign_claims(&authority, |claims| {
        claims.expires_at_unix_secs = now + 3600;
    });
    assert!(matches!(
        verifier.verify_data_plane_route_handover(&long_lived),
        Err(DataPlaneRouteHandoverError::InvalidLifetime)
    ));

    let future = sign_claims(&authority, |claims| {
        claims.issued_at_unix_secs = now + 60;
        claims.expires_at_unix_secs = now + 65;
    });
    assert!(matches!(
        verifier.verify_data_plane_route_handover(&future),
        Err(DataPlaneRouteHandoverError::InvalidLifetime)
    ));

    for (edit, expected) in [
        (
            Box::new(|claims: &mut DataPlaneRouteHandoverClaims| {
                claims.issuer = SpaceApiRouteEd25519Issuer::parse("someone-else").unwrap();
            }) as Box<dyn FnOnce(&mut DataPlaneRouteHandoverClaims)>,
            "wrong_issuer",
        ),
        (
            Box::new(|claims: &mut DataPlaneRouteHandoverClaims| {
                claims.audience = SPACE_API_ROUTE_ED25519_AUDIENCE.to_string();
            }),
            "wrong_audience",
        ),
        (
            Box::new(|claims: &mut DataPlaneRouteHandoverClaims| {
                claims.cluster_id = ClusterId::parse("elsewhere").unwrap();
            }),
            "wrong_cluster",
        ),
        (
            Box::new(|claims: &mut DataPlaneRouteHandoverClaims| {
                claims.schema_version = 9;
            }),
            "unsupported_version",
        ),
    ] {
        let handover = sign_claims(&authority, edit);
        assert_eq!(
            verifier
                .verify_data_plane_route_handover(&handover)
                .unwrap_err()
                .label(),
            expected
        );
    }
}

#[test]
fn oversized_handovers_are_neither_minted_nor_parsed() {
    let authority = authority("space-api", "testing");
    let mut large = route();
    large.adapter_state_json = format!(
        r#"{{"command":["{}"]}}"#,
        "x".repeat(DATA_PLANE_ROUTE_HANDOVER_MAX_BYTES)
    );
    assert!(matches!(
        authority.mint_data_plane_route_handover(large),
        Err(DataPlaneRouteHandoverError::TooLarge)
    ));
    assert!(matches!(
        verifier_for(&authority)
            .verify_data_plane_route_handover(&"x".repeat(DATA_PLANE_ROUTE_HANDOVER_MAX_BYTES + 1)),
        Err(DataPlaneRouteHandoverError::TooLarge)
    ));
}
