use aws_lc_rs::rand::SystemRandom;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId,
};

use super::*;

fn signer() -> SpaceApiRouteEd25519Signer {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    SpaceApiRouteEd25519Signer::from_pkcs8(
        SpaceApiRouteEd25519KeyId::parse("test-key").unwrap(),
        pkcs8.as_ref(),
    )
    .unwrap()
}

fn pem(label: &str, der: &[u8]) -> String {
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        STANDARD.encode(der)
    )
}

fn route_identity() -> SandboxRouteIdentity {
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

fn issuer(value: &str) -> SpaceApiRouteEd25519Issuer {
    SpaceApiRouteEd25519Issuer::parse(value).unwrap()
}

fn signed_token_for_json(
    signer: &SpaceApiRouteEd25519Signer,
    claims: &serde_json::Value,
) -> SpaceApiRouteEd25519Token {
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
    let signing_input = format!("{TOKEN_PREFIX}.{}.{payload}", signer.key_id);
    let signature = signer.key_pair.sign(signing_input.as_bytes());
    SpaceApiRouteEd25519Token::parse(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    ))
    .unwrap()
}

#[test]
fn ed25519_token_round_trips_and_is_redacted() {
    let signer = signer();
    let claims = SpaceApiRouteEd25519Claims::service_port_ingress(
        issuer("cluster-api"),
        ClusterId::parse("testing").unwrap(),
        route_identity(),
        GuestPort::try_from_u16(8080).unwrap(),
        SpaceApiRouteProtocol::Tcp,
    )
    .unwrap();
    let token = signer.mint(&claims).unwrap();
    let verifier = SpaceApiRouteEd25519Verifier::new(
        issuer("cluster-api"),
        ClusterId::parse("testing").unwrap(),
        [signer.public_key()],
    )
    .unwrap();

    assert!(token.secret().starts_with("ed25519."));
    assert_eq!(verifier.verify(&token).unwrap(), claims);
    assert_eq!(
        format!("{token:?}"),
        "SpaceApiRouteEd25519Token(<redacted>)"
    );
    assert!(!format!("{token:?}").contains(token.secret()));
    assert!(matches!(
        token
            .secret()
            .replacen("ed25519.", "v2.", 1)
            .parse::<SpaceApiRouteEd25519Token>(),
        Err(SpaceApiRouteEd25519Error::Malformed)
    ));
}

#[test]
fn ed25519_verifier_rejects_signed_invalid_route_identity_profiles() {
    let signer = signer();
    let verifier = SpaceApiRouteEd25519Verifier::new(
        issuer("cluster-api"),
        ClusterId::parse("testing").unwrap(),
        [signer.public_key()],
    )
    .unwrap();

    let kubernetes_claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer("cluster-api"),
        ClusterId::parse("testing").unwrap(),
        route_identity(),
    )
    .unwrap();
    let mut kubernetes_without_pod = serde_json::to_value(kubernetes_claims).unwrap();
    kubernetes_without_pod["route_identity"]
        .as_object_mut()
        .unwrap()
        .remove("pod_uid");

    let direct_identity = SandboxRouteIdentity::try_new(
        SandboxId::new(),
        RouteGeneration::new(7).unwrap(),
        NodeId::new(),
        NodeManagedBy::Space,
        ExecutionGeneration::new(11).unwrap(),
        SandboxActivationAttemptId::new(),
        None,
    )
    .unwrap();
    let direct_claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer("cluster-api"),
        ClusterId::parse("testing").unwrap(),
        direct_identity,
    )
    .unwrap();
    let mut direct_with_pod = serde_json::to_value(direct_claims).unwrap();
    direct_with_pod["route_identity"]
        .as_object_mut()
        .unwrap()
        .insert("pod_uid".to_string(), serde_json::json!("pod-uid"));

    for invalid_claims in [kubernetes_without_pod, direct_with_pod] {
        let token = signed_token_for_json(&signer, &invalid_claims);
        assert!(matches!(
            verifier.verify(&token),
            Err(SpaceApiRouteEd25519Error::InvalidPayload(_))
        ));
    }
}

#[test]
fn ed25519_claims_schema_version_is_independent_of_token_scheme() {
    let signer = signer();
    let mut claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer("cluster-api"),
        ClusterId::parse("testing").unwrap(),
        route_identity(),
    )
    .unwrap();

    assert_eq!(
        claims.schema_version,
        SPACE_API_ROUTE_ED25519_CLAIMS_SCHEMA_VERSION
    );
    let serialized = serde_json::to_value(&claims).unwrap();
    assert_eq!(
        serialized.get("version"),
        Some(&serde_json::json!(
            SPACE_API_ROUTE_ED25519_CLAIMS_SCHEMA_VERSION
        ))
    );
    assert!(serialized.get("schema_version").is_none());
    assert!(serialized.get("scheme").is_none());

    claims.schema_version = SPACE_API_ROUTE_ED25519_CLAIMS_SCHEMA_VERSION + 1;
    assert!(matches!(
        signer.mint(&claims),
        Err(SpaceApiRouteEd25519Error::UnsupportedClaimsSchemaVersion)
    ));
}

#[test]
fn ed25519_token_rejects_unknown_key_and_cross_cluster_replay() {
    let route_signer = signer();
    let claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer("public-api"),
        ClusterId::parse("testing").unwrap(),
        route_identity(),
    )
    .unwrap();
    let token = route_signer.mint(&claims).unwrap();

    let other_signer = signer();
    let mut other_key = other_signer.public_key();
    other_key.key_id = SpaceApiRouteEd25519KeyId::parse("other-key").unwrap();
    let unknown_key_verifier = SpaceApiRouteEd25519Verifier::new(
        issuer("public-api"),
        ClusterId::parse("testing").unwrap(),
        [other_key],
    )
    .unwrap();
    assert!(matches!(
        unknown_key_verifier.verify(&token),
        Err(SpaceApiRouteEd25519Error::UnknownKeyId)
    ));

    let wrong_cluster_verifier = SpaceApiRouteEd25519Verifier::new(
        issuer("public-api"),
        ClusterId::parse("prod").unwrap(),
        [route_signer.public_key()],
    )
    .unwrap();
    assert!(matches!(
        wrong_cluster_verifier.verify(&token),
        Err(SpaceApiRouteEd25519Error::WrongCluster)
    ));
}

#[test]
fn verifier_rejects_duplicate_and_unbounded_key_sets() {
    let route_signer = signer();
    let key = route_signer.public_key();
    assert!(matches!(
        SpaceApiRouteEd25519Verifier::new(
            issuer("public-api"),
            ClusterId::parse("testing").unwrap(),
            [key.clone(), key],
        ),
        Err(SpaceApiRouteEd25519Error::DuplicateKeyId)
    ));

    let keys = (0..=MAX_VERIFICATION_KEYS)
        .map(|index| {
            let mut key = signer().public_key();
            key.key_id = SpaceApiRouteEd25519KeyId::parse(format!("key-{index}")).unwrap();
            key
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        SpaceApiRouteEd25519Verifier::new(
            issuer("public-api"),
            ClusterId::parse("testing").unwrap(),
            keys,
        ),
        Err(SpaceApiRouteEd25519Error::InvalidVerificationKeyCount)
    ));
}

#[test]
fn ed25519_token_binds_key_id_and_claim_profile() {
    let signer = signer();
    let mut claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer("public-api"),
        ClusterId::parse("testing").unwrap(),
        route_identity(),
    )
    .unwrap();
    claims.service_port = Some(GuestPort::try_from_u16(8080).unwrap());
    assert!(matches!(
        signer.mint(&claims),
        Err(SpaceApiRouteEd25519Error::InvalidClaimProfile)
    ));

    let valid_claims = SpaceApiRouteEd25519Claims::space_api_bridge(
        issuer("public-api"),
        ClusterId::parse("testing").unwrap(),
        route_identity(),
    )
    .unwrap();
    let token = signer.mint(&valid_claims).unwrap();
    let tampered =
        SpaceApiRouteEd25519Token::parse(token.secret().replacen(".test-key.", ".other-key.", 1))
            .unwrap();
    let verifier = SpaceApiRouteEd25519Verifier::new(
        issuer("public-api"),
        ClusterId::parse("testing").unwrap(),
        [SpaceApiRouteEd25519VerificationKey {
            key_id: SpaceApiRouteEd25519KeyId::parse("other-key").unwrap(),
            public_key: signer.public_key().public_key,
        }],
    )
    .unwrap();
    assert!(matches!(
        verifier.verify(&tampered),
        Err(SpaceApiRouteEd25519Error::InvalidSignature)
    ));
}

#[test]
fn terraform_style_pem_keys_decode_to_ed25519_key_material() {
    const ED25519_SPKI_PREFIX: &[u8] = &[
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let private_pem = pem("PRIVATE KEY", pkcs8.as_ref());
    let private_der = decode_signing_key_file(private_pem.into_bytes()).unwrap();
    let pair = Ed25519KeyPair::from_pkcs8(&private_der).unwrap();

    let mut public_der = ED25519_SPKI_PREFIX.to_vec();
    public_der.extend_from_slice(pair.public_key().as_ref());
    let public_key =
        decode_public_verification_key(None, Some(pem("PUBLIC KEY", &public_der))).unwrap();

    assert_eq!(public_key, pair.public_key().as_ref());
}
