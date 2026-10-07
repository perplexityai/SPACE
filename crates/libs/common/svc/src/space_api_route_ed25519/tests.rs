use aws_lc_rs::rand::SystemRandom;
use pplx_space_common_core::types::cluster::CLUSTER_ID_ENV;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, NodeManagedBy, PodUid, RouteGeneration,
    SandboxActivationAttemptId, SandboxId,
};
use pplx_space_test_env::TestEnv;

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

/// The signing-configuration arms of `load_from_env`, which is the contract
/// every producer-side chart is written against. Who refuses what differs by
/// caller and the difference matters: `space-internal-api` loads the material
/// unconditionally at startup, the api server loads it for every surface but
/// `Admin`, and the cluster-controller loads it only when the public-operation
/// dispatcher is enabled — which is off by default, so an empty set is fatal
/// there only for a chart that turns the dispatcher on.
///
/// A *partial* set is fatal for all of them, and that is what these arms pin.
/// All-or-nothing is the whole point: a half-wired chart must not yield an
/// authority that mints tokens under a default issuer or key id.
#[test]
fn signing_configuration_is_all_or_nothing() {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let private_pem = pem("PRIVATE KEY", pkcs8.as_ref());

    {
        let env = TestEnv::default();
        // Nothing configured is the distinguishable case: a consumer that can
        // run without minting reads `Ok(None)`, and only the producer's
        // `load_required_from_env` turns it into a startup failure.
        assert!(
            SpaceApiRouteEd25519Authority::load_from_environment(&env)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            SpaceApiRouteEd25519Authority::load_required_from_environment(&env),
            Err(SpaceApiRouteEd25519Error::MissingSigningConfiguration)
        ));
    }

    // Each partial set below is `Incomplete`, never `Ok(None)` and never a
    // usable authority. Reaching `Ok` from any of these would mean a chart that
    // rendered a subset of the trio still started.
    let partials: [&[(&str, &str)]; 6] = [
        &[(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1")],
        &[(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api")],
        &[
            (SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1"),
            (SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api"),
        ],
        &[
            (SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, "irrelevant"),
            (SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api"),
        ],
        // The key alone, and the key with an id but no issuer. Both omitted at
        // first, and they are the subsets a chart reaches by deleting one line
        // from the trio rather than two — the likelier edit.
        &[(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, "irrelevant")],
        &[
            (SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, "irrelevant"),
            (SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1"),
        ],
    ];
    for partial in partials {
        let mut env = TestEnv::default();
        for (var, value) in partial {
            env.set(var, value);
        }
        assert!(
            matches!(
                SpaceApiRouteEd25519Authority::load_from_environment(&env),
                Err(SpaceApiRouteEd25519Error::IncompleteSigningConfiguration)
            ),
            "partial signing configuration {partial:?} was accepted"
        );
    }

    // The key has two spellings and they are mutually exclusive: a chart that
    // renders both leaves the loader unable to say which one is authoritative,
    // so it refuses rather than picking.
    {
        let mut env = TestEnv::default();
        env.set(CLUSTER_ID_ENV, "testing");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1");
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, &private_pem);
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV, "/dev/null");
        assert!(matches!(
            SpaceApiRouteEd25519Authority::load_from_environment(&env),
            Err(SpaceApiRouteEd25519Error::IncompleteSigningConfiguration)
        ));
    }

    // And the complete set loads, so the assertions above are rejecting the
    // partial sets rather than failing for an unrelated reason.
    {
        let mut env = TestEnv::default();
        env.set(CLUSTER_ID_ENV, "testing");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1");
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, &private_pem);
        SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap();
    }

    // The cluster id is part of the authority, and its absence is its own error
    // rather than "incomplete": every signing input has been accepted by this
    // point, so an operator reading the message is pointed at the right knob.
    {
        let mut env = TestEnv::default();
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1");
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, &private_pem);
        assert!(matches!(
            SpaceApiRouteEd25519Authority::load_from_environment(&env),
            Err(SpaceApiRouteEd25519Error::MissingClusterId)
        ));
    }

    // The by-path spelling loads too. Charts are written against both — one
    // projects the key as a file and sets only the path — so an authority that
    // could only be built from the inline spelling would reject a correctly
    // configured workload, and nothing else here reads a key off disk.
    let key_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key_file.path(), private_pem.as_bytes()).unwrap();
    {
        let mut env = TestEnv::default();
        env.set(CLUSTER_ID_ENV, "testing");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1");
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        env.set(
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
            key_file.path(),
        );
        SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap();
    }

    // A path that does not exist is its own error rather than "incomplete", so
    // an operator sees the mount problem instead of a configuration one.
    {
        let mut env = TestEnv::default();
        env.set(CLUSTER_ID_ENV, "testing");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1");
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        env.set(
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
            "/nonexistent/route-signing-key.pem",
        );
        assert!(matches!(
            SpaceApiRouteEd25519Authority::load_from_environment(&env),
            Err(SpaceApiRouteEd25519Error::ReadSigningKey { .. })
        ));
    }

    // A *present but malformed* value is its own error too, and each is
    // distinct from `Incomplete`. Pinned so a wrong value fails at startup
    // instead of being coerced into an identity that looks valid: without the
    // guards these arms exercise, a mistyped key id or issuer would be carried
    // as-is into signed tokens and only surface at the verifier, one hop away
    // from the chart that set it.
    //
    // The env var, the malformed value, and the error it must produce.
    type MalformedCase = (
        &'static str,
        &'static str,
        fn(&SpaceApiRouteEd25519Error) -> bool,
    );
    let malformed: [MalformedCase; 3] = [
        (
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
            "kind.local",
            |error| matches!(error, SpaceApiRouteEd25519Error::InvalidKeyId),
        ),
        (SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster api", |error| {
            matches!(error, SpaceApiRouteEd25519Error::InvalidIssuer)
        }),
        (
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV,
            "not-a-key",
            |error| matches!(error, SpaceApiRouteEd25519Error::InvalidPrivateKey),
        ),
    ];
    for (var, value, expected) in malformed {
        let mut env = TestEnv::default();
        env.set(CLUSTER_ID_ENV, "testing");
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ENV, &private_pem);
        env.set(SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, "key-1");
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        env.set(var, value);
        // `match`, not `expect_err`: the authority is deliberately not
        // `Debug` — it holds private key material and must not be
        // formattable — so the `T: Debug` bound cannot be satisfied here.
        match SpaceApiRouteEd25519Authority::load_from_environment(&env) {
            Ok(_) => panic!("{var} = {value:?} still built an authority"),
            Err(error) => assert!(
                expected(&error),
                "{var} = {value:?} produced {error:?}, not the variant \
                 that names the malformed value"
            ),
        }
    }
}
