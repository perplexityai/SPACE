use super::*;
use aws_lc_rs::hmac;
use x509_parser::prelude::{FromDer, X509Certificate};

fn catalog() -> EpochCatalog {
    EpochCatalog::new(
        "fixture".into(),
        vec![KeyEpoch {
            version: 1,
            since_ms: 0,
            key_id: "fixture-key-a".into(),
        }],
    )
    .unwrap()
}

// Public fixture keys only. Production MACs come from the cloud HSM.
fn material(catalog: &EpochCatalog, id: Uuid) -> Zeroizing<[u8; 48]> {
    let key_byte = match catalog.epoch(id).unwrap().key_id.as_str() {
        "fixture-key-a" => 42,
        "fixture-key-b" => 43,
        _ => panic!("unknown fixture key"),
    };
    let key = hmac::Key::new(hmac::HMAC_SHA384, &[key_byte; 48]);
    Zeroizing::new(
        hmac::sign(&key, &catalog.derivation_input(id).unwrap())
            .as_ref()
            .try_into()
            .unwrap(),
    )
}

fn derive(catalog: &EpochCatalog, id: Uuid) -> Anchor {
    derive_anchor(catalog, id, &material(catalog, id)).unwrap()
}

#[test]
fn stable_isolated_anchors_survive_rotation_and_recovery() {
    let id = Uuid::parse_str("019a1234-5678-7000-8000-000000000001").unwrap();
    let first = derive(&catalog(), id);
    assert_eq!(
        hex::encode(Sha256::digest(&first.certificate_der)),
        "62ba1631401a1d5ea7ac27de003e57b7a7e30c83c6c94e29ebaf28a762011e40"
    );
    // Independently calculated with Python hmac and cryptography/OpenSSL,
    // public fixture key [42;48], scheme-2 framing and P-256 reduction.
    assert_eq!(
        hex::encode(*material(&catalog(), id)),
        "ff9416745db28b5a55ea2b9cda959705abf63874d15ba2c6a8fe44e0a58d90bde8b74f45336dc3b4bbc6e79ffa41635d"
    );
    let (_, certificate) = X509Certificate::from_der(&first.certificate_der).unwrap();
    assert_eq!(
        hex::encode(certificate.public_key().raw),
        "3059301306072a8648ce3d020106082a8648ce3d0301070342000426d3ad3eac3ed2ee0797051addf805b9026be53ac4253748282e65e9c657898344e3879d1f3fa8f10e5b26a356c3a00fe22a206925bd6128d105b557ca59a91f"
    );
    assert_eq!(
        hex::encode(certificate.raw_serial()),
        "06fecc4271aea17e4acf092f0605dfe1c4bdd711"
    );
    certificate.verify_signature(None).unwrap();
    let constraints = certificate.basic_constraints().unwrap().unwrap();
    assert!(constraints.critical);
    assert_eq!(constraints.value.path_len_constraint, Some(1));
    assert!(
        certificate
            .key_usage()
            .unwrap()
            .unwrap()
            .value
            .key_cert_sign()
    );

    let recovered = EpochCatalog::decode(&catalog().encode().unwrap()).unwrap();
    let second = derive(&recovered, id);
    assert_eq!(first.certificate_der, second.certificate_der);
    assert_eq!(first.private_key_der, second.private_key_der);
    let child = derive(
        &catalog(),
        Uuid::parse_str("019a1234-5678-7000-8000-000000000002").unwrap(),
    );
    assert_ne!(first.certificate_der, child.certificate_der);
    assert_ne!(first.private_key_der, child.private_key_der);

    let activation = sandbox_timestamp_ms(id).unwrap() + 3_600_000;
    let mut epochs = catalog().epochs().to_vec();
    epochs.push(KeyEpoch {
        version: 2,
        since_ms: activation,
        key_id: "fixture-key-b".into(),
    });
    let rotated = EpochCatalog::new("fixture".into(), epochs).unwrap();
    catalog().validate_successor(&rotated).unwrap();
    let old_after_rotation = derive(&rotated, id);
    assert_eq!(first.private_key_der, old_after_rotation.private_key_der);
    assert_eq!(first.certificate_der, old_after_rotation.certificate_der);
    let mut bytes = *id.as_bytes();
    bytes[..6].copy_from_slice(&activation.to_be_bytes()[2..]);
    let future = Uuid::from_bytes(bytes);
    assert_eq!(rotated.epoch(future).unwrap().version, 2);
    assert_ne!(
        derive(&rotated, future).private_key_der,
        derive(&catalog(), future).private_key_der,
    );
    assert!(rotated.validate_successor(&catalog()).is_err());
}

#[test]
fn malformed_or_mutated_catalogs_fail_closed() {
    EpochCatalog::new("fixture".into(), vec![]).unwrap_err();
    EpochCatalog::new("".into(), catalog().epochs().to_vec()).unwrap_err();
    let mut changed = catalog().epochs().to_vec();
    changed[0].key_id = "fixture-key-b".into();
    let changed = EpochCatalog::new("fixture".into(), changed).unwrap();
    assert!(matches!(
        catalog().validate_successor(&changed),
        Err(AnchorError::ChangedEpoch)
    ));
    let other_environment = EpochCatalog::new("other".into(), catalog().epochs().to_vec()).unwrap();
    assert!(catalog().validate_successor(&other_environment).is_err());
    assert!(matches!(
        catalog().epoch(Uuid::nil()),
        Err(AnchorError::InvalidId)
    ));
    let mut duplicate = catalog().epochs().to_vec();
    duplicate.push(KeyEpoch {
        version: 2,
        since_ms: 1,
        key_id: "fixture-key-a".into(),
    });
    EpochCatalog::new("fixture".into(), duplicate).unwrap_err();
}

#[test]
fn gateway_certificate_expiry_is_bounded_and_chain_verifies() {
    let anchor = derive(
        &catalog(),
        Uuid::parse_str("019a1234-5678-7000-8000-000000000001").unwrap(),
    );
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let public = rcgen::SubjectPublicKeyInfo::from_der(&key.subject_public_key_info()).unwrap();
    for now in [
        rcgen::date_time_ymd(2026, 10, 23),
        rcgen::date_time_ymd(2125, 12, 1),
    ] {
        let der = anchor.sign_gateway(&public, now).unwrap();
        let (_, cert) = X509Certificate::from_der(&der).unwrap();
        let (_, root) = X509Certificate::from_der(&anchor.certificate_der).unwrap();
        cert.verify_signature(Some(root.public_key())).unwrap();
        assert_eq!(
            cert.basic_constraints()
                .unwrap()
                .unwrap()
                .value
                .path_len_constraint,
            Some(0)
        );
        assert_eq!(
            cert.validity().not_after.timestamp(),
            (now.unix_timestamp() + GATEWAY_LIFETIME_SECONDS)
                .min(root.validity().not_after.timestamp())
        );
    }
}
