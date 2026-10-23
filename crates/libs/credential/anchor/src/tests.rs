use super::*;
use x509_parser::prelude::{FromDer, X509Certificate};

fn ring() -> Keyring {
    Keyring::new(vec![SeedVersion::new(1, 0, [42; 32])]).unwrap()
}

#[test]
fn stable_isolated_anchors_survive_rotation_and_recovery() {
    let id = Uuid::parse_str("019a1234-5678-7000-8000-000000000001").unwrap();
    let first = ring().derive(id).unwrap();
    // Scheme 1 includes rcgen's exact DER encoding, not only the derived key.
    assert_eq!(
        hex::encode(Sha256::digest(&first.certificate_der)),
        "431a0d4afdfe341b1f12621e3fa67ac1dba5b12ff89fa729c160049fb8ac1c38"
    );
    let recovered = Keyring::decode(&ring().encode().unwrap()).unwrap();
    let second = recovered.derive(id).unwrap();
    assert_eq!(first.certificate_der, second.certificate_der);
    assert_eq!(first.private_key_der, second.private_key_der);
    let (_, certificate) = X509Certificate::from_der(&first.certificate_der).unwrap();
    // Independently derived with Python hmac HKDF and cryptography/OpenSSL
    // P-256 from the public fixture seed [42; 32], version u64be(1), UUID bytes.
    assert_eq!(
        hex::encode(certificate.public_key().raw),
        "3059301306072a8648ce3d020106082a8648ce3d03010703420004b795bbd86de9a32d66fba101a39da023c22dfec2e905b61816b96f8ef5793ed7e380294a42f67c74453f8a28079270c4051c6e891aa1312501278a00b5c623f2"
    );
    assert_eq!(
        hex::encode(certificate.raw_serial()),
        "091e8078db03e82e63d873b3739a2bbffccb49e2"
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
    let child = ring()
        .derive(Uuid::parse_str("019a1234-5678-7000-8000-000000000002").unwrap())
        .unwrap();
    assert_ne!(first.certificate_der, child.certificate_der);
    assert_ne!(first.private_key_der, child.private_key_der);
    let now = sandbox_timestamp_ms(id).unwrap();
    let rotated = ring().rotate(now, 3_600_000).unwrap();
    ring().validate_successor(&rotated).unwrap();
    assert_eq!(
        first.certificate_der,
        rotated.derive(id).unwrap().certificate_der
    );
    let mut bytes = *id.as_bytes();
    bytes[..6].copy_from_slice(&(now + 3_600_000).to_be_bytes()[2..]);
    let future = Uuid::from_bytes(bytes);
    assert_eq!(rotated.derive(future).unwrap().version, 2);
    assert_eq!(rotated.retire_before(Some(now)).versions().count(), 2);
    let retired = rotated.retire_before(Some(now + 3_600_000));
    assert!(matches!(retired.derive(id), Err(AnchorError::MissingSeed)));
    assert_eq!(
        retired.derive(future).unwrap().certificate_der,
        rotated.derive(future).unwrap().certificate_der
    );
    assert_eq!(rotated.retire_before(None).versions().count(), 2);
}

#[test]
fn malformed_or_mutated_keyrings_fail_closed() {
    assert!(Keyring::new(vec![]).is_err());
    assert!(
        Keyring::new(vec![
            SeedVersion::new(1, 10, [0; 32]),
            SeedVersion::new(2, 10, [1; 32])
        ])
        .is_err()
    );
    let changed = Keyring::new(vec![SeedVersion::new(1, 0, [43; 32])]).unwrap();
    assert!(matches!(
        ring().validate_successor(&changed),
        Err(AnchorError::ChangedSeed)
    ));
    assert!(matches!(
        ring().derive(Uuid::nil()),
        Err(AnchorError::InvalidId)
    ));
    assert!(ring().rotate(0, 0).is_err());
}

#[test]
fn gateway_certificate_expiry_is_bounded_and_chain_verifies() {
    let anchor = ring()
        .derive(Uuid::parse_str("019a1234-5678-7000-8000-000000000001").unwrap())
        .unwrap();
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
