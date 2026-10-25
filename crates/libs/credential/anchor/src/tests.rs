use super::*;
use x509_parser::prelude::{FromDer, X509Certificate};

// Public fixture key only. Runtime validation uses real non-exportable KMS keys.
fn material(environment: &str, key_byte: u8) -> Zeroizing<[u8; 48]> {
    let environment: AnchorEnvironment = environment.parse().unwrap();
    let key = hmac::Key::new(hmac::HMAC_SHA384, &[key_byte; 48]);
    Zeroizing::new(
        hmac::sign(&key, &environment.seed_input())
            .as_ref()
            .try_into()
            .unwrap(),
    )
}

#[test]
fn stable_isolated_anchors_depend_on_the_selected_seed_not_creation_time() {
    let id = Uuid::parse_str("019a1234-5678-7000-8000-000000000001").unwrap();
    let seed = material("fixture", 42);
    let first = derive_anchor(id, &seed).unwrap();
    // Persistent certificate identity, not just repeatability within one build.
    assert_eq!(
        Sha256::digest(&first.certificate_der).as_slice(),
        &[
            112, 125, 229, 163, 245, 138, 131, 47, 96, 214, 82, 133, 146, 200, 116, 149, 102, 156,
            33, 60, 211, 85, 248, 137, 147, 168, 10, 20, 114, 53, 67, 108,
        ],
    );
    let second = derive_anchor(id, &material("fixture", 42)).unwrap();
    assert_eq!(first.certificate_der, second.certificate_der);
    assert_eq!(first.private_key_der, second.private_key_der);
    let (_, certificate) = X509Certificate::from_der(&first.certificate_der).unwrap();
    certificate.verify_signature(None).unwrap();
    assert_eq!(
        certificate
            .basic_constraints()
            .unwrap()
            .unwrap()
            .value
            .path_len_constraint,
        Some(1)
    );
    assert!(
        certificate
            .key_usage()
            .unwrap()
            .unwrap()
            .value
            .key_cert_sign()
    );
    assert!(certificate.raw_serial().len() <= 20);
    for (other_id, other_seed) in [
        (
            Uuid::parse_str("019a1234-5678-7000-8000-000000000002").unwrap(),
            seed.clone(),
        ),
        (id, material("fixture", 43)),
        (id, material("other-environment", 42)),
    ] {
        let other = derive_anchor(other_id, &other_seed).unwrap();
        assert_ne!(first.certificate_der, other.certificate_der);
        assert_ne!(first.private_key_der, other.private_key_der);
        let (_, foreign) = X509Certificate::from_der(&other.certificate_der).unwrap();
        assert!(
            certificate
                .verify_signature(Some(foreign.public_key()))
                .is_err()
        );
    }
}

#[test]
fn derivation_domains_and_uuid_variants_are_strict() {
    for environment in ["", "space/production", "space production", "é"] {
        environment.parse::<AnchorEnvironment>().unwrap_err();
    }
    assert!(matches!(
        derive_anchor(Uuid::nil(), &[42; 48]),
        Err(AnchorError::InvalidId)
    ));
    let mut id = *Uuid::parse_str("019a1234-5678-7000-8000-000000000001")
        .unwrap()
        .as_bytes();
    id[8] = 0;
    assert!(matches!(
        derive_anchor(Uuid::from_bytes(id), &[42; 48]),
        Err(AnchorError::InvalidId)
    ));
}

#[test]
fn gateway_certificate_expiry_is_bounded_and_chain_verifies() {
    let anchor = derive_anchor(
        Uuid::parse_str("019a1234-5678-7000-8000-000000000001").unwrap(),
        &material("fixture", 42),
    )
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
