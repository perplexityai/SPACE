//! Every rejection here is a field that would otherwise reach AWS unexamined,
//! so the validator is tested one refusal at a time.

use rcgen::{
    CertificateParams, DistinguishedName, DnType, KeyPair, PKCS_ECDSA_P256_SHA256,
    PKCS_ECDSA_P384_SHA384, PKCS_ED25519, SanType,
};

use super::*;

/// A node CSR as the agent will produce one: real key, real signature.
fn signed_csr(algorithm: &'static rcgen::SignatureAlgorithm) -> (KeyPair, Vec<u8>) {
    let key = KeyPair::generate_for(algorithm).expect("generate keypair");
    let der = params(CSR_COMMON_NAME)
        .serialize_request(&key)
        .expect("sign the request")
        .der()
        .to_vec();
    (key, der)
}

fn params(common_name: &str) -> CertificateParams {
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, common_name);
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name;
    params
}

fn parse_err(der: &[u8]) -> CsrError {
    NodeCsr::parse(der).expect_err("the request should be refused")
}

#[test]
fn accepts_a_signed_p256_request_and_keeps_its_key() {
    let (key, der) = signed_csr(&PKCS_ECDSA_P256_SHA256);
    let csr = NodeCsr::parse(&der).expect("a well-formed node request is accepted");

    use rcgen::PublicKeyData;
    assert_eq!(
        csr.public_key_der(),
        key.subject_public_key_info(),
        "the request must certify the key it was signed with"
    );
    assert!(
        csr.pem()
            .starts_with("-----BEGIN CERTIFICATE REQUEST-----\n")
    );
}

/// The load-bearing check: without it the authority forwards a request it
/// never proved was signed by the key inside it.
#[test]
fn rejects_a_request_whose_signature_does_not_match_its_key() {
    let (_, mut der) = signed_csr(&PKCS_ECDSA_P256_SHA256);
    // The signature is the last DER element, so corrupting the final byte
    // leaves the structure parseable and the signature wrong.
    let last = der.len() - 1;
    der[last] ^= 0xff;
    assert_eq!(parse_err(&der), CsrError::BadSignature);
}

/// The key is judged before the signature that covers it: a curve we will not
/// certify is a refusal whatever the request was signed with.
#[test]
fn rejects_keys_that_are_not_p256() {
    let (_, p384) = signed_csr(&PKCS_ECDSA_P384_SHA384);
    assert_eq!(
        parse_err(&p384),
        CsrError::UnsupportedCurve {
            // secp384r1, in the Certicom SEC arc rather than the X9.62 one
            // P-256 lives in.
            oid: "1.3.132.0.34".to_string()
        }
    );

    let (_, ed25519) = signed_csr(&PKCS_ED25519);
    assert!(matches!(
        parse_err(&ed25519),
        CsrError::UnsupportedKeyAlgorithm { .. }
    ));
}

/// An extension request is how a caller would try to widen its own certificate,
/// and the `APIPassthrough` template discarding them is a property of a config
/// string rather than of this code.
#[test]
fn rejects_a_request_carrying_attributes() {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate keypair");
    let mut params = params(CSR_COMMON_NAME);
    params.subject_alt_names.push(SanType::DnsName(
        "evil.example.com".try_into().expect("dns name"),
    ));
    let der = params
        .serialize_request(&key)
        .expect("sign the request")
        .der()
        .to_vec();

    assert_eq!(parse_err(&der), CsrError::UnexpectedAttributes { count: 1 });
}

#[test]
fn rejects_any_subject_but_the_expected_one() {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate keypair");
    let der = params("something-else")
        .serialize_request(&key)
        .expect("sign the request")
        .der()
        .to_vec();

    assert_eq!(parse_err(&der), CsrError::UnexpectedSubject);
}

/// `iter_attributes` yields every distinguished-name attribute, so matching on
/// the value alone would accept the expected string under any attribute type.
#[test]
fn rejects_the_expected_value_under_the_wrong_attribute() {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate keypair");
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::OrganizationName, CSR_COMMON_NAME);
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name;
    let der = params
        .serialize_request(&key)
        .expect("sign the request")
        .der()
        .to_vec();

    assert_eq!(parse_err(&der), CsrError::UnexpectedSubject);
}

#[test]
fn rejects_oversized_malformed_and_trailing_bytes() {
    assert_eq!(
        parse_err(&vec![0u8; MAX_CSR_LEN + 1]),
        CsrError::TooLarge {
            len: MAX_CSR_LEN + 1
        }
    );
    assert_eq!(parse_err(b"not der at all"), CsrError::Malformed);

    let (_, mut der) = signed_csr(&PKCS_ECDSA_P256_SHA256);
    der.extend_from_slice(b"trailing");
    assert_eq!(parse_err(&der), CsrError::TrailingBytes { len: 8 });
}
