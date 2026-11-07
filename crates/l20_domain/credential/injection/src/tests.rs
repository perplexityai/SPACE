use super::proto::ResolvedNetworkCredential;
use zeroize::Zeroize;

#[test]
fn secret_bearing_wire_types_redact_debug_output() {
    let secret = b"do-not-log-this";
    let rendered = format!(
        "{:?}",
        ResolvedNetworkCredential {
            credential_reference_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
            value: secret.to_vec(),
        },
    );
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains(std::str::from_utf8(secret).unwrap()));
}

#[test]
fn secret_bearing_wire_types_zeroize() {
    fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>() {}
    assert_zeroize_on_drop::<ResolvedNetworkCredential>();

    let mut network_credential = ResolvedNetworkCredential {
        credential_reference_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        value: b"do-not-retain-this".to_vec(),
    };
    network_credential.zeroize();
    assert!(network_credential.credential_reference_id.is_empty());
    assert!(network_credential.value.is_empty());
}
