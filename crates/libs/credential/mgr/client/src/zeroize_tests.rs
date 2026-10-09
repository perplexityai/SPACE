use super::proto::{
    AwsCredentials, DecryptResponse, EncryptRequest, ExchangeTokenRequest, ExchangeTokenResponse,
    GetOrIssueSandboxVolumeDekResponse, IssueSandboxArtifactDekResponse,
    ResolveNetworkCredentialsResponse, UnwrapSandboxArtifactDekResponse, exchange_token_response,
};
use zeroize::Zeroize;

#[test]
fn secret_bearing_wire_types_zeroize_on_drop() {
    fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>() {}

    assert_zeroize_on_drop::<ExchangeTokenRequest>();
    assert_zeroize_on_drop::<ExchangeTokenResponse>();
    assert_zeroize_on_drop::<exchange_token_response::Payload>();
    assert_zeroize_on_drop::<AwsCredentials>();
    assert_zeroize_on_drop::<IssueSandboxArtifactDekResponse>();
    assert_zeroize_on_drop::<UnwrapSandboxArtifactDekResponse>();
    assert_zeroize_on_drop::<EncryptRequest>();
    assert_zeroize_on_drop::<DecryptResponse>();
    assert_zeroize_on_drop::<GetOrIssueSandboxVolumeDekResponse>();
    assert_zeroize_on_drop::<ResolveNetworkCredentialsResponse>();
}

#[test]
fn token_exchange_secrets_zeroize() {
    let mut request = ExchangeTokenRequest {
        grant_type: String::new(),
        resource: String::new(),
        audience: None,
        scope: None,
        requested_token_type: String::new(),
        subject_token: Some("secret-subject-token".to_string()),
        subject_token_type: None,
        actor_token: Some("secret-actor-token".to_string()),
        actor_token_type: None,
        requested_expires_in: Some(3600),
    };
    request.zeroize();
    assert!(request.subject_token.is_none());
    assert!(request.actor_token.is_none());

    let mut response = ExchangeTokenResponse {
        issued_token_type: String::new(),
        token_type: String::new(),
        expires_in: None,
        scope: None,
        refresh_token: Some("secret-refresh-token".to_string()),
        payload: Some(exchange_token_response::Payload::AccessToken(
            "secret-access-token".to_string(),
        )),
    };
    response.zeroize();
    assert!(response.refresh_token.is_none());
    assert!(response.payload.is_none());

    let mut payload =
        exchange_token_response::Payload::AccessToken("secret-access-token".to_string());
    payload.zeroize();
    assert!(matches!(
        payload,
        exchange_token_response::Payload::AccessToken(ref token) if token.is_empty()
    ));

    let mut credentials = AwsCredentials {
        access_key_id: "secret-access-key-id".to_string(),
        secret_access_key: "secret-access-key".to_string(),
        session_token: Some("secret-session-token".to_string()),
        expires_at_unix: Some(300),
    };
    credentials.zeroize();
    assert!(credentials.access_key_id.is_empty());
    assert!(credentials.secret_access_key.is_empty());
    assert!(credentials.session_token.is_none());
}

#[test]
fn plaintext_deks_zeroize() {
    let mut issued = IssueSandboxArtifactDekResponse {
        plaintext_dek: b"secret-issued-dek".to_vec(),
        wrapped_dek: b"wrapped-dek".to_vec(),
    };
    issued.zeroize();
    assert!(issued.plaintext_dek.is_empty());
    assert!(issued.wrapped_dek.is_empty());

    let mut unwrapped = UnwrapSandboxArtifactDekResponse {
        plaintext_dek: b"secret-unwrapped-dek".to_vec(),
    };
    unwrapped.zeroize();
    assert!(unwrapped.plaintext_dek.is_empty());

    let mut volume = GetOrIssueSandboxVolumeDekResponse {
        plaintext_dek: b"secret-volume-dek".to_vec(),
    };
    volume.zeroize();
    assert!(volume.plaintext_dek.is_empty());
}

#[test]
fn encryption_plaintexts_zeroize() {
    let mut encrypt = EncryptRequest {
        organization_id: String::new(),
        user_id: None,
        plaintext: b"secret-encryption-input".to_vec(),
        aad: Vec::new(),
        purpose: String::new(),
    };
    encrypt.zeroize();
    assert!(encrypt.plaintext.is_empty());

    let mut decrypt = DecryptResponse {
        plaintext: b"secret-decryption-output".to_vec(),
    };
    decrypt.zeroize();
    assert!(decrypt.plaintext.is_empty());
}

#[test]
fn resolved_network_credentials_zeroize() {
    let mut response = ResolveNetworkCredentialsResponse {
        effective_network_policy_ref: "policy-ref".into(),
        credentials: vec![
            pplx_space_credential_injection_core::proto::ResolvedNetworkCredential {
                credential_reference_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".into(),
                value: b"secret-header-value".to_vec(),
            },
        ],
    };
    response.zeroize();
    assert!(response.effective_network_policy_ref.is_empty());
    assert!(response.credentials.is_empty());
}
