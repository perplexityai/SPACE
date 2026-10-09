use super::proto::{
    AwsCredentials, DecryptResponse, EncryptRequest, ExchangeTokenRequest, ExchangeTokenResponse,
    GetOrIssueSandboxVolumeDekResponse, IssueSandboxArtifactDekResponse,
    ResolveNetworkCredentialsResponse, UnwrapSandboxArtifactDekResponse, exchange_token_response,
};

fn assert_redacted(value: &impl std::fmt::Debug, type_name: &str) {
    assert_eq!(format!("{value:?}"), format!("{type_name}(<redacted>)"));
}

#[test]
fn token_exchange_debug_output_is_redacted() {
    assert_redacted(
        &ExchangeTokenRequest {
            grant_type: "grant".to_string(),
            resource: "resource".to_string(),
            audience: Some("audience".to_string()),
            scope: Some("scope".to_string()),
            requested_token_type: "requested-token-type".to_string(),
            subject_token: Some("secret-subject-token".to_string()),
            subject_token_type: Some("subject-token-type".to_string()),
            actor_token: Some("secret-actor-token".to_string()),
            actor_token_type: Some("actor-token-type".to_string()),
            requested_expires_in: Some(3600),
        },
        "ExchangeTokenRequest",
    );
    assert_redacted(
        &ExchangeTokenResponse {
            issued_token_type: "issued-token-type".to_string(),
            token_type: "token-type".to_string(),
            expires_in: Some(300),
            scope: Some("scope".to_string()),
            refresh_token: Some("secret-refresh-token".to_string()),
            payload: Some(exchange_token_response::Payload::AccessToken(
                "secret-access-token".to_string(),
            )),
        },
        "ExchangeTokenResponse",
    );
    assert_redacted(
        &AwsCredentials {
            access_key_id: "secret-access-key-id".to_string(),
            secret_access_key: "secret-access-key".to_string(),
            session_token: Some("secret-session-token".to_string()),
            expires_at_unix: Some(300),
        },
        "AwsCredentials",
    );
    assert_redacted(
        &exchange_token_response::Payload::AccessToken("secret-access-token".to_string()),
        "Payload",
    );
}

#[test]
fn plaintext_dek_debug_output_is_redacted() {
    assert_redacted(
        &IssueSandboxArtifactDekResponse {
            plaintext_dek: b"secret-issued-dek".to_vec(),
            wrapped_dek: b"wrapped-dek".to_vec(),
        },
        "IssueSandboxArtifactDekResponse",
    );
    assert_redacted(
        &UnwrapSandboxArtifactDekResponse {
            plaintext_dek: b"secret-unwrapped-dek".to_vec(),
        },
        "UnwrapSandboxArtifactDekResponse",
    );
    assert_redacted(
        &GetOrIssueSandboxVolumeDekResponse {
            plaintext_dek: b"secret-volume-dek".to_vec(),
        },
        "GetOrIssueSandboxVolumeDekResponse",
    );
}

#[test]
fn encryption_plaintext_debug_output_is_redacted() {
    assert_redacted(
        &EncryptRequest {
            organization_id: "organization-id".to_string(),
            user_id: Some("user-id".to_string()),
            plaintext: b"secret-encryption-input".to_vec(),
            aad: b"aad".to_vec(),
            purpose: "credential".to_string(),
        },
        "EncryptRequest",
    );
    assert_redacted(
        &DecryptResponse {
            plaintext: b"secret-decryption-output".to_vec(),
        },
        "DecryptResponse",
    );
}

#[test]
fn resolved_network_credentials_debug_output_is_redacted() {
    assert_redacted(
        &ResolveNetworkCredentialsResponse {
            effective_network_policy_ref: "policy-ref".into(),
            credentials: vec![
                pplx_space_credential_injection_core::proto::ResolvedNetworkCredential {
                    credential_reference_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".into(),
                    value: b"secret-header-value".to_vec(),
                },
            ],
        },
        "ResolveNetworkCredentialsResponse",
    );
}
