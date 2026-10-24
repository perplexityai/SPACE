use super::*;

#[test]
fn x509_profile_rejects_storage_actor_and_noncanonical_identity_fields() {
    let valid = || ExchangeTokenRequest {
        grant_type: "urn:ietf:params:oauth:grant-type:token-exchange".into(),
        resource: "urn:space:credential:sandbox-tls-anchor".into(),
        requested_token_type: "urn:space:token-type:x509-certificate".into(),
        subject_token: Some("sandbox:019a7a10-0000-7000-8000-000000000001".into()),
        subject_token_type: Some("urn:space:token-type:subject-id".into()),
        audience: None,
        scope: None,
        actor_token: None,
        actor_token_type: None,
        requested_expires_in: None,
        copy_source_resource: None,
    };
    assert_eq!(
        AnchorRequest::parse(&valid()).unwrap().kind,
        CredentialKind::Certificate
    );
    let mutations: &[fn(&mut ExchangeTokenRequest)] = &[
        |r| r.resource = "s3://example-bucket/object".into(),
        |r| r.grant_type = "client_credentials".into(),
        |r| r.audience = Some("s3".into()),
        |r| r.scope = Some("object:read".into()),
        |r| r.actor_token = Some("service:volume-manager".into()),
        |r| r.actor_token_type = Some("urn:space:token-type:subject-id".into()),
        |r| r.requested_expires_in = Some(60),
        |r| r.copy_source_resource = Some("s3://example-bucket/source".into()),
        |r| r.subject_token_type = Some("urn:space:token-type:access-token".into()),
        |r| r.subject_token = Some("sandbox:019a7a10-0000-4000-8000-000000000001".into()),
        |r| r.subject_token = Some("sandbox:019A7A10-0000-7000-8000-000000000001".into()),
        |r| r.requested_token_type = "urn:ietf:params:oauth:token-type:aws-credentials".into(),
    ];
    for mutate in mutations {
        let mut request = valid();
        mutate(&mut request);
        assert!(AnchorRequest::parse(&request).is_err());
    }
    let mut private = valid();
    private.requested_token_type = "urn:space:token-type:x509-signing-credentials".into();
    assert_eq!(
        AnchorRequest::parse(&private).unwrap().kind,
        CredentialKind::SigningCredentials
    );
}
