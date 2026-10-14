use super::*;

#[test]
fn network_credential_snapshot_rejects_malformed_unauthorized_and_duplicate_entries() {
    use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
    use pplx_space_net_common::{
        CredentialHeaderBindingV1, NetworkCredentialDestinationV1, NetworkCredentialHttpMethodV1,
        NetworkCredentialRuleV1,
    };

    let reference_id: CredentialReferenceId =
        "018f1e9d-7b7a-7d3a-8000-000000000001".parse().unwrap();
    let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
    policy.tls_interception = pplx_space_net_common::TlsInterception::On;
    policy.network_credential_rules = vec![NetworkCredentialRuleV1 {
        rule_id: "api".into(),
        destination: NetworkCredentialDestinationV1::new(
            "api.example.com".parse().unwrap(),
            None,
            None,
        )
        .unwrap(),
        credential_header_bindings: vec![CredentialHeaderBindingV1 {
            header_name: "authorization".into(),
            credential_reference_id: reference_id.as_uuid(),
        }],
        supported_http_methods: vec![NetworkCredentialHttpMethodV1::Get],
    }];
    let credential = |id: String| ResolvedNetworkCredential {
        credential_reference_id: id,
        value: b"Bearer test-only".to_vec(),
    };

    assert!(matches!(
        NetworkCredentialSnapshot::from_wire(
            vec![credential(reference_id.to_string().to_uppercase())],
            &policy,
        ),
        Err(InvalidNetworkCredentialSnapshot::MalformedReferenceId)
    ));
    assert!(matches!(
        NetworkCredentialSnapshot::from_wire(
            vec![credential("018f1e9d-7b7a-7d3a-8000-000000000002".into())],
            &policy,
        ),
        Err(InvalidNetworkCredentialSnapshot::UnauthorizedReferenceId)
    ));
    assert!(matches!(
        NetworkCredentialSnapshot::from_wire(
            vec![
                credential(reference_id.to_string()),
                credential(reference_id.to_string()),
            ],
            &policy,
        ),
        Err(InvalidNetworkCredentialSnapshot::DuplicateReferenceId)
    ));
}
