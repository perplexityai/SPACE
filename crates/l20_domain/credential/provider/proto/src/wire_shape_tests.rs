use prost::Message;

use super::v2::*;

#[test]
fn failure_reason_wire_numbering() {
    fn issue_number(reason: IssueFailureReason) -> i32 {
        match reason {
            IssueFailureReason::Unspecified => 0,
            IssueFailureReason::InvalidTenant => 1,
            IssueFailureReason::AuthorizationRejected => 2,
            IssueFailureReason::AccessRevoked => 3,
            IssueFailureReason::Unavailable => 4,
        }
    }

    fn unwrap_number(reason: UnwrapFailureReason) -> i32 {
        match reason {
            UnwrapFailureReason::Unspecified => 0,
            UnwrapFailureReason::InvalidKey => 1,
            UnwrapFailureReason::AuthorizationRejected => 2,
            UnwrapFailureReason::AccessRevoked => 3,
            UnwrapFailureReason::Unavailable => 4,
        }
    }

    for number in 0..=4 {
        let reason = IssueFailureReason::try_from(number).expect("known issue failure reason");
        assert_eq!(issue_number(reason), number);

        let reason = UnwrapFailureReason::try_from(number).expect("known unwrap failure reason");
        assert_eq!(unwrap_number(reason), number);
    }
}

#[test]
fn request_and_response_wire_shapes() {
    let issue = IssueIntermediateKeyRequest {
        tenant: Some(TenantRef {
            namespace: "n".to_string(),
            id: "t".to_string(),
        }),
        authorization: Some(AuthorizationEvidence {
            scheme: "s".to_string(),
            subject: Some("u".to_string()),
            evidence: vec![0x07],
        }),
    }
    .encode_to_vec();
    assert_eq!(
        issue,
        b"\x0a\x06\x0a\x01n\x12\x01t\x12\x09\x0a\x01s\x12\x01u\x1a\x01\x07"
    );

    let issue = IssueIntermediateKeyResponse {
        outcome: Some(issue_intermediate_key_response::Outcome::Key(
            WrappedIntermediateKey {
                key_id: "k".to_string(),
                wrapped_key: vec![0x08],
            },
        )),
    }
    .encode_to_vec();
    assert_eq!(issue, b"\x0a\x06\x0a\x01k\x12\x01\x08");

    let unwrap = UnwrapIntermediateKeyRequest {
        tenant: Some(TenantRef {
            namespace: "n".to_string(),
            id: "t".to_string(),
        }),
        key_id: "k".to_string(),
        wrapped_key: vec![0x08],
        authorization: None,
    }
    .encode_to_vec();
    assert_eq!(unwrap, b"\x0a\x06\x0a\x01n\x12\x01t\x12\x01k\x1a\x01\x08");

    let unwrap = UnwrapIntermediateKeyResponse {
        outcome: Some(unwrap_intermediate_key_response::Outcome::Key(
            IntermediateKeyMaterial {
                plaintext_key: vec![0x42; 32],
                max_cache_ttl: Some(prost_types::Duration {
                    seconds: 600,
                    nanos: 0,
                }),
            },
        )),
    }
    .encode_to_vec();
    assert_eq!(
        unwrap,
        b"\x0a\x27\x0a\x20\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x42\x12\x03\x08\xd8\x04"
    );
}
