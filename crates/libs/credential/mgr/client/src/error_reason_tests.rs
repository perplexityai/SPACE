use super::*;

#[test]
fn detects_the_revocation_reason_and_nothing_else() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        CREDMGR_ERROR_REASON_ORG_KEK_ACCESS_REVOKED.parse().unwrap(),
    );
    let revoked =
        tonic::Status::with_metadata(tonic::Code::FailedPrecondition, "revoked", metadata);
    assert!(status_is_org_kek_access_revoked(&revoked));

    // Same code, no reason metadata: not a revocation.
    let plain = tonic::Status::failed_precondition("KEK provider not configured");
    assert!(!status_is_org_kek_access_revoked(&plain));

    // Reason key present with a different value: not a revocation.
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        "some-future-reason".parse().unwrap(),
    );
    let other = tonic::Status::with_metadata(tonic::Code::FailedPrecondition, "other", metadata);
    assert!(!status_is_org_kek_access_revoked(&other));
}

#[test]
fn detects_the_release_already_published_reason_and_nothing_else() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        CREDMGR_ERROR_REASON_RELEASE_ALREADY_PUBLISHED
            .parse()
            .unwrap(),
    );
    let published =
        tonic::Status::with_metadata(tonic::Code::PermissionDenied, "published", metadata);
    assert!(status_is_release_already_published(&published));

    // Same code, no reason metadata: an unrelated denial.
    let plain = tonic::Status::permission_denied("caller is not authorized");
    assert!(!status_is_release_already_published(&plain));

    // Reason present on another code: not a publication refusal.
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        CREDMGR_ERROR_REASON_RELEASE_ALREADY_PUBLISHED
            .parse()
            .unwrap(),
    );
    let wrong_code = tonic::Status::with_metadata(tonic::Code::Internal, "published", metadata);
    assert!(!status_is_release_already_published(&wrong_code));
}

#[test]
fn detects_a_node_ca_unavailable_reason_of_any_kind_and_nothing_else() {
    for suffix in ["authority_not_configured", "authority_unavailable"] {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            CREDMGR_ERROR_REASON_METADATA_KEY,
            format!("{CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX}{suffix}")
                .parse()
                .unwrap(),
        );
        let status = tonic::Status::with_metadata(tonic::Code::Unavailable, "no CA", metadata);
        assert!(status_is_node_ca_unavailable(&status), "{suffix}");
    }

    // Same code, no reason metadata: an ordinary outage, not a node CA verdict.
    assert!(!status_is_node_ca_unavailable(&tonic::Status::unavailable(
        "node_ca_unavailable:authority_refused in the message only"
    )));

    // The reason on a code that the node CA never uses for it.
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        format!("{CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX}authority_refused")
            .parse()
            .unwrap(),
    );
    assert!(!status_is_node_ca_unavailable(
        &tonic::Status::with_metadata(tonic::Code::FailedPrecondition, "no CA", metadata)
    ));

    // A different reason on the same code.
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        CREDMGR_ERROR_REASON_ORG_KEK_ACCESS_REVOKED.parse().unwrap(),
    );
    assert!(!status_is_node_ca_unavailable(
        &tonic::Status::with_metadata(tonic::Code::Unavailable, "revoked", metadata)
    ));
}

#[test]
fn detects_the_invalid_key_reason_and_nothing_else() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        CREDMGR_ERROR_REASON_METADATA_KEY,
        CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE.parse().unwrap(),
    );
    let invalid_key =
        tonic::Status::with_metadata(tonic::Code::FailedPrecondition, "invalid key", metadata);

    assert!(status_is_invalid_key_reference(&invalid_key));
    assert!(!status_is_invalid_key_reference(
        &tonic::Status::failed_precondition("other precondition")
    ));
    assert!(!status_is_invalid_key_reference(&tonic::Status::internal(
        "invalid key"
    )));
}
