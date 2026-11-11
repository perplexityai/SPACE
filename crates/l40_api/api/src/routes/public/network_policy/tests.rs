use super::*;

#[test]
fn document_revision_etag_is_opaque_and_input_sensitive() {
    use pplx_space_net_common::NetworkPolicyV1;

    let deny_all = NetworkPolicyV1::deny_all();
    let allow_all = NetworkPolicyV1::allow_all();

    let base = document::revision_etag(3, false, &deny_all);
    assert_eq!(base, "\"e37a9ad8c5efe30ee649f0c0f2b02bd6\"");
    // Deterministic for identical inputs; quoted; no parseable structure.
    assert_eq!(base, document::revision_etag(3, false, &deny_all));
    assert!(base.starts_with('"') && base.ends_with('"'));
    assert!(!base.contains(':') && !base.contains("generation"));
    // Every input to the effective revision changes it: generation,
    // override-vs-inherited, and policy content.
    assert_ne!(base, document::revision_etag(4, false, &deny_all));
    assert_ne!(base, document::revision_etag(3, true, &deny_all));
    assert_ne!(base, document::revision_etag(3, false, &allow_all));
}
