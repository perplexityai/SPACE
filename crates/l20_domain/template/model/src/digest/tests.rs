use std::str::FromStr;

use super::*;

#[test]
fn template_index_digest_short_string_uses_default_prefix_len() {
    let digest = TemplateIndexDigest::new(ContentHash::new(Digest::sha256([0x11; 32])));
    assert_eq!(digest.short_string(), "sha256:111111111111");
}

#[test]
fn digest_prefix_resolves_unique_candidate() {
    let first = TemplateArtifactDigest::new(ContentHash::new(Digest::sha256([0x11; 32])));
    let second = TemplateArtifactDigest::new(ContentHash::new(Digest::sha256([0x22; 32])));
    let prefix = TemplateArtifactDigestPrefix::from_str("sha256:111111111111").unwrap();

    assert_eq!(prefix.resolve_unique([first, second]).unwrap(), first);
}

#[test]
fn digest_prefix_resolution_reports_missing_and_ambiguous() {
    let first = TemplateArtifactDigest::new(ContentHash::new(Digest::sha256([0x11; 32])));
    let second = TemplateArtifactDigest::new(ContentHash::new(Digest::sha256([0x22; 32])));
    let mut colliding_bytes = [0x22; 32];
    colliding_bytes[..6].fill(0x11);
    let colliding = TemplateArtifactDigest::new(ContentHash::new(Digest::sha256(colliding_bytes)));
    let prefix = TemplateArtifactDigestPrefix::from_str("sha256:111111111111").unwrap();

    assert!(matches!(
        prefix.resolve_unique([second]),
        Err(DigestPrefixResolveError::NoMatch(_))
    ));
    assert!(matches!(
        prefix.resolve_unique([first, colliding]),
        Err(DigestPrefixResolveError::Ambiguous(_))
    ));
}
