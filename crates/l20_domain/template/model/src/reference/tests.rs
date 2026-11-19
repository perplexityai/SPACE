use super::*;
use crate::hash::Digest;

const ARTIFACT_HEX_A: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const ARTIFACT_HEX_B: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const INDEX_HEX: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";

fn name(value: &str) -> TemplateName {
    TemplateName::parse(value).expect("valid template name")
}

fn artifact_a() -> TemplateArtifactDigest {
    TemplateArtifactDigest::from_str(ARTIFACT_HEX_A).expect("valid artifact digest")
}

fn artifact_b() -> TemplateArtifactDigest {
    TemplateArtifactDigest::from_str(ARTIFACT_HEX_B).expect("valid artifact digest")
}

fn index_digest() -> TemplateIndexDigest {
    TemplateIndexDigest::from_str(INDEX_HEX).expect("valid index digest")
}

#[test]
fn template_selector_default_is_latest_tag() {
    let selector = TemplateSelector::default();
    assert_eq!(selector, TemplateSelector::latest());
    assert_eq!(selector.to_string(), "latest");
    assert_eq!(selector.tag().unwrap().as_str(), "latest");
    assert!(selector.artifact_digest().is_none());
    assert!(selector.artifact_digest_prefix().is_none());
}

#[test]
fn template_selector_parses_tag_full_digest_and_prefix() {
    let tag = TemplateSelector::from_str("stable").unwrap();
    assert!(matches!(tag, TemplateSelector::Tag(_)));
    assert_eq!(tag.tag().unwrap().as_str(), "stable");

    let full = TemplateSelector::from_str(ARTIFACT_HEX_A).unwrap();
    assert!(matches!(full, TemplateSelector::ArtifactDigest(_)));
    assert_eq!(full.artifact_digest().unwrap(), artifact_a());
    assert!(full.tag().is_none());

    let prefix = TemplateSelector::from_str("sha256:111111111111").unwrap();
    assert!(matches!(prefix, TemplateSelector::ArtifactDigestPrefix(_)));
    assert_eq!(
        prefix.artifact_digest_prefix().unwrap().to_string(),
        "sha256:111111111111"
    );
    assert!(prefix.artifact_digest().is_none());
}

#[test]
fn template_selector_rejects_invalid_inputs() {
    assert!(matches!(
        TemplateSelector::from_str("bad/tag"),
        Err(TemplateReferenceParseError::InvalidTag(_))
    ));
    assert!(matches!(
        TemplateSelector::from_str("md5:abcabcabcabcabcabcabcabcabcabcab"),
        Err(TemplateReferenceParseError::InvalidArtifactDigestPrefix(_))
    ));
    assert!(matches!(
        TemplateSelector::from_str("sha256:zzzzzzzzzzzz"),
        Err(TemplateReferenceParseError::InvalidArtifactDigestPrefix(_))
    ));
}

#[test]
fn template_selector_round_trips_via_string() {
    let original = TemplateSelector::ArtifactDigest(artifact_a());
    let encoded = String::from(original.clone());
    let decoded = TemplateSelector::try_from(encoded.clone()).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(encoded, ARTIFACT_HEX_A);
}

#[test]
fn template_reference_constructors_match_parsed_form() {
    let latest = TemplateReference::latest(name("seed"));
    assert!(latest.uses_default_selector());

    let from_with_tag =
        TemplateReference::with_tag(name("seed"), TemplateTag::parse("prod").unwrap());
    assert!(!from_with_tag.uses_default_selector());
    assert_eq!(from_with_tag.to_string(), "seed:prod");
    assert_eq!(
        from_with_tag,
        TemplateReference::from_str("seed:prod").unwrap()
    );

    let from_with_digest = TemplateReference::with_artifact_digest(name("seed"), artifact_a());
    assert_eq!(
        from_with_digest,
        TemplateReference::from_str(&format!("seed@{ARTIFACT_HEX_A}")).unwrap()
    );

    let prefix = TemplateArtifactDigestPrefix::from_str("sha256:111111111111").unwrap();
    let from_with_prefix = TemplateReference::with_artifact_digest_prefix(name("seed"), prefix);
    assert_eq!(from_with_prefix.to_string(), "seed@sha256:111111111111");
}

#[test]
fn template_reference_rejects_empty_string() {
    assert!(matches!(
        TemplateReference::from_str(""),
        Err(TemplateReferenceParseError::Empty)
    ));
}

#[test]
fn template_reference_rejects_invalid_name_in_tag_form() {
    // Empty name component before the tag separator.
    assert!(matches!(
        TemplateReference::from_str(":latest"),
        Err(TemplateReferenceParseError::InvalidName(_))
    ));
    // Slash is not a valid name character.
    assert!(matches!(
        TemplateReference::from_str("a/b:latest"),
        Err(TemplateReferenceParseError::InvalidName(_))
    ));
}

#[test]
fn template_reference_rejects_digest_without_algorithm_separator() {
    let err = TemplateReference::from_str("seed@deadbeef").unwrap_err();
    assert!(matches!(
        err,
        TemplateReferenceParseError::InvalidArtifactDigest(
            DigestParseError::MissingAlgorithmSeparator
        )
    ));
}

#[test]
fn template_reference_rejects_unsupported_digest_algorithm() {
    let err = TemplateReference::from_str("seed@md5:abcabcabcabcabcabcabcabcabcabcab").unwrap_err();
    assert!(matches!(
        err,
        TemplateReferenceParseError::InvalidArtifactDigestPrefix(
            DigestPrefixParseError::UnsupportedAlgorithm(_)
        )
    ));
}

#[test]
fn template_reference_round_trips_via_string() {
    let original = TemplateReference::with_artifact_digest(name("seed"), artifact_a());
    let encoded: String = original.clone().into();
    assert_eq!(encoded, format!("seed@{ARTIFACT_HEX_A}"));
    assert_eq!(TemplateReference::try_from(encoded).unwrap(), original);
}

#[test]
fn resolve_artifact_selector_passes_full_digest_through_without_candidates() {
    let reference = TemplateReference::with_artifact_digest(name("seed"), artifact_a());
    let resolved = reference
        .resolve_artifact_selector(RuntimeKind::Firecracker, std::iter::empty())
        .unwrap();
    assert_eq!(resolved.runtime(), RuntimeKind::Firecracker);
    assert_eq!(resolved.name().as_str(), "seed");
    assert_eq!(resolved.artifact_digest(), artifact_a());
}

#[test]
fn resolve_artifact_selector_rejects_tag_selector() {
    let reference =
        TemplateReference::with_tag(name("seed"), TemplateTag::parse("stable").unwrap());
    let err = reference
        .resolve_artifact_selector(RuntimeKind::Firecracker, [artifact_a()])
        .unwrap_err();
    assert!(matches!(
        err,
        TemplateReferenceResolveError::ExpectedDigestSelector
    ));
}

#[test]
fn resolve_artifact_selector_propagates_prefix_resolution_errors() {
    let prefix = TemplateArtifactDigestPrefix::from_str("sha256:111111111111").unwrap();
    let reference = TemplateReference::with_artifact_digest_prefix(name("seed"), prefix.clone());

    let no_match_err = reference
        .clone()
        .resolve_artifact_selector(RuntimeKind::Firecracker, [artifact_b()])
        .unwrap_err();
    assert!(matches!(
        no_match_err,
        TemplateReferenceResolveError::DigestPrefix(DigestPrefixResolveError::NoMatch(_))
    ));

    // Construct two artifacts that both share the prefix to trigger ambiguity.
    let mut colliding_bytes = [0x22u8; 32];
    colliding_bytes[..6].fill(0x11);
    let colliding = TemplateArtifactDigest::new(ContentHash::new(Digest::sha256(colliding_bytes)));
    let ambiguous_err = reference
        .resolve_artifact_selector(RuntimeKind::Firecracker, [artifact_a(), colliding])
        .unwrap_err();
    assert!(matches!(
        ambiguous_err,
        TemplateReferenceResolveError::DigestPrefix(DigestPrefixResolveError::Ambiguous(_))
    ));
}

#[test]
fn runtime_scoped_template_reference_round_trips_parts() {
    let reference = TemplateReference::with_artifact_digest(name("seed"), artifact_a());
    let scoped = RuntimeScopedTemplateReference::new(RuntimeKind::Gvisor, reference.clone());
    assert_eq!(scoped.runtime(), RuntimeKind::Gvisor);
    assert_eq!(scoped.reference(), &reference);
    assert_eq!(scoped.into_parts(), (RuntimeKind::Gvisor, reference));
}

#[test]
fn resolved_template_reference_short_string_uses_short_digest() {
    let resolved = ResolvedTemplateReference::new(name("seed"), index_digest());
    assert_eq!(resolved.name().as_str(), "seed");
    assert_eq!(resolved.index_digest(), index_digest());
    assert_eq!(resolved.short_string(), "seed@index:sha256:333333333333");
    assert_eq!(resolved.to_string(), format!("seed@index:{INDEX_HEX}"));
}

#[test]
fn resolved_template_reference_round_trips_via_string() {
    let resolved = ResolvedTemplateReference::new(name("seed"), index_digest());
    let encoded: String = resolved.clone().into();
    assert_eq!(
        ResolvedTemplateReference::try_from(encoded).unwrap(),
        resolved
    );
}

#[test]
fn resolved_template_reference_rejects_tag_and_digest_form() {
    // A `name:something@index:...` input should fall into the tag-and-digest guard.
    let err =
        ResolvedTemplateReference::from_str(&format!("seed:tag@index:{INDEX_HEX}")).unwrap_err();
    assert!(matches!(err, TemplateReferenceParseError::TagAndDigest));
}

#[test]
fn resolved_template_variant_exposes_artifact_views() {
    let variant =
        ResolvedTemplateVariant::new(RuntimeKind::CloudHypervisor, name("seed"), artifact_a());
    assert_eq!(variant.content_hash(), artifact_a().content_hash());
    assert_eq!(
        variant.short_string(),
        "runtime=cloud-hypervisor template=seed artifact=sha256:111111111111"
    );

    let parts = variant.clone().into_parts();
    assert_eq!(
        parts,
        (RuntimeKind::CloudHypervisor, name("seed"), artifact_a())
    );

    let build_id = variant.into_build_id();
    assert_eq!(build_id.artifact_digest(), artifact_a());
}

#[cfg(feature = "serde")]
#[test]
fn template_reference_serde_round_trip_uses_string_form() {
    let reference = TemplateReference::with_artifact_digest(name("seed"), artifact_a());
    let json = serde_json::to_string(&reference).unwrap();
    assert_eq!(json, format!("\"seed@{ARTIFACT_HEX_A}\""));
    let decoded: TemplateReference = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, reference);
}

// Moved from the standalone `tests/template_identifiers.rs` binary. Each test
// below parses (or rejects) a full `name[:tag|@digest]` string through
// `TemplateReference`/`ResolvedTemplateReference` directly; the
// `template_selector_*` tests above cover only the selector suffix in
// isolation, so name extraction, whole-reference `Display`, and each type's
// own independently-implemented tag-and-digest guard are not covered
// anywhere else.

#[test]
fn template_reference_defaults_to_latest() {
    let reference = TemplateReference::from_str("seed").unwrap();
    assert_eq!(reference.name().as_str(), "seed");
    assert_eq!(reference.tag().unwrap().as_str(), DEFAULT_TEMPLATE_TAG);
    assert_eq!(reference.to_string(), "seed:latest");
}

#[test]
fn template_reference_parses_tag_selector() {
    let reference = TemplateReference::from_str("code-runtime:py312").unwrap();
    assert_eq!(reference.name().as_str(), "code-runtime");
    assert_eq!(reference.tag().unwrap().as_str(), "py312");
    assert_eq!(reference.artifact_digest(), None);
}

#[test]
fn template_reference_parses_digest_selector() {
    let reference = TemplateReference::from_str(
        "seed@sha256:0000000000000000000000000000000000000000000000000000000000000000",
    )
    .unwrap();
    assert_eq!(reference.name().as_str(), "seed");
    assert!(reference.tag().is_none());
    assert_eq!(
        reference.artifact_digest().unwrap().to_string(),
        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    );
}

#[test]
fn template_reference_accepts_digest_prefix_selector() {
    let reference = TemplateReference::from_str("seed@sha256:111111111111").unwrap();
    assert_eq!(reference.name().as_str(), "seed");
    assert!(reference.tag().is_none());
    assert_eq!(reference.artifact_digest(), None);
    assert_eq!(
        reference.artifact_digest_prefix().unwrap().to_string(),
        "sha256:111111111111"
    );
    assert_eq!(reference.to_string(), "seed@sha256:111111111111");
}

#[test]
fn template_reference_rejects_tag_and_digest() {
    let err = TemplateReference::from_str(
        "seed:v1@sha256:0000000000000000000000000000000000000000000000000000000000000000",
    )
    .unwrap_err();
    assert!(matches!(err, TemplateReferenceParseError::TagAndDigest));
}

#[test]
fn resolved_template_reference_rejects_digest_prefix() {
    // A runtime-specific artifact digest (`@sha256:...`) is not an index
    // digest (`@index:sha256:...`), so `ResolvedTemplateReference` — which
    // requires the explicit `@index:` form — must refuse it.
    let err = ResolvedTemplateReference::from_str("seed@sha256:111111111111").unwrap_err();
    assert!(matches!(
        err,
        TemplateReferenceParseError::ExpectedIndexDigest
    ));
}

#[test]
fn resolved_template_reference_requires_digest() {
    ResolvedTemplateReference::from_str("seed:latest").unwrap_err();
    ResolvedTemplateReference::from_str(
        "seed@sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .unwrap_err();
    let resolved = ResolvedTemplateReference::from_str(
        "seed@index:sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .unwrap();
    assert_eq!(
        resolved.to_string(),
        "seed@index:sha256:1111111111111111111111111111111111111111111111111111111111111111"
    );
    assert_eq!(
        resolved.index_digest().to_string(),
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    );
}

#[test]
fn resolved_template_variant_tracks_runtime_artifact_digest() {
    let variant_name = TemplateName::parse("seed").unwrap();
    let artifact_digest = TemplateArtifactDigest::from_str(
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    )
    .unwrap();
    let variant =
        ResolvedTemplateVariant::new(RuntimeKind::Firecracker, variant_name, artifact_digest);
    assert_eq!(variant.runtime(), RuntimeKind::Firecracker);
    assert_eq!(variant.name().as_str(), "seed");
    assert_eq!(variant.artifact_digest(), artifact_digest);
    assert_eq!(
        variant.to_string(),
        "runtime=firecracker template=seed artifact=sha256:2222222222222222222222222222222222222222222222222222222222222222"
    );
}

#[test]
fn resolve_artifact_selector_resolves_digest_prefix_against_candidates() {
    // Complements `resolve_artifact_selector_propagates_prefix_resolution_errors`
    // above, which only covers the no-match/ambiguous failure paths; this
    // covers the successful prefix-resolves-to-exactly-one-candidate path.
    assert_eq!(
        TemplateReference::from_str("seed@sha256:111111111111")
            .unwrap()
            .resolve_artifact_selector(RuntimeKind::Firecracker, [artifact_a(), artifact_b()])
            .unwrap()
            .to_string(),
        format!("runtime=firecracker template=seed artifact={ARTIFACT_HEX_A}")
    );
}
