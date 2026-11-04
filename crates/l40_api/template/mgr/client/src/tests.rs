use std::str::FromStr;

use pplx_space_common_core::types::TemplateReference;

use super::proto::TemplateRef;

const HASH: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn template_ref_from_reference_uses_tag_selector() {
    let reference = TemplateReference::from_str("code-runtime:latest").unwrap();

    let template_ref = TemplateRef::try_from(&reference).unwrap();

    assert_eq!(template_ref.name, "code-runtime");
    assert_eq!(template_ref.tag.as_deref(), Some("latest"));
    assert_eq!(template_ref.content_hash, None);
}

#[test]
fn template_ref_from_reference_uses_exact_content_hash() {
    let reference = TemplateReference::from_str(&format!("code-runtime@{HASH}")).unwrap();

    let template_ref = TemplateRef::try_from(&reference).unwrap();

    assert_eq!(template_ref.name, "code-runtime");
    assert_eq!(template_ref.content_hash.as_deref(), Some(HASH));
    assert_eq!(template_ref.tag, None);
}

#[test]
fn template_ref_from_reference_rejects_digest_prefix() {
    let reference = TemplateReference::from_str("code-runtime@sha256:0123456789abcdef")
        .expect("digest prefix should parse as a template selector");

    let error = TemplateRef::try_from(&reference).unwrap_err();

    assert!(error.to_string().contains("full artifact digest"));
}
