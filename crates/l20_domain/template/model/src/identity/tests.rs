use super::*;

#[test]
fn template_name_rejects_traversal_components() {
    TemplateName::parse("../seed").unwrap_err();
    TemplateName::parse("seed:latest").unwrap_err();
    TemplateName::parse("seed@sha256").unwrap_err();
    TemplateName::parse("seed name").unwrap_err();
    TemplateName::parse("seed\nname").unwrap_err();
    TemplateName::parse(".".to_string()).unwrap_err();
    TemplateName::parse("a".repeat(256)).unwrap_err();
    TemplateName::parse("seed").unwrap();
    TemplateName::parse("code_runtime.v1").unwrap();
}

#[test]
fn template_tag_follows_oci_tag_shape() {
    TemplateTag::parse("latest").unwrap();
    TemplateTag::parse("release_1.2-rc1").unwrap();
    TemplateTag::parse("-latest").unwrap_err();
    TemplateTag::parse("bad/tag").unwrap_err();
    TemplateTag::parse("bad:tag").unwrap_err();
}
