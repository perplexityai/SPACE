use super::*;

#[test]
fn sandbox_roundtrip() {
    let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap();
    let subject = SubjectId::Sandbox(id);
    let rendered = subject.to_string();
    assert_eq!(rendered, "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2");
    let parsed: SubjectId = rendered.parse().unwrap();
    assert_eq!(parsed, subject);
    assert_eq!(parsed.kind(), SubjectKind::Sandbox);
}

#[test]
fn template_roundtrip() {
    let subject = SubjectId::Template(TemplateName::parse("seed").unwrap());
    let rendered = subject.to_string();
    assert_eq!(rendered, "template:seed");
    let parsed: SubjectId = rendered.parse().unwrap();
    assert_eq!(parsed, subject);
    assert_eq!(parsed.kind(), SubjectKind::Template);
}

#[test]
fn rejects_missing_separator() {
    assert!(matches!(
        "sandbox-no-colon".parse::<SubjectId>(),
        Err(SubjectIdParseError::Malformed)
    ));
}

#[test]
fn rejects_empty_body() {
    assert!(matches!(
        "sandbox:".parse::<SubjectId>(),
        Err(SubjectIdParseError::Malformed)
    ));
    assert!(matches!(
        "template:".parse::<SubjectId>(),
        Err(SubjectIdParseError::Malformed)
    ));
}

#[test]
fn rejects_unknown_kind() {
    let err = "build:abc".parse::<SubjectId>().unwrap_err();
    assert!(matches!(
        err,
        SubjectIdParseError::UnknownKind { ref kind } if kind == "build"
    ));
}

#[test]
fn rejects_invalid_body() {
    assert!(matches!(
        "sandbox:not-a-uuid".parse::<SubjectId>(),
        Err(SubjectIdParseError::Sandbox(_))
    ));
    assert!(matches!(
        "template:..".parse::<SubjectId>(),
        Err(SubjectIdParseError::Template(_))
    ));
}
