use super::*;

#[test]
fn artifact_id_round_trips_through_string() {
    let id = ArtifactId::new();
    let parsed: ArtifactId = id.to_string().parse().unwrap();
    assert_eq!(id, parsed);
}

#[test]
fn artifact_id_rejects_garbage() {
    "not-a-uuid".parse::<ArtifactId>().unwrap_err();
}

#[test]
fn entry_role_validates() {
    assert_eq!(
        ArtifactEntryRole::parse("memfile").unwrap().as_str(),
        "memfile"
    );
    assert!(matches!(
        ArtifactEntryRole::parse(""),
        Err(ArtifactEntryRoleError::Empty)
    ));
    assert!(matches!(
        ArtifactEntryRole::parse("x".repeat(MAX_ENTRY_ROLE_LEN + 1)),
        Err(ArtifactEntryRoleError::TooLong)
    ));
    assert!(matches!(
        ArtifactEntryRole::parse("a\0b"),
        Err(ArtifactEntryRoleError::ContainsNul)
    ));
}
