use std::str::FromStr;

use super::*;

#[test]
fn build_run_id_uses_local_parse_error() {
    assert!(matches!(
        BuildRunId::from_str("not-a-uuid"),
        Err(BuildRunIdParseError::InvalidUuid(_))
    ));
}

#[test]
fn build_run_id_round_trips_uuid_encoding() {
    let id = BuildRunId::new();
    assert_eq!(BuildRunId::from_str(&id.to_string()).unwrap(), id);
}

#[test]
fn template_build_id_tracks_artifact_digest() {
    let template = TemplateName::parse("seed").unwrap();
    let content_hash = ContentHash::from_str(
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    )
    .unwrap();
    let artifact_digest = TemplateArtifactDigest::new(content_hash);
    let build = TemplateBuildId::from_artifact_digest(template, artifact_digest);
    assert_eq!(build.artifact_digest(), artifact_digest);
    assert_eq!(build.content_hash(), content_hash);
}
