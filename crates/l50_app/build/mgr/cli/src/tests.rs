use super::*;

#[test]
fn build_flags_default_to_publish_enabled() {
    let cli = Cli::parse_from([
        "space-buildmgr",
        "build",
        "--recipe",
        "/tmp/r.toml",
        "--id",
        "seed",
    ]);
    let Commands::Build { publish_tags, .. } = cli.command else {
        panic!("expected build command");
    };
    assert!(publish_tags.is_empty());
}

#[test]
fn build_flags_allow_explicit_publish_tags() {
    let cli = Cli::parse_from([
        "space-buildmgr",
        "build",
        "--recipe",
        "/tmp/r.toml",
        "--id",
        "seed",
        "--publish-tag",
        "latest",
        "--publish-tag",
        "prod",
    ]);
    let Commands::Build { publish_tags, .. } = cli.command else {
        panic!("expected build command");
    };
    assert_eq!(publish_tags, vec!["latest", "prod"]);
}
