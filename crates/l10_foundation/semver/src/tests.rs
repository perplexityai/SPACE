use super::SemVer;

#[test]
fn parses_and_orders_semantic_versions() {
    let older = SemVer::parse("0.1.1").unwrap();
    let newer = SemVer::parse("0.1.2").unwrap();

    assert!(older < newer);
    assert_eq!(newer.to_string(), "0.1.2");
}

#[test]
fn rejects_non_semver_values() {
    SemVer::parse("0.1").unwrap_err();
    SemVer::parse("latest").unwrap_err();
}
