use super::*;

fn entry(digest: &str) -> String {
    format!(
        "[[migrations]]\nstore = 'sandbox-manager'\nversion = 2\nchecksum = '{digest}'\nreason = 'adds a table the release before never names'"
    )
}

#[test]
fn only_the_listed_store_version_and_checksum_are_accepted() {
    let digest = [0xab; 48];
    let registry = ForwardCompatible::from_toml(&entry(&hex::encode(digest))).unwrap();

    assert!(registry.accepts("sandbox-manager", 2, &digest));
    let mut edited = digest;
    edited[47] = 0xac;
    assert!(!registry.accepts("sandbox-manager", 2, &edited));
    assert!(!registry.accepts("sandbox-manager", 3, &digest));
    assert!(!registry.accepts("node-manager", 2, &digest));
}

#[test]
fn malformed_or_ambiguous_manifests_are_rejected() {
    let digest = "ab".repeat(48);
    let valid = entry(&digest);
    for source in [
        "not toml".into(),
        valid.replace(&digest, "abc"),
        valid.replace(&digest, &"zz".repeat(48)),
        format!("{valid}\n{valid}"),
        valid.replace("version = 2", "version = 0"),
        valid.replace("'sandbox-manager'", "''"),
        valid.replace("'adds a table the release before never names'", "' '"),
        valid.replace(
            "\nreason = 'adds a table the release before never names'",
            "",
        ),
        format!("{valid}\nunknown = true"),
    ] {
        assert!(
            ForwardCompatible::from_toml(&source).is_err(),
            "accepted {source}"
        );
    }
}

#[test]
fn the_embedded_manifest_parses() {
    ForwardCompatible::embedded().unwrap();
}
