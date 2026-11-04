use super::*;

fn configured() -> GuestSeeds {
    serde_json::from_str(r#"{"ubuntu":{"24.04":"seed-noble:validated"}}"#).unwrap()
}

fn selected(root: &Path, base: &str) -> String {
    configured()
        .select(root, &base.parse().unwrap())
        .unwrap()
        .to_string()
}

#[test]
fn selects_only_the_configured_os_and_release() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("etc")).unwrap();
    for (contents, expected) in [
        ("ID=ubuntu\nVERSION_ID=24.04\n", "seed-noble:validated"),
        ("ID=ubuntu\nVERSION_ID=\"24.04\"\n", "seed-noble:validated"),
        ("ID='ubuntu'\nVERSION_ID='24.04'\n", "seed-noble:validated"),
        ("ID=ubuntu\nVERSION_ID=26.04\n", "seed:latest"),
        ("ID=ubuntu\nVERSION_ID=22.04\n", "seed:latest"),
        ("ID=debian\nVERSION_ID=12\n", "seed:latest"),
        (
            "ID=custom\nID_LIKE=ubuntu\nVERSION_ID=24.04\n",
            "seed:latest",
        ),
        ("ID=ubuntu\nVERSION_ID=24.04.3\n", "seed:latest"),
        ("ID=ubuntu\n", "seed:latest"),
        ("ID=$(echo ubuntu)\nVERSION_ID=24.04\n", "seed:latest"),
        ("ID=ubuntu\nVERSION_ID=\"24.04\n", "seed:latest"),
        (
            "ID=ubuntu\nVERSION_ID=24.04\nVERSION_ID=26.04\n",
            "seed:latest",
        ),
    ] {
        fs::write(root.path().join("etc/os-release"), contents).unwrap();
        assert_eq!(selected(root.path(), "seed"), expected, "{contents}");
    }
    fs::write(
        root.path().join("etc/os-release"),
        "ID=ubuntu\nVERSION_ID=24.04\n",
    )
    .unwrap();
    assert_eq!(
        selected(
            root.path(),
            "seed@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ),
        "seed-noble:validated"
    );
}

#[test]
fn disabled_configuration_and_explicit_bases_do_not_inspect_the_image() {
    let missing = Path::new("/nonexistent-guest-seed-test-root");
    let base = "seed".parse().unwrap();
    for seeds in [GuestSeeds::default(), serde_json::from_str("{}").unwrap()] {
        assert_eq!(seeds.select(missing, &base).unwrap(), base);
    }
    for raw in [
        "custom-seed",
        "custom-seed:stable",
        "custom-seed@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        let explicit = raw.parse().unwrap();
        assert_eq!(configured().select(missing, &explicit).unwrap(), explicit);
    }
}

#[test]
fn os_release_precedence_and_missing_identity() {
    let root = tempfile::tempdir().unwrap();
    let base = "seed".parse().unwrap();
    assert_eq!(configured().select(root.path(), &base).unwrap(), base);
    fs::create_dir_all(root.path().join("usr/lib")).unwrap();
    fs::write(
        root.path().join("usr/lib/os-release"),
        "ID=ubuntu\nVERSION_ID=24.04\n",
    )
    .unwrap();
    assert_eq!(selected(root.path(), "seed"), "seed-noble:validated");
    fs::create_dir(root.path().join("etc")).unwrap();
    fs::write(
        root.path().join("etc/os-release"),
        "ID=debian\nVERSION_ID=12\n",
    )
    .unwrap();
    assert_eq!(configured().select(root.path(), &base).unwrap(), base);
}

#[cfg(unix)]
#[test]
fn resolves_image_symlinks_without_reading_host_paths() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("etc")).unwrap();
    fs::create_dir_all(root.path().join("usr/lib")).unwrap();
    let contents = "ID=ubuntu\nVERSION_ID=24.04\n";
    fs::write(root.path().join("usr/lib/os-release"), contents).unwrap();
    fs::write(outside.path().join("os-release"), contents).unwrap();
    let link = root.path().join("etc/os-release");
    let base = "seed".parse().unwrap();
    for target in [
        "../usr/lib/os-release",
        "/usr/lib/os-release",
        "../../usr/lib/os-release",
    ] {
        symlink(target, &link).unwrap();
        assert_eq!(
            selected(root.path(), "seed"),
            "seed-noble:validated",
            "{target}"
        );
        fs::remove_file(&link).unwrap();
    }
    // Directory symlinks also resolve relative to the image root.
    fs::remove_dir(root.path().join("etc")).unwrap();
    symlink("/usr/lib", root.path().join("etc")).unwrap();
    assert_eq!(selected(root.path(), "seed"), "seed-noble:validated");
    fs::remove_file(root.path().join("etc")).unwrap();
    fs::create_dir(root.path().join("etc")).unwrap();
    fs::remove_file(root.path().join("usr/lib/os-release")).unwrap();
    // A host file at the same absolute path cannot supply the image identity.
    symlink(outside.path().join("os-release"), &link).unwrap();
    assert_eq!(configured().select(root.path(), &base).unwrap(), base);
    fs::remove_file(&link).unwrap();
    symlink("os-release", &link).unwrap();
    assert!(matches!(
        configured().select(root.path(), &base),
        Err(GuestSeedError::InvalidFile)
    ));
    fs::remove_file(&link).unwrap();
    fs::create_dir(&link).unwrap();
    assert!(matches!(
        configured().select(root.path(), &base),
        Err(GuestSeedError::InvalidFile)
    ));
}

#[test]
fn rejects_invalid_config_and_oversized_identity_files() {
    for value in ["", "[]", r#"{"ubuntu":{"24.04":"../seed"}}"#] {
        serde_json::from_str::<GuestSeeds>(value).expect_err("invalid seed configuration");
    }
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("etc")).unwrap();
    fs::write(
        root.path().join("etc/os-release"),
        vec![b'x'; MAX_OS_RELEASE_BYTES as usize + 1],
    )
    .unwrap();
    assert!(matches!(
        configured().select(root.path(), &"seed".parse().unwrap()),
        Err(GuestSeedError::InvalidFile)
    ));
}
