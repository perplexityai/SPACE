use super::paths;

#[test]
fn svc_binary_allowlists_derive_from_the_bin_dir_constants() {
    // The allowlists and the named bin-dir constants expand from one macro;
    // this pins that a future edit to either keeps them in agreement — a
    // diverged entry would deny every credmgr RPC from that peer under
    // enforce.
    for (allowlist, names) in [
        (paths::SPACE_SBXMGR_UID_BINARIES, &["space-sbxmgr-svc"][..]),
        (paths::SPACE_VOLMGR_SVC_BINARIES, &["space-volmgr-svc"][..]),
        (
            paths::SPACE_TMPLMGR_SVC_BINARIES,
            &["space-tmplmgr-svc"][..],
        ),
    ] {
        let expected: Vec<String> = names
            .iter()
            .flat_map(|name| {
                [
                    format!("{}/{name}", paths::SPACE_HELM_BIN_DIR),
                    format!("{}/{name}", paths::SPACE_SYSTEMD_BIN_DIR),
                ]
            })
            .collect();
        assert_eq!(
            allowlist
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn systemd_apps_dir_is_a_sibling_of_the_bin_dir() {
    // The named invariant: digest targets and the symlink dir share a parent
    // (the /space volume), so it survives a coordinated rename but fires if
    // /space/apps moves under a different root.
    assert_eq!(
        std::path::Path::new(paths::SPACE_SYSTEMD_APPS_DIR).parent(),
        std::path::Path::new(paths::SPACE_SYSTEMD_BIN_DIR).parent(),
    );
    // The literals themselves are pinned separately: the digest-form matcher
    // compares against these exact strings in the supported systemd layout.
    assert_eq!(paths::SPACE_SYSTEMD_BIN_DIR, "/space/bin");
    assert_eq!(paths::SPACE_SYSTEMD_APPS_DIR, "/space/apps");
}

/// A template without a dir has no cached partitions, but a template dir that
/// can't be listed is an error, never read as none.
#[test]
fn cached_template_partitions_report_a_dir_they_cannot_list() {
    use crate::types::{Digest, EnvironmentReference, TemplateCachePartition};
    let root = tempfile::tempdir().unwrap();
    let hash = format!("sha256:{}", "b".repeat(64));
    assert!(
        paths::cached_template_partitions_in(root.path(), "absent", &hash)
            .unwrap()
            .is_empty()
    );

    let twin =
        TemplateCachePartition::Environment(EnvironmentReference::new(Digest::sha256([2; 32])));
    for partition in [twin, TemplateCachePartition::Legacy] {
        std::fs::create_dir_all(
            root.path()
                .join("seed")
                .join(partition.build_dir_name(&hash)),
        )
        .unwrap();
    }
    assert_eq!(
        paths::cached_template_partitions_in(root.path(), "seed", &hash).unwrap(),
        [TemplateCachePartition::Legacy, twin]
    );

    std::fs::write(root.path().join("unlistable"), b"not a dir").unwrap();
    paths::cached_template_partitions_in(root.path(), "unlistable", &hash).unwrap_err();
}
