use std::path::Path;

use super::paths;

#[test]
fn svc_binary_allowlists_derive_from_the_bin_dir_constants() {
    // The allowlists and the named bin-dir constants expand from one macro;
    // this pins that a future edit to either keeps them in agreement — a
    // diverged entry would deny every credmgr RPC from that peer under
    // enforce.
    for (allowlist, names) in [
        (
            paths::SPACE_SBXMGR_UID_BINARIES,
            &["space-sbxmgr-svc", "space-volmgr-svc"][..],
        ),
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

#[test]
fn strip_exe_deleted_marker_strips_trailing_marker_only() {
    assert_eq!(
        paths::strip_exe_deleted_marker(Path::new("/opt/space/bin/space-sbxmgr-svc (deleted)")),
        Path::new("/opt/space/bin/space-sbxmgr-svc")
    );
    let clean = Path::new("/opt/space/bin/space-sbxmgr-svc");
    assert_eq!(paths::strip_exe_deleted_marker(clean), clean);
    // " (deleted)" anywhere except the trailing position is left alone.
    let weird = Path::new("/weird/path (deleted)/bin/foo");
    assert_eq!(paths::strip_exe_deleted_marker(weird), weird);
}

#[test]
fn exe_link_deleted_detects_trailing_marker_only() {
    assert!(paths::exe_link_was_deleted(Path::new(
        "/space/bin/space-credmgr-svc (deleted)"
    )));
    assert!(!paths::exe_link_was_deleted(Path::new(
        "/space/bin/space-credmgr-svc"
    )));
    assert!(!paths::exe_link_was_deleted(Path::new(
        "/weird (deleted)/bin/space-credmgr-svc"
    )));
}

#[test]
fn content_addressed_file_name_is_exactly_name_dash_hex8() {
    assert!(paths::is_content_addressed_binary_file_name(
        "space-sbxmgr-svc-1a2b3c4d",
        "space-sbxmgr-svc"
    ));
    for lookalike in [
        "space-sbxmgr-svc-1A2B3C4D",   // uppercase digest
        "space-sbxmgr-svc-1a2b3c",     // digest too short
        "space-sbxmgr-svc-1a2b3c4d5e", // digest too long
        "space-sbxmgr-svc",            // no digest suffix
        "space-sbxmgr-svc2-1a2b3c4d",  // different binary name
    ] {
        assert!(
            !paths::is_content_addressed_binary_file_name(lookalike, "space-sbxmgr-svc"),
            "{lookalike} must not match"
        );
    }
}
