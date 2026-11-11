use std::path::Path;

use super::*;

#[test]
fn strip_exe_deleted_marker_strips_trailing_marker_only() {
    assert_eq!(
        strip_exe_deleted_marker(Path::new("/opt/example/bin/example-svc (deleted)")),
        Path::new("/opt/example/bin/example-svc")
    );
    let clean = Path::new("/opt/example/bin/example-svc");
    assert_eq!(strip_exe_deleted_marker(clean), clean);
    // " (deleted)" anywhere except the trailing position is left alone.
    let weird = Path::new("/weird/path (deleted)/bin/foo");
    assert_eq!(strip_exe_deleted_marker(weird), weird);
}

#[test]
fn exe_link_deleted_detects_trailing_marker_only() {
    assert!(exe_link_was_deleted(Path::new(
        "/example/bin/example-svc (deleted)"
    )));
    assert!(!exe_link_was_deleted(Path::new("/example/bin/example-svc")));
    assert!(!exe_link_was_deleted(Path::new(
        "/weird (deleted)/bin/example-svc"
    )));
}

#[test]
fn content_addressed_file_name_is_exactly_name_dash_hex8() {
    assert!(is_content_addressed_binary_file_name(
        "example-svc-1a2b3c4d",
        "example-svc"
    ));
    for lookalike in [
        "example-svc-1A2B3C4D",   // uppercase digest
        "example-svc-1a2b3c",     // digest too short
        "example-svc-1a2b3c4d5e", // digest too long
        "example-svc",            // no digest suffix
        "example-svc2-1a2b3c4d",  // different binary name
    ] {
        assert!(
            !is_content_addressed_binary_file_name(lookalike, "example-svc"),
            "{lookalike} must not match"
        );
    }
}
