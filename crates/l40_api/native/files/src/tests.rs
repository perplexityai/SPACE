use pplx_space_public_api::routes::{RENAME_FILE, WRITE_FILE, WRITE_FILE_BATCH};

use super::*;

/// The sandbox replays a keyed operation by key alone, and can remember a key
/// after its binding has expired. The key it sees therefore changes with the
/// request, so a reused key's different request never reaches the first
/// request's record, while a retry of the same request keeps its key.
#[test]
fn the_sandbox_key_changes_with_the_request() {
    let fingerprint = |data: &[u8]| {
        let mut digest = RequestDigest::default();
        digest.field(b"/workspace/a").field(data);
        digest.finish().unwrap()
    };
    let key = IdempotencyKey::parse("client-key").unwrap();
    let first = guest_key(WRITE_FILE, &key, &fingerprint(b"first")).unwrap();
    assert_eq!(
        guest_key(WRITE_FILE, &key, &fingerprint(b"first")).unwrap(),
        first
    );
    assert_ne!(first, key);
    for other in [
        guest_key(WRITE_FILE, &key, &fingerprint(b"other")).unwrap(),
        guest_key(WRITE_FILE_BATCH, &key, &fingerprint(b"first")).unwrap(),
        guest_key(RENAME_FILE, &key, &fingerprint(b"first")).unwrap(),
        guest_key(
            WRITE_FILE,
            &IdempotencyKey::parse("other-key").unwrap(),
            &fingerprint(b"first"),
        )
        .unwrap(),
    ] {
        assert_ne!(other, first);
    }
}

/// Each Native `files:` route is exactly one [`FileOperation`], named by its
/// action.
#[test]
fn every_files_route_is_one_operation() {
    let mut routes: Vec<&str> = pplx_space_public_api::routes::NATIVE_V1_OPERATIONS
        .iter()
        .map(|route| route.path())
        .filter(|path| path.contains("/files:"))
        .collect();
    routes.sort_unstable();
    let mut operations: Vec<&str> = FileOperation::ALL
        .iter()
        .map(|operation| {
            let path = operation.route().path();
            assert!(
                path.ends_with(&format!("/files:{operation}")),
                "{operation} is {path}"
            );
            path
        })
        .collect();
    operations.sort_unstable();
    assert_eq!(operations, routes);
}
