use std::collections::{BTreeMap, BTreeSet};

use super::*;

#[test]
fn manifest_keys_and_operation_ids_are_unique() {
    let route_keys = NATIVE_V1_OPERATIONS
        .iter()
        .map(|operation| (operation.path(), operation.method()))
        .collect::<BTreeSet<_>>();
    let operation_ids = NATIVE_V1_OPERATIONS
        .iter()
        .map(|operation| operation.operation_id())
        .collect::<BTreeSet<_>>();

    assert_eq!(route_keys.len(), NATIVE_V1_OPERATIONS.len());
    assert_eq!(operation_ids.len(), NATIVE_V1_OPERATIONS.len());
}

#[test]
fn manifest_paths_are_rooted_at_v1() {
    for operation in NATIVE_V1_OPERATIONS {
        assert!(
            operation.path().starts_with("/v1/") || operation.path() == "/v1",
            "{} path is not rooted at /v1",
            operation.operation_id()
        );
        assert!(
            operation.runtime_path().starts_with("/v1/") || operation.runtime_path() == "/v1",
            "{} runtime path is not rooted at /v1",
            operation.operation_id()
        );
    }
}

/// Operations that share one mount must say so by declaring a runtime path that
/// differs from their own path. Without this a documented path could silently
/// become the representative for a whole group of custom `:action` operations,
/// so removing it would unmount all of them.
#[test]
fn operations_sharing_a_mount_declare_a_distinct_runtime_path() {
    let mut groups: BTreeMap<(&str, NativeHttpMethod), Vec<NativeOperation>> = BTreeMap::new();
    for operation in NATIVE_V1_OPERATIONS {
        groups
            .entry((operation.runtime_path(), operation.method()))
            .or_default()
            .push(*operation);
    }

    for ((runtime_path, method), operations) in groups {
        if operations.len() == 1 {
            continue;
        }
        for operation in operations {
            assert_ne!(
                operation.path(),
                runtime_path,
                "{} is documented at the mount shared by {method:?} {runtime_path}",
                operation.operation_id()
            );
        }
    }
}

/// An unclassified mount is only acceptable when a declared operation already
/// covers it. Anchoring the exclusion to `PROXY_SESSION`'s path means a future
/// route cannot be quietly exempted by adding a constant.
#[test]
fn unclassified_mounts_sit_under_a_declared_operation() {
    let declared = NATIVE_V1_OPERATIONS
        .iter()
        .filter(|operation| operation.surface() == NativeSurface::DataPlane)
        .map(|operation| operation.path())
        .collect::<Vec<_>>();

    assert!(
        declared
            .iter()
            .any(|path| PROXY_SESSION_SUBPATH_MOUNT.starts_with(&format!("{path}/"))),
        "{PROXY_SESSION_SUBPATH_MOUNT} is not under any declared data-plane operation: {declared:?}"
    );
}
