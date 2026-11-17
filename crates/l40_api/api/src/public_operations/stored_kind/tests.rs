use pplx_space_public_api::OperationKind;

use super::StoredOperationKind;

/// Every stored kind: each public one, and each system one.
fn every_stored_kind() -> Vec<StoredOperationKind> {
    OperationKind::ALL
        .iter()
        .copied()
        .map(StoredOperationKind::Public)
        .chain([StoredOperationKind::DrainRestoreSandbox])
        .collect()
}

/// Each stored kind parses back from its stored name, and none shares a
/// name with another.
#[test]
fn every_stored_kind_round_trips_through_its_stored_name() {
    let kinds = every_stored_kind();
    for kind in &kinds {
        assert_eq!(kind.as_str().parse::<StoredOperationKind>(), Ok(*kind));
    }
    let mut names = kinds.iter().map(|kind| kind.as_str()).collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), kinds.len(), "two kinds share a stored name");
    "restore".parse::<StoredOperationKind>().unwrap_err();
}

/// A public kind renders as itself; a system kind as the public kind it does
/// the work of, which is one the frozen contract lists.
#[test]
fn a_client_sees_only_public_kinds() {
    for kind in every_stored_kind() {
        let public = kind.public_kind();
        assert!(OperationKind::ALL.contains(&public), "{kind}");
        match kind {
            StoredOperationKind::Public(inner) => {
                assert_eq!(public, inner);
                assert_eq!(kind.as_public(), Some(inner));
            }
            StoredOperationKind::DrainRestoreSandbox => {
                assert_eq!(public, OperationKind::RestoreSandbox);
                assert_eq!(kind.as_public(), None);
                assert!(!kind.is_teardown());
            }
        }
    }
}
