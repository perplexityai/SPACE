use super::*;

fn key(components: &[u32]) -> VersionSortKey {
    VersionSortKey::new(components.to_vec()).unwrap()
}

#[test]
fn keys_order_element_wise_with_prefixes_first() {
    let mut keys = [
        key(&[1, 18]),
        key(&[1, 15, 1]),
        key(&[2]),
        key(&[1, 15]),
        key(&[1, 9, 9]),
        key(&[1, 17, 0]),
    ];
    keys.sort();
    let sorted: Vec<_> = keys.iter().map(VersionSortKey::components).collect();
    assert_eq!(
        sorted,
        [
            &[1, 9, 9][..],
            &[1, 15],
            &[1, 15, 1],
            &[1, 17, 0],
            &[1, 18],
            &[2]
        ]
    );
}

#[test]
fn keys_stay_within_signed_integer_arrays() {
    assert_eq!(
        VersionSortKey::new(Vec::new()),
        Err(VersionSortKeyError::Empty)
    );
    assert_eq!(
        VersionSortKey::new(vec![0; VERSION_SORT_KEY_MAX_COMPONENTS + 1]),
        Err(VersionSortKeyError::TooLong(
            VERSION_SORT_KEY_MAX_COMPONENTS + 1
        ))
    );
    assert_eq!(
        VersionSortKey::new(vec![1, VERSION_SORT_KEY_MAX_COMPONENT + 1]),
        Err(VersionSortKeyError::ComponentTooLarge(
            VERSION_SORT_KEY_MAX_COMPONENT + 1
        ))
    );
    assert_eq!(
        key(&[VERSION_SORT_KEY_MAX_COMPONENT; VERSION_SORT_KEY_MAX_COMPONENTS]).components(),
        [VERSION_SORT_KEY_MAX_COMPONENT; VERSION_SORT_KEY_MAX_COMPONENTS]
    );
}

#[test]
fn an_empty_wire_key_means_no_order() {
    assert_eq!(VersionSortKey::from_wire(&[]), Ok(None));
    assert_eq!(VersionSortKey::to_wire(None), Vec::<u32>::new());
    let key = VersionSortKey::from_wire(&[1, 17, 0]).unwrap();
    assert_eq!(VersionSortKey::to_wire(key.as_ref()), [1, 17, 0]);
    assert_eq!(
        VersionSortKey::from_wire(&[VERSION_SORT_KEY_MAX_COMPONENT + 1]),
        Err(VersionSortKeyError::ComponentTooLarge(
            VERSION_SORT_KEY_MAX_COMPONENT + 1
        ))
    );
}
