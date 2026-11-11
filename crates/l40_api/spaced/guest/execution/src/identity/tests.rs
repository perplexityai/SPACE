use super::supplementary_groups_match;
use nix::unistd::Gid;

#[test]
fn same_user_requires_equivalent_supplementary_groups() {
    for (current, requested, matches) in [
        (vec![], vec![1000], true),
        (vec![10, 20], vec![20, 1000, 10, 10], true),
        (vec![10, 20], vec![1000, 10], false),
        (vec![10], vec![1000, 10, 20], false),
    ] {
        assert_eq!(
            supplementary_groups_match(
                current.into_iter().map(Gid::from_raw).collect(),
                &requested.into_iter().map(Gid::from_raw).collect::<Vec<_>>(),
                Gid::from_raw(1000),
            ),
            matches
        );
    }
}
