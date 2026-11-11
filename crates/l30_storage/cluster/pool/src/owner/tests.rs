use super::*;

#[test]
fn parses_canonical_storage_values() {
    assert_eq!(
        "manual".parse::<PoolConfigOwner>().unwrap(),
        PoolConfigOwner::Manual
    );
    assert_eq!(
        "autoscaler".parse::<PoolConfigOwner>().unwrap(),
        PoolConfigOwner::Autoscaler
    );
    "controller".parse::<PoolConfigOwner>().unwrap_err();
}
