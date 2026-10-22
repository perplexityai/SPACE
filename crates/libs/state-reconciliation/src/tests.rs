use super::*;

#[test]
fn network_policy_target_round_trips_template() {
    let target = NetworkPolicyTarget::Template(TemplateName::parse("seed").unwrap());
    assert_eq!(target.to_string(), "template:seed");
    assert_eq!(
        target.to_string().parse::<NetworkPolicyTarget>().unwrap(),
        target
    );
}

#[test]
fn network_policy_target_rejects_missing_prefix() {
    assert!(matches!(
        "seed".parse::<NetworkPolicyTarget>(),
        Err(NetworkPolicyTargetParseError::MissingPrefix)
    ));
}
