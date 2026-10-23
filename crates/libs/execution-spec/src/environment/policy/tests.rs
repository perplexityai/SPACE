use super::*;

#[test]
fn stored_policy_requires_valid_schema_references_and_default() {
    let reference = format!("sha256:{}", "11".repeat(32));
    let other = format!("sha256:{}", "22".repeat(32));
    for (schema, allowed, default, valid) in [
        (1, vec![reference.clone()], Some(reference.clone()), true),
        (1, vec![], None, true),
        (2, vec![reference.clone()], Some(reference.clone()), false),
        (
            1,
            vec![reference.clone(), reference.clone()],
            Some(reference.clone()),
            false,
        ),
        (1, vec![reference.clone()], Some(other), false),
        (1, vec![], Some(reference.clone()), false),
        (1, vec![reference], None, false),
    ] {
        let document = serde_json::json!({
            "schema_version": schema,
            "allowed_environment_references": allowed,
            "default_environment_reference": default,
        });
        let result = serde_json::from_value::<ExecutionPolicyObservation>(document.clone());
        if valid {
            let observation = result.unwrap();
            assert!(!observation.legacy_unbound_allowed());
            let serialized = serde_json::to_value(&observation).unwrap();
            assert_eq!(serialized["schema_version"], document["schema_version"]);
            assert_eq!(
                serialized["allowed_environment_references"],
                document["allowed_environment_references"]
            );
            assert_eq!(
                serialized["default_environment_reference"],
                document["default_environment_reference"]
            );
            assert_eq!(
                serde_json::from_value::<ExecutionPolicyObservation>(serialized).unwrap(),
                observation
            );
        } else {
            result.unwrap_err();
        }
    }
}
