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

#[test]
fn applied_policy_carries_its_generation_and_may_lack_a_default() {
    let reference = format!("sha256:{}", "11".repeat(32));
    for (schema, allowed, default, generation, valid) in [
        (
            2,
            vec![reference.clone()],
            Some(reference.clone()),
            Some(4),
            true,
        ),
        // The configured default was removed and the cluster's was refused.
        (2, vec![reference.clone()], None, Some(4), true),
        (2, vec![], None, Some(0), true),
        (
            2,
            vec![reference.clone()],
            Some(reference.clone()),
            None,
            false,
        ),
        (
            1,
            vec![reference.clone()],
            Some(reference.clone()),
            Some(4),
            false,
        ),
        (1, vec![reference.clone()], None, None, false),
        (3, vec![], None, Some(4), false),
    ] {
        let document = serde_json::json!({
            "schema_version": schema,
            "allowed_environment_references": allowed,
            "default_environment_reference": default,
            "applied_generation": generation,
        });
        let result = serde_json::from_value::<ExecutionPolicyObservation>(document.clone());
        if !valid {
            result.unwrap_err();
            continue;
        }
        let observation = result.unwrap();
        assert_eq!(observation.applied_generation(), generation);
        let serialized = serde_json::to_value(&observation).unwrap();
        assert_eq!(
            serialized["applied_generation"],
            document["applied_generation"]
        );
        assert_eq!(
            serde_json::from_value::<ExecutionPolicyObservation>(serialized).unwrap(),
            observation
        );
    }
    // Startup policies keep their stored shape, so older readers decode them.
    let startup = ExecutionPolicyObservation::new(1, vec![], None, true, None, false).unwrap();
    let stored = serde_json::to_value(&startup).unwrap();
    assert!(stored.get("applied_generation").is_none(), "{stored}");
}

#[test]
fn unreadable_cluster_policy_admits_nothing_and_has_no_generation() {
    let reference = format!("sha256:{}", "11".repeat(32));
    for (schema, allowed, default, legacy, generation, valid) in [
        (2, vec![], None, false, None, true),
        (2, vec![reference.clone()], None, false, None, false),
        (
            2,
            vec![reference.clone()],
            Some(reference.clone()),
            false,
            None,
            false,
        ),
        (2, vec![], None, true, None, false),
        (2, vec![], None, false, Some(3), false),
        (1, vec![], None, false, None, false),
    ] {
        let document = serde_json::json!({
            "schema_version": schema,
            "allowed_environment_references": allowed,
            "default_environment_reference": default,
            "legacy_unbound_allowed": legacy,
            "applied_generation": generation,
            "cluster_policy_unreadable": true,
        });
        let result = serde_json::from_value::<ExecutionPolicyObservation>(document);
        if !valid {
            result.unwrap_err();
            continue;
        }
        let observation = result.unwrap();
        assert!(observation.cluster_policy_unreadable());
        let stored = serde_json::to_value(&observation).unwrap();
        assert_eq!(stored["cluster_policy_unreadable"], true);
        assert_eq!(
            serde_json::from_value::<ExecutionPolicyObservation>(stored).unwrap(),
            observation
        );
    }
    // A governed policy names a generation or an unreadable record.
    ExecutionPolicyObservation::new(2, vec![], None, false, None, false).unwrap_err();
    let applied = ExecutionPolicyObservation::new(2, vec![], None, false, Some(1), false).unwrap();
    let stored = serde_json::to_value(&applied).unwrap();
    assert!(
        stored.get("cluster_policy_unreadable").is_none(),
        "{stored}"
    );
}

#[test]
fn legacy_default_can_coexist_with_explicit_compiled_environments() {
    let reference = format!("sha256:{}", "33".repeat(32));
    let value: ExecutionPolicyObservation = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "allowed_environment_references": [reference],
        "default_environment_reference": null,
        "legacy_unbound_allowed": true,
    }))
    .unwrap();
    assert_eq!(value.allowed_environment_references().len(), 1);
    assert!(value.default_environment_reference().is_none());
    assert!(value.legacy_unbound_allowed());
    assert_eq!(
        serde_json::from_value::<ExecutionPolicyObservation>(serde_json::to_value(&value).unwrap())
            .unwrap(),
        value
    );
}
