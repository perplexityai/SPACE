use super::*;

#[test]
fn requirements_are_bounded_and_canonical() {
    let requirement =
        |name: &str, count| DeviceRequirement::new(name.parse().unwrap(), count).unwrap();
    let a = requirement("compute-a", 1);
    let b = requirement("compute-b", 2);
    assert_eq!(
        DeviceRequirements::new(vec![b.clone(), a.clone()]).unwrap(),
        DeviceRequirements::new(vec![a.clone(), b]).unwrap()
    );
    for values in [
        vec![],
        vec![a.clone(), a],
        vec![requirement("a", 16), requirement("b", 1)],
    ] {
        DeviceRequirements::new(values).unwrap_err();
    }
    for count in [0, 17, u32::MAX] {
        DeviceRequirement::new("compute".parse().unwrap(), count).unwrap_err();
    }
    for profile in ["", "../device", "a/b", "profile\n", "UPPER"] {
        DeviceProfile::parse(profile).unwrap_err();
    }
}

#[cfg(feature = "serde")]
#[test]
fn persisted_requirements_cannot_bypass_admission_validation() {
    for entries in [
        "[]",
        r#"[{"profile":"compute","count":0}]"#,
        r#"[{"profile":"compute","count":1},{"profile":"compute","count":2}]"#,
        r#"[{"profile":"compute","count":1,"path":"/dev/anything"}]"#,
    ] {
        let raw = format!(r#"{{"schema_version":1,"entries":{entries}}}"#);
        serde_json::from_str::<DeviceRequirements>(&raw).unwrap_err();
    }
    for raw in [
        r#"{"schema_version":2,"entries":[{"profile":"compute","count":1}]}"#,
        r#"{"entries":[{"profile":"compute","count":1}]}"#,
        r#"{"schema_version":1,"entries":[{"profile":"compute","count":1}],"ignored":true}"#,
    ] {
        serde_json::from_str::<DeviceRequirements>(raw).unwrap_err();
    }
    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("compute".parse().unwrap(), 1).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        serde_json::from_str::<DeviceRequirements>(&serde_json::to_string(&requirements).unwrap())
            .unwrap(),
        requirements
    );
}

#[test]
fn compatible_clauses_are_homogeneous_and_selection_is_complete() {
    use std::collections::BTreeMap;
    let alternatives = |names: &[&str], count| {
        DeviceRequirement::compatible(names.iter().map(|p| p.parse().unwrap()).collect(), count)
            .unwrap()
    };
    let capacity = BTreeMap::from([("a".parse().unwrap(), 1), ("b".parse().unwrap(), 1)]);
    let preferences = BTreeMap::from([
        ("a".parse().unwrap(), 10),
        ("b".parse().unwrap(), 1),
        ("c".parse().unwrap(), 0),
    ]);
    let request = DeviceRequirements::new(vec![
        alternatives(&["b", "a"], 1),
        alternatives(&["a", "c"], 1),
    ])
    .unwrap();
    assert_eq!(
        request
            .select_profiles(&capacity, &preferences)
            .unwrap()
            .unwrap(),
        vec!["b".parse().unwrap(), "a".parse().unwrap()]
    );
    assert!(request.is_satisfied_by(&capacity).unwrap());
    let homogeneous = DeviceRequirements::new(vec![alternatives(&["b", "a"], 2)]).unwrap();
    assert!(!homogeneous.is_satisfied_by(&capacity).unwrap());
    assert_eq!(
        homogeneous
            .select_profiles(&capacity, &preferences)
            .unwrap(),
        None
    );
    let tied = DeviceRequirements::new(vec![alternatives(&["b", "a"], 1)]).unwrap();
    let equal = BTreeMap::from([("a".parse().unwrap(), 1), ("b".parse().unwrap(), 1)]);
    assert_eq!(
        tied.select_profiles(&capacity, &equal),
        Err(DeviceSelectionError::Ambiguous)
    );
    assert_eq!(
        tied.select_profiles(&capacity, &BTreeMap::new()).unwrap(),
        None
    );
}

#[test]
fn overlapping_alternative_search_has_an_explicit_work_budget() {
    let profiles: Vec<DeviceProfile> = (0..16)
        .map(|i| format!("p{i:02}").parse().unwrap())
        .collect();
    let clauses = (0..16)
        .map(|i| {
            DeviceRequirement::compatible(
                profiles
                    .iter()
                    .enumerate()
                    .filter(|(n, _)| *n != i)
                    .map(|(_, p)| p.clone())
                    .collect(),
                1,
            )
            .unwrap()
        })
        .collect();
    let requirements = DeviceRequirements::new(clauses).unwrap();
    let capacity = profiles.iter().cloned().map(|p| (p, 1)).collect();
    let preferences = profiles.iter().cloned().map(|p| (p, 0)).collect();
    assert_eq!(
        requirements.select_profiles(&capacity, &preferences),
        Err(DeviceSelectionError::Complexity)
    );
}
