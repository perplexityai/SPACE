use super::*;

#[test]
fn acknowledgement_requires_exact_coverage_and_persisted_leases() {
    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("compute".parse().unwrap(), 1).unwrap(),
    ])
    .unwrap();
    let assignment = ExclusiveDeviceAssignment::new(
        DeviceLeaseId::new(),
        pplx_space_execution_spec::devices::DeviceParentId::new(),
        "compute".parse().unwrap(),
        vec!["pci:0000:01:00.0".parse().unwrap()],
    )
    .unwrap();
    let ack = proto::DeviceAcknowledgement {
        requirements_digest: requirements_digest(&requirements).to_string(),
        assignments: vec![(&assignment).into()],
    };
    assert_eq!(
        validate_acknowledgement(
            &requirements,
            Some(&ack),
            Some(std::slice::from_ref(&assignment))
        )
        .unwrap(),
        vec![assignment.clone()]
    );
    validate_acknowledgement(&requirements, None, None).unwrap_err();
    for change in 0..7 {
        let mut bad = ack.clone();
        match change {
            0 => bad.requirements_digest.clear(),
            1 => bad.assignments.clear(),
            2 => bad
                .assignments
                .push(bad.assignments.first().unwrap().clone()),
            3 => bad.assignments.first_mut().unwrap().profile = "different".into(),
            4 => bad.assignments.first_mut().unwrap().lease_id = DeviceLeaseId::new().to_string(),
            5 => bad.assignments.first_mut().unwrap().parent_id.clear(),
            6 => bad.assignments.first_mut().unwrap().parent_id = DeviceParentId::new().to_string(),
            _ => unreachable!(),
        }
        validate_acknowledgement(
            &requirements,
            Some(&bad),
            Some(std::slice::from_ref(&assignment)),
        )
        .unwrap_err();
    }
}

#[test]
fn digest_matches_the_persisted_canonical_requirements() {
    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("compute".parse().unwrap(), 1).unwrap(),
    ])
    .unwrap();
    let alternative = DeviceRequirements::new(vec![
        DeviceRequirement::compatible(
            vec!["other".parse().unwrap(), "compute".parse().unwrap()],
            2,
        )
        .unwrap(),
    ])
    .unwrap();
    for requirements in [requirements, alternative] {
        let encoded = serde_json::to_string(&requirements).unwrap();
        let bytes = digest(
            &SHA256,
            format!("space.exclusive-device-requirements.v1\n{encoded}").as_bytes(),
        );
        assert_eq!(
            requirements_digest(&requirements).to_string(),
            format!(
                "sha256:{}",
                bytes
                    .as_ref()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )
        );
        let wire = proto::DeviceRequirements::from(&requirements);
        assert_eq!(DeviceRequirements::try_from(&wire).unwrap(), requirements);
    }
}

#[test]
fn guarded_allocation_rejects_ambiguous_identity_and_missing_requirements() {
    let sandbox = pplx_space_common_core::types::SandboxId::new();
    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("compute".parse().unwrap(), 1).unwrap(),
    ])
    .unwrap();
    let input = proto::DeviceAllocationInput {
        sandbox_id: sandbox.to_string(),
        requirements: Some((&requirements).into()),
    };
    assert_eq!(
        decode_allocation("", Some(&input)).unwrap(),
        (sandbox, Some(requirements))
    );
    assert_eq!(
        decode_allocation(&sandbox.to_string(), None).unwrap(),
        (sandbox, None)
    );
    for case in 0..5 {
        let mut input = input.clone();
        let mut legacy = String::new();
        match case {
            0 => legacy = sandbox.to_string(),
            1 => input.sandbox_id.clear(),
            2 => input.requirements = None,
            3 => input.requirements.as_mut().unwrap().entries.clear(),
            4 => input.requirements.as_mut().unwrap().entries[0].count = 0,
            _ => unreachable!(),
        }
        decode_allocation(&legacy, Some(&input)).unwrap_err();
    }
}

#[test]
fn device_execution_rejects_conflicting_evidence_and_normalizes_order() {
    let sandbox = pplx_space_common_core::types::SandboxId::new();
    let requirements = DeviceRequirements::new(vec![
        DeviceRequirement::new("compute".parse().unwrap(), 2).unwrap(),
    ])
    .unwrap();
    let input = proto::DeviceExecutionInput {
        sandbox_id: sandbox.to_string(),
        requirements_digest: requirements_digest(&requirements).to_string(),
        assignments: ["pci:0000:01:00.0", "pci:0000:02:00.0"]
            .into_iter()
            .map(|member| {
                (&ExclusiveDeviceAssignment::new(
                    DeviceLeaseId::new(),
                    DeviceParentId::new(),
                    "compute".parse().unwrap(),
                    vec![member.parse().unwrap()],
                )
                .unwrap())
                    .into()
            })
            .collect(),
    };
    let (_, parsed) = decode_execution("", Some(&input)).unwrap();
    let parsed = parsed.unwrap();
    let mut reordered = input.clone();
    reordered.assignments.reverse();
    assert_eq!(
        decode_execution("", Some(&reordered)).unwrap().1,
        Some(parsed.clone())
    );
    assert_eq!(
        decode_execution("", Some(&parsed.input(sandbox)))
            .unwrap()
            .1,
        Some(parsed.clone())
    );
    validate_acknowledgement(
        &requirements,
        Some(&parsed.acknowledgement()),
        Some(parsed.assignments()),
    )
    .unwrap();
    let mut ack = parsed.acknowledgement();
    ack.assignments.reverse();
    validate_execution_acknowledgement(Some(&parsed), Some(&ack)).unwrap();
    validate_execution_acknowledgement(None, None).unwrap();
    validate_execution_acknowledgement(None, Some(&ack)).unwrap_err();
    validate_execution_acknowledgement(Some(&parsed), None).unwrap_err();
    for case in 0..4 {
        let mut bad = ack.clone();
        match case {
            0 => bad.requirements_digest = format!("sha256:{}", "00".repeat(32)),
            1 => {
                bad.assignments.pop();
            }
            2 => bad.assignments[0].parent_id = DeviceParentId::new().to_string(),
            3 => bad.assignments[1] = bad.assignments[0].clone(),
            _ => unreachable!(),
        }
        validate_execution_acknowledgement(Some(&parsed), Some(&bad)).unwrap_err();
    }
    for case in 0..8 {
        let mut bad = input.clone();
        let mut legacy = String::new();
        match case {
            0 => legacy = sandbox.to_string(),
            1 => bad.requirements_digest.clear(),
            2 => bad.assignments.clear(),
            3 => bad.assignments[1].lease_id = bad.assignments[0].lease_id.clone(),
            4 => bad.assignments[1].parent_id = bad.assignments[0].parent_id.clone(),
            5 => bad.assignments[1].host_device_ids = bad.assignments[0].host_device_ids.clone(),
            6 => bad.assignments[0].host_device_ids.clear(),
            7 => bad.sandbox_id.clear(),
            _ => unreachable!(),
        }
        decode_execution(&legacy, Some(&bad)).unwrap_err();
    }
}
