use super::*;

#[test]
fn activation_attempt_allocator_advances_past_a_foreign_replica_value() {
    let previous = SandboxActivationAttemptId::from_uuid(
        uuid::Uuid::parse_str("ffffffff-fffe-7fff-bfff-fffffffffff0").unwrap(),
    );
    let successor = SandboxActivationAttemptId::new_after(previous).unwrap();

    assert!(successor > previous);
    let another = SandboxActivationAttemptId::new_after(previous).unwrap();
    assert!(another > previous);
    assert_ne!(
        successor, another,
        "a retained route is not an allocation counter"
    );
    assert_eq!(successor.as_uuid().get_version_num(), 7);
    let exhausted: SandboxActivationAttemptId =
        "ffffffff-ffff-7fff-bfff-ffffffffffff".parse().unwrap();
    assert!(SandboxActivationAttemptId::new_after(exhausted).is_none());
}

#[test]
fn quarantine_reason_codes_round_trip_through_the_wire_vocabulary() {
    for reason in [
        SandboxQuarantineReasonCode::Unspecified,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
        SandboxQuarantineReasonCode::CreateInterrupted,
        SandboxQuarantineReasonCode::ColdActivationInterrupted,
        SandboxQuarantineReasonCode::RestoreActivationInterrupted,
        SandboxQuarantineReasonCode::ColdActivationPrepareFailed,
        SandboxQuarantineReasonCode::ColdActivationMetadataFailed,
        SandboxQuarantineReasonCode::ColdActivationEncryptedRootfsPrepareFailed,
        SandboxQuarantineReasonCode::ColdActivationFirecrackerLaunchFailed,
        SandboxQuarantineReasonCode::ColdActivationSidecarLaunchFailed,
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        SandboxQuarantineReasonCode::RestoreMetadataFailed,
        SandboxQuarantineReasonCode::RestoreEncryptedDevicesPrepareFailed,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
        SandboxQuarantineReasonCode::RestoreNetGatewayLaunchFailed,
        SandboxQuarantineReasonCode::RestoreSnapshotLoadFailed,
        SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
        SandboxQuarantineReasonCode::RestoreResumeFailed,
        SandboxQuarantineReasonCode::PauseFailed,
        SandboxQuarantineReasonCode::CheckpointResumeFailed,
        SandboxQuarantineReasonCode::ResidentResumeFailed,
        SandboxQuarantineReasonCode::SnapshotRollbackCleanupFailed,
        SandboxQuarantineReasonCode::NodePublishedQuarantined,
        SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact,
        SandboxQuarantineReasonCode::OrphanedExecutionLegacyDeletingState,
    ] {
        assert_eq!(reason.as_str().parse(), Ok(reason));
    }
}

#[test]
fn direct_failure_quarantine_reasons_have_no_automatic_recovery() {
    for reason in [
        SandboxQuarantineReasonCode::PauseFailed,
        SandboxQuarantineReasonCode::CheckpointResumeFailed,
        SandboxQuarantineReasonCode::ResidentResumeFailed,
        SandboxQuarantineReasonCode::SnapshotRollbackCleanupFailed,
    ] {
        assert_eq!(reason.automatic_recovery_policy(), None);
    }
}

#[test]
fn execution_generation_accepts_only_positive_values() {
    assert_eq!(ExecutionGeneration::new(1).unwrap().get(), 1);
    assert_eq!(ExecutionGeneration::try_from("7").unwrap().get(), 7);
    ExecutionGeneration::new(0).unwrap_err();
    ExecutionGeneration::try_from("-1").unwrap_err();
}

#[test]
fn route_generation_accepts_only_positive_values() {
    assert_eq!(RouteGeneration::new(1).unwrap().get(), 1);
    assert_eq!(
        RouteGeneration::new(i64::MAX as u64).unwrap().get(),
        i64::MAX as u64
    );
    assert_eq!(RouteGeneration::try_from("7").unwrap().get(), 7);
    RouteGeneration::new(0).unwrap_err();
    RouteGeneration::new(i64::MAX as u64 + 1).unwrap_err();
    RouteGeneration::try_from("").unwrap_err();
}
#[test]
fn strict_route_identity_enforces_substrate_pod_uid_profile() {
    let sandbox_id = SandboxId::new();
    let route_generation = RouteGeneration::new(1).unwrap();
    let node_id = NodeId::new();
    let execution_generation = ExecutionGeneration::new(1).unwrap();
    let activation_attempt_id = SandboxActivationAttemptId::new();

    assert_eq!(
        SandboxRouteIdentity::try_new(
            sandbox_id,
            route_generation,
            node_id,
            NodeManagedBy::Kubernetes,
            execution_generation,
            activation_attempt_id,
            None,
        ),
        Err(SandboxRouteIdentityInvariantError::MissingPodUid)
    );
    assert_eq!(
        SandboxRouteIdentity::try_new(
            sandbox_id,
            route_generation,
            node_id,
            NodeManagedBy::Space,
            execution_generation,
            activation_attempt_id,
            Some(PodUid::parse("pod-uid").unwrap()),
        ),
        Err(SandboxRouteIdentityInvariantError::UnexpectedPodUid)
    );
}

#[cfg(feature = "serde")]
#[test]
fn execution_generation_deserialization_enforces_the_invariant() {
    let generation: ExecutionGeneration = serde_json::from_str("9").unwrap();
    assert_eq!(generation.get(), 9);
    serde_json::from_str::<ExecutionGeneration>("0").unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn route_generation_deserialization_enforces_the_invariant() {
    let generation: RouteGeneration = serde_json::from_str("9").unwrap();
    assert_eq!(generation.get(), 9);
    serde_json::from_str::<RouteGeneration>("0").unwrap_err();
    serde_json::from_str::<RouteGeneration>(&((i64::MAX as u64) + 1).to_string()).unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_route_identity_deserialization_rejects_kubernetes_without_pod_uid() {
    let identity = SandboxRouteIdentity::try_new(
        SandboxId::new(),
        RouteGeneration::new(1).unwrap(),
        NodeId::new(),
        NodeManagedBy::Kubernetes,
        ExecutionGeneration::new(1).unwrap(),
        SandboxActivationAttemptId::new(),
        Some(PodUid::parse("pod-uid").unwrap()),
    )
    .unwrap();
    let mut json = serde_json::to_value(identity).unwrap();
    json.as_object_mut().unwrap().remove("pod_uid");

    let error = serde_json::from_value::<SandboxRouteIdentity>(json)
        .expect_err("Kubernetes route identity without pod_uid must reject");

    assert!(error.to_string().contains("requires pod_uid"));
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_route_identity_deserialization_rejects_direct_with_pod_uid() {
    let identity = SandboxRouteIdentity::try_new(
        SandboxId::new(),
        RouteGeneration::new(1).unwrap(),
        NodeId::new(),
        NodeManagedBy::Space,
        ExecutionGeneration::new(1).unwrap(),
        SandboxActivationAttemptId::new(),
        None,
    )
    .unwrap();
    let mut json = serde_json::to_value(identity).unwrap();
    json.as_object_mut()
        .unwrap()
        .insert("pod_uid".to_string(), serde_json::json!("pod-uid"));

    let error = serde_json::from_value::<SandboxRouteIdentity>(json)
        .expect_err("Direct route identity with pod_uid must reject");

    assert!(error.to_string().contains("must not contain pod_uid"));
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_route_identity_valid_profiles_round_trip() {
    let sandbox_id = SandboxId::new();
    let route_generation = RouteGeneration::new(1).unwrap();
    let node_id = NodeId::new();
    let execution_generation = ExecutionGeneration::new(1).unwrap();
    let activation_attempt_id = SandboxActivationAttemptId::new();
    for identity in [
        SandboxRouteIdentity::try_new(
            sandbox_id,
            route_generation,
            node_id,
            NodeManagedBy::Kubernetes,
            execution_generation,
            activation_attempt_id,
            Some(PodUid::parse("pod-uid").unwrap()),
        )
        .unwrap(),
        SandboxRouteIdentity::try_new(
            sandbox_id,
            route_generation,
            node_id,
            NodeManagedBy::Space,
            execution_generation,
            activation_attempt_id,
            None,
        )
        .unwrap(),
    ] {
        let json = serde_json::to_string(&identity).unwrap();
        assert_eq!(
            serde_json::from_str::<SandboxRouteIdentity>(&json).unwrap(),
            identity
        );
    }
}

#[test]
fn sandbox_selector_normalizes_and_rejects_short_values() {
    assert_eq!(SandboxSelector::parse("AB-cd").unwrap().as_str(), "abcd");
    assert!(matches!(
        SandboxSelector::parse("abc"),
        Err(SandboxSelectorParseError::TooShort)
    ));
}

#[test]
fn sandbox_label_uses_uuid_suffix() {
    let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap();
    assert_eq!(id.label().to_string(), "77f3a3a2");
}

#[test]
fn sandbox_runtime_label_accepts_uuid_suffixes_and_slot_labels() {
    let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap();
    assert_eq!(
        SandboxRuntimeLabel::from_sandbox_id(id).as_str(),
        "77f3a3a2"
    );
    assert_eq!(
        SandboxRuntimeLabel::parse("000017").unwrap().as_str(),
        "000017"
    );
    let max_slot_label = format!(
        "{MAX_LINUX_NETWORK_SLOT:0width$}",
        width = LINUX_NETWORK_SLOT_LABEL_WIDTH
    );
    assert_eq!(
        SandboxRuntimeLabel::parse(max_slot_label.clone())
            .unwrap()
            .as_str(),
        max_slot_label
    );
}

#[test]
fn sandbox_runtime_label_rejects_pathlike_or_oversized_values() {
    SandboxRuntimeLabel::parse("").unwrap_err();
    SandboxRuntimeLabel::parse("uuid/123").unwrap_err();
    SandboxRuntimeLabel::parse("0123456789").unwrap_err();
}

#[test]
fn sandbox_selector_matches_label_or_uuid_prefix() {
    let id = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap();

    assert!(SandboxSelector::parse("77f3").unwrap().matches_id(id));
    assert!(SandboxSelector::parse("77f3a3a2").unwrap().matches_id(id));
    assert!(
        SandboxSelector::parse("019d84bd-280d")
            .unwrap()
            .matches_id(id)
    );
    assert!(!SandboxSelector::parse("019d").unwrap().matches_id(id));
}

#[test]
fn sandbox_selector_finds_zero_one_or_many_candidates() {
    #[derive(Debug, Eq, PartialEq)]
    struct Candidate {
        id: SandboxId,
        name: &'static str,
    }

    let first = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap();
    let second = SandboxId::from_str("019d84bd-280d-7331-99c6-b63377f3b4b5").unwrap();
    let other = SandboxId::from_str("019d84bd-280d-7331-99c6-b63300112233").unwrap();
    let candidates = [
        Candidate {
            id: first,
            name: "first",
        },
        Candidate {
            id: second,
            name: "second",
        },
        Candidate {
            id: other,
            name: "other",
        },
    ];

    let one = SandboxSelector::parse("0011").unwrap();
    assert!(matches!(
        one.find(candidates.iter(), |candidate| candidate.id),
        Some(OneOrMany::One(candidate)) if candidate.name == "other"
    ));

    let many = SandboxSelector::parse("77f3").unwrap();
    assert!(matches!(
        many.find(candidates.iter(), |candidate| candidate.id),
        Some(OneOrMany::Many(candidates)) if candidates.len() == 2
    ));

    let none = SandboxSelector::parse("ffff").unwrap();
    assert_eq!(none.find(candidates.iter(), |candidate| candidate.id), None);
}

#[test]
fn pii_sandbox_name_debug_is_redacted() {
    let name = SandboxName::parse("customer@example.com").unwrap();
    assert_eq!(format!("{name:?}"), "SandboxName(<redacted>)");
}

#[test]
fn metadata_key_debug_is_redacted() {
    let key = SandboxMetadataKey::parse("customer@example.com").unwrap();
    assert_eq!(format!("{key:?}"), "SandboxMetadataKey(<redacted>)");
}

#[test]
fn metadata_value_string_uses_validated_newtype() {
    let SandboxMetadataValue::String(value) = SandboxMetadataValue::string("ok").unwrap() else {
        panic!("expected string metadata");
    };
    assert_eq!(value.expose(), "ok");
    SandboxMetadataString::parse("x".repeat(4097)).unwrap_err();
    assert_eq!(format!("{value:?}"), "SandboxMetadataString(<redacted>)");
    assert_eq!(
        format!("{:?}", SandboxMetadataValue::Integer(7)),
        "SandboxMetadataValue(<redacted>)"
    );
}

#[test]
fn metadata_map_enforces_total_size() {
    let mut values = BTreeMap::new();
    for idx in 0..SandboxMetadata::MAX_ENTRIES {
        values.insert(
            SandboxMetadataKey::parse(format!("k{idx}")).unwrap(),
            SandboxMetadataValue::string("x".repeat(4096)).unwrap(),
        );
    }
    SandboxMetadata::new(values).unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn metadata_value_deserialization_checks_string_variant() {
    let value: SandboxMetadataValue = serde_json::from_str(r#"{"String":"ok"}"#).unwrap();
    let SandboxMetadataValue::String(value) = value else {
        panic!("expected string metadata");
    };
    assert_eq!(value.expose(), "ok");

    let too_long = format!(r#"{{"String":"{}"}}"#, "x".repeat(4097));
    serde_json::from_str::<SandboxMetadataValue>(&too_long).unwrap_err();
    serde_json::from_str::<SandboxMetadataValue>(r#"{"String":"bad\u0000value"}"#).unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn metadata_map_deserialization_checks_map_bounds() {
    let metadata: SandboxMetadata =
        serde_json::from_str(r#"{"ticket":{"String":"S-123"}}"#).unwrap();
    assert_eq!(metadata.expose().len(), 1);

    let mut json = String::from("{");
    for idx in 0..=SandboxMetadata::MAX_ENTRIES {
        if idx > 0 {
            json.push(',');
        }
        json.push_str(&format!(r#""k{idx}":{{"Bool":true}}"#));
    }
    json.push('}');
    serde_json::from_str::<SandboxMetadata>(&json).unwrap_err();
}

/// What can and cannot be checked about [`SandboxLifecycle::all`].
///
/// **Can:** that every entry is a distinct variant and round-trips through
/// `as_str`/`from_str`, so a typo or a duplicate in the list fails here. And —
/// the useful part — that adding a variant is a *compile* error in `tag`'s
/// exhaustive `match` below. (`counted_by` in the cluster-controller's
/// `termination_gate_tests` is a second such tripwire, and the more useful one:
/// it forces the new lifecycle to be classified against both node-release
/// gates.)
///
/// **Cannot:** force `all()` to grow. Adding the match arm and forgetting the
/// list compiles and passes, because `VARIANTS` is itself hand-written and there
/// is no way to derive the count without a macro (no `strum` in this workspace).
/// An earlier version of this test claimed to close that direction; it did not,
/// and the claim was worse than the gap because it invited trust. If `strum`
/// ever lands, `assert_eq!(all().len(), SandboxLifecycle::COUNT)` closes it for
/// real.
#[test]
fn all_lists_every_variant() {
    use SandboxLifecycle as L;

    // Exhaustive, no wildcard: a new variant stops the build right here. Add the
    // arm, then add the variant to `all()` and bump `VARIANTS` — nothing below
    // will remind you, so do it now.
    fn tag(lifecycle: SandboxLifecycle) -> u8 {
        match lifecycle {
            L::Created => 0,
            L::Creating => 1,
            L::Starting => 2,
            L::Started => 3,
            L::Restoring => 4,
            L::Pausing => 5,
            L::Paused => 6,
            L::Resuming => 7,
            L::Suspending => 8,
            L::Suspended => 9,
            L::Stopping => 10,
            L::Stopped => 11,
            L::Deleting => 12,
            L::Checkpointing => 13,
            L::Quarantined => 14,
            L::Failed => 15,
        }
    }
    const VARIANTS: usize = 16;

    let tags: std::collections::HashSet<u8> =
        SandboxLifecycle::all().iter().copied().map(tag).collect();
    assert_eq!(
        tags.len(),
        SandboxLifecycle::all().len(),
        "all() must not repeat a variant"
    );
    assert_eq!(
        SandboxLifecycle::all().len(),
        VARIANTS,
        "all() and VARIANTS disagree — one of the two hand-written lists was \
         updated without the other"
    );

    // Every entry survives the wire round-trip, so a wrong entry in `all()`
    // (a variant whose `as_str` does not parse back) fails here rather than in
    // whatever consumes the list.
    for lifecycle in SandboxLifecycle::all() {
        assert_eq!(
            lifecycle.as_str().parse::<SandboxLifecycle>().ok(),
            Some(*lifecycle),
            "{lifecycle} does not round-trip through as_str/from_str"
        );
    }
}
