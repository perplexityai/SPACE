use super::*;

#[test]
fn automatic_selection_is_forward_same_family_and_marker_driven() {
    let digest = model::ArtifactDigest::of_bytes(b"candidate");
    let mut policy = policy::Policy {
        authorization: Uuid::now_v7(),
        promotion: model::policy::Promotion {
            channel: "stable".into(),
            target: digest,
            target_version: "0.8.145".parse().unwrap(),
            guest_update_required_since: "0.8.143".parse().unwrap(),
            intent: model::policy::SelectionIntent::Upgrade,
            minimum_host: "0.8.1".parse().unwrap(),
            maximum_guest: "0.8.145".parse().unwrap(),
        },
    };
    for (guest, required) in [
        ("0.8.140", true),
        ("0.8.143", false),
        ("0.8.144", false),
        ("0.8.145", false),
        ("0.8.146", false),
        ("0.7.140", false),
        ("bad", false),
    ] {
        let seen = wire::Status {
            committed_version: guest.into(),
            committed: Some(
                model::Selection {
                    slot: model::Slot::A,
                    digest: model::ArtifactDigest::of_bytes(b"original"),
                }
                .into(),
            ),
            ..Default::default()
        };
        assert_eq!(automatic_target(&policy, &seen), required, "{guest}");
    }
    let seen = wire::Status {
        committed_version: "0.8.140".into(),
        committed: Some(
            model::Selection {
                slot: model::Slot::A,
                digest: model::ArtifactDigest::of_bytes(b"original"),
            }
            .into(),
        ),
        ..Default::default()
    };
    policy.promotion.maximum_guest = "0.8.146".parse().unwrap();
    assert!(!automatic_target(&policy, &seen));
    policy.promotion.maximum_guest = policy.promotion.target_version.clone();
    policy.promotion.intent = model::policy::SelectionIntent::Rollback;
    assert!(!automatic_target(&policy, &seen));
}

#[test]
fn target_cutoff_counts_distinct_sandboxes_and_resets_only_with_manager() {
    let automatic = Automatic::default();
    let mut record = Record {
        intent: Intent {
            operation: Uuid::now_v7(),
            sandbox: SandboxId::new(),
            execution: SandboxActivationAttemptId::new(),
            action: Action::Apply {
                promotion: Box::new(model::policy::Promotion {
                    channel: "stable".into(),
                    target: model::ArtifactDigest::of_bytes(b"bad"),
                    target_version: "0.8.2".parse().unwrap(),
                    guest_update_required_since: "0.8.2".parse().unwrap(),
                    intent: model::policy::SelectionIntent::Upgrade,
                    minimum_host: "0.8.1".parse().unwrap(),
                    maximum_guest: "0.8.2".parse().unwrap(),
                }),
                authorization: Uuid::now_v7(),
            },
            force: false,
            configuration_revision: 1,
        },
        phase: Phase::Pending,
        prior: None,
        observed: None,
        deadline: i64::MAX,
        outcome: None,
    };
    let target = match &record.intent.action {
        Action::Apply { promotion, .. } => promotion.target,
        _ => unreachable!(),
    };
    for outcome in [
        Outcome::Deferred,
        Outcome::AwaitingHost,
        Outcome::Cancelled,
        Outcome::Superseded,
        Outcome::Committed,
    ] {
        record.phase = Phase::Complete;
        record.outcome = Some(outcome);
        automatic.observe(&record);
    }
    assert!(!automatic.cut_off(target));
    record.outcome = Some(Outcome::CandidateFailed);
    record.phase = Phase::Settling;
    automatic.observe(&record);
    record.phase = Phase::Complete;
    automatic.observe(&record);
    automatic.observe(&record);
    assert!(!automatic.cut_off(target));
    // Repeated failures across executions and operations of one sandbox count once.
    for _ in 0..4 {
        record.intent.operation = Uuid::now_v7();
        record.intent.execution = SandboxActivationAttemptId::new();
        record.outcome = Some(Outcome::RetryExhausted);
        automatic.observe(&record);
        assert!(!automatic.cut_off(target));
    }
    record.intent.sandbox = SandboxId::new();
    automatic.observe(&record);
    assert!(!automatic.cut_off(target));
    record.intent.sandbox = SandboxId::new();
    record.outcome = Some(Outcome::RecoveryRequired);
    automatic.observe(&record);
    assert!(automatic.cut_off(target));
    automatic.activation(&record.intent.sandbox.to_string());
    assert!(
        automatic.cut_off(target),
        "deletion or execution replacement cannot replenish the node cutoff"
    );
    assert!(!automatic.cut_off(model::ArtifactDigest::of_bytes(b"next-target")));
    assert!(!Automatic::default().cut_off(target));
}

#[tokio::test(start_paused = true)]
async fn discovery_backoff_and_activation_do_not_reuse_cached_eligibility() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let episode = crate::db::test_fixtures::state_write_fixtures::begin_cold_start(&conn, &id);
    assert!(crate::db::mark_start_succeeded(&conn, &id, Some(episode.id)).unwrap());
    let mut row = crate::db::read::sandbox(&conn, &id).unwrap().unwrap();
    let automatic = Automatic::default();
    assert!(customer(&row));
    assert!(!automatic.due(&row, None));
    tokio::time::advance(Duration::from_secs(121)).await;
    assert!(automatic.due(&row, None));
    assert_eq!(automatic.quiet_remaining(&row), 0);
    for (reason, seconds) in [
        (Defer::Quiet, Some(120)),
        (Defer::Active, Some(60)),
        (Defer::Busy, None),
        (Defer::Lifecycle, None),
        (Defer::Disk, None),
        (Defer::Unavailable, None),
    ] {
        automatic.defer(&id, reason, seconds);
        tokio::time::advance(Duration::from_secs(seconds.unwrap_or(900) - 1)).await;
        assert!(!automatic.due(&row, None));
        tokio::time::advance(Duration::from_secs(62)).await;
        assert!(automatic.due(&row, None));
    }
    for reason in [
        Defer::NoTarget,
        Defer::Unsupported,
        Defer::NotRequired,
        Defer::Cutoff,
    ] {
        automatic.defer(&id, reason, None);
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert!(!automatic.due(&row, None));
        automatic.activation(&id);
        assert!(!automatic.due(&row, None));
        tokio::time::advance(Duration::from_secs(121)).await;
        assert!(automatic.due(&row, None));
    }
    automatic.defer(&id, Defer::NoTarget, None);
    let installed = Some(model::ArtifactDigest::of_bytes(
        b"new-qualified-catalog-entry",
    ));
    assert!(!automatic.due(&row, installed));
    tokio::time::advance(Duration::from_secs(121)).await;
    assert!(
        automatic.due(&row, installed),
        "catalog change clears negative eligibility cache"
    );
    row.auto_lifecycle_state.last_access_at = Some(1000);
    row.auto_lifecycle_state.last_command_at = Some(1010);
    assert_eq!(quiet_remaining(&row, 1100), 30);
    assert_eq!(quiet_remaining(&row, 1130), 0);
    assert_eq!(row.auto_lifecycle_state.last_command_at, Some(1010));
    for state in [
        SandboxState::Paused,
        SandboxState::Suspended,
        SandboxState::Stopped,
        SandboxState::Starting,
        SandboxState::Stopping,
    ] {
        row.state = state.into();
        assert!(!customer(&row), "{state}");
    }
}

#[test]
fn recovery_preserves_known_manual_origin_until_completion() {
    let automatic = Automatic::default();
    let manual = Uuid::now_v7();
    let scheduled = Uuid::now_v7();
    assert!(
        automatic.requires_quiet(manual),
        "pre-restart receipts are conservative"
    );
    automatic.remember_origin(manual, false).unwrap();
    automatic.remember_origin(scheduled, true).unwrap();
    assert!(!automatic.requires_quiet(manual));
    assert!(automatic.requires_quiet(scheduled));
    assert!(
        automatic.origin_operations().is_empty(),
        "receipt scan cannot prune an admission not yet persisted"
    );
    automatic.admitted(manual);
    assert!(automatic.origin_operations().contains(&manual));
    automatic.remember_origin(manual, true).unwrap();
    assert!(
        !automatic.requires_quiet(manual),
        "recovery cannot relabel a manual intent"
    );
    automatic.forget_origin(manual);
    assert!(automatic.requires_quiet(manual));
}
