use super::*;
use journal::{JournalError, JournalState};
use policy::Promotion;
use uuid::Uuid;

fn selection(slot: Slot, bytes: &[u8]) -> Selection {
    Selection {
        slot,
        digest: ArtifactDigest::of_bytes(bytes),
    }
}
fn attempt(boot: Uuid) -> Attempt {
    Attempt {
        operation: Uuid::now_v7(),
        execution: Uuid::now_v7(),
        boot,
        incarnation: Uuid::now_v7(),
        configuration_revision: 3,
        target: selection(Slot::B, b"candidate"),
        confirmation_deadline_ms: 100,
    }
}

#[test]
fn one_attempt_and_durable_commit_survive_every_start_boundary() {
    let boot = Uuid::now_v7();
    let attempt = attempt(boot);
    let committed = selection(Slot::A, b"old");
    let states = [
        JournalState::Stable,
        JournalState::Staged {
            attempt: attempt.clone(),
        },
        JournalState::Armed {
            attempt: attempt.clone(),
        },
        JournalState::Trial {
            attempt: attempt.clone(),
            progress: TrialProgress::Starting,
        },
        JournalState::Trial {
            attempt: attempt.clone(),
            progress: TrialProgress::AwaitingHost,
        },
        JournalState::Committed {
            attempt: attempt.clone(),
        },
        JournalState::RolledBack {
            attempt: attempt.clone(),
            progress: TrialProgress::AwaitingHost,
        },
        JournalState::RecoveryRequired,
        JournalState::Restarting {
            restart: journal::Restart {
                operation: Uuid::now_v7(),
                execution: Uuid::now_v7(),
                boot,
                incarnation: Uuid::now_v7(),
            },
        },
        JournalState::Restarted {
            restart: journal::Restart {
                operation: Uuid::now_v7(),
                execution: Uuid::now_v7(),
                boot,
                incarnation: Uuid::now_v7(),
            },
        },
    ];
    for state in states {
        let expected = match &state {
            JournalState::Armed { .. } | JournalState::Committed { .. } => attempt.target.clone(),
            JournalState::Stable
            | JournalState::Staged { .. }
            | JournalState::Trial { .. }
            | JournalState::RolledBack { .. }
            | JournalState::RecoveryRequired
            | JournalState::Restarting { .. }
            | JournalState::Restarted { .. } => committed.clone(),
        };
        let is_committed = matches!(state, JournalState::Committed { .. });
        let mut journal = Journal {
            schema: 1,
            committed: if is_committed {
                attempt.target.clone()
            } else {
                committed.clone()
            },
            state,
        };
        assert_eq!(journal.select(boot, 10).unwrap(), expected);
        // A second startup never executes an uncommitted candidate again.
        assert_eq!(
            journal.select(boot, 11).unwrap(),
            if is_committed {
                attempt.target.clone()
            } else {
                committed.clone()
            }
        );
    }
    for (current_boot, now) in [(Uuid::now_v7(), 10), (boot, 100)] {
        let mut journal = Journal {
            schema: 1,
            committed: committed.clone(),
            state: JournalState::Armed {
                attempt: attempt.clone(),
            },
        };
        assert_eq!(journal.select(current_boot, now).unwrap(), committed);
    }
}

#[test]
fn commit_requires_self_test_live_fence_and_deadline() {
    let boot = Uuid::now_v7();
    let attempt = attempt(boot);
    let mut journal = Journal {
        schema: 1,
        committed: selection(Slot::A, b"old"),
        state: JournalState::Trial {
            attempt: attempt.clone(),
            progress: TrialProgress::Starting,
        },
    };
    assert!(matches!(
        journal.commit(&attempt, boot, 10),
        Err(JournalError::Phase)
    ));
    journal.self_test_passed(&attempt).unwrap();
    let mut stale = attempt.clone();
    stale.incarnation = Uuid::now_v7();
    assert!(matches!(
        journal.commit(&stale, boot, 10),
        Err(JournalError::Fence)
    ));
    assert!(matches!(
        journal.commit(&attempt, Uuid::now_v7(), 10),
        Err(JournalError::Fence)
    ));
    assert!(matches!(
        journal.commit(&attempt, boot, 100),
        Err(JournalError::Expired)
    ));
    journal.commit(&attempt, boot, 99).unwrap();
    journal.commit(&attempt, boot, 101).unwrap(); // Lost reply observes the same commit.
    assert_eq!(journal.select(Uuid::now_v7(), 0).unwrap(), attempt.target);
}

#[test]
fn workspace_release_marker_skips_unnecessary_restarts_and_fences_rollback() {
    use policy::SelectionIntent;
    let target = Promotion {
        channel: "stable".into(),
        target: ArtifactDigest::of_bytes(b"0.8.145"),
        target_version: "0.8.145".parse().unwrap(),
        guest_update_required_since: "0.8.143".parse().unwrap(),
        intent: SelectionIntent::Upgrade,
        minimum_host: "0.8.143".parse().unwrap(),
        maximum_guest: "0.8.200".parse().unwrap(),
    };
    for (guest, expected) in [
        ("0.8.140", true),
        ("0.8.142", true),
        ("0.8.143", false),
        ("0.8.144", false),
        ("0.8.146", false),
    ] {
        assert_eq!(
            target
                .should_install(
                    &guest.parse().unwrap(),
                    ArtifactDigest::of_bytes(guest.as_bytes())
                )
                .unwrap(),
            expected
        );
    }
    assert!(
        !target
            .should_install(&target.target_version, target.target)
            .unwrap()
    );
    let Err(_) = target.should_install(
        &target.target_version,
        ArtifactDigest::of_bytes(b"different build"),
    ) else {
        panic!("operation must be rejected");
    };
    let next = Promotion {
        target_version: "0.8.146".parse().unwrap(),
        guest_update_required_since: "0.8.146".parse().unwrap(),
        target: ArtifactDigest::of_bytes(b"fix"),
        ..target.clone()
    };
    next.accept_after(Some(&target)).unwrap();
    let Err(_) = target.accept_after(Some(&next)) else {
        panic!("operation must be rejected");
    };
    let rollback = Promotion {
        intent: SelectionIntent::Rollback,
        ..target.clone()
    };
    rollback.accept_after(Some(&next)).unwrap();
    assert!(
        rollback
            .should_install(&next.target_version, next.target)
            .unwrap()
    );
    assert!(
        !rollback
            .should_install(
                &"0.8.140".parse().unwrap(),
                ArtifactDigest::of_bytes(b"older")
            )
            .unwrap()
    );
    let Err(_) = Promotion {
        target: next.target,
        ..target.clone()
    }
    .accept_after(Some(&target)) else {
        panic!("operation must be rejected");
    };
    let Err(_) = Promotion {
        guest_update_required_since: "0.8.147".parse().unwrap(),
        ..next
    }
    .accept_after(Some(&target)) else {
        panic!("operation must be rejected");
    };
}

#[test]
fn descriptor_identity_is_canonical_and_excludes_promotion() {
    let binary = BinaryIdentity {
        length: 1,
        digest: ArtifactDigest::of_bytes(b"x"),
    };
    let descriptor = BundleDescriptor {
        schema: 1,
        source_revision: "a".repeat(40),
        built_at: "2026-01-01T00:00:00Z".into(),
        version: "0.8.1".parse().unwrap(),
        guest_update_required_since: "0.8.1".parse().unwrap(),
        compatibility_family: "0.8".into(),
        architecture: pplx_space_cpu_arch::Arch::X86_64,
        seed_contracts: vec![SEED_CONTRACT.into()],
        service: binary.clone(),
        cli: binary,
    };
    let pretty = serde_json::to_vec_pretty(&descriptor).unwrap();
    assert_eq!(
        BundleDescriptor::parse(&descriptor.canonical_bytes().unwrap())
            .unwrap()
            .digest()
            .unwrap(),
        descriptor.digest().unwrap()
    );
    assert!(BundleDescriptor::parse(&pretty).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&pretty).unwrap();
    value["authorization"] = 9.into();
    let Err(_) = BundleDescriptor::parse(&serde_json::to_vec(&value).unwrap()) else {
        panic!("operation must be rejected");
    };
    let mut malformed = descriptor.clone();
    malformed.service.length = MAX_BINARY_BYTES + 1;
    let Err(_) = malformed.canonical_bytes() else {
        panic!("operation must be rejected");
    };
    let Err(_) = "blake3:0000000000000000000000000000000000000000000000000000000000000000"
        .parse::<ArtifactDigest>()
    else {
        panic!("operation must be rejected");
    };
}

#[test]
fn publisher_bytes_are_the_descriptor_identity() {
    let directory = tempfile::tempdir().unwrap();
    for name in ["spaced", "spaced-svc"] {
        std::fs::write(directory.path().join(name), b"synthetic agent").unwrap();
    }
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../scripts/build/agent-bundle.py");
    let output = std::process::Command::new("python3")
        .arg(script)
        .arg(directory.path())
        .args([
            "--revision",
            &"a".repeat(40),
            "--built-at",
            "2026-01-01T00:00:00Z",
            "--version",
            "0.8.1",
            "--guest-update-required-since",
            "0.8.1",
            "--arch",
            "x86_64",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let bytes = std::fs::read(directory.path().join("artifact.json")).unwrap();
    let descriptor = BundleDescriptor::parse(&bytes).unwrap();
    assert_eq!(
        ArtifactDigest::of_bytes(&bytes),
        descriptor.digest().unwrap()
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        descriptor.digest().unwrap().to_string()
    );
    let alternate = serde_json::to_vec_pretty(&descriptor).unwrap();
    assert!(
        BundleDescriptor::parse(&alternate).is_err(),
        "different bytes must not silently acquire a canonical digest"
    );
}
