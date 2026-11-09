use super::*;
use pplx_space_spaced_update::{BinaryIdentity, SEED_CONTRACT, TrialProgress};
use std::os::unix::fs::PermissionsExt;

fn fixture() -> (tempfile::TempDir, SlotStore, BundleDescriptor) {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("slots")).unwrap();
    let store = SlotStore::open(dir.path()).unwrap();
    let descriptor = bundle(&store, Slot::A, b"old-agent");
    store.initialize(descriptor.digest().unwrap()).unwrap();
    (dir, store, descriptor)
}

fn bundle(store: &SlotStore, slot: Slot, bytes: &[u8]) -> BundleDescriptor {
    let path = store.slot_path(slot);
    fs::create_dir_all(&path).unwrap();
    for binary in Binary::ALL {
        fs::write(path.join(binary.file_name()), bytes).unwrap();
        fs::set_permissions(
            path.join(binary.file_name()),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }
    let identity = BinaryIdentity {
        length: bytes.len() as u64,
        digest: ArtifactDigest::of_bytes(bytes),
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
        service: identity.clone(),
        cli: identity,
    };
    fs::write(
        path.join("artifact.json"),
        descriptor.canonical_bytes().unwrap(),
    )
    .unwrap();
    descriptor
}

fn arm(store: &SlotStore, boot: Uuid) -> Attempt {
    let descriptor = bundle(store, Slot::B, b"candidate");
    let attempt = Attempt {
        operation: Uuid::now_v7(),
        execution: Uuid::now_v7(),
        boot,
        incarnation: Uuid::now_v7(),
        configuration_revision: 1,
        target: Selection {
            slot: Slot::B,
            digest: descriptor.digest().unwrap(),
        },
        confirmation_deadline_ms: 100,
    };
    let mut journal = store.journal().unwrap();
    journal.state = JournalState::Staged {
        attempt: attempt.clone(),
    };
    store.write_journal(&journal).unwrap();
    store.arm(&attempt).unwrap();
    attempt
}

#[test]
fn stable_start_does_not_require_writable_metadata() {
    if !pplx_space_test_env::unprivileged_or_skip("read-only metadata permissions") {
        return;
    }
    use std::os::unix::fs::MetadataExt;
    let (dir, store, descriptor) = fixture();
    drop(store);
    let paths = ["update-state", "committed", "current", "update-lock"];
    let before: Vec<_> = paths
        .iter()
        .map(|name| fs::symlink_metadata(dir.path().join(name)).unwrap().ino())
        .collect();
    for name in ["update-lock", "update-state"] {
        fs::set_permissions(dir.path().join(name), fs::Permissions::from_mode(0o400)).unwrap();
    }
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).unwrap();
    let result = SlotStore::open(dir.path()).and_then(|store| store.select(Uuid::now_v7(), 0));
    // Restore permissions even on failure so the disposable fixture is removed.
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(result.unwrap().digest, descriptor.digest().unwrap());
    let after: Vec<_> = paths
        .iter()
        .map(|name| fs::symlink_metadata(dir.path().join(name)).unwrap().ino())
        .collect();
    assert_eq!(
        before, after,
        "stable startup must not replace durable metadata"
    );
}

#[test]
fn restart_recovers_disk_decisions_not_stale_links() {
    for interruption in ["trial", "committed_record", "committed_link", "all_links"] {
        let (_dir, store, old) = fixture();
        let boot = Uuid::now_v7();
        let attempt = arm(&store, boot);
        assert_eq!(store.select(boot, 1).unwrap(), attempt.target);
        if interruption != "trial" {
            let mut journal = store.journal().unwrap();
            journal.self_test_passed(&attempt).unwrap();
            journal.commit(&attempt, boot, 2).unwrap();
            store.write_journal(&journal).unwrap();
            if interruption != "committed_record" {
                store.set_link("committed", Slot::B).unwrap();
            }
            if interruption == "all_links" {
                store.set_link("current", Slot::B).unwrap();
            }
        }
        let recovered = store.select(Uuid::now_v7(), 0).unwrap();
        assert_eq!(
            recovered.digest,
            if interruption == "trial" {
                old.digest().unwrap()
            } else {
                attempt.target.digest
            }
        );
        assert_eq!(store.link_slot("current").unwrap(), recovered.slot);
        assert_eq!(store.link_slot("committed").unwrap(), recovered.slot);
    }
}

#[test]
fn corrupt_journal_falls_back_but_corrupt_fallback_never_executes() {
    let (dir, store, old) = fixture();
    fs::write(dir.path().join("update-state"), b"broken").unwrap();
    store.set_link("current", Slot::B).unwrap();
    assert_eq!(
        store.select(Uuid::now_v7(), 0).unwrap().digest,
        old.digest().unwrap()
    );
    assert!(matches!(
        store.journal().unwrap().state,
        JournalState::RecoveryRequired
    ));
    fs::write(store.slot_path(Slot::A).join("spaced-svc"), b"corrupt").unwrap();
    let Err(_) = store.select(Uuid::now_v7(), 0) else {
        panic!("operation must be rejected");
    };
}

#[test]
fn additive_journal_metadata_preserves_a_durable_commit_before_link_repair() {
    let (dir, store, _) = fixture();
    let boot = Uuid::now_v7();
    let attempt = arm(&store, boot);
    store.select(boot, 1).unwrap();
    let mut journal = store.journal().unwrap();
    journal.self_test_passed(&attempt).unwrap();
    journal.commit(&attempt, boot, 2).unwrap();
    let mut value = serde_json::to_value(journal).unwrap();
    value["diagnostic"] = "new optional metadata".into();
    value["state"]["attempt"]["diagnostic"] = "new optional metadata".into();
    fs::write(
        dir.path().join("update-state"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    assert_eq!(store.select(boot, 3).unwrap(), attempt.target);
}

#[test]
fn journal_read_errors_do_not_replace_durable_authority() {
    let (dir, store, _) = fixture();
    fs::rename(
        dir.path().join("update-state"),
        dir.path().join("saved-journal"),
    )
    .unwrap();
    std::os::unix::fs::symlink("saved-journal", dir.path().join("update-state")).unwrap();
    assert!(matches!(
        store.select(Uuid::now_v7(), 0),
        Err(UpdateError::Io(_))
    ));
    assert!(
        fs::symlink_metadata(dir.path().join("update-state"))
            .unwrap()
            .is_symlink()
    );
}

#[test]
fn consumed_attempt_cannot_be_staged_again() {
    for progress in [TrialProgress::Starting, TrialProgress::AwaitingHost] {
        let (_dir, store, _) = fixture();
        let boot = Uuid::now_v7();
        let attempt = arm(&store, boot);
        store.select(boot, 1).unwrap();
        // Model the fence recorded by the candidate after adoption. This test
        // process is not the candidate executable, so Linux inode checks preclude
        // invoking adopt_trial from the fixture itself.
        let mut adopted = store.journal().unwrap();
        let JournalState::Trial {
            attempt: fence,
            progress: recorded_progress,
        } = &mut adopted.state
        else {
            panic!("selector must start a trial");
        };
        fence.incarnation = Uuid::now_v7();
        *recorded_progress = progress;
        store.write_journal(&adopted).unwrap();
        store.select(boot, 2).unwrap();
        let descriptor = store.descriptor(Slot::B).unwrap();
        assert!(matches!(
            store.begin_stage(descriptor.clone(), attempt.clone()),
            Err(UpdateError::Phase)
        ));
    }
}

#[test]
fn stage_rejects_incompatible_bundles_before_reusing_a_slot() {
    for wrong_architecture in [false, true] {
        let (dir, store, _) = fixture();
        let mut descriptor = bundle(&store, Slot::B, b"wrong-architecture");
        if wrong_architecture {
            descriptor.architecture = pplx_space_cpu_arch::Arch::Aarch64;
        } else {
            descriptor.seed_contracts = vec!["unsupported-seed".into()];
        }
        let attempt = Attempt {
            operation: Uuid::now_v7(),
            execution: Uuid::now_v7(),
            boot: Uuid::now_v7(),
            incarnation: Uuid::now_v7(),
            configuration_revision: 1,
            target: Selection {
                slot: Slot::B,
                digest: descriptor.digest().unwrap(),
            },
            confirmation_deadline_ms: 100,
        };
        assert!(matches!(
            store.begin_stage(descriptor, attempt),
            Err(UpdateError::Identity)
        ));
        for binary in Binary::ALL {
            assert_eq!(
                fs::read(dir.path().join("slots/b").join(binary.file_name())).unwrap(),
                b"wrong-architecture"
            );
        }
    }
}

#[test]
fn invalid_candidate_consumes_attempt_then_next_start_restores_committed() {
    let (_dir, store, old) = fixture();
    let boot = Uuid::now_v7();
    let _attempt = arm(&store, boot);
    fs::write(store.slot_path(Slot::B).join("spaced"), b"bad").unwrap();
    let Err(_) = store.select(boot, 1) else {
        panic!("operation must be rejected");
    };
    assert!(matches!(
        store.journal().unwrap().state,
        JournalState::Trial { .. }
    ));
    assert_eq!(store.select(boot, 2).unwrap().digest, old.digest().unwrap());
}

#[test]
fn cancellation_and_commit_serialize_to_one_terminal_decision() {
    for commit_first in [true, false] {
        let (_dir, store, _) = fixture();
        let boot = Uuid::now_v7();
        let attempt = arm(&store, boot);
        store.select(boot, 1).unwrap();
        store.self_test_passed(&attempt).unwrap();
        if commit_first {
            store.commit(&attempt, boot, 2).unwrap();
        }
        store.abort(&attempt).unwrap();
        if commit_first {
            assert!(matches!(
                store.journal().unwrap().state,
                JournalState::Committed { .. }
            ));
        } else {
            let Err(_) = store.commit(&attempt, boot, 2) else {
                panic!("operation must be rejected");
            };
            assert!(matches!(
                store.journal().unwrap().state,
                JournalState::RolledBack {
                    progress: TrialProgress::AwaitingHost,
                    ..
                }
            ));
        }
    }
}

#[test]
fn lock_and_path_validation_prevent_concurrent_writers_and_selector_escape() {
    let (dir, store, _) = fixture();
    assert!(matches!(
        SlotStore::open(dir.path()),
        Err(UpdateError::Busy)
    ));
    fs::remove_file(dir.path().join("committed")).unwrap();
    std::os::unix::fs::symlink("../escape", dir.path().join("committed")).unwrap();
    fs::write(dir.path().join("update-state"), b"broken").unwrap();
    assert!(matches!(
        store.select(Uuid::now_v7(), 0),
        Err(UpdateError::Layout)
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn streaming_stage_enforces_two_slots_and_does_not_unlink_an_open_previous_release() {
    if !pplx_space_test_env::root_or_skip("inspect every live process's slot references") {
        return;
    }
    let (dir, store, _) = fixture();
    let boot = Uuid::now_v7();
    let attempt = arm(&store, boot);
    store.select(boot, 1).unwrap();
    store.self_test_passed(&attempt).unwrap();
    store.commit(&attempt, boot, 2).unwrap();
    let old_fd = File::open(store.slot_path(Slot::A).join("spaced-svc")).unwrap();
    let mut descriptor = store.descriptor(Slot::B).unwrap();
    let payload = b"next-release";
    descriptor.service = BinaryIdentity {
        length: payload.len() as u64,
        digest: ArtifactDigest::of_bytes(payload),
    };
    descriptor.cli = descriptor.service.clone();
    let next = Attempt {
        target: Selection {
            slot: Slot::A,
            digest: descriptor.digest().unwrap(),
        },
        operation: Uuid::now_v7(),
        ..attempt
    };
    assert!(matches!(
        store.begin_stage(descriptor.clone(), next.clone()),
        Err(UpdateError::Referenced)
    ));
    assert!(dir.path().join("slots/a/spaced-svc").exists());
    drop(old_fd);
    let store = SlotStore::open(dir.path()).unwrap();
    let mut stage = store.begin_stage(descriptor, next.clone()).unwrap();
    let Err(_) = stage.write(Binary::Service, 1, payload) else {
        panic!("operation must be rejected");
    };
    assert!(stage.write(Binary::Service, 0, &[0; 65 * 1024]).is_err());
    stage.write(Binary::Service, 0, payload).unwrap();
    stage.write(Binary::Cli, 0, payload).unwrap();
    assert_eq!(stage.finish().unwrap(), next);
    let store = SlotStore::open(dir.path()).unwrap();
    assert_eq!(store.link_slot("current").unwrap(), Slot::B);
    assert_eq!(fs::read_dir(dir.path().join("slots")).unwrap().count(), 2);
    store.verify(&next.target).unwrap();
    // A stop can retire manager authority after staging but before Arm. The
    // next execution may replace these inactive bytes; Staged is not a trial.
    let descriptor = store.descriptor(next.target.slot).unwrap();
    let replacement = Attempt {
        operation: Uuid::now_v7(),
        execution: Uuid::now_v7(),
        ..next
    };
    let stage = store
        .begin_stage(descriptor.clone(), replacement.clone())
        .unwrap();
    drop(stage);
    let store = SlotStore::open(dir.path()).unwrap();
    assert!(matches!(
        store.journal().unwrap().state,
        JournalState::Stable
    ));
    assert!(store.arm(&replacement).is_err());
    let mut stage = store.begin_stage(descriptor, replacement.clone()).unwrap();
    stage.write(Binary::Service, 0, payload).unwrap();
    stage.write(Binary::Cli, 0, payload).unwrap();
    assert_eq!(stage.finish().unwrap(), replacement);
    let store = SlotStore::open(dir.path()).unwrap();
    store.arm(&replacement).unwrap();
    store.select(boot, 3).unwrap();
    store.self_test_passed(&replacement).unwrap();
    store.select(boot, 4).unwrap();
    // AwaitingHost permits the manager's one retry under the same operation,
    // fenced to the fallback daemon's new incarnation and a fresh deadline.
    let retry = Attempt {
        incarnation: Uuid::now_v7(),
        confirmation_deadline_ms: 200,
        ..replacement
    };
    let descriptor = store.descriptor(retry.target.slot).unwrap();
    let mut stage = store.begin_stage(descriptor, retry.clone()).unwrap();
    stage.write(Binary::Service, 0, payload).unwrap();
    stage.write(Binary::Cli, 0, payload).unwrap();
    assert_eq!(stage.finish().unwrap(), retry);
    let store = SlotStore::open(dir.path()).unwrap();
    store.arm(&retry).unwrap();
    store.select(boot, 5).unwrap();
    store.self_test_passed(&retry).unwrap();
    store.commit(&retry, boot, 6).unwrap();
    assert_eq!(store.link_slot("committed").unwrap(), retry.target.slot);
}

#[test]
fn replaying_staged_bytes_preserves_the_verified_slot_on_abort() {
    for complete in [false, true] {
        let (dir, store, _) = fixture();
        let attempt = arm(&store, Uuid::now_v7());
        let mut journal = store.journal().unwrap();
        journal.state = JournalState::Staged {
            attempt: attempt.clone(),
        };
        store.write_journal(&journal).unwrap();
        let descriptor = store.descriptor(Slot::B).unwrap();
        let mut replay = store.begin_stage(descriptor, attempt.clone()).unwrap();
        replay.write(Binary::Service, 0, b"candidate").unwrap();
        if complete {
            replay.write(Binary::Cli, 0, b"candidate").unwrap();
            assert_eq!(replay.finish().unwrap(), attempt);
        } else {
            drop(replay);
        }
        let store = SlotStore::open(dir.path()).unwrap();
        store
            .verify(&attempt.target)
            .expect("a retried upload cannot destroy verified bytes");
        assert_eq!(store.journal().unwrap(), journal);
    }
}

#[test]
fn selection_samples_clock_only_after_committed_verification() {
    let (_dir, store, _) = fixture();
    let boot = Uuid::now_v7();
    let _attempt = arm(&store, boot);
    fs::write(store.slot_path(Slot::A).join("spaced"), b"corrupt").unwrap();
    let sampled = std::cell::Cell::new(false);
    let result = store.select_with_clock(|| {
        sampled.set(true);
        Ok((boot, 0))
    });
    assert!(matches!(result, Err(UpdateError::Identity)));
    assert!(!sampled.get());
}
