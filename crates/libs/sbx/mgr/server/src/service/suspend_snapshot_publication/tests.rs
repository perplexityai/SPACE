use super::*;

const TEST_SANDBOX_ID: &str = "019e31d9-79a6-7132-b90f-007f1c62ddea";

#[test]
fn publication_replaces_the_complete_snapshot_and_preserves_runtime_siblings() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let sandbox_dir = tmp.path();
    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    let staging_dir = sandbox_dir.join("publish-source");
    let instance_dir = sandbox_dir.join("instance");
    std::fs::create_dir_all(&instance_dir).unwrap();

    write_full_suspend_artifact(
        &snapshot_dir,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        b"old-artifact",
    );
    let new_artifact_id = LocalArtifactId::new();
    write_full_suspend_artifact(
        &staging_dir,
        TEST_SANDBOX_ID,
        new_artifact_id,
        b"new-artifact",
    );
    std::fs::write(instance_dir.join("rootfs.ext4"), b"live-fc-rootfs").unwrap();

    publish_suspend_snapshot_dir(
        &staging_dir,
        &snapshot_dir,
        new_artifact_id,
        &Default::default(),
    )
    .unwrap();

    assert_eq!(
        std::fs::read(snapshot_dir.join("vmstate")).unwrap(),
        b"new-artifact"
    );
    assert!(!staging_dir.exists());

    let live = instance_dir.join("rootfs.ext4");
    assert!(live.exists(), "publication must not touch runtime siblings");
    assert_eq!(
        std::fs::read(&live).unwrap(),
        b"live-fc-rootfs",
        "publication must not rewrite the live rootfs",
    );
}

#[test]
fn publication_makes_a_new_snapshot_visible_only_at_publish_time() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let sandbox_dir = tmp.path();
    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    let staging_dir = sandbox_dir.join("publish-source");
    let local_artifact_id = LocalArtifactId::new();
    write_full_suspend_artifact(
        &staging_dir,
        TEST_SANDBOX_ID,
        local_artifact_id,
        b"complete-artifact",
    );

    assert!(!snapshot_dir.exists());
    publish_suspend_snapshot_dir(
        &staging_dir,
        &snapshot_dir,
        local_artifact_id,
        &Default::default(),
    )
    .unwrap();

    assert_eq!(
        std::fs::read(snapshot_dir.join("vmstate")).unwrap(),
        b"complete-artifact"
    );
}

#[test]
fn publication_is_idempotent_when_canonical_already_matches_artifact() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let sandbox_dir = tmp.path();
    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    let staging_dir = sandbox_dir.join("publish-source");
    let local_artifact_id = LocalArtifactId::new();
    write_full_suspend_artifact(
        &snapshot_dir,
        TEST_SANDBOX_ID,
        local_artifact_id,
        b"canonical-artifact",
    );

    publish_suspend_snapshot_dir(
        &staging_dir,
        &snapshot_dir,
        local_artifact_id,
        &Default::default(),
    )
    .expect("retry after a completed publish must be idempotent");

    assert_eq!(
        std::fs::read(snapshot_dir.join("vmstate")).unwrap(),
        b"canonical-artifact"
    );
    assert!(!staging_dir.exists());
}

#[test]
fn classifier_prefers_complete_staging_over_unpinned_canonical_artifact() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let sandbox_dir = tmp.path();
    let old_artifact_id = LocalArtifactId::new();
    let new_artifact_id = LocalArtifactId::new();
    write_full_suspend_artifact(
        &canonical_suspend_snapshot_dir(sandbox_dir),
        TEST_SANDBOX_ID,
        old_artifact_id,
        b"old-canonical",
    );
    let staging_dir = suspend_snapshot_staging_dir(sandbox_dir, new_artifact_id);
    write_full_suspend_artifact(
        &staging_dir,
        TEST_SANDBOX_ID,
        new_artifact_id,
        b"new-staging",
    );

    let decision =
        classify_suspend_snapshot_publication(sandbox_dir, None, false, &Default::default())
            .unwrap();

    assert_eq!(
        decision,
        SuspendSnapshotPublicationDecision::StagedComplete(StagedSuspendSnapshotArtifact {
            local_artifact_id: new_artifact_id,
            staging_dir
        })
    );
}

#[test]
fn classifier_refuses_mismatched_canonical_when_expected_artifact_is_known() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let sandbox_dir = tmp.path();
    let expected_artifact_id = LocalArtifactId::new();
    let stale_artifact_id = LocalArtifactId::new();
    write_full_suspend_artifact(
        &canonical_suspend_snapshot_dir(sandbox_dir),
        TEST_SANDBOX_ID,
        stale_artifact_id,
        b"stale-canonical",
    );

    let decision = classify_suspend_snapshot_publication(
        sandbox_dir,
        Some(expected_artifact_id),
        false,
        &Default::default(),
    )
    .unwrap();

    assert_eq!(
        decision,
        SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact,
        "mismatched canonical artifact must not be adopted when DB names an expected artifact"
    );
}

#[test]
fn classifier_treats_runtime_live_as_source_of_truth() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let sandbox_dir = tmp.path();
    let local_artifact_id = LocalArtifactId::new();
    let staging_dir = suspend_snapshot_staging_dir(sandbox_dir, local_artifact_id);
    write_full_suspend_artifact(
        &staging_dir,
        TEST_SANDBOX_ID,
        local_artifact_id,
        b"complete-staging",
    );

    let decision =
        classify_suspend_snapshot_publication(sandbox_dir, None, true, &Default::default())
            .unwrap();

    assert_eq!(
        decision,
        SuspendSnapshotPublicationDecision::RuntimeLiveWithoutUsableArtifact
    );
}

fn retained(snapshot_dir: &Path, source_id: uuid::Uuid) -> RetainedSource {
    RetainedSource {
        sandbox_id: TEST_SANDBOX_ID.to_owned(),
        artifact_dir: snapshot_dir.to_owned(),
        source_id,
    }
}

#[test]
fn deferred_artifact_completeness_follows_node_manager_verdicts() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    let source_id = uuid::Uuid::now_v7();
    let bundle = write_deferred_suspend_artifact(
        &snapshot_dir,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        source_id,
        100,
    );
    let validate = |check: Option<RetainedSourceCheck>| {
        let mut verdicts = RetainedSourceVerdicts::default();
        if let Some(check) = check {
            verdicts.record(retained(&snapshot_dir, source_id), check);
        }
        let result = validate_complete_suspend_snapshot_dir(&snapshot_dir, &verdicts).map(|_| ());
        (result, verdicts.take_unresolved())
    };

    let (result, unresolved) = validate(Some(RetainedSourceCheck::Present { rootfs_bytes: 100 }));
    result.unwrap();
    assert!(unresolved.is_empty());

    let (result, _) = validate(Some(RetainedSourceCheck::Present { rootfs_bytes: 99 }));
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        SuspendSnapshotValidationError::ArtifactSizeMismatch { .. }
    ));
    assert!(error.is_unsafe_evidence());

    // An absent answer with the bundle directory still present predates a
    // rename that brought this artifact back: pending, not missing.
    let (result, _) = validate(Some(RetainedSourceCheck::Absent));
    assert!(result.unwrap_err().is_pending());
    drop(bundle);
    std::fs::remove_dir_all(snapshot_dir.join(crate::retained_source::BUNDLE_DIRECTORY)).unwrap();
    let (result, _) = validate(Some(RetainedSourceCheck::Absent));
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        SuspendSnapshotValidationError::MissingArtifact {
            artifact: crate::retained_source::BUNDLE_DIRECTORY,
            ..
        }
    ));
    assert!(error.is_unsafe_evidence());
    // Only the bundle entry's definite absence is evidence. A path that cannot
    // be resolved (a symlink loop fails whatever the test's privileges) proves
    // nothing.
    let looped = tmp.path().join("looped");
    std::os::unix::fs::symlink(&looped, &looped).unwrap();
    let mut verdicts = RetainedSourceVerdicts::default();
    verdicts.record(retained(&looped, source_id), RetainedSourceCheck::Absent);
    let error =
        retained_rootfs_bytes(&looped, &retained(&looped, source_id), &verdicts).unwrap_err();
    assert!(error.is_inconclusive(), "{error}");
    assert!(!error.is_unsafe_evidence(), "{error}");

    let (result, _) = validate(Some(RetainedSourceCheck::Invalid {
        reason: "identity changed".to_owned(),
    }));
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        SuspendSnapshotValidationError::RetainedSourceInvalid { .. }
    ));
    assert!(error.is_unsafe_evidence());

    for check in [
        Some(RetainedSourceCheck::Unavailable {
            reason: "node-manager restarting".to_owned(),
        }),
        None,
    ] {
        let (result, unresolved) = validate(check);
        let error = result.unwrap_err();
        assert!(error.is_pending(), "{error}");
        assert!(!error.is_unsafe_evidence(), "{error}");
        assert_eq!(unresolved, vec![retained(&snapshot_dir, source_id)]);
    }
}

#[test]
fn classification_waits_for_an_unverified_retained_source() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path();
    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    let local_artifact_id = LocalArtifactId::new();
    let source_id = uuid::Uuid::now_v7();
    let bundle = write_deferred_suspend_artifact(
        &snapshot_dir,
        TEST_SANDBOX_ID,
        local_artifact_id,
        source_id,
        8,
    );
    let classify = |check: Option<RetainedSourceCheck>| {
        let mut verdicts = RetainedSourceVerdicts::default();
        if let Some(check) = check {
            verdicts.record(retained(&snapshot_dir, source_id), check);
        }
        classify_suspend_snapshot_publication(
            sandbox_dir,
            Some(local_artifact_id),
            false,
            &verdicts,
        )
    };

    // Without an answer the runtime-gone conclusion would quarantine an
    // artifact that may be complete.
    assert!(matches!(
        classify(None),
        Err(SuspendSnapshotPublicationError::Validation(error)) if error.is_pending()
    ));
    assert!(matches!(
        classify(Some(RetainedSourceCheck::Present { rootfs_bytes: 8 })).unwrap(),
        SuspendSnapshotPublicationDecision::CanonicalReady(artifact)
            if artifact.local_artifact_id == local_artifact_id
    ));
    // Absent beside a bundle directory is pending; once the bundle is gone
    // it is the runtime-gone conclusion.
    assert!(matches!(
        classify(Some(RetainedSourceCheck::Absent)),
        Err(SuspendSnapshotPublicationError::Validation(error)) if error.is_pending()
    ));
    drop(bundle);
    std::fs::remove_dir_all(snapshot_dir.join(crate::retained_source::BUNDLE_DIRECTORY)).unwrap();
    assert_eq!(
        classify(Some(RetainedSourceCheck::Absent)).unwrap(),
        SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact
    );

    // A read that fails for any reason but absence is no more a conclusion
    // than a missing answer, for the canonical artifact or a staged one:
    // retry, never runtime-gone. A self-referencing sidecar fails whatever
    // the test's privileges, and still does after a rename.
    let sidecar = snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    std::fs::remove_file(&sidecar).unwrap();
    std::os::unix::fs::symlink(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME, &sidecar)
        .unwrap();
    assert!(matches!(
        classify(None),
        Err(SuspendSnapshotPublicationError::Validation(error)) if error.is_inconclusive()
    ));
    let staged = suspend_snapshot_staging_dir(sandbox_dir, local_artifact_id);
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    std::fs::rename(&snapshot_dir, &staged).unwrap();
    assert!(matches!(
        classify(None),
        Err(SuspendSnapshotPublicationError::Validation(error)) if error.is_inconclusive()
    ));
}

#[test]
fn retained_sources_are_discovered_from_canonical_and_staged_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path();
    let canonical = canonical_suspend_snapshot_dir(sandbox_dir);
    let staged = suspend_snapshot_staging_dir(sandbox_dir, LocalArtifactId::new());
    let full = suspend_snapshot_staging_dir(sandbox_dir, LocalArtifactId::new());
    let (canonical_id, staged_id) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
    let _canonical = write_deferred_suspend_artifact(
        &canonical,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        canonical_id,
        1,
    );
    let _staged = write_deferred_suspend_artifact(
        &staged,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        staged_id,
        1,
    );
    write_full_suspend_artifact(&full, TEST_SANDBOX_ID, LocalArtifactId::new(), b"full");

    let mut found = retained_sources_in_sandbox_dir(sandbox_dir);
    found.sort();
    let mut expected = vec![
        retained(&canonical, canonical_id),
        retained(&staged, staged_id),
    ];
    expected.sort();
    assert_eq!(found, expected);
}

fn retired_entries(sandbox_dir: &Path) -> Vec<LocalArtifactId> {
    let prefix = crate::service::recovery::staging_cleanup::RETIRED_PREFIX;
    let mut ids: Vec<_> = std::fs::read_dir(suspend_snapshot_staging_parent_dir(sandbox_dir))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter_map(|name| {
            name.strip_prefix(prefix).map(|id| {
                id.parse::<LocalArtifactId>()
                    .expect("the sweep parses this suffix")
            })
        })
        .collect();
    ids.sort();
    ids
}

#[test]
fn publication_retires_the_superseded_snapshot_for_the_staging_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path();
    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    let old_id = LocalArtifactId::new();
    let new_id = LocalArtifactId::new();
    write_full_suspend_artifact(&snapshot_dir, TEST_SANDBOX_ID, old_id, b"old-artifact");
    let staging_dir = suspend_snapshot_staging_dir(sandbox_dir, new_id);
    write_full_suspend_artifact(&staging_dir, TEST_SANDBOX_ID, new_id, b"new-artifact");

    publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, new_id, &Default::default()).unwrap();

    assert_eq!(
        std::fs::read(snapshot_dir.join("vmstate")).unwrap(),
        b"new-artifact"
    );
    // The old snapshot is renamed into the sweep's lane, never deleted here:
    // it can hold read-only subvolumes only node-manager can remove. The hold
    // it waited in during publication is gone.
    assert!(!retired_suspend_snapshot_dir(sandbox_dir, new_id).exists());
    let garbage = retired_entries(sandbox_dir);
    assert_eq!(garbage.len(), 1);
    let garbage_dir = suspend_snapshot_staging_parent_dir(sandbox_dir).join(format!(
        "{}{}",
        crate::service::recovery::staging_cleanup::RETIRED_PREFIX,
        garbage[0]
    ));
    assert_eq!(
        std::fs::read(garbage_dir.join("vmstate")).unwrap(),
        b"old-artifact"
    );

    // A duplicate staging of the published artifact is retired the same way.
    write_full_suspend_artifact(&staging_dir, TEST_SANDBOX_ID, new_id, b"new-artifact");
    publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, new_id, &Default::default()).unwrap();
    assert!(!staging_dir.exists());
    assert_eq!(retired_entries(sandbox_dir).len(), 2);
}

/// The hold is outside the sweep's lane, so a crash between the two renames
/// cannot lose the snapshot the sweep would otherwise collect. A retry either
/// restores it or, once the replacement is canonical, releases it.
#[test]
fn publication_resumes_an_interrupted_replacement_from_its_hold() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path();
    let snapshot_dir = canonical_suspend_snapshot_dir(sandbox_dir);
    let (old_id, new_id) = (LocalArtifactId::new(), LocalArtifactId::new());
    let hold = retired_suspend_snapshot_dir(sandbox_dir, new_id);
    let staging_dir = suspend_snapshot_staging_dir(sandbox_dir, new_id);

    // Crashed after retiring the old snapshot, before publishing the new one.
    write_full_suspend_artifact(&hold, TEST_SANDBOX_ID, old_id, b"old-artifact");
    write_full_suspend_artifact(&staging_dir, TEST_SANDBOX_ID, new_id, b"new-artifact");
    publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, new_id, &Default::default()).unwrap();
    assert_eq!(
        std::fs::read(snapshot_dir.join("vmstate")).unwrap(),
        b"new-artifact"
    );
    assert!(!hold.exists());
    assert_eq!(retired_entries(sandbox_dir).len(), 1);

    // Crashed after publishing, before releasing the hold.
    write_full_suspend_artifact(&hold, TEST_SANDBOX_ID, old_id, b"old-artifact");
    publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, new_id, &Default::default()).unwrap();
    assert!(!hold.exists());
    assert_eq!(retired_entries(sandbox_dir).len(), 2);

    // A hold beside a canonical snapshot of another artifact is not ours to
    // resolve either way.
    let other = LocalArtifactId::new();
    write_full_suspend_artifact(&snapshot_dir, TEST_SANDBOX_ID, other, b"other");
    write_full_suspend_artifact(&hold, TEST_SANDBOX_ID, old_id, b"old-artifact");
    assert!(matches!(
        publish_suspend_snapshot_dir(&staging_dir, &snapshot_dir, new_id, &Default::default()),
        Err(SuspendSnapshotPublicationError::RetiredResidue { .. })
    ));
    assert!(hold.exists());
}

#[test]
fn discovery_names_only_the_sources_validation_needs() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path();
    let canonical = canonical_suspend_snapshot_dir(sandbox_dir);
    let source_id = uuid::Uuid::now_v7();
    let _bundle = write_deferred_suspend_artifact(
        &canonical,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        source_id,
        100,
    );
    let expected = vec![retained(&canonical, source_id)];
    // Deferred: the retained image is the only rootfs.
    assert_eq!(retained_sources_in_sandbox_dir(sandbox_dir), expected);

    // A materialized stream the sidecar counts needs no inspection.
    let stream = canonical.join(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME);
    std::fs::write(&stream, vec![0u8; 37]).unwrap();
    let sidecar_path = canonical.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let mut sidecar: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&std::fs::read(&sidecar_path).unwrap()).unwrap();
    let image_counting = sidecar.size_bytes;
    sidecar.size_bytes = image_counting - 100 + 37;
    std::fs::write(&sidecar_path, serde_json::to_vec(&sidecar).unwrap()).unwrap();
    assert!(retained_sources_in_sandbox_dir(sandbox_dir).is_empty());
    validate_complete_suspend_snapshot_dir(&canonical, &Default::default()).unwrap();

    // Reclaim rewrote the sidecar to count the image and was interrupted
    // before the unlink: validation needs the image length, so recovery must
    // inspect this source even though the stream still exists.
    crate::snapshot_upload_jobs::rewrite_sidecar_for_reclaim(&canonical, 100).unwrap();
    assert_eq!(retained_sources_in_sandbox_dir(sandbox_dir), expected);
    let error =
        validate_complete_suspend_snapshot_dir(&canonical, &Default::default()).unwrap_err();
    assert!(error.is_pending());
    let mut verdicts = RetainedSourceVerdicts::default();
    verdicts.record(
        retained(&canonical, source_id),
        RetainedSourceCheck::Present { rootfs_bytes: 100 },
    );
    validate_complete_suspend_snapshot_dir(&canonical, &verdicts).unwrap();
}

#[test]
fn an_absent_verdict_counts_as_missing_only_once_the_bundle_is_gone() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    let source_id = uuid::Uuid::now_v7();
    let bundle = write_deferred_suspend_artifact(
        &snapshot_dir,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        source_id,
        100,
    );
    let mut verdicts = RetainedSourceVerdicts::default();
    verdicts.record(
        retained(&snapshot_dir, source_id),
        RetainedSourceCheck::Absent,
    );
    // The verdict may predate this validation: while the bundle directory
    // exists the source is pending another look, not missing.
    let error = validate_complete_suspend_snapshot_dir(&snapshot_dir, &verdicts).unwrap_err();
    assert!(error.is_pending(), "{error}");
    drop(bundle);
    std::fs::remove_dir_all(snapshot_dir.join(crate::retained_source::BUNDLE_DIRECTORY)).unwrap();
    let error = validate_complete_suspend_snapshot_dir(&snapshot_dir, &verdicts).unwrap_err();
    assert!(error.is_unsafe_evidence(), "{error}");
}

/// A hold beside a canonical artifact whose source is not inspected yet is a
/// pending publication, not retired residue.
#[test]
fn publication_with_a_hold_waits_for_an_unverified_canonical() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_dir = tmp.path();
    let canonical = canonical_suspend_snapshot_dir(sandbox_dir);
    let id = LocalArtifactId::new();
    let source_id = uuid::Uuid::now_v7();
    let _bundle = write_deferred_suspend_artifact(&canonical, TEST_SANDBOX_ID, id, source_id, 100);
    let hold = retired_suspend_snapshot_dir(sandbox_dir, id);
    write_full_suspend_artifact(&hold, TEST_SANDBOX_ID, LocalArtifactId::new(), b"old");
    let staging = suspend_snapshot_staging_dir(sandbox_dir, id);

    let error =
        publish_suspend_snapshot_dir(&staging, &canonical, id, &Default::default()).unwrap_err();
    assert!(
        matches!(&error, SuspendSnapshotPublicationError::Validation(inner) if inner.is_pending()),
        "{error}"
    );
    assert!(hold.exists());

    let mut verdicts = RetainedSourceVerdicts::default();
    verdicts.record(
        retained(&canonical, source_id),
        RetainedSourceCheck::Present { rootfs_bytes: 100 },
    );
    let garbage = publish_suspend_snapshot_dir(&staging, &canonical, id, &verdicts).unwrap();
    assert_eq!(garbage.len(), 1);
    assert!(!hold.exists());
}

#[test]
fn a_retained_artifact_stays_complete_across_stream_reclaim() {
    let tmp = tempfile::tempdir().unwrap();
    let snapshot_dir = tmp.path().join("snapshot");
    let source_id = uuid::Uuid::now_v7();
    let _bundle = write_deferred_suspend_artifact(
        &snapshot_dir,
        TEST_SANDBOX_ID,
        LocalArtifactId::new(),
        source_id,
        100,
    );
    // A materialized stream beside the retained source: its length differs
    // from the image and never enters the local accounting.
    let stream = snapshot_dir.join(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME);
    std::fs::write(&stream, vec![0u8; 37]).unwrap();
    let mut verdicts = RetainedSourceVerdicts::default();
    verdicts.record(
        retained(&snapshot_dir, source_id),
        RetainedSourceCheck::Present { rootfs_bytes: 100 },
    );

    validate_complete_suspend_snapshot_dir(&snapshot_dir, &verdicts).unwrap();
    // Reclaim after a durable backup removes only the stream.
    std::fs::remove_file(&stream).unwrap();
    validate_complete_suspend_snapshot_dir(&snapshot_dir, &verdicts).unwrap();
    // Node-manager, not the file, still decides.
    let error =
        validate_complete_suspend_snapshot_dir(&snapshot_dir, &Default::default()).unwrap_err();
    assert!(error.is_pending());
}
