use super::*;

#[test]
fn snapshot_sweeper_grace_preserves_optional_duration_semantics() {
    let default = Some(Duration::from_secs(300));
    assert_eq!(snapshot_sweeper_grace_seconds(None, default), default);
    assert_eq!(
        snapshot_sweeper_grace_seconds(Some("invalid"), default),
        default
    );
    assert_eq!(snapshot_sweeper_grace_seconds(Some("0"), default), None);
    assert_eq!(
        snapshot_sweeper_grace_seconds(Some(" 900 "), default),
        Some(Duration::from_secs(900))
    );
}

#[test]
fn snapshot_name_rejects_traversal_components() {
    SnapshotName::parse("..").unwrap_err();
    SnapshotName::parse(".").unwrap_err();
    SnapshotName::parse("../snap").unwrap_err();
    SnapshotName::parse("nested\\snap").unwrap_err();
    SnapshotName::parse("snap name").unwrap_err();
    SnapshotName::parse("snap\nname").unwrap_err();
    SnapshotName::parse("a".repeat(256)).unwrap_err();
    SnapshotName::parse("snap").unwrap();
    SnapshotName::parse("snap_name.1").unwrap();
}

#[test]
fn snapshot_id_round_trips_uuid_encoding() {
    let id = SnapshotId::new();
    assert_eq!(SnapshotId::from_str(&id.to_string()).unwrap(), id);
}

#[test]
fn snapshot_id_uses_local_parse_error() {
    assert!(matches!(
        SnapshotId::from_str("not-a-uuid"),
        Err(SnapshotIdParseError::InvalidUuid(_))
    ));
}

#[test]
fn local_artifact_and_backup_attempt_ids_round_trip_uuid_encoding() {
    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = BackupAttemptId::new();
    assert_eq!(
        LocalArtifactId::from_str(&local_artifact_id.to_string()).unwrap(),
        local_artifact_id
    );
    assert_eq!(
        BackupAttemptId::from_str(&backup_attempt_id.to_string()).unwrap(),
        backup_attempt_id
    );
}

#[test]
fn snapshot_kind_round_trips_through_storage_string() {
    for kind in SnapshotKind::all() {
        assert_eq!(SnapshotKind::from_str(kind.as_str()).unwrap(), *kind);
        assert_eq!(kind.to_string(), kind.as_str());
    }
    SnapshotKind::from_str("memory").unwrap_err();
    SnapshotKind::from_str("provider_managed").unwrap_err();
}

#[test]
fn snapshot_content_hash_preserves_legacy_storage_and_exposes_canonical_digests() {
    let digest = format!("sha256:{}", "a".repeat(64))
        .parse::<TemplateArtifactDigest>()
        .unwrap();
    let canonical = SnapshotContentHash::from(digest);
    assert_eq!(canonical.as_stored(), digest.to_string());
    assert_eq!(canonical.artifact_digest(), Some(digest));

    let bare = "b".repeat(64);
    let bare_hash = SnapshotContentHash::from_stored(bare.clone());
    assert_eq!(bare_hash.as_stored(), bare);
    assert_eq!(
        bare_hash.artifact_digest(),
        Some(format!("sha256:{}", "b".repeat(64)).parse().unwrap())
    );

    let git_sha = "66c258cefa258e64704c18490fee0c7e2ef4da15";
    let legacy = SnapshotContentHash::from_stored(git_sha);
    assert_eq!(legacy.as_stored(), git_sha);
    assert_eq!(legacy.artifact_digest(), None);
}

#[test]
fn artifact_kind_round_trips_through_wire_string() {
    for kind in [ArtifactKind::Disk, ArtifactKind::Full] {
        assert_eq!(ArtifactKind::from_str(kind.as_str()).unwrap(), kind);
        assert_eq!(kind.to_string(), kind.as_str());
    }
    ArtifactKind::from_str("suspended_restore_point").unwrap_err();
}

#[test]
fn artifact_origin_round_trips_through_wire_string() {
    for origin in [
        ArtifactOrigin::Stop,
        ArtifactOrigin::Suspend,
        ArtifactOrigin::Snapshot,
        ArtifactOrigin::Fork,
    ] {
        assert_eq!(ArtifactOrigin::from_str(origin.as_str()).unwrap(), origin);
        assert_eq!(origin.to_string(), origin.as_str());
    }
    ArtifactOrigin::from_str("checkpoint").unwrap_err();
}

#[test]
fn snapshot_state_round_trips_through_wire_string() {
    for state in SnapshotState::all() {
        let s = state.as_str();
        assert_eq!(SnapshotState::from_str(s).unwrap(), *state);
        assert_eq!(state.to_string(), s);
    }
}

#[test]
fn snapshot_state_rejects_unknown_strings() {
    let err = SnapshotState::from_str("READY").unwrap_err();
    assert_eq!(err.input(), "READY");
    SnapshotState::from_str("").unwrap_err();
    SnapshotState::from_str("done").unwrap_err();
}

#[test]
fn snapshot_state_terminal_set_excludes_creating() {
    for state in SnapshotState::all() {
        assert_eq!(state.is_terminal(), *state != SnapshotState::Creating);
    }
}

#[test]
fn snapshot_state_is_ready_matches_only_ready() {
    for state in SnapshotState::all() {
        assert_eq!(state.is_ready(), *state == SnapshotState::Ready);
        assert_eq!(state.is_failed(), *state == SnapshotState::Failed);
        assert_eq!(state.is_deleting(), *state == SnapshotState::Deleting);
    }
}

#[test]
fn base_snapshot_shape_defaults_reject_tiny_values() {
    assert_eq!(
        base_snapshot_vcpu_or_default(Some(0)),
        DEFAULT_BASE_SNAPSHOT_VCPU
    );
    assert_eq!(
        base_snapshot_memory_mib_or_default(Some(1)),
        DEFAULT_BASE_SNAPSHOT_MEMORY_MIB
    );
    assert_eq!(base_snapshot_vcpu_or_default(Some(4)), 4);
    assert_eq!(base_snapshot_memory_mib_or_default(Some(8192)), 8192);
    assert_eq!(
        BaseSnapshotShape::from_optional_i32(Some(4), Some(8192)),
        BaseSnapshotShape::new(
            VcpuCount::try_from_u32(4).unwrap(),
            MemorySize::mib(8192).unwrap()
        )
    );
}
