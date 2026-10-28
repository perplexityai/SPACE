use super::*;
use pplx_space_sbx_mgr_client::proto::ArtifactKind as ProtoArtifactKind;

fn entry() -> PendingLocalBackup {
    PendingLocalBackup {
        sandbox_id: uuid::Uuid::from_u128(1).to_string(),
        local_artifact_id: uuid::Uuid::from_u128(2).to_string(),
        artifact_kind: ProtoArtifactKind::Full.into(),
    }
}

#[test]
fn absent_empty_and_unusable_reports_remain_distinct() {
    assert!(
        PendingBackupReport::from_proto(None)
            .into_parsed()
            .is_none()
    );
    let empty = PendingBackupReport::from_proto(Some(&PendingLocalBackupReport::default()))
        .into_parsed()
        .unwrap()
        .unwrap();
    assert_eq!(empty.entry_count(), 0);
    for entries in [
        vec![PendingLocalBackup {
            sandbox_id: "invalid".to_owned(),
            ..entry()
        }],
        vec![PendingLocalBackup {
            local_artifact_id: "invalid".to_owned(),
            ..entry()
        }],
        vec![PendingLocalBackup {
            artifact_kind: 999,
            ..entry()
        }],
        vec![entry(), entry()],
    ] {
        let report = PendingBackupReport::from_proto(Some(&PendingLocalBackupReport {
            pending: entries.clone(),
        }));
        assert_eq!(report.reported_count(), Some(entries.len()));
        report.into_parsed().unwrap().unwrap_err();
    }
    let valid = ParsedPendingLocalBackups::parse(&[entry()]).unwrap();
    assert!(valid.names(SandboxId::from_uuid(uuid::Uuid::from_u128(1))));
    let (sandboxes, artifacts, kinds) = valid.unnest_columns();
    assert_eq!(sandboxes, [SandboxId::from_uuid(uuid::Uuid::from_u128(1))]);
    assert_eq!(
        artifacts,
        [LocalArtifactId::from_uuid(uuid::Uuid::from_u128(2))]
    );
    assert_eq!(kinds, ["full"]);
}
