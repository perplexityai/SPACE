use super::*;
use pplx_space_sbx_mgr_client::proto::SnapshotDetail;

fn state(id: u128) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: uuid::Uuid::from_u128(id).to_string(),
        present: true,
        status: Some("started".to_owned()),
        ..Default::default()
    }
}

#[test]
fn duplicate_resolution_precedes_field_validation_and_preserves_absence() {
    let input = ValidatedSandboxInventory::from_proto(&[
        PublishedSandboxState {
            pid: Some(u32::MAX),
            ..state(1)
        },
        state(3),
        PublishedSandboxState {
            sandbox_id: "not-an-identity".to_owned(),
            ..state(9)
        },
        PublishedSandboxState {
            status: Some(" Failed ".to_owned()),
            state_degraded: true,
            ..state(2)
        },
        PublishedSandboxState {
            present: false,
            pid: Some(u32::MAX),
            ..state(3)
        },
        PublishedSandboxState {
            status: Some(" StArTeD ".to_owned()),
            ..state(1)
        },
    ])
    .into_legacy_input();
    assert_eq!(input.rows.len(), 1);
    assert_eq!(input.rows[0]["sandbox_id"], state(1).sandbox_id);
    assert_eq!(input.rows[0]["status"], "StArTeD");
    assert_eq!(
        input.absent_ids,
        [SandboxId::from_uuid(uuid::Uuid::from_u128(3))]
    );
    assert_eq!(input.state_only.len(), 1);
    assert_eq!(
        input.state_only[0].sandbox_id(),
        SandboxId::from_uuid(uuid::Uuid::from_u128(2))
    );
    assert_eq!(input.state_only[0].legacy_row()["status"], "Failed");
    assert_eq!(input.failure_episode_rows.len(), 2);
    assert_eq!(
        input.failure_episode_rows[0].legacy_row()["state_degraded"],
        false
    );
    assert_eq!(
        input.failure_episode_rows[1].legacy_row()["state_degraded"],
        true
    );
}

#[test]
fn incomplete_rich_evidence_drops_only_its_own_snapshots_policy_and_quarantine_claim() {
    for rejection in ["core", "snapshot", "explicitly-degraded"] {
        let mut rejected = PublishedSandboxState {
            status: Some(" QuArAnTiNeD ".to_owned()),
            observed_network_policy_generation: 1,
            snapshots: vec![
                SnapshotDetail {
                    id: "discard-first".to_owned(),
                    ..Default::default()
                },
                SnapshotDetail {
                    id: "discard-second".to_owned(),
                    ..Default::default()
                },
            ],
            ..state(2)
        };
        match rejection {
            "core" => rejected.pid = Some(u32::MAX),
            "snapshot" => rejected.snapshots[1].vcpu = u32::MAX,
            "explicitly-degraded" => rejected.state_degraded = true,
            _ => unreachable!(),
        }
        let input = ValidatedSandboxInventory::from_proto(&[
            PublishedSandboxState {
                snapshots: vec![SnapshotDetail {
                    id: "keep-later".to_owned(),
                    ..Default::default()
                }],
                ..state(3)
            },
            rejected,
            PublishedSandboxState {
                snapshots: vec![SnapshotDetail {
                    id: "keep-earlier".to_owned(),
                    ..Default::default()
                }],
                ..state(1)
            },
        ])
        .into_legacy_input();
        assert_eq!(input.rows.len(), 2, "{rejection}");
        assert_eq!(input.snapshot_rows.len(), 2, "{rejection}");
        assert_eq!(input.snapshot_rows[0]["snapshot_id"], "keep-earlier");
        assert_eq!(input.snapshot_rows[1]["snapshot_id"], "keep-later");
        assert!(input.quarantine_inputs.is_empty(), "{rejection}");
        assert!(!input.publishes_network_policy, "{rejection}");
        assert_eq!(input.state_only.len(), 1, "{rejection}");
        assert_eq!(input.state_only[0].legacy_row()["status"], "QuArAnTiNeD");
        assert_eq!(
            input.failure_episode_rows[1].legacy_row()["state_degraded"],
            true
        );
    }
}
