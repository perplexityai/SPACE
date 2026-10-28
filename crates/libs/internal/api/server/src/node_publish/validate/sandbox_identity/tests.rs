use super::*;
use pplx_space_common_core::types::SandboxTransitionTargetState;
use pplx_space_sbx_mgr_client::proto::{ArtifactKind as ProtoKind, ArtifactOrigin as ProtoOrigin};
use tonic::Code;

const ARTIFACT_ID: &str = "01990000-0000-7000-8000-000000000001";

#[test]
fn local_artifact_identity_is_all_present_or_all_absent() {
    for fields in 0..8 {
        let state = PublishedSandboxState {
            current_local_artifact_id: (fields & 1 != 0).then(|| ARTIFACT_ID.to_string()),
            current_local_artifact_kind: (fields & 2 != 0).then_some(ProtoKind::Full as i32),
            current_local_artifact_origin: (fields & 4 != 0).then_some(ProtoOrigin::Suspend as i32),
            ..PublishedSandboxState::default()
        };
        let result = SandboxIdentityEvidence::try_from_proto(&state, None);
        match fields {
            0 => assert!(result.unwrap().current_local_artifact().is_none()),
            7 => {
                let evidence = result.unwrap();
                let artifact = evidence.current_local_artifact().unwrap();
                assert_eq!(artifact.id().to_string(), ARTIFACT_ID);
                assert_eq!(artifact.kind(), ArtifactKind::Full);
                assert_eq!(artifact.origin(), ArtifactOrigin::Suspend);
            }
            _ => {
                let error = result.unwrap_err();
                assert_eq!(error.code(), Code::InvalidArgument);
                assert_eq!(
                    error.message(),
                    "current local artifact identity must be fully present or absent"
                );
            }
        }
    }
}

#[test]
fn recovery_receipts_require_full_suspend_artifacts() {
    let receipt = SandboxTransitionReceipt::try_new(
        "01990000-0000-7000-8000-000000000002".parse().unwrap(),
        SandboxTransitionReceiptKind::QuarantineRecovery,
        SandboxTransitionTargetState::Suspended,
        ExecutionGeneration::new(1).unwrap(),
        Some(
            "pause_failed"
                .parse::<SandboxQuarantineReasonCode>()
                .unwrap(),
        ),
        None,
    )
    .unwrap();
    for kind in [ProtoKind::Disk, ProtoKind::Full] {
        for origin in [
            ProtoOrigin::Stop,
            ProtoOrigin::Suspend,
            ProtoOrigin::Snapshot,
            ProtoOrigin::Fork,
        ] {
            let state = PublishedSandboxState {
                current_local_artifact_id: Some(ARTIFACT_ID.to_string()),
                current_local_artifact_kind: Some(kind as i32),
                current_local_artifact_origin: Some(origin as i32),
                ..PublishedSandboxState::default()
            };
            SandboxIdentityEvidence::try_from_proto(&state, None).unwrap();
            let result = SandboxIdentityEvidence::try_from_proto(&state, Some(&receipt));
            if kind == ProtoKind::Full && origin == ProtoOrigin::Suspend {
                assert!(result.is_ok());
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code(), Code::InvalidArgument);
                assert_eq!(
                    error.message(),
                    "suspended recovery artifact must be a full suspend artifact"
                );
            }
        }
    }
}

#[test]
fn sandbox_identity_rejections_preserve_status_and_message() {
    let malformed_id_error = format!(
        "invalid current_local_artifact_id: {}",
        "invalid-artifact".parse::<LocalArtifactId>().unwrap_err()
    );
    let cases = [
        (
            PublishedSandboxState {
                current_local_artifact_id: Some("invalid-artifact".to_string()),
                current_local_artifact_kind: Some(ProtoKind::Full as i32),
                current_local_artifact_origin: Some(ProtoOrigin::Suspend as i32),
                ..PublishedSandboxState::default()
            },
            malformed_id_error.as_str(),
        ),
        (
            PublishedSandboxState {
                current_local_artifact_id: Some(ARTIFACT_ID.to_string()),
                current_local_artifact_kind: Some(123),
                current_local_artifact_origin: Some(ProtoOrigin::Suspend as i32),
                ..PublishedSandboxState::default()
            },
            "current_local_artifact_kind must be DISK or FULL",
        ),
        (
            PublishedSandboxState {
                current_local_artifact_id: Some(ARTIFACT_ID.to_string()),
                current_local_artifact_kind: Some(ProtoKind::Full as i32),
                current_local_artifact_origin: Some(123),
                ..PublishedSandboxState::default()
            },
            "current_local_artifact_origin must be STOP, SUSPEND, SNAPSHOT, or FORK",
        ),
        (
            PublishedSandboxState {
                state_changed_at: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: -1,
                }),
                ..PublishedSandboxState::default()
            },
            "state_changed_at has invalid nanos",
        ),
        (
            PublishedSandboxState {
                state_changed_at: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                status: Some("  ".to_string()),
                ..PublishedSandboxState::default()
            },
            "state_changed_at requires status",
        ),
        (
            PublishedSandboxState {
                execution_generation: Some(0),
                ..PublishedSandboxState::default()
            },
            "sandbox execution generation must be positive",
        ),
        (
            PublishedSandboxState {
                pod_uid: Some("  ".to_string()),
                ..PublishedSandboxState::default()
            },
            "pod uid must not be empty",
        ),
    ];
    for (state, message) in cases {
        let error = SandboxIdentityEvidence::try_from_proto(&state, None).unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), message);
    }
}
