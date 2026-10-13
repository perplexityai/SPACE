use super::*;
use crate::proto::{ComponentArtifact, RuntimeReleaseArtifact};

fn release(runtime_kind: i32, version: &str) -> NodeArtifact {
    NodeArtifact {
        artifact: Some(node_artifact::Artifact::RuntimeRelease(
            RuntimeReleaseArtifact {
                runtime_kind,
                version: version.to_string(),
                digest: pplx_space_common_core::types::Digest::sha256([7; 32]).to_string(),
            },
        )),
    }
}

#[test]
fn an_artifact_is_named_by_what_it_is() {
    let firecracker = pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32;
    assert!(matches!(
        Artifact::parse(Some(&release(firecracker, "1.16.2"))),
        Ok(Artifact::Release(release, _)) if release.version.as_str() == "1.16.2"
    ));
    let kernel = NodeArtifact {
        artifact: Some(node_artifact::Artifact::Component(ComponentArtifact {
            version: "6.1.158".to_string(),
            kind: pplx_space_execution_common::proto::ArtifactComponentKind::Kernel as i32,
            digest: pplx_space_common_core::types::Digest::sha256([7; 32]).to_string(),
        })),
    };
    assert!(matches!(
        Artifact::parse(Some(&kernel)),
        Ok(Artifact::Kernel(version, _)) if version.as_str() == "6.1.158"
    ));
    for invalid in [
        None,
        Some(NodeArtifact { artifact: None }),
        Some(release(0, "1.16.2")),
        Some(release(firecracker, "")),
    ] {
        let status = Artifact::parse(invalid.as_ref()).unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{invalid:?}");
    }
}

/// Bytes other than pinned are the caller's fault and final; a store that
/// cannot be trusted or holds something else needs an operator; only I/O
/// is worth retrying.
#[test]
fn publication_failures_map_to_what_a_caller_can_do() {
    let path = std::path::PathBuf::from("/staged");
    let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        .parse()
        .unwrap();
    let cases = [
        (
            PublishError::Mismatch {
                path: path.clone(),
                found: digest,
                pinned: digest,
            },
            tonic::Code::InvalidArgument,
        ),
        (
            PublishError::TargetDiffers { path: path.clone() },
            tonic::Code::FailedPrecondition,
        ),
        (
            PublishError::Io {
                action: "write",
                path,
                source: std::io::ErrorKind::Other.into(),
            },
            tonic::Code::Internal,
        ),
    ];
    for (error, code) in cases {
        assert_eq!(publish_status(error).code(), code);
    }
    assert_eq!(
        remove_status(RemoveError::IdentityChanged {
            path: "/managed/v1.15.1".into()
        })
        .code(),
        tonic::Code::FailedPrecondition
    );
}

/// While one store change runs, another is answered at once with a status
/// the runtime manager retries, rather than queued on a blocking thread.
#[tokio::test]
async fn a_second_store_change_is_turned_away_while_one_runs() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let running = service.artifact_slot().unwrap();
    let status = service.artifact_slot().unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    drop(running);
    let _again = service.artifact_slot().unwrap();
}

#[test]
fn execution_roots_must_not_alias_the_managed_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut paths = crate::config::RuntimePaths {
        vendor: dir.path().join("execution/releases"),
        assets: dir.path().join("execution/assets"),
        kernels: dir.path().join("execution/kernels"),
        store: crate::config::ArtifactStorePaths {
            releases: dir.path().join("managed/releases"),
            kernels: dir.path().join("managed/kernels"),
            staging: dir.path().join("staging"),
            catalog: dir.path().join("catalog"),
        },
    };
    std::fs::create_dir_all(&paths.store.releases).unwrap();
    std::fs::create_dir_all(paths.vendor.parent().unwrap()).unwrap();
    separate_store(&paths).unwrap();
    std::os::unix::fs::symlink(&paths.store.releases, &paths.vendor).unwrap();
    assert!(separate_store(&paths).is_err());
    std::fs::remove_file(&paths.vendor).unwrap();
    paths.kernels = paths.store.kernels.clone();
    assert!(separate_store(&paths).is_err());
}
