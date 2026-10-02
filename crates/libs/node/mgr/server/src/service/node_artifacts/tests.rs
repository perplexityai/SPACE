use super::*;
use crate::proto::{KernelArtifact, RuntimeReleaseArtifact};

fn release(runtime_kind: i32, version: &str) -> NodeArtifact {
    NodeArtifact {
        artifact: Some(node_artifact::Artifact::RuntimeRelease(
            RuntimeReleaseArtifact {
                runtime_kind,
                version: version.to_string(),
            },
        )),
    }
}

#[test]
fn an_artifact_is_named_by_what_it_is() {
    let firecracker = pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32;
    assert!(matches!(
        Artifact::parse(Some(&release(firecracker, "1.16.2"))),
        Ok(Artifact::Release(release)) if release.version.as_str() == "1.16.2"
    ));
    let kernel = NodeArtifact {
        artifact: Some(node_artifact::Artifact::Kernel(KernelArtifact {
            version: "6.1.158".to_string(),
        })),
    };
    assert!(matches!(
        Artifact::parse(Some(&kernel)),
        Ok(Artifact::Kernel(version)) if version.as_str() == "6.1.158"
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
        remove_status(RemoveError::Protected {
            subject: "firecracker 1.15.1".to_string()
        })
        .code(),
        tonic::Code::FailedPrecondition
    );
}

/// A discovery that began before the store changed never stays cached,
/// whichever order its store and the change land in.
#[test]
fn an_inventory_discovered_across_a_store_change_is_not_kept() {
    let cache = super::super::InventoryCache::<u32>::default();
    let generation = cache.generation();
    cache.store(std::sync::Arc::new(1), generation);
    assert_eq!(cache.load().as_deref(), Some(&1));

    // Changed while discovering, stored after.
    let generation = cache.generation();
    cache.forget();
    cache.store(std::sync::Arc::new(2), generation);
    assert!(cache.load().is_none());

    // Stored first, changed after.
    let generation = cache.generation();
    cache.store(std::sync::Arc::new(3), generation);
    cache.forget();
    assert!(cache.load().is_none());
}

/// A publication or removal drops the inventory however it ends.
#[test]
fn a_store_change_forgets_the_inventory_on_every_outcome() {
    let cache = RuntimeInventoryCache::default();
    let refused: Result<(), Status> = {
        let _forget = ForgetOnDrop(&cache);
        Err(Status::failed_precondition("refused"))
    };
    assert!(refused.is_err());
    assert_eq!(cache.generation(), 1);
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
