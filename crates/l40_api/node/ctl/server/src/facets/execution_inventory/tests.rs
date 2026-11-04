use super::*;
use pplx_space_execution_common::proto::{RuntimeIdentity, RuntimeInstallation, RuntimeKind};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn collection_outcomes_keep_failure_distinct_from_empty_success() {
    let installed = NodeExecutionInventory {
        policy: Some(
            pplx_space_execution_common::proto::ExecutionPolicyObservation {
                schema_version: 1,
                legacy_unbound_allowed: true,
                allowed_environment_references: vec![format!("sha256:{}", "11".repeat(32))],
                default_environment_reference: Some(format!("sha256:{}", "11".repeat(32))),
                applied_generation: None,
                cluster_policy_unreadable: false,
            },
        ),
        runtimes: vec![RuntimeInstallation {
            runtime: Some(RuntimeIdentity {
                kind: RuntimeKind::Firecracker.into(),
                version: "1.15.1".into(),
            }),
            ..Default::default()
        }],
        components: vec![],
        environments: None,
    };
    for (result, expected) in [
        (
            Ok(NodeExecutionInventory::default()),
            Some(NodeExecutionInventory::default()),
        ),
        (Ok(installed.clone()), Some(installed)),
        (
            Err(CollectionError::Read(tonic::Status::unavailable(
                "manager restarting",
            ))),
            None,
        ),
        (
            Err(CollectionError::Read(tonic::Status::unimplemented(
                "old manager",
            ))),
            None,
        ),
        (
            Err(CollectionError::Read(tonic::Status::deadline_exceeded(
                "request deadline",
            ))),
            None,
        ),
        (
            Err(CollectionError::Read(tonic::Status::data_loss(
                "inventory unavailable",
            ))),
            None,
        ),
    ] {
        let (legacy, observation) = bounded_collection(Some(NodeRole::Worker), async { result })
            .await
            .expect("workers report a collection outcome")
            .into_publication();
        assert_eq!(legacy, expected);
        match expected {
            Some(inventory) => assert_eq!(observation.outcome, Some(Outcome::Complete(inventory))),
            None => assert!(matches!(observation.outcome, Some(Outcome::Unavailable(_)))),
        }
    }
}

struct FetchGuard(Arc<AtomicBool>);

impl Drop for FetchGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_collection_is_cancelled_and_reports_unavailable() {
    let released = Arc::new(AtomicBool::new(false));
    let guard = FetchGuard(released.clone());
    let observation = bounded_collection(Some(NodeRole::Worker), async move {
        let _guard = guard;
        std::future::pending().await
    })
    .await;
    assert!(released.load(Ordering::SeqCst));
    let (legacy, report) = observation
        .expect("workers report a collection outcome")
        .into_publication();
    assert!(legacy.is_none());
    assert!(matches!(report.outcome, Some(Outcome::Unavailable(_))));
}

#[tokio::test]
async fn cluster_role_omits_collection_without_polling_the_manager() {
    for role in [
        None,
        Some(NodeRole::Worker),
        Some(NodeRole::Builder),
        Some(NodeRole::Cluster),
    ] {
        let expected = match role {
            None | Some(NodeRole::Worker | NodeRole::Builder) => {
                Some(NodeExecutionInventory::default())
            }
            Some(NodeRole::Cluster) => None,
        };
        let polled = AtomicBool::new(false);
        let report = bounded_collection(role, async {
            polled.store(true, Ordering::SeqCst);
            Ok(NodeExecutionInventory::default())
        })
        .await;
        assert_eq!(
            polled.load(Ordering::SeqCst),
            expected.is_some(),
            "{role:?}"
        );
        assert_eq!(report.is_some(), expected.is_some(), "{role:?}");
        if let Some(report) = report {
            let (legacy, observation) = report.into_publication();
            assert_eq!(legacy, expected);
            assert_eq!(observation.outcome, expected.map(Outcome::Complete));
        }
    }
}
