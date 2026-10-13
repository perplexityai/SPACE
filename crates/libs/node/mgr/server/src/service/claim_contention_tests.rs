//! Per-sandbox claim contention for the claiming node-manager RPCs.
//!
//! Startup work (Prepare, Launch, LoadSnapshot, device and port setup) shares
//! a sandbox; cleanup, reset, Release and artifact deletion own it
//! exclusively. Concurrent startup calls are legitimate, so each case pins
//! both the refusal and the overlap that must keep succeeding.
//!
//! Requests carry no execution fence and no lease, so an admitted call stops
//! at its first post-claim check, before any host access. That outcome is
//! distinct from every claim refusal. The CleanupSandbox wrapper inspects the
//! fixed cache root whatever its outcome, so cleanup_inspect::tests pins its
//! claim through the cleanup operation instead.

use super::tests::service_tests::service_for_unit_tests;
use super::*;
use futures_util::future::BoxFuture;
use pplx_space_common_core::host::linux::paths::{NET_GATEWAY_SVC_BIN, SPACED_HOST_SVC_BIN};
use pplx_space_common_svc::PeerIdentity;

const WORK_REFUSED: &str = "sandbox cleanup or reset is in progress; retry";
const MUTATION_REFUSED: &str = "sandbox host mutation or reset is in progress; retry";
const WORK_WAIT_TIMED_OUT: &str = "timed out waiting for sandbox cleanup or reset; retry";
const NO_EXECUTION: &str = "sandbox execution identity is missing or superseded";
const NOT_ALLOCATED: &str = "sandbox not allocated";
/// Prepare and LaunchSandbox parse the runtime only under their claim. A
/// valid runtime would go on to discover the host runtime inventory.
const UNSUPPORTED_RUNTIME: &str = "unsupported runtime 'invalid-runtime'";

fn root<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, None));
    request
}

/// The two claim kinds an in-flight RPC can hold on a sandbox.
#[derive(Clone, Copy, Debug)]
enum Holder {
    StartupWork,
    CleanupOrReset,
}

impl Holder {
    fn claim(
        self,
        service: &NodeManagerService,
        sandbox_id: &str,
    ) -> std::sync::Arc<crate::slot_pool::SandboxMutationClaim> {
        match self {
            Self::StartupWork => service.pool.try_claim_sandbox_work(sandbox_id),
            Self::CleanupOrReset => service.pool.try_claim_sandbox_mutation(sandbox_id),
        }
        .expect("fixture claim on an unowned sandbox")
    }
}

type Rpc = fn(NodeManagerService, String) -> BoxFuture<'static, Result<(), Status>>;

/// The first post-claim refusal of a call that carries no execution or lease.
#[derive(Clone, Copy)]
struct Admitted(tonic::Code, &'static str);

struct Case {
    rpc: &'static str,
    call: Rpc,
    admitted: Admitted,
}

impl Case {
    async fn run(&self, service: &NodeManagerService, sandbox_id: &str) -> Result<(), Status> {
        (self.call)(service.clone(), sandbox_id.to_owned()).await
    }

    fn assert_admitted(&self, result: Result<(), Status>, sandbox_id: &str, context: &str) {
        let Admitted(code, message) = self.admitted;
        let message = message.replace("{sandbox_id}", sandbox_id);
        match result {
            Err(error) if error.code() == code && error.message() == message => {}
            result => panic!(
                "{} {context}: unexpected admitted outcome {result:?}",
                self.rpc
            ),
        }
    }
}

fn assert_refused(result: Result<(), Status>, message: &str, context: &str) {
    let error = result.expect_err(context);
    assert_eq!(error.code(), tonic::Code::Unavailable, "{context}: {error}");
    assert_eq!(error.message(), message, "{context}");
}

/// The RPC returned; neither a refused nor an admitted call may keep a share.
fn assert_unowned(service: &NodeManagerService, sandbox_id: &str, context: &str) {
    drop(
        service
            .pool
            .try_claim_sandbox_mutation(sandbox_id)
            .unwrap_or_else(|| panic!("{context}: RPC retained a sandbox claim")),
    );
}

/// Shared-work RPCs that refuse instead of waiting for cleanup or reset.
fn immediate_work_rpcs() -> [Case; 8] {
    [
        Case {
            rpc: "ExposePort",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .expose_port(root(ExposePortRequest {
                            sandbox_id,
                            port: 8080,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::NotFound, NOT_ALLOCATED),
        },
        Case {
            rpc: "UnexposePort",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .unexpose_port(root(UnexposePortRequest {
                            sandbox_id,
                            port: Some(8080),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::NotFound, NOT_ALLOCATED),
        },
        Case {
            rpc: "AllowPort",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .allow_port(root(AllowPortRequest {
                            sandbox_id,
                            port: 8080,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::NotFound, "sandbox not allocated: {sandbox_id}"),
        },
        Case {
            rpc: "DenyPort",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .deny_port(root(DenyPortRequest {
                            sandbox_id,
                            port: Some(8080),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::NotFound, NOT_ALLOCATED),
        },
        Case {
            rpc: "PrepareEncryptedRootfs",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .prepare_encrypted_rootfs(root(PrepareEncryptedRootfsRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "PrepareSnapshotRootfs",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .prepare_snapshot_rootfs(root(PrepareSnapshotRootfsRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "PrepareEncryptedVolume",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .prepare_encrypted_volume(root(PrepareEncryptedVolumeRequest {
                            sandbox_id,
                            volume_name: "snapshot".into(),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "ReleaseEncryptedVolume",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .release_encrypted_volume(root(ReleaseEncryptedVolumeRequest {
                            sandbox_id,
                            volume_name: "snapshot".into(),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
    ]
}

/// Startup RPCs that wait, bounded, for cleanup or reset to release the sandbox.
fn waiting_work_rpcs() -> [Case; 9] {
    [
        Case {
            rpc: "Allocate",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .allocate(root(AllocateRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(
                tonic::Code::FailedPrecondition,
                "execution identity is required",
            ),
        },
        Case {
            rpc: "PrepareSandbox",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .prepare_sandbox(root(PrepareSandboxRequest {
                            sandbox_id,
                            runtime: "invalid-runtime".into(),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::InvalidArgument, UNSUPPORTED_RUNTIME),
        },
        Case {
            rpc: "LaunchSandbox",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .launch_sandbox(root(LaunchSandboxRequest {
                            sandbox_id,
                            runtime: "invalid-runtime".into(),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::InvalidArgument, UNSUPPORTED_RUNTIME),
        },
        Case {
            rpc: "Launch",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .launch(root(LaunchRequest {
                            sandbox_id,
                            program: NET_GATEWAY_SVC_BIN.into(),
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "LoadSnapshot",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .load_snapshot(root(LoadSnapshotRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "CreateSnapshot",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .create_snapshot(root(CreateSnapshotRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "PauseSandbox",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .pause_sandbox(root(PauseSandboxRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "ResumeSandbox",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .resume_sandbox(root(ResumeSandboxRequest {
                            sandbox_id,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
        Case {
            rpc: "StopProcess",
            call: |service, sandbox_id| {
                Box::pin(async move {
                    service
                        .stop_process(root(StopProcessRequest {
                            sandbox_id,
                            signal: libc::SIGTERM as u32,
                            ..Default::default()
                        }))
                        .await
                        .map(drop)
                })
            },
            admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
        },
    ]
}

/// Release owns the sandbox exclusively and refuses any concurrent claim.
fn release() -> Case {
    Case {
        rpc: "Release",
        call: |service, sandbox_id| {
            Box::pin(async move {
                service
                    .release(root(ReleaseRequest {
                        sandbox_id,
                        ..Default::default()
                    }))
                    .await
                    .map(drop)
            })
        },
        admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
    }
}

/// Artifact deletion also owns the sandbox exclusively, but once admitted it
/// stats paths under the fixed cache root, so only its refusal runs here.
fn artifact_deletion_rpcs() -> [(&'static str, Rpc); 2] {
    [
        ("DeleteBtrfsSnapshot", |service, sandbox_id| {
            Box::pin(async move {
                service
                    .delete_btrfs_snapshot(root(DeleteBtrfsSnapshotRequest {
                        sandbox_id,
                        snapshot_name: "d-1m-20260922T120000Z".into(),
                    }))
                    .await
                    .map(drop)
            })
        }),
        ("DeleteCacheArtifact", |service, sandbox_id| {
            Box::pin(async move {
                service
                    .delete_cache_artifact(root(DeleteCacheArtifactRequest {
                        local_path: format!(
                            "{CACHE_DIR}/{sandbox_id}/snapshots/.suspend-gc-pending-{sandbox_id}"
                        ),
                    }))
                    .await
                    .map(drop)
            })
        }),
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn shared_work_rpcs_refuse_cleanup_or_reset_and_overlap_startup_work() {
    for case in immediate_work_rpcs() {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();

        let owner = Holder::CleanupOrReset.claim(&service, &sandbox_id);
        let context = format!("{} during cleanup or reset", case.rpc);
        assert_refused(
            case.run(&service, &sandbox_id).await,
            WORK_REFUSED,
            &context,
        );
        drop(owner);
        assert_unowned(&service, &sandbox_id, &context);

        let startup = Holder::StartupWork.claim(&service, &sandbox_id);
        let context = format!("{} during startup work", case.rpc);
        case.assert_admitted(case.run(&service, &sandbox_id).await, &sandbox_id, &context);
        assert!(
            service
                .pool
                .try_claim_sandbox_mutation(&sandbox_id)
                .is_none(),
            "{context}: the in-flight startup share must survive"
        );
        drop(startup);
        assert_unowned(&service, &sandbox_id, &context);
    }
}

#[tokio::test(start_paused = true)]
async fn clone_refuses_either_owned_sandbox_without_retaining_the_other() {
    let service = service_for_unit_tests().await;
    let mut ids = [SandboxId::new().to_string(), SandboxId::new().to_string()];
    ids.sort();
    let [low, high] = ids;
    let clone = |donor: &str, target: &str| {
        service.clone_sandbox_into_prepared_root(root(CloneSandboxRequest {
            donor_sandbox_id: donor.to_owned(),
            new_sandbox_id: target.to_owned(),
            template: "seed".into(),
            prepared_destination: Some(crate::proto::CloneDirectoryIdentity {
                device: 1,
                inode: 1,
            }),
        }))
    };
    // Claims are taken in id order, so cover a refusal of the first and of
    // the second claim from both clone directions.
    for (donor, target) in [(&low, &high), (&high, &low)] {
        for (owned, other) in [(donor, target), (target, donor)] {
            // Holding the owned sandbox's cgroup gate stops any clone that
            // gets past both claims before it opens the clone tree. That is
            // the in-flight launch below, and a barrier for a regressed
            // refusal here.
            let launch_gate = service
                .sandbox_cgroup_gate_lock(owned)
                .await
                .read_owned()
                .await;
            let context = format!("clone {donor} -> {target} with {owned} in cleanup");
            let owner = Holder::CleanupOrReset.claim(&service, owned);
            assert_refused(clone(donor, target).await.map(drop), WORK_REFUSED, &context);
            assert_unowned(&service, other, &context);
            drop(owner);

            // A donor or target with in-flight startup work may still clone.
            let context = format!("clone {donor} -> {target} with {owned} launching");
            let startup = Holder::StartupWork.claim(&service, owned);
            let error = clone(donor, target).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable, "{context}");
            assert_eq!(
                error.message(),
                "clone lifecycle ownership is busy",
                "{context}"
            );
            drop(launch_gate);
            assert!(service.pool.try_claim_sandbox_mutation(owned).is_none());
            assert_unowned(&service, other, &context);
            drop(startup);
            assert_unowned(&service, owned, &context);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exclusive_rpcs_refuse_any_claim_and_release_ownership_when_done() {
    let case = release();
    let service = service_for_unit_tests().await;
    let sandbox_id = SandboxId::new().to_string();
    for holder in [Holder::StartupWork, Holder::CleanupOrReset] {
        let context = format!("{} during {holder:?}", case.rpc);
        let owner = holder.claim(&service, &sandbox_id);
        assert_refused(
            case.run(&service, &sandbox_id).await,
            MUTATION_REFUSED,
            &context,
        );
        drop(owner);
        assert_unowned(&service, &sandbox_id, &context);
    }
    let context = format!("{} uncontended", case.rpc);
    case.assert_admitted(case.run(&service, &sandbox_id).await, &sandbox_id, &context);
    assert_unowned(&service, &sandbox_id, &context);

    // filesystem_work::tests pins their refusal of shared host work.
    for (rpc, call) in artifact_deletion_rpcs() {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();
        let context = format!("{rpc} during cleanup or reset");
        let owner = Holder::CleanupOrReset.claim(&service, &sandbox_id);
        assert_refused(
            call(service.clone(), sandbox_id.clone()).await,
            MUTATION_REFUSED,
            &context,
        );
        drop(owner);
        assert_unowned(&service, &sandbox_id, &context);
    }
}

#[tokio::test(start_paused = true)]
async fn startup_rpcs_time_out_behind_cleanup_without_taking_ownership() {
    for case in waiting_work_rpcs() {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();
        let owner = Holder::CleanupOrReset.claim(&service, &sandbox_id);
        let context = format!("{} behind cleanup or reset", case.rpc);
        assert_refused(
            case.run(&service, &sandbox_id).await,
            WORK_WAIT_TIMED_OUT,
            &context,
        );
        drop(owner);
        assert_unowned(&service, &sandbox_id, &context);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn startup_rpcs_overlap_startup_work_and_proceed_once_cleanup_releases() {
    // startup_rpc_admission_shares_work_and_waits_for_cleanup pins both halves
    // for PrepareSandbox and LoadSnapshot.
    for case in waiting_work_rpcs()
        .into_iter()
        .filter(|case| !matches!(case.rpc, "PrepareSandbox" | "LoadSnapshot"))
    {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();

        let startup = Holder::StartupWork.claim(&service, &sandbox_id);
        let context = format!("{} during startup work", case.rpc);
        case.assert_admitted(case.run(&service, &sandbox_id).await, &sandbox_id, &context);
        assert!(
            service
                .pool
                .try_claim_sandbox_mutation(&sandbox_id)
                .is_none()
        );
        drop(startup);
        assert_unowned(&service, &sandbox_id, &context);

        let owner = Holder::CleanupOrReset.claim(&service, &sandbox_id);
        let context = format!("{} after cleanup or reset", case.rpc);
        let mut call = Box::pin(case.run(&service, &sandbox_id));
        assert!(
            futures_util::poll!(&mut call).is_pending(),
            "{context}: must wait rather than refuse"
        );
        drop(owner);
        case.assert_admitted(call.await, &sandbox_id, &context);
        assert_unowned(&service, &sandbox_id, &context);
    }
}

/// A regression that admits an overlapping RPC into the parked call's cgroup
/// gate deadlocks instead of failing, so every call in that test is bounded.
async fn returns<T>(call: impl std::future::Future<Output = T>, context: &str) -> T {
    tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .unwrap_or_else(|_| {
            panic!("{context}: did not return within 5 s; deadlocked on the parked call?")
        })
}

fn case(rpc: &str) -> Case {
    waiting_work_rpcs()
        .into_iter()
        .chain([release()])
        .find(|case| case.rpc == rpc)
        .unwrap_or_else(|| panic!("no case for {rpc}"))
}

/// Parks one startup RPC inside its resource-family gate, where it already
/// holds its shared sandbox claim, then issues the overlapping calls a
/// concurrent activation or restore makes.
#[tokio::test(flavor = "current_thread")]
async fn in_flight_launch_or_snapshot_load_admits_concurrent_startup_rpcs() {
    enum Family {
        Vm,
        Sidecar,
    }
    // The table's Launch starts the network gateway sidecar.
    let gateway = case("Launch");
    let spaced_host = Case {
        rpc: "Launch(spaced-host)",
        call: |service, sandbox_id| {
            Box::pin(async move {
                service
                    .launch(root(LaunchRequest {
                        sandbox_id,
                        program: SPACED_HOST_SVC_BIN.into(),
                        ..Default::default()
                    }))
                    .await
                    .map(drop)
            })
        },
        admitted: Admitted(tonic::Code::FailedPrecondition, NO_EXECUTION),
    };
    let prepare = case("PrepareSandbox");
    let load = case("LoadSnapshot");
    let release = release();

    for (in_flight, family, overlapping) in [
        // Prepare and snapshot load overlap a gateway launch during activation.
        (&gateway, Family::Sidecar, vec![&prepare, &load]),
        // Sidecar launches overlap snapshot load during restore.
        (&load, Family::Vm, vec![&prepare, &spaced_host, &gateway]),
    ] {
        let service = service_for_unit_tests().await;
        let sandbox_id = SandboxId::new().to_string();
        let family_locks = match family {
            Family::Vm => &service.sandbox_vm_cgroup_locks,
            Family::Sidecar => &service.sandbox_sidecar_cgroup_locks,
        };
        let parked = NodeManagerService::sandbox_cgroup_family_lock(family_locks, &sandbox_id)
            .await
            .lock_owned()
            .await;
        let mut first = Box::pin(in_flight.run(&service, &sandbox_id));
        assert!(
            futures_util::poll!(&mut first).is_pending(),
            "{}",
            in_flight.rpc
        );

        for case in overlapping {
            let context = format!("{} during {}", case.rpc, in_flight.rpc);
            let result = returns(case.run(&service, &sandbox_id), &context).await;
            case.assert_admitted(result, &sandbox_id, &context);
        }
        let context = format!("Release during {}", in_flight.rpc);
        let result = returns(release.run(&service, &sandbox_id), &context).await;
        assert_refused(result, MUTATION_REFUSED, &context);
        assert!(
            futures_util::poll!(&mut first).is_pending(),
            "{}",
            in_flight.rpc
        );

        drop(parked);
        let context = format!("{} after its gate opened", in_flight.rpc);
        in_flight.assert_admitted(returns(first, &context).await, &sandbox_id, &context);
        assert_unowned(&service, &sandbox_id, in_flight.rpc);
    }
}
