//! An ordinary cold start of a suspended encrypted sandbox with the staged
//! checkpoint disk switch on, as a runtime upgrade boots it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_node_mgr_client::proto::{
    AllocateRequest, AllocateResponse, CleanupSandboxRequest, CleanupSandboxResponse,
    ExecutionEnvironmentSelection, NetworkPoolStatusRequest, NetworkPoolStatusResponse,
    PrepareEncryptedRootfsRequest, PrepareEncryptedRootfsResponse, PrepareSandboxRequest,
    PrepareSandboxResponse, ResolveExecutionEnvironmentRequest,
    ResolveExecutionEnvironmentResponse,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use tonic::Status;

use crate::service::{SandboxManagerService, SandboxManagerServiceOptions};
use crate::state::SandboxState;

const DIGEST: &str = "sha256:5e0f1d2c3b4a59687766554433221100ffeeddccbbaa99887766554433221100";
const INSTANCE_COW: &[u8] = b"the instance's guest writes";

struct Fixture {
    service: SandboxManagerService,
    id: String,
    sandbox_dir: std::path::PathBuf,
    /// Each `PrepareEncryptedRootfs` request's `restore_cow_source`.
    sent: Arc<Mutex<Vec<Option<String>>>>,
    _node: crate::node_manager_transport::fake::ServedNodeManager,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

/// A suspended encrypted sandbox whose sidecar names the artifact and
/// template the row pins, with `checkpoint_cow` as the checkpoint's COW and
/// [`INSTANCE_COW`] as the instance's. node-manager refuses the encrypted
/// prepare, so a start stops there once it's asked.
fn fixture(checkpoint_cow: &[u8]) -> Fixture {
    use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
    use pplx_space_cpu_arch::Arch;
    use pplx_space_execution_spec::RuntimeKind;
    use pplx_space_sandbox_model::SandboxRuntimeLabel;
    use pplx_space_template_mgr_client::proto::GetTemplateResponse;

    let compiled =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let definition = compiled[0].definition().clone();
    let producer = definition.execution_requirements().unwrap();
    let template = tempfile::tempdir().unwrap();
    std::fs::write(
        template.path().join("metadata.json"),
        serde_json::json!({
            "arch": std::env::consts::ARCH,
            "spaced_version": pplx_space_spaced_protocol::spaced_version(),
            "spaced_compatibility_family": pplx_space_spaced_guest_protocol::spaced_compatibility_family(),
        })
        .to_string(),
    )
    .unwrap();
    let template_dir = template.path().to_str().unwrap().to_owned();
    let templates = Arc::new(crate::template_resolver::fake::FakeTemplateResolver::new(
        move || {
            Ok(tonic::Response::new(GetTemplateResponse {
                template_dir: template_dir.clone(),
                arch: std::env::consts::ARCH.into(),
                network_policy_authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
                auto_lifecycle_authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
                ..Default::default()
            }))
        },
    ));

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    crate::db::insert_sandbox(
        &conn,
        crate::db::InsertSandboxParams {
            id: &id,
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
            state: SandboxState::Suspended.into(),
            template: "active-template",
            template_hash: Some(DIGEST),
            user: &pplx_space_accounts::SandboxOwner::parse("user").unwrap(),
            runtime: RuntimeKind::Firecracker.alias(),
            guest_image: Default::default(),
            snapshots_enabled: false,
            rootfs_size_mb: Some(2048),
            disk_io_limits: Default::default(),
            // A pool member skips the effective network policy lookup.
            pool_template: Some("pool-template"),
            volume_encryption: true,
            process_env: &Default::default(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
    let pin = LocalArtifactId::new();
    let cache = tempfile::tempdir().unwrap();
    let sandbox_dir = cache.path().join(&id);
    assert!(
        crate::db::adopt_suspended_local_artifact(
            &conn,
            &id,
            pin,
            &sandbox_dir.join("snapshot").to_string_lossy(),
        )
        .unwrap()
    );
    std::fs::create_dir_all(sandbox_dir.join("instance")).unwrap();
    std::fs::create_dir_all(sandbox_dir.join("snapshot")).unwrap();
    std::fs::write(sandbox_dir.join("instance/cow.luks"), INSTANCE_COW).unwrap();
    std::fs::write(sandbox_dir.join("snapshot/cow.luks"), checkpoint_cow).unwrap();
    std::fs::write(
        sandbox_dir.join("snapshot/snapshot.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "sandbox_id": id,
            "size_bytes": 1,
            "created_at": "2026-11-09T00:00:00Z",
            "source": {"runtime": "fc", "template_id": "seed", "content_hash": DIGEST},
            "encrypted": true,
            "local_artifact_id": pin.to_string(),
            "artifact_kind": "full",
            "artifact_origin": "suspend",
        }))
        .unwrap(),
    )
    .unwrap();

    let sent = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&sent);
    let selected = producer.clone();
    let node = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary("GetNetworkPoolStatus", |_: NetworkPoolStatusRequest| {
            Ok(NetworkPoolStatusResponse {
                sandbox_cgroup_memory_mib: 4096,
                sandbox_guest_memory_max_mib: 3968,
                sandbox_memory_headroom_mib: 128,
                ..Default::default()
            })
        })
        .unary(
            "ResolveExecutionEnvironment",
            move |_: ResolveExecutionEnvironmentRequest| {
                Ok(ResolveExecutionEnvironmentResponse {
                    accepted_selection: ExecutionEnvironmentSelection::ColdBoot as i32,
                    definition: Some((&definition).into()),
                    execution: Some((&selected).into()),
                })
            },
        )
        .unary("Allocate", |_: AllocateRequest| {
            Ok(AllocateResponse {
                sandbox_uid: 10_000,
                ..Default::default()
            })
        })
        .unary("PrepareSandbox", |request: PrepareSandboxRequest| {
            Ok(PrepareSandboxResponse {
                resolved_execution: request.requested_execution,
                ..Default::default()
            })
        })
        .unary("CleanupSandbox", |_: CleanupSandboxRequest| {
            Ok(CleanupSandboxResponse::default())
        })
        .unary(
            "PrepareEncryptedRootfs",
            move |request: PrepareEncryptedRootfsRequest| -> Result<PrepareEncryptedRootfsResponse, Status> {
                recorded.lock().unwrap().push(request.restore_cow_source);
                Err(Status::failed_precondition("encrypted prepare refused by the test"))
            },
        )
        .serve();
    let mut service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            template_resolver: templates,
            create_cache_dir: Some(cache.path().to_path_buf()),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    service.staged_checkpoint_disk = true;
    Fixture {
        service,
        id,
        sandbox_dir,
        sent,
        _node: node,
        _dirs: (template, cache),
    }
}

async fn cold_start(fixture: &Fixture) -> Status {
    tokio::time::timeout(
        Duration::from_secs(30),
        fixture.service.start_sandbox_with_upgrade_authority(
            crate::proto::StartRequest {
                sandbox_id: fixture.id.clone(),
                ..Default::default()
            },
            false,
            crate::service::activation_support::IdentityFreeDirectActivationAuthority::LocalProducer,
            crate::service::start::StartAdmission::default(),
        ),
    )
    .await
    .expect("the start finishes within 30 seconds")
    .expect_err("the start stops at the encrypted prepare")
}

/// The failed attempt's cleanup has settled the row, as a retry finds it.
async fn settled(fixture: &Fixture) -> crate::state::SandboxStatus {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let state = crate::db::get_sandbox(&fixture.service.db.lock().unwrap(), &fixture.id)
                .unwrap()
                .unwrap()
                .state;
            if !matches!(state, crate::state::SandboxStatus::Owned { .. }) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the failed attempt's cleanup settles within 30 seconds")
}

async fn shut_down(fixture: Fixture) {
    fixture.service.shutdown_token.cancel();
    fixture.service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), fixture.service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

/// node-manager replaces the instance's COW with the one it's sent, so an
/// empty pinned checkpoint COW is refused before it's asked, on the first
/// attempt and on its retry, and the instance's COW keeps its bytes.
#[tokio::test]
async fn an_empty_pinned_checkpoint_cow_is_refused_on_every_cold_start() {
    let fixture = fixture(b"");
    for attempt in ["first", "retry"] {
        let error = cold_start(&fixture).await;
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{attempt}: {error}"
        );
        assert!(
            error
                .message()
                .contains("encrypted rootfs preparation failed")
                && error.message().contains("single-link regular file"),
            "{attempt}: the pinned COW's check refuses it: {error}"
        );
        assert!(
            fixture.sent.lock().unwrap().is_empty(),
            "{attempt}: node-manager was asked"
        );
        assert_eq!(
            std::fs::read(fixture.sandbox_dir.join("instance/cow.luks")).unwrap(),
            INSTANCE_COW,
            "{attempt}"
        );
        assert_eq!(
            settled(&fixture).await,
            crate::state::SandboxStatus::from(SandboxState::Suspended),
            "{attempt}: the cleanup returns the row to suspended"
        );
    }
    shut_down(fixture).await;
}

/// A non-empty pinned checkpoint COW is what the same start sends.
#[tokio::test]
async fn a_pinned_checkpoint_cow_is_what_a_cold_start_lays() {
    let fixture = fixture(b"sealed");
    let error = cold_start(&fixture).await;
    assert!(
        error
            .message()
            .contains("encrypted prepare refused by the test"),
        "{error}"
    );
    assert_eq!(
        *fixture.sent.lock().unwrap(),
        vec![Some(
            fixture
                .sandbox_dir
                .join("snapshot/cow.luks")
                .to_string_lossy()
                .into_owned()
        )]
    );
    shut_down(fixture).await;
}
