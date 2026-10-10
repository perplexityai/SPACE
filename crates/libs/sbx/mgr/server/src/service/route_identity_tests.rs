//! Route verification bundle refresh, authoritative route identity adoption,
//! and resident resume admission up to the node-manager call.
//!
//! Every test runs in its own child process with route, node, and bundle
//! environment removed or pointed at test-owned files. Services use temporary
//! socket paths, an explicit node identity, and a shutdown token and task
//! tracker that each test drains.

use super::{
    RouteStreamFence, RouteVerificationBundleRefresh, RouteVerificationBundleRefreshError,
    SandboxManagerService, SandboxManagerServiceOptions, SandboxState, SpacedRpc,
};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::Ed25519KeyPair;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecyclePolicy, AutoLifecycleStage, CLUSTER_ID_ENV, ClusterId, ExecutionGeneration,
    NodeId, NodeManagedBy, PodUid, RouteGeneration, SandboxActivationAttemptId, SandboxId,
    SandboxOwner, SandboxRouteIdentity, SandboxRuntimeLabel, SpaceApiRouteIdentity,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV,
    SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV, SpaceApiRouteEd25519Claims,
    SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId, SpaceApiRouteEd25519Signer,
    insert_space_api_route_ed25519_token,
};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::Code;

const ROUTE_TEST_CLUSTER: &str = "route-test";
const ROUTE_TEST_ISSUER: &str = "route-test-authority";

/// Isolate bundle, cluster, node, and route-fence configuration from the host.
fn enter_route_identity_test_process(bundle_path: Option<&OsStr>) -> bool {
    crate::service::tests::enter_route_test_process(&[
        (
            SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV,
            bundle_path,
        ),
        (SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV, None),
        (CLUSTER_ID_ENV, Some(OsStr::new(ROUTE_TEST_CLUSTER))),
    ])
}

fn local_node() -> NodeId {
    NodeId::from_uuid(uuid::Uuid::from_u128(
        0x019f_8000_0000_7000_8000_0000_0000_00fe,
    ))
}

/// A service whose background tasks belong to the test and whose sockets live
/// in a test-owned directory.
struct RouteTestService {
    service: SandboxManagerService,
    shutdown: CancellationToken,
    tasks: TaskTracker,
    _sockets: tempfile::TempDir,
}

impl RouteTestService {
    fn new(conn: rusqlite::Connection) -> Self {
        let sockets = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        let tasks = TaskTracker::new();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                host_control_socket_path: sockets.path().join("host-control.sock"),
                credential_manager_socket_path: sockets.path().join("credmgr.sock"),
                sandbox_runtime_live: Arc::new(|_| false),
                shutdown_token: shutdown.clone(),
                task_tracker: tasks.clone(),
                local_node_id: Some(local_node()),
                node_role: None,
                node_managed_by: None,
                route_ed25519_verification_bundle: None,
            },
        );
        Self {
            service,
            shutdown,
            tasks,
            _sockets: sockets,
        }
    }

    async fn shut_down(self) {
        self.shutdown.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }
}

impl std::ops::Deref for RouteTestService {
    type Target = SandboxManagerService;

    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

fn insert_sandbox(
    conn: &rusqlite::Connection,
    sandbox_id: SandboxId,
    state: SandboxState,
    pool_template: Option<&str>,
    auto_lifecycle: &AutoLifecyclePolicy,
) {
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            id: &sandbox_id.to_string(),
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
            state,
            template: "seed",
            template_hash: Some("hash"),
            user: &SandboxOwner::parse("user").unwrap(),
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template,
            volume_encryption: false,
            process_env: &BTreeMap::new(),
            auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
}

fn access_idle_policy() -> AutoLifecyclePolicy {
    AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 300,
            action: AutoLifecycleIdleAction::Pause,
        }],
    }
    .normalized()
}

fn kubernetes_identity(sandbox_id: SandboxId, generation: u64) -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        sandbox_id: Some(sandbox_id),
        route_generation: Some(RouteGeneration::new(generation).unwrap()),
        current_node_id: Some(local_node()),
        substrate_kind: Some(NodeManagedBy::Kubernetes),
        execution_generation: Some(
            ExecutionGeneration::new(i64::try_from(generation).unwrap()).unwrap(),
        ),
        pod_uid: Some(PodUid::parse("route-test-pod").unwrap()),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
    }
}

/// A started Kubernetes row whose complete route identity was recorded
/// without cluster fencing.
fn started_unfenced_sandbox(conn: &rusqlite::Connection) -> (SandboxId, SpaceApiRouteIdentity) {
    let sandbox_id = SandboxId::new();
    insert_sandbox(
        conn,
        sandbox_id,
        SandboxState::Started,
        None,
        &AutoLifecyclePolicy::disabled(),
    );
    let identity = kubernetes_identity(sandbox_id, 4);
    crate::db::update_space_api_route_identity(
        conn,
        sandbox_id,
        &identity,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    (sandbox_id, identity)
}

fn route_identity_provenance(service: &SandboxManagerService, sandbox_id: SandboxId) -> String {
    service
        .db_conn()
        .unwrap()
        .query_row(
            "SELECT space_api_route_identity_provenance FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap()
}

/// Attach a route stream to the sandbox's current persisted identity.
fn open_route_stream(service: &SandboxManagerService, sandbox_id: SandboxId) -> RouteStreamFence {
    let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
    service.route_stream_identity_token(&row).unwrap()
}

#[tokio::test]
async fn route_identity_adoption_without_an_identity_changes_nothing() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    let conn = crate::db::test_conn();
    let (sandbox_id, identity) = started_unfenced_sandbox(&conn);
    let service = RouteTestService::new(conn);
    let stream = open_route_stream(&service, sandbox_id);

    // A wake that carries no route identity has nothing to adopt, whether or
    // not the sandbox exists locally.
    for target in [sandbox_id, SandboxId::new()] {
        service
            .adopt_authoritative_route_identity(target, &SpaceApiRouteIdentity::default())
            .unwrap();
    }

    let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
    assert_eq!(row.space_api_route_identity, identity);
    assert_eq!(route_identity_provenance(&service, sandbox_id), "unfenced");
    assert!(!stream.child_token().is_cancelled());
    service.shut_down().await;
}

#[tokio::test]
async fn route_identity_adoption_requires_a_readable_local_row() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    for (store_readable, expected) in [(true, Code::NotFound), (false, Code::Internal)] {
        let service = RouteTestService::new(crate::db::test_conn());
        if !store_readable {
            service
                .db_conn()
                .unwrap()
                .execute_batch("DROP TABLE sandboxes")
                .unwrap();
        }
        let sandbox_id = SandboxId::new();

        let error = service
            .adopt_authoritative_route_identity(sandbox_id, &kubernetes_identity(sandbox_id, 4))
            .expect_err("adoption needs the local row it would update");

        assert_eq!(error.code(), expected, "store readable: {store_readable}");
        service.shut_down().await;
    }
}

#[derive(Clone, Copy, Debug)]
enum ExecutionOwner {
    Cleanup,
    PendingDeleteReceipt,
}

#[tokio::test]
async fn identity_equal_adoption_does_not_overwrite_an_owned_execution() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    for owner in [
        ExecutionOwner::Cleanup,
        ExecutionOwner::PendingDeleteReceipt,
    ] {
        let conn = crate::db::test_conn();
        let (sandbox_id, identity) = started_unfenced_sandbox(&conn);
        let service = RouteTestService::new(conn);
        let stream = open_route_stream(&service, sandbox_id);
        {
            let db = service.db_conn().unwrap();
            match owner {
                ExecutionOwner::Cleanup => {
                    db.execute(
                        "UPDATE sandboxes
                            SET state = 'deleting',
                                cleanup_completion = 'delete_row',
                                cleanup_keep_filesystem = 0
                          WHERE id = ?1",
                        [sandbox_id.to_string()],
                    )
                    .unwrap();
                }
                ExecutionOwner::PendingDeleteReceipt => {
                    crate::auto_lifecycle_delete_receipts::db::insert_pending_receipt(
                        &db,
                        crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
                            receipt_id:
                                pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
                            sandbox_id,
                            execution_generation: identity.execution_generation.unwrap(),
                            route_activation_id: identity.activation_attempt_id.unwrap(),
                            policy_fingerprint:
                                pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                                    &AutoLifecyclePolicy::disabled(),
                                ),
                        },
                    )
                    .unwrap();
                }
            }
        }

        // The route fields already match, so only the provenance would be
        // promoted; the execution's current owner refuses even that write.
        let error = service
            .adopt_authoritative_route_identity(sandbox_id, &identity)
            .expect_err("an owned execution refuses route metadata promotion");

        assert_eq!(error.code(), Code::Aborted, "{owner:?}");
        assert_eq!(
            route_identity_provenance(&service, sandbox_id),
            "unfenced",
            "{owner:?}"
        );
        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.space_api_route_identity, identity, "{owner:?}");
        assert!(!stream.child_token().is_cancelled(), "{owner:?}");
        service.shut_down().await;
    }
}

struct RouteSigningKey {
    signer: SpaceApiRouteEd25519Signer,
}

impl RouteSigningKey {
    fn generate(key_id: &str) -> Self {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        Self {
            signer: SpaceApiRouteEd25519Signer::from_pkcs8(
                SpaceApiRouteEd25519KeyId::parse(key_id).unwrap(),
                pkcs8.as_ref(),
            )
            .unwrap(),
        }
    }

    fn bundle_json(&self, generation: u64) -> String {
        let key = self.signer.public_key();
        serde_json::json!({
            "generation": generation,
            "issuer": ROUTE_TEST_ISSUER,
            "keys": [{
                "key_id": key.key_id.as_str(),
                "public_key_base64url": URL_SAFE_NO_PAD.encode(&key.public_key),
            }],
        })
        .to_string()
    }

    fn bridge_request(&self, identity: &SandboxRouteIdentity) -> tonic::Request<()> {
        let claims = SpaceApiRouteEd25519Claims::space_api_bridge(
            SpaceApiRouteEd25519Issuer::parse(ROUTE_TEST_ISSUER).unwrap(),
            ClusterId::parse(ROUTE_TEST_CLUSTER).unwrap(),
            identity.clone(),
        )
        .unwrap();
        let token = self.signer.mint(&claims).unwrap();
        let mut request = tonic::Request::new(());
        insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
        request
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RefreshOutcome {
    Updated(u64),
    Unchanged(u64),
    Absent,
    Invalid,
    Missing(u64),
    Stale { current: u64, incoming: u64 },
    Conflict(u64),
}

fn refresh_outcome(
    result: Result<RouteVerificationBundleRefresh, RouteVerificationBundleRefreshError>,
) -> RefreshOutcome {
    match result {
        Ok(RouteVerificationBundleRefresh::Updated(generation)) => {
            RefreshOutcome::Updated(generation.get())
        }
        Ok(RouteVerificationBundleRefresh::Unchanged(generation)) => {
            RefreshOutcome::Unchanged(generation.get())
        }
        Ok(RouteVerificationBundleRefresh::Absent) => RefreshOutcome::Absent,
        Err(RouteVerificationBundleRefreshError::Invalid(_)) => RefreshOutcome::Invalid,
        Err(RouteVerificationBundleRefreshError::Missing { generation }) => {
            RefreshOutcome::Missing(generation.get())
        }
        Err(RouteVerificationBundleRefreshError::Stale { current, incoming }) => {
            RefreshOutcome::Stale {
                current: current.get(),
                incoming: incoming.get(),
            }
        }
        Err(RouteVerificationBundleRefreshError::Conflict { generation }) => {
            RefreshOutcome::Conflict(generation.get())
        }
        Err(RouteVerificationBundleRefreshError::Internal(message)) => {
            panic!("unexpected internal refresh failure: {message}")
        }
    }
}

#[tokio::test]
async fn route_verification_bundle_refresh_installs_only_forward_updates() {
    let bundle_dir = tempfile::tempdir().unwrap();
    let bundle_path = bundle_dir.path().join("route-verification-bundle.json");
    if !enter_route_identity_test_process(Some(bundle_path.as_os_str())) {
        return;
    }
    // The child reads the parent-owned path the loader was configured with.
    let bundle_path = PathBuf::from(
        std::env::var_os(SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV).unwrap(),
    );
    let service = RouteTestService::new(crate::db::test_conn());
    let key_a = RouteSigningKey::generate("route-key-a");
    let key_b = RouteSigningKey::generate("route-key-b");
    let identity = SandboxRouteIdentity::try_new(
        SandboxId::new(),
        RouteGeneration::new(4).unwrap(),
        local_node(),
        NodeManagedBy::Kubernetes,
        ExecutionGeneration::new(4).unwrap(),
        SandboxActivationAttemptId::new(),
        Some(PodUid::parse("route-test-pod").unwrap()),
    )
    .unwrap();
    let sandbox_id = identity.sandbox_id().to_string();
    let accepts = |key: &RouteSigningKey| {
        service
            .prepare_space_api_bridge(
                &key.bridge_request(&identity),
                &sandbox_id,
                SpacedRpc::PingGuest,
            )
            .is_ok()
    };

    // (bundle file contents, refresh outcome, route keys accepted afterwards)
    let steps: [(Option<String>, RefreshOutcome, [bool; 2]); 10] = [
        (None, RefreshOutcome::Invalid, [false, false]),
        (Some(String::new()), RefreshOutcome::Absent, [false, false]),
        (
            Some(key_a.bundle_json(1)),
            RefreshOutcome::Updated(1),
            [true, false],
        ),
        (
            Some(key_a.bundle_json(1)),
            RefreshOutcome::Unchanged(1),
            [true, false],
        ),
        (
            Some(key_b.bundle_json(1)),
            RefreshOutcome::Conflict(1),
            [true, false],
        ),
        (
            Some(key_b.bundle_json(2)),
            RefreshOutcome::Updated(2),
            [false, true],
        ),
        (
            Some(key_a.bundle_json(1)),
            RefreshOutcome::Stale {
                current: 2,
                incoming: 1,
            },
            [false, true],
        ),
        (
            Some("{not a bundle".to_string()),
            RefreshOutcome::Invalid,
            [false, true],
        ),
        (
            Some(" \n".to_string()),
            RefreshOutcome::Missing(2),
            [false, true],
        ),
        (None, RefreshOutcome::Invalid, [false, true]),
    ];
    for (step, (contents, expected, accepted)) in steps.into_iter().enumerate() {
        match contents {
            Some(contents) => std::fs::write(&bundle_path, contents).unwrap(),
            None => {
                if bundle_path.exists() {
                    std::fs::remove_file(&bundle_path).unwrap();
                }
            }
        }

        let outcome = refresh_outcome(service.refresh_route_ed25519_verification_bundle());

        assert_eq!(outcome, expected, "step {step}");
        assert_eq!([accepts(&key_a), accepts(&key_b)], accepted, "step {step}");
    }

    // The periodic refresher re-distributes keys only after an installed
    // update.
    let unconfigured = RouteTestService::new(crate::db::test_conn());
    std::fs::write(&bundle_path, "").unwrap();
    assert!(!unconfigured.refresh_route_ed25519_verification_bundle_now());
    unconfigured.shut_down().await;
    let key_c = RouteSigningKey::generate("route-key-c");
    for (contents, updated, accepted) in [
        (key_c.bundle_json(3), true, true),
        (key_c.bundle_json(3), false, true),
        (key_a.bundle_json(2), false, true),
        (String::new(), false, true),
    ] {
        std::fs::write(&bundle_path, contents).unwrap();
        assert_eq!(
            service.refresh_route_ed25519_verification_bundle_now(),
            updated
        );
        assert_eq!(accepts(&key_c), accepted);
    }
    service.shut_down().await;
}

#[tokio::test]
async fn resident_resume_refuses_rows_that_are_not_paused_before_any_work() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    // Pool rows and an activation policy keep every case short of the
    // node-manager connection even if this precheck regresses.
    let policy = AutoLifecyclePolicy::disabled();
    for (state, expected) in [
        (None, Code::NotFound),
        (Some(SandboxState::Started), Code::FailedPrecondition),
        (Some(SandboxState::Stopped), Code::FailedPrecondition),
        (Some(SandboxState::Suspended), Code::FailedPrecondition),
        (Some(SandboxState::Failed), Code::FailedPrecondition),
        (Some(SandboxState::Pausing), Code::Unavailable),
        (Some(SandboxState::Resuming), Code::Unavailable),
        (Some(SandboxState::Suspending), Code::Unavailable),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        if let Some(state) = state {
            insert_sandbox(&conn, sandbox_id, state, Some("route-test-pool"), &policy);
        }
        let service = RouteTestService::new(conn);

        let error = service
            .resume_after_wake_lock(&sandbox_id.to_string(), Some((&policy, None)), false, 1)
            .await
            .expect_err("only a paused sandbox can resume");

        assert_eq!(error.code(), expected, "{state:?}");
        if let Some(state) = state {
            let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
            assert_eq!(row.state, state);
        }
        service.shut_down().await;
    }
}

#[tokio::test]
async fn resident_resume_refuses_a_replaced_execution_without_touching_the_row() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let resident_policy = AutoLifecyclePolicy::disabled();
    // A warm-pool member has no network identity, so the pre-wake policy gate
    // has nothing to confirm with a gateway.
    insert_sandbox(
        &conn,
        sandbox_id,
        SandboxState::Paused,
        Some("route-test-pool"),
        &resident_policy,
    );
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            ..SpaceApiRouteIdentity::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    let service = RouteTestService::new(conn);
    let activation_policy = access_idle_policy();

    let error = service
        .resume_after_wake_lock(
            &sandbox_id.to_string(),
            Some((
                &activation_policy,
                Some(ExecutionGeneration::new(3).unwrap()),
            )),
            false,
            1,
        )
        .await
        .expect_err("a resume admitted for another execution must not thaw this one");

    assert_eq!(error.code(), Code::FailedPrecondition);
    let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
    assert_eq!(row.state, SandboxState::Paused);
    assert_eq!(row.auto_lifecycle, resident_policy);
    service.shut_down().await;
}
