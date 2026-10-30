//! Route verification bundle refresh, authoritative route identity adoption,
//! and resident resume admission up to the node-manager call.
//!
//! Every test runs in its own child process with route, node, bundle, and
//! feature-flag provider environment removed or pointed at test-owned files.
//! Services use temporary socket paths, an explicit node identity, and a
//! shutdown token and task tracker that each test drains.

use super::{
    RouteVerificationBundleRefresh, RouteVerificationBundleRefreshError, SandboxManagerService,
    SandboxManagerServiceOptions, SandboxState, SpacedRpc,
};
use crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance;
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
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::Code;

const ROUTE_TEST_CLUSTER: &str = "route-test";
const ROUTE_TEST_ISSUER: &str = "route-test-authority";

/// Isolate bundle, cluster, node, and route-fence configuration from the host,
/// and keep service construction from starting a feature-flag provider.
fn enter_route_identity_test_process(bundle_path: Option<&OsStr>) -> bool {
    crate::service::tests::enter_route_test_process(&[
        (
            SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV,
            bundle_path,
        ),
        (SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV, None),
        (CLUSTER_ID_ENV, Some(OsStr::new(ROUTE_TEST_CLUSTER))),
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ])
}

fn local_node() -> NodeId {
    crate::service::test_options::test_node_id()
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
    fn new(conn: crate::db::Connection) -> Self {
        let sockets = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        let tasks = TaskTracker::new();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                shutdown_token: shutdown.clone(),
                task_tracker: tasks.clone(),
                ..SandboxManagerServiceOptions::for_test_in(sockets.path())
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
    conn: &crate::db::Connection,
    sandbox_id: SandboxId,
    state: SandboxState,
    pool_template: Option<&str>,
    auto_lifecycle: &AutoLifecyclePolicy,
) {
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
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
fn started_unfenced_sandbox(conn: &crate::db::Connection) -> (SandboxId, SpaceApiRouteIdentity) {
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
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    (sandbox_id, identity)
}

fn route_identity_provenance(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
) -> Option<RouteIdentityProvenance> {
    crate::db::auto_lifecycle_delete_receipts::route_identity_adoption_metadata(
        &service.db_conn().unwrap(),
        sandbox_id,
    )
    .unwrap()
    .current_provenance
}

#[tokio::test]
async fn route_identity_adoption_without_an_identity_changes_nothing() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    let conn = crate::db::test_conn();
    let (sandbox_id, identity) = started_unfenced_sandbox(&conn);
    let service = RouteTestService::new(conn);

    // A wake that carries no route identity has nothing to adopt, whether or
    // not the sandbox exists locally.
    for target in [sandbox_id, SandboxId::new()] {
        service
            .adopt_authoritative_route_identity(target, &SpaceApiRouteIdentity::default())
            .unwrap();
    }

    let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
    assert_eq!(row.space_api_route_identity, identity);
    assert_eq!(
        route_identity_provenance(&service, sandbox_id),
        Some(RouteIdentityProvenance::Unfenced)
    );
    service.shut_down().await;
}

#[tokio::test]
async fn route_identity_adoption_requires_a_readable_local_row() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    // Wake callers forward the NotFound message. The Internal message embeds
    // the store's own diagnostic, so only its code is pinned.
    for (store_readable, expected_code, expected_message) in [
        (true, Code::NotFound, Some("sandbox not found")),
        (false, Code::Internal, None),
    ] {
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

        assert_eq!(
            error.code(),
            expected_code,
            "store readable: {store_readable}"
        );
        if let Some(expected_message) = expected_message {
            assert_eq!(error.message(), expected_message);
        }
        service.shut_down().await;
    }
}

#[derive(Clone, Copy, Debug)]
enum ExecutionOwner {
    /// Cleanup intent armed on a still-started row.
    CleanupIntent,
    /// A lifecycle transition has moved the row out of started/paused.
    LifecycleTransition,
    /// A pending auto-lifecycle delete receipt for the same execution.
    PendingDeleteReceipt,
}

#[tokio::test]
async fn identity_equal_adoption_does_not_overwrite_an_owned_execution() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    for (owner, expected_message) in [
        (
            ExecutionOwner::CleanupIntent,
            "sandbox lifecycle or cleanup owns the sandbox execution",
        ),
        (
            ExecutionOwner::LifecycleTransition,
            "sandbox lifecycle or cleanup owns the sandbox execution",
        ),
        (
            ExecutionOwner::PendingDeleteReceipt,
            "pending auto-lifecycle delete receipt owns the sandbox execution",
        ),
    ] {
        let conn = crate::db::test_conn();
        let (sandbox_id, identity) = started_unfenced_sandbox(&conn);
        let id = sandbox_id.to_string();
        let expected_state = match owner {
            ExecutionOwner::CleanupIntent => {
                assert!(
                    crate::db::set_cleanup_intent(
                        &conn,
                        &id,
                        false,
                        crate::db::CleanupCompletion::DeleteRow,
                    )
                    .unwrap()
                );
                SandboxState::Started
            }
            ExecutionOwner::LifecycleTransition => {
                crate::db::test_fixtures::state_write_fixtures::apply_edge(
                    &conn,
                    &id,
                    &crate::db::edges::STOP_ADMISSION,
                );
                SandboxState::Stopping
            }
            ExecutionOwner::PendingDeleteReceipt => {
                crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(
                    &conn,
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
                SandboxState::Started
            }
        };
        let service = RouteTestService::new(conn);

        // The route fields already match, so only the provenance would be
        // promoted; the execution's current owner refuses even that write.
        let error = service
            .adopt_authoritative_route_identity(sandbox_id, &identity)
            .expect_err("an owned execution refuses route metadata promotion");

        assert_eq!(error.code(), Code::Aborted, "{owner:?}");
        assert_eq!(error.message(), expected_message, "{owner:?}");
        assert_eq!(
            route_identity_provenance(&service, sandbox_id),
            Some(RouteIdentityProvenance::Unfenced),
            "{owner:?}"
        );
        let row = service.lookup_sandbox(&id).unwrap();
        assert_eq!(row.space_api_route_identity, identity, "{owner:?}");
        assert_eq!(row.state, expected_state, "{owner:?}");
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

    /// Claims are minted and verified against the wall clock (120 s lifetime,
    /// 15 s skew); there is no controlled-clock mint/verify. Each request is
    /// minted immediately before its verification, so a check can fail only
    /// if that single step stalls for about two minutes.
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

/// What a refresh does to the installed verifier. Only the Ok/Err split of the
/// private function's result is kept; the variants inside each side differ
/// only in telemetry.
#[derive(Debug, PartialEq, Eq)]
enum RefreshOutcome {
    Installed(u64),
    NotInstalled,
    Rejected,
}

fn refresh_outcome(
    result: Result<RouteVerificationBundleRefresh, RouteVerificationBundleRefreshError>,
) -> RefreshOutcome {
    match result {
        Ok(RouteVerificationBundleRefresh::Updated(generation)) => {
            RefreshOutcome::Installed(generation.get())
        }
        Ok(
            RouteVerificationBundleRefresh::Unchanged(_) | RouteVerificationBundleRefresh::Absent,
        ) => RefreshOutcome::NotInstalled,
        Err(RouteVerificationBundleRefreshError::Internal(message)) => {
            panic!("unexpected internal refresh failure: {message}")
        }
        Err(_) => RefreshOutcome::Rejected,
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
        (None, RefreshOutcome::Rejected, [false, false]),
        (
            Some(String::new()),
            RefreshOutcome::NotInstalled,
            [false, false],
        ),
        (
            Some(key_a.bundle_json(1)),
            RefreshOutcome::Installed(1),
            [true, false],
        ),
        (
            Some(key_a.bundle_json(1)),
            RefreshOutcome::NotInstalled,
            [true, false],
        ),
        // Same generation, different keys.
        (
            Some(key_b.bundle_json(1)),
            RefreshOutcome::Rejected,
            [true, false],
        ),
        (
            Some(key_b.bundle_json(2)),
            RefreshOutcome::Installed(2),
            [false, true],
        ),
        // Older generation.
        (
            Some(key_a.bundle_json(1)),
            RefreshOutcome::Rejected,
            [false, true],
        ),
        (
            Some("{not a bundle".to_string()),
            RefreshOutcome::Rejected,
            [false, true],
        ),
        // Blank after a bundle was installed.
        (
            Some(" \n".to_string()),
            RefreshOutcome::Rejected,
            [false, true],
        ),
        // Unreadable after a bundle was installed.
        (None, RefreshOutcome::Rejected, [false, true]),
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

/// Status a policy-bearing resume returns for a row in `state` whose resident
/// execution differs from the one the request names. Callers forward the
/// message (Direct implicit wake turns `FailedPrecondition` into an HTTP 409
/// body), so it is pinned with the code.
fn expected_resume_refusal(state: SandboxState) -> (Code, &'static str) {
    use SandboxState::*;
    match state {
        Paused => (
            Code::FailedPrecondition,
            "resident execution changed before resume",
        ),
        Created => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is created, not paused",
        ),
        Started => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is started, not paused",
        ),
        Stopped => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is stopped, not paused",
        ),
        Suspended => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is suspended, not paused",
        ),
        CleanupRequired => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is cleanup_required, not paused",
        ),
        CleanupFailed => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is cleanup_failed, not paused",
        ),
        Quarantined => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is quarantined, not paused",
        ),
        Failed => (
            Code::FailedPrecondition,
            "cannot resume: sandbox is failed, not paused",
        ),
        Creating => (
            Code::Unavailable,
            "cannot resume: sandbox is creating — wait for creation to complete",
        ),
        Starting => (
            Code::Unavailable,
            "cannot resume: sandbox is starting — wait for start to complete",
        ),
        Stopping => (
            Code::Unavailable,
            "cannot resume: sandbox is stopping — wait for stop to complete",
        ),
        Deleting => (
            Code::Unavailable,
            "cannot resume: sandbox is deleting — wait for deletion to complete",
        ),
        Pausing => (
            Code::Unavailable,
            "cannot resume: sandbox is pausing — wait for pause to complete",
        ),
        Resuming => (
            Code::Unavailable,
            "cannot resume: sandbox is resuming — wait for resume to complete",
        ),
        Checkpointing => (
            Code::Unavailable,
            "cannot resume: sandbox is checkpointing — wait for checkpoint to complete",
        ),
        Suspending => (
            Code::Unavailable,
            "cannot resume: sandbox is suspending — wait for suspend to complete",
        ),
        Restoring => (
            Code::Unavailable,
            "cannot resume: sandbox is restoring — wait for restore to complete",
        ),
    }
}

#[tokio::test]
async fn resident_resume_refusal_status_for_every_state() {
    if !enter_route_identity_test_process(None) {
        return;
    }
    let resident_policy = AutoLifecyclePolicy::disabled();
    let activation_policy = access_idle_policy();
    let replaced_execution = ExecutionGeneration::new(3).unwrap();

    let missing = SandboxId::new().to_string();
    let service = RouteTestService::new(crate::db::test_conn());
    let error = service
        .resume_after_wake_lock(
            &missing,
            Some((&activation_policy, Some(replaced_execution))),
            false,
            1,
        )
        .await
        .expect_err("an unknown sandbox cannot resume");
    assert_eq!(error.code(), Code::NotFound);
    assert_eq!(error.message(), format!("sandbox not found: {missing}"));
    service.shut_down().await;

    for state in SandboxState::all() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        // A warm-pool member has no network identity, so the pre-wake policy
        // gate has nothing to confirm with a gateway. Every row names a
        // different resident execution than the request, so the policy
        // admission refuses even a paused row before the node-manager call.
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
            RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        conn.execute(
            "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
            crate::db::fixture::params![state.as_str(), sandbox_id.to_string()],
        )
        .unwrap();
        let service = RouteTestService::new(conn);

        let error = service
            .resume_after_wake_lock(
                &sandbox_id.to_string(),
                Some((&activation_policy, Some(replaced_execution))),
                false,
                1,
            )
            .await
            .expect_err("a resume for another execution never thaws this row");

        let (expected_code, expected_message) = expected_resume_refusal(state);
        assert_eq!(error.code(), expected_code, "{state}");
        assert_eq!(error.message(), expected_message, "{state}");
        // db::tests::resident_policy_resume_is_generation_fenced_and_arms_only_after_thaw
        // owns the admission's row effects; this checks only that a refused
        // resume leaves the row as it was.
        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.state, state, "{state}");
        assert_eq!(row.auto_lifecycle, resident_policy, "{state}");
        service.shut_down().await;
    }
}
