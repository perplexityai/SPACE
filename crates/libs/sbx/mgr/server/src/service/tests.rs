use pplx_space_common_core::types::{JobId, SandboxId, SnapshotId, UserId};

use super::activation::{
    WAKE_ROUTE_SECURITY_CONNECT_RETRY_POLICY, WAKE_ROUTE_SECURITY_SYNC_TIMEOUT,
    WakeRouteSecuritySyncError, require_wake_route_security_sync,
};
use super::activation_support::{
    GuestActivityKind, IdentityFreeDirectActivationAuthority, StartGate, WakeNetworkContext,
    WakeStartConfig, evaluate_start_gate, fresh_runtime_activation_identity, grpc_timeout_budget,
    internal_request, local_space_node_id, runtime_activation_wire_identity_can_open_claim,
    sandbox_peer_authorizer, space_api_route_identity_for_runtime_activation,
    space_api_route_identity_from_start_request, space_api_route_identity_from_wake_start_context,
    wait_ready_and_mark_started, wake_ready_timeout_secs,
};
use super::cache_cleanup::{
    InventoryDbAccess, InventoryOwnershipGuards, ReconcileSource, SandboxInventoryPaths,
    cleanup_orphaned_dirs_in, inventory_cleanup_guard_for_test, inventory_execution_for_test,
    list_sandbox_dirs_in, reclaim_abandoned_restore_metadata_staging,
    reconcile_sandbox_inventory_ids, reconcile_sandbox_inventory_in,
    sandbox_state_may_lack_cache_root,
};
use super::cleanup_support::{
    ActivationCleanupSpec, CleanupCompletion, CleanupLockGuard, CleanupLockTryResult,
    CleanupMetrics, CleanupPermit, CleanupRecoveryTarget,
};
use super::create_support::*;
use super::grpc_lifecycle::{SUSPEND_TRIGGER_METADATA, teardown_suspend_event_trigger};
use super::network_runtime::*;
use super::spaced_bridge::loopback_bridge_open_warrants_rearm;
use super::tail_support::*;
use super::validation::{
    canonical_batched_sandbox_ids, reject_incompatible_template_spaced_compatibility,
    template_execution_metadata,
};
use super::*;
use pplx_space_events::SandboxEventTrigger;

/// Isolate route policy overrides from the host and from other tests.
pub(crate) fn enter_route_test_process(overrides: &[(&str, Option<&std::ffi::OsStr>)]) -> bool {
    use pplx_space_common_svc::space_api_route::SPACE_API_ROUTE_GENERATION_FENCE_ENABLED_ENV;
    let mut vars = vec![
        (SPACE_API_ROUTE_GENERATION_FENCE_ENABLED_ENV, None),
        ("SPACE_NODE_ID", None),
        ("SPACE_NODE_ROLE", None),
    ];
    for &(key, value) in overrides {
        if let Some(entry) = vars.iter_mut().find(|(name, _)| *name == key) {
            entry.1 = value;
        } else {
            vars.push((key, value));
        }
    }
    pplx_space_test_env::enter_test_process(&vars)
}

impl SandboxManagerService {
    pub(super) async fn admit_lifecycle_cleanup(
        &self,
        sandbox_id: SandboxId,
        target_state: SandboxState,
        keep_filesystem: bool,
        completion: CleanupCompletion,
        persist_intent: bool,
    ) -> Result<super::cleanup::CleanupAdmission, super::cleanup::CleanupAdmissionError> {
        self.admit_lifecycle_cleanup_for_observed_execution(
            sandbox_id,
            target_state,
            keep_filesystem,
            completion,
            persist_intent,
            None,
        )
        .await
    }
}

pub(crate) fn closed_drain_for_test() -> Arc<DrainState> {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    service.begin_draining_for_restart();
    Arc::clone(&service.drain_state)
}

#[test]
fn startup_cleanup_discovery_reports_database_failure_instead_of_empty_inventory() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    db.lock()
        .unwrap()
        .execute_batch("DROP TABLE sandboxes")
        .unwrap();
    assert!(matches!(
        SandboxManagerService::recover_required_cleanup_targets(
            &db,
            super::recovery::CleanupRecoveryAuthority::Startup,
        ),
        Err(StartupRecoveryError::Database(_)),
    ));
}

#[tokio::test]
async fn restore_scope_cleanup_rejects_paths_not_owned_by_its_identity_before_remote_calls() {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let id = SandboxId::new().to_string();
    let scope_id = pplx_space_common_core::types::TransferScopeId::new();
    let other_id = SandboxId::new().to_string();
    let other_scope = pplx_space_common_core::types::TransferScopeId::new();
    for staging_dir in [
        "/".to_string(),
        crate::snapshot_restore::remote_restore_metadata_staging_dir(&other_id, scope_id)
            .to_string_lossy()
            .into_owned(),
        crate::snapshot_restore::remote_restore_metadata_staging_dir(&id, other_scope)
            .to_string_lossy()
            .into_owned(),
    ] {
        let scope = crate::db::RestoreTransferScopeRow {
            transfer_scope_id: scope_id,
            execution_generation: None,
            activation_attempt_id: None,
            staging_dir,
        };
        let error = service
            .cancel_restore_transfer_scope_and_remove_staging(&id, &scope)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}

pub(super) async fn call_through_restart_admission<T, R, F, Fut>(
    service: &SandboxManagerService,
    path: &'static str,
    request: Request<T>,
    handler: F,
) -> Result<(), Status>
where
    T: Send + 'static,
    F: FnOnce(SandboxManagerService, Request<T>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Response<R>, Status>> + Send + 'static,
{
    let handler = std::sync::Arc::new(std::sync::Mutex::new(Some(handler)));
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut admitted = tower::Layer::layer(
        &service.restart_admission_layer(),
        tower::service_fn({
            let handler_service = service.clone();
            move |request: http::Request<Request<T>>| {
                let handler = handler
                    .lock()
                    .expect("handler mutex poisoned")
                    .take()
                    .expect("test handler must run once");
                let handler_service = handler_service.clone();
                let result_tx = result_tx.clone();
                async move {
                    let result = handler(handler_service, request.into_body())
                        .await
                        .map(|_| ());
                    result_tx.send(result).expect("test receiver must remain");
                    Ok::<_, std::convert::Infallible>(http::Response::new(
                        tonic::body::Body::default(),
                    ))
                }
            }
        }),
    );
    let response = tower::Service::call(
        &mut admitted,
        http::Request::builder()
            .uri(path)
            .body(request)
            .expect("test request must build"),
    )
    .await
    .expect("restart admission must return a response");
    drop(admitted);

    result_rx.recv().await.unwrap_or_else(|| {
        panic!(
            "restart admission rejected {path} with grpc-status {:?}",
            response.headers().get("grpc-status")
        )
    })
}

#[test]
fn encrypted_restore_rootfs_error_preserves_invalid_key_marker() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE
            .parse()
            .unwrap(),
    );
    let error =
        crate::rootfs_encryption::EncryptedRootfsPrepareError::Rpc(tonic::Status::with_metadata(
            tonic::Code::FailedPrecondition,
            "provider rejected key",
            metadata,
        ));

    let status = super::create::encrypted_restore_rootfs_error_status(error);

    assert!(pplx_space_credmgr_client::status_is_invalid_key_reference(
        &status
    ));
    assert!(status.message().contains("provider rejected key"));
}

#[test]
fn encrypted_restore_snapshot_error_preserves_invalid_key_status() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE
            .parse()
            .unwrap(),
    );
    let error = crate::snapshot_encryption::EncryptedSnapshotPrepareError::Rpc(
        tonic::Status::with_details_and_metadata(
            tonic::Code::FailedPrecondition,
            "provider rejected snapshot key",
            tonic::codegen::Bytes::from_static(b"provider-details"),
            metadata,
        ),
    );

    let status = super::create::encrypted_restore_snapshot_error_status(error);

    assert!(pplx_space_credmgr_client::status_is_invalid_key_reference(
        &status
    ));
    assert!(status.message().contains("provider rejected snapshot key"));
    assert_eq!(status.details(), b"provider-details");
}

#[cfg(test)]
mod cancelled_cleanup_rpc_tests;

#[cfg(test)]
mod endpoint_capture_tests;
#[cfg(test)]
mod local_backup_inventory_tests;

#[cfg(test)]
pub(crate) mod node_manager_transport_tests;

#[cfg(test)]
pub(crate) mod service_tests {
    use super::start_support::*;
    use super::*;
    use crate::auto_lifecycle::{
        AutoLifecycleCoordinator, TEST_GUEST_ACTIVITY_HOLD_CAP, test_guest_activity_hold_cap,
    };
    use crate::service::cleanup::{
        AsyncCleanupRequest, CleanupCompletionDisposition, ObservedCleanupClaim,
        SynchronousCleanupRequest,
    };
    use crate::service::grpc_lifecycle::{
        KeyedUploadUse, SubmitSuspendSnapshotOutcome, SuspendRequestContext,
        SuspendSnapshotStagingDisposition, teardown_keyed_upload_use,
    };
    use crate::service::recovery::SuspendSnapshotPublicationRecoveryOrigin;
    use crate::service::suspend_snapshot_publication::{
        SuspendSnapshotPublicationDecision, canonical_suspend_snapshot_dir,
        suspend_snapshot_staging_dir, write_deferred_suspend_artifact, write_full_suspend_artifact,
    };
    use pplx_space_common_core::host::linux::identities::{user_names, users};
    use pplx_space_common_core::types::{
        ArtifactKind, ArtifactOrigin, AutoLifecycleConditionSet, AutoLifecycleConditionSetId,
        AutoLifecycleStage, BUILDER_SANDBOX_OWNER, ClusterId, DEPLOY_SANDBOX_OWNER,
        DEV_SANDBOX_OWNER, ExecutionGeneration, LocalArtifactId, NodeId, NodeManagedBy, PodUid,
        ROOT_SANDBOX_OWNER, RouteGeneration, SandboxActivationAttemptId,
        SandboxQuarantineHistoryAction, SandboxQuarantineNextAction, SandboxQuarantineReasonCode,
        SandboxQuarantineSource, SandboxRouteIdentity, SpaceApiRouteIdentity,
    };
    use pplx_space_common_svc::space_api_route::{
        SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY, SpaceApiBridgeCallerPolicy,
        set_space_api_bridge_caller_policy,
    };
    use pplx_space_common_svc::space_api_route_ed25519::{
        SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId,
        SpaceApiRouteEd25519Signer, SpaceApiRouteEd25519Token,
        SpaceApiRouteEd25519VerificationBundle, SpaceApiRouteEd25519VerificationGeneration,
        insert_space_api_route_ed25519_token,
    };
    use pplx_space_common_svc::types::k8s::space_k8s_node_id;

    use prost::Message as _;

    fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<()> {
        rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(Into::into)
    }

    struct DependencyDrainFiles {
        _dir: tempfile::TempDir,
        capability: std::path::PathBuf,
        admission: std::path::PathBuf,
        work: std::path::PathBuf,
    }

    impl DependencyDrainFiles {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let capability = dir.path().join("capability.lock");
            let admission = dir.path().join("admission.lock");
            let work = dir.path().join("work.lock");
            for path in [&capability, &admission, &work] {
                std::fs::File::create(path).unwrap();
            }
            Self {
                _dir: dir,
                capability,
                admission,
                work,
            }
        }

        fn enable_for(&self, service: &SandboxManagerService) {
            service
                .enable_dependency_restart_drain(&self.capability, &self.admission, &self.work)
                .unwrap();
        }

        fn open(path: &std::path::Path) -> std::fs::File {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap()
        }

        fn lock_admission(&self) -> std::fs::File {
            let file = Self::open(&self.admission);
            try_lock_exclusive(&file).unwrap();
            file
        }

        fn open_work(&self) -> std::fs::File {
            Self::open(&self.work)
        }

        fn lock_work(&self) -> std::fs::File {
            let file = self.open_work();
            try_lock_exclusive(&file).unwrap();
            file
        }
    }

    fn teardown_generation() -> ExecutionGeneration {
        ExecutionGeneration::new(1).unwrap()
    }

    /// Captures `space.sandbox` events off the tracing stream.
    ///
    /// `pub(crate)` because sandbox events are emitted from several modules and the
    /// tests that assert them live beside those modules, not here. Keeping this
    /// private to this file is what made emissions elsewhere look untestable.
    /// Keep a process-wide dispatcher registered before any test installs a
    /// scoped one. While a scoped subscriber is the only dispatcher in the
    /// process, `tracing-core` rebuilds its callsite interest cache through
    /// whichever thread registers a new callsite first; a concurrent test
    /// thread without a subscriber then sets the global max level to OFF, and
    /// the capturing thread's events are dropped before dispatch. With a
    /// second dispatcher present, the rebuild consults every dispatcher.
    pub(crate) fn anchor_global_tracing_dispatch() {
        static ANCHORED: std::sync::Once = std::sync::Once::new();
        ANCHORED.call_once(|| {
            // Another global subscriber already serves the same purpose.
            let _ = tracing::subscriber::set_global_default(
                tracing::subscriber::NoSubscriber::default(),
            );
        });
    }

    #[derive(Clone)]
    pub(crate) struct SandboxEventCapture {
        pub(crate) types: Arc<Mutex<Vec<String>>>,
        pub(crate) events: Arc<Mutex<Vec<std::collections::BTreeMap<String, String>>>>,
    }

    impl Default for SandboxEventCapture {
        fn default() -> Self {
            anchor_global_tracing_dispatch();
            Self {
                types: Arc::default(),
                events: Arc::default(),
            }
        }
    }

    impl<S> tracing_subscriber::Layer<S> for SandboxEventCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = EventFieldGrabber::default();
            event.record(&mut visitor);
            if visitor.get("event_stream").as_deref() == Some("space.sandbox")
                && let Some(event_type) = visitor.get("event_type")
            {
                self.types.lock().unwrap().push(event_type);
                self.events
                    .lock()
                    .unwrap()
                    .push(visitor.fields.into_iter().collect());
            }
        }
    }

    /// Captures one named field off every tracing event, whatever the stream.
    ///
    /// `SandboxEventCapture` filters on `event_stream`, so it cannot see an ordinary
    /// log line. Without this, assertions about what a log line contains — that a
    /// value is bounded, that a URI is not disclosed — have no oracle, and "there is
    /// no capture seam" becomes the reason a redaction goes unverified.
    #[derive(Clone)]
    pub(crate) struct LogFieldCapture {
        field: &'static str,
        pub(crate) values: Arc<Mutex<Vec<String>>>,
    }

    impl LogFieldCapture {
        pub(crate) fn new(field: &'static str) -> Self {
            anchor_global_tracing_dispatch();
            Self {
                field,
                values: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl<S> tracing_subscriber::Layer<S> for LogFieldCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = EventFieldGrabber::default();
            event.record(&mut visitor);
            if let Some(value) = visitor.get(self.field) {
                self.values.lock().unwrap().push(value);
            }
        }
    }

    #[derive(Clone, Default)]
    struct SpanFieldCapture {
        fields: Arc<Mutex<std::collections::BTreeMap<String, String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanFieldCapture {
        fn on_record(
            &self,
            _id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = EventFieldGrabber::default();
            values.record(&mut visitor);
            self.fields.lock().unwrap().extend(visitor.fields);
        }
    }

    #[derive(Default)]
    struct EventFieldGrabber {
        fields: Vec<(String, String)>,
    }

    impl EventFieldGrabber {
        fn get(&self, name: &str) -> Option<String> {
            self.fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| value.clone())
        }
    }

    impl tracing::field::Visit for EventFieldGrabber {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let rendered = format!("{value:?}");
            let normalized = rendered
                .strip_prefix("Some(\"")
                .and_then(|value| value.strip_suffix("\")"))
                .unwrap_or_else(|| rendered.trim_matches('"'))
                .to_string();
            self.fields.push((field.name().to_string(), normalized));
        }
    }

    fn internal_request_with_invalid_space_api_route_token<T>(message: T) -> Request<T> {
        let mut request = internal_request(message);
        request.metadata_mut().insert(
            SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY,
            tonic::metadata::MetadataValue::from_static("bogus"),
        );
        request
    }

    pub(crate) fn node_ctl_request<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_NODECTL.uid,
            users::SPACE_NODECTL.primary_group.gid,
            None,
        ));
        request
    }

    pub(crate) fn node_api_trusted_lifecycle_request<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_NODEAPI.uid,
            users::SPACE_NODEAPI.primary_group.gid,
            None,
        ));
        set_space_api_bridge_caller_policy(
            request.metadata_mut(),
            SpaceApiBridgeCallerPolicy::TrustedLifecycle,
        );
        request
    }

    fn tmplmgr_request<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(PeerIdentity::new(
            users::SPACE_TMPLMGR.uid,
            users::SPACE_TMPLMGR.primary_group.gid,
            None,
        ));
        request
    }

    #[test]
    fn route_identity_request_wire_encoding_snapshot() {
        // The four request envelopes below each wrap this identical
        // `SpaceApiRouteIdentity` submessage at a different field number. The
        // submessage's own bytes are pinned once here; each envelope
        // assertion below only needs to show the envelope embeds those exact
        // bytes verbatim, and its own snapshot keeps pinning the envelope's
        // unique field-number wrapping and other fields.
        let identity = crate::proto::SpaceApiRouteIdentity {
            sandbox_id: None,
            route_generation: Some(42),
            current_node_id: Some("019f8000-0000-7000-8000-0000000000aa".to_string()),
            execution_generation: Some(7),
            pod_uid: Some("pod-uid-42".to_string()),
            activation_attempt_id: Some("019f8000-0000-7000-8000-0000000000bb".to_string()),
            substrate_kind: crate::proto::SandboxSubstrateKind::Kubernetes as i32,
        };
        let identity_hex = hex::encode(identity.encode_to_vec());
        insta::assert_snapshot!("route_identity_submessage_wire_encoding", identity_hex);

        for binding in [
            None,
            Some("019f8000-0000-7000-8000-000000000001"),
            Some("019f8000-0000-7000-8000-000000000002"),
            Some("invalid"),
        ] {
            let mut wire = identity.clone();
            wire.sandbox_id = binding.map(str::to_owned);
            let parsed = super::activation_support::space_api_route_identity_from_wire(Some(&wire));
            if binding == Some("invalid") {
                assert_eq!(parsed.unwrap_err().code(), tonic::Code::InvalidArgument);
                continue;
            }
            let parsed = parsed.unwrap();
            let result = super::activation_support::validate_route_identity_sandbox(
                &parsed,
                "019f8000-0000-7000-8000-000000000001",
            );
            assert_eq!(
                result.is_ok(),
                binding != Some("019f8000-0000-7000-8000-000000000002")
            );
            assert_eq!(
                super::activation_support::space_api_route_identity_to_wire(&parsed)
                    .unwrap()
                    .sandbox_id
                    .as_deref(),
                binding
            );
        }

        let create = CreateRequest {
            sandbox_id: Some("019f8000-0000-7000-8000-000000000001".to_string()),
            template: "seed".to_string(),
            owner: DEPLOY_SANDBOX_OWNER.to_string(),
            route_identity: Some(identity.clone()),
            ..Default::default()
        };
        let start = StartRequest {
            sandbox_id: "019f8000-0000-7000-8000-000000000001".to_string(),
            route_identity: Some(identity.clone()),
            ..Default::default()
        };
        let wake_start = pplx_space_sbx_mgr_client::proto::WakeStartContext {
            route_identity: Some(identity.clone()),
            ..Default::default()
        };
        let adopt = AdoptPoolSandboxRequest {
            template: "seed".to_string(),
            owner: DEPLOY_SANDBOX_OWNER.to_string(),
            route_identity: Some(identity.clone()),
            ..Default::default()
        };

        let create_hex = hex::encode(create.encode_to_vec());
        assert!(
            create_hex.contains(&identity_hex),
            "create request must embed the route identity submessage verbatim"
        );
        insta::assert_snapshot!("create_request_route_identity_wire_encoding", create_hex);

        let start_hex = hex::encode(start.encode_to_vec());
        assert!(
            start_hex.contains(&identity_hex),
            "start request must embed the route identity submessage verbatim"
        );
        insta::assert_snapshot!("start_request_route_identity_wire_encoding", start_hex);

        let wake_start_hex = hex::encode(wake_start.encode_to_vec());
        assert!(
            wake_start_hex.contains(&identity_hex),
            "wake start context must embed the route identity submessage verbatim"
        );
        insta::assert_snapshot!(
            "wake_start_context_route_identity_wire_encoding",
            wake_start_hex
        );

        let adopt_hex = hex::encode(adopt.encode_to_vec());
        assert!(
            adopt_hex.contains(&identity_hex),
            "adopt pool request must embed the route identity submessage verbatim"
        );
        insta::assert_snapshot!("adopt_pool_request_route_identity_wire_encoding", adopt_hex);
    }

    #[test]
    fn pool_create_forwards_route_identity_to_adoption() {
        let route_identity = crate::proto::SpaceApiRouteIdentity {
            route_generation: Some(1),
            ..Default::default()
        };
        let create = CreateRequest {
            route_identity: Some(route_identity.clone()),
            ..Default::default()
        };

        let adopt = super::grpc_core::pool_adoption_request(
            &create,
            "seed".to_string(),
            DEPLOY_SANDBOX_OWNER.to_string(),
            crate::proto::AutoLifecyclePolicy::default(),
            crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            Some("sha256:test".to_string()),
        );

        assert_eq!(adopt.route_identity, Some(route_identity));
    }

    fn insert_pool_donor(conn: &crate::db::Connection, sandbox_id: SandboxId) {
        let sandbox_id_string = sandbox_id.to_string();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
        crate::db::insert_sandbox(
            conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id: &sandbox_id_string,
                uuid8: &uuid8,
                state: SandboxState::Paused,
                template: "seed",
                template_hash: Some("sha256:test"),
                user: &TEMPLATE_MANAGER_SANDBOX_OWNER,
                runtime: RuntimeKind::Firecracker.as_str(),
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: Some("seed"),
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
    }

    fn pool_adoption_request(
        route_identity: Option<crate::proto::SpaceApiRouteIdentity>,
    ) -> AdoptPoolSandboxRequest {
        AdoptPoolSandboxRequest {
            template: "seed".to_string(),
            owner: "user:00000000-0000-0000-0000-000000000020".to_string(),
            auto_lifecycle: Some((&AutoLifecyclePolicy::disabled()).into()),
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            content_hash: Some("sha256:test".to_string()),
            network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            template_network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            route_identity,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn pool_adoption_persists_complete_direct_route_identity() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_pool_donor(&conn, sandbox_id);
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                route_generation: Some(RouteGeneration::new(1).unwrap()),
                current_node_id: Some(NodeId::new()),
                substrate_kind: Some(NodeManagedBy::Space),
                execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
                activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                ..Default::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let donor = {
            let db = service.db.lock().unwrap();
            crate::db::get_sandbox(&db, &sandbox_id_string)
                .unwrap()
                .unwrap()
        };
        assert_eq!(donor.pool_template.as_deref(), Some("seed"));
        assert_eq!(
            donor.space_api_route_identity.execution_generation,
            Some(ExecutionGeneration::new(1).unwrap())
        );

        let response = service
            .adopt_pool_sandbox(internal_request(pool_adoption_request(None)))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.sandbox_id, sandbox_id_string);
        assert_eq!(
            response
                .route_identity
                .as_ref()
                .and_then(|identity| identity.execution_generation),
            Some(2)
        );
        let db = service.db.lock().unwrap();
        let adopted = crate::db::get_sandbox(&db, &response.sandbox_id)
            .unwrap()
            .unwrap();
        assert_eq!(adopted.pool_template, None);
        assert_eq!(
            adopted.space_api_route_identity.route_generation,
            Some(RouteGeneration::new(2).unwrap())
        );
        assert_eq!(
            adopted.space_api_route_identity.execution_generation,
            Some(ExecutionGeneration::new(2).unwrap())
        );
        assert!(adopted.space_api_route_identity.current_node_id.is_some());
        assert_eq!(
            adopted.space_api_route_identity.substrate_kind,
            Some(NodeManagedBy::Space)
        );
        assert!(
            adopted
                .space_api_route_identity
                .activation_attempt_id
                .is_some()
        );
    }

    #[tokio::test]
    async fn pool_adoption_template_policy_failure_leaves_donor_unclaimed() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        insert_pool_donor(&conn, id);
        let service = service_for_teardown_test(conn);
        let cache = service.template_network_policies.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (fail, failure) = tokio::sync::oneshot::channel();
        let fetch = tokio::spawn(async move {
            cache
                .get_or_fetch_with("seed", |_| async move {
                    started.send(()).unwrap();
                    failure.await.unwrap();
                    Err(Status::unavailable("template owner unavailable"))
                })
                .await
        });
        ready.await.unwrap();
        let mut request = pool_adoption_request(None);
        request.template_network_policy =
            Some(crate::service::network_policy::network_policy_to_proto(
                &pplx_space_net_common::NetworkPolicy::deny_all(),
            ));
        let adoption = service.adopt_pool_sandbox(internal_request(request));
        tokio::pin!(adoption);
        assert!(futures_util::poll!(&mut adoption).is_pending());
        fail.send(()).unwrap();
        let error = adoption.await.unwrap_err();
        fetch.await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(pplx_space_sbx_mgr_client::is_template_policy_dependency(
            &error
        ));
        let donor = crate::db::get_sandbox(&service.db.lock().unwrap(), &id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(donor.pool_template.as_deref(), Some("seed"));
        assert_eq!(donor.user, TEMPLATE_MANAGER_SANDBOX_OWNER);
        assert_eq!(donor.state, SandboxState::Paused);
    }

    #[tokio::test]
    async fn identity_free_pool_adoption_mints_route_identity() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_pool_donor(&conn, sandbox_id);
        let service = service_for_teardown_test(conn);

        let response = service
            .adopt_pool_sandbox(internal_request(pool_adoption_request(None)))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.sandbox_id, sandbox_id_string);
        assert_eq!(
            response
                .route_identity
                .as_ref()
                .and_then(|identity| identity.route_generation),
            Some(1)
        );
        assert_eq!(
            response
                .route_identity
                .as_ref()
                .and_then(|identity| identity.execution_generation),
            Some(1)
        );
        let db = service.db.lock().unwrap();
        let adopted = crate::db::get_sandbox(&db, &response.sandbox_id)
            .unwrap()
            .unwrap();
        assert_eq!(adopted.pool_template, None);
        assert_eq!(
            adopted.space_api_route_identity.substrate_kind,
            Some(NodeManagedBy::Space)
        );
        assert!(adopted.space_api_route_identity.current_node_id.is_some());
        assert!(
            adopted
                .space_api_route_identity
                .activation_attempt_id
                .is_some()
        );
    }

    #[test]
    fn route_identity_response_wire_encoding_snapshot() {
        let route_identity = crate::proto::SpaceApiRouteIdentity {
            sandbox_id: None,
            route_generation: Some(42),
            current_node_id: Some("019f8000-0000-7000-8000-0000000000aa".to_string()),
            execution_generation: Some(7),
            pod_uid: None,
            activation_attempt_id: Some("019f8000-0000-7000-8000-0000000000bb".to_string()),
            substrate_kind: crate::proto::SandboxSubstrateKind::Direct as i32,
        };
        let sandbox_id = "019f8000-0000-7000-8000-000000000001".to_string();

        insta::assert_snapshot!(
            "create_response_route_identity_wire_encoding",
            hex::encode(
                CreateResponse {
                    sandbox_id: sandbox_id.clone(),
                    route_identity: Some(route_identity.clone()),
                    ..Default::default()
                }
                .encode_to_vec()
            )
        );
        insta::assert_snapshot!(
            "start_response_route_identity_wire_encoding",
            hex::encode(
                StartResponse {
                    sandbox_id: sandbox_id.clone(),
                    route_identity: Some(route_identity.clone()),
                    ..Default::default()
                }
                .encode_to_vec()
            )
        );
        insta::assert_snapshot!(
            "resume_response_route_identity_wire_encoding",
            hex::encode(
                ResumeResponse {
                    route_identity: Some(route_identity.clone()),
                }
                .encode_to_vec()
            )
        );
        insta::assert_snapshot!(
            "wake_response_route_identity_wire_encoding",
            hex::encode(
                WakeResponse {
                    sandbox_id,
                    route_identity: Some(route_identity),
                    ..Default::default()
                }
                .encode_to_vec()
            )
        );
    }

    #[test]
    fn auto_lifecycle_delete_receipt_wire_encoding_snapshot() {
        insta::assert_snapshot!(
            "auto_lifecycle_delete_receipt_wire_encoding",
            hex::encode(
                AutoLifecycleDeleteReceipt {
                    receipt_id: "019f8000-0000-7000-8000-0000000000aa".to_string(),
                    sandbox_id: "019f8000-0000-7000-8000-000000000001".to_string(),
                    execution_generation: 7,
                    route_activation_id: "019f8000-0000-7000-8000-0000000000bb".to_string(),
                    auto_lifecycle_policy_fingerprint: "v1:0123456789abcdef".to_string(),
                }
                .encode_to_vec()
            )
        );
    }

    #[test]
    fn runtime_activation_identity_normalizes_kubernetes_node_name() {
        if !crate::service::tests::enter_route_test_process(&[(
            "SPACE_NODE_ID",
            Some(std::ffi::OsStr::new("ip-10-0-0-1.ec2.internal")),
        )]) {
            return;
        }

        let identity = space_api_route_identity_for_runtime_activation(SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            ..Default::default()
        })
        .identity;
        assert_eq!(
            identity.current_node_id,
            Some(space_k8s_node_id("ip-10-0-0-1.ec2.internal", "worker"))
        );
    }

    #[test]
    fn runtime_activation_identity_preserves_uuid() {
        if !crate::service::tests::enter_route_test_process(&[(
            "SPACE_NODE_ID",
            Some(std::ffi::OsStr::new("019f8000-0000-7000-8000-0000000000aa")),
        )]) {
            return;
        }

        let identity = space_api_route_identity_for_runtime_activation(SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            ..Default::default()
        })
        .identity;
        assert_eq!(
            identity.current_node_id,
            Some(NodeId::from_uuid(
                uuid::Uuid::parse_str("019f8000-0000-7000-8000-0000000000aa").unwrap()
            ))
        );
    }

    #[test]
    fn runtime_activation_identity_does_not_invent_route_generation_when_tokens_are_required() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let identity =
            space_api_route_identity_for_runtime_activation(SpaceApiRouteIdentity::default())
                .identity;

        assert_eq!(identity.route_generation, None);
        assert_eq!(identity.execution_generation, None);
        assert_eq!(identity.activation_attempt_id, None);
    }

    #[test]
    fn strict_pool_donor_activation_keeps_route_identity_absent() {
        if !crate::service::tests::enter_route_test_process(&[(
            "SPACE_NODE_ID",
            Some(std::ffi::OsStr::new("019f8000-0000-7000-8000-0000000000aa")),
        )]) {
            return;
        }

        let identity =
            space_api_route_identity_for_runtime_activation(SpaceApiRouteIdentity::default())
                .identity;

        assert!(
            identity.is_absent(),
            "an identity-free pool donor must not become a strict-incomplete identity"
        );
    }

    #[tokio::test]
    async fn local_direct_activation_advances_retained_generation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(1).unwrap(), 1),
            )
            .unwrap();
        {
            let db = service.db.lock().unwrap();
            crate::db::claim_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                ExecutionGeneration::new(1).unwrap(),
            )
            .unwrap();
            assert!(
                crate::db::complete_teardown_suspend(
                    &db,
                    &sandbox_id.to_string(),
                    ExecutionGeneration::new(1).unwrap(),
                    None,
                )
                .unwrap()
            );
            crate::db::clear_runtime_fields(&db, &sandbox_id.to_string()).unwrap();
        }

        let resolved = service
            .resolve_runtime_activation_identity_with_authority(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity::default(),
                false,
                IdentityFreeDirectActivationAuthority::LocalProducer,
            )
            .unwrap()
            .identity;

        assert_eq!(
            resolved.route_generation,
            Some(RouteGeneration::new(2).unwrap())
        );
        assert_eq!(
            resolved.execution_generation,
            Some(ExecutionGeneration::new(2).unwrap())
        );
        assert!(resolved.current_node_id.is_some());
        assert_eq!(resolved.substrate_kind, Some(NodeManagedBy::Space));
        assert!(
            !crate::db::route_activation_claims::claim_exists(
                &service.db.lock().unwrap(),
                &sandbox_id.to_string()
            )
            .unwrap(),
            "Direct activation must stay on the retained-generation path"
        );
        let mut replacement = runtime_fields(ExecutionGeneration::new(2).unwrap(), 2);
        replacement.space_api_route_identity = resolved;
        service
            .update_runtime_fields(sandbox_id, replacement)
            .expect("the generation after a completed teardown must pass the lease fence");
    }

    #[test]
    fn local_direct_activation_fails_when_node_identity_is_unavailable() {
        if !crate::service::tests::enter_route_test_process(&[(
            "SPACE_NODE_ID",
            Some(std::ffi::OsStr::new("")),
        )]) {
            return;
        }

        let error =
            fresh_runtime_activation_identity(SpaceApiRouteIdentity::default(), false, None)
                .expect_err("an authoritative Direct identity requires a stable node id");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("SPACE_NODE_ID"));
    }

    #[test]
    fn local_node_name_uses_the_configured_builder_role() {
        if !crate::service::tests::enter_route_test_process(&[
            ("SPACE_NODE_ID", Some(std::ffi::OsStr::new("ip-10-0-0-42"))),
            ("SPACE_NODE_ROLE", Some(std::ffi::OsStr::new("builder"))),
        ]) {
            return;
        }

        assert_eq!(
            local_space_node_id(),
            Some(space_k8s_node_id("ip-10-0-0-42", "builder"))
        );
    }

    #[test]
    fn local_direct_activation_recovers_node_identity_after_service_construction() {
        if !crate::service::tests::enter_route_test_process(&[(
            "SPACE_NODE_ID",
            Some(std::ffi::OsStr::new("019f8000-0000-7000-8000-0000000000ab")),
        )]) {
            return;
        }

        let identity =
            fresh_runtime_activation_identity(SpaceApiRouteIdentity::default(), false, None)
                .expect("a node id populated after construction must be re-read")
                .identity;

        assert_eq!(
            identity.current_node_id,
            Some(NodeId::from_uuid(
                uuid::Uuid::parse_str("019f8000-0000-7000-8000-0000000000ab").unwrap()
            ))
        );
    }

    #[tokio::test]
    async fn requested_activation_identity_cannot_target_another_node() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);

        let error = service
            .resolve_runtime_activation_identity(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity {
                    route_generation: Some(RouteGeneration::new(1).unwrap()),
                    current_node_id: Some(NodeId::new()),
                    substrate_kind: Some(NodeManagedBy::Space),
                    execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
                    activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                    ..Default::default()
                },
                false,
            )
            .expect_err("a node cannot publish another node's route identity");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("targets node"));
    }

    #[tokio::test]
    async fn requested_activation_identity_cannot_reuse_retained_generation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        conn.execute(
            "UPDATE sandboxes
                SET last_observed_route_generation = 7,
                    last_observed_execution_generation = 7,
                    last_observed_route_generation_substrate_kind = 'space'
              WHERE id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let error = service
            .resolve_runtime_activation_identity(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity {
                    route_generation: Some(RouteGeneration::new(1).unwrap()),
                    current_node_id: service.local_node_id,
                    substrate_kind: Some(NodeManagedBy::Space),
                    execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
                    activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                    ..Default::default()
                },
                false,
            )
            .expect_err("a supplied identity must not bypass retained generation fencing");

        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(error.message().contains("not newer than retained"));
    }

    #[tokio::test]
    async fn pool_donor_gateway_rollback_attempts_policy_after_identity_failure() {
        let policy_attempted = std::cell::Cell::new(false);

        let error = super::grpc_pool_wait_clone::run_pool_donor_gateway_rollback(
            || async { Err(Status::internal("identity rollback failed")) },
            || async {
                policy_attempted.set(true);
                Ok(())
            },
        )
        .await
        .expect_err("the identity rollback failure must propagate");

        assert!(
            policy_attempted.get(),
            "donor policy must still be restored"
        );
        assert!(error.message().contains("identity rollback failed"));
    }

    #[tokio::test]
    async fn restored_pool_donor_is_retired_when_gateway_hydration_fails() {
        let retired = std::cell::Cell::new(false);

        let error = super::grpc_pool_wait_clone::finish_pool_donor_rollback(
            crate::db::PoolDonorRestoreDisposition::Restored,
            Err(Status::failed_precondition(
                "legacy-incomplete identity cannot be restored",
            )),
            || async {
                retired.set(true);
                Ok(())
            },
        )
        .await
        .expect_err("an unhydrated donor must not return to the pool");

        assert!(retired.get(), "the restored donor must be retired");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);

        let retired = std::cell::Cell::new(false);
        super::grpc_pool_wait_clone::finish_pool_donor_rollback(
            crate::db::PoolDonorRestoreDisposition::NotAdopted,
            Err(Status::unavailable("current owner hydration failed")),
            || async {
                retired.set(true);
                Ok(())
            },
        )
        .await
        .expect_err("CAS loss must preserve the current owner's row");
        assert!(!retired.get(), "CAS loss must not retire a changed row");

        let retired = std::cell::Cell::new(false);
        super::grpc_pool_wait_clone::finish_pool_donor_rollback(
            crate::db::PoolDonorRestoreDisposition::LifecycleOwned,
            Ok(()),
            || async {
                retired.set(true);
                Ok(())
            },
        )
        .await
        .expect("a lifecycle-owned failed adoption must preserve the current owner");
        assert!(
            !retired.get(),
            "a concurrent lifecycle operation must not be converted into deletion"
        );
    }

    #[test]
    fn pool_adoption_requires_gateway_to_apply_route_identity() {
        assert_eq!(
            super::grpc_pool_wait_clone::pool_adoption_identity_profile(
                NetGatewayRouteIdentityPushOutcome::Applied {
                    identity_profile_version: 1,
                },
            )
            .expect("an applied identity must be accepted"),
            1
        );

        {
            let outcome = NetGatewayRouteIdentityPushOutcome::Absent;
            let error = super::grpc_pool_wait_clone::pool_adoption_identity_profile(outcome)
                .expect_err("a classified non-applied push must fail pool adoption");
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(!error.retires_donor());
        }

        let diverged = super::grpc_pool_wait_clone::pool_adoption_identity_profile(
            NetGatewayRouteIdentityPushOutcome::Diverged,
        )
        .expect_err("a diverged donor must not return to the pool");
        assert_eq!(diverged.code(), tonic::Code::Unavailable);
        assert!(diverged.retires_donor());
    }

    #[tokio::test]
    async fn pool_adoption_hydrates_gateway_in_route_security_order() {
        let operations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let identity_operations = operations.clone();
        let policy_operations = operations.clone();
        let key_operations = operations.clone();

        super::grpc_pool_wait_clone::run_pool_adoption_gateway_hydration(
            || async move {
                identity_operations
                    .lock()
                    .expect("operations mutex poisoned")
                    .push("identity");
                Ok(NetGatewayRouteIdentityPushOutcome::Applied {
                    identity_profile_version: 7,
                })
            },
            || async move {
                policy_operations
                    .lock()
                    .expect("operations mutex poisoned")
                    .push("policy");
                Ok(())
            },
            move |identity_profile_version| async move {
                assert_eq!(identity_profile_version, 7);
                key_operations
                    .lock()
                    .expect("operations mutex poisoned")
                    .push("verification_keys");
                Ok(())
            },
        )
        .await
        .expect("gateway hydration should succeed");

        assert_eq!(
            *operations.lock().expect("operations mutex poisoned"),
            ["identity", "policy", "verification_keys"]
        );

        for code in [
            tonic::Code::Unimplemented,
            tonic::Code::FailedPrecondition,
            tonic::Code::PermissionDenied,
            tonic::Code::Unavailable,
            tonic::Code::Aborted,
        ] {
            let error = super::grpc_pool_wait_clone::run_pool_adoption_gateway_hydration(
                || async {
                    Ok(NetGatewayRouteIdentityPushOutcome::Applied {
                        identity_profile_version: 7,
                    })
                },
                || async { Err(Status::new(code, "gateway readiness rejected")) },
                |_| async { panic!("keys must not be pushed after readiness fails") },
            )
            .await
            .expect_err("readiness failures must prevent adoption");
            assert!(!error.retires_donor(), "{code:?}");
            assert_eq!(error.code(), code);
        }

        for (code, retires_donor) in [
            (tonic::Code::Unimplemented, true),
            (tonic::Code::Unavailable, false),
        ] {
            let error = super::grpc_pool_wait_clone::run_pool_adoption_gateway_hydration(
                || async { Err(Status::new(code, "identity synchronization failed")) },
                || async { panic!("policy must not be pushed after identity fails") },
                |_| async { panic!("keys must not be pushed after identity fails") },
            )
            .await
            .expect_err("identity RPC failures must prevent adoption");
            assert_eq!(error.retires_donor(), retires_donor, "{code:?}");
            assert_eq!(error.code(), code);
        }
    }

    #[test]
    fn pool_adoption_rollback_failure_preserves_the_original_status_code() {
        let error = super::grpc_pool_wait_clone::pool_adoption_rollback_failure(
            Status::invalid_argument("malformed policy"),
            Status::unavailable("gateway rollback failed"),
        );

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("malformed policy"));
        assert!(error.message().contains("gateway rollback failed"));
    }

    #[tokio::test]
    async fn local_activation_rejects_retained_kubernetes_substrate() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        let mut fields = runtime_fields(ExecutionGeneration::new(1).unwrap(), 1);
        fields.space_api_route_identity.substrate_kind = Some(NodeManagedBy::Kubernetes);
        fields.space_api_route_identity.pod_uid =
            Some("019f8000-0000-7000-8000-0000000000cc".parse().unwrap());
        service.update_runtime_fields(sandbox_id, fields).unwrap();
        {
            let db = service.db.lock().unwrap();
            crate::db::clear_runtime_fields(&db, &sandbox_id.to_string()).unwrap();
        }

        let error = service
            .resolve_runtime_activation_identity_with_authority(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity::default(),
                false,
                IdentityFreeDirectActivationAuthority::LocalProducer,
            )
            .expect_err("local activation cannot remint a Kubernetes identity");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("Kubernetes-managed"));
    }

    #[tokio::test]
    async fn strict_local_activation_rejects_unknown_retained_substrate() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        conn.execute(
            "UPDATE sandboxes
                SET last_observed_route_generation = 7,
                    last_observed_execution_generation = 7,
                    last_observed_route_generation_substrate_kind = NULL
              WHERE id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let error = service
            .resolve_runtime_activation_identity(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity::default(),
                false,
            )
            .expect_err("strict activation cannot infer ownership of migrated provenance");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("authoritative route identity"));
    }

    #[tokio::test]
    async fn identity_free_activation_requires_explicit_local_authority() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        conn.execute(
            "UPDATE sandboxes
                SET last_observed_route_generation = 7,
                    last_observed_execution_generation = 7,
                    last_observed_route_generation_substrate_kind = 'space'
              WHERE id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let error = service
            .resolve_runtime_activation_identity_with_authority(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity::default(),
                false,
                IdentityFreeDirectActivationAuthority::Denied,
            )
            .expect_err("identity-free activation requires explicit local authority");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("authoritative route identity"));
    }

    #[tokio::test]
    async fn trusted_direct_activation_can_enrich_unknown_retained_substrate() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        conn.execute(
            "UPDATE sandboxes
                SET last_observed_route_generation = 7,
                    last_observed_execution_generation = 7,
                    last_observed_route_generation_substrate_kind = NULL
              WHERE id = ?1",
            crate::db::fixture::params![sandbox_id.to_string()],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let identity = service
            .resolve_runtime_activation_identity_with_authority(
                &sandbox_id.to_string(),
                SpaceApiRouteIdentity::default(),
                false,
                IdentityFreeDirectActivationAuthority::LocalProducer,
            )
            .expect("trusted Direct activation can establish retained ownership")
            .identity;

        assert_eq!(identity.substrate_kind, Some(NodeManagedBy::Space));
        assert_eq!(
            identity.execution_generation,
            Some(ExecutionGeneration::new(8).unwrap())
        );
    }

    #[tokio::test]
    async fn identity_free_activation_without_a_local_row_requests_authoritative_identity() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);

        let error = service
            .resolve_runtime_activation_identity(
                &SandboxId::new().to_string(),
                SpaceApiRouteIdentity::default(),
                false,
            )
            .expect_err("a missing local row cannot mint from absent provenance");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("authoritative route identity"));
    }

    #[tokio::test]
    async fn strict_operator_start_rejects_unreported_control_plane_activation() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_platform_owned_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped, None);
        let service = service_for_teardown_test(conn);

        let error = service
            .start(internal_request(StartRequest {
                sandbox_id: sandbox_id.to_string(),
                blocking: false,
                ..Default::default()
            }))
            .await
            .expect_err("an operator cannot advance an API-owned generation without a receipt");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("use the SPACE API"));
    }

    #[tokio::test]
    async fn process_snapshot_rollback_rejects_unreported_control_plane_restart_before_mutation() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_platform_owned_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped, None);
        let service = service_for_teardown_test(conn);

        let error = service
            .rollback(internal_request(RollbackRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: "d-review-regression".to_string(),
                auto_restart: true,
                netns_path: None,
                route_identity: None,
            }))
            .await
            .expect_err("rollback must not mint a node-local identity for an API-owned sandbox");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("control plane"));
        assert_eq!(
            service
                .lookup_sandbox(&sandbox_id.to_string())
                .unwrap()
                .state,
            SandboxState::Stopped
        );
    }

    #[tokio::test(start_paused = true)]
    async fn process_snapshot_rollback_preserves_kubernetes_route_identity() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_platform_owned_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped, None);
        let service = service_for_teardown_test(conn);
        let route_identity = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(7).unwrap()),
            current_node_id: service.local_node_id,
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
            pod_uid: Some(PodUid::parse("019c0000-0000-7000-8000-000000000007").unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        };
        crate::db::update_runtime_fields(
            &service.db.lock().expect("mutex poisoned"),
            sandbox_id,
            crate::db::RuntimeFields {
                uid: 10000,
                slot: 7,
                guest_cid: 37,
                pid: std::process::id(),
                spaced_host_pid: Some(10001),
                netgtw_pid: 10002,
                vsock_uds_path: "/space/run/microvm/sandboxes/test/root/v.sock",
                api_socket_path: "/space/run/microvm/sandboxes/test/api.sock",
                spaced_endpoint_proof: None,
                space_api_route_identity: route_identity,
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            },
        )
        .unwrap();

        let error = service
            .rollback(internal_request(RollbackRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: "d-review-regression".to_string(),
                auto_restart: true,
                netns_path: None,
                route_identity: None,
            }))
            .await
            .expect_err("the fixture has no snapshot files");

        assert!(
            !error
                .message()
                .contains("restart it through the control plane"),
            "Kubernetes rollback must preserve its published identity before mutation: {error}"
        );
    }

    #[tokio::test]
    async fn rollback_rejects_missing_inputs_before_contacting_node_or_stopping_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_platform_owned_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started, None);
        let service = service_for_teardown_test(conn);
        for snapshot_name in ["d-missing-inputs", "f-missing-inputs"] {
            let error = service
                .rollback(internal_request(RollbackRequest {
                    sandbox_id: sandbox_id.to_string(),
                    snapshot_name: snapshot_name.into(),
                    auto_restart: false,
                    netns_path: None,
                    route_identity: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(error.message().contains("rootfs.ext4"), "{error}");
            assert_eq!(
                service
                    .lookup_sandbox(&sandbox_id.to_string())
                    .unwrap()
                    .state,
                SandboxState::Started
            );
        }
    }

    /// A suspended sandbox can't be rolled back, and neither can one that
    /// startup suspend recovery parked on its way to `suspended` or one a
    /// failed restore quarantined with its suspend pin: the refusal
    /// comes before anything is rewritten, and the sandbox keeps its suspend
    /// snapshot as its restore source. The same row in `stopped` gets past this
    /// check and fails later, on the fixture's missing snapshot files, which
    /// shows the refusal is the state check and not one of the input checks.
    #[tokio::test]
    async fn rollback_refuses_a_suspended_sandbox_before_rewriting_its_disk() {
        // `pinned: false` cases isolate the state and pending-completion terms:
        // with no suspend pin, the pin term can't refuse them.
        for (shape, pinned, refused) in [
            ("suspended", true, true),
            ("suspended, unpinned", false, true),
            ("recovering to suspended", true, true),
            ("recovering to suspended, unpinned", false, true),
            ("quarantined by a failed restore", true, true),
            // Isolates the pin term: the same quarantined row without a
            // suspend pin isn't restored from one, so it gets past the check.
            ("quarantined, unpinned", false, false),
            ("stopped", true, false),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            let pin = LocalArtifactId::new();
            match shape {
                "suspended" | "suspended, unpinned" => {
                    insert_platform_owned_sandbox(
                        &conn,
                        &sandbox_id,
                        SandboxState::Suspended,
                        None,
                    );
                }
                "recovering to suspended" | "recovering to suspended, unpinned" => {
                    insert_platform_owned_sandbox(
                        &conn,
                        &sandbox_id,
                        SandboxState::Suspending,
                        None,
                    );
                    crate::db::pin_suspended_artifact_for_cleanup(
                        &conn,
                        &sandbox_id,
                        pin,
                        "/cache/recovered-suspend",
                    )
                    .unwrap();
                }
                "quarantined by a failed restore" | "quarantined, unpinned" => {
                    insert_platform_owned_sandbox(
                        &conn,
                        &sandbox_id,
                        SandboxState::Quarantined,
                        None,
                    );
                }
                _ => {
                    insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Stopped, None);
                }
            }
            if pinned {
                conn.execute(
                    "UPDATE sandboxes
                        SET current_local_artifact_id = ?1,
                            current_local_artifact_kind = 'full',
                            current_local_artifact_origin = 'suspend'
                      WHERE id = ?2",
                    crate::db::fixture::params![pin.to_string(), sandbox_id],
                )
                .unwrap();
            } else {
                conn.execute(
                    "UPDATE sandboxes
                        SET current_local_artifact_id = NULL,
                            current_local_artifact_kind = NULL,
                            current_local_artifact_origin = NULL
                      WHERE id = ?1",
                    crate::db::fixture::params![sandbox_id],
                )
                .unwrap();
            }
            let service = service_for_teardown_test(conn);
            let before = service.lookup_sandbox(&sandbox_id).unwrap();

            let error = service
                .rollback(internal_request(RollbackRequest {
                    sandbox_id: sandbox_id.clone(),
                    snapshot_name: "f-before-suspend".into(),
                    auto_restart: false,
                    netns_path: None,
                    route_identity: None,
                }))
                .await
                .unwrap_err();

            assert_eq!(
                error.code(),
                tonic::Code::FailedPrecondition,
                "{shape}: {error}"
            );
            assert_eq!(
                error
                    .message()
                    .contains("whose restore source is its suspend snapshot"),
                refused,
                "{shape}: {error}"
            );
            assert_eq!(
                error.message().contains("rootfs.ext4"),
                !refused,
                "{shape}: {error}"
            );
            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            assert_eq!(row.state, before.state, "{shape}");
            assert_eq!(row.cleanup_completion, before.cleanup_completion, "{shape}");
            assert_eq!(
                row.current_local_artifact_id, before.current_local_artifact_id,
                "{shape}"
            );
            assert_eq!(
                row.current_local_artifact_origin, before.current_local_artifact_origin,
                "{shape}"
            );
            if pinned {
                assert_eq!(row.current_local_artifact_id, Some(pin), "{shape}");
                assert_eq!(
                    row.current_local_artifact_origin,
                    Some(ArtifactOrigin::Suspend),
                    "{shape}"
                );
            }
        }
    }

    /// **A pin recorded without the comparable marker reports no capture time,
    /// even when its times would read as current.** That's how an older
    /// binary left a pin it kept across a rollback of a stopped sandbox, seeded
    /// here as the rollback left the row. That binary didn't drop a stopped pin
    /// before rewriting the disk, and the rewrite changed no state, so the
    /// pin still names the pre-rollback bytes while its `created_at` is at or
    /// after `state_changed_at`. Reported, that time would read as current and
    /// let the drain give up the rolled-back disk. The same row recorded by
    /// this binary, with the comparable marker set and the same times, does
    /// report its time.
    #[tokio::test]
    async fn an_unmarked_pin_reports_no_capture_time_even_when_its_times_read_current() {
        let conn = crate::db::test_conn();
        let older = SandboxId::new().to_string();
        let current = SandboxId::new().to_string();
        let mut pin_of = std::collections::HashMap::new();
        for (id, comparable) in [(&older, 0), (&current, 1)] {
            insert_test_sandbox(&conn, id, SandboxState::Stopped);
            set_test_state_changed_at(&conn, id, "1700000050");
            let pin = LocalArtifactId::new();
            conn.execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     local_path, created_at, capture_time_comparable
                 ) VALUES (?1, ?2, 'disk', 'stop', '/cache/pre-rollback', '1700000060', ?3)",
                crate::db::fixture::params![pin.to_string(), id, comparable],
            )
            .unwrap();
            // The rollback rewrote the rootfs and wrote nothing to the row: the
            // pin stays, and so does the stamp of the row's entry into `stopped`.
            conn.execute(
                "UPDATE sandboxes
                    SET current_local_artifact_id = ?1,
                        current_local_artifact_kind = 'disk',
                        current_local_artifact_origin = 'stop'
                  WHERE id = ?2",
                crate::db::fixture::params![pin.to_string(), id],
            )
            .unwrap();
            pin_of.insert(id.clone(), pin);
        }
        let service = service_for_teardown_test(conn);

        let response = service
            .state(internal_request(StateRequest {
                sandbox_ids: vec![older.clone(), current.clone()],
            }))
            .await
            .unwrap()
            .into_inner();

        let info = |id: &str| {
            response
                .sandboxes
                .iter()
                .find(|info| info.sandbox_id == id)
                .unwrap_or_else(|| panic!("{id} missing from {response:?}"))
        };
        let seconds = |seconds| Some(prost_types::Timestamp { seconds, nanos: 0 });
        let older_info = info(&older);
        assert_eq!(
            older_info.current_local_artifact_id,
            Some(pin_of[&older].to_string()),
            "the pin is still reported, so the drain refuses rather than minting"
        );
        assert_eq!(older_info.state_changed_at, seconds(1_700_000_050));
        assert_eq!(older_info.current_local_artifact_created_at, None);
        let current_info = info(&current);
        assert_eq!(
            current_info.current_local_artifact_created_at,
            seconds(1_700_000_060)
        );
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    /// The node publishes when its pinned artifact was captured, beside the
    /// time the row entered its state, both in the node's own epoch seconds,
    /// including a capture in the stop's own second. A pin whose catalog row is
    /// missing is still reported as a pin, with no capture time, and so is one
    /// whose stored time can't be read and one an older node recorded, which
    /// isn't marked comparable; an unpinned sandbox reports neither, even when
    /// it owns an artifact.
    #[tokio::test]
    async fn sandbox_state_publishes_the_pinned_artifact_capture_time() {
        let conn = crate::db::test_conn();
        let captured = SandboxId::new().to_string();
        let tied = SandboxId::new().to_string();
        let dangling = SandboxId::new().to_string();
        let unreadable = SandboxId::new().to_string();
        let legacy = SandboxId::new().to_string();
        let unpinned = SandboxId::new().to_string();
        for id in [&captured, &tied, &dangling, &unreadable, &legacy, &unpinned] {
            insert_test_sandbox(&conn, id, SandboxState::Stopped);
            set_test_state_changed_at(&conn, id, "1700000050");
        }
        let mut artifact_for = std::collections::HashMap::new();
        for (id, created_at, comparable) in [
            (&captured, "1700000100", 1),
            (&tied, "1700000050", 1),
            (&unreadable, "soon", 1),
            (&legacy, "1700000100", 0),
            (&unpinned, "1700000200", 1),
        ] {
            let artifact = LocalArtifactId::new();
            conn.execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     local_path, created_at, capture_time_comparable
                 ) VALUES (?1, ?2, 'disk', 'stop', '/cache/artifact', ?3, ?4)",
                crate::db::fixture::params![artifact.to_string(), id, created_at, comparable],
            )
            .unwrap();
            artifact_for.insert(id.clone(), artifact);
        }
        let dangling_pin = LocalArtifactId::new();
        artifact_for.insert(dangling.clone(), dangling_pin);
        for id in [&captured, &tied, &dangling, &unreadable, &legacy] {
            conn.execute(
                "UPDATE sandboxes
                    SET current_local_artifact_id = ?1,
                        current_local_artifact_kind = 'disk',
                        current_local_artifact_origin = 'stop'
                  WHERE id = ?2",
                crate::db::fixture::params![artifact_for[id].to_string(), id],
            )
            .unwrap();
        }
        let service = service_for_teardown_test(conn);

        let response = service
            .state(internal_request(StateRequest {
                sandbox_ids: vec![
                    captured.clone(),
                    tied.clone(),
                    dangling.clone(),
                    unreadable.clone(),
                    legacy.clone(),
                    unpinned.clone(),
                ],
            }))
            .await
            .unwrap()
            .into_inner();

        let info = |id: &str| {
            response
                .sandboxes
                .iter()
                .find(|info| info.sandbox_id == id)
                .unwrap_or_else(|| panic!("{id} missing from {response:?}"))
        };
        let seconds = |seconds| Some(prost_types::Timestamp { seconds, nanos: 0 });
        let captured_info = info(&captured);
        assert_eq!(
            captured_info.current_local_artifact_id,
            Some(artifact_for[&captured].to_string())
        );
        assert_eq!(
            captured_info.current_local_artifact_created_at,
            seconds(1_700_000_100)
        );
        assert_eq!(captured_info.state_changed_at, seconds(1_700_000_050));
        let tied_info = info(&tied);
        assert_eq!(
            tied_info.current_local_artifact_created_at,
            seconds(1_700_000_050)
        );
        assert_eq!(tied_info.state_changed_at, seconds(1_700_000_050));
        for id in [&dangling, &unreadable, &legacy] {
            let info = info(id);
            assert_eq!(
                info.current_local_artifact_id,
                Some(artifact_for[id].to_string())
            );
            assert_eq!(info.current_local_artifact_created_at, None, "{id}");
            assert!(!info.state_degraded, "{id}");
        }
        let unpinned_info = info(&unpinned);
        assert_eq!(unpinned_info.current_local_artifact_id, None);
        assert_eq!(unpinned_info.current_local_artifact_created_at, None);
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    #[tokio::test]
    async fn template_manager_start_is_an_explicit_local_producer() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_pool_donor(&conn, sandbox_id);
        let service = service_for_teardown_test(conn);
        let request = tmplmgr_request(());
        let peer = service
            .authorize_peer(&request, "Start", ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR)
            .unwrap();
        let sandbox = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();

        assert_eq!(
            SandboxManagerService::identity_free_start_authority(&peer, &sandbox),
            IdentityFreeDirectActivationAuthority::LocalProducer
        );
    }

    #[test]
    fn runtime_activation_identity_completes_direct_route_incarnation_once() {
        if !crate::service::tests::enter_route_test_process(&[(
            "SPACE_NODE_ID",
            Some(std::ffi::OsStr::new("019f8000-0000-7000-8000-0000000000aa")),
        )]) {
            return;
        }

        let normalized = space_api_route_identity_for_runtime_activation(SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            ..SpaceApiRouteIdentity::default()
        });
        let identity = normalized.identity;
        let repeated = space_api_route_identity_for_runtime_activation(identity.clone()).identity;

        assert_eq!(
            identity.execution_generation,
            Some(ExecutionGeneration::new(1).unwrap())
        );
        assert_eq!(identity.substrate_kind, Some(NodeManagedBy::Space));
        assert!(identity.activation_attempt_id.is_some());
        assert!(normalized.provisional_activation_attempt);
        assert_eq!(identity, repeated);
    }

    #[test]
    fn runtime_activation_identity_does_not_promote_a_locally_minted_kubernetes_attempt() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let raw = SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            pod_uid: Some("019f8000-0000-7000-8000-0000000000bb".parse().unwrap()),
            ..SpaceApiRouteIdentity::default()
        };
        let normalized = space_api_route_identity_for_runtime_activation(raw);

        assert!(normalized.provisional_activation_attempt);
        assert_eq!(
            normalized.provenance,
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced
        );
        assert!(normalized.identity.activation_attempt_id.is_some());
    }

    #[test]
    fn runtime_activation_identity_preserves_explicit_kubernetes_substrate_without_pod_uid() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let identity = space_api_route_identity_from_wake_start_context(
            &pplx_space_sbx_mgr_client::proto::WakeStartContext {
                route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                    route_generation: Some(42),
                    execution_generation: Some(42),
                    substrate_kind: crate::proto::SandboxSubstrateKind::Kubernetes as i32,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let identity = space_api_route_identity_for_runtime_activation(identity).identity;

        assert_eq!(identity.substrate_kind, Some(NodeManagedBy::Kubernetes));
        assert_eq!(identity.pod_uid, None);
    }

    #[test]
    fn create_autostart_reuses_the_normalized_activation_attempt() {
        let normalized = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(7).unwrap()),
            execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..SpaceApiRouteIdentity::default()
        };

        let request = crate::service::create::autostart_request(
            "sandbox".to_string(),
            false,
            true,
            &normalized,
        );
        let forwarded = space_api_route_identity_from_start_request(&request).unwrap();

        assert_eq!(forwarded, normalized);
    }

    #[tokio::test]
    async fn create_autostart_does_not_fence_its_start_identity() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Created);
        let service = service_for_teardown_test(conn);
        let normalized = fresh_runtime_activation_identity(
            SpaceApiRouteIdentity::default(),
            false,
            service.local_node_id,
        )
        .unwrap();
        if let Some(evidence) = crate::service::create::create_route_evidence_before_start(
            &normalized.identity,
            normalized.provenance,
            true,
        ) {
            crate::db::auto_lifecycle_delete_receipts::record_retained_route_evidence(
                &service.db.lock().unwrap(),
                sandbox_id,
                evidence,
            )
            .unwrap();
        }

        let resolved = service
            .resolve_runtime_activation_identity(
                &sandbox_id.to_string(),
                normalized.identity.clone(),
                false,
            )
            .expect("autostart must be able to activate with the Create identity");

        assert_eq!(resolved.identity, normalized.identity);
    }

    fn service_with_base_snapshot_create_route_evidence()
    -> (SandboxManagerService, SandboxId, SpaceApiRouteIdentity) {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let service = service_for_teardown_test(conn);
        let identity = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: service.local_node_id,
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            pod_uid: Some(
                "019f8000-0000-7000-8000-0000000000dd"
                    .parse::<PodUid>()
                    .unwrap(),
            ),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        };
        let evidence = crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
            &identity,
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        let cleanup_id = SandboxCleanupEpisodeId::new();
        let sandbox_id_string = sandbox_id.to_string();
        {
            let db = service.db.lock().unwrap();
            crate::db::insert_sandbox_with_cleanup_episode_and_route_evidence(
                &db,
                crate::db::InsertSandboxParams {
                    guest_image: Default::default(),
                    id: &sandbox_id_string,
                    uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
                    state: SandboxState::Creating,
                    template: "seed",
                    template_hash: Some("sha256:test"),
                    user: &DEPLOY_SANDBOX_OWNER,
                    runtime: RuntimeKind::Firecracker.as_str(),
                    snapshots_enabled: false,
                    rootfs_size_mb: None,
                    disk_io_limits: Default::default(),
                    pool_template: None,
                    volume_encryption: false,
                    process_env: &BTreeMap::new(),
                    auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                    network_policy_override: None,
                    network_policy_generation: 0,
                    network_policy_authority: crate::db::PolicyAuthority::Cluster,
                    auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                },
                crate::db::CleanupEpisodeSpec {
                    id: cleanup_id,
                    completion: crate::db::CleanupCompletion::DeleteRow,
                    keep_filesystem: false,
                    entered_from_state: None,
                    activation_state: SandboxState::Creating,
                    execution_generation: identity.execution_generation,
                    activation_attempt_id: identity.activation_attempt_id,
                },
                Some(evidence),
                crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
                    &identity,
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
                )
                .as_ref(),
                None,
            )
            .unwrap();
            crate::db::test_fixtures::state_write_fixtures::apply_edge(
                &db,
                &sandbox_id_string,
                &crate::db::edges::CREATE_COMPLETE,
            );
            crate::db::test_fixtures::state_write_fixtures::apply_edge(
                &db,
                &sandbox_id_string,
                &crate::db::edges::CREATE_BASE_SNAPSHOT_COMPLETE,
            );
            assert!(crate::db::complete_create(&db, &sandbox_id_string, cleanup_id, true).unwrap());
            assert!(
                crate::db::route_activation_claims::claim_exists(&db, &sandbox_id_string).unwrap()
            );
        }
        (service, sandbox_id, identity)
    }

    #[tokio::test]
    async fn base_snapshot_create_wake_accepts_the_open_activation_identity() {
        let (service, sandbox_id, identity) = service_with_base_snapshot_create_route_evidence();

        let resolved = service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), identity.clone(), false)
            .expect("Wake must continue the complete identity retained by Create");

        assert_eq!(resolved.identity, identity);

        let without_node_id = SpaceApiRouteIdentity {
            current_node_id: None,
            ..identity.clone()
        };
        let resolved = service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), without_node_id, false)
            .expect("Wake may use the sandbox-manager's authoritative local node id");
        assert_eq!(resolved.identity, identity);
        assert!(resolved.open_activation_claim_eligible);
    }

    #[tokio::test]
    async fn two_step_create_wake_rejects_a_same_generation_replacement() {
        let (service, sandbox_id, retained) = service_with_base_snapshot_create_route_evidence();
        let replacement = SpaceApiRouteIdentity {
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..retained.clone()
        };

        let error = service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), replacement, false)
            .expect_err("a different attempt must advance the retained generation");

        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(error.message().contains("not newer than retained"));

        {
            let db = service.db.lock().unwrap();
            crate::db::mark_cleanup_required(
                &db,
                &sandbox_id.to_string(),
                true,
                crate::db::CleanupCompletion::MarkStopped,
                "test completed stop",
            )
            .unwrap();
            crate::db::complete_row_intent_cleanup(
                &db,
                &sandbox_id.to_string(),
                crate::db::CleanupCompletion::MarkStopped,
                None,
            )
            .unwrap();
            assert!(
                !crate::db::route_activation_claims::claim_exists(&db, &sandbox_id.to_string())
                    .unwrap()
            );
        }
        let error = service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), retained, false)
            .expect_err("a completed stop must close the activation claim");
        assert_eq!(error.code(), tonic::Code::Aborted);
    }

    #[tokio::test]
    async fn newer_kubernetes_validation_does_not_replace_the_open_claim() {
        let (service, sandbox_id, original) = service_with_base_snapshot_create_route_evidence();
        let replacement = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(2).unwrap()),
            execution_generation: Some(ExecutionGeneration::new(2).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..original.clone()
        };

        service
            .resolve_runtime_activation_identity(
                &sandbox_id.to_string(),
                replacement.clone(),
                false,
            )
            .expect("a newer authoritative activation must pass validation");

        let stored_attempt: String = service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT activation_attempt_id
                   FROM sandbox_open_route_activations
                  WHERE sandbox_id = ?1",
                [sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            stored_attempt,
            original.activation_attempt_id.unwrap().to_string(),
            "identity validation must not replace the durable Create claim"
        );

        service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), original.clone(), false)
            .expect("validation alone must not supersede the open Create claim");

        let direct_attempt = SandboxActivationAttemptId::new();
        {
            let db = service.db.lock().unwrap();
            crate::db::begin_activation_with_route_evidence(
                &db,
                &sandbox_id.to_string(),
                SandboxState::Suspended,
                crate::db::ActivationKind::ColdStart,
                Some(ExecutionGeneration::new(2).unwrap()),
                Some(direct_attempt),
                crate::db::ActivationRouteAdmission {
                    auto_lifecycle: None,
                    operation_activation_attempt_id: None,
                    evidence: Some(
                        crate::auto_lifecycle_delete_receipts::RouteEvidence {
                            execution_generation: ExecutionGeneration::new(2).unwrap(),
                            activation_attempt_id: direct_attempt,
                            substrate: NodeManagedBy::Space,
                            provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                        },
                    ),
                    open_claim: None,
                    runtime_upgrade: None,
                },
            )
            .unwrap()
            .expect("the later Direct activation must claim the sandbox row");
            assert!(
                !crate::db::route_activation_claims::claim_exists(&db, &sandbox_id.to_string())
                    .unwrap(),
                "a later non-Kubernetes activation must supersede the open Kubernetes claim"
            );
        }

        let error = service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), original, false)
            .expect_err("the superseded Kubernetes identity must not remain admissible");
        assert_eq!(error.code(), tonic::Code::Aborted);
    }

    #[tokio::test]
    async fn split_operation_wake_join_restart_receipt_proof() {
        use crate::db::SandboxStatePublicationMetadata;
        use crate::service::recovery::CleanupRecoveryAuthority;
        use pplx_space_common_core::types::SandboxTransitionReceiptKind;
        // Exercise the real Wake join before dropping all request/service state.
        // The NULL injection models carrying B only in volatile request memory.
        for (persist_operation, allocated) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("manager.sqlite");
            let mut conn = crate::db::Connection::open_at(&path).unwrap();
            crate::db::create_test_schema(&mut conn);
            let sandbox_id = SandboxId::new();
            let id = sandbox_id.to_string();
            insert_test_sandbox(&conn, &id, SandboxState::Stopped);
            let generation = ExecutionGeneration::new(7).unwrap();
            let route_attempt = SandboxActivationAttemptId::new();
            let operation = SandboxActivationAttemptId::new_after(route_attempt).unwrap();
            let route = SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                route_generation: Some(RouteGeneration::new(7).unwrap()),
                execution_generation: Some(generation),
                current_node_id: Some(NodeId::new()),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                activation_attempt_id: Some(route_attempt),
                pod_uid: Some("pod-proof".parse().unwrap()),
            };
            let provenance =
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced;
            let open_claim = crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(&route, provenance).unwrap();
            let episode = crate::db::begin_activation_with_route_evidence(
                &conn,
                &id,
                SandboxState::Stopped,
                crate::db::ActivationKind::ColdStart,
                Some(generation),
                Some(route_attempt),
                crate::db::ActivationRouteAdmission {
                    evidence: crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
                        &route, provenance,
                    ),
                    open_claim: Some(&open_claim),
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap();
            let service = service_for_teardown_test(conn);
            let result = service
                .wake_with_start_config(
                    id.clone(),
                    Some(Duration::ZERO),
                    WakeStartConfig {
                        operation_activation_attempt_id: Some(operation),
                        space_api_route_identity: route,
                        ..Default::default()
                    },
                    true,
                    None,
                )
                .await;
            assert!(result.is_err(), "request abandons its in-progress join");
            {
                let db = service.db.lock().unwrap();
                let joined = crate::db::cleanup_episode(&db, &id).unwrap().unwrap();
                assert_eq!(joined.id, episode.id);
                assert_eq!(joined.activation_attempt_id, Some(route_attempt));
                assert_eq!(joined.operation_activation_attempt_id, Some(operation));
                if !persist_operation {
                    db.execute("UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = NULL WHERE sandbox_id = ?1", [&id]).unwrap();
                }
            }
            // A launch acquired a runtime allocation before the process stopped.
            // No actual VM exists; only external teardown is substituted below.
            if allocated {
                service
                    .db
                    .lock()
                    .unwrap()
                    .execute("UPDATE sandboxes SET uid = 1000 WHERE id = ?1", [&id])
                    .unwrap();
            }
            drop(service);
            let recovered =
                service_for_teardown_test(crate::db::Connection::open_at(&path).unwrap());
            recovered
                .recover_interrupted_startup_states()
                .await
                .unwrap();
            let targets = SandboxManagerService::recover_required_cleanup_targets(
                &recovered.db,
                CleanupRecoveryAuthority::Startup,
            )
            .unwrap();
            assert_eq!(targets.len(), 1, "startup owns the abandoned activation");
            assert_eq!(targets[0].cleanup_id, Some(episode.id));
            *recovered.cleanup_executor.dispatch.lock().unwrap() =
                Some(Arc::new(|_, _| Box::pin(async { Ok(()) })));
            recovered.schedule_cleanup_recovery_targets(targets);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if crate::db::cleanup_episode(&recovered.db.lock().unwrap(), &id)
                        .unwrap()
                        .is_none()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            recovered.shutdown_token.cancel();
            recovered.task_tracker.close();
            recovered.task_tracker.wait().await;
            let db = recovered.db.lock().unwrap();
            let metadata =
                crate::db::sandbox_state_publication_metadata(&db, std::slice::from_ref(&id))
                    .unwrap()
                    .remove(&id)
                    .unwrap()
                    .unwrap()
                    .decode(SandboxState::Stopped)
                    .unwrap();
            assert!(
                matches!(metadata, SandboxStatePublicationMetadata::TransitionReceipt(receipt)
                if receipt.kind() == SandboxTransitionReceiptKind::ActivationRollback
                    && receipt.activation_attempt_id() == Some(if persist_operation { operation } else { route_attempt }))
            );
            assert!(crate::db::route_activation_claims::claim_exists(&db, &id).unwrap());
            assert!(
                crate::route_activation_claims::validate_cluster_fenced_activation(
                    &db,
                    &id,
                    &open_claim
                )
                .is_ok(),
                "rollback deliberately retains the continuation claim: a later user retry is still an owner, but does not run automatically"
            );
        }
    }

    #[tokio::test]
    async fn split_operation_reusing_episode_field_breaks_legacy_route_fence_proof() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Stopped);
        let route_a = SandboxActivationAttemptId::new();
        let operation_b = SandboxActivationAttemptId::new_after(route_a).unwrap();
        let generation = ExecutionGeneration::new(7).unwrap();
        let identity = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(7).unwrap()),
            execution_generation: Some(generation),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            activation_attempt_id: Some(route_a),
            pod_uid: Some("pod-proof".parse().unwrap()),
        };
        let episode = crate::db::begin_activation(
            &conn,
            &id,
            SandboxState::Stopped,
            crate::db::ActivationKind::ColdStart,
            Some(generation),
            Some(route_a),
        )
        .unwrap()
        .unwrap();
        let incoming = RouteStreamEpochIdentity::from_runtime_update(sandbox_id, identity)
            .unwrap()
            .unwrap();
        assert!(
            persisted_activation_owns_route_identity(&conn, sandbox_id, episode.id, &incoming)
                .unwrap()
        );
        // This predicate is unchanged from the old manager: reusing its field for B
        // makes a rolled-back manager reject the same legitimate route installation.
        conn.execute(
            "UPDATE sandbox_cleanup_episodes SET activation_attempt_id = ?2 WHERE sandbox_id = ?1",
            crate::db::fixture::params![id, operation_b.to_string()],
        )
        .unwrap();
        assert!(
            !persisted_activation_owns_route_identity(&conn, sandbox_id, episode.id, &incoming)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn open_activation_survives_startup_recovery() {
        let (service, sandbox_id, identity) = service_with_base_snapshot_create_route_evidence();
        crate::db::recover_interrupted_startup_states(&service.db.lock().unwrap())
            .expect("startup recovery must preserve an open activation claim");

        service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), identity, false)
            .expect("the surviving pod and attempt may continue after manager restart");
        assert!(
            crate::db::route_activation_claims::claim_exists(
                &service.db.lock().unwrap(),
                &sandbox_id.to_string()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn incomplete_kubernetes_identity_does_not_enter_the_claim_path() {
        let (service, sandbox_id, mut identity) =
            service_with_base_snapshot_create_route_evidence();
        {
            let db = service.db.lock().unwrap();
            crate::db::route_activation_claims::close_claim(&db, &sandbox_id.to_string()).unwrap();
        }
        identity.route_generation = Some(RouteGeneration::new(2).unwrap());
        identity.execution_generation = Some(ExecutionGeneration::new(2).unwrap());
        identity.activation_attempt_id = Some(SandboxActivationAttemptId::new());
        identity.substrate_kind = None;

        service
            .resolve_runtime_activation_identity(&sandbox_id.to_string(), identity, false)
            .expect("a newer rollout-compatible identity remains on the existing path");
        assert!(
            !crate::db::route_activation_claims::claim_exists(
                &service.db.lock().unwrap(),
                &sandbox_id.to_string()
            )
            .unwrap(),
            "only a complete wire identity may open a Kubernetes activation claim"
        );
    }

    #[tokio::test]
    async fn no_op_wake_leaves_a_locally_minted_provisional_attempt_inert() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let initial_identity = {
            let db = service.db.lock().unwrap();
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
                .space_api_route_identity
        };
        let normalized = space_api_route_identity_for_runtime_activation(SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            ..SpaceApiRouteIdentity::default()
        });
        assert!(normalized.provisional_activation_attempt);
        let provisional_attempt = normalized.identity.activation_attempt_id;

        service
            .ensure_active_for_guest_activity_with_start(
                None,
                &sandbox_id.to_string(),
                false,
                None,
                GuestActivityKind::Command,
                WakeStartConfig {
                    space_api_route_identity: normalized.identity,
                    authoritative_route_identity: true,
                    provisional_activation_attempt: true,
                    ..WakeStartConfig::default()
                },
            )
            .await
            .expect("a provisional route identity must not fail a no-op wake");

        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(row.space_api_route_identity, initial_identity);
        assert_ne!(initial_identity.activation_attempt_id, provisional_attempt);
        drop(db);
        assert!(
            service
                .route_stream_epochs
                .lock()
                .unwrap()
                .get(&sandbox_id)
                .is_none(),
            "an inert provisional identity must not publish a route epoch"
        );
    }

    #[tokio::test]
    async fn wake_path_adopts_only_control_plane_owned_identity_dimensions() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);

        let node_id = NodeId::new();
        let pod_uid: pplx_space_common_core::types::PodUid =
            "019f8000-0000-7000-8000-0000000000bb".parse().unwrap();
        // A pooled pod boots with no activation-attempt annotation, so the
        // runtime published a locally minted placeholder the control plane
        // has never seen.
        let placeholder_attempt = SandboxActivationAttemptId::new();
        let mut published = runtime_fields(ExecutionGeneration::new(4).unwrap(), 4);
        published.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(node_id),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            pod_uid: Some(pod_uid.clone()),
            activation_attempt_id: Some(placeholder_attempt),
            ..Default::default()
        };
        service
            .update_runtime_fields(sandbox_id, published)
            .unwrap();
        service.network_policy_reconcile_queue.drain_pending();

        // Claiming the pod mints an attempt and wakes an already-Started VM.
        let claimed_attempt = SandboxActivationAttemptId::new();
        let stale_node_id = NodeId::new();
        let stale_pod_uid: pplx_space_common_core::types::PodUid =
            "019f8000-0000-7000-8000-0000000000cc".parse().unwrap();
        assert_ne!(claimed_attempt, placeholder_attempt);
        let start = WakeStartConfig {
            space_api_route_identity: SpaceApiRouteIdentity {
                sandbox_id: Some(SandboxId::new()),
                route_generation: Some(RouteGeneration::new(4).unwrap()),
                current_node_id: Some(stale_node_id),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
                pod_uid: Some(stale_pod_uid),
                activation_attempt_id: Some(claimed_attempt),
            },
            authoritative_route_identity: true,
            ..WakeStartConfig::default()
        };
        service
            .finish_resident_wake_with_gateway_push(
                &sandbox_id.to_string(),
                &start,
                Some(GuestActivityKind::Command),
                |_, _| async {
                    Ok(NetGatewayRouteSecurityPushOutcome {
                        identity: NetGatewayRouteIdentityPushOutcome::Applied {
                            identity_profile_version: 1,
                        },
                        verification_keys: None,
                    })
                },
            )
            .await
            .expect("a no-op wake adopts the minted activation attempt");

        let row = {
            let db = service.db.lock().unwrap();
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            row.space_api_route_identity.activation_attempt_id,
            Some(claimed_attempt),
            "the token the API signs after the claim must match the runtime identity"
        );
        assert_eq!(row.space_api_route_identity.pod_uid, Some(pod_uid.clone()));
        assert_eq!(row.space_api_route_identity.current_node_id, Some(node_id));
        assert_eq!(row.space_api_route_identity.sandbox_id, Some(sandbox_id));
        assert_eq!(
            service
                .route_stream_epochs
                .lock()
                .unwrap()
                .get(&sandbox_id)
                .expect("adoption installs the authoritative epoch")
                .identity
                .wire_identity()
                .activation_attempt_id,
            Some(claimed_attempt)
        );

        // A repeated wake with the already-adopted identity is inert.
        service.network_policy_reconcile_queue.drain_pending();
        let repeated_start = WakeStartConfig {
            space_api_route_identity: SpaceApiRouteIdentity {
                activation_attempt_id: Some(claimed_attempt),
                ..SpaceApiRouteIdentity::default()
            },
            authoritative_route_identity: true,
            ..WakeStartConfig::default()
        };
        service
            .finish_resident_wake_with_gateway_push(
                &sandbox_id.to_string(),
                &repeated_start,
                Some(GuestActivityKind::Command),
                |_, _| async {
                    Ok(NetGatewayRouteSecurityPushOutcome {
                        identity: NetGatewayRouteIdentityPushOutcome::Applied {
                            identity_profile_version: 1,
                        },
                        verification_keys: None,
                    })
                },
            )
            .await
            .expect("an already-adopted wake identity remains a no-op");
        assert!(
            service
                .network_policy_reconcile_queue
                .drain_pending()
                .is_empty(),
            "an unchanged identity must not enqueue a reconcile"
        );

        // A second activation on the same execution generation — a resume —
        // rotates the attempt again, and the runtime must follow.
        let resumed_attempt = SandboxActivationAttemptId::new();
        let resumed_start = WakeStartConfig {
            space_api_route_identity: SpaceApiRouteIdentity {
                activation_attempt_id: Some(resumed_attempt),
                ..SpaceApiRouteIdentity::default()
            },
            authoritative_route_identity: true,
            ..WakeStartConfig::default()
        };
        service
            .finish_resident_wake_with_gateway_push(
                &sandbox_id.to_string(),
                &resumed_start,
                None,
                |_, _| async {
                    Ok(NetGatewayRouteSecurityPushOutcome {
                        identity: NetGatewayRouteIdentityPushOutcome::Applied {
                            identity_profile_version: 1,
                        },
                        verification_keys: None,
                    })
                },
            )
            .await
            .expect("a later wake adopts the next activation attempt");
        let row = {
            let db = service.db.lock().unwrap();
            crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            row.space_api_route_identity.activation_attempt_id,
            Some(resumed_attempt)
        );
    }

    #[tokio::test]
    async fn failed_paused_wake_does_not_adopt_the_next_activation_attempt() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = service_for_teardown_test(conn);

        let current_attempt = SandboxActivationAttemptId::new();
        let mut published = runtime_fields(ExecutionGeneration::new(4).unwrap(), 4);
        published.space_api_route_identity.activation_attempt_id = Some(current_attempt);
        service
            .update_runtime_fields(sandbox_id, published)
            .unwrap();

        let next_attempt = SandboxActivationAttemptId::new();
        let result = service
            .ensure_active_for_guest_activity_with_start(
                None,
                &sandbox_id.to_string(),
                false,
                None,
                GuestActivityKind::Command,
                WakeStartConfig {
                    space_api_route_identity: SpaceApiRouteIdentity {
                        activation_attempt_id: Some(next_attempt),
                        ..SpaceApiRouteIdentity::default()
                    },
                    authoritative_route_identity: true,
                    ..WakeStartConfig::default()
                },
            )
            .await;
        assert!(
            result.is_err(),
            "resume must fail when the resident net-gateway is unavailable"
        );

        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert_eq!(
            row.space_api_route_identity.activation_attempt_id,
            Some(current_attempt),
            "a failed resume must retain the resident gateway's activation identity"
        );
    }

    #[tokio::test]
    async fn paused_wake_adopts_the_next_activation_attempt_after_resume() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = SandboxManagerService::for_test(conn);

        let current_attempt = SandboxActivationAttemptId::new();
        let current_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            activation_attempt_id: Some(current_attempt),
            ..SpaceApiRouteIdentity::default()
        };
        crate::db::update_space_api_route_identity(
            &service.db.lock().unwrap(),
            sandbox_id,
            &current_identity,
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();

        let next_attempt = SandboxActivationAttemptId::new();
        let start = WakeStartConfig {
            space_api_route_identity: SpaceApiRouteIdentity {
                activation_attempt_id: Some(next_attempt),
                ..SpaceApiRouteIdentity::default()
            },
            authoritative_route_identity: true,
            ..WakeStartConfig::default()
        };
        let push_attempts = std::cell::Cell::new(0);
        service
            .finish_resident_wake_after_resume_with_gateway_push(
                &sandbox_id.to_string(),
                &start,
                None,
                async {
                    let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
                    assert_eq!(
                        row.space_api_route_identity.activation_attempt_id,
                        Some(current_attempt),
                        "the resident identity must remain current throughout resume"
                    );
                    Ok(())
                },
                |_, _| async {
                    push_attempts.set(push_attempts.get() + 1);
                    Ok(NetGatewayRouteSecurityPushOutcome {
                        identity: NetGatewayRouteIdentityPushOutcome::Applied {
                            identity_profile_version: 1,
                        },
                        verification_keys: None,
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(push_attempts.get(), 1, "resume must synchronize once");
        assert!(
            service
                .auto_lifecycle
                .since_last_wake_completed(&sandbox_id.to_string())
                .is_some(),
            "a completed resume must be recorded before gateway synchronization"
        );

        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(
            row.space_api_route_identity.activation_attempt_id,
            Some(next_attempt),
            "a successful resume must adopt before the wake returns"
        );
    }

    #[tokio::test]
    async fn authoritative_wake_requires_and_retries_gateway_identity_acknowledgement() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);

        let current_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            pod_uid: Some(PodUid::parse("wake-gateway-ack-pod").unwrap()),
            ..SpaceApiRouteIdentity::default()
        };
        crate::db::update_space_api_route_identity(
            &service.db.lock().unwrap(),
            sandbox_id,
            &current_identity,
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();

        let next_attempt = SandboxActivationAttemptId::new();
        let start = WakeStartConfig {
            space_api_route_identity: SpaceApiRouteIdentity {
                activation_attempt_id: Some(next_attempt),
                ..SpaceApiRouteIdentity::default()
            },
            authoritative_route_identity: true,
            ..WakeStartConfig::default()
        };
        let push_attempts = std::cell::Cell::new(0);
        let error = match service
            .finish_resident_wake_with_gateway_push(
                &sandbox_id.to_string(),
                &start,
                None,
                |_, _| async {
                    push_attempts.set(push_attempts.get() + 1);
                    Err(Status::unavailable("injected gateway outage"))
                },
            )
            .await
        {
            Ok(_) => panic!("wake must fail while the gateway has not acknowledged the identity"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(
            service
                .auto_lifecycle
                .since_last_wake_completed(&sandbox_id.to_string())
                .is_some(),
            "a running sandbox must record the wake even when gateway synchronization fails"
        );

        let pushed_identity = std::cell::RefCell::new(None);

        service
            .finish_resident_wake_with_gateway_push(
                &sandbox_id.to_string(),
                &start,
                None,
                |pushed_sandbox_id, identity| async {
                    push_attempts.set(push_attempts.get() + 1);
                    pushed_identity.replace(Some((pushed_sandbox_id, identity)));
                    Ok(NetGatewayRouteSecurityPushOutcome {
                        identity: NetGatewayRouteIdentityPushOutcome::Applied {
                            identity_profile_version: 1,
                        },
                        verification_keys: None,
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(
            push_attempts.get(),
            2,
            "a retry must republish even when SQLite already contains the identity"
        );

        let (pushed_sandbox_id, pushed_identity) = pushed_identity
            .into_inner()
            .expect("wake must publish the adopted identity before returning");
        assert_eq!(pushed_sandbox_id, sandbox_id.to_string());
        assert_eq!(
            pushed_identity.activation_attempt_id,
            Some(next_attempt),
            "the gateway must acknowledge the newly adopted attempt"
        );
    }

    #[tokio::test]
    async fn resident_wake_rejects_mismatched_binding_before_mutation() {
        for blocking in [false, true] {
            for state in [SandboxState::Started, SandboxState::Paused] {
                let conn = crate::db::test_conn();
                let id = SandboxId::new().to_string();
                insert_test_sandbox(&conn, &id, state);
                let service = service_for_teardown_test(conn);
                let start = WakeStartConfig {
                    space_api_route_identity: SpaceApiRouteIdentity {
                        sandbox_id: Some(SandboxId::new()),
                        ..Default::default()
                    },
                    ..Default::default()
                };
                let status = service
                    .wake_with_start_config(id.clone(), None, start, blocking, None)
                    .await
                    .unwrap_err();
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                assert_eq!(service.lookup_sandbox(&id).unwrap().state, state);
            }
        }
    }

    #[tokio::test]
    async fn authoritative_wake_route_security_outcomes_are_fail_closed() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        assert!(
            WAKE_ROUTE_SECURITY_CONNECT_RETRY_POLICY.max_elapsed < WAKE_ROUTE_SECURITY_SYNC_TIMEOUT,
            "the outer route-security deadline must not truncate its connect retries"
        );

        let outcome = |identity, verification_keys| NetGatewayRouteSecurityPushOutcome {
            identity,
            verification_keys,
        };
        require_wake_route_security_sync(outcome(
            NetGatewayRouteIdentityPushOutcome::Applied {
                identity_profile_version: 1,
            },
            Some(NetGatewayRouteVerificationKeyPushOutcome::Applied),
        ))
        .expect("applied route security");
        assert!(matches!(
            require_wake_route_security_sync(outcome(
                NetGatewayRouteIdentityPushOutcome::Absent,
                None,
            )),
            Err(WakeRouteSecuritySyncError::IdentityAbsent)
        ));
        assert!(matches!(
            require_wake_route_security_sync(outcome(
                NetGatewayRouteIdentityPushOutcome::Diverged,
                None,
            )),
            Err(WakeRouteSecuritySyncError::IdentityDiverged)
        ));
    }

    #[tokio::test]
    async fn route_identity_adoption_rejects_an_older_execution_generation() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);

        let mut published = runtime_fields(ExecutionGeneration::new(4).unwrap(), 4);
        published.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..Default::default()
        };
        service
            .update_runtime_fields(sandbox_id, published)
            .unwrap();

        let result = service
            .ensure_active_for_guest_activity_with_start(
                None,
                &sandbox_id.to_string(),
                false,
                None,
                GuestActivityKind::Command,
                WakeStartConfig {
                    space_api_route_identity: SpaceApiRouteIdentity {
                        execution_generation: Some(ExecutionGeneration::new(3).unwrap()),
                        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                        ..SpaceApiRouteIdentity::default()
                    },
                    authoritative_route_identity: true,
                    ..WakeStartConfig::default()
                },
            )
            .await;
        let Err(error) = result else {
            panic!("adoption stays fenced against a stale activation");
        };
        assert!(
            error
                .message()
                .contains("execution generation is older than the published stream epoch")
        );
    }

    #[tokio::test]
    async fn route_identity_adoption_validates_against_the_published_epoch() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);

        service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(3).unwrap(), 3),
            )
            .unwrap();
        let published = RouteStreamEpochIdentity(route_identity(
            sandbox_id,
            4,
            SandboxActivationAttemptId::new(),
        ));
        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(&mut epochs, sandbox_id, published.clone());
        }

        let error = service
            .adopt_authoritative_route_identity(
                sandbox_id,
                &SpaceApiRouteIdentity {
                    route_generation: Some(RouteGeneration::new(3).unwrap()),
                    execution_generation: Some(ExecutionGeneration::new(3).unwrap()),
                    activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                    ..SpaceApiRouteIdentity::default()
                },
            )
            .expect_err("adoption cannot move the published epoch backwards");

        assert_eq!(error.code(), tonic::Code::Aborted);
        assert_eq!(
            service
                .route_stream_epochs
                .lock()
                .unwrap()
                .get(&sandbox_id)
                .unwrap()
                .identity,
            published
        );
        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(
            row.space_api_route_identity.route_generation,
            Some(RouteGeneration::new(3).unwrap())
        );
    }

    #[tokio::test]
    async fn kubernetes_resident_adoption_repairs_only_a_missing_local_node() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        for (has_node, has_attempt, advances) in [
            (false, true, false),
            (false, true, true),
            (true, true, true),
            (false, false, true),
            (true, false, true),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new();
            insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
            let service = service_for_teardown_test(conn);
            let stored_node = has_node.then(NodeId::new);
            let attempt = SandboxActivationAttemptId::new();
            let identity = SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                route_generation: Some(RouteGeneration::new(4).unwrap()),
                current_node_id: stored_node,
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
                pod_uid: Some(PodUid::parse("resident-pod").unwrap()),
                activation_attempt_id: has_attempt.then_some(attempt),
            };
            crate::db::update_space_api_route_identity(
                &service.db.lock().unwrap(),
                sandbox_id,
                &identity,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
            let expected_node = stored_node.or(service.local_node_id);
            let candidate = if advances {
                SandboxActivationAttemptId::new_after(attempt).unwrap()
            } else {
                attempt
            };
            let start = WakeStartConfig {
                space_api_route_identity: SpaceApiRouteIdentity {
                    current_node_id: None,
                    activation_attempt_id: Some(candidate),
                    ..identity.clone()
                },
                authoritative_route_identity: true,
                ..WakeStartConfig::default()
            };
            let row = service
                .finish_resident_wake_with_gateway_push(
                    &sandbox_id.to_string(),
                    &start,
                    None,
                    |_, pushed| async move {
                        assert_eq!(pushed.current_node_id, expected_node);
                        assert_eq!(pushed.activation_attempt_id, Some(candidate));
                        pushed.try_into_complete().unwrap();
                        Ok(NetGatewayRouteSecurityPushOutcome {
                            identity: NetGatewayRouteIdentityPushOutcome::Applied {
                                identity_profile_version: 1,
                            },
                            verification_keys: None,
                        })
                    },
                )
                .await
                .unwrap();
            assert_eq!(row.space_api_route_identity.current_node_id, expected_node);
            assert_eq!(row.space_api_route_identity.pod_uid, identity.pod_uid);
            let epoch = service
                .route_stream_epochs
                .lock()
                .unwrap()
                .get(&sandbox_id)
                .unwrap()
                .identity
                .clone();
            assert_eq!(epoch.wire_identity().current_node_id, expected_node);
            service.network_policy_reconcile_queue.drain_pending();
            // An incoming node is never authority, and a retry does not install
            // another epoch or enqueue another reconciliation.
            let mut repeated = start.space_api_route_identity.clone();
            repeated.current_node_id = Some(NodeId::new());
            service
                .adopt_authoritative_route_identity(sandbox_id, &repeated)
                .unwrap();
            assert_eq!(
                service
                    .route_stream_epochs
                    .lock()
                    .unwrap()
                    .get(&sandbox_id)
                    .unwrap()
                    .identity,
                epoch
            );
            assert!(
                service
                    .network_policy_reconcile_queue
                    .drain_pending()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn resident_operation_receipt_is_durable_before_gateway_failure() {
        for (resident, target) in [
            (SandboxState::Started, SandboxState::Started),
            (SandboxState::Paused, SandboxState::Paused),
            (SandboxState::Started, SandboxState::Paused),
        ] {
            for gateway_fails in [false, true] {
                let conn = crate::db::test_conn();
                let sandbox_id = SandboxId::new();
                let id = sandbox_id.to_string();
                insert_test_sandbox(&conn, &id, resident);
                let service = service_for_teardown_test(conn);
                let route_attempt = SandboxActivationAttemptId::new();
                let operation = SandboxActivationAttemptId::new_after(route_attempt).unwrap();
                let route = SpaceApiRouteIdentity {
                    sandbox_id: Some(sandbox_id),
                    route_generation: Some(RouteGeneration::new(4).unwrap()),
                    current_node_id: service.local_node_id,
                    substrate_kind: Some(NodeManagedBy::Kubernetes),
                    execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
                    pod_uid: Some(PodUid::parse("resident-operation-pod").unwrap()),
                    activation_attempt_id: Some(route_attempt),
                };
                crate::db::update_space_api_route_identity(
                    &service.db.lock().unwrap(),
                    sandbox_id,
                    &route,
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
                )
                .unwrap();
                let start = WakeStartConfig {
                    operation_activation_attempt_id: Some(operation),
                    target_state: target,
                    space_api_route_identity: SpaceApiRouteIdentity {
                        current_node_id: None,
                        ..route.clone()
                    },
                    authoritative_route_identity: true,
                    ..Default::default()
                };
                let result = service
                    .finish_resident_wake_with_gateway_push(
                        &id,
                        &start,
                        None,
                        |_, pushed| async move {
                            assert_eq!(pushed.activation_attempt_id, Some(route_attempt));
                            if gateway_fails {
                                return Err(Status::unavailable("test gateway failure"));
                            }
                            Ok(NetGatewayRouteSecurityPushOutcome {
                                identity: NetGatewayRouteIdentityPushOutcome::Applied {
                                    identity_profile_version: 1,
                                },
                                verification_keys: None,
                            })
                        },
                    )
                    .await;
                assert_eq!(result.is_err(), gateway_fails);
                assert_eq!(
                    service
                        .lookup_sandbox(&id)
                        .unwrap()
                        .space_api_route_identity,
                    route
                );
                if resident != target {
                    // Wake(Paused) deliberately does not pause a Started VM.
                    // Keep that response contract, but do not claim operation
                    // completion for a target that was not reached.
                    let kind: Option<String> = service
                        .db
                        .lock()
                        .unwrap()
                        .query_row(
                            "SELECT transition_receipt_kind FROM sandboxes WHERE id = ?1",
                            [&id],
                            |row| row.get(0),
                        )
                        .unwrap();
                    assert!(kind.is_none());
                    continue;
                }
                let receipt = crate::db::sandbox_state_publication_metadata(
                    &service.db.lock().unwrap(),
                    std::slice::from_ref(&id),
                )
                .unwrap()
                .remove(&id)
                .unwrap()
                .unwrap()
                .decode(target)
                .unwrap();
                assert!(
                    matches!(receipt, crate::db::SandboxStatePublicationMetadata::TransitionReceipt(receipt)
                    if receipt.activation_attempt_id() == Some(operation))
                );
                // Retaining a higher-ID receipt must not reject a no-op Wake:
                // UUID order across already-completed owners is not a lease
                // sequence. Exact current ownership is checked centrally.
                crate::db::complete_resident_wake_operation(
                    &service.db.lock().unwrap(),
                    &id,
                    &route,
                    route_attempt,
                    target,
                )
                .unwrap();
                let retained: String = service.db.lock().unwrap().query_row(
                    "SELECT transition_receipt_activation_attempt_id FROM sandboxes WHERE id = ?1",
                    [&id], |row| row.get(0),
                ).unwrap();
                assert_eq!(retained, operation.to_string());
            }
        }
    }

    #[tokio::test]
    async fn kubernetes_node_hydration_does_not_relax_the_previous_epoch_floor() {
        for local_node_available in [false, true] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new();
            insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
            let mut service = service_for_teardown_test(conn);
            if !local_node_available {
                service.local_node_id = None;
            }
            let stale = SandboxActivationAttemptId::new();
            let identity = SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                route_generation: Some(RouteGeneration::new(4).unwrap()),
                current_node_id: None,
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
                pod_uid: Some(PodUid::parse("resident-pod").unwrap()),
                activation_attempt_id: Some(SandboxActivationAttemptId::new_after(stale).unwrap()),
            };
            crate::db::update_space_api_route_identity(
                &service.db.lock().unwrap(),
                sandbox_id,
                &identity,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
            let incoming = SpaceApiRouteIdentity {
                route_generation: Some(RouteGeneration::new(3).unwrap()),
                execution_generation: Some(ExecutionGeneration::new(3).unwrap()),
                activation_attempt_id: Some(stale),
                ..identity.clone()
            };
            let result = service.adopt_authoritative_route_identity(sandbox_id, &incoming);
            if local_node_available {
                assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
            } else {
                result.unwrap(); // Remains unpublishable; no invented node.
            }
            let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
            assert_eq!(row.space_api_route_identity, identity);
            assert!(
                !service
                    .route_stream_epochs
                    .lock()
                    .unwrap()
                    .contains_key(&sandbox_id)
            );
        }
    }

    #[tokio::test]
    async fn identity_equal_authoritative_adoption_promotes_unfenced_evidence() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..SpaceApiRouteIdentity::default()
        };
        {
            let db = service.db.lock().unwrap();
            crate::db::update_space_api_route_identity(
                &db,
                sandbox_id,
                &identity,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
        }

        service
            .adopt_authoritative_route_identity(sandbox_id, &identity)
            .unwrap();

        let db = service.db.lock().unwrap();
        let (current_provenance, retained_provenance): (String, String) = db
            .query_row(
                "SELECT space_api_route_identity_provenance,
                        last_observed_route_identity_provenance
                   FROM sandboxes WHERE id = ?1",
                [sandbox_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            (current_provenance.as_str(), retained_provenance.as_str()),
            ("cluster_fenced", "cluster_fenced")
        );
    }

    #[tokio::test]
    async fn authoritative_adoption_cannot_downgrade_complete_local_route_provenance() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let attempt = SandboxActivationAttemptId::new();
        let identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            activation_attempt_id: Some(attempt),
            ..SpaceApiRouteIdentity::default()
        };
        {
            let db = service.db.lock().unwrap();
            crate::db::update_space_api_route_identity(
                &db,
                sandbox_id,
                &identity,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
            )
            .unwrap();
        }
        let mut incoming = identity.clone();
        incoming.pod_uid = None;

        service
            .adopt_authoritative_route_identity(sandbox_id, &incoming)
            .unwrap();

        let db = service.db.lock().unwrap();
        let (current, retained): (String, String) = db
            .query_row(
                "SELECT space_api_route_identity_provenance,
                        last_observed_route_identity_provenance
                   FROM sandboxes WHERE id = ?1",
                [sandbox_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            (current.as_str(), retained.as_str()),
            ("cluster_fenced", "cluster_fenced")
        );
    }

    #[tokio::test]
    async fn authoritative_adoption_cannot_fence_an_incomplete_local_route() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let attempt = SandboxActivationAttemptId::new();
        let partial = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            activation_attempt_id: Some(attempt),
            ..SpaceApiRouteIdentity::default()
        };
        {
            let db = service.db.lock().unwrap();
            crate::db::update_space_api_route_identity(
                &db,
                sandbox_id,
                &partial,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
        }
        let incoming = SpaceApiRouteIdentity {
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            ..partial
        };

        service
            .adopt_authoritative_route_identity(sandbox_id, &incoming)
            .unwrap();

        let db = service.db.lock().unwrap();
        let provenance: String = db
            .query_row(
                "SELECT space_api_route_identity_provenance
                   FROM sandboxes WHERE id = ?1",
                [sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(provenance, "unfenced");
    }

    #[tokio::test]
    async fn direct_hydration_only_preserves_metadata_path_and_streams() {
        for missing_node in [true, false] {
            for has_cached_epoch in [false, true] {
                let conn = crate::db::test_conn();
                let sandbox_id = SandboxId::new();
                insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
                let service = service_for_teardown_test(conn);
                let complete = SpaceApiRouteIdentity {
                    sandbox_id: Some(sandbox_id),
                    route_generation: Some(RouteGeneration::new(4).unwrap()),
                    current_node_id: service.local_node_id,
                    substrate_kind: Some(NodeManagedBy::Space),
                    execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
                    activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                    ..SpaceApiRouteIdentity::default()
                };
                let mut partial = complete.clone();
                if missing_node {
                    partial.current_node_id = None;
                } else {
                    partial.substrate_kind = None;
                }
                {
                    let db = service.db.lock().unwrap();
                    crate::db::update_space_api_route_identity(
                        &db,
                        sandbox_id,
                        &partial,
                        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                    )
                    .unwrap();
                    // Migration-era metadata needs the existing Direct
                    // hydration write, not a new route-epoch transition.
                    db.execute(
                        "UPDATE sandboxes SET space_api_route_identity_provenance = NULL WHERE id = ?1",
                        [sandbox_id.to_string()],
                    ).unwrap();
                }
                let cached = has_cached_epoch.then(|| {
                    let identity =
                        RouteStreamEpochIdentity::from_runtime_update(sandbox_id, complete.clone())
                            .unwrap()
                            .unwrap();
                    let mut epochs = service.route_stream_epochs.lock().unwrap();
                    install_authoritative_route_epoch(&mut epochs, sandbox_id, identity.clone());
                    (identity, epochs.get(&sandbox_id).unwrap().fence.child())
                });

                for _ in 0..2 {
                    service
                        .adopt_authoritative_route_identity(sandbox_id, &complete)
                        .expect("Direct hydration-only adoption retains its metadata path");
                    let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
                    assert_eq!(row.space_api_route_identity, complete);
                    let epochs = service.route_stream_epochs.lock().unwrap();
                    match &cached {
                        Some((identity, stream)) => {
                            assert_eq!(&epochs.get(&sandbox_id).unwrap().identity, identity);
                            assert!(
                                !stream.is_cancelled(),
                                "hydration must not cancel Direct streams"
                            );
                        }
                        None => assert!(
                            !epochs.contains_key(&sandbox_id),
                            "metadata-only Direct hydration does not install an epoch"
                        ),
                    }
                    assert!(
                        service
                            .network_policy_reconcile_queue
                            .drain_pending()
                            .is_empty()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn direct_authoritative_adoption_hydrates_local_runtime_dimensions() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let incoming = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..SpaceApiRouteIdentity::default()
        };

        service
            .adopt_authoritative_route_identity(sandbox_id, &incoming)
            .unwrap();

        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(
            row.space_api_route_identity.current_node_id,
            service.local_node_id
        );
        assert_eq!(
            row.space_api_route_identity.substrate_kind,
            Some(NodeManagedBy::Space)
        );
    }

    #[tokio::test]
    async fn identity_equal_authoritative_adoption_retires_an_older_receipt() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..SpaceApiRouteIdentity::default()
        };
        let pending_receipt_id =
            pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate();
        {
            let db = service.db.lock().unwrap();
            crate::db::update_space_api_route_identity(
                &db,
                sandbox_id,
                &identity,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
            )
            .unwrap();
            crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(
                &db,
                crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
                    receipt_id: pending_receipt_id,
                    sandbox_id,
                    execution_generation: ExecutionGeneration::new(3).unwrap(),
                    route_activation_id: SandboxActivationAttemptId::new(),
                    policy_fingerprint:
                        pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                            &AutoLifecyclePolicy::disabled(),
                        ),
                },
            )
            .unwrap();
        }

        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        service
            .adopt_authoritative_route_identity(sandbox_id, &identity)
            .unwrap();
        drop(guard);

        let db = service.db.lock().unwrap();
        assert!(
            crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                &db, sandbox_id,
            )
            .unwrap()
            .is_none(),
            "an unchanged generation-4 adoption must retire a generation-3 receipt"
        );
        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "delete_superseded")
            })
            .expect("receipt supersession must be visible in the sandbox timeline");
        assert_eq!(
            event.get("reason").map(String::as_str),
            Some("pending delete receipt superseded by execution generation 4")
        );
        assert_eq!(
            event.get("execution_generation").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            event.get("receipt_id"),
            Some(&pending_receipt_id.to_string())
        );
    }

    #[tokio::test]
    async fn identity_equal_authoritative_adoption_preserves_a_current_receipt() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let attempt = SandboxActivationAttemptId::new();
        let identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
            activation_attempt_id: Some(attempt),
            ..SpaceApiRouteIdentity::default()
        };
        let pending = crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
            receipt_id: pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            execution_generation: ExecutionGeneration::new(4).unwrap(),
            route_activation_id: attempt,
            policy_fingerprint:
                pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                    &AutoLifecyclePolicy::disabled(),
                ),
        };
        {
            let db = service.db.lock().unwrap();
            crate::db::update_space_api_route_identity(
                &db,
                sandbox_id,
                &identity,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
            )
            .unwrap();
            crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(&db, pending)
                .unwrap();
            db.execute_batch(
                "CREATE TRIGGER reject_redundant_route_identity_adoption
                 BEFORE UPDATE OF space_api_route_identity_provenance ON sandboxes
                 BEGIN
                     SELECT RAISE(ABORT, 'route identity metadata was already current');
                 END;",
            )
            .unwrap();
        }

        service
            .adopt_authoritative_route_identity(sandbox_id, &identity)
            .expect("an identical wake must remain an inert success");

        {
            let db = service.db.lock().unwrap();
            db.execute(
                "UPDATE sandboxes
                    SET cleanup_completion = 'delete_row',
                        cleanup_keep_filesystem = 0
                  WHERE id = ?1",
                [sandbox_id.to_string()],
            )
            .unwrap();
        }
        service
            .adopt_authoritative_route_identity(sandbox_id, &identity)
            .expect("an identical wake must not disturb an armed cleanup intent");

        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                &db, sandbox_id,
            )
            .unwrap(),
            Some(pending)
        );
    }

    #[tokio::test]
    async fn route_identity_adoption_reports_cleanup_ownership_as_aborted() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let current_node_id = NodeId::new();
        let pod_uid = PodUid::parse("pod-uid").unwrap();
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                route_generation: Some(RouteGeneration::new(3).unwrap()),
                current_node_id: Some(current_node_id),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                execution_generation: Some(ExecutionGeneration::new(3).unwrap()),
                pod_uid: Some(pod_uid.clone()),
                activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        conn.execute(
            "UPDATE sandboxes
                SET state = 'deleting',
                    cleanup_completion = 'delete_row',
                    cleanup_keep_filesystem = 0
              WHERE id = ?1",
            [sandbox_id.to_string()],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);
        let incoming = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            current_node_id: Some(current_node_id),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(4).unwrap()),
            pod_uid: Some(pod_uid),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        };

        let metric_capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let error = service
            .adopt_authoritative_route_identity(sandbox_id, &incoming)
            .expect_err("cleanup ownership must reject route-only adoption");
        let counters = metric_capture.finish();

        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(counters.iter().any(|counter| {
            counter.name == "sandbox.worker.route_identity.adoption"
                && counter.tags.iter().any(|tag| tag == "outcome:rejected")
                && counter
                    .tags
                    .iter()
                    .any(|tag| tag == "reason:lifecycle_owns_execution")
        }));
    }

    #[test]
    fn route_identity_adoption_rejection_reasons_distinguish_write_fences() {
        use pplx_space_metrics::sandbox::worker::RouteIdentityAdoptionReason;

        assert_eq!(
            route_identity_adoption_write_rejection_reason(&crate::db::Error::no_rows()),
            RouteIdentityAdoptionReason::NotFound
        );
        assert_eq!(
            route_identity_adoption_write_rejection_reason(&crate::db::Error::to_sql_conversion(
                crate::db::auto_lifecycle_delete_receipts::PendingReceiptOwnsExecution,
            ),),
            RouteIdentityAdoptionReason::PendingReceiptOwnsExecution
        );
        assert_eq!(
            route_identity_adoption_write_rejection_reason(&crate::db::Error::to_sql_conversion(
                crate::db::auto_lifecycle_delete_receipts::LifecycleOwnsExecution,
            ),),
            RouteIdentityAdoptionReason::LifecycleOwnsExecution
        );
    }

    #[tokio::test]
    async fn list_build_referencing_sandboxes_includes_user_sandboxes_and_scopes_out_deleting() {
        let conn = crate::db::test_conn();

        let insert = |conn: &crate::db::Connection,
                      id: &str,
                      state: SandboxState,
                      hash: &str,
                      user: &SandboxOwner,
                      pool_template: Option<&str>| {
            let uuid8 = SandboxRuntimeLabel::from_sandbox_id(SandboxId::from_str(id).unwrap());
            crate::db::insert_sandbox(
                conn,
                crate::db::InsertSandboxParams {
                    guest_image: Default::default(),
                    id,
                    uuid8: &uuid8,
                    state,
                    template: "app-runtime",
                    template_hash: Some(hash),
                    user,
                    runtime: RuntimeKind::Firecracker.as_str(),
                    snapshots_enabled: false,
                    rootfs_size_mb: None,
                    disk_io_limits: Default::default(),
                    pool_template,
                    volume_encryption: false,
                    process_env: &BTreeMap::new(),
                    auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                    network_policy_override: None,
                    network_policy_generation: 0,
                    network_policy_authority: crate::db::PolicyAuthority::Cluster,
                    auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                },
            )
            .unwrap();
        };

        let user = SandboxOwner::parse("user:00000000-0000-0000-0000-000000000042").unwrap();
        let tmplmgr = &pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER;

        // Adopted user sandbox holding a live instance subvolume.
        let user_sandbox = SandboxId::new().to_string();
        insert(
            &conn,
            &user_sandbox,
            SandboxState::Started,
            "sha-user",
            &user,
            None,
        );
        // Template-manager-owned pool sandbox (visible to List too).
        let pool_sandbox = SandboxId::new().to_string();
        insert(
            &conn,
            &pool_sandbox,
            SandboxState::Paused,
            "sha-pool",
            tmplmgr,
            Some("app-runtime"),
        );
        // User sandbox mid-teardown: its instance subvolume is being removed,
        // so it must NOT pin its parent.
        let deleting_sandbox = SandboxId::new().to_string();
        insert(
            &conn,
            &deleting_sandbox,
            SandboxState::Deleting,
            "sha-deleting",
            &user,
            None,
        );

        let service = SandboxManagerService::for_test(conn);

        let referencing = service
            .list_build_referencing_sandboxes(tmplmgr_request(
                ListBuildReferencingSandboxesRequest {},
            ))
            .await
            .unwrap()
            .into_inner();
        let referencing_hashes: std::collections::HashSet<String> = referencing
            .sandboxes
            .iter()
            .filter_map(|s| s.image_hash.clone())
            .collect();
        assert!(
            referencing_hashes.contains("sha-user"),
            "user sandbox build must be protected: {referencing_hashes:?}"
        );
        assert!(
            referencing_hashes.contains("sha-pool"),
            "pool sandbox build must be protected: {referencing_hashes:?}"
        );
        assert!(
            !referencing_hashes.contains("sha-deleting"),
            "deleting sandbox must not pin its parent: {referencing_hashes:?}"
        );

        // Contrast: the owner-filtered List hides the user sandbox from a
        // template-manager peer — the gap this RPC closes.
        let listed = service
            .list(tmplmgr_request(ListRequest::default()))
            .await
            .unwrap()
            .into_inner();
        let listed_hashes: std::collections::HashSet<String> = listed
            .sandboxes
            .iter()
            .filter_map(|s| s.image_hash.clone())
            .collect();
        assert!(
            !listed_hashes.contains("sha-user"),
            "List must still hide user sandboxes from tmplmgr peer: {listed_hashes:?}"
        );
        assert!(
            listed_hashes.contains("sha-pool"),
            "List must still surface tmplmgr-owned sandboxes: {listed_hashes:?}"
        );

        service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET user = '' WHERE id = ?1",
                [&user_sandbox],
            )
            .unwrap();
        // Neither owner filtering nor build GC may interpret an undecodable
        // listed field as an authoritative absence of dependencies.
        for result in [
            service
                .list(tmplmgr_request(ListRequest::default()))
                .await
                .map(|_| ()),
            service
                .list_build_referencing_sandboxes(tmplmgr_request(
                    ListBuildReferencingSandboxesRequest {},
                ))
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Internal);
        }
    }

    async fn list_all_pages(
        service: &SandboxManagerService,
        tmplmgr: bool,
        page_size: u32,
    ) -> Vec<Vec<SandboxListElement>> {
        let mut pages = Vec::new();
        let mut page_token = String::new();
        loop {
            let request = ListRequest {
                page_size,
                page_token,
            };
            let request = if tmplmgr {
                tmplmgr_request(request)
            } else {
                internal_request(request)
            };
            let page = service.list(request).await.unwrap().into_inner();
            pages.push(page.sandboxes);
            if page.next_page_token.is_empty() {
                return pages;
            }
            page_token = page.next_page_token;
        }
    }

    #[tokio::test]
    async fn paged_list_covers_every_sandbox_once_and_filters_without_ending_early() {
        let conn = crate::db::test_conn();
        let mut ids = (0..5)
            .map(|_| {
                let id = SandboxId::new().to_string();
                insert_test_sandbox(&conn, &id, SandboxState::Started);
                id
            })
            .collect::<Vec<_>>();
        ids.sort();
        // Only the last sandbox is template-manager-owned, so earlier pages
        // are empty for a template-manager peer yet must continue the scan.
        conn.execute(
            "UPDATE sandboxes SET user = ?2 WHERE id = ?1",
            crate::db::fixture::params![
                ids[4],
                pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER.as_str()
            ],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let pages = list_all_pages(&service, false, 2).await;
        assert_eq!(
            pages.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        let listed = pages
            .into_iter()
            .flatten()
            .map(|sandbox| sandbox.sandbox_id)
            .collect::<Vec<_>>();
        assert_eq!(listed, ids);

        let pages = list_all_pages(&service, true, 2).await;
        assert_eq!(
            pages.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![0, 0, 1]
        );
        assert_eq!(pages[2][0].sandbox_id, ids[4]);

        assert_eq!(
            service
                .list(internal_request(ListRequest {
                    page_size: 1,
                    page_token: "x".repeat(257),
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    #[tokio::test]
    async fn paged_list_splits_at_the_encoded_budget_and_degrades_undecodable_rows() {
        use pplx_space_common_core::inventory::MAX_ENCODED_BYTES;
        let conn = crate::db::test_conn();
        let mut ids = (0..3)
            .map(|_| {
                let id = SandboxId::new().to_string();
                insert_test_sandbox(&conn, &id, SandboxState::Started);
                id
            })
            .collect::<Vec<_>>();
        ids.sort();
        // Two listed templates fit the raw budget but not one encoded page.
        let template = "t".repeat(MAX_ENCODED_BYTES / 2 + 1);
        for id in &ids[..2] {
            conn.execute(
                "UPDATE sandboxes SET template = ?2 WHERE id = ?1",
                crate::db::fixture::params![id, template],
            )
            .unwrap();
        }
        conn.execute("UPDATE sandboxes SET user = '' WHERE id = ?1", [&ids[2]])
            .unwrap();
        let service = service_for_teardown_test(conn);

        let pages = list_all_pages(&service, false, 128).await;
        assert_eq!(pages.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 2]);
        let degraded = &pages[1][1];
        assert_eq!(degraded.sandbox_id, ids[2]);
        assert_eq!(degraded.template, None);
        assert_eq!(
            degraded.lifecycle,
            Some(crate::proto::SandboxLifecycleState::Started as i32)
        );
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    #[tokio::test]
    async fn unary_build_referencing_list_reports_rather_than_rejects_an_over_budget_inventory() {
        use pplx_space_common_core::inventory::MAX_RAW_BYTES;
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        // A listed field past both the raw and the encoded budgets.
        let template = "t".repeat(MAX_RAW_BYTES + 1);
        conn.execute("UPDATE sandboxes SET template = ?1", [template])
            .unwrap();
        let service = service_for_teardown_test(conn);
        // Paged List rejects a single row it cannot fit on any page.
        assert_eq!(
            service
                .list(internal_request(ListRequest::default()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        service
            .list_build_referencing_sandboxes(tmplmgr_request(
                ListBuildReferencingSandboxesRequest {},
            ))
            .await
            .unwrap();
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    #[tokio::test]
    async fn inventory_limits_are_errors_not_partial_or_degraded_success() {
        use pplx_space_common_core::inventory::{MAX_ITEMS, MAX_RAW_BYTES};
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET process_env_json = zeroblob(?1)",
            [(MAX_RAW_BYTES + 1) as i64],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);
        let result = service
            .state(internal_request(StateRequest {
                sandbox_ids: vec![id.clone()],
            }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
        // Raw input count is checked before duplicate reduction or any node RPC.
        let duplicate_ids = vec![id; MAX_ITEMS + 1];
        for result in [
            service
                .state(internal_request(StateRequest {
                    sandbox_ids: duplicate_ids.clone(),
                }))
                .await
                .map(|_| ()),
            service
                .get_disk_usage(internal_request(GetDiskUsageRequest {
                    sandbox_ids: duplicate_ids.clone(),
                }))
                .await
                .map(|_| ()),
            service
                .list_snapshots(internal_request(ListSnapshotsRequest {
                    sandbox_ids: duplicate_ids,
                }))
                .await
                .map(|_| ()),
        ] {
            assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
        }
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    #[test]
    fn net_gateway_sidecar_pid_name_matches_launched_binary() {
        let bin_name = Path::new(pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN)
            .file_name()
            .and_then(|name| name.to_str())
            .expect("net-gateway service path has a file name");

        assert_eq!(NET_GATEWAY_SIDECAR_PID_NAME, bin_name);
    }

    #[test]
    fn request_sandbox_ids_dedupes_batch_ids_without_reordering() {
        assert_eq!(
            request_sandbox_ids(&[
                "sandbox-b".to_string(),
                " ".to_string(),
                "sandbox-a".to_string(),
                "sandbox-c".to_string(),
                "sandbox-a".to_string(),
            ]),
            vec![
                "sandbox-b".to_string(),
                "sandbox-a".to_string(),
                "sandbox-c".to_string(),
            ],
        );
    }

    #[test]
    fn node_ctl_can_observe_and_apply_reconciled_sandbox_policy_only() {
        let authorizer = sandbox_peer_authorizer();
        let node_ctl = PeerIdentity::new(
            users::SPACE_NODECTL.uid,
            users::SPACE_NODECTL.primary_group.gid,
            None,
        );

        authorizer
            .authorize_identity(node_ctl.clone(), "List", ALLOW_SANDBOX_OBSERVER_OR_TMPLMGR)
            .expect("node-ctl needs observer access to collect reconciliation scope");

        authorizer
            .authorize_identity(
                node_ctl.clone(),
                "SetNetworkPolicy",
                ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR,
            )
            .expect("node-ctl needs sandbox network policy update access");

        authorizer
            .authorize_identity(
                node_ctl.clone(),
                "SetAutoLifecyclePolicy",
                ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR,
            )
            .expect("node-ctl needs sandbox auto lifecycle policy update access");

        authorizer
            .authorize_identity(
                node_ctl.clone(),
                "EvictClusterDeletedSandbox",
                ALLOW_SANDBOX_CLUSTER_DELETED_EVICT,
            )
            .expect("node-ctl drives cluster-deleted evictions via the facet sync");

        authorizer
            .authorize_identity(
                node_ctl.clone(),
                "ListAutoLifecycleDeleteReceipts",
                ALLOW_AUTO_LIFECYCLE_DELETE_RECEIPTS,
            )
            .expect("node-ctl lists durable auto-delete completion evidence");
        authorizer
            .authorize_identity(
                node_ctl.clone(),
                "AcknowledgeAutoLifecycleDeleteReceipts",
                ALLOW_AUTO_LIFECYCLE_DELETE_RECEIPTS,
            )
            .expect("node-ctl prunes only cluster-acknowledged completion evidence");
        authorizer
            .authorize_identity(
                node_ctl.clone(),
                "ListPendingLocalBackups",
                ALLOW_LOCAL_BACKUP_INVENTORY,
            )
            .expect("node-ctl publishes the node's pending-backup set in the Full census");

        let error = authorizer
            .authorize_identity(node_ctl, "Create", ALLOW_SANDBOX_OPERATOR)
            .expect_err("node-ctl must not get broad sandbox operator access");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);

        let tmplmgr = PeerIdentity::new(
            users::SPACE_TMPLMGR.uid,
            users::SPACE_TMPLMGR.primary_group.gid,
            None,
        );
        let error = authorizer
            .authorize_identity(
                tmplmgr,
                "EvictClusterDeletedSandbox",
                ALLOW_SANDBOX_CLUSTER_DELETED_EVICT,
            )
            .expect_err(
                "only node-ctl, root, or a local operator may evict cluster-deleted sandboxes",
            );
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn builder_sandbox_owner_requires_privileged_local_creator() {
        let authorizer = sandbox_peer_authorizer();
        let root = authorizer
            .authorize_identity(
                PeerIdentity::new(0, 0, None),
                "Create",
                &[UdsPeerPrincipal::Root],
            )
            .unwrap();
        let sandbox_owner = SandboxManagerService::authorize_sandbox_owner_label(
            &root,
            BUILDER_SANDBOX_OWNER.as_str(),
        )
        .unwrap();
        assert_eq!(sandbox_owner.kind(), SandboxOwnerKind::Builder);

        let node_ctl = authorizer
            .authorize_identity(
                PeerIdentity::new(
                    users::SPACE_NODECTL.uid,
                    users::SPACE_NODECTL.primary_group.gid,
                    None,
                ),
                "Create",
                &[UdsPeerPrincipal::Service(user_names::SPACE_NODECTL)],
            )
            .unwrap();
        let error = SandboxManagerService::authorize_sandbox_owner_label(
            &node_ctl,
            BUILDER_SANDBOX_OWNER.as_str(),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn node_api_can_bridge_reserved_local_and_platform_users_but_not_builder() {
        let authorizer = sandbox_peer_authorizer();
        let node_api = authorizer
            .authorize_identity(
                PeerIdentity::new(
                    users::SPACE_NODEAPI.uid,
                    users::SPACE_NODEAPI.primary_group.gid,
                    None,
                ),
                "Create",
                ALLOW_SANDBOX_OPERATOR,
            )
            .unwrap();

        for user in [
            ROOT_SANDBOX_OWNER.as_str(),
            DEPLOY_SANDBOX_OWNER.as_str(),
            DEV_SANDBOX_OWNER.as_str(),
        ] {
            let sandbox_owner =
                SandboxManagerService::authorize_sandbox_owner_label(&node_api, user).unwrap();
            assert_eq!(sandbox_owner.as_str(), user);
        }
        let platform_owner = "user:00000000-0000-0000-0000-000000000020";
        let sandbox_owner =
            SandboxManagerService::authorize_sandbox_owner_label(&node_api, platform_owner)
                .unwrap();
        assert_eq!(sandbox_owner.as_str(), platform_owner);

        let error = SandboxManagerService::authorize_sandbox_owner_label(
            &node_api,
            BUILDER_SANDBOX_OWNER.as_str(),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn direct_local_create_rejects_arbitrary_local_owners_but_allows_platform_users() {
        let authorizer = sandbox_peer_authorizer();
        let root = authorizer
            .authorize_identity(
                PeerIdentity::new(0, 0, None),
                "Create",
                &[UdsPeerPrincipal::Root],
            )
            .unwrap();

        for owner in ["uid-1000@local", "james@local", "james@example.test"] {
            let error =
                SandboxManagerService::authorize_sandbox_owner_label(&root, owner).unwrap_err();
            assert_eq!(error.code(), tonic::Code::PermissionDenied);
        }

        let platform_owner = "user:00000000-0000-0000-0000-000000000020";
        let sandbox_owner =
            SandboxManagerService::authorize_sandbox_owner_label(&root, platform_owner).unwrap();
        assert_eq!(sandbox_owner.as_str(), platform_owner);
    }

    #[test]
    fn create_network_policy_generation_bumps_only_a_local_explicit_zero() {
        use crate::db::PolicyAuthority::{Cluster, Local};
        let policy = NetworkPolicyV1::deny_all();
        assert_eq!(create_network_policy_generation(Some(&policy), 0, Local), 1);
        assert_eq!(
            create_network_policy_generation(Some(&policy), 0, Cluster),
            0
        );
        assert_eq!(
            create_network_policy_generation(Some(&policy), 7, Cluster),
            7
        );
        assert_eq!(create_network_policy_generation(None, 0, Cluster), 0);
    }

    /// A cluster create may seed an inherited sandbox with its template's
    /// effective policy at generation 0. node-ctl then reconciles inheritance
    /// at the sandbox's own generation, which starts at 1. That reconcile must
    /// replace the seed, and a later user override must apply. A seed bumped to
    /// generation 1 made both conflict, and the policy state never converged.
    #[test]
    fn cluster_reconcile_replaces_a_cold_create_seed() {
        use crate::db::{NetworkPolicyOverrideUpdate, PolicyAuthority};

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let template_policy = NetworkPolicyV1::allow_all();
        let seed_generation =
            create_network_policy_generation(Some(&template_policy), 0, PolicyAuthority::Cluster);
        crate::db::set_network_policy_fields(
            &conn,
            &sandbox_id,
            Some(&template_policy),
            seed_generation,
            PolicyAuthority::Cluster,
        )
        .unwrap();
        let reconcile = |policy: Option<&NetworkPolicyV1>, generation: u64| {
            crate::db::update_network_policy_override(
                &conn,
                &sandbox_id,
                policy,
                generation,
                PolicyAuthority::Cluster,
                false,
                false,
            )
            .unwrap()
            .decision
        };

        assert_eq!(reconcile(None, 1), NetworkPolicyOverrideUpdate::Apply);
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.network_policy_override, None);
        assert_eq!(row.network_policy_generation, 1);

        let user_policy = NetworkPolicyV1::deny_all();
        assert_eq!(
            reconcile(Some(&user_policy), 2),
            NetworkPolicyOverrideUpdate::Apply
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.network_policy_override, Some(user_policy));
    }

    #[test]
    fn create_network_policy_authority_inherits_without_override() {
        let inherited_authority = crate::db::PolicyAuthority::Local;

        let authority = create_network_policy_authority(
            None,
            pplx_space_net_common::proto::PolicyAuthority::Unspecified as i32,
            inherited_authority,
        )
        .unwrap();

        assert_eq!(authority, inherited_authority);
    }

    #[test]
    fn create_network_policy_authority_requires_authority_with_override() {
        let policy = NetworkPolicyV1::deny_all();

        let error = create_network_policy_authority(
            Some(&policy),
            pplx_space_net_common::proto::PolicyAuthority::Unspecified as i32,
            crate::db::PolicyAuthority::Cluster,
        )
        .expect_err("network policy override must include authority");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("unspecified"));
    }

    #[test]
    fn set_network_policy_generation_bumps_corrupt_fallback_even_if_policy_matches() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Creating);
        conn.execute(
            "UPDATE sandboxes SET network_policy_override_json = ?1 WHERE id = ?2",
            crate::db::fixture::params![
                "{\"version\":\"v9\",\"egress\":{\"default_action\":\"deny\"}}",
                &sandbox_id
            ],
        )
        .unwrap();

        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert!(row.network_policy_override_is_corrupt_fallback);
        assert_eq!(
            set_network_policy_desired_generation(&row, &Some(NetworkPolicyV1::deny_all()), 0),
            1
        );
    }

    // The net-gateway begins pulling policy from host-control the moment its
    // process starts, and host-control authorizes it by matching the caller's
    // peer (pid, uid) against the persisted registration. Launch flows run the
    // gateway launch concurrently with the (much slower) jailer launch, so the
    // registration must be queryable before the jailer arm resolves — not
    // after the join. This drives the registration arm against a jailer arm
    // that performs the same lookup host-control auth uses while it is still
    // in flight.
    #[tokio::test]
    async fn net_gateway_pid_is_registered_before_jailer_launch_completes() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let db = crate::db::SharedConnection::new(conn);

        let sbx_uid = 10042;
        let netgtw_pid = 4242;

        let (gateway_arm_done_tx, gateway_arm_done_rx) = tokio::sync::oneshot::channel();
        let netgtw_arm = async {
            let result = register_net_gateway_launch(&db, &sandbox_id, sbx_uid, async {
                Ok(pplx_space_node_mgr_client::proto::LaunchResponse { pid: netgtw_pid })
            })
            .await;
            gateway_arm_done_tx
                .send(())
                .expect("jailer arm still pending");
            result
        };

        // Fake jailer launch: still in flight when the gateway arm completes.
        // It observes what host-control auth would see for a policy pull
        // issued by the just-launched gateway at this point.
        let jailer_db = db.clone();
        let jailer_arm = async move {
            gateway_arm_done_rx.await.expect("gateway arm dropped");
            let conn = jailer_db.lock().expect("mutex poisoned");
            crate::db::list_by_netgtw_pid_uid(&conn, netgtw_pid, sbx_uid).unwrap()
        };

        let (rows_visible_during_jailer_launch, netgtw_result) =
            tokio::join!(jailer_arm, netgtw_arm);
        netgtw_result.expect("net-gateway launch arm failed");
        assert_eq!(
            rows_visible_during_jailer_launch.len(),
            1,
            "net-gateway pid must be visible to host-control auth before the jailer launch \
             completes; a miss here is the transient PERMISSION_DENIED on startup policy pulls"
        );
        assert_eq!(rows_visible_during_jailer_launch[0].id, sandbox_id);
    }

    // The cold create/start path is fail-closed on the network policy: a guest
    // that booted but whose egress policy never installed must not be marked
    // Started, because the net-gateway denies egress until the policy lands and
    // the smoke probe would otherwise race the fire-and-forget push. The full
    // socket-level path is exercised by ECW smoke step22; these cover the gate
    // decision and its ordering.
    #[test]
    fn start_gate_marks_started_when_guest_ready_and_no_policy_expected() {
        let response = pplx_space_spaced_host_client::proto::WaitReadyResponse::default();
        assert!(matches!(
            evaluate_start_gate(Ok(response.into()), None),
            StartGate::Ready(_)
        ));
    }

    #[test]
    fn start_gate_marks_started_when_guest_ready_and_policy_installed() {
        let response = pplx_space_spaced_host_client::proto::WaitReadyResponse::default();
        assert!(matches!(
            evaluate_start_gate(Ok(response.into()), Some(Ok(()))),
            StartGate::Ready(_)
        ));
    }

    #[test]
    fn start_gate_fails_closed_when_policy_push_fails_even_if_guest_ready() {
        let response = pplx_space_spaced_host_client::proto::WaitReadyResponse::default();
        match evaluate_start_gate(
            Ok(response.into()),
            Some(Err(Status::internal("netgtw down"))),
        ) {
            StartGate::NetworkPolicyFailed(status) => {
                assert_eq!(status.code(), tonic::Code::Internal);
                assert!(status.message().contains("netgtw down"));
            }
            other => panic!("expected NetworkPolicyFailed, got {other:?}"),
        }
    }

    #[test]
    fn start_gate_surfaces_guest_failure_ahead_of_policy_failure() {
        // A guest-readiness failure is the more actionable signal, so it wins
        // even when the policy push also failed.
        assert!(matches!(
            evaluate_start_gate(
                Err(Status::deadline_exceeded("guest timeout")),
                Some(Err(Status::internal("netgtw down"))),
            ),
            StartGate::GuestNotReady(status) if status.code() == tonic::Code::DeadlineExceeded
        ));
    }

    #[tokio::test]
    async fn sync_set_network_policy_requires_live_gateway_confirmation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let policy = NetworkPolicyV1::deny_all();
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .set_network_policy(internal_request(SetNetworkPolicyRequest {
                sandbox_id: sandbox_id.clone(),
                policy: Some(pplx_space_net_common::proto::NetworkPolicyV1::from(&policy)),
                inherit_template: false,
                policy_generation: 1,
                authority: pplx_space_net_common::proto::PolicyAuthority::Cluster as i32,
                sync_to_gateway: true,
            }))
            .await
            .expect_err("sync network policy update must require gateway confirmation");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            error.message().contains("live net-gateway"),
            "unexpected sync failure message: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn sync_set_network_policy_rejected_for_pool_member() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(SandboxId::from_str(&sandbox_id).unwrap());
        let user = SandboxOwner::parse("user").unwrap();
        crate::db::insert_sandbox(
            &conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id: &sandbox_id,
                uuid8: &uuid8,
                state: SandboxState::Paused,
                template: "tmpl",
                template_hash: None,
                user: &user,
                runtime: RuntimeKind::Firecracker.as_str(),
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: Some("tmpl"),
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
        let policy = NetworkPolicyV1::deny_all();
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .set_network_policy(internal_request(SetNetworkPolicyRequest {
                sandbox_id: sandbox_id.clone(),
                policy: Some(pplx_space_net_common::proto::NetworkPolicyV1::from(&policy)),
                inherit_template: false,
                policy_generation: 1,
                authority: pplx_space_net_common::proto::PolicyAuthority::Cluster as i32,
                sync_to_gateway: true,
            }))
            .await
            .expect_err("sync network policy update must be rejected for a pool member");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("pool member"),
            "unexpected sync failure message: {}",
            error.message()
        );

        // The rejection must happen before any DB mutation: the pool member's
        // desired network-policy fields stay exactly as inserted.
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.network_policy_generation, 0);
        assert!(row.network_policy_override.is_none());
        assert_eq!(row.pool_template.as_deref(), Some("tmpl"));
    }

    #[tokio::test(start_paused = true)]
    async fn set_network_policy_serializes_behind_held_wake_lock() {
        // The pre-wake gate runs while the resume path holds the per-sandbox
        // wake lock across [confirm -> thaw]. SetNetworkPolicy must take that
        // same lock so a desired-policy update can't commit inside that window
        // and leave a live guest on a stale gateway policy. Here we hold the
        // wake lock (standing in for an in-flight resume) and prove the update
        // blocks -- and mutates nothing -- until the lock is released.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let service = SandboxManagerService::for_test(conn);

        let policy = NetworkPolicyV1::deny_all();
        let make_request = || {
            internal_request(SetNetworkPolicyRequest {
                sandbox_id: sandbox_id.clone(),
                policy: Some(pplx_space_net_common::proto::NetworkPolicyV1::from(&policy)),
                inherit_template: false,
                policy_generation: 1,
                authority: pplx_space_net_common::proto::PolicyAuthority::Cluster as i32,
                // No live net-gateway in this unit test, so keep the update on
                // the async path -- the serialization being exercised is the
                // wake-lock acquisition, not the gateway round-trip.
                sync_to_gateway: false,
            })
        };

        let wake_guard = service
            .auto_lifecycle
            .wake_lock(&sandbox_id)
            .lock_owned()
            .await;

        // While the wake lock is held the update can't proceed: it parks on the
        // lock, so the bounded wait elapses (paused-clock auto-advance) instead
        // of completing.
        let blocked = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            service.set_network_policy(make_request()),
        )
        .await;
        assert!(
            blocked.is_err(),
            "SetNetworkPolicy must block while the per-sandbox wake lock is held"
        );

        // The blocked-and-dropped attempt must not have mutated desired policy:
        // it never acquired the lock, so it never reached the DB commit.
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.network_policy_generation, 0);
        assert!(row.network_policy_override.is_none());

        // Once the resume releases the wake lock, the update proceeds and the
        // desired policy is committed.
        drop(wake_guard);
        service
            .set_network_policy(make_request())
            .await
            .expect("SetNetworkPolicy must succeed once the wake lock is free");

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert!(
            row.network_policy_override.is_some(),
            "desired policy override must be committed after the lock is released"
        );
        assert_ne!(
            row.network_policy_generation, 0,
            "desired policy generation must advance after the committed update"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pre_resume_network_policy_apply_fails_closed_without_gateway() {
        // The pre-resume gate must surface a failure — and leave the applied_*
        // markers untouched — when the net-gateway can't confirm the policy, so
        // resume never thaws the VM against an unconfirmed policy. With no
        // net-gateway control socket the blocking apply exhausts its bounded
        // retries and returns an error rather than recording the policy as applied.
        //
        // This exercises the blocking-apply helper directly; the resume wiring is
        // covered end-to-end by `resume_fails_closed_when_network_policy_cannot_confirm`.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(SandboxId::from_str(&sandbox_id).unwrap());
        let user = SandboxOwner::parse("user").unwrap();
        let policy = NetworkPolicyV1::deny_all();
        crate::db::insert_sandbox(
            &conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id: &sandbox_id,
                uuid8: &uuid8,
                state: SandboxState::Paused,
                template: "seed",
                template_hash: Some("hash"),
                user: &user,
                runtime: "fc",
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: None,
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: Some(&policy),
                network_policy_generation: 3,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .apply_network_policy_before_wake(&sandbox_id, "resume")
            .await
            .expect_err("pre-resume apply must fail when the net-gateway cannot confirm");
        assert!(
            matches!(
                error.code(),
                tonic::Code::Internal | tonic::Code::Unavailable
            ),
            "unexpected pre-wake apply failure code: {:?}",
            error.code()
        );

        // A confirmation that never happened must not be recorded as applied: the
        // applied generation stays at its inserted default.
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.applied_network_policy_generation, 0);
    }

    /// Only the terminal failure statuses warrant the host-driven
    /// close + open re-arm cycle: healthy statuses must pass through
    /// untouched, the transient `WaitingForListener` seed must be left to
    /// resolve on its own, and unknown wire values must not trigger guest
    /// RPC churn.
    #[test]
    fn loopback_bridge_rearm_classifies_only_terminal_failures() {
        use pplx_space_sbx_mgr_client::operations::LoopbackBridgeStatus as S;
        for status in [
            S::Unspecified,
            S::Direct,
            S::LoopbackForwarded,
            S::WaitingForListener,
        ] {
            assert!(
                !loopback_bridge_open_warrants_rearm(status as i32),
                "{status:?} must not trigger a re-arm"
            );
        }
        for status in [S::BindConflict, S::Failed] {
            assert!(
                loopback_bridge_open_warrants_rearm(status as i32),
                "{status:?} must trigger a re-arm"
            );
        }
        assert!(
            !loopback_bridge_open_warrants_rearm(42),
            "unknown wire values must not trigger a re-arm"
        );
    }

    #[tokio::test]
    async fn spaced_open_loopback_bridge_marks_non_reachable_states_terminal() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        // A non-bridge-reachable state that becomes reachable only via a
        // separate wake this ref does not drive is marked terminal, so node-ctl
        // completes the ref rather than retrying to the backoff cap: the
        // already-settled `Failed`, the settle-to-frozen `Suspending`→
        // `Suspended`, and `Checkpointing` (which resumes to its origin — a
        // `Paused` sandbox stays unreachable). A pre-run state (`Creating`)
        // progresses toward `Started` through its own create/start lifecycle,
        // so it stays retryable (unmarked) and the reconcile arms the bridge
        // within seconds of `Started` instead of at the periodic backstop.
        for (state, expect_terminal) in [
            (SandboxState::Failed, true),
            (SandboxState::Suspending, true),
            (SandboxState::Checkpointing, true),
            (SandboxState::Creating, false),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, state);
            let service = SandboxManagerService::for_test(conn);

            let error = service
                .spaced_open_loopback_bridge(node_ctl_request(SpacedOpenLoopbackBridgeRequest {
                    sandbox_id: sandbox_id.clone(),
                    operation: Some(
                        pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                            port: 9012,
                            wait_timeout: None,
                        },
                    ),
                }))
                .await
                .expect_err("an unreachable guest must be refused before the spaced-host dial");

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(
                error
                    .message()
                    .contains("not reachable for loopback bridge"),
                "unexpected message: {}",
                error.message()
            );
            assert_eq!(
                pplx_space_common_grpc::is_desired_state_apply_terminal(&error),
                expect_terminal,
                "state {state:?} terminal-marker mismatch"
            );
        }
    }

    #[tokio::test]
    async fn set_auto_lifecycle_policy_marks_stuck_state_terminal_but_not_transient() {
        // A stuck state (Failed) never settles into a policy-updatable state,
        // so the precondition error is marked terminal; a transient state
        // (Suspending) will settle, so it stays unmarked and the node-ctl ref
        // keeps retrying at the queue's fast cadence.
        for (state, expect_terminal) in [
            (SandboxState::Failed, true),
            (SandboxState::Suspending, false),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, state);
            let service = SandboxManagerService::for_test(conn);

            let error = service
                .set_auto_lifecycle_policy_exact(
                    &sandbox_id,
                    access_idle_policy(60),
                    false,
                    crate::db::PolicyAuthority::Cluster,
                )
                .await
                .expect_err("auto-lifecycle update must be refused in a non-stable state");

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(
                pplx_space_common_grpc::is_desired_state_apply_terminal(&error),
                expect_terminal,
                "state {state:?} terminal-marker mismatch"
            );
        }
    }

    #[tokio::test]
    async fn spaced_open_loopback_bridge_rejects_invalid_route_token() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = SandboxManagerService::for_test(conn);

        let metric_capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let error = service
            .spaced_open_loopback_bridge(internal_request_with_invalid_space_api_route_token(
                SpacedOpenLoopbackBridgeRequest {
                    sandbox_id: sandbox_id.clone(),
                    operation: Some(
                        pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                            port: 9012,
                            wait_timeout: None,
                        },
                    ),
                },
            ))
            .await
            .expect_err("invalid route token must be rejected before dialing spaced-host");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            error.message().contains("invalid SPACE API route token"),
            "unexpected error message: {}",
            error.message()
        );
        let counters = metric_capture.finish();
        assert!(counters.iter().any(|counter| {
            counter.name == "sandbox.worker.space_api_route_token.authorization"
                && counter.tags.iter().any(|tag| tag == "result:invalid_token")
                && counter
                    .tags
                    .iter()
                    .any(|tag| tag == "disposition:terminal_rejected")
        }));
    }

    #[tokio::test]
    async fn spaced_open_loopback_bridge_requires_route_token_for_non_node_ctl() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .spaced_open_loopback_bridge(internal_request(SpacedOpenLoopbackBridgeRequest {
                sandbox_id: sandbox_id.clone(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                        port: 9012,
                        wait_timeout: None,
                    },
                ),
            }))
            .await
            .expect_err("non-node-ctl loopback bridge callers must carry a route token");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            error.message().contains("requires a SPACE API route token"),
            "unexpected error message: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn spaced_close_loopback_bridge_rejects_invalid_route_token() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .spaced_close_loopback_bridge(internal_request_with_invalid_space_api_route_token(
                SpacedCloseLoopbackBridgeRequest {
                    sandbox_id: sandbox_id.clone(),
                    operation: Some(
                        pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeRequest {
                            port: 9012,
                        },
                    ),
                },
            ))
            .await
            .expect_err("invalid route token must be rejected before dialing spaced-host");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            error.message().contains("invalid SPACE API route token"),
            "unexpected error message: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn spaced_close_loopback_bridge_requires_route_token_for_non_node_ctl() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .spaced_close_loopback_bridge(internal_request(SpacedCloseLoopbackBridgeRequest {
                sandbox_id: sandbox_id.clone(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeRequest {
                        port: 9012,
                    },
                ),
            }))
            .await
            .expect_err("non-node-ctl loopback bridge callers must carry a route token");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            error.message().contains("requires a SPACE API route token"),
            "unexpected error message: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn spaced_wait_ready_allows_identity_free_pool_donor_to_reach_spaced_host() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox_with_runtime(&conn, &sandbox_id, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET pool_template = 'seed' WHERE id = ?1",
            [&sandbox_id],
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .spaced_wait_ready(node_api_trusted_lifecycle_request(SpacedWaitReadyRequest {
                sandbox_id,
                timeout: None,
            }))
            .await
            .expect_err("no spaced-host socket exists in unit tests");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            error.message().contains("spaced-host unavailable"),
            "the trusted readiness probe must reach the donor's live endpoint: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn spaced_open_loopback_bridge_still_rejects_suspended() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .spaced_open_loopback_bridge(node_ctl_request(SpacedOpenLoopbackBridgeRequest {
                sandbox_id: sandbox_id.clone(),
                operation: Some(
                    pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                        port: 9012,
                        wait_timeout: None,
                    },
                ),
            }))
            .await
            .expect_err("suspended sandbox must be rejected at the state gate");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    // The guest-live state gate admits wake-in-flight windows where the VM
    // is resident but FROZEN: `Resuming`/`Restoring` are committed before
    // node-mgr thaws the vCPUs. Against such a guest the spaced-host
    // connect succeeds (the host process is alive) and the bridge RPC then
    // parks in the host→guest vsock CONNECT handshake, which has no
    // timeout of its own. The sbx-mgr call site must bound the whole
    // connect+RPC pipeline and map elapse to retryable `Unavailable` —
    // otherwise one frozen guest pins a node-ctl exposure-reconcile slot
    // for the full client deadline on every tick. The hanging spaced-host
    // is simulated by routing a never-resolving future through the same
    // bounding helper both bridge handlers use; the real spaced-host
    // socket lives under the fixed `/space` run dir and cannot be faked in
    // unit tests.
    #[tokio::test(start_paused = true)]
    async fn loopback_bridge_op_times_out_as_unavailable_when_spaced_host_hangs() {
        // Test-side bound (virtual time) so the unbounded behavior —
        // awaiting the hung pipeline forever — fails crisply instead of
        // hanging the suite.
        let result = tokio::time::timeout(
            spaced_bridge::SPACED_LOOPBACK_BRIDGE_RPC_TIMEOUT * 4,
            spaced_bridge::bounded_loopback_bridge_op(
                "SpacedOpenLoopbackBridge",
                std::future::pending::<Result<(), Status>>(),
            ),
        )
        .await
        .expect("bridge op must hit its own bound instead of hanging on a frozen guest");

        let error = result.expect_err("a hung spaced-host pipeline cannot succeed");
        assert_eq!(
            error.code(),
            tonic::Code::Unavailable,
            "elapse must map to retryable Unavailable, got: {error:?}"
        );
        assert!(
            error.message().contains("SpacedOpenLoopbackBridge"),
            "timeout message must name the bridge RPC, got: {}",
            error.message()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resume_fails_closed_when_network_policy_cannot_confirm() {
        // Resume must run the same blocking pre-wake gate as start/restore: a
        // policy update committed while paused could otherwise reopen egress under
        // the stale gateway policy when the VM thaws. With no net-gateway socket
        // the gate fails closed, so resume_after_wake_lock returns an error before
        // it ever transitions out of Paused — the resident VM is left untouched
        // rather than torn down.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(SandboxId::from_str(&sandbox_id).unwrap());
        let user = SandboxOwner::parse("user").unwrap();
        let policy = NetworkPolicyV1::deny_all();
        crate::db::insert_sandbox(
            &conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id: &sandbox_id,
                uuid8: &uuid8,
                state: SandboxState::Paused,
                template: "seed",
                template_hash: Some("hash"),
                user: &user,
                runtime: "fc",
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: None,
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: Some(&policy),
                network_policy_generation: 2,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        for policy in [None, Some(AutoLifecyclePolicy::disabled())] {
            let error = service
                .resume(internal_request(ResumeRequest {
                    sandbox_id: sandbox_id.clone(),
                    activation_auto_lifecycle: policy.clone().map(Into::into),
                    ..Default::default()
                }))
                .await
                .expect_err("resume must fail closed when the gateway cannot confirm policy");
            assert!(
                matches!(
                    error.code(),
                    tonic::Code::Internal | tonic::Code::Unavailable
                ),
                "{error}"
            );
            let error = service
                .wake_with_start_config(
                    sandbox_id.clone(),
                    Some(std::time::Duration::from_secs(30)),
                    WakeStartConfig {
                        activation_auto_lifecycle: policy,
                        ..Default::default()
                    },
                    true,
                    None,
                )
                .await
                .expect_err("wake must reach the same network gate without a generation");
            assert!(
                matches!(
                    error.code(),
                    tonic::Code::Internal | tonic::Code::Unavailable
                ),
                "{error}"
            );
            // Admission failure leaves the resident and its policy untouched.
            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            assert_eq!(row.state, SandboxState::Paused);
        }
    }

    #[tokio::test]
    async fn wake_rejects_a_generation_fence_without_policy() {
        let service = SandboxManagerService::for_test(crate::db::test_conn());
        let error = service
            .wake(internal_request(WakeRequest {
                sandbox_id: SandboxId::new().to_string(),
                start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                    expected_execution_generation: Some(7),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .expect_err("a fence requires an explicit policy");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "wake execution generation requires a policy"
        );
    }

    #[test]
    fn resident_resume_setup_failure_restores_pause_and_preserves_compatibility_rejection() {
        for (failure, expected) in [
            (tonic::Code::Internal, tonic::Code::Unavailable),
            (
                tonic::Code::FailedPrecondition,
                tonic::Code::FailedPrecondition,
            ),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Resuming);
            let db = crate::db::SharedConnection::new(conn);

            let error = super::super::auth_lookup::finish_resident_resume_after_guest_setup(
                &db,
                &sandbox_id,
                Err(Status::new(failure, "guest setup failed")),
                Some(VmState::Paused),
            )
            .expect_err("failed guest setup must reach the caller");

            assert_eq!(error.code(), expected);
            let row = crate::db::get_sandbox(&db.lock().unwrap(), &sandbox_id)
                .unwrap()
                .unwrap();
            assert_eq!(row.state, SandboxState::Paused);
            assert!(row.failure_reason.is_none());
        }
    }

    #[test]
    fn resident_resume_env_failure_quarantines_without_confirmed_pause_rollback() {
        for rollback_vm_state in [Some(VmState::Running), None] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Resuming);
            let db = crate::db::SharedConnection::new(conn);

            let error = super::super::auth_lookup::finish_resident_resume_after_guest_setup(
                &db,
                &sandbox_id,
                Err(Status::internal("spaced-host unavailable")),
                rollback_vm_state,
            )
            .expect_err("unconfirmed environment rollback must reach the caller");

            assert_eq!(error.code(), tonic::Code::Unavailable);
            let db = db.lock().unwrap();
            let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
            assert_eq!(row.state, SandboxState::Quarantined);
            assert_eq!(
                crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
                    .unwrap()
                    .as_deref(),
                Some(SandboxQuarantineReasonCode::ResidentResumeFailed.as_str())
            );
        }
    }

    #[test]
    fn resident_resume_rpc_failure_uses_observed_firecracker_state() {
        for (vm_state, expected_state, should_continue) in [
            (Some(VmState::Running), SandboxState::Resuming, true),
            (Some(VmState::Paused), SandboxState::Paused, false),
            (None, SandboxState::Quarantined, false),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Resuming);
            let db = crate::db::SharedConnection::new(conn);

            let result = super::super::auth_lookup::finish_resident_resume_after_rpc_error(
                &db,
                &sandbox_id,
                Status::unavailable("node-manager response lost"),
                vm_state,
            );

            assert_eq!(result.is_ok(), should_continue, "vm_state={vm_state:?}");
            assert_eq!(
                crate::db::get_sandbox(&db.lock().unwrap(), &sandbox_id)
                    .unwrap()
                    .unwrap()
                    .state,
                expected_state,
                "vm_state={vm_state:?}"
            );
        }
    }

    #[tokio::test]
    async fn orphaned_sandbox_network_policy_does_not_fetch_template() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_orphaned_test_sandbox(&conn, &sandbox_id_string, SandboxState::Failed);
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        let service = SandboxManagerService::for_test(conn);

        let policy = service.effective_network_policy(&row).await.unwrap();

        assert_eq!(policy.policy, NetworkPolicyV1::deny_all());
        assert_eq!(policy.authority, crate::db::PolicyAuthority::Local);
        assert!(!policy.inherited);
        assert_eq!(policy.generation, 0);
    }

    #[tokio::test]
    async fn orphaned_sandbox_without_rootfs_start_prepare_does_not_fetch_template() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_orphaned_test_sandbox(&conn, &sandbox_id, SandboxState::Failed);
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        let templates = crate::template_resolver::fake::FakeTemplateResolver::refusing(
            Status::not_found("orphaned sandboxes must not look up templates"),
        );

        let error = template_source_for_sandbox_prepare(
            &crate::node_manager_transport::fake::unbound(
                tempfile::tempdir()
                    .unwrap()
                    .path()
                    .join("unused.sock")
                    .as_path(),
            ),
            &templates,
            &row,
        )
        .await
        .expect_err("orphaned sandbox without rootfs is not startable");
        assert!(templates.requests().is_empty());

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("cannot start orphaned sandbox"));
        assert!(error.message().contains(&sandbox_id));
    }

    #[test]
    fn template_execution_metadata_reads_versioned_identity() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("metadata.json"),
            r#"{
                "schema_version": 2,
                "execution": {
                    "runtime": { "kind": "cloud-hypervisor", "version": "42.0.0" },
                    "guest": { "os": "windows", "architecture": "x86_64" },
                    "components": []
                }
            }"#,
        )
        .unwrap();

        let execution =
            template_execution_metadata(directory.path().to_str().unwrap(), "aarch64").unwrap();

        assert_eq!(
            execution.runtime().kind,
            pplx_space_common_core::types::RuntimeKind::CloudHypervisor
        );
        assert_eq!(
            execution.guest().os,
            pplx_space_common_core::types::GuestOperatingSystem::Windows
        );
        assert_eq!(
            execution.guest().architecture,
            pplx_space_common_core::types::Arch::X86_64
        );
    }

    #[test]
    fn template_execution_metadata_backfills_legacy_firecracker_identity() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("metadata.json"),
            r#"{"arch":"aarch64"}"#,
        )
        .unwrap();

        let execution =
            template_execution_metadata(directory.path().to_str().unwrap(), "x86_64").unwrap();

        assert_eq!(
            execution.runtime().kind,
            pplx_space_common_core::types::RuntimeKind::Firecracker
        );
        assert_eq!(execution.runtime().version.as_str(), "1.15.1");
        assert_eq!(execution.state_format().unwrap().version.as_str(), "9.0.0");
        assert_eq!(
            execution.guest().architecture,
            pplx_space_common_core::types::Arch::Aarch64
        );
    }

    #[test]
    fn template_spaced_compatibility_uses_supported_guest_versions() {
        // A release bump must preserve legacy templates without accepting a
        // future breaking guest or trusting inconsistent provenance.
        // Detailed range and provenance cases are owned by spaced-common.
        for (version, family, supported) in [
            (Some("0.1.0"), Some("0.1"), true),
            (Some("0.8.0"), Some("0.8"), true),
            (Some("0.9.0"), Some("0.9"), false),
            (None, Some("0.1"), false),
        ] {
            let result = reject_incompatible_template_spaced_compatibility(
                "seed",
                "sha256:test",
                version,
                family,
            );
            assert_eq!(
                result.is_ok(),
                supported,
                "{version:?} / {family:?}: {result:?}"
            );
            if let Err(error) = result {
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            }
        }
    }

    #[test]
    fn grpc_timeout_budget_reads_request_metadata() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert(GRPC_TIMEOUT_METADATA_KEY, "1500m".parse().unwrap());

        assert_eq!(
            grpc_timeout_budget(&request),
            Some(Duration::from_millis(1500))
        );
    }

    #[test]
    fn wake_ready_timeout_secs_uses_remaining_budget() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);

        assert_eq!(
            wake_ready_timeout_secs(
                Some(deadline),
                DEFAULT_BLOCKING_READY_TIMEOUT_SECS,
                SandboxState::Starting,
            )
            .unwrap(),
            2
        );
    }

    #[test]
    fn wake_ready_timeout_secs_falls_back_without_request_deadline() {
        assert_eq!(
            wake_ready_timeout_secs(
                None,
                DEFAULT_RESUME_READY_TIMEOUT_SECS,
                SandboxState::Paused
            )
            .unwrap(),
            DEFAULT_RESUME_READY_TIMEOUT_SECS
        );
    }

    #[test]
    fn restore_clock_sync_timeouts_are_short_and_not_readiness_bound() {
        assert!(
            RESUME_CLOCK_SYNC_TIMEOUT
                < Duration::from_secs(DEFAULT_BLOCKING_READY_TIMEOUT_SECS as u64)
        );
    }

    #[tokio::test]
    async fn start_readiness_failure_does_not_overwrite_concurrent_delete() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Starting);
        crate::db::test_fixtures::state_write_fixtures::apply_edge(
            &conn,
            &sandbox_id,
            &crate::db::edges::DELETE_ADMISSION,
        );
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            false,
            crate::db::CleanupCompletion::DeleteRow,
        )
        .unwrap();
        let db = crate::db::SharedConnection::new(conn);

        let result = wait_ready_and_mark_started(
            db.clone(),
            &sandbox_id,
            &BTreeMap::new(),
            1,
            "start",
            false,
            None,
        )
        .await;

        assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
        let conn = db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Deleting);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::DeleteRow)
        );
    }

    #[test]
    fn requested_create_reuses_only_stable_sandbox_states() {
        assert!(requested_create_reuse_state(SandboxState::Created));
        assert!(requested_create_reuse_state(SandboxState::Started));
        assert!(requested_create_reuse_state(SandboxState::Paused));
        assert!(requested_create_reuse_state(SandboxState::Suspended));
        assert!(requested_create_reuse_state(SandboxState::Stopped));

        assert!(!requested_create_reuse_state(SandboxState::Creating));
        assert!(!requested_create_reuse_state(SandboxState::Starting));
        assert!(!requested_create_reuse_state(SandboxState::Pausing));
        assert!(!requested_create_reuse_state(SandboxState::Resuming));
        assert!(!requested_create_reuse_state(SandboxState::Checkpointing));
        assert!(!requested_create_reuse_state(SandboxState::Suspending));
        assert!(!requested_create_reuse_state(SandboxState::Restoring));
        assert!(!requested_create_reuse_state(SandboxState::Stopping));
        assert!(!requested_create_reuse_state(SandboxState::Deleting));
        assert!(!requested_create_reuse_state(SandboxState::CleanupRequired));
        assert!(!requested_create_reuse_state(SandboxState::CleanupFailed));
        assert!(!requested_create_reuse_state(SandboxState::Failed));
    }

    #[test]
    fn list_sandbox_dirs_uses_cache_root_not_rootfs() {
        let cache_dir = std::env::temp_dir().join(format!(
            "space-sbxmgr-list-test-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        ));
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        std::fs::create_dir_all(cache_dir.join(&sandbox_id_string)).unwrap();
        std::fs::create_dir_all(cache_dir.join("not-a-sandbox-id")).unwrap();
        std::fs::write(cache_dir.join("not-a-dir"), b"ignored").unwrap();

        assert_eq!(list_sandbox_dirs_in(&cache_dir), vec![sandbox_id]);

        let _ = std::fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn reconcile_inventory_quarantines_cache_only_sandbox() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let rootfs_dir = cache_dir.path().join(&sandbox_id_string).join("instance");
        std::fs::create_dir_all(&rootfs_dir).unwrap();
        std::fs::write(rootfs_dir.join("rootfs.ext4"), b"rootfs").unwrap();

        let report = reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
        assert_eq!(row.template, ORPHANED_SANDBOX_TEMPLATE);
        assert_eq!(row.user, ORPHANED_SANDBOX_OWNER);
        assert_eq!(row.rootfs_size_mb, Some(1));
        assert!(
            row.failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("without an sbx-manager metadata row")
        );
        assert_eq!(
            crate::db::current_quarantine_reason_wire_value(&conn, &sandbox_id_string).unwrap(),
            Some("orphaned_sandbox_row_missing".to_string())
        );
        let episode: (String, String, String) = conn
            .query_row(
                "SELECT current.next_action, history.action, history.reason_code
                 FROM sandbox_quarantine_state AS current
                 JOIN sandbox_quarantine_history AS history
                   ON history.quarantine_id = current.quarantine_id
                 WHERE current.sandbox_id = ?1",
                crate::db::fixture::params![sandbox_id_string],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            episode.0,
            SandboxQuarantineNextAction::RequiresOperatorAttention.as_str()
        );
        assert_eq!(
            episode.1,
            SandboxQuarantineHistoryAction::EnteredQuarantine.as_str()
        );
        assert_eq!(
            episode.2,
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str()
        );
    }

    #[test]
    fn reconcile_inventory_defers_an_unverified_deferred_orphan_and_recovers_a_verified_one() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let local_artifact_id = LocalArtifactId::new();
        let source_id = uuid::Uuid::now_v7();
        let snapshot_dir =
            canonical_suspend_snapshot_dir(&cache_dir.path().join(&sandbox_id_string));
        let _bundle = write_deferred_suspend_artifact(
            &snapshot_dir,
            &sandbox_id_string,
            local_artifact_id,
            source_id,
            64,
        );
        let reconcile = |verdicts: &crate::retained_source::RetainedSourceVerdicts| {
            reconcile_sandbox_inventory_ids(
                &conn,
                SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
                vec![sandbox_id],
                Vec::new(),
                Vec::new(),
                ReconcileSource::Test,
                InventoryOwnershipGuards::new(
                    inventory_cleanup_guard_for_test,
                    |_: SandboxId| Some(()),
                    &inventory_execution_for_test,
                )
                .with_retained_sources(verdicts),
            )
            .unwrap()
        };

        // Startup and cleanup reconciles carry no verdicts: a wait, not a
        // failed inspection.
        let metric_capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let report = reconcile(&Default::default());
        let counters = metric_capture.finish();
        assert!(report.changed_ids().is_empty());
        assert!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .is_none()
        );
        let reasons = counters
            .iter()
            .filter(|counter| counter.name == "sandbox.worker.inventory_reconcile")
            .flat_map(|counter| counter.tags.iter())
            .filter(|tag| tag.starts_with("reason:"))
            .collect::<Vec<_>>();
        assert!(
            reasons
                .iter()
                .any(|tag| *tag == "reason:retained_source_pending")
        );
        assert!(
            !reasons
                .iter()
                .any(|tag| *tag == "reason:filesystem_inspection_failed")
        );

        let mut verdicts = crate::retained_source::RetainedSourceVerdicts::default();
        verdicts.record(
            crate::retained_source::RetainedSource {
                sandbox_id: sandbox_id_string.clone(),
                artifact_dir: snapshot_dir.clone(),
                source_id,
            },
            crate::retained_source::RetainedSourceCheck::Present { rootfs_bytes: 64 },
        );
        assert_eq!(reconcile(&verdicts).changed_ids(), vec![sandbox_id]);
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    }

    #[test]
    fn reconcile_inventory_skips_orphan_removed_after_inventory_snapshot() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();

        let report = reconcile_sandbox_inventory_ids(
            &conn,
            SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
            vec![sandbox_id],
            Vec::new(),
            Vec::new(),
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                inventory_cleanup_guard_for_test,
                |_: SandboxId| Some(()),
                &inventory_execution_for_test,
            ),
        )
        .unwrap();

        assert!(report.changed_ids().is_empty());
        assert!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reconcile_inventory_recovers_complete_canonical_suspend_artifact() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let local_artifact_id = LocalArtifactId::new();
        let snapshot_dir =
            canonical_suspend_snapshot_dir(&cache_dir.path().join(&sandbox_id_string));
        let expected_execution = write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id_string,
            local_artifact_id,
            b"complete-suspend",
        );

        let report = reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        assert_eq!(row.template, "seed");
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
        assert_eq!(row.execution_metadata, Some(expected_execution));
        assert_eq!(
            crate::db::get_local_artifact(&conn, local_artifact_id).unwrap(),
            Some(crate::db::LocalArtifactRow {
                local_artifact_id,
                sandbox_id: sandbox_id_string,
                artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                snapshot_name: None,
                local_path: snapshot_dir.to_string_lossy().into_owned(),
            })
        );
    }

    #[test]
    fn reconcile_inventory_quarantines_unsafe_canonical_suspend_artifacts() {
        #[derive(Clone, Copy, Debug)]
        enum Case {
            MalformedMetadata,
            InvalidContentHash,
            MissingArtifact,
            SizeMismatch,
        }

        for (case, expected_detail) in [
            (Case::MalformedMetadata, "parse snapshot metadata"),
            (Case::InvalidContentHash, "invalid template content hash"),
            (
                Case::MissingArtifact,
                "no verified remote restore provenance",
            ),
            (Case::SizeMismatch, "declares"),
        ] {
            let conn = crate::db::test_conn();
            let cache_dir = tempfile::tempdir().unwrap();
            let run_dir = tempfile::tempdir().unwrap();
            let sandbox_id = SandboxId::new();
            let sandbox_id_string = sandbox_id.to_string();
            let snapshot_dir =
                canonical_suspend_snapshot_dir(&cache_dir.path().join(&sandbox_id_string));
            match case {
                Case::MalformedMetadata => {
                    std::fs::create_dir_all(&snapshot_dir).unwrap();
                    std::fs::write(
                        snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
                        b"not-json",
                    )
                    .unwrap();
                }
                Case::InvalidContentHash => {
                    write_full_suspend_artifact(
                        &snapshot_dir,
                        &sandbox_id_string,
                        LocalArtifactId::new(),
                        b"payload",
                    );
                    let metadata_path =
                        snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
                    let mut metadata: crate::snapshot_upload::SnapshotMeta =
                        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
                    metadata.source.content_hash = Some("not-a-content-hash".to_string());
                    std::fs::write(metadata_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
                }
                Case::MissingArtifact => {
                    write_full_suspend_artifact(
                        &snapshot_dir,
                        &sandbox_id_string,
                        LocalArtifactId::new(),
                        b"payload",
                    );
                    std::fs::remove_file(snapshot_dir.join("memory")).unwrap();
                }
                Case::SizeMismatch => {
                    write_full_suspend_artifact(
                        &snapshot_dir,
                        &sandbox_id_string,
                        LocalArtifactId::new(),
                        b"payload",
                    );
                    std::fs::write(snapshot_dir.join("vmstate"), b"larger-than-payload").unwrap();
                }
            }

            reconcile_sandbox_inventory_in(
                &conn,
                cache_dir.path(),
                run_dir.path(),
                ReconcileSource::Test,
            )
            .unwrap();

            let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap();
            assert_eq!(row.state, SandboxState::Quarantined, "case={case:?}");
            assert!(
                row.failure_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains(expected_detail)),
                "case={case:?}, reason={:?}",
                row.failure_reason
            );
        }
    }

    #[test]
    fn reconcile_inventory_recovers_incomplete_restore_as_retryable_restoring() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let snapshot_dir =
            canonical_suspend_snapshot_dir(&cache_dir.path().join(&sandbox_id_string));
        let local_artifact_id = LocalArtifactId::new();
        write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id_string,
            local_artifact_id,
            b"payload",
        );
        std::fs::remove_file(snapshot_dir.join("memory")).unwrap();
        let provenance_path = pplx_space_volmgr_client::verified_replica_provenance_path(
            snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        );
        let credential_marker = "inventory-restore-secret";
        std::fs::write(
            provenance_path,
            serde_json::to_vec(&pplx_space_volmgr_client::VerifiedReplicaProvenance {
                version: pplx_space_volmgr_client::VERIFIED_REPLICA_PROVENANCE_VERSION,
                subject_id: format!("sandbox:{sandbox_id}"),
                s3_uri: format!(
                    "s3://snapshots/suspend/{sandbox_id}/metadata.json?region=us-east-1&endpoint=https://user:{credential_marker}@example.invalid&path_style=true"
                ),
                raw: false,
                file_mode: None,
                snapshot_artifact: None,
                object_size: 1,
                object_etag: None,
                local_size: 1,
            })
            .unwrap(),
        )
        .unwrap();
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        let report = reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();
        drop(guard);

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .expect("incomplete durable-backed restore should remain retryable");
        assert_eq!(row.state, SandboxState::Restoring);
        assert_eq!(row.current_local_artifact_id, None);
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
        let operation = row
            .restore_operation
            .as_ref()
            .expect("interrupted restore ownership");
        assert_eq!(
            operation.origin(),
            crate::restore_operation::RestoreOrigin::Remote
        );
        assert_eq!(
            operation.phase(),
            crate::restore_operation::RestorePhase::ArtifactStaging
        );
        assert_eq!(
            operation
                .remote_source()
                .expect("interrupted restore source")
                .as_wire(),
            format!(
                "s3://snapshots/suspend/{sandbox_id}?region=us-east-1&endpoint=https://user:{credential_marker}@example.invalid&path_style=true"
            )
        );
        assert!(
            crate::db::current_quarantine_reason_wire_value(&conn, &sandbox_id_string)
                .unwrap()
                .is_none()
        );
        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "snapshot_restore_started")
            })
            .expect("interrupted restore recovery must emit a timeline event");
        assert_eq!(
            event.get("restore_storage_uri_present").map(String::as_str),
            Some("true")
        );
        let local_artifact_id = local_artifact_id.to_string();
        assert_eq!(
            event
                .get("restored_snapshot_local_artifact_id")
                .map(String::as_str),
            Some(local_artifact_id.as_str())
        );
        assert!(
            event
                .values()
                .all(|value| !value.contains(credential_marker)),
            "sandbox events must not expose exact restore URI credentials"
        );
    }

    #[test]
    fn runtime_activated_event_redacts_restore_storage_uri() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        let sandbox_id = SandboxId::new().to_string();
        let local_artifact_id = LocalArtifactId::new().to_string();
        let credential_marker = "runtime-restore-secret";
        let execution = pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
            pplx_space_common_core::types::Arch::Aarch64,
            vec![],
        );

        emit_runtime_activated_event(RuntimeActivatedEvent {
            sandbox_id: &sandbox_id,
            boot_type: "snapshot",
            pid: 1,
            spaced_host_pid: Some(2),
            net_gateway_pid: 3,
            sandbox_uid: 10_000,
            network_slot: 1,
            guest_cid: 4,
            guest_ip: "192.0.2.2",
            guest_gateway: "192.0.2.1",
            namespace: "test-netns",
            vcpu: Some(1),
            memory_mib: 512,
            has_netns_path: true,
            execution: &execution,
            execution_generation: Some(1),
            restored_snapshot_storage_uri: Some(format!(
                "s3://snapshots/suspend/{sandbox_id}/metadata.json?endpoint=https://user:{credential_marker}@example.invalid"
            )),
            restored_snapshot_local_artifact_id: Some(local_artifact_id.clone()),
            local_artifact_id: Some(local_artifact_id.clone()),
            volume_encryption: true,
        });
        drop(guard);

        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "runtime_activated")
            })
            .expect("runtime activation must emit a timeline event");
        assert_eq!(
            event.get("restore_storage_uri_present").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            event.get("volume_encryption").map(String::as_str),
            Some("true"),
            "runtime activation must say whether it booted an encrypted rootfs"
        );
        assert_eq!(
            event
                .get("restored_snapshot_local_artifact_id")
                .map(String::as_str),
            Some(local_artifact_id.as_str())
        );
        assert!(
            event
                .values()
                .all(|value| !value.contains(credential_marker)),
            "sandbox events must not expose exact restore URI credentials"
        );
    }

    #[test]
    fn reconcile_inventory_quarantines_without_touching_interrupted_staging() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let sandbox_dir = cache_dir.path().join(&sandbox_id_string);
        let canonical_id = LocalArtifactId::new();
        let canonical_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        write_full_suspend_artifact(
            &canonical_dir,
            &sandbox_id_string,
            canonical_id,
            b"canonical",
        );
        let staging_id = LocalArtifactId::new();
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, staging_id);
        write_full_suspend_artifact(&staging_dir, &sandbox_id_string, staging_id, b"staged");

        reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();

        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
        assert_eq!(row.current_local_artifact_id, None);
        assert_eq!(
            std::fs::read(canonical_dir.join("vmstate")).unwrap(),
            b"canonical"
        );
        assert_eq!(
            std::fs::read(staging_dir.join("vmstate")).unwrap(),
            b"staged"
        );
    }

    #[test]
    fn reconcile_inventory_removes_stale_db_row_without_cache_or_runtime() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Stopped);

        let report = reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        assert!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reconcile_inventory_does_not_delete_row_changed_after_inventory_snapshot() {
        struct MutatingInventoryDb {
            conn: std::cell::RefCell<crate::db::Connection>,
            calls: std::cell::Cell<usize>,
            sandbox_id: String,
        }

        impl InventoryDbAccess for MutatingInventoryDb {
            fn with_connection<T>(
                &self,
                operation: impl FnOnce(&crate::db::Connection) -> T,
            ) -> Result<T, super::super::cache_cleanup::InventoryError> {
                let call = self.calls.get();
                self.calls.set(call + 1);
                let conn = self.conn.borrow_mut();
                if call == 1 {
                    conn.execute(
                        "UPDATE sandboxes
                         SET state = 'started', uid = 10000, pid = 1234, updated_at = 'new'
                         WHERE id = ?1",
                        crate::db::fixture::params![&self.sandbox_id],
                    )?;
                }
                Ok(operation(&conn))
            }
        }

        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Stopped);
        let db = MutatingInventoryDb {
            conn: std::cell::RefCell::new(conn),
            calls: std::cell::Cell::new(0),
            sandbox_id: sandbox_id_string.clone(),
        };

        let report = reconcile_sandbox_inventory_ids(
            &db,
            SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                inventory_cleanup_guard_for_test,
                |_: SandboxId| Some(()),
                &inventory_execution_for_test,
            ),
        )
        .unwrap();

        assert!(report.changed_ids().is_empty());
        let row = crate::db::get_sandbox(&db.conn.borrow(), &sandbox_id_string)
            .unwrap()
            .expect("concurrent lifecycle row must survive");
        assert_eq!(row.state, SandboxState::Started);
        assert_eq!(row.uid, Some(10000));
    }

    #[test]
    fn reconcile_inventory_keeps_pre_rootfs_db_row_without_cache_or_runtime() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Created);

        let report = reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();

        assert!(report.changed_ids().is_empty());
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Created);
    }

    #[test]
    fn reconcile_inventory_skips_import_staging_markers() {
        for marker in ["instance.importing", "snapshot.importing", "instance.new"] {
            let conn = crate::db::test_conn();
            let cache_dir = tempfile::tempdir().unwrap();
            let run_dir = tempfile::tempdir().unwrap();
            let sandbox_id = SandboxId::new();
            let sandbox_id_string = sandbox_id.to_string();
            std::fs::create_dir_all(cache_dir.path().join(&sandbox_id_string).join(marker))
                .unwrap();

            let report = reconcile_sandbox_inventory_in(
                &conn,
                cache_dir.path(),
                run_dir.path(),
                ReconcileSource::Test,
            )
            .unwrap();

            assert!(
                report.changed_ids().is_empty(),
                "{marker} must not be materialized"
            );
            assert!(
                crate::db::get_sandbox(&conn, &sandbox_id_string)
                    .unwrap()
                    .is_none(),
                "{marker} must not create a DB row"
            );
        }
    }

    #[tokio::test]
    async fn reconcile_inventory_staging_lock_protects_active_and_reclaims_abandoned_restore() {
        let conn = crate::db::test_conn();
        let db = crate::db::SharedConnection::new(conn);
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        std::fs::create_dir_all(cache_dir.path().join(&sandbox_id_string).join("snapshot"))
            .unwrap();
        let metadata_staging = cache_dir
            .path()
            .join(format!(".{sandbox_id_string}.restore-metadata"));
        std::fs::create_dir_all(metadata_staging.join("snapshot")).unwrap();

        let auto_lifecycle = AutoLifecycleCoordinator::default();
        let staging_lock = auto_lifecycle.staging_lock(&sandbox_id_string);
        let guard = staging_lock.lock().await;

        reclaim_abandoned_restore_metadata_staging(
            cache_dir.path(),
            ReconcileSource::Test,
            &|id| auto_lifecycle.try_staging_guard(&id.to_string()),
        );
        assert!(metadata_staging.exists());

        let report = reconcile_sandbox_inventory_ids(
            &db,
            SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
            list_sandbox_dirs_in(cache_dir.path()),
            Vec::new(),
            Vec::new(),
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                inventory_cleanup_guard_for_test,
                |id: SandboxId| {
                    assert!(
                        db.try_lock().is_ok(),
                        "inventory inspection must not hold the service-wide SQLite mutex"
                    );
                    auto_lifecycle.try_staging_guard(&id.to_string())
                },
                &inventory_execution_for_test,
            ),
        )
        .unwrap();

        assert!(report.changed_ids().is_empty());
        assert!(
            crate::db::get_sandbox(&db.lock().unwrap(), &sandbox_id_string)
                .unwrap()
                .is_none(),
            "in-flight restore must not be materialized while staging is locked"
        );

        drop(guard);
        reclaim_abandoned_restore_metadata_staging(
            cache_dir.path(),
            ReconcileSource::Test,
            &|id| auto_lifecycle.try_staging_guard(&id.to_string()),
        );
        assert!(!metadata_staging.exists());
        let report = reconcile_sandbox_inventory_ids(
            &db,
            SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
            list_sandbox_dirs_in(cache_dir.path()),
            Vec::new(),
            Vec::new(),
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                inventory_cleanup_guard_for_test,
                |id: SandboxId| {
                    assert!(
                        db.try_lock().is_ok(),
                        "inventory inspection must not hold the service-wide SQLite mutex"
                    );
                    auto_lifecycle.try_staging_guard(&id.to_string())
                },
                &inventory_execution_for_test,
            ),
        )
        .unwrap();

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        let row = crate::db::get_sandbox(&db.lock().unwrap(), &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
    }

    #[test]
    fn reconcile_inventory_marks_live_orphan_for_cleanup() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let sandbox_run_dir = run_dir.path().join(&sandbox_id_string);
        let live_target = run_dir.path().join("live-fc");
        std::fs::create_dir_all(&sandbox_run_dir).unwrap();
        std::fs::create_dir_all(&live_target).unwrap();
        std::os::unix::fs::symlink(&live_target, sandbox_run_dir.join("fc.pid")).unwrap();
        let receiver = pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            Some(pplx_space_common_core::types::SandboxActivationAttemptId::new()),
        );

        let report = reconcile_sandbox_inventory_ids(
            &conn,
            SandboxInventoryPaths::new(cache_dir.path(), run_dir.path()),
            Vec::new(),
            vec![sandbox_id],
            vec![sandbox_id],
            ReconcileSource::Test,
            InventoryOwnershipGuards::new(
                inventory_cleanup_guard_for_test,
                |_: SandboxId| Some(()),
                &|_| Ok(Some(receiver)),
            ),
        )
        .unwrap();

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        assert_eq!(
            crate::db::node_execution_fence(&conn, &sandbox_id_string).unwrap(),
            Some(receiver)
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::Quarantine)
        );
        assert_eq!(row.cleanup_keep_filesystem, Some(true));
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id_string,
            CleanupCompletion::Quarantine,
            None,
        )
        .unwrap();
        let held = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(held.state, SandboxState::Quarantined);
        assert!(held.cleanup_completion.is_none());
        assert_eq!(
            crate::db::current_quarantine_reason_wire_value(&conn, &sandbox_id_string)
                .unwrap()
                .as_deref(),
            Some(SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str())
        );
        assert!(
            crate::db::scan_quarantine_recovery_candidates(&conn)
                .unwrap()
                .candidates
                .is_empty(),
            "live orphan cleanup must not restore a pre-quarantine snapshot"
        );
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id_string,
            CleanupCompletion::Quarantine,
            None,
        )
        .unwrap();
        assert_eq!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Quarantined
        );
    }

    #[test]
    fn reconcile_inventory_marks_stale_run_dir_for_cleanup() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        std::fs::create_dir_all(run_dir.path().join(&sandbox_id_string)).unwrap();

        let report = reconcile_sandbox_inventory_in(
            &conn,
            cache_dir.path(),
            run_dir.path(),
            ReconcileSource::Test,
        )
        .unwrap();

        assert_eq!(report.changed_ids(), vec![sandbox_id]);
        let row = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::DeleteRow)
        );
        assert_eq!(row.cleanup_keep_filesystem, Some(false));
    }

    #[test]
    fn cleanup_orphaned_dirs_removes_only_invalid_junk() {
        let cache_dir = tempfile::tempdir().unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let known_id = SandboxId::new().to_string();
        let invalid_dir = "not-a-sandbox-id";

        std::fs::create_dir_all(cache_dir.path().join(&known_id)).unwrap();
        std::fs::create_dir_all(run_dir.path().join(&sandbox_id_string)).unwrap();
        std::fs::create_dir_all(run_dir.path().join(&known_id)).unwrap();
        std::fs::create_dir_all(run_dir.path().join(invalid_dir)).unwrap();

        let mut cleaned =
            cleanup_orphaned_dirs_in(cache_dir.path(), run_dir.path(), ReconcileSource::Test);
        cleaned.sort();

        assert_eq!(cleaned, {
            let mut expected = vec!["run:not-a-sandbox-id".to_string()];
            expected.sort();
            expected
        });
        assert!(run_dir.path().join(&sandbox_id_string).exists());
        assert!(run_dir.path().join(&known_id).exists());
        assert!(!run_dir.path().join(invalid_dir).exists());
    }

    #[test]
    fn pre_rootfs_states_are_not_stale_without_cache_root() {
        assert!(sandbox_state_may_lack_cache_root(SandboxState::Creating));
        assert!(sandbox_state_may_lack_cache_root(SandboxState::Created));
        assert!(sandbox_state_may_lack_cache_root(SandboxState::Suspended));
        assert!(sandbox_state_may_lack_cache_root(SandboxState::Restoring));
        assert!(sandbox_state_may_lack_cache_root(
            SandboxState::CleanupRequired
        ));
        assert!(sandbox_state_may_lack_cache_root(
            SandboxState::CleanupFailed
        ));
        assert!(sandbox_state_may_lack_cache_root(SandboxState::Quarantined));

        assert!(!sandbox_state_may_lack_cache_root(SandboxState::Started));
        assert!(!sandbox_state_may_lack_cache_root(SandboxState::Stopped));
        assert!(!sandbox_state_may_lack_cache_root(SandboxState::Failed));
    }

    #[test]
    fn canonical_batched_sandbox_ids_trims_deduplicates_full_ids() {
        let first = Uuid::now_v7().to_string();
        let second = Uuid::now_v7().to_string();
        let ids = canonical_batched_sandbox_ids(&[
            format!(" {first} "),
            String::new(),
            first.clone(),
            second.clone(),
        ])
        .expect("canonical ids should validate");

        assert_eq!(ids, vec![first, second]);
    }

    #[tokio::test]
    async fn inventory_requests_keep_all_ids_across_internal_batches() {
        let service = service_for_teardown_test(crate::db::test_conn());
        let ids = (0..=pplx_space_common_core::inventory::READ_BATCH_ITEMS)
            .map(|_| Uuid::now_v7().to_string())
            .collect::<Vec<_>>();
        assert_eq!(canonical_batched_sandbox_ids(&ids).unwrap(), ids);
        let state = service
            .state(internal_request(StateRequest {
                sandbox_ids: ids.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(state.sandboxes.is_empty());
        assert_eq!(state.missing_ids, ids);
        let disk = service
            .get_disk_usage(internal_request(GetDiskUsageRequest {
                sandbox_ids: ids.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(disk.usage_bytes.is_empty());
        assert_eq!(
            disk.missing_ids
                .into_iter()
                .map(|missing| missing.id)
                .collect::<Vec<_>>(),
            ids
        );
    }

    #[test]
    fn canonical_batched_sandbox_ids_bounds_raw_work_before_deduplication() {
        use pplx_space_common_core::inventory::{MAX_ENCODED_BYTES, MAX_ITEMS};
        let id = Uuid::now_v7().to_string();
        assert_eq!(
            canonical_batched_sandbox_ids(&vec![id.clone(); MAX_ITEMS]).unwrap(),
            vec![id.clone()]
        );
        for ids in [
            vec![id; MAX_ITEMS + 1],
            vec![String::new(); MAX_ITEMS + 1],
            vec![" ".repeat(MAX_ENCODED_BYTES + 1)],
        ] {
            assert_eq!(
                canonical_batched_sandbox_ids(&ids).unwrap_err().code(),
                tonic::Code::ResourceExhausted
            );
        }
    }

    #[tokio::test]
    async fn state_raw_budget_is_not_reset_between_internal_batches() {
        use pplx_space_common_core::inventory::{MAX_RAW_BYTES, READ_BATCH_ITEMS};
        let conn = crate::db::test_conn();
        let mut ids = Vec::new();
        // Valid, padded JSON consumes raw-read budget but produces a tiny wire
        // result, so the response budget cannot mask a reset between batches.
        let env = format!("{{}}{}", " ".repeat(MAX_RAW_BYTES / 2));
        for index in 0..=READ_BATCH_ITEMS {
            let id = Uuid::now_v7().to_string();
            insert_test_sandbox(&conn, &id, SandboxState::Stopped);
            if index == 0 || index == READ_BATCH_ITEMS {
                conn.execute(
                    "UPDATE sandboxes SET process_env_json = ?2 WHERE id = ?1",
                    crate::db::fixture::params![id, env],
                )
                .unwrap();
            }
            ids.push(id);
        }
        let service = service_for_teardown_test(conn);
        let error = service
            .state(internal_request(StateRequest { sandbox_ids: ids }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn canonical_batched_sandbox_ids_rejects_prefixes_and_noncanonical_ids() {
        let id = Uuid::now_v7().to_string();
        let prefix = id
            .get(..8)
            .expect("UUID string has 8-byte prefix")
            .to_string();
        let error = canonical_batched_sandbox_ids(&[prefix])
            .expect_err("batched ids must not accept prefixes");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        let no_hyphens = id.replace('-', "");
        let error = canonical_batched_sandbox_ids(&[no_hyphens])
            .expect_err("batched ids must use canonical UUID formatting");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("non-canonical"));
    }

    #[tokio::test]
    async fn unfamiliar_cleanup_authority_is_retained_across_startup_and_delete() {
        // 052 permits a future intent while the compatibility release has no
        // producer or decoder for it. Malformed identities are equally opaque.
        for completion in ["delete_pool_clone", "future_cleanup"] {
            let conn = crate::db::test_conn();
            conn.execute_batch("PRAGMA ignore_check_constraints = ON;")
                .unwrap();
            let sandbox_id = SandboxId::new();
            let id = sandbox_id.to_string();
            insert_test_sandbox(&conn, &id, SandboxState::Creating);
            conn.execute(
                "INSERT INTO sandbox_cleanup_episodes
                (sandbox_id,cleanup_id,completion,keep_filesystem,phase,activation_state,created_at)
                VALUES (?1,?2,?3,0,'armed','creating',1)",
                crate::db::fixture::params![
                    id,
                    SandboxCleanupEpisodeId::new().to_string(),
                    completion
                ],
            )
            .unwrap();
            let service = service_for_teardown_test(conn);
            let cache = tempfile::tempdir().unwrap();
            service
                .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
                .unwrap();
            assert_eq!(
                service.lookup_sandbox(&id).unwrap().state,
                SandboxState::Creating
            );
            assert!(
                !cache.path().join(&id).exists(),
                "recovery must not prepare someone else's tree"
            );
            let error = service
                .delete_snapshots(internal_request(DeleteSnapshotsRequest {
                    sandbox_id: id.clone(),
                    all: true,
                    snapshot_name: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            let error = service
                .rollback(internal_request(RollbackRequest {
                    sandbox_id: id.clone(),
                    snapshot_name: "d-review-regression".into(),
                    auto_restart: false,
                    netns_path: None,
                    route_identity: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            let admission = service
                .admit_lifecycle_cleanup(
                    sandbox_id,
                    SandboxState::Deleting,
                    false,
                    CleanupCompletion::DeleteRow,
                    false,
                )
                .await;
            assert!(
                admission.is_err(),
                "unfamiliar authority must not admit ordinary cleanup"
            );
            assert!(crate::db::has_cleanup_episode(&service.db.lock().unwrap(), &id).unwrap());
        }
    }

    #[tokio::test]
    async fn unreadable_ownership_blocks_an_already_admitted_cleanup_fallback() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        let admission = service
            .admit_lifecycle_cleanup(
                sandbox_id,
                SandboxState::Deleting,
                false,
                CleanupCompletion::DeleteRow,
                false,
            )
            .await
            .unwrap();
        service
            .db
            .lock()
            .unwrap()
            .execute_batch("DROP TABLE sandbox_cleanup_episodes;")
            .unwrap();
        let mut dispatched = false;
        let result = service
            .run_cleanup_after_persistence_failure_with_node_dispatch(
                SynchronousCleanupRequest {
                    sandbox_id: &id,
                    uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id).as_str(),
                    keep_filesystem: false,
                    context: "ownership failure",
                    completion: CleanupCompletion::DeleteRow,
                    published_artifact: None,
                    immediate_kill: false,
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                },
                &admission,
                || async {
                    dispatched = true;
                    Ok(())
                },
            )
            .await;
        assert!(result.is_err());
        assert!(!dispatched);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_completes_for_quarantine_without_a_caller_deadline() {
        for state in [
            SandboxState::Quarantined,
            SandboxState::Stopped,
            SandboxState::Suspended,
            SandboxState::Failed,
            SandboxState::CleanupFailed,
        ] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &id, state);
            let service = service_for_teardown_test(conn);
            let response = tokio::time::timeout(
                Duration::from_secs(2),
                service.wait(internal_request(WaitRequest {
                    sandbox_id: id,
                    timeout_secs: None,
                })),
            )
            .await
            .expect("Wait must complete without requiring an API timeout")
            .unwrap()
            .into_inner();
            assert_eq!(
                response.observed_lifecycle().unwrap(),
                crate::proto::SandboxLifecycleState::from(state)
            );
        }
    }

    pub(crate) use crate::db::fixture::{insert_test_sandbox, insert_test_sandbox_with_policy};

    fn mark_test_remote_restore_artifact_staging(conn: &crate::db::Connection, id: &str) {
        conn.execute(
            "UPDATE sandboxes
             SET restore_origin = 'remote',
                 restore_phase = 'metadata_staging',
                 restore_storage_uri = ?2
             WHERE id = ?1 AND state = 'restoring'",
            crate::db::fixture::params![
                id,
                format!("s3://snapshots/suspend/{id}?region=us-east-1")
            ],
        )
        .unwrap();
        assert!(crate::db::record_remote_restore_artifact_staging(conn, id).unwrap());
    }

    pub(crate) fn insert_test_sandbox_with_runtime(
        conn: &crate::db::Connection,
        id: &str,
        state: SandboxState,
    ) {
        insert_test_sandbox(conn, id, SandboxState::Started);
        crate::db::update_runtime_fields(
            conn,
            id.parse().unwrap(),
            crate::db::RuntimeFields {
                uid: 10000,
                slot: 7,
                guest_cid: 37,
                pid: std::process::id(),
                spaced_host_pid: Some(10001),
                netgtw_pid: 10002,
                vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
                api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
                spaced_endpoint_proof: None,
                space_api_route_identity:
                    pplx_space_common_core::types::SpaceApiRouteIdentity::default(),
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            },
        )
        .unwrap();
        if state != SandboxState::Started {
            conn.execute(
                "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
                crate::db::fixture::params![state.as_str(), id],
            )
            .unwrap();
        }
    }

    fn insert_started_test_sandbox(conn: &crate::db::Connection, id: &str) {
        insert_test_sandbox_with_runtime(conn, id, SandboxState::Started);
    }

    /// Move a live `started` row to `restoring`, keeping its empty restore
    /// columns. No production edge writes this.
    fn restage_started_as_restoring(conn: &crate::db::Connection, id: &str) {
        assert_eq!(
            crate::db::update_state_from_states(
                conn,
                id,
                crate::db::StateSet::of(&[SandboxState::Started]),
                SandboxState::Restoring,
            )
            .unwrap(),
            crate::db::StateWriteOutcome::Applied {
                from: SandboxState::Started
            }
        );
    }

    pub(crate) fn complete_runtime_endpoint_identity(conn: &crate::db::Connection, id: &str) {
        conn.execute(
            "UPDATE sandboxes
                SET space_api_route_execution_generation = 1,
                    space_api_route_activation_attempt_id = ?1
              WHERE id = ?2",
            crate::db::fixture::params![SandboxActivationAttemptId::new().to_string(), id],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_rejects_request_without_peer_credentials() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = service_with_runtime_probe(conn, false);

        let error = service
            .evict_cluster_deleted_sandbox(tonic::Request::new(
                crate::proto::EvictClusterDeletedSandboxRequest {
                    sandbox_id: sandbox_id.clone(),
                },
            ))
            .await
            .expect_err(
                "a request without UDS peer credentials must be rejected before any row inspection",
            );
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap();
        assert!(
            row.is_some_and(|row| row.state == SandboxState::Suspended),
            "unauthenticated purge attempt must leave the row untouched"
        );
    }

    fn root_peer_request(
        sandbox_id: &str,
    ) -> tonic::Request<crate::proto::EvictClusterDeletedSandboxRequest> {
        let mut request = tonic::Request::new(crate::proto::EvictClusterDeletedSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
        });
        request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));
        request
    }

    fn insert_platform_owned_sandbox(
        conn: &crate::db::Connection,
        id: &str,
        state: SandboxState,
        pool_template: Option<&str>,
    ) {
        let sandbox_id = SandboxId::from_str(id).unwrap();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
        let user = SandboxOwner::platform_user(UserId::new());
        crate::db::insert_sandbox(
            conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id,
                uuid8: &uuid8,
                state,
                template: "seed",
                template_hash: Some("hash"),
                user: &user,
                runtime: "fc",
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template,
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
    }

    #[tokio::test]
    async fn worker_auto_lifecycle_delete_claims_cleanup_and_receipt_atomically() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 900,
                action: AutoLifecycleIdleAction::Delete,
            }],
        }
        .normalized();
        insert_test_sandbox_with_policy(
            &conn,
            &sandbox_id.to_string(),
            SandboxState::Started,
            &policy,
        );
        let activation_attempt_id = SandboxActivationAttemptId::new();
        let current_node_id = NodeId::new();
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                route_generation: Some(RouteGeneration::new(7).unwrap()),
                current_node_id: Some(current_node_id),
                execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
                activation_attempt_id: Some(activation_attempt_id),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                pod_uid: Some(PodUid::parse("pod-uid-7").unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_role: Some(NodeRole::Worker),
                ..SandboxManagerServiceOptions::for_test()
            },
        );

        let admission = service
            .admit_auto_lifecycle_delete(
                sandbox_id,
                crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation {
                    policy_fingerprint:
                        pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                            &policy,
                        ),
                    authority: crate::db::PolicyAuthority::Cluster,
                },
            )
            .await
            .unwrap();
        let claimed_receipt = admission
            .auto_lifecycle_delete_receipt(sandbox_id)
            .unwrap()
            .expect("worker claim carries receipt evidence");

        {
            let db = service.db.lock().unwrap();
            let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert_eq!(row.state, SandboxState::Deleting);
            assert_eq!(row.cleanup_completion, Some(CleanupCompletion::DeleteRow));
            assert_eq!(row.cleanup_keep_filesystem, Some(false));
            assert!(
                crate::db::cleanup_episode(&db, &sandbox_id.to_string())
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                    &db, sandbox_id,
                )
                .unwrap(),
                Some(claimed_receipt)
            );
        }

        let successor_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(8).unwrap()),
            current_node_id: Some(current_node_id),
            execution_generation: Some(ExecutionGeneration::new(8).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            pod_uid: Some(PodUid::parse("pod-uid-8").unwrap()),
            ..SpaceApiRouteIdentity::default()
        };
        let error = service
            .adopt_authoritative_route_identity(sandbox_id, &successor_identity)
            .expect_err("route-only adoption cannot supersede cleanup-owned deletion");
        assert_eq!(error.code(), tonic::Code::Aborted);
        {
            let db = service.db.lock().unwrap();
            let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert_eq!(row.state, SandboxState::Deleting);
            assert_eq!(row.cleanup_completion, Some(CleanupCompletion::DeleteRow));
            assert_eq!(row.cleanup_keep_filesystem, Some(false));
            assert_eq!(
                crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                    &db, sandbox_id,
                )
                .unwrap(),
                Some(claimed_receipt)
            );
        }
        drop(admission);

        let error = service
            .admit_lifecycle_cleanup(
                sandbox_id,
                SandboxState::Deleting,
                false,
                CleanupCompletion::DeleteRow,
                true,
            )
            .await
            .err()
            .expect("ordinary cleanup cannot bypass a pending delete receipt")
            .into_status();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            error
                .message()
                .contains("pending auto-lifecycle delete receipt")
        );
    }

    async fn assert_evict_refused_and_row_untouched(
        service: &SandboxManagerService,
        sandbox_id: &str,
        expected_state: SandboxState,
        expected_message_fragment: &str,
    ) {
        let error = service
            .evict_cluster_deleted_sandbox(root_peer_request(sandbox_id))
            .await
            .expect_err("guard must refuse the eviction");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains(expected_message_fragment),
            "expected {expected_message_fragment:?} in {:?}",
            error.message()
        );
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, expected_state, "row must be untouched");
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_non_suspended_rows() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Started, None);
        let service = service_with_runtime_probe(conn, false);

        assert_evict_refused_and_row_untouched(
            &service,
            &sandbox_id,
            SandboxState::Started,
            "not suspended",
        )
        .await;
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_restoring_rows_without_a_persisted_failure() {
        // A `restoring` row with no staging failure on it is a restore in
        // flight (or one whose failure was never persisted); the cluster
        // reporting the sandbox deleted is not enough to tear it down.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Restoring, None);
        conn.execute(
            "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        assert_evict_refused_and_row_untouched(
            &service,
            &sandbox_id,
            SandboxState::Restoring,
            "no persisted staging failure",
        )
        .await;
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_treats_a_blank_failure_reason_as_no_marker() {
        // node-ctl ignores whitespace-only reasons; sbx-manager must agree so
        // the two sides never disagree about what a retained row looks like.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Restoring, None);
        assert!(
            crate::db::mark_cross_node_restore_staging_failed(&conn, &sandbox_id, "   ").unwrap()
        );
        conn.execute(
            "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        assert_evict_refused_and_row_untouched(
            &service,
            &sandbox_id,
            SandboxState::Restoring,
            "no persisted staging failure",
        )
        .await;
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_reclaims_retained_restoring_and_quarantined_rows() {
        for state in [
            SandboxState::Restoring,
            SandboxState::Quarantined,
            SandboxState::Stopped,
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            let initial_state = if state == SandboxState::Quarantined {
                SandboxState::Suspending
            } else {
                state
            };
            insert_platform_owned_sandbox(&conn, &sandbox_id, initial_state, None);
            if state == SandboxState::Restoring {
                // The real retained row still carries its remote restore columns
                // (origin, phase, storage uri); the claim must clear them or trip
                // the table's restore-columns CHECK.
                mark_test_remote_restore_artifact_staging(&conn, &sandbox_id);
                assert!(
                    crate::db::mark_cross_node_restore_staging_failed(
                        &conn,
                        &sandbox_id,
                        "cross-node restore artifact staging failed: btrfs receive exited with 1",
                    )
                    .unwrap()
                );
            } else if state == SandboxState::Quarantined {
                crate::db::mark_quarantined(
                    &conn,
                    &sandbox_id,
                    SandboxQuarantineSource::SbxManager,
                    SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
                    "synthetic snapshot publication failure",
                    SandboxQuarantineNextAction::RequiresOperatorAttention,
                )
                .unwrap();
            }
            conn.execute(
                "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
                crate::db::fixture::params![sandbox_id],
            )
            .unwrap();
            if state == SandboxState::Quarantined {
                // Inventory keeps a hold that has no local evidence; this
                // eviction is one explicit disposition that releases it.
                let cache_dir = tempfile::tempdir().unwrap();
                let run_dir = tempfile::tempdir().unwrap();
                let report = reconcile_sandbox_inventory_in(
                    &conn,
                    cache_dir.path(),
                    run_dir.path(),
                    ReconcileSource::Test,
                )
                .unwrap();
                assert!(report.changed_ids().is_empty());
                assert_eq!(
                    crate::db::get_sandbox(&conn, &sandbox_id)
                        .unwrap()
                        .map(|row| row.state),
                    Some(SandboxState::Quarantined)
                );
            }
            let service = service_with_runtime_probe(conn, false);

            let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
            let observed = Arc::clone(&dispatched);
            *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
                observed.lock().unwrap().push((id, kill));
                Box::pin(async { Ok(()) })
            }));

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                service.evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id)),
            )
            .await;
            service.shutdown_token.cancel();
            service.task_tracker.close();
            tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
                .await
                .expect("eviction and cleanup tasks must drain");
            result
                .expect("eviction should complete")
                .expect("a retained row must be reclaimed once the cluster row is gone");
            assert_eq!(
                *dispatched.lock().unwrap(),
                vec![(sandbox_id.clone(), false)]
            );
            let db = service.db.lock().unwrap();
            assert!(crate::db::get_sandbox(&db, &sandbox_id).unwrap().is_none());
            assert!(
                crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
                    .unwrap()
                    .is_none()
            );
        }
    }

    /// node-manager refuses cleanup while another host mutation, such as a
    /// network-slot reset, holds the sandbox's exclusive claim. A refused
    /// first attempt fails the eviction RPC after the row was claimed, so the
    /// claim must leave durable cleanup work that recovery completes without
    /// another eviction call.
    #[tokio::test]
    async fn evict_cluster_deleted_stopped_sandbox_converges_after_a_refused_cleanup() {
        const REFUSED: &str = "sandbox host mutation or reset is in progress; retry";
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Stopped, None);
        conn.execute(
            "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&dispatched);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
            let mut observed = observed.lock().unwrap();
            observed.push((id, kill));
            let refused = observed.len() == 1;
            Box::pin(async move {
                if refused {
                    Err(Status::unavailable(REFUSED))
                } else {
                    Ok(())
                }
            })
        }));

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            service.evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id)),
        )
        .await
        .expect("the refused attempt must return")
        .expect_err("the refused cleanup attempt fails the eviction call");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains(REFUSED), "{error:?}");

        let converged = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let gone = crate::db::get_sandbox(&service.db.lock().unwrap(), &sandbox_id)
                    .unwrap()
                    .is_none();
                if gone {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
            .await
            .expect("eviction and cleanup tasks must drain");
        converged.expect("durable cleanup recovery must delete the claimed row");
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec![(sandbox_id.clone(), false), (sandbox_id, false)],
            "recovery retries the same full-delete cleanup once"
        );
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_retained_rows_while_a_pull_holds_the_staging_lock()
     {
        // The marker is a shape filter, not a liveness proof: a retry of the
        // failed restore reuses the row and holds the per-sandbox staging
        // lock for its whole pull without writing the row. The eviction must
        // refuse while that lock is held, leave the row untouched, and say
        // so as `Aborted` so node-ctl retries on the next sync instead of
        // backing the id off.
        for state in [SandboxState::Restoring, SandboxState::Quarantined] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_platform_owned_sandbox(&conn, &sandbox_id, state, None);
            if state == SandboxState::Restoring {
                mark_test_remote_restore_artifact_staging(&conn, &sandbox_id);
                assert!(
                    crate::db::mark_cross_node_restore_staging_failed(
                        &conn,
                        &sandbox_id,
                        "cross-node restore artifact staging failed: btrfs receive exited with 1",
                    )
                    .unwrap()
                );
            }
            conn.execute(
                "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
                crate::db::fixture::params![sandbox_id],
            )
            .unwrap();
            let service = service_with_runtime_probe(conn, false);
            let pull_in_flight = service
                .auto_lifecycle
                .staging_lock(&sandbox_id)
                .lock_owned()
                .await;

            let error = tokio::time::timeout(
                Duration::from_secs(5),
                service.evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id)),
            )
            .await
            .expect("eviction must not block on the staging lock")
            .expect_err("a live staging pull must refuse the eviction");
            assert_eq!(error.code(), tonic::Code::Aborted);
            assert!(
                error.message().contains("restore staging pull in flight"),
                "unexpected message {:?}",
                error.message()
            );
            {
                let db = service.db.lock().expect("mutex poisoned");
                let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
                assert_eq!(row.state, state, "row must be untouched");
                if state == SandboxState::Restoring {
                    assert!(row.failure_reason.is_some(), "marker must be untouched");
                }
            }
            drop(pull_in_flight);
            service.shutdown_token.cancel();
            service.task_tracker.close();
            tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
                .await
                .expect("tasks must drain");
        }
    }

    #[test]
    fn quarantined_eviction_claim_rejects_stale_version_and_recovered_state() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &id, SandboxState::Quarantined, None);
        let version = crate::db::get_sandbox(&conn, &id)
            .unwrap()
            .unwrap()
            .updated_at;
        assert!(!crate::db::claim_quarantined_sandbox_for_eviction(&conn, &id, "0").unwrap());
        conn.execute(
            "UPDATE sandboxes SET state = 'suspended' WHERE id = ?1",
            [&id],
        )
        .unwrap();
        assert!(!crate::db::claim_quarantined_sandbox_for_eviction(&conn, &id, &version).unwrap());
        conn.execute(
            "UPDATE sandboxes SET state = 'quarantined' WHERE id = ?1",
            [&id],
        )
        .unwrap();
        assert!(crate::db::claim_quarantined_sandbox_for_eviction(&conn, &id, &version).unwrap());
        assert_eq!(
            crate::db::get_sandbox(&conn, &id).unwrap().unwrap().state,
            SandboxState::Deleting
        );
    }

    #[test]
    fn failed_restoring_eviction_claim_requires_the_marker_and_the_row_version() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Restoring, None);
        let version = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .unwrap()
            .updated_at;

        // No persisted failure: never claimable, even at the right version.
        assert!(
            !crate::db::claim_failed_restoring_sandbox_for_eviction(&conn, &sandbox_id, &version)
                .unwrap()
        );
        // A marker that is only tabs/newlines is "no marker" to both Rust
        // guards (`str::trim`); the SQL must agree, and SQLite's one-argument
        // `trim` would not (it strips spaces only).
        assert!(
            crate::db::mark_cross_node_restore_staging_failed(&conn, &sandbox_id, "\t\n\r ")
                .unwrap()
        );
        let version = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .unwrap()
            .updated_at;
        assert!(
            !crate::db::claim_failed_restoring_sandbox_for_eviction(&conn, &sandbox_id, &version)
                .unwrap(),
            "a whitespace-only marker must not be claimable"
        );
        assert!(
            crate::db::mark_cross_node_restore_staging_failed(&conn, &sandbox_id, "staging failed")
                .unwrap()
        );
        // A version other than the row's current one must not claim
        // (`updated_at` is second-granular, so the mark may share the insert's
        // stamp; make the staleness explicit).
        assert!(
            !crate::db::claim_failed_restoring_sandbox_for_eviction(&conn, &sandbox_id, "0")
                .unwrap()
        );
        // The realistic race: the eviction was decided against this version,
        // then a retry of the restore advanced the row (the marker stays —
        // retries never clear it). Pin the pre-retry version to a distinct
        // stamp so the advance is observable regardless of clock granularity.
        let decided_version = "1000000000";
        conn.execute(
            "UPDATE sandboxes SET updated_at = ?1, restore_origin = 'remote', \
             restore_phase = 'metadata_staging', restore_storage_uri = ?3 WHERE id = ?2",
            crate::db::fixture::params![
                decided_version,
                sandbox_id,
                format!("s3://snapshots/suspend/{sandbox_id}?region=us-east-1")
            ],
        )
        .unwrap();
        assert!(crate::db::record_remote_restore_artifact_staging(&conn, &sandbox_id).unwrap());
        let after_retry = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(after_retry.state, SandboxState::Restoring);
        assert!(
            after_retry.failure_reason.is_some(),
            "a retry keeps the stale marker"
        );
        assert_ne!(after_retry.updated_at, decided_version);
        assert!(
            !crate::db::claim_failed_restoring_sandbox_for_eviction(
                &conn,
                &sandbox_id,
                decided_version
            )
            .unwrap(),
            "a claim decided before the retry advanced the row must lose"
        );
        let version = after_retry.updated_at;
        assert!(
            crate::db::claim_failed_restoring_sandbox_for_eviction(&conn, &sandbox_id, &version)
                .unwrap()
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Deleting);
    }

    /// Build the row a teardown suspend strands when the sandbox was deleted
    /// while one of its executions was still starting: the delete found no
    /// reachable runtime and skipped sandbox-manager entirely, the activation
    /// then finished, and the pod teardown's generation suspend captured and
    /// staged its artifact locally but had the durable backup permanently
    /// refused by a cluster that no longer has a row for the sandbox.
    ///
    /// Assembled out of the production writers rather than raw SQL so the
    /// fixture cannot drift from the shape those writers actually produce.
    pub(crate) fn insert_stranded_teardown_suspend_sandbox(
        conn: &crate::db::Connection,
        sandbox_id: SandboxId,
        execution_generation: ExecutionGeneration,
    ) -> LocalArtifactId {
        let sandbox_id_string = sandbox_id.to_string();
        insert_platform_owned_sandbox(conn, &sandbox_id_string, SandboxState::Paused, None);
        crate::db::update_space_api_route_identity(
            conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                route_generation: Some(
                    RouteGeneration::new(execution_generation.get().unsigned_abs()).unwrap(),
                ),
                current_node_id: Some(NodeId::new()),
                execution_generation: Some(execution_generation),
                activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                pod_uid: Some(PodUid::parse("pod-uid-stranded").unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        crate::db::begin_teardown_suspend(conn, &sandbox_id_string, execution_generation).unwrap();
        let local_artifact_id = LocalArtifactId::new();
        crate::db::stage_teardown_suspend_artifact(
            conn,
            &sandbox_id_string,
            execution_generation,
            local_artifact_id,
            &format!("/var/lib/space/sandboxes/{sandbox_id_string}/suspend"),
        )
        .unwrap();
        local_artifact_id
    }

    fn set_test_row_version(conn: &crate::db::Connection, sandbox_id: &str, updated_at: &str) {
        conn.execute(
            "UPDATE sandboxes SET updated_at = ?1 WHERE id = ?2",
            crate::db::fixture::params![updated_at, sandbox_id],
        )
        .unwrap();
    }

    fn set_test_state_changed_at(
        conn: &crate::db::Connection,
        sandbox_id: &str,
        state_changed_at: &str,
    ) {
        conn.execute(
            "UPDATE sandboxes SET state_changed_at = ?1 WHERE id = ?2",
            crate::db::fixture::params![state_changed_at, sandbox_id],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_reclaims_a_terminally_failed_teardown_suspend() {
        // The deadlock: the pod teardown's generation suspend can never
        // publish (the cluster refuses the backup for a sandbox it no longer
        // has), delete is refused while a suspend awaits publication, and
        // periodic recovery skips a row whose artifact is already staged. The
        // shim therefore polls PrepareTeardownSuspend forever and the pod
        // stays Terminating. The cluster-deleted eviction must reclaim the
        // row, and the delete receipt it records must let the next poll be
        // answered DELETED so the shim can release the task.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let execution_generation = ExecutionGeneration::new(7).unwrap();
        let staged =
            insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, execution_generation);
        assert!(
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap()
        );
        set_test_state_changed_at(&conn, &sandbox_id_string, "0");
        set_test_row_version(&conn, &sandbox_id_string, "0");
        let service = service_with_runtime_probe(conn, false);

        let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&dispatched);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
            observed.lock().unwrap().push((id, kill));
            Box::pin(async { Ok(()) })
        }));

        tokio::time::timeout(
            Duration::from_secs(5),
            service.evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id_string)),
        )
        .await
        .expect("eviction should complete")
        .expect(
            "a terminally failed teardown suspend must be reclaimed once the cluster row is gone",
        );

        // The stranded execution's runtime is torn down, not just forgotten:
        // unlike the suspended and failed-restoring shapes the VM can still
        // be resident when the backup is refused.
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec![(sandbox_id_string.clone(), false)]
        );
        {
            let db = service.db.lock().unwrap();
            assert!(
                crate::db::get_sandbox(&db, &sandbox_id_string)
                    .unwrap()
                    .is_none()
            );
        }

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id_string,
                execution_generation.get(),
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Deleted,
            "the shim must be released instead of polling a row that no longer exists"
        );

        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
            .await
            .expect("eviction and cleanup tasks must drain");
    }

    /// Stand in for the node loops that keep re-stamping a stranded row's
    /// `updated_at` with a value identical to what is already there. Stopped
    /// and joined on drop too, so a test that fails or times out does not
    /// leave the thread writing.
    struct RowToucher {
        stop: Arc<std::sync::atomic::AtomicBool>,
        handle: Option<std::thread::JoinHandle<usize>>,
    }

    impl RowToucher {
        /// Start touching; returns once the first touch has landed.
        async fn start(db: crate::db::SharedConnection, sandbox_id: String) -> Self {
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = Arc::clone(&stop);
            let (landed, first_touch) = tokio::sync::oneshot::channel();
            let handle = std::thread::spawn(move || {
                let mut landed = Some(landed);
                let mut touches = 0;
                while !flag.load(std::sync::atomic::Ordering::Relaxed) {
                    {
                        let db = db.lock().expect("mutex poisoned");
                        touches += db
                            .execute(
                                "UPDATE sandboxes SET updated_at = ?1 WHERE id = ?2",
                                crate::db::fixture::params![
                                    crate::sandbox_eviction::now_secs().to_string(),
                                    sandbox_id
                                ],
                            )
                            .expect("touch must not fail");
                    }
                    if touches > 0
                        && let Some(landed) = landed.take()
                    {
                        let _ = landed.send(());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                touches
            });
            let toucher = Self {
                stop,
                handle: Some(handle),
            };
            tokio::time::timeout(Duration::from_secs(5), first_touch)
                .await
                .expect("the toucher must land its first write")
                .expect("the toucher must not exit before its first write");
            toucher
        }

        /// Stop touching and return how many touches landed.
        fn finish(mut self) -> usize {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            self.handle
                .take()
                .expect("the toucher is joined once")
                .join()
                .expect("toucher must not panic")
        }
    }

    impl Drop for RowToucher {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_reclaims_a_constantly_touched_teardown_suspend() {
        // The stranded row is not quiet on a real node, and the eviction may
        // not assume it is. Two independent loops re-stamp `updated_at` on it
        // for as long as it is stuck, each writing values identical to what
        // is already stored:
        //
        //   * the sidecar watchdog's staggered net-gateway stats probe, every
        //     30 s while the refused suspend's runtime is still resident —
        //     `update_network_counters` is `MAX()`-guarded, so an idle
        //     sandbox's counters never move but the row version always does;
        //   * the auto-lifecycle scheduler, every 5 s once an `idle:delete`
        //     stage falls due, re-recording the identical refusal through
        //     `update_auto_lifecycle_state`, because a `suspending` row
        //     refuses delete and `Delete` is not one of the gated actions.
        //
        // So a quiescence window measured on `updated_at` never opens, and a
        // claim CAS that asserts `updated_at` can always lose the race. Both
        // have to key on the row's own state instead: `state_changed_at`
        // moves only when the suspend actually ends, which is the event the
        // window exists to wait out.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let execution_generation = ExecutionGeneration::new(7).unwrap();
        let staged =
            insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, execution_generation);
        assert!(
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap()
        );
        // Long past the window by the only clock that tracks the row's state,
        // and permanently inside it by the row version.
        set_test_state_changed_at(&conn, &sandbox_id_string, "1000000000");
        set_test_row_version(
            &conn,
            &sandbox_id_string,
            &crate::sandbox_eviction::now_secs().to_string(),
        );
        let service = service_with_runtime_probe(conn, false);

        let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&dispatched);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
            observed.lock().unwrap().push((id, kill));
            Box::pin(async { Ok(()) })
        }));

        // A write has landed before the RPC starts, and the toucher keeps
        // writing through it, so the row version has moved since the setup
        // above and may move again at any point during the RPC. Nothing here
        // forces a write into the gap between the decision and the claim;
        // the claim must commit whether or not one lands there.
        let toucher = RowToucher::start(service.db.clone(), sandbox_id_string.clone()).await;
        let evicted = tokio::time::timeout(
            Duration::from_secs(5),
            service.evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id_string)),
        )
        .await
        .expect("eviction should complete");
        let touches = toucher.finish();

        evicted.expect(
            "a row whose only recent writes are incidental touches must still be reclaimed",
        );
        assert!(
            touches > 0,
            "the toucher must actually have written the row"
        );
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec![(sandbox_id_string.clone(), false)]
        );
        {
            let db = service.db.lock().unwrap();
            assert!(
                crate::db::get_sandbox(&db, &sandbox_id_string)
                    .unwrap()
                    .is_none()
            );
        }

        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
            .await
            .expect("eviction and cleanup tasks must drain");
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_still_defers_a_fresh_teardown_suspend() {
        // Reading the window off `state_changed_at` must not remove it. A row
        // that entered `suspending` moments ago is refused with the remaining
        // window as its retry horizon, exactly as an inert shape is — the
        // marker says the suspend's publication path is over, not that the
        // node has finished moving the row.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let staged = insert_stranded_teardown_suspend_sandbox(
            &conn,
            sandbox_id,
            ExecutionGeneration::new(7).unwrap(),
        );
        assert!(
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap()
        );
        // Old by the row version, fresh by the state clock: the inverse of
        // the stranded row, and it must be read the other way round.
        set_test_row_version(&conn, &sandbox_id_string, "1000000000");
        let service = service_with_runtime_probe(conn, false);

        let refusal = service
            .evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id_string))
            .await
            .expect_err("a suspend that just began is still inside the window");
        assert_eq!(refusal.code(), tonic::Code::FailedPrecondition);
        assert!(
            refusal.message().contains("state_changed_at"),
            "the refusal must name the clock it used: {:?}",
            refusal.message()
        );
        let horizon = pplx_space_common_grpc::retry_after(&refusal)
            .expect("quiescence refusal must carry a retry horizon");
        assert!(
            horizon > std::time::Duration::from_secs(590)
                && horizon <= std::time::Duration::from_secs(600),
            "horizon should be the remaining window, got {horizon:?}"
        );
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::get_sandbox(&db, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Suspending
        );
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_malformed_state_changed_at() {
        // The teardown-suspend window has no fallback clock: an unreadable
        // `state_changed_at` is an operator repair, not a licence to reclaim
        // on whatever `updated_at` happens to say.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let staged = insert_stranded_teardown_suspend_sandbox(
            &conn,
            sandbox_id,
            ExecutionGeneration::new(7).unwrap(),
        );
        assert!(
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap()
        );
        set_test_state_changed_at(&conn, &sandbox_id_string, "not-a-timestamp");
        set_test_row_version(&conn, &sandbox_id_string, "1000000000");
        let service = service_with_runtime_probe(conn, false);

        assert_evict_refused_and_row_untouched(
            &service,
            &sandbox_id_string,
            SandboxState::Suspending,
            "malformed state_changed_at",
        )
        .await;
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_a_marked_row_with_no_staged_artifact() {
        // The marker is only ever written together with the artifact the
        // refused backup covered, so a marked row whose pin is gone has been
        // changed by something else and is no longer the shape the claim
        // describes.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let staged = insert_stranded_teardown_suspend_sandbox(
            &conn,
            sandbox_id,
            ExecutionGeneration::new(7).unwrap(),
        );
        assert!(
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap()
        );
        conn.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = NULL,
                 current_local_artifact_kind = NULL,
                 current_local_artifact_origin = NULL
             WHERE id = ?1",
            crate::db::fixture::params![sandbox_id_string],
        )
        .unwrap();
        set_test_state_changed_at(&conn, &sandbox_id_string, "1000000000");
        let service = service_with_runtime_probe(conn, false);

        assert_evict_refused_and_row_untouched(
            &service,
            &sandbox_id_string,
            SandboxState::Suspending,
            "no staged artifact pinned",
        )
        .await;
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_suspending_rows_that_can_still_publish() {
        // Only a permanently refused backup ends a suspend's own publication
        // path. A suspend still uploading, or one carrying an unrelated
        // failure that recovery can still finish, must survive the deleted
        // report — its artifact may be the only copy of the sandbox's work.
        for (label, failure_reason) in [
            ("no failure recorded", None),
            (
                "an unrelated suspend failure",
                Some("publish failed: rename_failed"),
            ),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new();
            let sandbox_id_string = sandbox_id.to_string();
            insert_stranded_teardown_suspend_sandbox(
                &conn,
                sandbox_id,
                ExecutionGeneration::new(7).unwrap(),
            );
            if let Some(reason) = failure_reason {
                conn.execute(
                    "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
                    crate::db::fixture::params![reason, sandbox_id_string],
                )
                .unwrap();
            }
            set_test_row_version(&conn, &sandbox_id_string, "0");
            let service = service_with_runtime_probe(conn, false);

            assert_evict_refused_and_row_untouched(
                &service,
                &sandbox_id_string,
                SandboxState::Suspending,
                "no permanently refused teardown backup",
            )
            .await;
            assert!(
                service
                    .db
                    .lock()
                    .unwrap()
                    .query_row(
                        "SELECT current_local_artifact_id IS NOT NULL FROM sandboxes WHERE id = ?1",
                        crate::db::fixture::params![sandbox_id_string],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap(),
                "{label}: the staged artifact must survive the refusal"
            );
        }
    }

    #[test]
    fn terminal_teardown_suspend_marker_is_scoped_and_leaves_the_row_version_alone() {
        // The shim re-polls PrepareTeardownSuspend for as long as the pod is
        // terminating, and every poll re-drives the suspend through the same
        // terminal backup job. A marker that rewrote the row each time would
        // hold it permanently inside the eviction's quiescence window, so the
        // repeat write must be a no-op. The marker is also scoped to the
        // artifact the refused backup covered.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let staged = insert_stranded_teardown_suspend_sandbox(
            &conn,
            sandbox_id,
            ExecutionGeneration::new(7).unwrap(),
        );
        set_test_row_version(&conn, &sandbox_id_string, "1000000000");

        assert!(
            !crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                LocalArtifactId::new(),
            )
            .unwrap(),
            "a backup for some other artifact must not mark this row"
        );
        assert_eq!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .updated_at,
            "1000000000"
        );

        assert!(
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap()
        );
        let marked = crate::db::get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(
            marked.failure_reason.as_deref(),
            Some(pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON)
        );
        assert_eq!(marked.state, SandboxState::Suspending);

        set_test_row_version(&conn, &sandbox_id_string, "1000000001");
        assert!(
            !crate::db::mark_teardown_suspend_backup_permanently_failed(
                &conn,
                &sandbox_id_string,
                staged,
            )
            .unwrap(),
            "a re-poll must not restart the quiescence clock"
        );
        assert_eq!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .updated_at,
            "1000000001"
        );

        // The claim re-asserts the row's identity, not its version, so the
        // touch above does not lock the row out of being reclaimed.
        assert!(
            crate::db::claim_teardown_suspend_failed_sandbox_for_eviction(
                &conn,
                &sandbox_id_string,
                staged,
                &state_changed_at(&conn, &sandbox_id_string),
            )
            .unwrap()
        );
        assert_eq!(
            crate::db::get_sandbox(&conn, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Deleting
        );
    }

    fn state_changed_at(conn: &crate::db::Connection, sandbox_id: &str) -> String {
        crate::db::sandbox_state_changed_at_witness(conn, sandbox_id)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn terminal_teardown_suspend_claim_asserts_identity_not_the_row_version() {
        // What the CAS has to prove is "this is still the row the eviction
        // decided about", and on this shape the row version cannot carry
        // that: live loops advance `updated_at` without the row changing, so
        // asserting it would refuse a claim that is perfectly safe, forever.
        // Identity is instead the state, the exact marker, the exact staged
        // artifact, and `state_changed_at` — a tuple nothing can leave intact
        // while moving the sandbox on.
        let execution_generation = ExecutionGeneration::new(7).unwrap();
        let claim_after = |mutate: &dyn Fn(&crate::db::Connection, &str, LocalArtifactId)| {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new();
            let sandbox_id_string = sandbox_id.to_string();
            let staged =
                insert_stranded_teardown_suspend_sandbox(&conn, sandbox_id, execution_generation);
            assert!(
                crate::db::mark_teardown_suspend_backup_permanently_failed(
                    &conn,
                    &sandbox_id_string,
                    staged,
                )
                .unwrap()
            );
            let decided_state_changed_at = state_changed_at(&conn, &sandbox_id_string);
            mutate(&conn, &sandbox_id_string, staged);
            crate::db::claim_teardown_suspend_failed_sandbox_for_eviction(
                &conn,
                &sandbox_id_string,
                staged,
                &decided_state_changed_at,
            )
            .unwrap()
        };

        assert!(claim_after(&|_, _, _| {}), "the undisturbed row is claimed");
        assert!(
            claim_after(&|conn, id, _| set_test_row_version(conn, id, "2000000000")),
            "a bare row-version touch must not cost the claim"
        );
        assert!(
            !claim_after(&|conn, id, _| set_test_state_changed_at(conn, id, "2000000000")),
            "a row that changed state under the caller must not be claimed"
        );
        assert!(
            !claim_after(&|conn, id, _| {
                conn.execute(
                    "UPDATE sandboxes SET current_local_artifact_id = ?1 WHERE id = ?2",
                    crate::db::fixture::params![LocalArtifactId::new().to_string(), id],
                )
                .unwrap();
            }),
            "a re-staged artifact means the suspend moved on"
        );
        assert!(
            !claim_after(&|conn, id, _| {
                conn.execute(
                    "UPDATE sandboxes SET failure_reason = NULL WHERE id = ?1",
                    crate::db::fixture::params![id],
                )
                .unwrap();
            }),
            "a cleared marker means the row is no longer terminal"
        );
        assert!(
            !claim_after(&|conn, id, _| {
                conn.execute(
                    "UPDATE sandboxes SET state = 'suspended' WHERE id = ?1",
                    crate::db::fixture::params![id],
                )
                .unwrap();
            }),
            "a suspend that published must not be claimed"
        );
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_local_provenance_owners() {
        for state in [SandboxState::Suspended, SandboxState::Quarantined] {
            // The fixture default owner is not a `user:<uuid>` platform user:
            // rows the cluster never minted must survive a deleted report.
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, state);
            let service = service_with_runtime_probe(conn, false);

            assert_evict_refused_and_row_untouched(
                &service,
                &sandbox_id,
                state,
                "not a platform user",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_pool_slots() {
        for state in [SandboxState::Suspended, SandboxState::Quarantined] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_platform_owned_sandbox(&conn, &sandbox_id, state, Some("seed"));
            let service = service_with_runtime_probe(conn, false);

            assert_evict_refused_and_row_untouched(&service, &sandbox_id, state, "pool slot").await;
        }
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_rows_inside_quiescence_window() {
        for (configured, seconds) in [
            (None, 600),
            (Some("0"), 600),
            (Some("invalid"), 600),
            (Some("10"), 10),
        ] {
            for state in [
                SandboxState::Stopped,
                SandboxState::Suspended,
                SandboxState::Quarantined,
            ] {
                // insert stamps updated_at = now, so the row is inside the window.
                let conn = crate::db::test_conn();
                let sandbox_id = SandboxId::new().to_string();
                insert_platform_owned_sandbox(&conn, &sandbox_id, state, None);
                let mut service = service_with_runtime_probe(conn, false);
                service.cluster_deleted_eviction_quiescence =
                    super::eviction::cluster_deleted_eviction_quiescence(configured);

                assert_evict_refused_and_row_untouched(
                    &service,
                    &sandbox_id,
                    state,
                    "quiescence window",
                )
                .await;

                // The refusal carries the remaining window as a retry horizon so
                // node-ctl can come back at quiescence end instead of applying its
                // flat back-off; a row inserted just now has (almost) the whole
                // configured window left.
                let refusal = service
                    .evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id))
                    .await
                    .expect_err("still inside the window");
                let horizon = pplx_space_common_grpc::retry_after(&refusal)
                    .expect("quiescence refusal must carry a retry horizon");
                assert!(
                    horizon > std::time::Duration::from_secs(seconds - 5)
                        && horizon <= std::time::Duration::from_secs(seconds),
                    "horizon should be the remaining window, got {horizon:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_other_refusals_carry_no_retry_horizon() {
        // Only the quiescence refusal has a horizon; a pool slot never
        // becomes evictable by waiting, so node-ctl must keep its flat
        // back-off for it.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Suspended, Some("seed"));
        let service = service_with_runtime_probe(conn, false);

        let refusal = service
            .evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id))
            .await
            .expect_err("pool slot must be refused");
        assert_eq!(refusal.code(), tonic::Code::FailedPrecondition);
        assert!(pplx_space_common_grpc::retry_after(&refusal).is_none());
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_refuses_malformed_updated_at() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Suspended, None);
        conn.execute(
            "UPDATE sandboxes SET updated_at = 'not-a-timestamp' WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        assert_evict_refused_and_row_untouched(
            &service,
            &sandbox_id,
            SandboxState::Suspended,
            "malformed updated_at",
        )
        .await;
    }

    #[tokio::test]
    async fn evict_cluster_deleted_sandbox_without_feature_flags() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_platform_owned_sandbox(&conn, &sandbox_id, SandboxState::Suspended, None);
        conn.execute(
            "UPDATE sandboxes SET updated_at = '0' WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);
        assert!(!service.feature_flags.is_configured());

        // Stub only node-manager dispatch; run the real eviction claim,
        // durable cleanup executor, and database finalization.
        let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&dispatched);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
            observed.lock().unwrap().push((id, kill));
            Box::pin(async { Ok(()) })
        }));

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            service.evict_cluster_deleted_sandbox(root_peer_request(&sandbox_id)),
        )
        .await;
        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(5), service.task_tracker.wait())
            .await
            .expect("eviction and cleanup tasks must drain");
        result
            .expect("eviction should complete")
            .expect("eligible cluster-deleted sandbox should evict without a flag provider");
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec![(sandbox_id.clone(), false)]
        );
        let db = service.db.lock().unwrap();
        assert!(crate::db::get_sandbox(&db, &sandbox_id).unwrap().is_none());
    }

    pub(crate) fn runtime_fields(
        generation: ExecutionGeneration,
        slot: u32,
    ) -> crate::db::RuntimeFields<'static> {
        let route_generation = RouteGeneration::new(u64::from(slot)).unwrap();
        crate::db::RuntimeFields {
            uid: 10000,
            slot,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10001),
            netgtw_pid: 10002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity: pplx_space_common_core::types::SpaceApiRouteIdentity {
                route_generation: Some(route_generation),
                current_node_id: Some(NodeId::from_uuid(uuid::Uuid::from_u128(1))),
                substrate_kind: Some(NodeManagedBy::Space),
                execution_generation: Some(generation),
                activation_attempt_id: Some(SandboxActivationAttemptId::from_uuid(
                    uuid::Uuid::from_u128(u128::from(slot)),
                )),
                ..Default::default()
            },
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        }
    }

    #[tokio::test]
    async fn pending_delete_receipt_reports_runtime_attachment_conflict() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let generation = ExecutionGeneration::new(4).unwrap();
        let receipt = crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
            receipt_id: pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            execution_generation: generation,
            route_activation_id: SandboxActivationAttemptId::new(),
            policy_fingerprint:
                pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                    &AutoLifecyclePolicy::disabled(),
                ),
        };
        crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(&conn, receipt).unwrap();
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .update_runtime_fields(sandbox_id, runtime_fields(generation, 7))
            .expect_err("the pending delete receipt must own the same-generation row");

        assert_eq!(
            error.code(),
            tonic::Code::Aborted,
            "unexpected status: {error:?}"
        );
        assert!(
            error
                .message()
                .contains("pending auto-lifecycle delete receipt")
        );
    }

    #[test]
    fn cleanup_recovery_rejects_a_receipt_for_a_replaced_route_attempt() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let generation = ExecutionGeneration::new(4).unwrap();
        let receipt_attempt = SandboxActivationAttemptId::new();
        let replacement_attempt = SandboxActivationAttemptId::new_after(receipt_attempt).unwrap();
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                route_generation: Some(RouteGeneration::new(4).unwrap()),
                current_node_id: Some(NodeId::new()),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                execution_generation: Some(generation),
                pod_uid: Some(PodUid::parse("replacement-pod").unwrap()),
                activation_attempt_id: Some(replacement_attempt),
                ..SpaceApiRouteIdentity::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(
            &conn,
            crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
                receipt_id: pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
                sandbox_id,
                execution_generation: generation,
                route_activation_id: receipt_attempt,
                policy_fingerprint:
                    pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                        &AutoLifecyclePolicy::disabled(),
                    ),
            },
        )
        .unwrap();
        conn.execute(
            "UPDATE sandboxes
                SET state = 'deleting',
                    cleanup_completion = 'delete_row',
                    cleanup_keep_filesystem = 0
              WHERE id = ?1",
            [sandbox_id.to_string()],
        )
        .unwrap();

        assert!(
            !SandboxManagerService::cleanup_attempt_still_current(
                &crate::db::SharedConnection::new(conn),
                &sandbox_id.to_string(),
                CleanupCompletion::DeleteRow,
            )
            .unwrap(),
            "recovery must not dispatch a stale receipt against a replacement attempt"
        );
    }

    #[test]
    fn restore_success_cas_emits_completed_event() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        assert!(
            crate::db::mark_restore_succeeded(&conn, &sandbox_id, SandboxState::Paused, None)
                .unwrap()
        );
        drop(guard);

        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "restore_completed")
            })
            .expect("successful restore CAS must be visible in the sandbox timeline");
        assert_eq!(event.get("source").map(String::as_str), Some("sbx_manager"));
        assert_eq!(event.get("trigger").map(String::as_str), Some("system"));
        assert_eq!(
            event.get("from_state").map(String::as_str),
            Some("restoring")
        );
        assert_eq!(event.get("to_state").map(String::as_str), Some("paused"));
    }

    #[test]
    fn cleanup_episode_failure_emits_the_cleanup_failed_transition() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let cleanup_id = SandboxCleanupEpisodeId::new();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::CleanupRequired);
        conn.execute(
            "UPDATE sandboxes
                SET cleanup_completion = 'mark_failed',
                    cleanup_keep_filesystem = TRUE
              WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sandbox_cleanup_episodes (
                 sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                 entered_from_state, activation_state, requested_at,
                 quarantine_reason_code, failure_message, created_at
             ) VALUES (?1, ?2, 'mark_failed', TRUE, 'requested', 'restoring',
                       'restoring', 1, 'restore_prepare_failed', 'cleanup requested', 1)",
            crate::db::fixture::params![sandbox_id, cleanup_id.to_string()],
        )
        .unwrap();
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        assert!(
            crate::db::record_cleanup_episode_failure(
                &conn,
                &sandbox_id,
                cleanup_id,
                "node cleanup timed out",
            )
            .unwrap()
        );
        drop(guard);

        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "cleanup_failed")
            })
            .expect("cleanup_failed state transition must be visible in the sandbox timeline");
        assert_eq!(
            event.get("from_state").map(String::as_str),
            Some("cleanup_required")
        );
        assert_eq!(
            event.get("to_state").map(String::as_str),
            Some("cleanup_failed")
        );
        assert_eq!(
            event.get("error").map(String::as_str),
            Some("node cleanup timed out")
        );

        for attempt in 2..=3 {
            assert!(
                crate::db::record_cleanup_episode_failure(
                    &conn,
                    &sandbox_id,
                    cleanup_id,
                    &format!("node cleanup attempt {attempt} timed out"),
                )
                .unwrap()
            );
        }
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupFailed);
        assert!(crate::db::cleanup_episode_current(&conn, &sandbox_id, cleanup_id).unwrap());
        assert_eq!(
            crate::db::cleanup_episode(&conn, &sandbox_id)
                .unwrap()
                .unwrap()
                .attempt_count,
            3,
            "repeated operational failures remain durable retry work"
        );
    }

    #[tokio::test]
    async fn blocking_kill_cleanup_failure_persists_retryable_stopped_intent() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let typed_sandbox_id = SandboxId::new();
        let sandbox_id = typed_sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopping);
        let service = SandboxManagerService::for_test(conn);
        // Park the retry attempt at node dispatch so its start is a signal and
        // the row it would rewrite stays as the failure persisted it.
        let (started, mut starts) = tokio::sync::mpsc::channel(1);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, _| {
            let started = started.clone();
            Box::pin(async move {
                started.send(id).await.unwrap();
                std::future::pending().await
            })
        }));
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        service.persist_and_retry_cleanup_failure(
            typed_sandbox_id,
            SandboxRuntimeLabel::from_sandbox_id(typed_sandbox_id),
            true,
            CleanupCompletion::MarkStopped,
            "node cleanup timed out",
        );
        drop(guard);

        let dispatched = tokio::time::timeout(Duration::from_secs(30), starts.recv())
            .await
            .expect("blocking cleanup failure must start a live retry runner")
            .unwrap();
        assert_eq!(dispatched, sandbox_id);
        assert!(service.cleanup_executor.active.contains_key(&sandbox_id));

        let conn = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupFailed);
        assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
        assert_eq!(row.cleanup_keep_filesystem, Some(true));
        assert_eq!(
            crate::db::cleanup_intent_for_startup_recovery(&row),
            Some((true, CleanupCompletion::MarkStopped))
        );
        assert_eq!(
            row.failure_reason.as_deref(),
            Some("node cleanup timed out")
        );
        assert_eq!(crate::db::list_node_dirty_refs(&conn, 10).unwrap().len(), 1);

        let events = capture.events.lock().unwrap();
        assert!(events.iter().any(|event| {
            event
                .get("event_type")
                .is_some_and(|value| value == "cleanup_failed")
                && event
                    .get("from_state")
                    .is_some_and(|value| value == "stopping")
                && event
                    .get("to_state")
                    .is_some_and(|value| value == "cleanup_failed")
        }));
        assert!(!events.iter().any(|event| {
            event
                .get("event_type")
                .is_some_and(|value| value == "sandbox_failed")
        }));
    }

    /// A service whose runtime probe answers `runtime_live` for every sandbox
    /// instead of reading the host's run directory.
    pub(crate) fn service_with_runtime_probe(
        conn: crate::db::Connection,
        runtime_live: bool,
    ) -> SandboxManagerService {
        SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                sandbox_runtime_live: Arc::new(move |_| runtime_live),
                ..SandboxManagerServiceOptions::for_test()
            },
        )
    }

    /// Teardown tests compare route identities against this node's identity,
    /// [`crate::service::test_options::test_node_id`].
    pub(crate) fn service_for_teardown_test(conn: crate::db::Connection) -> SandboxManagerService {
        SandboxManagerService::for_test(conn)
    }

    fn route_identity(
        sandbox_id: SandboxId,
        route_generation: u64,
        activation_attempt_id: SandboxActivationAttemptId,
    ) -> SandboxRouteIdentity {
        SandboxRouteIdentity::try_new(
            sandbox_id,
            RouteGeneration::new(route_generation).unwrap(),
            NodeId::new(),
            NodeManagedBy::Space,
            ExecutionGeneration::new(i64::try_from(route_generation).unwrap()).unwrap(),
            activation_attempt_id,
            None,
        )
        .unwrap()
    }

    fn direct_route_identity_proto(generation: i64) -> crate::proto::SpaceApiRouteIdentity {
        crate::proto::SpaceApiRouteIdentity {
            route_generation: u64::try_from(generation).ok(),
            current_node_id: Some("019f8000-0000-7000-8000-0000000000fe".to_string()),
            substrate_kind: crate::proto::SandboxSubstrateKind::Direct as i32,
            execution_generation: Some(generation),
            activation_attempt_id: Some(SandboxActivationAttemptId::new().to_string()),
            ..Default::default()
        }
    }

    pub(crate) fn prepare_teardown_suspend_request(
        sandbox_id: impl Into<String>,
        execution_generation: i64,
        action: TeardownSuspendAction,
    ) -> PrepareTeardownSuspendRequest {
        PrepareTeardownSuspendRequest {
            sandbox_id: sandbox_id.into(),
            execution_generation,
            action: action as i32,
        }
    }

    /// `StartRequest` for the checkpoint/base-snapshot restore path, with
    /// every other field at its wire default.
    fn checkpoint_start_request(
        sandbox_id: impl Into<String>,
        checkpoint: &str,
        route_identity: Option<crate::proto::SpaceApiRouteIdentity>,
    ) -> StartRequest {
        StartRequest {
            activation_auto_lifecycle: None,
            operation_activation_attempt_id: None,
            sandbox_id: sandbox_id.into(),
            vcpu: None,
            memory_mib: None,
            checkpoint: Some(checkpoint.to_string()),
            from_warm: false,
            blocking: false,
            netns_path: None,
            ready_timeout_secs: None,
            target_state: 0,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            route_identity,
            restore_artifact_as_new: None,
            expected_local_artifact_id: None,
        }
    }

    #[tokio::test]
    async fn authoritative_route_identity_replacement_fences_only_older_stream_epoch() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);
        let sandbox_id = SandboxId::new();
        let current = route_identity(sandbox_id, 1, SandboxActivationAttemptId::new());

        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(current.clone()),
            );
        }
        let old_stream = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .unwrap()
            .fence
            .child();

        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(current.clone()),
            );
        }
        assert!(!old_stream.is_cancelled());

        let replacement = route_identity(sandbox_id, 2, SandboxActivationAttemptId::new());
        validate_authoritative_route_epoch_transition(
            &RouteStreamEpochIdentity(current),
            &RouteStreamEpochIdentity(replacement.clone()),
            ActivationRouteReplacementAuthority::Unowned,
        )
        .unwrap();
        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(replacement.clone()),
            );
        }
        old_stream.cancelled().await;

        let new_stream = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .unwrap()
            .fence
            .child();
        let retry_attempt = SandboxRouteIdentity::try_new(
            sandbox_id,
            replacement.route_generation(),
            replacement.current_node_id(),
            replacement.substrate_kind(),
            replacement.execution_generation(),
            SandboxActivationAttemptId::new(),
            None,
        )
        .unwrap();
        validate_authoritative_route_epoch_transition(
            &RouteStreamEpochIdentity(replacement),
            &RouteStreamEpochIdentity(retry_attempt.clone()),
            ActivationRouteReplacementAuthority::PersistedOwner,
        )
        .expect("activation-owner retry may replace an attempt at the same generation");
        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(retry_attempt.clone()),
            );
        }
        new_stream.cancelled().await;
        let retry_stream = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .unwrap()
            .fence
            .child();

        service
            .cancel_route_streams_for_generation(
                sandbox_id,
                Some(ExecutionGeneration::new(1).unwrap()),
            )
            .unwrap();
        assert!(!retry_stream.is_cancelled());
        service
            .cancel_route_streams_for_generation(
                sandbox_id,
                Some(retry_attempt.execution_generation()),
            )
            .unwrap();
        retry_stream.cancelled().await;
    }

    #[tokio::test]
    async fn service_port_ingress_admission_is_stable_across_identity_epochs() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);
        let sandbox_id = SandboxId::new();
        let current = route_identity(sandbox_id, 1, SandboxActivationAttemptId::new());

        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(current),
            );
        }
        let mut admissions = (0
            ..pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS)
            .map(|_| {
                service
                    .service_port_ingress_admission
                    .try_acquire(sandbox_id)
                    .expect("ingress slot")
            })
            .collect::<Vec<_>>();
        assert!(
            service
                .service_port_ingress_admission
                .try_acquire(sandbox_id)
                .is_err()
        );

        drop(admissions.pop());
        let released_slot = service
            .service_port_ingress_admission
            .try_acquire(sandbox_id)
            .expect("stream drop releases ingress admission");

        let replacement = route_identity(sandbox_id, 2, SandboxActivationAttemptId::new());
        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(replacement),
            );
        }
        assert_eq!(
            service
                .service_port_ingress_admission
                .available_permits(sandbox_id),
            0,
            "rotating the identity fence must not manufacture new process capacity"
        );
        assert!(
            service
                .service_port_ingress_admission
                .try_acquire(sandbox_id)
                .is_err()
        );

        drop(released_slot);
        drop(admissions);
        assert_eq!(
            service
                .service_port_ingress_admission
                .available_permits(sandbox_id),
            pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS
        );
        assert_eq!(
            service.service_port_ingress_admission.tracked_sandboxes(),
            0,
            "idle sandbox admission entries must not accumulate"
        );
    }

    #[tokio::test]
    async fn unknown_cleanup_generation_cancels_current_stream_epoch() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);
        let sandbox_id = SandboxId::new();
        let identity = route_identity(sandbox_id, 1, SandboxActivationAttemptId::new());

        {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(
                &mut epochs,
                sandbox_id,
                RouteStreamEpochIdentity(identity),
            );
        }
        let stream = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .unwrap()
            .fence
            .child();

        service
            .cancel_route_streams_for_generation(sandbox_id, None)
            .expect("unknown generation must conservatively cancel the current epoch");

        assert!(stream.is_cancelled());
        assert!(
            !service
                .route_stream_epochs
                .lock()
                .unwrap()
                .contains_key(&sandbox_id)
        );
    }

    #[tokio::test]
    async fn runtime_update_publishes_and_fences_complete_wire_identity() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        let node_id = NodeId::new();
        let mut current = runtime_fields(ExecutionGeneration::new(1).unwrap(), 7);
        current.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(node_id),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..Default::default()
        };
        service.update_runtime_fields(sandbox_id, current).unwrap();
        let old_stream = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .expect("runtime update publishes its route epoch")
            .fence
            .child();

        let mut replacement = runtime_fields(ExecutionGeneration::new(2).unwrap(), 8);
        replacement.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(2).unwrap()),
            current_node_id: Some(node_id),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(2).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..Default::default()
        };
        service
            .update_runtime_fields(sandbox_id, replacement)
            .unwrap();

        old_stream.cancelled().await;
    }

    #[tokio::test]
    async fn owned_restore_replaces_legacy_suspended_publication_identity() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = service_for_teardown_test(conn);
        let node_id = NodeId::new();
        let generation_1 = ExecutionGeneration::new(1).unwrap();
        let pod_uid_1 = PodUid::parse(uuid::Uuid::now_v7().to_string()).unwrap();
        let mut current = runtime_fields(generation_1, 1);
        current.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(node_id),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(generation_1),
            pod_uid: Some(pod_uid_1.clone()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..Default::default()
        };
        service.update_runtime_fields(sandbox_id, current).unwrap();

        {
            let conn = service.db_conn().unwrap();
            crate::db::begin_suspend(&conn, &sandbox_id.to_string()).unwrap();
            crate::db::complete_row_intent_cleanup(
                &conn,
                &sandbox_id.to_string(),
                crate::db::CleanupCompletion::MarkSuspended,
                Some((LocalArtifactId::new(), "/cache/suspended-publication")),
            )
            .unwrap();
            let suspended = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert_eq!(
                suspended.space_api_route_identity.execution_generation,
                Some(generation_1)
            );
            assert_eq!(suspended.space_api_route_identity.pod_uid, Some(pod_uid_1));
            assert!(
                suspended
                    .space_api_route_identity
                    .route_generation
                    .is_none()
            );
            assert!(suspended.space_api_route_identity.current_node_id.is_none());
            assert_eq!(
                suspended.space_api_route_identity.substrate_kind,
                Some(NodeManagedBy::Kubernetes)
            );
            assert!(
                suspended
                    .space_api_route_identity
                    .activation_attempt_id
                    .is_none()
            );
            assert_eq!(suspended.state, SandboxState::Suspended);
            assert!(suspended.current_local_artifact_id.is_some());
            assert_eq!(
                suspended.current_local_artifact_kind,
                Some(ArtifactKind::Full)
            );
            assert_eq!(
                suspended.current_local_artifact_origin,
                Some(ArtifactOrigin::Suspend)
            );
            assert!(
                persisted_route_stream_epoch(
                    sandbox_id,
                    &suspended,
                    ActivationRouteReplacementAuthority::Unowned,
                )
                .unwrap()
                .is_none()
            );

            conn.execute(
                "UPDATE sandboxes
                 SET current_local_artifact_id = NULL,
                     current_local_artifact_kind = NULL,
                     current_local_artifact_origin = NULL
                 WHERE id = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
            )
            .unwrap();
            let legacy = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert_eq!(
                persisted_route_stream_epoch(
                    sandbox_id,
                    &legacy,
                    ActivationRouteReplacementAuthority::Unowned,
                )
                .unwrap_err()
                .code(),
                tonic::Code::FailedPrecondition,
                "an unowned activation must not bypass strict identity validation",
            );
        }
        service
            .route_stream_epochs
            .lock()
            .unwrap()
            .remove(&sandbox_id);

        let generation_2 = ExecutionGeneration::new(2).unwrap();
        let activation_attempt_2 = SandboxActivationAttemptId::new();
        let cleanup_id = {
            let conn = service.db_conn().unwrap();
            let cleanup_id = crate::db::begin_activation(
                &conn,
                &sandbox_id.to_string(),
                SandboxState::Suspended,
                crate::db::ActivationKind::Restore,
                Some(generation_2),
                Some(activation_attempt_2),
            )
            .unwrap()
            .expect("restore must claim the suspended row before publishing runtime fields")
            .id;
            assert_eq!(
                crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                    .unwrap()
                    .unwrap()
                    .state,
                SandboxState::Restoring
            );
            cleanup_id
        };

        let mut restored = runtime_fields(generation_2, 2);
        restored.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(2).unwrap()),
            current_node_id: Some(node_id),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(generation_2),
            pod_uid: Some(PodUid::parse(uuid::Uuid::now_v7().to_string()).unwrap()),
            activation_attempt_id: Some(activation_attempt_2),
            ..Default::default()
        };

        service
            .update_runtime_fields_for_activation(sandbox_id, cleanup_id, restored)
            .expect("a complete newer execution must replace publication-only identity");
    }

    #[tokio::test]
    async fn recovered_execution_provenance_requires_owned_complete_restore() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        #[derive(Clone, Copy, Debug)]
        enum Case {
            NewerExecution,
            DirectExecution,
            SameExecution,
            OlderExecution,
            WrongCleanupOwner,
            WrongAttempt,
            IncompleteIncoming,
            MissingArtifact,
            PriorNodeIdentity,
        }

        for case in [
            Case::NewerExecution,
            Case::DirectExecution,
            Case::SameExecution,
            Case::OlderExecution,
            Case::WrongCleanupOwner,
            Case::WrongAttempt,
            Case::IncompleteIncoming,
            Case::MissingArtifact,
            Case::PriorNodeIdentity,
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
                &conn,
                SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
            );
            let candidate = crate::db::scan_quarantine_recovery_candidates(&conn)
                .unwrap()
                .candidates
                .into_iter()
                .find(|candidate| candidate.sandbox_id == sandbox_id)
                .unwrap();
            let claimed = crate::db::claim_quarantine_recovery_candidate(
                &conn,
                &candidate,
                crate::db::PlanRejectionRunEffect::Leaves,
            )
            .unwrap()
            .unwrap();
            // The staging transfer has recreated the orphan's row. Exercise
            // the actual recovery writers before attempting runtime publication.
            conn.execute(
                "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
                crate::db::fixture::params![sandbox_id.to_string()],
            )
            .unwrap();
            let producing_generation = ExecutionGeneration::new(7).unwrap();
            assert!(
                crate::db::set_orphan_quarantine_recovery_generation(
                    &conn,
                    &claimed,
                    producing_generation,
                )
                .unwrap()
            );
            let artifact_id = LocalArtifactId::new();
            assert!(
                crate::db::publish_orphan_quarantine_recovered_artifact(
                    &conn,
                    &claimed,
                    Some(artifact_id),
                    "/space/cache/sandboxes/test/snapshot",
                    producing_generation,
                    SandboxCleanupEpisodeId::new(),
                )
                .unwrap()
            );

            let generation = ExecutionGeneration::new(match case {
                Case::SameExecution => 7,
                Case::OlderExecution => 6,
                _ => 8,
            })
            .unwrap();
            let attempt = SandboxActivationAttemptId::new();
            let mut cleanup_id = crate::db::begin_activation(
                &conn,
                &sandbox_id.to_string(),
                SandboxState::Suspended,
                crate::db::ActivationKind::Restore,
                Some(generation),
                Some(attempt),
            )
            .unwrap()
            .unwrap()
            .id;
            let mut restored = runtime_fields(generation, 8);
            restored.space_api_route_identity = SpaceApiRouteIdentity {
                route_generation: Some(
                    RouteGeneration::new(u64::try_from(generation.get()).unwrap()).unwrap(),
                ),
                current_node_id: Some(NodeId::new()),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                execution_generation: Some(generation),
                pod_uid: Some(PodUid::parse(uuid::Uuid::now_v7().to_string()).unwrap()),
                activation_attempt_id: Some(attempt),
                ..Default::default()
            };
            let expected_error = match case {
                Case::NewerExecution | Case::SameExecution => None,
                Case::DirectExecution => {
                    restored.space_api_route_identity.substrate_kind = Some(NodeManagedBy::Space);
                    restored.space_api_route_identity.pod_uid = None;
                    None
                }
                Case::OlderExecution => Some(tonic::Code::Aborted),
                Case::WrongCleanupOwner => {
                    cleanup_id = SandboxCleanupEpisodeId::new();
                    Some(tonic::Code::FailedPrecondition)
                }
                Case::WrongAttempt => {
                    restored.space_api_route_identity.activation_attempt_id =
                        Some(SandboxActivationAttemptId::new());
                    Some(tonic::Code::FailedPrecondition)
                }
                Case::IncompleteIncoming => {
                    restored.space_api_route_identity.route_generation = None;
                    Some(tonic::Code::FailedPrecondition)
                }
                Case::MissingArtifact => {
                    conn.execute(
                        "UPDATE sandboxes SET current_local_artifact_id = NULL,
                         current_local_artifact_kind = NULL, current_local_artifact_origin = NULL
                         WHERE id = ?1",
                        crate::db::fixture::params![sandbox_id.to_string()],
                    )
                    .unwrap();
                    Some(tonic::Code::FailedPrecondition)
                }
                Case::PriorNodeIdentity => {
                    conn.execute(
                        "UPDATE sandboxes SET space_api_route_node_id = ?1 WHERE id = ?2",
                        crate::db::fixture::params![
                            NodeId::new().to_string(),
                            sandbox_id.to_string()
                        ],
                    )
                    .unwrap();
                    Some(tonic::Code::FailedPrecondition)
                }
            };
            let prior = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            // A new service has no cached route epoch, as after a manager restart.
            let service = service_for_teardown_test(conn);
            let mut expected_identity = restored.space_api_route_identity.clone();
            expected_identity.sandbox_id = Some(sandbox_id);
            let result =
                service.update_runtime_fields_for_activation(sandbox_id, cleanup_id, restored);
            let conn = service.db_conn().unwrap();
            let current = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            let epochs = service.route_stream_epochs.lock().unwrap();
            if let Some(code) = expected_error {
                assert_eq!(result.unwrap_err().code(), code, "{case:?}");
                assert_eq!(
                    current.space_api_route_identity, prior.space_api_route_identity,
                    "{case:?}"
                );
                assert_eq!(current.pid, prior.pid, "{case:?}");
                assert!(!epochs.contains_key(&sandbox_id), "{case:?}");
            } else {
                result.unwrap_or_else(|error| panic!("{case:?}: {error}"));
                assert_eq!(
                    current.space_api_route_identity, expected_identity,
                    "{case:?}"
                );
                assert_eq!(
                    epochs.get(&sandbox_id).unwrap().identity.wire_identity(),
                    expected_identity,
                    "{case:?}"
                );
                assert_eq!(current.current_local_artifact_id, Some(artifact_id));
            }
        }
    }

    #[tokio::test]
    async fn conflicting_route_epoch_is_rejected_before_runtime_row_commit() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        let activation_attempt_id = SandboxActivationAttemptId::new();
        let current_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(activation_attempt_id),
            ..Default::default()
        };
        let mut current = runtime_fields(ExecutionGeneration::new(1).unwrap(), 7);
        current.space_api_route_identity = current_identity.clone();
        service.update_runtime_fields(sandbox_id, current).unwrap();

        let mut conflicting = runtime_fields(ExecutionGeneration::new(1).unwrap(), 99);
        conflicting.space_api_route_identity = SpaceApiRouteIdentity {
            current_node_id: Some(NodeId::new()),
            ..current_identity
        };
        service
            .update_runtime_fields(sandbox_id, conflicting)
            .expect_err("same-attempt route conflicts reject before persistence");

        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(row.slot, Some(7));
        drop(db);
        let epoch = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .expect("original route epoch remains installed")
            .identity
            .wire_identity();
        assert_eq!(epoch.current_node_id, current_identity.current_node_id);
    }

    #[tokio::test]
    async fn higher_route_generation_requires_a_coherent_execution_replacement() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        let current_node_id = NodeId::new();
        let current_attempt_id = SandboxActivationAttemptId::new();
        let current_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(2).unwrap()),
            current_node_id: Some(current_node_id),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(2).unwrap()),
            activation_attempt_id: Some(current_attempt_id),
            ..Default::default()
        };
        let persisted_current_identity = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            ..current_identity.clone()
        };
        let mut current = runtime_fields(ExecutionGeneration::new(2).unwrap(), 7);
        current.space_api_route_identity = current_identity.clone();
        service.update_runtime_fields(sandbox_id, current).unwrap();
        assert_eq!(
            service.network_policy_reconcile_queue.drain_pending(),
            [sandbox_id.to_string()].into_iter().collect(),
            "a newly published runtime queues its gateway for current-key propagation"
        );
        let current_epoch = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .expect("current route epoch is installed")
            .clone();
        let current_stream = current_epoch.fence.child();

        let lower_execution = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(3).unwrap()),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            ..current_identity.clone()
        };
        let lower_execution =
            RouteStreamEpochIdentity::from_runtime_update(sandbox_id, lower_execution)
                .unwrap()
                .unwrap();
        let error = validate_authoritative_route_epoch_transition(
            &current_epoch.identity,
            &lower_execution,
            ActivationRouteReplacementAuthority::Unowned,
        )
        .expect_err("a higher route generation cannot hide an older execution generation");
        assert!(
            error
                .message()
                .contains("execution generation is older than the published stream epoch")
        );

        let replacement_node_id = NodeId::new();
        let replacement_attempt_id = SandboxActivationAttemptId::new();
        let mut incoherent = runtime_fields(ExecutionGeneration::new(2).unwrap(), 99);
        incoherent.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(3).unwrap()),
            current_node_id: Some(replacement_node_id),
            activation_attempt_id: Some(replacement_attempt_id),
            ..current_identity.clone()
        };
        service
            .update_runtime_fields(sandbox_id, incoherent)
            .expect_err("a route generation alone cannot replace execution identity");

        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.slot, Some(7));
        assert_eq!(row.space_api_route_identity, persisted_current_identity);
        let retained_epoch = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .expect("rejected update retains the current route epoch")
            .identity
            .wire_identity();
        assert_eq!(retained_epoch, persisted_current_identity);
        assert!(!current_stream.is_cancelled());
        assert!(
            service
                .network_policy_reconcile_queue
                .drain_pending()
                .is_empty(),
            "a rejected runtime replacement must not queue gateway propagation"
        );

        let coherent_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(3).unwrap()),
            current_node_id: Some(replacement_node_id),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(3).unwrap()),
            activation_attempt_id: Some(replacement_attempt_id),
            ..Default::default()
        };
        let persisted_coherent_identity = SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            ..coherent_identity.clone()
        };
        let mut coherent = runtime_fields(ExecutionGeneration::new(3).unwrap(), 8);
        coherent.space_api_route_identity = coherent_identity.clone();
        service
            .update_runtime_fields(sandbox_id, coherent)
            .expect("a coherent route and execution replacement is accepted");
        assert_eq!(
            service.network_policy_reconcile_queue.drain_pending(),
            [sandbox_id.to_string()].into_iter().collect(),
            "an accepted replacement queues the new gateway for current-key propagation"
        );

        current_stream.cancelled().await;
        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.slot, Some(8));
        assert_eq!(row.space_api_route_identity, persisted_coherent_identity);
    }

    #[test]
    fn persisted_activation_owner_cannot_change_substrate_at_same_generation() {
        let sandbox_id = SandboxId::new();
        let current = RouteStreamEpochIdentity(
            pplx_space_common_core::types::SandboxRouteIdentity::try_new(
                sandbox_id,
                RouteGeneration::new(1).unwrap(),
                NodeId::new(),
                NodeManagedBy::Kubernetes,
                ExecutionGeneration::new(1).unwrap(),
                SandboxActivationAttemptId::new(),
                Some("019f8000-0000-7000-8000-0000000000cc".parse().unwrap()),
            )
            .unwrap(),
        );
        let replacement = RouteStreamEpochIdentity(
            pplx_space_common_core::types::SandboxRouteIdentity::try_new(
                sandbox_id,
                RouteGeneration::new(1).unwrap(),
                NodeId::new(),
                NodeManagedBy::Space,
                ExecutionGeneration::new(1).unwrap(),
                SandboxActivationAttemptId::new(),
                None,
            )
            .unwrap(),
        );

        validate_authoritative_route_epoch_transition(
            &current,
            &replacement,
            ActivationRouteReplacementAuthority::PersistedOwner,
        )
        .expect_err("activation ownership cannot change the runtime substrate");
    }

    #[tokio::test]
    async fn same_generation_activation_replacement_requires_persisted_ownership() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        let node_id = NodeId::new();
        let current_attempt = SandboxActivationAttemptId::new();
        let mut current = runtime_fields(ExecutionGeneration::new(1).unwrap(), 7);
        current.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(node_id),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(current_attempt),
            ..Default::default()
        };
        service.update_runtime_fields(sandbox_id, current).unwrap();

        let replacement_attempt = SandboxActivationAttemptId::new();
        let replacement_node = NodeId::new();
        let mut replacement = runtime_fields(ExecutionGeneration::new(1).unwrap(), 8);
        replacement.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(replacement_node),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(replacement_attempt),
            ..Default::default()
        };
        service
            .update_runtime_fields(sandbox_id, replacement)
            .expect_err("a new attempt cannot replace a route epoch without activation ownership");

        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.slot, Some(7));
        assert_eq!(
            row.space_api_route_identity.activation_attempt_id,
            Some(current_attempt)
        );

        let old_stream = service
            .route_stream_epochs
            .lock()
            .unwrap()
            .get(&sandbox_id)
            .unwrap()
            .fence
            .child();
        let cleanup_id = {
            let db = service.db.lock().unwrap();
            crate::db::begin_activation(
                &db,
                &sandbox_id.to_string(),
                SandboxState::Stopped,
                crate::db::ActivationKind::ColdStart,
                Some(ExecutionGeneration::new(1).unwrap()),
                Some(replacement_attempt),
            )
            .unwrap()
            .expect("replacement activation owns a persisted episode")
            .id
        };
        {
            let db = service.db.lock().unwrap();
            let sandbox = crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert_eq!(
                sandbox.space_api_route_identity.activation_attempt_id,
                Some(current_attempt)
            );
            assert_ne!(
                sandbox.space_api_route_identity.activation_attempt_id,
                Some(replacement_attempt),
                "installed-route equality is not evidence authorizing the pending replacement"
            );
        }
        let mut wrongly_owned_replacement = runtime_fields(ExecutionGeneration::new(1).unwrap(), 8);
        wrongly_owned_replacement.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(replacement_node),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(replacement_attempt),
            ..Default::default()
        };
        service
            .update_runtime_fields_for_activation(
                sandbox_id,
                SandboxCleanupEpisodeId::new(),
                wrongly_owned_replacement,
            )
            .expect_err("a different cleanup episode cannot authorize the replacement");
        assert!(!old_stream.is_cancelled());
        let mut owned_replacement = runtime_fields(ExecutionGeneration::new(1).unwrap(), 8);
        owned_replacement.space_api_route_identity = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(1).unwrap()),
            current_node_id: Some(replacement_node),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(replacement_attempt),
            ..Default::default()
        };
        service
            .update_runtime_fields_for_activation(sandbox_id, cleanup_id, owned_replacement)
            .expect("the persisted activation owner may replace its route identity");
        old_stream.cancelled().await;
        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.slot, Some(8));
        assert_eq!(
            row.space_api_route_identity.activation_attempt_id,
            Some(replacement_attempt)
        );
    }

    #[tokio::test]
    async fn teardown_suspend_returns_pending_for_a_missing_sandbox() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let mut service = service_for_teardown_test(conn);
        let node_id = NodeId::new();
        service.local_node_id = Some(node_id);
        let sandbox_id = SandboxId::new();
        let capture = SpanFieldCapture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        // Exercise the production span declaration as well as the handler: a
        // record() call silently drops fields absent from the span's metadata.
        let traced = tower::Layer::layer(
            &pplx_space_common_svc::bootstrap::grpc_trace_layer(),
            tower::service_fn(move |_: http::Request<tonic::body::Body>| {
                let service = service.clone();
                async move {
                    let response = service
                        .prepare_teardown_suspend(internal_request(
                            prepare_teardown_suspend_request(
                                sandbox_id.to_string(),
                                1,
                                TeardownSuspendAction::ObserveOnly,
                            ),
                        ))
                        .await
                        .unwrap()
                        .into_inner();
                    assert_eq!(
                        TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
                        TeardownSuspendOutcome::Pending,
                        "an absent local sandbox cannot prove teardown durability",
                    );
                    Ok::<_, std::convert::Infallible>(http::Response::new(
                        tonic::body::Body::default(),
                    ))
                }
            }),
        );
        tower::ServiceExt::oneshot(
            traced,
            http::Request::builder()
                .uri("/space.sandbox.manager.v1.SandboxManager/PrepareTeardownSuspend")
                .body(tonic::body::Body::default())
                .unwrap(),
        )
        .await
        .unwrap();
        let fields = capture.fields.lock().unwrap();
        assert_eq!(fields["sandbox_id"], sandbox_id.to_string());
        assert_eq!(fields["execution_generation"], "1");
        assert_eq!(fields["current_node_id"], node_id.to_string());
        assert_eq!(
            fields["grpc.response.outcome"],
            "TEARDOWN_SUSPEND_OUTCOME_PENDING"
        );
        assert_eq!(
            fields["grpc.response.reason"],
            "sandbox record is not present on this node"
        );
    }

    #[tokio::test]
    async fn teardown_suspend_does_not_start_without_a_local_generation_lease() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_started_test_sandbox(&conn, &sandbox_id.to_string());
        let service = service_for_teardown_test(conn);

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                1,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
        assert!(
            service.teardown_suspend_drivers.is_empty(),
            "a pre-migration runtime must be fenced before starting teardown"
        );
    }

    #[tokio::test]
    async fn runtime_attachment_generation_is_monotonic_before_teardown() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);

        let service = service_for_teardown_test(conn);
        let current_generation = ExecutionGeneration::new(2).unwrap();

        service
            .update_runtime_fields(sandbox_id, runtime_fields(current_generation, 8))
            .unwrap();
        let error = service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(1).unwrap(), 9),
            )
            .expect_err("a delayed runtime attachment must not lower the live generation");

        assert_eq!(error.code(), tonic::Code::Aborted);
        {
            let db = service.db.lock().expect("mutex poisoned");
            assert_eq!(
                crate::db::runtime_execution_generation(&db, &sandbox_id.to_string()).unwrap(),
                Some(current_generation)
            );
            let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
                .unwrap()
                .unwrap();
            assert_eq!(row.slot, Some(8));
        }

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                1,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Superseded,
            "the delayed teardown poll must detach from the newer live execution",
        );
    }

    #[tokio::test]
    async fn runtime_attachment_generation_survives_manager_restart() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("sandbox-manager.db");
        let sandbox_id = SandboxId::new();
        let current_generation = ExecutionGeneration::new(2).unwrap();

        {
            let mut conn = crate::db::Connection::open_at(&db_path).unwrap();
            crate::db::create_test_schema(&mut conn);
            insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
            let service = service_for_teardown_test(conn);
            service
                .update_runtime_fields(sandbox_id, runtime_fields(current_generation, 8))
                .unwrap();
        }

        let service = service_for_teardown_test(crate::db::Connection::open_at(&db_path).unwrap());
        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                1,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Superseded,
            "a delayed teardown must not claim a pre-restart execution generation",
        );
    }

    #[test]
    fn stopped_provenance_requires_new_generation_and_owned_reconstruction() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let mut row = crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        row.space_api_route_identity = SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(2).unwrap()),
            ..Default::default()
        };
        use SandboxState::*;
        for state in [
            Creating,
            Created,
            Starting,
            Started,
            Stopping,
            Stopped,
            Deleting,
            Pausing,
            Paused,
            Resuming,
            Checkpointing,
            Suspending,
            Suspended,
            Restoring,
            CleanupRequired,
            CleanupFailed,
            Quarantined,
            Failed,
        ] {
            row.state = state;
            for generation in [1, 2, 3] {
                let incoming = RouteStreamEpochIdentity::from_runtime_update(
                    sandbox_id,
                    runtime_fields(ExecutionGeneration::new(generation).unwrap(), 8)
                        .space_api_route_identity,
                )
                .unwrap()
                .unwrap();
                let newer = state != SandboxState::Starting || generation > 2;
                assert_eq!(
                    validate_cold_start_provenance(&row, &incoming).is_ok(),
                    newer,
                    "{state:?} {generation}"
                );
                for authority in [
                    ActivationRouteReplacementAuthority::PersistedOwner,
                    ActivationRouteReplacementAuthority::Unowned,
                ] {
                    let reconstructed = validate_cold_start_provenance(&row, &incoming)
                        .and_then(|()| persisted_route_stream_epoch(sandbox_id, &row, authority));
                    let allowed = state == SandboxState::Starting
                        && generation > 2
                        && authority == ActivationRouteReplacementAuthority::PersistedOwner;
                    assert_eq!(
                        reconstructed.is_ok(),
                        allowed,
                        "{state:?} {generation} {authority:?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn reconstructed_cold_activation_cannot_reuse_stopped_generation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = service_for_teardown_test(conn);
        let generation = ExecutionGeneration::new(2).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(generation, 7))
            .unwrap();
        {
            let db = service.db_conn().unwrap();
            crate::db::mark_cleanup_required(
                &db,
                &sandbox_id.to_string(),
                true,
                CleanupCompletion::MarkStopped,
                "stopped execution",
            )
            .unwrap();
            crate::db::complete_row_intent_cleanup(
                &db,
                &sandbox_id.to_string(),
                CleanupCompletion::MarkStopped,
                None,
            )
            .unwrap();
        }
        service.route_stream_epochs.lock().unwrap().clear();
        let replacement = runtime_fields(generation, 8);
        let cleanup_id = {
            let db = service.db_conn().unwrap();
            crate::db::begin_activation(
                &db,
                &sandbox_id.to_string(),
                SandboxState::Stopped,
                crate::db::ActivationKind::ColdStart,
                Some(generation),
                replacement.space_api_route_identity.activation_attempt_id,
            )
            .unwrap()
            .unwrap()
            .id
        };
        let before = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        let error = service
            .update_runtime_fields_for_activation(sandbox_id, cleanup_id, replacement)
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Aborted);
        let after = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(
            after.space_api_route_identity,
            before.space_api_route_identity
        );
        assert_eq!(after.pid, before.pid);
        assert_eq!(after.state, before.state);
        assert!(service.route_stream_epochs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn delayed_cleanup_cannot_remove_a_newer_runtime_generation() {
        for cached_epoch in [true, false] {
            assert_delayed_cleanup_preserves_replacement(cached_epoch).await;
        }
    }

    async fn assert_delayed_cleanup_preserves_replacement(cached_epoch: bool) {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = service_for_teardown_test(conn);

        service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
            )
            .unwrap();
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::mark_cleanup_required(
                &db,
                &sandbox_id.to_string(),
                true,
                crate::db::CleanupCompletion::MarkStopped,
                "generation 1 cleanup",
            )
            .unwrap();
            crate::db::complete_row_intent_cleanup(
                &db,
                &sandbox_id.to_string(),
                CleanupCompletion::MarkStopped,
                None,
            )
            .unwrap();
        }
        // A restarted manager has no route epoch for the stopped execution.
        if !cached_epoch {
            service.route_stream_epochs.lock().unwrap().clear();
        }
        let replacement = runtime_fields(ExecutionGeneration::new(2).unwrap(), 8);
        let cleanup_id = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::begin_activation(
                &db,
                &sandbox_id.to_string(),
                SandboxState::Stopped,
                crate::db::ActivationKind::ColdStart,
                Some(ExecutionGeneration::new(2).unwrap()),
                if cached_epoch {
                    Some(SandboxActivationAttemptId::new())
                } else {
                    replacement.space_api_route_identity.activation_attempt_id
                },
            )
            .unwrap()
            .expect("a stopped row may begin a replacement activation")
            .id
        };
        service
            .update_runtime_fields_for_activation(sandbox_id, cleanup_id, replacement)
            .unwrap();

        // Model generation 1 node cleanup completing after generation 2
        // attached through the same post-node-release path production uses.
        let completion = SandboxManagerService::complete_cleanup_after_node_release(
            &service.db,
            &sandbox_id.to_string(),
            CleanupCompletion::MarkStopped,
            Some(ExecutionGeneration::new(1).unwrap()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(completion, CleanupCompletionDisposition::Superseded);
        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.state, SandboxState::Starting);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkStopped),
            "the replacement activation must retain its own rollback intent"
        );

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                1,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Superseded,
            "cleanup from an older execution must not erase the newer durable lease",
        );
    }

    #[tokio::test]
    async fn superseded_delete_receipt_completion_emits_a_domain_event() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let receipt_generation = ExecutionGeneration::new(4).unwrap();
        let receipt = crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
            receipt_id: pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            execution_generation: receipt_generation,
            route_activation_id: SandboxActivationAttemptId::new(),
            policy_fingerprint:
                pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                    &AutoLifecyclePolicy::disabled(),
                ),
        };
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                execution_generation: Some(receipt_generation),
                ..SpaceApiRouteIdentity::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(&conn, receipt).unwrap();
        conn.execute(
            "UPDATE sandboxes
                SET state = 'deleting',
                    cleanup_completion = 'delete_row',
                    cleanup_keep_filesystem = 0
              WHERE id = ?1",
            [sandbox_id.to_string()],
        )
        .unwrap();
        crate::db::auto_lifecycle_delete_receipts::record_retained_route_evidence(
            &conn,
            sandbox_id,
            crate::auto_lifecycle_delete_receipts::RouteEvidence {
                execution_generation: ExecutionGeneration::new(5).unwrap(),
                activation_attempt_id: SandboxActivationAttemptId::new(),
                substrate: NodeManagedBy::Kubernetes,
                provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
            },
        )
        .unwrap();
        conn.execute(
            "DELETE FROM auto_lifecycle_delete_outbox
              WHERE sandbox_id = ?1
                AND state = 'pending'
                AND execution_generation < ?2",
            crate::db::fixture::params![sandbox_id.to_string(), 5],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        let completion = SandboxManagerService::complete_cleanup_after_node_release(
            &service.db,
            &sandbox_id.to_string(),
            CleanupCompletion::DeleteRow,
            Some(receipt_generation),
            Some(receipt),
            None,
        )
        .unwrap();
        drop(guard);

        assert_eq!(completion, CleanupCompletionDisposition::Superseded);
        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| event.get("receipt_id") == Some(&receipt.receipt_id.to_string()))
            .expect("receipt supersession must be visible in the sandbox timeline");
        assert_eq!(
            event.get("event_type").map(String::as_str),
            Some("delete_superseded")
        );
        assert_eq!(
            event.get("execution_generation").map(String::as_str),
            Some("4")
        );
    }

    #[tokio::test]
    async fn cleanup_generation_fence_is_atomic_with_completion() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = service_for_teardown_test(conn);

        service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
            )
            .unwrap();
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::mark_cleanup_required(
                &db,
                &sandbox_id.to_string(),
                true,
                crate::db::CleanupCompletion::Quarantine,
                "generation 1 cleanup",
            )
            .unwrap();
        }

        let replacement_service = service.clone();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (attempting_tx, attempting_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let replacement = std::thread::spawn(move || {
            start_rx.recv().unwrap();
            assert!(matches!(
                replacement_service.db.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            attempting_tx.send(()).unwrap();
            replacement_service
                .update_runtime_fields(
                    sandbox_id,
                    runtime_fields(ExecutionGeneration::new(2).unwrap(), 8),
                )
                .unwrap();
            done_tx.send(()).unwrap();
        });

        let completion = SandboxManagerService::complete_cleanup_after_node_release_with_hook(
            &service.db,
            &sandbox_id.to_string(),
            CleanupCompletion::Quarantine,
            Some(ExecutionGeneration::new(1).unwrap()),
            None,
            None,
            || {
                start_tx.send(()).unwrap();
                attempting_rx.recv().unwrap();
                assert!(matches!(
                    done_rx.try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Empty)
                ));
            },
        )
        .unwrap();
        assert_eq!(completion, CleanupCompletionDisposition::Applied);
        replacement.join().unwrap();

        let db = service.db.lock().expect("mutex poisoned");
        assert_eq!(
            crate::db::runtime_execution_generation(&db, &sandbox_id.to_string()).unwrap(),
            Some(ExecutionGeneration::new(2).unwrap()),
            "a replacement attaching during completion must retain its runtime identity",
        );
    }

    #[tokio::test]
    async fn cleanup_arriving_after_activation_claim_cleans_the_replacement_generation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        let generation_1 = ExecutionGeneration::new(1).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(generation_1, 7))
            .unwrap();
        let sandbox_id_string = sandbox_id.to_string();
        let lock = service.cleanup_lock(&sandbox_id_string);
        let lock_owner = crate::service::cleanup_support::CleanupLockGuard::new(
            Arc::clone(&service.cleanup_locks),
            sandbox_id_string,
            lock,
        );
        let activation_fence = lock_owner.lock().await;
        let cleanup_id = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::begin_activation(
                &db,
                &sandbox_id.to_string(),
                SandboxState::Stopped,
                crate::db::ActivationKind::ColdStart,
                Some(ExecutionGeneration::new(2).unwrap()),
                Some(SandboxActivationAttemptId::new()),
            )
            .unwrap()
            .expect("the replacement activation must own a cleanup episode")
            .id
        };
        let cleanup_service = service.clone();
        let dispatched = Arc::new(AtomicBool::new(false));
        let dispatch_observer = Arc::clone(&dispatched);
        let (attempting_tx, attempting_rx) = tokio::sync::oneshot::channel();
        let mut cleanup = tokio::spawn(async move {
            let sandbox_id = sandbox_id.to_string();
            attempting_tx.send(()).unwrap();
            let admission = cleanup_service
                .admit_lifecycle_cleanup(
                    SandboxId::from_str(&sandbox_id).unwrap(),
                    SandboxState::Stopping,
                    true,
                    CleanupCompletion::MarkStopped,
                    false,
                )
                .await
                .map_err(crate::service::cleanup::CleanupAdmissionError::into_status)?;
            cleanup_service
                .run_cleanup_to_completion_with_node_dispatch_and_admission(
                    crate::service::cleanup::SynchronousCleanupRequest {
                        sandbox_id: &sandbox_id,
                        uuid8: &sandbox_id,
                        keep_filesystem: true,
                        context: "generation-fenced test cleanup",
                        completion: CleanupCompletion::MarkStopped,
                        published_artifact: None,
                        immediate_kill: false,
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    Some(admission),
                    None,
                    || async { Ok(()) },
                    move |()| async move {
                        dispatch_observer.store(true, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
        });
        attempting_rx
            .await
            .expect("cleanup must begin lifecycle admission");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut cleanup)
                .await
                .is_err(),
            "cleanup lifecycle admission must wait for the activation fence"
        );
        assert_eq!(
            service
                .lookup_sandbox(&sandbox_id.to_string())
                .unwrap()
                .state,
            SandboxState::Starting,
            "cleanup must not mutate lifecycle while activation owns the fence"
        );

        service
            .update_runtime_fields_for_activation(
                sandbox_id,
                cleanup_id,
                runtime_fields(ExecutionGeneration::new(2).unwrap(), 8),
            )
            .unwrap();
        drop(activation_fence);

        cleanup.await.unwrap().unwrap();
        assert!(
            dispatched.load(Ordering::SeqCst),
            "cleanup admitted after activation claims must clean the attached replacement"
        );
        let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
        assert_eq!(row.state, SandboxState::Stopped);
        assert_eq!(
            row.space_api_route_identity.execution_generation,
            Some(ExecutionGeneration::new(2).unwrap())
        );
        assert!(row.space_api_route_identity.activation_attempt_id.is_none());
        assert!(row.space_api_route_identity.route_generation.is_none());
    }

    #[tokio::test]
    async fn untracked_cleanup_admission_has_no_persistence_error() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);

        let admission = service
            .admit_lifecycle_cleanup(
                SandboxId::new(),
                SandboxState::Deleting,
                false,
                CleanupCompletion::DeleteRow,
                false,
            )
            .await;

        assert!(
            matches!(
                admission,
                Ok(crate::service::cleanup::CleanupAdmission::Unpersisted {
                    persistence_error: None,
                    ..
                })
            ),
            "a missing sandbox row is not a persistence error"
        );
    }

    pub(super) async fn add_test_monitoring(service: &SandboxManagerService, sandbox_id: &str) {
        service
            .watchdog
            .add_sandbox(crate::watchdog::SidecarInfo {
                guest_image: Default::default(),
                node_execution_id: None,
                sandbox_id: sandbox_id.to_string(),
                uid: None,
                runtime_pid: None,
                spaced_host_pid: None,
                netgtw_pid: None,
                vsock_uds_path: None,
                process_env: BTreeMap::new(),
                network_tx_bytes: 0,
                network_rx_bytes: 0,
            })
            .await;
        service
            .snapshot_scheduler
            .add_sandbox(sandbox_id.to_string(), "seed".to_string())
            .await;
    }

    #[tokio::test]
    async fn delete_defers_suspending_sandbox_without_removing_monitoring() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        let service = SandboxManagerService::for_test(conn);
        add_test_monitoring(&service, &sandbox_id).await;

        let error = service
            .delete(internal_request(DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: false,
            }))
            .await
            .expect_err("an in-flight suspend must defer delete");

        assert_eq!(error.code(), tonic::Code::Aborted);
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Suspending
        );
        assert!(service.watchdog.get_health(&sandbox_id).await.is_some());
        assert!(service.snapshot_scheduler.is_active(&sandbox_id).await);
    }

    #[tokio::test]
    async fn delete_defers_failed_cleanup_that_still_owns_suspend_publication() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();
        crate::db::mark_cleanup_failed(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
            "injected cgroup cleanup failure",
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        add_test_monitoring(&service, &sandbox_id).await;

        let error = service
            .delete(internal_request(DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: false,
            }))
            .await
            .expect_err("durable suspend cleanup intent must defer delete");

        assert_eq!(error.code(), tonic::Code::Aborted);
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::CleanupFailed);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkSuspended)
        );
        assert_eq!(row.cleanup_keep_filesystem, Some(true));
        assert!(service.watchdog.get_health(&sandbox_id).await.is_some());
        assert!(service.snapshot_scheduler.is_active(&sandbox_id).await);
    }

    #[tokio::test]
    async fn delete_defers_quarantined_suspend_when_the_filesystem_is_the_only_copy() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id,
            crate::db::CleanupCompletion::MarkSuspended,
            None,
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        add_test_monitoring(&service, &sandbox_id).await;

        let quarantined = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(quarantined.state, SandboxState::Quarantined);
        assert_eq!(
            quarantined.failure_reason.as_deref(),
            Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
        );
        assert!(quarantined.current_local_artifact_id.is_none());
        assert_eq!(
            crate::db::current_quarantine_reason_wire_value(
                &service.db.lock().expect("mutex poisoned"),
                &sandbox_id,
            )
            .unwrap()
            .as_deref(),
            Some(SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.as_str()),
        );

        for action in [
            TeardownSuspendAction::ObserveOnly,
            TeardownSuspendAction::BeginOrJoin,
        ] {
            let error = service
                .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                    sandbox_id.clone(),
                    1,
                    action,
                )))
                .await
                .expect_err("quarantine ends polling without authorizing runtime release");
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        }

        let error = service
            .delete(internal_request(DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: false,
            }))
            .await
            .expect_err("the only retained filesystem copy must defer delete");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(&error));
        let retained = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(retained.state, SandboxState::Quarantined);
        assert_eq!(
            retained.failure_reason.as_deref(),
            Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
        );
        assert!(service.watchdog.get_health(&sandbox_id).await.is_some());
        assert!(service.snapshot_scheduler.is_active(&sandbox_id).await);
    }

    #[tokio::test]
    async fn delete_allows_completed_suspended_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = SandboxManagerService::for_test(conn);

        let response = service
            .delete(internal_request(DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: false,
            }))
            .await
            .expect("a completed suspend must not block an explicit delete");

        assert_eq!(response.into_inner().deleted_ids, vec![sandbox_id]);
    }

    #[tokio::test]
    async fn delete_rechecks_suspend_state_inside_the_cleanup_claim() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);
        add_test_monitoring(&service, &sandbox_id_string).await;

        let cleanup_lock = service.cleanup_lock(&sandbox_id_string);
        let cleanup_blocker = Arc::clone(&cleanup_lock).lock_owned().await;
        let baseline_refs = Arc::strong_count(&cleanup_lock);
        let delete_service = service.clone();
        let delete_id = sandbox_id_string.clone();
        let delete = tokio::spawn(async move {
            delete_service
                .delete(internal_request(DeleteRequest {
                    sandbox_id: delete_id,
                    template_filter: None,
                    blocking: false,
                }))
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&cleanup_lock) <= baseline_refs {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete must reach the cleanup claim");
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::test_fixtures::state_write_fixtures::force_state(
                &db,
                &sandbox_id_string,
                SandboxState::Suspending,
            );
        }
        drop(cleanup_blocker);

        let error = delete
            .await
            .unwrap()
            .expect_err("the authoritative cleanup claim must observe the raced suspend");
        assert_eq!(error.code(), tonic::Code::Aborted);
        assert_eq!(
            service.lookup_sandbox(&sandbox_id_string).unwrap().state,
            SandboxState::Suspending
        );
        assert!(
            service
                .watchdog
                .get_health(&sandbox_id_string)
                .await
                .is_some()
        );
        assert!(
            service
                .snapshot_scheduler
                .is_active(&sandbox_id_string)
                .await
        );
    }

    #[tokio::test]
    async fn ordinary_stop_admission_ignores_unrelated_malformed_metadata() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
            [&sandbox_id_string],
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        let admission = service
            .admit_lifecycle_cleanup(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                false,
            )
            .await
            .expect("ordinary stop admission only depends on lifecycle state");
        drop(admission);

        let state = service
            .db
            .lock()
            .expect("mutex poisoned")
            .query_row(
                "SELECT state FROM sandboxes WHERE id = ?1",
                [&sandbox_id_string],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(state, SandboxState::Stopping.as_str());
    }

    #[tokio::test]
    async fn lifecycle_cleanup_rejects_bulk_and_prefix_selectors_without_mutation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);
        for selector in ["all", "", "not-a-uuid", sandbox_id.get(..8).unwrap()] {
            for delete in [false, true] {
                let code = if delete {
                    service
                        .delete(internal_request(DeleteRequest {
                            sandbox_id: selector.to_string(),
                            template_filter: Some("seed".to_string()),
                            blocking: false,
                        }))
                        .await
                        .unwrap_err()
                        .code()
                } else {
                    service
                        .stop(internal_request(StopRequest {
                            sandbox_id: selector.to_string(),
                            template_filter: Some("seed".to_string()),
                            blocking: false,
                            immediate_kill: false,
                        }))
                        .await
                        .unwrap_err()
                        .code()
                };
                assert_eq!(code, tonic::Code::InvalidArgument, "{selector}");
                let db = service.db_conn().unwrap();
                assert_eq!(
                    crate::db::current_state_label(&db, &sandbox_id)
                        .unwrap()
                        .as_deref(),
                    Some("started")
                );
                assert!(
                    crate::db::cleanup_episode(&db, &sandbox_id)
                        .unwrap()
                        .is_none()
                );
                assert!(crate::db::list_node_dirty_refs(&db, 10).unwrap().is_empty());
            }
        }
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
    }

    #[tokio::test]
    async fn unpersisted_cleanup_keeps_replacement_out_of_node_dispatch() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        conn.execute_batch(
            "CREATE TRIGGER fail_cleanup_intent
             BEFORE UPDATE OF cleanup_completion ON sandboxes
             BEGIN
                 SELECT RAISE(FAIL, 'injected cleanup-intent failure');
             END;",
        )
        .unwrap();
        let service = service_for_teardown_test(conn);
        let generation_1 = ExecutionGeneration::new(1).unwrap();
        let generation_2 = ExecutionGeneration::new(2).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(generation_1, 7))
            .unwrap();

        let admission = service
            .admit_lifecycle_cleanup(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                true,
            )
            .await
            .unwrap();
        assert!(admission.is_unpersisted_for_sandbox(sandbox_id).unwrap());

        let replacement_service = service.clone();
        let replacement_sandbox_id = sandbox_id_string.clone();
        let (attempting_tx, attempting_rx) = tokio::sync::oneshot::channel();
        let mut replacement = tokio::spawn(async move {
            attempting_tx.send(()).unwrap();
            let lock = replacement_service.cleanup_lock(&replacement_sandbox_id);
            let owner = CleanupLockGuard::new(
                Arc::clone(&replacement_service.cleanup_locks),
                replacement_sandbox_id,
                lock,
            );
            let _fence = owner.lock().await;
            replacement_service
                .update_runtime_fields(sandbox_id, runtime_fields(generation_2, 8))
                .unwrap();
        });
        attempting_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut replacement)
                .await
                .is_err(),
            "replacement attachment must wait while unpersisted cleanup owns admission"
        );

        let observed_generation = Arc::new(Mutex::new(None));
        let dispatch_observation = Arc::clone(&observed_generation);
        service
            .run_cleanup_after_persistence_failure_with_node_dispatch(
                SynchronousCleanupRequest {
                    sandbox_id: &sandbox_id_string,
                    uuid8: &sandbox_id_string,
                    keep_filesystem: true,
                    context: "unpersisted fence regression",
                    completion: CleanupCompletion::MarkStopped,
                    published_artifact: None,
                    immediate_kill: true,
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                },
                &admission,
                || async {
                    let generation = {
                        let db = service.db.lock().unwrap();
                        crate::db::runtime_execution_generation(&db, &sandbox_id_string).unwrap()
                    };
                    *dispatch_observation.lock().unwrap() = generation;
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(*observed_generation.lock().unwrap(), Some(generation_1));
        assert!(!replacement.is_finished());

        drop(admission);
        replacement.await.unwrap();
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::runtime_execution_generation(&db, &sandbox_id_string).unwrap(),
            Some(generation_2)
        );
    }

    #[tokio::test]
    async fn observed_generation_cleanup_preserves_unpersisted_fallback() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        conn.execute_batch(
            "CREATE TRIGGER fail_observed_cleanup_intent
             BEFORE UPDATE OF cleanup_completion ON sandboxes
             BEGIN
                 SELECT RAISE(FAIL, 'injected observed cleanup-intent failure');
             END;",
        )
        .unwrap();
        let service = service_for_teardown_test(conn);
        let generation = ExecutionGeneration::new(1).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(generation, 7))
            .unwrap();

        let admission = service
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                true,
                Some(Some(generation)),
            )
            .await
            .expect("observed-generation cleanup must retain best-effort node cleanup");

        assert!(admission.is_unpersisted_for_sandbox(sandbox_id).unwrap());
        assert_eq!(
            service.lookup_sandbox(&sandbox_id_string).unwrap().state,
            SandboxState::Started
        );
    }

    #[tokio::test]
    async fn observed_execution_cleanup_fails_closed_without_readable_generation_lineage() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let generation = ExecutionGeneration::new(1).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(generation, 7))
            .unwrap();
        service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET space_api_route_execution_generation = 0 WHERE id = ?1",
                [&sandbox_id_string],
            )
            .unwrap();

        let error = service
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                true,
                Some(Some(generation)),
            )
            .await
            .err()
            .expect("a generation fence needs readable lineage");
        assert_eq!(error.into_status().code(), tonic::Code::Unavailable);
        assert_eq!(
            crate::db::current_state_label(&service.db.lock().unwrap(), &sandbox_id_string)
                .unwrap()
                .as_deref(),
            Some(SandboxState::Started.as_str())
        );

        // Unfenced cleanup keeps its best-effort node cleanup fallback.
        let admission = service
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                true,
                None,
            )
            .await
            .expect("unfenced cleanup falls back to unpersisted node cleanup");
        assert!(admission.is_unpersisted_for_sandbox(sandbox_id).unwrap());
    }

    #[tokio::test]
    async fn auto_lifecycle_delete_fails_closed_when_cleanup_intent_cannot_be_persisted() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 900,
                action: AutoLifecycleIdleAction::Delete,
            }],
        }
        .normalized();
        insert_test_sandbox_with_policy(&conn, &sandbox_id_string, SandboxState::Started, &policy);
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                route_generation: Some(RouteGeneration::new(7).unwrap()),
                current_node_id: Some(NodeId::new()),
                execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
                activation_attempt_id: Some(SandboxActivationAttemptId::new()),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                pod_uid: Some(PodUid::parse("pod-uid-7").unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_auto_lifecycle_cleanup_intent
             BEFORE UPDATE OF cleanup_completion ON sandboxes
             BEGIN
                 SELECT RAISE(FAIL, 'injected auto-lifecycle cleanup-intent failure');
             END;",
        )
        .unwrap();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_role: Some(NodeRole::Worker),
                ..SandboxManagerServiceOptions::for_test()
            },
        );

        let error = service
            .admit_auto_lifecycle_delete(
                sandbox_id,
                crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation {
                    policy_fingerprint:
                        pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                            &policy,
                        ),
                    authority: crate::db::PolicyAuthority::Cluster,
                },
            )
            .await
            .err()
            .expect("auto-lifecycle delete must not run unpersisted");
        assert_eq!(error.into_status().code(), tonic::Code::Unavailable);

        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::get_sandbox(&db, &sandbox_id_string)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Started
        );
        assert_eq!(
            crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                &db, sandbox_id,
            )
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn fatal_kill_admission_keeps_replacement_out_of_node_dispatch() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let generation_1 = ExecutionGeneration::new(1).unwrap();
        let generation_2 = ExecutionGeneration::new(2).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(generation_1, 7))
            .unwrap();

        let admission = service
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                false,
                Some(Some(generation_1)),
            )
            .await
            .unwrap();

        let replacement_service = service.clone();
        let replacement_sandbox_id = sandbox_id_string.clone();
        let (attempting_tx, attempting_rx) = tokio::sync::oneshot::channel();
        let mut replacement = tokio::spawn(async move {
            attempting_tx.send(()).unwrap();
            let lock = replacement_service.cleanup_lock(&replacement_sandbox_id);
            let owner = CleanupLockGuard::new(
                Arc::clone(&replacement_service.cleanup_locks),
                replacement_sandbox_id,
                lock,
            );
            let _fence = owner.lock().await;
            replacement_service
                .update_runtime_fields(sandbox_id, runtime_fields(generation_2, 8))
                .unwrap();
        });
        attempting_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut replacement)
                .await
                .is_err(),
            "replacement attachment must wait before fatal Kill node dispatch"
        );

        let dispatched_generation = Arc::new(Mutex::new(None));
        let dispatch_observation = Arc::clone(&dispatched_generation);
        service
            .dispatch_node_action_using_admission(sandbox_id, &admission, || async {
                let generation = {
                    let db = service.db.lock().unwrap();
                    crate::db::runtime_execution_generation(&db, &sandbox_id_string).unwrap()
                };
                *dispatch_observation.lock().unwrap() = generation;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(*dispatched_generation.lock().unwrap(), Some(generation_1));
        assert!(!replacement.is_finished());

        drop(admission);
        replacement.await.unwrap();
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::runtime_execution_generation(&db, &sandbox_id_string).unwrap(),
            Some(generation_2)
        );
    }

    #[tokio::test]
    async fn snapshot_rollback_cleanup_admission_rejects_a_changed_lifecycle_state() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        crate::db::test_fixtures::state_write_fixtures::force_state(
            &conn,
            &sandbox_id_string,
            SandboxState::Paused,
        );
        let service = SandboxManagerService::for_test(conn);

        let result = service
            .admit_lifecycle_cleanup_from_observed_state(
                sandbox_id,
                ObservedCleanupClaim {
                    observed_state: SandboxState::Started,
                    target_state: SandboxState::Stopping,
                    keep_filesystem: true,
                    completion: CleanupCompletion::MarkStopped,
                    persist_intent: false,
                    observed_execution_generation: Some(None),
                },
            )
            .await;

        assert!(
            result.is_err(),
            "stale rollback cleanup must not be admitted"
        );
        assert_eq!(
            service.lookup_sandbox(&sandbox_id_string).unwrap().state,
            SandboxState::Paused
        );
    }

    #[tokio::test]
    async fn fatal_kill_rejects_replacement_attached_before_admission() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
        let service = service_for_teardown_test(conn);
        let observed_generation = ExecutionGeneration::new(1).unwrap();
        let replacement_generation = ExecutionGeneration::new(2).unwrap();
        service
            .update_runtime_fields(sandbox_id, runtime_fields(observed_generation, 7))
            .unwrap();

        service
            .update_runtime_fields(sandbox_id, runtime_fields(replacement_generation, 8))
            .unwrap();

        let error = match service
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                CleanupCompletion::MarkStopped,
                false,
                Some(Some(observed_generation)),
            )
            .await
        {
            Ok(_) => panic!("fatal Kill must reject a replaced execution"),
            Err(error) => error,
        };
        assert!(
            matches!(
                error,
                crate::service::cleanup::CleanupAdmissionError::Status(ref status)
                    if status.code() == tonic::Code::Aborted
            ),
            "fatal Kill must not admit cleanup for a replaced execution: {error:?}"
        );

        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id_string)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Started);
        assert_eq!(
            crate::db::runtime_execution_generation(&db, &sandbox_id_string).unwrap(),
            Some(replacement_generation)
        );
    }

    #[tokio::test]
    async fn unpersisted_delete_clears_activity_without_replacing_live_lock_identity() {
        let service = SandboxManagerService::for_test(crate::db::Connection::open_in_memory());
        let sandbox_id = "sbx-delete-persistence-failure";
        let wake_before = service.auto_lifecycle.wake_lock(sandbox_id);
        service.auto_lifecycle.record_wake_completed(sandbox_id);

        let error = service
            .finish_unpersisted_delete_cleanup(
                sandbox_id,
                Some("injected persistence failure".to_string()),
            )
            .expect_err("persistence failure must still be returned");
        assert_eq!(error.code(), tonic::Code::Internal);

        let wake_after = service.auto_lifecycle.wake_lock(sandbox_id);
        assert!(
            Arc::ptr_eq(&wake_before, &wake_after),
            "runtime cleanup cannot replace a lock still owned by a caller"
        );
        assert!(
            service
                .auto_lifecycle
                .last_wake_completed_at(sandbox_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn abandoned_backup_bookkeeping_retains_drain_ownership_and_defers_new_work() {
        let conn = crate::db::test_conn();
        let artifact_ids = [LocalArtifactId::new(), LocalArtifactId::new()];
        for artifact_id in artifact_ids {
            conn.execute(
                "INSERT INTO local_artifacts (
                    local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                    local_path, created_at
                 ) VALUES (?1, 'bookkeeping-test', 'full', 'suspend', '/snapshot', '0')",
                [artifact_id.to_string()],
            )
            .unwrap();
        }
        let payloads = artifact_ids.map(|artifact_id| {
            crate::snapshot_upload_jobs::tests::reclaim_fixture_payload(
                &artifact_id.to_string(),
                "/snapshot".into(),
            )
        });
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, payload, completed_at)
             VALUES ('019d84bd-280d-7331-99c6-b63377f3a3a2', 'snapshot_upload', 'completed', ?1,
                     datetime('now', '-5 days'))",
            [serde_json::to_string(&payloads[1]).unwrap()],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        // Hold the writer before it can perform any SQL. Dropping its handle
        // is what the executor's outer timeout does to a queued blocking task.
        {
            let conn = service.db.lock().unwrap();
            let bookkeeping = crate::snapshot_upload_jobs::spawn_backup_bookkeeping(
                service.db.clone(),
                &service.drain_state,
                &payloads[0],
                Arc::new(crate::retained_source::FakeInspector::new(
                    crate::retained_source::RetainedSourceCheck::Absent,
                )),
            )
            .unwrap();
            drop(bookkeeping);
            service.begin_draining_for_restart();
            let active = service.active_state_transition_count();
            drop(conn);
            assert_eq!(active, 1, "caller abandonment must not detach its writer");
        }
        tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_for_state_transitions_drained(),
        )
        .await
        .expect("blocking bookkeeping must release ownership when it completes");
        assert!(
            crate::snapshot_upload_jobs::spawn_backup_bookkeeping(
                service.db.clone(),
                &service.drain_state,
                &payloads[1],
                Arc::new(crate::retained_source::FakeInspector::new(
                    crate::retained_source::RetainedSourceCheck::Absent,
                )),
            )
            .is_none(),
            "draining must defer optional local bookkeeping"
        );
        let conn = service.db.lock().unwrap();
        for (artifact_id, completed) in artifact_ids.into_iter().zip([true, false]) {
            let stamped: bool = conn
                .query_row(
                    "SELECT backup_completed_at IS NOT NULL FROM local_artifacts
                     WHERE local_artifact_id = ?1",
                    [artifact_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stamped, completed);
        }
        pplx_space_common_jobs::db::prune_terminal_jobs(
            conn.raw(),
            pplx_space_common_jobs::COMPLETED_JOB_RETENTION,
            pplx_space_common_jobs::FAILED_JOB_RETENTION,
        )
        .unwrap();
        assert!(
            pplx_space_common_jobs::db::get_job(
                conn.raw(),
                &"019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap()
            )
            .unwrap()
            .is_some(),
            "deferred bookkeeping keeps durable evidence for maintenance"
        );
    }

    #[tokio::test]
    async fn stopped_teardown_detaches_only_a_retired_execution() {
        for (requested, expected) in [
            (1, TeardownSuspendOutcome::Superseded),
            (2, TeardownSuspendOutcome::Superseded),
            (3, TeardownSuspendOutcome::Pending),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
            crate::db::update_runtime_fields(
                &conn,
                sandbox_id.parse().unwrap(),
                runtime_fields(ExecutionGeneration::new(2).unwrap(), 7),
            )
            .unwrap();
            crate::db::test_fixtures::state_write_fixtures::apply_edge(
                &conn,
                &sandbox_id,
                &crate::db::edges::STOP_ADMISSION,
            );
            crate::db::set_cleanup_intent(
                &conn,
                &sandbox_id,
                true,
                crate::db::CleanupCompletion::MarkStopped,
            )
            .unwrap();
            crate::db::complete_row_intent_cleanup(
                &conn,
                &sandbox_id,
                crate::db::CleanupCompletion::MarkStopped,
                None,
            )
            .unwrap();
            let service = service_for_teardown_test(conn);
            let response = service
                .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                    sandbox_id.clone(),
                    requested,
                    TeardownSuspendAction::BeginOrJoin,
                )))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
                expected
            );
            assert!(response.published_artifact.is_none());
            assert_eq!(
                service.lookup_sandbox(&sandbox_id).unwrap().state,
                SandboxState::Stopped
            );
        }
    }

    #[tokio::test]
    async fn teardown_suspend_reports_durable_only_for_the_pinned_backed_artifact() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let local_artifact_id = LocalArtifactId::new();
        let generation = teardown_generation();
        crate::db::begin_teardown_suspend(&conn, &sandbox_id, generation).unwrap();
        crate::db::stage_teardown_suspend_artifact(
            &conn,
            &sandbox_id,
            generation,
            local_artifact_id,
            "/snapshot",
        )
        .unwrap();
        crate::db::mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string())
            .unwrap();
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id,
            crate::db::CleanupCompletion::MarkSuspended,
            Some((local_artifact_id, "/snapshot")),
        )
        .unwrap();
        crate::db::complete_teardown_suspend(
            &conn,
            &sandbox_id,
            generation,
            Some(local_artifact_id),
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let durable = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.clone(),
                1,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(durable.outcome).unwrap(),
            TeardownSuspendOutcome::Durable,
        );

        {
            let db = service.db.lock().expect("mutex poisoned");
            db.execute(
                "UPDATE local_artifacts SET backup_completed_at = NULL
                 WHERE local_artifact_id = ?1",
                crate::db::fixture::params![local_artifact_id.to_string()],
            )
            .unwrap();
        }
        let unbacked = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id,
                1,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_ne!(
            TeardownSuspendOutcome::try_from(unbacked.outcome).unwrap(),
            TeardownSuspendOutcome::Durable,
        );
    }

    fn delete_sandbox_with_generation_receipt(
        conn: &crate::db::Connection,
        sandbox_id: &str,
        deleted_generation: ExecutionGeneration,
    ) {
        insert_test_sandbox(conn, sandbox_id, SandboxState::Started);
        crate::db::update_runtime_fields(
            conn,
            sandbox_id.parse().unwrap(),
            runtime_fields(deleted_generation, 7),
        )
        .unwrap();
        crate::db::test_fixtures::state_write_fixtures::apply_edge(
            conn,
            sandbox_id,
            &crate::db::edges::DELETE_ADMISSION,
        );
        crate::db::set_cleanup_intent(
            conn,
            sandbox_id,
            false,
            crate::db::CleanupCompletion::DeleteRow,
        )
        .unwrap();
        crate::db::complete_row_intent_cleanup(
            conn,
            sandbox_id,
            crate::db::CleanupCompletion::DeleteRow,
            None,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn teardown_release_after_activation_rollback_is_generation_fenced() {
        // Real activation admission and cleanup completion create the receipt;
        // reopening SQLite proves release does not depend on the old request.
        for target in [SandboxState::Created, SandboxState::Stopped] {
            for action in [
                TeardownSuspendAction::ObserveOnly,
                TeardownSuspendAction::BeginOrJoin,
            ] {
                for (case, requested, mutation, expected) in [
                    ("matching", 7, "", TeardownSuspendOutcome::Superseded),
                    ("newer_receipt", 6, "", TeardownSuspendOutcome::Superseded),
                    ("older_receipt", 8, "", TeardownSuspendOutcome::Pending),
                    (
                        "missing_receipt",
                        7,
                        "UPDATE sandboxes SET transition_receipt_id = NULL, transition_receipt_kind = NULL, transition_receipt_target_state = NULL, transition_receipt_execution_generation = NULL, transition_receipt_reason_code = NULL, transition_receipt_activation_attempt_id = NULL WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "malformed_receipt",
                        7,
                        "UPDATE sandboxes SET transition_receipt_id = 'invalid' WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "wrong_kind",
                        7,
                        "UPDATE sandboxes SET transition_receipt_kind = 'activation_completed', transition_receipt_target_state = 'created', transition_receipt_reason_code = NULL WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "wrong_target",
                        7,
                        "UPDATE sandboxes SET transition_receipt_target_state = 'suspended' WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "runtime_residue",
                        7,
                        "UPDATE sandboxes SET uid = 10000 WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "cleanup_intent",
                        7,
                        "UPDATE sandboxes SET cleanup_completion = 'mark_stopped', cleanup_keep_filesystem = 1 WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "failed_suspend",
                        7,
                        "UPDATE sandboxes SET state = 'failed', failure_reason = 'suspend_publish_failed_no_artifact_runtime_gone' WHERE id = ?1",
                        TeardownSuspendOutcome::Pending,
                    ),
                    (
                        "same_generation_restart",
                        7,
                        "",
                        TeardownSuspendOutcome::Pending,
                    ),
                    ("newer_execution", 6, "", TeardownSuspendOutcome::Superseded),
                    ("cleanup_episode", 7, "", TeardownSuspendOutcome::Pending),
                    (
                        "pending_suspend_job",
                        7,
                        "",
                        TeardownSuspendOutcome::Pending,
                    ),
                    ("failed_suspend_job", 7, "", TeardownSuspendOutcome::Pending),
                    (
                        "completed_suspend_job",
                        7,
                        "",
                        TeardownSuspendOutcome::Superseded,
                    ),
                ] {
                    let directory = tempfile::tempdir().unwrap();
                    let db_path = directory.path().join("manager.sqlite");
                    let sandbox_id = SandboxId::new();
                    let id = sandbox_id.to_string();
                    let generation = ExecutionGeneration::new(7).unwrap();
                    {
                        crate::db::create_test_schema_on_disk(&db_path);
                        let conn = crate::db::Connection::open_at(&db_path).unwrap();
                        insert_test_sandbox(&conn, &id, target);
                        let fields = runtime_fields(generation, 7);
                        let episode = crate::db::begin_activation(
                            &conn,
                            &id,
                            target,
                            crate::db::ActivationKind::ColdStart,
                            Some(generation),
                            fields.space_api_route_identity.activation_attempt_id,
                        )
                        .unwrap()
                        .unwrap();
                        crate::db::update_runtime_fields_for_activation(
                            &conn, sandbox_id, episode.id, fields,
                        )
                        .unwrap();
                        crate::db::request_cleanup_episode(
                            &conn,
                            &id,
                            episode.id,
                            SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                            "activation failed",
                        )
                        .unwrap();
                        assert!(
                            crate::db::complete_cleanup_episode(&conn, &id, episode.id)
                                .unwrap()
                                .is_some()
                        );
                        assert_eq!(
                            crate::db::runtime_execution_generation(&conn, &id).unwrap(),
                            None
                        );
                        assert_eq!(
                            crate::db::get_sandbox(&conn, &id).unwrap().unwrap().state,
                            target
                        );
                        if !mutation.is_empty() {
                            conn.execute(mutation, [&id]).unwrap();
                        }
                        if matches!(
                            case,
                            "pending_suspend_job" | "failed_suspend_job" | "completed_suspend_job"
                        ) {
                            crate::db::claim_teardown_suspend_operation(&conn, &id, generation)
                                .unwrap();
                            if case == "failed_suspend_job" {
                                let operation = crate::db::teardown_suspend_status(&conn, &id)
                                    .unwrap()
                                    .unwrap()
                                    .operation
                                    .unwrap();
                                pplx_space_common_jobs::db::fail_job_permanent(
                                    conn.raw(),
                                    &operation.job_id,
                                    "suspend publication failed",
                                    None,
                                )
                                .unwrap();
                            } else if case == "completed_suspend_job" {
                                assert!(
                                    crate::db::complete_teardown_suspend(
                                        &conn, &id, generation, None
                                    )
                                    .unwrap()
                                );
                            }
                        }
                        if matches!(
                            case,
                            "same_generation_restart" | "newer_execution" | "cleanup_episode"
                        ) {
                            let old_receipt = crate::db::sandbox_state_publication_metadata(
                                &conn,
                                std::slice::from_ref(&id),
                            )
                            .unwrap()
                            .remove(&id)
                            .unwrap()
                            .unwrap();
                            let fields = runtime_fields(generation, 8);
                            let replacement = crate::db::begin_activation(
                                &conn,
                                &id,
                                target,
                                crate::db::ActivationKind::ColdStart,
                                Some(generation),
                                fields.space_api_route_identity.activation_attempt_id,
                            )
                            .unwrap()
                            .unwrap();
                            if case == "cleanup_episode" {
                                // A conflicting episode must block even with a dormant row.
                                conn.execute("UPDATE sandboxes SET state = ?2, cleanup_completion = NULL, cleanup_keep_filesystem = NULL WHERE id = ?1", crate::db::fixture::params![id, target.as_str()]).unwrap();
                            } else {
                                crate::db::update_runtime_fields_for_activation(
                                    &conn,
                                    sandbox_id,
                                    replacement.id,
                                    fields,
                                )
                                .unwrap();
                                if case == "newer_execution" {
                                    assert!(
                                        crate::db::mark_start_succeeded(
                                            &conn,
                                            &id,
                                            Some(replacement.id)
                                        )
                                        .unwrap()
                                    );
                                }
                            }
                            // A retained historical receipt must not override a new owner.
                            conn.execute("UPDATE sandboxes SET transition_receipt_id = ?2, transition_receipt_kind = ?3, transition_receipt_target_state = ?4, transition_receipt_execution_generation = ?5, transition_receipt_reason_code = ?6, transition_receipt_activation_attempt_id = ?7 WHERE id = ?1", crate::db::fixture::params![id, old_receipt.transition_receipt_id, old_receipt.transition_receipt_kind, old_receipt.transition_receipt_target_state, old_receipt.transition_receipt_execution_generation, old_receipt.transition_receipt_reason_code, old_receipt.transition_receipt_activation_attempt_id]).unwrap();
                        }
                    }
                    let service = service_for_teardown_test(
                        crate::db::Connection::open_at(&db_path).unwrap(),
                    );
                    let before = crate::db::get_sandbox(&service.db.lock().unwrap(), &id)
                        .unwrap()
                        .unwrap();
                    let response = service
                        .prepare_teardown_suspend(internal_request(
                            prepare_teardown_suspend_request(id.clone(), requested, action),
                        ))
                        .await;
                    if case == "failed_suspend_job" {
                        assert_eq!(
                            response.unwrap_err().code(),
                            tonic::Code::FailedPrecondition
                        );
                    } else {
                        let response = response.unwrap().into_inner();
                        assert_eq!(
                            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
                            expected,
                            "{target:?}/{action:?}/{case}"
                        );
                        assert!(response.published_artifact.is_none());
                    }
                    let db = service.db.lock().unwrap();
                    let after = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
                    assert_eq!(
                        after.state, before.state,
                        "{case} must preserve lifecycle state"
                    );
                    assert_eq!(
                        after.space_api_route_identity,
                        before.space_api_route_identity
                    );
                    assert!(service.teardown_suspend_drivers.is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn delete_receipt_is_generation_fenced() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let deleted_generation = ExecutionGeneration::new(2).unwrap();
        delete_sandbox_with_generation_receipt(&conn, &sandbox_id, deleted_generation);
        let service = service_for_teardown_test(conn);

        let deleted = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.clone(),
                deleted_generation.get(),
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();
        let newer = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id,
                deleted_generation.get() + 1,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(deleted.outcome).unwrap(),
            TeardownSuspendOutcome::Deleted,
        );
        assert_eq!(
            TeardownSuspendOutcome::try_from(newer.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
    }

    #[tokio::test]
    async fn inventory_row_delete_leaves_teardown_receipt() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let generation = ExecutionGeneration::new(2).unwrap();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopped);
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id.parse().unwrap(),
            runtime_fields(generation, 7),
        )
        .unwrap();
        let observed = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();

        assert_eq!(
            crate::db::delete_sandbox_if_inventory_witness_matches(&conn, &observed).unwrap(),
            crate::db::InventoryStaleRowMutation::Removed,
        );
        let service = service_for_teardown_test(conn);
        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id,
                generation.get(),
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Deleted,
        );
    }

    #[tokio::test]
    async fn delete_receipt_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("sandbox-manager.db");
        let sandbox_id = SandboxId::new().to_string();
        let deleted_generation = ExecutionGeneration::new(2).unwrap();
        {
            let mut conn = crate::db::Connection::open_at(&db_path).unwrap();
            crate::db::create_test_schema(&mut conn);
            delete_sandbox_with_generation_receipt(&conn, &sandbox_id, deleted_generation);
        }
        let service = service_for_teardown_test(crate::db::Connection::open_at(&db_path).unwrap());

        let deleted = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.clone(),
                deleted_generation.get(),
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();
        let newer = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id,
                deleted_generation.get() + 1,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(deleted.outcome).unwrap(),
            TeardownSuspendOutcome::Deleted,
        );
        assert_eq!(
            TeardownSuspendOutcome::try_from(newer.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
    }

    #[tokio::test]
    async fn delete_receipt_expires_after_seven_days() {
        let conn = crate::db::test_conn();
        let retained_sandbox_id = SandboxId::new().to_string();
        let expired_sandbox_id = SandboxId::new().to_string();
        let deleted_generation = ExecutionGeneration::new(2).unwrap();
        delete_sandbox_with_generation_receipt(&conn, &retained_sandbox_id, deleted_generation);
        delete_sandbox_with_generation_receipt(&conn, &expired_sandbox_id, deleted_generation);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        conn.execute(
            "UPDATE sandbox_execution_delete_receipts SET deleted_at = ?1 WHERE sandbox_id = ?2",
            crate::db::fixture::params![now - 7 * 24 * 60 * 60 + 60, retained_sandbox_id],
        )
        .unwrap();
        conn.execute(
            "UPDATE sandbox_execution_delete_receipts SET deleted_at = ?1 WHERE sandbox_id = ?2",
            crate::db::fixture::params![now - 7 * 24 * 60 * 60 - 1, expired_sandbox_id],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let retained = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                retained_sandbox_id,
                deleted_generation.get(),
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();
        let expired = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                expired_sandbox_id,
                deleted_generation.get(),
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(retained.outcome).unwrap(),
            TeardownSuspendOutcome::Deleted,
        );
        assert_eq!(
            TeardownSuspendOutcome::try_from(expired.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
    }

    #[tokio::test]
    async fn teardown_suspend_adopts_an_existing_backed_suspend_for_the_sandbox() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        let local_artifact_id = LocalArtifactId::new();
        let previous_generation = ExecutionGeneration::new(6).unwrap();
        crate::db::claim_teardown_suspend_operation(&conn, &sandbox_id, previous_generation)
            .unwrap();
        crate::db::complete_teardown_suspend(&conn, &sandbox_id, previous_generation, None)
            .unwrap();
        assert_eq!(
            crate::db::teardown_suspend_status(&conn, &sandbox_id)
                .unwrap()
                .unwrap()
                .operation
                .unwrap()
                .status,
            crate::db::TeardownSuspendOperationStatus::Completed,
        );
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id,
            crate::db::CleanupCompletion::MarkSuspended,
            Some((local_artifact_id, "/snapshot")),
        )
        .unwrap();
        crate::db::mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string())
            .unwrap();
        let service = service_for_teardown_test(conn);
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.clone(),
                7,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap()
            .into_inner();
        drop(guard);

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Durable,
        );
        let published_artifact = response
            .published_artifact
            .expect("durable response must identify the exact published artifact");
        assert_eq!(
            published_artifact.local_artifact_id,
            local_artifact_id.to_string()
        );
        assert_eq!(
            published_artifact.artifact_kind,
            crate::proto::ArtifactKind::Full as i32
        );
        assert_eq!(
            published_artifact.origin,
            crate::proto::ArtifactOrigin::Suspend as i32
        );
        let db = service.db.lock().expect("mutex poisoned");
        let status = crate::db::teardown_suspend_status(&db, &sandbox_id)
            .unwrap()
            .unwrap();
        assert!(status.is_durable_for(ExecutionGeneration::new(7).unwrap()));
        drop(db);

        let events = capture.events.lock().unwrap();
        for event_type in ["execution_suspend_requested", "execution_suspend_completed"] {
            let event = events
                .iter()
                .find(|event| {
                    event
                        .get("event_type")
                        .is_some_and(|value| value == event_type)
                })
                .unwrap_or_else(|| panic!("missing {event_type} event: {events:?}"));
            assert_eq!(event.get("trigger").map(String::as_str), Some("system"));
            assert_eq!(
                event.get("execution_generation").map(String::as_str),
                Some("7")
            );
        }
    }

    #[tokio::test]
    async fn teardown_suspend_upgrades_same_generation_legacy_completed_journal() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let generation = ExecutionGeneration::new(7).unwrap();
        let local_artifact_id = LocalArtifactId::new();

        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        crate::db::claim_teardown_suspend_operation(&conn, &sandbox_id, generation).unwrap();
        assert!(
            crate::db::complete_teardown_suspend(&conn, &sandbox_id, generation, None).unwrap()
        );
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id,
            crate::db::CleanupCompletion::MarkSuspended,
            Some((local_artifact_id, "/snapshot")),
        )
        .unwrap();
        crate::db::mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string())
            .unwrap();

        let service = service_for_teardown_test(conn);
        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.clone(),
                generation.get(),
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .expect("legacy same-generation journal should upgrade in place")
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Durable,
        );
        assert_eq!(
            response
                .published_artifact
                .expect("durable response must identify the bound artifact")
                .local_artifact_id,
            local_artifact_id.to_string(),
        );
        let db = service.db.lock().expect("mutex poisoned");
        assert!(
            crate::db::teardown_suspend_status(&db, &sandbox_id)
                .unwrap()
                .unwrap()
                .is_durable_for(generation)
        );
    }

    #[test]
    fn teardown_suspend_event_trigger_uses_forwarded_causality() {
        let request = Request::new(prepare_teardown_suspend_request(
            SandboxId::new().to_string(),
            1,
            TeardownSuspendAction::BeginOrJoin,
        ));
        assert_eq!(
            teardown_suspend_event_trigger(&request).unwrap(),
            SandboxEventTrigger::System,
        );

        let mut request = request;
        request.metadata_mut().insert(
            SUSPEND_TRIGGER_METADATA,
            tonic::metadata::MetadataValue::from_static("user"),
        );
        assert_eq!(
            teardown_suspend_event_trigger(&request).unwrap(),
            SandboxEventTrigger::User,
        );
    }

    #[tokio::test]
    async fn planned_teardown_suspend_requested_event_retains_execution_generation() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let service = service_for_teardown_test(conn);
        let wake_guard = service
            .auto_lifecycle
            .wake_lock(&sandbox_id.to_string())
            .lock_owned()
            .await;
        let artifact_guard = service
            .auto_lifecycle
            .artifact_lock(&sandbox_id.to_string())
            .lock_owned()
            .await;
        let context = SuspendRequestContext::for_teardown(
            sandbox_id.to_string(),
            ExecutionGeneration::new(7).unwrap(),
        );
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.prepare_suspend_snapshot_publication_after_locks(
                &context,
                &wake_guard,
                &artifact_guard,
            ),
        )
        .await;
        drop(guard);

        let events = capture.events.lock().unwrap();
        let event = events
            .iter()
            .find(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "suspend_requested")
            })
            .unwrap_or_else(|| panic!("missing suspend_requested event: {events:?}"));
        assert_eq!(
            event.get("operation").map(String::as_str),
            Some("planned_teardown")
        );
        assert_eq!(
            event.get("execution_generation").map(String::as_str),
            Some("7")
        );
    }

    #[tokio::test]
    async fn stale_teardown_poll_cannot_use_a_newer_execution_artifact_as_release_proof() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let local_artifact_id = LocalArtifactId::new();
        crate::db::begin_teardown_suspend(&conn, &sandbox_id, ExecutionGeneration::new(2).unwrap())
            .unwrap();
        crate::db::stage_teardown_suspend_artifact(
            &conn,
            &sandbox_id,
            ExecutionGeneration::new(2).unwrap(),
            local_artifact_id,
            "/snapshot",
        )
        .unwrap();
        crate::db::mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string())
            .unwrap();
        crate::db::complete_row_intent_cleanup(
            &conn,
            &sandbox_id,
            crate::db::CleanupCompletion::MarkSuspended,
            Some((local_artifact_id, "/snapshot")),
        )
        .unwrap();
        crate::db::complete_teardown_suspend(
            &conn,
            &sandbox_id,
            ExecutionGeneration::new(2).unwrap(),
            Some(local_artifact_id),
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id,
                1,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Superseded,
            "a delayed poll must detach instead of accepting the replacement execution's artifact",
        );
    }

    #[tokio::test]
    async fn newer_teardown_request_replaces_the_local_lease_before_driving_suspend() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        crate::db::begin_teardown_suspend(
            &conn,
            &sandbox_id.to_string(),
            ExecutionGeneration::new(1).unwrap(),
        )
        .unwrap();
        crate::db::test_fixtures::state_write_fixtures::apply_edge(
            &conn,
            &sandbox_id.to_string(),
            &crate::db::edges::SUSPEND_ROLLBACK,
        );
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(2).unwrap(), 8),
        )
        .unwrap();
        let service = service_for_teardown_test(conn);
        let wake_guard = service
            .auto_lifecycle
            .wake_lock(&sandbox_id.to_string())
            .lock_owned()
            .await;

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                2,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
        let db = service.db.lock().expect("mutex poisoned");
        let operation = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .operation
            .expect("new execution must own a local teardown lease");
        assert_eq!(operation.execution_generation.get(), 2);
        drop(db);
        drop(wake_guard);
    }

    #[tokio::test]
    async fn replacement_runtime_fences_an_already_spawned_teardown_driver() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
        crate::db::claim_teardown_suspend_operation(
            &conn,
            &sandbox_id.to_string(),
            ExecutionGeneration::new(1).unwrap(),
        )
        .unwrap();
        let service = service_for_teardown_test(conn);
        let wake_guard = service
            .auto_lifecycle
            .wake_lock(&sandbox_id.to_string())
            .lock_owned()
            .await;

        service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                1,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap();
        assert_eq!(service.teardown_suspend_drivers.len(), 1);

        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        let first_claim = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::update_runtime_fields(
                &db,
                sandbox_id,
                crate::db::RuntimeFields {
                    uid: 10000,
                    slot: 8,
                    guest_cid: 38,
                    pid: std::process::id(),
                    spaced_host_pid: Some(10003),
                    netgtw_pid: 10004,
                    vsock_uds_path: "/space/run/firecracker/sandboxes/replacement/root/v.sock",
                    api_socket_path: "/space/run/firecracker/sandboxes/replacement/api.sock",
                    spaced_endpoint_proof: None,
                    space_api_route_identity:
                        pplx_space_common_core::types::SpaceApiRouteIdentity {
                            execution_generation: Some(ExecutionGeneration::new(2).unwrap()),
                            ..pplx_space_common_core::types::SpaceApiRouteIdentity::default()
                        },
                    route_identity_provenance:
                        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                },
            )
            .unwrap()
        };
        assert_eq!(first_claim, Some(ExecutionGeneration::new(1).unwrap()));
        let repeated_claim = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::claim_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                ExecutionGeneration::new(2).unwrap(),
            )
            .unwrap()
        };
        assert_eq!(repeated_claim, None);
        drop(guard);
        drop(wake_guard);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !service.teardown_suspend_drivers.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stale teardown driver must exit");

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap();
        assert_eq!(
            row.state,
            SandboxState::Paused,
            "generation 1 must not start suspending the generation 2 runtime",
        );
        let operation = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .operation
            .expect("replacement runtime must own the local execution lease");
        assert_eq!(operation.execution_generation.get(), 2);
        drop(db);

        let events = capture.events.lock().unwrap();
        let superseded = events
            .iter()
            .filter(|event| {
                event
                    .get("event_type")
                    .is_some_and(|value| value == "execution_suspend_superseded")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            superseded.len(),
            1,
            "advancing a local execution lease must emit one deduplicated supersession event: {events:?}",
        );
        assert_eq!(
            superseded[0]
                .get("execution_generation")
                .map(String::as_str),
            Some("1")
        );
        assert!(
            superseded[0]
                .get("reason")
                .is_some_and(|reason| reason.contains('2')),
            "the supersession reason must identify the replacement generation: {superseded:?}",
        );
    }

    #[tokio::test]
    async fn observing_a_runtime_lease_does_not_start_teardown() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        let generation = ExecutionGeneration::new(3).unwrap();
        crate::db::update_runtime_fields(&conn, sandbox_id, runtime_fields(generation, 7)).unwrap();
        crate::db::claim_teardown_suspend_operation(&conn, &sandbox_id.to_string(), generation)
            .unwrap();
        let service = service_for_teardown_test(conn);

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                3,
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
        assert!(service.teardown_suspend_drivers.is_empty());
        let db = service.db.lock().expect("mutex poisoned");
        let operation = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .operation
            .unwrap();
        assert_eq!(
            operation.status,
            crate::db::TeardownSuspendOperationStatus::Pending,
        );
    }

    #[tokio::test]
    async fn teardown_suspend_observes_an_in_progress_sandbox_suspend() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let generation = teardown_generation();
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id.parse().unwrap(),
            runtime_fields(generation, 7),
        )
        .unwrap();
        crate::db::begin_teardown_suspend(&conn, &sandbox_id, generation).unwrap();
        let service = service_for_teardown_test(conn);

        let response = service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.clone(),
                generation.get(),
                TeardownSuspendAction::ObserveOnly,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            TeardownSuspendOutcome::try_from(response.outcome).unwrap(),
            TeardownSuspendOutcome::Pending,
        );
        let db = service.db.lock().expect("mutex poisoned");
        let operation = crate::db::teardown_suspend_status(&db, &sandbox_id)
            .unwrap()
            .unwrap()
            .operation
            .expect("the matching in-progress suspend journal must be preserved");
        assert_eq!(operation.execution_generation, generation);
        assert_eq!(
            operation.status,
            crate::db::TeardownSuspendOperationStatus::Running,
        );
    }

    #[tokio::test]
    async fn teardown_suspend_polling_keeps_one_driver_per_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id,
            runtime_fields(teardown_generation(), 7),
        )
        .unwrap();
        crate::db::begin_teardown_suspend(&conn, &sandbox_id.to_string(), teardown_generation())
            .unwrap();
        let service = service_for_teardown_test(conn);
        let wake_lock = service.auto_lifecycle.wake_lock(&sandbox_id.to_string());
        let wake_guard = wake_lock.lock_owned().await;

        for _ in 0..2 {
            service
                .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                    sandbox_id.to_string(),
                    1,
                    TeardownSuspendAction::ObserveOnly,
                )))
                .await
                .unwrap();
        }

        assert_eq!(service.teardown_suspend_drivers.len(), 1);
        assert!(service.teardown_suspend_drivers.get(&sandbox_id).is_some());
        drop(wake_guard);
    }

    #[tokio::test]
    async fn existing_teardown_driver_does_not_compete_for_saturated_capacity() {
        let conn = crate::db::test_conn();
        let service = service_for_teardown_test(conn);
        let sandbox_id = SandboxId::new();
        let existing_permit = Arc::clone(&service.teardown_suspend_driver_sem)
            .try_acquire_owned()
            .unwrap();
        service
            .teardown_suspend_drivers
            .insert(sandbox_id, existing_permit);
        let mut held_permits = Vec::new();
        while let Ok(permit) = Arc::clone(&service.teardown_suspend_driver_sem).try_acquire_owned()
        {
            held_permits.push(permit);
        }

        assert_eq!(
            service.spawn_teardown_suspend_driver(
                sandbox_id,
                teardown_generation(),
                SandboxEventTrigger::System,
            ),
            TeardownSuspendDriverAdmission::Existing,
        );
    }

    #[tokio::test]
    async fn teardown_suspend_bounds_drivers_across_sandboxes() {
        let conn = crate::db::test_conn();
        let sandbox_ids = (0..=TEARDOWN_SUSPEND_DRIVER_CONCURRENCY)
            .map(|_| SandboxId::new())
            .collect::<Vec<_>>();
        for sandbox_id in &sandbox_ids {
            insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
            crate::db::update_runtime_fields(
                &conn,
                *sandbox_id,
                runtime_fields(teardown_generation(), 7),
            )
            .unwrap();
        }

        let shutdown_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                shutdown_token: shutdown_token.clone(),
                task_tracker: task_tracker.clone(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let mut wake_guards = Vec::new();
        for sandbox_id in &sandbox_ids {
            wake_guards.push(
                service
                    .auto_lifecycle
                    .wake_lock(&sandbox_id.to_string())
                    .lock_owned()
                    .await,
            );
            service
                .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                    sandbox_id.to_string(),
                    1,
                    TeardownSuspendAction::BeginOrJoin,
                )))
                .await
                .unwrap();
        }

        assert_eq!(
            service.teardown_suspend_drivers.len(),
            TEARDOWN_SUSPEND_DRIVER_CONCURRENCY,
        );
        assert!(
            !service
                .teardown_suspend_drivers
                .contains_key(sandbox_ids.last().unwrap()),
            "a saturated node must leave the next sandbox for its later poll instead of spawning an unbounded driver",
        );

        shutdown_token.cancel();
        task_tracker.close();
        tokio::time::timeout(Duration::from_secs(1), task_tracker.wait())
            .await
            .expect("teardown drivers should participate in service shutdown");
        drop(wake_guards);
    }

    #[tokio::test]
    async fn teardown_suspend_driver_drains_on_service_shutdown() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Paused);
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(9).unwrap(), 7),
        )
        .unwrap();
        let shutdown_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                shutdown_token: shutdown_token.clone(),
                task_tracker: task_tracker.clone(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let wake_lock = service.auto_lifecycle.wake_lock(&sandbox_id.to_string());
        let wake_guard = wake_lock.lock_owned().await;
        task_tracker.close();

        service
            .prepare_teardown_suspend(internal_request(prepare_teardown_suspend_request(
                sandbox_id.to_string(),
                9,
                TeardownSuspendAction::BeginOrJoin,
            )))
            .await
            .unwrap();
        assert_eq!(service.teardown_suspend_drivers.len(), 1);

        shutdown_token.cancel();
        tokio::time::timeout(Duration::from_secs(1), task_tracker.wait())
            .await
            .expect("teardown driver should participate in service shutdown");
        assert!(
            service.teardown_suspend_drivers.is_empty(),
            "shutdown must release the in-process teardown driver slot"
        );

        drop(wake_guard);
    }

    /// [`service_with_runtime_probe`] with its host dependencies under the
    /// caller's `sockets` directory.
    pub(crate) fn service_with_runtime_liveness_and_absent_sockets(
        conn: crate::db::Connection,
        runtime_live: bool,
        sockets: &std::path::Path,
    ) -> SandboxManagerService {
        SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                sandbox_runtime_live: Arc::new(move |_| runtime_live),
                ..SandboxManagerServiceOptions::for_test_in(sockets)
            },
        )
    }

    fn insert_suspending_resuspend(conn: &crate::db::Connection, id: &str) -> LocalArtifactId {
        insert_test_sandbox(conn, id, SandboxState::Paused);
        let predecessor = LocalArtifactId::new();
        conn.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = ?1,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?2",
            crate::db::fixture::params![predecessor.to_string(), id],
        )
        .unwrap();
        assert!(crate::db::begin_suspend(conn, id).unwrap());
        predecessor
    }

    #[tokio::test]
    async fn suspend_snapshot_publication_failure_preserves_restored_base_for_live_retry() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let predecessor = insert_suspending_resuspend(&conn, &sandbox_id);

        let service = service_with_runtime_probe(conn, true);
        service.record_suspend_snapshot_publication_failure_after_begin(
            &sandbox_id,
            "snapshot failed",
        );

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert!(row.failure_reason.is_none());
        assert_eq!(row.current_local_artifact_id, Some(predecessor));
        assert_eq!(row.current_local_artifact_kind, Some(ArtifactKind::Full));
        assert_eq!(
            row.current_local_artifact_origin,
            Some(ArtifactOrigin::Suspend)
        );
        assert!(
            !crate::snapshot_encryption::capture_volume_is_fresh(&row),
            "the retry must reuse the restored encrypted memory base"
        );
    }

    #[test]
    fn a_successful_cold_start_captures_a_fresh_memory_volume() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Starting);
        conn.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = ?1,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?2",
            crate::db::fixture::params![
                pplx_space_common_core::types::LocalArtifactId::new().to_string(),
                sandbox_id
            ],
        )
        .unwrap();
        let before = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert!(!crate::snapshot_encryption::capture_volume_is_fresh(
            &before
        ));

        assert!(crate::db::mark_start_succeeded(&conn, &sandbox_id, None).unwrap());

        let started = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert!(
            crate::snapshot_encryption::capture_volume_is_fresh(&started),
            "a successful cold start must not reuse an older suspend memory image"
        );
    }

    #[test]
    fn failed_encrypted_capture_keeps_live_memory_base_mounted() {
        assert!(!super::grpc_lifecycle::failed_encrypted_capture_may_release_volume(true));
        assert!(super::grpc_lifecycle::failed_encrypted_capture_may_release_volume(false));
    }

    #[tokio::test]
    async fn node_local_backup_fence_failure_preserves_runtime_and_rolls_back_to_paused() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        let generation = ExecutionGeneration::new(2).unwrap();
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id.parse().unwrap(),
            runtime_fields(generation, 7),
        )
        .unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let service = service_with_runtime_probe(conn, true);
        let begin_failure = super::grpc_lifecycle::artifact_backup_fence_status(
            "ClusterApi BeginArtifactBackup unavailable".to_string(),
        );

        let returned = service
            .require_node_local_backup_handoff(&sandbox_id, local_artifact_id, Err(begin_failure))
            .expect_err("failed Begin must stop before runtime finalization");

        assert_eq!(returned.code(), tonic::Code::Internal);
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert_eq!(row.pid, Some(std::process::id()));
        assert_eq!(
            row.space_api_route_identity.execution_generation,
            Some(generation)
        );
        assert!(row.current_local_artifact_id.is_none());
    }

    #[tokio::test]
    async fn deduplicated_artifact_backup_returns_the_durable_jobs_attempt_id() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = SandboxManagerService::for_test(conn);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let snapshot_root = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let snapshot_dir = snapshot_root.path().join("snapshot");
        write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"deduplicated-backup",
        );
        let upload_config = crate::snapshot_upload::SnapshotUploadConfig::from_uri(
            "s3://test-bucket/prefix?region=us-east-1",
        )
        .unwrap();
        let first_attempt = pplx_space_common_core::types::BackupAttemptId::new();

        let first = service
            .submit_artifact_backup_for_test(
                &sandbox,
                (local_artifact_id, first_attempt),
                snapshot_dir.to_str().unwrap(),
                &upload_config,
                false,
            )
            .await
            .unwrap();
        let second_attempt = pplx_space_common_core::types::BackupAttemptId::new();
        let second = service
            .submit_artifact_backup_for_test(
                &sandbox,
                (local_artifact_id, second_attempt),
                snapshot_dir.to_str().unwrap(),
                &upload_config,
                false,
            )
            .await
            .unwrap();

        let SubmitSuspendSnapshotOutcome::Enqueued {
            job_id: first_job_id,
            backup_attempt_id: first_returned_attempt,
        } = first
        else {
            panic!("configured upload must enqueue");
        };
        let SubmitSuspendSnapshotOutcome::Enqueued {
            job_id: second_job_id,
            backup_attempt_id: second_returned_attempt,
        } = second
        else {
            panic!("configured upload must enqueue");
        };
        assert_eq!(second_job_id, first_job_id);
        assert_eq!(first_returned_attempt, first_attempt.to_string());
        assert_eq!(second_returned_attempt, first_attempt.to_string());
        assert_ne!(second_returned_attempt, second_attempt.to_string());
    }

    #[tokio::test]
    async fn restore_visible_backup_requires_begin_before_staging() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = SandboxManagerService::for_test(conn);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let snapshot_root = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let snapshot_dir = snapshot_root.path().join("snapshot");
        write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"restore-visible-backup",
        );
        let upload_config = crate::snapshot_upload::SnapshotUploadConfig::from_uri(
            "s3://test-bucket/prefix?region=us-east-1",
        )
        .unwrap();
        let attempt_id = pplx_space_common_core::types::BackupAttemptId::new();

        let error = service
            .submit_artifact_backup_for_test(
                &sandbox,
                (local_artifact_id, attempt_id),
                snapshot_dir.to_str().unwrap(),
                &upload_config,
                true,
            )
            .await
            .expect_err("unavailable ClusterApi must prevent a restore-visible response");

        assert!(
            error.contains("begin restore-visible snapshot backup"),
            "the response must be gated at the ClusterApi Begin boundary: {error}"
        );
        assert!(
            !error.contains("stage snapshot artifacts in vol-mgr"),
            "ClusterApi rejection must happen before volume staging: {error}"
        );
        let job = service
            .snapshot_upload_worker()
            .find_job_by_dedup_key(
                crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                &local_artifact_id.to_string(),
            )
            .unwrap()
            .expect("failed handoff must retain its durable retry owner");
        let payload = job
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .unwrap();
        assert_eq!(payload.snapshot_id, attempt_id.to_string());
        assert!(payload.job_ids.is_empty());
    }

    #[test]
    fn teardown_joins_a_keyed_upload_unless_a_new_attempt_can_succeed() {
        use crate::snapshot_upload::UploadFailureScope::{Artifact, Attempt};
        use KeyedUploadUse::{Join, NewAttempt};
        use pplx_space_common_jobs::JobStatus::{
            Cancelled, Cancelling, Completed, Failed, Pending, Running,
        };
        let retriable = Some(pplx_space_common_jobs::JobFailure::Retriable);
        let permanent = Some(pplx_space_common_jobs::JobFailure::Permanent { reason: None });
        // The last column but one counts the artifact's permanently failed
        // uploads, the newest included when it is one of them.
        let cases = [
            // An active upload owns the backup.
            (Pending, None, Attempt, 0, Join),
            (Running, None, Artifact, 1, Join),
            (Cancelling, None, Attempt, 2, Join),
            // An upload that ended without a failure answers at once.
            (Completed, None, Attempt, 0, Join),
            (Completed, None, Attempt, 2, Join),
            (Cancelled, None, Attempt, 0, Join),
            // A retriable failure only ran out of attempts.
            (Failed, retriable, Attempt, 0, NewAttempt),
            (Failed, retriable, Attempt, 2, NewAttempt),
            // An attempt's permanent failure gets one recovery attempt: the
            // original and its recovery may fail, and no more.
            (Failed, permanent, Attempt, 1, NewAttempt),
            (Failed, permanent, Attempt, 2, Join),
            (Failed, permanent, Attempt, 3, Join),
            // A permanent failure of the artifact itself is final.
            (Failed, permanent, Artifact, 1, Join),
        ];

        for (status, failure, scope, permanently_failed, expected) in cases {
            assert_eq!(
                teardown_keyed_upload_use(status, failure, scope, permanently_failed),
                expected,
                "{status:?} {failure:?} {scope:?} {permanently_failed}"
            );
        }
    }

    #[tokio::test]
    async fn auto_suspend_backup_fence_failure_is_bounded_to_one_idle_epoch() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 1,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        }
        .normalized();
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Paused, &policy);
        let mut runtime_state = initial_runtime_state(&policy, now_secs().saturating_sub(10));
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();
        let service = SandboxManagerService::for_test(conn);
        let error = super::grpc_lifecycle::artifact_backup_fence_status(
            "ClusterApi BeginArtifactBackup unavailable".to_string(),
        );

        service
            .handle_auto_lifecycle_error(&sandbox_id, sandbox_id.parse().unwrap(), &error)
            .await;

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let condition = row
            .auto_lifecycle_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .expect("access-idle condition remains persisted");
        assert!(!condition.armed);
        assert_eq!(row.auto_lifecycle_state.next_auto_event_at, None);
        assert!(due_stage(&policy, &row.auto_lifecycle_state, u64::MAX).is_none());
        assert!(
            row.auto_lifecycle_state
                .last_auto_error
                .as_deref()
                .is_some_and(|error| error.contains("BeginArtifactBackup unavailable"))
        );
    }

    #[tokio::test]
    async fn suspend_snapshot_publication_failure_quarantines_when_runtime_is_gone() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);

        let service = service_with_runtime_probe(conn, false);
        service
            .record_suspend_snapshot_publication_failure_after_begin(&sandbox_id, "publish failed");

        {
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
            assert_eq!(row.state, SandboxState::Quarantined);
            assert_eq!(row.failure_reason.as_deref(), Some("publish failed"));
            let quarantine: (String, String, String) = db
                .query_row(
                    "SELECT source, reason_code, next_action
                       FROM sandbox_quarantine_state
                      WHERE sandbox_id = ?1",
                    [&sandbox_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(quarantine.0, "sbx_manager");
            assert_eq!(quarantine.1, "suspend_snapshot_publication_failed");
            assert_eq!(quarantine.2, "requires_operator_attention");
        }

        let published =
            <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::state(
                &service,
                internal_request(StateRequest {
                    sandbox_ids: vec![sandbox_id.clone()],
                }),
            )
            .await
            .expect("quarantined sandbox should remain publishable")
            .into_inner()
            .sandboxes
            .pop()
            .expect("quarantined sandbox should be returned");
        assert_eq!(
            published.observed_lifecycle().unwrap(),
            crate::proto::SandboxLifecycleState::Quarantined
        );
        assert_eq!(
            published.quarantine_reason_code.as_deref(),
            Some("suspend_snapshot_publication_failed")
        );
    }

    #[tokio::test]
    async fn state_forwards_direct_failure_quarantine_reasons() {
        let conn = crate::db::test_conn();
        let cases = [
            (
                SandboxId::new().to_string(),
                SandboxState::Checkpointing,
                SandboxQuarantineReasonCode::CheckpointResumeFailed,
            ),
            (
                SandboxId::new().to_string(),
                SandboxState::Resuming,
                SandboxQuarantineReasonCode::ResidentResumeFailed,
            ),
            (
                SandboxId::new().to_string(),
                SandboxState::Started,
                SandboxQuarantineReasonCode::SnapshotRollbackCleanupFailed,
            ),
        ];
        for (sandbox_id, state, reason) in &cases {
            insert_test_sandbox(&conn, sandbox_id, *state);
            assert!(
                crate::db::mark_quarantined_from_state(
                    &conn,
                    sandbox_id,
                    *state,
                    SandboxQuarantineSource::SbxManager,
                    *reason,
                    "runtime outcome is ambiguous",
                    SandboxQuarantineNextAction::RequiresOperatorAttention,
                )
                .unwrap()
            );
        }
        let service = SandboxManagerService::for_test(conn);

        let response =
            <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::state(
                &service,
                internal_request(StateRequest {
                    sandbox_ids: cases
                        .iter()
                        .map(|(sandbox_id, _, _)| sandbox_id.clone())
                        .collect(),
                }),
            )
            .await
            .unwrap()
            .into_inner();

        for (sandbox_id, _, reason) in &cases {
            let published = response
                .sandboxes
                .iter()
                .find(|sandbox| sandbox.sandbox_id == *sandbox_id)
                .unwrap();
            assert_eq!(
                published.observed_lifecycle().unwrap(),
                crate::proto::SandboxLifecycleState::Quarantined
            );
            assert_eq!(
                published.quarantine_reason_code.as_deref(),
                Some(reason.as_str())
            );
        }
    }

    #[tokio::test]
    async fn state_degrades_bad_metadata_without_rejecting_healthy_siblings() {
        let conn = crate::db::test_conn();
        let quarantined_id = SandboxId::new().to_string();
        let malformed_receipt_id = SandboxId::new().to_string();
        let malformed_core_id = SandboxId::new().to_string();
        let malformed_provenance_id = SandboxId::new().to_string();
        let healthy_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &quarantined_id, SandboxState::Suspending);
        insert_test_sandbox(&conn, &malformed_receipt_id, SandboxState::Suspended);
        insert_test_sandbox(&conn, &healthy_id, SandboxState::Created);
        insert_test_sandbox(&conn, &malformed_core_id, SandboxState::Created);
        insert_test_sandbox(&conn, &malformed_provenance_id, SandboxState::Created);

        let service = service_with_runtime_probe(conn, false);
        service.record_suspend_snapshot_publication_failure_after_begin(
            &quarantined_id,
            "publish failed",
        );
        {
            let db = service.db.lock().expect("mutex poisoned");
            db.execute(
                "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                [&quarantined_id],
            )
            .unwrap();
            db.pragma_update(None, "ignore_check_constraints", true)
                .unwrap();
            db.execute(
                "UPDATE sandboxes
                    SET transition_receipt_id = ?1
                  WHERE id = ?2",
                [
                    SandboxCleanupEpisodeId::new().to_string(),
                    malformed_receipt_id.clone(),
                ],
            )
            .unwrap();
            db.pragma_update(None, "ignore_check_constraints", false)
                .unwrap();
            crate::db::seed_historical_lifecycle_rows(&db, |db| {
                db.execute(
                    "UPDATE sandboxes SET state = 'state_from_newer_worker' WHERE id = ?1",
                    [&malformed_core_id],
                )
                .unwrap();
            });
            db.execute(
                "UPDATE sandboxes
                    SET last_observed_route_generation = 7,
                        last_observed_execution_generation = 9,
                        last_observed_route_generation_substrate_kind = 'space'
                  WHERE id = ?1",
                [&malformed_core_id],
            )
            .unwrap();
            db.execute(
                "UPDATE sandboxes
                    SET last_observed_route_generation = 11,
                        last_observed_execution_generation = 13,
                        last_observed_route_generation_substrate_kind = 'space'
                  WHERE id = ?1",
                [&malformed_receipt_id],
            )
            .unwrap();
            db.execute(
                "UPDATE sandboxes
                    SET last_observed_route_generation = 17,
                        last_observed_execution_generation = 17,
                        last_observed_route_generation_substrate_kind = 'future-substrate'
                  WHERE id = ?1",
                [&malformed_provenance_id],
            )
            .unwrap();
        }

        let response =
            <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::state(
                &service,
                internal_request(StateRequest {
                    sandbox_ids: vec![
                        quarantined_id.clone(),
                        malformed_receipt_id.clone(),
                        malformed_core_id.clone(),
                        malformed_provenance_id.clone(),
                        healthy_id.clone(),
                    ],
                }),
            )
            .await
            .expect("one malformed row must not reject a node state batch")
            .into_inner();

        let listed =
            <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::list(
                &service,
                internal_request(crate::proto::ListRequest::default()),
            )
            .await
            .expect("a malformed row must not poison full inventory enumeration")
            .into_inner();

        assert_eq!(response.sandboxes.len(), 5);
        assert_eq!(listed.sandboxes.len(), 5);
        assert!(!response.missing_ids.contains(&malformed_core_id));
        let malformed_core = response
            .sandboxes
            .iter()
            .find(|sandbox| sandbox.sandbox_id == malformed_core_id)
            .expect("a malformed local row must still report its node lifecycle");
        malformed_core.observed_lifecycle().unwrap_err();
        assert!(malformed_core.state_degraded);
        assert_eq!(malformed_core.route_provenance_generation, Some(9));
        assert_eq!(
            malformed_core.route_provenance_execution_generation,
            Some(9)
        );
        assert_eq!(
            malformed_core.route_provenance_substrate_kind,
            crate::proto::SandboxSubstrateKind::Direct as i32
        );
        let quarantined = response
            .sandboxes
            .iter()
            .find(|sandbox| sandbox.sandbox_id == quarantined_id)
            .unwrap();
        assert_eq!(
            quarantined.observed_lifecycle().unwrap(),
            crate::proto::SandboxLifecycleState::Quarantined
        );
        assert_eq!(
            quarantined.quarantine_reason_code.as_deref(),
            Some(SandboxQuarantineReasonCode::NodePublishedQuarantined.as_str())
        );
        let malformed = response
            .sandboxes
            .iter()
            .find(|sandbox| sandbox.sandbox_id == malformed_receipt_id)
            .unwrap();
        assert!(malformed.transition_receipt.is_none());
        assert_eq!(malformed.route_provenance_generation, Some(13));
        assert_eq!(malformed.route_provenance_execution_generation, Some(13));
        assert_eq!(
            malformed.route_provenance_substrate_kind,
            crate::proto::SandboxSubstrateKind::Direct as i32
        );
        let malformed_provenance = response
            .sandboxes
            .iter()
            .find(|sandbox| sandbox.sandbox_id == malformed_provenance_id)
            .expect("invalid provenance must degrade only its own sandbox");
        assert!(malformed_provenance.state_degraded);
        assert_eq!(malformed_provenance.route_provenance_generation, None);
        assert_eq!(
            malformed_provenance.route_provenance_execution_generation,
            None
        );
        assert_eq!(
            malformed_provenance.route_provenance_substrate_kind,
            crate::proto::SandboxSubstrateKind::Unspecified as i32
        );
        let healthy = response
            .sandboxes
            .iter()
            .find(|sandbox| sandbox.sandbox_id == healthy_id)
            .unwrap();
        assert_eq!(
            healthy.observed_lifecycle().unwrap(),
            crate::proto::SandboxLifecycleState::Created
        );
        assert!(!healthy.state_degraded);
    }

    #[tokio::test]
    async fn state_forwards_unknown_quarantine_reason_across_version_skew() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);

        let service = service_with_runtime_probe(conn, false);
        service
            .record_suspend_snapshot_publication_failure_after_begin(&sandbox_id, "publish failed");
        {
            let db = service.db.lock().expect("mutex poisoned");
            db.execute(
                "UPDATE sandbox_quarantine_state
                    SET reason_code = 'reason_added_by_newer_sbx_manager'
                  WHERE sandbox_id = ?1",
                [&sandbox_id],
            )
            .unwrap();
        }

        let published =
            <SandboxManagerService as crate::proto::sandbox_manager_server::SandboxManager>::state(
                &service,
                internal_request(StateRequest {
                    sandbox_ids: vec![sandbox_id],
                }),
            )
            .await
            .expect("unknown quarantine reasons must remain publishable")
            .into_inner()
            .sandboxes
            .pop()
            .expect("quarantined sandbox should be returned");
        assert_eq!(
            published.quarantine_reason_code.as_deref(),
            Some("reason_added_by_newer_sbx_manager")
        );
    }

    #[tokio::test]
    async fn suspend_monitoring_guard_rearms_watchdog_only_when_runtime_live() {
        // The encrypted capture unsubscribes the watchdog before the fallible
        // create_snapshot teardown. If the suspend is then abandoned while the
        // runtime is still live (rollback to paused, or a dropped future), the
        // guard must restore watchdog monitoring on drop — but NOT the snapshot
        // scheduler, since the rolled-back state is paused (a paused sandbox is
        // watchdog-monitored, not snapshot-scheduled).
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        crate::db::update_snapshots_enabled(&conn, &sandbox_id, true).unwrap();

        let service = service_with_runtime_probe(conn, true);
        let row = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap()
        };
        // Simulate the pre-capture unsubscribe.
        service.watchdog.remove_sandbox(&sandbox_id).await;
        service.snapshot_scheduler.remove_sandbox(&sandbox_id).await;

        service.drop_armed_suspend_monitoring_guard_for_test(&row);
        // Drop spawns the re-add on the runtime; let it run.
        for _ in 0..20 {
            if service.watchdog.get_health(&sandbox_id).await.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            service.watchdog.get_health(&sandbox_id).await.is_some(),
            "watchdog monitoring must be re-armed when the runtime is still live"
        );
        assert!(
            !service.snapshot_scheduler.is_active(&sandbox_id).await,
            "snapshot scheduling must NOT be re-armed for a paused sandbox"
        );
    }

    #[tokio::test]
    async fn suspend_monitoring_guard_reloads_current_sidecar_authority() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        conn.execute(
            "UPDATE sandboxes SET uid = ?1, spaced_host_pid = ?2, netgtw_pid = ?3 WHERE id = ?4",
            crate::db::fixture::params![20_001, 30_001, 30_002, sandbox_id],
        )
        .unwrap();

        let service = service_with_runtime_probe(conn, true);
        let stale_row = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap()
        };
        service.watchdog.remove_sandbox(&sandbox_id).await;

        // A sidecar replacement commits while the suspend guard still owns
        // its older lifecycle snapshot.
        {
            let db = service.db.lock().expect("mutex poisoned");
            db.execute(
                "UPDATE sandboxes SET spaced_host_pid = ?1, netgtw_pid = ?2 WHERE id = ?3",
                crate::db::fixture::params![40_001, 40_002, sandbox_id],
            )
            .unwrap();
        }
        service.drop_armed_suspend_monitoring_guard_for_test(&stale_row);

        let restored = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Some(info) = service.watchdog.sidecar_info_for_test(&sandbox_id).await {
                    break info;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("watchdog re-arm must complete");
        assert_eq!(restored.spaced_host_pid, Some(40_001));
        assert_eq!(restored.netgtw_pid, Some(40_002));
    }

    #[tokio::test]
    async fn suspend_monitoring_guard_is_noop_when_runtime_gone_or_disarmed() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);

        // Runtime gone → no re-arm even though armed.
        let dead = service_with_runtime_probe(conn, false);
        let row = {
            let db = dead.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap()
        };
        dead.drop_armed_suspend_monitoring_guard_for_test(&row);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            dead.watchdog.get_health(&sandbox_id).await.is_none(),
            "a dead runtime must not be re-armed"
        );

        // Disarmed (intentional teardown) → no re-arm even with a live runtime.
        let live_conn = crate::db::test_conn();
        let sandbox_id2 = SandboxId::new().to_string();
        insert_test_sandbox(&live_conn, &sandbox_id2, SandboxState::Suspending);
        let live = service_with_runtime_probe(live_conn, true);
        let row2 = {
            let db = live.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id2).unwrap().unwrap()
        };
        live.drop_disarmed_suspend_monitoring_guard_for_test(&row2);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            live.watchdog.get_health(&sandbox_id2).await.is_none(),
            "a disarmed guard must not re-arm on drop"
        );
    }

    #[tokio::test]
    async fn suspend_publish_failure_finishes_complete_staging_when_runtime_is_gone() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);

        let cache_dir = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let sandbox_dir = cache_dir.path().join(&sandbox_id);
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id);
        write_full_suspend_artifact(
            &staging_dir,
            &sandbox_id,
            local_artifact_id,
            b"complete-staging",
        );

        let service = service_with_runtime_probe(conn, false);
        let disposition = service.record_suspend_snapshot_publication_failure_after_staging(
            &sandbox_id,
            &sandbox_dir,
            &staging_dir,
            "publish failed",
            &Default::default(),
        );

        assert_eq!(disposition, SuspendSnapshotStagingDisposition::Cleanup);
        assert!(!staging_dir.exists());
        assert!(
            canonical_suspend_snapshot_dir(&sandbox_dir)
                .join("snapshot.json")
                .exists()
        );
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert!(row.failure_reason.is_none());
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
        assert_eq!(
            row.current_local_artifact_kind,
            Some(pplx_space_common_core::types::ArtifactKind::Full)
        );
        assert_eq!(
            row.current_local_artifact_origin,
            Some(pplx_space_common_core::types::ArtifactOrigin::Suspend)
        );
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkSuspended)
        );
        assert!(
            crate::db::get_local_artifact(&db, local_artifact_id)
                .unwrap()
                .is_some(),
            "live failure recovery must register the exact staged artifact"
        );
    }

    #[tokio::test]
    async fn suspend_publish_failure_pins_complete_canonical_when_runtime_is_gone() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);

        let cache_dir = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let sandbox_dir = cache_dir.path().join(&sandbox_id);
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id);
        write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"complete-canonical",
        );

        let service = service_with_runtime_probe(conn, false);
        let disposition = service.record_suspend_snapshot_publication_failure_after_staging(
            &sandbox_id,
            &sandbox_dir,
            &staging_dir,
            "publish failed",
            &Default::default(),
        );

        assert_eq!(disposition, SuspendSnapshotStagingDisposition::Cleanup);
        assert!(snapshot_dir.join("snapshot.json").exists());
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert!(row.failure_reason.is_none());
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
        assert!(
            crate::db::get_local_artifact(&db, local_artifact_id)
                .unwrap()
                .is_some(),
            "live failure recovery must register the exact canonical artifact"
        );
    }

    #[tokio::test]
    async fn suspend_snapshot_publication_failure_does_not_clobber_non_suspending_state() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);

        let service = service_with_runtime_probe(conn, false);
        service
            .record_suspend_snapshot_publication_failure_after_begin(&sandbox_id, "stale failure");

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert!(row.failure_reason.is_none());
    }

    fn insert_orphaned_test_sandbox(conn: &crate::db::Connection, id: &str, state: SandboxState) {
        let sandbox_id = SandboxId::from_str(id).unwrap();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
        crate::db::insert_sandbox(
            conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id,
                uuid8: &uuid8,
                state,
                template: ORPHANED_SANDBOX_TEMPLATE,
                template_hash: None,
                user: &ORPHANED_SANDBOX_OWNER,
                runtime: RuntimeKind::Firecracker.as_str(),
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: None,
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
    }

    #[tokio::test]
    async fn restart_drain_rejects_new_writers_and_waits_for_active_transitions() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);

        let writer = service.state_writer_lease("Start").unwrap();
        assert_eq!(service.active_state_transition_count(), 1);

        service.begin_draining_for_restart();
        let error = match service.state_writer_lease("Start") {
            Ok(_) => panic!("new state transitions must be rejected while draining"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("state changes are draining"));
        assert_eq!(service.active_state_transition_count(), 1);

        let waiter_service = service.clone();
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            waiter_service
                .wait_for_state_transitions_drained_after_observation(observed_tx)
                .await;
        });
        observed_rx
            .await
            .expect("drain waiter should observe the active state writer");
        assert!(
            !waiter.is_finished(),
            "drain waiter must wait for active state writers"
        );

        drop(writer);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("drain waiter should finish after the last writer exits")
            .unwrap();
        assert_eq!(service.active_state_transition_count(), 0);
    }

    #[tokio::test]
    async fn restart_drain_reports_active_state_transition_methods() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);

        let first_start = service.state_writer_lease("Start").unwrap();
        let wake = service.state_writer_lease("Wake").unwrap();
        let second_start = service.state_writer_lease("Start").unwrap();

        assert_eq!(
            service.active_state_transition_methods(),
            vec![("Start", 2), ("Wake", 1)]
        );
        drop(first_start);
        assert_eq!(
            service.active_state_transition_methods(),
            vec![("Start", 1), ("Wake", 1)]
        );
        drop(second_start);
        drop(wake);
        assert!(service.active_state_transition_methods().is_empty());
    }

    #[tokio::test]
    async fn dependency_drain_after_cleanup_commit_releases_rpc_and_recovers_job() {
        use crate::db::OptionalExtension as _;
        use pplx_space_common_jobs::db as jobs;

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = service_for_teardown_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        *service.cleanup_executor.dispatch.lock().unwrap() =
            Some(Arc::new(|_, _| Box::pin(async { Ok(()) })));
        // Admit the real blocking RPC, but prevent its durable attempt from
        // acquiring a writer until dependency replacement has closed admission.
        let capacity = service
            .cleanup_sem
            .clone()
            .acquire_many_owned(service.cleanup_sem.available_permits() as u32)
            .await
            .unwrap();
        let rpc_service = service.clone();
        let rpc_sandbox_id = sandbox_id.clone();
        let mut rpc = tokio::spawn(async move {
            call_through_restart_admission(
                &rpc_service,
                "/space.sandbox.manager.v1.SandboxManager/Delete",
                internal_request(DeleteRequest {
                    sandbox_id: rpc_sandbox_id,
                    template_filter: None,
                    blocking: true,
                }),
                |service, request| async move { service.delete(request).await },
            )
            .await
        });
        let job_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let id = service
                    .db
                    .lock()
                    .unwrap()
                    .query_row(
                        "SELECT job_id FROM jobs WHERE kind = 'sandbox_cleanup' LIMIT 1",
                        [],
                        |row| row.get::<_, JobId>(0),
                    )
                    .optional()
                    .unwrap();
                if let Some(id) = id {
                    break id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let admission = locks.lock_admission();
        drop(capacity);
        let result = tokio::time::timeout(Duration::from_secs(5), &mut rpc).await;
        if result.is_err() {
            rpc.abort();
            assert!(rpc.await.unwrap_err().is_cancelled());
            service.shutdown_token.cancel();
            service.task_tracker.close();
            service.task_tracker.wait().await;
            panic!(
                "dependency admission rejection must release the blocking RPC without caller cancellation"
            );
        }
        assert_eq!(
            result.unwrap().unwrap().unwrap_err().code(),
            tonic::Code::Unavailable
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_for_state_transitions_drained(),
        )
        .await
        .unwrap();
        let work_file = locks.open_work();
        try_lock_exclusive(&work_file).expect("replacement must acquire the work gate");
        let job = jobs::get_job(service.db.lock().unwrap().raw(), &job_id)
            .unwrap()
            .unwrap();
        assert_eq!(job.status, "pending");
        assert!(job.error.is_some());
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Deleting
        );
        drop(work_file);
        drop(admission);
        // Advance the persisted retry deadline without waiting for backoff.
        jobs::retry_job(
            service.db.lock().unwrap().raw(),
            &job_id,
            job.error.as_deref().unwrap(),
            0,
        )
        .unwrap();
        service.ensure_cleanup_executor();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if jobs::get_job(service.db.lock().unwrap().raw(), &job_id)
                    .unwrap()
                    .unwrap()
                    .status
                    == "completed"
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable cleanup must complete after dependency admission reopens");
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
        assert!(service.lookup_sandbox(&sandbox_id).is_err());
    }

    #[tokio::test]
    async fn dependency_restart_gate_blocks_new_writers_before_draining_active_writer() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let active_writer = service.state_writer_lease("Start").unwrap();

        let admission_file = locks.lock_admission();
        let error = match service.state_writer_lease("Wake") {
            Ok(_) => panic!("dependency restart must close state-writer admission"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::Unavailable);

        let work_file = locks.open_work();
        let error = try_lock_exclusive(&work_file)
            .expect_err("active state writer must hold the dependency work gate");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(active_writer);
        try_lock_exclusive(&work_file).unwrap();

        drop(work_file);
        drop(admission_file);
        service.state_writer_lease("Wake").unwrap();
    }

    #[tokio::test]
    async fn dependency_restart_gate_blocks_during_local_eviction_cleanup() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);

        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let cleanup_blocker = service.cleanup_lock(&sandbox_id).lock_owned().await;

        let refused = service
            .evict_local_sandbox(
                &sandbox_id,
                super::eviction::EvictionClaim::SuspendedPinned(None),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unavailable);
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Suspended
        );
        drop(cleanup_blocker);
        let dispatched = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new({
            let dispatched = dispatched.clone();
            let release = release.clone();
            move |_, _| {
                let dispatched = dispatched.clone();
                let release = release.clone();
                Box::pin(async move {
                    dispatched.notify_one();
                    release.notified().await;
                    Ok(())
                })
            }
        }));

        let eviction_service = service.clone();
        let eviction_sandbox_id = sandbox_id.clone();
        let eviction = tokio::spawn(async move {
            eviction_service
                .evict_local_sandbox(
                    &eviction_sandbox_id,
                    super::eviction::EvictionClaim::SuspendedPinned(None),
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), dispatched.notified())
            .await
            .expect("eviction should claim the sandbox before cleanup");

        let admission_file = locks.lock_admission();
        let work_file = locks.open_work();
        let work_gate_result = try_lock_exclusive(&work_file);

        drop(work_file);
        drop(admission_file);
        release.notify_one();
        eviction.await.unwrap().unwrap();
        service.shutdown_token.cancel();
        assert!(
            work_gate_result.is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
            "local eviction cleanup must keep the dependency work gate shared"
        );
    }

    #[tokio::test]
    async fn dependency_restart_gate_waits_for_detached_recovery_cleanup() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::CleanupRequired);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id_string,
            false,
            crate::db::CleanupCompletion::DeleteRow,
        )
        .unwrap();

        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let startup_recovery = service.startup_recovery_lease().unwrap();
        // Observed, not attempted: a stale cleanup that reached node-manager
        // fails the assertion below by name, rather than by spending the
        // connect retry budget against a socket that is not there.
        let node_dispatched = Arc::new(AtomicBool::new(false));
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new({
            let node_dispatched = node_dispatched.clone();
            move |_, _| {
                node_dispatched.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }
        }));

        let cleanup_lock = service.cleanup_lock(&sandbox_id_string);
        service.schedule_async_cleanup(AsyncCleanupRequest {
            sandbox_id,
            uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
            keep_filesystem: false,
            context: "dependency drain recovery test cleanup failed",
            completion: CleanupCompletion::DeleteRow,
            immediate_kill: false,
            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            persist_intent: false,
            cleanup_id: None,
        });
        let cleanup_blocker = cleanup_lock.lock_owned().await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while service.cleanup_executor.active.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovered cleanup should claim its detached runner");
        tokio::time::timeout(Duration::from_secs(1), async {
            while service.active_state_transition_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached cleanup should join the dependency work set");

        drop(startup_recovery);
        let admission_file = locks.lock_admission();
        let work_file = locks.open_work();
        let work_gate_result = try_lock_exclusive(&work_file);
        let cleanup_held_work_gate = work_gate_result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock);

        drop(work_file);
        drop(admission_file);
        // The fence holder finishes the delete this cleanup was recovered for,
        // so the runner's authority is stale by the time it takes the fence.
        // Clearing only the intent would not leave it stale: a CleanupRequired
        // row without an intent is an implicit quarantine cleanup, which
        // recovery adopts as soon as this job exits, and that cleanup contacts
        // node-manager.
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::delete_sandbox(&db, &sandbox_id_string).unwrap();
        }
        drop(cleanup_blocker);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !service.cleanup_executor.active.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stale recovered cleanup should release its detached runner");

        assert!(
            cleanup_held_work_gate,
            "detached recovery cleanup must keep the dependency work gate shared"
        );
        assert!(
            !node_dispatched.load(Ordering::SeqCst),
            "stale recovered cleanup must exit without contacting node-manager"
        );
        // An empty runner set is only an end state if nothing is left for
        // recovery to start again.
        let db = service.db.lock().expect("mutex poisoned");
        let statuses = db
            .prepare("SELECT status FROM jobs WHERE kind = 'sandbox_cleanup'")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            statuses,
            ["cancelled"],
            "the stale cleanup must be superseded, and no other cleanup scheduled"
        );
        assert!(
            crate::db::cleanup_recovery_page(&db, None, 128)
                .unwrap()
                .ids
                .is_empty(),
            "no cleanup obligation may remain for recovery to adopt"
        );
    }

    #[tokio::test]
    async fn dependency_restart_capability_hands_off_between_active_instances() {
        let predecessor = {
            let conn = crate::db::test_conn();
            SandboxManagerService::for_test(conn)
        };
        let successor = {
            let conn = crate::db::test_conn();
            SandboxManagerService::for_test(conn)
        };
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&predecessor);

        assert!(
            successor
                .enable_dependency_restart_drain(&locks.capability, &locks.admission, &locks.work,)
                .is_err(),
            "only the service-mutex holder may advertise the capability"
        );

        predecessor.begin_draining_for_restart();
        predecessor.wait_for_state_transitions_drained().await;
        predecessor
            .release_dependency_restart_drain_capability()
            .unwrap();
        locks.enable_for(&successor);

        let startup_recovery = successor.startup_recovery_lease().unwrap();
        let work_file = locks.open_work();
        let error = try_lock_exclusive(&work_file)
            .expect_err("startup recovery must hold the dependency work gate");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(startup_recovery);
        try_lock_exclusive(&work_file).unwrap();
    }

    #[tokio::test]
    async fn startup_recovery_waits_for_dependency_replacement_to_finish() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let admission_file = locks.lock_admission();
        let work_file = locks.lock_work();
        let waiter_service = service.clone();
        let waiter = tokio::spawn(async move {
            waiter_service
                .wait_for_startup_recovery_lease(&CancellationToken::new(), Duration::from_secs(1))
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "temporary dependency replacement must not fail startup recovery"
        );
        assert_eq!(service.active_state_transition_count(), 0);

        drop(work_file);
        drop(admission_file);
        let lease = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("startup recovery should proceed when dependency gates reopen")
            .expect("startup recovery waiter should join")
            .expect("startup recovery should acquire admission");
        assert_eq!(service.active_state_transition_count(), 1);

        let work_file = locks.open_work();
        let error = try_lock_exclusive(&work_file)
            .expect_err("startup recovery must hold the dependency work gate");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(lease);
        try_lock_exclusive(&work_file).unwrap();
    }

    #[tokio::test]
    async fn startup_recovery_admission_wait_is_cancellable() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let admission_file = locks.lock_admission();
        let shutdown_token = CancellationToken::new();
        let waiter_service = service.clone();
        let waiter_shutdown = shutdown_token.clone();
        let waiter = tokio::spawn(async move {
            waiter_service
                .wait_for_startup_recovery_lease(&waiter_shutdown, Duration::from_secs(1))
                .await
        });

        tokio::task::yield_now().await;
        shutdown_token.cancel();
        let error = match waiter.await.expect("startup recovery waiter should join") {
            Ok(_) => panic!("shutdown must cancel startup recovery admission"),
            Err(error) => error,
        };

        assert!(matches!(error, StartupRecoveryAdmissionError::Cancelled));
        assert_eq!(service.active_state_transition_count(), 0);
        drop(admission_file);
        let work_file = locks.open_work();
        try_lock_exclusive(&work_file).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn startup_recovery_admission_wait_has_a_bounded_timeout() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let _admission_file = locks.lock_admission();
        let wait_timeout = Duration::from_millis(250);

        let error = match service
            .wait_for_startup_recovery_lease(&CancellationToken::new(), wait_timeout)
            .await
        {
            Ok(_) => panic!("closed admission must time out"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            StartupRecoveryAdmissionError::Timeout { waited } if waited == wait_timeout
        ));
        assert_eq!(service.active_state_transition_count(), 0);
        let work_file = locks.open_work();
        try_lock_exclusive(&work_file).unwrap();
    }

    #[tokio::test]
    async fn dependency_restart_gate_defers_due_auto_lifecycle_transition() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 1,
                action: AutoLifecycleIdleAction::Pause,
            }],
        }
        .normalized();
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state = initial_runtime_state(&policy, now_secs().saturating_sub(10));
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();

        let service = SandboxManagerService::for_test(conn);
        let locks = DependencyDrainFiles::new();
        locks.enable_for(&service);
        let _admission_file = locks.lock_admission();

        let error = service
            .run_due_auto_lifecycle(&sandbox_id)
            .await
            .expect_err("dependency restart must defer due auto-lifecycle work");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(
            error
                .metadata()
                .get(pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA)
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Started
        );
        assert_eq!(service.active_state_transition_count(), 0);

        service
            .handle_auto_lifecycle_error(&sandbox_id, sandbox_id.parse().unwrap(), &error)
            .await;
        assert!(
            service
                .lookup_sandbox(&sandbox_id)
                .unwrap()
                .auto_lifecycle_state
                .last_auto_error
                .is_none(),
            "deployment deferral must not be persisted as a sandbox lifecycle failure"
        );
    }

    #[tokio::test]
    async fn restart_drain_keeps_net_gateway_policy_hydration_read_only() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_orphaned_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let (peer, _other) = tokio::net::UnixStream::pair().unwrap();
        let peer_cred = peer.peer_cred().unwrap();
        let peer_pid = u32::try_from(peer_cred.pid().unwrap()).unwrap();
        crate::db::update_netgtw_pid(&conn, &sandbox_id, peer_cred.uid(), peer_pid).unwrap();

        let service = SandboxManagerService::for_test(conn);
        let before = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap()
        };
        service.begin_draining_for_restart();
        let mut request = Request::new(HostControlGetNetworkPolicyRequest::default());
        request
            .extensions_mut()
            .insert(tonic::transport::server::UdsConnectInfo {
                peer_addr: None,
                peer_cred: Some(peer_cred),
            });

        call_through_restart_admission(
            &service,
            "/space.spaced.host.control.v1.HostControl/GetNetworkPolicy",
            request,
            |service, request| async move {
                HostControl::get_network_policy(&service, request).await
            },
        )
            .await
            .expect("net-gateway policy hydration must remain available during restart drain");

        let after = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap()
        };
        assert_eq!(
            after.applied_network_policy_generation,
            before.applied_network_policy_generation
        );
        assert_eq!(
            after.applied_network_policy_source,
            before.applied_network_policy_source
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restart_drain_observes_concurrent_writer_completion() {
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);
        let writers = (0..64)
            .map(|_| service.state_writer_lease("Start").unwrap())
            .collect::<Vec<_>>();
        let barrier = Arc::new(tokio::sync::Barrier::new(writers.len() + 1));
        let tasks = writers
            .into_iter()
            .map(|writer| {
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    drop(writer);
                })
            })
            .collect::<Vec<_>>();
        service.begin_draining_for_restart();
        let waiter_service = service.clone();
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            waiter_service
                .wait_for_state_transitions_drained_after_observation(observed_tx)
                .await;
        });
        observed_rx
            .await
            .expect("drain waiter should observe the active state writers");
        assert!(
            !waiter.is_finished(),
            "drain waiter must remain pending while state writers are active"
        );

        barrier.wait().await;
        for task in tasks {
            task.await.unwrap();
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("drain waiter should observe all concurrent writers finishing")
            .unwrap();
        assert_eq!(service.active_state_transition_count(), 0);
    }

    #[tokio::test]
    async fn startup_recovery_finishes_interrupted_create_by_creating_cache_root() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Creating);

        let service = SandboxManagerService::for_test(conn);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].previous_state, SandboxState::Creating);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::State(SandboxState::Created)
        );
        assert!(cache_dir.path().join(&sandbox_id).is_dir());

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Created);
    }

    #[tokio::test]
    async fn startup_recovery_rewinds_unlaunched_start_without_rootfs_to_created() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Starting);

        let service = SandboxManagerService::for_test(conn);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].previous_state, SandboxState::Starting);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::State(SandboxState::Created)
        );
        assert!(cache_dir.path().join(&sandbox_id).is_dir());

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Created);
    }

    #[tokio::test]
    async fn startup_recovery_rewinds_unlaunched_start_with_rootfs_to_stopped() {
        let conn = crate::db::test_conn();
        let cache_dir = tempfile::tempdir().unwrap();
        let sandbox_id = SandboxId::new().to_string();
        let instance_dir = cache_dir.path().join(&sandbox_id).join("instance");
        std::fs::create_dir_all(&instance_dir).unwrap();
        std::fs::write(instance_dir.join("rootfs.ext4"), []).unwrap();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Starting);

        let service = SandboxManagerService::for_test(conn);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].previous_state, SandboxState::Starting);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::State(SandboxState::Stopped)
        );

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Stopped);
    }

    #[tokio::test]
    async fn startup_resident_malformed_identity_does_not_block_siblings() {
        let conn = crate::db::test_conn();
        let sibling = SandboxId::new().to_string();
        let corrupt = SandboxId::new().to_string();
        insert_test_sandbox_with_runtime(&conn, &corrupt, SandboxState::Resuming);
        conn.execute(
            "UPDATE sandboxes SET id = 'malformed-id' WHERE id = ?1",
            [&corrupt],
        )
        .unwrap();
        insert_test_sandbox(&conn, &sibling, SandboxState::Checkpointing);
        let service = SandboxManagerService::for_test(conn);
        let records = service
            .recover_interrupted_resident_operations()
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        for id in ["malformed-id", sibling.as_str()] {
            let row = service.lookup_sandbox(id).unwrap();
            assert_eq!(row.state, SandboxState::Quarantined);
            assert!(row.cleanup_completion.is_none());
        }
        let row = service.lookup_sandbox("malformed-id").unwrap();
        assert!(
            row.pid.is_some(),
            "invalid evidence must not destroy retained resources"
        );
        assert!(row.failure_reason.unwrap().contains("invalid sandbox ID"));
        assert!(
            service
                .recover_interrupted_resident_operations()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn startup_recovery_retains_malformed_ownership_without_blocking_siblings() {
        let conn = crate::db::test_conn();
        let cleanup = SandboxId::new().to_string();
        let transfer = SandboxId::new().to_string();
        let transient = SandboxId::new().to_string();
        let good = SandboxId::new().to_string();
        for id in [&cleanup, &transfer, &transient, &good] {
            insert_test_sandbox(&conn, id, SandboxState::Resuming);
        }
        for id in [&cleanup, &transient] {
            conn.execute("INSERT INTO sandbox_cleanup_episodes (sandbox_id, cleanup_id, completion, keep_filesystem, phase, activation_state, created_at) VALUES (?1, ?1, 'mark_stopped', 1, 'armed', 'starting', 0)", [id]).unwrap();
            conn.execute(
                "UPDATE sandbox_cleanup_episodes SET cleanup_id = ?1 WHERE sandbox_id = ?2",
                crate::db::fixture::params![format!("malformed-{id}"), id],
            )
            .unwrap();
        }
        crate::db::insert_restore_transfer_scope(
            &conn,
            &transfer,
            pplx_space_common_core::types::TransferScopeId::new(),
            None,
            None,
            "/retained/staging",
        )
        .unwrap();
        conn.execute(
            "UPDATE sandbox_restore_transfer_scopes SET transfer_scope_id = 'malformed'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE sandboxes SET state = 'starting' WHERE id = ?1",
            [&transient],
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        service.recover_interrupted_startup_states().await.unwrap();
        assert_eq!(
            service.lookup_sandbox(&good).unwrap().state,
            SandboxState::Quarantined
        );
        for id in [&cleanup, &transfer] {
            assert_eq!(
                service.lookup_sandbox(id).unwrap().state,
                SandboxState::Resuming
            );
        }
        assert_eq!(
            service.lookup_sandbox(&transient).unwrap().state,
            SandboxState::Starting
        );
        let db = service.db.lock().unwrap();
        crate::db::cleanup_episode(&db, &cleanup)
            .expect_err("malformed ownership must be retained");
        crate::db::restore_transfer_scope(&db, &transfer)
            .expect_err("malformed scope must be retained");
    }

    #[tokio::test]
    async fn startup_resident_persistence_failure_is_not_silently_deferred() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        let sibling = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Resuming);
        insert_test_sandbox(&conn, &sibling, SandboxState::Checkpointing);
        let mut cohort = vec![id.clone(), sibling.clone()];
        for _ in 2..=super::super::recovery::STARTUP_RECOVERY_CONCURRENCY {
            let queued = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &queued, SandboxState::Checkpointing);
            cohort.push(queued);
        }
        for (position, candidate) in cohort.iter().enumerate() {
            conn.execute(
                "UPDATE sandboxes SET created_at = ?1 WHERE id = ?2",
                crate::db::fixture::params![format!("{position:03}"), candidate],
            )
            .unwrap();
        }
        conn.execute_batch(
            "CREATE TRIGGER reject_recovery_write BEFORE UPDATE OF state ON sandboxes
             WHEN OLD.state = 'resuming'
             BEGIN SELECT RAISE(ABORT, 'injected recovery persistence failure'); END;",
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        service
            .recover_interrupted_resident_operations()
            .await
            .expect_err("persistence failure must fail startup so its supervisor can retry");
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Resuming
        );
        assert_eq!(
            service.lookup_sandbox(&sibling).unwrap().state,
            SandboxState::Quarantined,
            "startup must finish sibling recovery before returning the persistence failure"
        );
        assert_eq!(
            service
                .lookup_sandbox(cohort.last().unwrap())
                .unwrap()
                .state,
            SandboxState::Checkpointing,
            "persistence failure must stop admission beyond the in-flight cohort"
        );
    }

    #[tokio::test]
    async fn startup_resume_requires_attested_peer_even_without_environment() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Resuming);
        let service = SandboxManagerService::for_test(conn);
        let row = service.lookup_sandbox(&id).unwrap();
        let error = service
            .prepare_startup_resident(&row, SandboxState::Started)
            .await
            .expect_err("missing peer identity must prevent healthy publication");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().updated_at,
            row.updated_at
        );
    }

    #[tokio::test(start_paused = true)]
    async fn startup_resident_recovery_preserves_runtime_and_requires_resume_setup() {
        use crate::runtime_reconciliation::VmStateObservation as Observation;
        for state in [SandboxState::Resuming, SandboxState::Checkpointing] {
            for (observations, runtime, setup_ok, expected) in [
                (
                    vec![Some(VmState::Running); 3],
                    true,
                    true,
                    SandboxState::Started,
                ),
                (
                    vec![Some(VmState::Paused); 3],
                    true,
                    true,
                    if state == SandboxState::Checkpointing {
                        SandboxState::Quarantined
                    } else {
                        SandboxState::Paused
                    },
                ),
                (
                    vec![Some(VmState::NotStarted); 3],
                    true,
                    true,
                    SandboxState::Quarantined,
                ),
                (vec![None; 3], true, true, SandboxState::Quarantined),
                (
                    vec![
                        Some(VmState::Running),
                        Some(VmState::Paused),
                        Some(VmState::Running),
                    ],
                    true,
                    true,
                    SandboxState::Quarantined,
                ),
                (vec![], false, true, SandboxState::Quarantined),
                (
                    vec![Some(VmState::Running); 3],
                    true,
                    false,
                    SandboxState::Quarantined,
                ),
            ] {
                let conn = crate::db::test_conn();
                let id = SandboxId::new().to_string();
                if runtime {
                    insert_test_sandbox_with_runtime(&conn, &id, state);
                } else {
                    insert_test_sandbox(&conn, &id, state);
                }
                use tracing_subscriber::prelude::*;
                let capture = SandboxEventCapture::default();
                let _subscriber = tracing::subscriber::set_default(
                    tracing_subscriber::registry().with(capture.clone()),
                );
                let service = SandboxManagerService::for_test(conn);
                let before = service.lookup_sandbox(&id).unwrap();
                let pid = before.pid;
                let mut observations = observations.into_iter();
                let prepared = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&prepared);
                let record = service
                    .recover_resident_with_probe(
                        before,
                        Path::new(RUN_DIR),
                        |_| {
                            let observation = match observations.next().expect("bounded probes") {
                                Some(state) => Observation::Observed(state),
                                None => Observation::Unobservable("state RPC unavailable".into()),
                            };
                            async { observation }
                        },
                        |_| async move {
                            count.fetch_add(1, Ordering::Relaxed);
                            if setup_ok {
                                Ok(())
                            } else {
                                Err(Status::unavailable("environment unavailable"))
                            }
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    record.outcome,
                    crate::db::StartupRecoveryOutcome::State(expected)
                );
                let row = service.lookup_sandbox(&id).unwrap();
                assert_eq!(row.state, expected);
                assert_eq!(row.pid, pid);
                let events = capture.events.lock().unwrap();
                let event = events
                    .iter()
                    .find(|event| {
                        event.get("event_type").is_some_and(|kind| {
                            kind == "lifecycle_converged" || kind == "sandbox_quarantined"
                        })
                    })
                    .expect("settlement must emit an outcome event");
                if expected != SandboxState::Quarantined {
                    assert_eq!(
                        event.get("reason").map(String::as_str),
                        Some("startup_recovery_runtime_state_observed")
                    );
                    assert!(
                        !event.contains_key("error"),
                        "healthy convergence is not an error: {event:?}"
                    );
                }
                assert!(row.cleanup_completion.is_none());
                assert_eq!(
                    prepared.load(Ordering::Relaxed),
                    usize::from(
                        matches!(expected, SandboxState::Started | SandboxState::Paused)
                            || !setup_ok
                    )
                );
                let conn = service.db.lock().unwrap();
                assert!(crate::db::cleanup_intent_for_startup_recovery(&row).is_none());
                assert_eq!(
                    crate::db::current_quarantine_reason_wire_value(&conn, &id)
                        .unwrap()
                        .is_some(),
                    expected == SandboxState::Quarantined
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_resident_recovery_does_not_persist_peer_error_payloads() {
        use crate::runtime_reconciliation::VmStateObservation as Observation;
        use tracing_subscriber::prelude::*;
        const PAYLOAD: &str = "guest-controlled-secret-and-forged-event";
        for probe_fails in [false, true] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Resuming);
            let service = SandboxManagerService::for_test(conn);
            let capture = SandboxEventCapture::default();
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(capture.clone()),
            );
            service
                .recover_resident_with_probe(
                    service.lookup_sandbox(&id).unwrap(),
                    Path::new(RUN_DIR),
                    |_| async {
                        if probe_fails {
                            Observation::Unobservable(PAYLOAD.into())
                        } else {
                            Observation::Observed(VmState::Running)
                        }
                    },
                    |_| async { Err(Status::unavailable(PAYLOAD)) },
                )
                .await
                .unwrap();
            let row = service.lookup_sandbox(&id).unwrap();
            assert_eq!(row.state, SandboxState::Quarantined);
            let reason = row.failure_reason.unwrap();
            assert!(!reason.contains(PAYLOAD), "{reason}");
            let reason: String = service
                .db
                .lock()
                .unwrap()
                .query_row(
                    "SELECT reason_message FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                    [&id],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(!reason.contains(PAYLOAD), "{reason}");
            let events = capture.events.lock().unwrap();
            assert!(!events.is_empty(), "must capture the quarantine event");
            assert!(
                events
                    .iter()
                    .flat_map(|e| e.values())
                    .all(|v| !v.contains(PAYLOAD))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_resident_recovery_rejects_changed_identity_and_cleanup_ownership() {
        use crate::runtime_reconciliation::VmStateObservation as Observation;
        for replace_pid in [false, true] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Checkpointing);
            let service = SandboxManagerService::for_test(conn);
            let before = service.lookup_sandbox(&id).unwrap();
            let record = service
                .recover_resident_with_probe(
                    before,
                    Path::new(RUN_DIR),
                    |_| {
                        let conn = service.db.lock().unwrap();
                        if replace_pid {
                            conn.execute("UPDATE sandboxes SET pid = 123 WHERE id = ?1", [&id])
                                .unwrap();
                        } else {
                            crate::db::mark_cleanup_required(
                                &conn,
                                &id,
                                false,
                                CleanupCompletion::DeleteRow,
                                "delete won",
                            )
                            .unwrap();
                        }
                        async { Observation::Observed(VmState::Running) }
                    },
                    |_| async { panic!("checkpoint recovery must not configure resume") },
                )
                .await
                .unwrap();
            assert_eq!(record.outcome, crate::db::StartupRecoveryOutcome::Deferred);
            let row = service.lookup_sandbox(&id).unwrap();
            assert_eq!(
                row.state,
                if replace_pid {
                    SandboxState::Checkpointing
                } else {
                    SandboxState::CleanupRequired
                }
            );
        }
    }

    #[tokio::test]
    async fn startup_recovery_heals_definitive_pause_and_quarantines_ambiguity() {
        use crate::runtime_reconciliation::VmStateObservation;

        for (observed, expected) in [
            (VmState::Running, SandboxState::Started),
            (VmState::Paused, SandboxState::Paused),
        ] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox_with_runtime(&conn, &sandbox_id, SandboxState::Pausing);
            let service = SandboxManagerService::for_test(conn);
            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            let probe_count = Arc::new(AtomicUsize::new(0));

            let record = service
                .recover_interrupted_pause_with_probe(row, {
                    let probe_count = Arc::clone(&probe_count);
                    move |_| {
                        probe_count.fetch_add(1, Ordering::Relaxed);
                        async move { VmStateObservation::Observed(observed) }
                    }
                })
                .await
                .unwrap();

            assert_eq!(probe_count.load(Ordering::Relaxed), 2);
            assert_eq!(
                record.outcome,
                crate::db::StartupRecoveryOutcome::State(expected)
            );
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
            assert_eq!(row.state, expected);
            assert!(crate::db::sandbox_has_runtime_fields(&row));
            assert!(
                crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
                    .unwrap()
                    .is_none()
            );
        }

        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox_with_runtime(&conn, &sandbox_id, SandboxState::Pausing);
        let service = SandboxManagerService::for_test(conn);
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let observations = Arc::new(Mutex::new(
            [VmState::Running, VmState::Paused, VmState::Running].into_iter(),
        ));

        let record = service
            .recover_interrupted_pause_with_probe(row, {
                let observations = Arc::clone(&observations);
                move |_| {
                    let observed = observations.lock().unwrap().next().unwrap();
                    async move { VmStateObservation::Observed(observed) }
                }
            })
            .await
            .unwrap();

        assert_eq!(
            record.outcome,
            crate::db::StartupRecoveryOutcome::State(SandboxState::Quarantined)
        );
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
        assert_eq!(
            crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
                .unwrap()
                .as_deref(),
            Some(SandboxQuarantineReasonCode::PauseFailed.as_str()),
        );
    }

    #[tokio::test]
    async fn startup_recovery_isolates_a_malformed_pausing_row() {
        let conn = crate::db::test_conn();
        let malformed_id = "malformed-pausing-id";
        let original_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &original_id, SandboxState::Pausing);
        conn.execute(
            "UPDATE sandboxes SET id = ?1, pid = ?2 WHERE id = ?3",
            crate::db::fixture::params![malformed_id, std::process::id(), original_id],
        )
        .unwrap();
        let stopping_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &stopping_id, SandboxState::Stopping);
        let service = SandboxManagerService::for_test(conn);

        let records = service.recover_interrupted_startup_states().await.unwrap();

        assert!(records.iter().any(|record| {
            record.id == malformed_id
                && record.outcome
                    == crate::db::StartupRecoveryOutcome::State(SandboxState::Quarantined)
        }));
        assert!(records.iter().any(|record| {
            record.id == stopping_id
                && record.outcome
                    == crate::db::StartupRecoveryOutcome::CleanupRequired {
                        keep_filesystem: true,
                        completion: CleanupCompletion::MarkStopped,
                    }
        }));
        let db = service.db.lock().expect("mutex poisoned");
        assert_eq!(
            crate::db::get_sandbox(&db, malformed_id)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Quarantined
        );
        assert_eq!(
            crate::db::get_sandbox(&db, &stopping_id)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::CleanupRequired
        );
    }

    #[tokio::test]
    async fn suspend_capture_admission_keeps_the_cleanup_fence_until_handoff() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Paused);
        let service = service_with_runtime_probe(conn, false);
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        let artifact = service.auto_lifecycle.artifact_lock(&id).lock_owned().await;
        let CleanupLockTryResult::Acquired(delete_fence) = service.try_cleanup_fence(&id) else {
            panic!("free fence")
        };
        let context = super::super::grpc_lifecycle::SuspendRequestContext::new(
            id.clone(),
            true,
            0,
            false,
            None,
        );
        let result = service
            .prepare_suspend_snapshot_publication_after_locks(&context, &wake, &artifact)
            .await;
        assert!(matches!(result, Err(status) if status.code() == tonic::Code::Unavailable));
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Paused
        );
        drop(delete_fence);
        let (_, publication) = service
            .prepare_suspend_snapshot_publication_after_locks(&context, &wake, &artifact)
            .await
            .unwrap();
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspending
        );
        assert!(matches!(
            service.try_cleanup_fence(&id),
            CleanupLockTryResult::Busy
        ));
        drop(publication);
        assert!(matches!(
            service.try_cleanup_fence(&id),
            CleanupLockTryResult::Acquired(_)
        ));
    }

    #[tokio::test]
    async fn periodic_suspend_recovery_skips_live_owners_then_publishes_without_restart() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_suspending_resuspend(&conn, &id);
        let cache = tempfile::tempdir().unwrap();
        let artifact_id = LocalArtifactId::new();
        let staging = suspend_snapshot_staging_dir(&cache.path().join(&id), artifact_id);
        write_full_suspend_artifact(&staging, &id, artifact_id, b"retained-capture");
        let service = service_with_runtime_probe(conn, false);
        let wake = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
        service
            .recover_deferred_suspend_publications_in(
                cache.path(),
                vec![service.lookup_sandbox(&id).unwrap()],
                &CancellationToken::new(),
                &Default::default(),
            )
            .unwrap();
        assert!(staging.exists());
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspending
        );
        drop(wake);
        // An obstructed retirement destination makes the real publication
        // rename fail. Startup must preserve the complete staged capture.
        let sandbox_dir = cache.path().join(&id);
        std::fs::create_dir_all(canonical_suspend_snapshot_dir(&sandbox_dir)).unwrap();
        let obstruction =
            crate::service::suspend_snapshot_publication::retired_suspend_snapshot_dir(
                &sandbox_dir,
                artifact_id,
            );
        std::fs::create_dir_all(obstruction.parent().unwrap()).unwrap();
        std::fs::write(&obstruction, b"transient obstruction").unwrap();
        let deferred = service
            .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
            .unwrap();
        assert_eq!(
            deferred[0].outcome,
            crate::db::StartupRecoveryOutcome::Deferred
        );
        assert!(staging.exists());
        // The live failure path must leave the same complete capture eligible
        // for periodic recovery when publication is still obstructed.
        let disposition = service.record_suspend_snapshot_publication_failure_after_staging(
            &id,
            &sandbox_dir,
            &staging,
            "publication temporarily unavailable",
            &Default::default(),
        );
        assert_eq!(disposition, SuspendSnapshotStagingDisposition::Preserve);
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspending
        );
        assert!(staging.exists());
        std::fs::remove_file(obstruction).unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        service
            .recover_deferred_suspend_publications_in(
                cache.path(),
                vec![service.lookup_sandbox(&id).unwrap()],
                &shutdown,
                &Default::default(),
            )
            .unwrap();
        assert!(staging.exists(), "shutdown must not admit a publication");
        service
            .recover_deferred_suspend_publications_in(
                cache.path(),
                vec![service.lookup_sandbox(&id).unwrap()],
                &CancellationToken::new(),
                &Default::default(),
            )
            .unwrap();
        let row = service.lookup_sandbox(&id).unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(row.current_local_artifact_id, Some(artifact_id));
        assert_eq!(
            row.cleanup_completion,
            Some(CleanupCompletion::MarkSuspended)
        );
        assert!(!staging.exists());
    }

    #[tokio::test]
    async fn live_publish_failure_with_an_unconfirmed_retained_source_stays_suspending() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_suspending_resuspend(&conn, &id);
        let cache = tempfile::tempdir().unwrap();
        let sandbox_dir = cache.path().join(&id);
        let artifact_id = LocalArtifactId::new();
        let staging = suspend_snapshot_staging_dir(&sandbox_dir, artifact_id);
        let _bundle =
            write_deferred_suspend_artifact(&staging, &id, artifact_id, uuid::Uuid::now_v7(), 64);
        let service = service_with_runtime_probe(conn, false);

        // No node-manager answer yet: periodic recovery owns the retry.
        let disposition = service.record_suspend_snapshot_publication_failure_after_staging(
            &id,
            &sandbox_dir,
            &staging,
            "publish failed",
            &Default::default(),
        );

        assert_eq!(disposition, SuspendSnapshotStagingDisposition::Preserve);
        let row = service.lookup_sandbox(&id).unwrap();
        assert_eq!(row.state, SandboxState::Suspending);
        assert!(row.failure_reason.is_none());
        assert!(staging.exists());
    }

    #[tokio::test]
    async fn periodic_suspend_recovery_follows_node_manager_for_a_deferred_capture() {
        use crate::retained_source::{FakeInspector, RetainedSourceCheck};
        for (answer, expected_state) in [
            (
                RetainedSourceCheck::Unavailable {
                    reason: "node-manager restarting".to_owned(),
                },
                SandboxState::Suspending,
            ),
            (
                RetainedSourceCheck::Present { rootfs_bytes: 64 },
                SandboxState::CleanupRequired,
            ),
            (RetainedSourceCheck::Absent, SandboxState::Quarantined),
        ] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_suspending_resuspend(&conn, &id);
            let cache = tempfile::tempdir().unwrap();
            let artifact_id = LocalArtifactId::new();
            let source_id = uuid::Uuid::now_v7();
            let staging = suspend_snapshot_staging_dir(&cache.path().join(&id), artifact_id);
            let bundle = write_deferred_suspend_artifact(&staging, &id, artifact_id, source_id, 64);
            // An absent answer is missing only once the bundle directory is
            // gone too; with it present the source would be pending.
            let _bundle = if matches!(answer, RetainedSourceCheck::Absent) {
                drop(bundle);
                std::fs::remove_dir_all(staging.join(crate::retained_source::BUNDLE_DIRECTORY))
                    .unwrap();
                None
            } else {
                Some(bundle)
            };
            let inspector = Arc::new(FakeInspector::new(answer.clone()));
            let service = service_with_runtime_probe(conn, false)
                .with_retained_source_inspector(inspector.clone());

            let verdicts = service
                .recover_deferred_suspend_publication_batch(
                    cache.path(),
                    vec![service.lookup_sandbox(&id).unwrap()],
                    &CancellationToken::new(),
                    &mut Default::default(),
                    (),
                )
                .await;
            let calls = inspector.calls();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].source_id, source_id);
            let row = service.lookup_sandbox(&id).unwrap();
            assert_eq!(row.state, expected_state, "{answer}");
            match answer {
                RetainedSourceCheck::Present { .. } => {
                    assert_eq!(row.current_local_artifact_id, Some(artifact_id));
                    assert!(!staging.exists());
                }
                // Evidence stays in place for retry or an operator.
                RetainedSourceCheck::Unavailable { .. } => {
                    assert!(staging.exists());
                    assert_eq!(verdicts.take_unresolved().len(), 1);
                }
                _ => assert!(staging.exists()),
            }
        }
    }

    #[tokio::test]
    async fn paged_periodic_recovery_resolves_each_deferred_capture_in_its_own_pass() {
        use crate::retained_source::{FakeInspector, RetainedSourceCheck};
        let conn = crate::db::test_conn();
        let cache = tempfile::tempdir().unwrap();
        let mut ids = Vec::new();
        let mut bundles = Vec::new();
        // More than one discovery page of eight candidates.
        for _ in 0..9 {
            let id = SandboxId::new().to_string();
            insert_suspending_resuspend(&conn, &id);
            let artifact_id = LocalArtifactId::new();
            let staging = suspend_snapshot_staging_dir(&cache.path().join(&id), artifact_id);
            bundles.push(write_deferred_suspend_artifact(
                &staging,
                &id,
                artifact_id,
                uuid::Uuid::now_v7(),
                64,
            ));
            ids.push(id);
        }
        let service =
            service_with_runtime_probe(conn, false).with_retained_source_inspector(Arc::new(
                FakeInspector::new(RetainedSourceCheck::Present { rootfs_bytes: 64 }),
            ));
        let mut scan = crate::db::PeriodicLifecycleRecoveryScan::default();
        for _ in 0..2 {
            let rows = crate::db::periodic_lifecycle_recovery_batch(
                &service.db.lock().unwrap(),
                &mut scan,
            )
            .unwrap();
            service
                .recover_deferred_suspend_publication_batch(
                    cache.path(),
                    rows,
                    &CancellationToken::new(),
                    &mut Default::default(),
                    (),
                )
                .await;
        }
        let remaining: Vec<_> = ids
            .iter()
            .filter(|id| service.lookup_sandbox(id).unwrap().state == SandboxState::Suspending)
            .collect();
        assert!(remaining.is_empty(), "{remaining:?}");
    }

    #[tokio::test]
    async fn periodic_suspend_recovery_isolates_malformed_associated_evidence() {
        let conn = crate::db::test_conn();
        let bad = SandboxId::new().to_string();
        let good = SandboxId::new().to_string();
        for id in [&bad, &good] {
            insert_test_sandbox(&conn, id, SandboxState::Suspending);
        }
        conn.execute("INSERT INTO jobs (job_id, kind, subject_id, payload) VALUES (?1, 'teardown_suspend', ?2, 'malformed')", crate::db::fixture::params![uuid::Uuid::now_v7().to_string(), bad]).unwrap();
        let cache = tempfile::tempdir().unwrap();
        let artifact = LocalArtifactId::new();
        let staging = suspend_snapshot_staging_dir(&cache.path().join(&good), artifact);
        write_full_suspend_artifact(&staging, &good, artifact, b"retained-capture");
        let service = service_with_runtime_probe(conn, false);
        let mut scan = crate::db::PeriodicLifecycleRecoveryScan::default();
        for _ in 0..2 {
            let rows = crate::db::periodic_lifecycle_recovery_batch(
                &service.db.lock().unwrap(),
                &mut scan,
            )
            .unwrap();
            service
                .recover_deferred_suspend_publications_in(
                    cache.path(),
                    rows,
                    &CancellationToken::new(),
                    &Default::default(),
                )
                .unwrap();
        }
        let row = service.lookup_sandbox(&good).unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(row.current_local_artifact_id, Some(artifact));
        assert!(!staging.exists());
        assert_eq!(
            service.lookup_sandbox(&bad).unwrap().state,
            SandboxState::Suspending
        );
        crate::db::teardown_suspend_status(&service.db.lock().unwrap(), &bad)
            .expect_err("malformed teardown evidence must be retained");
    }

    #[tokio::test(start_paused = true)]
    async fn paused_recovery_never_claims_lost_memory_was_resumed() {
        use crate::runtime_reconciliation::VmStateObservation as Observation;
        use VmState::{NotStarted, Paused, Running, Shutdown};
        // None = absent, Some(false) = resolving symlink, Some(true) = ELOOP.
        for pid_witness in [None, Some(false), Some(true)] {
            for (observations, stays_paused) in [
                (vec![Some(Paused); 3], true),
                (vec![Some(Running); 3], false),
                (vec![Some(NotStarted); 3], false),
                (vec![Some(Shutdown); 3], false),
                (vec![None; 3], pid_witness.is_some()),
                (
                    vec![Some(Running), Some(Paused), Some(Running)],
                    pid_witness.is_some(),
                ),
                // An authoritative loss observation cannot be erased by a later
                // connection failure or a replacement runtime appearing healthy.
                (vec![Some(NotStarted), None, None], false),
                (vec![Some(NotStarted), Some(Paused), Some(Paused)], false),
            ] {
                let conn = crate::db::test_conn();
                let id = SandboxId::new().to_string();
                insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Paused);
                let run_dir = tempfile::tempdir().unwrap();
                if let Some(unreadable) = pid_witness {
                    std::fs::create_dir(run_dir.path().join(&id)).unwrap();
                    let link = run_dir.path().join(&id).join("fc.pid");
                    let target = if unreadable {
                        link.as_path()
                    } else {
                        run_dir.path()
                    };
                    std::os::unix::fs::symlink(target, &link).unwrap();
                    assert_eq!(std::fs::exists(&link).is_err(), unreadable);
                }
                let service = service_with_runtime_probe(conn, false);
                let shutdown = CancellationToken::new();
                shutdown.cancel();
                service
                    .recover_periodic_resident_operations(
                        vec![service.lookup_sandbox(&id).unwrap()],
                        &shutdown,
                    )
                    .await;
                assert_eq!(
                    service.lookup_sandbox(&id).unwrap().state,
                    SandboxState::Paused
                );
                let mut observations = observations.into_iter();
                service
                    .recover_resident_with_probe(
                        service.lookup_sandbox(&id).unwrap(),
                        run_dir.path(),
                        |_| {
                            let observation = match observations.next().expect("bounded probes") {
                                Some(state) => Observation::Observed(state),
                                None => Observation::Unobservable("unavailable".into()),
                            };
                            async { observation }
                        },
                        |_| async {
                            panic!("stable pause recovery must not configure or resume a guest")
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    service.lookup_sandbox(&id).unwrap().state,
                    if stays_paused {
                        SandboxState::Paused
                    } else {
                        SandboxState::Quarantined
                    }
                );
                let row = service.lookup_sandbox(&id).unwrap();
                assert!(row.pid.is_some(), "quarantine retains runtime evidence");
                assert!(
                    row.cleanup_completion.is_none(),
                    "quarantine must not authorize teardown"
                );
            }
        }
    }

    #[tokio::test]
    async fn startup_recovery_finishes_complete_suspend_staging() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);

        let cache_dir = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let sandbox_dir = cache_dir.path().join(&sandbox_id);
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id);
        write_full_suspend_artifact(
            &staging_dir,
            &sandbox_id,
            local_artifact_id,
            b"staged-artifact",
        );

        let service = service_with_runtime_probe(conn, false);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].previous_state, SandboxState::Suspending);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::CleanupRequired {
                keep_filesystem: true,
                completion: crate::db::CleanupCompletion::MarkSuspended
            }
        );

        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        assert!(snapshot_dir.join("snapshot.json").exists());
        assert!(!staging_dir.exists());

        {
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
            assert_eq!(row.state, SandboxState::CleanupRequired);
            assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
            assert_eq!(
                row.current_local_artifact_kind,
                Some(pplx_space_common_core::types::ArtifactKind::Full)
            );
            assert_eq!(
                row.current_local_artifact_origin,
                Some(pplx_space_common_core::types::ArtifactOrigin::Suspend)
            );
            assert!(row.failure_reason.is_none());
            assert!(
                crate::db::get_local_artifact(&db, local_artifact_id)
                    .unwrap()
                    .is_some(),
                "startup recovery must register the exact staged artifact"
            );
        }

        SandboxManagerService::complete_async_cleanup(
            &service.db,
            &sandbox_id,
            CleanupCompletion::MarkSuspended,
            None,
        )
        .unwrap();
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    }

    #[tokio::test]
    async fn startup_suspend_recovery_defers_when_quarantine_cas_loses_race() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        let service = service_with_runtime_probe(conn, false);
        let stale_row = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap()
        };
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::test_fixtures::state_write_fixtures::apply_edge(
                &db,
                &sandbox_id,
                &crate::db::edges::SUSPEND_ROLLBACK,
            );
        }
        let cache_dir = tempfile::tempdir().unwrap();

        let record = service
            .apply_suspend_snapshot_publication_recovery_decision(
                stale_row,
                cache_dir.path().join(&sandbox_id),
                SuspendSnapshotPublicationDecision::RuntimeGoneWithoutUsableArtifact,
                SuspendSnapshotPublicationRecoveryOrigin::StartupRecovery,
                None,
                &Default::default(),
            )
            .unwrap();

        assert_eq!(record.outcome, crate::db::StartupRecoveryOutcome::Deferred);
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Paused);
    }

    #[tokio::test]
    async fn startup_recovery_does_not_cleanup_a_teardown_suspend_before_backup() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let generation = teardown_generation();
        crate::db::begin_teardown_suspend(&conn, &sandbox_id, generation).unwrap();

        let cache_dir = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let snapshot_dir = canonical_suspend_snapshot_dir(&cache_dir.path().join(&sandbox_id));
        write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"not-yet-backed",
        );
        crate::db::stage_teardown_suspend_artifact(
            &conn,
            &sandbox_id,
            generation,
            local_artifact_id,
            &snapshot_dir.to_string_lossy(),
        )
        .unwrap();

        let service = service_with_runtime_probe(conn, false);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::Deferred
        );
        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Suspending);
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    }

    /// Recovery adopts a teardown capture it finds staged or already
    /// published, pins it for backup without cleaning up, and records its
    /// execution generation in the published capture.
    #[tokio::test]
    async fn startup_recovery_adopts_but_does_not_cleanup_an_unbacked_teardown_artifact() {
        for staged in [true, false] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
            crate::db::begin_teardown_suspend(&conn, &sandbox_id, teardown_generation()).unwrap();

            let cache_dir = tempfile::tempdir().unwrap();
            let local_artifact_id = LocalArtifactId::new();
            let sandbox_dir = cache_dir.path().join(&sandbox_id);
            let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
            let written_dir = if staged {
                suspend_snapshot_staging_dir(&sandbox_dir, local_artifact_id)
            } else {
                snapshot_dir.clone()
            };
            write_full_suspend_artifact(
                &written_dir,
                &sandbox_id,
                local_artifact_id,
                b"published-before-db-pin",
            );

            let service = service_with_runtime_probe(conn, false);
            let records = service
                .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
                .unwrap();

            assert_eq!(records.len(), 1, "staged: {staged}");
            assert_eq!(
                records[0].outcome,
                crate::db::StartupRecoveryOutcome::Deferred,
                "planned teardown must wait for remote durability before runtime cleanup \
                 (staged: {staged})",
            );
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
            assert_eq!(row.state, SandboxState::Suspending, "staged: {staged}");
            assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
            assert!(
                crate::db::get_local_artifact(&db, local_artifact_id)
                    .unwrap()
                    .is_some(),
                "recovery must pin the complete artifact so backup can resume (staged: {staged})",
            );
            assert_eq!(
                crate::capture_record::read(&snapshot_dir, local_artifact_id).unwrap(),
                Some(teardown_generation()),
                "staged: {staged}"
            );
        }
    }

    #[tokio::test]
    async fn startup_recovery_adopts_resuspend_successor_instead_of_predecessor() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let predecessor_id = insert_suspending_resuspend(&conn, &sandbox_id);
        let admitted = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert!(admitted.current_local_artifact_id.is_none());

        let cache_dir = tempfile::tempdir().unwrap();
        let local_artifact_id = LocalArtifactId::new();
        let sandbox_dir = cache_dir.path().join(&sandbox_id);
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        write_full_suspend_artifact(
            &snapshot_dir,
            &sandbox_id,
            local_artifact_id,
            b"canonical-artifact",
        );

        let service = service_with_runtime_probe(conn, false);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].previous_state, SandboxState::Suspending);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::CleanupRequired {
                keep_filesystem: true,
                completion: crate::db::CleanupCompletion::MarkSuspended
            }
        );

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
        assert_ne!(row.current_local_artifact_id, Some(predecessor_id));
        assert!(row.failure_reason.is_none());
        assert!(
            crate::db::get_local_artifact(&db, local_artifact_id)
                .unwrap()
                .is_some(),
            "startup recovery must register the complete canonical artifact"
        );
    }

    #[tokio::test]
    async fn build_sandbox_info_forwards_the_recovery_publication() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = service_with_runtime_probe(conn, false);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let receipt_id = SandboxCleanupEpisodeId::new();
        let execution_generation = ExecutionGeneration::new(7).unwrap();

        let info = service
            .build_sandbox_info(
                sandbox,
                Some(Ok(crate::db::RawSandboxStatePublicationMetadata {
                    quarantine_reason_code: None,
                    transition_receipt_id: Some(receipt_id.to_string()),
                    transition_receipt_kind: Some("quarantine_recovery".to_string()),
                    transition_receipt_target_state: Some("suspended".to_string()),
                    transition_receipt_execution_generation: Some(execution_generation.get()),
                    transition_receipt_reason_code: Some(
                        SandboxQuarantineReasonCode::RestorePrepareFailed.to_string(),
                    ),
                    transition_receipt_activation_attempt_id: None,
                })),
                Some("123".to_string()),
                None,
                None,
            )
            .await
            .unwrap();

        let receipt = info.transition_receipt.as_ref().unwrap();
        assert_eq!(
            info.state_changed_at,
            Some(prost_types::Timestamp {
                seconds: 123,
                nanos: 0,
            })
        );
        assert_eq!(
            receipt.reason_code,
            SandboxQuarantineReasonCode::RestorePrepareFailed.as_str()
        );
        assert_eq!(receipt.id, receipt_id.to_string());
        assert_eq!(receipt.execution_generation, execution_generation.get());
        assert_eq!(
            info.health_status,
            crate::proto::SandboxHealthStatus::Unspecified as i32
        );
    }

    #[tokio::test]
    async fn build_sandbox_info_publishes_quarantine_recovery_observation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = service_with_runtime_probe(conn, false);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let local_quarantine_id = Uuid::now_v7();

        let info = service
            .build_sandbox_info(
                sandbox,
                None,
                None,
                None,
                Some(Ok(crate::db::RawQuarantineRecoveryObservation {
                    local_quarantine_id: local_quarantine_id.to_string(),
                    owner: Some(
                        crate::db::QuarantineRecoveryOwner::LegacyRecovery
                            .as_str()
                            .to_string(),
                    ),
                    attempt_count: 3,
                    next_attempt_at: Some("1786800000".to_string()),
                })),
            )
            .await
            .unwrap();

        let observation = info.quarantine_recovery.unwrap();
        assert_eq!(
            observation.local_quarantine_id,
            local_quarantine_id.to_string()
        );
        assert_eq!(
            observation.observed_owner,
            crate::proto::QuarantineRecoveryObservedOwner::LegacyRecovery as i32
        );
        assert_eq!(observation.attempt_count, 3);
        assert_eq!(observation.next_attempt_at.unwrap().seconds, 1_786_800_000);
        assert_eq!(observation.schema_version, 1);
        assert!(!observation.degraded);
    }

    #[tokio::test]
    async fn build_sandbox_info_marks_malformed_recovery_observation_degraded() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = service_with_runtime_probe(conn, false);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();

        let info = service
            .build_sandbox_info(
                sandbox,
                None,
                None,
                None,
                Some(Ok(crate::db::RawQuarantineRecoveryObservation {
                    local_quarantine_id: "not-a-uuid".to_string(),
                    owner: Some(
                        crate::db::QuarantineRecoveryOwner::LegacyRecovery
                            .as_str()
                            .to_string(),
                    ),
                    attempt_count: 3,
                    next_attempt_at: None,
                })),
            )
            .await
            .unwrap();

        assert!(!info.state_degraded);
        assert!(info.quarantine_recovery.unwrap().degraded);
    }

    #[tokio::test]
    async fn build_sandbox_info_preserves_recovery_facts_for_an_unknown_owner() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let service = service_with_runtime_probe(conn, false);
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let local_quarantine_id = Uuid::now_v7();

        let info = service
            .build_sandbox_info(
                sandbox,
                None,
                None,
                None,
                Some(Ok(crate::db::RawQuarantineRecoveryObservation {
                    local_quarantine_id: local_quarantine_id.to_string(),
                    owner: Some("desired_state_recovery".to_string()),
                    attempt_count: 7,
                    next_attempt_at: Some("1786800000".to_string()),
                })),
            )
            .await
            .unwrap();

        let observation = info.quarantine_recovery.unwrap();
        assert!(!observation.degraded);
        assert_eq!(
            observation.local_quarantine_id,
            local_quarantine_id.to_string()
        );
        assert_eq!(
            observation.observed_owner,
            crate::proto::QuarantineRecoveryObservedOwner::Unspecified as i32
        );
        assert_eq!(observation.attempt_count, 7);
        assert_eq!(observation.next_attempt_at.unwrap().seconds, 1_786_800_000);
    }

    #[tokio::test]
    async fn startup_recovery_rolls_live_resuspend_back_to_its_predecessor() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let predecessor_id = insert_suspending_resuspend(&conn, &sandbox_id);
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id.parse().unwrap(),
            crate::db::RuntimeFields {
                uid: 10000,
                slot: 7,
                guest_cid: 37,
                pid: std::process::id(),
                spaced_host_pid: Some(10001),
                netgtw_pid: 10002,
                vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
                api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
                spaced_endpoint_proof: None,
                space_api_route_identity:
                    pplx_space_common_core::types::SpaceApiRouteIdentity::default(),
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            },
        )
        .unwrap();

        let cache_dir = tempfile::tempdir().unwrap();
        let sandbox_dir = cache_dir.path().join(&sandbox_id);
        let staging_dir = suspend_snapshot_staging_dir(&sandbox_dir, LocalArtifactId::new());
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::write(staging_dir.join("vmstate"), b"partial").unwrap();

        let service = service_with_runtime_probe(conn, true);
        let records = service
            .recover_prelaunch_startup_states_in(cache_dir.path(), &Default::default())
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].previous_state, SandboxState::Suspending);
        assert_eq!(
            records[0].outcome,
            crate::db::StartupRecoveryOutcome::State(SandboxState::Paused)
        );
        assert!(
            staging_dir.exists(),
            "state rollback must retain staging until node-manager confirms writer exclusion and deletion"
        );

        let db = service.db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Paused);
        assert_eq!(row.uid, Some(10000));
        assert_eq!(row.current_local_artifact_id, Some(predecessor_id));
        assert_eq!(row.current_local_artifact_kind, Some(ArtifactKind::Full));
        assert_eq!(
            row.current_local_artifact_origin,
            Some(ArtifactOrigin::Suspend)
        );
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
        assert!(row.failure_reason.is_none());
    }

    fn access_idle_policy(timeout_secs: u64) -> AutoLifecyclePolicy {
        AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: timeout_secs,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        }
        .normalized()
    }

    #[tokio::test]
    async fn suspend_queued_behind_concurrent_wake_is_aborted() {
        // The ECW pause/wake oscillation: a Suspend RPC arrives while a wake
        // holds the sandbox's wake lock, queues behind it, and — once the wake
        // completes and releases the lock — re-suspends the sandbox the caller
        // just woke, interrupting the data plane. A suspend whose request
        // predates the completed wake is a stale decision and must lose the race.
        //
        // The wake here is the real Wake RPC, not a hand-rolled
        // `record_wake_completed`: the recording must happen while the wake
        // still holds the wake lock, or the queued suspend acquires the freed
        // lock and reads the stale record before the wake task records
        // (review-pointed ordering bug). The test guard below only sequences
        // wake-before-suspend in the lock's FIFO queue.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);

        // Park the lock so the wake and the suspend queue behind it in a
        // deterministic order: wake first, suspend second.
        let wake_lock = service.auto_lifecycle.wake_lock(&sandbox_id);
        let sequencing_guard = wake_lock.lock_owned().await;

        let wake_service = service.clone();
        let wake_sandbox_id = sandbox_id.clone();
        let wake = tokio::spawn(async move {
            wake_service
                .wake(internal_request(WakeRequest {
                    implicit_wake_source: 0,
                    sandbox_id: wake_sandbox_id,
                    blocking: true,
                    start_context: None,
                    target_state: 0,
                    runtime_upgrade: None,
                }))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let suspend_service = service.clone();
        let suspend_sandbox_id = sandbox_id.clone();
        let suspend = tokio::spawn(async move {
            suspend_service
                .suspend(internal_request(SuspendRequest {
                    sandbox_id: suspend_sandbox_id,
                    blocking: true,
                    format: 0,
                }))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Release the queue: the wake completes (on the Started sandbox it is
        // a touch no-op, but it must record its completion before releasing
        // the lock), then the suspend acquires the lock and must observe it.
        drop(sequencing_guard);
        wake.await
            .unwrap()
            .expect("wake of a started sandbox must succeed");

        let error = suspend
            .await
            .unwrap()
            .expect_err("suspend that queued behind a concurrent wake must be rejected");
        assert_eq!(error.code(), tonic::Code::Aborted, "{}", error.message());
        assert!(
            error.message().contains("superseded"),
            "unexpected rejection message: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn wake_of_started_sandbox_does_not_require_netns_path() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);

        service
            .wake(internal_request(WakeRequest {
                implicit_wake_source: 0,
                sandbox_id,
                blocking: true,
                start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                    netns_path: Some(String::new()),
                    ..Default::default()
                }),
                target_state: 0,
                runtime_upgrade: None,
            }))
            .await
            .expect("a no-op wake does not consume launch context");
    }

    #[tokio::test(start_paused = true)]
    async fn suspend_proceeds_when_wake_predates_request() {
        // The complement of the arbitration case: a wake that completed
        // before the suspend request arrived does not supersede it — an
        // explicit pause of a running (previously woken) sandbox must still
        // be honored. In this harness the suspend fails later for
        // infrastructure reasons (no node-manager), but it must get past
        // arbitration: any code but Aborted.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);

        service.auto_lifecycle.record_wake_completed(&sandbox_id);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let result = service
            .suspend(internal_request(SuspendRequest {
                sandbox_id: sandbox_id.clone(),
                blocking: true,
                format: 0,
            }))
            .await;

        if let Err(error) = result {
            assert_ne!(
                error.code(),
                tonic::Code::Aborted,
                "a suspend arriving after the wake completed must not be \
                 arbitrated away: {}",
                error.message()
            );
        }
    }

    #[tokio::test]
    async fn repeated_suspend_returns_the_committed_artifact() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let artifact_id = LocalArtifactId::new();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        assert!(
            crate::db::adopt_suspended_local_artifact(
                &conn,
                &sandbox_id,
                artifact_id,
                &format!("/space/cache/{sandbox_id}/snapshot"),
            )
            .unwrap()
        );
        let service = SandboxManagerService::for_test(conn);

        let response = service
            .suspend(internal_request(SuspendRequest {
                sandbox_id,
                blocking: true,
                format: 0,
            }))
            .await
            .expect("a completed suspend must be safe to retry")
            .into_inner();

        let published = response
            .published_artifact
            .expect("the retry must return the committed artifact");
        assert_eq!(published.local_artifact_id, artifact_id.to_string());
        assert_eq!(
            published.artifact_kind,
            crate::proto::ArtifactKind::Full as i32
        );
        assert_eq!(
            published.origin,
            crate::proto::ArtifactOrigin::Suspend as i32
        );
    }

    /// Fake "open foreground stream" plumbing for the presence-touch tests:
    /// an mpsc channel whose receiver side drives the forward loop. Holding
    /// the sender keeps the host-side stream open; dropping the returned
    /// output stream simulates the client disconnecting.
    fn open_presence_test_stream(
        presence: Option<super::spaced_bridge::StreamPresence>,
    ) -> (
        tokio::sync::mpsc::Sender<Result<u32, Status>>,
        BoxedStatusStream<u32>,
    ) {
        let (host_tx, host_rx) = tokio::sync::mpsc::channel::<Result<u32, Status>>(8);
        let stream = super::spaced_bridge::forward_host_stream_with_presence(
            tokio_stream::wrappers::ReceiverStream::new(host_rx),
            Ok,
            None,
            presence,
        );
        (host_tx, stream)
    }

    fn test_stream_presence(
        service: &SandboxManagerService,
        sandbox_id: &str,
        interval_secs: u64,
        cap_secs: u64,
    ) -> super::spaced_bridge::StreamPresence {
        super::spaced_bridge::StreamPresence {
            service: service.clone(),
            sandbox_id: sandbox_id.to_string(),
            activity: GuestActivityKind::Command,
            interval: std::time::Duration::from_secs(interval_secs),
            cap: std::time::Duration::from_secs(cap_secs),
            tick_observer: None,
        }
    }

    fn seed_due_access_idle_sandbox(
        conn: &crate::db::Connection,
        sandbox_id: &str,
        timeout_secs: u64,
        idle_for_secs: u64,
    ) -> AutoLifecyclePolicy {
        let policy = access_idle_policy(timeout_secs);
        insert_test_sandbox_with_policy(conn, sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state =
            initial_runtime_state(&policy, now_secs().saturating_sub(idle_for_secs));
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(conn, sandbox_id, &runtime_state).unwrap();
        policy
    }

    fn access_idle_epoch(service: &SandboxManagerService, sandbox_id: &str) -> Option<u64> {
        service
            .lookup_sandbox(sandbox_id)
            .unwrap()
            .auto_lifecycle_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .and_then(|condition| condition.epoch_at)
    }

    fn access_idle_last_touched(service: &SandboxManagerService, sandbox_id: &str) -> Option<u64> {
        service
            .lookup_sandbox(sandbox_id)
            .unwrap()
            .auto_lifecycle_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .and_then(|condition| condition.last_touched_at)
    }

    #[tokio::test]
    async fn read_only_guest_activity_skips_state_touch_during_restart_drain() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);
        let before = access_idle_last_touched(&service, &sandbox_id);
        service.begin_draining_for_restart();

        let sandbox = service
            .ensure_active_for_read_only_guest_activity(
                None,
                &sandbox_id,
                true,
                None,
                GuestActivityKind::Access,
                "SpacedStat",
            )
            .await
            .expect("read-only activity should remain available for a started sandbox");

        assert_eq!(sandbox.state, SandboxState::Started);
        assert_eq!(access_idle_last_touched(&service, &sandbox_id), before);
        assert_eq!(service.active_state_transition_count(), 0);
    }

    #[tokio::test]
    async fn read_only_guest_activity_preserves_drain_status_for_non_started_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = SandboxManagerService::for_test(conn);
        service.begin_draining_for_restart();

        let error = match service
            .ensure_active_for_read_only_guest_activity(
                None,
                &sandbox_id,
                true,
                None,
                GuestActivityKind::Access,
                "SpacedStat",
            )
            .await
        {
            Ok(_) => panic!("implicit activation must retry on the successor during drain"),
            Err(error) => error,
        };

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("retry on successor"));
    }

    fn connect_process_request(
        sandbox_id: &str,
        route_token: &SpaceApiRouteEd25519Token,
    ) -> Request<SpacedConnectProcessRequest> {
        let mut request = internal_request(SpacedConnectProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(
                pplx_space_spaced_common::proto::operations::ConnectProcessRequest {
                    process_id: "process".to_string(),
                    timeout: None,
                    output_cursor: Some(7),
                    resume_from_oldest_retained: false,
                },
            ),
        });
        insert_space_api_route_ed25519_token(request.metadata_mut(), route_token).unwrap();
        request
    }

    fn route_authorized_service(
        conn: crate::db::Connection,
        sandbox_id: SandboxId,
    ) -> (SandboxManagerService, SpaceApiRouteEd25519Token) {
        route_authorized_service_for_port(conn, sandbox_id, None)
    }

    fn route_authorized_service_for_port(
        conn: crate::db::Connection,
        sandbox_id: SandboxId,
        port: Option<pplx_space_common_core::types::GuestPort>,
    ) -> (SandboxManagerService, SpaceApiRouteEd25519Token) {
        let identity = SandboxRouteIdentity::try_new(
            sandbox_id,
            RouteGeneration::new(1).unwrap(),
            NodeId::new(),
            NodeManagedBy::Kubernetes,
            ExecutionGeneration::new(1).unwrap(),
            SandboxActivationAttemptId::new(),
            Some(PodUid::parse("pod-uid-1").unwrap()),
        )
        .unwrap();
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &identity.clone().into(),
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();

        let key_pair = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(
            &aws_lc_rs::rand::SystemRandom::new(),
        )
        .unwrap();
        let signer = SpaceApiRouteEd25519Signer::from_pkcs8(
            SpaceApiRouteEd25519KeyId::parse("unit-test-key").unwrap(),
            key_pair.as_ref(),
        )
        .unwrap();
        let issuer = SpaceApiRouteEd25519Issuer::parse("unit-test-authority").unwrap();
        let cluster_id = ClusterId::parse("unit-test").unwrap();
        let claims = match port {
            Some(port) => SpaceApiRouteEd25519Claims::service_port_ingress(
                issuer.clone(),
                cluster_id.clone(),
                identity,
                port,
                pplx_space_common_svc::space_api_route::SpaceApiRouteProtocol::Tcp,
            ),
            None => SpaceApiRouteEd25519Claims::space_api_bridge(
                issuer.clone(),
                cluster_id.clone(),
                identity,
            ),
        }
        .unwrap();
        let route_token = signer.mint(&claims).unwrap();
        let verification_bundle = SpaceApiRouteEd25519VerificationBundle::new(
            SpaceApiRouteEd25519VerificationGeneration::new(1).unwrap(),
            issuer,
            cluster_id,
            vec![signer.public_key()],
        )
        .unwrap();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                route_ed25519_verification_bundle: Some(Arc::new(verification_bundle)),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        (service, route_token)
    }

    #[tokio::test]
    async fn guest_read_after_suspend_cleanup_preserves_refusal_and_route_fencing() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        for (wake_on_access, stale_execution, draining, expected) in [
            (false, false, false, tonic::Code::FailedPrecondition),
            (true, false, false, tonic::Code::FailedPrecondition),
            (false, true, false, tonic::Code::PermissionDenied),
            (false, false, true, tonic::Code::Unavailable),
            (false, true, true, tonic::Code::PermissionDenied),
        ] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new();
            let mut policy = access_idle_policy(120);
            if wake_on_access {
                policy
                    .wake
                    .on
                    .push(pplx_space_common_core::types::AutoLifecycleWakeSource::Access);
            }
            insert_test_sandbox_with_policy(&conn, &id.to_string(), SandboxState::Paused, &policy);
            let (service, token) = route_authorized_service(conn, id);
            if stale_execution {
                let mut identity = service
                    .lookup_sandbox(&id.to_string())
                    .unwrap()
                    .space_api_route_identity;
                identity.execution_generation = Some(ExecutionGeneration::new(2).unwrap());
                crate::db::update_space_api_route_identity(
                    &service.db_conn().unwrap(),
                    id,
                    &identity,
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
                )
                .unwrap();
            }
            {
                let db = service.db_conn().unwrap();
                crate::db::begin_suspend(&db, &id.to_string()).unwrap();
                crate::db::complete_row_intent_cleanup(
                    &db,
                    &id.to_string(),
                    CleanupCompletion::MarkSuspended,
                    Some((LocalArtifactId::new(), "/cache/test/snapshot")),
                )
                .unwrap();
            }
            let identity = service
                .lookup_sandbox(&id.to_string())
                .unwrap()
                .space_api_route_identity;
            identity
                .clone()
                .try_into_complete()
                .expect_err("suspend cleanup must clear the active route binding");
            assert!(identity.execution_generation.is_some());
            let before = service.lookup_sandbox(&id.to_string()).unwrap();
            if draining {
                service.begin_draining_for_restart();
            }
            let mut request = internal_request(SpacedReadRequest {
                sandbox_id: id.to_string(),
                operation: Some(pplx_space_spaced_common::proto::operations::ReadRequest {
                    path: "/marker".to_string(),
                    ..Default::default()
                }),
            });
            insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
            let error = service
                .spaced_read(request)
                .await
                .err()
                .expect("read must be refused");
            assert_eq!(
                error.code(),
                expected,
                "wake={wake_on_access}, stale={stale_execution}, drain={draining}: {error}"
            );
            if expected == tonic::Code::FailedPrecondition {
                assert_eq!(
                    error.message(),
                    if wake_on_access {
                        "sandbox is not running (suspended); reconnect before guest access"
                    } else {
                        "sandbox is suspended, not started"
                    }
                );
            }
            let after = service.lookup_sandbox(&id.to_string()).unwrap();
            assert_eq!(after.state, SandboxState::Suspended);
            assert_eq!(after.auto_lifecycle_state, before.auto_lifecycle_state);
            assert_eq!(
                after.space_api_route_identity,
                before.space_api_route_identity
            );
            assert_eq!(
                after.current_local_artifact_id,
                before.current_local_artifact_id
            );
        }
    }

    #[tokio::test]
    async fn stale_guest_route_cannot_touch_activity_after_waiting_for_wake_lock() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }

        for state in [SandboxState::Started, SandboxState::Paused] {
            let db = crate::db::test_conn();
            let id = SandboxId::new();
            insert_test_sandbox(&db, &id.to_string(), state);
            let (service, token) = route_authorized_service(db, id);
            let request = connect_process_request(&id.to_string(), &token);
            let auth = service
                .prepare_space_api_bridge(&request, &id.to_string(), SpacedRpc::ConnectProcess)
                .unwrap();
            let before = service.lookup_sandbox(&id.to_string()).unwrap();
            let lock = service
                .auto_lifecycle
                .wake_lock(&before.id)
                .lock_owned()
                .await;
            let pending = service.ensure_active_for_authorized_guest_activity(
                &auth,
                &before.id,
                true,
                None,
                GuestActivityKind::Access,
            );
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
            let mut newer = before.space_api_route_identity.clone();
            newer.route_generation = Some(RouteGeneration::new(2).unwrap());
            crate::db::update_space_api_route_identity(
                &service.db_conn().unwrap(),
                id,
                &newer,
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
            )
            .unwrap();
            drop(lock);
            assert_eq!(
                pending
                    .await
                    .err()
                    .expect("stale route must be rejected")
                    .code(),
                tonic::Code::PermissionDenied
            );
            let after = service.lookup_sandbox(&before.id).unwrap();
            assert_eq!(after.state, before.state);
            assert_eq!(after.auto_lifecycle_state, before.auto_lifecycle_state);
        }
    }

    #[tokio::test]
    async fn restart_drain_allows_process_reconnect_for_started_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let (service, route_token) = route_authorized_service(conn, sandbox_id.parse().unwrap());
        service.begin_draining_for_restart();

        let error = match call_through_restart_admission(
            &service,
            "/space.sandbox.manager.v1.SandboxManager/SpacedConnectProcess",
            connect_process_request(&sandbox_id, &route_token),
            |service, request| async move {
                SandboxManager::spaced_connect_process(&service, request).await
            },
        )
        .await
        {
            Ok(_) => panic!("the unit test has no spaced-host socket"),
            Err(error) => error,
        };

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            error
                .message()
                .contains("spaced endpoint proof is temporarily unavailable"),
            "started reconnect should reach endpoint attestation during drain: {error}"
        );
    }

    #[tokio::test]
    async fn restart_drain_rejects_process_reconnect_that_requires_activation() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let (service, route_token) = route_authorized_service(conn, sandbox_id.parse().unwrap());
        service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = 'suspended' WHERE id = ?1",
                crate::db::fixture::params![sandbox_id],
            )
            .unwrap();
        service.begin_draining_for_restart();

        let error = match SandboxManager::spaced_connect_process(
            &service,
            connect_process_request(&sandbox_id, &route_token),
        )
        .await
        {
            Ok(_) => panic!("activation should be rejected during drain"),
            Err(error) => error,
        };

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("retry on successor"));
    }

    fn last_access_at(service: &SandboxManagerService, sandbox_id: &str) -> Option<u64> {
        service
            .lookup_sandbox(sandbox_id)
            .unwrap()
            .auto_lifecycle_state
            .last_access_at
    }

    fn last_command_at(service: &SandboxManagerService, sandbox_id: &str) -> Option<u64> {
        service
            .lookup_sandbox(sandbox_id)
            .unwrap()
            .auto_lifecycle_state
            .last_command_at
    }

    #[tokio::test]
    async fn implicit_wake_honors_local_policy_after_cluster_replay() {
        for state in [SandboxState::Paused, SandboxState::Started] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &id, state);
            let service = SandboxManagerService::for_test(conn);
            service
                .set_auto_lifecycle_policy_exact(
                    &id,
                    AutoLifecyclePolicy::disabled(),
                    false,
                    crate::db::PolicyAuthority::Local,
                )
                .await
                .unwrap();
            let mut cluster_policy = AutoLifecyclePolicy::disabled();
            cluster_policy
                .wake
                .on
                .push(pplx_space_common_core::types::AutoLifecycleWakeSource::Access);
            service
                .set_auto_lifecycle_policy(internal_request(
                    crate::proto::SetAutoLifecyclePolicyRequest {
                        sandbox_id: id.clone(),
                        policy: Some(crate::auto_lifecycle::policy_to_proto(&cluster_policy)),
                        authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
                    },
                ))
                .await
                .unwrap();
            assert!(
                !service
                    .lookup_sandbox(&id)
                    .unwrap()
                    .auto_lifecycle
                    .wake_on_access()
            );
            let wake_lock = service.auto_lifecycle.wake_lock(&id);
            let held = wake_lock.lock().await;
            let wake = service.wake(internal_request(WakeRequest {
                sandbox_id: id.clone(),
                blocking: true,
                implicit_wake_source: crate::proto::AutoLifecycleWakeSource::Access as i32,
                ..Default::default()
            }));
            tokio::pin!(wake);
            assert!(futures_util::poll!(&mut wake).is_pending());
            drop(held);
            let result = wake.await;
            if state == SandboxState::Paused {
                let error = result.unwrap_err();
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert_eq!(error.message(), "sandbox is paused, not started");
                assert_eq!(
                    pplx_space_common_grpc::sandbox_not_running_state(&error),
                    Some("paused")
                );
                assert!(service.auto_lifecycle.last_wake_completed_at(&id).is_none());
            } else {
                result.unwrap();
            }
            assert_eq!(service.lookup_sandbox(&id).unwrap().state, state);
        }
    }

    #[tokio::test]
    async fn guest_access_to_a_stopped_sandbox_is_a_not_running_refusal() {
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }
        for (state, tagged) in [
            (SandboxState::Stopped, true),
            (SandboxState::Suspended, false),
        ] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new();
            let mut policy = access_idle_policy(120);
            policy
                .wake
                .on
                .push(pplx_space_common_core::types::AutoLifecycleWakeSource::Access);
            insert_test_sandbox_with_policy(&conn, &id.to_string(), SandboxState::Paused, &policy);
            let (service, token) = route_authorized_service(conn, id);
            {
                // Teardown retires the route binding; only the end state differs.
                let db = service.db_conn().unwrap();
                crate::db::begin_suspend(&db, &id.to_string()).unwrap();
                crate::db::complete_row_intent_cleanup(
                    &db,
                    &id.to_string(),
                    CleanupCompletion::MarkSuspended,
                    Some((LocalArtifactId::new(), "/cache/test/snapshot")),
                )
                .unwrap();
                crate::db::test_fixtures::state_write_fixtures::force_state(
                    &db,
                    &id.to_string(),
                    state,
                );
            }
            let mut request = internal_request(SpacedReadRequest {
                sandbox_id: id.to_string(),
                operation: Some(pplx_space_spaced_common::proto::operations::ReadRequest {
                    path: "/marker".to_string(),
                    ..Default::default()
                }),
            });
            insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
            let error = service.spaced_read(request).await.err().expect("refused");
            assert_eq!(
                error.code(),
                tonic::Code::FailedPrecondition,
                "{state}: {error}"
            );
            assert_eq!(
                pplx_space_common_grpc::sandbox_not_running_state(&error),
                tagged.then_some(state.as_str()),
                "{state}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn implicit_wake_of_a_terminal_sandbox_is_a_not_running_refusal() {
        for state in [
            SandboxState::Deleting,
            SandboxState::Failed,
            SandboxState::Quarantined,
        ] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &id, state);
            let service = SandboxManagerService::for_test(conn);
            let error = service
                .wake(internal_request(WakeRequest {
                    sandbox_id: id.clone(),
                    blocking: true,
                    implicit_wake_source: crate::proto::AutoLifecycleWakeSource::Command as i32,
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{state}");
            assert_eq!(
                pplx_space_common_grpc::sandbox_not_running_state(&error),
                Some(state.as_str()),
                "{state}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn explicit_wake_does_not_touch_access_idle_clock() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(60);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state = initial_runtime_state(&policy, 1_234);
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();
        let service = SandboxManagerService::for_test(conn);

        let before = access_idle_last_touched(&service, &sandbox_id);
        let before_access = last_access_at(&service, &sandbox_id);
        service
            .wake(internal_request(WakeRequest {
                implicit_wake_source: 0,
                sandbox_id: sandbox_id.clone(),
                blocking: true,
                start_context: None,
                target_state: 0,
                runtime_upgrade: None,
            }))
            .await
            .expect("explicit wake of a started sandbox should be a no-op");

        assert_eq!(
            access_idle_last_touched(&service, &sandbox_id),
            before,
            "explicit lifecycle wake must not reset access idle timers"
        );
        assert_eq!(
            last_access_at(&service, &sandbox_id),
            before_access,
            "explicit lifecycle wake must not create guest activity"
        );
        assert!(
            service
                .auto_lifecycle
                .last_wake_completed_at(&sandbox_id)
                .is_some(),
            "explicit wake should still record wake completion for suspend arbitration"
        );
    }

    // An identity-free wake of a dormant direct sandbox carries a start context
    // without a namespace. It must be told to supply a route identity, which
    // the caller can act on, not that a runtime namespace is missing.
    #[tokio::test]
    async fn identity_free_wake_with_a_start_context_asks_for_a_route_identity() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let service = SandboxManagerService::for_test(conn);

        let error = service
            .wake(internal_request(WakeRequest {
                implicit_wake_source: 0,
                sandbox_id,
                blocking: true,
                start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                    netns_path: Some(String::new()),
                    ..Default::default()
                }),
                target_state: 0,
                runtime_upgrade: None,
            }))
            .await
            .expect_err("an identity-free dormant wake must not start the sandbox");

        assert!(
            pplx_space_sbx_mgr_client::is_authoritative_route_identity_required(&error),
            "{error:?}"
        );
    }

    #[test]
    fn direct_wake_resources_preserve_manager_owned_network_context() {
        use pplx_space_sbx_mgr_client::proto::{
            SandboxSubstrateKind, SpaceApiRouteIdentity, WakeStartContext,
        };

        for substrate in [
            SandboxSubstrateKind::Unspecified,
            SandboxSubstrateKind::Direct,
            SandboxSubstrateKind::Kubernetes,
        ] {
            for vcpu in [None, Some(8)] {
                let start_context = WakeStartContext {
                    vcpu,
                    route_identity: Some(SpaceApiRouteIdentity {
                        route_generation: Some(1),
                        current_node_id: Some(Uuid::now_v7().to_string()),
                        substrate_kind: substrate as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let network_context = WakeNetworkContext::for_wake_start_context(&start_context)
                    .netns_path_for_launch();
                match substrate {
                    SandboxSubstrateKind::Unspecified | SandboxSubstrateKind::Direct => {
                        assert_eq!(
                            network_context.expect("Direct wake uses the manager-owned namespace"),
                            None,
                            "{substrate:?}, {vcpu:?}"
                        );
                    }
                    SandboxSubstrateKind::Kubernetes => {
                        assert_eq!(
                            network_context
                                .expect_err("Kubernetes wake requires a network namespace")
                                .message(),
                            pplx_space_common_core::wake_signals::MISSING_RUNTIME_CONTEXT_MARKER
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn checkpoint_stage_without_netns_emits_wake_failed() {
        use tracing_subscriber::layer::SubscriberExt as _;

        // Arrange.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let service = SandboxManagerService::for_test(conn);
        let capture = SandboxEventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let guard = tracing::subscriber::set_default(subscriber);

        // Act.
        let error = service
            .wake(internal_request(WakeRequest {
                implicit_wake_source: 0,
                sandbox_id,
                blocking: true,
                start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                    netns_path: Some(String::new()),
                    ..Default::default()
                }),
                target_state: 0,
                runtime_upgrade: None,
            }))
            .await
            .expect_err("checkpoint staging without a network namespace must fail");
        drop(guard);

        // Assert.
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let emitted = capture.types.lock().unwrap().clone();
        assert!(
            emitted.iter().any(|event_type| event_type == "wake_failed"),
            "checkpoint staging failure must emit wake_failed; saw {emitted:?}"
        );
    }

    #[test]
    fn wake_start_mapping_preserves_wait_shape_and_restore_context() {
        for populated in [false, true] {
            for target_state in [
                SandboxState::Started,
                SandboxState::Paused,
                SandboxState::Stopped,
            ] {
                let start = WakeStartConfig {
                    network_context: WakeNetworkContext::RuntimePath(
                        "/var/run/netns/sns-test".into(),
                    ),
                    target_state,
                    vcpu: populated.then_some(0),
                    memory_mib: populated.then_some(512),
                    suspend_restore_storage_uri: populated.then(|| "s3://bucket/checkpoint".into()),
                    suspend_restore_source_node: populated.then(|| "source-node".into()),
                    expected_local_artifact_id: populated
                        .then(|| pplx_space_common_core::types::LocalArtifactId::new().to_string()),
                    operation_activation_attempt_id: populated
                        .then(SandboxActivationAttemptId::new),
                    ..Default::default()
                };
                for restore in [true, false] {
                    for timeout in [None, Some(0), Some(30)] {
                        let request = start.start_request("sandbox-id", restore, timeout).unwrap();
                        assert_eq!(request.sandbox_id, "sandbox-id");
                        assert_eq!(request.vcpu, start.vcpu);
                        assert_eq!(request.memory_mib, start.memory_mib);
                        assert_eq!(request.blocking, timeout.is_some());
                        assert_eq!(request.ready_timeout_secs, timeout);
                        assert_eq!(
                            request.target_state,
                            match target_state {
                                SandboxState::Paused =>
                                    crate::proto::WakeTargetState::Paused as i32,
                                SandboxState::Stopped => {
                                    crate::proto::WakeTargetState::Stopped as i32
                                }
                                _ => crate::proto::WakeTargetState::Started as i32,
                            }
                        );
                        assert_eq!(
                            request.netns_path.as_deref(),
                            Some("/var/run/netns/sns-test")
                        );
                        assert_eq!(request.checkpoint.as_deref(), restore.then_some("suspend"));
                        assert_eq!(
                            request.suspend_restore_storage_uri,
                            restore
                                .then(|| start.suspend_restore_storage_uri.clone())
                                .flatten()
                        );
                        assert_eq!(
                            request.suspend_restore_source_node,
                            restore
                                .then(|| start.suspend_restore_source_node.clone())
                                .flatten()
                        );
                        // The artifact id travels with the rest of the restore
                        // context and only with it: a populated config that is
                        // not restoring must not forward the id.
                        assert_eq!(
                            request.expected_local_artifact_id,
                            restore
                                .then(|| start.expected_local_artifact_id.clone())
                                .flatten()
                        );
                        assert_eq!(
                            request.operation_activation_attempt_id,
                            start
                                .operation_activation_attempt_id
                                .map(|id| id.to_string())
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn wake_start_requests_preserve_claim_ineligible_wire_identity() {
        let complete = SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(7).unwrap()),
            execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            pod_uid: Some(PodUid::parse("019f8000-0000-7000-8000-0000000000bb").unwrap()),
            ..Default::default()
        };
        let mut incomplete_identities = vec![("all", SpaceApiRouteIdentity::default())];
        for missing_field in [
            "route_generation",
            "execution_generation",
            "activation_attempt_id",
            "substrate_kind",
            "pod_uid",
        ] {
            let mut identity = complete.clone();
            match missing_field {
                "route_generation" => identity.route_generation = None,
                "execution_generation" => identity.execution_generation = None,
                "activation_attempt_id" => identity.activation_attempt_id = None,
                "substrate_kind" => identity.substrate_kind = None,
                "pod_uid" => identity.pod_uid = None,
                _ => unreachable!(),
            }
            incomplete_identities.push((missing_field, identity));
        }

        for (missing_field, requested) in incomplete_identities {
            assert!(!runtime_activation_wire_identity_can_open_claim(&requested));
            let normalized = space_api_route_identity_for_runtime_activation(requested.clone());
            let start = WakeStartConfig {
                network_context: WakeNetworkContext::RuntimePath(
                    "/var/run/netns/sns-test".to_string(),
                ),
                // A resident fence must never supply missing activation authority.
                activation_auto_lifecycle: Some(AutoLifecyclePolicy::disabled()),
                expected_execution_generation: Some(ExecutionGeneration::new(99).unwrap()),
                requested_space_api_route_identity: Some(requested.clone()),
                space_api_route_identity: normalized.identity,
                provisional_activation_attempt: normalized.provisional_activation_attempt,
                ..Default::default()
            };

            let nonblocking = start
                .start_request("sandbox-id", false, None)
                .expect("runtime path should build a non-blocking start request");
            let blocking = start
                .start_request("sandbox-id", false, Some(30))
                .expect("runtime path should build a blocking start request");

            for request in [nonblocking, blocking] {
                let forwarded = space_api_route_identity_from_start_request(&request)
                    .expect("forwarded route identity should parse");
                assert_eq!(forwarded, requested);
                assert!(
                    !runtime_activation_wire_identity_can_open_claim(&forwarded),
                    "missing {missing_field} became claim-eligible across Wake -> Start"
                );
            }
        }
    }

    #[tokio::test]
    async fn guest_activity_records_clock_without_lifecycle_policy() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);
        let before = now_secs();

        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        service
            .touch_runtime_state_for_guest_activity(&sandbox, GuestActivityKind::Command)
            .unwrap();

        assert!(last_access_at(&service, &sandbox_id).is_some_and(|timestamp| timestamp >= before));
        assert!(
            last_command_at(&service, &sandbox_id).is_some_and(|timestamp| timestamp >= before)
        );
        assert!(
            service
                .lookup_sandbox(&sandbox_id)
                .unwrap()
                .auto_lifecycle_state
                .conditions
                .is_empty(),
            "disabled lifecycle policy must not synthesize scheduling conditions"
        );
    }

    // Guest activity is recorded at one-second resolution, so a repeat within
    // the same second changes nothing. It must return without taking the
    // database lock that every other sandbox's operations queue on.
    #[tokio::test]
    async fn unchanged_guest_activity_does_not_wait_for_the_database() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);
        for _ in 0..5 {
            let second = now_secs();
            let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
            service
                .touch_runtime_state_for_guest_activity(&sandbox, GuestActivityKind::Command)
                .unwrap();
            let touched = service.lookup_sandbox(&sandbox_id).unwrap();
            let held = service.db_conn().unwrap();
            let (done, finished) = std::sync::mpsc::channel();
            let toucher = service.clone();
            std::thread::spawn(move || {
                let result = toucher
                    .touch_runtime_state_for_guest_activity(&touched, GuestActivityKind::Command);
                let _ = done.send(result.is_ok());
            });
            let returned = finished.recv_timeout(Duration::from_millis(500));
            drop(held);
            if now_secs() != second {
                // The clock crossed a second, so the repeat could need a write.
                let _ = finished.recv_timeout(Duration::from_secs(5));
                continue;
            }
            assert_eq!(
                returned,
                Ok(true),
                "an unchanged touch waited for the database lock"
            );
            return;
        }
        panic!("could not touch twice within one second");
    }

    /// Let the spawned forward task run its due interval ticks after a
    /// `tokio::time::advance`.
    async fn yield_to_forward_task() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_suspend_due_remains_without_stream_presence() {
        // Baseline contract: a forwarded stream WITHOUT presence touches does
        // not refresh the idle clock — the sandbox stays due for suspend.
        // Exercise a foreground stream open past the idle timeout while the
        // epoch goes stale.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);

        let (_host_tx, _client_stream) = open_presence_test_stream(None);
        yield_to_forward_task().await;
        tokio::time::advance(std::time::Duration::from_secs(180)).await;
        yield_to_forward_task().await;

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert!(
            due_stage(&policy, &row.auto_lifecycle_state, now_secs()).is_some(),
            "without presence touches an open stream must not defer the due suspend"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_prevents_idle_suspend() {
        // The core idle-mid-command fix: while a foreground stream is open,
        // the idle clock is refreshed every interval, so the access_idle
        // stage never comes due under a running command.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);
        let seeded_epoch = access_idle_epoch(&service, &sandbox_id);

        let (host_tx, _client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 3600)));
        yield_to_forward_task().await;

        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        yield_to_forward_task().await;

        let touched_epoch = access_idle_epoch(&service, &sandbox_id);
        assert!(
            touched_epoch > seeded_epoch,
            "an open foreground stream must refresh the idle epoch \
             (seeded {seeded_epoch:?}, got {touched_epoch:?})"
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert!(
            due_stage(&policy, &row.auto_lifecycle_state, now_secs()).is_none(),
            "presence touches must keep the access_idle stage from being due"
        );
        service
            .run_due_auto_lifecycle(&sandbox_id)
            .await
            .expect("not-due transition must be a no-op");
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Started,
            "sandbox with an open foreground stream must stay started"
        );
        drop(host_tx);
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_stops_when_restart_drain_begins() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let _policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);
        let seeded_epoch = access_idle_epoch(&service, &sandbox_id);

        let (tick_observer, mut ticks) = tokio::sync::mpsc::unbounded_channel();
        let mut presence = test_stream_presence(&service, &sandbox_id, 60, 3600);
        presence.tick_observer = Some(tick_observer);
        let (_host_tx, _client_stream) = open_presence_test_stream(Some(presence));
        yield_to_forward_task().await;

        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        assert_eq!(
            ticks.recv().await,
            Some(super::spaced_bridge::StreamPresenceTouch::Applied)
        );
        let touched_epoch = access_idle_epoch(&service, &sandbox_id);
        assert!(
            touched_epoch > seeded_epoch,
            "the presence loop must touch lifecycle state before restart drain"
        );

        service.begin_draining_for_restart();
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        assert_eq!(
            ticks.recv().await,
            Some(super::spaced_bridge::StreamPresenceTouch::SkippedDraining),
            "the post-drain tick must reach and be rejected by writer admission"
        );

        assert_eq!(
            access_idle_epoch(&service, &sandbox_id),
            touched_epoch,
            "a predecessor must not write stream-presence state after restart drain begins"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_stops_after_stream_close() {
        // Client disconnect (dropping the output stream) ends the forward
        // loop — and with it the presence touches, so an abandoned session
        // is reclaimed on the normal idle schedule.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);

        let (_host_tx, client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 3600)));
        yield_to_forward_task().await;
        drop(client_stream);
        yield_to_forward_task().await;

        // Re-seed a stale epoch after the disconnect; no further ticks may
        // refresh it.
        let stale = seed_due_access_idle_sandbox_state(&service, &sandbox_id, &policy, 300);
        tokio::time::advance(std::time::Duration::from_secs(180)).await;
        yield_to_forward_task().await;

        assert_eq!(
            access_idle_epoch(&service, &sandbox_id),
            stale,
            "presence touches must stop once the client disconnected"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_stops_after_cap() {
        // The cap bounds a pathological client that never disconnects: after
        // `cap` of continuous open-stream, touches stop and idle reclaim
        // resumes even though the stream stays open.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);

        // cap = 2 intervals: ticks at +60s and +120s land (the +120s tick
        // observes elapsed >= cap and disarms), later ticks must not fire.
        let (_host_tx, _client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 120)));
        yield_to_forward_task().await;

        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        yield_to_forward_task().await;
        assert!(
            access_idle_epoch(&service, &sandbox_id) > Some(now_secs().saturating_sub(30)),
            "pre-cap tick must touch"
        );

        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        yield_to_forward_task().await;

        let stale = seed_due_access_idle_sandbox_state(&service, &sandbox_id, &policy, 300);
        tokio::time::advance(std::time::Duration::from_secs(180)).await;
        yield_to_forward_task().await;

        assert_eq!(
            access_idle_epoch(&service, &sandbox_id),
            stale,
            "presence touches must stop after the cap"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_skips_non_started_sandbox() {
        // touch_sources re-arms conditions regardless of row state; the
        // presence path must gate on Started so a background task never
        // re-arms a suspended row.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::test_fixtures::state_write_fixtures::force_state(
                &db,
                &sandbox_id,
                SandboxState::Suspended,
            );
        }
        let stale = seed_due_access_idle_sandbox_state(&service, &sandbox_id, &policy, 300);

        let (_host_tx, _client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 3600)));
        yield_to_forward_task().await;
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        yield_to_forward_task().await;

        assert_eq!(
            access_idle_epoch(&service, &sandbox_id),
            stale,
            "a presence touch must not re-arm a non-Started sandbox"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_skips_while_wake_lock_held_and_does_not_block_forwarding() {
        // Pins the try_lock decision: with the wake lock held (e.g. a long
        // suspend publication in flight), the tick is skipped rather than
        // queued — and crucially the data stream keeps forwarding instead of
        // stalling behind the lock.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);
        let stale = seed_due_access_idle_sandbox_state(&service, &sandbox_id, &policy, 300);

        let wake_lock = service.auto_lifecycle.wake_lock(&sandbox_id);
        let _wake_guard = wake_lock.lock_owned().await;

        let (host_tx, mut client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 3600)));
        yield_to_forward_task().await;
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        yield_to_forward_task().await;

        assert_eq!(
            access_idle_epoch(&service, &sandbox_id),
            stale,
            "a contended wake lock must skip the tick, not queue the touch"
        );

        host_tx.send(Ok(7)).await.unwrap();
        let forwarded = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures_util::StreamExt::next(&mut client_stream),
        )
        .await
        .expect("forwarding must not stall behind the held wake lock")
        .expect("stream must still be open")
        .expect("event must forward cleanly");
        assert_eq!(forwarded, 7);
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_cap_zero_disables_touches_without_warn() {
        // An explicit cap of zero means "presence disabled for this stream":
        // no tick ever arms (and therefore no cap-reached warn fires on the
        // first tick — the knob is not a one-warn footgun).
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);
        let stale = seed_due_access_idle_sandbox_state(&service, &sandbox_id, &policy, 300);

        let (_host_tx, _client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 0)));
        yield_to_forward_task().await;
        tokio::time::advance(std::time::Duration::from_secs(180)).await;
        yield_to_forward_task().await;

        assert_eq!(
            access_idle_epoch(&service, &sandbox_id),
            stale,
            "cap=0 must disable presence touches entirely"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_presence_touch_survives_deleted_sandbox_and_keeps_forwarding() {
        // The touch-error path: a sandbox row deleted under an open stream
        // makes every tick fail with NotFound — logged, never torn down. The
        // stream must keep forwarding events as if presence weren't there.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        seed_due_access_idle_sandbox(&conn, &sandbox_id, 120, 300);
        let service = SandboxManagerService::for_test(conn);

        let (host_tx, mut client_stream) =
            open_presence_test_stream(Some(test_stream_presence(&service, &sandbox_id, 60, 3600)));
        yield_to_forward_task().await;
        {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::delete_sandbox(&db, &sandbox_id).unwrap();
        }

        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        yield_to_forward_task().await;

        host_tx.send(Ok(11)).await.unwrap();
        let forwarded = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures_util::StreamExt::next(&mut client_stream),
        )
        .await
        .expect("forwarding must survive failing presence touches")
        .expect("stream must still be open")
        .expect("event must forward cleanly");
        assert_eq!(forwarded, 11);
    }

    /// Re-seed a stale (due) access_idle epoch directly in the DB and return
    /// it, for asserting that no later presence tick refreshes it.
    fn seed_due_access_idle_sandbox_state(
        service: &SandboxManagerService,
        sandbox_id: &str,
        policy: &AutoLifecyclePolicy,
        idle_for_secs: u64,
    ) -> Option<u64> {
        let mut runtime_state =
            initial_runtime_state(policy, now_secs().saturating_sub(idle_for_secs));
        recompute_runtime_state(&mut runtime_state, policy);
        let db = service.db.lock().expect("mutex poisoned");
        crate::db::update_auto_lifecycle_state(&db, sandbox_id, &runtime_state).unwrap();
        runtime_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .and_then(|condition| condition.epoch_at)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wake_completion_record_is_visible_to_next_wake_lock_holder() {
        // Pins the under-lock recording rule (review-pointed ordering bug):
        // the Wake RPC used to write the wake-completion record after the
        // wake lock was already released, so a suspend parked on the lock
        // could acquire it and run its superseded-by-wake check against the
        // stale record. The probe below takes the suspend's place in the
        // lock queue: whenever it acquires the lock after a wake, the record
        // must already be visible. With the record written under the lock
        // this holds deterministically (mutex happens-before). The converse
        // is a tripwire, not a guarantee: the buggy ordering only loses this
        // race under preemption (its unlock→record window has no await
        // point), so on a quiet runtime a regression may pass some rounds —
        // but any failure here means the record has moved outside the lock.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Started);
        let service = SandboxManagerService::for_test(conn);

        for round in 0..32 {
            // Clears the wake-completion record (and the lock maps) so each
            // round starts with no recorded wake.
            service.auto_lifecycle.remove_sandbox(&sandbox_id);
            let wake_lock = service.auto_lifecycle.wake_lock(&sandbox_id);
            let sequencing_guard = wake_lock.clone().lock_owned().await;

            let wake_service = service.clone();
            let wake_sandbox_id = sandbox_id.clone();
            let wake = tokio::spawn(async move {
                wake_service
                    .wake(internal_request(WakeRequest {
                        implicit_wake_source: 0,
                        sandbox_id: wake_sandbox_id,
                        blocking: true,
                        start_context: None,
                        target_state: 0,
                        runtime_upgrade: None,
                    }))
                    .await
            });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;

            let probe_service = service.clone();
            let probe_sandbox_id = sandbox_id.clone();
            let probe_lock = wake_lock.clone();
            let probe = tokio::spawn(async move {
                let _guard = probe_lock.lock_owned().await;
                probe_service
                    .auto_lifecycle
                    .last_wake_completed_at(&probe_sandbox_id)
                    .is_some()
            });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;

            drop(sequencing_guard);
            let record_visible = probe.await.unwrap();
            wake.await.unwrap().expect("wake of started sandbox");
            assert!(
                record_visible,
                "round {round}: the wake-completion record must be written \
                 before the wake lock is released, or a queued suspend reads \
                 a stale record and arbitration misses the wake"
            );
        }
    }

    #[tokio::test]
    async fn due_auto_lifecycle_records_nothing_on_a_retained_restoring_row() {
        // An auto-lifecycle reconciler can attempt an
        // `access_idle -> suspend` transition on the retained `restoring`
        // row of an already-deleted sandbox: Suspend skipped the action
        // (not Started/Paused) but still recorded the stage as fired, and
        // that write bumped `updated_at`, which is the quiescence clock the
        // cluster-deleted eviction of that very row was waiting on. A row
        // whose due action cannot act on its state must leave the transition
        // with no write at all.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(1);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state = initial_runtime_state(&policy, now_secs().saturating_sub(10));
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();
        // Stage the retained shape: `restoring` with a persisted staging
        // failure and an `updated_at` well in the past (the restore columns
        // may be NULL on a restoring row; the CHECK only forbids them off it).
        let stale_updated_at = (now_secs() - 3600).to_string();
        conn.execute(
            "UPDATE sandboxes SET state = 'restoring', \
             failure_reason = 'cross-node restore artifact staging failed: test', \
             updated_at = ?1 WHERE id = ?2",
            crate::db::fixture::params![stale_updated_at, sandbox_id],
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        service
            .run_due_auto_lifecycle(&sandbox_id)
            .await
            .expect("a non-actionable row is skipped, not an error");

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Restoring, "state untouched");
        assert_eq!(
            row.updated_at, stale_updated_at,
            "no write may bump updated_at"
        );
        assert_eq!(
            row.auto_lifecycle_state, runtime_state,
            "no stage may be recorded as fired"
        );
        assert_eq!(
            row.failure_reason.as_deref(),
            Some("cross-node restore artifact staging failed: test"),
            "marker untouched"
        );
        assert_gated_row_deferred_not_repicked(&service, &sandbox_id, &runtime_state);
    }

    /// A gated row's scan deadline is pushed out by the grace — and only
    /// that: the runtime state still carries the true deadline — so the scan
    /// does not re-pick it inside the grace.
    fn assert_gated_row_deferred_not_repicked(
        service: &SandboxManagerService,
        sandbox_id: &str,
        runtime_state: &AutoLifecycleRuntimeState,
    ) {
        let row = service.lookup_sandbox(sandbox_id).unwrap();
        let deferred_to = row
            .next_auto_event_at
            .expect("the scan deadline stays set, only pushed out");
        assert!(
            deferred_to > now_secs() && deferred_to <= now_secs() + 120,
            "deadline deferred by a short grace, got {deferred_to} vs now {}",
            now_secs()
        );
        assert_eq!(
            row.auto_lifecycle_state.next_auto_event_at, runtime_state.next_auto_event_at,
            "the runtime state's own deadline is untouched"
        );
        let db = service.db.lock().expect("mutex poisoned");
        let mut cursor = crate::db::AutoLifecycleScan::default();
        let picked = crate::db::due_auto_lifecycle_batch(&db, now_secs(), &[], 8, &mut cursor)
            .unwrap()
            .into_iter()
            .any(|due| due.storage_id == sandbox_id);
        assert!(
            !picked,
            "a gated row must not be re-picked inside the grace"
        );
    }

    #[tokio::test]
    async fn due_auto_lifecycle_no_due_stage_writes_nothing_on_a_retained_restoring_row() {
        // The other write path: a row selected on a due deadline whose
        // re-read finds no due stage (here: the only stage already fired)
        // normally persists the recomputed state to clear the deadline.
        // On a retained `restoring` row that persist would bump
        // `updated_at` all the same, so it must be skipped too.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(1);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state = initial_runtime_state(&policy, now_secs().saturating_sub(10));
        recompute_runtime_state(&mut runtime_state, &policy);
        // Mark the only stage fired so `due_stage` returns None, but leave
        // a stale due deadline on the row as the scan would have seen it.
        runtime_state
            .conditions
            .get_mut(&AutoLifecycleConditionSetId::AccessIdle)
            .unwrap()
            .last_fired_stage_index = Some(0);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();
        let stale_updated_at = (now_secs() - 3600).to_string();
        conn.execute(
            "UPDATE sandboxes SET state = 'restoring', \
             failure_reason = 'cross-node restore artifact staging failed: test', \
             next_auto_event_at = 1, updated_at = ?1 WHERE id = ?2",
            crate::db::fixture::params![stale_updated_at, sandbox_id],
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        service
            .run_due_auto_lifecycle(&sandbox_id)
            .await
            .expect("nothing due on a settled row is Ok; on a restoring row it is a skip");

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Restoring);
        assert_eq!(
            row.updated_at, stale_updated_at,
            "no write may bump updated_at"
        );
        assert_eq!(
            row.auto_lifecycle_state, runtime_state,
            "runtime state untouched"
        );
        assert_gated_row_deferred_not_repicked(&service, &sandbox_id, &runtime_state);
    }

    #[tokio::test]
    async fn due_auto_lifecycle_skips_when_wake_completed_while_waiting() {
        // The scheduler-side arbitration: an idle-suspend decision made
        // before queuing on the wake lock must not execute after a wake
        // completed while it waited. The skip is silent (Ok) — there is no
        // caller to inform — and the sandbox must still be Started.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(1);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state = initial_runtime_state(&policy, now_secs().saturating_sub(10));
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();
        let service = SandboxManagerService::for_test(conn);

        let wake_lock = service.auto_lifecycle.wake_lock(&sandbox_id);
        let wake_guard = wake_lock.lock_owned().await;

        let scheduler_service = service.clone();
        let scheduler_storage_id = sandbox_id.clone();
        let transition = tokio::spawn(async move {
            scheduler_service
                .run_due_auto_lifecycle(&scheduler_storage_id)
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        service.auto_lifecycle.record_wake_completed(&sandbox_id);
        drop(wake_guard);

        let result = transition.await.unwrap();
        assert!(
            result.is_ok(),
            "superseded due transition must skip, not error: {result:?}"
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            row.state,
            SandboxState::Started,
            "a due suspend that queued behind a wake must not execute"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn due_auto_lifecycle_suspend_does_not_reacquire_wake_lock() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(1);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let mut runtime_state = initial_runtime_state(&policy, now_secs().saturating_sub(10));
        recompute_runtime_state(&mut runtime_state, &policy);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &runtime_state).unwrap();
        let service = SandboxManagerService::for_test(conn);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            service.run_due_auto_lifecycle(&sandbox_id),
        )
        .await;

        assert!(
            result.is_ok(),
            "auto-lifecycle suspend must not deadlock on its own wake lock"
        );
    }

    fn touch_auto_lifecycle_access_request(
        sandbox_id: &str,
        observed_at: u64,
    ) -> TouchAutoLifecycleRequest {
        TouchAutoLifecycleRequest {
            sandbox_id: sandbox_id.to_string(),
            source: crate::proto::AutoLifecycleActivitySource::Access as i32,
            access_idle_update: None,
            observed_at: Some(prost_types::Timestamp {
                seconds: observed_at as i64,
                nanos: 0,
            }),
        }
    }

    #[tokio::test]
    async fn restore_as_new_applies_create_owner_policy_before_materialization() {
        let service = service_for_teardown_test(crate::db::test_conn());
        for owner in ["tmplmgr@local", "orphaned@local"] {
            let id = SandboxId::new().to_string();
            let request = StartRequest {
                sandbox_id: id.clone(),
                restore_artifact_as_new: Some(crate::proto::RestoreArtifactAsNewContext {
                    target_owner: owner.to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            assert_eq!(
                service
                    .start(internal_request(request))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
            assert!(
                crate::db::get_sandbox(&service.db_conn().unwrap(), &id)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_start_rejects_non_restorable_local_states() {
        // `Failed` is intentionally absent: a failed row with no registered
        // artifact and no runtime self-heals instead of refusing (see
        // `checkpoint_start_retries_after_self_heal_reach_the_restore_path`),
        // and the still-refused failed variants are pinned by the
        // `checkpoint_start_keeps_refusing_failed_row_*` tests.
        let conn = crate::db::test_conn();
        let service = SandboxManagerService::for_test(conn);

        for state in [SandboxState::Created, SandboxState::Stopped] {
            let sandbox_id = SandboxId::new().to_string();
            {
                let db = service.db.lock().expect("mutex poisoned");
                insert_test_sandbox(&db, &sandbox_id, state);
            }

            let error = service
                .start_sandbox(
                    checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                    false,
                )
                .await
                .expect_err("checkpoint start must fail closed");

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(
                error.message().contains("expected Suspended or NotFound"),
                "unexpected checkpoint start error: {}",
                error.message()
            );

            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            assert_eq!(row.state, state);
        }
    }

    #[tokio::test]
    async fn checkpoint_start_reattaches_existing_started_runtime() {
        // Reproduces the ECW access-idle reconnect hang: the first restore
        // attempt already relaunched Firecracker and flipped the row to
        // `Started`, then the restore pod retries `Start(checkpoint="suspend")`.
        // Before the fix this looped on `FailedPrecondition` until the API
        // readiness probe timed out; now it reattaches to the live runtime.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let service = service_with_runtime_probe(conn, true);

        let before = service.lookup_sandbox(&sandbox_id).unwrap();
        let mut request = checkpoint_start_request(sandbox_id.clone(), "suspend", None);
        request.activation_auto_lifecycle = Some(access_idle_policy(60).into());
        let response = service
            .start_sandbox(request, false)
            .await
            .expect("started restore retry should reattach")
            .into_inner();

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Started);
        assert_eq!(row.auto_lifecycle, before.auto_lifecycle);
        assert_eq!(row.auto_lifecycle_state, before.auto_lifecycle_state);
        assert_eq!(response.sandbox_id, sandbox_id);
        assert_eq!(response.pid, std::process::id());
        assert_eq!(response.guest_ip, "169.254.0.21");
        assert_eq!(response.guest_gateway, "169.254.0.22");
        assert_eq!(response.namespace, format!("sns-{}", row.uuid8));
    }

    #[tokio::test]
    async fn restore_artifact_as_new_start_reattaches_existing_started_runtime() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let source_sandbox_id = SandboxId::new().to_string();
        let artifact_snapshot_id = SnapshotId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let service = service_with_runtime_probe(conn, true);

        let response = service
            .start_sandbox(
                StartRequest {
                    sandbox_id: sandbox_id.clone(),
                    suspend_restore_storage_uri: Some(format!(
                        "s3://snapshots/suspend/{source_sandbox_id}/{artifact_snapshot_id}?region=us-east-1"
                    )),
                    route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                        execution_generation: Some(7),
                        ..Default::default()
                    }),
                    restore_artifact_as_new: Some(crate::proto::RestoreArtifactAsNewContext {
                        source_sandbox_id,
                        artifact_snapshot_id,
                        target_owner: format!("user:{}", Uuid::now_v7()),
                        artifact_kind: crate::proto::ArtifactKind::Disk as i32,
                        disk_io_max_operations_per_sec: None,
                        disk_io_max_bandwidth_mib_per_sec: None,
                        volume_encryption: false,
                    }),
                    ..Default::default()
                },
                false,
            )
            .await
            .expect("started restore-as-new retry should reattach")
            .into_inner();

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Started);
        assert_eq!(response.sandbox_id, sandbox_id);
        assert_eq!(response.pid, std::process::id());
        assert_eq!(response.guest_ip, "169.254.0.21");
        assert_eq!(response.guest_gateway, "169.254.0.22");
        assert_eq!(response.namespace, format!("sns-{}", row.uuid8));
    }

    /// The request the cluster-controller sends to place an evacuated stopped
    /// sandbox on this node: the sandbox's own id, a suspend checkpoint, and
    /// the exact URI of the `stop`-origin disk artifact the drain published.
    /// Exhaust the node's Firecracker launch admission.
    ///
    /// Every microVM launch on this node takes a token from this bucket first
    /// — the cold path in `start_sandbox_with_authority` and the restore path
    /// in `create.rs`, which share one bucket. Spending the only token with a
    /// near-zero refill rate leaves the gate shut for the rest of the test, so
    /// anything that still completes promptly did not attempt a launch.
    fn shut_the_launch_gate(service: &mut SandboxManagerService) {
        service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
            activation_admission_config(1, 0.001, 1.0),
        ));
        let spent = service
            .activation_admission_limiter
            .try_acquire(ActivationAdmissionKind::Restore)
            .expect("test setup should spend the only launch token");
        drop(spent);
        assert!(
            service
                .activation_admission_limiter
                .try_acquire(ActivationAdmissionKind::Cold)
                .is_err(),
            "the launch gate must actually be shut, or an absence assertion below proves nothing"
        );
    }

    /// The property, in operator terms: "your bytes are here" is answered only
    /// for the artifact that was asked for.
    ///
    /// A row that was already `Stopped` on this node is not evidence — a local
    /// stop whose disk was published for backup leaves the same shape an
    /// evacuation does. The
    /// request names the artifact it expects, so the two cases separate on a
    /// local comparison, and the whole point is that the same row gives
    /// opposite answers depending on which artifact is asked for.
    #[tokio::test]
    async fn a_stopped_row_answers_only_for_the_artifact_it_pins() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopped);
        let pin = pplx_space_common_core::types::LocalArtifactId::new();
        conn.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = 'disk',
                 current_local_artifact_origin = 'stop'
             WHERE id = ?2",
            crate::db::fixture::params![pin.to_string(), &sandbox_id],
        )
        .unwrap();
        let mut service = service_with_runtime_probe(conn, false);
        shut_the_launch_gate(&mut service);

        // Asked for the artifact it pins: converged, and reported with no
        // attachment because a stopped sandbox has no runtime.
        let mut request =
            crate::service::start::tests::materialize_stopped_request_for(&sandbox_id);
        request.expected_local_artifact_id = Some(pin.to_string());
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.start_sandbox(request, false),
        )
        .await
        .expect("a local comparison must not wait behind the Firecracker launch gate")
        .expect("the row pins exactly the requested artifact")
        .into_inner();
        assert_eq!(response.sandbox_id, sandbox_id);

        // Same row, different artifact asked for: refused rather than vouched
        // for, and the row is left exactly as found.
        let mut stale = crate::service::start::tests::materialize_stopped_request_for(&sandbox_id);
        stale.expected_local_artifact_id =
            Some(pplx_space_common_core::types::LocalArtifactId::new().to_string());
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.start_sandbox(stale, false),
        )
        .await
        .expect("the refusal must not wait behind the Firecracker launch gate")
        .expect_err("a row pinning a different artifact must not be reported as materialized");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("refusing to report a stale row"),
            "unexpected refusal: {error}"
        );

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Stopped);
        assert_eq!(row.current_local_artifact_id, Some(pin));
    }

    /// Fails closed on a row that is not the materialization's own result.
    ///
    /// The possible presence is real: a `Suspended` row under a suspend
    /// checkpoint is the input that selects `ResumeSuspendCheckpoint`, whose
    /// handler restores and launches the VM
    /// (`a_suspended_row_under_a_checkpoint_is_a_launching_fixture` in
    /// `service::start::tests` pins that classification). The stopped
    /// boundaries refuse it rather than report a sandbox they did not
    /// converge, leaving the row for whoever does own it.
    #[tokio::test]
    async fn a_stopped_target_refuses_a_row_it_did_not_materialize() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let mut service = service_with_runtime_probe(conn, false);
        shut_the_launch_gate(&mut service);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.start_sandbox(
                crate::service::start::tests::materialize_stopped_request_for(&sandbox_id),
                false,
            ),
        )
        .await
        .expect("the refusal must not wait behind the Firecracker launch gate")
        .expect_err("a suspended row is not a materialized stopped sandbox");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("while local state is"),
            "unexpected refusal: {error}"
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            row.state,
            SandboxState::Suspended,
            "the refusal must leave the row it did not own where it found it"
        );
        assert_eq!(
            row.restore_operation, None,
            "the refusal must not have opened a restore on it either"
        );
    }

    /// The RPC the cluster-controller actually calls. `Wake` is the entry
    /// point for the drain's lifecycle-preserving evacuation — `Paused` rides
    /// it today — so the stopped target has to be inert here too, not only on
    /// the `Start` it delegates staging to.
    ///
    /// `Wake` inherits the `Start` boundary's rule because it delegates the
    /// stopped target to it: a row pinning a different artifact than the one
    /// asked for is refused, not reported as evacuated.
    #[tokio::test]
    async fn waking_a_stale_stopped_row_does_not_report_it_evacuated() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopped);
        let pin = pplx_space_common_core::types::LocalArtifactId::new();
        conn.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = 'disk',
                 current_local_artifact_origin = 'stop'
             WHERE id = ?2",
            crate::db::fixture::params![pin.to_string(), &sandbox_id],
        )
        .unwrap();
        let mut service = service_with_runtime_probe(conn, false);
        shut_the_launch_gate(&mut service);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            service.wake(internal_request(WakeRequest {
                sandbox_id: sandbox_id.clone(),
                blocking: true,
                start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                    netns_path: Some("/var/run/netns/test".to_string()),
                    expected_local_artifact_id: Some(
                        pplx_space_common_core::types::LocalArtifactId::new().to_string(),
                    ),
                    suspend_restore_storage_uri: Some(format!(
                        "s3://snapshots/suspend/{sandbox_id}/{}?region=us-east-1",
                        Uuid::now_v7()
                    )),
                    route_identity: Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                        execution_generation: Some(7),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                target_state: crate::proto::WakeTargetState::Stopped as i32,
                implicit_wake_source: 0,
                runtime_upgrade: None,
            })),
        )
        .await
        .expect("a stopped target must not wait behind the Firecracker launch gate")
        .expect_err("a stale stopped row must not be reported as evacuated");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("refusing to report a stale row"),
            "unexpected refusal: {error}"
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Stopped);
        assert_eq!(row.pid, None);
        assert_eq!(row.current_local_artifact_id, Some(pin));
    }

    /// A stopped `Wake` from a Kubernetes identity needs no runtime namespace:
    /// it never launches. The same request without one reaches the stopped
    /// boundary and is answered there, rather than refused for a missing
    /// launch namespace. `Wake` inherits the boundary's liveness rule too: the
    /// otherwise-identical row whose Firecracker runtime is still live is
    /// refused, because a live VM can still write the disk the pin names.
    #[tokio::test]
    async fn waking_to_stopped_needs_no_runtime_namespace() {
        for runtime_live in [false, true] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopped);
            let pin = pplx_space_common_core::types::LocalArtifactId::new();
            conn.execute(
                "UPDATE sandboxes
                 SET current_local_artifact_id = ?1,
                     current_local_artifact_kind = 'disk',
                     current_local_artifact_origin = 'stop'
                 WHERE id = ?2",
                crate::db::fixture::params![pin.to_string(), &sandbox_id],
            )
            .unwrap();
            let mut service = service_with_runtime_probe(conn, runtime_live);
            shut_the_launch_gate(&mut service);

            let outcome = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                service.wake(internal_request(WakeRequest {
                    sandbox_id: sandbox_id.clone(),
                    blocking: true,
                    start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                        netns_path: None,
                        expected_local_artifact_id: Some(pin.to_string()),
                        suspend_restore_storage_uri: Some(format!(
                            "s3://snapshots/suspend/{sandbox_id}/{}?region=us-east-1",
                            Uuid::now_v7()
                        )),
                        route_identity: Some(
                            pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                                execution_generation: Some(7),
                                pod_uid: Some("019f8000-0000-7000-8000-0000000000bb".to_string()),
                                substrate_kind:
                                    pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes
                                        as i32,
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    }),
                    target_state: crate::proto::WakeTargetState::Stopped as i32,
                    implicit_wake_source: 0,
                    runtime_upgrade: None,
                })),
            )
            .await
            .expect("a stopped target must not wait behind the Firecracker launch gate");

            if runtime_live {
                let error = outcome
                    .expect_err("a stopped row with a live runtime must not be reported placed");
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert!(
                    error.message().contains("its runtime is still live"),
                    "unexpected refusal: {error}"
                );
            } else {
                let response = outcome
                    .expect("a materialized row pinning the requested artifact converges without a namespace")
                    .into_inner();
                assert_eq!(response.sandbox_id, sandbox_id);
            }
            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            assert_eq!(row.state, SandboxState::Stopped);
            assert_eq!(row.current_local_artifact_id, Some(pin));
        }
    }

    /// A `Wake` asking for the stopped target with no start context carries
    /// neither the artifact URI nor the expected artifact id, so delegating it
    /// to `Start` now refuses it at the wire boundary instead of letting it
    /// reach a row it has no evidence about. Better diagnosis for the same
    /// fail-closed outcome.
    #[tokio::test]
    async fn waking_to_stopped_without_a_start_context_is_refused_at_the_wire() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Paused);
        let mut service = service_with_runtime_probe(conn, true);
        shut_the_launch_gate(&mut service);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.wake(internal_request(WakeRequest {
                sandbox_id: sandbox_id.clone(),
                blocking: true,
                start_context: None,
                target_state: crate::proto::WakeTargetState::Stopped as i32,
                implicit_wake_source: 0,
                runtime_upgrade: None,
            })),
        )
        .await
        .expect("the refusal must not wait behind the Firecracker launch gate")
        .expect_err("a stopped target with no artifact to materialize is malformed");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("stopped target state requires"),
            "unexpected refusal: {error}"
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            row.state,
            SandboxState::Paused,
            "the refusal must not have resumed it on the way out"
        );
    }

    /// An interrupted materialization must stay retryable.
    ///
    /// A materialization that dies after adopting the row and before
    /// finalizing it leaves `Restoring` behind. Nothing else on the node
    /// drives that row — `wake_transition_for_state` answers `Wait` for it —
    /// so if the stopped short-circuit answered here, the sandbox would be
    /// stranded on a node it was only passing through, with its bytes in S3
    /// and no path back. The resume is Start's `ResumeRestoring` arm, which
    /// this has to reach rather than pre-empt.
    ///
    /// Reaching it is asserted positively rather than by the absence of the
    /// refusal: with the staging lock held, the first thing that arm does is
    /// answer `Unavailable`, and only code inside
    /// `restore_suspended_checkpoint_start` can produce that. Before the
    /// `Restoring` carve-out this returned `FailedPrecondition` instead.
    #[tokio::test]
    async fn an_interrupted_stopped_materialization_reaches_the_restore_retry() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let mut service = service_with_runtime_probe(conn, false);
        shut_the_launch_gate(&mut service);
        let staging_lock = service.auto_lifecycle.staging_lock(&sandbox_id);
        let _held = staging_lock.lock().await;

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.start_sandbox(
                crate::service::start::tests::materialize_stopped_request_for(&sandbox_id),
                false,
            ),
        )
        .await
        .expect("the retry must not wait behind the Firecracker launch gate")
        .expect_err("the staging lock is held, so the resume must defer");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            error.message().contains("restore staging is in progress"),
            "expected the resume arm's staging deferral, got: {error}"
        );
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Restoring,
            "a deferred resume must leave the interrupted row alone"
        );
    }

    /// The same carve-out at the RPC the cluster-controller calls. `Wake`
    /// delegates staging to `Start` only when the row is absent, so without
    /// widening that condition an interrupted materialization would reach
    /// `wake_with_start_config`, which answers `Wait` for `Restoring` and
    /// drives nothing.
    #[tokio::test]
    async fn waking_an_interrupted_stopped_materialization_resumes_it() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let mut service = service_with_runtime_probe(conn, false);
        shut_the_launch_gate(&mut service);
        let staging_lock = service.auto_lifecycle.staging_lock(&sandbox_id);
        let _held = staging_lock.lock().await;

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.wake(internal_request(WakeRequest {
                sandbox_id: sandbox_id.clone(),
                blocking: true,
                start_context: Some(pplx_space_sbx_mgr_client::proto::WakeStartContext {
                    netns_path: Some("/var/run/netns/test".to_string()),
                    expected_local_artifact_id: Some(
                        pplx_space_common_core::types::LocalArtifactId::new().to_string(),
                    ),
                    suspend_restore_storage_uri: Some(format!(
                        "s3://snapshots/suspend/{sandbox_id}/{}?region=us-east-1",
                        Uuid::now_v7()
                    )),
                    route_identity: Some(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                        execution_generation: Some(7),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                target_state: crate::proto::WakeTargetState::Stopped as i32,
                implicit_wake_source: 0,
                runtime_upgrade: None,
            })),
        )
        .await
        .expect("the retry must not wait behind the Firecracker launch gate")
        .expect_err("the staging lock is held, so the resume must defer");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(
            error.message().contains("restore staging is in progress"),
            "expected the resume arm's staging deferral, got: {error}"
        );
        assert_eq!(
            service.lookup_sandbox(&sandbox_id).unwrap().state,
            SandboxState::Restoring
        );
    }

    #[test]
    fn artifact_restore_as_new_can_materialize_a_missing_target() {
        let request = StartRequest {
            restore_artifact_as_new: Some(crate::proto::RestoreArtifactAsNewContext {
                artifact_kind: crate::proto::ArtifactKind::Disk as i32,
                disk_io_max_operations_per_sec: None,
                disk_io_max_bandwidth_mib_per_sec: None,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(SandboxManagerService::start_can_materialize_missing_sandbox(&request));
    }

    #[tokio::test]
    async fn checkpoint_start_reattach_bypasses_restore_activation_admission() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let mut service = service_with_runtime_probe(conn, true);
        service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
            activation_admission_config(1, 0.001, 1.0),
        ));

        let spent = service
            .activation_admission_limiter
            .try_acquire(ActivationAdmissionKind::Restore)
            .expect("test setup should spend the only restore token");
        drop(spent);

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            ),
        )
        .await
        .expect("idempotent reattach must not wait behind restore activation admission")
        .expect("started restore retry should reattach")
        .into_inner();

        assert_eq!(response.sandbox_id, sandbox_id);
        assert_eq!(response.pid, std::process::id());
    }

    #[tokio::test(start_paused = true)]
    async fn missing_checkpoint_restore_stages_before_activation_admission() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let mut service = service_with_runtime_probe(conn, false);
        service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
            activation_admission_config(1, 100.0, 100.0),
        ));
        let held = service
            .activation_admission_limiter
            .try_acquire(ActivationAdmissionKind::Restore)
            .expect("test setup should fill the only activation slot");

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(1),
            service.start_sandbox(checkpoint_start_request(sandbox_id, "suspend", None), false),
        )
        .await;

        result
            .expect("remote restore lookup must not wait behind process launch admission")
            .expect_err("the test cluster API is unavailable");
        drop(held);
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_start_retires_existing_restoring_runtime_before_retry() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        restage_started_as_restoring(&conn, &sandbox_id);
        // The test service's node-manager refuses every dial, so the
        // retirement's cleanup cannot start and the row keeps the intent it
        // persisted.
        let service = service_with_runtime_probe(conn, true);

        service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await
            .expect_err("ambiguous restoring runtime must be retired before retry");

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkSuspended)
        );
        assert_eq!(
            row.failure_reason.as_deref(),
            Some("checkpoint retry found an ambiguous restoring runtime")
        );
    }

    /// Apply the row's persisted cleanup completion, standing in for
    /// node-manager finishing the requested runtime kill + filesystem wipe.
    /// Tests cannot reach a real node-manager, so this drives the same
    /// `complete_async_cleanup` step production runs after the node-manager
    /// cleanup RPC succeeds.
    fn complete_persisted_cleanup(service: &SandboxManagerService, sandbox_id: &str) {
        let completion = {
            let db = service.db.lock().expect("mutex poisoned");
            crate::db::get_sandbox(&db, sandbox_id)
                .unwrap()
                .expect("sandbox row present with persisted cleanup intent")
                .cleanup_completion
                .expect("cleanup intent persisted")
        };
        SandboxManagerService::complete_async_cleanup(&service.db, sandbox_id, completion, None)
            .expect("persisted cleanup completion applies");
    }

    #[tokio::test]
    async fn cancelled_activation_persists_and_atomically_completes_its_cleanup_episode() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Created);
        let episode = crate::db::begin_activation(
            &conn,
            &sandbox_id,
            SandboxState::Created,
            crate::db::ActivationKind::ColdStart,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        let uuid8 = service.lookup_sandbox(&sandbox_id).unwrap().uuid8;
        let cleanup_guard = service
            .activation_cleanup_guard(
                &sandbox_id,
                &uuid8,
                ActivationCleanupSpec {
                    cleanup_id: episode.id,
                    completion: CleanupCompletion::MarkCreated,
                    context: "cleanup after cancelled cold activation",
                    interrupted_reason_code: SandboxQuarantineReasonCode::ColdActivationInterrupted,
                    interrupted_reason: "cold activation did not complete",
                },
            )
            .unwrap();

        drop(cleanup_guard);

        let pending = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(pending.state, SandboxState::CleanupRequired);
        assert_eq!(
            pending.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkCreated)
        );

        {
            let db = service.db.lock().unwrap();
            assert!(
                crate::db::complete_cleanup_episode(&db, &sandbox_id, episode.id)
                    .unwrap()
                    .is_some()
            );
            assert!(
                crate::db::cleanup_episode(&db, &sandbox_id)
                    .unwrap()
                    .is_none()
            );
        }
        let completed = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(completed.state, SandboxState::Created);
        assert!(completed.cleanup_completion.is_none());
    }

    #[test]
    fn operator_cleanup_does_not_adopt_an_armed_activation_episode() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Created);
        let episode = crate::db::begin_activation(
            &conn,
            &sandbox_id,
            SandboxState::Created,
            crate::db::ActivationKind::ColdStart,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        let db = crate::db::SharedConnection::new(conn);

        let targets = SandboxManagerService::recover_required_cleanup_targets(
            &db,
            super::recovery::CleanupRecoveryAuthority::RequestedOnly,
        )
        .unwrap();

        assert!(targets.is_empty());
        let db = db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Starting);
        let persisted = crate::db::cleanup_episode(&db, &sandbox_id)
            .unwrap()
            .unwrap();
        assert_eq!(persisted.id, episode.id);
        assert!(persisted.requested_at.is_none());
    }

    #[tokio::test]
    async fn requested_cleanup_recovery_does_not_infer_intent_from_live_pause() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox_with_runtime(&conn, &sandbox_id_string, SandboxState::Pausing);
        let stopping_id = SandboxId::new();
        insert_test_sandbox(&conn, &stopping_id.to_string(), SandboxState::Stopping);
        let service = SandboxManagerService::for_test(conn);

        let targets = service.recover_requested_cleanup_targets();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].sandbox_id, stopping_id);
        assert_eq!(targets[0].completion, CleanupCompletion::MarkStopped);
        let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
        assert_eq!(row.state, SandboxState::Pausing);
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
    }

    #[test]
    fn failed_rows_retain_stale_cleanup_episodes_across_recovery() {
        use super::recovery::CleanupRecoveryAuthority;
        for requested in [false, true] {
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Created);
            let episode = crate::db::begin_activation(
                &conn,
                &sandbox_id,
                SandboxState::Created,
                crate::db::ActivationKind::ColdStart,
                None,
                None,
            )
            .unwrap()
            .unwrap();
            if requested {
                crate::db::request_cleanup_episode(
                    &conn,
                    &sandbox_id,
                    episode.id,
                    SandboxQuarantineReasonCode::ColdActivationInterrupted,
                    "interrupted start",
                )
                .unwrap();
            }
            // Reconstruct a persisted Failed row with an unfinished older operation.
            conn.execute(
                "UPDATE sandboxes SET state = 'failed' WHERE id = ?1",
                [&sandbox_id],
            )
            .unwrap();
            let db = crate::db::SharedConnection::new(conn);
            for authority in [
                CleanupRecoveryAuthority::Startup,
                CleanupRecoveryAuthority::RequestedOnly,
            ] {
                assert!(
                    SandboxManagerService::recover_required_cleanup_targets(&db, authority)
                        .unwrap()
                        .is_empty()
                );
                let conn = db.lock().unwrap();
                let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
                let retained = crate::db::cleanup_episode(&conn, &sandbox_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(row.state, SandboxState::Failed);
                assert_eq!(retained.id, episode.id);
                assert_eq!(retained.requested_at.is_some(), requested);
            }
        }
    }

    #[tokio::test]
    async fn requested_cleanup_recovery_retries_cleanup_failed_and_preserves_failed() {
        let conn = crate::db::test_conn();
        let failed_id = SandboxId::new();
        let cleanup_failed_id = SandboxId::new();
        insert_test_sandbox_with_runtime(&conn, &failed_id.to_string(), SandboxState::Failed);
        insert_test_sandbox_with_runtime(
            &conn,
            &cleanup_failed_id.to_string(),
            SandboxState::CleanupFailed,
        );
        let service = SandboxManagerService::for_test(conn);

        let mut targets = service.recover_requested_cleanup_targets();
        targets.sort_by_key(|target| target.sandbox_id);

        let retained = service.lookup_sandbox(&failed_id.to_string()).unwrap();
        assert_eq!(retained.state, SandboxState::Failed);
        assert!(retained.cleanup_completion.is_none());
        let mut expected_ids = vec![cleanup_failed_id];
        expected_ids.sort_unstable();
        assert_eq!(
            targets
                .iter()
                .map(|target| target.sandbox_id)
                .collect::<Vec<_>>(),
            expected_ids
        );
        assert!(targets.iter().all(|target| {
            target.keep_filesystem && target.completion == CleanupCompletion::Quarantine
        }));
        for sandbox_id in expected_ids {
            let row = service.lookup_sandbox(&sandbox_id.to_string()).unwrap();
            assert_eq!(row.state, SandboxState::CleanupRequired);
            assert_eq!(row.cleanup_completion, Some(CleanupCompletion::Quarantine));
            assert_eq!(row.cleanup_keep_filesystem, Some(true));
        }
    }

    #[tokio::test]
    async fn requested_cleanup_recovery_does_not_adopt_live_suspend() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox_with_runtime(&conn, &sandbox_id_string, SandboxState::Suspending);
        conn.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = ?1,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?2",
            crate::db::fixture::params![Uuid::now_v7().to_string(), &sandbox_id_string],
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);

        let targets = service.recover_requested_cleanup_targets_for_ids(vec![sandbox_id]);

        assert!(targets.is_empty());
        let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
        assert_eq!(row.state, SandboxState::Suspending);
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
    }

    #[tokio::test]
    async fn requested_cleanup_recovery_treats_deleting_as_durable_delete_intent() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Deleting);
        let service = SandboxManagerService::for_test(conn);

        let targets = service.recover_requested_cleanup_targets_for_ids(vec![sandbox_id]);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].sandbox_id, sandbox_id);
        assert_eq!(targets[0].completion, CleanupCompletion::DeleteRow);
        assert!(!targets[0].keep_filesystem);
        let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
        assert_eq!(row.state, SandboxState::Deleting);
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
    }

    #[tokio::test]
    async fn requested_cleanup_recovery_completes_a_durable_pending_delete_receipt() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Deleting);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id_string,
            false,
            CleanupCompletion::DeleteRow,
        )
        .unwrap();
        let receipt = crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteReceipt {
            receipt_id: pplx_space_common_core::types::AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id,
            execution_generation: ExecutionGeneration::new(4).unwrap(),
            route_activation_id: SandboxActivationAttemptId::new(),
            policy_fingerprint:
                pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(
                    &AutoLifecyclePolicy::disabled(),
                ),
        };
        crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(&conn, receipt).unwrap();
        let service = SandboxManagerService::for_test(conn);

        let targets = service.recover_requested_cleanup_targets_for_ids(vec![sandbox_id]);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].completion, CleanupCompletion::DeleteRow);
        SandboxManagerService::complete_async_cleanup(
            &service.db,
            &sandbox_id_string,
            CleanupCompletion::DeleteRow,
            None,
        )
        .unwrap();
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::get_sandbox(&db, &sandbox_id_string)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.query_row(
                "SELECT state FROM auto_lifecycle_delete_outbox WHERE receipt_id = ?1",
                [receipt.receipt_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "completed"
        );
    }

    #[tokio::test]
    async fn requested_cleanup_recovery_defers_while_lifecycle_holds_fence() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Stopping);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id_string,
            true,
            CleanupCompletion::MarkStopped,
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        let owner = CleanupLockGuard::new(
            Arc::clone(&service.cleanup_locks),
            sandbox_id_string.clone(),
            service.cleanup_lock(&sandbox_id_string),
        );
        let fence = owner.lock_owned().await;

        let targets = service.recover_requested_cleanup_targets_for_ids(vec![sandbox_id]);

        assert!(targets.is_empty());
        assert_eq!(
            service.lookup_sandbox(&sandbox_id_string).unwrap().state,
            SandboxState::Stopping
        );
        drop(fence);

        let rediscovered = service.recover_requested_cleanup_targets();
        assert_eq!(rediscovered.len(), 1);
        assert_eq!(rediscovered[0].sandbox_id, sandbox_id);
        let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
        assert_eq!(row.state, SandboxState::Stopping);
        assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
        assert_eq!(row.cleanup_keep_filesystem, Some(true));
    }

    #[tokio::test]
    async fn recovery_scheduling_defers_to_active_runner_without_losing_durable_intent() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::CleanupRequired);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id_string,
            true,
            CleanupCompletion::Quarantine,
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        let incumbent = match service.try_cleanup_fence(&sandbox_id_string) {
            CleanupLockTryResult::Acquired(fence) => fence,
            CleanupLockTryResult::Busy => panic!("test fence must be available"),
        };
        let target = CleanupRecoveryTarget {
            sandbox_id,
            uuid8: service.lookup_sandbox(&sandbox_id_string).unwrap().uuid8,
            keep_filesystem: true,
            completion: CleanupCompletion::Quarantine,
            cleanup_id: None,
        };

        service.schedule_cleanup_recovery_targets(vec![target]);

        drop(incumbent);
        let rediscovered = service.recover_requested_cleanup_targets();
        assert_eq!(rediscovered.len(), 1);
        assert_eq!(rediscovered[0].sandbox_id, sandbox_id);
    }

    #[test]
    fn explicit_teardown_supersedes_an_armed_activation_episode() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Stopped);
        crate::db::begin_activation(
            &conn,
            &sandbox_id,
            SandboxState::Stopped,
            crate::db::ActivationKind::ColdStart,
            None,
            None,
        )
        .unwrap()
        .unwrap();

        crate::db::mark_stopping_superseding_activation(&conn, &sandbox_id).unwrap();

        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Stopping);
        assert!(row.cleanup_completion.is_none());
        assert!(
            crate::db::cleanup_episode(&conn, &sandbox_id)
                .unwrap()
                .is_none()
        );
        assert!(
            crate::db::mark_cleanup_failed(
                &conn,
                &sandbox_id,
                true,
                CleanupCompletion::MarkStopped,
                "kill cleanup timed out",
            )
            .unwrap()
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupFailed);
        assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
        assert!(
            crate::db::cleanup_episode(&conn, &sandbox_id)
                .unwrap()
                .is_none()
        );

        let requested_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &requested_id, SandboxState::Created);
        let requested_episode = crate::db::begin_activation(
            &conn,
            &requested_id,
            SandboxState::Created,
            crate::db::ActivationKind::ColdStart,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        crate::db::request_cleanup_episode(
            &conn,
            &requested_id,
            requested_episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "interrupted cold activation",
        )
        .unwrap();

        assert!(
            !crate::db::set_cleanup_intent(
                &conn,
                &requested_id,
                true,
                CleanupCompletion::MarkStopped,
            )
            .unwrap()
        );
        assert_eq!(
            crate::db::get_sandbox(&conn, &requested_id)
                .unwrap()
                .unwrap()
                .cleanup_completion,
            Some(CleanupCompletion::MarkCreated),
            "generic cleanup intent must not overwrite an owned episode"
        );

        crate::db::mark_stopping_superseding_activation(&conn, &requested_id).unwrap();
        let requested_row = crate::db::get_sandbox(&conn, &requested_id)
            .unwrap()
            .unwrap();
        assert_eq!(requested_row.state, SandboxState::Stopping);
        assert!(requested_row.cleanup_completion.is_none());
        assert!(
            crate::db::cleanup_episode(&conn, &requested_id)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn cleanup_persistence_failure_keeps_activation_capacity_owned_until_retry_succeeds() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Created);
        let episode = crate::db::begin_activation(
            &conn,
            &sandbox_id,
            SandboxState::Created,
            crate::db::ActivationKind::ColdStart,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        let mut service = SandboxManagerService::for_test(conn);
        service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
            activation_admission_config(1, 1_000.0, 1.0),
        ));
        let uuid8 = service.lookup_sandbox(&sandbox_id).unwrap().uuid8;
        let mut cleanup_guard = service
            .activation_cleanup_guard(
                &sandbox_id,
                &uuid8,
                ActivationCleanupSpec {
                    cleanup_id: episode.id,
                    completion: CleanupCompletion::MarkCreated,
                    context: "cleanup after cancelled cold activation",
                    interrupted_reason_code: SandboxQuarantineReasonCode::ColdActivationInterrupted,
                    interrupted_reason: "cold activation did not complete",
                },
            )
            .unwrap();
        cleanup_guard.admit(
            service
                .activation_admission_limiter
                .acquire(ActivationAdmissionKind::Cold, &sandbox_id)
                .await
                .unwrap(),
        );
        service
            .db
            .lock()
            .unwrap()
            .execute("ALTER TABLE sandboxes RENAME TO sandboxes_unavailable", [])
            .unwrap();

        drop(cleanup_guard);

        assert_eq!(
            service.activation_admission_limiter.in_flight(),
            1,
            "cleanup ownership must retain admission capacity while persistence is unavailable"
        );

        {
            let db = service.db.lock().unwrap();
            db.execute("ALTER TABLE sandboxes_unavailable RENAME TO sandboxes", [])
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while service.activation_admission_limiter.in_flight() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup persistence retry should release admission capacity");
        let pending = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(pending.state, SandboxState::CleanupRequired);
    }

    pub(in crate::service) fn register_full_suspend_artifact(
        conn: &crate::db::Connection,
        sandbox_id: &str,
    ) {
        // `adopt_suspended_local_artifact` registers-and-pins exactly the
        // shape `sandbox_has_registered_full_suspend_artifact` checks, with no
        // hidden state flip. Its CAS requires a `suspended` row with no pinned
        // artifact, so callers insert as Suspended and move the row to the
        // state under test afterwards.
        let adopted = crate::db::adopt_suspended_local_artifact(
            conn,
            sandbox_id,
            LocalArtifactId::new(),
            &format!("/space/cache/{sandbox_id}/snapshot"),
        )
        .unwrap();
        assert!(
            adopted,
            "test sandbox must be suspended with no pinned artifact before adoption"
        );
    }

    #[tokio::test]
    async fn checkpoint_retry_leaves_live_restore_staging_untouched() {
        // A live cross-node restore holds the per-sandbox staging lock while
        // its `Restoring` row still has no pinned artifact and no runtime —
        // locally indistinguishable from an interrupted restore. A concurrent
        // suspend-checkpoint start must not discard the row (or anything else)
        // from under the in-flight attempt; it backs off with a retryable
        // status and leaves the row exactly as it found it.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        mark_test_remote_restore_artifact_staging(&conn, &sandbox_id);
        let mut service = service_with_runtime_probe(conn, false);
        service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
            activation_admission_config(1, 0.001, 1.0),
        ));
        let held = service
            .activation_admission_limiter
            .try_acquire(ActivationAdmissionKind::Restore)
            .expect("test setup should fill the only process launch slot");
        assert_eq!(service.activation_admission_limiter.in_flight(), 1);

        let staging_lock = service.auto_lifecycle.staging_lock(&sandbox_id);
        let _staging_guard = staging_lock.lock().await;

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            ),
        )
        .await
        .expect("restore staging must not wait behind process launch admission")
        .expect_err("held staging lock must defer the retry, not discard the row");
        assert_eq!(
            error.code(),
            tonic::Code::Unavailable,
            "staging contention must surface a retryable status: {}",
            error.message()
        );
        assert!(
            error.message().contains("restore staging is in progress"),
            "staging contention must be identified without touching the live owner: {}",
            error.message()
        );

        {
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id)
                .unwrap()
                .expect("live restore's row must survive the concurrent retry");
            assert_eq!(row.state, SandboxState::Restoring);
            assert_eq!(row.cleanup_completion, None);
        }
        assert_eq!(
            service.activation_admission_limiter.in_flight(),
            1,
            "restore staging must not acquire another process launch permit"
        );
        drop(held);
        assert_eq!(service.activation_admission_limiter.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_retry_keeps_mark_suspended_for_restoring_row_with_registered_artifact() {
        // A `Restoring` row that still holds a registered full suspend
        // artifact is a genuine in-place restore retry: retiring the ambiguous
        // runtime must converge back to `Suspended` so the artifact can be
        // consumed, exactly as before.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        register_full_suspend_artifact(&conn, &sandbox_id);
        crate::db::test_fixtures::state_write_fixtures::apply_edge(
            &conn,
            &sandbox_id,
            &crate::db::edges::RESTORE_BEGIN,
        );
        let service = service_with_runtime_probe(conn, false);

        let _ = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await;

        {
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id)
                .unwrap()
                .expect("sandbox row");
            assert_eq!(
                row.cleanup_completion,
                Some(crate::db::CleanupCompletion::MarkSuspended)
            );
            assert_eq!(row.cleanup_keep_filesystem, Some(true));
        }

        complete_persisted_cleanup(&service, &sandbox_id);

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        assert!(row.current_local_artifact_id.is_some());
    }

    #[tokio::test]
    async fn checkpoint_start_leaves_failed_rows_untouched() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Failed);
        let service = service_with_runtime_probe(conn, false);
        for _ in 0..2 {
            let error = service
                .start_sandbox(
                    checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                    false,
                )
                .await
                .expect_err("Failed rows require explicit deletion");
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            let row = service.lookup_sandbox(&sandbox_id).unwrap();
            assert_eq!(row.state, SandboxState::Failed);
            assert!(row.cleanup_completion.is_none());
        }
    }

    #[tokio::test]
    async fn checkpoint_start_keeps_refusing_rows_with_pending_delete_cleanup() {
        // A pending `DeleteRow` cleanup intent belongs to another producer —
        // user delete, eviction, inventory reconcile, or startup recovery. A
        // restore must not race the delete back to life or mislabel its
        // completion; it keeps the terminal fail-closed refusal.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        crate::db::mark_cleanup_required(
            &conn,
            &sandbox_id,
            false,
            crate::db::CleanupCompletion::DeleteRow,
            "user requested delete",
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        let error = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await
            .expect_err("a row mid-delete must keep the fail-closed refusal");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::DeleteRow)
        );
    }

    #[tokio::test]
    async fn checkpoint_start_keeps_refusing_failed_row_with_registered_artifact() {
        // A `failed` row that still pins a registered full suspend artifact
        // may hold the only copy of the sandbox's state; keep the fail-closed
        // refusal so nothing is discarded automatically.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        register_full_suspend_artifact(&conn, &sandbox_id);
        conn.execute(
            "UPDATE sandboxes SET state = 'failed', failure_reason = 'runtime died' WHERE id = ?1",
            [&sandbox_id],
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        let error = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await
            .expect_err("failed row with a registered artifact must fail closed");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Failed);
        assert!(row.current_local_artifact_id.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_retry_keeps_retirement_for_dangling_pin() {
        // The Restoring-row discard is gated on the absence of any artifact
        // pin, registered or dangling: a pin claims bytes whose bookkeeping
        // this path cannot account for, so the row keeps the pre-existing
        // mark-suspended retirement instead of being discarded.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        conn.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = 'full',
                 current_local_artifact_origin = 'suspend'
             WHERE id = ?2",
            crate::db::fixture::params![LocalArtifactId::new().to_string(), &sandbox_id],
        )
        .unwrap();
        // The test service's node-manager refuses every dial, so the
        // retirement's cleanup cannot start and the row keeps the intent it
        // persisted.
        let service = service_with_runtime_probe(conn, false);

        let _ = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await;

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            row.state,
            SandboxState::CleanupRequired,
            "a pinned Restoring row must be retired, not discarded"
        );
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkSuspended)
        );
    }

    #[tokio::test]
    async fn checkpoint_retry_preserves_foreign_cleanup_intent() {
        // A Restoring row carrying persisted cleanup intent belongs to
        // another producer's in-flight cleanup (a pending user delete). The
        // checkpoint retry must neither discard the row nor let the
        // retirement's mark_cleanup_required overwrite the `delete_row`
        // intent with `mark_suspended` — that would silently convert the
        // user's delete into a retirement. The row stays exactly as found
        // and the pending cleanup keeps its intent.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            false,
            crate::db::CleanupCompletion::DeleteRow,
        )
        .unwrap();
        let service = service_with_runtime_probe(conn, false);

        let _ = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await;

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::DeleteRow),
            "the pending delete intent must survive the checkpoint retry"
        );
        assert_eq!(row.cleanup_keep_filesystem, Some(false));
        assert_eq!(
            row.state,
            SandboxState::Restoring,
            "the retry must leave the row untouched for the pending cleanup"
        );
    }

    // Paused, like the sibling checkpoint-retry cases. The `Unavailable` this
    // asserts comes from restore staging under the fixed cache root: as an
    // ordinary user, as CI runs it, the staging directory's `create_dir_all`
    // fails; as root, the volume-manager connect that follows exhausts its
    // `tokio::time` retry budget against an absent socket, which pausing
    // makes virtual.
    #[tokio::test(start_paused = true)]
    async fn checkpoint_retry_preserves_row_for_resumable_restore() {
        // Attempt 1 of a cross-node restore can be cancelled mid-stage (the
        // caller's runtime request deadline drops the RPC future), leaving a
        // `Restoring` row with no artifact pin and no runtime. The retry keeps
        // that ownership row and re-enters durable-backed staging directly;
        // dropping it would let the periodic inventory reconcile relabel the
        // surviving cache directory as an orphan before another retry.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        mark_test_remote_restore_artifact_staging(&conn, &sandbox_id);
        let service = service_with_runtime_probe(conn, false);

        let error = service
            .start_sandbox(
                checkpoint_start_request(
                    sandbox_id.clone(),
                    "suspend",
                    Some(direct_route_identity_proto(1)),
                ),
                false,
            )
            .await
            .expect_err("the synthetic test has no reachable durable snapshot authority");
        assert_eq!(error.code(), tonic::Code::Unavailable);

        {
            let db = service.db.lock().expect("mutex poisoned");
            let row = crate::db::get_sandbox(&db, &sandbox_id)
                .unwrap()
                .expect("retryable restoring ownership must survive a failed reattach");
            assert_eq!(row.state, SandboxState::Restoring);
            let operation = row
                .restore_operation
                .as_ref()
                .expect("remote restore ownership must remain persisted");
            assert_eq!(
                operation.origin(),
                crate::restore_operation::RestoreOrigin::Remote
            );
            assert_eq!(
                operation.phase(),
                crate::restore_operation::RestorePhase::ArtifactStaging
            );
            assert_eq!(
                operation
                    .remote_source()
                    .expect("remote restore source")
                    .as_wire(),
                format!("s3://snapshots/suspend/{sandbox_id}?region=us-east-1")
            );
            assert!(row.cleanup_completion.is_none());
            assert!(row.cleanup_keep_filesystem.is_none());
            assert!(
                crate::db::restore_transfer_scope(&db, &sandbox_id)
                    .unwrap()
                    .is_some(),
                "retry must remain owned by a cancellable transfer scope"
            );
        }
    }
    #[tokio::test]
    async fn checkpoint_start_keeps_refusing_failed_row_with_live_runtime() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Failed);
        let service = service_with_runtime_probe(conn, true);

        let error = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await
            .expect_err("failed row with a live runtime must fail closed");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Failed);
    }

    #[test]
    fn checkpoint_retry_resumes_uncommitted_restore_and_cleans_partial_runtime() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        let incomplete = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");

        assert!(matches!(
            checkpoint_restore_retry_action(&incomplete, SandboxState::Started, false),
            CheckpointRestoreRetryAction::CleanupRestoring
        ));
        mark_test_remote_restore_artifact_staging(&conn, &sandbox_id);
        let remote_restore = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");
        assert!(matches!(
            checkpoint_restore_retry_action(&remote_restore, SandboxState::Started, false),
            CheckpointRestoreRetryAction::ResumeRestoring
        ));
        assert!(matches!(
            checkpoint_restore_retry_action(&remote_restore, SandboxState::Started, true),
            CheckpointRestoreRetryAction::CleanupRestoring
        ));

        let complete_but_dead_id = Uuid::now_v7().to_string();
        insert_started_test_sandbox(&conn, &complete_but_dead_id);
        restage_started_as_restoring(&conn, &complete_but_dead_id);
        let complete_but_dead = crate::db::get_sandbox(&conn, &complete_but_dead_id)
            .unwrap()
            .expect("sandbox row");
        assert!(matches!(
            checkpoint_restore_retry_action(&complete_but_dead, SandboxState::Started, false),
            CheckpointRestoreRetryAction::CleanupRestoring
        ));

        for target_state in [SandboxState::Started, SandboxState::Paused] {
            assert!(matches!(
                checkpoint_restore_retry_action(&complete_but_dead, target_state, true),
                CheckpointRestoreRetryAction::CleanupRestoring
            ));
        }
    }

    #[test]
    fn checkpoint_retry_does_not_discard_staging_failed_restore() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        mark_test_remote_restore_artifact_staging(&conn, &sandbox_id);
        let staging_failure = "cross-node restore artifact staging failed: test";
        conn.execute(
            "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
            crate::db::fixture::params![staging_failure, sandbox_id],
        )
        .unwrap();
        let restoring = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");

        let action = checkpoint_restore_retry_action(&restoring, SandboxState::Started, false);

        assert!(matches!(
            action,
            CheckpointRestoreRetryAction::ResumeRestoring
        ));

        conn.execute(
            "UPDATE sandboxes
             SET cleanup_completion = 'delete_row', cleanup_keep_filesystem = 0
             WHERE id = ?1",
            crate::db::fixture::params![sandbox_id],
        )
        .unwrap();
        let pending_delete = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");

        assert!(matches!(
            checkpoint_restore_retry_action(&pending_delete, SandboxState::Started, false),
            CheckpointRestoreRetryAction::CleanupRestoring
        ));
    }

    #[tokio::test]
    async fn checkpoint_start_rejects_started_row_without_live_runtime() {
        // A stale DB-only `Started` row (no live Firecracker process) must still
        // fail closed so the caller does not reattach to a dead runtime.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let service = service_with_runtime_probe(conn, false);

        let error = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await
            .expect_err("stale started row must still fail closed");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("expected Suspended or NotFound"),
            "unexpected checkpoint start error: {}",
            error.message()
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Started);
    }

    #[tokio::test]
    async fn checkpoint_start_rejects_non_suspend_checkpoint_on_started_runtime() {
        // Reattach is scoped to the suspend checkpoint; any other checkpoint
        // name against a `Started` row keeps the existing fail-closed behavior.
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_started_test_sandbox(&conn, &sandbox_id);
        let service = service_with_runtime_probe(conn, true);

        let error = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "manual-checkpoint", None),
                false,
            )
            .await
            .expect_err("non-suspend checkpoint starts must still fail closed");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("manual-checkpoint"),
            "unexpected checkpoint start error: {}",
            error.message()
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Started);
    }

    #[tokio::test(start_paused = true)]
    async fn restore_routes_provisional_suspended_retirement_through_cleanup_before_consumption() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();
        // The test service's node-manager refuses every dial, so the
        // retirement's cleanup cannot start and the row keeps the intent it
        // persisted.
        let service = SandboxManagerService::for_test(conn);

        let _ = service
            .start_sandbox(
                checkpoint_start_request(sandbox_id.clone(), "suspend", None),
                false,
            )
            .await;

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            row.state,
            SandboxState::CleanupRequired,
            "restore must route pending suspend retirement through cleanup before reading artifacts"
        );
        assert_eq!(
            row.cleanup_completion,
            Some(crate::db::CleanupCompletion::MarkSuspended)
        );
    }

    #[tokio::test]
    async fn checkpoint_retry_completes_owned_restore_cleanup_episode() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        register_full_suspend_artifact(&conn, &sandbox_id);
        let episode = crate::db::begin_activation(
            &conn,
            &sandbox_id,
            SandboxState::Suspended,
            crate::db::ActivationKind::Restore,
            Some(ExecutionGeneration::new(1).unwrap()),
            Some(SandboxActivationAttemptId::new()),
        )
        .unwrap()
        .unwrap();
        crate::db::request_cleanup_episode(
            &conn,
            &sandbox_id,
            episode.id,
            SandboxQuarantineReasonCode::RestoreActivationInterrupted,
            "restore activation was interrupted",
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        add_test_monitoring(&service, &sandbox_id).await;
        service.auto_lifecycle.record_wake_completed(&sandbox_id);
        let pending = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(
            service
                .checkpoint_restore_cleanup_episode_id(&pending)
                .unwrap(),
            Some(episode.id)
        );
        let uuid8 = pending.uuid8;

        service
            .run_cleanup_to_completion_with_node_dispatch_and_admission(
                SynchronousCleanupRequest {
                    sandbox_id: &sandbox_id,
                    uuid8: uuid8.as_str(),
                    keep_filesystem: true,
                    context: "checkpoint retry cleanup",
                    completion: CleanupCompletion::MarkSuspended,
                    published_artifact: None,
                    immediate_kill: true,
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                },
                None,
                Some(episode.id),
                || async { Ok(()) },
                |()| async { Ok(()) },
            )
            .await
            .unwrap();

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        {
            let db = service.db.lock().unwrap();
            assert!(
                crate::db::cleanup_episode(&db, &sandbox_id)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(service.watchdog.get_health(&sandbox_id).await.is_none());
        assert!(!service.snapshot_scheduler.is_active(&sandbox_id).await);
        assert!(
            service
                .auto_lifecycle
                .since_last_wake_completed(&sandbox_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn ingress_admission_does_not_persist_after_restart_drain() {
        for drain_while_waiting in [false, true] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new();
            let key = id.to_string();
            let policy = access_idle_policy(300);
            insert_test_sandbox_with_policy(&conn, &key, SandboxState::Started, &policy);
            let identity = route_identity(id, 1, SandboxActivationAttemptId::new());
            crate::db::update_space_api_route_identity(
                &conn,
                id,
                &identity.clone().into(),
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
            let before = initial_runtime_state(&policy, now_secs().saturating_sub(1000));
            crate::db::update_auto_lifecycle_state(&conn, &key, &before).unwrap();
            let service = SandboxManagerService::for_test(conn);
            install_authoritative_route_epoch(
                &mut service.route_stream_epochs.lock().unwrap(),
                id,
                RouteStreamEpochIdentity(identity),
            );
            let lock = service.auto_lifecycle.wake_lock(&key);
            let blocker = lock.lock().await;
            let mut admission =
                Box::pin(service.admit_ingress(&key, Duration::from_secs(1), |_| Ok(())));
            if drain_while_waiting {
                assert!(futures_util::poll!(&mut admission).is_pending());
            }
            service.begin_draining_for_restart();
            drop(blocker);
            let stream = admission
                .await
                .expect("resident ingress remains read-only during drain");
            assert_eq!(
                service.lookup_sandbox(&key).unwrap().auto_lifecycle_state,
                before
            );
            assert_eq!(service.active_state_transition_count(), 0);
            assert!(!stream.fence.child_token().is_cancelled());
            drop(stream);
        }
    }

    #[tokio::test]
    async fn ingress_wake_retains_mutation_ownership_after_disconnect() {
        use prost::Message;
        use tonic::codec::Codec;
        if !crate::service::tests::enter_route_test_process(&[]) {
            return;
        }
        struct StartBody(Option<bytes::Bytes>);
        impl http_body::Body for StartBody {
            type Data = bytes::Bytes;
            type Error = std::convert::Infallible;
            fn poll_frame(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>>
            {
                std::task::Poll::Ready(self.0.take().map(|data| Ok(http_body::Frame::data(data))))
            }
        }
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        let key = id.to_string();
        let port = pplx_space_common_core::types::GuestPort::try_from_u16(8080).unwrap();
        seed_due_access_idle_sandbox(&conn, &key, 120, 300);
        let (service, token) = route_authorized_service_for_port(conn, id, Some(port));
        let before = access_idle_last_touched(&service, &key);
        let frame = ServicePortIngressFrame {
            message: Some(service_port_ingress_frame::Message::Start(
                pplx_space_net_gateway_client::proto::ServicePortIngressStart {
                    sandbox_id: key.clone(),
                    port: 8080,
                },
            )),
        }
        .encode_to_vec();
        let mut wire = vec![0];
        wire.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        wire.extend_from_slice(&frame);
        let mut codec =
            tonic_prost::ProstCodec::<ServicePortIngressFrame, ServicePortIngressFrame>::default();
        let inbound = tonic::Streaming::new_request(
            codec.decoder(),
            StartBody(Some(wire.into())),
            None,
            None,
        );
        let mut request = internal_request(inbound);
        insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
        let lock = service.auto_lifecycle.wake_lock(&key);
        let blocker = lock.lock().await;
        let mut caller = Box::pin(service.service_port_ingress(request));
        assert!(futures_util::poll!(&mut caller).is_pending());
        assert_eq!(service.active_state_transition_count(), 1);
        drop(caller);
        service.begin_draining_for_restart();
        assert_eq!(service.active_state_transition_count(), 1);
        drop(blocker);
        tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_for_state_transitions_drained(),
        )
        .await
        .unwrap();
        assert_ne!(access_idle_last_touched(&service, &key), before);
        assert_eq!(
            service.lookup_sandbox(&key).unwrap().state,
            SandboxState::Started
        );
    }

    #[tokio::test]
    async fn ingress_protection_defers_due_work_and_resumes_idle_at_release() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        let policy = access_idle_policy(300);
        insert_test_sandbox_with_policy(&conn, &id.to_string(), SandboxState::Started, &policy);
        let identity = route_identity(id, 1, SandboxActivationAttemptId::new());
        crate::db::update_space_api_route_identity(
            &conn,
            id,
            &identity.clone().into(),
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        let old = initial_runtime_state(&policy, now_secs().saturating_sub(1000));
        crate::db::update_auto_lifecycle_state(&conn, &id.to_string(), &old).unwrap();
        let service = SandboxManagerService::for_test(conn);
        let protection = {
            let mut epochs = service.route_stream_epochs.lock().unwrap();
            install_authoritative_route_epoch(&mut epochs, id, RouteStreamEpochIdentity(identity));
            epochs[&id].ingress.clone()
        };
        let hold = protection.acquire();
        service
            .run_due_auto_lifecycle(&id.to_string())
            .await
            .unwrap();
        assert_eq!(
            service
                .lookup_sandbox(&id.to_string())
                .unwrap()
                .auto_lifecycle_state,
            old
        );
        drop(hold);
        let released = protection.observation().1.unwrap();
        service.db.lock().unwrap().execute_batch(
            "CREATE TEMP TRIGGER reject_release BEFORE UPDATE ON sandboxes BEGIN SELECT RAISE(FAIL, 'release persistence unavailable'); END;"
        ).unwrap();
        service
            .run_due_auto_lifecycle(&id.to_string())
            .await
            .unwrap_err();
        assert_eq!(protection.observation().1, Some(released));
        assert_eq!(
            service.lookup_sandbox(&id.to_string()).unwrap().state,
            SandboxState::Started
        );
        service
            .db
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_release")
            .unwrap();
        service
            .run_due_auto_lifecycle(&id.to_string())
            .await
            .unwrap();
        let row = service.lookup_sandbox(&id.to_string()).unwrap();
        assert_eq!(row.state, SandboxState::Started);
        assert_eq!(row.next_auto_event_at, Some(released + 300));
        assert_eq!(protection.observation().1, None);
    }

    #[tokio::test(start_paused = true)]
    async fn ingress_admission_arbitrates_with_the_running_scheduler() {
        for admission_first in [false, true] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new();
            let key = id.to_string();
            let policy = access_idle_policy(300);
            insert_test_sandbox_with_policy(&conn, &key, SandboxState::Started, &policy);
            let identity = route_identity(id, 1, SandboxActivationAttemptId::new());
            crate::db::update_space_api_route_identity(
                &conn,
                id,
                &identity.clone().into(),
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
            let due = initial_runtime_state(&policy, now_secs().saturating_sub(1000));
            crate::db::update_auto_lifecycle_state(&conn, &key, &due).unwrap();
            let service = SandboxManagerService::for_test(conn);
            install_authoritative_route_epoch(
                &mut service.route_stream_epochs.lock().unwrap(),
                id,
                RouteStreamEpochIdentity(identity),
            );
            let wake = service.auto_lifecycle.wake_lock(&key);
            let blocker = wake.lock().await;
            let artifact = service.auto_lifecycle.artifact_lock(&key);
            let artifact_blocker = artifact.lock().await;
            let admission = service.admit_ingress(&key, Duration::from_secs(1), |_| Ok(()));
            let scheduler = service.run_due_auto_lifecycle(&key);
            tokio::pin!(admission, scheduler);
            // Queue both real entry points behind the lifecycle lock in FIFO order.
            if admission_first {
                assert!(futures_util::poll!(&mut admission).is_pending());
                assert!(futures_util::poll!(&mut scheduler).is_pending());
            } else {
                assert!(futures_util::poll!(&mut scheduler).is_pending());
                assert!(futures_util::poll!(&mut admission).is_pending());
            }
            drop(blocker);
            if admission_first {
                let owner = admission.await.unwrap();
                // Keep the policy due even after admission's normal access touch:
                // only live protection may prevent this suspension.
                crate::db::update_auto_lifecycle_state(&service.db_conn().unwrap(), &key, &due)
                    .unwrap();
                scheduler.await.unwrap();
                assert_eq!(
                    service.lookup_sandbox(&key).unwrap().state,
                    SandboxState::Started
                );
                assert_eq!(
                    service.lookup_sandbox(&key).unwrap().auto_lifecycle_state,
                    due
                );
                drop(owner);
            } else {
                // The scheduler retains the wake lock while awaiting its artifact lock.
                assert!(futures_util::poll!(&mut scheduler).is_pending());
                let error = admission.await.err().unwrap();
                assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
                let row = service.lookup_sandbox(&key).unwrap();
                assert_eq!(
                    service
                        .ingress_protection(&row)
                        .unwrap()
                        .unwrap()
                        .observation()
                        .0,
                    0
                );
            }
            drop(artifact_blocker);
        }
    }

    #[tokio::test]
    async fn ingress_release_from_replaced_execution_cannot_release_current_stream() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        let key = id.to_string();
        let policy = access_idle_policy(300);
        insert_test_sandbox_with_policy(&conn, &key, SandboxState::Started, &policy);
        let service = SandboxManagerService::for_test(conn);
        let mut owners = Vec::new();
        for generation in [1, 2] {
            let identity = route_identity(id, generation, SandboxActivationAttemptId::new());
            crate::db::update_space_api_route_identity(
                &service.db_conn().unwrap(),
                id,
                &identity.clone().into(),
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
            install_authoritative_route_epoch(
                &mut service.route_stream_epochs.lock().unwrap(),
                id,
                RouteStreamEpochIdentity(identity),
            );
            owners.push(
                service
                    .admit_ingress(&key, Duration::from_secs(1), |_| Ok(()))
                    .await
                    .unwrap(),
            );
        }
        let current = service
            .ingress_protection(&service.lookup_sandbox(&key).unwrap())
            .unwrap()
            .unwrap();
        drop(owners.remove(0));
        assert_eq!(
            (current.observation().0, current.observation().1),
            (1, None)
        );
        let due = initial_runtime_state(&policy, now_secs().saturating_sub(1000));
        crate::db::update_auto_lifecycle_state(&service.db_conn().unwrap(), &key, &due).unwrap();
        service.run_due_auto_lifecycle(&key).await.unwrap();
        assert_eq!(
            service.lookup_sandbox(&key).unwrap().auto_lifecycle_state,
            due
        );
        drop(owners);
        let released = current.observation().1.unwrap();
        service.run_due_auto_lifecycle(&key).await.unwrap();
        assert_eq!(
            service.lookup_sandbox(&key).unwrap().next_auto_event_at,
            Some(released + 300)
        );
    }

    #[tokio::test]
    async fn access_deadline_reset_retry_and_execution_fence() {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        let policy = access_idle_policy(300);
        insert_test_sandbox_with_policy(&conn, &id.to_string(), SandboxState::Started, &policy);
        let identity = route_identity(id, 1, SandboxActivationAttemptId::new());
        crate::db::update_space_api_route_identity(
            &conn,
            id,
            &identity.into(),
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        let now = now_secs();
        let mut request = touch_auto_lifecycle_access_request(&id.to_string(), now);
        let extend =
            crate::proto::access_idle_update::Operation::ExtendUntil(prost_types::Timestamp {
                seconds: (now + 900) as i64,
                nanos: 0,
            });
        request.access_idle_update = Some(crate::proto::AccessIdleUpdate {
            execution_generation: 1,
            expires_at: Some(prost_types::Timestamp {
                seconds: (now + 60) as i64,
                nanos: 0,
            }),
            operation: Some(extend.clone()),
        });
        let response = service
            .touch_auto_lifecycle(internal_request(request.clone()))
            .await
            .unwrap();
        assert_eq!(
            response
                .metadata()
                .get(pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA)
                .unwrap(),
            "1"
        );
        let replay = service
            .touch_auto_lifecycle(internal_request(request.clone()))
            .await
            .unwrap();
        assert_eq!(
            replay
                .metadata()
                .get(pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA)
                .unwrap(),
            "1"
        );
        let mut stripped = request.clone();
        stripped.access_idle_update = None;
        let ordinary = service
            .touch_auto_lifecycle(internal_request(stripped))
            .await
            .unwrap();
        assert!(
            !ordinary
                .metadata()
                .contains_key(pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA),
            "old-hop decoded requests cannot acknowledge an extension"
        );
        assert_eq!(
            service
                .lookup_sandbox(&id.to_string())
                .unwrap()
                .auto_lifecycle_state
                .access_extended_until,
            Some(now + 900)
        );
        request.access_idle_update.as_mut().unwrap().operation = Some(
            crate::proto::access_idle_update::Operation::ResetPolicy(policy_to_proto(&policy)),
        );
        service
            .touch_auto_lifecycle(internal_request(request.clone()))
            .await
            .unwrap();
        assert_eq!(
            service
                .lookup_sandbox(&id.to_string())
                .unwrap()
                .auto_lifecycle_state
                .access_extended_until,
            None
        );
        request.access_idle_update.as_mut().unwrap().operation = Some(extend);
        service
            .touch_auto_lifecycle(internal_request(request.clone()))
            .await
            .unwrap();
        let expected = service
            .lookup_sandbox(&id.to_string())
            .unwrap()
            .auto_lifecycle_state;
        request
            .access_idle_update
            .as_mut()
            .unwrap()
            .execution_generation = 2;
        assert_eq!(
            service
                .touch_auto_lifecycle(internal_request(request.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        request
            .access_idle_update
            .as_mut()
            .unwrap()
            .execution_generation = 1;
        let wake_lock = service.auto_lifecycle.wake_lock(&id.to_string());
        let guard = wake_lock.lock().await;
        let mut expired_request = internal_request(request.clone());
        expired_request.set_timeout(std::time::Duration::from_millis(1));
        let operation = service.touch_auto_lifecycle(expired_request);
        tokio::pin!(operation);
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut operation)
            .await
            .unwrap_err();
        drop(guard);
        assert_eq!(
            operation.await.unwrap_err().code(),
            tonic::Code::DeadlineExceeded
        );
        request
            .access_idle_update
            .as_mut()
            .unwrap()
            .expires_at
            .as_mut()
            .unwrap()
            .seconds = 0;
        assert_eq!(
            service
                .touch_auto_lifecycle(internal_request(request))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::DeadlineExceeded
        );
        assert_eq!(
            service
                .lookup_sandbox(&id.to_string())
                .unwrap()
                .auto_lifecycle_state,
            expected
        );
    }

    #[tokio::test]
    async fn identical_policy_replay_preserves_connect_floor() {
        let conn = crate::db::test_conn();
        let id = Uuid::now_v7().to_string();
        let policy = access_idle_policy(300);
        insert_test_sandbox_with_policy(&conn, &id, SandboxState::Started, &policy);
        let mut runtime = initial_runtime_state(&policy, now_secs());
        crate::auto_lifecycle::extend_access_deadline(&mut runtime, &policy, now_secs() + 900)
            .unwrap();
        crate::db::update_auto_lifecycle_state(&conn, &id, &runtime).unwrap();
        let service = SandboxManagerService::for_test(conn);
        service
            .set_auto_lifecycle_policy_exact(
                &id,
                policy.clone(),
                false,
                crate::db::PolicyAuthority::Cluster,
            )
            .await
            .unwrap();
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().auto_lifecycle_state,
            runtime
        );
        service
            .set_auto_lifecycle_policy_exact(
                &id,
                access_idle_policy(600),
                false,
                crate::db::PolicyAuthority::Cluster,
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .lookup_sandbox(&id)
                .unwrap()
                .auto_lifecycle_state
                .access_extended_until,
            None
        );
    }

    #[tokio::test]
    async fn touch_auto_lifecycle_rearms_started_access_idle_with_observed_at() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(30);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let service = SandboxManagerService::for_test(conn);
        let observed_at = now_secs() + 600;

        let response = service
            .touch_auto_lifecycle(internal_request(touch_auto_lifecycle_access_request(
                &sandbox_id,
                observed_at,
            )))
            .await
            .expect("touch accepted")
            .into_inner();

        assert_eq!(
            response.result,
            crate::proto::AutoLifecycleTouchResult::Applied as i32
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let condition = row
            .auto_lifecycle_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .expect("access idle condition");
        assert!(condition.armed);
        assert_eq!(condition.epoch_at, Some(observed_at));
        assert_eq!(condition.last_touched_at, Some(observed_at));
        assert_eq!(condition.last_fired_stage_index, None);
        assert_eq!(
            row.auto_lifecycle_state.next_auto_event_at,
            Some(observed_at + 30)
        );
        assert_eq!(row.next_auto_event_at, Some(observed_at + 30));
    }

    #[tokio::test]
    async fn touch_auto_lifecycle_ignores_older_observed_at() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(30);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let service = SandboxManagerService::for_test(conn);
        let observed_at = now_secs() + 600;

        service
            .touch_auto_lifecycle(internal_request(touch_auto_lifecycle_access_request(
                &sandbox_id,
                observed_at,
            )))
            .await
            .expect("initial touch accepted");
        let stale_response = service
            .touch_auto_lifecycle(internal_request(touch_auto_lifecycle_access_request(
                &sandbox_id,
                observed_at - 10,
            )))
            .await
            .expect("stale touch handled")
            .into_inner();

        assert_eq!(
            stale_response.result,
            crate::proto::AutoLifecycleTouchResult::AlreadyCurrent as i32
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let condition = row
            .auto_lifecycle_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .expect("access idle condition");
        assert_eq!(condition.epoch_at, Some(observed_at));
        assert_eq!(condition.last_touched_at, Some(observed_at));
        assert_eq!(
            row.auto_lifecycle_state.next_auto_event_at,
            Some(observed_at + 30)
        );
    }

    #[tokio::test]
    async fn touch_auto_lifecycle_initializes_empty_runtime_state() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(30);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        crate::db::update_auto_lifecycle_state(
            &conn,
            &sandbox_id,
            &AutoLifecycleRuntimeState::default(),
        )
        .unwrap();
        let service = SandboxManagerService::for_test(conn);
        let observed_at = now_secs() + 600;

        let response = service
            .touch_auto_lifecycle(internal_request(touch_auto_lifecycle_access_request(
                &sandbox_id,
                observed_at,
            )))
            .await
            .expect("touch accepted")
            .into_inner();

        assert_eq!(
            response.result,
            crate::proto::AutoLifecycleTouchResult::Applied as i32
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let condition = row
            .auto_lifecycle_state
            .conditions
            .get(&AutoLifecycleConditionSetId::AccessIdle)
            .expect("access idle condition");
        assert_eq!(condition.epoch_at, Some(observed_at));
        assert_eq!(row.next_auto_event_at, Some(observed_at + 30));
    }

    #[tokio::test]
    async fn touch_auto_lifecycle_does_not_mutate_non_started_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(30);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Paused, &policy);
        let before = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row")
            .auto_lifecycle_state;
        let service = SandboxManagerService::for_test(conn);

        let response = service
            .touch_auto_lifecycle(internal_request(touch_auto_lifecycle_access_request(
                &sandbox_id,
                now_secs() + 600,
            )))
            .await
            .expect("touch handled")
            .into_inner();

        assert_eq!(
            response.result,
            crate::proto::AutoLifecycleTouchResult::NotStarted as i32
        );
        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        assert_eq!(row.auto_lifecycle_state, before);
    }

    #[tokio::test]
    async fn touch_auto_lifecycle_reports_disabled_policy() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox_with_policy(
            &conn,
            &sandbox_id,
            SandboxState::Started,
            &AutoLifecyclePolicy::disabled(),
        );
        let service = SandboxManagerService::for_test(conn);

        let response = service
            .touch_auto_lifecycle(internal_request(touch_auto_lifecycle_access_request(
                &sandbox_id,
                now_secs() + 600,
            )))
            .await
            .expect("touch handled")
            .into_inner();

        assert_eq!(
            response.result,
            crate::proto::AutoLifecycleTouchResult::Disabled as i32
        );
    }

    #[test]
    fn requested_create_reuse_ignores_runtime_updated_process_env() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(60);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Suspended, &policy);

        // Warm-pool adoption and the Update RPC rewrite the row's process env
        // after create; a later restore pod re-derives its CreateRequest env
        // from the control-plane spec plus the OCI process env, so the two
        // representations legitimately diverge for the same sandbox.
        let adopted_env =
            BTreeMap::from([("SANDBOX_TYPE".to_string(), "example-session".to_string())]);
        crate::db::update_process_env(&conn, &sandbox_id, &adopted_env)
            .expect("process env update should succeed");
        let existing = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");
        assert_eq!(existing.process_env, adopted_env);

        let user = SandboxOwner::parse("user").unwrap();
        let requested_policy = access_idle_policy(120);
        let requested = RequestedSandboxCreateSpec {
            template: "seed",
            template_hash: "hash",
            user: &user,
            runtime: "fc",
            rootfs_size_mb: None,
            pool_template: None,
            volume_encryption: false,
            auto_lifecycle: &requested_policy,
            environment: None,
        };

        assert!(
            requested_sandbox_create_mismatches(&existing, &requested).is_empty(),
            "runtime-updated process env must not block requested-id create adoption"
        );

        // Adoption keeps the row's env: it is the authoritative value for
        // processes launched after wake. The reuse path's only row write is
        // the auto-lifecycle refresh, which must not clobber it.
        update_existing_sandbox_create_auto_lifecycle(
            &conn,
            &sandbox_id,
            existing.state,
            &requested_policy,
        )
        .expect("policy update should succeed");
        let row = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");
        assert_eq!(row.process_env, adopted_env);
        assert_eq!(row.auto_lifecycle, requested_policy);
    }

    #[test]
    fn requested_create_reuse_ignores_runtime_updated_snapshots_enabled() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(60);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Suspended, &policy);

        // EnableSnapshots/DisableSnapshots and the snapshot scheduler's
        // auto-disable rewrite the row after create, while the Kubernetes
        // shim never sets CreateRequest.snapshots_enabled, so a
        // pod-recreate create always carries the proto default.
        crate::db::update_snapshots_enabled(&conn, &sandbox_id, true)
            .expect("snapshots_enabled update should succeed");
        let existing = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");
        assert!(existing.snapshots_enabled);

        let user = SandboxOwner::parse("user").unwrap();
        let requested = RequestedSandboxCreateSpec {
            template: "seed",
            template_hash: "hash",
            user: &user,
            runtime: "fc",
            rootfs_size_mb: None,
            pool_template: None,
            volume_encryption: false,
            auto_lifecycle: &policy,
            environment: None,
        };

        assert!(
            requested_sandbox_create_mismatches(&existing, &requested).is_empty(),
            "runtime-updated snapshots_enabled must not block requested-id create adoption"
        );
        let row = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");
        assert!(row.snapshots_enabled);
    }

    #[test]
    fn requested_create_reuse_still_rejects_identity_changes() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = access_idle_policy(60);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Suspended, &policy);
        let existing = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");

        // Diverge every identity field at once so the assertion enumerates
        // the closed set; deleting any comparison from
        // requested_sandbox_create_mismatches breaks this test.
        let user = SandboxOwner::parse("other-user").unwrap();
        let requested = RequestedSandboxCreateSpec {
            template: "other-template",
            template_hash: "other-hash",
            user: &user,
            runtime: "other-runtime",
            rootfs_size_mb: Some(4096),
            pool_template: Some("other-pool"),
            volume_encryption: true,
            auto_lifecycle: &policy,
            environment: None,
        };

        assert_eq!(
            requested_sandbox_create_mismatches(&existing, &requested),
            vec![
                "template",
                "template_hash",
                "user",
                "runtime",
                "rootfs_size_mb",
                "pool_template",
                "volume_encryption"
            ]
        );
    }

    #[test]
    fn requested_create_reuse_refreshes_auto_lifecycle_for_suspended_sandbox() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let old_policy = access_idle_policy(60);
        let requested_policy = access_idle_policy(120);
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Suspended, &old_policy);

        let existing = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("sandbox row");
        let user = SandboxOwner::parse("user").unwrap();
        let requested = RequestedSandboxCreateSpec {
            template: "seed",
            template_hash: "hash",
            user: &user,
            runtime: "fc",
            rootfs_size_mb: None,
            pool_template: None,
            volume_encryption: false,
            auto_lifecycle: &requested_policy,
            environment: None,
        };

        assert!(requested_sandbox_create_mismatches(&existing, &requested).is_empty());
        assert!(requested_sandbox_create_auto_lifecycle_changed(
            &existing, &requested
        ));

        update_existing_sandbox_create_auto_lifecycle(
            &conn,
            &sandbox_id,
            existing.state,
            &requested_policy,
        )
        .expect("policy update should succeed");

        let updated = crate::db::get_sandbox(&conn, &sandbox_id)
            .unwrap()
            .expect("updated sandbox row");
        assert_eq!(updated.auto_lifecycle, requested_policy);
        assert_eq!(
            updated.auto_lifecycle_state,
            AutoLifecycleRuntimeState::default()
        );
        assert_eq!(updated.next_auto_event_at, None);
    }

    #[tokio::test]
    async fn guest_active_for_persists_future_idle_epoch() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity,
                pplx_space_common_core::types::AutoLifecycleConditionSet::guest_activity(Some(
                    test_guest_activity_hold_cap(),
                )),
            )]),
            stages: vec![pplx_space_common_core::types::AutoLifecycleStage {
                condition_set:
                    pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity,
                at: 30,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        };
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let service = SandboxManagerService::for_test(conn);

        let before = now_secs();
        let outcome = service
            .request_guest_active_for(&sandbox_id, TEST_GUEST_ACTIVITY_HOLD_CAP * 2)
            .await
            .expect("first hold accepted");
        let HostControlActiveForOutcome::Accepted(accepted) = outcome else {
            panic!("active-for should be accepted");
        };
        assert_eq!(
            accepted.granted_for,
            Some(duration_to_proto(TEST_GUEST_ACTIVITY_HOLD_CAP))
        );

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let condition = row
            .auto_lifecycle_state
            .conditions
            .get(&pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity)
            .expect("guest activity condition");
        let epoch_at = condition.epoch_at.expect("future guest idle epoch");
        assert!(condition.armed);
        assert!(epoch_at >= before + TEST_GUEST_ACTIVITY_HOLD_CAP.as_secs());
        assert!(epoch_at <= now_secs() + TEST_GUEST_ACTIVITY_HOLD_CAP.as_secs());
        assert_eq!(row.auto_lifecycle_state.open_activity_leases, 0);
        assert_eq!(
            row.auto_lifecycle_state.next_auto_event_at,
            Some(epoch_at + 30)
        );
        assert_eq!(row.next_auto_event_at, Some(epoch_at + 30));
    }

    #[tokio::test]
    async fn guest_active_for_denies_missing_hold_cap() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity,
                pplx_space_common_core::types::AutoLifecycleConditionSet::guest_activity(None),
            )]),
            stages: vec![pplx_space_common_core::types::AutoLifecycleStage {
                condition_set:
                    pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity,
                at: 30,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        };
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let service = SandboxManagerService::for_test(conn);

        let outcome = service
            .request_guest_active_for(&sandbox_id, Duration::from_secs(10))
            .await
            .expect("request handled");
        let HostControlActiveForOutcome::Denied(denied) = outcome else {
            panic!("active-for without cap should be denied");
        };
        assert!(denied.reason.contains("cap"));
    }

    #[tokio::test]
    async fn guest_active_state_extends_idle_epoch_with_policy_cap() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: BTreeMap::from([(
                pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity,
                pplx_space_common_core::types::AutoLifecycleConditionSet::guest_activity(Some(
                    test_guest_activity_hold_cap(),
                )),
            )]),
            stages: vec![pplx_space_common_core::types::AutoLifecycleStage {
                condition_set:
                    pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity,
                at: 30,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        };
        insert_test_sandbox_with_policy(&conn, &sandbox_id, SandboxState::Started, &policy);
        let service = SandboxManagerService::for_test(conn);

        let before = now_secs();
        assert!(
            service
                .set_guest_activity_state(&sandbox_id, false)
                .await
                .expect("active state handled")
        );

        let row = service.lookup_sandbox(&sandbox_id).unwrap();
        let condition = row
            .auto_lifecycle_state
            .conditions
            .get(&pplx_space_common_core::types::AutoLifecycleConditionSetId::GuestActivity)
            .expect("guest activity condition");
        let epoch_at = condition.epoch_at.expect("future guest idle epoch");
        assert!(condition.armed);
        assert!(epoch_at >= before + TEST_GUEST_ACTIVITY_HOLD_CAP.as_secs());
        assert!(epoch_at <= now_secs() + TEST_GUEST_ACTIVITY_HOLD_CAP.as_secs());
        assert_eq!(
            row.auto_lifecycle_state.next_auto_event_at,
            Some(epoch_at + 30)
        );
    }

    #[test]
    fn requested_and_startup_recovery_keep_distinct_authority() {
        let conn = crate::db::test_conn();

        let stopping_id = SandboxId::new().to_string();
        let cleanup_failed_id = SandboxId::new().to_string();
        let cleanup_required_id = SandboxId::new().to_string();
        let failed_runtime_id = SandboxId::new().to_string();
        let started_runtime_id = SandboxId::new().to_string();
        let started_stale_intent_id = SandboxId::new().to_string();
        let created_id = SandboxId::new().to_string();
        let malformed_id = SandboxId::new().to_string();

        insert_test_sandbox(&conn, &stopping_id, SandboxState::Stopping);
        insert_test_sandbox(&conn, &cleanup_failed_id, SandboxState::Suspended);
        crate::db::set_cleanup_intent(
            &conn,
            &cleanup_failed_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();
        crate::db::mark_cleanup_failed(
            &conn,
            &cleanup_failed_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
            "node-manager unavailable",
        )
        .unwrap();
        insert_test_sandbox(&conn, &cleanup_required_id, SandboxState::CleanupRequired);
        insert_test_sandbox(&conn, &failed_runtime_id, SandboxState::Failed);
        conn.execute(
            "UPDATE sandboxes SET uid = 10000, slot = 0 WHERE id = ?1",
            crate::db::fixture::params![&failed_runtime_id],
        )
        .unwrap();
        insert_test_sandbox(&conn, &started_runtime_id, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET uid = 10001, slot = 1, pid = ?1 WHERE id = ?2",
            crate::db::fixture::params![std::process::id(), &started_runtime_id],
        )
        .unwrap();
        insert_test_sandbox(&conn, &started_stale_intent_id, SandboxState::Started);
        crate::db::set_cleanup_intent(
            &conn,
            &started_stale_intent_id,
            true,
            CleanupCompletion::MarkStopped,
        )
        .unwrap();
        insert_test_sandbox(&conn, &created_id, SandboxState::Created);
        insert_test_sandbox(&conn, &malformed_id, SandboxState::Created);
        crate::db::seed_historical_lifecycle_rows(&conn, |conn| {
            conn.execute(
                "UPDATE sandboxes SET state = 'state_from_newer_worker' WHERE id = ?1",
                [&malformed_id],
            )
            .unwrap();
        });

        let db = crate::db::SharedConnection::new(conn);
        let mut targets = SandboxManagerService::recover_required_cleanup_targets(
            &db,
            super::recovery::CleanupRecoveryAuthority::RequestedOnly,
        )
        .unwrap()
        .into_iter()
        .map(|target| (target.sandbox_id, target.keep_filesystem, target.completion))
        .collect::<Vec<_>>();
        targets.sort_by_key(|target| target.0);
        let mut expected_targets = vec![
            (
                stopping_id.parse::<SandboxId>().unwrap(),
                true,
                CleanupCompletion::MarkStopped,
            ),
            (
                cleanup_failed_id.parse::<SandboxId>().unwrap(),
                true,
                CleanupCompletion::MarkSuspended,
            ),
            (
                cleanup_required_id.parse::<SandboxId>().unwrap(),
                true,
                CleanupCompletion::Quarantine,
            ),
        ];
        expected_targets.sort_by_key(|target| target.0);

        assert_eq!(targets, expected_targets);

        let db_guard = db.lock().expect("mutex poisoned");
        let stopping = crate::db::get_sandbox(&db_guard, &stopping_id)
            .unwrap()
            .unwrap();
        assert_eq!(stopping.state, SandboxState::Stopping);
        assert!(stopping.cleanup_completion.is_none());
        assert!(stopping.cleanup_keep_filesystem.is_none());

        let failed_runtime = crate::db::get_sandbox(&db_guard, &failed_runtime_id)
            .unwrap()
            .unwrap();
        assert_eq!(failed_runtime.state, SandboxState::Failed);
        assert!(failed_runtime.cleanup_completion.is_none());
        assert!(failed_runtime.cleanup_keep_filesystem.is_none());

        assert_eq!(
            crate::db::get_sandbox(&db_guard, &cleanup_failed_id)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::CleanupFailed
        );
        assert_eq!(
            crate::db::get_sandbox(&db_guard, &started_runtime_id)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Started
        );
        let started_stale_intent = crate::db::get_sandbox(&db_guard, &started_stale_intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(started_stale_intent.state, SandboxState::Started);
        assert_eq!(
            started_stale_intent.cleanup_completion,
            Some(CleanupCompletion::MarkStopped)
        );
        assert_eq!(started_stale_intent.cleanup_keep_filesystem, Some(true));
        assert_eq!(
            crate::db::get_sandbox(&db_guard, &created_id)
                .unwrap()
                .unwrap()
                .state,
            SandboxState::Created
        );
        drop(db_guard);

        let mut startup_targets = SandboxManagerService::recover_required_cleanup_targets(
            &db,
            super::recovery::CleanupRecoveryAuthority::Startup,
        )
        .unwrap()
        .into_iter()
        .map(|target| (target.sandbox_id, target.keep_filesystem, target.completion))
        .collect::<Vec<_>>();
        startup_targets.sort_by_key(|target| target.0);
        let mut expected_startup_targets = expected_targets;
        expected_startup_targets.sort_by_key(|target| target.0);
        assert_eq!(startup_targets, expected_startup_targets);

        let db_guard = db.lock().expect("mutex poisoned");
        let started_stale_intent = crate::db::get_sandbox(&db_guard, &started_stale_intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(started_stale_intent.state, SandboxState::Started);
        assert_eq!(
            started_stale_intent.cleanup_completion,
            Some(CleanupCompletion::MarkStopped)
        );
        assert_eq!(started_stale_intent.cleanup_keep_filesystem, Some(true));
    }

    #[test]
    fn cleanup_completion_and_failure_apply_recovery_intent() {
        let conn = crate::db::test_conn();

        let stop_id = SandboxId::new().to_string();
        let delete_id = SandboxId::new().to_string();
        let failed_id = SandboxId::new().to_string();

        insert_test_sandbox(&conn, &stop_id, SandboxState::Created);
        crate::db::mark_cleanup_required(
            &conn,
            &stop_id,
            true,
            crate::db::CleanupCompletion::MarkStopped,
            "recover stop cleanup",
        )
        .unwrap();
        let receipt_id = Uuid::now_v7().to_string();
        conn.execute(
            "UPDATE sandboxes
                SET transition_receipt_id = ?1,
                    transition_receipt_kind = 'activation_rollback',
                    transition_receipt_target_state = 'stopped',
                    transition_receipt_execution_generation = 7,
                    transition_receipt_reason_code = 'cold_activation_interrupted',
                    transition_receipt_activation_attempt_id = ?2
              WHERE id = ?3",
            crate::db::fixture::params![&receipt_id, Uuid::now_v7().to_string(), &stop_id],
        )
        .unwrap();
        insert_test_sandbox(&conn, &delete_id, SandboxState::Created);
        crate::db::mark_cleanup_required(
            &conn,
            &delete_id,
            false,
            crate::db::CleanupCompletion::DeleteRow,
            "recover delete cleanup",
        )
        .unwrap();
        insert_test_sandbox(&conn, &failed_id, SandboxState::Created);
        crate::db::mark_cleanup_required(
            &conn,
            &failed_id,
            true,
            crate::db::CleanupCompletion::Quarantine,
            "recover failed runtime cleanup",
        )
        .unwrap();

        let db = crate::db::SharedConnection::new(conn);
        SandboxManagerService::complete_async_cleanup(
            &db,
            &stop_id,
            CleanupCompletion::MarkStopped,
            None,
        )
        .unwrap();
        SandboxManagerService::complete_async_cleanup(
            &db,
            &delete_id,
            CleanupCompletion::DeleteRow,
            None,
        )
        .unwrap();
        SandboxManagerService::mark_async_cleanup_failed(
            &db,
            &failed_id,
            true,
            CleanupCompletion::Quarantine,
            "node-manager unavailable",
        );

        {
            let db = db.lock().expect("mutex poisoned");
            let stop = crate::db::get_sandbox(&db, &stop_id).unwrap().unwrap();
            assert_eq!(stop.state, SandboxState::Stopped);
            assert!(stop.cleanup_completion.is_none());
            assert!(stop.cleanup_keep_filesystem.is_none());
            assert_eq!(stop.current_local_artifact_id, None);
            assert_eq!(stop.current_local_artifact_kind, None);
            assert_eq!(stop.current_local_artifact_origin, None);
            let retained_receipt = db
                .query_row(
                    "SELECT transition_receipt_id FROM sandboxes WHERE id = ?1",
                    crate::db::fixture::params![&stop_id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .unwrap();
            assert_eq!(retained_receipt.as_deref(), Some(receipt_id.as_str()));
            assert!(crate::db::get_sandbox(&db, &delete_id).unwrap().is_none());
            let failed = crate::db::get_sandbox(&db, &failed_id).unwrap().unwrap();
            assert_eq!(failed.state, SandboxState::CleanupFailed);
            assert_eq!(
                failed.failure_reason.as_deref(),
                Some("node-manager unavailable")
            );
        }

        SandboxManagerService::complete_async_cleanup(
            &db,
            &failed_id,
            CleanupCompletion::Quarantine,
            None,
        )
        .unwrap();

        let db = db.lock().expect("mutex poisoned");
        let failed = crate::db::get_sandbox(&db, &failed_id).unwrap().unwrap();
        assert_eq!(failed.state, SandboxState::Quarantined);
        assert!(failed.cleanup_completion.is_none());
        assert!(failed.cleanup_keep_filesystem.is_none());
        assert_eq!(
            failed.failure_reason.as_deref(),
            Some("node-manager unavailable")
        );
    }

    #[test]
    fn suspended_cleanup_without_artifact_pin_quarantines() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();

        let db = crate::db::SharedConnection::new(conn);
        SandboxManagerService::complete_async_cleanup(
            &db,
            &sandbox_id,
            CleanupCompletion::MarkSuspended,
            None,
        )
        .unwrap();

        let db = db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
        assert_eq!(
            row.failure_reason.as_deref(),
            Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
        );
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
        assert!(row.current_local_artifact_id.is_none());
        assert_eq!(
            crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
                .unwrap()
                .as_deref(),
            Some(SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.as_str()),
        );
    }

    #[test]
    fn suspended_cleanup_with_unregistered_artifact_pin_quarantines() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let local_artifact_id = LocalArtifactId::new();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspending);
        conn.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = ?1,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?2",
            crate::db::fixture::params![local_artifact_id.to_string(), &sandbox_id],
        )
        .unwrap();
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();

        let db = crate::db::SharedConnection::new(conn);
        SandboxManagerService::complete_async_cleanup(
            &db,
            &sandbox_id,
            CleanupCompletion::MarkSuspended,
            None,
        )
        .unwrap();

        let db = db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
        assert_eq!(
            row.failure_reason.as_deref(),
            Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
        );
        assert_eq!(
            crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
                .unwrap()
                .as_deref(),
            Some(SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.as_str()),
        );
    }

    #[test]
    fn cleanup_failed_suspend_completion_can_publish_artifact() {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        crate::db::set_cleanup_intent(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
        )
        .unwrap();
        crate::db::mark_cleanup_failed(
            &conn,
            &sandbox_id,
            true,
            crate::db::CleanupCompletion::MarkSuspended,
            "node-manager unavailable",
        )
        .unwrap();

        let db = crate::db::SharedConnection::new(conn);
        let local_artifact_id = LocalArtifactId::new();
        let local_path = format!("/space/cache/{sandbox_id}/snapshot");
        SandboxManagerService::complete_async_cleanup(
            &db,
            &sandbox_id,
            CleanupCompletion::MarkSuspended,
            Some((local_artifact_id, local_path.clone())),
        )
        .unwrap();

        let db = db.lock().expect("mutex poisoned");
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Suspended);
        assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
        assert!(row.cleanup_completion.is_none());
        assert!(row.cleanup_keep_filesystem.is_none());
        let artifact = crate::db::get_local_artifact(&db, local_artifact_id)
            .unwrap()
            .expect("published artifact must be registered");
        assert_eq!(artifact.local_path, local_path);
    }

    /// Tests for the suspend-after-wake oscillation signal: the classifier
    /// that decides whether a suspend request landed close enough to the
    /// last completed wake to flag a possible wake/suspend oscillation.
    mod suspend_after_wake {
        use std::time::Duration;

        use super::super::grpc_lifecycle::classify_suspend_after_wake;

        const THRESHOLD: Duration = Duration::from_secs(5);

        /// No recorded wake (fresh sandbox, or record dropped on cleanup)
        /// → no observation, no event field, no warn.
        #[test]
        fn no_recorded_wake_yields_no_observation() {
            assert!(classify_suspend_after_wake(None, THRESHOLD).is_none());
        }

        /// Suspend inside the threshold must carry the delta and flag the warn.
        #[test]
        fn suspend_within_threshold_flags_warn() {
            let observed = classify_suspend_after_wake(Some(Duration::from_millis(300)), THRESHOLD)
                .expect("a recorded wake must produce an observation");

            assert_eq!(observed.ms_since_wake, 300);
            assert!(observed.within_warn_threshold);
        }

        /// Suspend outside the threshold still carries the delta on the
        /// event but must not warn.
        #[test]
        fn suspend_outside_threshold_keeps_delta_without_warn() {
            let observed = classify_suspend_after_wake(Some(Duration::from_secs(120)), THRESHOLD)
                .expect("a recorded wake must produce an observation");

            assert_eq!(observed.ms_since_wake, 120_000);
            assert!(!observed.within_warn_threshold);
        }

        /// The boundary is inclusive: exactly-at-threshold still warns.
        #[test]
        fn suspend_exactly_at_threshold_flags_warn() {
            let observed = classify_suspend_after_wake(Some(THRESHOLD), THRESHOLD)
                .expect("a recorded wake must produce an observation");

            assert!(observed.within_warn_threshold);
        }
    }

    /// Tests for suspend-vs-wake arbitration: the ordering predicate that
    /// decides whether a suspend queued behind a concurrent wake. (The
    /// kill-switch value vocabulary is the shared
    /// `pplx_space_common_core::env::env_flag`, tested in common-core.)
    mod suspend_wake_arbitration {
        use std::time::{Duration, Instant};

        use super::super::grpc_lifecycle::suspend_superseded_by_wake;

        /// A wake that completed after the suspend request arrived means the
        /// suspend queued behind it and is stale.
        #[test]
        fn wake_after_arrival_supersedes() {
            let arrived_at = Instant::now();
            let wake_completed_at = arrived_at + Duration::from_millis(200);
            assert!(suspend_superseded_by_wake(
                arrived_at,
                Some(wake_completed_at)
            ));
        }

        /// A wake that completed before the suspend arrived is the normal
        /// "pause a running sandbox" case and must not be arbitrated away.
        #[test]
        fn wake_before_arrival_does_not_supersede() {
            let wake_completed_at = Instant::now();
            let arrived_at = wake_completed_at + Duration::from_millis(200);
            assert!(!suspend_superseded_by_wake(
                arrived_at,
                Some(wake_completed_at)
            ));
        }

        /// No recorded wake (fresh sandbox, or record dropped on cleanup)
        /// → nothing to arbitrate against.
        #[test]
        fn no_recorded_wake_does_not_supersede() {
            assert!(!suspend_superseded_by_wake(Instant::now(), None));
        }
    }

    /// Tests for the durable artifact-backup upload-config classifier.
    /// The classifier sits at the boundary between the org-override
    /// resolution helper (which can't return a `Status` because suspend
    /// needs to ignore lookup failures) and backup admission (which must
    /// surface lookup failures as `Internal`, not `FailedPrecondition`,
    /// to keep the drain reconciler's marker classifier from treating
    /// a transient cluster-api blip as terminal.
    mod reupload_cluster_api_env {
        use super::super::grpc_lifecycle::reupload_precondition_for_cluster_api_env;
        use pplx_space_common_core::node_drain_signals::{
            REUPLOAD_ARTIFACT_MISSING_MARKER, REUPLOAD_SANDBOX_UNKNOWN_MARKER,
        };

        /// Both env vars present → the RPC may proceed.
        #[test]
        fn both_envs_set_yields_no_precondition() {
            assert!(reupload_precondition_for_cluster_api_env(true, true).is_none());
        }

        /// Missing endpoint must surface as `FailedPrecondition`.
        #[test]
        fn missing_endpoint_is_failed_precondition() {
            let status = reupload_precondition_for_cluster_api_env(false, true)
                .expect("missing endpoint must produce a Status");
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(
                status.message().contains("SPACE_INTERNAL_API_ENDPOINT"),
                "operator-facing message should name the missing env var: {}",
                status.message(),
            );
        }

        /// Missing token must surface as `FailedPrecondition`.
        #[test]
        fn missing_token_is_failed_precondition() {
            let status = reupload_precondition_for_cluster_api_env(true, false)
                .expect("missing token must produce a Status");
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(
                status.message().contains("SPACE_INTERNAL_TOKEN"),
                "operator-facing message should name the missing env var: {}",
                status.message(),
            );
        }

        /// Missing both env vars must name both in the message.
        #[test]
        fn missing_both_names_both_in_message() {
            let status = reupload_precondition_for_cluster_api_env(false, false)
                .expect("missing both must produce a Status");
            assert!(status.message().contains("SPACE_INTERNAL_API_ENDPOINT"));
            assert!(status.message().contains("SPACE_INTERNAL_TOKEN"));
        }

        /// Critical invariant: the env-unwired precondition must NOT
        /// carry either of the terminal-classification markers. The
        /// future cluster-controller classifier keys off those markers
        /// to mark-as-failed; a deployment misconfig is operator-
        /// fixable, not a sandbox-lost signal.
        #[test]
        fn no_terminal_markers_in_env_unwired_message() {
            for (endpoint, token) in [(false, true), (true, false), (false, false)] {
                let status = reupload_precondition_for_cluster_api_env(endpoint, token)
                    .expect("missing wiring must produce a Status");
                assert!(
                    !status.message().contains(REUPLOAD_ARTIFACT_MISSING_MARKER),
                    "env-unwired must not look like artifact-missing: {}",
                    status.message(),
                );
                assert!(
                    !status.message().contains(REUPLOAD_SANDBOX_UNKNOWN_MARKER),
                    "env-unwired must not look like sandbox-unknown: {}",
                    status.message(),
                );
            }
        }
    }

    mod reupload_resolution {
        use super::super::grpc_lifecycle::{
            OrgSnapshotConfigResolution, reupload_resolution_to_config,
        };
        use crate::snapshot_upload::SnapshotUploadConfig;

        fn dummy_config() -> SnapshotUploadConfig {
            SnapshotUploadConfig::from_uri("s3://test-bucket/prefix?region=us-east-1")
                .expect("hard-coded test URI must parse")
        }

        /// A transient cluster-api lookup failure on a node with **no**
        /// env-derived fallback must surface as `Internal`. If it
        /// surfaced as `FailedPrecondition`, the drain reconciler's
        /// classifier would treat it as terminal and mark the sandbox
        /// failed on a recoverable blip.
        #[test]
        fn lookup_failed_without_env_fallback_is_internal_retryable() {
            let status = reupload_resolution_to_config(
                OrgSnapshotConfigResolution::LookupFailed("connect refused".into()),
                false,
            )
            .expect_err("LookupFailed must produce an Err Status");
            assert_eq!(
                status.code(),
                tonic::Code::Internal,
                "transient lookup failure must be Internal so the reconciler retries; \
                 got FailedPrecondition would risk spurious mark-as-failed",
            );
            assert!(
                status.message().contains("retry"),
                "Internal status should hint that retry is expected: {}",
                status.message(),
            );
        }

        /// Same as above, but with an env fallback configured. The env
        /// fallback must not suppress the `Internal` classification —
        /// the lookup failure is independent of whether we *could* fall
        /// back, and the caller deserves to see the transient signal.
        #[test]
        fn lookup_failed_with_env_fallback_is_still_internal_retryable() {
            let status = reupload_resolution_to_config(
                OrgSnapshotConfigResolution::LookupFailed("rpc timeout".into()),
                true,
            )
            .expect_err("LookupFailed must produce an Err regardless of env fallback");
            assert_eq!(status.code(), tonic::Code::Internal);
        }

        /// Definitively-absent override + no env fallback = terminal
        /// `FailedPrecondition`. The operator has to wire one or the
        /// other; the reconciler's classifier correctly treats this as
        /// "can never succeed from this node."
        #[test]
        fn absent_without_env_fallback_is_failed_precondition() {
            let status = reupload_resolution_to_config(OrgSnapshotConfigResolution::Absent, false)
                .expect_err("Absent + no env fallback must produce an Err");
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(
                status.message().contains("SPACE_SNAPSHOT_BUCKET")
                    || status.message().contains("override"),
                "operator-facing message should name the missing configuration: {}",
                status.message(),
            );
        }

        /// Definitively-absent override but env fallback is configured.
        /// The handler proceeds with the env default.
        #[test]
        fn absent_with_env_fallback_proceeds_with_no_override() {
            let resolved = reupload_resolution_to_config(OrgSnapshotConfigResolution::Absent, true)
                .expect("Absent + env fallback should not error");
            assert!(
                resolved.is_none(),
                "Absent + env fallback yields None at the call site; \
                 the helper does not synthesize the env default itself",
            );
        }

        /// A successfully-resolved override passes through unchanged
        /// regardless of whether an env fallback is also configured.
        #[test]
        fn resolved_passes_through_with_or_without_env_fallback() {
            for env_fallback_present in [false, true] {
                let resolved = reupload_resolution_to_config(
                    OrgSnapshotConfigResolution::Resolved(dummy_config()),
                    env_fallback_present,
                )
                .expect("Resolved must not error");
                assert!(
                    resolved.is_some(),
                    "Resolved should pass through (env_fallback_present={env_fallback_present})",
                );
            }
        }

        /// `ClusterRowMissing` (the cluster has no row for this sandbox)
        /// must surface as `FailedPrecondition` regardless of env
        /// fallback presence. If we let it fall through to env config,
        /// the durable worker's `BeginArtifactBackup` call
        /// would later return `NotFound`, classified by the upload-jobs
        /// module as `JobError::permanent` — and since callers don't
        /// poll job status, the RPC would appear "accepted" while the
        /// work could never complete.
        #[test]
        fn cluster_row_missing_is_failed_precondition_regardless_of_env_fallback() {
            for env_fallback_present in [false, true] {
                let status = reupload_resolution_to_config(
                    OrgSnapshotConfigResolution::ClusterRowMissing,
                    env_fallback_present,
                )
                .expect_err("ClusterRowMissing must produce an Err");
                assert_eq!(
                    status.code(),
                    tonic::Code::FailedPrecondition,
                    "ClusterRowMissing must be FailedPrecondition \
                     (env_fallback_present={env_fallback_present})",
                );
                assert!(
                    status.message().contains("NotFound")
                        || status.message().contains("cluster-api has no row"),
                    "operator-facing message should describe the cluster divergence: {}",
                    status.message(),
                );
            }
        }
    }

    #[test]
    fn start_path_classifies_request_shape() {
        let mut req = StartRequest {
            activation_auto_lifecycle: None,
            operation_activation_attempt_id: None,
            restore_artifact_as_new: None,
            expected_local_artifact_id: None,
            sandbox_id: "sbx".to_string(),
            vcpu: None,
            memory_mib: None,
            checkpoint: None,
            from_warm: false,
            blocking: false,
            netns_path: None,
            ready_timeout_secs: None,
            target_state: 0,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            route_identity: None,
        };
        assert_eq!(
            SandboxStartPath::from_request(&req),
            SandboxStartPath::FreshCold
        );

        req.from_warm = true;
        assert_eq!(
            SandboxStartPath::from_request(&req),
            SandboxStartPath::WarmSnapshot
        );

        req.checkpoint = Some(SUSPEND_CHECKPOINT.to_string());
        assert_eq!(
            SandboxStartPath::from_request(&req),
            SandboxStartPath::CheckpointRestore
        );
    }

    fn activation_admission_config(
        max_concurrent_activations: usize,
        refill_rps: f64,
        burst: f64,
    ) -> ActivationAdmissionConfig {
        ActivationAdmissionConfig {
            max_concurrent_activations,
            refill_rps,
            burst,
        }
    }

    #[test]
    fn activation_admission_config_sanitizes_invalid_values() {
        let sanitized = sanitize_config(activation_admission_config(0, f64::NAN, 0.0));
        assert_eq!(sanitized, ActivationAdmissionConfig::default());

        let too_many = sanitize_config(activation_admission_config(
            tokio::sync::Semaphore::MAX_PERMITS + 1,
            1.0,
            1.0,
        ));
        assert_eq!(
            too_many.max_concurrent_activations,
            ActivationAdmissionConfig::default().max_concurrent_activations
        );

        let subunit_burst = sanitize_config(activation_admission_config(5, 5.0, 0.5));
        assert!(
            (subunit_burst.burst - ActivationAdmissionConfig::default().burst).abs() < f64::EPSILON
        );
    }

    #[test]
    fn activation_admission_concurrency_rejects_when_full() {
        let limiter =
            ActivationAdmissionLimiter::with_config(activation_admission_config(1, 100.0, 100.0));
        let held = limiter
            .try_acquire(ActivationAdmissionKind::Cold)
            .expect("first activation holds the only slot");
        assert_eq!(limiter.in_flight_for(ActivationAdmissionKind::Cold), 1);
        assert_eq!(limiter.in_flight_for(ActivationAdmissionKind::Restore), 0);

        let error = limiter
            .try_acquire(ActivationAdmissionKind::Restore)
            .expect_err("second concurrent activation should reject");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(limiter.in_flight(), 1);
        assert!(
            error
                .metadata()
                .get("x-space-retry-after-ms")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .is_some()
        );

        drop(held);
        assert_eq!(limiter.in_flight(), 0);
        assert_eq!(limiter.in_flight_for(ActivationAdmissionKind::Cold), 0);
    }

    #[test]
    fn activation_admission_extreme_refill_rate_does_not_panic_retry_after() {
        let limiter =
            ActivationAdmissionLimiter::with_config(activation_admission_config(100, 1e-20, 1.0));
        let first = limiter
            .try_acquire(ActivationAdmissionKind::Cold)
            .expect("first burst token");
        drop(first);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            limiter.try_acquire(ActivationAdmissionKind::Restore)
        }));
        assert!(result.is_ok(), "retry-after calculation must not panic");
        let error = result
            .expect("panic already checked")
            .expect_err("empty bucket should reject");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(
            error
                .metadata()
                .get("x-space-retry-after-ms")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .is_some()
        );
    }

    #[tokio::test]
    async fn activation_admission_concurrency_wait_does_not_depend_on_refill_rate() {
        let limiter = Arc::new(ActivationAdmissionLimiter::with_config(
            activation_admission_config(1, 0.001, 100.0),
        ));
        let held = limiter
            .try_acquire(ActivationAdmissionKind::Cold)
            .expect("first activation holds the only slot");
        let waiter = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move {
                limiter
                    .acquire(
                        ActivationAdmissionKind::Restore,
                        "019ee08a-5c80-7e81-9241-01aeb9ec9d83",
                    )
                    .await
            })
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            !waiter.is_finished(),
            "second activation should wait for the busy slot instead of rejecting immediately"
        );

        drop(held);
        let permit = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter should acquire after the first permit drops")
            .expect("wait task should not panic")
            .expect("waited acquisition should succeed");
        assert_eq!(limiter.in_flight(), 1);
        drop(permit);
        assert_eq!(limiter.in_flight(), 0);
    }

    #[tokio::test]
    async fn cleanup_permit_drop_releases_semaphore_and_decrements_gauge() {
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let metrics = Arc::new(CleanupMetrics::default());
        {
            let raw = sem.clone().acquire_owned().await.unwrap();
            let _permit = CleanupPermit::new(raw, Arc::clone(&metrics));
            assert_eq!(metrics.in_flight(), 1);
            assert_eq!(sem.available_permits(), 0);
        } // permit drops here, releasing both.
        assert_eq!(metrics.in_flight(), 0);
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn cleanup_semaphore_caps_concurrent_holders() {
        // Independent of the full SandboxManagerService — just exercise the
        // semaphore-and-metrics wiring that schedule_async_cleanup applies.
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let metrics = Arc::new(CleanupMetrics::default());

        let mut handles = Vec::new();
        for _ in 0..5 {
            let sem = Arc::clone(&sem);
            let metrics = Arc::clone(&metrics);
            handles.push(tokio::spawn(async move {
                metrics.waiting_inc();
                let raw = sem.acquire_owned().await.unwrap();
                metrics.waiting_dec();
                let _permit = CleanupPermit::new(raw, Arc::clone(&metrics));
                tokio::time::sleep(Duration::from_millis(50)).await;
                // _permit drops here — releases semaphore + decrements gauge.
            }));
        }

        // Yield enough times for tasks to start and saturate the semaphore.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let in_flight = metrics.in_flight();
        assert!(in_flight <= 2, "cap exceeded: in_flight={in_flight}");

        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(metrics.in_flight(), 0);
        assert_eq!(metrics.waiting(), 0);
    }
}
