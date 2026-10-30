use super::*;
use crate::service::tests::service_tests::insert_test_sandbox;
use pplx_space_common_core::host::linux::identities::users;
use pplx_space_common_svc::PeerIdentity;
use pplx_space_template_mgr_client::proto::TemplateFullInfo;

fn peer_request<T>(message: T, peer: Option<PeerIdentity>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(peer) = peer {
        request.extensions_mut().insert(peer);
    }
    request
}

fn root_peer() -> PeerIdentity {
    PeerIdentity::new(0, 0, None)
}

fn service_peer(user: &pplx_space_common_core::host::linux::identities::LinuxUser) -> PeerIdentity {
    PeerIdentity::new(user.uid, user.primary_group.gid, None)
}

/// Run the test in a child process without the feature-flag provider and
/// route-fence environment, so service construction cannot start a provider.
/// Call before constructing any service; return when false.
fn enter_isolated_test_process() -> bool {
    crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ])
}

/// A service whose host dependencies all resolve under `scratch`; see
/// [`SandboxManagerServiceOptions::for_test_in`].
fn isolated_service(
    conn: crate::db::Connection,
    scratch: &std::path::Path,
    node_role: Option<NodeRole>,
) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_role,
            ..SandboxManagerServiceOptions::for_test_in(scratch)
        },
    )
}

/// An auto-lifecycle authority no proto value names. Create parses it last
/// before template lookup. A lookup past the barrier stops at the test
/// service's template-manager fake, whose socket is unbound: the lookup fails
/// to connect (`TemplateLookupError::Connect`), which create reports as
/// `Internal` "cannot connect to template-manager".
const BARRIER_AUTHORITY: i32 = 98;
/// The barrier's refusal; no case expects this message for its own refusal.
const BARRIER_REFUSAL: &str = "policy authority proto value 98 is unknown";

#[tokio::test]
async fn legacy_delete_after_durable_delete_does_not_dispatch_node_cleanup() {
    if !enter_isolated_test_process() {
        return;
    }
    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Started);
    let service = isolated_service(conn, scratch.path(), None);
    // Completed durable deletion leaves no canonical cache directory.
    let cache_root = tempfile::tempdir().unwrap();
    *service.cleanup_executor.cache_dir.lock().unwrap() = Some(cache_root.path().to_owned());

    // The shim observed a live sandbox before durable deletion completed.
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Started
    );
    {
        let db = service.db.lock().unwrap();
        crate::db::test_fixtures::state_write_fixtures::apply_edge(
            &db,
            &id,
            &crate::db::edges::DELETE_ADMISSION,
        );
        crate::db::set_cleanup_intent(&db, &id, false, CleanupCompletion::DeleteRow).unwrap();
        crate::db::complete_row_intent_cleanup(&db, &id, CleanupCompletion::DeleteRow, None)
            .unwrap();
        assert!(crate::db::get_sandbox(&db, &id).unwrap().is_none());
    }

    // Exercise Delete's missing-row admission and dispatch path. Injecting the
    // dispatch keeps even a regression from dialing the host's node-manager.
    let admission = service
        .admit_selected_cleanup(
            sandbox_id,
            CleanupCompletion::DeleteRow,
            false,
            true,
            None,
            false,
        )
        .await
        .unwrap();
    service
        .run_cleanup_after_persistence_failure_with_node_dispatch(
            SynchronousCleanupRequest {
                sandbox_id: &id,
                uuid8: &id,
                keep_filesystem: false,
                context: "cleanup after unpersisted delete",
                completion: CleanupCompletion::DeleteRow,
                published_artifact: None,
                immediate_kill: true,
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            },
            &admission,
            || async {
                panic!("an already-deleted sandbox must not dispatch or acknowledge node cleanup")
            },
        )
        .await
        .unwrap();
    service
        .finish_unpersisted_delete_cleanup(
            &id,
            admission.into_unpersisted_error(sandbox_id).unwrap(),
        )
        .unwrap();
}

#[tokio::test]
async fn delete_of_rowless_disk_sandbox_is_not_reported_deleted() {
    if !enter_isolated_test_process() {
        return;
    }
    let scratch = tempfile::tempdir().unwrap();
    let cache_root = tempfile::tempdir().unwrap();
    let service = isolated_service(crate::db::test_conn(), scratch.path(), None);
    *service.cleanup_executor.cache_dir.lock().unwrap() = Some(cache_root.path().to_owned());
    // No row, but the canonical cache directory selects it for cleanup.
    let id = SandboxId::new().to_string();
    let cache_dir = cache_root.path().join(&id);
    std::fs::create_dir_all(cache_dir.join("instance")).unwrap();

    for blocking in [true, false] {
        let error = service
            .delete(peer_request(
                DeleteRequest {
                    sandbox_id: id.clone(),
                    template_filter: None,
                    blocking,
                },
                Some(root_peer()),
            ))
            .await
            .expect_err("a rowless sandbox with remaining disk state is not deleted");
        assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
        assert!(
            cache_dir.is_dir(),
            "the selected cache directory must remain"
        );
    }
    assert!(service.lookup_sandbox(&id).is_err());
}

/// A create refused by the auto-lifecycle authority parse, the last check
/// before template lookup. Every create case starts from it, so a regression
/// that removes the refusal a case pins still stops here, with a refusal that
/// case does not expect.
fn create_refused_before_template_lookup() -> CreateRequest {
    CreateRequest {
        template: "seed".into(),
        owner: "root@local".into(),
        runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.into(),
        boot_mode: BootMode::Cold as i32,
        sandbox_id: Some(SandboxId::new().to_string()),
        auto_lifecycle: Some(Default::default()),
        auto_lifecycle_authority: BARRIER_AUTHORITY,
        ..Default::default()
    }
}

fn every_boot_mode() -> [BootMode; 4] {
    let modes = [
        BootMode::Unspecified,
        BootMode::Cold,
        BootMode::BaseSnapshot,
        BootMode::Pool,
    ];
    // A new variant fails to compile here until it joins the sweep.
    for mode in modes {
        match mode {
            BootMode::Unspecified | BootMode::Cold | BootMode::BaseSnapshot | BootMode::Pool => {}
        }
    }
    modes
}

#[tokio::test]
async fn forwarded_create_refuses_owner_and_network_policy_authority_before_template_lookup() {
    use pplx_space_net_common::{NetworkPolicyV1, proto::PolicyAuthority};

    if !enter_isolated_test_process() {
        return;
    }
    struct Case {
        name: &'static str,
        owner: &'static str,
        network_policy_authority: Option<i32>,
        code: tonic::Code,
        message: &'static str,
    }
    // Every declared network-policy authority plus an unknown value.
    let cases = [
        Case {
            name: "template-manager owner",
            owner: "tmplmgr@local",
            network_policy_authority: None,
            code: tonic::Code::PermissionDenied,
            message: "tmplmgr@local",
        },
        Case {
            name: "local network policy",
            owner: "root@local",
            network_policy_authority: Some(PolicyAuthority::Local as i32),
            code: tonic::Code::PermissionDenied,
            message: "authority=local",
        },
        Case {
            name: "unspecified network policy authority",
            owner: "root@local",
            network_policy_authority: Some(PolicyAuthority::Unspecified as i32),
            code: tonic::Code::InvalidArgument,
            message: "policy authority proto value is unspecified",
        },
        Case {
            name: "unknown network policy authority",
            owner: "root@local",
            network_policy_authority: Some(99),
            code: tonic::Code::InvalidArgument,
            message: "policy authority proto value 99 is unknown",
        },
        // Accepted from a forwarded peer: only the barrier refuses these.
        Case {
            name: "no network policy",
            owner: "root@local",
            network_policy_authority: None,
            code: tonic::Code::InvalidArgument,
            message: BARRIER_REFUSAL,
        },
        Case {
            name: "cluster network policy",
            owner: "root@local",
            network_policy_authority: Some(PolicyAuthority::Cluster as i32),
            code: tonic::Code::InvalidArgument,
            message: BARRIER_REFUSAL,
        },
    ];
    for boot_mode in every_boot_mode() {
        for case in &cases {
            let context = format!("{} ({boot_mode:?})", case.name);
            let scratch = tempfile::tempdir().unwrap();
            let service = isolated_service(crate::db::test_conn(), scratch.path(), None);
            let request = CreateRequest {
                owner: case.owner.into(),
                boot_mode: boot_mode as i32,
                network_policy: case
                    .network_policy_authority
                    .map(|_| (&NetworkPolicyV1::allow_all()).into()),
                network_policy_authority: case.network_policy_authority.unwrap_or_default(),
                ..create_refused_before_template_lookup()
            };

            let error = service
                .create(peer_request(
                    request,
                    Some(service_peer(&users::SPACE_NODEAPI)),
                ))
                .await
                .expect_err(case.name);
            assert_eq!(error.code(), case.code, "{context}: {error}");
            assert!(error.message().contains(case.message), "{context}: {error}");
        }
    }
}

/// Bounds every wait on cleanup, including correct runs: blocking RPCs and
/// job waits rely on the executor's wake-up, whose fallback is a one-second
/// tick, so the bound leaves room for a loaded CI runner. A regression that
/// blocks on held node cleanup fails here instead of hanging.
const CLEANUP_WAIT: Duration = Duration::from_secs(30);

async fn within_cleanup_wait<F: std::future::Future>(context: &str, future: F) -> F::Output {
    tokio::time::timeout(CLEANUP_WAIT, future)
        .await
        .unwrap_or_else(|_| panic!("{context}: waited longer than {CLEANUP_WAIT:?}"))
}

/// Stop and delete reach node cleanup only through the executor's test
/// dispatch, which waits for a permit the test grants. The runtime liveness
/// probe reads a test-owned run directory. Every admitted stop or ordinary
/// delete still removes watchdog monitoring, which stats the fixed sidecar
/// pid path of the fresh sandbox id; nothing exists there and nothing is
/// probed.
struct CleanupFixture {
    service: SandboxManagerService,
    run_dir: tempfile::TempDir,
    dispatch_permits: Arc<tokio::sync::Semaphore>,
}

impl CleanupFixture {
    fn new(conn: crate::db::Connection, dispatch_permits: usize) -> Self {
        Self::with_node_role(conn, dispatch_permits, None)
    }

    fn with_node_role(
        conn: crate::db::Connection,
        dispatch_permits: usize,
        node_role: Option<NodeRole>,
    ) -> Self {
        let run_dir = tempfile::tempdir().unwrap();
        let service = isolated_service(conn, run_dir.path(), node_role);
        let dispatch_permits = Arc::new(tokio::sync::Semaphore::new(dispatch_permits));
        let permits = Arc::clone(&dispatch_permits);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |_, _| {
            let permits = Arc::clone(&permits);
            Box::pin(async move {
                match permits.acquire().await {
                    Ok(permit) => {
                        permit.forget();
                        Ok(())
                    }
                    Err(_) => Err(Status::unavailable("test node cleanup closed")),
                }
            })
        }));
        Self {
            service,
            run_dir,
            dispatch_permits,
        }
    }

    /// Make the liveness probe report a runtime for `id`.
    fn mark_running(&self, id: &str) {
        let sandbox_dir = self.run_dir.path().join(id);
        std::fs::create_dir(&sandbox_dir).unwrap();
        std::os::unix::fs::symlink(self.run_dir.path(), sandbox_dir.join("fc.pid")).unwrap();
    }

    fn state(&self, id: &str) -> Option<SandboxState> {
        crate::db::get_sandbox(&self.service.db_conn().unwrap(), id)
            .unwrap()
            .map(|row| row.state)
    }

    fn active_cleanup_job(&self, id: &str) -> Option<pplx_space_common_core::types::JobId> {
        pplx_space_common_jobs::db::find_active_job(
            self.service.db_conn().unwrap().raw(),
            crate::db::cleanup_jobs::KIND,
            id,
        )
        .unwrap()
    }

    fn dirty_ids(&self) -> Vec<String> {
        crate::db::list_node_dirty_refs(&self.service.db_conn().unwrap(), 100)
            .unwrap()
            .into_iter()
            .map(|row| row.element_value)
            .collect()
    }

    /// The sandbox kept `state` and gained no cleanup work or publication.
    fn assert_untouched(&self, id: &str, state: SandboxState, context: &str) {
        assert_eq!(self.state(id), Some(state), "{context}");
        assert!(
            crate::db::cleanup_episode(&self.service.db_conn().unwrap(), id)
                .unwrap()
                .is_none(),
            "{context}"
        );
        assert!(self.active_cleanup_job(id).is_none(), "{context}");
        assert!(self.dirty_ids().is_empty(), "{context}");
    }

    async fn shutdown(self) {
        self.dispatch_permits.close();
        self.service.shutdown_token.cancel();
        self.service.task_tracker.close();
        within_cleanup_wait("shutdown", self.service.task_tracker.wait()).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum CleanupRpc {
    Stop,
    Delete,
}

impl CleanupRpc {
    async fn call(
        self,
        service: &SandboxManagerService,
        peer: Option<PeerIdentity>,
        sandbox_id: &str,
        template_filter: Option<&str>,
        blocking: bool,
    ) -> Result<Vec<String>, Status> {
        let template_filter = template_filter.map(str::to_owned);
        let call = async {
            match self {
                Self::Stop => service
                    .stop(peer_request(
                        StopRequest {
                            sandbox_id: sandbox_id.to_owned(),
                            template_filter,
                            blocking,
                            immediate_kill: false,
                        },
                        peer,
                    ))
                    .await
                    .map(|response| response.into_inner().stopped_ids),
                Self::Delete => service
                    .delete(peer_request(
                        DeleteRequest {
                            sandbox_id: sandbox_id.to_owned(),
                            template_filter,
                            blocking,
                        },
                        peer,
                    ))
                    .await
                    .map(|response| response.into_inner().deleted_ids),
            }
        };
        within_cleanup_wait(&format!("{self:?}"), call).await
    }

    /// Where a completed cleanup leaves the row.
    fn completed_state(self) -> Option<SandboxState> {
        match self {
            Self::Stop => Some(SandboxState::Stopped),
            Self::Delete => None,
        }
    }

    /// Where an admitted, not yet completed cleanup leaves the row.
    fn admitted_state(self) -> SandboxState {
        match self {
            Self::Stop => SandboxState::Stopping,
            Self::Delete => SandboxState::Deleting,
        }
    }
}

fn insert_owned_sandbox(conn: &crate::db::Connection, id: &str, owner: &str) {
    let sandbox_id = SandboxId::from_str(id).unwrap();
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id,
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
            state: SandboxState::Started,
            template: "seed",
            template_hash: Some("hash"),
            user: &SandboxOwner::parse(owner.to_owned()).unwrap(),
            runtime: "fc",
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
async fn core_rpcs_refuse_unauthenticated_and_unlisted_peers_without_mutation() {
    if !enter_isolated_test_process() {
        return;
    }
    // Create's owner check refuses node-ctl as well, so its refusal is pinned
    // by message to the peer check.
    for (peer, code, create_message) in [
        (
            None,
            tonic::Code::Unauthenticated,
            "UDS peer credentials missing",
        ),
        (
            Some(service_peer(&users::SPACE_NODECTL)),
            tonic::Code::PermissionDenied,
            "not authorized for Create",
        ),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        // Grant dispatch so a regression that admits the peer completes
        // instead of waiting for node cleanup.
        let fixture = CleanupFixture::new(conn, 4);
        fixture.mark_running(&id);
        let create = create_refused_before_template_lookup();
        let created_id = create.sandbox_id.clone().unwrap();

        let create_error = fixture
            .service
            .create(peer_request(create, peer.clone()))
            .await
            .expect_err("create");
        assert_eq!(create_error.code(), code, "create: {create_error}");
        assert!(
            create_error.message().contains(create_message),
            "create: {create_error}"
        );
        for rpc in [CleanupRpc::Stop, CleanupRpc::Delete] {
            let error = rpc
                .call(&fixture.service, peer.clone(), &id, None, true)
                .await
                .expect_err("unauthorized cleanup");
            assert_eq!(error.code(), code, "{rpc:?}");
        }

        assert!(
            !crate::db::sandbox_exists(&fixture.service.db_conn().unwrap(), &created_id).unwrap()
        );
        fixture.assert_untouched(&id, SandboxState::Started, &format!("{code:?}"));
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn stop_selects_only_a_live_runtime_and_both_treat_an_empty_template_filter_as_none() {
    if !enter_isolated_test_process() {
        return;
    }
    struct Case {
        name: &'static str,
        row: bool,
        running: bool,
        template_filter: Option<&'static str>,
        stop_selects: bool,
        delete_selects: bool,
    }
    let cases = [
        Case {
            name: "no template filter",
            row: true,
            running: true,
            template_filter: None,
            stop_selects: true,
            delete_selects: true,
        },
        Case {
            name: "empty template filter",
            row: true,
            running: true,
            template_filter: Some(""),
            stop_selects: true,
            delete_selects: true,
        },
        Case {
            name: "other template",
            row: true,
            running: true,
            template_filter: Some("other"),
            stop_selects: false,
            delete_selects: false,
        },
        Case {
            name: "no live runtime",
            row: true,
            running: false,
            template_filter: None,
            stop_selects: false,
            delete_selects: true,
        },
        // Absent sandboxes are idempotent successes. The lookup stats the
        // fixed sandbox cache path of a fresh UUID, which does not exist.
        Case {
            name: "absent sandbox",
            row: false,
            running: false,
            template_filter: None,
            stop_selects: false,
            delete_selects: false,
        },
    ];
    for case in cases {
        for rpc in [CleanupRpc::Stop, CleanupRpc::Delete] {
            let context = format!("{} ({rpc:?})", case.name);
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            if case.row {
                insert_test_sandbox(&conn, &id, SandboxState::Started);
            }
            let fixture = CleanupFixture::new(conn, 4);
            if case.running {
                fixture.mark_running(&id);
            }
            let selected = match rpc {
                CleanupRpc::Stop => case.stop_selects,
                CleanupRpc::Delete => case.delete_selects,
            };

            let ids = rpc
                .call(
                    &fixture.service,
                    Some(root_peer()),
                    &id,
                    case.template_filter,
                    true,
                )
                .await
                .unwrap();

            if selected {
                assert_eq!(ids, vec![id.clone()], "{context}");
                assert_eq!(fixture.state(&id), rpc.completed_state(), "{context}");
                assert!(fixture.dirty_ids().contains(&id), "{context}");
            } else {
                assert!(ids.is_empty(), "{context}");
                if case.row {
                    fixture.assert_untouched(&id, SandboxState::Started, &context);
                } else {
                    assert_eq!(fixture.state(&id), None, "{context}");
                    assert!(fixture.dirty_ids().is_empty(), "{context}");
                }
            }
            fixture.shutdown().await;
        }
    }
}

#[tokio::test]
async fn nonblocking_cleanup_returns_once_admitted_and_honors_skip_node_publish() {
    if !enter_isolated_test_process() {
        return;
    }
    for rpc in [CleanupRpc::Stop, CleanupRpc::Delete] {
        for skip_node_publish in [false, true] {
            let context = format!("{rpc:?} skip_node_publish={skip_node_publish}");
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &id, SandboxState::Started);
            // No dispatch permit: node cleanup cannot finish before the RPC
            // returns.
            let fixture = CleanupFixture::new(conn, 0);
            fixture.mark_running(&id);
            let mut request = peer_request((), Some(root_peer()));
            if skip_node_publish {
                request.metadata_mut().insert(
                    crate::node_state_sync::SKIP_NODE_PUBLISH_METADATA,
                    tonic::metadata::MetadataValue::from_static("true"),
                );
            }
            let (metadata, extensions, ()) = request.into_parts();
            let call = async {
                match rpc {
                    CleanupRpc::Stop => {
                        fixture
                            .service
                            .stop(Request::from_parts(
                                metadata,
                                extensions,
                                StopRequest {
                                    sandbox_id: id.clone(),
                                    template_filter: None,
                                    blocking: false,
                                    immediate_kill: false,
                                },
                            ))
                            .await
                            .unwrap()
                            .into_inner()
                            .stopped_ids
                    }
                    CleanupRpc::Delete => {
                        fixture
                            .service
                            .delete(Request::from_parts(
                                metadata,
                                extensions,
                                DeleteRequest {
                                    sandbox_id: id.clone(),
                                    template_filter: None,
                                    blocking: false,
                                },
                            ))
                            .await
                            .unwrap()
                            .into_inner()
                            .deleted_ids
                    }
                }
            };
            // A non-blocking RPC must return while node cleanup is still held.
            let ids = within_cleanup_wait(&context, call).await;

            assert_eq!(ids, vec![id.clone()], "{context}");
            assert_eq!(fixture.state(&id), Some(rpc.admitted_state()), "{context}");
            let job = fixture
                .active_cleanup_job(&id)
                .unwrap_or_else(|| panic!("{context}: admitted cleanup is durable"));
            assert_eq!(
                fixture.dirty_ids().contains(&id),
                !skip_node_publish,
                "{context}"
            );

            fixture.dispatch_permits.add_permits(1);
            within_cleanup_wait(&context, fixture.service.wait_cleanup_job(job))
                .await
                .unwrap();
            assert_eq!(fixture.state(&id), rpc.completed_state(), "{context}");
            // The header defers the dirty ref rather than suppressing it:
            // completion marks the sandbox dirty just after the job settles.
            within_cleanup_wait(&context, async {
                while !fixture.dirty_ids().contains(&id) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            fixture.shutdown().await;
        }
    }
}

#[tokio::test]
async fn template_manager_cleanup_requires_a_template_owned_sandbox() {
    if !enter_isolated_test_process() {
        return;
    }
    let template_manager_owner = SandboxOwnerKind::TemplateManager
        .reserved_owner_label()
        .unwrap();
    for (owner, allowed) in [("user", false), (template_manager_owner, true)] {
        for rpc in [CleanupRpc::Stop, CleanupRpc::Delete] {
            let context = format!("{owner} ({rpc:?})");
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            insert_owned_sandbox(&conn, &id, owner);
            let fixture = CleanupFixture::new(conn, 4);
            fixture.mark_running(&id);

            let result = rpc
                .call(
                    &fixture.service,
                    Some(service_peer(&users::SPACE_TMPLMGR)),
                    &id,
                    None,
                    true,
                )
                .await;

            if allowed {
                assert_eq!(result.unwrap(), vec![id.clone()], "{context}");
                assert_eq!(fixture.state(&id), rpc.completed_state(), "{context}");
            } else {
                assert_eq!(
                    result.unwrap_err().code(),
                    tonic::Code::PermissionDenied,
                    "{context}"
                );
                fixture.assert_untouched(&id, SandboxState::Started, &context);
            }
            fixture.shutdown().await;
        }
    }
}

#[tokio::test]
async fn cleanup_rpcs_refuse_after_shutdown_without_mutation() {
    if !enter_isolated_test_process() {
        return;
    }
    for rpc in [CleanupRpc::Stop, CleanupRpc::Delete] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        let fixture = CleanupFixture::new(conn, 4);
        fixture.mark_running(&id);
        fixture.service.shutdown_token.cancel();

        let error = rpc
            .call(&fixture.service, Some(root_peer()), &id, None, false)
            .await
            .expect_err("shutdown closes cleanup admission");

        assert_eq!(error.code(), tonic::Code::Unavailable, "{rpc:?}");
        fixture.assert_untouched(&id, SandboxState::Started, &format!("{rpc:?}"));
        fixture.shutdown().await;
    }
}

fn auto_lifecycle_expectation(
    policy: &AutoLifecyclePolicy,
) -> crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation {
    crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteExpectation {
        policy_fingerprint:
            pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint::for_policy(policy),
        authority: crate::db::PolicyAuthority::Cluster,
    }
}

fn access_idle_delete_policy(at: u64) -> AutoLifecyclePolicy {
    use pplx_space_common_core::types::{
        AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleStage,
    };
    AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at,
            action: AutoLifecycleIdleAction::Delete,
        }],
    }
    .normalized()
}

#[tokio::test]
async fn auto_lifecycle_delete_requires_one_canonical_id_and_the_expected_policy() {
    use crate::service::tests::service_tests::insert_test_sandbox_with_policy;

    if !enter_isolated_test_process() {
        return;
    }

    let policy = access_idle_delete_policy(900);
    let stale = access_idle_delete_policy(1800);
    struct Case<'a> {
        name: &'static str,
        uppercase_id: bool,
        template_filter: Option<&'static str>,
        expected_policy: &'a AutoLifecyclePolicy,
        /// The refusal's code and part of its message; one code comes from
        /// more than one branch.
        refusal: Option<(tonic::Code, &'static str)>,
    }
    for case in [
        Case {
            name: "template filter",
            uppercase_id: false,
            template_filter: Some("seed"),
            expected_policy: &policy,
            refusal: Some((
                tonic::Code::Internal,
                "auto-lifecycle delete requires one exact sandbox id",
            )),
        },
        Case {
            name: "non-canonical id",
            uppercase_id: true,
            template_filter: None,
            expected_policy: &policy,
            refusal: Some((
                tonic::Code::Internal,
                "auto-lifecycle delete requires a canonical sandbox id",
            )),
        },
        Case {
            name: "stale policy",
            uppercase_id: false,
            template_filter: None,
            expected_policy: &stale,
            refusal: Some((tonic::Code::Aborted, "auto-lifecycle delete lost its")),
        },
        Case {
            name: "current policy",
            uppercase_id: false,
            template_filter: None,
            expected_policy: &policy,
            refusal: None,
        },
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox_with_policy(&conn, &id, SandboxState::Started, &policy);
        // Auto-lifecycle delete is admitted only for a cluster-fenced route.
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &pplx_space_common_core::types::SpaceApiRouteIdentity {
                route_generation: Some(
                    pplx_space_common_core::types::RouteGeneration::new(7).unwrap(),
                ),
                current_node_id: Some(pplx_space_common_core::types::NodeId::new()),
                execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
                activation_attempt_id: Some(
                    pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                ),
                substrate_kind: Some(NodeManagedBy::Kubernetes),
                pod_uid: Some(pplx_space_common_core::types::PodUid::parse("pod-uid-7").unwrap()),
                ..Default::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap();
        let fixture = CleanupFixture::with_node_role(conn, 4, Some(NodeRole::Worker));
        fixture.mark_running(&id);
        let selector = if case.uppercase_id {
            id.to_uppercase()
        } else {
            id.clone()
        };

        let delete = fixture.service.delete_with_auto_lifecycle_expectation(
            peer_request(
                DeleteRequest {
                    sandbox_id: selector,
                    template_filter: case.template_filter.map(str::to_owned),
                    blocking: true,
                },
                Some(root_peer()),
            ),
            Some(auto_lifecycle_expectation(case.expected_policy)),
        );
        let result = within_cleanup_wait(case.name, delete).await;

        match case.refusal {
            Some((code, message)) => {
                let error = result.unwrap_err();
                assert_eq!(error.code(), code, "{}: {error}", case.name);
                assert!(error.message().contains(message), "{}: {error}", case.name);
                // The reconciler retries a lost policy comparison by this key.
                assert_eq!(
                    error.metadata().contains_key(
                        crate::service::cleanup::AUTO_LIFECYCLE_DELETE_CAS_LOST_METADATA
                    ),
                    code == tonic::Code::Aborted,
                    "{}",
                    case.name
                );
                fixture.assert_untouched(&id, SandboxState::Started, case.name);
                assert_eq!(
                    crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                        &fixture.service.db_conn().unwrap(),
                        sandbox_id,
                    )
                    .unwrap(),
                    None,
                    "{}",
                    case.name
                );
            }
            None => {
                assert_eq!(
                    result.unwrap().into_inner().deleted_ids,
                    vec![id.clone()],
                    "{}",
                    case.name
                );
                assert_eq!(fixture.state(&id), None, "{}", case.name);
            }
        }
        fixture.shutdown().await;
    }
}

/// Insert a pool clone whose publication still owns an armed disposal
/// episode, as `PoolWaitClone` leaves it before publication completes.
fn insert_armed_pool_clone(
    conn: &crate::db::Connection,
    owner: &str,
) -> (SandboxId, SandboxCleanupEpisodeId) {
    let id = SandboxId::new();
    let cleanup_id = SandboxCleanupEpisodeId::new();
    crate::db::insert_sandbox_with_cleanup_episode_and_route_evidence(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &id.to_string(),
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(id),
            state: SandboxState::Creating,
            template: "seed",
            template_hash: Some("sha256:test"),
            user: &SandboxOwner::parse(owner.to_owned()).unwrap(),
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
            completion: CleanupCompletion::DeletePoolClone,
            keep_filesystem: false,
            entered_from_state: None,
            activation_state: SandboxState::Creating,
            execution_generation: None,
            activation_attempt_id: None,
        },
        None,
        None,
        None,
    )
    .unwrap();
    (id, cleanup_id)
}

#[tokio::test]
async fn delete_of_a_pool_clone_disposal_requires_an_operator_after_quarantine() {
    if !enter_isolated_test_process() {
        return;
    }
    let template_manager_owner = SandboxOwnerKind::TemplateManager
        .reserved_owner_label()
        .unwrap();
    struct Case {
        name: &'static str,
        owner: &'static str,
        quarantined: bool,
        template_manager_peer: bool,
        auto_lifecycle: bool,
        hold_cleanup_fence: bool,
        blocking: bool,
        /// The refusal's code and part of its message; one code comes from
        /// more than one branch.
        refusal: Option<(tonic::Code, &'static str)>,
    }
    for case in [
        Case {
            name: "publication still owns the clone",
            owner: "deploy",
            quarantined: false,
            template_manager_peer: false,
            auto_lifecycle: false,
            hold_cleanup_fence: false,
            blocking: false,
            refusal: Some((
                tonic::Code::FailedPrecondition,
                "pool clone publication still owns this sandbox",
            )),
        },
        Case {
            name: "template manager after quarantine",
            owner: template_manager_owner,
            quarantined: true,
            template_manager_peer: true,
            auto_lifecycle: false,
            hold_cleanup_fence: false,
            blocking: false,
            refusal: Some((
                tonic::Code::FailedPrecondition,
                "clone disposal hold requires an explicit operator delete",
            )),
        },
        Case {
            name: "auto-lifecycle after quarantine",
            owner: "deploy",
            quarantined: true,
            template_manager_peer: false,
            auto_lifecycle: true,
            hold_cleanup_fence: false,
            blocking: false,
            refusal: Some((
                tonic::Code::FailedPrecondition,
                "clone disposal hold requires an explicit operator delete",
            )),
        },
        Case {
            name: "operator while cleanup is busy",
            owner: "deploy",
            quarantined: true,
            template_manager_peer: false,
            auto_lifecycle: false,
            hold_cleanup_fence: true,
            blocking: false,
            refusal: Some((tonic::Code::Unavailable, "clone disposal is busy")),
        },
        Case {
            name: "operator after quarantine",
            owner: "deploy",
            quarantined: true,
            template_manager_peer: false,
            auto_lifecycle: false,
            hold_cleanup_fence: false,
            blocking: false,
            refusal: None,
        },
        Case {
            name: "blocking operator after quarantine",
            owner: "deploy",
            quarantined: true,
            template_manager_peer: false,
            auto_lifecycle: false,
            hold_cleanup_fence: false,
            blocking: true,
            refusal: None,
        },
    ] {
        let conn = crate::db::test_conn();
        let (sandbox_id, cleanup_id) = insert_armed_pool_clone(&conn, case.owner);
        let id = sandbox_id.to_string();
        if case.quarantined {
            assert!(
                crate::db::mark_quarantined_from_state(
                    &conn,
                    &id,
                    SandboxState::Creating,
                    pplx_space_common_core::types::SandboxQuarantineSource::SbxManager,
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    "operator hold during publication",
                    pplx_space_common_core::types::SandboxQuarantineNextAction::RequiresOperatorAttention,
                )
                .unwrap()
            );
        }
        let held_state = if case.quarantined {
            SandboxState::Quarantined
        } else {
            SandboxState::Creating
        };
        // Without a dispatch permit an admitted non-blocking disposal stays
        // observable.
        let fixture = CleanupFixture::new(conn, if case.blocking { 1 } else { 0 });
        let fence = case.hold_cleanup_fence.then(|| {
            let CleanupLockTryResult::Acquired(fence) = fixture.service.try_cleanup_fence(&id)
            else {
                panic!("unused cleanup fence");
            };
            fence
        });
        let peer = if case.template_manager_peer {
            service_peer(&users::SPACE_TMPLMGR)
        } else {
            root_peer()
        };

        let delete = fixture.service.delete_with_auto_lifecycle_expectation(
            peer_request(
                DeleteRequest {
                    sandbox_id: id.clone(),
                    template_filter: None,
                    blocking: case.blocking,
                },
                Some(peer),
            ),
            case.auto_lifecycle
                .then(|| auto_lifecycle_expectation(&AutoLifecyclePolicy::disabled())),
        );
        // A non-blocking delete must return while node cleanup is still held.
        let result = within_cleanup_wait(case.name, delete).await;
        drop(fence);

        let episode = crate::db::cleanup_episode(&fixture.service.db_conn().unwrap(), &id).unwrap();
        match case.refusal {
            Some((code, message)) => {
                let episode = episode.unwrap();
                assert_eq!(episode.id, cleanup_id, "{}", case.name);
                let error = result.unwrap_err();
                assert_eq!(error.code(), code, "{}: {error}", case.name);
                assert!(error.message().contains(message), "{}: {error}", case.name);
                assert_eq!(fixture.state(&id), Some(held_state), "{}", case.name);
                assert_eq!(episode.requested_at, None, "{}", case.name);
                assert!(fixture.active_cleanup_job(&id).is_none(), "{}", case.name);
            }
            None => {
                assert_eq!(
                    result.unwrap().into_inner().deleted_ids,
                    vec![id.clone()],
                    "{}",
                    case.name
                );
                if case.blocking {
                    assert_eq!(fixture.state(&id), None, "{}", case.name);
                    assert!(episode.is_none(), "{}", case.name);
                } else {
                    let episode = episode.unwrap();
                    assert_eq!(episode.id, cleanup_id, "{}", case.name);
                    assert!(episode.requested_at.is_some(), "{}", case.name);
                    assert!(fixture.active_cleanup_job(&id).is_some(), "{}", case.name);
                }
            }
        }
        fixture.shutdown().await;
    }
}

#[test]
fn template_catalog_preserves_owner_identity_with_corrupt_supplemental_metadata() {
    let cache = tempfile::tempdir().unwrap();
    let hash = format!("sha256:{}", "a".repeat(64));
    let directory = cache.path().join("seed").join(&hash);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("metadata.json"), b"not json").unwrap();
    let templates = project_template_catalog(
        vec![TemplateFullInfo {
            name: "seed".into(),
            content_hash: Some(hash.clone()),
            built_at: Some("2026-09-18".into()),
            artifact_size_bytes: Some(42),
            ..Default::default()
        }],
        cache.path(),
    )
    .unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].id, "seed");
    assert_eq!(templates[0].sha256, hash);
    assert_eq!(templates[0].built_at, "2026-09-18");
    assert_eq!(templates[0].size_bytes, 42);
    assert!(templates[0].description.is_empty());
    assert!(templates[0].base.is_empty());
    assert!(templates[0].spaced_version.is_empty());
}

#[test]
fn template_catalog_selects_latest_owned_build_independent_of_cache_or_input_order() {
    let cache = tempfile::tempdir().unwrap();
    let hash = |digit: char| format!("sha256:{}", digit.to_string().repeat(64));
    let build = |name: &str, day: &str, digit: char| TemplateFullInfo {
        name: name.into(),
        built_at: Some(day.into()),
        content_hash: Some(hash(digit)),
        artifact_size_bytes: Some(42),
        ..Default::default()
    };
    let selected = cache.path().join("alpha").join(hash('b'));
    std::fs::create_dir_all(&selected).unwrap();
    std::fs::write(
        selected.join("metadata.json"),
        br#"{"description":"selected","built_at":"wrong","spaced_version":"v1"}"#,
    )
    .unwrap();
    let unowned = cache.path().join("unowned").join(hash('f'));
    std::fs::create_dir_all(&unowned).unwrap();
    std::fs::write(unowned.join("rootfs.ext4"), b"leftover").unwrap();
    let builds = vec![
        build("alpha", "2026-09-01", 'a'),
        build("alpha", "2026-09-02", 'b'),
        build("zeta", "2026-09-01", 'c'),
    ];
    let expected = project_template_catalog(builds.clone(), cache.path()).unwrap();
    let reversed =
        project_template_catalog(builds.into_iter().rev().collect(), cache.path()).unwrap();
    assert_eq!(expected, reversed);
    assert_eq!(expected.len(), 2);
    assert_eq!(expected[0].sha256, hash('b'));
    assert_eq!(expected[0].description, "selected");
    assert_eq!(expected[0].built_at, "2026-09-02");
    assert_eq!(expected[0].size_bytes, 42);
    assert_eq!(expected[0].spaced_version, "v1");
    // Owner-listed uncached builds remain visible without forcing a download.
    assert_eq!(expected[1].id, "zeta");
    assert!(expected[1].description.is_empty());
}

#[test]
fn template_catalog_rejects_invalid_owner_identity_before_reading_paths() {
    for (name, digest) in [
        ("../escape", Some(format!("sha256:{}", "a".repeat(64)))),
        ("valid", None),
        ("valid", Some("../escape".into())),
    ] {
        let build = TemplateFullInfo {
            name: name.into(),
            content_hash: digest,
            ..Default::default()
        };
        assert_eq!(
            project_template_catalog(vec![build], std::path::Path::new("/unused"))
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }
}

#[tokio::test]
async fn delete_removes_a_quarantine_whose_automatic_recovery_stopped() {
    if !enter_isolated_test_process() {
        return;
    }
    let conn = crate::db::test_conn();
    let sandbox_id = crate::db::test_fixtures::setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    let id = sandbox_id.to_string();
    for _ in 0..crate::db::QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND {
        crate::db::test_fixtures::record_quarantine_recovery_attempt(
            &conn,
            sandbox_id,
            crate::db::QuarantineRecoveryNoProgressReason::PlanIneligible,
        );
    }
    // Due by its retry time and still not offered: recovery stopped.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_after = NULL WHERE sandbox_id = ?1",
        crate::db::fixture::params![id],
    )
    .unwrap();
    assert!(
        crate::db::scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .is_empty()
    );
    let fixture = CleanupFixture::new(conn, 1);

    // Stopping automatic recovery hands the hold to an operator; this is the
    // operator's disposition.
    let deleted = CleanupRpc::Delete
        .call(&fixture.service, Some(root_peer()), &id, None, true)
        .await
        .unwrap();

    assert_eq!(deleted, vec![id.clone()]);
    assert_eq!(fixture.state(&id), None);
    assert_eq!(
        crate::db::current_quarantine_reason_wire_value(&fixture.service.db_conn().unwrap(), &id)
            .unwrap(),
        None
    );
    fixture.shutdown().await;
}
