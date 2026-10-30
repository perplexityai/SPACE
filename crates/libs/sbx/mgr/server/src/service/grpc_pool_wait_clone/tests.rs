use super::*;
use crate::service::activation_support::internal_request;
use crate::service::cleanup_support::{
    CleanupCompletion, NodeCleanupOptions, admit_prepared_clone_disposal_attempt,
    service_cleanup_via_node_with_options,
};
use crate::service::recovery::CleanupRecoveryAuthority;

fn armed_clone() -> (SandboxManagerService, SandboxId, SandboxCleanupEpisodeId) {
    let conn = crate::db::test_conn();
    let id = SandboxId::new();
    let cleanup_id = SandboxCleanupEpisodeId::new();
    crate::db::insert_sandbox_with_cleanup_episode_and_route_evidence(
        &conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &id.to_string(),
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(id),
            state: SandboxState::Creating,
            template: "seed",
            template_hash: Some("sha256:test"),
            user: &pplx_space_common_core::types::SandboxOwner::parse("deploy").unwrap(),
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
    (SandboxManagerService::for_test(conn), id, cleanup_id)
}

#[tokio::test]
async fn pool_clone_publication_and_disposal_are_exclusive() {
    for (outcome, rpc_code) in [
        ("success", tonic::Code::Ok),
        ("old_peer", tonic::Code::Unimplemented),
        ("connect_error", tonic::Code::Unavailable),
        ("partial_clone_error", tonic::Code::Internal),
        ("cancelled", tonic::Code::Cancelled),
        ("superseded", tonic::Code::Aborted),
    ]
    .into_iter()
    .chain((1..=16).map(|code| ("node_error", tonic::Code::from_i32(code))))
    {
        let (service, id, cleanup_id) = armed_clone();
        let id = id.to_string();
        let uuid8 = service.lookup_sandbox(&id).unwrap().uuid8;
        let clone_resources = async {
            match outcome {
                "success" => Ok(()),
                "old_peer" => Err(pool_clone_rpc_error(Status::unimplemented(
                    "node-peer-payload",
                ))),
                "connect_error" => Err(Status::unavailable("cannot connect to node-manager")),
                "cancelled" => std::future::pending().await,
                "superseded" => {
                    crate::db::request_cleanup_episode(
                        &service.db.lock().unwrap(),
                        &id,
                        cleanup_id,
                        SandboxQuarantineReasonCode::CreateInterrupted,
                        "superseded",
                    )
                    .unwrap();
                    Ok(())
                }
                _ => Err(pool_clone_rpc_error(Status::new(
                    rpc_code,
                    "node-peer-payload",
                ))),
            }
        };
        {
            let publication = service.publish_pool_clone(&id, &uuid8, cleanup_id, clone_resources);
            tokio::pin!(publication);
            if outcome == "cancelled" {
                assert!(futures_util::poll!(&mut publication).is_pending());
            } else if outcome == "success" {
                publication.await.unwrap();
            } else {
                let error = publication.await.unwrap_err();
                assert_eq!(
                    error.code(),
                    if outcome == "superseded" {
                        tonic::Code::Aborted
                    } else {
                        rpc_code
                    }
                );
            }
        }
        let db = service.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
        if outcome == "success" {
            assert_eq!(row.state, SandboxState::Suspended);
            assert!(row.cleanup_completion.is_none());
            assert!(crate::db::cleanup_episode(&db, &id).unwrap().is_none());
            assert_eq!(
                crate::db::request_cleanup_episode(
                    &db,
                    &id,
                    cleanup_id,
                    SandboxQuarantineReasonCode::CreateInterrupted,
                    "late failure"
                )
                .unwrap(),
                crate::db::CleanupEpisodeRequestOutcome::Superseded
            );
        } else {
            assert_eq!(row.state, SandboxState::CleanupRequired, "{outcome}");
            assert!(
                !row.failure_reason
                    .as_deref()
                    .unwrap_or("")
                    .contains("node-peer-payload")
            );
            assert_eq!(
                row.cleanup_completion,
                Some(CleanupCompletion::DeletePoolClone)
            );
            assert_eq!(row.cleanup_keep_filesystem, Some(false));
            assert!(
                crate::db::record_cleanup_episode_failure(&db, &id, cleanup_id, "node unavailable")
                    .unwrap()
            );
            assert!(
                crate::db::get_sandbox(&db, &id).unwrap().is_some(),
                "failed cleanup must retain its row"
            );
            assert!(!crate::db::complete_pool_clone(&db, &id, cleanup_id).unwrap());
            // This completion stands for the successful node cleanup acknowledgement.
            assert!(
                crate::db::complete_cleanup_episode(&db, &id, cleanup_id)
                    .unwrap()
                    .is_some()
            );
            assert!(crate::db::get_sandbox(&db, &id).unwrap().is_none());
        }
        assert!(
            crate::db::complete_cleanup_episode(&db, &id, cleanup_id)
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn pool_clone_restart_preserves_disposal_ownership() {
    let (service, id, cleanup_id) = armed_clone();
    let root = crate::db::PreparedCloneRoot {
        device: 7,
        inode: u64::MAX,
    };
    {
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::bind_prepared_clone_root(&db, &id.to_string(), cleanup_id, root).unwrap()
        );
    }
    let persisted = tempfile::tempdir().unwrap();
    let database = persisted.path().join("sandbox.sqlite");
    service
        .db
        .lock()
        .unwrap()
        .execute("VACUUM INTO ?1", [database.to_str().unwrap()])
        .unwrap();
    drop(service);
    let service =
        SandboxManagerService::for_test(crate::db::Connection::open_at(database).unwrap());
    let cache = tempfile::tempdir().unwrap();
    service
        .recover_prelaunch_startup_states_in(cache.path(), &Default::default())
        .unwrap();
    let targets = SandboxManagerService::recover_required_cleanup_targets(
        &service.db,
        CleanupRecoveryAuthority::Startup,
    )
    .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].cleanup_id, Some(cleanup_id));
    assert_eq!(targets[0].completion, CleanupCompletion::DeletePoolClone);
    assert!(!targets[0].keep_filesystem);
    let db = service.db.lock().unwrap();
    assert_eq!(
        crate::db::get_sandbox(&db, &id.to_string())
            .unwrap()
            .unwrap()
            .state,
        SandboxState::CleanupRequired
    );
    assert!(!crate::db::complete_pool_clone(&db, &id.to_string(), cleanup_id).unwrap());
    assert_eq!(
        crate::db::cleanup_episode(&db, &id.to_string())
            .unwrap()
            .unwrap()
            .prepared_clone_root,
        Some(root)
    );
    assert!(
        crate::db::quarantine_cleanup_episode(
            &db,
            &id.to_string(),
            cleanup_id,
            "prepared root was replaced"
        )
        .unwrap()
    );
    drop(db);
    assert!(
        SandboxManagerService::recover_required_cleanup_targets(
            &service.db,
            CleanupRecoveryAuthority::Startup
        )
        .unwrap()
        .is_empty()
    );
    let db = service.db.lock().unwrap();
    assert!(
        crate::db::list_requested_cleanup_recovery_candidate_ids(&db)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        crate::db::cleanup_episode(&db, &id.to_string())
            .unwrap()
            .unwrap()
            .prepared_clone_root,
        Some(root)
    );
    assert!(
        crate::db::complete_cleanup_episode(&db, &id.to_string(), cleanup_id)
            .unwrap()
            .is_none()
    );
    assert!(crate::db::rearm_prepared_clone_disposal(&db, &id.to_string(), cleanup_id).unwrap());
    assert_eq!(
        crate::db::cleanup_episode(&db, &id.to_string())
            .unwrap()
            .unwrap()
            .prepared_clone_root,
        Some(root)
    );

    // An independent quarantine can supersede publication while disposal is
    // still armed. Startup must retain that authority just like a requested hold.
    let (service, id, cleanup_id) = armed_clone();
    let db = service.db.lock().unwrap();
    assert!(crate::db::bind_prepared_clone_root(&db, &id.to_string(), cleanup_id, root).unwrap());
    assert!(
        crate::db::mark_quarantined_from_state(
            &db,
            &id.to_string(),
            SandboxState::Creating,
            pplx_space_common_core::types::SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::CreateInterrupted,
            "operator hold during publication",
            pplx_space_common_core::types::SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap()
    );
    assert!(
        !crate::db::retire_stale_armed_cleanup_episode(
            &db,
            &id.to_string(),
            cleanup_id,
            SandboxState::Quarantined,
        )
        .unwrap()
    );
    assert!(crate::db::rearm_prepared_clone_disposal(&db, &id.to_string(), cleanup_id).unwrap());
    let episode = crate::db::cleanup_episode(&db, &id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(episode.prepared_clone_root, Some(root));
    assert!(episode.requested_at.is_some());
}

#[tokio::test]
async fn pool_clone_requires_a_stable_donor_and_pins_its_template_revision() {
    let (service, id, _) = armed_clone();
    let mut row = service.lookup_sandbox(&id.to_string()).unwrap();
    row.state = SandboxState::Paused;
    for case in [
        "valid",
        "running",
        "encrypted",
        "missing_hash",
        "different_template",
    ] {
        let mut donor = row.clone();
        let template = if case == "different_template" {
            "other"
        } else {
            "seed"
        };
        match case {
            "running" => donor.state = SandboxState::Started,
            "encrypted" => donor.volume_encryption = true,
            "missing_hash" => donor.template_hash = None,
            _ => {}
        }
        let result = pool_clone_template_ref(&donor, template);
        if case == "valid" {
            let reference = result.unwrap();
            assert_eq!(reference.content_hash.as_deref(), Some("sha256:test"));
            assert!(reference.tag.is_none());
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
        }
    }
}

#[tokio::test]
async fn pool_clone_and_adoption_keep_donor_ownership_exclusive() {
    use pplx_space_common_core::host::linux::identities::users;

    let (service, id, cleanup_id) = armed_clone();
    let id = id.to_string();
    {
        let db = service.db.lock().unwrap();
        assert!(crate::db::complete_pool_clone(&db, &id, cleanup_id).unwrap());
        crate::db::test_fixtures::state_write_fixtures::force_state(&db, &id, SandboxState::Paused);
        crate::db::update_user(&db, &id, &TEMPLATE_MANAGER_SANDBOX_OWNER).unwrap();
        crate::db::set_pool_template(&db, &id, Some("seed")).unwrap();
    }
    let mut request = Request::new(());
    request.extensions_mut().insert(PeerIdentity::new(
        users::SPACE_TMPLMGR.uid,
        users::SPACE_TMPLMGR.primary_group.gid,
        None,
    ));
    let peer = service
        .authorize_peer(&request, "CloneFromDonor", ALLOW_SANDBOX_ADMIN_OR_TMPLMGR)
        .unwrap();
    let tenant = SandboxOwner::parse("deploy").unwrap();
    let env = BTreeMap::from([("TENANT_SECRET".to_string(), "private".to_string())]);

    // A clone already holding its donor prevents adoption from changing the row.
    let (_, clone_guard, clone_fence) = service.lock_pool_clone_donor(&peer, &id).unwrap();
    let claim = || service.claim_pool_donor(&id, "seed", None, None, &tenant, &env);
    assert_eq!(claim().err().unwrap().code(), tonic::Code::Unavailable);
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().user,
        TEMPLATE_MANAGER_SANDBOX_OWNER
    );
    drop(clone_guard);
    drop(clone_fence);

    // Adoption between the initial authorization and locked reload invalidates it.
    let initial = service.lookup_sandbox(&id).unwrap();
    service
        .authorize_template_manager_sandbox(&peer, &initial, "CloneFromDonor")
        .unwrap();
    let (donor, adoption_guard) = claim().unwrap();
    assert_eq!(
        service
            .lock_pool_clone_donor(&peer, &id)
            .err()
            .unwrap()
            .code(),
        tonic::Code::Unavailable
    );
    drop(adoption_guard);
    assert_eq!(
        service
            .lock_pool_clone_donor(&peer, &id)
            .err()
            .unwrap()
            .code(),
        tonic::Code::PermissionDenied
    );

    // A stale selection must not silently claim a different, unfenced donor.
    assert_eq!(claim().err().unwrap().code(), tonic::Code::Unavailable);
    assert_eq!(service.lookup_sandbox(&id).unwrap().process_env, env);
    {
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::restore_pool_donor_if_adopted(&db, id.parse().unwrap(), &donor).unwrap(),
            crate::db::PoolDonorRestoreDisposition::Restored
        );
        crate::db::update_process_env(&db, &id, &env).unwrap();
    }
    let (_, guard) = service
        .claim_pool_donor(&id, "seed", None, None, &tenant, &BTreeMap::new())
        .unwrap();
    assert!(service.lookup_sandbox(&id).unwrap().process_env.is_empty());
    drop(guard);
}

#[test]
fn clone_preparation_never_adopts_existing_trees() {
    let cache = tempfile::tempdir().unwrap();
    let root = prepare_clone_root(cache.path(), "target").unwrap();
    assert!(root.metadata().unwrap().is_dir());
    std::fs::write(cache.path().join("target/sentinel"), b"retained").unwrap();
    prepare_clone_root(cache.path(), "target").unwrap_err();
    assert_eq!(
        std::fs::read(cache.path().join("target/sentinel")).unwrap(),
        b"retained"
    );
}

#[derive(Clone)]
struct CleanupPeer {
    code: tonic::Code,
    capability: Option<bool>,
    methods: Arc<Mutex<Vec<String>>>,
}
impl tonic::server::NamedService for CleanupPeer {
    const NAME: &'static str = "space.node.v1.NodeManager";
}
impl<B> tower::Service<http::Request<B>> for CleanupPeer {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        self.methods
            .lock()
            .unwrap()
            .push(request.uri().path().to_owned());
        if let Some(supported) = self.capability {
            use prost::Message;
            let response = pplx_space_node_mgr_client::proto::CloneCapabilitiesResponse {
                prepared_clone_disposal: supported,
            }
            .encode_to_vec();
            let mut frame = vec![0];
            frame.extend_from_slice(&(response.len() as u32).to_be_bytes());
            frame.extend_from_slice(&response);
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            return std::future::ready(Ok(http::Response::builder()
                .header("content-type", "application/grpc")
                .body(tonic::body::Body::new(CapabilityBody(
                    std::collections::VecDeque::from([
                        http_body::Frame::data(bytes::Bytes::from(frame)),
                        http_body::Frame::trailers(trailers),
                    ]),
                )))
                .unwrap()));
        }
        std::future::ready(Ok(
            Status::new(self.code, "peer-controlled-payload").into_http()
        ))
    }
}

#[tokio::test]
async fn pool_clone_cleanup_never_falls_back_and_holds_unsafe_or_exhausted_disposal() {
    for (code, failures, held) in [
        (tonic::Code::Unimplemented, 0, true),
        (tonic::Code::FailedPrecondition, 0, true),
        (tonic::Code::Unavailable, 0, false),
        (tonic::Code::Unavailable, 7, true),
        (tonic::Code::FailedPrecondition, 8, true),
    ] {
        let (service, id, cleanup_id) = armed_clone();
        let id = id.to_string();
        {
            let db = service.db.lock().unwrap();
            crate::db::request_cleanup_episode(
                &db,
                &id,
                cleanup_id,
                SandboxQuarantineReasonCode::CreateInterrupted,
                "interrupted clone",
            )
            .unwrap();
            for _ in 0..failures {
                assert!(
                    crate::db::charge_prepared_clone_disposal_attempt(&db, &id, cleanup_id)
                        .unwrap()
                );
                crate::db::record_cleanup_episode_failure(&db, &id, cleanup_id, "unavailable")
                    .unwrap();
            }
        }
        let job_id = {
            let db = service.db.lock().unwrap();
            let tx = crate::db::Transaction::deferred(&db).unwrap();
            let sandbox_id = id.parse().unwrap();
            let job_id = crate::db::cleanup_jobs::persist(
                &tx,
                crate::db::cleanup_jobs::CleanupWork {
                    version: 1,
                    sandbox_id,
                    uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
                    execution_generation: None,
                    cleanup_id: Some(cleanup_id),
                    completion: CleanupCompletion::DeletePoolClone,
                    keep_filesystem: false,
                    immediate_kill: false,
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    published_artifact: None,
                },
            )
            .unwrap();
            assert!(pplx_space_common_jobs::db::mark_job_running(tx.raw(), &job_id).unwrap());
            tx.commit().unwrap();
            job_id
        };
        let methods = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = tokio_util::sync::CancellationToken::new();
        let shutdown = stop.clone();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(CleanupPeer {
                    code,
                    capability: None,
                    methods: methods.clone(),
                })
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                ),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = pplx_space_node_mgr_client::NodeManagerGrpcClient::new(
            pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
        );
        let result = match admit_prepared_clone_disposal_attempt(&service.db, &id, Some(&job_id)) {
            Err(error) => Err(error),
            Ok(()) => {
                service_cleanup_via_node_with_options(
                    &service.db,
                    &mut client,
                    &id,
                    NodeCleanupOptions {
                        job_id: Some(&job_id),
                        keep_filesystem: false,
                        return_after_slot_release: false,
                        immediate_kill: false,
                    },
                    "test clone disposal",
                )
                .await
            }
        };
        let error = result.unwrap_err();
        assert_eq!(error.code(), code);
        assert!(!error.message().contains("peer-controlled-payload"));
        assert_eq!(
            *methods.lock().unwrap(),
            if failures == 8 {
                Vec::<String>::new()
            } else {
                vec!["/space.node.v1.NodeManager/CleanupPreparedClone".to_owned()]
            }
        );
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state == SandboxState::Quarantined,
            held
        );
        assert!(
            crate::db::cleanup_episode(&service.db.lock().unwrap(), &id)
                .unwrap()
                .is_some()
        );
        {
            let db = service.db.lock().unwrap();
            let job = pplx_space_common_jobs::db::get_job(db.raw(), &job_id)
                .unwrap()
                .unwrap();
            assert_eq!(job.status, if held { "failed" } else { "running" });
            if held {
                assert!(
                    crate::db::cleanup_recovery_page(&db, None, 128)
                        .unwrap()
                        .ids
                        .is_empty(),
                    "operator-held clones must not be rescheduled by recovery"
                );
                assert!(crate::db::rearm_prepared_clone_disposal(&db, &id, cleanup_id).unwrap());
                assert_eq!(
                    crate::db::cleanup_recovery_page(&db, None, 128)
                        .unwrap()
                        .ids,
                    vec![id.clone()],
                    "explicit operator rearm must restore recovery eligibility"
                );
            }
        }
        stop.cancel();
        server.await.unwrap().unwrap();
    }
}

struct CapabilityBody(std::collections::VecDeque<http_body::Frame<bytes::Bytes>>);
impl http_body::Body for CapabilityBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Ready(self.0.pop_front().map(Ok))
    }
}

#[tokio::test]
async fn pool_clone_admission_precedes_all_durable_allocation() {
    for (capability, code, expected) in [
        (None, tonic::Code::Unimplemented, tonic::Code::Unimplemented),
        (Some(false), tonic::Code::Ok, tonic::Code::Unimplemented),
        (None, tonic::Code::Unavailable, tonic::Code::Unavailable),
        (Some(true), tonic::Code::Ok, tonic::Code::NotFound),
    ] {
        let service = SandboxManagerService::for_test(crate::db::test_conn());
        let methods = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = tokio_util::sync::CancellationToken::new();
        let shutdown = stop.clone();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(CleanupPeer {
                    code,
                    capability,
                    methods: methods.clone(),
                })
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                ),
        );
        for _ in 0..3 {
            let connect = async {
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                Ok(pplx_space_node_mgr_client::NodeManagerGrpcClient::new(
                    pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
                ))
            };
            // A supported peer reaches donor validation; all other cases must
            // stop earlier. Repeated refill attempts must create no local rows.
            let error = service
                .clone_from_donor_with_node(
                    internal_request(CloneFromDonorRequest {
                        donor_sandbox_id: SandboxId::new().to_string(),
                        template: "seed".into(),
                        owner: "deploy".into(),
                        pool_template: Some("seed".into()),
                    }),
                    connect,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), expected);
            assert!(!error.message().contains("peer-controlled-payload"));
            let db = service.db.lock().unwrap();
            assert!(crate::db::list_sandboxes(&db).unwrap().is_empty());
            assert_eq!(
                db.query_row("SELECT count(*) FROM sandbox_cleanup_episodes", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
                0
            );
        }
        assert_eq!(methods.lock().unwrap().len(), 3);
        assert!(
            methods
                .lock()
                .unwrap()
                .iter()
                .all(|path| path.ends_with("/GetCloneCapabilities"))
        );
        stop.cancel();
        server.await.unwrap().unwrap();
    }
}
