//! Stop and delete when the lifecycle claim cannot be persisted.
//!
//! An ordinary stop or delete whose admission cannot write the `stopping` or
//! `deleting` claim still cleans the runtime up, through node-manager directly
//! rather than the durable cleanup executor, and then reports the persistence
//! failure. A SQLite trigger refuses the claim write here, and an in-process
//! node-manager answers the cleanup. Stops pass `immediate_kill`, which skips
//! the guest filesystem flush: that flush dials the guest's fixed spaced-host
//! socket.

use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::tests::service_tests::insert_test_sandbox;
use pplx_space_common_svc::PeerIdentity;
use pplx_space_node_mgr_client::proto::{CleanupSandboxRequest, CleanupSandboxResponse};

const CLAIM_REFUSAL: &str = "scripted lifecycle claim refusal";

/// Refuse every write of a sandbox's `stopping` or `deleting` claim.
fn refuse_lifecycle_claims(conn: &crate::db::Connection) {
    conn.execute_batch(&format!(
        "CREATE TEMP TRIGGER unpersisted_cleanup_claim_refusal
         BEFORE UPDATE OF state ON main.sandboxes
         WHEN NEW.state IN ('{}', '{}')
         BEGIN
             SELECT RAISE(ABORT, '{CLAIM_REFUSAL}');
         END;",
        SandboxState::Stopping.as_str(),
        SandboxState::Deleting.as_str(),
    ))
    .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Rpc {
    Stop,
    Delete,
}

impl Rpc {
    async fn call(self, service: &SandboxManagerService, id: &str, blocking: bool) -> Status {
        let peer = PeerIdentity::new(0, 0, None);
        let call = async {
            match self {
                Self::Stop => {
                    let mut request = Request::new(StopRequest {
                        sandbox_id: id.to_owned(),
                        template_filter: None,
                        blocking,
                        immediate_kill: true,
                    });
                    request.extensions_mut().insert(peer);
                    service.stop(request).await.map(|_| ())
                }
                Self::Delete => {
                    let mut request = Request::new(DeleteRequest {
                        sandbox_id: id.to_owned(),
                        template_filter: None,
                        blocking,
                    });
                    request.extensions_mut().insert(peer);
                    service.delete(request).await.map(|_| ())
                }
            }
        };
        // Bounds a regression that waits on cleanup instead of failing.
        tokio::time::timeout(Duration::from_secs(30), call)
            .await
            .unwrap_or_else(|_| panic!("{self:?} waited longer than 30 s"))
            .expect_err("the lifecycle claim was refused")
    }

    /// The node cleanup an unpersisted cleanup of `id` asks for.
    fn node_cleanup(
        self,
        id: &str,
        execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    ) -> CleanupSandboxRequest {
        let stop = matches!(self, Self::Stop);
        CleanupSandboxRequest {
            execution_fence,
            sandbox_id: id.to_owned(),
            force: true,
            // A stop keeps the filesystem and returns once the runtime's slot
            // is released; a delete removes it.
            keep_filesystem: stop,
            return_after_slot_release: stop,
            immediate_kill: stop,
        }
    }

    fn transition(self) -> &'static str {
        match self {
            Self::Stop => "stopping",
            Self::Delete => "deleting",
        }
    }

    fn context(self) -> &'static str {
        match self {
            Self::Stop => "cleanup after unpersisted stop",
            Self::Delete => "cleanup after unpersisted delete",
        }
    }
}

#[tokio::test]
async fn unpersisted_stop_and_delete_clean_up_through_node_manager_and_report_the_claim_failure() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    for rpc in [Rpc::Stop, Rpc::Delete] {
        for blocking in [false, true] {
            for node_refuses in [false, true] {
                let context = format!("{rpc:?} blocking={blocking} node_refuses={node_refuses}");
                let conn = crate::db::test_conn();
                let id = SandboxId::new().to_string();
                insert_test_sandbox(&conn, &id, SandboxState::Started);
                let execution_fence = crate::db::node_execution_fence(&conn, &id)
                    .unwrap()
                    .map(Into::into);
                refuse_lifecycle_claims(&conn);
                let cleanups = Arc::new(Mutex::new(Vec::new()));
                let node_manager = FakeNodeManager::default()
                    .unary("CleanupSandbox", {
                        let cleanups = Arc::clone(&cleanups);
                        move |request: CleanupSandboxRequest| {
                            cleanups.lock().unwrap().push(request);
                            if node_refuses {
                                Err(Status::unavailable("scripted cleanup refusal"))
                            } else {
                                Ok(CleanupSandboxResponse::default())
                            }
                        }
                    })
                    .serve();
                let run_dir = tempfile::tempdir().unwrap();
                // Stop selects only a live runtime: link its `fc.pid`.
                std::fs::create_dir(run_dir.path().join(&id)).unwrap();
                std::os::unix::fs::symlink(run_dir.path(), run_dir.path().join(&id).join("fc.pid"))
                    .unwrap();
                let service = SandboxManagerService::new_with_options(
                    conn,
                    SandboxManagerServiceOptions {
                        node_manager: node_manager.connector(),
                        ..SandboxManagerServiceOptions::for_test_in(run_dir.path())
                    },
                );

                let error = rpc.call(&service, &id, blocking).await;

                if node_refuses {
                    assert_eq!(error.code(), tonic::Code::Unavailable, "{context}: {error}");
                    assert_eq!(
                        error.message(),
                        format!("{}: scripted cleanup refusal", rpc.context()),
                        "{context}"
                    );
                } else {
                    assert_eq!(error.code(), tonic::Code::Internal, "{context}: {error}");
                    let message = error.message();
                    assert!(
                        message.starts_with(&format!(
                            "runtime cleanup completed but {} transition could not be persisted for sandbox {id}: claim cleanup lifecycle failed: ",
                            rpc.transition()
                        )) && message.contains(CLAIM_REFUSAL),
                        "{context}: {error}"
                    );
                }
                assert_eq!(
                    *cleanups.lock().unwrap(),
                    vec![rpc.node_cleanup(&id, execution_fence)],
                    "{context}"
                );
                assert_eq!(
                    node_manager.calls(),
                    vec!["CleanupSandbox".to_owned()],
                    "{context}"
                );
                let db = service.db_conn().unwrap();
                let row = crate::db::get_sandbox(&db, &id).unwrap().unwrap();
                assert_eq!(row.state, SandboxState::Started, "{context}");
                assert_eq!(row.cleanup_completion, None, "{context}");
                assert!(
                    crate::db::cleanup_episode(&db, &id).unwrap().is_none(),
                    "{context}"
                );
                assert!(
                    pplx_space_common_jobs::db::find_active_job(
                        db.raw(),
                        crate::db::cleanup_jobs::KIND,
                        &id
                    )
                    .unwrap()
                    .is_none(),
                    "{context}"
                );
                assert!(
                    crate::db::list_node_dirty_refs(&db, 100)
                        .unwrap()
                        .is_empty(),
                    "{context}"
                );
            }
        }
    }
}
