//! `Kill` RPC: forwards a signal to the sandbox process; SIGKILL and SIGTERM
//! also stop the sandbox through lifecycle cleanup.

use super::*;

impl SandboxManagerService {
    pub(in crate::service) async fn kill(
        &self,
        request: Request<KillRequest>,
    ) -> Result<Response<KillResponse>, Status> {
        self.authorize_peer(&request, "Kill", ALLOW_SANDBOX_OPERATOR)?;
        let _request_permit = if matches!(request.get_ref().signal, 9 | 15) {
            Some(self.cleanup_request_permit().await?)
        } else {
            None
        };
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let id = &sandbox.id;
        let signal = req.signal;
        let blocking = req.blocking;

        if signal == 0 {
            return Err(Status::invalid_argument("signal must be non-zero"));
        }

        // Must be running or paused to receive a signal
        if !matches!(sandbox.state, SandboxState::Started | SandboxState::Paused) {
            return Err(Status::failed_precondition(format!(
                "cannot kill: sandbox is {}, not started or paused",
                sandbox.state
            )));
        }

        // Only fatal signals (SIGKILL=9, SIGTERM=15) trigger state transition and cleanup.
        // Non-fatal signals (e.g. SIGHUP, SIGUSR1) are just forwarded to the process.
        let is_fatal = signal == 9 || signal == 15;
        if !is_fatal {
            let mut node_client = connect_node(&self.node_manager)
                .await
                .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;
            node_client
                .stop_process(StopProcessRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: id.clone(),
                    signal,
                    pid: None,
                })
                .await
                .map_err(|e| Status::internal(format!("send signal failed: {e}")))?;
            return Ok(Response::new(KillResponse {}));
        }

        sandbox
            .state
            .transition(SandboxState::Stopping)
            .map_err(Status::from)?;

        let (sandbox_id, uuid8) = Self::cleanup_identity(id, Some(&sandbox.uuid8))?;
        let completion = CleanupCompletion::MarkStopped;
        let admission = self
            .admit_lifecycle_cleanup_for_observed_execution(
                sandbox_id,
                SandboxState::Stopping,
                true,
                completion,
                !blocking,
                Some(sandbox.space_api_route_identity.execution_generation),
            )
            .await
            .map_err(super::cleanup::CleanupAdmissionError::into_status)?;

        let execution_fence = sandbox.node_execution_fence();
        let dispatch_error = match self
            .dispatch_node_action_using_admission(sandbox_id, &admission, || async move {
                let mut node_client = connect_node(&self.node_manager).await.map_err(|error| {
                    Status::internal(format!("cannot connect to node-manager: {error}"))
                })?;
                node_client
                    .stop_process(StopProcessRequest {
                        execution_fence,
                        sandbox_id: id.clone(),
                        signal,
                        pid: None,
                    })
                    .await
                    .map(|_| ())
                    .map_err(|error| Status::internal(format!("send fatal signal failed: {error}")))
            })
            .await
        {
            Ok(CleanupDispatchDisposition::Dispatched) => None,
            Ok(CleanupDispatchDisposition::Superseded) => {
                return Err(Status::aborted(format!(
                    "fatal kill for sandbox {id} was superseded by a replacement execution"
                )));
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %id,
                    %error,
                    "fatal signal dispatch failed after cleanup admission; continuing cleanup"
                );
                Some(error)
            }
        };

        // Cleanup: stop sidecars, release resources, keep filesystem
        self.watchdog.remove_sandbox(id).await;
        self.snapshot_scheduler.remove_sandbox(id).await;

        if admission.is_unpersisted_for_sandbox(sandbox_id)? {
            let persistence_error = self
                .run_cleanup_after_persistence_failure(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: uuid8.as_str(),
                        keep_filesystem: true,
                        context: "cleanup after unpersisted kill",
                        completion,
                        published_artifact: None,
                        immediate_kill: Self::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    admission,
                )
                .await?;
            if let Some(error) = persistence_error {
                return Err(Status::internal(format!(
                    "runtime cleanup completed but stopping transition could not be persisted for sandbox {id}: {error}"
                )));
            }
            if let Some(error) = dispatch_error {
                return Err(error);
            }
            return Ok(Response::new(KillResponse {}));
        }
        if blocking {
            if let Err(error) = self
                .run_admitted_cleanup_to_completion(
                    SynchronousCleanupRequest {
                        sandbox_id: id,
                        uuid8: uuid8.as_str(),
                        keep_filesystem: true,
                        context: "cleanup after kill",
                        completion,
                        published_artifact: None,
                        immediate_kill: Self::cleanup_immediate_kill(completion),
                        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    },
                    admission,
                )
                .await
            {
                self.persist_and_retry_cleanup_failure(
                    sandbox_id,
                    uuid8,
                    true,
                    CleanupCompletion::MarkStopped,
                    error.message(),
                );
                return Err(error);
            }
        } else {
            self.schedule_admitted_async_cleanup(
                AsyncCleanupRequest {
                    sandbox_id,
                    uuid8,
                    keep_filesystem: true,
                    context: "cleanup after kill",
                    completion,
                    immediate_kill: Self::cleanup_immediate_kill(completion),
                    guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
                    persist_intent: false,
                    cleanup_id: None,
                },
                admission,
            )
            .await;
        }

        if let Some(error) = dispatch_error {
            return Err(error);
        }
        Ok(Response::new(KillResponse {}))
    }
}
