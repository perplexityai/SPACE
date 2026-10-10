//! `SetAutoLifecyclePolicy` and `TouchAutoLifecycle` RPCs and their wire
//! conversion helpers.

use super::*;

fn touch_auto_lifecycle_source(value: i32) -> Result<AutoLifecycleActivitySource, Status> {
    match crate::proto::AutoLifecycleActivitySource::try_from(value)
        .unwrap_or(crate::proto::AutoLifecycleActivitySource::Unspecified)
    {
        crate::proto::AutoLifecycleActivitySource::Access => {
            Ok(AutoLifecycleActivitySource::Access)
        }
        crate::proto::AutoLifecycleActivitySource::Command => {
            Ok(AutoLifecycleActivitySource::Command)
        }
        crate::proto::AutoLifecycleActivitySource::Unspecified => {
            Err(Status::invalid_argument("source is required"))
        }
    }
}

fn touch_auto_lifecycle_observed_at_secs(
    observed_at: Option<&prost_types::Timestamp>,
) -> Result<u64, Status> {
    let Some(observed_at) = observed_at else {
        return Ok(now_secs());
    };
    if observed_at.seconds < 0 || observed_at.nanos < 0 || observed_at.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument(
            "observed_at must be a valid non-negative timestamp",
        ));
    }
    Ok(observed_at.seconds.cast_unsigned())
}

fn touch_auto_lifecycle_result_value(result: crate::proto::AutoLifecycleTouchResult) -> i32 {
    result as i32
}

impl SandboxManagerService {
    pub(in crate::service) async fn set_auto_lifecycle_policy(
        &self,
        request: Request<SetAutoLifecyclePolicyRequest>,
    ) -> Result<Response<SetAutoLifecyclePolicyResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            "SetAutoLifecyclePolicy",
            ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR,
        )?;
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        let sandbox_id = req.sandbox_id.clone();
        let requested_authority = crate::db::PolicyAuthority::try_from(req.authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        // `authority=local` auto-lifecycle is a node-local, cluster-immune
        // override (the cluster-over-local branch below is a no-op), so a
        // forwarded remote caller must not set it -- only an on-node operator
        // via the CLI. node-api and node-ctl only ever push cluster here.
        require_local_authority_peer(peer.principal(), requested_authority)?;
        let policy = req
            .policy
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        let policy = policy_from_proto(policy)?;
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        if requested_authority == crate::db::PolicyAuthority::Cluster
            && sandbox.auto_lifecycle_authority == crate::db::PolicyAuthority::Local
        {
            return Ok(Response::new(SetAutoLifecyclePolicyResponse {
                effective_policy: Some(policy_to_proto(&sandbox.auto_lifecycle)),
                authority: sandbox.auto_lifecycle_authority.as_proto_i32(),
            }));
        }
        let effective_policy = self
            .set_auto_lifecycle_policy_exact(&req.sandbox_id, policy, false, requested_authority)
            .await?;
        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox_id);
        }
        Ok(Response::new(SetAutoLifecyclePolicyResponse {
            effective_policy: Some(policy_to_proto(&effective_policy)),
            authority: requested_authority.as_proto_i32(),
        }))
    }

    pub(in crate::service) async fn touch_auto_lifecycle(
        &self,
        request: Request<TouchAutoLifecycleRequest>,
    ) -> Result<Response<TouchAutoLifecycleResponse>, Status> {
        self.authorize_peer(
            &request,
            "TouchAutoLifecycle",
            ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR,
        )?;
        let retry_context = pplx_space_common_grpc::GrpcRetryContext::none()
            .with_grpc_timeout_metadata(request.metadata());
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        let source = touch_auto_lifecycle_source(req.source)?;
        let observed_at = touch_auto_lifecycle_observed_at_secs(req.observed_at.as_ref())?;

        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        if retry_context
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Err(Status::deadline_exceeded(
                "access activity request expired while waiting for lifecycle lock",
            ));
        }
        if let Some(update) = &req.access_idle_update {
            if source != AutoLifecycleActivitySource::Access {
                return Err(Status::invalid_argument(
                    "deadline updates require access source",
                ));
            }
            let expires = update
                .expires_at
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("deadline update requires expires_at"))?;
            let expires = touch_auto_lifecycle_observed_at_secs(Some(expires))?;
            if now_secs() >= expires {
                return Err(Status::deadline_exceeded("access deadline update expired"));
            }
            if sandbox
                .space_api_route_identity
                .execution_generation
                .map(|g| g.get())
                != Some(update.execution_generation)
            {
                return Err(Status::failed_precondition(
                    "access deadline execution changed",
                ));
            }
            if sandbox.state != SandboxState::Started {
                return Err(Status::failed_precondition(
                    "access deadline requires started execution",
                ));
            }
            let mut runtime = sandbox.auto_lifecycle_state.clone();
            let policy = match update
                .operation
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("deadline operation is required"))?
            {
                crate::proto::access_idle_update::Operation::ExtendUntil(until) => {
                    let minimum = touch_auto_lifecycle_observed_at_secs(Some(until))?;
                    apply_observed_access_touch(&mut runtime, &sandbox.auto_lifecycle, observed_at);
                    crate::auto_lifecycle::extend_access_deadline(
                        &mut runtime,
                        &sandbox.auto_lifecycle,
                        minimum,
                    )?;
                    sandbox.auto_lifecycle.clone()
                }
                crate::proto::access_idle_update::Operation::ResetPolicy(policy) => {
                    if sandbox.auto_lifecycle_authority == crate::db::PolicyAuthority::Local {
                        return Err(Status::failed_precondition(
                            "local lifecycle policy overrides cluster timeout",
                        ));
                    }
                    let policy = policy_from_proto(policy)?;
                    runtime = initial_runtime_state(&policy, now_secs());
                    policy
                }
            };
            let changed =
                runtime != sandbox.auto_lifecycle_state || policy != sandbox.auto_lifecycle;
            if changed {
                let db = self.db_conn()?;
                crate::db::update_auto_lifecycle_with_state(
                    &db,
                    &sandbox.id,
                    &policy,
                    &runtime,
                    sandbox.auto_lifecycle_authority,
                )
                .map_err(|e| Status::internal(format!("persist access deadline: {e}")))?;
                drop(db);
                if publish_node_state {
                    mark_sandbox_dirty(&self.db, &sandbox.id);
                }
            }
            let mut response = Response::new(TouchAutoLifecycleResponse {
                result: if changed {
                    crate::proto::AutoLifecycleTouchResult::Applied as i32
                } else {
                    crate::proto::AutoLifecycleTouchResult::AlreadyCurrent as i32
                },
            });
            response.metadata_mut().insert(
                pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA,
                tonic::metadata::MetadataValue::from_static("1"),
            );
            return Ok(response);
        }
        let mut runtime_state = sandbox.auto_lifecycle_state.clone();
        let mut touched = false;
        let result = if sandbox.state != SandboxState::Started {
            recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);
            crate::proto::AutoLifecycleTouchResult::NotStarted
        } else if sandbox.auto_lifecycle.is_disabled() {
            recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);
            crate::proto::AutoLifecycleTouchResult::Disabled
        } else {
            let initialized_runtime = runtime_state.conditions.is_empty();
            if initialized_runtime {
                let floor = runtime_state.access_extended_until;
                runtime_state = initial_runtime_state(&sandbox.auto_lifecycle, observed_at);
                runtime_state.access_extended_until = floor;
            }
            match source {
                AutoLifecycleActivitySource::Access => {
                    touched = apply_observed_access_touch(
                        &mut runtime_state,
                        &sandbox.auto_lifecycle,
                        observed_at,
                    );
                }
                AutoLifecycleActivitySource::Command => {
                    touched = apply_observed_command_and_access_touch(
                        &mut runtime_state,
                        &sandbox.auto_lifecycle,
                        observed_at,
                    );
                }
            }
            touched |= initialized_runtime && !runtime_state.conditions.is_empty();
            if touched {
                disarm_guest_activity_on_external_access(
                    &mut runtime_state,
                    &sandbox.auto_lifecycle,
                    observed_at,
                );
                clear_error(&mut runtime_state, &sandbox.auto_lifecycle);
                let db = self.db_conn()?;
                crate::db::update_auto_lifecycle_state(&db, &sandbox.id, &runtime_state).map_err(
                    |e| Status::internal(format!("db auto_lifecycle_state update failed: {e}")),
                )?;
                crate::proto::AutoLifecycleTouchResult::Applied
            } else {
                recompute_runtime_state(&mut runtime_state, &sandbox.auto_lifecycle);
                crate::proto::AutoLifecycleTouchResult::AlreadyCurrent
            }
        };

        if touched && publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox.id);
        }
        Ok(Response::new(TouchAutoLifecycleResponse {
            result: touch_auto_lifecycle_result_value(result),
        }))
    }
}
