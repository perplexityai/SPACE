use super::activation_support::{
    GuestActivityKind, configure_process_env_via_spaced_host, grpc_timeout_budget,
};
use super::create_support::sanitize_process_env;
use super::spaced_bridge::*;
use super::tail_support::*;
use super::*;

fn cpu_stat_usage_usec(content: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        line.strip_prefix("usage_usec ")
            .and_then(|value| value.trim().parse::<u64>().ok())
    })
}

fn parse_operation_user(user: Option<String>) -> Result<Option<GuestUserName>, Status> {
    user.map(GuestUserName::parse)
        .transpose()
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn required_operation<T: Clone>(operation: &Option<T>) -> Result<T, Status> {
    operation
        .clone()
        .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))
}

impl SandboxManagerService {
    pub(super) async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        self.authorize_peer(&request, "Stats", ALLOW_SANDBOX_OBSERVER)?;
        let sandbox = self.lookup_sandbox(&request.get_ref().sandbox_id)?;
        let id = &sandbox.id;
        let uid = sandbox
            .uid
            .ok_or_else(|| Status::failed_precondition("sandbox has no allocated uid"))?;
        let slot = sandbox
            .slot
            .ok_or_else(|| Status::failed_precondition("sandbox has no allocated network slot"))?;

        let mut node_client = connect_node()
            .await
            .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;

        let inspect = node_client
            .inspect_sandbox(pplx_space_node_mgr_client::proto::InspectSandboxRequest {
                sandbox_id: id.clone(),
                uuid8: sandbox.uuid8.to_string(),
                uid: Some(uid),
                slot: Some(slot),
                template: Some(sandbox.template.clone()),
                execution_only: false,
            })
            .await
            .map_err(|e| Status::internal(format!("inspect failed: {e}")))?
            .into_inner();

        // CPU time comes from the same jailer cgroup inspect reads `cpu_max` from.
        // Stats stay available without it, but a failed read is logged: a
        // silent zero is how a wrong cgroup path once went unnoticed.
        let cpu_stat =
            pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path(id).join("cpu.stat");
        let cpu_usage_usec = match tokio::fs::read_to_string(&cpu_stat).await {
            Ok(content) => cpu_stat_usage_usec(&content).unwrap_or_else(|| {
                tracing::warn!(sandbox_id = %id, path = %cpu_stat.display(), "cpu.stat has no usage_usec");
                0
            }),
            Err(error) => {
                tracing::warn!(sandbox_id = %id, path = %cpu_stat.display(), %error, "failed to read cpu.stat");
                0
            }
        };
        let health = self.watchdog.get_health(id).await;
        let network_tx_bytes = health
            .as_ref()
            .map(|health| health.network_tx_bytes)
            .unwrap_or(sandbox.network_tx_bytes);
        let network_rx_bytes = health
            .as_ref()
            .map(|health| health.network_rx_bytes)
            .unwrap_or(sandbox.network_rx_bytes);
        let probe_net_gateway =
            sandbox.netgtw_pid.is_some() || health.as_ref().is_some_and(|h| h.netgtw_alive);
        let net_gateway_stats = self
            .collect_net_gateway_stats(id, probe_net_gateway, network_tx_bytes, network_rx_bytes)
            .await;

        Ok(Response::new(StatsResponse {
            sandbox_id: id.clone(),
            cpu_max: inspect.cgroup_cpu_max,
            memory_max: inspect.cgroup_memory_max.trim().parse().unwrap_or(0),
            memory_current: inspect.cgroup_memory_current.trim().parse().unwrap_or(0),
            pids_max: inspect.cgroup_pids_max.trim().parse().unwrap_or(0),
            pids_current: inspect.cgroup_pids_current.trim().parse().unwrap_or(0),
            cpu_usage_usec,
            network_tx_bytes: net_gateway_stats.network_tx_bytes,
            network_rx_bytes: net_gateway_stats.network_rx_bytes,
            net_gateway_reachable: net_gateway_stats.reachable,
            tcp_active_connections: net_gateway_stats.tcp_active_connections,
            tcp_total_connections: net_gateway_stats.tcp_total_connections,
            tcp_denied_connections: net_gateway_stats.tcp_denied_connections,
            udp_active_mappings: net_gateway_stats.udp_active_mappings,
            bytes_forwarded: net_gateway_stats.bytes_forwarded,
        }))
    }

    pub(super) async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        let peer = self.authorize_peer(&request, "Update", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;

        if sandbox.state != SandboxState::Started && sandbox.state != SandboxState::Paused {
            return Err(Status::failed_precondition(format!(
                "cannot update: sandbox is {}, not started or paused",
                sandbox.state
            )));
        }

        let updates_owner = req.expected_current_owner.is_some() || req.owner.is_some();
        let updates_resources =
            req.vcpu.is_some() || req.memory_mib.is_some() || req.pids_max.is_some();
        let process_env = if req.replace_process_env {
            Some(sanitize_process_env(
                req.process_env.clone().into_iter().collect(),
                "sandbox_update",
            ))
        } else {
            if !req.process_env.is_empty() {
                return Err(Status::invalid_argument(
                    "replace_process_env must be true when process_env is provided",
                ));
            }
            None
        };
        let updates_process_env = process_env.is_some();
        if updates_resources && (updates_owner || updates_process_env) {
            return Err(Status::invalid_argument(
                "resource and metadata updates must be sent separately",
            ));
        }

        match (&req.expected_current_owner, &req.owner) {
            (Some(expected_current_owner), Some(owner)) => {
                let expected_current_owner = SandboxOwner::parse(expected_current_owner.clone())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let owner = Self::authorize_sandbox_owner_label(&peer, owner)?;
                let previous = {
                    let db = self.db_conn()?;
                    crate::db::set_sandbox_owner_if_current(
                        &db,
                        &sandbox.id,
                        &expected_current_owner,
                        &owner,
                    )
                    .map_err(|e| Status::internal(format!("db set_sandbox_owner: {e}")))?
                }
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "sandbox {} owner is not {}",
                        sandbox.id,
                        expected_current_owner.as_str()
                    ))
                })?;
                mark_sandbox_dirty(&self.db, &previous.id);
            }
            (None, None) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "expected_current_owner and owner must be provided together",
                ));
            }
        }

        if let Some(process_env) = process_env {
            let env_count = process_env.len();
            {
                let db = self.db_conn()?;
                crate::db::update_process_env(&db, &sandbox.id, &process_env)
                    .map_err(|e| Status::internal(format!("db update_process_env: {e}")))?;
            }
            mark_sandbox_dirty(&self.db, &sandbox.id);
            if sandbox.state == SandboxState::Started {
                configure_process_env_via_spaced_host(&sandbox.id, &process_env)
                    .await
                    .map_err(|error| {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            "failed to replace process env in running sandbox guest"
                        );
                        Status::internal(format!(
                            "sandbox process env was persisted, but live guest replacement failed: {error}"
                        ))
                    })?;
            }
            tracing::info!(
                sandbox_id = %sandbox.id,
                env_count,
                "updated sandbox process env"
            );
        }

        if updates_resources {
            let mut node_client = connect_node()
                .await
                .map_err(|e| Status::unavailable(format!("cannot connect to node-manager: {e}")))?;

            node_client
                .update_cgroup(pplx_space_node_mgr_client::proto::UpdateCgroupRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: sandbox.id.clone(),
                    vcpu_count: req.vcpu,
                    memory_mib: req.memory_mib,
                    pids_max: req.pids_max,
                })
                .await
                .map_err(|e| {
                    Status::new(e.code(), format!("update cgroup failed: {}", e.message()))
                })?;
        }

        Ok(Response::new(UpdateResponse {}))
    }

    pub(super) async fn spaced_ping_guest(
        &self,
        request: Request<SpacedPingGuestRequest>,
    ) -> Result<Response<SpacedPingGuestResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::PingGuest.as_str(),
            ALLOW_SANDBOX_OBSERVER,
        )?;
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::PingGuest)?;
        let sandbox = self.lookup_started_sandbox(&req.sandbox_id)?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client.ping_guest().await?;
        Ok(Response::new(SpacedPingGuestResponse {}))
    }

    pub(super) async fn spaced_get_status(
        &self,
        request: Request<SpacedGetStatusRequest>,
    ) -> Result<Response<SpacedGetStatusResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::GetStatus.as_str(),
            ALLOW_SANDBOX_OBSERVER,
        )?;
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::GetStatus)?;
        let sandbox = self.lookup_started_sandbox(&req.sandbox_id)?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let status = client.get_status().await?;
        Ok(Response::new(SpacedGetStatusResponse {
            ready: status.ready,
            guest_uptime: status.guest_uptime,
            host_version: spaced_version_info(status.host_version),
            guest_version: spaced_version_info(status.guest_version),
        }))
    }

    pub(super) async fn spaced_wait_ready(
        &self,
        request: Request<SpacedWaitReadyRequest>,
    ) -> Result<Response<SpacedWaitReadyResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            SpacedRpc::WaitReady.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let req = request.get_ref();
        let route_auth = self.prepare_classified_space_api_bridge(
            &request,
            &req.sandbox_id,
            SpacedRpc::WaitReady,
            &peer,
        )?;
        let sandbox = self.lookup_started_sandbox(&req.sandbox_id)?;
        if let Some(route_auth) = &route_auth {
            route_auth.authorize_sandbox(&sandbox)?;
        }
        let endpoint_proof_policy = if route_auth.is_none() {
            SpacedEndpointProofPolicy::AllowIdentityFreePoolDonor
        } else {
            SpacedEndpointProofPolicy::Required
        };
        let timeout = proto_duration_to_std(req.timeout)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_BLOCKING_READY_TIMEOUT_SECS as u64));
        let client = self
            .connect_spaced_host_with_policy(&sandbox, endpoint_proof_policy)
            .await?;
        let response = client.wait_ready(timeout).await?;
        Ok(Response::new(SpacedWaitReadyResponse {
            guest_uptime: response.guest_uptime,
        }))
    }

    pub(super) async fn spaced_stat(
        &self,
        request: Request<SpacedStatRequest>,
    ) -> Result<Response<SpacedStatResponse>, Status> {
        self.authorize_peer(&request, SpacedRpc::Stat.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::Stat)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedStat",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let response = client
            .stat_as_user(operation.path, parse_operation_user(operation.user)?)
            .await?;
        Ok(Response::new(stat_to_file_info(response)?))
    }

    pub(super) async fn spaced_list(
        &self,
        request: Request<SpacedListRequest>,
    ) -> Result<Response<BoxedStatusStream<SpacedFileInfo>>, Status> {
        self.authorize_peer(&request, SpacedRpc::List.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::List)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedList",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let identity_token = self.route_stream_identity_token(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let stream = client
            .list_stream_with_depth_as_user(
                operation.path,
                operation.depth,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(fence_status_stream(
            forward_host_stream(stream, Ok),
            identity_token,
        )))
    }

    pub(super) async fn spaced_read(
        &self,
        request: Request<SpacedReadRequest>,
    ) -> Result<Response<BoxedStatusStream<SpacedFileChunk>>, Status> {
        self.authorize_peer(&request, SpacedRpc::Read.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::Read)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedRead",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let identity_token = self.route_stream_identity_token(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let stream = client
            .read_stream_as_user(
                operation.path,
                operation.offset,
                operation.length,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(fence_status_stream(
            forward_host_stream(stream, |chunk| Ok(SpacedFileChunk { data: chunk.data })),
            identity_token,
        )))
    }

    pub(super) async fn spaced_write(
        &self,
        request: Request<tonic::Streaming<SpacedWriteRequest>>,
    ) -> Result<Response<SpacedWriteResponse>, Status> {
        self.authorize_peer(&request, SpacedRpc::Write.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let route_metadata = request.metadata().clone();
        let wake_budget = grpc_timeout_budget(&request);
        let mut request_stream = request.into_inner();
        let Some(first) = request_stream.message().await? else {
            return Err(Status::invalid_argument("write open message is required"));
        };
        let first_operation = required_operation(&first.operation)?;
        let route_auth = self.prepare_space_api_bridge_metadata(
            &route_metadata,
            &first.sandbox_id,
            SpacedRpc::Write,
        )?;
        let open = match first_operation.message {
            Some(spaced_write_request::Message::Open(open)) => open,
            Some(spaced_write_request::Message::Data(_)) => {
                return Err(Status::invalid_argument("write open message must be first"));
            }
            None => return Err(Status::invalid_argument("write message is required")),
        };

        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &first.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let identity_token = self.route_stream_identity_token(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let (err_tx, err_rx) = tokio::sync::oneshot::channel::<Status>();
        let sandbox_id = first.sandbox_id;
        let request_identity_token = identity_token.child_token();
        tokio::spawn(async move {
            let mut err_tx = Some(err_tx);
            let send_err = |slot: &mut Option<tokio::sync::oneshot::Sender<Status>>,
                            status: Status| {
                if let Some(sender) = slot.take() {
                    let _ = sender.send(status);
                }
            };
            loop {
                let message = tokio::select! {
                    message = request_stream.message() => message,
                    _ = request_identity_token.cancelled() => {
                        send_err(
                            &mut err_tx,
                            Status::aborted(
                                "sandbox execution identity changed while write was open",
                            ),
                        );
                        break;
                    }
                };
                match message {
                    Ok(Some(message)) if message.sandbox_id != sandbox_id => {
                        send_err(
                            &mut err_tx,
                            Status::invalid_argument("write stream changed sandbox target"),
                        );
                        break;
                    }
                    Ok(Some(SpacedWriteRequest {
                        operation:
                            Some(pplx_space_sbx_mgr_client::operations::WriteRequest {
                                message: Some(spaced_write_request::Message::Data(data)),
                            }),
                        ..
                    })) => {
                        if tx.send(data).await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(SpacedWriteRequest {
                        operation:
                            Some(pplx_space_sbx_mgr_client::operations::WriteRequest {
                                message: Some(spaced_write_request::Message::Open(_)),
                            }),
                        ..
                    })) => {
                        send_err(
                            &mut err_tx,
                            Status::invalid_argument("write open message must be first"),
                        );
                        break;
                    }
                    Ok(Some(SpacedWriteRequest {
                        operation:
                            Some(pplx_space_sbx_mgr_client::operations::WriteRequest { message: None }),
                        ..
                    }))
                    | Ok(Some(SpacedWriteRequest {
                        operation: None, ..
                    })) => {
                        send_err(
                            &mut err_tx,
                            Status::invalid_argument("write message is required"),
                        );
                        break;
                    }
                    Ok(None) => break,
                    Err(status) => {
                        send_err(&mut err_tx, status);
                        break;
                    }
                }
            }
        });
        // Parse the wire-shaped user into the typed wrapper at the proto
        // boundary, mirroring how `path` and `idempotency_key` are validated
        // before crossing into the host client (and the bridge below it).
        let user = parse_operation_user(open.user)?;
        let write = client.write_stream_with_idempotency(
            open.path,
            open.append,
            open.idempotency_key,
            user,
            tokio_stream::wrappers::ReceiverStream::new(rx),
        );
        let response = tokio::select! {
            response = write => response?,
            _ = identity_token.cancelled() => {
                return Err(Status::aborted(
                    "sandbox execution identity changed while write was open",
                ));
            }
        };
        if let Ok(status) = err_rx.await {
            return Err(Status::new(
                status.code(),
                format!(
                    "write request stream failed after guest committed {} bytes: {}",
                    response.bytes_written,
                    status.message()
                ),
            ));
        }
        Ok(Response::new(SpacedWriteResponse {
            bytes_written: response.bytes_written,
        }))
    }

    pub(super) async fn spaced_write_batch(
        &self,
        request: Request<SpacedWriteBatchRequest>,
    ) -> Result<Response<SpacedWriteBatchResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::WriteBatch.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::WriteBatch)?;
        let operation = required_operation(&req.operation)?;
        if operation.entries.len() > MAX_WRITE_BATCH_ENTRIES {
            return Err(Status::invalid_argument(format!(
                "entries has {} items, max is {MAX_WRITE_BATCH_ENTRIES}",
                operation.entries.len()
            )));
        }
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let entries = operation
            .entries
            .into_iter()
            .map(write_batch_entry_to_host)
            .collect();
        let response = client
            .write_batch_as_user(
                entries,
                operation.idempotency_key,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(SpacedWriteBatchResponse {
            files_written: response.files_written,
        }))
    }

    pub(super) async fn spaced_mkdir(
        &self,
        request: Request<SpacedMkdirRequest>,
    ) -> Result<Response<SpacedMkdirResponse>, Status> {
        self.authorize_peer(&request, SpacedRpc::Mkdir.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::Mkdir)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .mkdir(
                operation.path,
                operation.parents,
                operation.exist_ok,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(SpacedMkdirResponse {}))
    }

    pub(super) async fn spaced_remove(
        &self,
        request: Request<SpacedRemoveRequest>,
    ) -> Result<Response<SpacedRemoveResponse>, Status> {
        self.authorize_peer(&request, SpacedRpc::Remove.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::Remove)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .remove_as_user(
                operation.path,
                operation.recursive,
                operation.missing_ok,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(SpacedRemoveResponse {}))
    }

    pub(super) async fn spaced_rename(
        &self,
        request: Request<SpacedRenameRequest>,
    ) -> Result<Response<SpacedRenameResponse>, Status> {
        self.authorize_peer(&request, SpacedRpc::Rename.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::Rename)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .rename_as_user(
                operation.old_path,
                operation.new_path,
                operation.idempotency_key,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(SpacedRenameResponse {}))
    }

    pub(super) async fn spaced_watch(
        &self,
        request: Request<SpacedWatchRequest>,
    ) -> Result<Response<BoxedStatusStream<SpacedFileEvent>>, Status> {
        self.authorize_peer(&request, SpacedRpc::Watch.as_str(), ALLOW_SANDBOX_OPERATOR)?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::Watch)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedWatch",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let identity_token = self.route_stream_identity_token(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let stream = client
            .watch_stream_with_options(
                operation.path,
                pplx_space_spaced_host_client::WatchOptions {
                    recursive: operation.recursive,
                    include_metadata_events: operation.include_metadata_events,
                    include_globs: pplx_space_spaced_common::parse_globs(operation.include_globs)?,
                    exclude_globs: pplx_space_spaced_common::parse_globs(operation.exclude_globs)?,
                    use_default_excludes: operation.use_default_excludes,
                    user: parse_operation_user(operation.user)?,
                },
            )
            .await?;
        Ok(Response::new(fence_status_stream(
            forward_host_stream(stream, file_event_to_spaced),
            identity_token,
        )))
    }

    pub(super) async fn spaced_create_watcher(
        &self,
        request: Request<SpacedCreateWatcherRequest>,
    ) -> Result<Response<SpacedCreateWatcherResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::CreateWatcher.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::CreateWatcher)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let response = client
            .create_watcher_with_options(
                operation.root,
                pplx_space_spaced_host_client::WatchOptions {
                    recursive: operation.recursive,
                    include_metadata_events: operation.include_metadata_events,
                    include_globs: pplx_space_spaced_common::parse_globs(operation.include_globs)?,
                    exclude_globs: pplx_space_spaced_common::parse_globs(operation.exclude_globs)?,
                    use_default_excludes: operation.use_default_excludes,
                    user: parse_operation_user(operation.user)?,
                },
            )
            .await?;
        Ok(Response::new(SpacedCreateWatcherResponse {
            watcher_id: response.watcher_id,
            root: response.root,
        }))
    }

    pub(super) async fn spaced_normalize_watch_root(
        &self,
        request: Request<SpacedNormalizeWatchRootRequest>,
    ) -> Result<Response<SpacedNormalizeWatchRootResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::NormalizeWatchRoot.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth = self.prepare_space_api_bridge(
            &request,
            &req.sandbox_id,
            SpacedRpc::NormalizeWatchRoot,
        )?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedNormalizeWatchRoot",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let response = client
            .normalize_watch_root_as_user(operation.path, parse_operation_user(operation.user)?)
            .await?;
        Ok(Response::new(SpacedNormalizeWatchRootResponse {
            root: response.root,
        }))
    }

    pub(super) async fn spaced_get_watcher_events(
        &self,
        request: Request<SpacedGetWatcherEventsRequest>,
    ) -> Result<Response<SpacedGetWatcherEventsResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::GetWatcherEvents.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::GetWatcherEvents)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedGetWatcherEvents",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let response = client.get_watcher_events(operation.watcher_id).await?;
        Ok(Response::new(SpacedGetWatcherEventsResponse {
            events: response
                .events
                .into_iter()
                .map(file_event_to_spaced)
                .collect::<Result<_, Status>>()?,
        }))
    }

    pub(super) async fn spaced_remove_watcher(
        &self,
        request: Request<SpacedRemoveWatcherRequest>,
    ) -> Result<Response<SpacedRemoveWatcherResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::RemoveWatcher.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::RemoveWatcher)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client.remove_watcher(operation.watcher_id).await?;
        Ok(Response::new(SpacedRemoveWatcherResponse {}))
    }

    pub(super) async fn spaced_enable_file_changes(
        &self,
        request: Request<SpacedEnableFileChangesRequest>,
    ) -> Result<Response<SpacedEnableFileChangesResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::EnableFileChanges.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::EnableFileChanges)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let response = client
            .enable_file_changes_with_options_as_user(
                operation.roots,
                pplx_space_spaced_common::parse_globs(operation.include_globs)?,
                pplx_space_spaced_common::parse_globs(operation.exclude_globs)?,
                operation.use_default_excludes,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(SpacedEnableFileChangesResponse {
            observed_since: response.observed_since,
        }))
    }

    pub(super) async fn spaced_disable_file_changes(
        &self,
        request: Request<SpacedDisableFileChangesRequest>,
    ) -> Result<Response<SpacedDisableFileChangesResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::DisableFileChanges.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth = self.prepare_space_api_bridge(
            &request,
            &req.sandbox_id,
            SpacedRpc::DisableFileChanges,
        )?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .disable_file_changes_as_user(parse_operation_user(operation.user)?)
            .await?;
        Ok(Response::new(SpacedDisableFileChangesResponse {}))
    }

    pub(super) async fn spaced_get_modified_files(
        &self,
        request: Request<SpacedGetModifiedFilesRequest>,
    ) -> Result<Response<SpacedGetModifiedFilesResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::GetModifiedFiles.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::GetModifiedFiles)?;
        let operation = required_operation(&req.operation)?;
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedGetModifiedFiles",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let response = client
            .get_modified_files_as_user(
                operation.window,
                operation.since,
                operation.cursor,
                parse_operation_user(operation.user)?,
            )
            .await?;
        Ok(Response::new(SpacedGetModifiedFilesResponse {
            files: response
                .files
                .into_iter()
                .map(|file| {
                    Ok(SpacedModifiedFile {
                        path: file.path,
                        root: file.root,
                        last_modified_at: file.last_modified_at,
                        kinds: file
                            .kinds
                            .into_iter()
                            .map(file_event_kind_to_spaced)
                            .collect::<Result<_, Status>>()?,
                    })
                })
                .collect::<Result<_, Status>>()?,
            observed_since: response.observed_since,
            retained_since: response.retained_since,
            cursor: response.cursor,
            monitoring_enabled: response.monitoring_enabled,
            overflowed: response.overflowed,
        }))
    }
}

#[cfg(test)]
mod tests;
