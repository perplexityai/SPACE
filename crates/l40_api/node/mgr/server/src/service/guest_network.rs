//! One DHCP socket per firmware execution. No guest packet selects host state.
use super::*;
use pplx_space_net_common::slot_dhcp;

#[derive(Default)]
pub(super) struct GuestNetworks {
    entries: dashmap::DashMap<SandboxId, DhcpTask>,
}
struct DhcpTask {
    execution: pplx_space_common_core::types::SandboxActivationAttemptId,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for DhcpTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl NodeManagerService {
    /// Drain the responder before releasing its namespace to the slot pool.
    /// Recovery and periodic repair use the same execution gates as launch.
    pub(crate) fn start_guest_network_reconciler(&self) {
        let service = self.clone();
        self.device_op_tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    biased;
                    _ = service.pool.shutdown_token.cancelled() => break,
                    _ = interval.tick() => {},
                }
                let records = match service.pool.all_records() {
                    Ok(records) => records,
                    Err(error) => { tracing::warn!(%error, "guest network reconciliation inventory failed"); continue; }
                };
                for record in records {
                    let id = record.info.sandbox_id;
                    if id.is_empty() { continue; }
                    let result = async {
                        // Inspect durable intent before taking mutation locks. The inner
                        // admission repeats this check after obtaining the fence.
                        if !service.recorded_guest_profile(id.clone()).await?.is_some_and(|profile| profile.uses_dhcp()) { return Ok::<(), Status>(()); }
                        let Some(_claim) = service.pool.try_claim_sandbox_mutation(&id) else { return Ok(()); };
                        let Ok(_gate) = tokio::time::timeout(Duration::from_secs(2), service.lock_sandbox_cgroups(&id)).await else { return Ok(()); };
                        let Some(lease) = service.pool.get(&id).map_err(slot_store_status)? else { return Ok::<(), Status>(()); };
                        if !lease.pid.is_some_and(launch::is_pid_alive) { return Ok(()); }
                        let fence = service.pool.execution_fence(&id).map_err(slot_store_status)?.map(Into::into);
                        service.ensure_guest_dhcp(&id, fence.as_ref()).await?;
                        Ok(())
                    };
                    tokio::select! {
                        biased;
                        _ = service.pool.shutdown_token.cancelled() => return,
                        result = result => if let Err(error) = result { tracing::warn!(sandbox_id = %id, %error, "guest network reconciliation failed"); },
                    }
                }
            }
        });
    }

    /// Caller holds the sandbox mutation claim and cgroup gate.
    pub(super) async fn stop_guest_dhcp(&self, sandbox_id: &str) -> Result<(), Status> {
        let task = self.guest_networks.entries.remove(
            &sandbox_id
                .parse::<SandboxId>()
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
        );

        if let Some((_, mut task)) = task {
            task.task.abort();
            match (&mut task.task).await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(Status::internal(format!("guest DHCP task: {error}"))),
            }
        }
        Ok(())
    }

    /// Caller holds the execution mutation gate until socket admission completes.
    pub(super) async fn ensure_guest_dhcp(
        &self,
        sandbox_id: &str,
        fence: Option<&crate::proto::ExecutionFence>,
    ) -> Result<(), Status> {
        let execution = self
            .check_execution(sandbox_id, fence, false)?
            .ok_or_else(|| Status::failed_precondition("guest DHCP requires an execution fence"))?
            .current();
        let key = sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if !self
            .recorded_guest_profile(sandbox_id.to_owned())
            .await?
            .is_some_and(|profile| profile.uses_dhcp())
        {
            return Err(Status::failed_precondition(
                "execution does not require DHCP",
            ));
        }
        self.guest_networks
            .entries
            .remove_if(&key, |_, task| task.task.is_finished());
        let running = match self.guest_networks.entries.get(&key) {
            Some(task) if task.execution == execution => true,
            Some(_) => {
                return Err(Status::unavailable(
                    "previous guest network task is still releasing",
                ));
            }
            None => false,
        };
        let lease = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::failed_precondition("guest DHCP requires a network lease"))?;
        let ns = lease.ns_name();
        let expected_mac = NetId::from_slot_info(&lease).guest_mac_bytes();
        let socket = self
            .off_runtime("open guest DHCP socket", move || {
                crate::firewall::ensure_guest_dhcp_bootstrap(&ns).map_err(|e| {
                    Status::unavailable(format!("prepare guest DHCP firewall: {e}"))
                })?;
                if running {
                    return Ok(None);
                }
                net::guest_dhcp_socket(&ns)
                    .map(Some)
                    .map_err(|e| Status::unavailable(format!("open guest DHCP: {e}")))
            })
            .await?;
        self.check_execution(sandbox_id, fence, false)?;
        let Some(socket) = socket else {
            return Ok(());
        };
        let socket = tokio::net::UdpSocket::from_std(socket)
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let pool = self.pool.clone();
        let id = sandbox_id.to_owned();
        let task = self.device_op_tasks.spawn(async move {
            let run = async {
                let mut buffer = vec![0; slot_dhcp::MAX_PACKET + 1];
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                let mut budget = 1_u8;
                loop {
                    tokio::select! {
                        biased;
                        _ = pool.shutdown_token.cancelled() => return Ok::<(), Status>(()),
                        _ = interval.tick() => {
                            if pool.execution_fence(&id).map_err(slot_store_status)?.is_none_or(|f| f.current() != execution)
                                || pool.get(&id).map_err(slot_store_status)?.is_none() { return Ok(()); }
                            budget = budget.saturating_add(1).min(10);
                        }
                        packet = socket.recv_from(&mut buffer) => {
                            let (count, source) = packet.map_err(|e| Status::unavailable(e.to_string()))?;
                            // Drop excess work, oversized packets and non-client ports before parsing.
                            if budget == 0 || count > slot_dhcp::MAX_PACKET || source.port() != 68 { continue; }
                            budget -= 1;
                            // A client can renew an address from another link. Let the
                            // MAC-validated parser NAK it; replies stay on this namespace
                            // and are broadcast, never sent to the claimed source address.
                            let Some(packet) = buffer.get(..count) else { continue };
                            let Ok(Some(reply)) = slot_dhcp::response(packet, expected_mac) else { continue };
                            // Recheck the fence immediately before granting a lease. Ambiguity stops service.
                            if pool.execution_fence(&id).map_err(slot_store_status)?.is_none_or(|f| f.current() != execution)
                                || pool.get(&id).map_err(slot_store_status)?.is_none() { return Ok(()); }
                            socket.send_to(&reply, std::net::SocketAddr::from(([255,255,255,255], 68))).await
                                .map_err(|e| Status::unavailable(e.to_string()))?;
                        }
                    }
                }
            };
            if let Err(error) = run.await { tracing::warn!(sandbox_id = %id, %error, "guest DHCP responder stopped"); }
        });
        self.guest_networks
            .entries
            .insert(key, DhcpTask { execution, task });
        Ok(())
    }
}

#[cfg(test)]
mod tests;
