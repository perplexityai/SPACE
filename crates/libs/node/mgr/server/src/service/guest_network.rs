//! One DHCP socket per firmware execution. No guest packet selects host state.
use super::*;
use pplx_space_net_common::slot_dhcp;
use std::sync::Mutex;

#[derive(Default)]
pub(super) struct GuestNetworks {
    entries: Mutex<std::collections::HashMap<String, DhcpTask>>,
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
                        let _claim = service.pool.claim_sandbox_mutation(&id).await;
                        let _gate = service.lock_sandbox_cgroups(&id).await;
                        let Some(lease) = service.pool.get(&id).map_err(slot_store_status)? else { return Ok::<(), Status>(()); };
                        if !lease.pid.is_some_and(launch::is_pid_alive) { return Ok(()); }
                        let Some(execution) = service.recorded_execution(id.clone()).await? else { return Ok(()); };
                        if pplx_space_node_runtime_core::boot::BootSource::from_execution(&execution)
                            .map_err(|error| Status::failed_precondition(error.to_string()))?
                            == pplx_space_node_runtime_core::boot::BootSource::Uefi {
                            let fence = service.pool.execution_fence(&id).map_err(slot_store_status)?.map(Into::into);
                            service.ensure_guest_dhcp(&id, fence.as_ref()).await?;
                        }
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
        let task = self
            .guest_networks
            .entries
            .lock()
            .map_err(|_| Status::internal("guest network task map poisoned"))?
            .remove(sandbox_id);
        if let Some(mut task) = task {
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
        {
            let mut entries = self
                .guest_networks
                .entries
                .lock()
                .map_err(|_| Status::internal("guest network task map poisoned"))?;
            entries.retain(|_, task| !task.task.is_finished());
            if entries
                .get(sandbox_id)
                .is_some_and(|task| task.execution == execution)
            {
                return Ok(());
            }
            if entries.contains_key(sandbox_id) {
                return Err(Status::unavailable(
                    "previous guest network task is still releasing",
                ));
            }
        }
        let lease = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::failed_precondition("guest DHCP requires a network lease"))?;
        let ns = lease.ns_name();
        let mac = NetId::from_slot_info(&lease).guest_mac();
        let mut expected_mac = [0; 6];
        let parts: Vec<_> = mac.split(':').collect();
        if parts.len() != expected_mac.len() {
            return Err(Status::internal("invalid assigned MAC"));
        }
        for (byte, part) in expected_mac.iter_mut().zip(parts) {
            *byte = u8::from_str_radix(part, 16)
                .map_err(|_| Status::internal("invalid assigned MAC"))?;
        }
        let socket = self
            .off_runtime("open guest DHCP socket", move || {
                crate::firewall::ensure_guest_dhcp_bootstrap(&ns).map_err(|e| {
                    Status::unavailable(format!("prepare guest DHCP firewall: {e}"))
                })?;
                net::guest_dhcp_socket(&ns)
                    .map_err(|e| Status::unavailable(format!("open guest DHCP: {e}")))
            })
            .await?;
        self.check_execution(sandbox_id, fence, false)?;
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
                            if source.ip() != std::net::IpAddr::from([0, 0, 0, 0]) && source.ip() != std::net::IpAddr::from(slot_dhcp::GUEST) { continue; }
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
        let mut entries = self
            .guest_networks
            .entries
            .lock()
            .map_err(|_| Status::internal("guest network task map poisoned"))?;
        entries.insert(sandbox_id.to_owned(), DhcpTask { execution, task });
        Ok(())
    }
}
