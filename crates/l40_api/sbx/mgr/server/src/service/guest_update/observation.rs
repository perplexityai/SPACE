//! One pinned metadata subscription per resident execution. Neither discovery
//! nor reconnect touches activity, readiness or wake APIs. Ordinary guests that
//! predate this contract retain their existing startup behavior.
use super::*;

impl SandboxManagerService {
    pub fn start_guest_configuration_observation(
        &self,
        shutdown: CancellationToken,
        tracker: &TaskTracker,
    ) {
        let service = self.clone();
        tracker.spawn(async move {
            let mut observers = std::collections::HashMap::<String, (String, CancellationToken)>::new();
            let tasks = TaskTracker::new();
            loop {
                let rows = service.db_conn().and_then(|db| crate::db::scan::all_sandboxes(&db, crate::db::scan::GuestHealthRows).map_err(internal));
                if let Ok(rows) = rows {
                    let running: std::collections::HashSet<_> = rows.into_iter().filter(|r| r.state == SandboxState::Started).map(|r| r.id).collect();
                    observers.retain(|id, (_, token)| { if running.contains(id) && !token.is_cancelled() { true } else { token.cancel(); false } });
                    for id in running {
                        let Ok(row) = service.lookup_sandbox(&id) else { continue; };
                        let Some(execution) = row.node_execution_id else { continue; };
                        if row.pool_template.is_some() || matches!(row.user.kind(), SandboxOwnerKind::Builder | SandboxOwnerKind::TemplateManager) { continue; }
                        if observers.get(&id).is_some_and(|(old, _)| old == &execution.to_string()) { continue; }
                        if let Some((_, old)) = observers.remove(&id) { old.cancel(); }
                        let cancel = shutdown.child_token();
                        observers.insert(id.clone(), (execution.to_string(), cancel.clone()));
                        let service = service.clone();
                        tasks.spawn(async move {
                            tokio::select! {
                                _ = cancel.cancelled() => {},
                                _ = service.observe_guest_configuration(&id, execution) => {},
                            }
                            cancel.cancel();
                        });
                    }
                }
                tokio::select! { _ = shutdown.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
            }
            for (_, (_, token)) in observers { token.cancel(); }
            tasks.close(); tasks.wait().await;
        });
    }

    async fn observe_guest_configuration(
        &self,
        id: &str,
        execution: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) {
        let mut delay = Duration::from_millis(100);
        loop {
            let row = match self.lookup_sandbox(id) {
                Ok(row) if row.node_execution_id == Some(execution) => row,
                _ => return,
            };
            if row.state != SandboxState::Started {
                return;
            }
            let result = async {
                let mut client = self.connect_spaced_host(&row).await?.guest_update();
                let mut stream = tokio::time::timeout(RPC_BUDGET, client.subscribe(wire::StatusRequest {})).await
                    .map_err(|_| Status::deadline_exceeded("metadata subscription connect deadline"))??.into_inner();
                let mut acknowledged: Option<(String, String)> = None;
                let mut resync = tokio::time::interval(Duration::from_secs(5));
                loop {
                    let seen = tokio::select! {
                        value = stream.message() => value?.ok_or_else(|| Status::unavailable("metadata stream closed"))?,
                        _ = resync.tick() => client.get_status(request(wire::StatusRequest {})).await?.into_inner(),
                    };
                    let current = self.lookup_sandbox(id)?;
                    if current.state != SandboxState::Started || current.node_execution_id != Some(execution) { return Ok::<_, Status>(()); }
                    let generation = (seen.boot_id.clone(), seen.incarnation.clone());
                    if acknowledged.as_ref() == Some(&generation) && !seen.customer_admission_held { continue; }
                    if crate::db::read::active_guest_update(&*self.db_conn()?, id).map_err(internal)?.is_some() { continue; }
                    if !matches!(phase(&seen)?, wire::Phase::Stable | wire::Phase::Committed | wire::Phase::RolledBack | wire::Phase::Restarted | wire::Phase::Staged | wire::Phase::RecoveryRequired) { continue; }
                    let CleanupLockTryResult::Acquired(_cleanup) = self.try_cleanup_fence(id) else { continue; };
                    let Ok(_wake) = self.auto_lifecycle.wake_lock(id).try_lock_owned() else { continue; };
                    let Ok(_artifact) = self.auto_lifecycle.artifact_lock(id).try_lock_owned() else { continue; };
                    let current = self.lookup_sandbox(id)?;
                    if current.state != SandboxState::Started || current.node_execution_id != Some(execution) { return Ok(()); }
                    if crate::db::read::active_guest_update(&*self.db_conn()?, id).map_err(internal)?.is_some() { continue; }
                    // Stable identity across manager restarts lets the next
                    // observer recover an acknowledgement interrupted by one.
                    let owner = wire::HoldRequest { operation_id: execution.to_string(), execution_id: execution.to_string() };
                    client.hold(request(owner.clone())).await?;
                    client.configure(request(wire::ConfigureRequest { attempt: None, owner: Some(owner.clone()),
                        environment: current.process_env.into_iter().collect(), forward_ports: seen.forward_ports,
                        expected_boot_id: seen.boot_id, expected_incarnation: seen.incarnation.clone(), configuration_revision: 1 })).await?;
                    let current = self.lookup_sandbox(id)?;
                    if current.state != SandboxState::Started || current.node_execution_id != Some(execution) { return Ok(()); }
                    client.release(request(wire::ReleaseRequest { owner: Some(owner), expected_incarnation: seen.incarnation, configuration_revision: 1 })).await?;
                    acknowledged = Some(generation);
                }
            }.await;
            match result {
                Ok(()) => return,
                Err(error) if error.code() == tonic::Code::Unimplemented => {
                    // Capability cannot appear in a legacy execution without
                    // a template refresh. The outer owner cancels on lifecycle
                    // or execution changes; avoid polling unsupported guests.
                    self.shutdown_token.cancelled().await;
                    return;
                }
                Err(error) => {
                    tracing::debug!(sandbox_id = id, %error, "guest configuration observation reconnecting")
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(1));
        }
    }
}
