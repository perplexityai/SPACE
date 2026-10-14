// Pool manager — maintains sandbox pools using clone-based filling.
//
// If a paused pool sandbox exists, new sandboxes are cloned from it (fast).
// If none exist, one is created from the base snapshot (slow), then the
// rest are cloned from it.
// Exponential backoff on failures (5s → 160s max).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::ids::sandbox_label_or_id;
use pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER;

const BASE_TICK: u64 = 5;
const MAX_TICK: u64 = 160;

#[derive(Debug, thiserror::Error)]
enum PoolError {
    #[error("sandbox-manager client: {0}")]
    SandboxClient(String),
    #[error("pool sandbox {sandbox_id} is failed")]
    FailedSandbox { sandbox_id: String },
    #[error("sandbox-manager RPC failed: {0}")]
    SandboxRpc(#[from] tonic::Status),
    #[error("template-manager DB failed: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("pool clone task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

type PoolResult<T> = Result<T, PoolError>;

#[derive(Default)]
struct PoolTickOutcome {
    failures: Vec<PoolError>,
}

impl PoolTickOutcome {
    fn record_failure(&mut self, error: PoolError) {
        self.failures.push(error);
    }

    fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }
}

/// Main loop: runs forever with adaptive tick interval.
pub async fn pool_loop(db: Arc<Mutex<rusqlite::Connection>>) {
    let mut consecutive_failures: u32 = 0;

    loop {
        let tick_secs;
        match pool_tick(&db).await {
            Ok(outcome) => {
                if outcome.has_failures() {
                    for error in &outcome.failures {
                        eprintln!("[pool-manager] pool fill failure: {error}");
                    }
                    consecutive_failures += 1;
                    tick_secs = (BASE_TICK << consecutive_failures.min(5)).min(MAX_TICK);
                    eprintln!(
                        "[pool-manager] failures detected, backing off to {}s ticks",
                        tick_secs
                    );
                } else {
                    if consecutive_failures > 0 {
                        eprintln!(
                            "[pool-manager] tick succeeded, resetting to {}s ticks",
                            BASE_TICK
                        );
                    }
                    consecutive_failures = 0;
                    tick_secs = BASE_TICK;
                }
            }
            Err(e) => {
                eprintln!("[pool-manager] tick error: {e}");
                consecutive_failures += 1;
                tick_secs = (BASE_TICK << consecutive_failures.min(5)).min(MAX_TICK);
            }
        }
        tokio::time::sleep(Duration::from_secs(tick_secs)).await;
    }
}

/// Returns the tick outcome, including any pool sandboxes that failed this tick.
async fn pool_tick(db: &Arc<Mutex<rusqlite::Connection>>) -> PoolResult<PoolTickOutcome> {
    let mut outcome = PoolTickOutcome::default();
    let pools = {
        let conn = db
            .lock()
            .map_err(|_| crate::db::mutex_poison_error("pool manager tick"))?;
        crate::db::get_all_pools(&conn)?
    };

    if pools.is_empty() {
        return Ok(outcome);
    }

    let mut sbx_client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|e| PoolError::SandboxClient(e.to_string()))?;

    let all_sandboxes = pplx_space_sbx_mgr_client::list_all_sandboxes(&mut sbx_client).await?;

    // Unknown inventory cannot prove pool capacity or authorize its cleanup.
    for sandbox in &all_sandboxes {
        sandbox
            .observed_lifecycle()
            .map_err(|error| PoolError::SandboxClient(error.to_string()))?;
    }

    // Clean up failed pool sandboxes
    for sbx in &all_sandboxes {
        if sbx.pool_template.is_some()
            && sbx.observed_lifecycle().is_ok_and(|state| {
                state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Failed
            })
        {
            outcome.record_failure(PoolError::FailedSandbox {
                sandbox_id: sbx.sandbox_id.clone(),
            });
            let short = sandbox_label_or_id(&sbx.sandbox_id);
            eprintln!("[pool-manager] cleaning up failed pool sandbox {short}");
            if let Err(error) = sbx_client
                .stop(pplx_space_sbx_mgr_client::proto::StopRequest {
                    sandbox_id: sbx.sandbox_id.clone(),
                    template_filter: None,
                    blocking: true,
                    immediate_kill: false,
                })
                .await
            {
                eprintln!("[pool-manager] failed to stop failed pool sandbox {short}: {error}");
            }
            if let Err(error) = sbx_client
                .delete(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                    sandbox_id: sbx.sandbox_id.clone(),
                    template_filter: None,
                    blocking: true,
                })
                .await
            {
                eprintln!("[pool-manager] failed to delete failed pool sandbox {short}: {error}");
            }
            if let Some(template) = sbx.pool_template.as_deref() {
                crate::node_state_sync::mark_template_dirty(db, template);
            }
        }
    }

    let mut handles = Vec::new();

    for pool in pools {
        let available = all_sandboxes
            .iter()
            .filter(|s| {
                s.pool_template.as_deref() == Some(pool.template.as_str())
                    && s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused
                    })
            })
            .count();
        let available = count_to_u32(available);
        let pending = all_sandboxes
            .iter()
            .filter(|s| {
                s.pool_template.as_deref() == Some(pool.template.as_str())
                    && (s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Creating
                    }) || s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Created
                    }) || s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Starting
                    }) || s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started
                    }) || s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Pausing
                    }))
            })
            .count();
        let pending = count_to_u32(pending);

        let deficit = pool.target_count.saturating_sub(available + pending);
        if deficit == 0 {
            continue;
        }

        // Find any paused pool sandbox to clone from
        let clone_source = all_sandboxes
            .iter()
            .find(|s| {
                s.pool_template.as_deref() == Some(pool.template.as_str())
                    && s.observed_lifecycle().is_ok_and(|state| {
                        state == pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused
                    })
            })
            .map(|s| s.sandbox_id.clone());

        if let Some(source_id) = clone_source {
            // Fast path: clone from existing paused sandbox
            let short = sandbox_label_or_id(&source_id);
            let clones_this_tick = deficit;
            eprintln!(
                "[pool-manager] {}: available={available}, pending={pending}, target={}, cloning {clones_this_tick} from {short}",
                pool.template, pool.target_count
            );
            for _ in 0..clones_this_tick {
                handles.push(tokio::spawn(clone_and_start(
                    Arc::clone(db),
                    source_id.clone(),
                    pool.template.clone(),
                    pool.vcpu,
                    pool.memory_mib,
                )));
            }
        } else {
            // Slow path: no paused sandbox to clone from — create one from base snapshot
            eprintln!(
                "[pool-manager] {}: no paused sandbox to clone from, creating from base snapshot",
                pool.template
            );
            match create_from_base(
                Arc::clone(db),
                pool.template.clone(),
                pool.from_snapshot(),
                pool.vcpu,
                pool.memory_mib,
            )
            .await
            {
                Ok(new_id) => {
                    let short = sandbox_label_or_id(&new_id);
                    eprintln!(
                        "[pool-manager] {}: base sandbox ready: {short}",
                        pool.template
                    );
                    // Clone the rest from the newly created sandbox
                    let remaining = deficit.saturating_sub(1);
                    for _ in 0..remaining {
                        handles.push(tokio::spawn(clone_and_start(
                            Arc::clone(db),
                            new_id.clone(),
                            pool.template.clone(),
                            pool.vcpu,
                            pool.memory_mib,
                        )));
                    }
                }
                Err(e) => {
                    outcome.record_failure(e);
                }
            }
        }
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => outcome.record_failure(e),
            Err(e) => outcome.record_failure(e.into()),
        }
    }

    Ok(outcome)
}

/// Create a pool sandbox from base snapshot (slow path). Returns sandbox_id on success.
async fn create_from_base(
    db: Arc<Mutex<rusqlite::Connection>>,
    template: String,
    from_snapshot: bool,
    vcpu: u32,
    memory_mib: u32,
) -> PoolResult<String> {
    match do_create_from_base(&template, from_snapshot, vcpu, memory_mib).await {
        Ok(id) => {
            crate::node_state_sync::mark_template_dirty(&db, &template);
            Ok(id)
        }
        Err(e) => {
            eprintln!("[pool-manager] failed to create pool sandbox for '{template}': {e}");
            Err(e)
        }
    }
}

async fn do_create_from_base(
    template: &str,
    from_snapshot: bool,
    vcpu: u32,
    memory_mib: u32,
) -> PoolResult<String> {
    let mut sbx_client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|e| PoolError::SandboxClient(e.to_string()))?;

    let create_resp = sbx_client
        .create(pplx_space_sbx_mgr_client::proto::CreateRequest {
            template: template.to_string(),
            owner: TEMPLATE_MANAGER_SANDBOX_OWNER.to_string(),
            runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.into(),
            snapshots_enabled: false,
            autostart: false,
            pool_template: Some(template.to_string()),
            volume_encryption: false,
            encryptable_pool_member: false,
            // Prefer a prepared snapshot; resolve_boot_mode falls through to cold
            // when required apps prevented this digest's snapshot publication.
            boot_mode: if from_snapshot {
                pplx_space_sbx_mgr_client::proto::BootMode::BaseSnapshot.into()
            } else {
                pplx_space_sbx_mgr_client::proto::BootMode::Cold.into()
            },
            blocking: false,
            sandbox_id: None,
            content_hash: None,
            env: Default::default(),
            auto_lifecycle: None,
            rootfs_size_mb: None,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
            network_policy: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            template_network_policy: None,
            template_network_policy_generation: 0,
            template_network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            route_identity: None,
        })
        .await?;
    let created = create_resp.into_inner();
    let from_warm =
        created.boot_mode_used == pplx_space_sbx_mgr_client::proto::BootMode::BaseSnapshot as i32;
    let sandbox_id = created.sandbox_id;

    let result = async {
        sbx_client
            .start(pplx_space_sbx_mgr_client::proto::StartRequest {
                activation_auto_lifecycle: None,
                operation_activation_attempt_id: None,
                restore_artifact_as_new: None,
                sandbox_id: sandbox_id.clone(),
                vcpu: Some(vcpu),
                memory_mib: Some(memory_mib),
                checkpoint: None,
                from_warm,
                blocking: true,
                netns_path: None,
                ready_timeout_secs: None,
                target_state: 0,
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: None,
                route_identity: None,
            })
            .await?;

        sbx_client
            .pause(pplx_space_sbx_mgr_client::proto::PauseRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .await?;

        Ok::<(), tonic::Status>(())
    }
    .await;

    if let Err(e) = result {
        let short = sandbox_label_or_id(&sandbox_id);
        eprintln!("[pool-manager] cleaning up sandbox {short} after error: {e}");
        if let Err(stop_error) = sbx_client
            .stop(pplx_space_sbx_mgr_client::proto::StopRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
                immediate_kill: false,
            })
            .await
        {
            eprintln!("[pool-manager] failed to stop sandbox {short} after error: {stop_error}");
        }
        if let Err(delete_error) = sbx_client
            .delete(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
            })
            .await
        {
            eprintln!(
                "[pool-manager] failed to delete sandbox {short} after error: {delete_error}"
            );
        }
        return Err(e.into());
    }

    Ok(sandbox_id)
}

/// Clone from a paused sandbox, start, and pause. Returns sandbox_id on success.
async fn clone_and_start(
    db: Arc<Mutex<rusqlite::Connection>>,
    source_id: String,
    template: String,
    vcpu: u32,
    memory_mib: u32,
) -> PoolResult<String> {
    match do_clone_and_start(&source_id, &template, vcpu, memory_mib).await {
        Ok(id) => {
            let short = sandbox_label_or_id(&id);
            eprintln!("[pool-manager] pool sandbox ready (cloned): {short} (template={template})");
            crate::node_state_sync::mark_template_dirty(&db, &template);
            Ok(id)
        }
        Err(e) => {
            eprintln!("[pool-manager] failed to clone pool sandbox for '{template}': {e}");
            Err(e)
        }
    }
}

async fn do_clone_and_start(
    source_id: &str,
    template: &str,
    vcpu: u32,
    memory_mib: u32,
) -> PoolResult<String> {
    let mut sbx_client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|e| PoolError::SandboxClient(e.to_string()))?;

    let clone_resp = sbx_client
        .clone_from_donor(pplx_space_sbx_mgr_client::proto::CloneFromDonorRequest {
            donor_sandbox_id: source_id.to_string(),
            template: template.to_string(),
            owner: TEMPLATE_MANAGER_SANDBOX_OWNER.to_string(),
            pool_template: Some(template.to_string()),
        })
        .await?;
    let sandbox_id = clone_resp.into_inner().sandbox_id;

    let result = async {
        sbx_client
            .start(pplx_space_sbx_mgr_client::proto::StartRequest {
                activation_auto_lifecycle: None,
                operation_activation_attempt_id: None,
                restore_artifact_as_new: None,
                sandbox_id: sandbox_id.clone(),
                vcpu: Some(vcpu),
                memory_mib: Some(memory_mib),
                checkpoint: None,
                from_warm: true,
                blocking: true,
                netns_path: None,
                ready_timeout_secs: None,
                target_state: 0,
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: None,
                route_identity: None,
            })
            .await?;

        sbx_client
            .pause(pplx_space_sbx_mgr_client::proto::PauseRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .await?;

        Ok::<(), tonic::Status>(())
    }
    .await;

    if let Err(e) = result {
        let short = sandbox_label_or_id(&sandbox_id);
        eprintln!("[pool-manager] cleaning up cloned sandbox {short} after error: {e}");
        if let Err(stop_error) = sbx_client
            .stop(pplx_space_sbx_mgr_client::proto::StopRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
                immediate_kill: false,
            })
            .await
        {
            eprintln!(
                "[pool-manager] failed to stop cloned sandbox {short} after error: {stop_error}"
            );
        }
        if let Err(delete_error) = sbx_client
            .delete(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
            })
            .await
        {
            eprintln!(
                "[pool-manager] failed to delete cloned sandbox {short} after error: {delete_error}"
            );
        }
        return Err(e.into());
    }

    Ok(sandbox_id)
}

fn count_to_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}
