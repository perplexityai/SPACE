use super::*;
use pplx_space_common_svc::core::host::linux::identities::users;

const UNIX_SOCKET_READY_RETRY_DELAYS: [std::time::Duration; 9] = [
    std::time::Duration::from_millis(0),
    std::time::Duration::from_millis(1),
    std::time::Duration::from_millis(1),
    std::time::Duration::from_millis(2),
    std::time::Duration::from_millis(5),
    std::time::Duration::from_millis(10),
    std::time::Duration::from_millis(20),
    std::time::Duration::from_millis(25),
    std::time::Duration::from_millis(25),
];
const UNIX_SOCKET_READY_MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

pub(super) fn unix_socket_ready_retry_delay(attempt: usize) -> std::time::Duration {
    UNIX_SOCKET_READY_RETRY_DELAYS
        .get(attempt)
        .copied()
        .unwrap_or(UNIX_SOCKET_READY_MAX_RETRY_DELAY)
}

pub(super) fn request_protocol(protocol: i32) -> Result<&'static str, Status> {
    match NetworkProtocol::try_from(protocol)
        .map_err(|_| Status::invalid_argument("protocol must be TCP or UDP"))?
    {
        NetworkProtocol::Unspecified | NetworkProtocol::Tcp => Ok("tcp"),
        NetworkProtocol::Udp => Ok("udp"),
    }
}

pub(super) fn allocate_error_to_status(error: AllocateSlotError) -> Status {
    match error {
        AllocateSlotError::Store(error) => Status::unavailable(error.to_string()),
        AllocateSlotError::InvalidArgument(error) => Status::invalid_argument(error),
        AllocateSlotError::ResourceExhausted(error) => Status::resource_exhausted(error),
        AllocateSlotError::FailedPrecondition(error) => Status::failed_precondition(error),
        AllocateSlotError::InProgress(error) => Status::unavailable(error),
        AllocateSlotError::Internal(error) => Status::internal(error),
    }
}

pub(super) fn pool_status(counts: &PoolCounts, draining: bool, config: &NetworkPoolConfig) -> i32 {
    use crate::proto::NetworkPoolStatus;
    if draining {
        NetworkPoolStatus::Draining as i32
    } else if counts.ready >= config.min_ready_slots {
        NetworkPoolStatus::Healthy as i32
    } else if counts.quarantined > 0 || counts.degraded > 0 {
        NetworkPoolStatus::Degraded as i32
    } else if counts.ready == 0
        && counts.creating == 0
        && counts.dirty == 0
        && counts.resetting == 0
    {
        NetworkPoolStatus::Empty as i32
    } else {
        NetworkPoolStatus::Warming as i32
    }
}

pub(super) fn pool_config_info(config: NetworkPoolConfig) -> NetworkPoolConfigInfo {
    NetworkPoolConfigInfo {
        max_slots: config.max_slots,
        target_ready_slots: config.target_ready_slots,
        min_ready_slots: config.min_ready_slots,
        pod_netns_path: config.pod_netns_path,
        retained_ready_slots: config.retained_ready_slots,
    }
}

pub(super) fn network_pool_status_response(
    counts: &PoolCounts,
    draining: bool,
    config: &NetworkPoolConfig,
    sandbox_cgroup_config: SandboxCgroupConfig,
) -> NetworkPoolStatusResponse {
    NetworkPoolStatusResponse {
        network_pool_status: pool_status(counts, draining, config),
        network_pool_target: config.target_ready_slots,
        network_pool_min_ready: config.min_ready_slots,
        network_slots_total: counts.total,
        network_slots_ready: counts.ready,
        network_slots_leased: counts.leased,
        network_slots_dirty: counts.dirty,
        network_slots_resetting: counts.resetting,
        network_slots_creating: counts.creating,
        network_slots_quarantined: counts.quarantined,
        network_slots_degraded: counts.degraded,
        network_pool_schema_version: NETWORK_POOL_SCHEMA_VERSION,
        network_pool_max_slots: config.max_slots,
        network_pool_pod_netns_path: config.pod_netns_path.clone(),
        network_pool_retained_ready: config.retained_ready_slots,
        sandbox_cgroup_memory_mib: sandbox_cgroup_config.memory_mib_or_default(),
        sandbox_guest_memory_max_mib: sandbox_cgroup_config.guest_memory_max_mib(),
        sandbox_memory_headroom_mib: crate::cgroup::DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB,
        sandbox_cgroup_total_memory_mib: sandbox_cgroup_config.total_memory_mib(config.max_slots),
    }
}

pub(super) fn network_lease_from_record(record: &SlotRecord) -> Option<NetworkLease> {
    let info = &record.info;
    if info.sandbox_id.is_empty() {
        return None;
    }

    let net = NetId::from_slot_info(info);
    Some(NetworkLease {
        sandbox_id: info.sandbox_id.clone(),
        slot: info.slot,
        namespace: info.ns_name(),
        sveth: net.sveth(),
        vpeer: net.vpeer(),
        slot_label: info.slot_label(),
        slot_generation: info.generation,
        slot_state: record.state.proto(),
        slot_health: record.health.proto(),
        last_error: record.last_error.clone(),
        sandbox_uid: (info.sandbox_uid != 0).then_some(info.sandbox_uid),
        pod_netns_path: info.pod_netns.clone(),
        pid: info.pid,
    })
}

pub(super) async fn wait_for_unix_socket_accepting(
    socket_path: &str,
    timeout: std::time::Duration,
) -> Result<(), Status> {
    let started_at = tokio::time::Instant::now();
    let deadline = started_at + timeout;
    let mut attempt = 0usize;

    loop {
        let delay = unix_socket_ready_retry_delay(attempt);
        if delay > std::time::Duration::from_millis(0) {
            let now = tokio::time::Instant::now();
            if now < deadline {
                tokio::time::sleep_until((now + delay).min(deadline)).await;
            }
        }

        match tokio::net::UnixStream::connect(socket_path).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(Status::deadline_exceeded(format!(
                        "unix socket {socket_path} did not accept connections within {}ms: {error}",
                        timeout.as_millis()
                    )));
                }
            }
        }
        attempt = attempt.saturating_add(1);
    }
}

pub(super) fn describe_port_selector(port: crate::firewall::PortSelector) -> String {
    match port {
        crate::firewall::PortSelector::All => "all".to_string(),
        crate::firewall::PortSelector::Port(port) => port.to_string(),
    }
}

pub(super) fn node_peer_authorizer() -> UdsPeerAuthorizer {
    UdsPeerAuthorizer::new_with_default_local_roles(
        "space-node-manager",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
            UdsPeerAccount::with_linux_user(users::SPACE_NODEAPI),
            UdsPeerAccount::with_linux_user(users::SPACE_NODECTL),
            UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR),
            UdsPeerAccount::with_linux_user(users::SPACE_VOLMGR),
            // Publishes into and removes from the node's artifact store only.
            UdsPeerAccount::with_linux_user(users::SPACE_RTMGR),
        ],
    )
}
