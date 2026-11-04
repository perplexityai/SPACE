//! Process-wide file-descriptor budgets for net-gateway.
//!
//! Node-manager starts sidecars with `RLIMIT_NOFILE=4096`. Keep the caps in
//! one place so independently bounded data planes cannot collectively exceed
//! that limit. An egress TCP flow owns its accepted and upstream sockets; a
//! UDP flow can own an upstream and distinct response socket; ingress owns a
//! guest socket while its gRPC transport is counted in the control budget.

pub const PROCESS_NOFILE_LIMIT: usize =
    pplx_space_common_svc::core::host::linux::SANDBOX_PROCESS_NOFILE_LIMIT as usize;
pub const MAX_ACTIVE_TCP_CONNECTIONS: usize = 1744;
/// Each active flow owns an upstream socket and a response task with a 64 KiB
/// receive buffer. At 128 flows those buffers use 8 MiB and worst-case distinct
/// destinations use 256 FDs. This is a deliberately small partition of the
/// sidecar's 4,096-FD rlimit; TCP, ingress, listeners, and control own separate
/// budgets and must not rely on UDP leaving all remaining descriptors free.
pub const MAX_ACTIVE_UDP_SESSIONS: usize = 128;
/// Reserved-DNS datagrams do not create generic UDP sessions, but each active
/// operation owns one transparent response socket. Bound those tasks and file
/// descriptors independently so a guest flood cannot grow either without
/// limit. Together with the TCP cap, this retains the required 64-descriptor
/// process headroom below. Each of the two reserved resolvers gets its own
/// slots so one stalled upstream cannot starve the guest's fallback.
pub const MAX_ACTIVE_RESERVED_DNS_DATAGRAMS_PER_RESOLVER: usize = 64;
pub const MAX_ACTIVE_RESERVED_DNS_DATAGRAMS: usize =
    2 * MAX_ACTIVE_RESERVED_DNS_DATAGRAMS_PER_RESOLVER;
pub use pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS;
pub const MAX_CONTROL_CONNECTIONS: usize = 96;
/// Accepted control transports left for policy, stats, health, and lifecycle
/// RPCs when every ServicePort ingress slot is active.
pub const MIN_CONTROL_CONNECTION_HEADROOM: usize = 32;

const MAX_DATA_PLANE_DESCRIPTORS: usize = MAX_ACTIVE_TCP_CONNECTIONS * 2
    + MAX_ACTIVE_UDP_SESSIONS * 2
    + MAX_ACTIVE_RESERVED_DNS_DATAGRAMS
    + MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS
    + MAX_CONTROL_CONNECTIONS;

/// Headroom for listeners, stdio, telemetry, transient setup, and runtime
/// bookkeeping at simultaneous admission limits.
pub const RESERVED_DESCRIPTORS: usize = PROCESS_NOFILE_LIMIT - MAX_DATA_PLANE_DESCRIPTORS;

const _: () = assert!(MAX_DATA_PLANE_DESCRIPTORS < PROCESS_NOFILE_LIMIT);
const _: () = assert!(RESERVED_DESCRIPTORS >= 64);
const _: () = assert!(
    MAX_CONTROL_CONNECTIONS
        >= MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS + MIN_CONTROL_CONNECTION_HEADROOM
);
