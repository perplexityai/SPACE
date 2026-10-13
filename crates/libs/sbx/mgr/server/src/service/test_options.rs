//! Service options for tests, with every host dependency redirected.
//!
//! [`SandboxManagerServiceOptions::default`] is production wiring. It reads
//! `SPACE_NODE_ID` or the host's node-id file, names the production
//! host-control and credential-manager sockets, probes `fc.pid` under the
//! fixed sandbox run directory, dials node-manager's fixed socket and asks the
//! host's template-manager. Tests start from
//! [`SandboxManagerServiceOptions::for_test`] instead, and override the fields
//! they exercise with struct update syntax.
//!
//! Service construction still reads process environment of its own, such as
//! the feature-flag provider settings. A test that depends on those runs in a
//! child test process; see `crate::service::tests::enter_route_test_process`.

use std::path::Path;
use std::sync::Arc;

use pplx_space_common_core::types::NodeId;

use super::{SandboxManagerService, SandboxManagerServiceOptions};

/// The node identity of every test service.
pub(crate) fn test_node_id() -> NodeId {
    NodeId::from_uuid(uuid::Uuid::from_u128(
        0x019f_8000_0000_7000_8000_0000_0000_00fe,
    ))
}

impl SandboxManagerServiceOptions {
    /// [`Self::for_test_in`] under a fresh directory that nothing creates, so
    /// no runtime is ever live and no socket path names a live service.
    pub(crate) fn for_test() -> Self {
        Self::for_test_in(
            &std::env::temp_dir().join(format!("space-sbx-mgr-test-{}", uuid::Uuid::now_v7())),
        )
    }

    /// Options whose host dependencies all resolve under `scratch`, a
    /// directory the test owns:
    ///
    /// - the node identity is [`test_node_id`];
    /// - both runtime liveness probes read `scratch/<sandbox id>/fc.pid`: the
    ///   service's `sandbox_runtime_live` and stop and delete target
    ///   selection. A test makes a runtime live by linking that path to an
    ///   existing file;
    /// - the credential-manager and node-manager sockets are paths under
    ///   `scratch` that nothing binds, so every call to either fails to
    ///   connect;
    /// - the host-control socket is only passed through to node-manager's
    ///   `Launch`, and also sits under `scratch`, so it names no live host
    ///   service;
    /// - template-manager is a socket under `scratch` that nothing binds, so a
    ///   dialed lookup fails to connect and a shared-channel lookup is
    ///   `Unavailable`.
    pub(crate) fn for_test_in(scratch: &Path) -> Self {
        let run_dir = scratch.to_owned();
        Self {
            host_control_socket_path: scratch.join("absent-host-control.sock"),
            credential_manager_socket_path: scratch.join("absent-credential-manager.sock"),
            sandbox_runtime_live: Arc::new(move |id| {
                super::cache_cleanup::is_sandbox_running_in(&run_dir, id)
            }),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            local_node_id: Some(test_node_id()),
            node_role: None,
            node_managed_by: None,
            route_ed25519_verification_bundle: None,
            node_manager: crate::node_manager_transport::fake::unbound(
                &scratch.join("absent-node-manager.sock"),
            ),
            template_resolver: crate::template_resolver::fake::unbound(
                &scratch.join("absent-template-manager.sock"),
            ),
            cleanup_run_dir: Some(scratch.to_owned()),
        }
    }
}

impl SandboxManagerService {
    /// A service built from [`SandboxManagerServiceOptions::for_test`].
    pub(crate) fn for_test(conn: rusqlite::Connection) -> Self {
        Self::new_with_options(conn, SandboxManagerServiceOptions::for_test())
    }
}
