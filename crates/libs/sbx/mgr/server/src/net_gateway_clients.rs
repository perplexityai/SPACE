//! Net-gateway control clients kept per sandbox.
//!
//! A sandbox's frequent reads, its `Stats` calls and the watchdog's health
//! and stats probes, would otherwise dial the gateway's control socket for
//! every call. They reuse one connection per sandbox instead. Lifecycle pushes
//! keep dialing fresh connections with their own retry policies.
//!
//! A kept client is tied to the gateway process it was dialed for: a caller
//! naming another PID gets a fresh dial, so a replaced gateway is never asked
//! through its predecessor's connection. A caller that saw an RPC fail
//! [`discard`]s the client, teardown [`forget`]s the sandbox, and a client
//! older than [`MAX_AGE`] is redialed. Expired entries are swept on lookups
//! and dials alike, at most once per [`SWEEP_INTERVAL`], so an entry for a
//! sandbox that left without teardown is gone within their sum.
//!
//! The watchdog's health probe does not use these clients: it dials afresh,
//! because a dial is what shows the gateway still accepts connections.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::{LinuxPid, SandboxId};
use pplx_space_net_gateway_client::{ConnectError, NetGatewayControlGrpcClient};
use tokio::time::Instant;

const MAX_AGE: Duration = Duration::from_secs(60);
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

static KEPT: LazyLock<KeptClients<NetGatewayControlGrpcClient>> =
    LazyLock::new(|| KeptClients::new(MAX_AGE, SWEEP_INTERVAL));

/// The kept client for `sandbox_id`'s gateway process `gateway_pid`, dialing
/// one with the default connect policy when none is kept. A dial failure is
/// returned, and reported, as it would be without the cache.
pub(crate) async fn client(
    sandbox_id: SandboxId,
    gateway_pid: Option<LinuxPid>,
) -> Result<NetGatewayControlGrpcClient, ConnectError> {
    let socket_sandbox_id = sandbox_id.to_string();
    KEPT.get_or_connect(sandbox_id, gateway_pid, || {
        pplx_space_net_gateway_client::connect(&socket_sandbox_id)
    })
    .await
}

/// Drop the kept client for that gateway process after an RPC on it failed,
/// so the next call dials again.
pub(crate) fn discard(sandbox_id: SandboxId, gateway_pid: Option<LinuxPid>) {
    KEPT.discard(sandbox_id, gateway_pid);
}

/// Drop whatever is kept for a sandbox being torn down.
pub(crate) fn forget(sandbox_id: SandboxId) {
    KEPT.forget(sandbox_id);
}

struct Kept<T> {
    gateway_pid: Option<LinuxPid>,
    connected_at: Instant,
    client: T,
}

pub(crate) struct KeptClients<T> {
    max_age: Duration,
    sweep_interval: Duration,
    kept: Mutex<Entries<T>>,
}

struct Entries<T> {
    by_sandbox: HashMap<SandboxId, Kept<T>>,
    swept_at: Instant,
}

impl<T: Clone> KeptClients<T> {
    pub(crate) fn new(max_age: Duration, sweep_interval: Duration) -> Self {
        Self {
            max_age,
            sweep_interval,
            kept: Mutex::new(Entries {
                by_sandbox: HashMap::new(),
                swept_at: Instant::now(),
            }),
        }
    }

    pub(crate) async fn get_or_connect<E, F, Fut>(
        &self,
        sandbox_id: SandboxId,
        gateway_pid: Option<LinuxPid>,
        connect: F,
    ) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        if let Some(client) = self.fresh(sandbox_id, gateway_pid) {
            return Ok(client);
        }
        let client = connect().await?;
        let now = Instant::now();
        let mut kept = self.lock();
        self.sweep(&mut kept, now);
        kept.by_sandbox.insert(
            sandbox_id,
            Kept {
                gateway_pid,
                connected_at: now,
                client: client.clone(),
            },
        );
        Ok(client)
    }

    fn fresh(&self, sandbox_id: SandboxId, gateway_pid: Option<LinuxPid>) -> Option<T> {
        let now = Instant::now();
        let mut kept = self.lock();
        if now.duration_since(kept.swept_at) >= self.sweep_interval {
            self.sweep(&mut kept, now);
        }
        let entry = kept.by_sandbox.get(&sandbox_id)?;
        (entry.gateway_pid == gateway_pid && now.duration_since(entry.connected_at) < self.max_age)
            .then(|| entry.client.clone())
    }

    /// Drop every entry older than the maximum age.
    fn sweep(&self, kept: &mut Entries<T>, now: Instant) {
        kept.by_sandbox
            .retain(|_, entry| now.duration_since(entry.connected_at) < self.max_age);
        kept.swept_at = now;
    }

    pub(crate) fn discard(&self, sandbox_id: SandboxId, gateway_pid: Option<LinuxPid>) {
        let mut kept = self.lock();
        if kept
            .by_sandbox
            .get(&sandbox_id)
            .is_some_and(|entry| entry.gateway_pid == gateway_pid)
        {
            kept.by_sandbox.remove(&sandbox_id);
        }
    }

    pub(crate) fn forget(&self, sandbox_id: SandboxId) {
        self.lock().by_sandbox.remove(&sandbox_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Entries<T>> {
        // The map holds no invariant a panicking holder could break halfway.
        self.kept
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
