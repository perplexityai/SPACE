//! Kept node-api channels for a direct substrate's per-node runtime RPCs.
//!
//! Connect, keepalive and timeout calls each make one or two small RPCs to a
//! sandbox's worker. Dialing a channel for each one costs a TCP handshake, the
//! HTTP/2 preface and, with internal TLS, a TLS handshake: more than the RPCs
//! themselves on a nearby worker. A channel multiplexes concurrent RPCs, so
//! one per worker serves every caller, and it redials after its connection
//! drops with the same retries as a first dial.
//!
//! These channels are for direct workers, whose endpoint is stable. The
//! Kubernetes substrate keeps its sandbox pods' channels differently, per pod
//! instance and only while the dialed connection is open, because a failed
//! dial is also how it learns that a pod is gone and needs recovery; see
//! `kubernetes::pod_worker_channels`.
//!
//! Each worker has one entry, created by its first caller, holding one dial
//! that every caller shares, success or failure: concurrent callers neither
//! race their own dials nor take turns retrying a failed one, and none waits
//! longer than one dial's budget. A failed dial's entry is removed, so the
//! next call dials afresh. An entry is retired [`MAX_AGE`] after it was
//! created, so rotated transport configuration takes effect, and after
//! [`IDLE_TTL`] unused, so removed nodes do not keep connections. Every new
//! entry sweeps retired ones, and the map holds at most [`MAX_ENTRIES`]; past
//! that, calls dial without keeping the channel.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use pplx_space_common_grpc::GrpcRetryPolicy;
use tonic::transport::Channel;

use super::{ControllerError, RuntimeClient};

const MAX_AGE: Duration = Duration::from_secs(300);
const IDLE_TTL: Duration = Duration::from_secs(60);
const MAX_ENTRIES: usize = 4096;

type Dial = Shared<BoxFuture<'static, Result<Channel, ControllerError>>>;

pub(super) struct WorkerChannels {
    connect_timeout: Duration,
    request_timeout: Duration,
    retry_policy: GrpcRetryPolicy,
    // A plain map behind one lock: finding a live entry, sweeping retired
    // ones and inserting a new one must be one step, and the lock is never
    // held across a dial.
    entries: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    dial: Dial,
    created_at: Instant,
    last_used: Instant,
}

impl Entry {
    fn is_live(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.created_at) < MAX_AGE
            && now.saturating_duration_since(self.last_used) < IDLE_TTL
    }
}

impl WorkerChannels {
    pub(super) fn new(connect_timeout: Duration, request_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            request_timeout,
            retry_policy: pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
            entries: Mutex::default(),
        }
    }

    /// A runtime client for the worker at `addr`, on its kept channel.
    pub(super) async fn client(&self, addr: &str) -> Result<RuntimeClient, ControllerError> {
        let Some(dial) = self.entry(addr, Instant::now()) else {
            return Ok(RuntimeClient::new(self.dial(addr).await?));
        };
        // A caller that joins a dial late waits only for its remainder.
        let result = dial.clone().await;
        if result.is_err() {
            self.forget(addr, &dial);
        }
        Ok(RuntimeClient::new(result?))
    }

    /// Dial `addr` within one dial's budget: every attempt its retry policy
    /// allows and the TLS handshake its transport applies after them.
    fn dial(&self, addr: &str) -> BoxFuture<'static, Result<Channel, ControllerError>> {
        let addr = addr.to_string();
        let (connect_timeout, request_timeout, retry_policy) = (
            self.connect_timeout,
            self.request_timeout,
            self.retry_policy,
        );
        async move {
            let endpoint = super::node_api_endpoint(&addr, connect_timeout, request_timeout)
                .map_err(ControllerError::from)?;
            let endpoint = super::node_api_connect_endpoint(endpoint).await?;
            // The bound covers a dial that outlives its own budget.
            let budget = endpoint.retrying_redial_bound(retry_policy, connect_timeout);
            tokio::time::timeout(
                budget,
                endpoint.connect_with_retrying_redials(
                    "worker-node-api",
                    connect_timeout,
                    retry_policy,
                ),
            )
            .await
            .map_err(|_| {
                ControllerError::service_unavailable(format!(
                    "node-api not reachable within {budget:?}"
                ))
            })?
            .map_err(|err| {
                ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
            })
        }
        .boxed()
    }

    /// The live entry's dial for `addr`, started if there is none; `None`
    /// when the map is full of live entries.
    fn entry(&self, addr: &str, now: Instant) -> Option<Dial> {
        let mut entries = self.lock();
        if let Some(entry) = entries.get_mut(addr)
            && entry.is_live(now)
        {
            entry.last_used = now;
            return Some(entry.dial.clone());
        }
        entries.retain(|_, entry| entry.is_live(now));
        if entries.len() >= MAX_ENTRIES {
            return None;
        }
        let dial = self.dial(addr).shared();
        entries.insert(
            addr.to_string(),
            Entry {
                dial: dial.clone(),
                created_at: now,
                last_used: now,
            },
        );
        Some(dial)
    }

    /// Drop `addr`'s entry if it still holds `dial`, which failed.
    fn forget(&self, addr: &str, dial: &Dial) {
        let mut entries = self.lock();
        if entries
            .get(addr)
            .is_some_and(|entry| entry.dial.ptr_eq(dial))
        {
            entries.remove(addr);
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        // The map holds no invariant a panicking holder could break.
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
