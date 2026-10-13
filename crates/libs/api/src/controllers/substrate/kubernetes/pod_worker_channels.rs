//! Kept node-api channels for a sandbox pod's runtime RPCs.
//!
//! A data-plane call such as a file stat is one small RPC to the sandbox
//! pod's proxy. Dialing a channel for each call costs a TCP handshake, the
//! HTTP/2 preface and, with internal TLS, a TLS handshake, which together can
//! cost more than the RPC; and a dial whose SYN is lost waits out the whole
//! dial timeout before its retry. A channel multiplexes concurrent RPCs, so
//! one per pod serves every caller.
//!
//! The Kubernetes substrate's callers treat a failed dial as the sign that a
//! pod's proxy is gone and needs recovery, so a kept channel must never hide
//! that. An entry is therefore keyed by the pod instance it reached (address,
//! pod UID and execution generation), so a replaced pod is always dialed
//! afresh, and it is handed out only while the connection it was dialed with
//! is open: once that connection ends (the peer closed or reset it, or an
//! HTTP/2 keepalive timed out) the entry is dropped and the next caller dials,
//! seeing any failure exactly as it would have without the cache. A caller
//! can also retire an entry on its own evidence, such as a draining node-api,
//! through [`super::RuntimeClient::retire_kept_connection`].
//!
//! Failed dials are not kept. An entry is retired [`MAX_AGE`] after it was
//! created, so rotated transport configuration takes effect, and after
//! [`IDLE_TTL`] unused, so departed pods do not keep connections. Every new
//! entry sweeps retired ones, and the map holds at most [`MAX_ENTRIES`]; past
//! that, calls dial without keeping the channel.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::PodUid;
use pplx_space_common_grpc::ConnectionWatch;
use tonic::transport::Channel;

use crate::controllers::ControllerError;
use crate::controllers::substrate::RuntimeClient;

/// Tags `grpc.client.channel_cache`, as the dial tags its connect spans.
const SERVICE: &str = "worker-node-api";
pub(super) const MAX_AGE: Duration = Duration::from_secs(300);
pub(super) const IDLE_TTL: Duration = Duration::from_secs(60);
pub(super) const MAX_ENTRIES: usize = 4096;

/// The pod instance a kept channel reached.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PodWorkerKey {
    pub(super) addr: SocketAddr,
    pub(super) pod_uid: Option<PodUid>,
    pub(super) generation: i64,
}

#[derive(Default)]
pub(super) struct PodWorkerChannels {
    // A plain map behind one lock, never held across a dial.
    entries: Mutex<HashMap<PodWorkerKey, Entry>>,
}

struct Entry {
    channel: Channel,
    watch: ConnectionWatch,
    created_at: Instant,
    last_used: Instant,
}

impl Entry {
    fn is_live(&self, now: Instant) -> bool {
        !self.watch.is_closed()
            && now.saturating_duration_since(self.created_at) < MAX_AGE
            && now.saturating_duration_since(self.last_used) < IDLE_TTL
    }

    fn client(&self) -> RuntimeClient {
        RuntimeClient::kept(self.channel.clone(), self.watch.clone())
    }
}

impl PodWorkerChannels {
    /// A runtime client for the pod at `key`: its kept channel while that
    /// channel's connection is open, otherwise a fresh `dial`, kept on
    /// success. A failed dial is returned as is and not kept.
    pub(super) async fn client<F>(
        &self,
        key: PodWorkerKey,
        dial: F,
    ) -> Result<RuntimeClient, ControllerError>
    where
        F: Future<Output = Result<(Channel, ConnectionWatch), ControllerError>>,
    {
        if let Some(client) = self.live(&key, Instant::now()) {
            pplx_space_metrics::grpc::client_channel_cache(SERVICE, "hit");
            return Ok(client);
        }
        pplx_space_metrics::grpc::client_channel_cache(SERVICE, "miss");
        let (channel, watch) = dial.await?;
        Ok(self.keep(key, channel, watch, Instant::now()))
    }

    fn live(&self, key: &PodWorkerKey, now: Instant) -> Option<RuntimeClient> {
        let mut entries = self.lock();
        let entry = entries.get_mut(key)?;
        if !entry.is_live(now) {
            entries.remove(key);
            return None;
        }
        entry.last_used = now;
        Some(entry.client())
    }

    /// Keep a freshly dialed channel, unless a concurrent caller already
    /// kept a live one for the same pod, which is then shared instead.
    fn keep(
        &self,
        key: PodWorkerKey,
        channel: Channel,
        watch: ConnectionWatch,
        now: Instant,
    ) -> RuntimeClient {
        let mut entries = self.lock();
        if let Some(existing) = entries.get_mut(&key)
            && existing.is_live(now)
        {
            existing.last_used = now;
            return existing.client();
        }
        entries.retain(|_, entry| entry.is_live(now));
        let entry = Entry {
            channel,
            watch,
            created_at: now,
            last_used: now,
        };
        let client = entry.client();
        if entries.len() < MAX_ENTRIES {
            entries.insert(key, entry);
        }
        client
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PodWorkerKey, Entry>> {
        // The map holds no invariant a panicking holder could break.
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
