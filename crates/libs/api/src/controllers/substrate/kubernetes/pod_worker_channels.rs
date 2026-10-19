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
//! Callers that find no open connection share one dial: a burst of concurrent
//! calls to a pod pays one handshake, and each of them sees that dial's
//! outcome. Failed dials are not kept. An entry is retired [`MAX_AGE`] after
//! it was created, so rotated transport configuration takes effect, and after
//! [`IDLE_TTL`] unused, so departed pods do not keep connections. Every new
//! entry sweeps retired ones, and the map holds at most [`MAX_ENTRIES`]; past
//! that, a new pod's channel displaces the one unused longest.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use pplx_space_common_core::types::PodUid;
use pplx_space_common_grpc::ConnectionWatch;
use tonic::transport::Channel;
use tracing::Instrument;

use crate::controllers::ControllerError;
use crate::controllers::substrate::RuntimeClient;

/// Tags `grpc.client.channel_cache`, as the dial tags its connect spans.
const SERVICE: &str = "worker-node-api";
/// Rotates kept connections so changed transport configuration, such as a
/// renewed internal TLS certificate, takes effect within this long.
pub(super) const MAX_AGE: Duration = Duration::from_secs(30 * 60);
/// How long a pod's connection is kept unused. Agent traffic to one sandbox
/// arrives in turns that are often minutes apart, and the API's replicas each
/// keep their own connection, so a short window makes most calls dial. Idle
/// connections cost little: HTTP/2 keepalive pings them, which also retires
/// one whose peer was lost.
pub(super) const IDLE_TTL: Duration = Duration::from_secs(15 * 60);
pub(super) const MAX_ENTRIES: usize = 4096;

/// The pod instance a kept channel reached.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PodWorkerKey {
    pub(super) addr: SocketAddr,
    pub(super) pod_uid: Option<PodUid>,
    pub(super) generation: i64,
}

type DialOutput = Result<(Channel, ConnectionWatch), ControllerError>;
type Dial = Shared<BoxFuture<'static, DialOutput>>;

#[derive(Default)]
pub(super) struct PodWorkerChannels {
    // A plain map behind one lock, never held across a dial.
    entries: Mutex<HashMap<PodWorkerKey, Slot>>,
}

enum Slot {
    /// A dial every concurrent caller for the pod waits on, so a burst of
    /// calls to a pod with no open connection pays one dial, not one each.
    Dialing(Dial),
    Kept(Entry),
}

impl Slot {
    fn is_live(&self, now: Instant) -> bool {
        match self {
            // A dial is bounded by its own retry budget.
            Self::Dialing(_) => true,
            Self::Kept(entry) => entry.is_live(now),
        }
    }
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

enum Lookup {
    Hit(Box<RuntimeClient>),
    /// A dial to wait on, and whether this caller started it.
    Join(Dial, bool),
    /// The map is full of live entries: the dial, to run without keeping it.
    Full(BoxFuture<'static, DialOutput>),
}

impl PodWorkerChannels {
    /// A runtime client for the pod at `key`: its kept channel while that
    /// channel's connection is open, otherwise the result of one `dial`
    /// shared by every caller that arrives while it runs, kept on success.
    /// A failed dial reaches each of those callers and is not kept.
    pub(super) async fn client<F>(
        &self,
        key: PodWorkerKey,
        dial: F,
    ) -> Result<RuntimeClient, ControllerError>
    where
        F: Future<Output = DialOutput> + Send + 'static,
    {
        let dial = match self.lookup(&key, dial.boxed(), Instant::now()) {
            Lookup::Hit(client) => {
                pplx_space_metrics::grpc::client_channel_cache(SERVICE, "hit");
                return Ok(*client);
            }
            Lookup::Join(dial, started) => {
                let outcome = if started { "miss" } else { "joined" };
                pplx_space_metrics::grpc::client_channel_cache(SERVICE, outcome);
                dial
            }
            Lookup::Full(dial) => {
                pplx_space_metrics::grpc::client_channel_cache(SERVICE, "miss");
                let (channel, watch) = dial.await?;
                return Ok(RuntimeClient::kept(channel, watch));
            }
        };
        let result = dial.clone().await;
        self.settle(&key, &dial, &result, Instant::now());
        let (channel, watch) = result?;
        Ok(RuntimeClient::kept(channel, watch))
    }

    fn lookup(
        &self,
        key: &PodWorkerKey,
        dial: BoxFuture<'static, DialOutput>,
        now: Instant,
    ) -> Lookup {
        let mut entries = self.lock();
        match entries.get_mut(key) {
            Some(Slot::Kept(entry)) if entry.is_live(now) => {
                entry.last_used = now;
                return Lookup::Hit(Box::new(entry.client()));
            }
            Some(Slot::Dialing(dial)) => return Lookup::Join(dial.clone(), false),
            _ => {}
        }
        entries.retain(|_, slot| slot.is_live(now));
        if entries.len() >= MAX_ENTRIES && !evict_least_recently_used(&mut entries) {
            return Lookup::Full(dial);
        }
        // The dial runs on its own task, so a caller that gives up does not
        // stall it for the callers still waiting, and it stays in the trace of
        // the call that started it.
        let task = tokio::spawn(dial.in_current_span());
        let dial = async move {
            task.await.unwrap_or_else(|error| {
                Err(ControllerError::service_unavailable(format!(
                    "node-api dial did not finish: {error}"
                )))
            })
        }
        .boxed()
        .shared();
        entries.insert(key.clone(), Slot::Dialing(dial.clone()));
        Lookup::Join(dial, true)
    }

    /// Record how `dial` for `key` ended, unless its slot has moved on.
    fn settle(&self, key: &PodWorkerKey, dial: &Dial, result: &DialOutput, now: Instant) {
        let mut entries = self.lock();
        if !matches!(entries.get(key), Some(Slot::Dialing(current)) if current.ptr_eq(dial)) {
            return;
        }
        match result {
            Ok((channel, watch)) => {
                entries.insert(
                    key.clone(),
                    Slot::Kept(Entry {
                        channel: channel.clone(),
                        watch: watch.clone(),
                        created_at: now,
                        last_used: now,
                    }),
                );
            }
            Err(_) => {
                entries.remove(key);
            }
        }
    }

    #[cfg(test)]
    fn live(&self, key: &PodWorkerKey, now: Instant) -> Option<RuntimeClient> {
        let mut entries = self.lock();
        let Some(Slot::Kept(entry)) = entries.get_mut(key) else {
            return None;
        };
        if !entry.is_live(now) {
            entries.remove(key);
            return None;
        }
        entry.last_used = now;
        Some(entry.client())
    }

    #[cfg(test)]
    fn keep(&self, key: PodWorkerKey, channel: Channel, watch: ConnectionWatch, now: Instant) {
        let mut entries = self.lock();
        entries.retain(|_, slot| slot.is_live(now));
        if entries.len() < MAX_ENTRIES || evict_least_recently_used(&mut entries) {
            entries.insert(
                key,
                Slot::Kept(Entry {
                    channel,
                    watch,
                    created_at: now,
                    last_used: now,
                }),
            );
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PodWorkerKey, Slot>> {
        // The map holds no invariant a panicking holder could break.
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Make room for a new pod by dropping the kept connection unused longest.
/// Its holders keep it; only later callers dial. `false` when every slot is
/// a dial in progress.
fn evict_least_recently_used(entries: &mut HashMap<PodWorkerKey, Slot>) -> bool {
    let oldest = entries
        .iter()
        .filter_map(|(key, slot)| match slot {
            Slot::Kept(entry) => Some((key, entry.last_used)),
            Slot::Dialing(_) => None,
        })
        .min_by_key(|(_, last_used)| *last_used)
        .map(|(key, _)| key.clone());
    match oldest {
        Some(key) => {
            entries.remove(&key);
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests;
