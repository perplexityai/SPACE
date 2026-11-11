//! Shared driver for watcher-backed Kubernetes stores.

use kube::{
    Api,
    runtime::{WatchStreamExt, reflector, watcher},
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

/// Drive a reflector with retry backoff until service shutdown. The caller
/// owns the store and chooses its resource scope and readiness policy.
pub fn spawn_reflector<K>(
    label: &'static str,
    writer: reflector::store::Writer<K>,
    api: Api<K>,
    config: watcher::Config,
    shutdown: CancellationToken,
) where
    K: kube::Resource
        + Clone
        + serde::de::DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
{
    let stream = reflector::reflector(writer, watcher::watcher(api, config)).default_backoff();
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(stream);
        loop {
            let event = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                event = stream.next() => event,
            };
            let Some(event) = event else { break };
            if let Err(err) = event {
                tracing::warn!(reflector = label, error = %err, "watcher error");
            }
        }
        tracing::warn!(reflector = label, "watcher stream ended");
    });
}
