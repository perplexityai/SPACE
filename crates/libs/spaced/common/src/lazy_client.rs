use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::Mutex;

/// Tracks the channel generation used by one logical call across its retries.
///
/// Clones belong to the same call, never to independent concurrent calls. Record
/// each attempt's lease before calling and clear it on success; the retry policy
/// decides which failures require invalidation, including the final failed attempt.
#[derive(Clone, Default)]
pub struct FailedGeneration(std::sync::Arc<std::sync::Mutex<Option<u64>>>);

impl FailedGeneration {
    // This optional scalar has no compound invariants to repair after unwinding;
    // conditional invalidation still protects a concurrently replaced channel.
    /// Records the channel used by the current attempt.
    pub fn set(&self, generation: u64) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(generation);
    }

    /// Discards the recorded generation after a successful attempt.
    pub fn clear(&self) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// Invalidates a failed attempt without clearing a replacement channel.
    pub async fn invalidate<T: Clone>(&self, client: &LazyClient<T>) {
        let generation = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(generation) = generation {
            client.clear_if_generation(generation).await;
        }
    }
}

pub struct LazyClient<T> {
    client: Mutex<Option<CachedClient<T>>>,
    connected: AtomicBool,
    next_generation: AtomicU64,
}

impl<T> std::fmt::Debug for LazyClient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyClient")
            .field("connected", &self.connected.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

struct CachedClient<T> {
    generation: u64,
    client: T,
    connection: Option<crate::PinnedConnection>,
}

impl<T> CachedClient<T> {
    fn is_usable(&self) -> bool {
        !self
            .connection
            .as_ref()
            .is_some_and(crate::PinnedConnection::is_closed)
    }
}

pub struct LazyClientLease<T> {
    generation: u64,
    client: T,
}

impl<T> std::fmt::Debug for LazyClientLease<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyClientLease")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl<T> LazyClientLease<T> {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn into_client(self) -> T {
        self.client
    }
}

impl<T> Default for LazyClient<T> {
    fn default() -> Self {
        Self {
            client: Mutex::new(None),
            connected: AtomicBool::new(false),
            next_generation: AtomicU64::new(1),
        }
    }
}

impl<T> LazyClient<T>
where
    T: Clone,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub async fn clear(&self) {
        *self.client.lock().await = None;
        self.connected.store(false, Ordering::Relaxed);
    }

    pub async fn clear_if_generation(&self, generation: u64) -> bool {
        let mut cached = self.client.lock().await;
        if cached
            .as_ref()
            .is_some_and(|cached| cached.generation == generation)
        {
            *cached = None;
            self.connected.store(false, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub async fn get_or_connect<E, F, Fut>(&self, connect: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        self.get_or_connect_with_generation(connect)
            .await
            .map(LazyClientLease::into_client)
    }

    pub async fn get_or_connect_with_generation<E, F, Fut>(
        &self,
        connect: F,
    ) -> Result<LazyClientLease<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        self.get_or_connect_pinned_with_generation(move || async move {
            connect().await.map(|client| (client, None))
        })
        .await
    }

    /// Like [`Self::get_or_connect_with_generation`], but a client whose pinned
    /// connection has closed is replaced before it is leased again.
    pub async fn get_or_connect_pinned_with_generation<E, F, Fut>(
        &self,
        connect: F,
    ) -> Result<LazyClientLease<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(T, Option<crate::PinnedConnection>), E>>,
    {
        {
            let mut cached = self.client.lock().await;
            match cached.as_ref() {
                Some(cached) if cached.is_usable() => {
                    return Ok(LazyClientLease {
                        generation: cached.generation,
                        client: cached.client.clone(),
                    });
                }
                Some(_) => {
                    *cached = None;
                    self.connected.store(false, Ordering::Relaxed);
                }
                None => {}
            }
        }

        match connect().await {
            Ok((client, connection)) => {
                let mut cached = self.client.lock().await;
                if let Some(existing) = cached.as_ref() {
                    self.connected.store(true, Ordering::Relaxed);
                    return Ok(LazyClientLease {
                        generation: existing.generation,
                        client: existing.client.clone(),
                    });
                }
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                *cached = Some(CachedClient {
                    generation,
                    client: client.clone(),
                    connection,
                });
                self.connected.store(true, Ordering::Relaxed);
                Ok(LazyClientLease { generation, client })
            }
            Err(error) => {
                if self.client.lock().await.is_none() {
                    self.connected.store(false, Ordering::Relaxed);
                }
                Err(error)
            }
        }
    }

    pub fn set_connected(&self) {
        self.connected.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;
