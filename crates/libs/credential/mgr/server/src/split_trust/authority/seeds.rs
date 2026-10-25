//! Bounded historical-key cache, not a sandbox cache or key-selection registry.
//! The active seed is held separately by authority and cannot be evicted here.
//! Seeds have no clock expiry: immutable-key material does not change, and
//! staying usable during a hardware outage is intentional. LRU bounds retention.

use super::HardwareError;
use pplx_space_credmgr_client::x509::AnchorKeyId;
use std::{collections::VecDeque, future::Future, sync::Arc};
use tokio::sync::{Mutex, OnceCell};
use zeroize::Zeroizing;

pub(super) type Seed = Arc<Zeroizing<[u8; 48]>>;
type Slot = (AnchorKeyId, Arc<OnceCell<Seed>>);
const CAPACITY: usize = 32;

#[derive(Clone, Default)]
pub(super) struct SeedKeyring {
    slots: Arc<Mutex<VecDeque<Slot>>>,
}

impl SeedKeyring {
    pub async fn get(
        &self,
        key_id: AnchorKeyId,
        load: impl Future<Output = Result<Zeroizing<[u8; 48]>, HardwareError>>,
    ) -> Result<Seed, HardwareError> {
        let cell = {
            let mut slots = self.slots.lock().await;
            let existing = slots.iter().position(|(id, _)| id == &key_id);
            let cell = existing
                .and_then(|index| slots.remove(index))
                .map(|(_, cell)| cell)
                .unwrap_or_default();
            if slots.len() == CAPACITY {
                slots.pop_front();
            }
            slots.push_back((key_id, cell.clone()));
            cell
        };
        // No network operation holds the map lock. Failed/cancelled loads leave
        // the cell retryable; successful loads singleflight until LRU eviction.
        cell.get_or_try_init(|| async { load.await.map(Arc::new) })
            .await
            .cloned()
    }
}

#[cfg(test)]
mod tests;
