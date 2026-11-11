//! Explicit unary inventory limits, not estimates of physical node capacity.
//!
//! Bound request-ID work as well as complete response payloads. The byte budget
//! leaves room for envelopes and scalar fields under the default 4 MiB decode
//! limit; it is not an estimate of the work or response bytes for one ID.

pub const MAX_ITEMS: usize = 4096;
/// Bound work per SQLite lock hold and inventory-client round trip, independently
/// of the legacy unary request size accepted during a caller-first rollout.
pub const READ_BATCH_ITEMS: usize = 128;
pub const MAX_ENCODED_BYTES: usize = 3 * 1024 * 1024;
/// Bound retained SQLite/sidecar input before deserializing richer structures.
pub const MAX_RAW_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "inventory exceeds its item or byte budget; request fewer IDs or use streamed cleanup discovery"
)]
pub struct InventoryLimitExceeded;

#[derive(Debug)]
pub struct InventoryBudget {
    remaining_items: usize,
    remaining_bytes: usize,
}

impl InventoryBudget {
    pub const fn new(items: usize, bytes: usize) -> Self {
        Self {
            remaining_items: items,
            remaining_bytes: bytes,
        }
    }

    pub fn charge(&mut self, bytes: usize) -> Result<(), InventoryLimitExceeded> {
        let items = self
            .remaining_items
            .checked_sub(1)
            .ok_or(InventoryLimitExceeded)?;
        self.charge_bytes(bytes)?;
        self.remaining_items = items;
        Ok(())
    }

    pub fn charge_bytes(&mut self, bytes: usize) -> Result<(), InventoryLimitExceeded> {
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(bytes)
            .ok_or(InventoryLimitExceeded)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
