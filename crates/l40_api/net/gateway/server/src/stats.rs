//! Shared proxy statistics counters.

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared proxy statistics, updated by proxy tasks and read by control service.
pub struct ProxyStats {
    pub tcp_active: AtomicU64,
    pub tcp_total: AtomicU64,
    pub tcp_denied: AtomicU64,
    pub udp_active: AtomicU64,
    pub udp_pending_datagrams: AtomicU64,
    pub udp_pending_bytes: AtomicU64,
    network_tx_bytes: AtomicU64,
    network_rx_bytes: AtomicU64,
}

impl Default for ProxyStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ProxyStats {
    pub fn new() -> Self {
        Self::new_with_counters(0, 0)
    }

    pub fn new_with_counters(network_tx_bytes: u64, network_rx_bytes: u64) -> Self {
        Self {
            tcp_active: AtomicU64::new(0),
            tcp_total: AtomicU64::new(0),
            tcp_denied: AtomicU64::new(0),
            udp_active: AtomicU64::new(0),
            udp_pending_datagrams: AtomicU64::new(0),
            udp_pending_bytes: AtomicU64::new(0),
            network_tx_bytes: AtomicU64::new(network_tx_bytes),
            network_rx_bytes: AtomicU64::new(network_rx_bytes),
        }
    }

    pub fn tcp_active(&self) -> u64 {
        self.tcp_active.load(Ordering::Relaxed)
    }
    pub fn tcp_total(&self) -> u64 {
        self.tcp_total.load(Ordering::Relaxed)
    }
    pub fn tcp_denied(&self) -> u64 {
        self.tcp_denied.load(Ordering::Relaxed)
    }
    pub fn udp_active(&self) -> u64 {
        self.udp_active.load(Ordering::Relaxed)
    }
    pub fn udp_pending_datagrams(&self) -> u64 {
        self.udp_pending_datagrams.load(Ordering::Relaxed)
    }
    pub fn udp_pending_bytes(&self) -> u64 {
        self.udp_pending_bytes.load(Ordering::Relaxed)
    }
    pub fn bytes_forwarded(&self) -> u64 {
        self.network_tx_bytes()
            .saturating_add(self.network_rx_bytes())
    }
    pub fn network_tx_bytes(&self) -> u64 {
        self.network_tx_bytes.load(Ordering::Relaxed)
    }
    pub fn network_rx_bytes(&self) -> u64 {
        self.network_rx_bytes.load(Ordering::Relaxed)
    }
    pub fn add_network_tx_bytes(&self, bytes: u64) {
        self.network_tx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn add_network_rx_bytes(&self, bytes: u64) {
        self.network_rx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn add_network_bytes(&self, tx_bytes: u64, rx_bytes: u64) {
        self.add_network_tx_bytes(tx_bytes);
        self.add_network_rx_bytes(rx_bytes);
    }
    pub fn merge_persisted_network_bytes(&self, tx_bytes: u64, rx_bytes: u64) {
        self.network_tx_bytes.fetch_max(tx_bytes, Ordering::Relaxed);
        self.network_rx_bytes.fetch_max(rx_bytes, Ordering::Relaxed);
    }
}
