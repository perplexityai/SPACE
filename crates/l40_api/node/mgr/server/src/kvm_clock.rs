//! Whether the host KVM can advance the guest wall clock when a Firecracker
//! snapshot is restored (`clock_realtime`).
//!
//! Firecracker refuses `clock_realtime: true` on `PUT /snapshot/load` when the
//! snapshot's KVM clock state lacks `KVM_CLOCK_REALTIME`, and the kernel only
//! sets that flag for hosts with a stable TSC and a usable wall clock — which
//! virtualized hosts may not provide. A failed load leaves the Firecracker
//! process unusable, so the node probes the host and caches a successful
//! answer instead of discovering it per restore.

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod imp {
    use std::sync::OnceLock;

    static HOST_SUPPORT: OnceLock<bool> = OnceLock::new();

    /// Whether Firecracker snapshot restores on this host may request
    /// `clock_realtime: true`. `false` means restores omit the flag and the
    /// guest wall clock lags by the suspension time; the monotonic clock is
    /// unaffected either way.
    pub fn host_supports_clock_realtime() -> bool {
        if let Some(supported) = HOST_SUPPORT.get() {
            return *supported;
        }
        match probe_host() {
            // Only a successful probe is cached: a transient error (for
            // example `/dev/kvm` momentarily unavailable) must not disable
            // `clock_realtime` for the node's whole lifetime.
            Ok(supported) => {
                let _ = HOST_SUPPORT.set(supported);
                supported
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "could not probe host KVM clock; this restore omits \
                     clock_realtime and the next one retries the probe"
                );
                false
            }
        }
    }

    fn probe_host() -> Result<bool, kvm_ioctls::Error> {
        let kvm = kvm_ioctls::Kvm::new()?;
        let vm = kvm.create_vm()?;
        let clock = vm.get_clock()?;
        Ok(clock.flags & kvm_bindings::KVM_CLOCK_REALTIME != 0)
    }
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
mod imp {
    /// Firecracker only accepts `clock_realtime` on x86_64 Linux, and KVM
    /// exists only on Linux; every other target omits the flag.
    pub fn host_supports_clock_realtime() -> bool {
        false
    }
}

pub use imp::host_supports_clock_realtime;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_is_cached_and_deterministic() {
        let first = host_supports_clock_realtime();
        assert_eq!(host_supports_clock_realtime(), first);
    }
}
