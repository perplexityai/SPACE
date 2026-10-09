use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_util::sync::CancellationToken;

use super::PoolOpenError;
use crate::{firewall, net};

pub(super) const CONCURRENCY: NonZeroUsize = NonZeroUsize::new(4).unwrap();

// All namespace work finishes before the caller can activate or release its
// service mutex. Errors do not skip other namespaces: live VMs still need their
// containment applied even when another namespace prevents activation.
pub(super) fn contain_namespaces(
    namespaces: &[String],
    concurrency: NonZeroUsize,
    shutdown: &CancellationToken,
    apply: impl Fn(&str) -> Result<bool, PoolOpenError> + Sync,
) -> Result<usize, PoolOpenError> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..concurrency.get().min(namespaces.len()))
            .map(|_| {
                let (next, apply) = (&next, &apply);
                let span = tracing::Span::current();
                scope.spawn(move || {
                    let _entered = span.enter();
                    let mut first_error = None;
                    let mut contained = 0;
                    while !shutdown.is_cancelled() {
                        let Some(namespace) = namespaces.get(next.fetch_add(1, Ordering::Relaxed)) else { break; };
                        match apply(namespace) {
                            Ok(applied) => contained += usize::from(applied),
                            Err(error) => {
                                tracing::error!(namespace, %error, "startup network containment failed");
                                first_error.get_or_insert(error);
                            }
                        }
                    }
                    (contained, first_error)
                })
            })
            .collect();
        let mut contained = 0;
        let mut first_error = None;
        for worker in workers {
            // A panic unwinds this scope only after its remaining workers join.
            // The tracked pool-open task then reports failure, still under ownership.
            let (count, error) = worker.join().expect("startup containment worker panicked");
            contained += count;
            if first_error.is_none() {
                first_error = error;
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if shutdown.is_cancelled() {
            return Err(PoolOpenError::StartupCancelled);
        }
        Ok(contained)
    })
}

pub(super) fn contain_namespace(namespace: &str) -> Result<bool, PoolOpenError> {
    if !net::ns_exists(namespace) {
        return Ok(false);
    }
    let tap_exists =
        net::try_link_exists_in(net::TAP_DEVICE, Some(namespace)).map_err(|source| {
            PoolOpenError::ExistingTapProbe {
                namespace: namespace.into(),
                source,
            }
        })?;
    if !tap_exists {
        return Ok(false);
    }
    let mut error = None;
    if let Err(source) = firewall::ensure_guest_l2_ingress_policy(namespace) {
        let failure = PoolOpenError::GuestL2Ingress {
            namespace: namespace.into(),
            source,
        };
        tracing::error!(%failure, "cannot install existing TAP containment");
        error = Some(failure);
    }
    if let Err(source) = firewall::constrain_direct_inbound_reply_path(namespace) {
        let failure = PoolOpenError::DirectInboundReplyPath {
            namespace: namespace.into(),
            source,
        };
        tracing::error!(%failure, "cannot constrain existing direct inbound reply path");
        error.get_or_insert(failure);
    }
    match error {
        Some(error) => Err(error),
        None => Ok(true),
    }
}

#[cfg(test)]
mod tests;
