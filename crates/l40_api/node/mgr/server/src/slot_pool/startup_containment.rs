use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_util::sync::CancellationToken;

use super::PoolOpenError;
use crate::{firewall, net};

pub(super) const CONCURRENCY: NonZeroUsize = NonZeroUsize::new(4).unwrap();

// All namespace work finishes before the caller can activate or release its
// service mutex. Errors do not skip other namespaces: live VMs still need their
// containment applied even when another namespace prevents activation. On
// success, returns each namespace's result in the order given.
pub(super) fn contain_namespaces<T: Send>(
    namespaces: &[String],
    concurrency: NonZeroUsize,
    shutdown: &CancellationToken,
    apply: impl Fn(&str) -> Result<T, PoolOpenError> + Sync,
) -> Result<Vec<T>, PoolOpenError> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..concurrency.get().min(namespaces.len()))
            .map(|_| {
                let (next, apply) = (&next, &apply);
                let span = tracing::Span::current();
                scope.spawn(move || {
                    let _entered = span.enter();
                    let mut first_error = None;
                    let mut contained = Vec::new();
                    while !shutdown.is_cancelled() {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(namespace) = namespaces.get(index) else { break; };
                        match apply(namespace) {
                            Ok(result) => contained.push((index, result)),
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
        let mut contained = Vec::with_capacity(namespaces.len());
        let mut first_error = None;
        for worker in workers {
            // A panic unwinds this scope only after its remaining workers join.
            // The tracked pool-open task then reports failure, still under ownership.
            let (results, error) = worker.join().expect("startup containment worker panicked");
            contained.extend(results);
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
        contained.sort_unstable_by_key(|(index, _)| *index);
        Ok(contained.into_iter().map(|(_, result)| result).collect())
    })
}

/// Contain one namespace and return the stamps its listing holds, or `None`
/// when it has no TAP and so was not visited.
pub(super) fn contain_namespace(
    namespace: &str,
) -> Result<Option<firewall::NamespaceStamps>, PoolOpenError> {
    if !net::ns_exists(namespace) {
        return Ok(None);
    }
    let tap_exists =
        net::try_link_exists_in(net::TAP_DEVICE, Some(namespace)).map_err(|source| {
            PoolOpenError::ExistingTapProbe {
                namespace: namespace.into(),
                source,
            }
        })?;
    if !tap_exists {
        return Ok(None);
    }
    let mut error = None;
    // Apply first, so the listing below shows the policy as just installed.
    let l2_applied = match firewall::apply_guest_l2_ingress_policy(namespace) {
        Ok(()) => true,
        Err(source) => {
            let failure = PoolOpenError::GuestL2Ingress {
                namespace: namespace.into(),
                source: source.into(),
            };
            tracing::error!(%failure, "cannot install existing TAP containment");
            error = Some(failure);
            false
        }
    };
    // Both checks read one listing, so a listing failure fails both.
    let ruleset = match firewall::NamespaceRuleset::list(namespace) {
        Ok(ruleset) => ruleset,
        Err(source) => {
            let failure = PoolOpenError::NamespaceRuleset {
                namespace: namespace.into(),
                source,
            };
            tracing::error!(%failure, "cannot inspect existing TAP containment");
            return Err(error.unwrap_or(failure));
        }
    };
    if l2_applied
        && let Err(source) = firewall::validate_guest_l2_ingress_policy(namespace, &ruleset)
    {
        let failure = PoolOpenError::GuestL2Ingress {
            namespace: namespace.into(),
            source,
        };
        tracing::error!(%failure, "cannot install existing TAP containment");
        error = Some(failure);
    }
    if let Err(source) = firewall::constrain_direct_inbound_reply_path(namespace, &ruleset) {
        let failure = PoolOpenError::DirectInboundReplyPath {
            namespace: namespace.into(),
            source,
        };
        tracing::error!(%failure, "cannot constrain existing direct inbound reply path");
        error.get_or_insert(failure);
    }
    match error {
        Some(error) => Err(error),
        None => Ok(Some(ruleset.stamps())),
    }
}

#[cfg(test)]
mod tests;
