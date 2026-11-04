use std::sync::atomic::{AtomicBool, Ordering};

const DIRTY_MARK_INITIAL_RETRY: std::time::Duration = std::time::Duration::from_secs(1);
const DIRTY_MARK_MAX_RETRY: std::time::Duration = std::time::Duration::from_secs(300);
static NODE_STATUS_DIRTY_FLUSH_RUNNING: AtomicBool = AtomicBool::new(false);

/// Receives the network pool's notice that node status (slot leases, pool
/// counts, draining or pool configuration) changed. The pool calls it
/// synchronously on the changing path, sometimes holding its store lock, so an
/// implementation must return promptly and must not call back into the pool.
pub trait NodeStatusDirtySink: std::fmt::Debug + Send + Sync {
    fn mark_node_status_dirty(&self);
}

/// Tells node-ctl, over its node-local socket, to republish node status. On a
/// Tokio runtime, a mark starts one detached notification for the whole
/// process, retried with backoff until node-ctl accepts it; marks made while it
/// is in flight are absorbed by it. Off a runtime, a mark is dropped.
#[derive(Clone, Copy, Debug)]
pub struct NodeCtlNodeStatusDirtySink;

impl NodeStatusDirtySink for NodeCtlNodeStatusDirtySink {
    fn mark_node_status_dirty(&self) {
        mark_node_status_dirty();
    }
}

fn mark_node_status_dirty() {
    if NODE_STATUS_DIRTY_FLUSH_RUNNING.swap(true, Ordering::AcqRel) {
        return;
    }

    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        NODE_STATUS_DIRTY_FLUSH_RUNNING.store(false, Ordering::Release);
        tracing::debug!("skipping node-status dirty mark outside a Tokio runtime");
        return;
    };

    handle.spawn(async move {
        let mut delay = std::time::Duration::ZERO;
        loop {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            match pplx_space_node_ctl_client::mark_node_status_dirty().await {
                Ok(()) => {
                    NODE_STATUS_DIRTY_FLUSH_RUNNING.store(false, Ordering::Release);
                    return;
                }
                Err(error) => {
                    let retry_after_secs = next_dirty_mark_delay(delay).as_secs();
                    tracing::warn!(
                        %error,
                        retry_after_secs,
                        "failed to mark node-status facet dirty in node-ctl; retrying"
                    );
                    delay = next_dirty_mark_delay(delay);
                }
            }
        }
    });
}

fn next_dirty_mark_delay(previous: std::time::Duration) -> std::time::Duration {
    if previous.is_zero() {
        DIRTY_MARK_INITIAL_RETRY
    } else {
        previous.saturating_mul(2).min(DIRTY_MARK_MAX_RETRY)
    }
}

#[cfg(test)]
pub(crate) mod tests;
