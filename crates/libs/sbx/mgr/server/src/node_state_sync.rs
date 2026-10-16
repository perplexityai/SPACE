use pplx_space_common_svc::{
    core::types::SandboxId,
    types::{FACET_NETWORK_POLICY, FACET_SANDBOX_INVENTORY},
};
use pplx_space_node_api_client::proto::{
    FacetRef, MarkFacetDirtyRequest, NetworkPolicyTargetRef, ReconcileFacet, facet_ref,
    network_policy_target_ref,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::Request;

pub(crate) const SKIP_NODE_PUBLISH_METADATA: &str = "x-space-skip-cluster-sync";
const DIRTY_MARK_INITIAL_RETRY: std::time::Duration = std::time::Duration::from_secs(1);
const DIRTY_MARK_MAX_RETRY: std::time::Duration = std::time::Duration::from_secs(300);
const DIRTY_OUTBOX_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const DIRTY_OUTBOX_POLL_JITTER_MAX: std::time::Duration = std::time::Duration::from_millis(500);
const DIRTY_OUTBOX_BATCH_LIMIT: usize = 100;

#[derive(Debug, thiserror::Error)]
#[error("sandbox database mutex is poisoned")]
pub(crate) struct DatabaseMutexPoisoned;

/// Raw-connection helpers cannot initiate service drain themselves. Wake the
/// supervised outbox worker, which observes the same poisoned connection and
/// terminates the process through the normal ownership-preserving drain.
pub(crate) fn connection_or_signal_failure(
    db: &crate::db::SharedConnection,
) -> Result<crate::db::ConnectionGuard<'_>, DatabaseMutexPoisoned> {
    db.lock().map_err(|_| {
        tracing::error!("sandbox database mutex poisoned; notifying supervised outbox worker");
        database_poison_notify().notify_one();
        DatabaseMutexPoisoned
    })
}

pub(crate) fn should_skip_node_publish<T>(request: &Request<T>) -> bool {
    request
        .metadata()
        .get(SKIP_NODE_PUBLISH_METADATA)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

pub(crate) fn mark_sandbox_dirty(db: &crate::db::SharedConnection, sandbox_id: &str) {
    if sandbox_id.trim().is_empty() {
        return;
    }
    if enqueue_sandbox_inventory_dirty(db, sandbox_id) {
        dirty_outbox_notify().notify_one();
    }
}

pub(crate) fn mark_sandboxes_dirty<I>(db: &crate::db::SharedConnection, sandbox_ids: I)
where
    I: IntoIterator<Item = String>,
{
    let sandbox_ids = sandbox_ids
        .into_iter()
        .filter(|sandbox_id| !sandbox_id.trim().is_empty())
        .collect::<Vec<_>>();
    if sandbox_ids.is_empty() {
        return;
    }
    let mut enqueued_any = false;
    {
        let Ok(db) = connection_or_signal_failure(db) else {
            return;
        };
        for sandbox_id in &sandbox_ids {
            match crate::db::enqueue_node_dirty_sandbox_inventory(&db, sandbox_id) {
                Ok(_) => {
                    enqueued_any = true;
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        %error,
                        "failed to enqueue sandbox inventory dirty ref"
                    );
                }
            }
        }
    }
    if enqueued_any {
        dirty_outbox_notify().notify_one();
    }
}

fn enqueue_sandbox_inventory_dirty(db: &crate::db::SharedConnection, sandbox_id: &str) -> bool {
    let Ok(db) = connection_or_signal_failure(db) else {
        return false;
    };
    match crate::db::enqueue_node_dirty_sandbox_inventory(&db, sandbox_id) {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %error,
                "failed to enqueue sandbox inventory dirty ref"
            );
            false
        }
    }
}

fn dirty_outbox_notify() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

fn database_poison_notify() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

pub(crate) fn start_dirty_outbox_worker(
    db: crate::db::SharedConnection,
    shutdown_token: CancellationToken,
    task_tracker: &TaskTracker,
    critical: crate::service::drain::CriticalTaskGuard,
) {
    task_tracker.spawn(async move {
        let _critical = critical;
        let mut delay = std::time::Duration::ZERO;
        let poll_interval = DIRTY_OUTBOX_POLL_INTERVAL;
        let poll_jitter = DIRTY_OUTBOX_POLL_JITTER_MAX;
        let batch_limit = DIRTY_OUTBOX_BATCH_LIMIT;
        let mut poll_tick = 0_u64;
        loop {
            let sleep_for = if delay.is_zero() {
                poll_tick = poll_tick.wrapping_add(1);
                jittered_dirty_outbox_poll_interval(poll_interval, poll_jitter, poll_tick)
            } else {
                delay
            };
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!("node dirty outbox worker shutting down");
                    break;
                }
                _ = tokio::time::sleep(sleep_for) => {}
                _ = database_poison_notify().notified() => {
                    if db.is_poisoned() {
                        return;
                    }
                    continue;
                }
                _ = dirty_outbox_notify().notified(), if delay.is_zero() => {}
            }

            let rows = {
                let Ok(db) = connection_or_signal_failure(&db) else {
                    return;
                };
                match crate::db::list_node_dirty_refs(&db, batch_limit) {
                    Ok(rows) => rows,
                    Err(error) => {
                        tracing::warn!(%error, "failed to read node dirty outbox");
                        delay = next_dirty_mark_delay(delay);
                        continue;
                    }
                }
            };
            if rows.is_empty() {
                delay = std::time::Duration::ZERO;
                continue;
            }

            let refs = rows
                .iter()
                .filter_map(dirty_row_to_facet_ref)
                .collect::<Vec<_>>();
            if refs.is_empty() {
                let Ok(db) = connection_or_signal_failure(&db) else {
                    return;
                };
                if let Err(error) = crate::db::delete_node_dirty_refs(&db, &rows) {
                    tracing::warn!(%error, "failed to delete invalid node dirty outbox rows");
                }
                delay = std::time::Duration::ZERO;
                continue;
            }

            let facets = refs
                .iter()
                .filter_map(|reference| facet_label(reference.facet))
                .collect::<Vec<_>>();
            match mark_dirty_once(refs.clone()).await {
                Ok(()) => {
                    for &facet in &facets {
                        pplx_space_metrics::reconcile::dirty_mark(facet, "ok");
                    }
                    let Ok(db) = connection_or_signal_failure(&db) else {
                        return;
                    };
                    if let Err(error) = crate::db::delete_node_dirty_refs(&db, &rows) {
                        tracing::warn!(%error, "failed to delete sent node dirty outbox rows");
                    }
                    delay = std::time::Duration::ZERO;
                }
                Err(error) => {
                    for &facet in &facets {
                        pplx_space_metrics::reconcile::dirty_mark(facet, "error");
                    }
                    delay = next_dirty_mark_delay(delay);
                    tracing::warn!(
                        %error,
                        retry_after_secs = delay.as_secs(),
                        "failed to flush node dirty outbox; retrying"
                    );
                }
            }
        }
    });
}

fn jittered_dirty_outbox_poll_interval(
    interval: std::time::Duration,
    jitter_max: std::time::Duration,
    tick: u64,
) -> std::time::Duration {
    let jitter_max_millis = jitter_max.as_millis() as u64;
    if jitter_max_millis == 0 {
        return interval;
    }
    let mut hasher = DefaultHasher::new();
    std::env::var("SPACE_NODE_ID")
        .unwrap_or_default()
        .hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    tick.hash(&mut hasher);
    interval + std::time::Duration::from_millis(hasher.finish() % jitter_max_millis)
}

fn sandbox_inventory_ref(sandbox_id: String) -> FacetRef {
    FacetRef {
        facet: ReconcileFacet::SandboxInventory as i32,
        element: Some(facet_ref::Element::SandboxId(sandbox_id)),
    }
}

fn network_policy_sandbox_ref(sandbox_id: SandboxId) -> FacetRef {
    FacetRef {
        facet: ReconcileFacet::NetworkPolicy as i32,
        element: Some(facet_ref::Element::NetworkPolicyTarget(
            NetworkPolicyTargetRef {
                target: Some(network_policy_target_ref::Target::SandboxId(
                    sandbox_id.to_string(),
                )),
            },
        )),
    }
}

async fn mark_dirty_once(refs: Vec<FacetRef>) -> Result<(), tonic::Status> {
    let mut client = pplx_space_node_api_client::node_ctl_lazy();
    client
        .mark_facet_dirty(Request::new(MarkFacetDirtyRequest { refs }))
        .await
        .map(|_| ())
}

fn next_dirty_mark_delay(previous: std::time::Duration) -> std::time::Duration {
    if previous.is_zero() {
        DIRTY_MARK_INITIAL_RETRY
    } else {
        previous.saturating_mul(2).min(DIRTY_MARK_MAX_RETRY)
    }
}

fn facet_label(facet: i32) -> Option<&'static str> {
    match ReconcileFacet::try_from(facet).unwrap_or(ReconcileFacet::Unspecified) {
        ReconcileFacet::NetworkPolicy => Some(FACET_NETWORK_POLICY),
        ReconcileFacet::SandboxInventory => Some(FACET_SANDBOX_INVENTORY),
        _ => None,
    }
}

fn dirty_row_to_facet_ref(row: &crate::db::NodeDirtyRefRow) -> Option<FacetRef> {
    let facet = ReconcileFacet::try_from(row.facet).ok()?;
    match (facet, row.element_kind.as_str()) {
        (ReconcileFacet::NetworkPolicy, "network_policy_sandbox") => {
            let sandbox_id = row.element_value.parse::<SandboxId>().ok()?;
            Some(network_policy_sandbox_ref(sandbox_id))
        }
        (ReconcileFacet::SandboxInventory, "sandbox_inventory") => {
            Some(sandbox_inventory_ref(row.element_value.clone()))
        }
        _ => {
            tracing::warn!(
                facet = row.facet,
                element_kind = %row.element_kind,
                element_value = %row.element_value,
                "dropping unknown node dirty outbox row"
            );
            None
        }
    }
}
