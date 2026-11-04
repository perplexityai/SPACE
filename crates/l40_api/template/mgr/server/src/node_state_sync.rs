use pplx_space_common_svc::{
    core::types::TemplateName,
    types::{FACET_NETWORK_POLICY, FACET_TEMPLATE_INVENTORY},
};
use pplx_space_node_api_client::proto::{
    FacetRef, MarkFacetDirtyRequest, NetworkPolicyTargetRef, ReconcileFacet, facet_ref,
    network_policy_target_ref,
};
use rusqlite::Connection;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use tokio::sync::Notify;
use tonic::Request;

const SKIP_NODE_PUBLISH_METADATA: &str = "x-space-skip-cluster-sync";
const DIRTY_MARK_INITIAL_RETRY: std::time::Duration = std::time::Duration::from_secs(1);
const DIRTY_MARK_MAX_RETRY: std::time::Duration = std::time::Duration::from_secs(300);
const DIRTY_OUTBOX_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const DIRTY_OUTBOX_POLL_JITTER_MAX: std::time::Duration = std::time::Duration::from_millis(500);
const DIRTY_OUTBOX_BATCH_LIMIT: usize = 100;

pub(crate) fn should_skip_node_publish<T>(request: &Request<T>) -> bool {
    request
        .metadata()
        .get(SKIP_NODE_PUBLISH_METADATA)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

pub(crate) fn mark_template_dirty(db: &Arc<Mutex<Connection>>, template: &str) {
    if template.trim().is_empty() {
        return;
    }
    if enqueue_template_inventory_dirty(db, template) {
        dirty_outbox_notify().notify_one();
    }
}

fn enqueue_template_inventory_dirty(db: &Arc<Mutex<Connection>>, template: &str) -> bool {
    let Some(db) = lock_db(db, "enqueue template inventory dirty") else {
        return false;
    };
    match crate::db::enqueue_node_dirty_template_inventory(&db, template) {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(
                template,
                %error,
                "failed to enqueue template inventory dirty ref"
            );
            false
        }
    }
}

fn dirty_outbox_notify() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

pub(crate) fn spawn_dirty_outbox_worker(db: Arc<Mutex<Connection>>) {
    tokio::spawn(async move {
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
                _ = tokio::time::sleep(sleep_for) => {}
                _ = dirty_outbox_notify().notified(), if delay.is_zero() => {}
            }

            let rows = {
                let Some(db) = lock_db(&db, "list template node dirty outbox") else {
                    delay = next_dirty_mark_delay(delay);
                    continue;
                };
                match crate::db::list_node_dirty_refs(&db, batch_limit) {
                    Ok(rows) => rows,
                    Err(error) => {
                        tracing::warn!(%error, "failed to read template node dirty outbox");
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
                if let Some(db) = lock_db(&db, "delete invalid template node dirty outbox rows")
                    && let Err(error) = crate::db::delete_node_dirty_refs(&db, &rows)
                {
                    tracing::warn!(
                        %error,
                        "failed to delete invalid template node dirty outbox rows"
                    );
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
                    let Some(db) = lock_db(&db, "delete sent template node dirty outbox rows")
                    else {
                        delay = next_dirty_mark_delay(delay);
                        continue;
                    };
                    if let Err(error) = crate::db::delete_node_dirty_refs(&db, &rows) {
                        tracing::warn!(%error, "failed to delete sent template node dirty outbox rows");
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
                        "failed to flush template node dirty outbox; retrying"
                    );
                }
            }
        }
    });
}

fn lock_db<'a>(
    db: &'a Arc<Mutex<Connection>>,
    context: &'static str,
) -> Option<MutexGuard<'a, Connection>> {
    match db.lock() {
        Ok(guard) => Some(guard),
        Err(_) => {
            tracing::warn!(context, "template-manager sqlite mutex poisoned");
            None
        }
    }
}

fn jittered_dirty_outbox_poll_interval(
    interval: std::time::Duration,
    jitter_max: std::time::Duration,
    tick: u64,
) -> std::time::Duration {
    let jitter_max_millis = u64::try_from(jitter_max.as_millis()).unwrap_or(u64::MAX);
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

fn template_inventory_ref(template: String) -> FacetRef {
    FacetRef {
        facet: ReconcileFacet::TemplateInventory as i32,
        element: Some(facet_ref::Element::Template(template)),
    }
}

fn network_policy_template_ref(template: TemplateName) -> FacetRef {
    FacetRef {
        facet: ReconcileFacet::NetworkPolicy as i32,
        element: Some(facet_ref::Element::NetworkPolicyTarget(
            NetworkPolicyTargetRef {
                target: Some(network_policy_target_ref::Target::Template(
                    template.to_string(),
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
        ReconcileFacet::TemplateInventory => Some(FACET_TEMPLATE_INVENTORY),
        _ => None,
    }
}

fn dirty_row_to_facet_ref(row: &crate::db::NodeDirtyRefRow) -> Option<FacetRef> {
    let facet = ReconcileFacet::try_from(row.facet).ok()?;
    match (facet, row.element_kind.as_str()) {
        (ReconcileFacet::NetworkPolicy, "network_policy_template") => {
            let template = TemplateName::parse(&row.element_value).ok()?;
            Some(network_policy_template_ref(template))
        }
        (ReconcileFacet::TemplateInventory, "template_inventory") => {
            Some(template_inventory_ref(row.element_value.clone()))
        }
        _ => {
            tracing::warn!(
                facet = row.facet,
                element_kind = %row.element_kind,
                element_value = %row.element_value,
                "dropping unknown template node dirty outbox row"
            );
            None
        }
    }
}
