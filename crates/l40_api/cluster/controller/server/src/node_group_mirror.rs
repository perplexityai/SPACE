//! Periodic reconciler that mirrors each kube Node's
//! `node-restriction.kubernetes.io/space-node-group` label into
//! `nodes.node_group_id`.
//!
//! The node label is the *scheduling* source of truth for node-group
//! membership (it is what the sandbox pod's required node affinity matches,
//! and it lives under the NodeRestriction-protected prefix so only the
//! control plane can set it). `nodes.node_group_id` is the reporting mirror:
//! it powers the admin API's `nodes_count`, the assign-org/delete residue
//! guards, and the (deferred) Direct-substrate placement filter. Per the
//! org-scoped-templates RFC the column is never set by hand — this loop is
//! the only writer.
//!
//! Mirror semantics per labeled node (matched to its `nodes` row via
//! `node_k8s.k8s_node_name`):
//!
//! - label present and names a known `node_groups.name` → mirror that
//!   group's id;
//! - label present but unknown → warn and mirror NULL (NULL is read as the
//!   default generic group everywhere, which is also what kube-scheduler
//!   does with a group label no pod affinity references);
//! - label absent → mirror NULL.
//!
//! Idempotent and cheap: one kube LIST per tick plus a single bulk UPDATE, in a
//! short transaction of its own, that only touches rows whose mirrored value
//! actually changed. Each of the
//! tick's I/O steps is bounded ([`LIST_TIMEOUT`], [`QUERY_TIMEOUT`],
//! [`WRITE_BACKSTOP`]), and a shutdown abandons everything that writes nothing
//! rather than waiting on it. A sent UPDATE is never abandoned on shutdown;
//! see [`apply_mirror`].
//!
//! A converged tick writes nothing and logs nothing, so liveness rides on
//! [`publish_metrics`] rather than on the log line. See
//! `pplx_space_metrics::node_group_mirror` for what each series means.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::kubernetes::NODE_GROUP_LABEL;
use pplx_space_common_core::types::NodeGroupId;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT;

const DEFAULT_TICK: Duration = Duration::from_secs(60);

/// Client-side bound on the tick's kube LIST. Without it there is no ceiling:
/// `kube_client::Config` leaves `read_timeout` unset in every constructor,
/// including the `Config::infer` this process's client is built from, and
/// nothing in SPACE sets one. Client-side rather than `ListParams::timeout`,
/// because a server-side `timeoutSeconds` can't bound a request the apiserver
/// never answers.
const LIST_TIMEOUT: Duration = Duration::from_secs(15);

/// Client-side bound on the `node_groups` read and on the write's setup, pool
/// acquire included. Above the application roles' server-side
/// `statement_timeout` (10 s by `bootstrap-roles` default), so a slow statement
/// usually reports Postgres's own error, though a slow acquire or a
/// prepare-then-execute pair can still make this bound fire first. It is for
/// what a server-side timeout can't see, such as a connection that stopped
/// answering. Neither step writes anything, so abandoning one is safe.
/// Abandoning the setup closes its connection, which ends any transaction it
/// opened; on a connection that stopped answering, that end can wait for the
/// server to notice, holding whatever relation locks a first-use prepare took.
const QUERY_TIMEOUT: Duration = Duration::from_secs(12);

/// Server-side `statement_timeout` on the bulk UPDATE's transaction, set on
/// that transaction only. It bounds the UPDATE, not the commit: Postgres
/// disables the statement timer before committing, so neither the commit
/// record's WAL flush nor a wait for a synchronous replica has a server-side
/// bound. It counts row-lock waits too, so contention on
/// `nodes` rows can fail a tick the role's own `statement_timeout` would have
/// let finish (a shorter role `lock_timeout`, where set, fails it sooner);
/// the next tick retries.
const WRITE_STATEMENT_TIMEOUT: Duration = Duration::from_secs(3);

/// Server-side `idle_in_transaction_session_timeout` on the same transaction:
/// a write session left waiting for a message the network never delivers is
/// terminated, which rolls it back.
const WRITE_IDLE_TIMEOUT: Duration = Duration::from_secs(3);

/// Round trips the bounds allow for once [`store::begin_mirror_write`] has
/// returned. The write makes two: `begin_mirror_write` prepares the UPDATE on
/// the same connection just before, so its Bind/Execute is a statement-cache
/// hit, and then the COMMIT. The third is margin.
const WRITE_ROUND_TRIPS: u64 = 3;

/// Client-side bound on the write once its setup has returned.
///
/// Dropping a client future doesn't cancel its statement in Postgres, so a
/// client-side bound alone could abandon a write that later commits, after a
/// later tick's or after the next leader's. The two server-side bounds close
/// most of that: each round trip leaves the session idle at most
/// [`WRITE_IDLE_TIMEOUT`] waiting for its messages and runs the UPDATE for at
/// most [`WRITE_STATEMENT_TIMEOUT`], so within
/// `WRITE_ROUND_TRIPS * (WRITE_IDLE_TIMEOUT + WRITE_STATEMENT_TIMEOUT)` of setup
/// Postgres has either rolled the transaction back or received its COMMIT.
/// This backstop is longer, so by the time it fires the write can normally no
/// longer commit, and the tick may drop it.
///
/// The window assumes each round trip's messages arrive together, as sqlx
/// flushes them. Between an Execute finishing and the Sync that ends its round
/// trip neither bound runs, so a round trip whose tail the network delays
/// (a lost and retransmitted segment) can stretch it, and a COMMIT sent just
/// before the backstop fires can still land after it. So can a COMMIT that
/// Postgres received in time but whose commit stalls, on its WAL flush or on
/// a synchronous replica, which no server-side bound covers.
///
/// Those cases hold for every lease handoff. The backstop can fire inside a
/// clean shutdown drain too, when shutdown lands late in a write on a
/// connection that stopped answering, and a lease left to expire or *lost* to
/// a missed renewal (a standby may acquire `RENEW_DEADLINE_MARGIN`, 12 s,
/// after this process stops acting) can change hands inside the window as
/// well. So on a slow connection an old write can still commit after the new
/// leader's first tick. That is an exception to the leader-election module's
/// "ticks finish before a standby acquires", accepted here because the mirror
/// is idempotent: the stale values are seconds old, and the next tick
/// recomputes the whole fleet.
const WRITE_BACKSTOP: Duration = Duration::from_secs(20);

// The whole tick, at its worst, fits inside one interval, so a wedged
// dependency costs one tick's `tick_error` per interval rather than an
// unbounded stall that books nothing.
const _: () = assert!(
    LIST_TIMEOUT.as_millis() + 2 * QUERY_TIMEOUT.as_millis() + WRITE_BACKSTOP.as_millis()
        < DEFAULT_TICK.as_millis(),
    "node_group_mirror's I/O bounds must fit inside one tick"
);
// Postgres normally settles the write before the client gives up on it, and on
// a connection that answers, the write's server-side statement time fits inside
// a shutdown drain that waits for it. Round-trip latency and a COMMIT's commit
// work come on top; a drain they overrun leaves the lease to its TTL, which is
// the safe direction.
const _: () = assert!(
    WRITE_ROUND_TRIPS as u128
        * (WRITE_IDLE_TIMEOUT.as_millis() + WRITE_STATEMENT_TIMEOUT.as_millis())
        < WRITE_BACKSTOP.as_millis()
        && WRITE_ROUND_TRIPS as u128 * WRITE_STATEMENT_TIMEOUT.as_millis()
            < SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
    "the write's server-side bounds must settle it before the backstop and the drain"
);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub tick: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::truthy_default("SPACE_NODE_GROUP_MIRROR_ENABLED", default.enabled),
            tick: default.tick,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum MirrorError {
    #[error("kube list nodes failed: {0}")]
    Kube(String),
    #[error("kube list nodes timed out after {LIST_TIMEOUT:?}")]
    ListTimedOut,
    #[error("node-group mirror query failed: {0}")]
    Db(#[from] sqlx::Error),
    /// Carries which step outran its client-side bound.
    #[error("node-group mirror {0} timed out")]
    QueryTimedOut(&'static str),
}

impl MirrorError {
    fn kind(&self) -> pplx_space_metrics::node_group_mirror::TickErrorKind {
        use pplx_space_metrics::node_group_mirror::TickErrorKind;
        match self {
            Self::Kube(_) | Self::ListTimedOut => TickErrorKind::List,
            Self::Db(_) | Self::QueryTimedOut(_) => TickErrorKind::Query,
        }
    }
}

/// Per-tick outcome, for the log line and the metrics.
#[derive(Debug, Default)]
struct MirrorOutcome {
    /// Kube Nodes matching the node-pool label selector, whether or not they
    /// carry a node-group label. Published as `node_group_mirror.observed`.
    observed: usize,
    /// `nodes` rows whose `node_group_id` actually changed. Published as
    /// `node_group_mirror.rows_updated`.
    updated: u64,
    /// Nodes whose label names a group with no `node_groups` row (mirrored
    /// as NULL until the row exists or the label is fixed). Published as
    /// `node_group_mirror.unknown_groups`.
    unknown_groups: usize,
}

pub async fn run(
    config: ReconcilerConfig,
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "node-group-mirror reconciler is disabled \
             (SPACE_NODE_GROUP_MIRROR_ENABLED unset); skipping"
        );
        shutdown_token.cancelled().await;
        return;
    }

    run_loop(config, KubeMirrorBackend { pool, kube }, shutdown_token).await;
}

async fn run_loop<B: MirrorBackend>(
    config: ReconcilerConfig,
    backend: B,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        "starting node-group-mirror reconciler"
    );
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // The tick watches the token, not just this loop: its read bounds
                // (`LIST_TIMEOUT + QUERY_TIMEOUT`) still run far past
                // `SHUTDOWN_DRAIN_TIMEOUT`, and a drain that overruns abandons
                // the leader lease to its TTL instead of releasing it. An
                // abandoned tick books neither a completion nor an error.
                let Some(result) = mirror_node_groups(&backend, &shutdown_token).await else {
                    tracing::info!(
                        "node_group_mirror: shutdown requested; abandoning the tick in flight"
                    );
                    break;
                };
                publish_metrics(&result);
                match result {
                    Ok(outcome) if outcome.updated > 0 || outcome.unknown_groups > 0 => {
                        tracing::info!(
                            observed = outcome.observed,
                            updated = outcome.updated,
                            unknown_groups = outcome.unknown_groups,
                            "node_group_mirror: mirrored node-group labels"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "node_group_mirror: tick failed");
                    }
                }
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    tracing::info!("node-group-mirror reconciler stopping");
}

/// Publish one tick's result.
///
/// Two invariants, both load-bearing; `pplx_space_metrics::node_group_mirror`
/// carries the reasoning behind each.
///
/// 1. **Every** successful tick books `tick_completed`, no-ops included.
/// 2. The gauges are published inside the `Ok` arm and nowhere else, so no
///    failed tick can publish a *false* zero — one a `sustained > 0` monitor
///    over `unknown_groups` would read as "converged".
///
/// **What invariant 2 buys is the absence of a wrong value, not a monitor that
/// keeps evaluating.** Withholding is withholding: under a *standing* failure of
/// either kind the gauge eventually goes no-data, and a `sustained > 0` monitor
/// does not fire on no-data any more than it fires on zero. The series that
/// stays meaningful across a standing failure is `tick_completed` — by its
/// absence — so monitor it alongside the gauge.
///
/// The two error arms differ in whether the discarded measurement was *real*,
/// not in what the monitor does. A failed LIST or `node_groups` read measured
/// nothing, so withholding is the only correct answer. A failed bulk UPDATE
/// counted `observed` and `unknown_groups` in full before `apply_mirror` ran, so
/// a valid measurement goes in the bin with the `Err`. Publishing on that one
/// arm would need `MirrorError` to carry the partial outcome; until something
/// needs it, one rule is easier to keep true than two.
fn publish_metrics(result: &Result<MirrorOutcome, MirrorError>) {
    use pplx_space_metrics::node_group_mirror as m;

    match result {
        Ok(outcome) => {
            m::tick_completed();
            m::observed(outcome.observed);
            m::unknown_group_nodes(outcome.unknown_groups);
            m::rows_updated(outcome.updated);
        }
        Err(err) => m::tick_error(err.kind()),
    }
}

/// One selector-matching kube Node, reduced to what the mirror reads.
#[derive(Debug, Clone)]
struct ObservedNode {
    name: String,
    /// The node-group label's value, if the Node carries one.
    node_group: Option<String>,
}

/// The tick's three I/O steps, behind a trait so the loop can be driven
/// against a fake. Production impl: [`KubeMirrorBackend`].
trait MirrorBackend: Send + Sync + 'static {
    /// LIST the selector-matching kube Nodes, dropping any without a name.
    fn list_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ObservedNode>, MirrorError>> + Send;

    /// Read `node_groups` as `name -> id`.
    fn node_groups(
        &self,
    ) -> impl std::future::Future<Output = Result<HashMap<String, NodeGroupId>, MirrorError>> + Send;

    /// See [`apply_mirror`].
    fn apply_mirror(
        &self,
        node_names: &[String],
        group_ids: &[Option<NodeGroupId>],
        shutdown_token: &CancellationToken,
    ) -> impl std::future::Future<Output = Option<Result<u64, MirrorError>>> + Send;
}

struct KubeMirrorBackend {
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
}

impl MirrorBackend for KubeMirrorBackend {
    async fn list_nodes(&self) -> Result<Vec<ObservedNode>, MirrorError> {
        // Same node universe the kube-node-sync registration path uses: every
        // node carrying the sandbox node-selector label.
        let pod_config = &self.kube.k8s.sandbox_pod_config;
        let label_selector = format!(
            "{}={}",
            pod_config.sandbox_node_selector_key, pod_config.sandbox_node_selector_value
        );
        list_labeled_nodes(self.kube.k8s.client.clone(), &label_selector).await
    }

    async fn node_groups(&self) -> Result<HashMap<String, NodeGroupId>, MirrorError> {
        load_node_groups(&self.pool).await
    }

    async fn apply_mirror(
        &self,
        node_names: &[String],
        group_ids: &[Option<NodeGroupId>],
        shutdown_token: &CancellationToken,
    ) -> Option<Result<u64, MirrorError>> {
        apply_mirror(&self.pool, node_names, group_ids, shutdown_token).await
    }
}

/// LIST the Nodes matching `label_selector`, reduced to what the mirror reads.
///
/// A live LIST rather than a scan of the shared Node reflector, on purpose:
/// `observed` is documented as the apiserver's current view, one that can't go
/// stale the way a store whose watch has stalled does. Metadata-only, because
/// the mirror reads nothing past a Node's name and labels.
async fn list_labeled_nodes(
    client: kube::Client,
    label_selector: &str,
) -> Result<Vec<ObservedNode>, MirrorError> {
    use kube::Api;
    use kube::api::ListParams;

    let nodes_api: Api<k8s_openapi::api::core::v1::Node> = Api::all(client);
    let node_list = nodes_api
        .list_metadata(&ListParams::default().labels(label_selector))
        .await
        .map_err(|err| MirrorError::Kube(err.to_string()))?;
    Ok(node_list
        .items
        .into_iter()
        .filter_map(|node| {
            let name = node.metadata.name?;
            let node_group = node
                .metadata
                .labels
                .and_then(|mut labels| labels.remove(NODE_GROUP_LABEL));
            Some(ObservedNode { name, node_group })
        })
        .collect())
}

/// One tick. `None` when shutdown arrived before the UPDATE was sent.
///
/// Shutdown abandons everything that writes nothing — the reads and the
/// write's setup — but never a sent UPDATE; see [`apply_mirror`].
async fn mirror_node_groups<B: MirrorBackend>(
    backend: &B,
    shutdown_token: &CancellationToken,
) -> Option<Result<MirrorOutcome, MirrorError>> {
    let resolved = match shutdown_token
        .run_until_cancelled(resolve_mirror(backend))
        .await?
    {
        Ok(resolved) => resolved,
        Err(err) => return Some(Err(err)),
    };
    let ResolvedMirror {
        mut outcome,
        node_names,
        group_ids,
    } = resolved;
    if node_names.is_empty() {
        return Some(Ok(outcome));
    }

    let written = backend
        .apply_mirror(&node_names, &group_ids, shutdown_token)
        .await?;
    Some(written.map(|updated| {
        outcome.updated = updated;
        outcome
    }))
}

/// The tick's reads and the label resolution, ready for the write.
struct ResolvedMirror {
    /// Everything but `updated`, which the write fills in.
    outcome: MirrorOutcome,
    node_names: Vec<String>,
    group_ids: Vec<Option<NodeGroupId>>,
}

async fn resolve_mirror<B: MirrorBackend>(backend: &B) -> Result<ResolvedMirror, MirrorError> {
    let nodes = bounded(
        LIST_TIMEOUT,
        MirrorError::ListTimedOut,
        backend.list_nodes(),
    )
    .await?;
    let groups = bounded(
        QUERY_TIMEOUT,
        MirrorError::QueryTimedOut("node_groups read"),
        backend.node_groups(),
    )
    .await?;

    let mut outcome = MirrorOutcome::default();
    let mut node_names: Vec<String> = Vec::with_capacity(nodes.len());
    let mut group_ids: Vec<Option<NodeGroupId>> = Vec::with_capacity(nodes.len());
    for node in nodes {
        let resolved = match node.node_group.as_deref() {
            Some(group_name) => match groups.get(group_name) {
                Some(id) => Some(*id),
                None => {
                    tracing::warn!(
                        node = %node.name,
                        node_group = %group_name,
                        "node_group_mirror: node labeled with unknown node group; \
                         mirroring NULL until the node_groups row exists"
                    );
                    outcome.unknown_groups += 1;
                    None
                }
            },
            None => None,
        };
        node_names.push(node.name);
        group_ids.push(resolved);
        outcome.observed += 1;
    }

    Ok(ResolvedMirror {
        outcome,
        node_names,
        group_ids,
    })
}

/// Run one of the tick's I/O steps under `timeout`, failing with `on_timeout`
/// once it elapses.
async fn bounded<T>(
    timeout: Duration,
    on_timeout: MirrorError,
    step: impl std::future::Future<Output = Result<T, MirrorError>>,
) -> Result<T, MirrorError> {
    tokio::time::timeout(timeout, step)
        .await
        .map_err(|_| on_timeout)?
}

/// Run [`store::mirror_worker_groups`] in its own committed transaction, and
/// return the number of rows that changed.
///
/// Returns `None`, having written nothing, when `shutdown_token` fires before
/// the UPDATE is sent: during the pool acquire or
/// [`store::begin_mirror_write`], which together get one [`QUERY_TIMEOUT`]. A sent UPDATE is never abandoned on
/// shutdown: dropping it wouldn't cancel it in Postgres, so a clean drain could
/// release the lease while it was still able to commit after the next leader's
/// write. On a connection that answers, [`WRITE_STATEMENT_TIMEOUT`] settles it
/// inside the drain; otherwise [`WRITE_BACKSTOP`] gives up on it.
async fn apply_mirror(
    pool: &PgPool,
    node_names: &[String],
    group_ids: &[Option<NodeGroupId>],
    shutdown_token: &CancellationToken,
) -> Option<Result<u64, MirrorError>> {
    apply_mirror_within(
        pool,
        node_names,
        group_ids,
        shutdown_token,
        WriteBounds::DEFAULT,
    )
    .await
}

/// The write's two client-side bounds. Only tests pass anything but
/// [`Self::DEFAULT`]: Postgres's own bounds fire well before these on a live
/// connection, so a test makes one fire first by shortening it.
#[derive(Debug, Clone, Copy)]
struct WriteBounds {
    /// Pool acquire plus [`store::begin_mirror_write`].
    setup: Duration,
    /// The UPDATE and COMMIT, once setup has returned.
    backstop: Duration,
}

impl WriteBounds {
    const DEFAULT: Self = Self {
        setup: QUERY_TIMEOUT,
        backstop: WRITE_BACKSTOP,
    };
}

/// [`apply_mirror`] under explicit client-side bounds.
async fn apply_mirror_within(
    pool: &PgPool,
    node_names: &[String],
    group_ids: &[Option<NodeGroupId>],
    shutdown_token: &CancellationToken,
    bounds: WriteBounds,
) -> Option<Result<u64, MirrorError>> {
    let setup_deadline = tokio::time::Instant::now() + bounds.setup;
    let setup_left = || setup_deadline.saturating_duration_since(tokio::time::Instant::now());
    // Abandoning an acquire holds nothing, so shutdown may: one waiting out the
    // pool's acquire timeout would otherwise spend the drain.
    let acquire = bounded(
        setup_left(),
        MirrorError::QueryTimedOut("bulk update setup"),
        async { Ok(pool.acquire().await?) },
    );
    let mut conn = match shutdown_token.run_until_cancelled(acquire).await? {
        Ok(conn) => conn,
        Err(err) => return Some(Err(err)),
    };
    let written = write_mirror(
        &mut conn,
        node_names,
        group_ids,
        setup_left(),
        bounds.backstop,
        shutdown_token,
    )
    .await;
    // A write that didn't succeed may have left the connection mid-transaction
    // in a way sqlx doesn't roll back: a BEGIN dropped before its reply leaves
    // a transaction open on the server that sqlx isn't tracking, and the pool's
    // release check is a ping. Close it rather than hand that to another loop.
    if !matches!(written, Some(Ok(_))) {
        conn.close_on_drop();
    }
    written
}

/// [`apply_mirror`] on an acquired connection: [`store::begin_mirror_write`]
/// within `setup_budget`, abandoned on shutdown, then the UPDATE and COMMIT
/// under `backstop`, not abandoned.
async fn write_mirror(
    conn: &mut sqlx::PgConnection,
    node_names: &[String],
    group_ids: &[Option<NodeGroupId>],
    setup_budget: Duration,
    backstop: Duration,
    shutdown_token: &CancellationToken,
) -> Option<Result<u64, MirrorError>> {
    let setup = bounded(
        setup_budget,
        MirrorError::QueryTimedOut("bulk update setup"),
        async {
            Ok(
                store::begin_mirror_write(conn, WRITE_STATEMENT_TIMEOUT, WRITE_IDLE_TIMEOUT)
                    .await?,
            )
        },
    );
    let mut tx = match shutdown_token.run_until_cancelled(setup).await? {
        Ok(tx) => tx,
        Err(err) => return Some(Err(err)),
    };
    let write = async {
        let updated = store::mirror_worker_groups(&mut tx, node_names, group_ids).await?;
        tx.commit().await?;
        Ok(updated)
    };
    Some(bounded(backstop, MirrorError::QueryTimedOut("bulk update"), write).await)
}

/// Read `node_groups` as `name -> id`.
///
/// On its own connection, closed unless the read completes: [`QUERY_TIMEOUT`]
/// or shutdown can drop it mid-query, and a connection dropped that way goes
/// back through a release ping with no time limit, holding its pool slot for
/// as long as a connection that stopped answering takes to fail.
async fn load_node_groups(pool: &PgPool) -> Result<HashMap<String, NodeGroupId>, MirrorError> {
    let mut conn = CloseUnlessReleased(Some(pool.acquire().await?));
    let groups = store::group_ids_by_name(&mut **conn.get()).await?;
    conn.release();
    Ok(groups)
}

/// A pooled connection that is closed when dropped, unless [`Self::release`]
/// hands it back to the pool first.
struct CloseUnlessReleased(Option<sqlx::pool::PoolConnection<sqlx::Postgres>>);

impl CloseUnlessReleased {
    fn get(&mut self) -> &mut sqlx::pool::PoolConnection<sqlx::Postgres> {
        self.0
            .as_mut()
            .expect("the connection is held until release or drop")
    }

    /// Return the connection to the pool as usual.
    fn release(mut self) {
        drop(self.0.take());
    }
}

impl Drop for CloseUnlessReleased {
    fn drop(&mut self) {
        if let Some(conn) = self.0.as_mut() {
            conn.close_on_drop();
        }
    }
}

mod store;

/// Postgres-backed regression tests for the mirror's bulk UPDATE (label →
/// column propagation, NULL-clearing, steady-state no-op behavior, and the
/// write's server-side bound and cancellable setup), plus fake-backend tests
/// for the tick's bounds and what a shutdown abandons or waits for.
#[cfg(test)]
mod tests;
