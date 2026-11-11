//! Bounds cluster-wide observed-state projection tables.
//!
//! Node-published observed state is an operational cache: it lets API/debug
//! paths answer "what does the node currently report?" without calling every
//! node. It is not the durable recovery source for suspended sandboxes. Durable
//! restore comes from `sandboxes`, `snapshots`, `snapshot_storage_locations`,
//! and the backing object store.
//!
//! The projection tables are also high-churn: nodes publish full snapshots
//! repeatedly. This reconciler bounds their cardinality by deleting cold cache
//! rows whose source-of-truth row is either gone or old enough that local-node
//! observation is no longer part of the wake contract.

use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::PositiveDuration;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::env_config;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_STALE_SUSPENDED_AGE_SECS: u64 = 7 * 24 * 60 * 60;
pub(crate) const DEFAULT_ORPHAN_AGE_SECS: u64 = 24 * 60 * 60;
pub(crate) const DEFAULT_MAX_PER_TICK: u32 = 5_000;
/// Rows examined per windowed sweep query. Eligibility is mostly a per-row
/// probe into another table (`sandboxes` by PK, or the facet existence
/// checks), so an unwindowed pass costs one probe per candidate row per tick,
/// even when it deletes nothing. Each windowed query examines at most this many
/// rows of a primary-key range; as tables grow, query cost stays bounded while
/// the lap time grows (as with warm-pool receipt backstop sweeps).
pub(crate) const DEFAULT_SCAN_WINDOW: i64 = 10_000;
/// Windows per table per tick. At these defaults the sweep examines up to
/// 100k rows per five-minute tick; size the grace windows for the table's lap time.
pub(crate) const DEFAULT_WINDOWS_PER_TICK: u32 = 10;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Defaults on so projection tables remain bounded; the
    /// env override is an operational kill switch.
    pub enabled: bool,
    pub tick: Duration,
    /// Suspended sandboxes older than this lose their node-observed cache row.
    /// The sandbox row, snapshots, and S3 data are untouched.
    pub stale_suspended_age: PositiveDuration,
    /// Observed rows whose source-of-truth row is gone are retained this long
    /// for short-term debugging before being removed.
    pub orphan_age: PositiveDuration,
    /// Upper bound per candidate class in one tick.
    pub max_per_tick: u32,
    /// Rows examined per windowed sweep query; see [`DEFAULT_SCAN_WINDOW`].
    pub scan_window: i64,
    /// Windowed sweep queries per table per tick; see
    /// [`DEFAULT_WINDOWS_PER_TICK`].
    pub windows_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: DEFAULT_TICK,
            stale_suspended_age: PositiveDuration::new_panic(Duration::from_secs(
                DEFAULT_STALE_SUSPENDED_AGE_SECS,
            )),
            orphan_age: PositiveDuration::new_panic(Duration::from_secs(DEFAULT_ORPHAN_AGE_SECS)),
            max_per_tick: DEFAULT_MAX_PER_TICK,
            scan_window: DEFAULT_SCAN_WINDOW,
            windows_per_tick: DEFAULT_WINDOWS_PER_TICK,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::destructive_gate("SPACE_OBSERVED_PROJECTION_GC_ENABLED"),
            ..default
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PruneRequest {
    pub stale_suspended_age: PositiveDuration,
    pub orphan_age: PositiveDuration,
    pub max_per_tick: u32,
    pub scan_window: i64,
    pub windows_per_tick: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickReport {
    pub stale_suspended_observed: u64,
    pub orphaned_observed: u64,
    pub orphaned_facet_status: u64,
    pub orphaned_facet_conditions: u64,
}

impl TickReport {
    fn total(self) -> u64 {
        self.stale_suspended_observed
            .saturating_add(self.orphaned_observed)
            .saturating_add(self.orphaned_facet_status)
            .saturating_add(self.orphaned_facet_conditions)
    }
}

pub trait ProjectionGcSink: Send + Sync + 'static {
    fn prune_observed_projections(
        &self,
        request: PruneRequest,
    ) -> impl std::future::Future<Output = Result<TickReport, sqlx::Error>> + Send;
}

pub struct ObservedProjectionDbStore {
    pool: PgPool,
    /// Cross-tick sweep positions; see [`DEFAULT_SCAN_WINDOW`]. Reset to the
    /// table start on process restart, which only shortens the current lap.
    laps: Mutex<GcLaps>,
}

#[derive(Default)]
struct GcLaps {
    /// Shared by the stale-suspended and orphaned-observed sweeps — both
    /// scan `sandbox_observed_states`, so each window runs both deletes.
    observed: Option<ObservedLap>,
    facet: Option<FacetLap>,
}

/// One in-progress sweep lap over `sandbox_observed_states`: resume position
/// plus the high-water id captured when the lap started. Sandbox ids are
/// UUIDv7, so pinning the lap to the ids that existed at its start keeps lap
/// length independent of the insert rate; rows born mid-lap wait for the next
/// lap, which is inside every eligibility grace here. `None` in [`GcLaps`]
/// means the next window starts a fresh lap.
#[derive(Clone, Copy)]
struct ObservedLap {
    cursor: Uuid,
    high_water: Uuid,
}

/// One in-progress sweep lap over `node_facet_observed_status`, keyed by its
/// composite primary key `(node_id, facet, element)` via row-wise
/// comparisons. Unlike UUIDv7 ids the key space is not time-ordered, so rows
/// born mid-lap can land behind the cursor and wait for the next lap — which
/// is inside the orphan-age grace, same as above.
#[derive(Clone)]
struct FacetLap {
    cursor: FacetKey,
    high_water: FacetKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FacetKey {
    node_id: NodeId,
    facet: String,
    element: String,
}

impl FacetKey {
    /// Sorts at or before every real key: `node_id` is a FK to `nodes`, so a
    /// live row with the nil UUID (let alone empty facet and element) cannot
    /// exist.
    fn origin() -> Self {
        Self {
            node_id: NodeId::from_uuid(Uuid::nil()),
            facet: String::new(),
            element: String::new(),
        }
    }
}

struct WindowRange<K> {
    last: K,
    exhausted: bool,
}

impl ObservedProjectionDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            laps: Mutex::new(GcLaps::default()),
        }
    }

    fn observed_lap(&self) -> Option<ObservedLap> {
        self.laps.lock().expect("gc lap mutex poisoned").observed
    }

    fn store_observed_lap(&self, lap: Option<ObservedLap>) {
        self.laps.lock().expect("gc lap mutex poisoned").observed = lap;
    }

    fn facet_lap(&self) -> Option<FacetLap> {
        self.laps
            .lock()
            .expect("gc lap mutex poisoned")
            .facet
            .clone()
    }

    fn store_facet_lap(&self, lap: Option<FacetLap>) {
        self.laps.lock().expect("gc lap mutex poisoned").facet = lap;
    }

    /// The newest observed-row id, used as a lap's high-water bound. `None`
    /// means the table is empty and there is nothing to sweep.
    async fn newest_observed_sandbox_id(&self) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(sql!(
            "SELECT sandbox_id
             FROM sandbox_observed_states
             ORDER BY sandbox_id DESC
             LIMIT 1"
        ))
        .fetch_optional(&self.pool)
        .await
    }

    /// Measure the next sweep window over `sandbox_observed_states`: the id
    /// closing the half-open range `(cursor, last]` containing at most
    /// `window` rows inside `(cursor, through]`, from a primary-key range
    /// scan (the one place ordering is load-bearing: the cursor is a resume
    /// position, not a fairness policy). `None` means the lap reached
    /// `through`.
    async fn observed_window_range(
        &self,
        cursor: Uuid,
        through: Uuid,
        window: i64,
    ) -> Result<Option<WindowRange<Uuid>>, sqlx::Error> {
        let ids: Vec<Uuid> = sqlx::query_scalar(sql!(
            "SELECT sandbox_id
             FROM sandbox_observed_states
             WHERE sandbox_id > $1
               AND sandbox_id <= $2
             ORDER BY sandbox_id
             LIMIT $3"
        ))
        .bind(cursor)
        .bind(through)
        .bind(window.max(0))
        .fetch_all(&self.pool)
        .await?;

        let Some(last) = ids.last().copied() else {
            return Ok(None);
        };
        Ok(Some(WindowRange {
            last,
            exhausted: (ids.len() as i64) < window,
        }))
    }

    async fn newest_facet_key(&self) -> Result<Option<FacetKey>, sqlx::Error> {
        let row: Option<(NodeId, String, String)> = sqlx::query_as(sql!(
            "SELECT node_id, facet, element
             FROM node_facet_observed_status
             ORDER BY node_id DESC, facet DESC, element DESC
             LIMIT 1"
        ))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(node_id, facet, element)| FacetKey {
            node_id,
            facet,
            element,
        }))
    }

    /// Facet-table counterpart of [`Self::observed_window_range`], row-wise
    /// over the composite primary key.
    async fn facet_window_range(
        &self,
        cursor: &FacetKey,
        through: &FacetKey,
        window: i64,
    ) -> Result<Option<WindowRange<FacetKey>>, sqlx::Error> {
        let rows: Vec<(NodeId, String, String)> = sqlx::query_as(sql!(
            "SELECT node_id, facet, element
             FROM node_facet_observed_status
             WHERE (node_id, facet, element) > ROW($1, $2, $3)
               AND (node_id, facet, element) <= ROW($4, $5, $6)
             ORDER BY node_id, facet, element
             LIMIT $7"
        ))
        .bind(cursor.node_id)
        .bind(&cursor.facet)
        .bind(&cursor.element)
        .bind(through.node_id)
        .bind(&through.facet)
        .bind(&through.element)
        .bind(window.max(0))
        .fetch_all(&self.pool)
        .await?;

        let exhausted = (rows.len() as i64) < window;
        let Some((node_id, facet, element)) = rows.into_iter().last() else {
            return Ok(None);
        };
        Ok(Some(WindowRange {
            last: FacetKey {
                node_id,
                facet,
                element,
            },
            exhausted,
        }))
    }
}

fn secs_i64(duration: PositiveDuration) -> i64 {
    i64::try_from(duration.get().as_secs()).unwrap_or(i64::MAX)
}

fn remaining(cap: u32, spent: i64) -> i64 {
    i64::from(cap).saturating_sub(spent).max(0)
}

type DeleteReportRow = (i64, i64, i64);

impl ProjectionGcSink for ObservedProjectionDbStore {
    async fn prune_observed_projections(
        &self,
        request: PruneRequest,
    ) -> Result<TickReport, sqlx::Error> {
        let mut stale_deleted: i64 = 0;
        let mut orphaned_deleted: i64 = 0;
        let mut facet_status_deleted: i64 = 0;
        let mut facet_conditions_deleted: i64 = 0;

        // Observed-table lap: both observed sweeps share each window.
        for _ in 0..request.windows_per_tick {
            let stale_budget = remaining(request.max_per_tick, stale_deleted);
            let orphan_budget = remaining(request.max_per_tick, orphaned_deleted);
            if stale_budget == 0 && orphan_budget == 0 {
                break;
            }
            let lap = match self.observed_lap() {
                Some(lap) => lap,
                None => match self.newest_observed_sandbox_id().await? {
                    Some(high_water) => {
                        let lap = ObservedLap {
                            cursor: Uuid::nil(),
                            high_water,
                        };
                        self.store_observed_lap(Some(lap));
                        lap
                    }
                    None => break,
                },
            };
            let Some(range) = self
                .observed_window_range(lap.cursor, lap.high_water, request.scan_window)
                .await?
            else {
                self.store_observed_lap(None);
                break;
            };

            let stale = delete_stale_suspended_observed(
                &self.pool,
                request,
                stale_budget,
                lap.cursor,
                range.last,
            )
            .await?;
            let orphaned = delete_orphaned_observed(
                &self.pool,
                request,
                orphan_budget,
                lap.cursor,
                range.last,
            )
            .await?;
            stale_deleted += stale.0;
            orphaned_deleted += orphaned.0;
            facet_status_deleted += stale.1.saturating_add(orphaned.1);
            facet_conditions_deleted += stale.2.saturating_add(orphaned.2);

            if range.exhausted {
                self.store_observed_lap(None);
                break;
            }
            self.store_observed_lap(Some(ObservedLap {
                cursor: range.last,
                high_water: lap.high_water,
            }));
        }

        // Facet-table lap. Budgeted by its own deletions only: the observed
        // sweeps above also remove facet-status rows as side effects of each
        // stale observed row, and charging those here would zero this budget
        // under ordinary suspend churn — the facet cursor would then never
        // advance. `facet_status_deleted` keeps aggregating everything for
        // the tick report.
        let mut facet_lap_deleted: i64 = 0;
        for _ in 0..request.windows_per_tick {
            let facet_budget = remaining(request.max_per_tick, facet_lap_deleted);
            if facet_budget == 0 {
                break;
            }
            let lap = match self.facet_lap() {
                Some(lap) => lap,
                None => match self.newest_facet_key().await? {
                    Some(high_water) => {
                        let lap = FacetLap {
                            cursor: FacetKey::origin(),
                            high_water,
                        };
                        self.store_facet_lap(Some(lap.clone()));
                        lap
                    }
                    None => break,
                },
            };
            let Some(range) = self
                .facet_window_range(&lap.cursor, &lap.high_water, request.scan_window)
                .await?
            else {
                self.store_facet_lap(None);
                break;
            };

            let facets =
                delete_orphaned_facets(&self.pool, request, facet_budget, &lap.cursor, &range.last)
                    .await?;
            facet_lap_deleted += facets.1;
            facet_status_deleted += facets.1;
            facet_conditions_deleted += facets.2;

            if range.exhausted {
                self.store_facet_lap(None);
                break;
            }
            self.store_facet_lap(Some(FacetLap {
                cursor: range.last,
                high_water: lap.high_water,
            }));
        }

        Ok(TickReport {
            stale_suspended_observed: u64::try_from(stale_deleted).unwrap_or(0),
            orphaned_observed: u64::try_from(orphaned_deleted).unwrap_or(0),
            orphaned_facet_status: u64::try_from(facet_status_deleted).unwrap_or(0),
            orphaned_facet_conditions: u64::try_from(facet_conditions_deleted).unwrap_or(0),
        })
    }
}

/// One windowed sweep of stale-suspended observed rows over the primary-key
/// range `(from, to]`. Candidate selection is deliberately unordered and
/// window-bounded: eligibility is a per-row probe (a `sandboxes` PK join plus
/// a facet existence check), so an unwindowed pass paid one probe per
/// candidate row per tick even when it found nothing new; the previous
/// ORDER BY + MATERIALIZED candidate CTE additionally forced the full
/// eligible set to be evaluated and sorted before LIMIT applied. Pick order
/// cannot matter for a GC: each window deletes what it selects, and the lap
/// cursor guarantees every row is revisited within one lap.
async fn delete_stale_suspended_observed(
    pool: &PgPool,
    request: PruneRequest,
    limit: i64,
    from: Uuid,
    to: Uuid,
) -> Result<DeleteReportRow, sqlx::Error> {
    sqlx::query_as(sql!(
        // `stale` keeps MATERIALIZED for its multiple downstream references;
        // its own LIMIT still evaluates lazily.
        "WITH stale AS MATERIALIZED (
             SELECT sos.sandbox_id, sos.node_id
             FROM sandbox_observed_states sos
             JOIN sandboxes s ON s.id = sos.sandbox_id
             WHERE sos.sandbox_id > $4
               AND sos.sandbox_id <= $5
               AND s.lifecycle = 'suspended'
               AND s.updated_at < NOW() - make_interval(secs => $1::BIGINT)
               AND NOT EXISTS (
                   SELECT 1
                   FROM node_facet_observed_status nfos
                   WHERE nfos.node_id = sos.node_id
                     AND nfos.facet = 'sandbox-desired-state'
                     AND nfos.element = sos.sandbox_id::TEXT
                     AND nfos.observed_payload ? 'quarantine_recovery'
                     AND nfos.observed_at
                         >= NOW() - make_interval(secs => $3::BIGINT)
               )
             LIMIT $2
         ),
         stale_refs AS MATERIALIZED (
             SELECT
                 stale.node_id,
                 'network-policy'::TEXT AS facet,
                 'sandbox:' || stale.sandbox_id::TEXT AS element
             FROM stale
             UNION ALL
             SELECT
                 stale.node_id,
                 sandbox_facets.facet,
                 stale.sandbox_id::TEXT AS element
             FROM stale
             CROSS JOIN (
                 VALUES
                     ('sandbox-inventory'::TEXT),
                     ('sandbox-desired-state'::TEXT)
             ) AS sandbox_facets(facet)
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING stale_refs
             WHERE nfc.node_id = stale_refs.node_id
               AND nfc.facet = stale_refs.facet
               AND nfc.element = stale_refs.element
             RETURNING 1
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING stale_refs
             WHERE nfos.node_id = stale_refs.node_id
               AND nfos.facet = stale_refs.facet
               AND nfos.element = stale_refs.element
             RETURNING 1
         ),
         deleted_observed AS (
             DELETE FROM sandbox_observed_states sos
             USING stale
             WHERE sos.sandbox_id = stale.sandbox_id
             RETURNING 1
         )
         SELECT
             (SELECT COUNT(*)::BIGINT FROM deleted_observed),
             (SELECT COUNT(*)::BIGINT FROM deleted_status),
             (SELECT COUNT(*)::BIGINT FROM deleted_conditions)"
    ))
    .bind(secs_i64(request.stale_suspended_age))
    .bind(limit)
    .bind(secs_i64(request.orphan_age))
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await
}

/// One windowed sweep of orphaned observed rows (source-of-truth sandbox row
/// gone) over the primary-key range `(from, to]`. Windowed and unordered for
/// the reasons on [`delete_stale_suspended_observed`]; shares that sweep's
/// lap windows since both scan `sandbox_observed_states`.
async fn delete_orphaned_observed(
    pool: &PgPool,
    request: PruneRequest,
    limit: i64,
    from: Uuid,
    to: Uuid,
) -> Result<DeleteReportRow, sqlx::Error> {
    sqlx::query_as(sql!(
        "WITH stale AS MATERIALIZED (
             SELECT sos.sandbox_id, sos.node_id
             FROM sandbox_observed_states sos
             LEFT JOIN sandboxes s ON s.id = sos.sandbox_id
             WHERE sos.sandbox_id > $3
               AND sos.sandbox_id <= $4
               AND s.id IS NULL
               AND sos.updated_at < NOW() - make_interval(secs => $1::BIGINT)
               AND NOT EXISTS (
                   SELECT 1
                   FROM node_facet_observed_status nfos
                   WHERE nfos.node_id = sos.node_id
                     AND nfos.facet = 'sandbox-desired-state'
                     AND nfos.element = sos.sandbox_id::TEXT
                     AND nfos.observed_payload ? 'quarantine_recovery'
                     AND nfos.observed_at
                         >= NOW() - make_interval(secs => $1::BIGINT)
               )
             LIMIT $2
         ),
         stale_refs AS MATERIALIZED (
             SELECT
                 stale.node_id,
                 'network-policy'::TEXT AS facet,
                 'sandbox:' || stale.sandbox_id::TEXT AS element
             FROM stale
             UNION ALL
             SELECT
                 stale.node_id,
                 sandbox_facets.facet,
                 stale.sandbox_id::TEXT AS element
             FROM stale
             CROSS JOIN (
                 VALUES
                     ('sandbox-inventory'::TEXT),
                     ('sandbox-desired-state'::TEXT)
             ) AS sandbox_facets(facet)
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING stale_refs
             WHERE nfc.node_id = stale_refs.node_id
               AND nfc.facet = stale_refs.facet
               AND nfc.element = stale_refs.element
             RETURNING 1
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING stale_refs
             WHERE nfos.node_id = stale_refs.node_id
               AND nfos.facet = stale_refs.facet
               AND nfos.element = stale_refs.element
             RETURNING 1
         ),
         deleted_observed AS (
             DELETE FROM sandbox_observed_states sos
             USING stale
             WHERE sos.sandbox_id = stale.sandbox_id
             RETURNING 1
         )
         SELECT
             (SELECT COUNT(*)::BIGINT FROM deleted_observed),
             (SELECT COUNT(*)::BIGINT FROM deleted_status),
             (SELECT COUNT(*)::BIGINT FROM deleted_conditions)"
    ))
    .bind(secs_i64(request.orphan_age))
    .bind(limit)
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await
}

/// One windowed sweep of orphaned facet rows over the composite-primary-key
/// range `(from, to]` of `node_facet_observed_status`. Windowed and unordered
/// for the reasons on [`delete_stale_suspended_observed`]; the four
/// eligibility branches are each a per-row existence probe, which is what an
/// unwindowed pass paid for every aged row every tick.
async fn delete_orphaned_facets(
    pool: &PgPool,
    request: PruneRequest,
    limit: i64,
    from: &FacetKey,
    to: &FacetKey,
) -> Result<DeleteReportRow, sqlx::Error> {
    sqlx::query_as(sql!(
        "WITH stale AS MATERIALIZED (
             SELECT nfos.node_id, nfos.facet, nfos.element
             FROM node_facet_observed_status nfos
             WHERE (nfos.node_id, nfos.facet, nfos.element) > ROW($3, $4, $5)
               AND (nfos.node_id, nfos.facet, nfos.element) <= ROW($6, $7, $8)
               AND nfos.updated_at < NOW() - make_interval(secs => $1::BIGINT)
               AND (
                   (
                       nfos.facet = 'network-policy'
                       AND nfos.element LIKE 'sandbox:%'
                       AND NOT EXISTS (
                           SELECT 1
                           FROM sandbox_observed_states sos
                           WHERE sos.node_id = nfos.node_id
                             AND nfos.element = 'sandbox:' || sos.sandbox_id::TEXT
                       )
                   )
                   OR (
                       nfos.facet = 'network-policy'
                       AND nfos.element LIKE 'template:%'
                       AND NOT EXISTS (
                           SELECT 1
                           FROM node_template_inventory nti
                           WHERE nti.node_id = nfos.node_id
                             AND nfos.element = 'template:' || nti.template_name
                       )
                   )
                   OR (
                       nfos.facet IN ('sandbox-inventory', 'sandbox-desired-state')
                       AND NOT EXISTS (
                           SELECT 1
                           FROM sandbox_observed_states sos
                           WHERE sos.node_id = nfos.node_id
                             AND nfos.element = sos.sandbox_id::TEXT
                       )
                   )
                   OR (
                       nfos.facet IN (
                           'template-inventory',
                           'template-desired-state',
                           'template-base-snapshot-policy'
                       )
                       AND NOT EXISTS (
                           SELECT 1
                           FROM node_template_inventory nti
                           WHERE nti.node_id = nfos.node_id
                             AND nfos.element = nti.template_name
                       )
                   )
               )
             LIMIT $2
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING stale
             WHERE nfc.node_id = stale.node_id
               AND nfc.facet = stale.facet
               AND nfc.element = stale.element
             RETURNING 1
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING stale
             WHERE nfos.node_id = stale.node_id
               AND nfos.facet = stale.facet
               AND nfos.element = stale.element
             RETURNING 1
         )
         SELECT
             0::BIGINT,
             (SELECT COUNT(*)::BIGINT FROM deleted_status),
             (SELECT COUNT(*)::BIGINT FROM deleted_conditions)"
    ))
    .bind(secs_i64(request.orphan_age))
    .bind(limit)
    .bind(from.node_id)
    .bind(&from.facet)
    .bind(&from.element)
    .bind(to.node_id)
    .bind(&to.facet)
    .bind(&to.element)
    .fetch_one(pool)
    .await
}

pub struct Reconciler<S: ProjectionGcSink> {
    config: ReconcilerConfig,
    sink: Arc<S>,
}

impl<S: ProjectionGcSink> Reconciler<S> {
    pub fn new(config: ReconcilerConfig, sink: S) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
        }
    }

    pub async fn tick(&self) -> TickReport {
        let start = Instant::now();
        let request = PruneRequest {
            stale_suspended_age: self.config.stale_suspended_age,
            orphan_age: self.config.orphan_age,
            max_per_tick: self.config.max_per_tick,
            scan_window: self.config.scan_window,
            windows_per_tick: self.config.windows_per_tick,
        };
        let report = match self.sink.prune_observed_projections(request).await {
            Ok(report) => report,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "observed_projection_gc: prune failed; will retry next tick"
                );
                return TickReport::default();
            }
        };
        if report.total() > 0 {
            tracing::info!(
                stale_suspended_observed = report.stale_suspended_observed,
                orphaned_observed = report.orphaned_observed,
                orphaned_facet_status = report.orphaned_facet_status,
                orphaned_facet_conditions = report.orphaned_facet_conditions,
                elapsed_ms = start.elapsed().as_millis(),
                "observed_projection_gc: tick complete"
            );
        }
        report
    }
}

pub async fn run<S: ProjectionGcSink>(
    config: ReconcilerConfig,
    sink: S,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "observed-projection-gc reconciler is disabled \
             (SPACE_OBSERVED_PROJECTION_GC_ENABLED=false)"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        stale_suspended_age_secs = config.stale_suspended_age.get().as_secs(),
        orphan_age_secs = config.orphan_age.get().as_secs(),
        max_per_tick = config.max_per_tick,
        "starting observed-projection-gc reconciler"
    );

    let reconciler = Reconciler::new(config.clone(), sink);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            _ = interval.tick() => {
                let _ = reconciler.tick().await;
            }
        }
    }
}

pub fn run_with_pool(
    config: ReconcilerConfig,
    pool: PgPool,
    shutdown_token: CancellationToken,
) -> impl std::future::Future<Output = ()> {
    run(config, ObservedProjectionDbStore::new(pool), shutdown_token)
}

#[cfg(test)]
mod tests;
