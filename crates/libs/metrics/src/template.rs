use crate::emit::{counter, distribution, tag};

pub fn build_routing_fallback_missing_identity() {
    counter(
        "template.build.routing_fallback",
        1,
        &[tag("reason", "missing_builder_identity")],
    );
}

/// Why a template-visibility lookup could not produce a definite
/// answer; the `reason` tag on `template.visibility_lookup.degraded`.
///
/// The lookup that emits this is best-effort — it degrades to
/// `UNSPECIFIED` rather than failing release resolution — so a
/// `tracing::warn!` alone would leave the degradation unalertable. Each
/// variant is a distinct operational story, which is why they are
/// separate tag values rather than one `failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityLookupDegradation {
    /// No `templates.visibility` column (SQLSTATE 42703). The column is in
    /// the baseline schema, so any occurrence means schema drift.
    ColumnMissing,
    /// Any other query failure: pool exhaustion, connection reset,
    /// permission denied, or the app role's server-side
    /// `statement_timeout` firing — which is also where a lock wait
    /// behind an `ALTER TABLE templates` ends up, since a lock wait
    /// blocks rather than failing fast.
    QueryFailed,
    /// The stored value is not a recognized visibility spelling. The
    /// column's CHECK constraint should make this unreachable, so a
    /// non-zero rate means the constraint is missing or was bypassed.
    UnrecognizedValue,
    /// No `templates` row for the requested name.
    TemplateMissing,
}

impl VisibilityLookupDegradation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ColumnMissing => "column_missing",
            Self::QueryFailed => "query_failed",
            Self::UnrecognizedValue => "unrecognized_value",
            Self::TemplateMissing => "template_missing",
        }
    }

    /// Reasons a monitor acts on. `template_missing` is caller-driven and never
    /// alertable.
    pub const ALERTABLE: [Self; 3] = [
        Self::ColumnMissing,
        Self::QueryFailed,
        Self::UnrecognizedValue,
    ];
}

impl std::fmt::Display for VisibilityLookupDegradation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per release-resolution response whose template visibility fell
/// back to `UNSPECIFIED`.
///
/// Alertability differs per reason, so don't alert on the bare metric:
/// `template_missing` is caller-driven and never alertable;
/// `query_failed`, `unrecognized_value` and `column_missing` are all
/// alertable and feed [`visibility_lookup_alertable_last_hour`].
pub fn visibility_lookup_degraded(reason: VisibilityLookupDegradation) {
    counter(
        "template.visibility_lookup.degraded",
        1,
        &[tag("reason", reason.as_str())],
    );
}

/// Alertable `template.visibility_lookup.degraded` events one cluster-API
/// replica saw in the trailing hour (60 to 61 minutes), per `reason`.
///
/// Emitted on a fixed period with explicit zeros, so a monitor can test
/// persistence with `min(last_4h)` and treat absence as a dead emitter; the
/// sparse `degraded` counter can't separate a clean hour from lost data. A
/// distribution rather than a gauge so `max:` sees the worst replica: gauges
/// from replicas sharing a node agent collapse into one last-write-wins series.
/// Query it with `max:` only. Each replica reports the same trailing count
/// twice a minute, so `count:` counts samples rather than events, `sum:` counts
/// each event once per sample for the hour it stays in the window, and `avg:`
/// dilutes the worst replica across the healthy ones. A lookup answered from a stale cache entry is not a
/// degradation, so a read failure the cache absorbs doesn't count here; see
/// `template.visibility_lookup.cache{outcome:served_stale}`.
pub fn visibility_lookup_alertable_last_hour(reason: VisibilityLookupDegradation, count: u64) {
    distribution(
        "template.visibility_lookup.alertable_last_hour",
        count as f64,
        &[tag("reason", reason.as_str())],
    );
}

/// How one template-visibility lookup was answered; the `outcome` tag on
/// `template.visibility_lookup.cache`.
///
/// Deliberately a separate counter from
/// `template.visibility_lookup.degraded` rather than more `reason` values on
/// it. That one means exactly "the response carried UNSPECIFIED", which is
/// what a prefetch-suppression monitor fires on; folding an absorbed blip into
/// it would make the monitor fire on a lookup that answered correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityLookupCacheOutcome {
    /// Answered from a cached read still inside the freshness window. No
    /// query ran. The steady-state majority.
    Fresh,
    /// Queried and got a concrete answer.
    ///
    /// Not "and stored it": the store is dropped when both windows are disabled,
    /// when the name is too long to key, when a strictly-later observation is
    /// already resident, and when the map's write lock is poisoned. This counts
    /// reaching the database and getting a value that parsed, which is what the
    /// load check wants.
    ///
    /// So `refreshed` alone never accounts for the traffic, kill switch or not.
    /// The other outcomes are not only the failure ones: a lookup finding no row
    /// reports `absent`, a failed query reports `miss`, and — the one an earlier
    /// wording of this note mis-partitioned — a lookup that *does* reach a row
    /// whose value fails to parse also reports `miss`, not `refreshed`. Reaching
    /// a row is necessary for `refreshed` and not sufficient.
    Refreshed,
    /// The query could not reach the row, and a cached read inside the
    /// staleness ceiling answered instead. **This is the database-health
    /// signal:** a non-zero rate means the lookup is failing and only the
    /// cache is keeping visibility-gated consumers from failing closed.
    ServedStale,
    /// The row was read and there is none. Caller-driven and **never
    /// alertable**: the `ListLatestTemplateReleases` contract lets a caller name
    /// a template that does not exist.
    ///
    /// Split out from `Miss` deliberately, and for one reason precisely: merged,
    /// a node polling an unknown name holds `miss` permanently non-zero, which
    /// gives `miss` a routine floor unrelated to degraded enforcement and makes
    /// `miss`-based alerting unusable. (It does *not* affect the `refreshed`
    /// share, whose denominator is `fresh + refreshed` — an earlier version of
    /// this comment claimed it did.)
    Absent,
    /// Nothing usable for any other reason — the query failed with no cached read
    /// inside the ceiling, the answer would not parse, or a durable failure
    /// invalidated. The response carries UNSPECIFIED and
    /// `template.visibility_lookup.degraded` fires alongside this with the
    /// specific reason.
    Miss,
}

impl VisibilityLookupCacheOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Refreshed => "refreshed",
            Self::ServedStale => "served_stale",
            Self::Absent => "absent",
            Self::Miss => "miss",
        }
    }
}

impl std::fmt::Display for VisibilityLookupCacheOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per template-visibility lookup, tagged by how it was answered.
///
/// Exactly one of these is emitted per lookup, so the sum is the lookup rate
/// and `outcome:refreshed` is the share that reached the database — which is
/// how you check the cache is actually absorbing load. Alert on
/// `outcome:served_stale`, not on `outcome:miss` alone: `served_stale` is the
/// early warning (the read is broken but consumers are still being answered),
/// and `miss` is the state that has already degraded enforcement.
pub fn visibility_lookup_cache(outcome: VisibilityLookupCacheOutcome) {
    counter(
        "template.visibility_lookup.cache",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}
