//! How many sandboxes sit in each transient lifecycle, and for how long.
//!
//! The cluster controller's elected leader reads the census from the
//! `sandboxes` rows once a minute and emits two gauges:
//!
//! - `sandbox.transient_lifecycle.rows`, tagged `lifecycle` and `age`: the
//!   rows in that lifecycle whose lifecycle changed within that age bucket.
//! - `sandbox.transient_lifecycle.oldest_age_seconds`, tagged `lifecycle`:
//!   the age of the oldest such row, 0 when there is none.
//!
//! `sandbox.transient_lifecycle.loop_latency_ms` times each census tick,
//! failed ones included.
//!
//! `lifecycle` is every [`SandboxLifecycle::is_transient`] value, `deleting`
//! included, and `age` every [`LifecycleAge`]. [`census`] emits every pair
//! on every tick, zeros included, so a missing series always means no
//! report rather than no rows. No sandbox, node or org is ever a tag.

use pplx_space_common_core::types::SandboxLifecycle;

use std::time::{Duration, Instant};

use crate::emit::{elapsed_ms, gauge, tag, timing_ms};

const MINUTE: Duration = Duration::from_secs(60);

metric_vocabulary!(
    /// How long a sandbox has been in its current lifecycle; the `age` tag.
    ///
    /// The first bucket is a lifecycle running normally; the next two are a
    /// slow one, such as a large snapshot load, and one past any normal
    /// operation; the last three are a wedged row at the scales an operator
    /// and the sweeper's age limits work in.
    LifecycleAge {
        /// Under one minute.
        UnderOneMinute => "lt_1m",
        /// One to ten minutes.
        OneToTenMinutes => "1m_10m",
        /// Ten minutes to one hour.
        TenMinutesToOneHour => "10m_1h",
        /// One to six hours.
        OneToSixHours => "1h_6h",
        /// Six hours to one day.
        SixHoursToOneDay => "6h_1d",
        /// One day or more.
        OneDayOrMore => "ge_1d",
    }
);

impl LifecycleAge {
    /// Every bucket, youngest first.
    pub const ALL: [Self; 6] = [
        Self::UnderOneMinute,
        Self::OneToTenMinutes,
        Self::TenMinutesToOneHour,
        Self::OneToSixHours,
        Self::SixHoursToOneDay,
        Self::OneDayOrMore,
    ];

    /// The lower bound of every bucket but the first, in [`Self::ALL`]
    /// order; a row aged exactly a bound is in the bucket it starts.
    pub const LOWER_BOUNDS: [Duration; 5] = [
        MINUTE,
        Duration::from_secs(10 * 60),
        Duration::from_secs(60 * 60),
        Duration::from_secs(6 * 60 * 60),
        Duration::from_secs(24 * 60 * 60),
    ];
}

/// One transient lifecycle's census.
#[derive(Clone, Copy, Debug)]
pub struct LifecycleCensus<'a> {
    pub lifecycle: SandboxLifecycle,
    /// Rows per bucket, in [`LifecycleAge::ALL`] order. A missing entry
    /// reads as 0.
    pub rows_by_age: &'a [u64],
    /// The oldest row's age; `None` when there is none.
    pub oldest_age: Option<Duration>,
}

/// Emit both gauges for every transient lifecycle and every age bucket. A
/// transient lifecycle absent from `entries` reports zero rows, and an entry
/// for a lifecycle that is not transient is ignored.
pub fn census(entries: &[LifecycleCensus<'_>]) {
    for lifecycle in SandboxLifecycle::all()
        .iter()
        .copied()
        .filter(|lifecycle| lifecycle.is_transient())
    {
        let entry = entries.iter().find(|entry| entry.lifecycle == lifecycle);
        for (index, age) in LifecycleAge::ALL.into_iter().enumerate() {
            let rows = entry
                .and_then(|entry| entry.rows_by_age.get(index).copied())
                .unwrap_or(0);
            gauge(
                "sandbox.transient_lifecycle.rows",
                rows as f64,
                &[tag("lifecycle", lifecycle), tag("age", age)],
            );
        }
        let oldest = entry
            .and_then(|entry| entry.oldest_age)
            .map_or(0.0, |age| age.as_secs_f64());
        gauge(
            "sandbox.transient_lifecycle.oldest_age_seconds",
            oldest,
            &[tag("lifecycle", lifecycle)],
        );
    }
}

/// Wall-clock time of one census tick.
pub fn loop_latency(started_at: Instant) {
    timing_ms(
        "sandbox.transient_lifecycle.loop_latency_ms",
        elapsed_ms(started_at),
        &[],
    );
}

#[cfg(test)]
mod tests;
