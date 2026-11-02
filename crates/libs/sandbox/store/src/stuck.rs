//! A read-only census of the sandboxes sitting in a transient lifecycle and
//! how long each has been there.
//!
//! It writes nothing and decides nothing: it reports what loops such as the
//! transient-lifecycle sweeper and node-drain otherwise bound one case at a
//! time, so that a wedged lifecycle is visible whether or not any of them is
//! enabled.

use pplx_space_common_core::types::SandboxLifecycle;

use std::time::Duration;

use pplx_space_common_core::sql;

/// The census of one transient lifecycle.
#[derive(Clone, Debug, PartialEq)]
pub struct TransientLifecycleAges {
    pub lifecycle: SandboxLifecycle,
    /// Rows per age bucket: entry 0 counts rows younger than the first bound,
    /// entry `i` rows at least `bounds[i - 1]` and younger than `bounds[i]`,
    /// and the last entry rows at least the last bound. One entry more than
    /// the bounds passed in.
    pub rows_by_bucket: Vec<u64>,
    /// The age of the oldest row; `None` when the lifecycle has none.
    pub oldest_age: Option<Duration>,
}

/// [`transient_lifecycle_ages`] failed.
#[derive(Debug, thiserror::Error)]
pub enum StuckSandboxesError {
    /// The caller's bucket bounds are not strictly ascending, so the buckets
    /// would overlap.
    #[error("age bucket bounds must be strictly ascending, got {bounds:?}")]
    UnorderedBounds { bounds: Vec<Duration> },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// One `(lifecycle, bucket)` group of the census statement.
#[derive(Debug, sqlx::FromRow)]
struct AgeBucketRow {
    lifecycle: String,
    /// `width_bucket`'s 0-based bucket index.
    bucket: i32,
    rows: i64,
    oldest_age_secs: f64,
}

/// The census statement. A `const` rather than inline so its Postgres tests
/// can `EXPLAIN` the exact text.
///
/// `$1` is the lifecycle list and `$2` the bucket bounds in seconds. `NOW()`
/// is the transaction's start, the same instant for every row; from the
/// pool, as the controller calls it, that is this statement's start.
/// `lifecycle = ANY($1)` matches the leading column of the indexes on
/// `lifecycle`, so where transient rows are a small minority of the table,
/// as in production, the plan reads only their index entries and at most
/// the heap pages that hold them, not the table.
const TRANSIENT_LIFECYCLE_AGES: &str = sql!(
    "SELECT lifecycle,
            width_bucket(
                EXTRACT(EPOCH FROM NOW() - lifecycle_changed_at)::DOUBLE PRECISION,
                $2::DOUBLE PRECISION[]
            ) AS bucket,
            COUNT(*)::BIGINT AS rows,
            GREATEST(
                EXTRACT(EPOCH FROM NOW() - MIN(lifecycle_changed_at))::DOUBLE PRECISION,
                0
            ) AS oldest_age_secs
       FROM sandboxes
      WHERE lifecycle = ANY($1)
      GROUP BY lifecycle, bucket"
);

/// How many sandboxes sit in each transient lifecycle
/// ([`SandboxLifecycle::is_transient`]), bucketed by how long they have been
/// there, and the oldest one's age. Returns one entry for every transient
/// lifecycle, in [`SandboxLifecycle::all`] order, zeros included.
///
/// A read: it writes no fact and takes no lock. Age is
/// `NOW() - lifecycle_changed_at` on the database clock. The lifecycle
/// trigger stamps `lifecycle_changed_at` only when `lifecycle` changes, so a
/// retry within a lifecycle (a `restoring -> restoring` re-drive) or a write
/// that only bumps `updated_at` leaves a row's age alone, and a lifecycle
/// change starts it over. `deleting` counts too, although the sweeper never
/// ages it out.
///
/// `bounds` are the lower bounds of every bucket but the first, strictly
/// ascending; a row aged exactly a bound falls in the bucket that bound
/// starts. With no bounds there is one bucket.
///
/// Only rows whose `lifecycle` is exactly a canonical
/// [`SandboxLifecycle::as_str`] value are counted. A padded or differently
/// cased value is counted in no lifecycle and no bucket.
///
/// One statement, so the counts are one consistent snapshot.
pub async fn transient_lifecycle_ages(
    executor: impl sqlx::PgExecutor<'_>,
    bounds: &[Duration],
) -> Result<Vec<TransientLifecycleAges>, StuckSandboxesError> {
    if !bounds.is_sorted_by(|lower, upper| lower < upper) {
        return Err(StuckSandboxesError::UnorderedBounds {
            bounds: bounds.to_vec(),
        });
    }
    let lifecycles: Vec<SandboxLifecycle> = SandboxLifecycle::all()
        .iter()
        .copied()
        .filter(|lifecycle| lifecycle.is_transient())
        .collect();
    let lifecycle_binds: Vec<&str> = lifecycles.iter().map(|l| l.as_str()).collect();
    let bound_secs: Vec<f64> = bounds.iter().map(Duration::as_secs_f64).collect();

    let groups = sqlx::query_as::<_, AgeBucketRow>(TRANSIENT_LIFECYCLE_AGES)
        .bind(&lifecycle_binds)
        .bind(&bound_secs)
        .fetch_all(executor)
        .await?;

    let mut census: Vec<TransientLifecycleAges> = lifecycles
        .into_iter()
        .map(|lifecycle| TransientLifecycleAges {
            lifecycle,
            rows_by_bucket: vec![0; bounds.len() + 1],
            oldest_age: None,
        })
        .collect();
    for group in groups {
        let Some(entry) = census
            .iter_mut()
            .find(|entry| entry.lifecycle.as_str() == group.lifecycle)
        else {
            continue;
        };
        // `width_bucket` returns 0..=len(bounds) for a finite operand.
        let bucket = usize::try_from(group.bucket).unwrap_or(0).min(bounds.len());
        if let Some(rows) = entry.rows_by_bucket.get_mut(bucket) {
            *rows += u64::try_from(group.rows).unwrap_or(0);
        }
        let age = Duration::try_from_secs_f64(group.oldest_age_secs).unwrap_or(Duration::ZERO);
        entry.oldest_age = Some(entry.oldest_age.map_or(age, |oldest| oldest.max(age)));
    }
    Ok(census)
}

#[cfg(test)]
mod tests;
