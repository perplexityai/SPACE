//! The `node_status_snapshots` row: what each node last reported about
//! itself, one row per node.
//!
//! Its writers predate this store and haven't moved in yet: the internal
//! API's node status handler and the Direct substrate's own snapshot. What
//! lives here today is the rule for reading the row's published
//! environments, so every reader asks the same question.

use std::fmt;
use std::str::FromStr;

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// A boolean SQL predicate, TRUE iff the node status snapshot aliased as
/// `snapshot_alias` counts as evidence of the node's published environments
/// now (`usable_environment_refs` and `known_environment_refs`):
///
/// * its latest execution inventory report is `complete`;
/// * the snapshot is fresh: `stale_after` hasn't passed;
/// * the environment arrays were received within the node's freshness
///   window, `stale_after - observed_at`. The arrays have their own receipt
///   time because older writers refresh the inventory columns without them,
///   so arrays such a writer leaves behind age out.
///
/// A NULL in any of them (no inventory report, no arrays) is FALSE, never
/// NULL, so the predicate can be negated safely. It says nothing about
/// whether the node is reachable: a caller choosing a node to act on also
/// requires `reachable IS TRUE`.
///
/// Every value is the row's own or the database clock's, so a caller builds
/// its statement once (a `LazyLock<String>`) and binds the rest.
pub fn environment_evidence(snapshot_alias: &str) -> String {
    format!(
        "COALESCE(\n\
             {complete}\n\
             AND {fresh}\n\
             AND {arrays_fresh},\n\
             FALSE\n\
         )",
        complete = inventory_complete(snapshot_alias),
        fresh = snapshot_fresh(snapshot_alias),
        arrays_fresh = arrays_fresh(snapshot_alias),
    )
}

/// A text SQL expression: why the snapshot aliased as `snapshot_alias`
/// doesn't show the node can run `environment`, a text SQL expression, as an
/// [`EnvironmentRefusal`] wire value. NULL when there's nothing to refuse:
/// the snapshot is evidence ([`environment_evidence`]) listing `environment`
/// among its usable environments, or `environment` is NULL, which is work
/// bound to no environment.
///
/// A missing snapshot (a `LEFT JOIN` that found no row) has no evidence.
///
/// Built from the same conditions as [`environment_evidence`], so a snapshot
/// that's evidence is refused only as `environment_not_usable`.
pub fn environment_refusal(snapshot_alias: &str, environment: &str) -> String {
    let no_evidence = EnvironmentRefusal::NoEvidence.as_str();
    let incomplete = EnvironmentRefusal::Incomplete.as_str();
    let stale = EnvironmentRefusal::Stale.as_str();
    let not_usable = EnvironmentRefusal::EnvironmentNotUsable.as_str();
    format!(
        "CASE\n\
             WHEN {environment} IS NULL THEN NULL\n\
             WHEN {snapshot_alias}.execution_inventory_status IS NULL THEN '{no_evidence}'\n\
             WHEN NOT {complete} THEN '{incomplete}'\n\
             WHEN {snapshot_alias}.environment_refs_received_at IS NULL THEN '{no_evidence}'\n\
             WHEN NOT ({fresh} AND {arrays_fresh}) THEN '{stale}'\n\
             WHEN NOT COALESCE({environment} = ANY({snapshot_alias}.usable_environment_refs), FALSE)\n\
                 THEN '{not_usable}'\n\
         END",
        complete = inventory_complete(snapshot_alias),
        fresh = snapshot_fresh(snapshot_alias),
        arrays_fresh = arrays_fresh(snapshot_alias),
    )
}

fn inventory_complete(snapshot_alias: &str) -> String {
    format!("{snapshot_alias}.execution_inventory_status = 'complete'")
}

fn snapshot_fresh(snapshot_alias: &str) -> String {
    format!("{snapshot_alias}.stale_after > now()")
}

fn arrays_fresh(snapshot_alias: &str) -> String {
    format!(
        "{snapshot_alias}.environment_refs_received_at\n\
             + ({snapshot_alias}.stale_after - {snapshot_alias}.observed_at) > now()"
    )
}

/// Why a node's snapshot doesn't show it can run an environment
/// ([`environment_refusal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentRefusal {
    /// No snapshot, no inventory report, or a complete inventory without
    /// environment arrays, from a build that publishes none.
    NoEvidence,
    /// The latest inventory report isn't `complete`.
    Incomplete,
    /// The snapshot is past `stale_after`, or its arrays are older than the
    /// node's freshness window.
    Stale,
    /// Evidence that doesn't list the environment as usable.
    EnvironmentNotUsable,
}

impl EnvironmentRefusal {
    pub const ALL: [Self; 4] = [
        Self::NoEvidence,
        Self::Incomplete,
        Self::Stale,
        Self::EnvironmentNotUsable,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoEvidence => "no_evidence",
            Self::Incomplete => "incomplete",
            Self::Stale => "stale",
            Self::EnvironmentNotUsable => "environment_not_usable",
        }
    }
}

impl fmt::Display for EnvironmentRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown environment refusal {0:?}")]
pub struct EnvironmentRefusalParseError(pub String);

impl FromStr for EnvironmentRefusal {
    type Err = EnvironmentRefusalParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|refusal| refusal.as_str() == value)
            .ok_or_else(|| EnvironmentRefusalParseError(value.to_string()))
    }
}
