//! Data access for runtime upgrade campaigns (`runtime_upgrade_campaigns`)
//! and their items (`runtime_upgrade_items`).
//!
//! A campaign moves template releases or sandboxes from one execution
//! environment to another. Each item tracks one subject through its upgrade.
//! The cluster controller's `runtime_upgrade` reconciler is the reader and
//! writer here: it finds subjects ([`sandbox_candidates`]), records them
//! ([`insert_pending_sandboxes`]), leases them under the campaign's pacing
//! ([`lease_sandbox_items`]), and records each outcome under its lease
//! ([`finish_item`]).
//!
//! # Rules the store enforces
//!
//! The schema enforces that `finished_at` is set exactly when a campaign is
//! `completed` or `aborted`, and that a lease is held exactly while an item
//! is `in_flight`. On top of that:
//!
//! * **Database clock.** `finished_at` and `lease_expires_at` are written
//!   from `now()`, never from the caller's clock.
//! * **Bounded leases.** A lease is at most [`MAX_LEASE`] long, since a live
//!   lease blocks the down migration.
//! * **Fenced outcomes.** An item leaves `in_flight` only under the token
//!   that leased it, or by [`redrive_expired`] once that lease has lapsed. A
//!   worker that lost its lease records nothing.
//! * **Subject kind follows the campaign.** Sandbox items go only into
//!   unfinished sandbox campaigns.
//! * **Errors fit the column.** [`ItemError`] truncates to 4096 characters
//!   and maps an empty message to `NULL`.
//! * **Lock order.** A verb that touches a campaign and its items locks the
//!   campaign row first, as the down migration does.
//! * **Locks end with the verb.** A verb that opens a transaction commits or
//!   rolls it back before it returns, on every path. A dropped transaction
//!   rolls back only when its connection is next used, so a pooled connection
//!   could otherwise keep the campaign row locked indefinitely. Run inside a
//!   caller's transaction, a verb ends only its savepoint, and its locks last
//!   until the caller's transaction ends.
//!
//! What `:resume` does with failed items already counted against the failure
//! budget, and campaign deletion, belong to the admin route that adds them;
//! nothing here deletes a campaign.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{DigestParseError, EnvironmentReference};

mod items;
mod phase;

pub use items::{
    ItemOutcome, LeasedItem, RedriveReport, SandboxCandidate, finish_item,
    insert_pending_sandboxes, lease_sandbox_items, redrive_expired, renew_lease,
    sandbox_candidates,
};
pub use phase::{complete_if_drained, pause_if_over_budget};

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// The longest lease any verb grants. A live lease blocks the down migration,
/// so a lease must lapse within an operator's patience.
pub const MAX_LEASE: Duration = Duration::from_secs(60 * 60);

/// End a verb's transaction: commit it when `result` is `Ok`, roll it back
/// otherwise. A read-only `Ok` commits too, which releases its row locks just
/// as a rollback would. A failed rollback is left to the connection's own
/// cleanup; the verb's error is the one the caller needs.
pub(crate) async fn end_transaction<T, E: From<sqlx::Error>>(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    result: Result<T, E>,
) -> Result<T, E> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

/// A campaign's id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, sqlx::Type)]
#[sqlx(transparent)]
pub struct CampaignId(pub uuid::Uuid);

impl fmt::Display for CampaignId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Names the fixed vocabulary of a text column: `as_str`, `ALL`, `Display`
/// and a `FromStr` that refuses anything else, so a stored value outside the
/// set fails the read rather than being treated as one of them.
macro_rules! closed_vocabulary {
    ($(#[$meta:meta])* $name:ident, $error:literal { $($(#[$variant_meta:meta])* $variant:ident => $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = UnknownValue;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(UnknownValue { column: $error, value: other.to_owned() }),
                }
            }
        }
    };
}

/// A stored value outside a column's vocabulary.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown {column} {value:?}")]
pub struct UnknownValue {
    column: &'static str,
    value: String,
}

closed_vocabulary!(
    /// What a campaign upgrades (`runtime_upgrade_campaigns.kind`).
    CampaignKind, "campaign kind" {
        Templates => "templates",
        Sandboxes => "sandboxes",
    }
);

closed_vocabulary!(
    /// Where a campaign is (`runtime_upgrade_campaigns.phase`).
    CampaignPhase, "campaign phase" {
        /// Upgrading one item, then waiting for an operator's resume.
        Canary => "canary",
        /// Upgrading items under the campaign's pacing.
        Rolling => "rolling",
        /// Stopped by an operator or by the failure budget; nothing new is
        /// leased.
        Paused => "paused",
        Completed => "completed",
        Aborted => "aborted",
    }
);

impl CampaignPhase {
    /// Whether items may be leased in this phase.
    pub const fn leases(self) -> bool {
        matches!(self, Self::Canary | Self::Rolling)
    }
}

closed_vocabulary!(
    /// Where an item is (`runtime_upgrade_items.phase`).
    ItemPhase, "item phase" {
        Pending => "pending",
        InFlight => "in_flight",
        Done => "done",
        Failed => "failed",
        Skipped => "skipped",
    }
);

closed_vocabulary!(
    /// Which table an item's subject names (`runtime_upgrade_items.subject_kind`).
    SubjectKind, "subject kind" {
        TemplateRelease => "template_release",
        Sandbox => "sandbox",
    }
);

impl CampaignKind {
    /// The kind of subject this campaign's items name.
    pub const fn subject_kind(self) -> SubjectKind {
        match self {
            Self::Templates => SubjectKind::TemplateRelease,
            Self::Sandboxes => SubjectKind::Sandbox,
        }
    }
}

/// An unfinished campaign, as the reconciler drives it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Campaign {
    pub id: CampaignId,
    pub from: EnvironmentReference,
    pub to: EnvironmentReference,
    pub kind: CampaignKind,
    pub phase: CampaignPhase,
    /// At least 1, and at most [`Self::max_in_flight_per_cluster`]; the
    /// schema enforces both.
    pub max_in_flight_per_node: u64,
    pub max_in_flight_per_cluster: u64,
    /// Failed items at which the campaign pauses itself; at least 1.
    pub failure_budget: u64,
    /// Whether a sandbox campaign also takes running and paused sandboxes.
    pub include_running: bool,
}

/// Reading a campaign failed. Every variant fails closed.
#[derive(Debug, thiserror::Error)]
pub enum ReadCampaignError {
    #[error("campaign {campaign_id} holds an invalid environment reference {value:?}")]
    InvalidEnvironmentReference {
        campaign_id: CampaignId,
        value: String,
        #[source]
        source: DigestParseError,
    },
    #[error("campaign {campaign_id}: {source}")]
    UnknownValue {
        campaign_id: CampaignId,
        #[source]
        source: UnknownValue,
    },
    #[error("campaign {campaign_id} holds a negative {column}")]
    Negative {
        campaign_id: CampaignId,
        column: &'static str,
    },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(sqlx::FromRow)]
struct CampaignRow {
    id: CampaignId,
    from_environment_ref: String,
    to_environment_ref: String,
    kind: String,
    phase: String,
    max_in_flight_per_node: i64,
    max_in_flight_per_cluster: i64,
    failure_budget: i64,
    include_running: bool,
}

impl TryFrom<CampaignRow> for Campaign {
    type Error = ReadCampaignError;

    fn try_from(row: CampaignRow) -> Result<Self, Self::Error> {
        let campaign_id = row.id;
        let reference = |value: String| {
            value
                .parse()
                .map_err(|source| ReadCampaignError::InvalidEnvironmentReference {
                    campaign_id,
                    value,
                    source,
                })
        };
        let vocabulary = |source| ReadCampaignError::UnknownValue {
            campaign_id,
            source,
        };
        let count = |value: i64, column| {
            u64::try_from(value).map_err(|_| ReadCampaignError::Negative {
                campaign_id,
                column,
            })
        };
        Ok(Self {
            id: campaign_id,
            from: reference(row.from_environment_ref)?,
            to: reference(row.to_environment_ref)?,
            kind: row.kind.parse().map_err(vocabulary)?,
            phase: row.phase.parse().map_err(vocabulary)?,
            max_in_flight_per_node: count(row.max_in_flight_per_node, "max_in_flight_per_node")?,
            max_in_flight_per_cluster: count(
                row.max_in_flight_per_cluster,
                "max_in_flight_per_cluster",
            )?,
            failure_budget: count(row.failure_budget, "failure_budget")?,
            include_running: row.include_running,
        })
    }
}

/// Every campaign that hasn't finished (canary, rolling or paused), oldest
/// first. One unreadable row fails the whole read. Takes no lock.
pub async fn list_unfinished(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<Campaign>, ReadCampaignError> {
    let rows = sqlx::query_as::<_, CampaignRow>(sql!(
        "SELECT id, from_environment_ref, to_environment_ref, kind, phase,
                max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
                include_running
         FROM runtime_upgrade_campaigns
         WHERE finished_at IS NULL
         ORDER BY created_at, id"
    ))
    .fetch_all(executor)
    .await?;
    rows.into_iter().map(Campaign::try_from).collect()
}

/// A campaign's items, counted by phase.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ItemCounts {
    pub pending: u64,
    pub in_flight: u64,
    pub done: u64,
    pub failed: u64,
    pub skipped: u64,
}

impl ItemCounts {
    pub const fn get(&self, phase: ItemPhase) -> u64 {
        match phase {
            ItemPhase::Pending => self.pending,
            ItemPhase::InFlight => self.in_flight,
            ItemPhase::Done => self.done,
            ItemPhase::Failed => self.failed,
            ItemPhase::Skipped => self.skipped,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ItemCountsError {
    #[error(transparent)]
    UnknownValue(#[from] UnknownValue),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// `campaign_id`'s items counted by phase. Takes no lock.
pub async fn item_counts(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
) -> Result<ItemCounts, ItemCountsError> {
    let rows = sqlx::query_as::<_, (String, i64)>(sql!(
        "SELECT phase, count(*)
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
         GROUP BY phase"
    ))
    .bind(campaign_id)
    .fetch_all(executor)
    .await?;
    let mut counts = ItemCounts::default();
    for (phase, count) in rows {
        // count(*) is never negative.
        let count = u64::try_from(count).unwrap_or(0);
        match phase.parse::<ItemPhase>()? {
            ItemPhase::Pending => counts.pending = count,
            ItemPhase::InFlight => counts.in_flight = count,
            ItemPhase::Done => counts.done = count,
            ItemPhase::Failed => counts.failed = count,
            ItemPhase::Skipped => counts.skipped = count,
        }
    }
    Ok(counts)
}

/// An item's error as `last_error` stores it: at most 4096 characters, and
/// never empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ItemError(Option<String>);

impl ItemError {
    /// Upper bound on a stored error, in characters, matching the schema's
    /// `CHECK`.
    pub const MAX_CHARS: usize = 4096;

    /// `message` truncated to [`Self::MAX_CHARS`] characters, with NULs
    /// (which Postgres text can't hold) dropped; an empty message stores
    /// `NULL`.
    pub fn new(message: impl AsRef<str>) -> Self {
        let message: String = message
            .as_ref()
            .chars()
            .filter(|character| *character != '\0')
            .take(Self::MAX_CHARS)
            .collect();
        Self((!message.is_empty()).then_some(message))
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }
}
