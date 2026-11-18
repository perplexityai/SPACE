//! Data access for runtime upgrade campaigns (`runtime_upgrade_campaigns`)
//! and their items (`runtime_upgrade_items`).
//!
//! A campaign moves template releases or sandboxes from one execution
//! environment to another. Each item tracks one subject through its upgrade.
//! The verbs here are kind-agnostic: the cluster controller's
//! `runtime_upgrade` reconciler records the subjects a kind's driver finds
//! ([`insert_pending`]), leases them under the campaign's pacing
//! ([`lease_items`]), and records each outcome under its lease
//! ([`finish_item`]). The admin API starts ([`create_campaign`]), pauses
//! ([`pause_campaign`]), resumes ([`resume_campaign`]), aborts
//! ([`abort_campaign`]) and reads
//! ([`get_record`], [`list_records`]) campaigns.
//!
//! # Rules the store enforces
//!
//! The schema enforces that `finished_at` is set exactly when a campaign is
//! `completed` or `aborted`, and that a lease is held exactly while an item
//! is `in_flight`. On top of that:
//!
//! * **Database clock.** `finished_at` is written from `now()`, and
//!   `lease_expires_at` from `clock_timestamp()` when the lease is granted or
//!   renewed, never from the caller's clock.
//! * **Bounded leases.** A lease is at most [`MAX_LEASE`] long, since a live
//!   lease blocks the down migration.
//! * **Fenced outcomes.** An item leaves `in_flight` only under the token
//!   that leased it, or by [`redrive_expired`] or
//!   [`release_lapsed_in_finished`] once that lease has lapsed. A worker that
//!   lost its lease records nothing.
//! * **One worker per subject.** A lease skips a subject another campaign
//!   holds in flight under a live lease, and a finished campaign's leases
//!   can't be renewed, so an aborted campaign's worker stops before its lease
//!   lapses and a new campaign's worker starts only after ([`lease_items`]).
//! * **Subject kind follows the campaign.** Items go only into unfinished
//!   campaigns whose kind names their subject kind
//!   ([`CampaignKind::subject_kind`]).
//! * **Per-node pacing only where subjects have a node.** A sandbox item
//!   counts against `max_in_flight_per_node` on the node that last published
//!   its sandbox; a template release item has no node and counts against the
//!   cluster cap only ([`SubjectKind::has_node`]).
//! * **Generation.** Every change to a campaign row raises its `generation`
//!   by exactly one, under the row lock, in the same statement or
//!   transaction: an operator's pause, resume or abort, and the reconciler's pause
//!   at the failure budget or completion. Item progress doesn't touch the
//!   campaign row. A new campaign starts at generation 0.
//! * **Errors fit the column.** [`ItemError`] truncates to 4096 characters
//!   and maps an empty message to `NULL`.
//! * **Lock order.** A verb that touches a campaign and its items locks the
//!   campaign row first, as the down migration does.
//! * **Locks end with the verb.** A verb that opens a transaction commits or
//!   rolls it back before it returns, on every path. A dropped transaction
//!   rolls back only once its connection is next used or handed back to the
//!   pool, so on a caller's own connection it could keep the campaign row
//!   locked until that connection's next statement. Run inside a
//!   caller's transaction, a verb ends only its savepoint, and its locks last
//!   until the caller's transaction ends.
//! * **One unfinished campaign per kind and source.** [`create_campaign`]
//!   refuses a second, so two campaigns don't compete for the same subjects.
//!
//! Nothing here deletes a campaign.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use pplx_space_accounts::UserId;
use pplx_space_sql_locator::sql;
use pplx_space_content_digest::DigestParseError;
use pplx_space_execution_spec::EnvironmentReference;
use time::OffsetDateTime;

mod admin;
mod create;
mod items;
mod phase;

pub use admin::{
    ChangePhaseError, PhaseChange, PhaseChanged, abort_campaign, pause_campaign, resume_campaign,
};
pub use create::{
    CreateCampaignError, NewCampaign, REASON_MAX_CHARS, check_reason, create_campaign,
};
pub use items::{
    ItemOutcome, LeaseError, LeasedItem, RedriveReport, finish_item, insert_pending, lease_items,
    redrive_expired, release_lapsed_in_finished, renew_lease, retire_skipped,
};
pub use phase::{complete_if_drained, pause_if_over_budget};

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// The longest lease any verb grants. A live lease blocks the down migration,
/// so a lease must lapse within an operator's patience.
pub const MAX_LEASE: Duration = Duration::from_secs(60 * 60);

/// The lease a verb grants for a requested `lease`: clamped to 1 second
/// through [`MAX_LEASE`]. A worker that holds the lease measures its own
/// deadline with the same clamp, so it's never later than the database's.
pub fn clamp_lease(lease: Duration) -> Duration {
    lease.clamp(Duration::from_secs(1), MAX_LEASE)
}

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

    /// Whether the campaign is over: `finished_at` is set.
    pub const fn finished(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted)
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

impl SubjectKind {
    /// Whether a subject of this kind lives on a node, so its items count
    /// against a campaign's `max_in_flight_per_node`. A sandbox is on the node
    /// that last published it; a template release is cluster-wide.
    pub const fn has_node(self) -> bool {
        match self {
            Self::Sandbox => true,
            Self::TemplateRelease => false,
        }
    }
}

impl CampaignKind {
    /// The kind of subject this campaign's items name.
    pub const fn subject_kind(self) -> SubjectKind {
        match self {
            Self::Templates => SubjectKind::TemplateRelease,
            Self::Sandboxes => SubjectKind::Sandbox,
        }
    }
}

/// A campaign, as the reconciler drives it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Campaign {
    pub id: CampaignId,
    pub from: EnvironmentReference,
    pub to: EnvironmentReference,
    pub kind: CampaignKind,
    pub phase: CampaignPhase,
    /// At least 1, and at most [`Self::max_in_flight_per_cluster`]; the
    /// schema enforces both. Applies only to kinds whose subjects have a node.
    pub max_in_flight_per_node: u64,
    pub max_in_flight_per_cluster: u64,
    /// Failed items at which the campaign pauses itself; at least 1.
    pub failure_budget: u64,
    /// Whether a sandbox campaign also takes running and paused sandboxes.
    /// The sandbox driver gives it its meaning; other kinds ignore it.
    pub include_running: bool,
    /// Raised by exactly one with every change to the campaign row.
    pub generation: u64,
}

/// A campaign with its provenance, as the admin API reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CampaignRecord {
    pub campaign: Campaign,
    /// Why the campaign runs, as its creator gave it.
    pub reason: String,
    pub created_at: OffsetDateTime,
    pub created_by: Option<UserId>,
    pub updated_at: OffsetDateTime,
    /// `None` when the reconciler made the last change, or the user is gone.
    pub updated_by: Option<UserId>,
    pub finished_at: Option<OffsetDateTime>,
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
    generation: i64,
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
            generation: count(row.generation, "generation")?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct CampaignRecordRow {
    #[sqlx(flatten)]
    campaign: CampaignRow,
    reason: String,
    created_at: OffsetDateTime,
    created_by_user_id: Option<UserId>,
    updated_at: OffsetDateTime,
    updated_by_user_id: Option<UserId>,
    finished_at: Option<OffsetDateTime>,
}

impl TryFrom<CampaignRecordRow> for CampaignRecord {
    type Error = ReadCampaignError;

    fn try_from(row: CampaignRecordRow) -> Result<Self, Self::Error> {
        Ok(Self {
            campaign: row.campaign.try_into()?,
            reason: row.reason,
            created_at: row.created_at,
            created_by: row.created_by_user_id,
            updated_at: row.updated_at,
            updated_by: row.updated_by_user_id,
            finished_at: row.finished_at,
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
                include_running, generation
         FROM runtime_upgrade_campaigns
         WHERE finished_at IS NULL
         ORDER BY created_at, id"
    ))
    .fetch_all(executor)
    .await?;
    rows.into_iter().map(Campaign::try_from).collect()
}

/// `campaign_id` with its provenance, or `None` when there's no such
/// campaign. Takes no lock.
pub async fn get_record(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
) -> Result<Option<CampaignRecord>, ReadCampaignError> {
    let row = sqlx::query_as::<_, CampaignRecordRow>(sql!(
        "SELECT id, from_environment_ref, to_environment_ref, kind, phase,
                max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
                include_running, generation, reason, created_at, created_by_user_id,
                updated_at, updated_by_user_id, finished_at
         FROM runtime_upgrade_campaigns
         WHERE id = $1"
    ))
    .bind(campaign_id)
    .fetch_optional(executor)
    .await?;
    row.map(CampaignRecord::try_from).transpose()
}

/// Up to `limit` campaigns after `after`, in id order, which is creation
/// order: ids are UUIDv7. With `unfinished_only`, only canary, rolling and
/// paused ones. One unreadable row fails the whole read. Takes no lock.
pub async fn list_records(
    executor: impl sqlx::PgExecutor<'_>,
    unfinished_only: bool,
    after: Option<CampaignId>,
    limit: u32,
) -> Result<Vec<CampaignRecord>, ReadCampaignError> {
    let rows = sqlx::query_as::<_, CampaignRecordRow>(sql!(
        "SELECT id, from_environment_ref, to_environment_ref, kind, phase,
                max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
                include_running, generation, reason, created_at, created_by_user_id,
                updated_at, updated_by_user_id, finished_at
         FROM runtime_upgrade_campaigns
         WHERE (NOT $1 OR finished_at IS NULL)
           AND ($2::uuid IS NULL OR id > $2)
         ORDER BY id
         LIMIT $3"
    ))
    .bind(unfinished_only)
    .bind(after)
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await?;
    rows.into_iter().map(CampaignRecord::try_from).collect()
}

/// [`unfinished_from`] after taking the transaction-scoped advisory lock
/// that every [`create_campaign`] for `kind` and `source` takes first. Run
/// inside a caller's transaction, it holds off any such create until that
/// transaction ends, so a caller that refuses to act while a campaign is
/// unfinished (a policy block, say) can't race one into existence. The lock
/// lasts until the caller's transaction ends.
pub async fn unfinished_from_locked(
    conn: &mut sqlx::PgConnection,
    kind: CampaignKind,
    source: EnvironmentReference,
) -> Result<Option<CampaignId>, sqlx::Error> {
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(format!("runtime_upgrade_campaigns/{kind}/{source}"))
    .execute(&mut *conn)
    .await?;
    unfinished_from(&mut *conn, kind, source).await
}

/// Whether an unfinished campaign of `kind` moves `source`: the one
/// [`create_campaign`] would refuse a second for. Takes no lock, so it's a
/// read for a caller's check, not a guarantee; [`unfinished_from_locked`]
/// is the guarded form.
pub async fn unfinished_from(
    executor: impl sqlx::PgExecutor<'_>,
    kind: CampaignKind,
    source: EnvironmentReference,
) -> Result<Option<CampaignId>, sqlx::Error> {
    sqlx::query_scalar::<_, CampaignId>(sql!(
        "SELECT id
         FROM runtime_upgrade_campaigns
         WHERE from_environment_ref = $1
           AND kind = $2
           AND finished_at IS NULL
         ORDER BY id
         LIMIT 1"
    ))
    .bind(source.to_string())
    .bind(kind.as_str())
    .fetch_optional(executor)
    .await
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

    const fn get_mut(&mut self, phase: ItemPhase) -> &mut u64 {
        match phase {
            ItemPhase::Pending => &mut self.pending,
            ItemPhase::InFlight => &mut self.in_flight,
            ItemPhase::Done => &mut self.done,
            ItemPhase::Failed => &mut self.failed,
            ItemPhase::Skipped => &mut self.skipped,
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
        *counts.get_mut(phase.parse::<ItemPhase>()?) = count;
    }
    Ok(counts)
}

/// The items of each of `campaign_ids` counted by phase, in one read; a
/// campaign with no items gets zero counts. Takes no lock.
pub async fn item_counts_of(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_ids: &[CampaignId],
) -> Result<std::collections::HashMap<CampaignId, ItemCounts>, ItemCountsError> {
    let rows = sqlx::query_as::<_, (CampaignId, String, i64)>(sql!(
        "SELECT campaign_id, phase, count(*)
         FROM runtime_upgrade_items
         WHERE campaign_id = ANY($1)
         GROUP BY campaign_id, phase"
    ))
    .bind(campaign_ids)
    .fetch_all(executor)
    .await?;
    let mut counts: std::collections::HashMap<CampaignId, ItemCounts> = campaign_ids
        .iter()
        .map(|campaign_id| (*campaign_id, ItemCounts::default()))
        .collect();
    for (campaign_id, phase, count) in rows {
        // count(*) is never negative.
        *counts
            .entry(campaign_id)
            .or_default()
            .get_mut(phase.parse::<ItemPhase>()?) = u64::try_from(count).unwrap_or(0);
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
