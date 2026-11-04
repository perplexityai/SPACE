//! Data access for the cluster-wide execution environment policy
//! (`runtime_environment_policy` and `runtime_environment_states`).
//!
//! The policy is one seeded row naming the cluster default, the legacy
//! permission and a generation, plus a state per environment. [`get`] reads
//! all of it in one statement ([`get_record`] adds who changed each row and
//! why), so a reader never sees a default from one
//! policy and states from another. The write verbs ([`set_default`],
//! [`set_environment_state`], [`set_legacy_unbound_allowed`]) take the
//! caller's open transaction, lock the policy row first, and increment the
//! generation in the same transaction as the change.
//!
//! # Rules the store enforces
//!
//! * **The default can be replaced, never cleared.** No verb writes a `NULL`
//!   default. A replaced default keeps its `allowed` state. The schema doesn't
//!   enforce this: direct SQL can still clear the column, as the migration's
//!   rollback procedure requires.
//! * **The default stays `allowed`.** Setting a default records the
//!   environment as `allowed`, refuses one that is `blocked`, and no state
//!   write may move the default off `allowed`. The composite foreign key on
//!   the policy row backs this up: it refuses a non-null default whose state
//!   isn't `allowed`.
//! * **Blocking requires a cluster default.** Without one, state rows narrow
//!   nothing on a node, so a block would sit inert and then take effect
//!   unannounced when a later write first sets a default.
//! * **The singleton row must exist.** Nothing in the schema protects it from
//!   a `DELETE`, so a missing row is an error for readers and writers alike,
//!   never the bootstrap policy.
//!
//! # What the columns don't mean
//!
//! `updated_at` on either table is trigger-maintained and also changes when a
//! user is deleted (the `ON DELETE SET NULL` cascade fires the trigger), so it
//! isn't policy history. The generation restarts at 0 if the schema is rolled
//! back and reapplied, so it orders and identifies versions only within one
//! schema lifetime: across a rollback, equal generations can name different
//! policies.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{DigestParseError, EnvironmentReference, UserId};

mod write;

pub use write::{
    PolicyChange, PolicyChangeReason, PolicyChangeReasonError, SetDefaultError,
    SetEnvironmentStateError, SetLegacyUnboundAllowedError, set_default, set_environment_state,
    set_legacy_unbound_allowed,
};

/// The cluster's state for one execution environment.
///
/// Closed: a stored value outside it fails the read rather than being
/// treated as any of these.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RuntimeEnvironmentState {
    /// Executes; existing work is left alone.
    Allowed,
    /// Executes. Moving existing work off it, where that's free, is a future
    /// upgrade reconciler's job; this crate only records the state.
    Deprecated,
    /// Never executes.
    Blocked,
}

impl RuntimeEnvironmentState {
    /// Every state, for error messages and exhaustive iteration.
    pub const ALL: [Self; 3] = [Self::Allowed, Self::Deprecated, Self::Blocked];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Deprecated => "deprecated",
            Self::Blocked => "blocked",
        }
    }

    /// Whether work may execute on an environment in this state.
    pub const fn executes(self) -> bool {
        match self {
            Self::Allowed | Self::Deprecated => true,
            Self::Blocked => false,
        }
    }
}

impl fmt::Display for RuntimeEnvironmentState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown runtime environment state {0:?}")]
pub struct UnknownRuntimeEnvironmentState(String);

impl FromStr for RuntimeEnvironmentState {
    type Err = UnknownRuntimeEnvironmentState;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "allowed" => Ok(Self::Allowed),
            "deprecated" => Ok(Self::Deprecated),
            "blocked" => Ok(Self::Blocked),
            other => Err(UnknownRuntimeEnvironmentState(other.to_owned())),
        }
    }
}

/// The whole cluster policy, as of one statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeEnvironmentPolicy {
    /// Incremented by every write. See the crate docs for what it can't be
    /// used for.
    pub generation: u64,
    /// `None` until the first default is written: each node keeps its
    /// configured default.
    pub default_environment: Option<EnvironmentReference>,
    /// `None` until first written: each node keeps its configured permission.
    pub legacy_unbound_allowed: Option<bool>,
    /// Every environment with a state, the default included.
    pub environments: BTreeMap<EnvironmentReference, RuntimeEnvironmentState>,
}

impl RuntimeEnvironmentPolicy {
    /// The environments that may execute (`allowed` and `deprecated`), in
    /// `EnvironmentReference` order. Includes the default.
    pub fn executing(&self) -> impl Iterator<Item = EnvironmentReference> + '_ {
        self.environments
            .iter()
            .filter(|(_, state)| state.executes())
            .map(|(environment, _)| *environment)
    }
}

/// Reading the policy failed. Every variant fails closed: the caller gets no
/// policy rather than a partial or default one.
#[derive(Debug, thiserror::Error)]
pub enum GetPolicyError {
    #[error("the runtime environment policy row is missing")]
    PolicyRowMissing,
    #[error("runtime environment policy generation {0} is negative")]
    InvalidGeneration(i64),
    #[error("stored environment reference {value:?} is invalid")]
    InvalidEnvironmentReference {
        value: String,
        #[source]
        source: DigestParseError,
    },
    #[error("environment {environment} has an unknown stored state")]
    UnknownState {
        environment: EnvironmentReference,
        #[source]
        source: UnknownRuntimeEnvironmentState,
    },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Who last changed a row, when and why, as the row records it. Not policy
/// history: see the crate docs for what `updated_at` can't be used for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedChange {
    pub updated_at: time::OffsetDateTime,
    /// `None` for a change no user made, or once that user was deleted.
    pub updated_by: Option<UserId>,
    pub reason: String,
}

/// The policy as [`get`] reads it, plus each row's [`RecordedChange`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeEnvironmentPolicyRecord {
    pub policy: RuntimeEnvironmentPolicy,
    /// The policy row's last change. `None` at the bootstrap generation, which
    /// no writer made and which carries no reason.
    pub last_change: Option<RecordedChange>,
    /// The last change of each environment's state, keyed like
    /// [`RuntimeEnvironmentPolicy::environments`].
    pub environment_changes: BTreeMap<EnvironmentReference, RecordedChange>,
}

#[derive(sqlx::FromRow)]
struct PolicyStateRow {
    generation: i64,
    default_environment_ref: Option<String>,
    legacy_unbound_allowed: Option<bool>,
    policy_updated_at: time::OffsetDateTime,
    policy_updated_by_user_id: Option<UserId>,
    policy_reason: Option<String>,
    environment_ref: Option<String>,
    state: Option<String>,
    state_updated_at: Option<time::OffsetDateTime>,
    state_updated_by_user_id: Option<UserId>,
    state_reason: Option<String>,
}

/// Read the whole policy. Accepts a pool or a transaction connection; takes
/// no lock.
pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<RuntimeEnvironmentPolicy, GetPolicyError> {
    Ok(get_record(executor).await?.policy)
}

/// Read the whole policy with who changed each row, when and why. Same
/// snapshot and failure rules as [`get`], which reads through it.
pub async fn get_record(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<RuntimeEnvironmentPolicyRecord, GetPolicyError> {
    // One statement, so the policy row and the states come from one snapshot
    // even under READ COMMITTED. The LEFT JOIN keeps the policy row when no
    // environment has a state, and yields no rows when the policy row is gone.
    let rows = sqlx::query_as::<_, PolicyStateRow>(sql!(
        "SELECT p.generation, p.default_environment_ref, p.legacy_unbound_allowed,
                p.updated_at AS policy_updated_at,
                p.updated_by_user_id AS policy_updated_by_user_id,
                p.reason AS policy_reason,
                s.environment_ref, s.state,
                s.updated_at AS state_updated_at,
                s.updated_by_user_id AS state_updated_by_user_id,
                s.reason AS state_reason
         FROM runtime_environment_policy p
         LEFT JOIN runtime_environment_states s ON TRUE"
    ))
    .fetch_all(executor)
    .await?;

    let Some(first) = rows.first() else {
        return Err(GetPolicyError::PolicyRowMissing);
    };
    let generation = u64::try_from(first.generation)
        .map_err(|_| GetPolicyError::InvalidGeneration(first.generation))?;
    let default_environment = first
        .default_environment_ref
        .as_deref()
        .map(parse_environment)
        .transpose()?;
    let legacy_unbound_allowed = first.legacy_unbound_allowed;
    // Every writer stamps a reason with its generation bump, so a policy row
    // without one is the migration's seed.
    let last_change = first.policy_reason.clone().map(|reason| RecordedChange {
        updated_at: first.policy_updated_at,
        updated_by: first.policy_updated_by_user_id,
        reason,
    });

    let mut environments = BTreeMap::new();
    let mut environment_changes = BTreeMap::new();
    for row in rows {
        // The state table's columns are NOT NULL; they're optional here only
        // because of the LEFT JOIN, which fills all of them or none.
        let (Some(environment), Some(state), Some(updated_at), Some(reason)) = (
            row.environment_ref,
            row.state,
            row.state_updated_at,
            row.state_reason,
        ) else {
            continue;
        };
        let environment = parse_environment(&environment)?;
        let state = state
            .parse()
            .map_err(|source| GetPolicyError::UnknownState {
                environment,
                source,
            })?;
        environments.insert(environment, state);
        environment_changes.insert(
            environment,
            RecordedChange {
                updated_at,
                updated_by: row.state_updated_by_user_id,
                reason,
            },
        );
    }

    Ok(RuntimeEnvironmentPolicyRecord {
        policy: RuntimeEnvironmentPolicy {
            generation,
            default_environment,
            legacy_unbound_allowed,
            environments,
        },
        last_change,
        environment_changes,
    })
}

/// Read only the cluster default, without the state rows. Unlike [`get`], an
/// environment whose stored state this build can't read doesn't fail it, so a
/// caller that needs only the default can still act while that row awaits
/// repair. Takes no lock.
pub async fn get_default(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<EnvironmentReference>, GetPolicyError> {
    let row = sqlx::query_as::<_, (Option<String>,)>(sql!(
        "SELECT default_environment_ref FROM runtime_environment_policy WHERE singleton"
    ))
    .fetch_optional(executor)
    .await?;
    let Some((default,)) = row else {
        return Err(GetPolicyError::PolicyRowMissing);
    };
    default.as_deref().map(parse_environment).transpose()
}

/// Read only the policy generation, for a caller deciding whether a write
/// conditional on one could still apply. Like [`get_default`], an unreadable
/// state row doesn't fail it. Takes no lock.
pub async fn get_generation(executor: impl sqlx::PgExecutor<'_>) -> Result<u64, GetPolicyError> {
    let row = sqlx::query_as::<_, (i64,)>(sql!(
        "SELECT generation FROM runtime_environment_policy WHERE singleton"
    ))
    .fetch_optional(executor)
    .await?;
    let Some((generation,)) = row else {
        return Err(GetPolicyError::PolicyRowMissing);
    };
    u64::try_from(generation).map_err(|_| GetPolicyError::InvalidGeneration(generation))
}

fn parse_environment(value: &str) -> Result<EnvironmentReference, GetPolicyError> {
    value
        .parse()
        .map_err(|source| GetPolicyError::InvalidEnvironmentReference {
            value: value.to_owned(),
            source,
        })
}

#[cfg(test)]
mod tests;
