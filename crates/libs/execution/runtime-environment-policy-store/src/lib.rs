//! Data access for the cluster-wide execution environment policy
//! (`runtime_environment_policy` and `runtime_environment_states`).
//!
//! The policy is one seeded row naming the cluster default, the legacy
//! permission and a generation, plus a state per environment. [`get`] reads
//! all of it in one statement, so a reader never sees a default from one
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
use pplx_space_common_core::types::{DigestParseError, EnvironmentReference};

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

#[derive(sqlx::FromRow)]
struct PolicyStateRow {
    generation: i64,
    default_environment_ref: Option<String>,
    legacy_unbound_allowed: Option<bool>,
    environment_ref: Option<String>,
    state: Option<String>,
}

/// Read the whole policy. Accepts a pool or a transaction connection; takes
/// no lock.
pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<RuntimeEnvironmentPolicy, GetPolicyError> {
    // One statement, so the policy row and the states come from one snapshot
    // even under READ COMMITTED. The LEFT JOIN keeps the policy row when no
    // environment has a state, and yields no rows when the policy row is gone.
    let rows = sqlx::query_as::<_, PolicyStateRow>(sql!(
        "SELECT p.generation, p.default_environment_ref, p.legacy_unbound_allowed,
                s.environment_ref, s.state
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

    let mut environments = BTreeMap::new();
    for row in rows {
        let (Some(environment), Some(state)) = (row.environment_ref, row.state) else {
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
    }

    Ok(RuntimeEnvironmentPolicy {
        generation,
        default_environment,
        legacy_unbound_allowed,
        environments,
    })
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
