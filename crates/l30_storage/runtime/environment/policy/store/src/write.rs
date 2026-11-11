//! Policy writes. Each verb locks the policy row, checks the rules that span
//! writes, applies its change and increments the generation, all in the
//! caller's transaction. Holding the row lock until the caller commits
//! serializes writers, so a check can't be invalidated before its change lands.
//! Run them in a READ COMMITTED transaction: under REPEATABLE READ or
//! SERIALIZABLE, a write that another transaction commits after the caller's
//! snapshot makes the row lock fail with a serialization error once it's
//! acquired, rather than letting the verb see the new row.
//!
//! A write whose actor is being deleted at the same time fails with a
//! `Database` error: a foreign-key violation if the deletion commits first, or
//! a deadlock if the deleted user is also the actor already stamped on a row
//! the write locked. In that case the write holds the row and needs the user
//! row for its foreign key, while the deletion's `ON DELETE SET NULL` cascade
//! holds the user row and needs the locked one, and Postgres aborts one side.
//!
//! The verbs take a transaction rather than any executor: each runs several
//! statements under one row lock, which only a transaction the caller commits
//! can hold.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{EnvironmentReference, UserId};
use sqlx::{Postgres, Transaction};

use crate::{RuntimeEnvironmentState, UnknownRuntimeEnvironmentState};

/// Why a policy changed: 1 to 1024 characters, as the schema requires, and no
/// NUL, which Postgres text can't hold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyChangeReason(String);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolicyChangeReasonError {
    #[error("a policy change reason must not be empty")]
    Empty,
    #[error(
        "a policy change reason must be at most {max} characters, got {0}",
        max = PolicyChangeReason::MAX_CHARS
    )]
    TooLong(usize),
    #[error("a policy change reason must not contain a NUL character")]
    ContainsNul,
}

impl PolicyChangeReason {
    /// Upper bound on a reason, in characters, matching the schema's `CHECK`.
    pub const MAX_CHARS: usize = 1024;

    pub fn new(reason: impl Into<String>) -> Result<Self, PolicyChangeReasonError> {
        let reason = reason.into();
        if reason.contains('\0') {
            return Err(PolicyChangeReasonError::ContainsNul);
        }
        // Postgres `char_length` counts characters, as `chars()` does.
        match reason.chars().count() {
            0 => Err(PolicyChangeReasonError::Empty),
            count if count > Self::MAX_CHARS => Err(PolicyChangeReasonError::TooLong(count)),
            _ => Ok(Self(reason)),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Who made a change and why, recorded on every row the write touches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyChange {
    /// `None` for a change no user made.
    pub actor: Option<UserId>,
    pub reason: PolicyChangeReason,
}

#[derive(Debug, thiserror::Error)]
pub enum SetDefaultError {
    #[error("the runtime environment policy row is missing")]
    PolicyRowMissing,
    #[error("environment {0} is blocked and can't become the default")]
    Blocked(EnvironmentReference),
    #[error("environment {environment} has an unknown stored state")]
    UnknownState {
        environment: EnvironmentReference,
        #[source]
        source: UnknownRuntimeEnvironmentState,
    },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum SetEnvironmentStateError {
    #[error("the runtime environment policy row is missing")]
    PolicyRowMissing,
    #[error(
        "environment {environment} is the cluster default and must stay allowed, not {requested}"
    )]
    DefaultMustStayAllowed {
        environment: EnvironmentReference,
        requested: RuntimeEnvironmentState,
    },
    #[error("blocking environment {0} requires a cluster default")]
    BlockRequiresDefault(EnvironmentReference),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum SetLegacyUnboundAllowedError {
    #[error("the runtime environment policy row is missing")]
    PolicyRowMissing,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// The policy row as seen under its row lock.
struct LockedPolicy {
    default_environment_ref: Option<String>,
}

/// Lock the policy row. `None` when the row is gone.
async fn lock_policy(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<LockedPolicy>, sqlx::Error> {
    let row = sqlx::query_as::<_, (Option<String>,)>(sql!(
        "SELECT default_environment_ref
         FROM runtime_environment_policy
         WHERE singleton
         FOR UPDATE"
    ))
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|(default_environment_ref,)| LockedPolicy {
        default_environment_ref,
    }))
}

/// Make `environment` the cluster default, recording it as `allowed` if it
/// isn't already. A replaced default keeps its `allowed` state. Returns the
/// new generation.
pub async fn set_default(
    tx: &mut Transaction<'_, Postgres>,
    environment: EnvironmentReference,
    change: &PolicyChange,
) -> Result<u64, SetDefaultError> {
    if lock_policy(tx).await?.is_none() {
        return Err(SetDefaultError::PolicyRowMissing);
    }
    let environment_ref = environment.to_string();
    let current = sqlx::query_as::<_, (String,)>(sql!(
        "SELECT state FROM runtime_environment_states WHERE environment_ref = $1"
    ))
    .bind(&environment_ref)
    .fetch_optional(&mut **tx)
    .await?;
    let current = current
        .map(|(state,)| state.parse::<RuntimeEnvironmentState>())
        .transpose()
        .map_err(|source| SetDefaultError::UnknownState {
            environment,
            source,
        })?;
    match current {
        Some(RuntimeEnvironmentState::Allowed) => {}
        Some(RuntimeEnvironmentState::Blocked) => {
            return Err(SetDefaultError::Blocked(environment));
        }
        None | Some(RuntimeEnvironmentState::Deprecated) => {
            upsert_state(
                tx,
                &environment_ref,
                RuntimeEnvironmentState::Allowed,
                change,
            )
            .await?;
        }
    }
    sqlx::query(sql!(
        "UPDATE runtime_environment_policy
         SET default_environment_ref = $1
         WHERE singleton"
    ))
    .bind(&environment_ref)
    .execute(&mut **tx)
    .await?;
    Ok(bump_generation(tx, change).await?)
}

/// Record `state` for `environment`. The default must stay `allowed`, and
/// blocking requires a cluster default. Returns the new generation.
///
/// It assigns the state without reading the old one, so it also replaces a
/// stored value this binary doesn't know. That's the repair for a state that
/// fails [`crate::get`]. [`set_default`] refuses such an environment instead,
/// since it would have to decide what the unknown state means.
pub async fn set_environment_state(
    tx: &mut Transaction<'_, Postgres>,
    environment: EnvironmentReference,
    state: RuntimeEnvironmentState,
    change: &PolicyChange,
) -> Result<u64, SetEnvironmentStateError> {
    let Some(locked) = lock_policy(tx).await? else {
        return Err(SetEnvironmentStateError::PolicyRowMissing);
    };
    let environment_ref = environment.to_string();
    match locked.default_environment_ref {
        Some(default)
            if default == environment_ref && state != RuntimeEnvironmentState::Allowed =>
        {
            return Err(SetEnvironmentStateError::DefaultMustStayAllowed {
                environment,
                requested: state,
            });
        }
        None if state == RuntimeEnvironmentState::Blocked => {
            return Err(SetEnvironmentStateError::BlockRequiresDefault(environment));
        }
        _ => {}
    }
    upsert_state(tx, &environment_ref, state, change).await?;
    Ok(bump_generation(tx, change).await?)
}

/// Set whether unbound historical contracts may execute. Returns the new
/// generation.
pub async fn set_legacy_unbound_allowed(
    tx: &mut Transaction<'_, Postgres>,
    allowed: bool,
    change: &PolicyChange,
) -> Result<u64, SetLegacyUnboundAllowedError> {
    if lock_policy(tx).await?.is_none() {
        return Err(SetLegacyUnboundAllowedError::PolicyRowMissing);
    }
    sqlx::query(sql!(
        "UPDATE runtime_environment_policy
         SET legacy_unbound_allowed = $1
         WHERE singleton"
    ))
    .bind(allowed)
    .execute(&mut **tx)
    .await?;
    Ok(bump_generation(tx, change).await?)
}

async fn upsert_state(
    tx: &mut Transaction<'_, Postgres>,
    environment_ref: &str,
    state: RuntimeEnvironmentState,
    change: &PolicyChange,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states
             (environment_ref, state, updated_by_user_id, reason)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (environment_ref) DO UPDATE
         SET state = EXCLUDED.state,
             updated_by_user_id = EXCLUDED.updated_by_user_id,
             reason = EXCLUDED.reason"
    ))
    .bind(environment_ref)
    .bind(state.as_str())
    .bind(change.actor)
    .bind(change.reason.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Raise the generation and stamp the change on the locked policy row. Every
/// write ends with this, in the same transaction as its change.
async fn bump_generation(
    tx: &mut Transaction<'_, Postgres>,
    change: &PolicyChange,
) -> Result<u64, sqlx::Error> {
    let (generation,) = sqlx::query_as::<_, (i64,)>(sql!(
        "UPDATE runtime_environment_policy
         SET generation = generation + 1,
             updated_by_user_id = $1,
             reason = $2
         WHERE singleton
         RETURNING generation"
    ))
    .bind(change.actor)
    .bind(change.reason.as_str())
    .fetch_one(&mut **tx)
    .await?;
    // The schema keeps the generation non-negative, but it's still a stored
    // value: report a violation as a decode error rather than panicking.
    u64::try_from(generation).map_err(|_| {
        sqlx::Error::Decode(
            format!("runtime environment policy generation {generation} is negative").into(),
        )
    })
}
