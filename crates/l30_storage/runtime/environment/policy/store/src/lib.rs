//! Data access for the cluster-wide execution environment policy
//! (`runtime_environment_policy` and `runtime_environment_states`).
//!
//! The policy is one seeded row naming the cluster default, the legacy
//! permission and a generation, plus a state and memory-retirement flag per
//! environment. [`get`] reads
//! all of it in one statement ([`get_record`] adds who changed each row and
//! why), so a reader never sees a default from one
//! policy and states from another. The write verbs ([`set_default`],
//! [`set_environment_state`], [`set_environment_memory_retired`],
//! [`set_legacy_unbound_allowed`]) take the
//! caller's open transaction, lock the policy row first, and increment the
//! generation in the same transaction as the change. They don't tell nodes
//! about the change: the admin API's write handlers queue the node
//! reconciles in that same transaction. [`set_legacy_unbound_allowed`] has no
//! handler yet; the one that calls it has to queue them too.
//!
//! # Rules the store enforces
//!
//! * **The default can be replaced, never cleared.** No verb writes a `NULL`
//!   default. A replaced default keeps its `allowed` state. The schema doesn't
//!   enforce this: direct SQL can still clear the column, as the migration's
//!   rollback procedure requires.
//! * **The default stays `allowed`.** Setting a default records the
//!   environment as `allowed`, refuses one that is `blocked` or memory-retired,
//!   and no state write may move the default off `allowed`. The composite
//!   foreign key on the policy row backs this up: it refuses a non-null default
//!   whose state isn't `allowed`.
//! * **Blocking or retiring memory requires a cluster default.** Without one,
//!   state rows narrow nothing on a node and activation upgrades don't move
//!   sandboxes, so either write would sit inert and then take effect
//!   unannounced when a later write first sets a default.
//! * **Memory retirement applies only while `deprecated`, and unretiring is
//!   allowed until `blocked`.** Promoting a memory-retired environment back to
//!   `allowed` or making it the default is refused until its memory is
//!   unretired, and once an environment is `blocked` its memory can't be
//!   unretired.
//! * **The singleton row must exist.** Nothing in the schema protects it from
//!   a `DELETE`, so a missing row is an error for readers and writers alike,
//!   never the bootstrap policy.
//!
//! # What the columns don't mean
//!
//! `updated_at` on the policy row and on `runtime_environment_states` is
//! trigger-maintained on state writes (`state`, `updated_by_user_id`, `reason`)
//! and also changes when that actor is deleted (the `ON DELETE SET NULL`
//! cascade fires the trigger), so it isn't policy history. Memory writes stamp
//! `memory_updated_at` directly without moving `updated_at`. The generation
//! restarts at 0 if the schema is rolled back and reapplied, so it orders and
//! identifies versions only within one schema lifetime: across a rollback,
//! equal generations can name different policies.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use pplx_space_accounts::UserId;
use pplx_space_content_digest::DigestParseError;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sql_locator::sql;

mod write;

pub use write::{
    PolicyChange, PolicyChangeReason, PolicyChangeReasonError, SetDefaultError,
    SetEnvironmentMemoryError, SetEnvironmentStateError, SetLegacyUnboundAllowedError, set_default,
    set_environment_memory_retired, set_environment_state, set_legacy_unbound_allowed,
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
    /// Environments in [`Self::environments`] whose checkpoints' memory has
    /// been retired (`memory_retired = TRUE`).
    pub memory_retired_environments: BTreeSet<EnvironmentReference>,
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

    /// Whether `environment`'s checkpoints have their memory retired.
    pub fn is_memory_retired(&self, environment: EnvironmentReference) -> bool {
        self.memory_retired_environments.contains(&environment)
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
    /// The last memory-retirement change of each environment whose memory has
    /// been retired or unretired (`None` until first retired).
    pub environment_memory_changes: BTreeMap<EnvironmentReference, RecordedChange>,
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
    memory_retired: Option<bool>,
    memory_updated_at: Option<time::OffsetDateTime>,
    memory_updated_by_user_id: Option<UserId>,
    memory_reason: Option<String>,
}

/// Read the whole policy. Accepts a pool or a transaction connection; takes
/// no lock.
pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<RuntimeEnvironmentPolicy, GetPolicyError> {
    Ok(get_record(executor).await?.policy)
}

/// Read the policy after acquiring `FOR KEY SHARE` on the singleton policy
/// row, so a restore that already committed its `restoring` lease waits for any
/// in-flight policy write (`FOR UPDATE` on `runtime_environment_policy`) and
/// then reads the committed `runtime_environment_states` snapshot in the next
/// `READ COMMITTED` statement. `FOR KEY SHARE` is shared across concurrent
/// restores.
pub async fn get_serialized(
    pool: &sqlx::PgPool,
) -> Result<RuntimeEnvironmentPolicy, GetPolicyError> {
    let mut tx = pool.begin().await?;
    let locked: Option<(bool,)> = sqlx::query_as(sql!(
        "SELECT singleton
         FROM runtime_environment_policy
         WHERE singleton
         FOR KEY SHARE"
    ))
    .fetch_optional(&mut *tx)
    .await?;
    if locked.is_none() {
        return Err(GetPolicyError::PolicyRowMissing);
    }
    let policy = get(&mut *tx).await?;
    tx.commit().await?;
    Ok(policy)
}

/// Read the whole policy with who changed each row, when and why. Same
/// snapshot and failure rules as [`get`], which reads through it.
pub async fn get_record(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<RuntimeEnvironmentPolicyRecord, GetPolicyError> {
    // One statement, so the policy row and the states come from one snapshot
    // even under READ COMMITTED. The LEFT JOIN keeps the policy row when no
    // environment has a state, and yields no rows when the policy row is gone.
    // `to_jsonb(s)` reads the memory-retirement columns when their migration
    // has run and yields `NULL` before it has (or after its down migration),
    // so policy reads stay available across mixed schema rollouts.
    let rows = sqlx::query_as::<_, PolicyStateRow>(sql!(
        "SELECT p.generation, p.default_environment_ref, p.legacy_unbound_allowed,
                p.updated_at AS policy_updated_at,
                p.updated_by_user_id AS policy_updated_by_user_id,
                p.reason AS policy_reason,
                s.environment_ref, s.state,
                s.updated_at AS state_updated_at,
                s.updated_by_user_id AS state_updated_by_user_id,
                s.reason AS state_reason,
                (to_jsonb(s) ->> 'memory_retired')::boolean AS memory_retired,
                (to_jsonb(s) ->> 'memory_updated_at')::timestamptz AS memory_updated_at,
                (to_jsonb(s) ->> 'memory_updated_by_user_id')::uuid AS memory_updated_by_user_id,
                to_jsonb(s) ->> 'memory_reason' AS memory_reason
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
    let mut memory_retired_environments = BTreeSet::new();
    let mut environment_changes = BTreeMap::new();
    let mut environment_memory_changes = BTreeMap::new();
    for row in rows {
        // The state table's non-memory columns are NOT NULL; they're optional
        // here only because of the LEFT JOIN, which fills all of them or none.
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
        if row.memory_retired == Some(true) {
            memory_retired_environments.insert(environment);
        }
        environment_changes.insert(
            environment,
            RecordedChange {
                updated_at,
                updated_by: row.state_updated_by_user_id,
                reason,
            },
        );
        if let (Some(memory_updated_at), Some(memory_reason)) =
            (row.memory_updated_at, row.memory_reason)
        {
            environment_memory_changes.insert(
                environment,
                RecordedChange {
                    updated_at: memory_updated_at,
                    updated_by: row.memory_updated_by_user_id,
                    reason: memory_reason,
                },
            );
        }
    }

    Ok(RuntimeEnvironmentPolicyRecord {
        policy: RuntimeEnvironmentPolicy {
            generation,
            default_environment,
            legacy_unbound_allowed,
            environments,
            memory_retired_environments,
        },
        last_change,
        environment_changes,
        environment_memory_changes,
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

/// [`get_default`] under a share lock on the policy row, held until the
/// caller's transaction ends. Every write here takes the row `FOR UPDATE`
/// first, so no write changes the default between this read and the
/// caller's commit; two callers holding the share lock don't block each
/// other.
pub async fn share_lock_default(
    conn: &mut sqlx::PgConnection,
) -> Result<Option<EnvironmentReference>, GetPolicyError> {
    let row = sqlx::query_as::<_, (Option<String>,)>(sql!(
        "SELECT default_environment_ref FROM runtime_environment_policy WHERE singleton FOR SHARE"
    ))
    .fetch_optional(&mut *conn)
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

/// [`get_generation`] under the policy row lock, which every write here takes
/// first and holds until `tx` ends. A caller that reads before writing in the
/// same transaction takes it first, so no other policy write lands between
/// its read and its write.
pub async fn lock_generation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<u64, GetPolicyError> {
    let row = sqlx::query_as::<_, (i64,)>(sql!(
        "SELECT generation FROM runtime_environment_policy WHERE singleton FOR UPDATE"
    ))
    .fetch_optional(&mut **tx)
    .await?;
    let Some((generation,)) = row else {
        return Err(GetPolicyError::PolicyRowMissing);
    };
    u64::try_from(generation).map_err(|_| GetPolicyError::InvalidGeneration(generation))
}

/// Read the `deprecated` environments whose checkpoint memory is currently
/// retired (`memory_retired = true`). Like [`get_default`], an unreadable state
/// row on another environment doesn't fail it, and `to_jsonb(s)` yields `false`
/// before the memory-retirement migration has run.
pub async fn deprecated_memory_retired_environments(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<BTreeSet<EnvironmentReference>, GetPolicyError> {
    let rows = sqlx::query_as::<_, (String,)>(sql!(
        "SELECT s.environment_ref
           FROM runtime_environment_states s
          WHERE s.state = 'deprecated'
            AND COALESCE((to_jsonb(s) ->> 'memory_retired')::boolean, FALSE)"
    ))
    .fetch_all(executor)
    .await?;
    rows.into_iter()
        .map(|(reference,)| parse_environment(&reference))
        .collect()
}

/// Whether `runtime_environment_states.memory_retired` exists in the current
/// schema, so callers can defer pre-transaction checks to the write's own
/// [`SetEnvironmentMemoryError::SchemaNotMigrated`] refusal before the
/// migration has run.
pub async fn memory_retirement_schema_migrated(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM pg_attribute
              WHERE attrelid = 'runtime_environment_states'::regclass
                AND attname = 'memory_retired'
                AND NOT attisdropped
         )"
    ))
    .fetch_one(executor)
    .await
}

fn parse_environment(value: &str) -> Result<EnvironmentReference, GetPolicyError> {
    value
        .parse()
        .map_err(|source| GetPolicyError::InvalidEnvironmentReference {
            value: value.to_owned(),
            source,
        })
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;
