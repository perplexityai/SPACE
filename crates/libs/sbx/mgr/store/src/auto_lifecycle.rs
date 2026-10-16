use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, Weak},
};

use pplx_space_common_core::types::{
    ArtifactKind, AutoLifecycleActivitySource, AutoLifecycleConditionKind,
    AutoLifecycleConditionRuntimeState, AutoLifecycleConditionSet, AutoLifecycleConditionSetId,
    AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleRuntimeState, AutoLifecycleStatus,
    AutoLifecycleTransition, AutoLifecycleWakeSource,
};
use tonic::Status;

use pplx_space_sbx_mgr_client::proto;

pub const ACCESS_IDLE_CONDITION_SET: AutoLifecycleConditionSetId =
    AutoLifecycleConditionSetId::AccessIdle;
pub const COMMAND_IDLE_CONDITION_SET: AutoLifecycleConditionSetId =
    AutoLifecycleConditionSetId::CommandIdle;
pub const GUEST_ACTIVITY_CONDITION_SET: AutoLifecycleConditionSetId =
    AutoLifecycleConditionSetId::GuestActivity;

#[cfg(any(test, feature = "test-support"))]
pub const TEST_GUEST_ACTIVITY_HOLD_CAP: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
pub fn test_guest_activity_hold_cap() -> pplx_space_common_core::types::SimpleDuration {
    TEST_GUEST_ACTIVITY_HOLD_CAP.try_into().unwrap()
}

type LifecycleLockRegistry = Mutex<HashMap<String, Weak<LifecycleMutex>>>;

/// A keyed lock whose identity lives as long as any caller, waiter, or guard.
#[derive(Debug)]
pub struct LifecycleMutex {
    sandbox_id: String,
    registry: Weak<LifecycleLockRegistry>,
    mutex: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug)]
pub struct LifecycleMutexGuard {
    // Unlock before releasing the identity retained by this guard.
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _owner: Arc<LifecycleMutex>,
}

impl LifecycleMutex {
    pub async fn lock(self: &Arc<Self>) -> LifecycleMutexGuard {
        self.clone().lock_owned().await
    }

    pub async fn lock_owned(self: Arc<Self>) -> LifecycleMutexGuard {
        let guard = self.mutex.clone().lock_owned().await;
        LifecycleMutexGuard {
            _guard: guard,
            _owner: self,
        }
    }

    pub fn try_lock(self: &Arc<Self>) -> Result<LifecycleMutexGuard, tokio::sync::TryLockError> {
        self.clone().try_lock_owned()
    }

    pub fn try_lock_owned(
        self: Arc<Self>,
    ) -> Result<LifecycleMutexGuard, tokio::sync::TryLockError> {
        let guard = self.mutex.clone().try_lock_owned()?;
        Ok(LifecycleMutexGuard {
            _guard: guard,
            _owner: self,
        })
    }
}

impl Drop for LifecycleMutex {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        // The registry owns only weak references: removing an entry cannot
        // recursively drop another lock while this registry mutex is held.
        let mut locks = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if locks
            .get(&self.sandbox_id)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
        {
            locks.remove(&self.sandbox_id);
        }
    }
}

fn lifecycle_lock(registry: &Arc<LifecycleLockRegistry>, sandbox_id: &str) -> Arc<LifecycleMutex> {
    let mut locks = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(lock) = locks.get(sandbox_id).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(LifecycleMutex {
        sandbox_id: sandbox_id.to_owned(),
        registry: Arc::downgrade(registry),
        mutex: Arc::new(tokio::sync::Mutex::new(())),
    });
    locks.insert(sandbox_id.to_owned(), Arc::downgrade(&lock));
    lock
}

#[derive(Clone, Default)]
pub struct AutoLifecycleCoordinator {
    wake_locks: Arc<LifecycleLockRegistry>,
    /// Serializes publication and consumption of immutable local artifacts.
    /// Live full-artifact capture takes this after `wake_locks`; disk
    /// publication, direct restore, and backup admission take it directly.
    artifact_locks: Arc<LifecycleLockRegistry>,
    /// Per-sandbox-id mutex guarding S3 snapshot staging. Distinct from
    /// `wake_locks` because some staging call sites are reachable while
    /// the wake lock is already held on the same task, and
    /// `tokio::sync::Mutex` is non-reentrant.
    staging_locks: Arc<LifecycleLockRegistry>,
    /// Instant of the most recent completed wake per canonical sandbox id.
    /// Read by the suspend path both to surface suspend-shortly-after-wake
    /// oscillation (each cycle pays a full suspend snapshot) and to arbitrate
    /// it: a suspend whose request arrived before this instant queued behind
    /// the wake and is rejected as superseded. Bounded by sandboxes resident
    /// on this node; entries drop in [`Self::remove_sandbox`].
    last_wake_completed: Arc<dashmap::DashMap<String, std::time::Instant>>,
    /// A test's two-phase rendezvous before auto-lifecycle Stop writes a
    /// `suspended` row, so the test can move the row in between.
    #[cfg(any(test, feature = "test-support"))]
    pub suspended_stop_barrier: Arc<std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>>,
}

impl AutoLifecycleCoordinator {
    pub fn wake_lock(&self, sandbox_id: &str) -> Arc<LifecycleMutex> {
        lifecycle_lock(&self.wake_locks, sandbox_id)
    }

    /// Per-sandbox-id mutex for S3 staging serialization. Held independently
    /// of `wake_lock` so callers may hold both.
    pub fn staging_lock(&self, sandbox_id: &str) -> Arc<LifecycleMutex> {
        lifecycle_lock(&self.staging_locks, sandbox_id)
    }

    pub fn artifact_lock(&self, sandbox_id: &str) -> Arc<LifecycleMutex> {
        lifecycle_lock(&self.artifact_locks, sandbox_id)
    }

    pub async fn artifact_publication_locks(
        &self,
        sandbox_id: &str,
        artifact_kind: ArtifactKind,
    ) -> (Option<LifecycleMutexGuard>, LifecycleMutexGuard) {
        let wake_guard = if artifact_kind == ArtifactKind::Full {
            Some(self.wake_lock(sandbox_id).lock_owned().await)
        } else {
            None
        };
        let artifact_guard = self.artifact_lock(sandbox_id).lock_owned().await;
        (wake_guard, artifact_guard)
    }

    pub fn try_staging_guard(&self, sandbox_id: &str) -> Option<LifecycleMutexGuard> {
        self.staging_lock(sandbox_id).try_lock_owned().ok()
    }

    /// Record that a wake just completed for `sandbox_id` (the canonical
    /// id, not a request-supplied prefix — see the wake-lock keying note in
    /// the network-policy handler).
    pub fn record_wake_completed(&self, sandbox_id: &str) {
        self.last_wake_completed
            .insert(sandbox_id.to_string(), std::time::Instant::now());
    }

    /// Elapsed time since the most recent completed wake for `sandbox_id`,
    /// or `None` when no wake has completed since the entry was last
    /// cleaned up.
    pub fn since_last_wake_completed(&self, sandbox_id: &str) -> Option<std::time::Duration> {
        self.last_wake_completed
            .get(sandbox_id)
            .map(|completed_at| completed_at.elapsed())
    }

    /// Instant of the most recent completed wake for `sandbox_id`, or `None`
    /// when no wake has completed since the entry was last cleaned up. Used
    /// by suspend arbitration to order a suspend request's arrival against a
    /// wake it may have queued behind.
    pub fn last_wake_completed_at(&self, sandbox_id: &str) -> Option<std::time::Instant> {
        self.last_wake_completed
            .get(sandbox_id)
            .map(|completed_at| *completed_at)
    }

    pub fn remove_sandbox(&self, sandbox_id: &str) {
        // Never split the identity shared by in-flight owners and waiters.
        // Each lock retires its own weak registry entry on final release.
        self.last_wake_completed.remove(sandbox_id);
    }
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn validate_v1_policy(policy: &AutoLifecyclePolicy) -> Result<(), String> {
    let mut normalized = policy.clone();
    normalized.normalize_in_place();
    normalized.validate_basic()?;

    for source in &normalized.wake.on {
        if !matches!(
            source,
            AutoLifecycleWakeSource::Access | AutoLifecycleWakeSource::Command
        ) {
            return Err("only wake.on=['access', 'command'] is supported in v1".to_string());
        }
    }

    for (name, condition) in &normalized.condition_sets {
        match *name {
            ACCESS_IDLE_CONDITION_SET => {
                if condition.kind != AutoLifecycleConditionKind::Inactivity
                    || condition.activity_source != Some(AutoLifecycleActivitySource::Access)
                {
                    return Err(
                        "access_idle must be an inactivity condition with activity_source='access'"
                            .to_string(),
                    );
                }
            }
            COMMAND_IDLE_CONDITION_SET => {
                if condition.kind != AutoLifecycleConditionKind::Inactivity
                    || condition.activity_source != Some(AutoLifecycleActivitySource::Command)
                {
                    return Err(
                        "command_idle must be an inactivity condition with activity_source='command'"
                            .to_string(),
                    );
                }
            }
            GUEST_ACTIVITY_CONDITION_SET => {
                if condition.kind != AutoLifecycleConditionKind::GuestActivity
                    || condition.activity_source.is_some()
                {
                    return Err(
                        "guest_activity must be a guest_activity condition without activity_source"
                            .to_string(),
                    );
                }
                if normalized.has_stages_for_condition_set(name)
                    && condition.max_guest_idle_extension.is_none()
                {
                    return Err(
                        "guest_activity with lifecycle stages must specify max_guest_idle_extension"
                            .to_string(),
                    );
                }
            }
        }
    }

    Ok(())
}

pub fn policy_from_proto(
    policy: &proto::AutoLifecyclePolicy,
) -> Result<AutoLifecyclePolicy, Status> {
    let policy = AutoLifecyclePolicy::try_from(policy).map_err(Status::from)?;
    validate_v1_policy(&policy).map_err(Status::invalid_argument)?;
    Ok(policy.normalized())
}

pub fn policy_to_proto(policy: &AutoLifecyclePolicy) -> proto::AutoLifecyclePolicy {
    proto::AutoLifecyclePolicy::from(policy)
}

pub fn status_to_proto(status: &AutoLifecycleStatus) -> proto::AutoLifecycleStatus {
    proto::AutoLifecycleStatus::from(status)
}

fn is_v1_runtime_condition(
    condition_name: &AutoLifecycleConditionSetId,
    condition: &AutoLifecycleConditionSet,
) -> bool {
    match (*condition_name, condition.kind) {
        (ACCESS_IDLE_CONDITION_SET, AutoLifecycleConditionKind::Inactivity) => {
            condition.activity_source == Some(AutoLifecycleActivitySource::Access)
        }
        (COMMAND_IDLE_CONDITION_SET, AutoLifecycleConditionKind::Inactivity) => {
            condition.activity_source == Some(AutoLifecycleActivitySource::Command)
        }
        (GUEST_ACTIVITY_CONDITION_SET, AutoLifecycleConditionKind::GuestActivity) => {
            condition.activity_source.is_none()
        }
        _ => false,
    }
}

pub fn initial_runtime_state(policy: &AutoLifecyclePolicy, now: u64) -> AutoLifecycleRuntimeState {
    let mut state = initial_disarmed_runtime_state(policy);
    arm_inactivity_conditions(&mut state, policy, now);
    state
}

/// Build policy state before the sandbox can accept access or commands.
pub fn initial_disarmed_runtime_state(policy: &AutoLifecyclePolicy) -> AutoLifecycleRuntimeState {
    let mut state = AutoLifecycleRuntimeState::default();
    for (name, condition_set) in &policy.condition_sets {
        if !is_v1_runtime_condition(name, condition_set)
            || !policy.has_stages_for_condition_set(name)
        {
            continue;
        }
        let runtime_state = match condition_set.kind {
            AutoLifecycleConditionKind::Inactivity => AutoLifecycleConditionRuntimeState {
                armed: false,
                epoch_at: None,
                last_touched_at: None,
                last_fired_stage_index: None,
            },
            AutoLifecycleConditionKind::GuestActivity => AutoLifecycleConditionRuntimeState {
                armed: false,
                epoch_at: None,
                last_touched_at: None,
                last_fired_stage_index: None,
            },
        };
        state.conditions.insert(*name, runtime_state);
    }
    state.next_auto_event_at = compute_next_auto_event(policy, &state);
    state
}

/// Arm clocks exactly once when a sandbox first becomes activity-capable.
pub fn arm_inactivity_conditions(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
) {
    for (name, condition_set) in &policy.condition_sets {
        if condition_set.kind != AutoLifecycleConditionKind::Inactivity
            || !policy.has_stages_for_condition_set(name)
        {
            continue;
        }
        let condition = state.conditions.entry(*name).or_default();
        if condition.armed {
            continue;
        }
        condition.armed = true;
        condition.epoch_at = Some(now);
        condition.last_touched_at = Some(now);
        condition.last_fired_stage_index = None;
    }
    state.next_auto_event_at = compute_next_auto_event(policy, state);
}

fn touch_sources(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
    sources: &[AutoLifecycleActivitySource],
) {
    touch_activity_clocks(state, sources, now);

    let mut touched = false;
    for (name, condition_set) in &policy.condition_sets {
        if condition_set.kind != AutoLifecycleConditionKind::Inactivity {
            continue;
        }
        let Some(activity_source) = condition_set.activity_source else {
            continue;
        };
        if !sources.contains(&activity_source) || !policy.has_stages_for_condition_set(name) {
            continue;
        }

        let condition = state.conditions.entry(*name).or_default();
        condition.armed = true;
        condition.epoch_at = Some(now);
        condition.last_touched_at = Some(now);
        condition.last_fired_stage_index = None;
        touched = true;
    }

    if touched {
        clear_auto_error(state);
    }
}

fn apply_observed_touch_sources(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    observed_at: u64,
    sources: &[AutoLifecycleActivitySource],
) -> bool {
    let mut touched = apply_observed_activity_clocks(state, sources, observed_at);
    for (name, condition_set) in &policy.condition_sets {
        if condition_set.kind != AutoLifecycleConditionKind::Inactivity {
            continue;
        }
        let Some(activity_source) = condition_set.activity_source else {
            continue;
        };
        if !sources.contains(&activity_source) || !policy.has_stages_for_condition_set(name) {
            continue;
        }

        let condition = state.conditions.entry(*name).or_default();
        let next_epoch = condition
            .epoch_at
            .map(|epoch| epoch.max(observed_at))
            .unwrap_or(observed_at);
        let changed = !condition.armed || condition.epoch_at != Some(next_epoch);
        if !changed {
            continue;
        }

        condition.armed = true;
        condition.epoch_at = Some(next_epoch);
        condition.last_touched_at = Some(
            condition
                .last_touched_at
                .map(|last_touched| last_touched.max(observed_at))
                .unwrap_or(observed_at),
        );
        condition.last_fired_stage_index = None;
        touched = true;
    }

    if touched {
        clear_auto_error(state);
        recompute_runtime_state(state, policy);
    }

    touched
}

fn touch_activity_clocks(
    state: &mut AutoLifecycleRuntimeState,
    sources: &[AutoLifecycleActivitySource],
    now: u64,
) {
    for source in sources {
        match source {
            AutoLifecycleActivitySource::Access => state.last_access_at = Some(now),
            AutoLifecycleActivitySource::Command => state.last_command_at = Some(now),
        }
    }
}

fn apply_observed_activity_clocks(
    state: &mut AutoLifecycleRuntimeState,
    sources: &[AutoLifecycleActivitySource],
    observed_at: u64,
) -> bool {
    let mut touched = false;
    for source in sources {
        let slot = match source {
            AutoLifecycleActivitySource::Access => &mut state.last_access_at,
            AutoLifecycleActivitySource::Command => &mut state.last_command_at,
        };
        let next = slot
            .map(|current| current.max(observed_at))
            .unwrap_or(observed_at);
        if *slot != Some(next) {
            *slot = Some(next);
            touched = true;
        }
    }
    touched
}

pub fn touch_access(state: &mut AutoLifecycleRuntimeState, policy: &AutoLifecyclePolicy, now: u64) {
    touch_sources(state, policy, now, &[AutoLifecycleActivitySource::Access]);
}

/// Apply an externally observed access touch without moving an existing epoch backward.
pub fn apply_observed_access_touch(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    observed_at: u64,
) -> bool {
    apply_observed_touch_sources(
        state,
        policy,
        observed_at,
        &[AutoLifecycleActivitySource::Access],
    )
}

pub fn touch_command(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
) {
    touch_sources(state, policy, now, &[AutoLifecycleActivitySource::Command]);
}

/// Apply an externally observed command touch without moving an existing epoch backward.
pub fn apply_observed_command_and_access_touch(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    observed_at: u64,
) -> bool {
    apply_observed_touch_sources(
        state,
        policy,
        observed_at,
        &[
            AutoLifecycleActivitySource::Access,
            AutoLifecycleActivitySource::Command,
        ],
    )
}

pub fn touch_command_and_access(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
) {
    touch_sources(
        state,
        policy,
        now,
        &[
            AutoLifecycleActivitySource::Access,
            AutoLifecycleActivitySource::Command,
        ],
    );
}

pub fn disarm_guest_activity_on_external_access(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
) {
    let mut matched = false;
    for (name, condition_set) in &policy.condition_sets {
        if condition_set.kind != AutoLifecycleConditionKind::GuestActivity
            || !policy.has_stages_for_condition_set(name)
        {
            continue;
        }

        let condition = state.conditions.entry(*name).or_default();
        condition.armed = false;
        condition.epoch_at = None;
        condition.last_touched_at = Some(now);
        condition.last_fired_stage_index = None;
        matched = true;
    }

    if matched {
        clear_auto_error(state);
    }
}

pub fn notify_guest_activity_state(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    idle: bool,
    now: u64,
) -> bool {
    let mut matched = false;
    for (name, condition_set) in &policy.condition_sets {
        if condition_set.kind != AutoLifecycleConditionKind::GuestActivity
            || !policy.has_stages_for_condition_set(name)
        {
            continue;
        }

        let condition = state.conditions.entry(*name).or_default();
        condition.armed = idle;
        condition.epoch_at = idle.then_some(now);
        condition.last_touched_at = Some(now);
        condition.last_fired_stage_index = None;
        matched = true;
    }

    if matched {
        clear_auto_error(state);
    }

    matched
}

pub fn extend_guest_activity_idle_epoch(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
    active_for_secs: u64,
) -> bool {
    let mut matched = false;
    let active_until = now.saturating_add(active_for_secs);
    for (name, condition_set) in &policy.condition_sets {
        if condition_set.kind != AutoLifecycleConditionKind::GuestActivity
            || !policy.has_stages_for_condition_set(name)
        {
            continue;
        }

        let condition = state.conditions.entry(*name).or_default();
        condition.armed = true;
        condition.epoch_at = Some(condition.epoch_at.unwrap_or(0).max(active_until));
        condition.last_touched_at = Some(now);
        condition.last_fired_stage_index = None;
        matched = true;
    }

    if matched {
        clear_auto_error(state);
    }

    matched
}

pub fn recompute_runtime_state(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
) {
    state.open_activity_leases = 0;
    state.next_auto_event_at = compute_next_auto_event(policy, state);
}

pub fn mark_transition(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    condition_set: &AutoLifecycleConditionSetId,
    transition: AutoLifecycleTransition,
    now: u64,
    stage_index: usize,
) {
    if let Some(condition) = state.conditions.get_mut(condition_set) {
        condition.last_fired_stage_index = Some(stage_index as u32);
    }
    state.last_auto_transition = Some(transition);
    state.last_auto_transition_at = Some(now);
    clear_auto_error(state);
    recompute_runtime_state(state, policy);
}

pub fn mark_error(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    error: impl Into<String>,
) {
    state.last_auto_error = Some(error.into());
    recompute_runtime_state(state, policy);
}

/// A failed auto-suspend can only recur when its cause is a wedged capture
/// volume, so it gets a bounded budget instead of a retry every reconcile
/// tick: exponential backoff between attempts, then a slow-retry park — the
/// condition stays armed but the next attempt is pushed to a ~6h horizon, so
/// the sandbox self-heals when the cause clears (e.g. a completed
/// node-manager rollout) without needing tenant activity, while the fast
/// loop stays dead.
pub const MAX_AUTO_SUSPEND_ERROR_STREAK: u32 = 10;
const AUTO_SUSPEND_RETRY_BASE_SECS: u64 = 10;
const AUTO_SUSPEND_RETRY_CAP_SECS: u64 = 600;
const AUTO_SUSPEND_STREAK_STALE_SECS: u64 = 6 * 3600;

/// Record a failed auto-suspend attempt; returns `true` when the budget is
/// spent and this failure parked the sandbox. The streak counts consecutive
/// errors: anything that clears `last_auto_error` (activity, a completed
/// transition) resets it, so a stale streak cannot park a healthy sandbox.
pub fn mark_suspend_error(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    now: u64,
    error: impl Into<String>,
) -> bool {
    // The streak is evidence of a live failure loop, which retries within
    // seconds-to-minutes of the scheduled time; a failure landing hours past
    // it means the loop broke off, so old failures must not park a sandbox
    // that only just resumed failing. (Activity and successful transitions
    // reset the streak at the source, via `clear_auto_error`.)
    if state
        .next_auto_event_at
        .is_some_and(|at| now.saturating_sub(at) > AUTO_SUSPEND_STREAK_STALE_SECS)
    {
        state.auto_error_streak = 0;
    }
    state.auto_error_streak = state.auto_error_streak.saturating_add(1);
    if state.auto_error_streak >= MAX_AUTO_SUSPEND_ERROR_STREAK {
        // Budget spent: kill the fast retry loop, but do NOT disarm — a fixed
        // cause (e.g. a completed node-manager rollout on a mixed-version
        // fleet) must self-heal without tenant activity. Keep the condition
        // armed and push the next attempt to the decay horizon; that ~6h
        // retry either succeeds (clearing the streak) or re-parks. Signal
        // only on entry so the WARN/metric fires once, not every interval.
        mark_error(state, policy, error);
        state.next_auto_event_at = Some(now.saturating_add(AUTO_SUSPEND_STREAK_STALE_SECS));
        return state.auto_error_streak == MAX_AUTO_SUSPEND_ERROR_STREAK;
    }
    mark_error(state, policy, error);
    let backoff = (AUTO_SUSPEND_RETRY_BASE_SECS << (state.auto_error_streak - 1).min(6))
        .min(AUTO_SUSPEND_RETRY_CAP_SECS);
    state.next_auto_event_at = Some(
        state
            .next_auto_event_at
            .unwrap_or(0)
            .max(now.saturating_add(backoff)),
    );
    false
}

/// Record a transition failure and suppress the rest of the same idle epoch
/// until new activity re-arms the applicable conditions.
///
/// This is reserved for failures where retrying the unchanged idle decision
/// would create fresh durable side effects. Activity touches reset the
/// conditions normally, so a later idle epoch remains eligible.
pub fn mark_error_and_disarm_conditions(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    error: impl Into<String>,
) {
    for condition in state.conditions.values_mut() {
        condition.armed = false;
    }
    state.last_auto_error = Some(error.into());
    recompute_runtime_state(state, policy);
}

/// Clearing the error is also what resets the suspend retry budget: both mean
/// "the failure loop is over" (activity or a completed transition).
fn clear_auto_error(state: &mut AutoLifecycleRuntimeState) {
    state.last_auto_error = None;
    state.auto_error_streak = 0;
}

pub fn clear_error(state: &mut AutoLifecycleRuntimeState, policy: &AutoLifecyclePolicy) {
    clear_auto_error(state);
    recompute_runtime_state(state, policy);
}

fn effective_stage_deadline(
    state: &AutoLifecycleRuntimeState,
    condition: AutoLifecycleConditionSetId,
    epoch: u64,
    window: u64,
) -> u64 {
    let deadline = epoch.saturating_add(window).min(i64::MAX as u64);
    if condition == ACCESS_IDLE_CONDITION_SET {
        deadline.max(state.access_extended_until.unwrap_or(0))
    } else {
        deadline
    }
}

/// Extend only the supported single-stage access policy; ordinary activity
/// remains truthful and explicit policy replacement can independently reset it.
pub fn extend_access_deadline(
    state: &mut AutoLifecycleRuntimeState,
    policy: &AutoLifecyclePolicy,
    minimum: u64,
) -> Result<bool, Status> {
    let [stage] = policy.stages.as_slice() else {
        return Err(Status::failed_precondition(
            "connect requires a single AccessIdle stage",
        ));
    };
    if stage.condition_set != ACCESS_IDLE_CONDITION_SET {
        return Err(Status::failed_precondition(
            "connect requires a single AccessIdle stage",
        ));
    }
    let condition = state
        .conditions
        .get(&ACCESS_IDLE_CONDITION_SET)
        .filter(|condition| condition.armed && condition.last_fired_stage_index.is_none())
        .ok_or_else(|| Status::failed_precondition("AccessIdle is not armed"))?;
    let epoch = condition
        .epoch_at
        .ok_or_else(|| Status::failed_precondition("AccessIdle has no activity epoch"))?;
    let minimum = minimum.min(i64::MAX as u64);
    if minimum <= effective_stage_deadline(state, ACCESS_IDLE_CONDITION_SET, epoch, stage.at) {
        return Ok(false);
    }
    state.access_extended_until = Some(state.access_extended_until.unwrap_or(0).max(minimum));
    recompute_runtime_state(state, policy);
    Ok(true)
}

pub fn due_stage(
    policy: &AutoLifecyclePolicy,
    state: &AutoLifecycleRuntimeState,
    now: u64,
) -> Option<(
    usize,
    AutoLifecycleConditionSetId,
    AutoLifecycleIdleAction,
    u64,
)> {
    let mut due_by_condition =
        BTreeMap::<AutoLifecycleConditionSetId, (usize, u64, AutoLifecycleIdleAction)>::new();
    for (index, stage) in policy.stages.iter().enumerate() {
        let Some(condition) = state.conditions.get(&stage.condition_set) else {
            continue;
        };
        if !condition.armed {
            continue;
        }
        let Some(epoch_at) = condition.epoch_at else {
            continue;
        };
        if condition
            .last_fired_stage_index
            .is_some_and(|last| index as u32 <= last)
        {
            continue;
        }
        // Deadlines share SQLite's signed integer range. Clamp addition too:
        // even a valid delay can overflow when combined with its epoch.
        let due_at = effective_stage_deadline(state, stage.condition_set, epoch_at, stage.at);
        if due_at > now {
            continue;
        }
        due_by_condition
            .entry(stage.condition_set)
            .and_modify(|current| {
                if due_at >= current.1 {
                    *current = (index, due_at, stage.action);
                }
            })
            .or_insert((index, due_at, stage.action));
    }

    due_by_condition
        .into_iter()
        .max_by(|left, right| {
            left.1
                .2
                .dormancy_severity()
                .cmp(&right.1.2.dormancy_severity())
                .then_with(|| right.1.1.cmp(&left.1.1))
                .then_with(|| right.1.0.cmp(&left.1.0))
        })
        .map(|(condition_set, (index, due_at, action))| (index, condition_set, action, due_at))
}

pub fn compute_next_auto_event(
    policy: &AutoLifecyclePolicy,
    state: &AutoLifecycleRuntimeState,
) -> Option<u64> {
    policy
        .stages
        .iter()
        .enumerate()
        .filter_map(|(index, stage)| {
            let condition = state.conditions.get(&stage.condition_set)?;
            if !condition.armed {
                return None;
            }
            let epoch_at = condition.epoch_at?;
            match condition.last_fired_stage_index {
                Some(last) if index as u32 <= last => None,
                _ => Some(effective_stage_deadline(
                    state,
                    stage.condition_set,
                    epoch_at,
                    stage.at,
                )),
            }
        })
        .min()
}

#[cfg(test)]
mod tests;
