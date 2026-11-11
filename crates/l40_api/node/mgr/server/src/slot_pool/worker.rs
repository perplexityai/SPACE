use super::db::*;
use super::reset::*;
use super::*;

#[cfg(test)]
#[derive(Clone)]
pub(super) struct TestCreateSlotOutcome {
    pub delay: Duration,
    pub success: bool,
    pub completed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
static TEST_CREATE_SLOT_OUTCOMES: std::sync::OnceLock<Mutex<VecDeque<TestCreateSlotOutcome>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_HOST_READY_FOR_POOL_FILL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
pub(super) fn set_test_create_slot_outcomes(outcomes: Vec<TestCreateSlotOutcome>) {
    let mut guard = TEST_CREATE_SLOT_OUTCOMES
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap();
    *guard = outcomes.into();
}

#[cfg(test)]
pub(super) fn set_test_host_ready_for_pool_fill(ready: bool) {
    TEST_HOST_READY_FOR_POOL_FILL.store(ready, Ordering::SeqCst);
}

#[derive(Debug, thiserror::Error)]
enum SlotCreateError {
    #[error(transparent)]
    Handoff(#[from] NetworkHandoffError),
    #[error(transparent)]
    Setup(#[from] net::NetworkSetupError),
    #[error("slot create task failed: {0}")]
    TaskJoin(String),
    #[error(transparent)]
    Validation(#[from] SlotValidationError),
}

impl SlotCreateError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Handoff(_) => true,
            Self::Setup(error) => error.is_transient(),
            Self::TaskJoin(_) => false,
            Self::Validation(error) => error.is_transient(),
        }
    }
}

struct ValidationTaskOutcome<M, U> {
    key: ValidationRetryKey,
    slot_id: u32,
    meta: M,
    result: ValidationTaskResult<U>,
}

pub(super) enum ValidationTaskResult<U> {
    Update(U),
    Transient(String),
    RuntimeUnknown(U),
}

pub(super) enum StartupLeaseUpdate {
    Health(LeaseHealthValidationUpdate),
    Gone(SlotInfo),
}

struct LiveValidationMeta {
    generation: u64,
    sandbox_id: String,
}

pub(super) fn validate_startup_runtime(
    info: SlotInfo,
    host_ready: bool,
    evidence: LeaseLiveness,
) -> ValidationTaskResult<StartupLeaseUpdate> {
    let health = |health, last_error| {
        StartupLeaseUpdate::Health(LeaseHealthValidationUpdate {
            slot_id: info.slot,
            generation: info.generation,
            sandbox_id: info.sandbox_id.clone(),
            health,
            last_error,
        })
    };
    match evidence {
        LeaseLiveness::Gone => return ValidationTaskResult::Update(StartupLeaseUpdate::Gone(info)),
        LeaseLiveness::Unknown => {
            return ValidationTaskResult::RuntimeUnknown(health(
                SlotHealth::Unknown,
                Some("startup runtime evidence unknown; retrying".into()),
            ));
        }
        LeaseLiveness::Live => {}
    }
    if !host_ready {
        return ValidationTaskResult::RuntimeUnknown(health(
            SlotHealth::Unknown,
            Some("host networking not ready; retrying startup health validation".into()),
        ));
    }
    match validate_slot_ready(info.slot, info.pod_netns.as_deref()) {
        Ok(()) => ValidationTaskResult::Update(health(SlotHealth::Healthy, None)),
        Err(error) if error.is_transient() => ValidationTaskResult::Transient(error.to_string()),
        Err(error) => {
            ValidationTaskResult::Update(health(SlotHealth::Degraded, Some(error.to_string())))
        }
    }
}

struct RecoverableValidationMeta {
    generation: u64,
    previous_state: SlotState,
}

pub(super) enum RecoverableValidationResult {
    Update(ReadyValidationUpdate),
    Transient(String),
}

pub(super) enum TransientValidationDecision {
    Retry { attempts: u32 },
    Exhausted { attempts: u32, error: String },
}

pub(super) fn transient_validation_decision(
    pool: &NetworkPool,
    key: &ValidationRetryKey,
    error: String,
) -> TransientValidationDecision {
    let attempts = record_validation_transient_failure(pool, key);
    if attempts >= MAX_SLOT_TRANSIENT_VALIDATION_RETRIES {
        clear_validation_transient_failure(pool, key);
        TransientValidationDecision::Exhausted { attempts, error }
    } else {
        TransientValidationDecision::Retry { attempts }
    }
}

pub(super) fn record_validation_transient_failure(
    pool: &NetworkPool,
    key: &ValidationRetryKey,
) -> u32 {
    let mut attempts = pool
        .validation_transient_failures
        .entry(key.clone())
        .or_insert(0);
    *attempts = attempts.saturating_add(1);
    *attempts
}

pub(super) fn clear_validation_transient_failure(pool: &NetworkPool, key: &ValidationRetryKey) {
    pool.validation_transient_failures.remove(key);
}

pub(super) fn retain_validation_transient_failures(
    pool: &NetworkPool,
    domain: ValidationRetryDomain,
    current_keys: &HashSet<ValidationRetryKey>,
) {
    pool.validation_transient_failures
        .retain(|key, _| key.domain != domain || current_keys.contains(key));
}

struct ValidationPumpMessages {
    join_error: &'static str,
    transient_retry: &'static str,
    transient_exhausted: &'static str,
}

async fn run_validation_pump<M, U, Fut, Validate, Exhaust, Apply>(
    pool: std::sync::Arc<NetworkPool>,
    records: Vec<SlotRecord>,
    domain: ValidationRetryDomain,
    messages: ValidationPumpMessages,
    validate: Validate,
    exhausted_update: Exhaust,
    apply_updates: Apply,
) -> Result<bool, ()>
where
    M: Send + 'static,
    U: Send + 'static,
    Fut: std::future::Future<Output = ValidationTaskOutcome<M, U>> + Send + 'static,
    Validate: Fn(SlotRecord, ValidationRetryKey) -> Fut,
    Exhaust: Fn(u32, &M, u32, String) -> U,
    Apply: FnOnce(&NetworkPool, &[U]) -> Result<(), ()>,
{
    let current_keys = records
        .iter()
        .map(|record| ValidationRetryKey::from_record(record, domain))
        .collect::<HashSet<_>>();
    retain_validation_transient_failures(&pool, domain, &current_keys);
    let mut pending = VecDeque::from(records);
    let mut tasks = JoinSet::new();
    let mut updates = Vec::with_capacity(pending.len());
    let mut retry = false;

    loop {
        while tasks.len() < VALIDATION_CONCURRENCY {
            let Some(record) = pending.pop_front() else {
                break;
            };
            let key = ValidationRetryKey::from_record(&record, domain);
            tasks.spawn(validate(record, key));
        }

        let Some(joined) = tasks.join_next().await else {
            break;
        };
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(error) => {
                retry = true;
                tracing::warn!(
                    error = %error,
                    retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                    "{}", messages.join_error
                );
                continue;
            }
        };
        let ValidationTaskOutcome {
            key,
            slot_id,
            meta,
            result,
        } = outcome;
        match result {
            ValidationTaskResult::RuntimeUnknown(update) => {
                // Uncertain runtime ownership cannot consume the networking
                // retry budget: retain it until evidence or release resolves it.
                retry = true;
                updates.push(update);
                tracing::warn!(
                    slot_id,
                    retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                    "startup lease validation deferred; retaining ownership and retrying"
                );
            }
            ValidationTaskResult::Update(update) => {
                clear_validation_transient_failure(&pool, &key);
                updates.push(update);
            }
            ValidationTaskResult::Transient(error) => {
                match transient_validation_decision(&pool, &key, error) {
                    TransientValidationDecision::Exhausted { attempts, error } => {
                        tracing::warn!(
                            slot_id,
                            error = %error,
                            validation_attempts = attempts,
                            "{}", messages.transient_exhausted
                        );
                        updates.push(exhausted_update(slot_id, &meta, attempts, error));
                    }
                    TransientValidationDecision::Retry { attempts } => {
                        retry = true;
                        tracing::warn!(
                            slot_id,
                            validation_attempts = attempts,
                            max_validation_attempts = MAX_SLOT_TRANSIENT_VALIDATION_RETRIES,
                            retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                            "{}",
                            messages.transient_retry
                        );
                    }
                }
            }
        }
    }

    if !updates.is_empty() {
        apply_updates(&pool, &updates)?;
        pool.mark_node_status_dirty();
    }
    Ok(retry)
}

pub(super) fn record_transient_setup_failure_for_key(
    pool: &NetworkPool,
    key: SetupRetryKey,
) -> u32 {
    let mut attempts = pool.transient_setup_failures.entry(key).or_insert(0);
    *attempts = attempts.saturating_add(1);
    *attempts
}

pub(super) fn clear_transient_setup_failure_for_key(pool: &NetworkPool, key: SetupRetryKey) {
    pool.transient_setup_failures.remove(&key);
}

pub(super) fn clear_stale_transient_setup_failures_for_slot(
    pool: &NetworkPool,
    slot_id: u32,
    current_key: Option<SetupRetryKey>,
) {
    pool.transient_setup_failures
        .retain(|key, _| key.slot_id != slot_id || current_key == Some(*key));
}

#[cfg(test)]
async fn maybe_run_test_create_slot_outcome() -> Option<bool> {
    let outcome = TEST_CREATE_SLOT_OUTCOMES
        .get()
        .and_then(|outcomes| outcomes.lock().ok()?.pop_front());
    let outcome = outcome?;
    tokio::time::sleep(outcome.delay).await;
    outcome
        .completed
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Some(outcome.success)
}

pub(super) async fn fill_ready_slots(pool: std::sync::Arc<NetworkPool>) {
    if pool.shutdown_token.is_cancelled() {
        return;
    }
    // Startup ownership recovery must progress even while pool fill is drained.
    let recovery_pool = pool.clone();
    let recovery = tokio::task::spawn_blocking(move || {
        reprobe_startup_leasing_with(&recovery_pool, lease_liveness)
    })
    .await;
    match recovery {
        Ok(Ok(false)) => {}
        Ok(Ok(true)) => schedule_topup_retry(pool.clone()),
        error => {
            tracing::warn!(?error, "startup lease recovery deferred; retry scheduled");
            schedule_topup_retry(pool.clone());
        }
    }
    if pool
        .lease_health_recovery_pending
        .swap(false, Ordering::Relaxed)
    {
        match validate_live_leased_slots(pool.clone()).await {
            Ok(false) if !pool.lease_health_recovery_pending.load(Ordering::Relaxed) => {}
            Ok(false) => schedule_topup_retry(pool.clone()),
            Ok(true) | Err(()) => {
                pool.lease_health_recovery_pending
                    .store(true, Ordering::Relaxed);
                schedule_topup_retry(pool.clone());
            }
        }
    }
    let draining = match pool.is_draining() {
        Ok(draining) => draining,
        Err(error) => {
            tracing::warn!(%error, "deferring pool fill: drain state unavailable");
            schedule_topup_retry(pool);
            return;
        }
    };
    if draining {
        tracing::info!("network slot pool fill skipped while node is draining");
        return;
    }
    let config = pool.config();
    let mut ran_startup_recovery = false;
    if pool.startup_recovery_pending.swap(false, Ordering::Relaxed) {
        ran_startup_recovery = true;
        if let Err(error) = host_ready_for_pool_fill_off_runtime(&config).await {
            tracing::warn!(
                error = %error,
                retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                "startup network slot recovery deferred until host networking is ready"
            );
            pool.startup_recovery_pending.store(true, Ordering::Relaxed);
            schedule_topup_retry(pool);
            return;
        }
        match recover_startup_idle_slots(pool.clone()).await {
            Ok(retry_needed) => {
                if retry_needed {
                    pool.startup_recovery_pending.store(true, Ordering::Relaxed);
                    schedule_topup_retry(pool.clone());
                    return;
                }
            }
            Err(()) => {
                pool.startup_recovery_pending.store(true, Ordering::Relaxed);
                schedule_topup_retry(pool);
                return;
            }
        }
    }
    let config = pool.config();
    if !config.warm_pool_enabled() {
        return;
    }
    if let Err(error) = host_ready_for_pool_fill_off_runtime(&config).await {
        tracing::warn!(
            error = %error,
            retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
            "network slot pool fill deferred until host networking is ready"
        );
        schedule_topup_retry(pool);
        return;
    }

    if !ran_startup_recovery {
        let adoption_retry_needed = adopt_ready_slots(pool.clone()).await.unwrap_or(true);
        if adoption_retry_needed {
            schedule_topup_retry(pool.clone());
        }
    }

    let mut repair_tasks = JoinSet::new();
    let mut create_tasks = JoinSet::new();

    loop {
        let config = pool.config();
        let ready = match pool.counts() {
            Ok(counts) => counts.ready,
            Err(error) => {
                tracing::warn!(%error, "deferring pool fill: counts unavailable");
                drain_fill_tasks(&mut repair_tasks, &mut create_tasks).await;
                schedule_topup_retry(pool);
                return;
            }
        };
        let fill_target = config.target_ready_slots.max(config.retained_ready_slots);
        if ready >= fill_target {
            drain_fill_tasks(&mut repair_tasks, &mut create_tasks).await;
            return;
        }

        let mut launched = false;
        while repair_tasks.len() < REPAIR_CONCURRENCY {
            let Some((slot_id, generation)) = claim_missing_for_create(&pool, true) else {
                break;
            };
            launched = true;
            repair_tasks.spawn(create_slot(pool.clone(), slot_id, generation, true));
        }
        while create_tasks.len() < CREATE_CONCURRENCY {
            let Some((slot_id, generation)) = claim_missing_for_create(&pool, false) else {
                break;
            };
            launched = true;
            create_tasks.spawn(create_slot(pool.clone(), slot_id, generation, false));
        }

        tokio::select! {
            Some(result) = repair_tasks.join_next(), if !repair_tasks.is_empty() => {
                if !fill_task_succeeded(result) {
                    drain_fill_tasks(&mut repair_tasks, &mut create_tasks).await;
                    schedule_topup_retry(pool);
                    return;
                }
            }
            Some(result) = create_tasks.join_next(), if !create_tasks.is_empty() => {
                if !fill_task_succeeded(result) {
                    drain_fill_tasks(&mut repair_tasks, &mut create_tasks).await;
                    schedule_topup_retry(pool);
                    return;
                }
            }
            else => {
                if !launched {
                    return;
                }
            }
        }
    }
}

pub(super) async fn drain_fill_tasks(
    repair_tasks: &mut JoinSet<bool>,
    create_tasks: &mut JoinSet<bool>,
) {
    while !repair_tasks.is_empty() || !create_tasks.is_empty() {
        tokio::select! {
            _ = repair_tasks.join_next(), if !repair_tasks.is_empty() => {}
            _ = create_tasks.join_next(), if !create_tasks.is_empty() => {}
        }
    }
}

pub(super) fn fill_task_succeeded(result: Result<bool, tokio::task::JoinError>) -> bool {
    match result {
        Ok(true) => true,
        Ok(false) => false,
        Err(error) => {
            tracing::warn!(error = %error, "network slot fill task failed");
            false
        }
    }
}

pub(super) async fn prune_ready_slots_with(
    pool: std::sync::Arc<NetworkPool>,
    teardown: fn(&str, &NetId, Option<&str>) -> Result<(), net::NetworkTeardownError>,
) {
    for _ in 0..RESET_CONCURRENCY {
        let slot_id = match claim_ready_for_destroy(&pool) {
            Ok(Some(slot_id)) => slot_id,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%error, "deferring pool retirement: inventory unavailable");
                schedule_topup_retry(pool);
                return;
            }
        };
        destroy_slot_with(pool.clone(), slot_id, teardown).await;
    }
}

pub(super) fn claim_missing_for_create(pool: &NetworkPool, repair: bool) -> Option<(u32, u64)> {
    if pool.shutdown_token.is_cancelled() {
        return None;
    }
    let config = pool.config();
    if !config.warm_pool_enabled() {
        return None;
    }
    let Ok(mut conn) = pool.db.lock() else {
        return None;
    };
    let tx = conn.transaction().ok()?;
    if meta_bool(&tx, "draining").ok()? {
        return None;
    }
    let health = if repair { "degraded" } else { "unknown" };
    let (slot_id, generation) = tx
        .query_row(
            "SELECT slot_id, generation FROM network_slots
             WHERE state = 'missing' AND health = ?1 AND slot_id < ?2
             ORDER BY slot_id
             LIMIT 1",
            params![health, config.max_slots],
            |row| Ok((row.get::<_, u32>(0)?, row.get::<_, SqliteU64>(1)?.0)),
        )
        .optional()
        .ok()
        .flatten()?;
    let changed = tx
        .execute(
            concat!(
                "UPDATE network_slots
             SET state = 'creating', health = 'unknown',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 ",
                clear_cleanup_reset_retry_sql!(),
                ",
                 updated_at = ?2
             WHERE slot_id = ?3 AND generation = ?4
               AND state = 'missing' AND health = ?5 AND sandbox_id IS NULL"
            ),
            params![
                LeaseKind::Pooled.as_str(),
                SqliteU64(now_secs()),
                slot_id,
                SqliteU64(generation),
                health
            ],
        )
        .ok()?;
    if changed == 0 {
        return None;
    }
    tx.commit().ok()?;
    clear_stale_transient_setup_failures_for_slot(
        pool,
        slot_id,
        Some(SetupRetryKey::pooled(slot_id, generation)),
    );
    pool.mark_node_status_dirty();
    Some((slot_id, generation))
}

pub(super) fn claim_ready_for_destroy(pool: &NetworkPool) -> Result<Option<u32>, SlotStoreError> {
    if pool.shutdown_token.is_cancelled() {
        return Ok(None);
    }
    let retained = pool.config().retained_ready_slots;
    let mut conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
    let tx = conn.transaction()?;
    // Count all ready rows, including those awaiting startup revalidation.
    // A zero-sized pool can retire interrupted, unowned setup directly rather
    // than needing a successful rebuild before teardown. Quarantine still
    // requires an explicit operator repair to authorize this transition.
    let slot_id = tx
        .query_row(
            "SELECT slot_id FROM network_slots
             WHERE sandbox_id IS NULL AND sandbox_uid IS NULL AND pid IS NULL
               AND ((state = 'ready' AND
                    (SELECT COUNT(*) FROM network_slots WHERE state = 'ready') > ?1)
                 OR (?1 = 0 AND state = 'missing' AND
                     (health != 'unknown' OR pod_netns_path IS NOT NULL)))
             ORDER BY slot_id DESC
             LIMIT 1",
            params![retained],
            |row| row.get::<_, u32>(0),
        )
        .optional()?;
    let Some(slot_id) = slot_id else {
        return Ok(None);
    };
    if pool
        .active_resets
        .lock()
        .map_err(|_| SlotStoreError::LockPoisoned)?
        .contains(&slot_id)
    {
        return Ok(None);
    }
    tx.execute(
        "UPDATE network_slots
         SET state = 'destroying', health = 'unknown', memory_reservation_mib = NULL,
             updated_at = ?1
         WHERE slot_id = ?2 AND state IN ('ready', 'missing') AND sandbox_id IS NULL
           AND sandbox_uid IS NULL AND pid IS NULL",
        params![SqliteU64(now_secs()), slot_id],
    )?;
    tx.commit()?;
    pool.mark_node_status_dirty();
    Ok(Some(slot_id))
}

pub(super) fn persist_created_slot(
    conn: &Connection,
    config: &NetworkPoolConfig,
    slot_id: u32,
    generation: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'ready', health = 'healthy', sandbox_id = NULL,
                 sandbox_uid = NULL, pid = NULL, last_error = NULL,
                 pod_netns_path = ?1, lease_kind = ?2, memory_reservation_mib = NULL,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 updated_at = ?3, last_reset_at = ?3
             WHERE slot_id = ?4 AND generation = ?5 AND state = 'creating'"
        ),
        params![
            config.pod_netns_path,
            LeaseKind::Pooled.as_str(),
            SqliteU64(now_secs()),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

pub(super) fn persist_transient_create_failure(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    error: &str,
) -> rusqlite::Result<usize> {
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 last_error = ?2, updated_at = ?3
             WHERE slot_id = ?4 AND generation = ?5 AND state = 'creating'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now_secs()),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

pub(super) fn persist_create_quarantine(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    error: &str,
) -> rusqlite::Result<usize> {
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'quarantined', health = 'invalid',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 last_error = ?2, updated_at = ?3,
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?4 AND generation = ?5 AND state = 'creating'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now_secs()),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

pub(super) async fn create_slot(
    pool: std::sync::Arc<NetworkPool>,
    slot_id: u32,
    generation: u64,
    repair: bool,
) -> bool {
    let Some(claim) = ResetClaim::acquire(&pool, slot_id) else {
        return false;
    };
    let setup_key = SetupRetryKey::pooled(slot_id, generation);

    #[cfg(test)]
    if let Some(success) = maybe_run_test_create_slot_outcome().await {
        return success;
    }

    let config = pool.config();
    let pod_netns = config.pod_netns_path.clone();
    let blocking_claim = claim.clone();
    let setup_pool = pool.clone();
    let result = pool
        .task_tracker
        .spawn_blocking(move || -> Result<(), SlotCreateError> {
            let _claim = blocking_claim;
            let net = NetId::from_slot(slot_id);
            let ns = net.ns();
            let same_namespace = {
                let conn = setup_pool
                    .db
                    .lock()
                    .map_err(|_| NetworkHandoffError::from(SlotStoreError::LockPoisoned))?;
                query_record_by_slot(&conn, slot_id)
                    .map_err(SlotStoreError::from)
                    .map_err(NetworkHandoffError::from)?
                    .is_some_and(|record| record.info.pod_netns == pod_netns)
            };
            if !repair
                || !same_namespace
                || net::reset_network_slot(&ns, &net, pod_netns.as_deref()).is_err()
            {
                setup_pool.handoff_network_namespace(
                    slot_id,
                    generation,
                    SlotState::Creating,
                    pod_netns.as_deref(),
                )?;
                net::setup_network(&ns, &net, pod_netns.as_deref())
                    .map_err(SlotCreateError::Setup)?;
            }
            validate_slot_ready(slot_id, pod_netns.as_deref()).map_err(SlotCreateError::Validation)
        })
        .await
        .map_err(|e| SlotCreateError::TaskJoin(e.to_string()))
        .and_then(|result| result);

    let Ok(conn) = pool.db.lock() else {
        return false;
    };
    let (success, status_changed) = match result {
        Ok(()) => match persist_created_slot(&conn, &config, slot_id, generation) {
            Ok(changed) => {
                clear_transient_setup_failure_for_key(&pool, setup_key);
                (true, changed > 0)
            }
            Err(error) => {
                tracing::error!(slot_id, error = %error, "failed to persist created network slot");
                (false, false)
            }
        },
        Err(error) => {
            let error_message = error.to_string();
            if error.is_transient() {
                let attempts = record_transient_setup_failure_for_key(&pool, setup_key);
                if attempts >= MAX_SLOT_TRANSIENT_SETUP_RETRIES {
                    tracing::error!(
                        slot_id,
                        error = %error_message,
                        setup_attempts = attempts,
                        "network slot creation transient retries exhausted; quarantining slot"
                    );
                    let quarantine_error = format!(
                        "network slot creation exhausted transient retries after {attempts} attempts: {error_message}"
                    );
                    match persist_create_quarantine(&conn, slot_id, generation, &quarantine_error) {
                        Ok(changed) => {
                            clear_transient_setup_failure_for_key(&pool, setup_key);
                            (true, changed > 0)
                        }
                        Err(db_error) => {
                            tracing::error!(slot_id, error = %db_error, "failed to persist network slot creation quarantine after transient retries");
                            (false, false)
                        }
                    }
                } else {
                    tracing::warn!(
                        slot_id,
                        error = %error_message,
                        setup_attempts = attempts,
                        max_setup_attempts = MAX_SLOT_TRANSIENT_SETUP_RETRIES,
                        retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                        "network slot creation hit transient host networking error"
                    );
                    match persist_transient_create_failure(
                        &conn,
                        slot_id,
                        generation,
                        &error_message,
                    ) {
                        Ok(changed) => (false, changed > 0),
                        Err(db_error) => {
                            tracing::error!(slot_id, error = %db_error, "failed to persist transient network slot creation failure");
                            (false, false)
                        }
                    }
                }
            } else {
                match persist_create_quarantine(&conn, slot_id, generation, &error_message) {
                    Ok(changed) => {
                        clear_transient_setup_failure_for_key(&pool, setup_key);
                        (true, changed > 0)
                    }
                    Err(db_error) => {
                        tracing::error!(slot_id, error = %db_error, "failed to persist network slot creation quarantine");
                        (false, false)
                    }
                }
            }
        }
    };
    if status_changed {
        claim.disarm();
        pool.mark_node_status_dirty();
    }
    success
}

pub(super) async fn destroy_slot_with(
    pool: std::sync::Arc<NetworkPool>,
    slot_id: u32,
    teardown: impl FnOnce(&str, &NetId, Option<&str>) -> Result<(), net::NetworkTeardownError>
    + Send
    + 'static,
) {
    let Some(claim) = ResetClaim::acquire(&pool, slot_id) else {
        return;
    };
    let config = pool.config();
    let (pod_netns, generation) = {
        let Ok(conn) = pool.db.lock() else {
            schedule_topup_retry(pool.clone());
            return;
        };
        match query_record_by_slot(&conn, slot_id) {
            Ok(Some(record))
                if record.state == SlotState::Destroying && record.info.sandbox_id.is_empty() =>
            {
                (record.info.pod_netns, record.info.generation)
            }
            Ok(_) => {
                claim.disarm();
                return;
            }
            Err(error) => {
                tracing::warn!(slot_id, %error, "destroy intent unavailable; retrying maintenance");
                schedule_topup_retry(pool.clone());
                return;
            }
        }
    };
    let blocking_claim = claim.clone();
    let result = pool
        .task_tracker
        .spawn_blocking(move || {
            let _claim = blocking_claim;
            let net = NetId::from_slot(slot_id);
            let ns = net.ns();
            teardown(&ns, &net, pod_netns.as_deref()).map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| format!("slot destroy task failed: {e}"))
        .and_then(|result| result);

    let Ok(conn) = pool.db.lock() else {
        schedule_topup_retry(pool.clone());
        return;
    };
    let status_changed = match result {
        Ok(()) => {
            match conn.execute(
                concat!(
                    "UPDATE network_slots
                     SET state = 'missing', health = 'unknown',
                         sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                         pod_netns_path = NULL, lease_kind = ?1, memory_reservation_mib = NULL,
                         ",
                    clear_cleanup_reset_retry_sql!(),
                    ",
                         last_error = NULL, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4 AND state = 'destroying' AND sandbox_id IS NULL"
                ),
                params![LeaseKind::Pooled.as_str(), SqliteU64(now_secs()), slot_id, SqliteU64(generation)],
            ) {
                Ok(changed) => {
                    tracing::info!(
                        slot_id,
                        target_ready_slots = config.target_ready_slots,
                        retained_ready_slots = config.retained_ready_slots,
                        "destroyed surplus ready network slot"
                    );
                    changed > 0
                }
                Err(error) => {
                    tracing::error!(
                        slot_id,
                        error = %error,
                        "failed to persist destroyed network slot"
                    );
                    false
                }
            }
        }
        Err(error) => {
            // Retain durable destruction intent and namespace identity. The
            // maintenance worker retries after its normal backoff, including
            // after restart; a failed delete must never make the slot reusable.
            tracing::warn!(slot_id, %error, "network slot teardown incomplete; retrying maintenance");
            schedule_topup_retry(pool.clone());
            match conn.execute(
                "UPDATE network_slots
                 SET state = 'destroying', last_error = ?1, updated_at = ?2
                 WHERE slot_id = ?3 AND generation = ?4 AND state = 'destroying' AND sandbox_id IS NULL",
                params![
                    error,
                    SqliteU64(now_secs()),
                    slot_id,
                    SqliteU64(generation)
                ],
            ) {
                Ok(changed) => changed > 0,
                Err(db_error) => {
                    tracing::error!(slot_id, error = %db_error, "failed to persist incomplete network slot teardown");
                    false
                }
            }
        }
    };
    if status_changed {
        claim.disarm();
        pool.mark_node_status_dirty();
    } else {
        schedule_topup_retry(pool.clone());
    }
}

pub(super) async fn recover_startup_idle_slots(
    pool: std::sync::Arc<NetworkPool>,
) -> Result<bool, ()> {
    let config = pool.config();
    let records = {
        let Ok(conn) = pool.db.lock() else {
            return Err(());
        };
        match query_startup_recoverable_idle_records(&conn, &config) {
            Ok(records) => records,
            Err(error) => {
                tracing::error!(error = %error, "failed to query startup-recoverable network slots");
                return Err(());
            }
        }
    };
    validate_and_apply_recoverable_idle_records(
        pool,
        config,
        records,
        ValidationRetryDomain::StartupIdle,
    )
    .await
}

pub(super) async fn adopt_ready_slots(pool: std::sync::Arc<NetworkPool>) -> Result<bool, ()> {
    let config = pool.config();
    let records = {
        let Ok(conn) = pool.db.lock() else {
            return Err(());
        };
        match query_adoptable_ready_records(&conn, &config) {
            Ok(records) => records,
            Err(error) => {
                tracing::error!(error = %error, "failed to query adoptable ready network slots");
                return Err(());
            }
        }
    };
    validate_and_apply_recoverable_idle_records(
        pool,
        config,
        records,
        ValidationRetryDomain::ReadyAdoption,
    )
    .await
}

pub(super) fn reprobe_startup_leasing_with(
    pool: &NetworkPool,
    probe: impl Fn(&SlotInfo) -> LeaseLiveness,
) -> Result<bool, SlotStoreError> {
    let mut pending = pool
        .startup_leasing
        .lock()
        .map_err(|_| SlotStoreError::LockPoisoned)?;
    // This set only shrinks. On process death the durable rows rebuild it.
    let mut index = 0;
    while let Some(key) = pending.get(index) {
        let Some(_claim) = pool.try_claim_sandbox_mutation(&key.sandbox_id) else {
            index += 1;
            continue;
        };
        let record = {
            let conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
            if !pool
                .startup_leases
                .lock()
                .map_err(|_| SlotStoreError::LockPoisoned)?
                .contains(key)
            {
                pending.swap_remove(index);
                continue;
            }
            query_record_by_slot(&conn, key.slot_id)?
        };
        let Some(record) = record.filter(|record| {
            record.state == SlotState::Leasing
                && LeaseKey::from_record(record).as_ref() == Some(key)
        }) else {
            pending.swap_remove(index);
            continue;
        };
        let evidence = probe(&record.info);
        let changed = {
            let conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
            apply_startup_leasing_evidence(&conn, key, record.info.pid, evidence)?
        };
        if evidence == LeaseLiveness::Unknown {
            tracing::warn!(slot_id = key.slot_id, generation = key.generation,
                sandbox_id = %key.sandbox_id, retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                "startup lease runtime remains unknown; retaining ownership and retrying");
            index += 1;
            continue;
        }
        if !changed {
            // A PID publication can race the probe without changing the lease
            // identity. Keep the obligation until a fresh probe resolves it.
            let conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
            if query_record_by_slot(&conn, key.slot_id)?.is_some_and(|record| {
                record.state == SlotState::Leasing
                    && LeaseKey::from_record(&record).as_ref() == Some(key)
            }) {
                index += 1;
            } else {
                pending.swap_remove(index);
            }
            continue;
        }
        if changed {
            if evidence == LeaseLiveness::Gone {
                pool.enqueue_reset(key.slot_id);
            } else {
                pool.lease_health_recovery_pending
                    .store(true, Ordering::Relaxed);
            }
            pool.mark_node_status_dirty();
        }
        pending.swap_remove(index);
    }
    Ok(!pending.is_empty())
}

pub(super) async fn validate_live_leased_slots(
    pool: std::sync::Arc<NetworkPool>,
) -> Result<bool, ()> {
    let records = {
        let Ok(conn) = pool.db.lock() else {
            return Err(());
        };
        let Ok(startup) = pool.startup_leases.lock() else {
            return Err(());
        };
        match query_live_leased_records(&conn) {
            Ok(records) => records
                .into_iter()
                .filter(|record| {
                    LeaseKey::from_record(record).is_some_and(|key| startup.contains(&key))
                })
                .collect(),
            Err(error) => {
                tracing::error!(error = %error, "failed to query live leased network slots");
                return Err(());
            }
        }
    };

    let host_ready = host_ready_for_pool_fill_off_runtime(&pool.config())
        .await
        .is_ok();
    run_validation_pump(
        pool,
        records,
        ValidationRetryDomain::LiveLease,
        ValidationPumpMessages {
            join_error: "live leased network slot validation task failed",
            transient_retry: "live leased network slot validation hit transient host networking error",
            transient_exhausted: "live leased network slot validation retries exhausted",
        },
        move |record, key| async move {
            let slot_id = record.info.slot;
            let generation = record.info.generation;
            let sandbox_id = record.info.sandbox_id.clone();
            let meta = LiveValidationMeta {
                generation,
                sandbox_id: sandbox_id.clone(),
            };
            let info = record.info.clone();
            let result = match tokio::task::spawn_blocking(move || {
                let evidence = lease_liveness(&info);
                validate_startup_runtime(info, host_ready, evidence)
            })
            .await
            {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, slot_id, "live leased slot validation task failed");
                ValidationTaskResult::Transient(error.to_string())
            }
            };
            ValidationTaskOutcome {
                key,
                slot_id,
                meta,
                result,
            }
        },
        |slot_id, meta, attempts, error| {
            StartupLeaseUpdate::Health(LeaseHealthValidationUpdate {
                slot_id,
                generation: meta.generation,
                sandbox_id: meta.sandbox_id.clone(),
                health: SlotHealth::Degraded,
                last_error: Some(format!(
                    "transient validation failed after {attempts} attempts: {error}"
                )),
            })
        },
        |pool, updates| {
            apply_startup_lease_updates(pool, updates)
                .map_err(|error| {
                    tracing::error!(%error, "failed to persist live leased slot health validation");
                })?;
            Ok(())
        },
    )
    .await
}

async fn validate_and_apply_recoverable_idle_records(
    pool: std::sync::Arc<NetworkPool>,
    config: NetworkPoolConfig,
    records: Vec<SlotRecord>,
    domain: ValidationRetryDomain,
) -> Result<bool, ()> {
    let apply_config = config.clone();
    run_validation_pump(
        pool,
        records,
        domain,
        ValidationPumpMessages {
            join_error: "idle network slot validation hit transient host networking error",
            transient_retry: "idle network slot validation hit transient host networking error",
            transient_exhausted: "idle network slot validation retries exhausted; marking for rebuild",
        },
        move |record, key| {
            let config = config.clone();
            async move {
                let slot_id = record.info.slot;
                let meta = RecoverableValidationMeta {
                    generation: record.info.generation,
                    previous_state: record.state,
                };
                let result = match validate_recoverable_idle_record(&config, record).await {
                    RecoverableValidationResult::Update(update) => {
                        ValidationTaskResult::Update(update)
                    }
                    RecoverableValidationResult::Transient(error) => {
                        ValidationTaskResult::Transient(error)
                    }
                };
                ValidationTaskOutcome {
                    key,
                    slot_id,
                    meta,
                    result,
                }
            }
        },
        |slot_id, meta, attempts, error| ReadyValidationUpdate::Degraded {
            slot_id,
            generation: meta.generation,
            previous_state: meta.previous_state,
            error: format!("transient validation failed after {attempts} attempts: {error}"),
        },
        move |pool, updates| {
            let Ok(mut conn) = pool.db.lock() else {
                return Err(());
            };
            match apply_ready_validation_updates(&mut conn, &apply_config, updates) {
                Ok(()) => Ok(()),
                Err(error) => {
                    tracing::error!(error = %error, "failed to persist network slot validation updates");
                    Err(())
                }
            }
        },
    )
    .await
}

pub(super) async fn validate_recoverable_idle_record(
    config: &NetworkPoolConfig,
    record: SlotRecord,
) -> RecoverableValidationResult {
    validate_recoverable_idle_record_with(config, record, validate_slot_ready).await
}

pub(super) async fn validate_recoverable_idle_record_with<Validate>(
    config: &NetworkPoolConfig,
    record: SlotRecord,
    validate: Validate,
) -> RecoverableValidationResult
where
    Validate: FnOnce(u32, Option<&str>) -> Result<(), SlotValidationError> + Send + 'static,
{
    let slot_id = record.info.slot;
    if record.lease_kind != LeaseKind::Pooled {
        return RecoverableValidationResult::Update(ReadyValidationUpdate::Degraded {
            slot_id,
            generation: record.info.generation,
            previous_state: record.state,
            error: format!(
                "idle slot has {:?} lease, but pool recovery requires {:?}",
                record.lease_kind,
                LeaseKind::Pooled
            ),
        });
    }

    if record.info.pod_netns != config.pod_netns_path {
        return RecoverableValidationResult::Update(ReadyValidationUpdate::Degraded {
            slot_id,
            generation: record.info.generation,
            previous_state: record.state,
            error: format!(
                "idle slot retains namespace {:?}, but pool is configured for {:?}",
                record.info.pod_netns, config.pod_netns_path
            ),
        });
    }

    let pod_netns = record.info.pod_netns.clone();
    let result =
        match tokio::task::spawn_blocking(move || validate(slot_id, pod_netns.as_deref())).await {
            Ok(result) => result,
            Err(error) => {
                return RecoverableValidationResult::Transient(format!(
                    "slot validation task failed: {error}"
                ));
            }
        };

    match result {
        Ok(()) => RecoverableValidationResult::Update(ReadyValidationUpdate::Healthy {
            slot_id,
            generation: record.info.generation,
            previous_state: record.state,
        }),
        Err(error) => {
            if error.is_transient() {
                RecoverableValidationResult::Transient(error.to_string())
            } else {
                tracing::warn!(
                    slot_id,
                    error = %error,
                    "idle network slot failed startup validation; marking for rebuild"
                );
                RecoverableValidationResult::Update(ReadyValidationUpdate::Degraded {
                    slot_id,
                    generation: record.info.generation,
                    previous_state: record.state,
                    error: error.to_string(),
                })
            }
        }
    }
}

pub(super) enum ReadyValidationUpdate {
    Healthy {
        slot_id: u32,
        generation: u64,
        previous_state: SlotState,
    },
    Degraded {
        slot_id: u32,
        generation: u64,
        previous_state: SlotState,
        error: String,
    },
}

pub(super) fn apply_ready_validation_updates(
    conn: &mut Connection,
    config: &NetworkPoolConfig,
    updates: &[ReadyValidationUpdate],
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    let now = now_secs();
    for update in updates {
        match update {
            ReadyValidationUpdate::Healthy {
                slot_id,
                generation,
                previous_state,
            } => {
                tx.execute(
                    concat!(
                        "UPDATE network_slots
                         SET state = 'ready', health = 'healthy', last_error = NULL,
                             sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                             pod_netns_path = ?1, lease_kind = ?2, memory_reservation_mib = NULL,
                             ",
                        clear_cleanup_reset_retry_sql!(),
                        ",
                             updated_at = ?3
                         WHERE slot_id = ?4 AND generation = ?5 AND state = ?6
                           AND sandbox_id IS NULL"
                    ),
                    params![
                        config.pod_netns_path,
                        LeaseKind::Pooled.as_str(),
                        SqliteU64(now),
                        slot_id,
                        SqliteU64(*generation),
                        previous_state.as_str()
                    ],
                )?;
            }
            ReadyValidationUpdate::Degraded {
                slot_id,
                generation,
                previous_state,
                error,
            } => {
                tx.execute(
                    concat!(
                        "UPDATE network_slots
                         SET state = 'missing', health = 'degraded',
                             sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                             lease_kind = ?1, memory_reservation_mib = NULL,
                             ",
                        clear_cleanup_reset_retry_sql!(),
                        ",
                             last_error = ?2, updated_at = ?3
                         WHERE slot_id = ?4 AND generation = ?5 AND state = ?6
                           AND sandbox_id IS NULL"
                    ),
                    params![
                        LeaseKind::Pooled.as_str(),
                        error,
                        SqliteU64(now),
                        slot_id,
                        SqliteU64(*generation),
                        previous_state.as_str()
                    ],
                )?;
            }
        }
    }
    tx.commit()
}

pub(super) fn schedule_topup_retry(pool: std::sync::Arc<NetworkPool>) {
    // Completion reports failure to the pump; no sleeper or queued work item.
    // Normal refill hints do not arm this backoff.
    pool.maintenance_failed.store(true, Ordering::Relaxed);
}

/// Offloaded [`host_ready_for_pool_fill`]: the netlink readiness probe
/// hard-blocks the calling thread on a joined helper thread (up to the
/// netlink timeout), so async callers must not run it inline.
pub(super) async fn host_ready_for_pool_fill_off_runtime(
    config: &NetworkPoolConfig,
) -> Result<(), String> {
    let config = config.clone();
    tokio::task::spawn_blocking(move || host_ready_for_pool_fill(&config))
        .await
        .unwrap_or_else(|error| Err(format!("host readiness probe task failed to run: {error}")))
}

pub(super) fn host_ready_for_pool_fill(config: &NetworkPoolConfig) -> Result<(), String> {
    #[cfg(test)]
    if TEST_HOST_READY_FOR_POOL_FILL.load(Ordering::SeqCst) {
        return Ok(());
    }

    if let Some(pod_ns) = config.pod_netns_path.as_deref() {
        if !net::ns_exists(pod_ns) {
            return Err(format!("pod network namespace {pod_ns} does not exist"));
        }
        return Ok(());
    }

    net::host_network_ready()
}

pub(super) fn quarantine_resetting_slot(
    conn: &Connection,
    reset_key: &ResetKey,
    error: &str,
) -> rusqlite::Result<usize> {
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'quarantined', health = 'invalid',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 last_error = ?2, updated_at = ?3,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?4 AND generation = ?5
               AND state = 'resetting'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now_secs()),
            reset_key.slot_id,
            SqliteU64(reset_key.generation)
        ],
    )
}

pub(super) fn quarantine_resetting_slot_preserving_lease(
    conn: &Connection,
    reset_key: &ResetKey,
    error: &str,
) -> rusqlite::Result<usize> {
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'quarantined', health = 'invalid',
                 last_error = ?1, updated_at = ?2,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?3 AND generation = ?4
               AND state = 'resetting'"
        ),
        params![
            error,
            SqliteU64(now_secs()),
            reset_key.slot_id,
            SqliteU64(reset_key.generation)
        ],
    )
}
