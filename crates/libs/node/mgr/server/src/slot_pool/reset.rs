use super::db::*;
use super::worker::*;
use super::*;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlotResetDisposition {
    WaitForLeaseProcess,
    RetryCleanup,
    RetryTransient,
    Quarantine,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CleanupPendingReason {
    #[error(transparent)]
    RetainedCache(#[from] crate::service::RetainedCacheCleanupError),
    #[error("retained cache artifact is still owned by sandbox uid {uid}: {path}")]
    RetainedOwnership { uid: u32, path: PathBuf },
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SlotValidationError {
    #[error("{0}")]
    Missing(String),
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Probe(#[from] net::NetworkProbeError),
    /// A host-level configuration fault that every namespace on the node would
    /// hit identically. Fence the slot and retry; rebuilding cannot repair it.
    #[error(transparent)]
    PackagedPolicy(#[from] crate::firewall::PackagedPolicyError),
}

impl SlotValidationError {
    pub(super) fn is_transient(&self) -> bool {
        matches!(self, Self::Probe(_) | Self::PackagedPolicy(_))
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SlotResetError {
    #[error(
        "cannot reset slot {slot} generation {generation} for sandbox {sandbox_id}: runtime is {liveness:?}"
    )]
    RuntimeNotGone {
        slot: u32,
        generation: u64,
        sandbox_id: String,
        liveness: LeaseLiveness,
    },
    #[error(
        "cleanup required before resetting slot {slot} generation {generation} for sandbox {sandbox_id}: {reason}"
    )]
    CleanupPending {
        slot: u32,
        generation: u64,
        sandbox_id: String,
        #[source]
        reason: Box<CleanupPendingReason>,
    },
    #[error("{message}")]
    External {
        message: String,
        disposition: SlotResetDisposition,
    },
    #[error("slot reset task failed: {0}")]
    TaskJoin(String),
    #[error(transparent)]
    Teardown(#[from] net::NetworkTeardownError),
    #[error(transparent)]
    Handoff(#[from] NetworkHandoffError),
}

impl SlotResetError {
    pub(super) fn disposition(&self) -> SlotResetDisposition {
        match self {
            Self::RuntimeNotGone { .. } => SlotResetDisposition::WaitForLeaseProcess,
            Self::CleanupPending { .. } => SlotResetDisposition::RetryCleanup,
            Self::Teardown(_) | Self::Handoff(_) => SlotResetDisposition::RetryCleanup,
            Self::External { disposition, .. } => *disposition,
            Self::TaskJoin(_) => SlotResetDisposition::Quarantine,
        }
    }
}

fn classify_network_setup_error(error: net::NetworkSetupError) -> SlotResetError {
    let disposition = if error.is_transient() {
        SlotResetDisposition::RetryTransient
    } else {
        SlotResetDisposition::Quarantine
    };
    SlotResetError::External {
        message: error.to_string(),
        disposition,
    }
}

fn classify_cleanup_inspection_error(message: String) -> SlotResetError {
    SlotResetError::External {
        message,
        disposition: SlotResetDisposition::RetryCleanup,
    }
}

fn classify_slot_validation_error(error: SlotValidationError) -> SlotResetError {
    let disposition = if error.is_transient() {
        SlotResetDisposition::RetryTransient
    } else {
        SlotResetDisposition::Quarantine
    };
    SlotResetError::External {
        message: error.to_string(),
        disposition,
    }
}

/// Process-local ownership complements the durable generation fence. Clones
/// held by blocking work keep exclusion alive even if the async waiter drops.
pub(super) struct ResetClaim {
    pool: std::sync::Arc<NetworkPool>,
    slot_id: u32,
    retry: AtomicBool,
}

impl std::fmt::Debug for ResetClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResetClaim")
            .field("slot_id", &self.slot_id)
            .finish_non_exhaustive()
    }
}

impl ResetClaim {
    /// Call only after the durable transition completes. A colliding request
    /// owns its own deduplicated retry, so disarming cannot erase that wakeup.
    pub(super) fn disarm(&self) {
        self.retry.store(false, Ordering::Relaxed);
    }

    pub(super) fn acquire(
        pool: &std::sync::Arc<NetworkPool>,
        slot_id: u32,
    ) -> Option<std::sync::Arc<Self>> {
        let mut active = pool
            .active_resets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !active.insert(slot_id) {
            return None;
        }
        Some(std::sync::Arc::new(Self {
            pool: pool.clone(),
            slot_id,
            retry: AtomicBool::new(true),
        }))
    }
}

impl Drop for ResetClaim {
    fn drop(&mut self) {
        self.pool
            .active_resets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.slot_id);
        // Covers early DB errors, cancellation and panics as well as normal
        // retries. A completed/nontransitional row makes the next probe a no-op.
        if self.retry.load(Ordering::Relaxed) {
            schedule_reset_retry(self.pool.clone(), self.slot_id);
        }
    }
}

pub(super) async fn reset_slot(pool: std::sync::Arc<NetworkPool>, slot_id: u32) {
    if pool.shutdown_token.is_cancelled() {
        return;
    }
    let Some(claim) = ResetClaim::acquire(&pool, slot_id) else {
        schedule_reset_retry(pool, slot_id);
        return;
    };
    let reset_started = Instant::now();
    let config = pool.config();
    let (record, reset_key, mutation_claim) = {
        let Ok(mut conn) = pool.db.lock() else {
            return;
        };
        let Ok(tx) = conn.transaction() else {
            return;
        };
        let record = match query_record_by_slot(&tx, slot_id) {
            Ok(Some(record)) => record,
            Ok(None) => {
                claim.disarm();
                return;
            }
            Err(error) => {
                tracing::warn!(slot_id, %error, "reset inventory unavailable; retry scheduled");
                return;
            }
        };
        if record.state != SlotState::Dirty && record.state != SlotState::Resetting {
            claim.disarm();
            return;
        }
        let mutation_claim = if record.info.sandbox_id.is_empty() {
            None
        } else {
            let Some(mutation_claim) = pool.try_claim_sandbox_mutation(&record.info.sandbox_id)
            else {
                return; // ResetClaim schedules a bounded, deduplicated retry.
            };
            Some(mutation_claim)
        };
        let reset_key = ResetKey::from_record(&record);
        let changed = match tx.execute(
            "UPDATE network_slots
                 SET state = 'resetting', updated_at = ?1
                 WHERE slot_id = ?2 AND generation = ?3
                   AND state IN ('dirty', 'resetting')",
            params![
                SqliteU64(now_secs()),
                reset_key.slot_id,
                SqliteU64(reset_key.generation)
            ],
        ) {
            Ok(changed) => changed,
            Err(error) => {
                tracing::error!(slot_id, error = %error, "failed to persist network slot reset claim");
                return;
            }
        };
        if changed == 0 {
            return;
        }
        if let Err(error) = tx.commit() {
            tracing::error!(slot_id, error = %error, "failed to commit network slot reset claim");
            return;
        }
        (record, reset_key, mutation_claim)
    };
    clear_stale_cleanup_reset_db_failures_for_slot(&pool, slot_id, &reset_key);
    clear_stale_transient_reset_failures_for_slot(&pool, slot_id, &reset_key);
    pool.mark_node_status_dirty();

    let reset_kind = if record.lease_kind == LeaseKind::Pooled && config.warm_pool_enabled() {
        LeaseKind::Pooled
    } else {
        LeaseKind::Ephemeral
    };
    tracing::info!(
        slot_id,
        generation = reset_key.generation,
        sandbox_id = ?reset_key.sandbox_id,
        reset_kind = ?reset_kind,
        lease_kind = ?record.lease_kind,
        previous_state = ?record.state,
        "network slot reset started"
    );
    if reset_kind == LeaseKind::Pooled
        && let Err(error) = host_ready_for_pool_fill_off_runtime(&config).await
    {
        tracing::warn!(
            slot_id,
            error = %error,
            retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
            "network slot reset deferred until host networking is ready"
        );
        schedule_reset_retry(pool, slot_id);
        return;
    }

    let previous_pod_netns = record.info.pod_netns.clone();
    let target_pod_netns = config.pod_netns_path.clone();
    let lease_info = record.info.clone();
    let sandbox_cache_dir = config.sandbox_cache_dir.clone();
    tracing::info!(
        slot_id,
        generation = reset_key.generation,
        sandbox_id = ?reset_key.sandbox_id,
        previous_pod_netns = ?previous_pod_netns,
        target_pod_netns = ?target_pod_netns,
        "network slot reset entering blocking teardown/setup phase"
    );
    let blocking_claim = (claim.clone(), mutation_claim.clone());
    let setup_pool = pool.clone();
    let result = pool.task_tracker.spawn_blocking(move || -> Result<(), SlotResetError> {
        let _claim = blocking_claim;
        assert_lease_process_stopped(&lease_info)?;
        neutralize_retained_cache_for_lease(&lease_info)?;
        assert_retained_cache_clean_for_lease(&sandbox_cache_dir, &lease_info)?;
        let net = NetId::from_slot(slot_id);
        let ns = net.ns();
        if reset_kind == LeaseKind::Ephemeral {
            net::teardown_network(&ns, &net, previous_pod_netns.as_deref())?;
            return Ok(());
        }

        if previous_pod_netns == target_pod_netns {
            if let Err(error) = net::reset_network_slot(&ns, &net, target_pod_netns.as_deref()) {
                tracing::warn!(slot_id, error = %error, "in-place network slot reset failed; rebuilding slot");
                setup_pool.handoff_network_namespace(slot_id, lease_info.generation, SlotState::Resetting, target_pod_netns.as_deref())?;
                net::setup_network(&ns, &net, target_pod_netns.as_deref())
                    .map_err(classify_network_setup_error)?;
            }
        } else {
            setup_pool.handoff_network_namespace(slot_id, lease_info.generation, SlotState::Resetting, target_pod_netns.as_deref())?;
            net::setup_network(&ns, &net, target_pod_netns.as_deref())
                .map_err(classify_network_setup_error)?;
        }
        validate_slot_ready(slot_id, target_pod_netns.as_deref())
            .map_err(classify_slot_validation_error)
    })
    .await
    .map_err(|e| SlotResetError::TaskJoin(e.to_string()))
    .and_then(|result| result);

    match result {
        Ok(()) => {
            tracing::info!(
                slot_id,
                generation = reset_key.generation,
                sandbox_id = ?reset_key.sandbox_id,
                elapsed_ms = reset_started.elapsed().as_millis() as u64,
                "network slot reset blocking phase complete"
            );
            let changed = {
                let Ok(conn) = pool.db.lock() else {
                    return;
                };
                let now = now_secs();
                let result = if reset_kind == LeaseKind::Pooled {
                    conn.execute(
                        concat!(
                            "UPDATE network_slots
                             SET state = 'ready', health = 'healthy',
                                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                                 last_error = NULL, pod_netns_path = ?1, lease_kind = ?2,
                                 memory_reservation_mib = NULL,
                                 ",
                            clear_cleanup_reset_retry_sql!(),
                            ",
                                 updated_at = ?3, last_reset_at = ?3
                             WHERE slot_id = ?4 AND generation = ?5
                               AND state = 'resetting'"
                        ),
                        params![
                            config.pod_netns_path,
                            LeaseKind::Pooled.as_str(),
                            SqliteU64(now),
                            reset_key.slot_id,
                            SqliteU64(reset_key.generation)
                        ],
                    )
                } else {
                    conn.execute(
                        concat!(
                            "UPDATE network_slots
                             SET state = 'missing', health = 'unknown',
                                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                                 last_error = NULL, pod_netns_path = NULL, lease_kind = ?1,
                                 memory_reservation_mib = NULL,
                                 ",
                            clear_cleanup_reset_retry_sql!(),
                            ",
                                 updated_at = ?2, last_reset_at = ?2
                             WHERE slot_id = ?3 AND generation = ?4
                               AND state = 'resetting'"
                        ),
                        params![
                            LeaseKind::Pooled.as_str(),
                            SqliteU64(now),
                            reset_key.slot_id,
                            SqliteU64(reset_key.generation)
                        ],
                    )
                };
                match result {
                    Ok(changed) => changed,
                    Err(error) => {
                        tracing::error!(
                            slot_id,
                            generation = reset_key.generation,
                            sandbox_id = ?reset_key.sandbox_id,
                            error = %error,
                            "failed to persist reset network slot"
                        );
                        schedule_reset_retry(pool.clone(), slot_id);
                        return;
                    }
                }
            };
            clear_cleanup_reset_db_failure(&pool, &reset_key);
            clear_transient_reset_failure(&pool, &reset_key);
            if changed == 0 {
                tracing::warn!(
                    slot_id,
                    generation = reset_key.generation,
                    sandbox_id = ?reset_key.sandbox_id,
                    elapsed_ms = reset_started.elapsed().as_millis() as u64,
                    "network slot reset completed but lease changed before ready transition"
                );
                return;
            }
            tracing::info!(
                slot_id,
                generation = reset_key.generation,
                sandbox_id = ?reset_key.sandbox_id,
                elapsed_ms = reset_started.elapsed().as_millis() as u64,
                "network slot reset completed"
            );
            pool.mark_node_status_dirty();
            claim.disarm();
            pool.rebalance_ready_slots();
        }
        Err(error) => {
            tracing::warn!(
                slot_id,
                generation = reset_key.generation,
                sandbox_id = ?reset_key.sandbox_id,
                elapsed_ms = reset_started.elapsed().as_millis() as u64,
                error = %error,
                "network slot reset failed"
            );
            let Ok(conn) = pool.db.lock() else {
                return;
            };
            match error.disposition() {
                SlotResetDisposition::WaitForLeaseProcess => {
                    tracing::warn!(
                        slot_id,
                        error = %error,
                        retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                        "network slot reset deferred while leased process is still alive"
                    );
                    match conn.execute(
                        "UPDATE network_slots
                     SET state = 'dirty', health = 'degraded',
                         last_error = ?1, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4
                       AND state = 'resetting'",
                        params![
                            error.to_string(),
                            SqliteU64(now_secs()),
                            reset_key.slot_id,
                            SqliteU64(reset_key.generation)
                        ],
                    ) {
                        Ok(changed) if changed > 0 => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                            pool.mark_node_status_dirty();
                        }
                        Ok(_) => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                        }
                        Err(db_error) => {
                            tracing::error!(
                                slot_id,
                                generation = reset_key.generation,
                                sandbox_id = ?reset_key.sandbox_id,
                                error = %db_error,
                                "failed to persist live-process network slot reset deferral"
                            );
                        }
                    }
                    clear_transient_reset_failure(&pool, &reset_key);
                    schedule_reset_retry(pool.clone(), slot_id);
                }
                SlotResetDisposition::RetryCleanup => {
                    clear_transient_reset_failure(&pool, &reset_key);
                    let attempts = match record_cleanup_reset_failure(&conn, &reset_key) {
                        Ok(attempts) if attempts > 0 => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                            attempts
                        }
                        Ok(_) => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                            tracing::warn!(
                                slot_id,
                                generation = reset_key.generation,
                                sandbox_id = ?reset_key.sandbox_id,
                                "network slot reset retry skipped because lease changed before retry accounting"
                            );
                            return;
                        }
                        Err(db_error) => {
                            tracing::error!(
                                slot_id,
                                generation = reset_key.generation,
                                sandbox_id = ?reset_key.sandbox_id,
                                error = %db_error,
                                "failed to persist network slot reset retry attempt"
                            );
                            let attempts = record_cleanup_reset_db_failure(&pool, &reset_key);
                            if attempts >= MAX_SLOT_CLEANUP_RESET_RETRIES {
                                let quarantine_error = format!(
                                    "network slot reset retry accounting failed after {attempts} attempts; last reset error: {error}"
                                );
                                tracing::error!(
                                    slot_id,
                                    generation = reset_key.generation,
                                    sandbox_id = ?reset_key.sandbox_id,
                                    reset_attempts = attempts,
                                    "network slot reset retry accounting failures exhausted; quarantining slot"
                                );
                                match quarantine_resetting_slot_preserving_lease(
                                    &conn,
                                    &reset_key,
                                    &quarantine_error,
                                ) {
                                    Ok(changed) if changed > 0 => {
                                        clear_cleanup_reset_db_failure(&pool, &reset_key);
                                        pool.mark_node_status_dirty();
                                    }
                                    Ok(_) => {
                                        clear_cleanup_reset_db_failure(&pool, &reset_key);
                                        tracing::warn!(
                                            slot_id,
                                            generation = reset_key.generation,
                                            sandbox_id = ?reset_key.sandbox_id,
                                            "network slot reset retry accounting failed but lease changed before quarantine"
                                        );
                                    }
                                    Err(quarantine_db_error) => {
                                        tracing::error!(
                                            slot_id,
                                            generation = reset_key.generation,
                                            sandbox_id = ?reset_key.sandbox_id,
                                            error = %quarantine_db_error,
                                            "failed to persist network slot reset quarantine after retry accounting failures"
                                        );
                                        schedule_reset_retry(pool.clone(), slot_id);
                                    }
                                }
                            } else {
                                schedule_reset_retry(pool.clone(), slot_id);
                            }
                            return;
                        }
                    };
                    if attempts >= MAX_SLOT_CLEANUP_RESET_RETRIES {
                        let quarantine_error = format!(
                            "network slot reset exhausted cleanup retries after {attempts} attempts: {error}"
                        );
                        tracing::error!(
                            slot_id,
                            generation = reset_key.generation,
                            sandbox_id = ?reset_key.sandbox_id,
                            error = %error,
                            reset_attempts = attempts,
                            "network slot reset cleanup retries exhausted; quarantining slot"
                        );
                        match quarantine_resetting_slot_preserving_lease(
                            &conn,
                            &reset_key,
                            &quarantine_error,
                        ) {
                            Ok(changed) if changed > 0 => {
                                clear_cleanup_reset_db_failure(&pool, &reset_key);
                                pool.mark_node_status_dirty();
                            }
                            Ok(_) => {
                                clear_cleanup_reset_db_failure(&pool, &reset_key);
                            }
                            Err(db_error) => {
                                tracing::error!(
                                    slot_id,
                                    generation = reset_key.generation,
                                    sandbox_id = ?reset_key.sandbox_id,
                                    error = %db_error,
                                    "failed to persist network slot reset quarantine"
                                );
                                schedule_reset_retry(pool.clone(), slot_id);
                            }
                        }
                        return;
                    }
                    tracing::warn!(
                        slot_id,
                        error = %error,
                        reset_attempts = attempts,
                        max_reset_attempts = MAX_SLOT_CLEANUP_RESET_RETRIES,
                        retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                        "network slot reset deferred until previous sandbox cleanup completes"
                    );
                    match conn.execute(
                        "UPDATE network_slots
                     SET state = 'dirty', health = 'degraded',
                         last_error = ?1, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4
                       AND state = 'resetting'",
                        params![
                            error.to_string(),
                            SqliteU64(now_secs()),
                            reset_key.slot_id,
                            SqliteU64(reset_key.generation)
                        ],
                    ) {
                        Ok(changed) if changed > 0 => {
                            pool.mark_node_status_dirty();
                        }
                        Ok(_) => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                        }
                        Err(db_error) => {
                            tracing::error!(
                                slot_id,
                                generation = reset_key.generation,
                                sandbox_id = ?reset_key.sandbox_id,
                                error = %db_error,
                                "failed to persist transient network slot reset failure"
                            );
                        }
                    }
                    schedule_reset_retry(pool.clone(), slot_id);
                }
                SlotResetDisposition::RetryTransient => {
                    let attempts = record_transient_reset_failure(&pool, &reset_key);
                    if attempts >= MAX_SLOT_TRANSIENT_RESET_RETRIES {
                        let quarantine_error = format!(
                            "network slot reset exhausted transient retries after {attempts} attempts: {error}"
                        );
                        tracing::error!(
                            slot_id,
                            generation = reset_key.generation,
                            sandbox_id = ?reset_key.sandbox_id,
                            error = %error,
                            reset_attempts = attempts,
                            "network slot reset transient retries exhausted; quarantining slot"
                        );
                        match quarantine_resetting_slot_preserving_lease(
                            &conn,
                            &reset_key,
                            &quarantine_error,
                        ) {
                            Ok(changed) if changed > 0 => {
                                clear_transient_reset_failure(&pool, &reset_key);
                                pool.mark_node_status_dirty();
                            }
                            Ok(_) => {
                                clear_transient_reset_failure(&pool, &reset_key);
                            }
                            Err(db_error) => {
                                tracing::error!(
                                    slot_id,
                                    generation = reset_key.generation,
                                    sandbox_id = ?reset_key.sandbox_id,
                                    error = %db_error,
                                    "failed to persist network slot reset quarantine after transient retries"
                                );
                                schedule_reset_retry(pool.clone(), slot_id);
                            }
                        }
                        return;
                    }
                    tracing::warn!(
                        slot_id,
                        error = %error,
                        reset_attempts = attempts,
                        max_reset_attempts = MAX_SLOT_TRANSIENT_RESET_RETRIES,
                        retry_after_secs = POOL_FILL_RETRY_DELAY.as_secs(),
                        "network slot reset hit transient host networking error"
                    );
                    match conn.execute(
                        "UPDATE network_slots
                     SET state = 'dirty', health = 'degraded',
                         last_error = ?1, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4
                       AND state = 'resetting'",
                        params![
                            error.to_string(),
                            SqliteU64(now_secs()),
                            reset_key.slot_id,
                            SqliteU64(reset_key.generation)
                        ],
                    ) {
                        Ok(changed) if changed > 0 => {
                            pool.mark_node_status_dirty();
                        }
                        Ok(_) => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                        }
                        Err(db_error) => {
                            tracing::error!(
                                slot_id,
                                generation = reset_key.generation,
                                sandbox_id = ?reset_key.sandbox_id,
                                error = %db_error,
                                "failed to persist transient network slot reset failure"
                            );
                        }
                    }
                    clear_cleanup_reset_db_failure(&pool, &reset_key);
                    schedule_reset_retry(pool.clone(), slot_id);
                }
                SlotResetDisposition::Quarantine => {
                    clear_transient_reset_failure(&pool, &reset_key);
                    match quarantine_resetting_slot(&conn, &reset_key, &error.to_string()) {
                        Ok(changed) if changed > 0 => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                            pool.mark_node_status_dirty();
                        }
                        Ok(_) => {
                            clear_cleanup_reset_db_failure(&pool, &reset_key);
                        }
                        Err(db_error) => {
                            tracing::error!(
                                slot_id,
                                generation = reset_key.generation,
                                sandbox_id = ?reset_key.sandbox_id,
                                error = %db_error,
                                "failed to persist network slot reset quarantine"
                            );
                            schedule_reset_retry(pool.clone(), slot_id);
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn schedule_reset_retry(pool: std::sync::Arc<NetworkPool>, slot_id: u32) {
    pool.enqueue_reset(slot_id);
}

pub(super) fn record_cleanup_reset_db_failure(pool: &NetworkPool, reset_key: &ResetKey) -> u32 {
    let mut attempts = pool
        .cleanup_reset_db_failures
        .entry(reset_key.clone())
        .or_insert(0);
    *attempts += 1;
    *attempts
}

pub(super) fn clear_cleanup_reset_db_failure(pool: &NetworkPool, reset_key: &ResetKey) {
    pool.cleanup_reset_db_failures.remove(reset_key);
}

pub(super) fn clear_stale_cleanup_reset_db_failures_for_slot(
    pool: &NetworkPool,
    slot_id: u32,
    current_key: &ResetKey,
) {
    pool.cleanup_reset_db_failures
        .retain(|key, _| key.slot_id != slot_id || key == current_key);
}

pub(super) fn record_transient_reset_failure(pool: &NetworkPool, reset_key: &ResetKey) -> u32 {
    let mut attempts = pool
        .transient_reset_failures
        .entry(reset_key.clone())
        .or_insert(0);
    *attempts = attempts.saturating_add(1);
    *attempts
}

pub(super) fn clear_transient_reset_failure(pool: &NetworkPool, reset_key: &ResetKey) {
    pool.transient_reset_failures.remove(reset_key);
}

pub(super) fn clear_stale_transient_reset_failures_for_slot(
    pool: &NetworkPool,
    slot_id: u32,
    current_key: &ResetKey,
) {
    pool.transient_reset_failures
        .retain(|key, _| key.slot_id != slot_id || key == current_key);
}

pub(super) fn assert_lease_process_stopped(info: &SlotInfo) -> Result<(), SlotResetError> {
    assert_lease_process_stopped_with(info, lease_liveness(info))
}

pub(super) fn assert_lease_process_stopped_with(
    info: &SlotInfo,
    liveness: LeaseLiveness,
) -> Result<(), SlotResetError> {
    if liveness != LeaseLiveness::Gone {
        return Err(SlotResetError::RuntimeNotGone {
            slot: info.slot,
            generation: info.generation,
            sandbox_id: info.sandbox_id.clone(),
            liveness,
        });
    }
    Ok(())
}

pub(super) fn assert_retained_cache_clean_for_lease(
    sandbox_cache_dir: &Path,
    info: &SlotInfo,
) -> Result<(), SlotResetError> {
    if info.sandbox_id.is_empty() {
        return Ok(());
    }
    let cache_dir = sandbox_cache_dir.join(&info.sandbox_id);
    if let Some(path) = first_path_owned_by_uid(&cache_dir, info.sandbox_uid)
        .map_err(classify_cleanup_inspection_error)?
    {
        return Err(SlotResetError::CleanupPending {
            slot: info.slot,
            generation: info.generation,
            sandbox_id: info.sandbox_id.clone(),
            reason: CleanupPendingReason::RetainedOwnership {
                uid: info.sandbox_uid,
                path,
            }
            .into(),
        });
    }
    Ok(())
}

pub(super) fn neutralize_retained_cache_for_lease(info: &SlotInfo) -> Result<(), SlotResetError> {
    if info.sandbox_id.is_empty() {
        return Ok(());
    }
    crate::service::neutralize_retained_cache_owned_by_uid(&info.sandbox_id, info.sandbox_uid)
        .map(|_| ())
        .map_err(|error| SlotResetError::CleanupPending {
            slot: info.slot,
            generation: info.generation,
            sandbox_id: info.sandbox_id.clone(),
            reason: Box::new(error.into()),
        })
}

pub(super) use crate::cache_ownership::first_path_owned_by_uid;

pub(super) fn validate_slot_ready(
    slot_id: u32,
    pod_netns: Option<&str>,
) -> Result<(), SlotValidationError> {
    let net = NetId::from_slot(slot_id);
    let ns = net.ns();
    if !net::ns_exists(&ns) {
        return Err(SlotValidationError::Missing(format!(
            "missing namespace {ns}"
        )));
    }
    if !net::try_link_exists_in(&net.sveth(), pod_netns)? {
        return Err(SlotValidationError::Missing(format!(
            "missing host-side interface {}",
            net.sveth()
        )));
    }
    if !net::try_host_route_exists_in(&net, pod_netns)? {
        return Err(SlotValidationError::Missing(format!(
            "missing route for {}",
            net.vpeer_ip_bare()
        )));
    }
    if pod_netns.is_none() {
        if net::try_root_default_route_uses_space_link()? {
            return Err(SlotValidationError::Invalid(
                "root default route points at a SPACE slot interface".to_string(),
            ));
        }
        if net::try_link_exists_in(&net.vpeer(), None)? {
            return Err(SlotValidationError::Invalid(format!(
                "namespace interface {} leaked into root namespace",
                net.vpeer()
            )));
        }
        if net::try_link_exists_in(net::TAP_DEVICE, None)? {
            return Err(SlotValidationError::Invalid(format!(
                "namespace tap {} leaked into root namespace",
                net::TAP_DEVICE
            )));
        }
    }
    if !net::try_link_exists_in(&net.vpeer(), Some(&ns))? {
        return Err(SlotValidationError::Missing(format!(
            "missing namespace interface {}",
            net.vpeer()
        )));
    }
    if !net::try_link_exists_in(net::TAP_DEVICE, Some(&ns))? {
        return Err(SlotValidationError::Missing(format!(
            "missing namespace tap {}",
            net::TAP_DEVICE
        )));
    }
    let has_snat = crate::firewall::try_ns_has_snat(&ns).map_err(|source| {
        SlotValidationError::Probe(net::NetworkProbeError::Nft {
            detail: source.to_string(),
        })
    })?;
    let has_dnat = crate::firewall::try_ns_has_dnat(&ns).map_err(|source| {
        SlotValidationError::Probe(net::NetworkProbeError::Nft {
            detail: source.to_string(),
        })
    })?;
    if !has_snat || !has_dnat {
        return Err(SlotValidationError::Missing(
            "namespace sandbox nft rules are missing".to_string(),
        ));
    }
    crate::firewall::validate_namespace_firewall_config(&ns, &net.vpeer(), net::GUEST_IP).map_err(
        |error| match error {
            crate::firewall::FirewallValidationError::Inspect(source) => {
                SlotValidationError::Probe(net::NetworkProbeError::Nft {
                    detail: source.to_string(),
                })
            }
            crate::firewall::FirewallValidationError::Missing(message) => {
                SlotValidationError::Invalid(message)
            }
            crate::firewall::FirewallValidationError::PackagedPolicy(source) => {
                SlotValidationError::PackagedPolicy(source)
            }
        },
    )?;
    if !net::try_ns_sysctl_check(&ns, "net.ipv4.icmp_ratemask", net::NAMESPACE_ICMP_RATEMASK)? {
        return Err(SlotValidationError::Invalid(
            "namespace ICMP rate mask differs from packaged policy".to_string(),
        ));
    }
    if !net::try_ns_sysctl_check(&ns, "net.ipv4.ip_forward", "1")? {
        return Err(SlotValidationError::Invalid(
            "namespace IPv4 forwarding is not enabled".to_string(),
        ));
    }
    if !net::try_ns_sysctl_check(&ns, "net.ipv4.conf.tap0.rp_filter", "0")?
        || !net::try_ns_sysctl_check(&ns, "net.ipv4.conf.all.rp_filter", "0")?
    {
        return Err(SlotValidationError::Invalid(
            "namespace rp_filter is not disabled".to_string(),
        ));
    }
    if !net::try_ns_sysctl_check(&ns, "net.ipv4.conf.tap0.src_valid_mark", "0")?
        || !net::try_ns_sysctl_check(&ns, "net.ipv4.conf.all.src_valid_mark", "0")?
    {
        return Err(SlotValidationError::Invalid(
            "namespace src_valid_mark is not disabled".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_config(config: &NetworkPoolConfig) -> Result<(), String> {
    if config.max_slots == 0 {
        return Err("max_slots must be non-zero".to_string());
    }
    if config.max_slots > pplx_space_common_core::types::MAX_LINUX_NETWORK_SLOT + 1 {
        return Err("max_slots exceeds the six-digit slot namespace".to_string());
    }
    if config.max_slots > net::MAX_IPV4_NETWORK_SLOTS {
        return Err(format!(
            "max_slots exceeds IPv4 slot address capacity ({})",
            net::MAX_IPV4_NETWORK_SLOTS
        ));
    }
    if config.target_ready_slots > config.max_slots {
        return Err("network pool target must be <= max_slots".to_string());
    }
    if config.retained_ready_slots > config.max_slots {
        return Err("network pool retained_ready must be <= max_slots".to_string());
    }
    if config.retained_ready_slots < config.target_ready_slots {
        return Err("network pool retained_ready must be >= target".to_string());
    }
    if config.min_ready_slots > config.target_ready_slots {
        return Err("network pool min_ready must be <= target".to_string());
    }
    Ok(())
}
