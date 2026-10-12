use super::*;

const REDISCOVERY_INTERVAL: Duration = Duration::from_secs(5);

/// One coalesced hint and no pending queue. SQLite owns unfinished work; local
/// monotonic deadlines only pace attempts and may restart with the daemon.
pub(super) fn spawn_worker(pool: std::sync::Arc<NetworkPool>, mut wake: mpsc::Receiver<()>) {
    if tokio::runtime::Handle::try_current().is_err() {
        tracing::warn!("network slot pool opened without a runtime; scheduler disabled");
        return;
    }
    pool.task_tracker.clone().spawn(async move {
        let mut resets = JoinSet::new();
        let mut maintenance = JoinSet::new();
        let mut reset_backoff = HashMap::<u32, (u64, tokio::time::Instant)>::new();
        let mut maintenance_backoff = None;
        let mut requested = false;
        let mut tick = tokio::time::interval(REDISCOVERY_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cursor = u32::MAX;
        let mut maintenance_cursor = u32::MAX;
        loop {
            tokio::select! {
                biased;
                _ = pool.shutdown_token.cancelled() => break,
                Some(result) = resets.join_next(), if !resets.is_empty() => {
                    if let Err(error) = result { tracing::warn!(%error, "slot reset task failed; durable work retained"); }
                }
                Some(result) = maintenance.join_next(), if !maintenance.is_empty() => {
                    let failed = pool.maintenance_failed.swap(false, Ordering::Relaxed);
                    maintenance_backoff = (result.is_err() || failed)
                        .then(|| tokio::time::Instant::now() + POOL_FILL_RETRY_DELAY);
                    match result {
                        Ok(next_cursor) => maintenance_cursor = next_cursor,
                        Err(error) => tracing::warn!(%error, "pool maintenance failed; durable work retained"),
                    }
                }
                _ = tick.tick() => { requested = true; }
                hint = wake.recv() => {
                    if hint.is_none() { break; }
                    requested = true;
                }
            }
            if pool.shutdown_token.is_cancelled() { break; }
            let now = tokio::time::Instant::now();
            reset_backoff.retain(|_, (_, until)| *until > now);
            if resets.len() < RESET_CONCURRENCY {
                let scan_pool = pool.clone();
                let work = pool.task_tracker.spawn_blocking(move || reset_candidates(&scan_pool, cursor)).await;
                match work {
                    Ok(Ok(page)) => {
                        // Advance over held rows too: a page full of long-running
                        // owners must not hide later dirty slots.
                        cursor = page.next_cursor;
                        for (slot, generation) in page.entries {
                            if reset_backoff.get(&slot).is_some_and(|(old, _)| *old == generation) { continue; }
                            reset_backoff.insert(slot, (generation, now + POOL_FILL_RETRY_DELAY));
                            resets.spawn(reset_slot(pool.clone(), slot));
                            if resets.len() == RESET_CONCURRENCY { break; }
                        }
                    }
                    result => tracing::warn!(?result, "slot work inventory unavailable; periodic rediscovery will retry"),
                }
            }
            if requested && maintenance.is_empty() && maintenance_backoff.is_none_or(|until| until <= now) {
                requested = false;
                maintenance.spawn(reconcile_pool(pool.clone(), maintenance_cursor));
            }
        }
        // Never abort a parent while its blocking child can still mutate host state.
        while let Some(result) = resets.join_next().await {
            if let Err(error) = result { tracing::warn!(%error, "reset failed during scheduler drain"); }
        }
        while let Some(result) = maintenance.join_next().await {
            if let Err(error) = result { tracing::warn!(%error, "maintenance failed during scheduler drain"); }
        }
    });
}

#[derive(Debug)]
pub(super) struct ResetPage {
    pub next_cursor: u32,
    pub entries: Vec<(u32, u64)>,
}

pub(super) fn reset_candidates(
    pool: &NetworkPool,
    cursor: u32,
) -> Result<ResetPage, SlotStoreError> {
    let conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
    let active = pool
        .active_resets
        .lock()
        .map_err(|_| SlotStoreError::LockPoisoned)?;
    let candidates = query_reset_candidates(&conn, cursor, (RESET_CONCURRENCY * 2) as u32)?;
    let next_cursor = candidates.last().map_or(cursor, |(slot, _)| *slot);
    Ok(ResetPage {
        next_cursor,
        entries: candidates
            .into_iter()
            .filter(|(slot, _)| !active.contains(slot))
            .collect(),
    })
}

async fn reconcile_pool(pool: std::sync::Arc<NetworkPool>, cursor: u32) -> u32 {
    reconcile_pool_with(pool, cursor, net::teardown_network).await
}

pub(super) async fn reconcile_pool_with(
    pool: std::sync::Arc<NetworkPool>,
    cursor: u32,
    teardown: fn(&str, &NetId, Option<&str>) -> Result<(), net::NetworkTeardownError>,
) -> u32 {
    let recovery = pool.clone();
    let result = pool
        .task_tracker
        .spawn_blocking(move || recover_abandoned_work(&recovery, cursor))
        .await;
    let (next_cursor, destroying) = match result {
        Ok(Ok(slots)) => slots,
        result => {
            tracing::warn!(
                ?result,
                "abandoned pool work discovery failed; next pump will retry"
            );
            schedule_topup_retry(pool);
            return cursor;
        }
    };
    // Destruction is durable even when targets are zero or the node is drained.
    for slot in destroying {
        if pool.shutdown_token.is_cancelled() {
            return next_cursor;
        }
        destroy_slot_with(pool.clone(), slot, teardown).await;
    }
    // Retirement and recovery are obligations, not capacity admission. Neither
    // draining nor a failed readiness probe in fill may suppress retirement.
    prune_ready_slots_with(pool.clone(), teardown).await;
    fill_ready_slots(pool.clone()).await;
    // Configuration can shrink while an already-owned create is finishing.
    prune_ready_slots_with(pool, teardown).await;
    next_cursor
}

pub(super) fn recover_abandoned_work(
    pool: &NetworkPool,
    cursor: u32,
) -> Result<(u32, Vec<u32>), SlotStoreError> {
    let conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
    let active = pool
        .active_resets
        .lock()
        .map_err(|_| SlotStoreError::LockPoisoned)?;
    let candidates = query_abandoned_maintenance(&conn, cursor, (RESET_CONCURRENCY * 2) as u32)?;
    let mut next_cursor = cursor;
    let mut destroying = Vec::new();
    for (slot, generation, state) in candidates {
        next_cursor = slot;
        if active.contains(&slot) {
            continue;
        }
        if state == SlotState::Destroying {
            destroying.push(slot);
            if destroying.len() == RESET_CONCURRENCY {
                break;
            }
            continue;
        }
        let changed = conn.execute(
            "UPDATE network_slots SET state = 'missing', health = 'degraded',
             last_error = 'abandoned setup completion; revalidating host resources'
             WHERE slot_id = ?1 AND generation = ?2 AND state = 'creating' AND sandbox_id IS NULL",
            params![slot, SqliteU64(generation)],
        )?;
        if changed != 0 {
            pool.startup_recovery_pending.store(true, Ordering::Relaxed);
            pool.mark_node_status_dirty();
        }
    }
    Ok((next_cursor, destroying))
}
