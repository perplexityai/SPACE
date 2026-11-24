use super::*;

#[cfg(test)]
pub(super) use pplx_space_sqlite::SqliteU64;
use slots::SlotRow;

#[cfg(test)]
pub(super) fn configure_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA foreign_keys=ON;",
    )
}

pub(super) fn init_db(
    conn: &mut Connection,
    config: &NetworkPoolConfig,
) -> Result<(), SlotStoreError> {
    let tx = conn.transaction()?;
    ensure_slot_rows(&tx, config)?;
    reconcile_startup(&tx, config)?;
    Ok(tx.commit()?)
}

#[cfg(test)]
pub(super) fn init_in_memory_db(
    conn: &mut Connection,
    config: &NetworkPoolConfig,
) -> Result<(), SlotStoreError> {
    let tx = conn.transaction()?;
    init_schema(&tx)?;
    ensure_slot_rows(&tx, config)?;
    reconcile_startup(&tx, config)?;
    Ok(tx.commit()?)
}

#[cfg(test)]
fn init_schema(conn: &Connection) -> rusqlite::Result<()> {
    for migration in pplx_space_node_mgr_store::database::MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref())?;
    }
    Ok(())
}

pub(super) fn ensure_slot_rows(
    conn: &Connection,
    config: &NetworkPoolConfig,
) -> rusqlite::Result<()> {
    let now = now_secs();
    for slot_id in 0..config.max_slots {
        slots::insert_missing_slot(
            conn,
            slot_id,
            &net::slot_label(slot_id),
            NETWORK_POOL_SCHEMA_VERSION,
            now,
        )?;
    }
    Ok(())
}

pub(super) fn validate_startup_ownership(
    conn: &Connection,
    config: &NetworkPoolConfig,
) -> Result<(), SlotStoreError> {
    // Refuse unknown persisted vocabulary before any repair can erase it.
    let records = query_all_records(conn)?;
    if let Some(record) = records.iter().find(|record| {
        record.info.slot >= config.max_slots
            && (!record.info.sandbox_id.is_empty()
                || record.info.sandbox_uid != 0
                || record.info.pid.is_some()
                || record.info.pod_netns.is_some()
                || record.memory_reservation_mib.is_some()
                || record.state != SlotState::Missing
                || record.health != SlotHealth::Unknown)
    }) {
        return Err(SlotStoreError::UnsafeShrink {
            slot_id: record.info.slot,
            capacity: config.max_slots,
            state: record.state,
            health: record.health,
        });
    }
    Ok(())
}

pub(super) fn reconcile_startup(
    conn: &Connection,
    config: &NetworkPoolConfig,
) -> Result<(), SlotStoreError> {
    validate_startup_ownership(conn, config)?;
    let now = now_secs();
    slots::clear_slots_beyond_capacity(conn, config.max_slots, NETWORK_POOL_SCHEMA_VERSION, now)?;
    slots::degrade_unowned_slots_on_other_schema(conn, NETWORK_POOL_SCHEMA_VERSION, now)?;
    slots::mark_ready_slots_unvalidated(conn, now)?;
    slots::degrade_interrupted_setup(conn, now)?;
    // Keep interrupted destruction, including its namespace, for the durable
    // maintenance pump. Reclassifying it as missing would abandon teardown
    // when targets are zero and could make an uncleared slot reusable.
    slots::stamp_schema_version(conn, NETWORK_POOL_SCHEMA_VERSION, now)?;
    Ok(())
}

/// `young_leases` are the startup leases still inside their handoff grace
/// ([`query_startup_young_leases`]): a leased one without a runtime is left
/// for startup validation, which rechecks it, instead of being dirtied here.
pub(super) fn reconcile_runtime_state(
    conn: &mut Connection,
    config: &NetworkPoolConfig,
    young_leases: &HashMap<LeaseKey, u64>,
) -> rusqlite::Result<()> {
    let records = query_all_records(conn)?;
    let updates = runtime_reconcile_updates(records, lease_liveness, |record| {
        LeaseKey::from_record(record).is_some_and(|key| young_leases.contains_key(&key))
    });
    apply_runtime_reconcile_updates(conn, config, &updates)
}

pub(super) fn runtime_reconcile_updates(
    records: Vec<SlotRecord>,
    probe_liveness: impl Fn(&SlotInfo) -> LeaseLiveness,
    is_young: impl Fn(&SlotRecord) -> bool,
) -> Vec<RuntimeReconcileUpdate> {
    let mut updates = Vec::new();
    for record in records {
        let liveness = probe_liveness(&record.info);
        match record.state {
            SlotState::Leasing => {
                if record.info.sandbox_id.is_empty() {
                    updates.push(RuntimeReconcileUpdate::Quarantine {
                        slot_id: record.info.slot,
                        error: "leasing slot has no sandbox id".to_string(),
                    });
                } else if liveness == LeaseLiveness::Live {
                    updates.push(RuntimeReconcileUpdate::PromoteLeasing {
                        slot_id: record.info.slot,
                        generation: record.info.generation,
                        sandbox_id: record.info.sandbox_id,
                        health: record.health,
                        last_error: record.last_error,
                    });
                } else if liveness == LeaseLiveness::Gone {
                    updates.push(RuntimeReconcileUpdate::DirtyLease {
                        slot_id: record.info.slot,
                        generation: record.info.generation,
                        sandbox_id: record.info.sandbox_id,
                        error: "interrupted slot lease setup found at startup".to_string(),
                    });
                }
            }
            SlotState::Leased => {
                if record.info.sandbox_id.is_empty() {
                    updates.push(RuntimeReconcileUpdate::Quarantine {
                        slot_id: record.info.slot,
                        error: "leased slot has no sandbox id".to_string(),
                    });
                } else if liveness == LeaseLiveness::Gone && !is_young(&record) {
                    updates.push(RuntimeReconcileUpdate::DirtyLease {
                        slot_id: record.info.slot,
                        generation: record.info.generation,
                        sandbox_id: record.info.sandbox_id,
                        error: "leased slot had no live tracked process at startup".to_string(),
                    });
                }
            }
            SlotState::Dirty | SlotState::Resetting if record.info.sandbox_id.is_empty() => {
                updates.push(RuntimeReconcileUpdate::MissingDegraded {
                    slot_id: record.info.slot,
                    error: "dirty slot without active lease scheduled for rebuild".to_string(),
                });
            }
            SlotState::Quarantined if record.info.sandbox_id.is_empty() => {
                updates.push(RuntimeReconcileUpdate::MissingDegraded {
                    slot_id: record.info.slot,
                    error: "idle quarantined slot scheduled for rebuild".to_string(),
                });
            }
            SlotState::Quarantined if liveness == LeaseLiveness::Gone => {
                updates.push(RuntimeReconcileUpdate::DirtyLease {
                    slot_id: record.info.slot,
                    generation: record.info.generation,
                    sandbox_id: record.info.sandbox_id,
                    error: "quarantined lease is gone; retry owned cleanup".to_string(),
                });
            }
            _ => {}
        }
    }
    updates
}

pub(super) fn record_cleanup_reset_failure(
    conn: &Connection,
    reset_key: &ResetKey,
) -> rusqlite::Result<u32> {
    slots::reset::record_cleanup_reset_failure(
        conn,
        reset_key.slot_id,
        reset_key.generation,
        reset_key.sandbox_id.as_deref(),
        now_secs(),
    )
    .map(|attempts| attempts.unwrap_or(0))
}

pub(super) enum RuntimeReconcileUpdate {
    Quarantine {
        slot_id: u32,
        error: String,
    },
    DirtyLease {
        slot_id: u32,
        generation: u64,
        sandbox_id: String,
        error: String,
    },
    PromoteLeasing {
        slot_id: u32,
        generation: u64,
        sandbox_id: String,
        health: SlotHealth,
        last_error: Option<String>,
    },
    MissingDegraded {
        slot_id: u32,
        error: String,
    },
}

pub(super) fn apply_runtime_reconcile_updates(
    conn: &mut Connection,
    config: &NetworkPoolConfig,
    updates: &[RuntimeReconcileUpdate],
) -> rusqlite::Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let tx = conn.transaction()?;
    let now = now_secs();
    for update in updates {
        match update {
            RuntimeReconcileUpdate::Quarantine { slot_id, error } => {
                slots::quarantine_slot(&tx, *slot_id, error, now)?;
            }
            RuntimeReconcileUpdate::DirtyLease {
                slot_id,
                generation,
                sandbox_id,
                error,
            } => {
                slots::dirty_reconciled_lease(&tx, *slot_id, *generation, sandbox_id, error, now)?;
            }
            RuntimeReconcileUpdate::PromoteLeasing {
                slot_id,
                generation,
                sandbox_id,
                health,
                last_error,
            } => {
                slots::promote_reconciled_lease(
                    &tx,
                    *slot_id,
                    *generation,
                    sandbox_id,
                    *health,
                    last_error.as_deref(),
                    now,
                )?;
            }
            RuntimeReconcileUpdate::MissingDegraded { slot_id, error } => {
                slots::degrade_slot_for_rebuild(&tx, *slot_id, config.max_slots, error, now)?;
            }
        }
    }
    tx.commit()
}

pub(super) fn query_current_lease_by_sandbox(
    conn: &Connection,
    sandbox_id: &str,
) -> rusqlite::Result<Option<SlotRecord>> {
    Ok(slots::current_lease_by_sandbox(conn, sandbox_id)?.map(SlotRecord::from))
}

/// PID publication never accepts the prefix selectors used by inspection.
/// Caller holds the pool database lock through publication and adoption.
pub(super) fn record_lease_pid(
    conn: &Connection,
    sandbox_id: &str,
    pid: u32,
) -> Result<Option<SlotRecord>, SlotStoreError> {
    let mut records = slots::exact_leases(conn, sandbox_id)?;
    if records.len() != 1 {
        return Ok(None);
    }
    let Some(mut record) = records.pop().map(SlotRecord::from) else {
        return Ok(None);
    };
    let changed = slots::record_lease_pid(
        conn,
        record.info.slot,
        record.info.generation,
        sandbox_id,
        pid,
        now_secs(),
    )?;
    if changed != 1 {
        return Ok(None);
    }
    record.info.pid = Some(pid);
    Ok(Some(record))
}

pub(super) fn query_record_by_slot(
    conn: &Connection,
    slot_id: u32,
) -> rusqlite::Result<Option<SlotRecord>> {
    Ok(slots::slot_by_id(conn, slot_id)?.map(SlotRecord::from))
}

pub(super) fn query_adoptable_ready_records(
    conn: &Connection,
    config: &NetworkPoolConfig,
) -> rusqlite::Result<Vec<SlotRecord>> {
    Ok(
        slots::worker::adoptable_ready_slots(conn, config.max_slots)?
            .into_iter()
            .map(SlotRecord::from)
            .collect(),
    )
}

pub(super) fn query_startup_recoverable_idle_records(
    conn: &Connection,
    config: &NetworkPoolConfig,
) -> rusqlite::Result<Vec<SlotRecord>> {
    Ok(
        slots::worker::startup_recoverable_idle_slots(conn, config.max_slots)?
            .into_iter()
            .map(SlotRecord::from)
            .collect(),
    )
}

pub(super) fn query_live_leased_records(conn: &Connection) -> rusqlite::Result<Vec<SlotRecord>> {
    Ok(slots::live_leased_slots(conn)?
        .into_iter()
        .map(SlotRecord::from)
        .collect())
}

pub(super) fn query_startup_leasing(conn: &Connection) -> Result<Vec<LeaseKey>, SlotStoreError> {
    Ok(query_all_records(conn)?
        .iter()
        .filter(|record| record.state == SlotState::Leasing)
        .filter_map(LeaseKey::from_record)
        .collect())
}

pub(super) fn query_startup_leases(conn: &Connection) -> Result<HashSet<LeaseKey>, SlotStoreError> {
    Ok(query_all_records(conn)?
        .iter()
        .filter(|record| matches!(record.state, SlotState::Leasing | SlotState::Leased))
        .filter_map(LeaseKey::from_record)
        .collect())
}

/// Leases allocated but not yet launched (`leased` with no recorded PID) and
/// last written within `grace` before `now`, each with the time its grace ends.
/// A lease with a PID was launched, so a missing runtime means it died.
pub(super) fn query_startup_young_leases(
    conn: &Connection,
    now: u64,
    grace: Duration,
) -> Result<HashMap<LeaseKey, u64>, SlotStoreError> {
    let mut young = HashMap::new();
    for lease in slots::unlaunched_leases(conn)? {
        let grace_ends = lease.updated_at.saturating_add(grace.as_secs());
        if now < grace_ends {
            let key = LeaseKey {
                slot_id: lease.slot_id,
                generation: lease.generation,
                sandbox_id: lease.sandbox_id,
            };
            young.insert(key, grace_ends);
        }
    }
    Ok(young)
}

pub(super) fn apply_startup_leasing_evidence(
    conn: &Connection,
    key: &LeaseKey,
    pid: Option<u32>,
    evidence: LeaseLiveness,
) -> Result<bool, SlotStoreError> {
    let verb = match evidence {
        LeaseLiveness::Unknown => slots::worker::keep_unknown_startup_leasing,
        LeaseLiveness::Live => slots::worker::promote_live_startup_leasing,
        LeaseLiveness::Gone => slots::worker::dirty_gone_startup_leasing,
    };
    let changed = verb(
        conn,
        key.slot_id,
        key.generation,
        &key.sandbox_id,
        pid,
        now_secs(),
    )?;
    Ok(changed != 0)
}

pub(super) struct LeaseHealthValidationUpdate {
    pub slot_id: u32,
    pub generation: u64,
    pub sandbox_id: String,
    pub health: SlotHealth,
    pub last_error: Option<String>,
}

pub(super) fn apply_live_lease_validation_updates(
    conn: &Connection,
    updates: &[LeaseHealthValidationUpdate],
) -> rusqlite::Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let now = now_secs();
    for update in updates {
        slots::worker::record_lease_health(
            conn,
            update.slot_id,
            update.generation,
            &update.sandbox_id,
            update.health,
            update.last_error.as_deref(),
            now,
        )?;
    }
    Ok(())
}

pub(super) fn apply_startup_lease_updates(
    pool: &NetworkPool,
    updates: &[StartupLeaseUpdate],
) -> Result<(), SlotStoreError> {
    let mut conn = pool.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
    let mut startup = pool
        .startup_leases
        .lock()
        .map_err(|_| SlotStoreError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let mut reset = Vec::new();
    let mut claims = Vec::new();
    for update in updates {
        let key = match update {
            StartupLeaseUpdate::Health(update) => LeaseKey {
                slot_id: update.slot_id,
                generation: update.generation,
                sandbox_id: update.sandbox_id.clone(),
            },
            StartupLeaseUpdate::Gone(info) => LeaseKey {
                slot_id: info.slot,
                generation: info.generation,
                sandbox_id: info.sandbox_id.clone(),
            },
        };
        // Adoption and this write share db -> startup lock ordering. An old
        // probe can never release a lease handed to a new Prepare/Launch.
        if !startup.contains(&key) {
            continue;
        }
        let Some(claim) = pool.try_claim_sandbox_mutation(&key.sandbox_id) else {
            pool.lease_health_recovery_pending
                .store(true, Ordering::Relaxed);
            continue;
        };
        claims.push(claim);
        match update {
            StartupLeaseUpdate::Health(update) => {
                apply_live_lease_validation_updates(&tx, std::slice::from_ref(update))?;
            }
            StartupLeaseUpdate::Gone(info) => {
                let changed = slots::worker::dirty_gone_startup_leased(
                    &tx,
                    key.slot_id,
                    key.generation,
                    &key.sandbox_id,
                    info.pid,
                    now_secs(),
                )?;
                if changed != 0 {
                    reset.push(key);
                }
            }
        }
    }
    tx.commit()?;
    for key in &reset {
        startup.remove(key);
    }
    drop(startup);
    drop(conn);
    drop(claims);
    for key in reset {
        pool.enqueue_reset(key.slot_id);
    }
    pool.mark_node_status_dirty();
    Ok(())
}

pub(super) fn query_all_records(conn: &Connection) -> rusqlite::Result<Vec<SlotRecord>> {
    Ok(slots::all_slots(conn)?
        .into_iter()
        .map(SlotRecord::from)
        .collect())
}

impl From<SlotRow> for SlotRecord {
    fn from(row: SlotRow) -> Self {
        Self {
            info: SlotInfo {
                sandbox_id: row.sandbox_id.unwrap_or_default(),
                slot: row.slot_id,
                generation: row.generation,
                sandbox_uid: row.sandbox_uid.unwrap_or_default(),
                pid: row.pid,
                pod_netns: row.pod_netns_path,
            },
            state: row.state,
            health: row.health,
            lease_kind: row.lease_kind,
            last_error: row.last_error,
            memory_reservation_mib: row.memory_reservation_mib,
        }
    }
}
