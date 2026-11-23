use super::*;

pub(super) use pplx_space_sqlite::SqliteU64;
use slots::{SLOT_COLUMNS, SlotRow};

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
    for migration in sqlx::migrate!("../../../../../db/node-manager")
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
    let records = query_records(conn, None)?;
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
    let records = query_records(conn, None)?;
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
                        health: record.health.as_str().to_string(),
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
    let now = now_secs();
    conn.query_row(
        "UPDATE network_slots
         SET cleanup_reset_attempts = cleanup_reset_attempts + 1,
             last_reset_retry_at = ?1,
             updated_at = ?1
         WHERE slot_id = ?2 AND generation = ?3
           AND state = 'resetting'
           AND ((?4 IS NULL AND sandbox_id IS NULL) OR sandbox_id = ?4)
         RETURNING cleanup_reset_attempts",
        params![
            SqliteU64(now),
            reset_key.slot_id,
            SqliteU64(reset_key.generation),
            reset_key.sandbox_id.as_deref()
        ],
        |row| row.get::<_, u32>(0),
    )
    .optional()
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
        health: String,
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
                tx.execute(
                    "UPDATE network_slots
                     SET state = 'dirty', health = 'unknown',
                         memory_reservation_mib = NULL, last_error = ?1, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4 AND sandbox_id = ?5",
                    params![
                        error,
                        SqliteU64(now),
                        slot_id,
                        SqliteU64(*generation),
                        sandbox_id
                    ],
                )?;
            }
            RuntimeReconcileUpdate::PromoteLeasing {
                slot_id,
                generation,
                sandbox_id,
                health,
                last_error,
            } => {
                tx.execute(
                    "UPDATE network_slots
                     SET state = 'leased', health = ?1, last_error = ?2, updated_at = ?3
                     WHERE slot_id = ?4 AND generation = ?5 AND sandbox_id = ?6",
                    params![
                        health,
                        last_error,
                        SqliteU64(now),
                        slot_id,
                        SqliteU64(*generation),
                        sandbox_id
                    ],
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
    let mut records = query_records(
        conn,
        Some((
            "sandbox_id = ?1 AND state IN ('leasing', 'leased')",
            sandbox_id,
        )),
    )?;
    if records.len() != 1 {
        return Ok(None);
    }
    let Some(mut record) = records.pop() else {
        return Ok(None);
    };
    let changed = conn.execute(
        "UPDATE network_slots SET pid = ?1, updated_at = ?2
         WHERE slot_id = ?3 AND generation = ?4 AND sandbox_id = ?5
           AND state IN ('leasing', 'leased')",
        params![
            pid,
            SqliteU64(now_secs()),
            record.info.slot,
            SqliteU64(record.info.generation),
            sandbox_id
        ],
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
    query_recoverable_idle_records(conn, config, "state = 'ready' AND health != 'healthy'")
}

pub(super) fn query_startup_recoverable_idle_records(
    conn: &Connection,
    config: &NetworkPoolConfig,
) -> rusqlite::Result<Vec<SlotRecord>> {
    query_recoverable_idle_records(
        conn,
        config,
        "(state = 'ready' AND health != 'healthy')
         OR (state = 'missing' AND health = 'degraded')",
    )
}

pub(super) fn query_live_leased_records(conn: &Connection) -> rusqlite::Result<Vec<SlotRecord>> {
    Ok(slots::live_leased_slots(conn)?
        .into_iter()
        .map(SlotRecord::from)
        .collect())
}

pub(super) fn query_startup_leasing(conn: &Connection) -> Result<Vec<LeaseKey>, SlotStoreError> {
    Ok(query_records(conn, None)?
        .iter()
        .filter(|record| record.state == SlotState::Leasing)
        .filter_map(LeaseKey::from_record)
        .collect())
}

pub(super) fn query_startup_leases(conn: &Connection) -> Result<HashSet<LeaseKey>, SlotStoreError> {
    Ok(query_records(conn, None)?
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
    let changed = match evidence {
        LeaseLiveness::Unknown => conn.execute(
            "UPDATE network_slots SET health = 'unknown',
                 last_error = 'startup runtime evidence unknown; retrying', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
               AND state = 'leasing' AND pid IS ?5",
            params![SqliteU64(now_secs()), key.slot_id, SqliteU64(key.generation), key.sandbox_id, pid],
        )?,
        LeaseLiveness::Live => conn.execute(
            "UPDATE network_slots SET state = 'leased', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4 AND state = 'leasing' AND pid IS ?5",
            params![SqliteU64(now_secs()), key.slot_id, SqliteU64(key.generation), key.sandbox_id, pid],
        )?,
        LeaseLiveness::Gone => conn.execute(
            "UPDATE network_slots SET state = 'dirty', health = 'unknown',
                 memory_reservation_mib = NULL,
                 last_error = 'interrupted lease runtime is gone after startup re-probe', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4 AND state = 'leasing' AND pid IS ?5",
            params![SqliteU64(now_secs()), key.slot_id, SqliteU64(key.generation), key.sandbox_id, pid],
        )?,
    };
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
        conn.execute(
            "UPDATE network_slots
                     SET health = ?1, last_error = ?2, updated_at = ?3
                     WHERE slot_id = ?4 AND generation = ?5 AND sandbox_id = ?6
                       AND state = 'leased'",
            params![
                update.health.as_str(),
                update.last_error.as_deref(),
                SqliteU64(now),
                update.slot_id,
                SqliteU64(update.generation),
                update.sandbox_id
            ],
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
                let changed = tx.execute(
                    "UPDATE network_slots SET state = 'dirty', health = 'unknown',
                         memory_reservation_mib = NULL,
                         last_error = 'startup runtime gone; cleanup queued', updated_at = ?1
                     WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
                       AND state = 'leased' AND pid IS ?5",
                    params![
                        SqliteU64(now_secs()),
                        key.slot_id,
                        SqliteU64(key.generation),
                        key.sandbox_id,
                        info.pid
                    ],
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

fn query_recoverable_idle_records(
    conn: &Connection,
    config: &NetworkPoolConfig,
    state_predicate: &str,
) -> rusqlite::Result<Vec<SlotRecord>> {
    let sql = format!(
        "SELECT {SLOT_COLUMNS}
         FROM network_slots
         WHERE slot_id < ?1
           AND sandbox_id IS NULL
           AND ({state_predicate})
         ORDER BY CASE state
                    WHEN 'ready' THEN 0
                    WHEN 'missing' THEN 1
                  END,
                  slot_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![config.max_slots], row_to_record)?;
    rows.collect()
}

pub(super) fn query_records(
    conn: &Connection,
    where_clause: Option<(&str, &str)>,
) -> rusqlite::Result<Vec<SlotRecord>> {
    let base = format!("SELECT {SLOT_COLUMNS} FROM network_slots");
    let sql = if let Some((where_clause, _)) = where_clause {
        format!("{base} WHERE {where_clause} ORDER BY slot_id")
    } else {
        format!("{base} ORDER BY slot_id")
    };
    let mut stmt = conn.prepare(&sql)?;
    let rows = if let Some((_, value)) = where_clause {
        stmt.query_map(params![value], row_to_record)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        stmt.query_map([], row_to_record)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    Ok(rows)
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<SlotRecord> {
    SlotRow::from_row(row).map(SlotRecord::from)
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
