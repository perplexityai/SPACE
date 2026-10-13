use super::*;

pub(super) use pplx_space_common_svc::sql::sqlite::SqliteU64;

const SLOT_COLUMNS: &str = "slot_id, generation, state, health, sandbox_id, sandbox_uid,
                            pod_netns_path, lease_kind, pid, last_error, memory_reservation_mib";

pub(super) fn query_reset_candidates(
    conn: &Connection,
    cursor: u32,
    limit: u32,
) -> rusqlite::Result<Vec<(u32, u64)>> {
    conn.prepare(
        "SELECT slot_id, generation FROM network_slots
         WHERE state IN ('dirty', 'resetting')
         ORDER BY (slot_id <= ?1), slot_id LIMIT ?2",
    )?
    .query_map(params![cursor, limit], |row| {
        Ok((row.get(0)?, row.get::<_, SqliteU64>(1)?.0))
    })?
    .collect()
}

pub(super) fn query_abandoned_maintenance(
    conn: &Connection,
    cursor: u32,
    limit: u32,
) -> rusqlite::Result<Vec<(u32, u64, SlotState)>> {
    conn.prepare(
        "SELECT slot_id, generation, state FROM network_slots
         WHERE state IN ('creating', 'destroying') AND sandbox_id IS NULL
         ORDER BY (slot_id <= ?1), slot_id LIMIT ?2",
    )?
    .query_map(params![cursor, limit], |row| {
        Ok((
            row.get(0)?,
            row.get::<_, SqliteU64>(1)?.0,
            pool_value(row, 2)?,
        ))
    })?
    .collect()
}

pub(super) fn query_pending_reset_count(conn: &Connection) -> rusqlite::Result<u32> {
    conn.query_row(
        "SELECT COUNT(*) FROM network_slots WHERE state IN ('dirty', 'resetting')",
        [],
        |row| row.get(0),
    )
}

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
        conn.execute(
            "INSERT OR IGNORE INTO network_slots
             (slot_id, slot_label, generation, schema_version, state, health,
              created_at, updated_at, lease_kind)
             VALUES (?1, ?2, 0, ?3, 'missing', 'unknown', ?4, ?4, ?5)",
            params![
                slot_id,
                net::slot_label(slot_id),
                NETWORK_POOL_SCHEMA_VERSION,
                SqliteU64(now),
                LeaseKind::Pooled.as_str()
            ],
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
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'missing', health = 'unknown', sandbox_id = NULL,
                 sandbox_uid = NULL, pid = NULL, pod_netns_path = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL, last_error = NULL, schema_version = ?2,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 last_schema_migration_at = ?3, updated_at = ?3
             WHERE slot_id >= ?4"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            NETWORK_POOL_SCHEMA_VERSION,
            SqliteU64(now),
            config.max_slots
        ],
    )?;
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL, last_error = NULL,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 schema_version = ?2, last_schema_migration_at = ?3, updated_at = ?3
             WHERE schema_version != ?2 AND sandbox_id IS NULL AND state != 'destroying'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            NETWORK_POOL_SCHEMA_VERSION,
            SqliteU64(now)
        ],
    )?;
    conn.execute(
        "UPDATE network_slots
         SET state = 'ready', health = 'unknown', updated_at = ?1
         WHERE state = 'ready'",
        params![SqliteU64(now)],
    )?;
    conn.execute(
        concat!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 ",
            clear_cleanup_reset_retry_sql!(),
            ",
                 last_error = COALESCE(last_error, 'interrupted network slot setup found at startup'),
                 updated_at = ?2
             WHERE state IN ('creating', 'resetting') AND sandbox_id IS NULL"
        ),
        params![LeaseKind::Pooled.as_str(), SqliteU64(now)],
    )?;
    // Keep interrupted destruction, including its namespace, for the durable
    // maintenance pump. Reclassifying it as missing would abandon teardown
    // when targets are zero and could make an uncleared slot reusable.
    conn.execute(
        "UPDATE network_slots
         SET schema_version = ?1, last_schema_migration_at = ?2
         WHERE schema_version != ?1",
        params![NETWORK_POOL_SCHEMA_VERSION, SqliteU64(now)],
    )?;
    Ok(())
}

pub(super) fn reconcile_runtime_state(
    conn: &mut Connection,
    config: &NetworkPoolConfig,
) -> rusqlite::Result<()> {
    let records = query_records(conn, None)?;
    let updates = runtime_reconcile_updates(records, lease_liveness);
    apply_runtime_reconcile_updates(conn, config, &updates)
}

pub(super) fn runtime_reconcile_updates(
    records: Vec<SlotRecord>,
    probe_liveness: impl Fn(&SlotInfo) -> LeaseLiveness,
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
                } else if liveness == LeaseLiveness::Gone {
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

pub(super) fn backfill_active_memory_reservations(
    conn: &Connection,
    legacy_active_memory_reservation_mib: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE network_slots
         SET memory_reservation_mib = ?1
         WHERE state IN ('leasing', 'leased') AND memory_reservation_mib IS NULL",
        params![SqliteU64(legacy_active_memory_reservation_mib)],
    )?;
    Ok(())
}

pub(super) struct MemoryReservationRow {
    pub slot_id: u32,
    pub sandbox_id: String,
    pub generation: u64,
    pub memory_reservation_mib: Option<u64>,
}

pub(super) fn query_active_memory_reservations(
    conn: &Connection,
) -> rusqlite::Result<Vec<MemoryReservationRow>> {
    let mut stmt = conn.prepare(
        "SELECT slot_id, sandbox_id, generation, memory_reservation_mib
         FROM network_slots
         WHERE state IN ('leasing', 'leased') AND sandbox_id IS NOT NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(MemoryReservationRow {
            slot_id: row.get(0)?,
            sandbox_id: row.get(1)?,
            generation: row.get::<_, SqliteU64>(2)?.0,
            memory_reservation_mib: row.get::<_, Option<SqliteU64>>(3)?.map(|value| value.0),
        })
    })?;
    rows.collect()
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
                tx.execute(
                    "UPDATE network_slots
                     SET state = 'quarantined', health = 'invalid',
                         lease_kind = ?1, last_error = ?2, updated_at = ?3,
                         quarantine_count = quarantine_count + 1
                     WHERE slot_id = ?4",
                    params![LeaseKind::Pooled.as_str(), error, SqliteU64(now), slot_id],
                )?;
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
                         WHERE slot_id = ?4 AND slot_id < ?5"
                    ),
                    params![
                        LeaseKind::Pooled.as_str(),
                        error,
                        SqliteU64(now),
                        slot_id,
                        config.max_slots
                    ],
                )?;
            }
        }
    }
    tx.commit()
}

pub(super) fn meta_bool(conn: &Connection, key: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT value FROM network_pool_meta WHERE key = ?1",
        params![key],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map(|value| matches!(value.as_deref(), Some("true") | Some("1")))
}

pub(super) fn set_meta_bool(conn: &Connection, key: &str, value: bool) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO network_pool_meta (key, value, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        params![
            key,
            if value { "true" } else { "false" },
            SqliteU64(now_secs())
        ],
    )?;
    Ok(())
}

pub(super) fn query_current_lease_by_sandbox(
    conn: &Connection,
    sandbox_id: &str,
) -> rusqlite::Result<Option<SlotRecord>> {
    query_one_by_sandbox_states(conn, sandbox_id, "('leasing', 'leased')")
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

pub(super) fn query_one_by_sandbox_states(
    conn: &Connection,
    sandbox_id: &str,
    states_sql: &str,
) -> rusqlite::Result<Option<SlotRecord>> {
    let exact_where = format!("sandbox_id = ?1 AND state IN {states_sql}");
    let exact = query_records(conn, Some((exact_where.as_str(), sandbox_id)))?;
    if exact.len() == 1 {
        return Ok(exact.into_iter().next());
    }
    if exact.len() > 1 {
        return Ok(None);
    }

    let prefix = format!("{sandbox_id}%");
    let prefix_where = format!("sandbox_id LIKE ?1 AND state IN {states_sql}");
    let records = query_records(conn, Some((prefix_where.as_str(), &prefix)))?;
    if records.len() == 1 {
        Ok(records.into_iter().next())
    } else {
        Ok(None)
    }
}

pub(super) fn query_record_by_slot(
    conn: &Connection,
    slot_id: u32,
) -> rusqlite::Result<Option<SlotRecord>> {
    let sql = format!("SELECT {SLOT_COLUMNS} FROM network_slots WHERE slot_id = ?1");
    let mut stmt = conn.prepare(&sql)?;
    stmt.query_row(params![slot_id], row_to_record).optional()
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
    let sql = format!(
        "SELECT {SLOT_COLUMNS}
         FROM network_slots
         WHERE state = 'leased' AND sandbox_id IS NOT NULL
         ORDER BY slot_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], row_to_record)?;
    rows.collect()
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

fn pool_value<T: FromStr<Err = PoolValueParseError>>(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<T> {
    row.get::<_, String>(index)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

pub(super) fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<SlotRecord> {
    let slot_id = row.get::<_, u32>(0)?;
    let generation = row.get::<_, SqliteU64>(1)?.0;
    let state = pool_value(row, 2)?;
    let health = pool_value(row, 3)?;
    let sandbox_id = row.get::<_, Option<String>>(4)?;
    let sandbox_uid = row.get::<_, Option<u32>>(5)?.unwrap_or_default();
    let pod_netns = row.get::<_, Option<String>>(6)?;
    let lease_kind = pool_value(row, 7)?;
    let pid = row.get::<_, Option<u32>>(8)?;
    let last_error = row.get::<_, Option<String>>(9)?;
    let memory_reservation_mib = row.get::<_, Option<SqliteU64>>(10)?.map(|value| value.0);
    Ok(SlotRecord {
        info: SlotInfo {
            sandbox_id: sandbox_id.unwrap_or_default(),
            slot: slot_id,
            generation,
            sandbox_uid,
            pid,
            pod_netns,
        },
        state,
        health,
        lease_kind,
        last_error,
        memory_reservation_mib,
    })
}
