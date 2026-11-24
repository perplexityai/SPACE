//! Each guarded transition wins on the slot it names and, once one guarded
//! column disagrees, matches no row and changes nothing.

use super::*;
use crate::tests::migrated_db;

const NOW: u64 = 1_000;

/// Slot 0 as [`insert_missing_slot`] adds it, with `set` applied.
fn slot(set: &str) -> Connection {
    let conn = migrated_db();
    insert_missing_slot(&conn, 0, "slot-0", 4, 1).unwrap();
    conn.0
        .execute_batch(&format!("UPDATE network_slots SET {set} WHERE slot_id = 0"))
        .unwrap();
    conn
}

/// Every column of every slot.
fn rows(conn: &Connection) -> Vec<String> {
    let mut stmt = conn
        .0
        .prepare("SELECT * FROM network_slots ORDER BY slot_id")
        .unwrap();
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        (0..columns)
            .map(|index| row.get_ref(index).map(|value| format!("{value:?}")))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map(|values| values.join(", "))
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

/// `verb` changes the slot set up by `won`, and with `lost` applied on top
/// matches no row and leaves every column as it was. Returns the winning
/// case's database.
#[track_caller]
fn guarded(won: &str, lost: &str, verb: impl Fn(&Connection) -> Result<usize>) -> Connection {
    let won_conn = slot(won);
    let before = rows(&won_conn);
    assert_eq!(verb(&won_conn).unwrap(), 1, "winning case");
    assert_ne!(rows(&won_conn), before, "winning case");

    let conn = slot(&format!("{won}, {lost}"));
    let before = rows(&conn);
    assert_eq!(verb(&conn).unwrap(), 0, "losing case");
    assert_eq!(rows(&conn), before, "losing case");
    won_conn
}

/// Slot 0 matches `written`.
#[track_caller]
fn assert_wrote(conn: &Connection, written: &str) {
    let count: u32 = conn
        .0
        .query_row(
            &format!("SELECT COUNT(*) FROM network_slots WHERE slot_id = 0 AND {written}"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "slot 0 does not match {written}");
}

const IDLE_READY: &str = "state = 'ready', health = 'healthy', generation = 3";
const LEASING: &str = "state = 'leasing', generation = 3, sandbox_id = 'sbx'";
const EPHEMERAL_LEASING: &str =
    "state = 'leasing', generation = 3, sandbox_id = 'sbx', lease_kind = 'ephemeral'";
const LEASED: &str = "state = 'leased', generation = 3, sandbox_id = 'sbx'";
const RESETTING: &str = "state = 'resetting', generation = 3, sandbox_id = 'sbx'";
const CREATING: &str = "state = 'creating', generation = 3";
const DESTROYING: &str = "state = 'destroying', generation = 3";

fn setup_failure(state: SlotState, health: SlotHealth) -> EphemeralSetupFailure<'static> {
    EphemeralSetupFailure {
        slot_id: 0,
        generation: 3,
        sandbox_id: "sbx",
        state,
        health,
        error: "setup failed",
        now: NOW,
    }
}

#[test]
fn claim_ephemeral_lease_loses_to_a_newer_generation() {
    guarded(IDLE_READY, "generation = 4", |conn| {
        claim_ephemeral_lease(
            conn,
            &EphemeralClaim {
                slot_id: 0,
                read_generation: 3,
                read_state: "ready",
                generation: 4,
                sandbox_id: "sbx",
                sandbox_uid: 1,
                pod_netns_path: None,
                memory_reservation_mib: 512,
                now: NOW,
            },
        )
    });
}

#[test]
fn hand_off_namespace_loses_to_another_namespace() {
    let won = "state = 'leasing', generation = 3, pod_netns_path = 'old'";
    guarded(won, "pod_netns_path = 'other'", |conn| {
        hand_off_namespace(
            conn,
            0,
            3,
            SlotState::Leasing,
            Some("old"),
            Some("new"),
            NOW,
        )
    });
}

#[test]
fn mark_ephemeral_leased_loses_to_a_pooled_lease() {
    guarded(EPHEMERAL_LEASING, "lease_kind = 'pooled'", |conn| {
        mark_ephemeral_leased(conn, 0, 3, "sbx", NOW)
    });
}

#[test]
fn fail_ephemeral_setup_loses_to_a_newer_generation() {
    guarded(EPHEMERAL_LEASING, "generation = 4", |conn| {
        fail_ephemeral_setup(
            conn,
            &setup_failure(SlotState::Missing, SlotHealth::Degraded),
        )
    });
}

#[test]
fn quarantine_ephemeral_setup_loses_to_another_sandbox() {
    guarded(EPHEMERAL_LEASING, "sandbox_id = 'other'", |conn| {
        quarantine_ephemeral_setup(
            conn,
            &setup_failure(SlotState::Quarantined, SlotHealth::Invalid),
        )
    });
}

#[test]
fn dirty_failed_ephemeral_completion_loses_to_a_completed_lease() {
    guarded(EPHEMERAL_LEASING, "state = 'leased'", |conn| {
        dirty_failed_ephemeral_completion(conn, 0, 3, "sbx", NOW)
    });
}

#[test]
fn release_lease_loses_to_a_released_lease() {
    guarded(LEASED, "state = 'dirty'", |conn| {
        release_lease(conn, 0, 3, "sbx", NOW)
    });
}

#[test]
fn rebuild_idle_quarantined_loses_to_an_owned_slot() {
    let conn = guarded("state = 'quarantined'", "sandbox_id = 'sbx'", |conn| {
        rebuild_idle_quarantined(conn, NOW)
    });
    assert_wrote(
        &conn,
        "state = 'missing' AND health = 'degraded' AND lease_kind = 'pooled' AND sandbox_id IS NULL AND updated_at = 1000",
    );
}

#[test]
fn retire_stale_pod_namespace_lease_loses_to_a_leasing_lease() {
    guarded(LEASED, "state = 'leasing'", |conn| {
        retire_stale_pod_namespace_lease(conn, 0, 3, "sbx", "namespace gone", NOW)
    });
}

#[test]
fn dirty_reconciled_lease_loses_to_another_sandbox() {
    guarded(LEASED, "sandbox_id = 'other'", |conn| {
        dirty_reconciled_lease(conn, 0, 3, "sbx", "runtime gone", NOW)
    });
}

#[test]
fn promote_reconciled_lease_loses_to_a_newer_generation() {
    let conn = guarded(LEASING, "generation = 4", |conn| {
        promote_reconciled_lease(conn, 0, 3, "sbx", SlotHealth::Healthy, None, NOW)
    });
    assert_wrote(
        &conn,
        "state = 'leased' AND health = 'healthy' AND last_error IS NULL AND updated_at = 1000",
    );
}

#[test]
fn record_lease_pid_loses_to_a_released_lease() {
    guarded(LEASED, "state = 'dirty'", |conn| {
        record_lease_pid(conn, 0, 3, "sbx", 7, NOW)
    });
}

#[test]
fn begin_reset_loses_to_a_ready_slot() {
    guarded(
        "state = 'dirty', generation = 3",
        "state = 'ready'",
        |conn| reset::begin_reset(conn, 0, 3, NOW),
    );
}

#[test]
fn finish_pooled_reset_loses_to_a_newer_generation() {
    let conn = guarded(RESETTING, "generation = 4", |conn| {
        reset::finish_pooled_reset(conn, 0, 3, Some("pod"), NOW)
    });
    assert_wrote(
        &conn,
        "state = 'ready' AND health = 'healthy' AND sandbox_id IS NULL AND pod_netns_path = 'pod' AND lease_kind = 'pooled' AND updated_at = 1000 AND last_reset_at = 1000",
    );
}

#[test]
fn finish_ephemeral_reset_loses_to_a_deferred_reset() {
    guarded(RESETTING, "state = 'dirty'", |conn| {
        reset::finish_ephemeral_reset(conn, 0, 3, NOW)
    });
}

#[test]
fn defer_reset_loses_to_a_deferred_reset() {
    guarded(RESETTING, "state = 'dirty'", |conn| {
        reset::defer_reset(conn, 0, 3, "lease process alive", NOW)
    });
}

#[test]
fn quarantine_reset_loses_to_a_newer_generation() {
    let conn = guarded(RESETTING, "generation = 4", |conn| {
        reset::quarantine_reset(conn, 0, 3, "reset failed", NOW)
    });
    assert_wrote(
        &conn,
        "state = 'quarantined' AND health = 'invalid' AND sandbox_id IS NULL AND lease_kind = 'pooled' AND last_error = 'reset failed' AND updated_at = 1000 AND quarantine_count = 1",
    );
}

#[test]
fn quarantine_reset_preserving_lease_loses_to_a_quarantined_slot() {
    guarded(RESETTING, "state = 'quarantined'", |conn| {
        reset::quarantine_reset_preserving_lease(conn, 0, 3, "reset failed", NOW)
    });
}

#[test]
fn record_cleanup_reset_failure_loses_to_another_previous_lease() {
    let conn = slot(RESETTING);
    assert_eq!(
        reset::record_cleanup_reset_failure(&conn, 0, 3, Some("sbx"), NOW).unwrap(),
        Some(1)
    );
    let before = rows(&conn);
    assert_eq!(
        reset::record_cleanup_reset_failure(&conn, 0, 3, None, NOW).unwrap(),
        None
    );
    assert_eq!(
        reset::record_cleanup_reset_failure(&conn, 0, 3, Some("other"), NOW).unwrap(),
        None
    );
    assert_eq!(rows(&conn), before);
}

#[test]
fn claim_missing_for_create_loses_to_another_health() {
    guarded("generation = 3", "health = 'degraded'", |conn| {
        worker::claim_missing_for_create(conn, 0, 3, "unknown", NOW)
    });
}

#[test]
fn mark_created_ready_loses_to_an_abandoned_setup() {
    guarded(CREATING, "state = 'missing'", |conn| {
        worker::mark_created_ready(conn, 0, 3, None, NOW)
    });
}

#[test]
fn degrade_failed_create_loses_to_a_newer_generation() {
    guarded(CREATING, "generation = 4", |conn| {
        worker::degrade_failed_create(conn, 0, 3, "transient", NOW)
    });
}

#[test]
fn quarantine_failed_create_loses_to_an_abandoned_setup() {
    guarded(CREATING, "state = 'missing'", |conn| {
        worker::quarantine_failed_create(conn, 0, 3, "invalid", NOW)
    });
}

#[test]
fn revalidate_abandoned_setup_loses_to_an_owned_slot() {
    guarded(CREATING, "sandbox_id = 'sbx'", |conn| {
        worker::revalidate_abandoned_setup(conn, 0, 3)
    });
}

#[test]
fn claim_for_destroy_loses_to_a_launched_slot() {
    guarded("state = 'ready'", "pid = 7", |conn| {
        worker::claim_for_destroy(conn, 0, NOW)
    });
}

#[test]
fn finish_destroy_loses_to_a_newer_generation() {
    guarded(DESTROYING, "generation = 4", |conn| {
        worker::finish_destroy(conn, 0, 3, NOW)
    });
}

#[test]
fn record_destroy_failure_loses_to_an_owned_slot() {
    guarded(DESTROYING, "sandbox_id = 'sbx'", |conn| {
        worker::record_destroy_failure(conn, 0, 3, "teardown failed", NOW)
    });
}

#[test]
fn mark_validated_ready_loses_to_another_state() {
    guarded(IDLE_READY, "state = 'missing'", |conn| {
        worker::mark_validated_ready(conn, 0, 3, SlotState::Ready, None, NOW)
    });
}

#[test]
fn degrade_failed_validation_loses_to_a_newer_generation() {
    guarded(IDLE_READY, "generation = 4", |conn| {
        worker::degrade_failed_validation(conn, 0, 3, SlotState::Ready, "invalid", NOW)
    });
}

const PROBED_LEASING: &str = "state = 'leasing', generation = 3, sandbox_id = 'sbx', pid = 7";

#[test]
fn keep_unknown_startup_leasing_loses_to_a_published_pid() {
    guarded(PROBED_LEASING, "pid = 8", |conn| {
        worker::keep_unknown_startup_leasing(conn, 0, 3, "sbx", Some(7), NOW)
    });
}

#[test]
fn promote_live_startup_leasing_loses_to_a_published_pid() {
    guarded(PROBED_LEASING, "pid = 8", |conn| {
        worker::promote_live_startup_leasing(conn, 0, 3, "sbx", Some(7), NOW)
    });
}

#[test]
fn dirty_gone_startup_leasing_loses_to_a_published_pid() {
    guarded(PROBED_LEASING, "pid = 8", |conn| {
        worker::dirty_gone_startup_leasing(conn, 0, 3, "sbx", Some(7), NOW)
    });
}

#[test]
fn dirty_gone_startup_leased_loses_to_a_published_pid() {
    let won = "state = 'leased', generation = 3, sandbox_id = 'sbx', pid = 7";
    guarded(won, "pid = 8", |conn| {
        worker::dirty_gone_startup_leased(conn, 0, 3, "sbx", Some(7), NOW)
    });
}

#[test]
fn record_lease_health_loses_to_a_leasing_lease() {
    guarded(LEASED, "state = 'leasing'", |conn| {
        worker::record_lease_health(conn, 0, 3, "sbx", SlotHealth::Degraded, Some("probe"), NOW)
    });
}

#[test]
fn lease_reads_match_the_sandbox_id_exactly() {
    let conn = slot(LEASED);
    insert_missing_slot(&conn, 1, "slot-1", 4, 1).unwrap();
    insert_missing_slot(&conn, 2, "slot-2", 4, 1).unwrap();
    conn.0
        .execute_batch(
            "UPDATE network_slots SET state = 'leased', sandbox_id = 'sbx-2' WHERE slot_id = 1;
         UPDATE network_slots SET state = 'dirty', sandbox_id = 'sbx-3' WHERE slot_id = 2;",
        )
        .unwrap();

    let ids = |rows: Vec<SlotRow>| rows.iter().map(|row| row.slot_id).collect::<Vec<_>>();
    assert_eq!(ids(exact_leases(&conn, "sbx").unwrap()), [0]);
    assert_eq!(ids(exact_leases(&conn, "sbx-2").unwrap()), [1]);
    assert_eq!(
        ids(previous_leases(&conn, "sbx").unwrap()),
        Vec::<u32>::new()
    );
    assert_eq!(ids(previous_leases(&conn, "sbx-3").unwrap()), [2]);
}
