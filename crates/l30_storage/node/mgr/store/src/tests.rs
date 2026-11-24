use super::*;

#[test]
fn slot_vocabularies_round_trip_through_text() {
    for state in [
        SlotState::Missing,
        SlotState::Creating,
        SlotState::Ready,
        SlotState::Leasing,
        SlotState::Leased,
        SlotState::Dirty,
        SlotState::Resetting,
        SlotState::Quarantined,
        SlotState::Destroying,
    ] {
        // A new variant fails to compile here until it is added to this match;
        // add it to the array above too.
        match state {
            SlotState::Missing
            | SlotState::Creating
            | SlotState::Ready
            | SlotState::Leasing
            | SlotState::Leased
            | SlotState::Dirty
            | SlotState::Resetting
            | SlotState::Quarantined
            | SlotState::Destroying => {}
        }
        assert_eq!(state.to_string().parse::<SlotState>().unwrap(), state);
    }
    for health in [
        SlotHealth::Unknown,
        SlotHealth::Healthy,
        SlotHealth::Degraded,
        SlotHealth::Invalid,
    ] {
        // A new variant fails to compile here until it is added to this match;
        // add it to the array above too.
        match health {
            SlotHealth::Unknown
            | SlotHealth::Healthy
            | SlotHealth::Degraded
            | SlotHealth::Invalid => {}
        }
        assert_eq!(health.to_string().parse::<SlotHealth>().unwrap(), health);
    }
    for kind in [LeaseKind::Pooled, LeaseKind::Ephemeral] {
        // A new variant fails to compile here until it is added to this match;
        // add it to the array above too.
        match kind {
            LeaseKind::Pooled | LeaseKind::Ephemeral => {}
        }
        assert_eq!(kind.to_string().parse::<LeaseKind>().unwrap(), kind);
    }
    "parked".parse::<SlotState>().unwrap_err();
    "sick".parse::<SlotHealth>().unwrap_err();
    "borrowed".parse::<LeaseKind>().unwrap_err();
}

/// A migrated in-memory node-manager database.
pub(crate) fn migrated_db() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for migration in database::MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
    conn
}

#[test]
fn retire_execution_leaves_a_newer_execution_bound() {
    use pplx_space_node_mgr_protocol::execution_fence::ExecutionFence;
    use pplx_space_sandbox_model::SandboxActivationAttemptId;

    let conn = migrated_db();
    let stale = SandboxActivationAttemptId::new();
    let fence = ExecutionFence::new(SandboxActivationAttemptId::new(), Some(stale));
    executions::bind_execution(&conn, "sbx", fence).unwrap();

    assert!(!executions::retire_execution(&conn, "sbx", stale).unwrap());
    assert_eq!(
        executions::execution_fence(&conn, "sbx").unwrap(),
        Some(fence)
    );

    assert!(executions::retire_execution(&conn, "sbx", fence.current()).unwrap());
    assert_eq!(executions::execution_fence(&conn, "sbx").unwrap(), None);
}
