use pplx_space_common_core::types::JobId;

use super::*;

#[test]
fn cleanup_job_admission_is_atomic_deduplicated_and_strength_preserving() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let work = CleanupWork {
        version: 1,
        sandbox_id,
        uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
        execution_generation: None,
        cleanup_id: None,
        completion: CleanupCompletion::MarkSuspended,
        keep_filesystem: true,
        immediate_kill: false,
        guest_flush: GuestFlush::Unrequested,
        published_artifact: Some((LocalArtifactId::new(), "/cache/suspend".to_owned())),
    };
    {
        let tx = Transaction::deferred(&conn).unwrap();
        persist(&tx, work.clone()).unwrap();
    }
    assert!(
        jobs::find_active_job(conn.raw(), KIND, &sandbox_id.to_string())
            .unwrap()
            .is_none()
    );
    let tx = Transaction::deferred(&conn).unwrap();
    let id = persist(&tx, work.clone()).unwrap();
    tx.commit().unwrap();
    let started = crate::db::read::cleanup_job_created_at(&conn, &id)
        .unwrap()
        .unwrap();
    assert!(started.get() > 0);
    let mut kill = work.clone();
    kill.immediate_kill = true;
    let tx = Transaction::deferred(&conn).unwrap();
    assert_eq!(persist(&tx, kill).unwrap(), id);
    assert_eq!(persist(&tx, work.clone()).unwrap(), id);
    tx.commit().unwrap();
    assert!(
        decode(&jobs::get_job(conn.raw(), &id).unwrap().unwrap().payload)
            .unwrap()
            .immediate_kill
    );
    assert_eq!(
        crate::db::read::cleanup_job_created_at(&conn, &id).unwrap(),
        Some(started)
    );
    let mut recovery = work.clone();
    recovery.published_artifact = None;
    let tx = Transaction::deferred(&conn).unwrap();
    assert_eq!(persist(&tx, recovery).unwrap(), id);
    tx.commit().unwrap();
    assert_eq!(
        decode(&jobs::get_job(conn.raw(), &id).unwrap().unwrap().payload)
            .unwrap()
            .published_artifact,
        work.published_artifact
    );
    let mut replacement = work;
    replacement.execution_generation = Some(ExecutionGeneration::new(2).unwrap());
    let tx = Transaction::deferred(&conn).unwrap();
    let next = persist(&tx, replacement).unwrap();
    tx.commit().unwrap();
    assert_ne!(next, id);
    assert_eq!(
        jobs::get_job(conn.raw(), &id).unwrap().unwrap().status,
        JobStatus::Cancelled.as_str()
    );
}

// A stop's guest flush outlives a manager restart in its persisted work, a
// forced stop overrides an ordinary one in either order, and a retry that
// asks nothing keeps what the pending stop asked for.
#[test]
fn a_stops_guest_flush_persists_and_a_forced_stop_overrides_it() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let stop = |guest_flush, immediate_kill| CleanupWork {
        version: 1,
        sandbox_id,
        uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
        execution_generation: None,
        cleanup_id: None,
        completion: CleanupCompletion::MarkStopped,
        keep_filesystem: true,
        immediate_kill,
        guest_flush,
        published_artifact: None,
    };
    let persisted = |id: &JobId| {
        decode(&jobs::get_job(conn.raw(), id).unwrap().unwrap().payload)
            .unwrap()
            .guest_flush
    };
    let tx = Transaction::deferred(&conn).unwrap();
    let id = persist(&tx, stop(GuestFlush::BeforeTermination, false)).unwrap();
    assert_eq!(
        persist(&tx, stop(GuestFlush::Unrequested, false)).unwrap(),
        id
    );
    tx.commit().unwrap();
    assert_eq!(persisted(&id), GuestFlush::BeforeTermination);

    let tx = Transaction::deferred(&conn).unwrap();
    assert_eq!(persist(&tx, stop(GuestFlush::Forbidden, true)).unwrap(), id);
    assert_eq!(
        persist(&tx, stop(GuestFlush::BeforeTermination, false)).unwrap(),
        id
    );
    tx.commit().unwrap();
    assert_eq!(persisted(&id), GuestFlush::Forbidden);
}

// Only a VM known not to be running skips the flush; a state in transition
// is no proof that one already ran, a paused VM can hold writes that
// completed after the pause's own flush, and a forced stop stays forced.
#[test]
fn a_stops_guest_flush_is_skipped_only_where_no_vm_runs() {
    for state in [
        SandboxState::Suspended,
        SandboxState::Stopped,
        SandboxState::Created,
        SandboxState::Creating,
    ] {
        assert_eq!(
            GuestFlush::BeforeTermination.for_superseded_state(Some(state)),
            GuestFlush::Unrequested,
            "{state:?}"
        );
    }
    for state in [
        Some(SandboxState::Started),
        Some(SandboxState::Paused),
        Some(SandboxState::Pausing),
        Some(SandboxState::Resuming),
        Some(SandboxState::Checkpointing),
        Some(SandboxState::Suspending),
        None,
    ] {
        assert_eq!(
            GuestFlush::BeforeTermination.for_superseded_state(state),
            GuestFlush::BeforeTermination,
            "{state:?}"
        );
    }
    assert_eq!(
        GuestFlush::Forbidden.for_superseded_state(Some(SandboxState::Started)),
        GuestFlush::Forbidden
    );
}

// Work persisted before the field existed decodes, and does not flush.
#[test]
fn work_persisted_without_a_guest_flush_does_not_flush() {
    let sandbox_id = SandboxId::new();
    let mut payload = serde_json::to_value(CleanupWork {
        version: 1,
        sandbox_id,
        uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
        execution_generation: None,
        cleanup_id: None,
        completion: CleanupCompletion::MarkStopped,
        keep_filesystem: true,
        immediate_kill: false,
        guest_flush: GuestFlush::BeforeTermination,
        published_artifact: None,
    })
    .unwrap();
    payload
        .as_object_mut()
        .unwrap()
        .remove("guest_flush")
        .unwrap();
    let work = decode(&payload.to_string()).unwrap();
    assert_eq!(work.guest_flush, GuestFlush::Unrequested);
}

#[test]
fn recovery_cursor_crosses_empty_pages_without_skipping_obligations() {
    for limit in [1, 7, 128] {
        let conn = test_conn();
        let ids: Vec<_> = (0..40).map(|_| SandboxId::new().to_string()).collect();
        for (index, id) in ids.iter().enumerate() {
            let state = if [15, 31, 39].contains(&index) {
                SandboxState::Deleting
            } else {
                SandboxState::Suspended
            };
            crate::db::fixture::insert_test_sandbox(&conn, id, state);
        }
        // A later sweep must revisit intent added behind the prior cursor.
        for obligations in [vec![15, 31, 39], vec![15, 20, 31, 39]] {
            if obligations.contains(&20) {
                conn.execute(
                    "UPDATE sandboxes SET state = 'deleting' WHERE id = ?1",
                    [&ids[20]],
                )
                .unwrap();
            }
            let mut cursor = None;
            let mut found = Vec::new();
            for _ in 0..=(ids.len() / limit + 1) {
                let page = cleanup_recovery_page(&conn, cursor, limit).unwrap();
                assert!(page.ids.len() <= limit);
                assert!(page.next_cursor.is_none() || page.next_cursor > cursor);
                found.extend(page.ids);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert!(
                cursor.is_none(),
                "must wrap past inventory with no cleanup intent"
            );
            found.sort();
            let mut expected: Vec<_> = obligations
                .iter()
                .map(|index| ids[*index].clone())
                .collect();
            expected.sort();
            assert_eq!(found, expected, "limit {limit}");
        }
    }
}

#[test]
fn bounded_dispatch_preserves_retry_priority_and_other_workers() {
    use pplx_space_common_jobs::db::*;
    let conn = test_conn();
    for (id, kind, subject, priority) in [
        (
            "019d84bd-280d-7331-99c6-000000000001"
                .parse::<JobId>()
                .unwrap(),
            KIND,
            "a",
            0,
        ),
        (
            "019d84bd-280d-7331-99c6-000000000002"
                .parse::<JobId>()
                .unwrap(),
            KIND,
            "b",
            0,
        ),
        (
            "019d84bd-280d-7331-99c6-000000000003"
                .parse::<JobId>()
                .unwrap(),
            KIND,
            "c",
            0,
        ),
        (
            "019d84bd-280d-7331-99c6-000000000004"
                .parse::<JobId>()
                .unwrap(),
            KIND,
            "d",
            1,
        ),
        (
            "019d84bd-280d-7331-99c6-000000000005"
                .parse::<JobId>()
                .unwrap(),
            "upload",
            "e",
            0,
        ),
    ] {
        insert_job(
            conn.raw(),
            InsertJob {
                job_id: &id,
                kind,
                payload: "{}",
                subject_id: Some(subject),
                dedup_key: Some(subject),
                max_attempts: 5,
                traceparent: None,
                priority: JobPriority::new(priority),
            },
        )
        .unwrap();
    }
    mark_job_running(
        conn.raw(),
        &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
    )
    .unwrap();
    retry_running_job(
        conn.raw(),
        &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        "busy",
        3600,
    )
    .unwrap();
    mark_job_running(
        conn.raw(),
        &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap(),
    )
    .unwrap();
    mark_job_running(
        conn.raw(),
        &"019d84bd-280d-7331-99c6-000000000005".parse().unwrap(),
    )
    .unwrap();
    let active = vec!["b".to_owned()];
    assert_eq!(
        due_job_ids(&conn, JobPriority::new(0), &active, 1).unwrap(),
        ["019d84bd-280d-7331-99c6-000000000003"
            .parse::<JobId>()
            .unwrap()]
    );
    assert!(
        due_job_ids(&conn, JobPriority::new(0), &active, 0)
            .unwrap()
            .is_empty()
    );
    reset_inactive_running_kind(conn.raw(), KIND, &active).unwrap();
    assert_eq!(
        get_job(
            conn.raw(),
            &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap()
        )
        .unwrap()
        .unwrap()
        .status,
        "running"
    );
    update_active_job_payload(
        conn.raw(),
        &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        "{\"kill\":true}",
        JobPriority::new(1),
    )
    .unwrap();
    let promoted = due_job_ids(&conn, JobPriority::new(1), &[], 2).unwrap();
    assert_eq!(promoted.len(), 2);
    assert!(promoted.contains(&"019d84bd-280d-7331-99c6-000000000001".parse().unwrap()));
    assert_eq!(reset_running_kind(conn.raw(), KIND).unwrap(), 1);
    assert_eq!(
        get_job(
            conn.raw(),
            &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap()
        )
        .unwrap()
        .unwrap()
        .status,
        "pending"
    );
    assert_eq!(
        get_job(
            conn.raw(),
            &"019d84bd-280d-7331-99c6-000000000005".parse().unwrap()
        )
        .unwrap()
        .unwrap()
        .status,
        "running"
    );
}

#[test]
fn retry_discovery_does_not_scan_sleeping_jobs() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    static STEPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    fn trace(event: TraceEvent<'_>) {
        if let TraceEvent::Profile(statement, _) = event {
            STEPS.fetch_add(
                usize::try_from(statement.get_status(rusqlite::StatementStatus::VmStep)).unwrap(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
    let conn = test_conn();
    conn.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO jobs(job_id, kind, next_attempt_at, created_at) SELECT 'sleeping-'||x, 'sandbox_cleanup', '2999-01-01', '2000-01-01' FROM n; INSERT INTO jobs(job_id, kind) VALUES ('019d84bd-280d-7331-99c6-000000000003', 'sandbox_cleanup')").unwrap();
    conn.trace_v2(TraceEventCodes::SQLITE_TRACE_PROFILE, Some(trace));
    assert_eq!(
        due_job_ids(&conn, JobPriority::new(0), &[], 1).unwrap(),
        ["019d84bd-280d-7331-99c6-000000000003"
            .parse::<JobId>()
            .unwrap()]
    );
    conn.trace_v2(TraceEventCodes::empty(), None);
    assert!(
        STEPS.load(std::sync::atomic::Ordering::Relaxed) < 500,
        "sleeping jobs consumed dispatch SQL work"
    );
}
