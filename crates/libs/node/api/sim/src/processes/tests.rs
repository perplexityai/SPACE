use super::*;

fn spec(command: &[&str]) -> SimProcessSpec {
    SimProcessSpec {
        command: command.iter().map(|arg| (*arg).to_string()).collect(),
        ..SimProcessSpec::default()
    }
}

async fn collect(stream: SimProcessEventStream) -> Vec<sbx::spaced_process_event::Event> {
    stream
        .map(|event| event.unwrap().event.unwrap())
        .collect()
        .await
}

fn stdout_count(events: &[sbx::spaced_process_event::Event]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, sbx::spaced_process_event::Event::Stdout(_)))
        .count()
}

/// `ConnectProcessRequest.output_cursor` semantics, as the guest defines
/// them: the cursor is the number of output records already delivered, and
/// an omitted cursor follows the live tail. Before this the simulator
/// replayed the whole journal on every connect, so the data plane's
/// backend-retry path, which passes the count it already delivered,
/// received stdout twice.
#[tokio::test]
async fn connect_replays_the_journal_from_the_cursor_only() {
    let table = SimProcessTable::default();
    let started = collect(table.start("sbx", spec(&["echo", "hi"])).await).await;
    assert!(matches!(
        started[0],
        sbx::spaced_process_event::Event::Started(_)
    ));
    assert_eq!(stdout_count(&started), 1, "start delivers the journal");
    let process_id = table
        .inner
        .lock()
        .await
        .get("sbx")
        .and_then(|processes| processes.keys().next().cloned())
        .unwrap();

    // Everything already delivered: nothing to replay.
    let at_tail = collect(table.connect("sbx", &process_id, Some(1)).await.unwrap()).await;
    assert_eq!(
        at_tail[0],
        sbx::spaced_process_event::Event::Connected(sbx::SpacedProcessConnected {
            output_cursor: Some(1)
        }),
        "reports where it starts"
    );
    assert_eq!(stdout_count(&at_tail), 0);
    assert!(matches!(
        at_tail.last(),
        Some(sbx::spaced_process_event::Event::Exit(_))
    ));

    // Nothing delivered yet: replay from sequence 0.
    let from_start = collect(table.connect("sbx", &process_id, Some(0)).await.unwrap()).await;
    assert_eq!(stdout_count(&from_start), 1);

    // No cursor: live tail, no replay.
    let live = collect(table.connect("sbx", &process_id, None).await.unwrap()).await;
    assert_eq!(stdout_count(&live), 0);

    // A cursor past the tail fails, as the guest's does, instead of
    // skipping the output before it.
    let beyond: Vec<_> = table
        .connect("sbx", &process_id, Some(2))
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(beyond.len(), 2);
    assert!(matches!(
        beyond[0].as_ref().unwrap().event,
        Some(sbx::spaced_process_event::Event::Connected(_))
    ));
    let error = beyond[1].as_ref().unwrap_err();
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    assert_eq!(
        error
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok()),
        Some(pplx_space_common_core::spaced::PROCESS_OUTPUT_CURSOR_AHEAD)
    );
}

/// Every execution-teardown path (stop, kill, delete, cleanup) ends the
/// sandbox's processes and drops its table, so a restarted or recreated
/// sandbox id does not list its previous incarnation's processes, a stream
/// opened before teardown sees an `Exit`, and the table does not grow for
/// the life of the simulator.
#[tokio::test]
async fn terminating_a_sandbox_ends_its_processes_and_forgets_them() {
    let table = SimProcessTable::default();
    let live_stream = table.start("sbx-a", spec(&["sleep", "30"])).await;
    drop(table.start("sbx-b", spec(&["sleep", "30"])).await);
    assert_eq!(table.list("sbx-a").await.len(), 1);
    let process_id = table.list("sbx-a").await[0].process_id.clone();

    table.terminate_sandbox("sbx-a").await;

    let events = collect(live_stream).await;
    assert!(
        matches!(
            events.last(),
            Some(sbx::spaced_process_event::Event::Exit(
                sbx::SpacedProcessExit {
                    outcome: Some(sbx::spaced_process_exit::Outcome::Signaled(_)),
                }
            ))
        ),
        "a stream open across teardown sees the process end: {events:?}"
    );
    assert!(table.list("sbx-a").await.is_empty());
    assert!(matches!(
        table.connect("sbx-a", &process_id, None).await,
        Err(SimProcessError::NotFound { .. })
    ));
    assert_eq!(
        table.list("sbx-b").await.len(),
        1,
        "other sandboxes are untouched"
    );
    assert!(table.inner.lock().await.get("sbx-a").is_none());
    // Idempotent: tearing down an unknown or already torn-down sandbox is a no-op.
    table.terminate_sandbox("sbx-a").await;
}

/// A background start returns after `Started`, like production, so a client
/// that drains the start stream of a long-running command does not hang until
/// the process is killed. The process stays in the table.
#[tokio::test]
async fn background_start_ends_after_started_and_keeps_the_process() {
    let table = SimProcessTable::default();
    let events = collect(
        table
            .start(
                "sbx",
                SimProcessSpec {
                    background: true,
                    ..spec(&["sleep", "30"])
                },
            )
            .await,
    )
    .await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(matches!(
        events[0],
        sbx::spaced_process_event::Event::Started(_)
    ));
    let running = table.list("sbx").await;
    assert_eq!(running.len(), 1);
    // Connecting later still replays from the cursor and follows the exit.
    let process_id = running[0].process_id.clone();
    table.kill("sbx", &process_id).await.unwrap();
    let events = collect(table.connect("sbx", &process_id, Some(0)).await.unwrap()).await;
    assert_eq!(stdout_count(&events), 1);
    assert!(matches!(
        events.last(),
        Some(sbx::spaced_process_event::Event::Exit(_))
    ));
}

/// Signal and kill resolve only running processes, like the guest: an
/// exited process is `NotFound`, not silently acknowledged.
#[tokio::test]
async fn signaling_an_exited_process_is_not_found() {
    let table = SimProcessTable::default();
    drop(table.start("sbx", spec(&["sleep", "30"])).await);
    let process_id = table.list("sbx").await[0].process_id.clone();

    table.kill("sbx", &process_id).await.unwrap();
    assert!(table.list("sbx").await.is_empty());

    assert!(matches!(
        table.kill("sbx", &process_id).await,
        Err(SimProcessError::NotFound { .. })
    ));
    assert!(matches!(
        table.signal("sbx", &process_id, 15).await,
        Err(SimProcessError::NotFound { .. })
    ));
    // Connect still works for an exited process: it replays the exit.
    let events = collect(table.connect("sbx", &process_id, None).await.unwrap()).await;
    assert!(matches!(
        events.last(),
        Some(sbx::spaced_process_event::Event::Exit(_))
    ));
}
