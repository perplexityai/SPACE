use super::*;
use futures_util::StreamExt;
use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::StartRequest;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_server::Process;

#[test]
fn dropping_a_pending_pty_read_releases_blocking_capacity() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let PreparedPty {
            mut reader,
            writer,
            resize_fd,
            control_fd,
        } = prepare_pty(None).unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_millis(20), reader.read(&mut byte))
            .await
            .expect_err("PTY without slave output must stay pending");
        drop(reader);
        let mut other_work = tokio::task::spawn_blocking(|| ());
        let released = tokio::time::timeout(Duration::from_millis(100), &mut other_work)
            .await
            .is_ok();
        // Always release the slave before asserting: the old blocking adapter
        // would otherwise hang the test runtime during unwinding.
        drop((control_fd, writer, resize_fd));
        if !released {
            other_work.await.unwrap();
        }
        assert!(released, "cancelled PTY read retained a blocking worker");
    });
}

#[tokio::test]
async fn pty_close_stdin_rejects_without_disabling_input() {
    let service = cancellation_service();
    let response = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec!["/bin/cat".into()],
            pty: Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::PtyConfig { size: None },
            ),
            idempotency_key: Some("pty-close".into()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let mut stream = response.into_inner();
    let started = stream.next().await.unwrap().unwrap();
    let result = Process::close_stdin(
        &service,
        Request::new(CloseStdinRequest {
            process_id: started.process_id.clone(),
        }),
    )
    .await;
    let input = Process::send_input(
        &service,
        Request::new(SendInputRequest {
            process_id: started.process_id,
            data: b"still-open\n".to_vec(),
        }),
    )
    .await;
    service.inner.shutdown.cancel();
    service.inner.process_tasks.close();
    service.inner.process_tasks.wait().await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    input.expect("unsupported close must preserve PTY input");
}

// A reconnect re-sends Start for a session whose process may already run. It
// must get AlreadyExists, the only answer the data plane turns into a connect,
// even while the start path it would otherwise take is full or blocked.
#[tokio::test]
async fn a_live_process_id_is_answered_before_start_admission() {
    let service = cancellation_service();
    let process_id = "sbxps_2f1c9a4e-0000-7000-8000-000000000002";
    let start = |key: &str| {
        Request::new(StartRequest {
            command: vec!["/bin/cat".into()],
            pty: Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::PtyConfig { size: None },
            ),
            idempotency_key: Some(key.into()),
            process_id: Some(process_id.into()),
            ..Default::default()
        })
    };
    let _running = Process::start(&service, start("live-id-first"))
        .await
        .expect("first start");
    // Hold every start slot, as a busy start path would.
    let slots = service
        .inner
        .process_payload_slots
        .clone()
        .acquire_many_owned(pplx_space_spaced_guest_protocol::process_limits::PAYLOAD_SLOTS as u32)
        .await
        .unwrap();
    for key in ["live-id-first", "live-id-retry"] {
        let status =
            tokio::time::timeout(Duration::from_secs(1), Process::start(&service, start(key)))
                .await
                .expect("a live id must not wait on the start path")
                .err()
                .expect("a live id is not started twice");
        assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status:?}");
    }
    drop(slots);
    service.inner.shutdown.cancel();
    service.inner.process_tasks.close();
    service.inner.process_tasks.wait().await;
}

#[test]
fn process_tag_is_bounded_before_registry_retention() {
    validate_process_tag(Some("tag".to_string())).unwrap();
    validate_process_tag(Some("x".repeat(MAX_PROCESS_TAG_BYTES + 1))).unwrap_err();
    validate_process_tag(Some("bad\0tag".to_string())).unwrap_err();
}

fn output_slots() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(PROCESS_OUTPUT_SHARED_BYTES))
}

#[cfg(target_os = "linux")]
fn cancellation_start_request() -> Request<StartRequest> {
    Request::new(StartRequest {
        command: vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
        idempotency_key: Some("cancel-after-spawn".into()),
        ..Default::default()
    })
}

fn cancellation_service() -> ExecutionService {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("current user lookup")
        .expect("process tests require a passwd entry for the current uid");
    ExecutionService::with_config(
        CancellationToken::new(),
        ExecutionServiceConfig {
            default_operation_user: GuestUserName::parse(user.name).expect("valid current user"),
            filesystem_execution: FilesystemExecutionMode::Direct,
            identity_policy: IdentityPolicy::Guest,
            ..ExecutionServiceConfig::default()
        },
    )
    .expect("direct execution service")
}

#[tokio::test]
async fn process_payload_admission_rejects_before_decode_and_releases_at_headers() {
    use pplx_space_spaced_guest_protocol::process_limits;
    use tonic::codegen::{Service, http};
    let service = cancellation_service();
    let slots = service.inner.process_payload_slots.clone();
    let occupied = slots
        .clone()
        .acquire_many_owned(process_limits::PAYLOAD_SLOTS as u32)
        .await
        .unwrap();
    let mut grpc = service.process_grpc_service(service.clone());
    for (method, _) in process_limits::PAYLOAD_METHODS {
        // An empty body is invalid gRPC. Admission must reject it before the
        // decoder (and consequently before any process effect) is entered.
        let request = http::Request::builder()
            .uri(format!(
                "/{}/{method}",
                <ProcessServer<ExecutionService> as tonic::server::NamedService>::NAME
            ))
            .header("content-type", "application/grpc")
            .body(tonic::body::Body::empty())
            .unwrap();
        let response = grpc.call(request).await.unwrap();
        assert_eq!(response.headers()["grpc-status"], "8");
    }
    assert!(service.inner.processes.records.is_empty());
    drop(occupied);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = service.inner.shutdown.clone();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.process_grpc_service(service.clone()))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    let mut client =
        pplx_space_spaced_guest_protocol::proto::bridge_guest::process_client::ProcessClient::connect(
            format!("http://{address}"),
        )
        .await
        .unwrap();
    let mut stream = client
        .start(StartRequest {
            command: vec!["/bin/cat".into()],
            stdin: true,
            idempotency_key: Some("admitted-input".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let started = stream.message().await.unwrap().unwrap();
    assert_eq!(
        slots.available_permits(),
        process_limits::PAYLOAD_SLOTS,
        "a live Start stream must release its request payload admission"
    );
    let record = service
        .inner
        .processes
        .get_running(&ProcessId::parse(started.process_id.clone()).unwrap())
        .unwrap();
    let ProcessInput::Pipe(stdin) = &record.input else {
        panic!("pipe stdin")
    };
    let held = stdin.lock().await;
    let mut writes = Vec::new();
    for index in 0..process_limits::PAYLOAD_SLOTS {
        let mut client = client.clone();
        let native = service.clone();
        let process_id = started.process_id.clone();
        writes.push(tokio::spawn(async move {
            let request = SendInputRequest {
                process_id,
                data: b"a".to_vec(),
            };
            if index % 2 == 0 {
                Process::send_input(&native, Request::new(request)).await
            } else {
                client.send_input(request).await
            }
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let rejected = client
        .send_input(SendInputRequest {
            process_id: started.process_id.clone(),
            data: b"rejected".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::ResourceExhausted);
    drop(held);
    for write in writes {
        write.await.unwrap().unwrap();
    }
    client
        .close_stdin(CloseStdinRequest {
            process_id: started.process_id,
        })
        .await
        .unwrap();
    let mut output = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = stream.message().await.unwrap() {
            if let Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stdout(
                    bytes,
                ),
            ) = event.event
            {
                output.extend(bytes.data);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(output, b"aaaaaaaa", "rejected input must not execute");
    assert_eq!(slots.available_permits(), process_limits::PAYLOAD_SLOTS);
    shutdown.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn losing_start_response_preserves_output_supervision_and_idempotency() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event;

    for (completed, evicted) in [(false, false), (true, false), (false, true), (true, true)] {
        let service = cancellation_service();
        let request = StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                if evicted {
                    format!(
                        "printf stdout; printf stderr >&2; head -c {} /dev/zero; cat >/dev/null",
                        PROCESS_OUTPUT_RETENTION_BYTES + 65536
                    )
                } else {
                    "printf stdout; printf stderr >&2; cat >/dev/null".into()
                },
            ],
            stdin: true,
            idempotency_key: Some("lost-start-response".into()),
            ..Default::default()
        };
        let response = Process::start(&service, Request::new(request.clone()))
            .await
            .expect("process starts");
        // The transport can lose the response before delivering even its first
        // event. Output recovery cannot depend on consuming the original stream.
        drop(response);
        let record = service
            .inner
            .processes
            .records
            .iter()
            .next()
            .and_then(|entry| entry.value().record().cloned())
            .expect("spawned process is registered");
        if completed {
            record.input.close().await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut changed = record.journal.subscribe();
            while record.journal.tail_cursor() < 2
                || (completed && !record.journal.is_completed())
                || (evicted && !matches!(record.journal.read(0), ProcessJournalRead::Lagged { .. }))
            {
                changed.changed().await.unwrap();
            }
        })
        .await
        .expect("output must precede the retry");
        assert_eq!(record.journal.is_completed(), completed);

        let replay = tokio::time::timeout(
            Duration::from_secs(1),
            Process::start(&service, Request::new(request)),
        )
        .await
        .expect("same-key retry waited for an abandoned reservation")
        .expect("same-key retry replays the registered process");
        record.input.close().await.unwrap();
        let mut replay = replay.into_inner();
        let started = replay.next().await.unwrap().unwrap();
        assert_eq!(started.process_id, record.info.process_id);
        assert_eq!(started.pid, Some(record.info.pid));
        assert!(matches!(started.event, Some(Event::Started(_))));
        let (stdout, stderr, exited, lagged) =
            tokio::time::timeout(Duration::from_secs(2), async {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                let mut exited = false;
                let mut lagged = false;
                while let Some(event) = replay.next().await {
                    let event = match event {
                        Ok(event) => event,
                        Err(error) => {
                            assert!(evicted);
                            assert_eq!(error.code(), tonic::Code::ResourceExhausted);
                            assert_eq!(
                                error.message(),
                                pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED_MESSAGE
                            );
                            lagged = true;
                            assert!(replay.next().await.is_none());
                            break;
                        }
                    };
                    match event.event {
                        Some(Event::Stdout(output)) => stdout.extend(output.data),
                        Some(Event::Stderr(output)) => stderr.extend(output.data),
                        Some(Event::Exit(_)) => exited = true,
                        other => panic!("unexpected Start replay event: {other:?}"),
                    }
                }
                (stdout, stderr, exited, lagged)
            })
            .await
            .expect("cancelled start left the spawned child unsupervised");
        assert_eq!(stdout, if evicted { b"".as_slice() } else { b"stdout" });
        assert_eq!(stderr, if evicted { b"".as_slice() } else { b"stderr" });
        assert_eq!(exited, !evicted);
        assert_eq!(lagged, evicted);
        // Losing the stream must not abandon supervision of a still-running child.
        let mut changed = record.journal.subscribe();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !record.journal.is_completed() {
                changed.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(service.inner.processes.records.len(), 1);
        let process_id = ProcessId::parse(record.info.process_id.clone()).unwrap();
        drop(service.inner.processes.records.remove(&process_id));
        assert_eq!(
            service.inner.processes.record_slots.available_permits(),
            PROCESS_RECORD_LIMIT - 1,
            "a handler retaining the evicted record must retain its admission"
        );
        drop(record);
        assert_eq!(
            service.inner.processes.record_slots.available_permits(),
            PROCESS_RECORD_LIMIT
        );
    }
}

#[tokio::test]
async fn pending_process_stream_stops_when_receiver_closes() {
    let journal = Arc::new(ProcessEventJournal::new(output_slots()));
    let mut stream = process_event_stream_with_prefix(
        Vec::new(),
        Some(ProcessJournalCursor {
            journal: Arc::downgrade(&journal),
            changed: journal.subscribe(),
            next_sequence: 0,
            primary: None,
        }),
    );

    assert!(futures_util::poll!(stream.next()).is_pending());
    assert_eq!(journal.changed.receiver_count(), 1);
    drop(stream);
    assert_eq!(journal.changed.receiver_count(), 0);
}

#[tokio::test]
async fn failed_start_retains_admission_until_the_child_is_reaped() {
    for already_exited in [false, true] {
        let registry = ProcessRegistry::default();
        let reservation = registry.reserve().expect("process admission");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            if already_exited {
                "exit 0"
            } else {
                "exec sleep 30"
            },
        ]);
        command.kill_on_drop(true);
        command.process_group(0);
        let mut child = spawn_child(&mut command).expect("child spawned");
        let pid = child.id().expect("spawned pid");
        if already_exited {
            child.wait().await.expect("child exits");
            assert!(child.id().is_none());
        }
        terminate_and_reap_failed_start(child, reservation);
        assert_eq!(
            registry.record_slots.available_permits(),
            PROCESS_RECORD_LIMIT - 1
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while registry.record_slots.available_permits() != PROCESS_RECORD_LIMIT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed child was not reaped");
        // No waitable child remains once cleanup releases the admission permit.
        let result = nix::sys::wait::waitpid(
            nix::unistd::Pid::from_raw(pid.try_into().unwrap()),
            Some(nix::sys::wait::WaitPidFlag::WNOHANG),
        );
        assert_eq!(result, Err(nix::errno::Errno::ECHILD));
    }
}

#[test]
fn process_registry_bounds_retained_records() {
    let registry = ProcessRegistry::default();
    let mut reservations = Vec::with_capacity(PROCESS_RECORD_LIMIT);
    for _ in 0..PROCESS_RECORD_LIMIT {
        reservations.push(registry.reserve().expect("record slot"));
    }

    let error = match registry.reserve() {
        Ok(_) => panic!("registry must reject overflow"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    // Nothing started: the caller is told it may retry.
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));

    reservations.pop();
    registry
        .reserve()
        .expect("released record slot is reusable");
}

#[tokio::test]
async fn process_journal_reports_lag_when_local_ring_evicts_output() {
    let process_id = ProcessId::new();
    for (chunk_bytes, count) in [(8192, 600), (1, 100_000)] {
        let journal = Arc::new(ProcessEventJournal::new(output_slots()));
        let mut slow = process_event_stream_with_prefix(
            Vec::new(),
            Some(ProcessJournalCursor {
                journal: Arc::downgrade(&journal),
                changed: journal.subscribe(),
                next_sequence: 0,
                primary: None,
            }),
        );
        for _ in 0..count {
            journal.append_output(process_stdout_event(
                &process_id,
                Some(1),
                vec![b'a'; chunk_bytes],
            ));
        }
        assert!(
            matches!(journal.read(0), ProcessJournalRead::Lagged { .. }),
            "retention must charge tiny event overhead as well as payloads"
        );
        assert!(matches!(
            journal.read(count - 1),
            ProcessJournalRead::Event(_)
        ));
        assert_eq!(
            slow.next().await.unwrap().unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(slow.next().await.is_none());
        assert_eq!(journal.changed.receiver_count(), 0);
    }
}

#[tokio::test]
async fn attached_output_backpressures_and_disconnect_releases_the_producer() {
    // Exercise both the per-process limit and exhaustion of the shared pool.
    for shared_bytes in [0, PROCESS_OUTPUT_SHARED_BYTES] {
        let journal = Arc::new(ProcessEventJournal::new(Arc::new(Semaphore::new(
            shared_bytes,
        ))));
        let process_id = ProcessId::new();
        let cursor = journal.cursor(Some(0), false);
        let event = process_stdout_event(&process_id, Some(1), vec![b'a'; 8192]);
        while journal.try_append_output(event.clone()).is_none() {}
        let tail = journal.tail_cursor();
        let mut stream = process_event_stream_with_prefix(Vec::new(), Some(cursor));
        let pending = journal.publish_output(event.clone());
        tokio::pin!(pending);
        assert!(futures_util::poll!(&mut pending).is_pending());
        assert!(matches!(journal.read(0), ProcessJournalRead::Event(_)));
        assert_eq!(stream.next().await.unwrap().unwrap(), event);
        pending.await;
        assert_eq!(journal.tail_cursor(), tail + 1);

        let pending = journal.publish_output(event);
        tokio::pin!(pending);
        assert!(futures_util::poll!(&mut pending).is_pending());
        drop(stream);
        pending.await;
        assert_eq!(journal.tail_cursor(), tail + 2);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn slow_output_delivery_preserves_descendant_with_closed_output() {
    use futures_util::FutureExt;
    let directory = tempfile::tempdir().unwrap();
    let pid_file = directory.path().join("descendant");
    let service = cancellation_service();
    let mut stream = Process::start(&service, Request::new(StartRequest {
        command: vec!["/bin/sh".into(), "-c".into(),
            "read ready; sleep 30 </dev/null >/dev/null 2>&1 & echo $! > \"$1\"; printf '%8192s' tail".into(),
            "test".into(), pid_file.display().to_string()],
        stdin: true,
        idempotency_key: Some("slow-closed-output".into()),
        ..Default::default()
    })).await.unwrap().into_inner();
    let started = stream.next().await.unwrap().unwrap();
    let id = ProcessId::parse(started.process_id).unwrap();
    let record = service.inner.processes.get_running(&id).unwrap();
    let (_, leader_exit) = ProcessSignalHandle::open(record.info.pid).unwrap();
    let event = process_stdout_event(&id, Some(record.info.pid), vec![b'x'; 8192]);
    let mut expected_bytes = 8192;
    while record.journal.try_append_output(event.clone()).is_none() {
        expected_bytes += 8192;
    }
    record.write_stdin(b"go\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), leader_exit.exited())
        .await
        .unwrap()
        .unwrap();
    let descendant: u32 = tokio::fs::read_to_string(pid_file)
        .await
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let (descendant_handle, descendant_exit) = ProcessSignalHandle::open(descendant).unwrap();
    assert!(record.journal.try_append_output(event).is_some());
    assert!(!record.journal.is_completed());
    // Observe the real descendant beyond the automatic cleanup deadline.
    // A closed output pipe must not make its continued life depend on delivery.
    let killed = tokio::time::timeout(PROCESS_OUTPUT_DRAIN_TIMEOUT * 2, descendant_exit.exited())
        .await
        .is_ok();
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = 0;
        while let Some(event) = stream.next().await {
            match event.unwrap().event.unwrap() {
                pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stdout(output) => bytes += output.data.len(),
                pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Exit(_) => break,
                _ => {},
            }
        }
        bytes
    }).await;
    // Own fixture cleanup even when the regression fails.
    if descendant_exit.exited().now_or_never().is_none() {
        descendant_handle.signal(ProcessSignal::sigkill()).unwrap();
    }
    descendant_exit.exited().await.unwrap();
    service.inner.shutdown.cancel();
    service.inner.process_tasks.close();
    service.inner.process_tasks.wait().await;
    assert!(
        !killed,
        "slow delivery must not kill a descendant that closed its output"
    );
    assert_eq!(
        events.unwrap(),
        expected_bytes,
        "closed-pipe output must be delivered completely"
    );
}

#[tokio::test(start_paused = true)]
async fn fallback_drain_bounds_inherited_writer_despite_consuming_reader() {
    let (reader, _writer) = tokio::net::UnixStream::pair().unwrap();
    let id = ProcessId::new();
    let pid = std::process::id();
    let journal = Arc::new(ProcessEventJournal::new(output_slots()));
    let mut stream =
        process_event_stream_with_prefix(Vec::new(), Some(journal.cursor(Some(0), false)));
    let event = process_stdout_event(&id, Some(pid), vec![0; 8192]);
    while journal.try_append_output(event.clone()).is_none() {}
    let (blocked, mut pending) = tokio::sync::mpsc::channel(1);
    let publisher = Arc::clone(&journal);
    let task = tokio::spawn(async move {
        loop {
            let publish = publisher.publish_output(event.clone());
            tokio::pin!(publish);
            if futures_util::poll!(&mut publish).is_pending() {
                // The notification certifies actual publication backpressure,
                // including after the consumer advances and publication resumes.
                blocked.send(()).await.unwrap();
                publish.await;
            }
        }
    });
    let mut tasks = VecDeque::from([ProcessOutputTask {
        task,
        source: reader.into_std().unwrap().into(),
    }]);
    pending.recv().await.unwrap();
    let mut completed = false;
    let mut consumed = 0;
    {
        // This is the production final drain used after child.wait on non-Linux.
        // Keep a real writer open while the test-owned task publishes output.
        let drain = drain_process_output_tasks(&mut tasks, &id, pid, true);
        tokio::pin!(drain);
        for _ in 0..3 {
            let tail = journal.tail_cursor();
            tokio::select! {
                biased;
                _ = &mut drain => {
                    completed = true;
                    break;
                }
                _ = async {
                    stream.next().await.unwrap().unwrap();
                    pending.recv().await.unwrap();
                } => {
                    consumed += 1;
                    assert_eq!(journal.tail_cursor(), tail + 1);
                }
            }
            if tokio::time::timeout(PROCESS_OUTPUT_DRAIN_TIMEOUT, &mut drain)
                .await
                .is_ok()
            {
                completed = true;
                break;
            }
        }
    }
    for output in &tasks {
        output.task.abort();
    }
    for output in tasks {
        let _ = output.task.await;
    }
    assert!(consumed > 0, "the reader must advance before cleanup");
    assert!(
        completed,
        "an inherited writer must not renew the drain deadline through backpressure"
    );
}

#[tokio::test]
async fn kill_reaped_process_releases_stalled_output_and_capacity() {
    let service = cancellation_service();
    let mut stream = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "read line; printf '%8192s' x".into(),
            ],
            idempotency_key: Some("kill-reaped".into()),
            stdin: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let started = stream.next().await.unwrap().unwrap();
    let process_id = ProcessId::parse(started.process_id.clone()).unwrap();
    let record = service.inner.processes.get_running(&process_id).unwrap();
    // Fill the attached reader's journal before letting the child write and exit.
    let event = process_stdout_event(&process_id, Some(record.info.pid), vec![0; 8192]);
    while record.journal.try_append_output(event.clone()).is_none() {}
    record.write_stdin(b"exit\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let gone =
                nix::sys::signal::kill(nix::unistd::Pid::from_raw(record.info.pid as i32), None)
                    == Err(nix::errno::Errno::ESRCH);
            if gone {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("leader must be reaped while output remains blocked");
    assert!(record.journal.try_append_output(event).is_some());
    let missing = Process::send_signal(
        &service,
        Request::new(SendSignalRequest {
            process_id: started.process_id.clone(),
            signal: libc::SIGTERM,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
    assert!(!record.journal.is_completed());
    let result = Process::send_signal(
        &service,
        Request::new(SendSignalRequest {
            process_id: started.process_id,
            signal: libc::SIGKILL,
        }),
    )
    .await;
    let completed = tokio::time::timeout(Duration::from_secs(2), async {
        while !service.inner.processes.list(false, None).is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Clean up even on the unfixed implementation, without releasing the reader first.
    service.inner.shutdown.cancel();
    service.inner.process_tasks.close();
    service.inner.process_tasks.wait().await;
    result.expect("kill of an exited leader must release output");
    completed.expect("stalled reader must not prevent process cleanup");
    drop(record);
    service.inner.processes.records.remove(&process_id);
    assert_eq!(
        service.inner.processes.record_slots.available_permits(),
        PROCESS_RECORD_LIMIT
    );
    drop(stream);
}

#[test]
fn process_journal_drops_oversized_chunk_without_panicking() {
    let journal = ProcessEventJournal::new(output_slots());
    let process_id = ProcessId::new();

    journal.append_output(process_stdout_event(
        &process_id,
        Some(1),
        vec![0; PROCESS_OUTPUT_RETENTION_BYTES + 1],
    ));

    assert!(matches!(journal.read(0), ProcessJournalRead::Lagged { .. }));
}

#[test]
fn output_pressure_is_isolated_to_one_process() {
    let shared_output_slots = Arc::new(Semaphore::new(0));
    let noisy = ProcessEventJournal::new(Arc::clone(&shared_output_slots));
    let quiet = ProcessEventJournal::new(shared_output_slots);
    let noisy_id = ProcessId::new();
    let quiet_id = ProcessId::new();
    quiet.append_output(process_stdout_event(&quiet_id, Some(2), b"quiet".to_vec()));

    noisy.append_output(process_stdout_event(
        &noisy_id,
        Some(1),
        vec![0; PROCESS_OUTPUT_RETENTION_BYTES],
    ));
    noisy.append_output(process_stdout_event(&noisy_id, Some(1), vec![1]));

    assert!(matches!(noisy.read(0), ProcessJournalRead::Lagged { .. }));
    assert!(matches!(quiet.read(0), ProcessJournalRead::Event(_)));
}

#[test]
fn process_journal_releases_shared_burst_capacity_on_drop() {
    let shared_output_slots = Arc::new(Semaphore::new(1));
    let journal = ProcessEventJournal::new(Arc::clone(&shared_output_slots));
    let process_id = ProcessId::new();
    journal.append_output(process_stdout_event(
        &process_id,
        Some(1),
        vec![
            0;
            PROCESS_OUTPUT_GUARANTEED_BYTES + 1
                - std::mem::size_of::<SequencedProcessEvent>()
                - process_id.as_str().len()
        ],
    ));
    assert_eq!(shared_output_slots.available_permits(), 0);

    drop(journal);

    assert_eq!(shared_output_slots.available_permits(), 1);
}

#[tokio::test]
async fn completed_cursor_keeps_terminal_event_without_retaining_journal() {
    let journal = Arc::new(ProcessEventJournal::new(output_slots()));
    let process_id = ProcessId::new();
    let exit = process_stdout_event(&process_id, Some(1), b"terminal".to_vec());
    let cursor = ProcessJournalCursor {
        journal: Arc::downgrade(&journal),
        changed: journal.subscribe(),
        next_sequence: 0,
        primary: None,
    };
    journal.complete(exit.clone());
    drop(journal);

    let events = process_event_stream_with_prefix(Vec::new(), Some(cursor))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_ref().expect("terminal event"), &exit);
}

#[tokio::test]
async fn evicted_cursor_reports_lag_before_terminal_event() {
    let journal = Arc::new(ProcessEventJournal::new(output_slots()));
    let process_id = ProcessId::new();
    journal.append_output(process_stdout_event(
        &process_id,
        Some(1),
        b"unread".to_vec(),
    ));
    let exit = process_stdout_event(&process_id, Some(1), b"terminal".to_vec());
    let cursor = ProcessJournalCursor {
        journal: Arc::downgrade(&journal),
        changed: journal.subscribe(),
        next_sequence: 0,
        primary: None,
    };
    journal.complete(exit);
    drop(journal);

    let events = process_event_stream_with_prefix(Vec::new(), Some(cursor))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0]
            .as_ref()
            .expect_err("lagged replay must fail")
            .code(),
        tonic::Code::ResourceExhausted
    );
}

/// A cursor past the written output fails at once, whether the process is
/// still running or has exited, rather than waiting for output to reach it and
/// skipping everything written before. The end of the output stays valid.
#[tokio::test]
async fn cursor_past_the_output_fails_instead_of_waiting() {
    for completed in [false, true] {
        let journal = Arc::new(ProcessEventJournal::new(output_slots()));
        let process_id = ProcessId::new();
        journal.append_output(process_stdout_event(
            &process_id,
            Some(1),
            b"written".to_vec(),
        ));
        let exit = process_stdout_event(&process_id, Some(1), b"terminal".to_vec());
        if completed {
            journal.complete(exit.clone());
        }

        let mut ahead =
            process_event_stream_with_prefix(Vec::new(), Some(journal.cursor(Some(5), false)));
        let error = tokio::time::timeout(Duration::from_secs(1), ahead.next())
            .await
            .expect("a cursor past the output must not wait")
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::OutOfRange,
            "completed: {completed}"
        );
        assert_eq!(
            error
                .metadata()
                .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
                .and_then(|value| value.to_str().ok()),
            Some(pplx_space_common_core::spaced::PROCESS_OUTPUT_CURSOR_AHEAD)
        );
        assert!(ahead.next().await.is_none());

        let mut at_end =
            process_event_stream_with_prefix(Vec::new(), Some(journal.cursor(Some(1), false)));
        let next = tokio::time::timeout(Duration::from_millis(100), at_end.next()).await;
        if completed {
            assert_eq!(next.unwrap().unwrap().unwrap(), exit);
        } else {
            assert!(next.is_err(), "the end of a running process's output waits");
        }
    }
}

/// A cursor before the oldest retained output fails with a status that says
/// so and where that output starts, so the caller can continue from there
/// instead of jumping to the live tail.
#[tokio::test]
async fn lagged_cursor_reports_the_oldest_retained_output() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event;

    let journal = Arc::new(ProcessEventJournal::new(Arc::new(Semaphore::new(0))));
    let process_id = ProcessId::new();
    journal.append_output(process_stdout_event(
        &process_id,
        Some(1),
        vec![0; PROCESS_OUTPUT_RETENTION_BYTES],
    ));
    journal.append_output(process_stdout_event(&process_id, Some(1), b"kept".to_vec()));

    let mut lagged =
        process_event_stream_with_prefix(Vec::new(), Some(journal.cursor(Some(0), false)));
    let error = lagged.next().await.unwrap().unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    let metadata = |key| {
        error
            .metadata()
            .get(key)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    assert_eq!(
        metadata(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY).as_deref(),
        Some(pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED)
    );
    let first_sequence: u64 =
        metadata(pplx_space_common_core::spaced::PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY)
            .expect("the oldest retained output")
            .parse()
            .unwrap();
    assert_eq!(first_sequence, 1);
    assert!(lagged.next().await.is_none());

    let mut retained = process_event_stream_with_prefix(
        Vec::new(),
        Some(journal.cursor(Some(first_sequence), false)),
    );
    let event = retained.next().await.unwrap().unwrap();
    assert!(matches!(event.event, Some(Event::Stdout(output)) if output.data == b"kept"));
}

/// A lagged cursor that asks for the oldest retained output starts there,
/// and holds it: the start is chosen under the lock that makes the reader
/// primary, so output written before it reads anything waits for it instead
/// of evicting that output, and the reader cannot lag again.
#[tokio::test]
async fn lagged_cursor_from_oldest_retained_starts_there_and_holds_it() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event;

    let journal = Arc::new(ProcessEventJournal::new(Arc::new(Semaphore::new(0))));
    let process_id = ProcessId::new();
    journal.append_output(process_stdout_event(
        &process_id,
        Some(1),
        vec![0; PROCESS_OUTPUT_RETENTION_BYTES],
    ));
    journal.append_output(process_stdout_event(&process_id, Some(1), b"kept".to_vec()));

    let cursor = journal.cursor(Some(0), true);
    assert_eq!(
        cursor.next_sequence, 1,
        "starts at the oldest retained output"
    );
    let more = process_stdout_event(&process_id, Some(1), vec![0; 8192]);
    let waited = (0..2 * PROCESS_OUTPUT_RETENTION_BYTES / 8192)
        .any(|_| journal.try_append_output(more.clone()).is_some());
    assert!(
        waited,
        "later output waits for the reader instead of evicting"
    );

    let mut retained = process_event_stream_with_prefix(Vec::new(), Some(cursor));
    let event = retained.next().await.unwrap().unwrap();
    assert!(matches!(event.event, Some(Event::Stdout(output)) if output.data == b"kept"));
}

#[tokio::test]
async fn login_tty_pty_supports_output_input_and_resize() {
    let prepared = prepare_pty(Some(PtySize { rows: 24, cols: 80 })).expect("open test pty");
    let PreparedPty {
        mut reader,
        mut writer,
        resize_fd,
        control_fd,
    } = prepared;

    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("printf 'ready\n'; IFS= read -r line; stty size; printf 'input:%s\n' \"$line\"");

    let control_fd_raw = control_fd.as_raw_fd();
    // SAFETY: the child inherits this live slave, and the callback only calls login_tty.
    unsafe {
        command.pre_exec(move || configure_child_pty(control_fd_raw));
    }
    let mut child = spawn_child(&mut command).expect("spawn pty child");
    drop(control_fd);

    set_pty_size(
        &resize_fd,
        PtySize {
            rows: 33,
            cols: 101,
        },
    )
    .expect("resize pty");
    writer
        .write_all(b"hello-login-tty\n")
        .await
        .expect("write pty input");
    writer.flush().await.expect("flush pty input");
    drop(writer);

    let output = tokio::time::timeout(Duration::from_secs(5), async {
        let mut output = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) => break,
                Ok(read) => output.extend_from_slice(&chunk[..read]),
                Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                Err(error) => return Err(error),
            }
        }
        Ok::<_, io::Error>(output)
    })
    .await
    .expect("pty output timed out")
    .expect("read pty output");
    let output = String::from_utf8(output).expect("pty output is utf-8");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("child wait timed out")
        .expect("wait child");

    assert!(
        status.success(),
        "child failed with {status:?}; output={output:?}"
    );
    assert!(output.contains("ready"), "missing ready output: {output:?}");
    assert!(
        output.contains("33 101"),
        "child did not observe resized pty: {output:?}"
    );
    assert!(
        output.contains("input:hello-login-tty"),
        "child did not receive pty input: {output:?}"
    );
}

#[tokio::test]
async fn cancelled_account_lookup_retains_admission_until_nss_returns() {
    let slots = Arc::new(Semaphore::new(
        pplx_space_spaced_guest_protocol::process_limits::START_PAYLOAD_SLOTS as usize,
    ));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lookup = tokio::spawn(resolve_process_account(slots.clone(), None, move || {
        let _ = started_tx.send(());
        release_rx.recv().unwrap();
        Err(Status::unavailable("lookup fixture finished"))
    }));
    started_rx.await.unwrap();
    lookup.abort();
    let _ = lookup.await;
    let retry = resolve_process_account(slots.clone(), None, || {
        Err(Status::unavailable("lookup ran"))
    })
    .await;
    release_tx.send(()).unwrap();
    let retry = retry.unwrap_err();
    assert_eq!(retry.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::is_refused_before_start(&retry));
    drop(
        tokio::time::timeout(Duration::from_secs(1), slots.acquire())
            .await
            .unwrap()
            .unwrap(),
    );
}

#[cfg(target_os = "linux")]
fn open_pidfds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("list own descriptors")
        .filter_map(Result::ok)
        .filter(|entry| {
            std::fs::read_link(entry.path())
                .is_ok_and(|target| target.to_string_lossy().contains("pidfd"))
        })
        .count()
}

// The supervisor must release the exit-observer pidfd once the process exits,
// not after the record retention window: a descriptor held per start for that
// window exhausts the daemon's limit at sustained start rates.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn exited_process_releases_its_observer_pidfd_before_record_retention_ends() {
    let service = cancellation_service();
    let baseline = open_pidfds();
    let response = Process::start(&service, cancellation_start_request())
        .await
        .expect("process starts");
    let mut events = response.into_inner();
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    // The retained record keeps one pidfd for signalling; the observer must go.
    tokio::time::timeout(Duration::from_secs(2), async {
        while open_pidfds() > baseline + 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("observer pidfd must be released once the process exits");
}

// Only Linux spawns without a lock: elsewhere every spawn serializes on
// `PTY_INHERITANCE_LOCK`, so a stalled spawn holds up all of them by design.
/// A pipe with a full buffer that stands in for `workloads/cgroup.procs`. A
/// child writing its placement to it blocks between fork and exec, which holds
/// the parent's `spawn` like a spawn stalled by memory or I/O pressure. The
/// stall ends on `release`, or after 5s so a regression fails instead of
/// hanging.
#[cfg(target_os = "linux")]
struct StalledPlacement {
    release: std::sync::mpsc::Sender<()>,
}

#[cfg(target_os = "linux")]
impl StalledPlacement {
    fn release(self) {
        let _ = self.release.send(());
    }
}

#[cfg(target_os = "linux")]
fn stalled_placement_service() -> (ExecutionService, StalledPlacement) {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use std::io::Write;
    let (mut reader, mut writer) = std::io::pipe().expect("placement pipe");
    let set_nonblocking = |fd: std::os::fd::BorrowedFd<'_>, nonblocking: bool| {
        let flags =
            OFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFL).expect("pipe status flags"));
        let flags = if nonblocking {
            flags | OFlag::O_NONBLOCK
        } else {
            flags - OFlag::O_NONBLOCK
        };
        fcntl(fd, FcntlArg::F_SETFL(flags)).expect("set pipe status flags");
    };
    set_nonblocking(writer.as_fd(), true);
    loop {
        match writer.write(&[0; 4096]) {
            // Only a full buffer ends the fill, so a partial write is fine.
            Ok(_written) => {}
            Err(error) => {
                assert_eq!(
                    error.kind(),
                    io::ErrorKind::WouldBlock,
                    "fill placement pipe: {error}"
                );
                break;
            }
        }
    }
    set_nonblocking(writer.as_fd(), false);
    let (release, released) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = released.recv_timeout(Duration::from_secs(5));
        // Keep reading until every writer closes, so no placement fails.
        let _ = io::copy(&mut reader, &mut io::sink());
    });
    let mut service = cancellation_service();
    Arc::get_mut(&mut service.inner)
        .expect("service is not shared yet")
        .guest_cgroups = Some(Arc::new(crate::ExecutionCgroups::new(
        std::fs::File::from(OwnedFd::from(
            writer.try_clone().expect("clone placement pipe"),
        )),
        std::fs::File::from(OwnedFd::from(writer)),
    )));
    (service, StalledPlacement { release })
}

#[cfg(target_os = "linux")]
async fn wait_for_start_owner(service: &ExecutionService) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while service.inner.process_tasks.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("start reaches its owner task");
}

#[cfg(target_os = "linux")]
async fn shut_down(service: &ExecutionService) {
    service.close();
    service.inner.process_tasks.wait().await;
}

// A spawn can stall for seconds under memory or I/O pressure. It must hold up
// only its own start: another start on the same service is admitted and gets
// its owner while the first is still inside `spawn`, and the runtime that
// serves every other request keeps going.
#[cfg(target_os = "linux")]
#[test]
fn a_stalled_spawn_does_not_hold_up_other_starts() {
    // A single runtime thread, so a spawn blocking it would block everything.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (service, placement, stalled) = runtime.block_on(async {
        let (service, placement) = stalled_placement_service();
        let stalled = tokio::spawn({
            let service = service.clone();
            async move {
                Process::start(&service, cancellation_start_request())
                    .await
                    .map(drop)
            }
        });
        wait_for_start_owner(&service).await;
        // Let the owner enter its spawn.
        tokio::time::sleep(Duration::from_millis(100)).await;
        (service, placement, stalled)
    });

    // The second start runs on its own thread, so a start gate held across
    // the first spawn blocks only it and the wait below reports it.
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let other = std::thread::spawn({
        let service = service.clone();
        move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let start = tokio::spawn({
                    let service = service.clone();
                    async move {
                        Process::start(
                            &service,
                            Request::new(StartRequest {
                                command: vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
                                idempotency_key: Some("second-start".into()),
                                ..Default::default()
                            }),
                        )
                        .await
                        .map(drop)
                    }
                });
                while service.inner.process_tasks.len() < 2 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let _ = admitted_tx.send(());
                start.await.unwrap()
            })
        }
    });
    let admitted = admitted_rx.recv_timeout(Duration::from_secs(2));
    let stalled_was_pending = !stalled.is_finished();
    placement.release();
    admitted.expect("a second start is admitted while the first spawn is stalled");
    assert!(stalled_was_pending, "the first start was not stalled");
    other
        .join()
        .unwrap()
        .expect("the second start completes once spawns resume");
    runtime.block_on(async {
        stalled
            .await
            .unwrap()
            .expect("the stalled start completes once released");
        shut_down(&service).await;
    });
}

// A reconnect for an assigned id whose start is still inside its spawn must
// not be told the process is attachable: nothing can be connected to yet. The
// same key waits for the original start and replays it; another key is told
// to retry rather than to reconnect.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_reconnect_before_its_process_is_published_waits_or_retries() {
    let (service, placement) = stalled_placement_service();
    let process_id = pplx_space_common_core::types::ProcessId::new();
    let request = |key: &str| {
        Request::new(StartRequest {
            command: vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
            idempotency_key: Some(key.into()),
            process_id: Some(process_id.as_str().to_string()),
            ..Default::default()
        })
    };
    let first = tokio::spawn({
        let service = service.clone();
        let request = request("first-start");
        async move { Process::start(&service, request).await.map(drop) }
    });
    wait_for_start_owner(&service).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let other_key = Process::start(&service, request("another-start"))
        .await
        .map(drop)
        .expect_err("the id is held by a start that has not published");
    assert_eq!(other_key.code(), tonic::Code::Unavailable, "{other_key:?}");

    let same_key = tokio::spawn({
        let service = service.clone();
        let request = request("first-start");
        async move { Process::start(&service, request).await.map(drop) }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !same_key.is_finished(),
        "a same-key retry waits for the original start"
    );
    placement.release();
    first.await.unwrap().expect("the original start completes");
    same_key
        .await
        .unwrap()
        .expect("the same-key retry replays the started process");
    shut_down(&service).await;
}

// A guest that cannot create a spawn thread runs nothing, so the refused
// start must leave its key free: once threads are available again, a retry
// with the same key starts the command rather than replaying the refusal.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_start_refused_a_spawn_thread_can_be_retried_with_its_key() {
    let service = cancellation_service();
    REFUSE_SPAWN_THREADS.set(true);
    let refused = Process::start(&service, cancellation_start_request())
        .await
        .map(drop)
        .expect_err("no spawn thread, no start");
    REFUSE_SPAWN_THREADS.set(false);
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");

    let mut stream = Process::start(&service, cancellation_start_request())
        .await
        .expect("the same-key retry starts the command")
        .into_inner();
    let started = stream.next().await.unwrap().unwrap();
    assert!(!started.process_id.is_empty());
    shut_down(&service).await;
}

// From `begin`, the start belongs to its owner task. A request dropped while
// its spawn is in progress still yields one supervised process, and a same-key
// retry replays that process instead of starting another.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_start_dropped_during_its_spawn_still_runs_once() {
    let (service, placement) = stalled_placement_service();
    let mut start = Box::pin(Process::start(&service, cancellation_start_request()));
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.inner.process_tasks.is_empty() {
            assert!(futures_util::poll!(start.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("start reaches its owner task");
    drop(start);
    placement.release();

    let mut events = tokio::time::timeout(
        Duration::from_secs(5),
        Process::start(&service, cancellation_start_request()),
    )
    .await
    .expect("same-key retry waits for the owner")
    .expect("same-key retry replays the process")
    .into_inner();
    let started = events
        .next()
        .await
        .expect("replay has a started event")
        .expect("started event");
    let record_ids = service
        .inner
        .processes
        .records
        .iter()
        .map(|entry| entry.key().as_str().to_string())
        .collect::<Vec<_>>();
    assert_eq!(record_ids, [started.process_id]);
    while let Some(event) = events.next().await {
        event.expect("replayed stream remains successful");
    }
    shut_down(&service).await;
}

// Shutdown closes `process_tasks` under the start gate, so it must also wait
// for a start whose spawn is still in progress, then stop that process.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn shutdown_waits_for_a_start_stalled_in_its_spawn() {
    let (service, placement) = stalled_placement_service();
    let start = tokio::spawn({
        let service = service.clone();
        async move {
            Process::start(
                &service,
                Request::new(StartRequest {
                    command: vec!["/bin/sh".into(), "-c".into(), "exec sleep 30".into()],
                    idempotency_key: Some("shutdown-during-spawn".into()),
                    ..Default::default()
                }),
            )
            .await
            .map(drop)
        }
    });
    wait_for_start_owner(&service).await;
    service.close();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            service.inner.process_tasks.wait()
        )
        .await
        .is_err(),
        "the drain must wait for a start still in its spawn"
    );
    placement.release();
    tokio::time::timeout(Duration::from_secs(5), service.inner.process_tasks.wait())
        .await
        .expect("the drain completes once the spawn returns");
    start.await.unwrap().expect("the admitted start completes");
    let record = service
        .inner
        .processes
        .records
        .iter()
        .next()
        .and_then(|entry| entry.value().record().cloned())
        .expect("the spawned process is registered");
    assert!(
        record.journal.is_completed(),
        "shutdown stops the spawned process"
    );
}

// Starts spawn without a process-wide lock, so no descriptor may ever exist
// without CLOEXEC, or a concurrent spawn's child would inherit it.
#[test]
fn pty_descriptors_are_close_on_exec() {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    let pty = prepare_pty(None).expect("open pty");
    for fd in [pty.reader.as_fd(), pty.control_fd.as_fd()] {
        let flags = FdFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFD).unwrap());
        assert!(flags.contains(FdFlag::FD_CLOEXEC));
    }
}

// An exited process's task ends once its exit is recorded; the record's
// retention belongs to the registry's single reaper. A task kept alive per
// start for that window made spaced's memory grow with the start rate.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_exited_process_task_does_not_wait_out_its_record_retention() {
    let service = cancellation_service();
    let response = Process::start(&service, cancellation_start_request())
        .await
        .expect("process starts");
    let mut events = response.into_inner();
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while !service.inner.process_tasks.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the exited process's task ends without waiting out the retention");
    let registry = &service.inner.processes;
    assert_eq!(registry.records.len(), 1, "the record is still retained");
    assert!(registry.next_expiry().is_some());
    assert!(
        registry
            .reaper_running
            .load(std::sync::atomic::Ordering::Acquire),
        "the reaper owns the retention"
    );

    // Once the retention has passed, the reaper's pass removes the record and
    // has nothing left to wait for.
    for entry in registry.records.iter() {
        entry
            .record()
            .expect("published")
            .journal
            .backdate_completion(COMPLETED_PROCESS_RETENTION + Duration::from_secs(1));
    }
    registry.remove_expired();
    assert_eq!(registry.records.len(), 0);
    assert_eq!(registry.next_expiry(), None);
}

// A client that reattaches after its command ended still reads the exit, well
// past the start's idempotency window, until the completed record's retention
// ends. The process list reports it, with its exit, only when asked.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_completed_process_exit_stays_connectable_until_its_retention_ends() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        process_event::Event, process_exit::Outcome,
    };
    let service = cancellation_service();
    let process_id = "sbxps_2f1c9a4e-0000-7000-8000-000000000003";
    let response = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec!["/bin/sh".into(), "-c".into(), "exit 7".into()],
            idempotency_key: Some("exit-retention".into()),
            process_id: Some(process_id.into()),
            ..Default::default()
        }),
    )
    .await
    .expect("process starts");
    let mut events = response.into_inner();
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    let connect = || {
        Process::connect_process(
            &service,
            Request::new(ConnectProcessRequest {
                process_id: process_id.into(),
                timeout: None,
                output_cursor: None,
                resume_from_oldest_retained: false,
            }),
        )
    };
    let backdate = |by: Duration| {
        for entry in service.inner.processes.records.iter() {
            entry
                .record()
                .expect("published")
                .journal
                .backdate_completion(by);
        }
    };

    backdate(COMPLETED_PROCESS_OUTPUT_RETENTION + Duration::from_secs(1));
    service.inner.processes.remove_expired();
    let events = connect()
        .await
        .expect("the completed process is still retained")
        .into_inner()
        .collect::<Vec<_>>()
        .await;
    let exit = events
        .last()
        .and_then(|event| event.as_ref().ok())
        .and_then(|event| event.event.clone());
    let Some(Event::Exit(exit)) = exit else {
        panic!("reattach ends with the exit: {events:?}");
    };
    assert_eq!(
        exit.outcome,
        Some(Outcome::Exited(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessExited { exit_code: 7 }
        ))
    );
    assert!(
        service.inner.processes.list(false, None).is_empty(),
        "the default list reports running processes only"
    );
    assert!(
        service
            .inner
            .processes
            .list(true, Some("sbxps_2f1c9a4e-0000-7000-8000-000000000099"))
            .is_empty(),
        "a filtered list reports only the process asked for"
    );
    let listed = service.inner.processes.list(true, Some(process_id));
    let [listed] = listed.as_slice() else {
        panic!("the exited process is listed when asked: {listed:?}");
    };
    assert_eq!(listed.process_id, process_id);
    assert_eq!(
        listed.exit.as_ref().and_then(|exit| exit.outcome),
        Some(Outcome::Exited(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessExited { exit_code: 7 }
        ))
    );
    assert!(listed.ended_at.is_some(), "{listed:?}");

    backdate(COMPLETED_PROCESS_RETENTION);
    service.inner.processes.remove_expired();
    let status = connect()
        .await
        .err()
        .expect("the record is gone once its retention ends");
    assert_eq!(status.code(), tonic::Code::NotFound, "{status:?}");
    shut_down(&service).await;
}

// A finished process keeps its output only for its start's idempotency
// window; after that the reaper returns the shared output budget to running
// processes and the record keeps just its exit.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_completed_process_releases_its_output_after_the_idempotency_window() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event;
    let service = cancellation_service();
    let registry = &service.inner.processes;
    let process_id = "sbxps_2f1c9a4e-0000-7000-8000-000000000004";
    let idle_shared = registry.shared_output_slots.available_permits();
    let response = Process::start(
        &service,
        Request::new(StartRequest {
            // Past the per-record guaranteed share, so it borrows shared budget.
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "head -c 1048576 /dev/zero".into(),
            ],
            env: [("OUTPUT_RELEASE".into(), "kept".into())].into(),
            idempotency_key: Some("output-release".into()),
            process_id: Some(process_id.into()),
            ..Default::default()
        }),
    )
    .await
    .expect("process starts");
    let mut events = response.into_inner();
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    assert!(
        registry.shared_output_slots.available_permits() < idle_shared,
        "the finished process still holds its output for a retried start"
    );
    let listed_env = || {
        registry
            .list(true, Some(process_id))
            .pop()
            .expect("the exited process is listed")
            .env
    };
    assert_eq!(
        listed_env().get("OUTPUT_RELEASE").map(String::as_str),
        Some("kept")
    );

    for entry in registry.records.iter() {
        entry
            .record()
            .expect("published")
            .journal
            .backdate_completion(COMPLETED_PROCESS_OUTPUT_RETENTION + Duration::from_secs(1));
    }
    registry.remove_expired();
    assert_eq!(
        registry.shared_output_slots.available_permits(),
        idle_shared,
        "the shared output budget is released"
    );
    assert!(
        listed_env().is_empty(),
        "the environment goes with the output"
    );
    // The reaper's next work for it is the end of its retention.
    assert!(
        registry.next_expiry().is_some_and(
            |next| next > tokio::time::Instant::now() + COMPLETED_PROCESS_OUTPUT_RETENTION
        ),
        "{:?}",
        registry.next_expiry()
    );
    let events = Process::connect_process(
        &service,
        Request::new(ConnectProcessRequest {
            process_id: process_id.into(),
            timeout: None,
            output_cursor: None,
            resume_from_oldest_retained: false,
        }),
    )
    .await
    .expect("the exit is still retained")
    .into_inner()
    .collect::<Vec<_>>()
    .await;
    assert!(
        matches!(
            events.as_slice(),
            [Ok(started), Ok(exit)]
                if matches!(started.event, Some(Event::Started(_)))
                    && matches!(exit.event, Some(Event::Exit(_)))
        ),
        "a reattach without a cursor gets the start and the exit: {events:?}"
    );
    shut_down(&service).await;
}

// Each finished process's output is released on time, even while the reaper
// sleeps toward a later deadline. It used to wake only on its timer, so after
// releasing one record's output it slept until that record expired, and a
// command finishing meanwhile kept its output and shared budget until then.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_reaper_releases_a_later_exit_output_on_time() {
    let service = cancellation_service();
    let registry = &service.inner.processes;
    let idle_shared = registry.shared_output_slots.available_permits();
    let run = |key: &'static str, script: &'static str| {
        let service = &service;
        async move {
            let response = Process::start(
                service,
                Request::new(StartRequest {
                    command: vec!["/bin/sh".into(), "-c".into(), script.into()],
                    idempotency_key: Some(key.into()),
                    ..Default::default()
                }),
            )
            .await
            .expect("process starts");
            let mut events = response.into_inner();
            while let Some(event) = events.next().await {
                event.expect("process stream remains successful");
            }
        }
    };
    // Advances the reaper's clock past an output window and lets it work.
    let after_output_window = || async {
        tokio::time::pause();
        tokio::time::advance(COMPLETED_PROCESS_OUTPUT_RETENTION + Duration::from_secs(1)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    };

    run("reaper-first", "exit 0").await;
    after_output_window().await;
    assert!(
        registry.next_expiry().is_some_and(|next| {
            next > tokio::time::Instant::now() + COMPLETED_PROCESS_OUTPUT_RETENTION
        }),
        "the first output is released and the reaper sleeps toward its retention end"
    );
    tokio::time::resume();

    // Past the per-record guaranteed share, so it borrows shared budget.
    run("reaper-second", "head -c 1048576 /dev/zero").await;
    assert!(registry.shared_output_slots.available_permits() < idle_shared);
    after_output_window().await;
    let released = registry.shared_output_slots.available_permits();
    tokio::time::resume();
    assert_eq!(
        released, idle_shared,
        "the second output is released one window after its exit"
    );
    shut_down(&service).await;
}

// A write in flight when the process exits keeps the stdin pipe only until it
// returns; the release used to skip a held pipe and never come back to it, so
// a descendant read no EOF until the record went. A write that passed its
// running check just before the exit is answered as for an exited process.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_write_in_flight_at_exit_closes_stdin_when_it_returns() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::SendSignalRequest;
    let service = cancellation_service();
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("stdin-closed");
    let reading = directory.path().join("reading");
    let response = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                r#"exec 3<&0; (touch "$2"; cat <&3 >/dev/null; touch "$1") >/dev/null 2>&1 & exec sleep 30"#
                    .into(),
                "sh".into(),
                marker.to_str().unwrap().into(),
                reading.to_str().unwrap().into(),
            ],
            stdin: true,
            idempotency_key: Some("stdin-held-release".into()),
            ..Default::default()
        }),
    )
    .await
    .expect("process starts");
    let mut events = response.into_inner();
    let started = events
        .next()
        .await
        .expect("started")
        .expect("process stream remains successful");
    let record = service
        .inner
        .processes
        .get_running(&ProcessId::parse(started.process_id.clone()).unwrap())
        .expect("running");
    let ProcessInput::Pipe(stdin) = &record.input else {
        panic!("pipe stdin")
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !reading.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the descendant reads stdin");
    let held = stdin.lock().await;
    Process::send_signal(
        &service,
        Request::new(SendSignalRequest {
            process_id: started.process_id,
            signal: libc::SIGKILL,
        }),
    )
    .await
    .expect("the leader is killed");
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    assert!(record.journal.is_completed());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!marker.exists(), "the held write keeps the pipe open");

    drop(held);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the descendant reads EOF once the held write returns");
    let status = record
        .write_stdin(b"late")
        .await
        .expect_err("the process has exited");
    assert_eq!(status.code(), tonic::Code::NotFound, "{status:?}");
    drop(record);
    shut_down(&service).await;
}

// Once the process has exited its input descriptors close: a descendant still
// reading the stdin pipe sees EOF rather than blocking for the retention.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_exited_process_closes_its_stdin_for_descendants() {
    let service = cancellation_service();
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("stdin-closed");
    let response = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                // The background reader holds the stdin pipe, not the output.
                r#"exec 3<&0; (cat <&3 >/dev/null; touch "$1") >/dev/null 2>&1 & exit 0"#.into(),
                "sh".into(),
                marker.to_str().unwrap().into(),
            ],
            stdin: true,
            idempotency_key: Some("stdin-release".into()),
            ..Default::default()
        }),
    )
    .await
    .expect("process starts");
    let mut events = response.into_inner();
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the descendant reads EOF once the process has exited");
    shut_down(&service).await;
}

// The PTY's writer and resize descriptors close with the exit, so the master
// is gone and a descendant still writing to the terminal gets EIO. The
// descendant leaves the process group, so it outlives the output drain.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_exited_pty_process_closes_its_terminal() {
    let service = cancellation_service();
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("terminal-write");
    let response = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                // The leader exits once the descendant has left its group.
                concat!(
                    "setsid sh -c 'touch \"$2\"; for _ in $(seq 100); do ",
                    "echo x >&3 2>/dev/null || { echo failed > \"$1\"; exit; }; sleep 0.1; ",
                    "done; echo written > \"$1\"' sh \"$1\" \"$2\" 3>&1 </dev/null >/dev/null 2>&1 & ",
                    "while [ ! -e \"$2\" ]; do sleep 0.05; done; exit 0",
                )
                .into(),
                "sh".into(),
                marker.to_str().unwrap().into(),
                directory.path().join("detached").to_str().unwrap().into(),
            ],
            pty: Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::PtyConfig { size: None },
            ),
            idempotency_key: Some("pty-release".into()),
            ..Default::default()
        }),
    )
    .await
    .expect("process starts");
    let mut events = response.into_inner();
    while let Some(event) = events.next().await {
        event.expect("process stream remains successful");
    }
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(result) = std::fs::read_to_string(&marker) {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the descendant finishes writing");
    assert_eq!(
        result.trim(),
        "failed",
        "a terminal write fails once the process has exited"
    );
    shut_down(&service).await;
}

// The retained-output budget follows the guest's memory: a 2 GiB guest keeps
// the full 64 MiB, smaller guests a thirty-second of their memory down to
// 16 MiB, and a guest of unknown size the full budget.
#[test]
fn output_budget_scales_with_guest_memory() {
    const MIB: u64 = 1024 * 1024;
    let total = |mem: Option<u64>| ProcessOutputBudget::for_guest_memory(mem).total() as u64;
    assert_eq!(total(None), 64 * MIB);
    assert_eq!(total(Some(2048 * MIB)), 64 * MIB);
    assert_eq!(total(Some(8192 * MIB)), 64 * MIB);
    assert_eq!(total(Some(1024 * MIB)), 32 * MIB);
    assert_eq!(total(Some(512 * MIB)), 16 * MIB);
    assert_eq!(total(Some(128 * MIB)), 16 * MIB);
    let small = ProcessOutputBudget::for_guest_memory(Some(512 * MIB));
    assert_eq!(small.guaranteed_per_record, 64 * 1024);
    assert!(PROCESS_OUTPUT_RETENTION_BYTES <= small.total() / 4);
    // The default registry keeps today's numbers, which the tests above use.
    assert_eq!(
        ProcessOutputBudget::for_guest_memory(None).guaranteed_per_record,
        PROCESS_OUTPUT_GUARANTEED_BYTES
    );
    assert_eq!(
        ProcessOutputBudget::for_guest_memory(None).shared,
        PROCESS_OUTPUT_SHARED_BYTES
    );
}

#[tokio::test]
async fn native_process_payloads_obey_message_limits_before_lookup_or_spawn() {
    let service = cancellation_service();
    let oversized = vec![b'x'; pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES];
    let start = Process::start(
        &service,
        Request::new(StartRequest {
            command: vec![
                "/bin/true".into(),
                String::from_utf8(oversized.clone()).unwrap(),
            ],
            ..Default::default()
        }),
    )
    .await
    .err()
    .expect("oversized start");
    let input = Process::send_input(
        &service,
        Request::new(SendInputRequest {
            process_id: "sbxps_2f1c9a4e-0000-7000-8000-000000000002".into(),
            data: oversized,
        }),
    )
    .await
    .unwrap_err();
    for error in [start, input] {
        assert_eq!(error.code(), tonic::Code::OutOfRange);
    }
    assert!(service.inner.processes.records.is_empty());
}

#[tokio::test]
async fn image_working_directory_is_used_unless_command_overrides_it() {
    let image_directory = tempfile::tempdir().unwrap();
    let override_directory = tempfile::tempdir().unwrap();
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .unwrap()
        .unwrap();
    for (default, requested, expected) in [
        (Some(image_directory.path()), None, image_directory.path()),
        (
            Some(image_directory.path()),
            Some(override_directory.path()),
            override_directory.path(),
        ),
        (None, None, user.dir.as_path()),
    ] {
        let service = ExecutionService::with_config(
            CancellationToken::new(),
            ExecutionServiceConfig {
                default_operation_user: GuestUserName::parse(user.name.clone()).unwrap(),
                default_working_dir: default.map(std::path::Path::to_path_buf),
                filesystem_execution: FilesystemExecutionMode::Direct,
                identity_policy: IdentityPolicy::Guest,
                ..Default::default()
            },
        )
        .unwrap();
        let response = Process::start(
            &service,
            Request::new(StartRequest {
                command: vec!["/bin/pwd".into()],
                cwd: requested.map(|path| path.to_str().unwrap().to_string()),
                idempotency_key: Some("image-workdir".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let mut stream = response.into_inner();
        let mut stdout = Vec::new();
        while let Some(event) = stream.next().await {
            if let Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stdout(
                    output,
                ),
            ) = event.unwrap().event
            {
                stdout.extend(output.data);
            }
        }
        service.inner.shutdown.cancel();
        service.inner.process_tasks.close();
        service.inner.process_tasks.wait().await;
        assert_eq!(
            String::from_utf8(stdout).unwrap().trim(),
            expected.canonicalize().unwrap().to_str().unwrap()
        );
    }
}
