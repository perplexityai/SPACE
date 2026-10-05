use crate::guest_service::{GuestFixture, create_ready_guest_service};
use crate::process_events::{
    collect_process_events, first_process_event, process_exit, successful_stdout,
};
use bridge_guest::process_event::Event;
use bridge_guest::process_exit::Outcome as ProcessExitOutcome;
use bridge_guest::process_server::Process;
use futures_util::StreamExt;
use nix::unistd::{Uid, User};
use pplx_space_common_core::types::DEFAULT_GUEST_USER;
use pplx_space_spaced_common::proto::bridge_guest;
use pplx_space_spaced_guest_server::SpacedGuestServiceConfig;

#[test]
fn production_default_operation_user_is_user() {
    assert_eq!(
        SpacedGuestServiceConfig::default()
            .default_operation_user
            .as_str(),
        DEFAULT_GUEST_USER
    );
}

#[tokio::test]
async fn service_shutdown_signals_running_processes() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = GuestFixture::start(directory.path()).await;
    let mut process = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());
    // Finite fallback prevents a failed regression from leaving a long-lived child.
    let mut events = process
        .start(bridge_guest::StartRequest {
            command: vec!["/bin/sh".into(), "-c".into(), "exec sleep 3".into()],
            idempotency_key: Some("shutdown-process".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    events.message().await.unwrap().expect("started");
    fixture.begin_shutdown();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = events.message().await.unwrap() {
            if let Some(Event::Exit(exit)) = event.event {
                return exit.outcome;
            }
        }
        None
    })
    .await
    .expect("shutdown must terminate without waiting for natural exit");
    assert!(matches!(outcome, Some(ProcessExitOutcome::Signaled(_))));
    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_start_streams_output_and_exit() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let mut stream = client
        .start(bridge_guest::StartRequest {
            command: vec!["/bin/sh".into(), "-c".into(), "printf hello".into()],
            idempotency_key: Some("start-1".into()),
            ..Default::default()
        })
        .await
        .expect("start response")
        .into_inner();

    let started = first_process_event(&mut stream).await;
    assert!(matches!(started.event, Some(Event::Started(_))));
    let events = collect_process_events(stream).await;
    assert_eq!(successful_stdout(&events), b"hello");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_send_input_writes_and_close_stdin_signals_eof() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let mut stream = client
        .start(bridge_guest::StartRequest {
            command: vec!["/bin/cat".into()],
            idempotency_key: Some("stdin-1".into()),
            stdin: true,
            ..Default::default()
        })
        .await
        .expect("start response")
        .into_inner();

    let started = first_process_event(&mut stream).await;
    assert!(matches!(started.event, Some(Event::Started(_))));
    let process_id = started.process_id;

    client
        .send_input(bridge_guest::SendInputRequest {
            process_id: process_id.clone(),
            data: b"hello stdin\n".to_vec(),
        })
        .await
        .expect("send input");
    client
        .close_stdin(bridge_guest::CloseStdinRequest {
            process_id: process_id.clone(),
        })
        .await
        .expect("close stdin");

    let events = collect_process_events(stream).await;
    assert_eq!(successful_stdout(&events), b"hello stdin\n");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_start_preserves_large_stdout_for_delayed_reader() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let (service, shutdown) =
        create_ready_guest_service(temp_dir.path().join("spaced-bridge-host.sock"), None, None);

    let payload_len = 16 * 1024 * 1024usize;
    let mut stream = service
        .start(tonic::Request::new(bridge_guest::StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                format!("yes x | tr -d '\\n' | head -c {payload_len}"),
            ],
            idempotency_key: Some("large-stdout".into()),
            ..Default::default()
        }))
        .await
        .expect("start response")
        .into_inner();

    let started = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("started event timed out")
        .expect("process stream message")
        .expect("started event");
    assert!(matches!(started.event, Some(Event::Started(_))));

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let delayed = stream.then(|event| async move {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        event
    });
    let events = collect_process_events(Box::pin(delayed)).await;
    let output = successful_stdout(&events);
    assert_eq!(output.len(), payload_len);
    assert!(output.iter().all(|byte| *byte == b'x'));

    shutdown.cancel();
}

#[tokio::test]
async fn bridge_guest_process_stalled_stream_does_not_block_reconnect_exit() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let (service, shutdown) =
        create_ready_guest_service(temp_dir.path().join("spaced-bridge-host.sock"), None, None);

    let mut stalled = service
        .start(tonic::Request::new(bridge_guest::StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "yes x | tr -d '\\n' | head -c 2000000".into(),
            ],
            idempotency_key: Some("stalled-stream".into()),
            ..Default::default()
        }))
        .await
        .expect("start response")
        .into_inner();

    let started = stalled
        .next()
        .await
        .expect("started event")
        .expect("started event response");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut connected = service
        .connect_process(tonic::Request::new(bridge_guest::ConnectProcessRequest {
            process_id: started.process_id,
            timeout: None,
            output_cursor: None,
        }))
        .await
        .expect("connect response")
        .into_inner();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = connected.next().await {
            if matches!(
                event.expect("process event response").event,
                Some(Event::Exit(_))
            ) {
                return;
            }
        }
        panic!("connect stream ended without exit");
    })
    .await
    .expect("stalled subscriber blocked process completion");

    shutdown.cancel();
}

#[tokio::test]
async fn bridge_guest_process_connect_replays_from_output_cursor() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let (service, shutdown) =
        create_ready_guest_service(temp_dir.path().join("spaced-bridge-host.sock"), None, None);

    let mut started_stream = service
        .start(tonic::Request::new(bridge_guest::StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf replay; sleep 1".into(),
            ],
            idempotency_key: Some("cursor-replay".into()),
            ..Default::default()
        }))
        .await
        .expect("start response")
        .into_inner();

    let started = started_stream
        .next()
        .await
        .expect("started event")
        .expect("started event response");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut connected = service
        .connect_process(tonic::Request::new(bridge_guest::ConnectProcessRequest {
            process_id: started.process_id,
            timeout: None,
            output_cursor: Some(0),
        }))
        .await
        .expect("connect response")
        .into_inner();

    let mut replayed = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(event) = connected.next().await {
            let event = event.expect("process event response");
            if let Some(Event::Stdout(stdout)) = event.event {
                replayed.extend_from_slice(&stdout.data);
                if replayed == b"replay" {
                    return;
                }
            }
        }
        panic!("connect stream ended before replaying stdout");
    })
    .await
    .expect("cursor replay timed out");

    shutdown.cancel();
}

#[tokio::test]
async fn bridge_guest_process_start_merges_configured_and_request_env() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let channel = fixture.channel.clone();
    let mut meta = bridge_guest::meta_client::MetaClient::new(channel.clone());
    meta.configure_process_env(bridge_guest::ConfigureProcessEnvRequest {
        env: std::collections::HashMap::from([
            ("SANDBOX_ONLY".to_string(), "sandbox".to_string()),
            ("OVERRIDE".to_string(), "sandbox".to_string()),
            ("MULTILINE".to_string(), "line 1\nline 2".to_string()),
            ("BAD-NAME".to_string(), "drop".to_string()),
        ]),
    })
    .await
    .expect("configure process env");

    let mut client = bridge_guest::process_client::ProcessClient::new(channel);
    for (clean_env, expected) in [
        (false, "sandbox:request:line 1\nline 2:request"),
        (true, ":request::request"),
    ] {
        let stream = client.start(bridge_guest::StartRequest {
            command: vec!["/bin/sh".into(), "-c".into(),
                "printf '%s:%s:%s:%s' \"$SANDBOX_ONLY\" \"$OVERRIDE\" \"$MULTILINE\" \"$REQUEST_ONLY\"".into()],
            env: std::collections::HashMap::from([
                ("OVERRIDE".into(), "request".into()),
                ("REQUEST_ONLY".into(), "request".into()),
                ("1BAD".into(), "drop".into()),
            ]),
            idempotency_key: Some(format!("start-env-{clean_env}")),
            clean_env,
            ..Default::default()
        }).await.expect("start environment case").into_inner();
        let events = collect_process_events(stream).await;
        assert_eq!(
            successful_stdout(&events),
            expected.as_bytes(),
            "clean_env={clean_env}"
        );
    }

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_login_environment_and_relative_cwd() {
    let current = User::from_uid(Uid::current())
        .expect("current user lookup works")
        .expect("current user exists");
    let home_temp = tempfile::Builder::new()
        .prefix("space-relative-cwd-")
        .tempdir_in(&current.dir)
        .expect("temp dir created under user home");
    let relative_cwd = home_temp
        .path()
        .file_name()
        .expect("temp dir has file name")
        .to_string_lossy()
        .to_string();
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    for (cwd, command, expected) in [
        (
            None,
            vec![
                "/bin/sh",
                "-c",
                "printf '%s\n%s\n%s\n%s\n%s\n%s' \"$PWD\" \"$HOME\" \"$USER\" \"$LOGNAME\" \"$SHELL\" \"$PATH\"",
            ],
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}",
                current.dir.display(),
                current.dir.display(),
                current.name,
                current.name,
                current.shell.display(),
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ),
        ),
        (
            Some(relative_cwd),
            vec!["/bin/pwd"],
            format!("{}\n", home_temp.path().display()),
        ),
    ] {
        let stream = client
            .start(bridge_guest::StartRequest {
                command: command.into_iter().map(str::to_string).collect(),
                idempotency_key: Some(format!("login-cwd-{cwd:?}")),
                cwd,
                user: Some(current.name.clone()),
                ..Default::default()
            })
            .await
            .expect("start login/cwd case")
            .into_inner();
        assert_eq!(
            String::from_utf8(successful_stdout(&collect_process_events(stream).await))
                .expect("utf-8 process output"),
            expected
        );
    }
    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_start_rejects_unknown_user() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let error = client
        .start(bridge_guest::StartRequest {
            command: vec!["/bin/sh".into(), "-c".into(), "true".into()],
            user: Some("__space_missing_user__".into()),
            idempotency_key: Some("unknown-user".into()),
            ..Default::default()
        })
        .await
        .expect_err("unknown user rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    fixture.stop().await;
}

#[tokio::test]
async fn process_start_honors_configured_default_and_explicit_user_override() {
    use bridge_guest::StartRequest;
    use pplx_space_common_core::types::GuestUserName;
    use pplx_space_spaced_guest_server::SpacedGuestService;
    let current = User::from_uid(Uid::current()).unwrap().unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let service = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            default_operation_user: GuestUserName::parse("__space_missing_user__").unwrap(),
            ..Default::default()
        },
    );
    let request = StartRequest {
        command: vec!["/usr/bin/id".into(), "-un".into()],
        idempotency_key: Some("default-user".into()),
        ..Default::default()
    };
    service.ready();
    let error = match service.start(tonic::Request::new(request.clone())).await {
        Ok(_) => panic!("omitted user must resolve the configured identity"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let stream = service
        .start(tonic::Request::new(StartRequest {
            user: Some(current.name.clone()),
            ..request
        }))
        .await
        .unwrap()
        .into_inner();
    let output = successful_stdout(&collect_process_events(stream).await);
    assert_eq!(String::from_utf8(output).unwrap().trim(), current.name);
    shutdown.cancel();
}

#[tokio::test]
async fn bridge_guest_process_start_requires_and_dedups_idempotency_key() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let mut request = bridge_guest::StartRequest {
        command: vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()],
        background: true,
        ..Default::default()
    };
    let missing_key = client
        .start(request.clone())
        .await
        .expect_err("start without key rejected");
    assert_eq!(missing_key.code(), tonic::Code::FailedPrecondition);
    request.idempotency_key = Some("proc-1".into());
    let first = first_process_event(
        client
            .start(request.clone())
            .await
            .expect("first start")
            .into_inner(),
    )
    .await;
    let second = first_process_event(
        client
            .start(request)
            .await
            .expect("dedup start")
            .into_inner(),
    )
    .await;
    assert_eq!(first.process_id, second.process_id);
    assert_eq!(first.pid, second.pid);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_start_honors_assigned_process_id_and_refuses_reuse() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let assigned = "sbxps_2f1c9a4e-0000-7000-8000-000000000001";
    let mut request = bridge_guest::StartRequest {
        command: vec!["/bin/sh".into(), "-c".into(), "sleep 5".into()],
        background: true,
        idempotency_key: Some("assign-1".into()),
        process_id: Some(assigned.into()),
        ..Default::default()
    };
    let started = first_process_event(
        client
            .start(request.clone())
            .await
            .expect("assigned start")
            .into_inner(),
    )
    .await;
    assert_eq!(started.process_id, assigned);

    // Reusing the same key would otherwise replay, and a fresh key would
    // otherwise reserve; the id guard has to answer before either, because the
    // data plane reconnects only on AlreadyExists.
    let replayed_key = client
        .start(request.clone())
        .await
        .expect_err("same key with a live assigned id rejected");
    assert_eq!(replayed_key.code(), tonic::Code::AlreadyExists);

    request.idempotency_key = Some("assign-2".into());
    let fresh_key = client
        .start(request)
        .await
        .expect_err("fresh key with a live assigned id rejected");
    assert_eq!(fresh_key.code(), tonic::Code::AlreadyExists);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_start_replays_output_while_cursorless_connect_returns_exit() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let events = collect_process_events(
        client
            .start(bridge_guest::StartRequest {
                command: vec!["/bin/sh".into(), "-c".into(), "printf done".into()],
                idempotency_key: Some("replay-after-exit".into()),
                ..Default::default()
            })
            .await
            .expect("start process")
            .into_inner(),
    )
    .await;
    let started = events.first().expect("started event");
    assert!(matches!(started.event, Some(Event::Started(_))));
    process_exit(&events);
    for reconnect in [true, false] {
        let stream = if reconnect {
            client
                .connect_process(bridge_guest::ConnectProcessRequest {
                    process_id: started.process_id.clone(),
                    timeout: None,
                    output_cursor: None,
                })
                .await
                .expect("connect exited process")
                .into_inner()
        } else {
            client
                .start(bridge_guest::StartRequest {
                    command: vec!["/bin/sh".into(), "-c".into(), "printf duplicate".into()],
                    idempotency_key: Some("replay-after-exit".into()),
                    ..Default::default()
                })
                .await
                .expect("dedup exited process")
                .into_inner()
        };
        let replay = collect_process_events(stream).await;
        if reconnect {
            assert_eq!(replay.len(), 2);
        } else {
            assert_eq!(successful_stdout(&replay), b"done");
        }
        assert_eq!(replay[0].process_id, started.process_id);
        assert!(matches!(replay[0].event, Some(Event::Started(_))));
        process_exit(&replay);
    }

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_pty_start_input_and_resize_work() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let mut stream = client
        .start(bridge_guest::StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "stty size; read line; stty size; printf 'got:%s\\n' \"$line\"".into(),
            ],
            timeout: Some(prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            idempotency_key: Some("pty-1".into()),
            pty: Some(bridge_guest::PtyConfig {
                size: Some(bridge_guest::PtySize {
                    rows: 33,
                    cols: 101,
                }),
            }),
            ..Default::default()
        })
        .await
        .expect("start response")
        .into_inner();

    let started = first_process_event(&mut stream).await;
    assert!(matches!(started.event, Some(Event::Started(_))));
    let process_id = started.process_id;

    let mut list_stream = client
        .list_processes(bridge_guest::ListProcessesRequest {})
        .await
        .expect("list processes response")
        .into_inner();
    let mut listed_pty = false;
    while let Some(info) = list_stream.message().await.expect("process info") {
        if info.process_id == process_id {
            assert_eq!(info.pty, Some(true));
            listed_pty = true;
        }
    }
    assert!(listed_pty);

    let mut output = Vec::new();
    loop {
        let event = stream
            .message()
            .await
            .expect("process event")
            .expect("initial pty output");
        match event.event.expect("event payload") {
            Event::Stdout(chunk) => {
                output.extend_from_slice(&chunk.data);
                if String::from_utf8_lossy(&output).contains("33 101") {
                    break;
                }
            }
            Event::Exit(exit) => {
                panic!("process exited before initial pty size output: {exit:?}");
            }
            _ => {}
        }
    }

    client
        .resize_pty(bridge_guest::ResizePtyRequest {
            process_id: process_id.clone(),
            size: Some(bridge_guest::PtySize {
                rows: 34,
                cols: 102,
            }),
        })
        .await
        .expect("resize pty");
    client
        .send_input(bridge_guest::SendInputRequest {
            process_id,
            data: b"hello pty\n".to_vec(),
        })
        .await
        .expect("send pty input");

    let events = collect_process_events(stream).await;
    output.extend(successful_stdout(&events));

    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("33 101"), "output: {output:?}");
    assert!(output.contains("34 102"), "output: {output:?}");
    assert!(output.contains("got:hello pty"), "output: {output:?}");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_connect_and_signal_validation_work() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let started = first_process_event(
        client
            .start(bridge_guest::StartRequest {
                command: vec!["/bin/sh".into(), "-c".into(), "sleep 10".into()],
                background: true,
                idempotency_key: Some("connect-kill-1".into()),
                ..Default::default()
            })
            .await
            .expect("start background")
            .into_inner(),
    )
    .await;
    let process_id = started.process_id.clone();

    let connected = first_process_event(
        client
            .connect_process(bridge_guest::ConnectProcessRequest {
                process_id: process_id.clone(),
                timeout: None,
                output_cursor: None,
            })
            .await
            .expect("connect process")
            .into_inner(),
    )
    .await;
    assert_eq!(connected.process_id, process_id);
    assert_eq!(connected.pid, started.pid);

    let error = client
        .send_signal(bridge_guest::SendSignalRequest {
            process_id: process_id.clone(),
            signal: 0,
        })
        .await
        .expect_err("zero signal rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    client
        .send_signal(bridge_guest::SendSignalRequest {
            process_id: process_id.clone(),
            signal: 9,
        })
        .await
        .expect("kill process");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_list_returns_running_processes() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let cwd = temp_dir.path().to_string_lossy().to_string();
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let started = first_process_event(
        client
            .start(bridge_guest::StartRequest {
                command: vec!["/bin/sh".into(), "-c".into(), "sleep 30".into()],
                cwd: Some(cwd.clone()),
                env: std::collections::HashMap::from([(
                    "SPACE_PROCESS_LIST_TEST".into(),
                    "1".into(),
                )]),
                background: true,
                idempotency_key: Some("process-list".into()),
                tag: Some("server".into()),
                ..Default::default()
            })
            .await
            .expect("start process")
            .into_inner(),
    )
    .await;

    let mut stream = client
        .list_processes(bridge_guest::ListProcessesRequest {})
        .await
        .expect("list processes")
        .into_inner();
    let mut found = false;
    while let Some(info) = stream.message().await.expect("process info") {
        if info.process_id == started.process_id && started.pid == Some(info.pid) {
            assert_eq!(info.command, ["/bin/sh", "-c", "sleep 30"]);
            assert_eq!(info.cwd.as_deref(), Some(cwd.as_str()));
            assert_eq!(
                info.env.get("SPACE_PROCESS_LIST_TEST").map(String::as_str),
                Some("1")
            );
            assert_eq!(info.tag.as_deref(), Some("server"));
            assert_eq!(info.pty, Some(false));
            found = true;
        }
    }
    assert!(found);

    client
        .send_signal(bridge_guest::SendSignalRequest {
            process_id: started.process_id,
            signal: 9,
        })
        .await
        .expect("cleanup kill");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_connect_unknown_returns_not_found() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let error = client
        .connect_process(bridge_guest::ConnectProcessRequest {
            process_id: "missing".into(),
            timeout: None,
            output_cursor: None,
        })
        .await
        .expect_err("missing process rejected");
    assert_eq!(error.code(), tonic::Code::NotFound);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_start_timeout_kills_process() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = bridge_guest::process_client::ProcessClient::new(fixture.channel.clone());

    let stream = client
        .start(bridge_guest::StartRequest {
            command: vec!["/bin/sleep".into(), "5".into()],
            timeout: Some(prost_types::Duration {
                seconds: 0,
                nanos: 50_000_000,
            }),
            idempotency_key: Some("timeout-1".into()),
            ..Default::default()
        })
        .await
        .expect("start response")
        .into_inner();

    let events = collect_process_events(stream).await;
    let started = events.first().expect("started event");
    assert!(matches!(started.event, Some(Event::Started(_))));
    let outcome = process_exit(&events);
    assert!(
        matches!(outcome, ProcessExitOutcome::TimedOut(exit) if exit.signal == Some(libc::SIGKILL)),
        "expected SIGKILL timeout outcome, got {outcome:?}"
    );
    let process_id = &started.process_id;

    let mut list_stream = client
        .list_processes(bridge_guest::ListProcessesRequest {})
        .await
        .expect("list processes response")
        .into_inner();
    while let Some(info) = list_stream.message().await.expect("process info") {
        assert_ne!(&info.process_id, process_id);
    }

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_process_exit_does_not_wait_forever_on_inherited_output_pipes() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let (service, shutdown) =
        create_ready_guest_service(temp_dir.path().join("spaced-bridge-host.sock"), None, None);

    let mut stream = service
        .start(tonic::Request::new(bridge_guest::StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "sleep 30 & printf parent-exited; exit 7".into(),
            ],
            timeout: Some(prost_types::Duration {
                seconds: 0,
                nanos: 500_000_000,
            }),
            idempotency_key: Some("inherited-output-pipe".into()),
            ..Default::default()
        }))
        .await
        .expect("start response")
        .into_inner();

    let started = first_process_event(&mut stream).await;
    let pid = started.pid.expect("started pid");
    let mut saw_exit = false;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while let Some(event) = stream.next().await {
            if let Some(Event::Exit(exit)) =
                event.expect("process event response").event
            {
                assert!(matches!(exit.outcome, Some(bridge_guest::process_exit::Outcome::Exited(exit)) if exit.exit_code == 7),
                    "execution deadline must not overwrite an already-exited leader during output cleanup");
                saw_exit = true;
                break;
            }
        }
    })
    .await
    .expect("inherited output pipe blocked process exit event");
    assert!(saw_exit);

    // Non-Linux test hosts use the bounded-drain fallback and cannot pin a
    // process-group identity with pidfd before reaping. Clean up the deliberately
    // inherited sleeper there; Linux supervision already terminates it safely.
    #[cfg(not(target_os = "linux"))]
    {
        nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        )
        .expect("clean up inherited sleeper");
    }
    #[cfg(target_os = "linux")]
    let _ = pid;

    shutdown.cancel();
}

#[tokio::test]
async fn bridge_guest_process_exit_preserves_detached_descendant_with_closed_output() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let (service, shutdown) =
        create_ready_guest_service(temp_dir.path().join("spaced-bridge-host.sock"), None, None);

    let stream = service
        .start(tonic::Request::new(bridge_guest::StartRequest {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "sleep 30 </dev/null >/dev/null 2>/dev/null & printf '%s' \"$!\"".into(),
            ],
            idempotency_key: Some("detached-closed-output".into()),
            ..Default::default()
        }))
        .await
        .expect("start response")
        .into_inner();
    let events = collect_process_events(stream).await;
    let leader_pid = events
        .first()
        .expect("started event")
        .pid
        .expect("leader pid");
    let descendant_pid: i32 = String::from_utf8(successful_stdout(&events))
        .expect("descendant pid output is utf-8")
        .parse()
        .expect("descendant pid output is numeric");

    nix::sys::signal::kill(nix::unistd::Pid::from_raw(descendant_pid), None)
        .expect("detached descendant remains alive");
    // The surviving descendant keeps the leader-created process group alive.
    nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(leader_pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("clean up detached descendant");

    shutdown.cancel();
}
