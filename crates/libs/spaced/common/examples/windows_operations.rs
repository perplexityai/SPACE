//! Explicitly targeted native Windows component qualification (synthetic data).
use anyhow::{Context, ensure};
use pplx_space_spaced_common::{
    BRIDGE_GUEST_VSOCK_PORT, ExpectedUdsPeer, connect_firecracker_vsock_channel_with_expected_peer,
    proto::bridge_guest::*,
};
use std::time::Duration;
use tonic::transport::Channel;
async fn output(
    mut stream: tonic::Streaming<ProcessEvent>,
) -> anyhow::Result<(u32, Vec<u8>, Vec<u8>, ProcessExit)> {
    let mut pid = 0;
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    while let Some(event) = stream.message().await? {
        pid = event.pid.unwrap_or(pid);
        match event.event {
            Some(process_event::Event::Stdout(o)) => stdout.extend(o.data),
            Some(process_event::Event::Stderr(o)) => stderr.extend(o.data),
            Some(process_event::Event::Exit(exit)) => return Ok((pid, stdout, stderr, exit)),
            _ => {}
        }
    }
    anyhow::bail!("process stream omitted exit")
}
fn start(script: &str) -> StartRequest {
    let id = uuid::Uuid::now_v7().to_string();
    StartRequest {
        command: vec![
            "powershell.exe".into(),
            "-NoProfile".into(),
            "-NonInteractive".into(),
            "-Command".into(),
            script.into(),
        ],
        idempotency_key: Some(id.clone()),
        process_id: Some(id),
        ..Default::default()
    }
}
#[derive(Clone)]
struct Route {
    channel: Channel,
    host: bool,
}
impl tower::Service<http::Request<tonic::body::Body>> for Route {
    type Response = <Channel as tower::Service<http::Request<tonic::body::Body>>>::Response;
    type Error = <Channel as tower::Service<http::Request<tonic::body::Body>>>::Error;
    type Future = <Channel as tower::Service<http::Request<tonic::body::Body>>>::Future;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.channel.poll_ready(cx)
    }
    fn call(&mut self, mut request: http::Request<tonic::body::Body>) -> Self::Future {
        if self.host {
            let path = request
                .uri()
                .path()
                .replace("bridge.guest.v1", "host.v1")
                .replace("/StatInfo", "/Stat")
                .replace("/ListInfo", "/List");
            *request.uri_mut() = path.parse().expect("generated RPC path");
        }
        self.channel.call(request)
    }
}
async fn run(channel: Channel, host: bool) -> anyhow::Result<()> {
    if !host {
        let mut meta = meta_client::MetaClient::new(channel.clone());
        let ready = meta
            .wait_ready(WaitReadyRequest {
                timeout: None,
                host_contract_revision: 1,
            })
            .await?
            .into_inner();
        ensure!(ready.ready, "not ready");
    } else {
        let mut meta =
            pplx_space_spaced_common::proto::host::meta_client::MetaClient::new(channel.clone());
        meta.wait_ready(pplx_space_spaced_common::proto::host::WaitReadyRequest {
            timeout: Some(prost_types::Duration {
                seconds: 30,
                nanos: 0,
            }),
        })
        .await?;
    }
    let channel = Route { channel, host };
    let mut files = filesystem_client::FilesystemClient::new(channel.clone());
    let mut processes = process_client::ProcessClient::new(channel);
    let root = format!(
        r"C:\Users\user\space-qualification-{}",
        uuid::Uuid::now_v7()
    );
    files
        .mkdir(MkdirRequest {
            path: root.clone(),
            parents: false,
            exist_ok: false,
            user: None,
        })
        .await?;
    let path = format!(r"{root}\data.txt");
    let open = WriteOpen {
        path: path.clone(),
        append: false,
        idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
        user: None,
    };
    files
        .write(tokio_stream::iter([
            WriteRequest {
                message: Some(write_request::Message::Open(open.clone())),
            },
            WriteRequest {
                message: Some(write_request::Message::Data(b"synthetic-file".to_vec())),
            },
        ]))
        .await?;
    let info = files
        .stat_info(StatRequest {
            path: path.clone(),
            user: None,
        })
        .await?
        .into_inner()
        .info
        .context("missing info")?;
    ensure!(
        info.windows_metadata.is_some() && info.size_bytes == 14,
        "incorrect Windows metadata"
    );
    let mut read = files
        .read(ReadRequest {
            path: path.clone(),
            offset: 0,
            length: None,
            user: None,
        })
        .await?
        .into_inner();
    let mut data = Vec::new();
    while let Some(chunk) = read.message().await? {
        data.extend(chunk.data);
    }
    ensure!(data == b"synthetic-file", "file read differs");
    let append = WriteOpen {
        append: true,
        idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
        ..open
    };
    for _ in 0..2 {
        files
            .write(tokio_stream::iter([
                WriteRequest {
                    message: Some(write_request::Message::Open(append.clone())),
                },
                WriteRequest {
                    message: Some(write_request::Message::Data(b"!".to_vec())),
                },
            ]))
            .await?;
    }
    ensure!(
        files
            .stat_info(StatRequest {
                path: path.clone(),
                user: None
            })
            .await?
            .into_inner()
            .info
            .context("missing info")?
            .size_bytes
            == 15,
        "append retry ran twice"
    );
    let denied = files
        .read(ReadRequest {
            path: r"C:\ProgramData\SPACE\identity.json".into(),
            offset: 0,
            length: None,
            user: None,
        })
        .await;
    let status = match denied {
        Err(s) => s,
        Ok(r) => r
            .into_inner()
            .message()
            .await
            .expect_err("credential read unexpectedly succeeded"),
    };
    ensure!(
        status.code() == tonic::Code::PermissionDenied,
        "wrong ACL refusal: {status}"
    );
    println!("files: create/write/read/metadata/idempotent append/privileged-path denial passed");
    let req = start(
        "[Console]::Out.Write([Security.Principal.WindowsIdentity]::GetCurrent().Name); [Console]::Error.Write('synthetic-error'); exit 23",
    );
    let (pid, out, err, exit) = output(processes.start(req.clone()).await?.into_inner()).await?;
    ensure!(
        String::from_utf8_lossy(&out)
            .to_lowercase()
            .ends_with("\\user"),
        "wrong process identity"
    );
    ensure!(err == b"synthetic-error", "stderr missing");
    ensure!(
        matches!(
            exit.outcome,
            Some(process_exit::Outcome::Exited(ProcessExited {
                exit_code: 23
            }))
        ),
        "wrong exit code"
    );
    let (retry_pid, _, _, _) = output(processes.start(req).await?.into_inner()).await?;
    ensure!(pid == retry_pid, "retry spawned a new process");
    let mut req = start("$x=[Console]::In.ReadToEnd(); [Console]::Out.Write($x)");
    req.stdin = true;
    let id = req.process_id.clone().context("probe process ID")?;
    let stream = processes.start(req).await?.into_inner();
    processes
        .send_input(SendInputRequest {
            process_id: id.clone(),
            data: b"stdin-proof".to_vec(),
        })
        .await?;
    processes
        .close_stdin(CloseStdinRequest { process_id: id })
        .await?;
    let (_, out, _, _) = output(stream).await?;
    ensure!(out == b"stdin-proof", "stdin did not round trip");
    println!("processes: account/stdout/stderr/exit/retry/stdin passed");
    let mut req = start("Start-Sleep -Seconds 60");
    req.timeout = Some(prost_types::Duration {
        seconds: 1,
        nanos: 0,
    });
    let (_, _, _, exit) = output(processes.start(req).await?.into_inner()).await?;
    ensure!(
        matches!(exit.outcome, Some(process_exit::Outcome::TimedOut(_))),
        "timeout not reported"
    );
    let req = start(
        "$p=Start-Process powershell.exe -ArgumentList '-NoProfile -NonInteractive -Command Start-Sleep -Seconds 120' -PassThru; Write-Output $p.Id; Start-Sleep -Seconds 120",
    );
    let id = req.process_id.clone().context("probe process ID")?;
    let mut stream = processes.start(req).await?.into_inner();
    let mut child = String::new();
    while !child.contains('\n') {
        let event = stream.message().await?.context("missing child PID")?;
        if let Some(process_event::Event::Stdout(o)) = event.event {
            child.push_str(std::str::from_utf8(&o.data)?);
        }
    }
    let child: u32 = child.trim().parse()?;
    processes
        .send_signal(SendSignalRequest {
            process_id: id,
            signal: 0,
            intent: Some(ProcessControlIntent::Kill as i32),
        })
        .await?;
    let _ = output(stream).await?;
    let req = start(&format!(
        "if (Get-Process -Id {child} -ErrorAction SilentlyContinue) {{ exit 11 }} else {{ exit 0 }}"
    ));
    let (_, _, _, exit) = output(processes.start(req).await?.into_inner()).await?;
    ensure!(
        matches!(
            exit.outcome,
            Some(process_exit::Outcome::Exited(ProcessExited {
                exit_code: 0
            }))
        ),
        "descendant survived job kill"
    );
    println!("processes: timeout and descendant job cleanup passed");
    let renamed = format!(r"{root}\renamed.txt");
    files
        .rename(RenameRequest {
            old_path: path,
            new_path: renamed,
            idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
            user: None,
        })
        .await?;
    let mut list = files
        .list_info(ListRequest {
            path: root.clone(),
            depth: Some(1),
            user: None,
        })
        .await?
        .into_inner();
    ensure!(list.message().await?.is_some(), "listing empty");
    files
        .remove(RemoveRequest {
            path: root,
            recursive: true,
            missing_ok: false,
            user: None,
        })
        .await?;
    println!("files: rename/list/remove passed; fixture removed");
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().context("socket path or --sandbox required")?;
    let (path, peer) = if path == "--sandbox" {
        let sandbox = args
            .next()
            .context("sandbox ID required")?
            .parse::<pplx_space_common_core::types::SandboxId>()?;
        (
            pplx_space_common_core::host::linux::paths::spaced_host_socket(&sandbox.to_string()),
            "--host".to_owned(),
        )
    } else {
        (path, args.next().context("VMM PID or --host required")?)
    };
    let host = peer == "--host";
    let pid = if host { 0 } else { peer.parse()? };
    let uid = if host {
        0
    } else {
        args.next().context("VMM UID required")?.parse()?
    };
    ensure!(args.next().is_none(), "unexpected arguments");
    tokio::time::timeout(Duration::from_secs(120), async {
        let channel = if host {
            pplx_space_spaced_common::connect_uds_channel(
                "Windows host operations probe",
                std::path::Path::new(&path),
            )
            .await?
        } else {
            let (channel, _) = connect_firecracker_vsock_channel_with_expected_peer(
                "Windows operations probe",
                std::path::Path::new(&path),
                BRIDGE_GUEST_VSOCK_PORT,
                ExpectedUdsPeer::new(pid, uid),
            )
            .await?;
            channel
        };
        run(channel, host).await
    })
    .await??;
    Ok(())
}
