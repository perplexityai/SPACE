use clap::Parser;
use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_host_client::StartProcessOptions;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[derive(Parser)]
#[command(
    name = "spaced-host",
    about = "SPACE host service CLI",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Resolve the current spaced-host socket for this sandbox id.
    #[arg(long, conflicts_with = "socket")]
    sandbox: Option<String>,
    /// Connect to a specific spaced-host socket.
    #[arg(long)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Check that spaced-host-svc is reachable.
    Ping,
    /// Check whether the guest bridge is responding.
    IsConnected,
    /// Ask spaced-host-svc to ping its connected guest service.
    PingGuest,
    /// Return guest readiness status through spaced-host-svc.
    GetStatus,
    /// Wait until the guest reports ready.
    WaitReady {
        /// Timeout in milliseconds.
        #[arg(long)]
        timeout_ms: u64,
    },
    /// Return metadata for a guest path.
    Stat {
        path: String,
        /// Inspect the path as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// List entries in a guest directory.
    List {
        path: String,
        /// List entries as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Read a guest file and print it to stdout.
    Read {
        path: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        length: Option<u64>,
        /// Read as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Write text to a guest file.
    Write {
        path: String,
        data: String,
        #[arg(long, default_value_t = false)]
        append: bool,
        /// Write as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Create a guest directory.
    Mkdir {
        path: String,
        #[arg(long, default_value_t = false)]
        parents: bool,
        #[arg(long, default_value_t = false)]
        exist_ok: bool,
        /// Create the directory as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Remove a guest file or directory.
    Remove {
        path: String,
        #[arg(long, default_value_t = false)]
        recursive: bool,
        #[arg(long, default_value_t = false)]
        missing_ok: bool,
        /// Remove the path as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Rename a guest path using POSIX rename semantics.
    Rename {
        old_path: String,
        new_path: String,
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Rename as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Run a guest command and print output as it arrives.
    Run {
        #[arg(required = true)]
        command: Vec<String>,
        #[arg(long, default_value_t = false)]
        background: bool,
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Execute as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
    },
    /// Connect to a running guest process by `process_id`.
    Connect { process_id: String },
    /// Send a signal to a running guest process by `process_id`.
    SendSignal {
        process_id: String,
        #[arg(long)]
        signal: i32,
    },
    /// Kill a running guest process by `process_id` using SIGKILL.
    Kill { process_id: String },
}

#[tokio::main]
async fn main() {
    pplx_space_common_cli::init_logging("spaced-host");
    if let Err(error) = run().await {
        pplx_space_common_cli::exit_error(error);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut config = pplx_space_spaced_host_client::SpacedHostClientConfig::default();
    if let Some(sandbox_id) = cli.sandbox {
        config = config.with_socket_path(
            pplx_space_common_core::host::linux::paths::spaced_host_socket(&sandbox_id),
        );
    }
    if let Some(socket) = cli.socket {
        config = config.with_socket_path(socket);
    }
    let client = pplx_space_common_cli::unwrap_or_exit(
        pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config).await,
        "cannot connect to spaced-host (is space-spaced-host-svc running?)",
    );

    match cli.command {
        Commands::Ping => {
            client.ping().await?;
            println!("ok=true");
        }
        Commands::IsConnected => {
            let response = client.is_connected().await?;
            println!("connected={}", response.connected);
        }
        Commands::PingGuest => {
            client.ping_guest().await?;
            println!("ok=true");
        }
        Commands::GetStatus => {
            let response = client.get_status().await?;
            println!("ready={}", response.ready);
            print_duration("guest_uptime", response.guest_uptime);
            print_version("host", response.host_version);
            print_version("guest", response.guest_version);
        }
        Commands::WaitReady { timeout_ms } => {
            let response = client.wait_ready(Duration::from_millis(timeout_ms)).await?;
            print_duration("guest_uptime", response.guest_uptime);
        }
        Commands::Stat { path, user } => {
            let response = client.stat_as_user(path, user).await?;
            let info = response
                .info
                .unwrap_or_else(|| pplx_space_common_cli::exit_error("stat response missing info"));
            println!("file_type={:?}", info.file_type());
            println!("size_bytes={}", info.size_bytes);
            println!("mode={:#o}", info.mode);
            print_timestamp("modified_at", info.modified_at);
        }
        Commands::List { path, user } => {
            let mut stream = client
                .list_stream_with_depth_as_user(path, Some(1), user)
                .await?;
            while let Some(entry) = stream.message().await? {
                println!(
                    "name={} path={} file_type={:?}",
                    entry.name.as_deref().unwrap_or_default(),
                    entry.path,
                    entry.file_type()
                );
            }
        }
        Commands::Read {
            path,
            offset,
            length,
            user,
        } => {
            let mut stream = client
                .read_stream_as_user(path, offset, length, user)
                .await?;
            let mut stdout = tokio::io::stdout();
            while let Some(chunk) = stream.message().await? {
                write_output(&mut stdout, &chunk.data).await?;
            }
        }
        Commands::Write {
            path,
            data,
            append,
            user,
        } => {
            let response = client
                .write_with_idempotency(path, data.into_bytes(), append, None, user)
                .await?;
            println!("bytes_written={}", response.bytes_written);
        }
        Commands::Mkdir {
            path,
            parents,
            exist_ok,
            user,
        } => {
            client.mkdir(path, parents, exist_ok, user).await?;
            println!("ok=true");
        }
        Commands::Remove {
            path,
            recursive,
            missing_ok,
            user,
        } => {
            client
                .remove_as_user(path, recursive, missing_ok, user)
                .await?;
            println!("ok=true");
        }
        Commands::Rename {
            old_path,
            new_path,
            idempotency_key,
            user,
        } => {
            client
                .rename_as_user(old_path, new_path, idempotency_key, user)
                .await?;
            println!("ok=true");
        }
        Commands::Run {
            command,
            background,
            idempotency_key,
            user,
        } => {
            let mut stream = client
                .start_process_stream_with_options(StartProcessOptions {
                    background,
                    idempotency_key: idempotency_key
                        .map(pplx_space_common_core::types::IdempotencyKey::parse)
                        .transpose()?,
                    user,
                    ..StartProcessOptions::new(pplx_space_common_core::types::ProcessCommand::new(
                        command,
                    )?)
                })
                .await?;
            while let Some(event) = stream.message().await? {
                print_process_event(event).await?;
                if background {
                    break;
                }
            }
        }
        Commands::Connect { process_id } => {
            let mut stream = client
                .connect_process_stream_with_cursor(process_id, None, None, false)
                .await?;
            while let Some(event) = stream.message().await? {
                print_process_event(event).await?;
            }
        }
        Commands::SendSignal { process_id, signal } => {
            client.send_signal(process_id, signal).await?;
            println!("ok=true");
        }
        Commands::Kill { process_id } => {
            client.kill_process(process_id).await?;
            println!("ok=true");
        }
    }
    Ok(())
}

fn print_duration(label: &str, duration: Option<prost_types::Duration>) {
    let Some(duration) = duration else {
        println!("{label}=none");
        return;
    };
    println!("{label}={}.{:09}s", duration.seconds, duration.nanos.max(0));
}

fn print_timestamp(label: &str, timestamp: Option<prost_types::Timestamp>) {
    let Some(timestamp) = timestamp else {
        println!("{label}=none");
        return;
    };
    println!(
        "{label}={}.{:09}Z",
        timestamp.seconds,
        timestamp.nanos.max(0)
    );
}

fn print_version(label: &str, version: Option<pplx_space_spaced_host_client::proto::VersionInfo>) {
    let Some(version) = version else {
        println!("{label}_version=none");
        println!("{label}_compatibility_family=none");
        return;
    };
    println!("{label}_version={}", version.version);
    println!(
        "{label}_compatibility_family={}",
        version.compatibility_family
    );
}

fn display_optional<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn timestamp_to_unix_ms(timestamp: Option<prost_types::Timestamp>) -> Option<u64> {
    let timestamp = timestamp?;
    if timestamp.seconds < 0 || timestamp.nanos < 0 {
        return None;
    }
    let seconds = u64::try_from(timestamp.seconds).ok()?;
    let nanos = u64::try_from(timestamp.nanos).ok()?;
    Some(seconds.saturating_mul(1_000) + (nanos / 1_000_000))
}

async fn write_output(
    output: &mut (impl tokio::io::AsyncWrite + Unpin),
    data: &[u8],
) -> std::io::Result<()> {
    output.write_all(data).await?;
    output.flush().await
}

async fn print_process_event(
    event: pplx_space_spaced_host_client::proto::ProcessEvent,
) -> std::io::Result<()> {
    use pplx_space_spaced_host_client::proto::process_event::Event;
    let pid = display_optional(event.pid);
    let line = match event.event {
        Some(Event::Started(started)) => format!(
            "started process_id={} pid={} start_time_unix_ms={}\n",
            event.process_id,
            pid,
            display_optional(timestamp_to_unix_ms(started.start_time))
        ),
        Some(Event::Connected(_)) => {
            format!("connected process_id={} pid={}\n", event.process_id, pid)
        }
        Some(Event::Stdout(output)) => {
            return write_output(&mut tokio::io::stdout(), &output.data).await;
        }
        Some(Event::Stderr(output)) => {
            return write_output(&mut tokio::io::stderr(), &output.data).await;
        }
        Some(Event::Exit(exit)) => {
            use pplx_space_spaced_host_client::proto::process_exit::Outcome;
            match exit.outcome {
                Some(Outcome::Exited(exited)) => format!("exit_code={}\n", exited.exit_code),
                Some(Outcome::Signaled(signaled)) => {
                    format!("signal={}\n", display_optional(signaled.signal))
                }
                Some(Outcome::TimedOut(timed_out)) => {
                    format!(
                        "timed_out=true\nsignal={}\n",
                        display_optional(timed_out.signal)
                    )
                }
                Some(Outcome::Unknown(_)) | None => "unknown_exit=true\n".to_owned(),
            }
        }
        None => return Ok(()),
    };
    write_output(&mut tokio::io::stdout(), line.as_bytes()).await
}
