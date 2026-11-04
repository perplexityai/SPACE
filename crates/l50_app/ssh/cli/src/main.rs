use pplx_space_sbx_mgr_client::proto::{GetEnvRequest, StateRequest};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, exit};

use clap::Parser;
use pplx_space_common_core::ids::sandbox_label_or_id as sandbox_label;
use pplx_space_common_core::types::{
    DEFAULT_GUEST_USER, ExecutionMetadata, GuestOperatingSystem, OneOrMany, SandboxId,
    SandboxSelector,
};

const SSH_KEY: &str = "/root/.ssh/sandbox_ed25519";
const GUEST_IP: std::net::Ipv4Addr = pplx_space_net_common::guest_link::GUEST;

#[derive(Parser)]
#[command(
    name = "space-ssh",
    about = "SSH into a running sandbox",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Sandbox ID, long UUID prefix, or short random label
    id: SandboxSelector,

    /// SSH user (Linux: user; Windows: Administrator; otherwise SSH configuration)
    #[arg(long)]
    user: Option<String>,

    /// SSH identity file (non-Linux guests otherwise use your SSH configuration)
    #[arg(short = 'i', long)]
    identity_file: Option<PathBuf>,

    /// Command to run (default: interactive shell)
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() {
    pplx_space_common_cli::init_logging("space-ssh");
    let cli = Cli::parse();

    let mut client = pplx_space_common_cli::unwrap_or_exit(
        pplx_space_sbx_mgr_client::connect().await,
        "cannot connect to sbx-manager (is space-sbx-manager running?)",
    );

    let sandboxes = pplx_space_sbx_mgr_client::list_all_sandboxes(&mut client)
        .await
        .unwrap_or_else(|error| pplx_space_common_cli::exit_error(error));

    if sandboxes.is_empty() {
        pplx_space_common_cli::exit_message("No sandboxes found.");
    }

    let listed = match cli.id.find(sandboxes.iter(), |sandbox| {
        sandbox
            .sandbox_id
            .parse::<SandboxId>()
            .unwrap_or_else(|error| {
                eprintln!(
                    "ERROR: sbx-manager returned invalid sandbox id '{}': {error}",
                    sandbox.sandbox_id
                );
                exit(1);
            })
    }) {
        Some(OneOrMany::One(sandbox)) => sandbox,
        Some(OneOrMany::Many(matches)) => {
            eprintln!("Ambiguous sandbox id '{}', matches:", cli.id);
            for sandbox in &matches {
                eprintln!("  {}", sandbox_label(&sandbox.sandbox_id));
            }
            exit(1);
        }
        None => {
            pplx_space_common_cli::exit_message(format!("No sandbox matching '{}'", cli.id));
        }
    };
    let sandbox = client
        .state(StateRequest {
            sandbox_ids: vec![listed.sandbox_id.clone()],
        })
        .await
        .unwrap_or_else(|error| pplx_space_common_cli::exit_error(error))
        .into_inner()
        .sandboxes
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            pplx_space_common_cli::exit_message(format!(
                "Sandbox {} disappeared before state lookup.",
                sandbox_label(&listed.sandbox_id)
            ))
        });

    let lifecycle = sandbox
        .observed_lifecycle()
        .unwrap_or_else(|error| pplx_space_common_cli::exit_error(error));
    if !sandbox.pid.is_some_and(is_alive) {
        eprintln!(
            "Sandbox {} is not running (state: {}).",
            sandbox_label(&sandbox.sandbox_id),
            lifecycle.label()
        );
        exit(1);
    }

    let guest_os = sandbox
        .execution
        .as_ref()
        .map(ExecutionMetadata::try_from)
        .transpose()
        .unwrap_or_else(|error| pplx_space_common_cli::exit_error(error))
        .map(|execution| execution.guest().os)
        .unwrap_or(GuestOperatingSystem::Linux);
    match guest_os {
        GuestOperatingSystem::Windows | GuestOperatingSystem::Macos => {
            let sandbox_id = sandbox
                .sandbox_id
                .parse::<SandboxId>()
                .unwrap_or_else(|error| pplx_space_common_cli::exit_error(error));
            // Do not request Linux guest environment injection or enter a
            // caller-selected namespace. The node channel resolves and fences
            // its endpoint; OpenSSH authenticates the guest independently.
            let mut command = native_ssh_command(&cli, sandbox_id, guest_os);
            exit_with_command(&mut command);
        }
        GuestOperatingSystem::Linux => {}
    }

    let ns = sandbox.namespace.as_deref().unwrap_or_else(|| {
        eprintln!(
            "Sandbox {} has no network namespace.",
            sandbox_label(&sandbox.sandbox_id)
        );
        exit(1);
    });
    let sandbox_env = client
        .get_env(GetEnvRequest {
            sandbox_id: sandbox.sandbox_id.clone(),
        })
        .await
        .unwrap_or_else(|error| {
            pplx_space_common_cli::exit_error(format!("failed to fetch sandbox env: {error}"))
        })
        .into_inner()
        .env;
    let (sandbox_env, issues) = pplx_space_common_core::types::EnvMap::sanitize(sandbox_env);
    for issue in issues {
        eprintln!("WARNING: sandbox env sanitized: {issue:?}");
    }
    let sandbox_env: BTreeMap<String, String> = sandbox_env.into();

    exit_with_command(&mut linux_ssh_command(&cli, ns, &sandbox_env));
}

fn linux_ssh_command(cli: &Cli, ns: &str, sandbox_env: &BTreeMap<String, String>) -> Command {
    let user = cli.user.as_deref().unwrap_or(DEFAULT_GUEST_USER);
    let mut command = Command::new("sudo");
    command
        .arg("nsenter")
        .arg(format!("--net=/var/run/netns/{ns}"))
        .args(["ssh", "-i"])
        .arg(cli.identity_file.as_deref().unwrap_or(Path::new(SSH_KEY)))
        .args([
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-l",
            user,
        ]);
    for (key, value) in sandbox_env {
        let Some(value) = openssh_set_env_value(value) else {
            eprintln!("WARNING: skipping sandbox env '{key}' for ssh: value contains newline");
            continue;
        };
        command.arg("-o").arg(format!("SetEnv={key}={value}"));
    }
    command.arg(GUEST_IP.to_string()).args(&cli.command);
    command
}

fn native_ssh_command(cli: &Cli, sandbox_id: SandboxId, guest_os: GuestOperatingSystem) -> Command {
    let mut command = Command::new("ssh");
    command
        .arg("-o")
        .arg(format!(
            "ProxyCommand=sudo -n /space/bin/space-nodemgr guest-channel {sandbox_id} --port 22"
        ))
        .args(["-o", "StrictHostKeyChecking=yes"]);
    if let Some(identity) = &cli.identity_file {
        command.arg("-i").arg(identity);
    }
    let user = cli.user.as_deref().or(match guest_os {
        GuestOperatingSystem::Windows => Some("Administrator"),
        GuestOperatingSystem::Linux | GuestOperatingSystem::Macos => None,
    });
    if let Some(user) = user {
        command.args(["-l", user]);
    }
    // A sandbox-specific SSH host name keeps keys separate even though every
    // guest uses the same address. Credentials and known_hosts belong to the
    // calling operator, not to root or to a hard-coded fleet key.
    command
        .arg(format!("space-{sandbox_id}"))
        .args(&cli.command);
    command
}

fn exit_with_command(command: &mut Command) -> ! {
    let status = command.status().unwrap_or_else(|error| {
        pplx_space_common_cli::exit_error(format!("failed to run ssh: {error}"))
    });

    exit(status.code().unwrap_or(1))
}

fn is_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

fn openssh_set_env_value(value: &str) -> Option<String> {
    if value.contains('\n') || value.contains('\r') {
        return None;
    }
    if value
        .chars()
        .all(|ch| !ch.is_whitespace() && ch != '"' && ch != '\\')
    {
        return Some(value.to_string());
    }

    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    Some(quoted)
}

#[cfg(test)]
mod tests;
