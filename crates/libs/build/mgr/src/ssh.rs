use std::collections::HashMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::command_as_root;
use pplx_space_common_core::types::DEFAULT_GUEST_USER;

const SSH_KEY_PATH: &str = "/root/.ssh/sandbox_ed25519";
const SSH_CONNECT_TIMEOUT: u32 = 60;
const SSH_EXEC_RETRIES: u32 = 3;

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum SshError {
    #[error(
        "builder SSH key not found at {path}. Generate one with: sudo mkdir -p /space/etc/builder && sudo ssh-keygen -t ed25519 -f {path} -N \"\""
    )]
    MissingKey { path: String },
    #[error("SSH to {host} not ready after {timeout_secs}s")]
    NotReady { host: String, timeout_secs: u64 },
    #[error("ssh command exited with status {0}")]
    CommandStatus(std::process::ExitStatus),
    #[error("ssh command failed to start: {0}")]
    CommandStart(#[source] std::io::Error),
    #[error("ssh command failed after retries")]
    CommandRetries,
    #[error("bounded guest operation timed out or exceeded its output limit")]
    BoundedOperation,
    #[error("copy archive transfer failed: {0}")]
    CopyIo(#[source] std::io::Error),
    #[error("copy payload error: {0}")]
    CopyPayload(String),
}

pub(crate) struct SshSession {
    host: String,
    ns: String,
    key_path: String,
}

impl SshSession {
    /// Small construction protocol, with an explicit deadline and output bound.
    /// Keep declaration bytes off command lines and inherited build logs.
    pub async fn exchange(
        &self,
        command: &str,
        input: &[u8],
        limit: usize,
    ) -> Result<Vec<u8>, SshError> {
        exchange_command(
            tokio::process::Command::from(self.ssh_cmd("root", command)),
            input,
            limit,
            Duration::from_secs(90),
        )
        .await
    }

    pub fn new(host: String, ns: String) -> Result<Self, SshError> {
        let key_path = SSH_KEY_PATH.to_string();
        let key_exists = command_as_root("test")
            .args(["-f", &key_path])
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !key_exists {
            return Err(SshError::MissingKey { path: key_path });
        }
        Ok(SshSession { host, ns, key_path })
    }

    /// Poll SSH until the guest is reachable, or timeout.
    pub fn wait_ready(&self, timeout: Duration) -> Result<(), SshError> {
        println!("Waiting for SSH on {}...", self.host);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(SshError::NotReady {
                    host: self.host.clone(),
                    timeout_secs: timeout.as_secs(),
                });
            }

            let ok = self
                .ssh_cmd(DEFAULT_GUEST_USER, "true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);

            if ok {
                println!("SSH ready.");
                return Ok(());
            }

            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    /// Execute a command inside the guest via SSH.
    /// stdout/stderr are inherited (printed in real time).
    pub fn exec(
        &self,
        command: &str,
        user: &str,
        env: &HashMap<String, String>,
    ) -> Result<(), SshError> {
        let remote_cmd = if env.is_empty() {
            command.to_string()
        } else {
            let env_prefix: String = env
                .iter()
                .map(|(k, v)| {
                    let escaped = v.replace('\'', "'\\''");
                    format!("{k}='{escaped}'")
                })
                .collect::<Vec<_>>()
                .join(" ");
            format!("env {env_prefix} bash -c {}", shell_quote(command))
        };

        // Retry on SSH connection failures (exit code 255) which can happen
        // when the guest is under heavy load (e.g. large VMs on nested virt).
        for attempt in 0..SSH_EXEC_RETRIES {
            let status = self
                .ssh_cmd(user, &remote_cmd)
                .stdin(Stdio::null())
                .status();
            match status {
                Ok(s) if s.success() => return Ok(()),
                Ok(s) if s.code() == Some(255) && attempt + 1 < SSH_EXEC_RETRIES => {
                    eprintln!(
                        "SSH connection failed (attempt {}/{}), retrying in 5s...",
                        attempt + 1,
                        SSH_EXEC_RETRIES
                    );
                    std::thread::sleep(std::time::Duration::from_secs(5));
                }
                Ok(s) => return Err(SshError::CommandStatus(s)),
                Err(e) => return Err(SshError::CommandStart(e)),
            }
        }
        Err(SshError::CommandRetries)
    }

    /// Transfer private, immutable staging without dereferencing host symlinks.
    pub fn copy(&self, source: &str, dest: &str, user: &str) -> Result<(), SshError> {
        send_copy_archive(
            Path::new(source),
            self.ssh_cmd(user, &copy_unpack_command(dest)),
        )
    }

    /// Build an SSH Command that runs inside the network namespace.
    fn ssh_cmd(&self, user: &str, remote_cmd: &str) -> Command {
        let mut cmd = command_as_root("nsenter");
        cmd.args([
            &format!("--net=/var/run/netns/{}", self.ns),
            "ssh",
            "-i",
            &self.key_path,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-o",
            &format!("ConnectTimeout={SSH_CONNECT_TIMEOUT}"),
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=10",
            // `user` arrives from a tenant's template step, so it must never sit
            // where the local client is still reading options: a leading `-`
            // there configures this process (e.g. ProxyCommand) instead of
            // naming a guest account. `-l` consumes it as a value and `--` ends
            // option parsing, leaving the destination fixed to the trusted host.
            "-l",
            user,
            "--",
            &self.host,
            remote_cmd,
        ]);
        cmd
    }
}

async fn exchange_command(
    mut command: tokio::process::Command,
    input: &[u8],
    limit: usize,
    timeout: Duration,
) -> Result<Vec<u8>, SshError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(SshError::CommandStart)?;
    let mut stdin = child.stdin.take().ok_or(SshError::BoundedOperation)?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or(SshError::BoundedOperation)?
        .take(limit as u64 + 1);
    let operation = async {
        let send = async move {
            stdin
                .write_all(input)
                .await
                .map_err(SshError::CommandStart)?;
            drop(stdin); // Pipe shutdown is a no-op on Unix; close to deliver EOF.
            Ok::<_, SshError>(())
        };
        let receive = async {
            let mut bytes = Vec::new();
            stdout
                .read_to_end(&mut bytes)
                .await
                .map_err(SshError::CommandStart)?;
            if bytes.len() > limit {
                return Err(SshError::BoundedOperation);
            }
            Ok(bytes)
        };
        let ((), bytes) = tokio::try_join!(send, receive)?;
        let status = child.wait().await.map_err(SshError::CommandStart)?;
        if !status.success() {
            return Err(SshError::CommandStatus(status));
        }
        Ok(bytes)
    };
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| SshError::BoundedOperation)?
}

fn copy_unpack_command(dest: &str) -> String {
    format!("tar -xpf - -C {}", shell_quote(dest))
}

fn send_copy_archive(source: &Path, mut command: Command) -> Result<(), SshError> {
    let mut child = command
        .stdin(Stdio::piped())
        .spawn()
        .map_err(SshError::CommandStart)?;
    let stdin = child.stdin.take().expect("piped stdin");
    let packed = pack_copy_archive(source, stdin);
    // Always close stdin and reap the consumer, including on a packing failure.
    let status = child.wait().map_err(SshError::CopyIo)?;
    packed.map_err(SshError::CopyIo)?;
    if !status.success() {
        return Err(SshError::CommandStatus(status));
    }
    Ok(())
}

fn pack_copy_archive(source: &Path, writer: impl std::io::Write) -> std::io::Result<()> {
    let name = source.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "copy source has no filename",
        )
    })?;
    let mut archive = tar::Builder::new(writer);
    // scp -r follows links, including chains that lexically appear contained.
    archive.follow_symlinks(false);
    if source.symlink_metadata()?.is_dir() {
        archive.append_dir_all(name, source)?;
    } else {
        archive.append_path_with_name(source, name)?;
    }
    archive.finish()
}

/// Shell-quote a string for safe embedding in `bash -c '...'`.
fn shell_quote(s: &str) -> String {
    let escaped = s.replace('\'', "'\\''");
    format!("'{escaped}'")
}
