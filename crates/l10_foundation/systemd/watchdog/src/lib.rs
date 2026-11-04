//! systemd watchdog notifier for SPACE services.
//!
//! When systemd sets `WATCHDOG_USEC` and `NOTIFY_SOCKET` for this process,
//! `install_watchdog` spawns a task on the current tokio runtime that sends
//! `WATCHDOG=1` to the notify socket at half the watchdog interval. It is its
//! own crate so a service can arm the watchdog without the rest of
//! `pplx-space-common-svc`.

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::time::Duration;

use std::os::unix::net::UnixDatagram;

const WATCHDOG_ENV: &str = "WATCHDOG_USEC";
const WATCHDOG_PID_ENV: &str = "WATCHDOG_PID";
const NOTIFY_SOCKET_ENV: &str = "NOTIFY_SOCKET";

#[derive(Debug)]
struct WatchdogConfig {
    notify_socket: OsString,
    interval: Duration,
}

pub fn install_watchdog(service_name: &str) {
    let Some(config) = WatchdogConfig::from_env() else {
        return;
    };
    let service_name = service_name.to_string();

    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::warn!(
            service_name,
            "systemd watchdog requested, but no tokio runtime is active"
        );
        return;
    };

    handle.spawn(async move {
        run_watchdog(service_name, config).await;
    });
}

impl WatchdogConfig {
    fn from_env() -> Option<Self> {
        let notify_socket = std::env::var_os(NOTIFY_SOCKET_ENV)?;
        if notify_socket.is_empty() {
            tracing::warn!("systemd watchdog requested without a notify socket");
            return None;
        }

        if !watchdog_pid_matches_current(std::env::var_os(WATCHDOG_PID_ENV).as_deref()) {
            tracing::debug!("systemd watchdog belongs to a different process");
            return None;
        }

        let watchdog_usec = std::env::var(WATCHDOG_ENV).ok()?;
        let Some(interval) = watchdog_interval(&watchdog_usec) else {
            tracing::warn!(
                watchdog_usec = %watchdog_usec,
                "invalid systemd watchdog interval"
            );
            return None;
        };

        Some(Self {
            notify_socket,
            interval,
        })
    }
}

async fn run_watchdog(service_name: String, config: WatchdogConfig) {
    let notifier = match SystemdNotifier::new(config.notify_socket) {
        Ok(notifier) => notifier,
        Err(error) => {
            tracing::warn!(%error, "failed to initialize systemd watchdog notifier");
            return;
        }
    };
    let payload = format!("WATCHDOG=1\nSTATUS={service_name} alive");
    let mut failed = false;
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + config.interval,
        config.interval,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(
        service_name,
        interval_ms = config.interval.as_millis(),
        "systemd watchdog enabled"
    );

    match notifier.notify(payload.as_bytes()) {
        Ok(()) => {}
        Err(error) => {
            tracing::warn!(%error, "failed to notify systemd watchdog");
            failed = true;
        }
    }

    loop {
        interval.tick().await;
        match notifier.notify(payload.as_bytes()) {
            Ok(()) => {
                if failed {
                    tracing::info!("systemd watchdog notification recovered");
                    failed = false;
                }
            }
            Err(error) if !failed => {
                tracing::warn!(%error, "failed to notify systemd watchdog");
                failed = true;
            }
            Err(_) => {}
        }
    }
}

fn watchdog_interval(raw: &str) -> Option<Duration> {
    let watchdog_usec = raw.parse::<u64>().ok()?;
    if watchdog_usec == 0 {
        return None;
    }
    Some(Duration::from_micros((watchdog_usec / 2).max(1)))
}

fn watchdog_pid_matches_current(raw: Option<&OsStr>) -> bool {
    let Some(raw) = raw else {
        return true;
    };
    let Ok(pid) = raw.to_string_lossy().parse::<u32>() else {
        return false;
    };
    pid == std::process::id()
}

struct SystemdNotifier {
    socket: UnixDatagram,
    notify_socket: OsString,
}

impl SystemdNotifier {
    fn new(notify_socket: OsString) -> std::io::Result<Self> {
        Ok(Self {
            socket: UnixDatagram::unbound()?,
            notify_socket,
        })
    }

    fn notify(&self, payload: &[u8]) -> std::io::Result<()> {
        send_notify_payload(&self.socket, &self.notify_socket, payload).map(|_| ())
    }
}

#[cfg(target_os = "linux")]
fn send_notify_payload(
    socket: &UnixDatagram,
    notify_socket: &OsStr,
    payload: &[u8],
) -> std::io::Result<usize> {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::SocketAddr;

    let socket_bytes = notify_socket.as_bytes();
    if let Some(abstract_name) = socket_bytes.strip_prefix(b"@") {
        let address = SocketAddr::from_abstract_name(abstract_name)?;
        socket.send_to_addr(payload, &address)
    } else {
        socket.send_to(payload, Path::new(notify_socket))
    }
}

#[cfg(not(target_os = "linux"))]
fn send_notify_payload(
    socket: &UnixDatagram,
    notify_socket: &OsStr,
    payload: &[u8],
) -> std::io::Result<usize> {
    socket.send_to(payload, Path::new(notify_socket))
}

#[cfg(test)]
mod tests;
