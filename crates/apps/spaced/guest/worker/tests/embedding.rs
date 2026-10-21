//! Exercises a host that has no guest-server dependency and a separate worker binary.
#![cfg(test)]

#[cfg(target_os = "linux")]
#[path = "embedding/identity.rs"]
mod identity;
#[cfg(target_os = "linux")]
#[path = "embedding/incompatible.rs"]
mod incompatible;
#[cfg(target_os = "linux")]
#[path = "embedding/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
fn main() {
    if identity::probe() {
        return;
    }
    // An intentionally wrong artifact used to test cancellation during startup.
    // The successful path must execute the dedicated binary, never this branch.
    if pplx_space_spaced_guest_execution::filesystem_worker_requested() {
        if std::env::current_exe()
            .expect("test executable")
            .file_name()
            == Some(std::ffi::OsStr::new("incompatible-worker"))
        {
            incompatible::run();
            return;
        }
        loop {
            std::thread::park();
        }
    }
    if std::env::args().any(|arg| arg == "--list") {
        if !std::env::args().any(|arg| arg == "--ignored") {
            println!("embedding: test");
        }
        return;
    }
    if !pplx_space_test_env::root_or_skip("identity-worker embedding") {
        return;
    }
    let threaded = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .arg("--threaded-worker-probe")
        .env("SPACE_SPACED_FILESYSTEM_WORKER", "1")
        .env("SPACE_SPACED_FILESYSTEM_WORKER_USER", "nobody")
        .env(
            "SPACE_SPACED_FILESYSTEM_WORKER_IDENTITY_POLICY",
            "restricted",
        )
        .status()
        .expect("threaded worker probe");
    assert!(threaded.success(), "late worker dispatch must fail closed");
    identity::prepare_host();
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(30), linux::run())
                .await
                .expect("embedding deadline");
        });
}

#[cfg(not(target_os = "linux"))]
fn main() {}
