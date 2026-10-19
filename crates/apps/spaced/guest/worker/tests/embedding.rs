//! Exercises a host that has no guest-server dependency and a separate worker binary.
#![cfg(test)]

#[cfg(target_os = "linux")]
#[path = "embedding/incompatible.rs"]
mod incompatible;
#[cfg(target_os = "linux")]
#[path = "embedding/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
fn main() {
    // An intentionally wrong artifact used to test cancellation during startup.
    // The successful path must execute the dedicated binary, never this branch.
    if pplx_space_spaced_guest_execution::filesystem_worker_requested() {
        if std::env::var("SPACE_SPACED_FILESYSTEM_WORKER_USER").as_deref() == Ok("root") {
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
