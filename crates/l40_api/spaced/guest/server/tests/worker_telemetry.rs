//! Real Linux worker launch, credential switch and periodic OTLP export.
//! Ignored by default: the real credential switch requires root. Run on Linux:
//! ```sh
//! cargo test -p pplx-space-spaced-guest-server --all-features --test worker_telemetry \
//!   --config 'target.x86_64-unknown-linux-gnu.runner=["sudo","-n"]' -- --ignored
//! ```
//! Cargo stays unprivileged; only the test executable is elevated. CI includes
//! ignored tests, so missing privileges there fail instead of skipping coverage.
#![cfg(test)]

#[cfg(target_os = "linux")]
#[path = "worker_telemetry/linux.rs"]
mod linux;

fn main() {
    #[cfg(target_os = "linux")]
    linux::main();
}
