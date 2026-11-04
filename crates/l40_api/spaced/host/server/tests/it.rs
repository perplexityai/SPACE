//! Host relay integration tests and shared transport fixtures.
#![cfg(test)]

#[path = "support/bridge_host.rs"]
mod bridge_host;
#[path = "support/duration.rs"]
mod duration;
#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/mock_bridge_guest.rs"]
mod mock_bridge_guest;
#[path = "support/time.rs"]
mod time;

#[path = "it/filesystem.rs"]
mod filesystem;
#[path = "it/meta.rs"]
mod meta;
#[path = "it/process.rs"]
mod process;
