use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

pub(crate) fn spawn_tracked_child_reaper(
    _fork_children: Arc<Mutex<HashSet<u32>>>,
    _shutdown: CancellationToken,
    _tasks: &tokio_util::task::TaskTracker,
) -> std::io::Result<()> {
    Ok(())
}

pub(crate) fn process_matches_binary(_pid: u32, _binary: &str) -> bool {
    false
}
