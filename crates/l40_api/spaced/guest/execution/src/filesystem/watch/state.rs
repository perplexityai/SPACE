use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::event::WatchEvent;
use super::filter::{WatchFilter, WatchFilterConfig};
use super::ledger::Ledger;
use super::{MAX_WATCHED_DIRECTORIES, PULL_WATCHER_TTL};

#[derive(Default)]
pub(super) struct WatchManagerState {
    pub(super) next_consumer_id: u64,
    pub(super) ledger_generation: u64,
    pub(super) next_root_generation: u64,
    pub(super) roots: HashMap<WatchRootKey, WatchRoot>,
    pub(super) pull_watchers: HashMap<String, WatchConsumerRef>,
    pub(super) ledger: LedgerState,
    pub(super) pull_reaper_running: bool,
}

#[derive(Default)]
pub(super) enum LedgerState {
    #[default]
    Disabled,
    Active(Ledger),
    Lost,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct WatchRootKey {
    pub(super) root: PathBuf,
    pub(super) recursive: bool,
    pub(super) include_metadata_events: bool,
    pub(super) filter_config: WatchFilterConfig,
}

pub(super) struct WatchRoot {
    pub(super) generation: WatchRootGeneration,
    pub(super) consumers: HashMap<u64, WatchConsumer>,
    pub(super) directory_count: usize,
    pub(super) discovering: bool,
    pub(super) shutdown: CancellationToken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WatchRootGeneration(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DirectoryAdjustment {
    Applied,
    BudgetExceeded,
    RootGone,
}

#[derive(Clone)]
pub(super) struct WatchConsumerRef {
    pub(super) root: WatchRootKey,
    pub(super) consumer_id: u64,
}

pub(super) enum WatchConsumer {
    Stream {
        root: PathBuf,
        recursive: bool,
        tx: mpsc::Sender<Result<super::QueuedFileEvent, tonic::Status>>,
        bytes: std::sync::Arc<tokio::sync::Semaphore>,
        shutdown: CancellationToken,
        filter: WatchFilter,
    },
    Pull {
        root: PathBuf,
        recursive: bool,
        events: VecDeque<WatchEvent>,
        retained_bytes: usize,
        overflowed: bool,
        last_access: Instant,
        filter: WatchFilter,
    },
    Ledger {
        root: PathBuf,
        filter: WatchFilter,
    },
}

pub(super) fn active_consumer_count(state: &WatchManagerState) -> usize {
    state.roots.values().map(|root| root.consumers.len()).sum()
}

pub(super) fn watched_directory_count(state: &WatchManagerState) -> usize {
    state.roots.values().map(|root| root.directory_count).sum()
}

pub(super) fn next_root_generation_locked(state: &mut WatchManagerState) -> WatchRootGeneration {
    state.next_root_generation = state.next_root_generation.wrapping_add(1);
    WatchRootGeneration(state.next_root_generation)
}

pub(super) fn root_for_backend_mut<'a>(
    state: &'a mut WatchManagerState,
    key: &WatchRootKey,
    generation: WatchRootGeneration,
) -> Option<&'a mut WatchRoot> {
    state
        .roots
        .get_mut(key)
        .filter(|root| root.generation == generation)
}

pub(super) fn remove_root_for_backend(
    state: &mut WatchManagerState,
    key: &WatchRootKey,
    generation: WatchRootGeneration,
) -> Option<WatchRoot> {
    let matches = state
        .roots
        .get(key)
        .is_some_and(|root| root.generation == generation);
    matches.then(|| state.roots.remove(key)).flatten()
}

pub(super) fn remove_consumer_locked(
    state: &mut WatchManagerState,
    key: &WatchRootKey,
    consumer_id: u64,
) -> Option<CancellationToken> {
    let empty = if let Some(root) = state.roots.get_mut(key) {
        root.consumers.remove(&consumer_id);
        root.consumers.is_empty()
    } else {
        false
    };

    if empty {
        state.roots.remove(key).map(|root| root.shutdown)
    } else {
        None
    }
}

pub(super) fn reap_expired_pull_watchers_locked(
    state: &mut WatchManagerState,
) -> Vec<CancellationToken> {
    let expired: Vec<(String, WatchConsumerRef)> = state
        .pull_watchers
        .iter()
        .filter_map(|(watcher_id, reference)| {
            let root = state.roots.get(&reference.root)?;
            let consumer = root.consumers.get(&reference.consumer_id)?;
            let WatchConsumer::Pull { last_access, .. } = consumer else {
                return None;
            };
            (last_access.elapsed() > PULL_WATCHER_TTL)
                .then(|| (watcher_id.clone(), reference.clone()))
        })
        .collect();

    let mut root_shutdowns = Vec::new();
    for (watcher_id, reference) in expired {
        state.pull_watchers.remove(&watcher_id);
        if let Some(shutdown) =
            remove_consumer_locked(state, &reference.root, reference.consumer_id)
        {
            root_shutdowns.push(shutdown);
        }
    }
    root_shutdowns
}

pub(super) fn directory_budget_exceeded(state: &WatchManagerState, directory_count: usize) -> bool {
    watched_directory_count(state).saturating_add(directory_count) > MAX_WATCHED_DIRECTORIES
}

pub(super) fn adjust_directories_locked(
    state: &mut WatchManagerState,
    key: &WatchRootKey,
    generation: WatchRootGeneration,
    reserve_count: usize,
    release_count: usize,
) -> DirectoryAdjustment {
    let Some(current_count) = state
        .roots
        .get(key)
        .filter(|root| root.generation == generation)
        .map(|root| root.directory_count)
    else {
        return DirectoryAdjustment::RootGone;
    };
    let released_count = release_count.min(current_count);
    let adjusted_total = watched_directory_count(state).saturating_sub(released_count);
    if adjusted_total.saturating_add(reserve_count) > MAX_WATCHED_DIRECTORIES {
        if let Some(root) = state.roots.get_mut(key) {
            root.directory_count = current_count - released_count;
        }
        return DirectoryAdjustment::BudgetExceeded;
    }
    let root = state
        .roots
        .get_mut(key)
        .expect("root generation checked while holding watch state lock");
    root.directory_count = current_count - released_count + reserve_count;
    DirectoryAdjustment::Applied
}

#[cfg(test)]
mod tests;
