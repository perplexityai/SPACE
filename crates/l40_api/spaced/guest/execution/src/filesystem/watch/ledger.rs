use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    FileEventKind, GetModifiedFilesRequest, GetModifiedFilesResponse, ModifiedFile,
};
use tonic::Status;

use super::event::{WatchEvent, WatchPathEncoding};
use super::{
    DEFAULT_WINDOW_MS, LEDGER_BYTE_CAPACITY, LEDGER_PATH_CAPACITY, LEDGER_RETENTION,
    WatchConsumerRef, duration_to_ms, now_unix_ms, timestamp_to_unix_ms, unix_ms_to_timestamp,
};

pub(super) struct Ledger {
    pub(super) session_id: u64,
    pub(super) observed_since_unix_ms: u64,
    observed_at: Instant,
    last_pruned_at: Instant,
    retained_since_unix_ms: u64,
    wall_time_high_watermark: u64,
    pub(super) root_consumers: Vec<WatchConsumerRef>,
    events: VecDeque<LedgerEvent>,
    files: BTreeMap<LedgerFileKey, LedgerFile>,
    file_bytes: usize,
    overflowed_since_unix_ms: Option<u64>,
}

#[derive(Clone)]
struct LedgerEvent {
    observed_unix_ms: u64,
    observed_at: Instant,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct LedgerFileKey {
    root: PathBuf,
    relative_path: PathBuf,
}

struct LedgerFile {
    last_modified_unix_ms: u64,
    kinds: Vec<FileEventKind>,
}

impl LedgerFile {
    fn kind_bytes(&self) -> usize {
        self.kinds.capacity() * std::mem::size_of::<FileEventKind>()
    }
}

const LEDGER_PRUNE_INTERVAL: Duration = Duration::from_secs(1);

impl Ledger {
    pub(super) fn new(session_id: u64, observed_since_unix_ms: u64) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            observed_since_unix_ms,
            observed_at: now,
            last_pruned_at: now,
            retained_since_unix_ms: observed_since_unix_ms,
            wall_time_high_watermark: observed_since_unix_ms,
            root_consumers: Vec::new(),
            events: VecDeque::new(),
            files: BTreeMap::new(),
            file_bytes: 0,
            overflowed_since_unix_ms: None,
        }
    }

    pub(super) fn record_event(&mut self, root: &Path, event: &WatchEvent, is_regular_file: bool) {
        let kind = event.kind();
        let observed_unix_ms = event.observed_unix_ms();
        match event.path_encoding() {
            WatchPathEncoding::Exact => {}
            WatchPathEncoding::Lossy => self.record_loss(observed_unix_ms),
            WatchPathEncoding::Unsupported => {
                self.record_loss(observed_unix_ms);
                return;
            }
        }
        self.observe_wall_time(observed_unix_ms);
        let observed_at = Instant::now();
        self.events.push_back(LedgerEvent {
            observed_unix_ms,
            observed_at,
        });

        let Some(key) = ledger_file_key(root, event) else {
            self.enforce_capacity(observed_unix_ms);
            return;
        };
        if kind == FileEventKind::Removed {
            if let Some(file) = self.files.remove(&key) {
                self.file_bytes -= key.retained_bytes() + file.kind_bytes();
            }
        } else if is_regular_file {
            let bytes = key.retained_bytes();
            let entry = self.files.entry(key).or_insert_with(|| {
                self.file_bytes += bytes;
                LedgerFile {
                    last_modified_unix_ms: observed_unix_ms,
                    kinds: Vec::new(),
                }
            });
            entry.last_modified_unix_ms = entry.last_modified_unix_ms.max(observed_unix_ms);
            if !entry.kinds.contains(&kind) {
                let previous_bytes = entry.kind_bytes();
                entry.kinds.push(kind);
                self.file_bytes += entry.kind_bytes() - previous_bytes;
            }
        }
        let retention_advanced = self.enforce_capacity(observed_unix_ms);
        self.prune(false, retention_advanced);
    }

    pub(super) fn modified_files_response(
        &mut self,
        request: GetModifiedFilesRequest,
    ) -> Result<GetModifiedFilesResponse, Status> {
        let filters = [
            request.window.is_some(),
            request.since.is_some(),
            request.cursor.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if filters > 1 {
            return Err(Status::invalid_argument(
                "window, since, and cursor are mutually exclusive",
            ));
        }

        let now = now_unix_ms()?;
        self.observe_wall_time(now);
        self.prune(true, false);
        let retained_since_unix_ms = self.retained_since_unix_ms();
        let since_unix_ms = if let Some(window) = request.window {
            now.saturating_sub(duration_to_ms(window)?)
        } else if let Some(since) = request.since {
            timestamp_to_unix_ms(since)?
        } else if let Some(cursor) = request.cursor {
            parse_cursor(&cursor)?
        } else {
            now.saturating_sub(DEFAULT_WINDOW_MS)
        };
        let range_start = since_unix_ms.max(retained_since_unix_ms);
        let files = self
            .files
            .iter()
            .filter(|(_, file)| file.last_modified_unix_ms >= range_start)
            .map(|(key, file)| ModifiedFile {
                root: key.root.to_string_lossy().into_owned(),
                path: key.relative_path.to_string_lossy().into_owned(),
                last_modified_at: Some(unix_ms_to_timestamp(file.last_modified_unix_ms)),
                kinds: file.kinds.iter().copied().map(Into::into).collect(),
            })
            .collect();
        let overflowed = since_unix_ms < retained_since_unix_ms
            || self
                .overflowed_since_unix_ms
                .is_some_and(|overflow_since| since_unix_ms <= overflow_since);
        Ok(GetModifiedFilesResponse {
            files,
            observed_since: Some(unix_ms_to_timestamp(self.observed_since_unix_ms)),
            retained_since: Some(unix_ms_to_timestamp(retained_since_unix_ms)),
            cursor: Some(self.wall_time_high_watermark.to_string()),
            monitoring_enabled: true,
            overflowed,
        })
    }

    pub(super) fn record_loss(&mut self, observed_unix_ms: u64) {
        self.observe_wall_time(observed_unix_ms);
        self.overflowed_since_unix_ms = Some(
            self.overflowed_since_unix_ms
                .unwrap_or(0)
                .max(self.wall_time_high_watermark),
        );
    }

    fn observe_wall_time(&mut self, observed_unix_ms: u64) {
        // Cursors never move backwards. A realtime discontinuity can hide new
        // observations behind an issued cursor, so retain loss through that
        // cursor without changing monotonic retention or resetting the ledger.
        let previous = self
            .wall_time_high_watermark
            .max(self.retained_since_unix_ms);
        if observed_unix_ms < previous {
            self.overflowed_since_unix_ms =
                Some(self.overflowed_since_unix_ms.unwrap_or(0).max(previous));
        }
        self.wall_time_high_watermark = previous.max(observed_unix_ms);
    }

    fn retained_since_unix_ms(&self) -> u64 {
        self.retained_since_unix_ms
    }

    fn prune(&mut self, force: bool, mut retention_advanced: bool) {
        let now = Instant::now();
        if force || now.duration_since(self.last_pruned_at) >= LEDGER_PRUNE_INTERVAL {
            self.last_pruned_at = now;
            let cutoff = now
                .checked_sub(LEDGER_RETENTION)
                .unwrap_or(self.observed_at);
            while self
                .events
                .front()
                .is_some_and(|event| event.observed_at < cutoff)
            {
                self.discard_oldest_observation();
                retention_advanced = true;
            }
        }
        if retention_advanced {
            let retained_since_unix_ms = self.retained_since_unix_ms();
            self.files.retain(|key, file| {
                let retain = file.last_modified_unix_ms >= retained_since_unix_ms;
                if !retain {
                    self.file_bytes -= key.retained_bytes() + file.kind_bytes();
                }
                retain
            });
        }
    }

    fn discard_oldest_observation(&mut self) {
        if let Some(event) = self.events.pop_front() {
            // Inclusive timestamp queries can only be complete strictly after
            // a discarded observation, including when the history is empty.
            self.retained_since_unix_ms = self
                .retained_since_unix_ms
                .max(event.observed_unix_ms.saturating_add(1));
        }
    }

    fn enforce_capacity(&mut self, observed_unix_ms: u64) -> bool {
        let mut retention_advanced = false;
        while self.events.len() > LEDGER_PATH_CAPACITY {
            self.discard_oldest_observation();
            self.overflowed_since_unix_ms = Some(
                self.overflowed_since_unix_ms
                    .unwrap_or(0)
                    .max(observed_unix_ms),
            );
            retention_advanced = true;
        }
        while self.files.len() > LEDGER_PATH_CAPACITY || self.file_bytes > LEDGER_BYTE_CAPACITY {
            let Some(path) = self
                .files
                .iter()
                .min_by_key(|(_, file)| file.last_modified_unix_ms)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            if let Some(file) = self.files.remove(&path) {
                self.file_bytes -= path.retained_bytes() + file.kind_bytes();
            }
            self.overflowed_since_unix_ms = Some(
                self.overflowed_since_unix_ms
                    .unwrap_or(0)
                    .max(observed_unix_ms),
            );
        }
        retention_advanced
    }
}

impl LedgerFileKey {
    fn retained_bytes(&self) -> usize {
        // Include both owned paths and fixed record storage. Kind allocations
        // are charged by capacity as they grow; map overhead is count-bounded.
        self.root.as_os_str().as_encoded_bytes().len()
            + self.relative_path.as_os_str().as_encoded_bytes().len()
            + std::mem::size_of::<Self>()
            + std::mem::size_of::<LedgerFile>()
    }
}

fn ledger_file_key(root: &Path, event: &WatchEvent) -> Option<LedgerFileKey> {
    let absolute_path = event.absolute_path();
    let relative = absolute_path.strip_prefix(root).ok()?;
    let relative = if relative.as_os_str().is_empty() {
        Path::new(".")
    } else {
        relative
    };
    Some(LedgerFileKey {
        root: root.to_path_buf(),
        relative_path: relative.to_path_buf(),
    })
}

fn parse_cursor(cursor: &str) -> Result<u64, Status> {
    cursor
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("cursor is invalid"))
}

#[cfg(test)]
mod tests;
