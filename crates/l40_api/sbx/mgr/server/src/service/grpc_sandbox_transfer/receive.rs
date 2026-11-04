//! Validation and durable writes shared by the two sparse transfer receivers.

use std::collections::BTreeMap;
use std::path::Path;

use pplx_space_common_core::sandbox_storage::MAX_SANDBOX_ROOTFS_BYTES;

use sha2::Digest;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tonic::Status;

#[cfg(test)]
mod tests;

const MAX_TRANSFER_FILES: usize = 256;
// Receiver-owned limits: peer-declared sizes are not resource authorization.
// Count logical extents too, so sparse EOF markers cannot bypass the budget.
pub(crate) const MAX_TRANSFER_FILE_BYTES: u64 = 128 * 1024 * 1024 * 1024;
const MAX_TRANSFER_BYTES: u64 = 2 * MAX_TRANSFER_FILE_BYTES;

struct ImportLimits {
    file_bytes: u64,
    total_bytes: u64,
}

impl Default for ImportLimits {
    fn default() -> Self {
        Self {
            file_bytes: MAX_TRANSFER_FILE_BYTES,
            total_bytes: MAX_TRANSFER_BYTES,
        }
    }
}
// Import may add a header-only snapshot.json and its durable transfer receipt.
const MAX_STORED_SNAPSHOT_FILES: usize = MAX_TRANSFER_FILES + 2;

/// The terminal status is separate from the bounded data queue. A stalled
/// consumer must not force a producer to wait forever just to report timeout,
/// or turn a failed export into a clean EOF when the queue is full.
pub(crate) struct ExportSender<T> {
    data: tokio::sync::mpsc::Sender<Result<T, Status>>,
    outcome: std::sync::Arc<std::sync::Mutex<Option<Result<(), Status>>>>,
}

impl<T> Clone for ExportSender<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            outcome: self.outcome.clone(),
        }
    }
}

impl<T> ExportSender<T> {
    pub(crate) fn is_closed(&self) -> bool {
        self.data.is_closed()
    }

    pub(crate) async fn closed(&self) {
        self.data.closed().await;
    }

    pub(crate) fn finish(&self, result: Result<(), Status>) {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*outcome, Some(Err(_))) {
            *outcome = Some(result);
        }
    }

    pub(crate) fn blocking_send(&self, item: Result<T, Status>) -> Result<(), Status> {
        self.send_with_timeout(item, std::time::Duration::from_secs(60))
    }

    pub(crate) async fn send(&self, item: T) -> Result<(), Status> {
        self.send_async_with_timeout(Ok(item), std::time::Duration::from_secs(60))
            .await
    }

    fn send_with_timeout(
        &self,
        item: Result<T, Status>,
        timeout: std::time::Duration,
    ) -> Result<(), Status> {
        match item {
            Err(error) => {
                self.finish(Err(error.clone()));
                Err(error)
            }
            Ok(item) => tokio::runtime::Handle::current()
                .block_on(self.send_async_with_timeout(Ok(item), timeout)),
        }
    }

    async fn send_async_with_timeout(
        &self,
        item: Result<T, Status>,
        timeout: std::time::Duration,
    ) -> Result<(), Status> {
        let result = match item {
            Err(error) => Err(error),
            Ok(item) => {
                async {
                    tokio::time::timeout(timeout, self.data.send(Ok(item)))
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded(
                                "stream consumer made no progress for 60 seconds",
                            )
                        })?
                        .map_err(|_| Status::cancelled("stream consumer disconnected"))
                }
                .await
            }
        };
        if let Err(error) = &result {
            self.finish(Err(error.clone()));
        }
        result
    }
}

pub(crate) type ExportStream<T> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

pub(crate) fn export_channel<T: Send + 'static>() -> (ExportSender<T>, ExportStream<T>) {
    use futures_util::StreamExt;
    let (data, receiver) = tokio::sync::mpsc::channel(4);
    let outcome = std::sync::Arc::new(std::sync::Mutex::new(None));
    let sender = ExportSender {
        data,
        outcome: outcome.clone(),
    };
    let terminal = futures_util::stream::once(async move {
        let result = outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                Err(Status::internal(
                    "export producer exited without completion",
                ))
            });
        result.err().map(Err)
    })
    .filter_map(futures_util::future::ready);
    let stream = tokio_stream::wrappers::ReceiverStream::new(receiver).chain(terminal);
    (sender, Box::pin(stream))
}

pub(crate) fn sandbox_export_files(
    instance: &Path,
    snapshot: Option<&Path>,
    delta: Option<&Path>,
) -> Result<(Vec<(String, std::path::PathBuf)>, u64), Status> {
    let mut files = Vec::new();
    let mut bytes = 0_u64;
    for (directory, prefix) in [(Some(instance), ""), (snapshot, "snapshot/")] {
        let Some(directory) = directory else {
            continue;
        };
        for (index, entry) in std::fs::read_dir(directory)
            .map_err(|error| Status::internal(format!("read export directory: {error}")))?
            .enumerate()
        {
            if index >= MAX_TRANSFER_FILES || files.len() >= MAX_TRANSFER_FILES {
                return Err(Status::resource_exhausted("too many entries in export"));
            }
            let entry =
                entry.map_err(|error| Status::internal(format!("read export entry: {error}")))?;
            let kind = entry
                .file_type()
                .map_err(|error| Status::internal(format!("inspect export entry: {error}")))?;
            if kind.is_symlink() {
                return Err(Status::failed_precondition("export contains a symlink"));
            }
            if !kind.is_file() {
                continue;
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Status::failed_precondition("export filename is not UTF-8"))?;
            let (name, path) = match (prefix.is_empty() && name == "rootfs.ext4", delta) {
                (true, Some(delta)) => ("rootfs.ext4.diff".to_owned(), delta.to_owned()),
                _ => (format!("{prefix}{name}"), entry.path()),
            };
            let size = std::fs::metadata(&path)
                .map_err(|error| Status::internal(format!("stat export file: {error}")))?
                .len();
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| Status::out_of_range("export size overflow"))?;
            files.push((name, path));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((files, bytes))
}

/// Bounds admission and receiving even when a peer keeps sending chunks.
/// Filesystem work is awaited to completion, not cancelled while it may still
/// hold open files; expiry is observed at the next safe receive boundary.
#[derive(Clone, Copy)]
pub(crate) struct ImportDeadline(tokio::time::Instant);

impl ImportDeadline {
    pub(crate) fn new() -> Self {
        Self(tokio::time::Instant::now() + std::time::Duration::from_secs(60 * 60))
    }

    pub(crate) async fn wait<T>(
        &self,
        future: impl std::future::Future<Output = T>,
    ) -> Result<T, Status> {
        if tokio::time::Instant::now() >= self.0 {
            return Err(Status::deadline_exceeded(
                "import admission and receive deadline exceeded",
            ));
        }
        tokio::time::timeout_at(self.0, future).await.map_err(|_| {
            Status::deadline_exceeded("import admission and receive deadline exceeded")
        })
    }
}

pub(crate) async fn next_message<T>(
    stream: &mut (impl tokio_stream::Stream<Item = Result<T, Status>> + Unpin),
    deadline: ImportDeadline,
) -> Result<Option<T>, Status> {
    use tokio_stream::StreamExt;
    deadline
        .wait(tokio::time::timeout(
            std::time::Duration::from_secs(60),
            stream.next(),
        ))
        .await?
        .map_err(|_| Status::deadline_exceeded("import stream made no progress for 60 seconds"))?
        .transpose()
}

pub(crate) fn create_transfer_directory(path: &Path) -> Result<(), Status> {
    #[cfg(target_os = "linux")]
    {
        let parent = path
            .parent()
            .ok_or_else(|| Status::invalid_argument("transfer directory has no parent"))?;
        let filesystem = nix::sys::statfs::statfs(parent)
            .map_err(|error| Status::internal(format!("inspect transfer filesystem: {error}")))?;
        if filesystem.filesystem_type() == nix::sys::statfs::BTRFS_SUPER_MAGIC {
            return pplx_space_common_core::host::btrfs::create_subvolume(path)
                .map_err(|error| Status::internal(format!("create transfer subvolume: {error}")));
        }
    }
    std::fs::create_dir(path)
        .map_err(|error| Status::internal(format!("create transfer directory: {error}")))
}

pub(crate) fn remove_transfer_directory(path: &Path) -> std::io::Result<()> {
    if pplx_space_common_core::host::btrfs::is_subvolume(path) {
        pplx_space_common_core::host::btrfs::delete_subvolume_if_present(path)
            .map_err(std::io::Error::other)
    } else {
        std::fs::remove_dir_all(path)
    }
}

/// Compare outside the SQLite lock. The caller retains artifact exclusion;
/// publication rechecks the durable binding and namespace under its transaction.
pub(crate) fn verify_existing_snapshot(
    staging: &Path,
    destination: &Path,
) -> std::io::Result<bool> {
    use std::io::Read;
    match std::fs::symlink_metadata(destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "snapshot destination is not an owned directory",
            ));
        }
        Err(error) => return Err(error),
    }
    let names = |path: &Path| -> std::io::Result<BTreeMap<std::ffi::OsString, u64>> {
        let mut names = BTreeMap::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() || names.len() >= MAX_STORED_SNAPSHOT_FILES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot contains unexpected entries",
                ));
            }
            names.insert(entry.file_name(), metadata.len());
        }
        Ok(names)
    };
    let staged_names = names(staging)?;
    if staged_names != names(destination)? {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "snapshot file manifest differs",
        ));
    }
    let mut staged_buffer = vec![0; 512 * 1024];
    let mut existing_buffer = vec![0; 512 * 1024];
    for name in staged_names.keys() {
        let mut staged = std::fs::File::open(staging.join(name))?;
        let mut existing = std::fs::File::open(destination.join(name))?;
        loop {
            let count = staged.read(&mut staged_buffer)?;
            if count == 0 {
                break;
            }
            let staged_data = staged_buffer.get(..count).ok_or_else(|| {
                std::io::Error::other("staged file read exceeded comparison buffer")
            })?;
            let existing_data = existing_buffer.get_mut(..count).ok_or_else(|| {
                std::io::Error::other("staged file read exceeded comparison buffer")
            })?;
            existing.read_exact(existing_data)?;
            if staged_data != existing_data {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "snapshot contents differ",
                ));
            }
        }
    }
    Ok(true)
}

pub(crate) fn stream_sparse_file(
    path: &Path,
    required: bool,
    cancelled: impl Fn() -> bool,
    mut send: impl FnMut(u64, Vec<u8>, bool) -> Result<(), Status>,
) -> Result<(), Status> {
    use std::io::Read;
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if !required && error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Status::internal(format!("open export file: {error}"))),
    };
    let length = file
        .metadata()
        .map_err(|error| Status::internal(format!("stat export file: {error}")))?
        .len();
    let mut file = std::io::BufReader::new(file);
    let mut offset = 0_u64;
    let mut buffer = vec![0; 512 * 1024];
    loop {
        if cancelled() {
            return Err(Status::cancelled("export consumer disconnected"));
        }
        let count = file
            .read(&mut buffer)
            .map_err(|error| Status::internal(format!("read export file: {error}")))?;
        if count == 0 {
            if offset != length {
                return Err(Status::data_loss("export file changed during transfer"));
            }
            return send(length, Vec::new(), true);
        }
        let data = buffer
            .get(..count)
            .ok_or_else(|| Status::internal("export read exceeded buffer"))?;
        if data.iter().any(|byte| *byte != 0) {
            send(offset, data.to_vec(), false)?;
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| Status::out_of_range("export file size overflow"))?;
    }
}

#[derive(Default)]
pub(crate) struct SparseImport {
    current: Option<(String, tokio::fs::File, u64)>,
    completed: BTreeMap<String, u64>,
    bytes: u64,
    logical_bytes: u64,
    delta_expansion_reserved: bool,
    limits: ImportLimits,
    fingerprint: sha2::Sha256,
}

impl SparseImport {
    pub(super) fn sandbox(is_delta: bool) -> Self {
        let mut receiver = Self::default();
        if is_delta {
            // The compressed input and expanded rootfs coexist until apply
            // succeeds. Reserve the maximum expansion before receiving input.
            receiver.limits.total_bytes -= MAX_SANDBOX_ROOTFS_BYTES;
            receiver.delta_expansion_reserved = true;
        }
        receiver
    }

    pub(crate) async fn write(
        &mut self,
        name: &str,
        path: &Path,
        offset: u64,
        data: &[u8],
        eof: bool,
    ) -> Result<(), Status> {
        if self.completed.contains_key(name) {
            return Err(Status::data_loss("file repeated after eof"));
        }
        if self
            .current
            .as_ref()
            .is_some_and(|(current, _, _)| current != name)
        {
            return Err(Status::data_loss("transfer switched files before eof"));
        }
        let next = offset
            .checked_add(data.len() as u64)
            .filter(|value| *value <= i64::MAX as u64)
            .ok_or_else(|| Status::out_of_range("transfer offset overflow"))?;
        let end = self.current.as_ref().map_or(0, |(_, _, end)| *end);
        if offset < end || (eof && !data.is_empty()) || (!eof && data.is_empty()) {
            return Err(Status::data_loss(
                "overlapping data, empty data, or invalid eof",
            ));
        }
        let logical_bytes = self.logical_bytes.checked_add(next - end);
        let bytes = self.bytes.checked_add(data.len() as u64);
        // Snapshot transfers announce delta encoding by filename, possibly
        // after other files. Account for the reconstructed rootfs as well as
        // all received extents before creating or extending any file.
        let reserve_delta = name == "rootfs.ext4.diff" && !self.delta_expansion_reserved;
        let total_limit = if reserve_delta {
            self.limits
                .total_bytes
                .saturating_sub(self.limits.file_bytes)
        } else {
            self.limits.total_bytes
        };
        let file_limit = if matches!(name, "rootfs.ext4" | "snapshot/rootfs.ext4") {
            self.limits.file_bytes.min(MAX_SANDBOX_ROOTFS_BYTES)
        } else {
            self.limits.file_bytes
        };
        if next > file_limit
            || logical_bytes.is_none_or(|size| size > total_limit)
            || bytes.is_none_or(|size| size > total_limit)
        {
            return Err(Status::resource_exhausted("transfer size limit exceeded"));
        }
        if reserve_delta {
            self.limits.total_bytes = total_limit;
            self.delta_expansion_reserved = true;
        }
        if self.current.is_none() {
            if self.completed.len() >= MAX_TRANSFER_FILES {
                return Err(Status::resource_exhausted("too many files in transfer"));
            }
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .await
                .map_err(|error| {
                    Status::internal(format!("create transfer file {name}: {error}"))
                })?;
            self.current = Some((name.to_owned(), file, 0));
        }
        let Some((_, file, end)) = self.current.as_mut() else {
            return Err(Status::internal("transfer file was not opened"));
        };
        if !data.is_empty() {
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(|error| Status::internal(format!("seek {name}: {error}")))?;
            // tokio's write_all returns once the bytes are queued for a
            // blocking write. The later set_len and sync_all wait for that
            // write but drop its error, so flush to report it here.
            file.write_all(data)
                .await
                .map_err(|error| Status::internal(format!("write {name}: {error}")))?;
            file.flush()
                .await
                .map_err(|error| Status::internal(format!("write {name}: {error}")))?;
        }
        self.bytes += data.len() as u64;
        self.logical_bytes += next - *end;
        *end = next;
        if eof {
            file.set_len(offset)
                .await
                .map_err(|error| Status::internal(format!("size {name}: {error}")))?;
            file.sync_all()
                .await
                .map_err(|error| Status::internal(format!("sync {name}: {error}")))?;
            self.completed.insert(name.to_owned(), offset);
            self.current = None;
        }
        self.fingerprint.update((name.len() as u64).to_le_bytes());
        self.fingerprint.update(name.as_bytes());
        self.fingerprint.update(offset.to_le_bytes());
        self.fingerprint.update((data.len() as u64).to_le_bytes());
        self.fingerprint.update(data);
        self.fingerprint.update([u8::from(eof)]);
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<u64, Status> {
        if self.current.is_some() || self.completed.is_empty() {
            return Err(Status::data_loss("transfer ended before complete files"));
        }
        Ok(self.bytes)
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.completed.contains_key(name)
    }

    pub(crate) fn nonempty(&self, name: &str) -> bool {
        self.completed.get(name).is_some_and(|size| *size > 0)
    }

    pub(crate) fn has_snapshot(&self) -> bool {
        self.completed
            .keys()
            .any(|name| name.starts_with("snapshot/"))
    }

    pub(crate) fn fingerprint(&self, metadata: &[u8]) -> String {
        let mut digest = self.fingerprint.clone();
        digest.update((metadata.len() as u64).to_le_bytes());
        digest.update(metadata);
        hex::encode(digest.finalize())
    }
}
