//! File operations run synchronously under the provisioned account's token.
use super::{identity::Identity, path::LocalPath};
use pplx_space_guest_process::{
    IdempotencyKey,
    status::{ERROR_REASON_METADATA_KEY, GUEST_STORAGE_FULL},
};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::*;
use std::os::windows::fs::MetadataExt;
use std::{
    collections::HashMap,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

type Stream<T> = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, Status>> + Send>>;
const MAX_BYTES: usize = 16 * 1024 * 1024;
#[derive(Clone)]
pub(super) struct Filesystem {
    identity: Arc<Identity>,
    slots: Arc<Semaphore>,
    uploads: Arc<Semaphore>,
    mutations: Arc<Mutex<HashMap<IdempotencyKey, Mutation>>>,
    tasks: tokio_util::task::TaskTracker,
}
struct Mutation {
    fingerprint: blake3::Hash,
    outcome: Option<Result<u64, Status>>,
    completed: Option<std::time::Instant>,
}
impl Filesystem {
    pub(super) fn new(identity: Arc<Identity>, tasks: tokio_util::task::TaskTracker) -> Self {
        Self {
            identity,
            tasks,
            slots: Arc::new(Semaphore::new(8)),
            uploads: Arc::new(Semaphore::new(4)),
            mutations: Arc::default(),
        }
    }
    fn path(&self, path: &str, user: Option<&str>) -> Result<LocalPath, Status> {
        self.identity.check_user(user)?;
        LocalPath::parse(path, &self.identity.home)
    }
    async fn work<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Identity) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("Windows filesystem busy"))?;
        let identity = self.identity.clone();
        self.tasks
            .spawn_blocking(move || {
                let _permit = permit;
                work(&identity)
            })
            .await
            .map_err(|_| Status::internal("Windows file worker failed"))?
    }
    async fn mutation(
        &self,
        key: Option<String>,
        fingerprint: blake3::Hash,
        work: impl FnOnce(&Identity) -> io::Result<u64> + Send + 'static,
    ) -> Result<u64, Status> {
        let key = key
            .map(IdempotencyKey::parse)
            .transpose()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let records = self.mutations.clone();
        self.work(move |identity| {
            if let Some(key) = &key {
                let mut records = records
                    .lock()
                    .map_err(|_| Status::internal("file mutation journal poisoned"))?;
                if let Some(record) = records.get(key) {
                    if record.fingerprint != fingerprint {
                        return Err(Status::already_exists(
                            "idempotency key reused for different file mutation",
                        ));
                    }
                    return record.outcome.clone().unwrap_or_else(|| {
                        Err(Status::failed_precondition(
                            "file mutation pending or outcome unknown",
                        ))
                    });
                }
                // Pending/uncertain mutations are never evicted and replayed.
                records.retain(|_, record| {
                    !record
                        .completed
                        .is_some_and(|t| t.elapsed() >= std::time::Duration::from_secs(900))
                });
                if records.len() >= 1024 {
                    return Err(Status::resource_exhausted("file mutation journal full"));
                }
                records.insert(
                    key.clone(),
                    Mutation {
                        fingerprint,
                        outcome: None,
                        completed: None,
                    },
                );
            }
            let outcome = identity.run(|| work(identity)).map_err(io_status);
            if let Some(key) = key
                && let Ok(mut records) = records.lock()
                && let Some(record) = records.get_mut(&key)
            {
                record.outcome = Some(outcome.clone());
                record.completed = Some(std::time::Instant::now());
            }
            outcome
        })
        .await
    }
}
fn fingerprint<T: prost::Message>(kind: &str, request: &T) -> blake3::Hash {
    let mut h = blake3::Hasher::new();
    h.update(kind.as_bytes());
    h.update(&request.encode_to_vec());
    h.finalize()
}
pub(super) fn io_status(e: io::Error) -> Status {
    if e.kind() == io::ErrorKind::StorageFull {
        let mut status = Status::resource_exhausted("guest storage is full");
        status.metadata_mut().insert(
            ERROR_REASON_METADATA_KEY,
            tonic::metadata::MetadataValue::from_static(GUEST_STORAGE_FULL),
        );
        return status;
    }

    match e.kind() {
        io::ErrorKind::NotFound => Status::not_found("guest path not found"),
        io::ErrorKind::PermissionDenied => Status::permission_denied("guest access denied"),
        io::ErrorKind::AlreadyExists => Status::already_exists("guest path exists"),
        io::ErrorKind::InvalidInput => Status::invalid_argument("invalid guest file operation"),
        _ => Status::internal(format!("Windows file operation failed: {e}")),
    }
}
fn info(path: &Path, name: Option<String>) -> io::Result<FileInfo> {
    let m = std::fs::symlink_metadata(path)?;
    let file_type = if m.file_type().is_symlink() {
        FileType::Symlink
    } else if m.is_dir() {
        FileType::Directory
    } else if m.is_file() {
        FileType::File
    } else {
        FileType::Other
    };
    Ok(FileInfo {
        path: path
            .to_str()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "guest filename is not UTF-8")
            })?
            .to_owned(),
        name,
        file_type: file_type as i32,
        size_bytes: m.len(),
        mode: 0,
        owner_uid: 0,
        owner_gid: 0,
        modified_at: m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| prost_types::Timestamp {
                seconds: d.as_secs() as i64,
                nanos: d.subsec_nanos() as i32,
            }),
        symlink_target: None,
        link_count: None,
        windows_metadata: Some(WindowsFileMetadata {
            owner_sid: None,
            attributes: m.file_attributes(),
        }),
    })
}
fn write_file(path: &Path, data: &[u8], append: bool) -> io::Result<()> {
    if append {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        f.write_all(data)?;
        return f.sync_all();
    }
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let temp = parent.join(format!(".space-write-{}", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        let src = super::identity::wide(&temp.to_string_lossy()).map_err(io::Error::other)?;
        let dst = super::identity::wide(&path.to_string_lossy()).map_err(io::Error::other)?;
        // SAFETY: terminated paths remain live; impersonation is held by the caller.
        if unsafe {
            windows_sys::Win32::Storage::FileSystem::MoveFileExW(
                src.as_ptr(),
                dst.as_ptr(),
                windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING
                    | windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}
#[tonic::async_trait]
#[allow(deprecated)]
impl filesystem_server::Filesystem for Filesystem {
    type ListStream = Stream<LegacyDirEntry>;
    type ListInfoStream = Stream<FileInfo>;
    type ReadStream = Stream<FileChunk>;
    type WatchStream = Stream<FileEvent>;
    async fn stat(&self, _: Request<StatRequest>) -> Result<Response<LegacyStatResponse>, Status> {
        Err(super::unsupported("Windows requires StatInfo metadata"))
    }
    async fn list(&self, _: Request<ListRequest>) -> Result<Response<Self::ListStream>, Status> {
        Err(super::unsupported("Windows requires ListInfo metadata"))
    }
    async fn stat_info(&self, r: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let r = r.into_inner();
        let p = self.path(&r.path, r.user.as_deref())?;
        let info = self
            .work(move |i| i.run(|| info(p.as_path(), None)).map_err(io_status))
            .await?;
        Ok(Response::new(StatResponse { info: Some(info) }))
    }
    async fn list_info(
        &self,
        r: Request<ListRequest>,
    ) -> Result<Response<Self::ListInfoStream>, Status> {
        let r = r.into_inner();
        let p = self.path(&r.path, r.user.as_deref())?;
        let depth = r.depth.unwrap_or(1);
        if depth == 0 || depth > 32 {
            return Err(Status::invalid_argument("depth must be 1..32"));
        }
        let entries = self
            .work(move |i| {
                i.run(|| {
                    let mut pending = vec![(p.as_path().to_path_buf(), depth)];
                    let mut entries = Vec::new();
                    let mut bytes = 0;
                    while let Some((dir, left)) = pending.pop() {
                        for entry in std::fs::read_dir(dir)? {
                            let entry = entry?;
                            let entry_info = info(
                                &entry.path(),
                                Some(entry.file_name().into_string().map_err(|_| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "guest filename is not UTF-8",
                                    )
                                })?),
                            )?;
                            bytes += entry_info.path.len()
                                + entry_info.name.as_ref().map_or(0, String::len);
                            if entries.len() >= 10_000 || bytes > 4 * 1024 * 1024 {
                                return Err(io::Error::new(
                                    io::ErrorKind::OutOfMemory,
                                    "directory listing bound exceeded",
                                ));
                            }
                            if left > 1
                                && entry_info.file_type == FileType::Directory as i32
                                && entry_info
                                    .windows_metadata
                                    .as_ref()
                                    .is_some_and(|m| m.attributes & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT == 0)
                            {
                                pending.push((entry.path(), left - 1));
                            }
                            entries.push(entry_info);
                        }
                    }
                    Ok(entries)
                })
                .map_err(io_status)
            })
            .await?;
        Ok(Response::new(Box::pin(tokio_stream::iter(
            entries.into_iter().map(Ok),
        ))))
    }
    async fn read(&self, r: Request<ReadRequest>) -> Result<Response<Self::ReadStream>, Status> {
        let r = r.into_inner();
        let p = self.path(&r.path, r.user.as_deref())?;
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("Windows filesystem busy"))?;
        let identity = self.identity.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        self.tasks.spawn_blocking(move || {
            let _permit = permit;
            let result = identity.run(|| {
                let mut f = std::fs::File::open(p.as_path())?;
                f.seek(SeekFrom::Start(r.offset))?;
                let mut remain = r.length.unwrap_or(u64::MAX);
                while remain > 0 {
                    let mut data = vec![0; remain.min(64 * 1024) as usize];
                    let count = f.read(&mut data)?;
                    if count == 0 {
                        break;
                    }
                    data.truncate(count);
                    remain -= count as u64;
                    if tx
                        .blocking_send(Ok(FileChunk { data: data.into() }))
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(())
            });
            if let Err(e) = result {
                let _ = tx.blocking_send(Err(io_status(e)));
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
    async fn write(
        &self,
        r: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let _upload = self
            .uploads
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many file uploads"))?;
        let mut stream = r.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("write open missing"))?;
        let Some(write_request::Message::Open(open)) = first.message else {
            return Err(Status::invalid_argument("first write message must open"));
        };
        let p = self.path(&open.path, open.user.as_deref())?;
        if open.append && open.idempotency_key.is_none() {
            return Err(Status::failed_precondition(
                "append requires idempotency key",
            ));
        }
        let mut data = Vec::new();
        while let Some(message) = stream.message().await? {
            let Some(write_request::Message::Data(chunk)) = message.message else {
                return Err(Status::invalid_argument("write open repeated"));
            };
            if data.len().saturating_add(chunk.len()) > MAX_BYTES {
                return Err(Status::resource_exhausted("write exceeds 16 MiB"));
            }
            data.extend(chunk);
        }
        let mut h = blake3::Hasher::new();
        h.update(fingerprint("write", &open).as_bytes());
        h.update(&data);
        let fingerprint = h.finalize();
        let count = data.len() as u64;
        let bytes_written = self
            .mutation(open.idempotency_key, fingerprint, move |_| {
                write_file(p.as_path(), &data, open.append)?;
                Ok(count)
            })
            .await?;
        Ok(Response::new(WriteResponse { bytes_written }))
    }
    async fn write_batch(
        &self,
        r: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        let r = r.into_inner();
        if r.entries.len() > 128
            || r.entries.iter().map(|e| e.data.len()).sum::<usize>() > MAX_BYTES
        {
            return Err(Status::resource_exhausted("write batch exceeds bound"));
        }
        let hash = fingerprint("batch", &r);
        let mut entries = Vec::new();
        for e in r.entries {
            if e.append && r.idempotency_key.is_none() {
                return Err(Status::failed_precondition(
                    "append requires idempotency key",
                ));
            }
            entries.push((self.path(&e.path, r.user.as_deref())?, e));
        }
        let count = entries.len() as u64;
        let files_written = self
            .mutation(r.idempotency_key, hash, move |_| {
                for (p, e) in entries {
                    write_file(p.as_path(), &e.data, e.append)?;
                }
                Ok(count)
            })
            .await?;
        Ok(Response::new(WriteBatchResponse { files_written }))
    }
    async fn mkdir(&self, r: Request<MkdirRequest>) -> Result<Response<MkdirResponse>, Status> {
        let r = r.into_inner();
        let p = self.path(&r.path, r.user.as_deref())?;
        self.work(move |identity| {
            identity
                .run(|| {
                    let result = if r.parents {
                        if let Some(parent) = p.as_path().parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        std::fs::create_dir(p.as_path())
                    } else {
                        std::fs::create_dir(p.as_path())
                    };
                    match result {
                        Err(e)
                            if e.kind() == io::ErrorKind::AlreadyExists
                                && r.exist_ok
                                && p.as_path().is_dir() =>
                        {
                            Ok(())
                        }
                        other => other,
                    }
                })
                .map_err(io_status)
        })
        .await?;
        Ok(Response::new(MkdirResponse {}))
    }
    async fn remove(&self, r: Request<RemoveRequest>) -> Result<Response<RemoveResponse>, Status> {
        let r = r.into_inner();
        let p = self.path(&r.path, r.user.as_deref())?;
        self.work(move |identity| {
            identity
                .run(|| {
                    let result = (|| {
                        let m = std::fs::symlink_metadata(p.as_path())?;
                        if m.is_dir() {
                            if r.recursive {
                                std::fs::remove_dir_all(p.as_path())
                            } else {
                                std::fs::remove_dir(p.as_path())
                            }
                        } else {
                            std::fs::remove_file(p.as_path())
                        }
                    })();
                    match result {
                        Err(e) if e.kind() == io::ErrorKind::NotFound && r.missing_ok => Ok(()),
                        other => other,
                    }
                })
                .map_err(io_status)
        })
        .await?;
        Ok(Response::new(RemoveResponse {}))
    }
    async fn rename(&self, r: Request<RenameRequest>) -> Result<Response<RenameResponse>, Status> {
        let r = r.into_inner();
        let a = self.path(&r.old_path, r.user.as_deref())?;
        let b = self.path(&r.new_path, r.user.as_deref())?;
        let hash = fingerprint("rename", &r);
        self.mutation(r.idempotency_key, hash, move |_| {
            std::fs::rename(a.as_path(), b.as_path())?;
            Ok(0)
        })
        .await?;
        Ok(Response::new(RenameResponse {}))
    }
    async fn normalize_watch_root(
        &self,
        _: Request<NormalizeWatchRootRequest>,
    ) -> Result<Response<NormalizeWatchRootResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn create_watcher(
        &self,
        _: Request<CreateWatcherRequest>,
    ) -> Result<Response<CreateWatcherResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn get_watcher_events(
        &self,
        _: Request<GetWatcherEventsRequest>,
    ) -> Result<Response<GetWatcherEventsResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn remove_watcher(
        &self,
        _: Request<RemoveWatcherRequest>,
    ) -> Result<Response<RemoveWatcherResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn enable_file_changes(
        &self,
        _: Request<EnableFileChangesRequest>,
    ) -> Result<Response<EnableFileChangesResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn disable_file_changes(
        &self,
        _: Request<DisableFileChangesRequest>,
    ) -> Result<Response<DisableFileChangesResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn get_modified_files(
        &self,
        _: Request<GetModifiedFilesRequest>,
    ) -> Result<Response<GetModifiedFilesResponse>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
    async fn watch(&self, _: Request<WatchRequest>) -> Result<Response<Self::WatchStream>, Status> {
        Err(super::unsupported("Windows file watches are not qualified"))
    }
}
