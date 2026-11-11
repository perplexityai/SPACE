use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Barrier;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use dashmap::DashMap;
use jiff::{SignedDuration, Timestamp};
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::sandbox_storage::{RootfsSizeError, validate_rootfs_mib};
use pplx_space_common_core::types::SandboxId;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::{Request, Response, Status};

use crate::disk_admission::{DiskAdmission, DiskAdmissionReservation, DiskAllocationOperation};
use crate::proto::{
    CompletePreparedRootfsRequest, CompletePreparedRootfsResponse, PreparedRootfsDisposition,
    ReservePreparedRootfsRequest, ReservePreparedRootfsResponse,
};

use super::{ALLOW_NODE_ADMIN, CACHE_DIR, NodeManagerService};

mod files;

const IN_PROGRESS_MARKER: &str = ".space-prepared-rootfs-in-progress";
const READY_MARKER: &str = ".space-prepared-rootfs-ready";
const MIB: u64 = 1024 * 1024;
const REAPER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
// Build jobs time out after six hours; this buffer prevents reclaiming valid work.
const STALE_HANDOFF_AFTER: SignedDuration = SignedDuration::from_hours(7);

#[derive(Debug, Error)]
enum PreparedRootfsError {
    #[error(transparent)]
    InvalidSize(#[from] RootfsSizeError),
    #[error("prepared rootfs has conflicting handoff markers")]
    ConflictingMarkers,
    #[error("prepared rootfs marker {path} is invalid: {source}")]
    InvalidMarker {
        path: PathBuf,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("prepared rootfs is already committed")]
    AlreadyCommitted,
    #[error("prepared rootfs is still being assembled")]
    StillPreparing,
    #[error("prepared rootfs handoff is missing")]
    MissingHandoff,
    #[error("prepared rootfs transition lock is poisoned")]
    TransitionLockPoisoned,
    #[error("prepared rootfs reservation expired")]
    ReservationExpired,
    #[error("prepared rootfs size changed from {expected_mib} MiB to {actual_mib} MiB")]
    SizeChanged { expected_mib: u32, actual_mib: u32 },
    #[error("prepared rootfs {path} is not a single-link regular file")]
    NotRegularFile { path: PathBuf },
    #[error("prepared rootfs {path} has {actual_bytes} bytes; expected {expected_bytes}")]
    FileSizeMismatch {
        path: PathBuf,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("{operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} task failed: {source}")]
    BlockingTask {
        operation: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
}

impl PreparedRootfsError {
    fn into_status(self) -> Status {
        match self {
            Self::InvalidSize(_) => Status::invalid_argument(self.to_string()),
            Self::AlreadyCommitted
            | Self::StillPreparing
            | Self::MissingHandoff
            | Self::ReservationExpired
            | Self::SizeChanged { .. }
            | Self::NotRegularFile { .. }
            | Self::FileSizeMismatch { .. }
            | Self::ConflictingMarkers => Status::failed_precondition(self.to_string()),
            Self::InvalidMarker { .. }
            | Self::TransitionLockPoisoned
            | Self::Io { .. }
            | Self::BlockingTask { .. } => Status::internal(self.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandoffState {
    Absent,
    InProgress(u32),
    Ready(u32),
}

struct PreparedRootfsReservation {
    // None owns an admission hold, not a guessed byte reservation.
    rootfs_size_mib: Option<u32>,
    started_at: Timestamp,
    disk: DiskAdmissionReservation,
}

#[derive(Clone, Default)]
pub(super) struct PreparedRootfsReservations {
    task_tracker: tokio_util::task::TaskTracker,
    active: Arc<DashMap<SandboxId, PreparedRootfsReservation>>,
    // A failed directory scan may hide additional handoffs. Retain its hold
    // until a complete scan has restored every discoverable obligation.
    transition: Arc<Mutex<Option<DiskAdmissionReservation>>>,
    #[cfg(test)]
    reserve_pause: Option<Arc<ReservePause>>,
    #[cfg(test)]
    commit_pause: Option<Arc<ReservePause>>,
    #[cfg(test)]
    reclaim_pause: Option<Arc<ReservePause>>,
    #[cfg(test)]
    release_pause: Option<Arc<ReservePause>>,
}

#[cfg(test)]
struct ReservePause {
    armed: AtomicBool,
    entered: Barrier,
    release: Barrier,
}

#[cfg(test)]
impl ReservePause {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(true),
            entered: Barrier::new(2),
            release: Barrier::new(2),
        }
    }

    fn pause_once(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.entered.wait();
            self.release.wait();
        }
    }
}

impl PreparedRootfsReservations {
    fn lock_transition(&self) -> Result<MutexGuard<'_, Option<DiskAdmissionReservation>>, Status> {
        self.transition
            .lock()
            .map_err(|_| PreparedRootfsError::TransitionLockPoisoned.into_status())
    }

    pub(super) fn with_task_tracker(mut self, tracker: tokio_util::task::TaskTracker) -> Self {
        self.task_tracker = tracker;
        self
    }

    pub(super) fn recover(disk_admission: &DiskAdmission, cache_root: &Path) -> Self {
        let reservations = Self {
            transition: Arc::new(Mutex::new(Some(disk_admission.hold_unknown_reservation()))),
            ..Self::default()
        };
        if let Err(error) = reservations.recover_scan(disk_admission, cache_root) {
            tracing::warn!(path = %cache_root.display(), %error, "prepared rootfs recovery scan incomplete; disk admission held");
        }
        reservations
    }

    fn recover_scan(
        &self,
        disk_admission: &DiskAdmission,
        cache_root: &Path,
    ) -> Result<(), Status> {
        let mut scan_hold = self.lock_transition()?;
        if scan_hold.is_none() {
            return Ok(());
        }
        let entries = match fs::read_dir(cache_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                scan_hold.take();
                return Ok(());
            }
            Err(error) => {
                return Err(
                    io_error("scan prepared rootfs reservations", cache_root, error).into_status(),
                );
            }
        };
        for entry in entries {
            let entry = entry.map_err(|error| {
                io_error("read prepared rootfs cache entry", cache_root, error).into_status()
            })?;
            let Some(sandbox_id) = entry
                .file_name()
                .to_str()
                .and_then(|value| value.parse::<SandboxId>().ok())
            else {
                continue;
            };
            if entry.file_name() != sandbox_id.to_string().as_str() {
                // Reconciliation addresses canonical IDs; do not mistake a
                // differently named directory for a removed obligation.
                return Err(Status::unavailable(format!(
                    "non-canonical prepared rootfs cache entry: {}",
                    entry.path().display()
                )));
            }
            if self.active.contains_key(&sandbox_id) {
                continue;
            }
            let instance_dir = entry.path().join("instance");
            let (rootfs_size_mib, disk) = match read_handoff_state(&instance_dir) {
                Ok(HandoffState::InProgress(rootfs_size_mib)) => {
                    let disk = disk_admission.restore_reservation(
                        DiskAllocationOperation::PrepareExternalRootfs,
                        Some(u64::from(rootfs_size_mib).saturating_mul(MIB)),
                    );
                    (Some(rootfs_size_mib), disk)
                }
                Ok(HandoffState::Absent | HandoffState::Ready(_)) => continue,
                Err(error) => {
                    tracing::warn!(%sandbox_id, %error, "cannot recover prepared rootfs reservation; disk admission held");
                    (None, disk_admission.hold_unknown_reservation())
                }
            };
            self.active.insert(
                sandbox_id,
                PreparedRootfsReservation {
                    rootfs_size_mib,
                    started_at: marker_started_at(&instance_dir),
                    disk,
                },
            );
        }
        scan_hold.take();
        Ok(())
    }

    fn reserve(
        &self,
        disk_admission: &DiskAdmission,
        sandbox_id: SandboxId,
        rootfs_path: &Path,
        rootfs_size_mib: u32,
    ) -> Result<PathBuf, Status> {
        validate_rootfs_mib(rootfs_size_mib)
            .map_err(PreparedRootfsError::from)
            .map_err(PreparedRootfsError::into_status)?;
        let _transition = self.lock_transition()?;
        let instance_dir = rootfs_parent(rootfs_path)?;
        if let Some(active) = self.active.get(&sandbox_id) {
            let expected_mib = active.rootfs_size_mib.ok_or_else(|| {
                Status::unavailable(
                    "prepared rootfs reservation needs recovery or confirmed cleanup",
                )
            })?;
            if expected_mib != rootfs_size_mib {
                return Err(PreparedRootfsError::SizeChanged {
                    expected_mib,
                    actual_mib: rootfs_size_mib,
                }
                .into_status());
            }
            match read_handoff_state(instance_dir).map_err(PreparedRootfsError::into_status)? {
                HandoffState::Absent => {
                    write_marker(instance_dir, IN_PROGRESS_MARKER, rootfs_size_mib)
                        .map_err(PreparedRootfsError::into_status)?;
                }
                HandoffState::InProgress(actual_mib) if actual_mib == rootfs_size_mib => {}
                HandoffState::InProgress(actual_mib) => {
                    return Err(PreparedRootfsError::SizeChanged {
                        expected_mib: rootfs_size_mib,
                        actual_mib,
                    }
                    .into_status());
                }
                HandoffState::Ready(_) => {
                    return Err(PreparedRootfsError::AlreadyCommitted.into_status());
                }
            }
            return Ok(rootfs_path.to_path_buf());
        }
        #[cfg(test)]
        if let Some(pause) = &self.reserve_pause {
            pause.pause_once();
        }

        match read_handoff_state(instance_dir).map_err(PreparedRootfsError::into_status)? {
            HandoffState::Ready(_) => {
                return Err(PreparedRootfsError::AlreadyCommitted.into_status());
            }
            HandoffState::InProgress(expected_mib) if expected_mib != rootfs_size_mib => {
                return Err(PreparedRootfsError::SizeChanged {
                    expected_mib,
                    actual_mib: rootfs_size_mib,
                }
                .into_status());
            }
            HandoffState::Absent | HandoffState::InProgress(_) => {}
        }

        let reservation = disk_admission.reserve(
            DiskAllocationOperation::PrepareExternalRootfs,
            Some(u64::from(rootfs_size_mib).saturating_mul(MIB)),
        )?;
        create_instance_directory(instance_dir).map_err(PreparedRootfsError::into_status)?;
        write_marker(instance_dir, IN_PROGRESS_MARKER, rootfs_size_mib)
            .map_err(PreparedRootfsError::into_status)?;
        self.active.insert(
            sandbox_id,
            PreparedRootfsReservation {
                rootfs_size_mib: Some(rootfs_size_mib),
                started_at: Timestamp::now(),
                disk: reservation,
            },
        );
        Ok(rootfs_path.to_path_buf())
    }

    async fn reserve_on_blocking_pool(
        &self,
        disk_admission: Arc<DiskAdmission>,
        sandbox_id: SandboxId,
        rootfs_path: PathBuf,
        rootfs_size_mib: u32,
    ) -> Result<PathBuf, Status> {
        let reservations = self.clone();
        self.task_tracker
            .spawn_blocking(move || {
                reservations.reserve(&disk_admission, sandbox_id, &rootfs_path, rootfs_size_mib)
            })
            .await
            .map_err(|source| {
                PreparedRootfsError::BlockingTask {
                    operation: "reserve prepared rootfs",
                    source,
                }
                .into_status()
            })?
    }

    async fn complete(
        &self,
        disk_admission: &DiskAdmission,
        sandbox_id: SandboxId,
        rootfs_path: &Path,
        disposition: PreparedRootfsDisposition,
    ) -> Result<(), Status> {
        let reservations = self.clone();
        let rootfs_path = rootfs_path.to_path_buf();
        let active = self
            .task_tracker
            .spawn_blocking(move || {
                reservations.complete_transition(sandbox_id, &rootfs_path, disposition)
            })
            .await
            .map_err(|source| {
                PreparedRootfsError::BlockingTask {
                    operation: "complete prepared rootfs",
                    source,
                }
                .into_status()
            })??;

        if let Some(active) = active {
            disk_admission.release_after_success(active.disk).await;
        } else {
            disk_admission.refresh_after_external_allocation().await;
        }
        Ok(())
    }

    fn complete_transition(
        &self,
        sandbox_id: SandboxId,
        rootfs_path: &Path,
        disposition: PreparedRootfsDisposition,
    ) -> Result<Option<PreparedRootfsReservation>, Status> {
        let instance_dir = rootfs_parent(rootfs_path)?;
        let active = {
            let _transition = self.lock_transition()?;
            match disposition {
                PreparedRootfsDisposition::Commit => {
                    let handoff_state = read_handoff_state(instance_dir)
                        .map_err(PreparedRootfsError::into_status)?;
                    let rootfs_size_mib = match handoff_state {
                        HandoffState::InProgress(rootfs_size_mib)
                        | HandoffState::Ready(rootfs_size_mib) => rootfs_size_mib,
                        HandoffState::Absent => {
                            return Err(PreparedRootfsError::MissingHandoff.into_status());
                        }
                    };
                    if matches!(handoff_state, HandoffState::InProgress(_)) {
                        let active = self
                            .active
                            .get(&sandbox_id)
                            .ok_or_else(|| PreparedRootfsError::ReservationExpired.into_status())?;
                        let expected_mib = active.rootfs_size_mib.ok_or_else(|| {
                            Status::unavailable(
                                "prepared rootfs reservation needs recovery before commit",
                            )
                        })?;
                        if expected_mib != rootfs_size_mib {
                            return Err(PreparedRootfsError::SizeChanged {
                                expected_mib,
                                actual_mib: rootfs_size_mib,
                            }
                            .into_status());
                        }
                    }
                    validate_rootfs_file(rootfs_path, rootfs_size_mib)
                        .map_err(PreparedRootfsError::into_status)?;
                    #[cfg(test)]
                    if let Some(pause) = &self.commit_pause {
                        pause.pause_once();
                    }
                    match read_handoff_state(instance_dir)
                        .map_err(PreparedRootfsError::into_status)?
                    {
                        HandoffState::InProgress(actual_mib) if actual_mib == rootfs_size_mib => {
                            rename_marker(instance_dir, IN_PROGRESS_MARKER, READY_MARKER)
                                .map_err(PreparedRootfsError::into_status)?;
                        }
                        HandoffState::Ready(actual_mib) if actual_mib == rootfs_size_mib => {}
                        HandoffState::InProgress(actual_mib) | HandoffState::Ready(actual_mib) => {
                            return Err(PreparedRootfsError::SizeChanged {
                                expected_mib: rootfs_size_mib,
                                actual_mib,
                            }
                            .into_status());
                        }
                        HandoffState::Absent => {
                            return Err(PreparedRootfsError::MissingHandoff.into_status());
                        }
                    }
                }
                PreparedRootfsDisposition::Abort => {
                    let handoff_state = read_handoff_state(instance_dir)
                        .map_err(PreparedRootfsError::into_status)?;
                    let reservation_active = self.active.contains_key(&sandbox_id);
                    if matches!(handoff_state, HandoffState::InProgress(_)) && !reservation_active {
                        return Err(PreparedRootfsError::ReservationExpired.into_status());
                    }
                    if reservation_active && !matches!(handoff_state, HandoffState::Ready(_)) {
                        remove_if_present(rootfs_path, "remove partial prepared rootfs")
                            .map_err(PreparedRootfsError::into_status)?;
                        remove_if_present(
                            &instance_dir.join(IN_PROGRESS_MARKER),
                            "remove prepared rootfs marker",
                        )
                        .map_err(PreparedRootfsError::into_status)?;
                    }
                }
                PreparedRootfsDisposition::Unspecified => {
                    return Err(Status::invalid_argument(
                        "prepared rootfs disposition must be commit or abort",
                    ));
                }
            }
            self.active.remove(&sandbox_id).map(|(_, active)| active)
        };
        Ok(active)
    }

    async fn release_if_removed(
        &self,
        disk_admission: &DiskAdmission,
        sandbox_id: SandboxId,
        sandbox_cache_dir: &Path,
    ) {
        let reservations = self.clone();
        let sandbox_cache_dir = sandbox_cache_dir.to_path_buf();
        let active = match self
            .task_tracker
            .spawn_blocking(move || {
                reservations.release_if_removed_transition(sandbox_id, &sandbox_cache_dir)
            })
            .await
        {
            Ok(Ok(active)) => active,
            Ok(Err(error)) => {
                tracing::warn!(%sandbox_id, %error, "cannot release prepared rootfs reservation");
                return;
            }
            Err(error) => {
                tracing::warn!(%sandbox_id, %error, "prepared rootfs release task failed");
                return;
            }
        };
        if let Some(active) = active {
            disk_admission.release_after_success(active.disk).await;
        }
    }

    fn release_if_removed_transition(
        &self,
        sandbox_id: SandboxId,
        sandbox_cache_dir: &Path,
    ) -> Result<Option<PreparedRootfsReservation>, Status> {
        let _transition = self.lock_transition()?;
        #[cfg(test)]
        if let Some(pause) = &self.release_pause {
            pause.pause_once();
        }
        match fs::symlink_metadata(sandbox_cache_dir) {
            Ok(_) => Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(self.active.remove(&sandbox_id).map(|(_, active)| active))
            }
            Err(source) => Err(PreparedRootfsError::Io {
                operation: "inspect sandbox cache before releasing prepared rootfs reservation",
                path: sandbox_cache_dir.to_path_buf(),
                source,
            }
            .into_status()),
        }
    }

    async fn reclaim_stale(
        &self,
        disk_admission: &DiskAdmission,
        cache_root: &Path,
        now: Timestamp,
    ) -> usize {
        let reservations = self.clone();
        let disk = disk_admission.clone();
        let root = cache_root.to_path_buf();
        match self
            .task_tracker
            .spawn_blocking(move || reservations.recover_scan(&disk, &root))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "prepared rootfs recovery scan still incomplete")
            }
            Err(error) => tracing::warn!(%error, "prepared rootfs recovery scan task failed"),
        }
        let sandbox_ids = self
            .active
            .iter()
            .filter(|entry| {
                entry.rootfs_size_mib.is_none()
                    || now.duration_since(entry.started_at) >= STALE_HANDOFF_AFTER
            })
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        let mut reclaimed = 0;
        for sandbox_id in sandbox_ids {
            let reservations = self.clone();
            let cache_root = cache_root.to_path_buf();
            let disk = disk_admission.clone();
            let reservation = match self
                .task_tracker
                .spawn_blocking(move || {
                    reservations.reclaim_stale_transition(&disk, sandbox_id, &cache_root, now)
                })
                .await
            {
                Ok(Ok(reservation)) => reservation,
                Ok(Err(error)) => {
                    tracing::warn!(%sandbox_id, %error, "prepared rootfs accounting retained for retry or repair");
                    None
                }
                Err(error) => {
                    tracing::warn!(%sandbox_id, %error, "prepared rootfs reclamation task failed");
                    None
                }
            };
            if let Some(reservation) = reservation {
                disk_admission.release_after_success(reservation.disk).await;
                reclaimed += 1;
                tracing::info!(%sandbox_id, "reconciled prepared rootfs reservation");
            }
        }
        reclaimed
    }

    fn reclaim_stale_transition(
        &self,
        disk_admission: &DiskAdmission,
        sandbox_id: SandboxId,
        cache_root: &Path,
        now: Timestamp,
    ) -> Result<Option<PreparedRootfsReservation>, Status> {
        let _transition = self.lock_transition()?;
        let Some((_, reservation)) = self.active.remove_if(&sandbox_id, |_, reservation| {
            reservation.rootfs_size_mib.is_none()
                || now.duration_since(reservation.started_at) >= STALE_HANDOFF_AFTER
        }) else {
            return Ok(None);
        };
        let sandbox_id_string = sandbox_id.to_string();
        let paths = cache_scoped_prepared_rootfs_paths(cache_root, &sandbox_id_string);
        #[cfg(test)]
        if let Some(pause) = &self.reclaim_pause {
            pause.pause_once();
        }
        let result = paths.and_then(|(instance_dir, rootfs_path)| {
            if reservation.rootfs_size_mib.is_none() {
                let sandbox_cache = cache_root.join(sandbox_id_string);
                match fs::symlink_metadata(&sandbox_cache) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => {
                        return Err(io_error(
                            "inspect unknown prepared rootfs cache",
                            &sandbox_cache,
                            error,
                        ));
                    }
                    Ok(_) => {}
                }
                return match read_handoff_state(&instance_dir)? {
                    HandoffState::InProgress(size) => {
                        // Charge the recovered promise before releasing the unknown hold.
                        let disk = disk_admission.restore_reservation(
                            DiskAllocationOperation::PrepareExternalRootfs,
                            Some(u64::from(size) * MIB),
                        );
                        self.active.insert(
                            sandbox_id,
                            PreparedRootfsReservation {
                                rootfs_size_mib: Some(size),
                                started_at: marker_started_at(&instance_dir),
                                disk,
                            },
                        );
                        Ok(())
                    }
                    HandoffState::Ready(size) => validate_rootfs_file(&rootfs_path, size),
                    // Erasing a marker does not prove the external writer stopped.
                    HandoffState::Absent => Err(PreparedRootfsError::MissingHandoff),
                };
            }
            match read_handoff_state(&instance_dir) {
                Ok(HandoffState::InProgress(_)) => {
                    remove_if_present(&rootfs_path, "remove expired prepared rootfs").and_then(
                        |()| {
                            remove_if_present(
                                &instance_dir.join(IN_PROGRESS_MARKER),
                                "remove expired prepared rootfs marker",
                            )
                        },
                    )
                }
                Ok(HandoffState::Absent) => {
                    remove_if_present(&rootfs_path, "remove expired prepared rootfs")
                }
                Ok(HandoffState::Ready(_)) => Ok(()),
                Err(error) => Err(error),
            }
        });
        match result {
            Ok(()) => Ok(Some(reservation)),
            Err(error) => {
                self.active.insert(sandbox_id, reservation);
                Err(error.into_status())
            }
        }
    }
}

impl NodeManagerService {
    pub fn start_prepared_rootfs_reaper(
        &self,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        let reservations = self.prepared_rootfs_reservations.clone();
        let disk_admission = self.disk_admission.clone();
        task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(REAPER_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_token.cancelled() => return,
                    _ = interval.tick() => {
                        reservations
                            .reclaim_stale(
                                &disk_admission,
                                Path::new(CACHE_DIR),
                                Timestamp::now(),
                            )
                            .await;
                    }
                }
            }
        });
    }

    pub(super) async fn reserve_prepared_rootfs(
        &self,
        request: Request<ReservePreparedRootfsRequest>,
    ) -> Result<Response<ReservePreparedRootfsResponse>, Status> {
        self.authorize_peer(&request, "ReservePreparedRootfs", ALLOW_NODE_ADMIN)?;
        let request = request.into_inner();
        let sandbox_id = request
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let rootfs_path = PathBuf::from(paths::sandbox_rootfs_path(&sandbox_id.to_string()));
        let rootfs_path = self
            .prepared_rootfs_reservations
            .reserve_on_blocking_pool(
                Arc::clone(&self.disk_admission),
                sandbox_id,
                rootfs_path,
                request.rootfs_size_mb,
            )
            .await?;
        Ok(Response::new(ReservePreparedRootfsResponse {
            rootfs_path: rootfs_path.to_string_lossy().into_owned(),
        }))
    }

    pub(super) async fn complete_prepared_rootfs(
        &self,
        request: Request<CompletePreparedRootfsRequest>,
    ) -> Result<Response<CompletePreparedRootfsResponse>, Status> {
        self.authorize_peer(&request, "CompletePreparedRootfs", ALLOW_NODE_ADMIN)?;
        let request = request.into_inner();
        let sandbox_id = request
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let disposition = PreparedRootfsDisposition::try_from(request.disposition)
            .unwrap_or(PreparedRootfsDisposition::Unspecified);
        let rootfs_path = PathBuf::from(paths::sandbox_rootfs_path(&sandbox_id.to_string()));
        self.prepared_rootfs_reservations
            .complete(&self.disk_admission, sandbox_id, &rootfs_path, disposition)
            .await?;
        Ok(Response::new(CompletePreparedRootfsResponse {}))
    }

    pub(super) async fn release_prepared_rootfs_reservation(&self, sandbox_id: SandboxId) {
        let sandbox_cache_dir = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
        self.prepared_rootfs_reservations
            .release_if_removed(&self.disk_admission, sandbox_id, &sandbox_cache_dir)
            .await;
    }
}

pub(super) fn validate_ready_handoff(
    instance_dir: &Path,
    rootfs_path: &Path,
    requested_size_mib: Option<u32>,
) -> Result<bool, Status> {
    let rootfs_size_mib =
        match read_handoff_state(instance_dir).map_err(PreparedRootfsError::into_status)? {
            HandoffState::Absent => return Ok(false),
            HandoffState::InProgress(_) => {
                return Err(PreparedRootfsError::StillPreparing.into_status());
            }
            HandoffState::Ready(rootfs_size_mib) => rootfs_size_mib,
        };
    if let Some(requested_size_mib) = requested_size_mib
        && requested_size_mib != rootfs_size_mib
    {
        return Err(PreparedRootfsError::SizeChanged {
            expected_mib: rootfs_size_mib,
            actual_mib: requested_size_mib,
        }
        .into_status());
    }
    validate_rootfs_file(rootfs_path, rootfs_size_mib).map_err(PreparedRootfsError::into_status)?;
    Ok(true)
}

pub(super) fn require_ready_handoff(
    instance_dir: &Path,
    rootfs_path: &Path,
    requested_size_mib: Option<u32>,
) -> Result<(), Status> {
    if validate_ready_handoff(instance_dir, rootfs_path, requested_size_mib)? {
        Ok(())
    } else {
        Err(PreparedRootfsError::MissingHandoff.into_status())
    }
}

fn cache_scoped_prepared_rootfs_paths(
    cache_root: &Path,
    sandbox_id: &str,
) -> Result<(PathBuf, PathBuf), PreparedRootfsError> {
    let instance_dir = cache_scoped_path(cache_root, paths::sandbox_instance_dir(sandbox_id))?;
    let rootfs_path = cache_scoped_path(cache_root, paths::sandbox_rootfs_path(sandbox_id))?;
    Ok((instance_dir, rootfs_path))
}

fn cache_scoped_path(
    cache_root: &Path,
    canonical_path: String,
) -> Result<PathBuf, PreparedRootfsError> {
    let path = PathBuf::from(canonical_path);
    let relative = path
        .strip_prefix(CACHE_DIR)
        .map_err(|source| PreparedRootfsError::Io {
            operation: "resolve prepared rootfs cache path",
            path: path.clone(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, source),
        })?;
    Ok(cache_root.join(relative))
}

fn rootfs_parent(rootfs_path: &Path) -> Result<&Path, Status> {
    rootfs_path.parent().ok_or_else(|| {
        PreparedRootfsError::Io {
            operation: "resolve prepared rootfs parent",
            path: rootfs_path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "rootfs path has no parent",
            ),
        }
        .into_status()
    })
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> PreparedRootfsError {
    PreparedRootfsError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn marker_started_at(instance_dir: &Path) -> Timestamp {
    let started_at = files::directory(instance_dir, false)
        .and_then(|directory| files::regular(&directory, IN_PROGRESS_MARKER.as_ref()))
        .and_then(|marker| marker.metadata())
        .and_then(|metadata| metadata.modified())
        .and_then(|modified| Timestamp::try_from(modified).map_err(std::io::Error::other));
    match started_at {
        Ok(started_at) => started_at,
        Err(error) => {
            tracing::warn!(path = %instance_dir.display(), %error, "cannot read prepared rootfs marker timestamp");
            Timestamp::now()
        }
    }
}

fn create_instance_directory(instance_dir: &Path) -> Result<(), PreparedRootfsError> {
    files::directory(instance_dir, true)
        .map(|_| ())
        .map_err(|error| io_error("create prepared instance directory", instance_dir, error))
}

fn read_handoff_state(instance_dir: &Path) -> Result<HandoffState, PreparedRootfsError> {
    let directory = match files::directory(instance_dir, false) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HandoffState::Absent);
        }
        Err(error) => return Err(io_error("open prepared instance", instance_dir, error)),
    };
    let read_marker = |name: &str| -> Result<Option<u32>, PreparedRootfsError> {
        let path = instance_dir.join(name);
        files::read_marker(&directory, name)
            .map_err(|error| io_error("read prepared rootfs marker", &path, error))?
            .map(|value| {
                let size = value
                    .trim()
                    .parse::<u32>()
                    .map_err(|source| PreparedRootfsError::InvalidMarker { path, source })?;
                validate_rootfs_mib(size)?;
                Ok(size)
            })
            .transpose()
    };
    match (read_marker(IN_PROGRESS_MARKER)?, read_marker(READY_MARKER)?) {
        (None, None) => Ok(HandoffState::Absent),
        (Some(size), None) => Ok(HandoffState::InProgress(size)),
        (None, Some(size)) => Ok(HandoffState::Ready(size)),
        (Some(_), Some(_)) => Err(PreparedRootfsError::ConflictingMarkers),
    }
}

fn write_marker(
    instance_dir: &Path,
    name: &str,
    rootfs_size_mib: u32,
) -> Result<(), PreparedRootfsError> {
    files::directory(instance_dir, false)
        .and_then(|directory| files::write_marker(&directory, name, rootfs_size_mib))
        .map_err(|error| {
            io_error(
                "write prepared rootfs marker",
                &instance_dir.join(name),
                error,
            )
        })
}

fn rename_marker(instance_dir: &Path, from: &str, to: &str) -> Result<(), PreparedRootfsError> {
    files::directory(instance_dir, false)
        .and_then(|directory| files::rename(&directory, from, to))
        .map_err(|error| {
            io_error(
                "commit prepared rootfs marker",
                &instance_dir.join(from),
                error,
            )
        })
}

fn remove_if_present(path: &Path, operation: &'static str) -> Result<(), PreparedRootfsError> {
    let result = (|| {
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::other("missing artifact parent"))?;
        let name = path
            .file_name()
            .ok_or_else(|| std::io::Error::other("missing artifact name"))?;
        files::remove(&files::directory(parent, false)?, name)
    })();
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(operation, path, error)),
    }
}

fn validate_rootfs_file(
    rootfs_path: &Path,
    rootfs_size_mib: u32,
) -> Result<(), PreparedRootfsError> {
    let expected_bytes = validate_rootfs_mib(rootfs_size_mib)?.as_bytes();
    let metadata = (|| {
        let parent = rootfs_path
            .parent()
            .ok_or_else(|| std::io::Error::other("missing rootfs parent"))?;
        let name = rootfs_path
            .file_name()
            .ok_or_else(|| std::io::Error::other("missing rootfs name"))?;
        files::regular(&files::directory(parent, false)?, name)?.metadata()
    })()
    .map_err(|error| {
        if error.kind() == std::io::ErrorKind::InvalidData {
            PreparedRootfsError::NotRegularFile {
                path: rootfs_path.to_path_buf(),
            }
        } else {
            io_error("stat prepared rootfs", rootfs_path, error)
        }
    })?;
    if metadata.len() != expected_bytes {
        return Err(PreparedRootfsError::FileSizeMismatch {
            path: rootfs_path.to_path_buf(),
            expected_bytes,
            actual_bytes: metadata.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
