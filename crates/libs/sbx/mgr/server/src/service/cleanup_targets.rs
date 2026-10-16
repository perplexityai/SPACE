use super::*;

use super::grpc_sandbox_transfer::receive::{ExportSender, export_channel};
use crate::db::cleanup_target_scan::{BATCH_SIZE, CleanupTargetScan};
use crate::proto::{CleanupTargetBatch, CleanupTargetObservation, ScanCleanupTargetsRequest};

/// The directory iterator and rowid sweep live only as long as one bounded
/// stream. Ordinary List remains complete and is not silently truncated.
#[derive(Default)]
struct CleanupInventoryCursor {
    database: CleanupTargetScan,
    directory: Option<std::fs::ReadDir>,
    complete: bool,
}

impl CleanupInventoryCursor {
    fn next_batch(
        mut self,
        service: &SandboxManagerService,
        cache_dir: &Path,
    ) -> Result<(Self, CleanupTargetBatch), Status> {
        let mut targets = Vec::new();
        if !self.database.complete() {
            let observations = self
                .database
                .next_batch(&*service.db_conn()?)
                .map_err(|error| error.into_status())?;
            targets.extend(observations.into_iter().map(|row| {
                CleanupTargetObservation {
                    sandbox_id: row.raw_id,
                    template: row
                        .metadata
                        .as_ref()
                        .map(|metadata| metadata.template.clone()),
                    metadata_valid: row.metadata.is_some(),
                }
            }));
            return Ok((self, CleanupTargetBatch { targets }));
        }
        if self.directory.is_none() {
            self.directory = match std::fs::read_dir(cache_dir) {
                Ok(entries) => Some(entries),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    self.complete = true;
                    return Ok((self, CleanupTargetBatch { targets }));
                }
                Err(error) => return Err(Status::internal(format!("scan sandbox cache: {error}"))),
            };
        }
        if let Some(entries) = self.directory.as_mut() {
            for _ in 0..BATCH_SIZE {
                let Some(entry) = entries.next() else {
                    self.complete = true;
                    break;
                };
                let entry = entry
                    .map_err(|error| Status::internal(format!("scan sandbox cache: {error}")))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                let Ok(id) = name.parse::<SandboxId>() else {
                    continue;
                };
                let file_type = match entry.file_type() {
                    Ok(file_type) => file_type,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        return Err(Status::internal(format!("inspect sandbox cache: {error}")));
                    }
                };
                if !file_type.is_dir() {
                    continue;
                }
                // No filesystem operation runs while SQLite is locked. This
                // suppresses stable duplicates without retaining all seen IDs;
                // concurrent deletion can repeat a target, as the wire states.
                if crate::db::sandbox_exists(&*service.db_conn()?, &name)
                    .map_err(|error| Status::internal(format!("inspect cleanup target: {error}")))?
                {
                    continue;
                }
                targets.push(CleanupTargetObservation {
                    metadata_valid: id.to_string() == name,
                    sandbox_id: name,
                    template: None,
                });
            }
        }
        Ok((self, CleanupTargetBatch { targets }))
    }
}

impl SandboxManagerService {
    pub(super) async fn scan_cleanup_targets(
        &self,
        request: Request<ScanCleanupTargetsRequest>,
    ) -> Result<Response<BoxedStatusStream<CleanupTargetBatch>>, Status> {
        const ALLOWED: &[UdsPeerPrincipal] = &[
            UdsPeerPrincipal::Root,
            UdsPeerPrincipal::Role(LocalRole::Observer),
            UdsPeerPrincipal::Role(LocalRole::Operator),
            UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
            UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
            UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
        ];
        self.authorize_peer(&request, "ScanCleanupTargets", ALLOWED)?;
        // Match the ordinary node-local client's two-minute request budget.
        // A dispatched read-only page keeps its storage permit until it finishes,
        // but cannot keep the stream alive past its deadline or cancellation.
        let budget = super::activation_support::grpc_timeout_budget(&request)
            .unwrap_or(Duration::from_secs(120))
            .min(Duration::from_secs(120));
        let deadline = tokio::time::Instant::now() + budget;
        Ok(Response::new(self.start_cleanup_target_stream(
            PathBuf::from(CACHE_DIR),
            deadline,
        )?))
    }

    fn start_cleanup_target_stream(
        &self,
        cache_dir: PathBuf,
        deadline: tokio::time::Instant,
    ) -> Result<BoxedStatusStream<CleanupTargetBatch>, Status> {
        let permit = self
            .cleanup_target_streams
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("cleanup target inventory is busy"))?;
        if self.shutdown_token.is_cancelled() {
            return Err(Status::unavailable(
                "cleanup target inventory is shutting down",
            ));
        }
        let (sender, stream) = export_channel();
        let service = self.clone();
        self.task_tracker.spawn(async move {
            let _permit = permit;
            let result = service
                .stream_cleanup_targets(&sender, deadline, cache_dir)
                .await;
            sender.finish(result);
        });
        Ok(stream)
    }

    async fn stream_cleanup_targets(
        &self,
        sender: &ExportSender<CleanupTargetBatch>,
        deadline: tokio::time::Instant,
        cache_dir: PathBuf,
    ) -> Result<(), Status> {
        let mut cursor = CleanupInventoryCursor::default();
        loop {
            if self.shutdown_token.is_cancelled() || sender.is_closed() {
                return Err(Status::cancelled("cleanup target inventory cancelled"));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "cleanup target inventory incomplete",
                ));
            }
            let cache_dir = cache_dir.clone();
            let (next, batch) = tokio::select! {
                biased;
                _ = self.shutdown_token.cancelled() => return Err(Status::cancelled("cleanup target inventory cancelled")),
                _ = sender.closed() => return Err(Status::cancelled("cleanup target inventory reader closed")),
                _ = tokio::time::sleep_until(deadline) => return Err(Status::deadline_exceeded("cleanup target inventory incomplete")),
                result = self.blocking_io(move |service| cursor.next_batch(service, &cache_dir)) => result?,
            };
            cursor = next;
            if !batch.targets.is_empty() {
                tokio::select! {
                    biased;
                    _ = self.shutdown_token.cancelled() => return Err(Status::cancelled("cleanup target inventory cancelled")),
                    _ = tokio::time::sleep_until(deadline) => return Err(Status::deadline_exceeded("cleanup target inventory incomplete")),
                    result = sender.send(batch) => result?,
                }
            }
            if cursor.complete {
                return Ok(());
            }
        }
    }
}

impl SandboxManagerService {
    /// The root holding each sandbox's canonical cache directory. Tests may
    /// redirect it so rowless disk state is observable without host paths.
    pub(super) fn sandbox_cache_root(&self) -> PathBuf {
        #[cfg(test)]
        if let Some(cache_dir) = self.cleanup_executor.cache_dir.lock().unwrap().clone() {
            return cache_dir;
        }
        PathBuf::from(CACHE_DIR)
    }

    pub(super) async fn resolve_lifecycle_target(
        &self,
        sandbox_id: SandboxId,
        template: Option<&str>,
        template_manager: bool,
        running_only: bool,
    ) -> Result<Option<(String, Option<SandboxRuntimeLabel>)>, Status> {
        let permit = Arc::clone(&self.cleanup_io)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("cleanup storage is shutting down"))?;
        #[cfg(test)]
        let run_dir = self
            .cleanup_run_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(RUN_DIR));
        let cache_dir = self.sandbox_cache_root();
        let db = self.db.clone();
        let template = template.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(test)]
            let is_running = |id: &str| cache_cleanup::is_sandbox_running_in(&run_dir, id);
            #[cfg(not(test))]
            let is_running = is_sandbox_running;
            resolve_lifecycle_target_blocking(
                &db,
                &cache_dir,
                sandbox_id,
                template.as_deref(),
                template_manager,
                running_only,
                is_running,
            )
        })
        .await
        .map_err(|error| Status::internal(format!("cleanup lookup task failed: {error}")))?
    }
}

fn resolve_lifecycle_target_blocking(
    db: &crate::db::SharedConnection,
    cache_dir: &Path,
    sandbox_id: SandboxId,
    template: Option<&str>,
    template_manager: bool,
    running_only: bool,
    is_running: impl Fn(&str) -> bool,
) -> Result<Option<(String, Option<SandboxRuntimeLabel>)>, Status> {
    // One logical identity, one indexed lookup. Missing rows may still own a
    // cache directory; neither case enumerates unrelated database or disk state.
    let id = sandbox_id.to_string();
    let row = {
        let db = db
            .lock()
            .map_err(|_| Status::internal("cleanup database mutex poisoned"))?;
        crate::db::cleanup_target(&db, &id)
            .map_err(|error| Status::internal(format!("read cleanup target: {error}")))?
    };
    if row.is_none() {
        match std::fs::metadata(cache_dir.join(&id)) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Status::internal(format!("inspect sandbox cache: {error}"))),
        }
    }
    if running_only && !is_running(&id) {
        return Ok(None);
    }
    if template_manager {
        match row.as_ref() {
            Some(row) if row.user.kind() == SandboxOwnerKind::TemplateManager => {}
            Some(_) => {
                return Err(Status::permission_denied(
                    "space-tmplmgr may only clean up template-owned sandboxes",
                ));
            }
            None => return Ok(None),
        }
    }
    if template
        .is_some_and(|filter| filter != row.as_ref().map(|row| row.template.as_str()).unwrap_or(""))
    {
        return Ok(None);
    }
    Ok(Some((id, row.map(|row| row.uuid8))))
}

#[cfg(test)]
mod tests;
