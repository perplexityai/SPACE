//! Node-local operator ceiling, independent of persisted sandbox assignments.

use std::collections::HashSet;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::{DEFAULT_DISK_IO_BURST_SECS, RateLimiter, TokenBucket};
use serde_json::{Value, json};
use tokio::time::MissedTickBehavior;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::Status;

use super::{JAIL_DIR, NodeManagerService, SlotRecord, slot_store_status};

pub(crate) const ENV: &str = "SPACE_NODEMGR_DISK_IO_MAX_OPERATIONS_PER_SEC";
const INTERVAL: Duration = Duration::from_secs(10);
pub(super) const TARGET_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DiskIoCeiling(Option<NonZeroU64>);

#[derive(Debug, thiserror::Error)]
#[error("{ENV} must be an integer between 1 and {}", i64::MAX)]
pub struct InvalidCeiling;

impl DiskIoCeiling {
    pub(crate) fn from_env() -> Result<Self, InvalidCeiling> {
        match std::env::var(ENV) {
            Ok(value) => Self::parse(Some(&value)),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => Err(InvalidCeiling),
        }
    }

    fn parse(value: Option<&str>) -> Result<Self, InvalidCeiling> {
        value.map_or(Ok(Self::default()), |value| {
            let rate = value.parse::<u64>().map_err(|_| InvalidCeiling)?;
            if rate > i64::MAX as u64 {
                return Err(InvalidCeiling);
            }
            Ok(Self(Some(NonZeroU64::new(rate).ok_or(InvalidCeiling)?)))
        })
    }

    pub(crate) fn configured(self) -> Option<u64> {
        self.0.map(NonZeroU64::get)
    }

    /// A missing/disabled limiter is unlimited. Preserve lower assignments and
    /// bandwidth, and retain the current refill window when reducing a rate.
    fn update(self, configuration: &Value) -> Result<Option<(String, TokenBucket)>, Status> {
        if self.0.is_none() {
            return Ok(None);
        }
        let roots: Vec<_> = configuration
            .get("drives")
            .and_then(Value::as_array)
            .ok_or_else(|| Status::failed_precondition("Firecracker config has no drives"))?
            .iter()
            .filter(|drive| drive.get("is_root_device").and_then(Value::as_bool) == Some(true))
            .collect();
        let [drive] = roots.as_slice() else {
            return Err(Status::failed_precondition(
                "expected exactly one root drive",
            ));
        };
        let id = drive
            .get("drive_id")
            .and_then(Value::as_str)
            .filter(|id| {
                !id.is_empty()
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
            .ok_or_else(|| Status::failed_precondition("invalid root drive identity"))?;
        let ops = drive.pointer("/rate_limiter/ops");
        let bucket =
            ops.filter(|value| !value.is_null())
                .map(|ops| {
                    let size = ops.get("size").and_then(Value::as_u64).ok_or_else(|| {
                        Status::failed_precondition("invalid root operations size")
                    })?;
                    let refill_time_ms = ops
                        .get("refill_time")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            Status::failed_precondition("invalid root operations refill time")
                        })?;
                    Ok::<_, Status>(TokenBucket {
                        size,
                        refill_time_ms,
                    })
                })
                .transpose()?;
        let Some(bucket) = self.capped_bucket(bucket)? else {
            return Ok(None);
        };
        Ok(Some((id.to_owned(), bucket)))
    }

    fn capped_bucket(self, bucket: Option<TokenBucket>) -> Result<Option<TokenBucket>, Status> {
        let Some(maximum) = self.configured() else {
            return Ok(None);
        };
        let Some(bucket) = bucket.filter(|bucket| bucket.size != 0 && bucket.refill_time_ms != 0)
        else {
            return Ok(Some(default_bucket(maximum)));
        };
        let refill = bucket.refill_time_ms;
        if u128::from(bucket.size) * 1000 <= u128::from(maximum) * u128::from(refill) {
            return Ok(None);
        }
        let allowed = u128::from(maximum) * u128::from(refill) / 1000;
        Ok(Some(if allowed == 0 {
            TokenBucket {
                size: maximum,
                refill_time_ms: 1000,
            }
        } else {
            TokenBucket {
                size: u64::try_from(allowed)
                    .map_err(|_| Status::failed_precondition("operations bucket overflow"))?,
                refill_time_ms: refill,
            }
        }))
    }

    pub(super) fn clamp_limiter(self, mut limiter: RateLimiter) -> Result<RateLimiter, Status> {
        if let Some(bucket) = self.capped_bucket(Some(limiter.operations))? {
            limiter.operations = bucket;
        }
        Ok(limiter)
    }

    /// The jail is rebuilt for every boot; publish safely without syncing the
    /// disposable config or forcing a cache-filesystem transaction commit.
    pub(super) fn stage_cold_boot_config(
        self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), Status> {
        let (parent, name) = super::artifact_fs::parent(source)?;
        let mut input = super::trusted_paths::open_regular_file(&parent, name)?;
        super::artifact_fs::write_ephemeral_atomic(destination, |mut output| {
            use std::io::Write;
            if self.0.is_none() {
                std::io::copy(&mut input, &mut output).map_err(super::artifact_fs::error)?;
            } else {
                let mut config: Value =
                    serde_json::from_reader(input).map_err(super::artifact_fs::error)?;
                self.clamp_config(&mut config)?;
                let bytes = serde_json::to_vec(&config).map_err(super::artifact_fs::error)?;
                output
                    .write_all(&bytes)
                    .map_err(super::artifact_fs::error)?;
            }
            Ok(())
        })?;
        Ok(())
    }

    pub(super) async fn enforce_bounded(
        self,
        client: &crate::fc_api::FcApiClient,
    ) -> Result<Enforcement, Status> {
        tokio::time::timeout(TARGET_TIMEOUT, self.enforce(client))
            .await
            .map_err(|_| Status::deadline_exceeded("disk IOPS ceiling timed out"))?
    }

    /// A restore launch starts an idle VMM. Its drives are verified by the
    /// subsequent LoadSnapshot, never by replaying the launch response.
    pub(super) async fn enforce_launch_adoption(
        self,
        client: &crate::fc_api::FcApiClient,
        snapshot_restore: bool,
    ) -> Result<(), Status> {
        if snapshot_restore {
            return Ok(());
        }
        self.enforce_bounded(client).await?.require_verified()
    }

    /// The caller cannot adopt or resume a loaded VM until this succeeds.
    pub(super) async fn enforce_before_adoption<F, Fut>(
        self,
        client: &crate::fc_api::FcApiClient,
        rollback: F,
    ) -> Result<(), Status>
    where
        F: FnOnce(Status) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        if let Err(status) = self
            .enforce_bounded(client)
            .await
            .and_then(Enforcement::require_verified)
        {
            rollback(status.clone()).await;
            return Err(status);
        }
        Ok(())
    }

    pub(super) fn clamp_config(self, config: &mut Value) -> Result<bool, Status> {
        let Some((id, bucket)) = self.update(config)? else {
            return Ok(false);
        };
        let drive = config
            .get_mut("drives")
            .and_then(Value::as_array_mut)
            .and_then(|drives| {
                drives.iter_mut().find(|drive| {
                    drive.get("drive_id").and_then(Value::as_str) == Some(id.as_str())
                })
            })
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Status::failed_precondition("invalid root drive"))?;
        let limiter = drive.entry("rate_limiter").or_insert(Value::Null);
        if limiter.is_null() {
            *limiter = json!({});
        }
        limiter
            .as_object_mut()
            .ok_or_else(|| Status::failed_precondition("invalid drive rate limiter"))?
            .insert(
                "ops".to_owned(),
                json!({
                    "size":bucket.size,"refill_time":bucket.refill_time_ms,"one_time_burst":0
                }),
            );
        Ok(true)
    }

    pub(super) async fn enforce(
        self,
        client: &crate::fc_api::FcApiClient,
    ) -> Result<Enforcement, Status> {
        if self.0.is_none() {
            return Ok(Enforcement::Verified { updated: false });
        }
        let configuration = client.configuration().await.map_err(api_error)?;
        if configuration
            .get("drives")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return Ok(Enforcement::AwaitingSnapshot);
        }
        let Some((id, bucket)) = self.update(&configuration)? else {
            return Ok(Enforcement::Verified { updated: false });
        };
        client
            .update_drive_operations(&id, bucket)
            .await
            .map_err(api_error)?;
        // An unchanged limiter is not patched: periodically replenishing the
        // bucket would itself let a guest exceed its sustained allowance.
        let observed = client.configuration().await.map_err(api_error)?;
        if self.update(&observed)?.is_some() {
            return Err(Status::unavailable(
                "Firecracker did not apply the disk IOPS ceiling",
            ));
        }
        Ok(Enforcement::Verified { updated: true })
    }
}

fn default_bucket(rate: u64) -> TokenBucket {
    TokenBucket {
        size: rate.saturating_mul(u64::from(DEFAULT_DISK_IO_BURST_SECS.get())),
        refill_time_ms: u64::from(DEFAULT_DISK_IO_BURST_SECS.get()) * 1000,
    }
}

fn api_error(error: crate::fc_api::FcApiError) -> Status {
    Status::unavailable(format!("enforce rootfs disk IOPS ceiling: {error}"))
}

/// A verification is valid only for this particular leased VM. The cache is
/// owned by the sweep and pruned against each successful inventory.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct VerifiedVm {
    sandbox_id: String,
    generation: u64,
    slot: u32,
    pid: u32,
}

impl VerifiedVm {
    fn from_record(record: &SlotRecord) -> Option<Self> {
        (!record.info.sandbox_id.is_empty()).then_some(())?;
        Some(Self {
            sandbox_id: record.info.sandbox_id.clone(),
            generation: record.info.generation,
            slot: record.info.slot,
            pid: record.info.pid?,
        })
    }
}

#[derive(Default)]
struct VerifiedVms(HashSet<VerifiedVm>);

impl VerifiedVms {
    fn observe(&mut self, record: &SlotRecord, outcome: &Result<Enforcement, Status>) -> bool {
        if !matches!(outcome, Ok(Enforcement::Verified { .. })) {
            return false;
        }
        VerifiedVm::from_record(record).is_some_and(|key| self.0.insert(key))
    }

    fn pending(&mut self, records: &[SlotRecord]) -> Vec<SlotRecord> {
        let active: HashSet<_> = records.iter().filter_map(VerifiedVm::from_record).collect();
        self.0.retain(|key| active.contains(key));
        records
            .iter()
            .filter(|record| {
                VerifiedVm::from_record(record).is_some_and(|key| !self.0.contains(&key))
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Enforcement {
    Verified { updated: bool },
    AwaitingSnapshot,
    Superseded,
}

impl Enforcement {
    fn require_verified(self) -> Result<(), Status> {
        match self {
            Self::Verified { .. } => Ok(()),
            Self::AwaitingSnapshot => {
                Err(Status::failed_precondition("root drive is not configured"))
            }
            Self::Superseded => Err(Status::aborted("disk IOPS target was superseded")),
        }
    }
}

impl NodeManagerService {
    pub(crate) fn with_disk_io_ceiling(mut self, ceiling: DiskIoCeiling) -> Self {
        self.disk_io_ceiling = ceiling;
        self
    }

    pub(crate) fn start_disk_io_ceiling_reconciler(
        &self,
        shutdown: CancellationToken,
        tracker: &TaskTracker,
    ) {
        let service = self.clone();
        tracker.spawn(async move {
            let mut verified = VerifiedVms::default();
            let mut interval = tokio::time::interval(INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return,
                    _ = interval.tick() => {}
                }
                // DogStatsD gauges need periodic samples, including disabled nodes.
                pplx_space_metrics::node_disk::disk_io_ceiling_config(
                    service.disk_io_ceiling.configured(),
                );
                if service.disk_io_ceiling.0.is_none() {
                    continue;
                }
                let pool = service.pool.clone();
                let records = match service
                    .off_runtime("disk IOPS ceiling inventory", move || {
                        pool.all_records().map_err(slot_store_status)
                    })
                    .await
                {
                    Ok(records) => records,
                    Err(error) => {
                        pplx_space_metrics::node_disk::disk_io_ceiling_inventory_failed();
                        tracing::warn!(%error, "disk IOPS ceiling inventory failed; retrying next sweep");
                        continue;
                    }
                };
                let inventory_timestamp = jiff::Timestamp::now().as_second() as f64;
                let targets = verified.pending(&records);
                let mut pending = targets.len();
                publish_inventory(
                    service.disk_io_ceiling,
                    &verified,
                    pending,
                    inventory_timestamp,
                );
                let mut work = stream::iter(targets)
                    .map(|record| {
                        let service = service.clone();
                        async move {
                            let result = tokio::time::timeout(
                                TARGET_TIMEOUT,
                                service.enforce_disk_io_ceiling_on_record(&record),
                            )
                            .await
                            .unwrap_or_else(|_| {
                                Err(Status::deadline_exceeded("disk IOPS ceiling timed out"))
                            });
                            (record, result)
                        }
                    })
                    .buffer_unordered(4);
                loop {
                    let outcome = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return,
                        outcome = work.next() => outcome,
                    };
                    let Some((record, result)) = outcome else {
                        break;
                    };
                    if verified.observe(&record, &result) {
                        pending -= 1;
                    }
                    match result {
                        Ok(Enforcement::Verified { updated }) => {
                            if updated {
                                pplx_space_metrics::node_disk::disk_io_ceiling_updated();
                                tracing::info!(
                                    sandbox_id = record.info.sandbox_id,
                                    maximum_iops = service.disk_io_ceiling.configured(),
                                    "running VM disk IOPS ceiling applied"
                                );
                            }
                        }
                        // Keep superseded targets pending until a fresh inventory
                        // identifies the replacement; stale evidence is not success.
                        Ok(Enforcement::AwaitingSnapshot | Enforcement::Superseded) => {}
                        Err(error) => {
                            pplx_space_metrics::node_disk::disk_io_ceiling_update_failed(
                                error.code() == tonic::Code::DeadlineExceeded,
                            );
                            tracing::warn!(
                                sandbox_id = record.info.sandbox_id,
                                %error,
                                "disk IOPS ceiling not verified; retrying next sweep"
                            );
                        }
                    }
                    publish_inventory(
                        service.disk_io_ceiling,
                        &verified,
                        pending,
                        inventory_timestamp,
                    );
                }
            }
        });
    }

    /// The deadline includes lock acquisition, identity revalidation and API I/O.
    /// A failed target is retried by the next sweep; no saved assignment changes.
    async fn enforce_disk_io_ceiling_on_record(
        &self,
        record: &SlotRecord,
    ) -> Result<Enforcement, Status> {
        let id = &record.info.sandbox_id;
        let _guard = self.lock_sandbox_vm_cgroup_launch(id).await;
        let service = self.clone();
        let expected = record.info.clone();
        let current = self
            .off_runtime("validate disk IOPS ceiling target", move || {
                let id = &expected.sandbox_id;
                let Some(current) = service.pool.get(id).map_err(slot_store_status)? else {
                    return Ok(false);
                };
                if current.generation != expected.generation
                    || current.slot != expected.slot
                    || current.pid != expected.pid
                {
                    return Ok(false);
                }
                let fence = service
                    .pool
                    .execution_fence(id)
                    .map_err(slot_store_status)?
                    .ok_or_else(|| {
                        Status::failed_precondition("disk IOPS target lacks execution authority")
                    })?;
                service.check_execution(id, Some(&fence.into()), false)?;
                Ok(true)
            })
            .await?;
        if !current {
            return Ok(Enforcement::Superseded);
        }
        let client = crate::fc_api::FcApiClient::new(format!(
            "{JAIL_DIR}/firecracker/{id}/root/firecracker.sock"
        ));
        self.disk_io_ceiling.enforce(&client).await
    }
}

fn publish_inventory(
    ceiling: DiskIoCeiling,
    verified: &VerifiedVms,
    pending: usize,
    timestamp: f64,
) {
    pplx_space_metrics::node_disk::disk_io_ceiling_config(ceiling.configured());
    pplx_space_metrics::node_disk::disk_io_ceiling_inventory(
        verified.0.len() as u64,
        pending as u64,
        timestamp,
    );
}

#[cfg(test)]
mod tests;
