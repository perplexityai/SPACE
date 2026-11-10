//! Optional node-local work. Only receipts are durable: discovery hints and the
//! three-failure target cutoff deliberately reset with the manager process.
use super::*;
use std::collections::{HashMap, HashSet};
use tokio::time::Instant;

const PASS: Duration = Duration::from_secs(60);
const QUIET: u64 = 120;
const RETRY: u64 = 15 * 60;
const PROBES_PER_PASS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Defer {
    Grace,
    Quiet,
    Busy,
    Lifecycle,
    Disk,
    Unavailable,
    NoTarget,
    Unsupported,
    NotRequired,
    Cutoff,
    Active,
}
impl Defer {
    fn as_str(self) -> &'static str {
        match self {
            Self::Grace => "activation_grace",
            Self::Quiet => "customer_activity",
            Self::Busy => "guest_busy",
            Self::Lifecycle => "lifecycle_window",
            Self::Disk => "cache_disk_not_ready",
            Self::Unavailable => "observation_unavailable",
            Self::NoTarget => "no_qualified_target",
            Self::Unsupported => "unsupported_guest",
            Self::NotRequired => "target_not_required",
            Self::Cutoff => "target_cutoff",
            Self::Active => "operation_active",
        }
    }
    fn until_change(self) -> bool {
        matches!(
            self,
            Self::NoTarget | Self::Unsupported | Self::NotRequired | Self::Cutoff
        )
    }
}

struct Consideration {
    execution: SandboxActivationAttemptId,
    template: Option<String>,
    authority: Option<model::ArtifactDigest>,
    next: Option<Instant>,
    quiet_after: Instant,
    reason: Defer,
}

#[derive(Default)]
pub(in crate::service) struct Automatic {
    // Compound scheduling and cutoff decisions share one short lock. No I/O
    // or asynchronous work runs while it is held.
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    candidates: HashMap<String, Consideration>,
    failures: HashMap<model::ArtifactDigest, HashSet<Uuid>>,
    cutoff_capacity_reached: bool,
}
impl Automatic {
    pub(in crate::service) fn activation(&self, sandbox: &str) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .candidates
            .remove(sandbox);
    }
    pub(super) fn observe(&self, record: &Record) {
        if record.phase != Phase::Complete
            || !matches!(
                record.outcome,
                Some(Outcome::CandidateFailed | Outcome::RetryExhausted)
            )
        {
            return;
        }
        if let Action::Apply { promotion, .. } = &record.intent.action {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.failures.len() >= 256 && !state.failures.contains_key(&promotion.target) {
                if !state.cutoff_capacity_reached {
                    tracing::warn!(
                        "automatic guest updates disabled until manager restart: failed-target capacity reached"
                    );
                    state.cutoff_capacity_reached = true;
                }
                return;
            }
            let failures = state.failures.entry(promotion.target).or_default();
            if failures.len() < 3 {
                failures.insert(record.intent.operation);
            }
        }
    }
    fn cut_off(&self, target: model::ArtifactDigest) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.cutoff_capacity_reached
            || state
                .failures
                .get(&target)
                .is_some_and(|ops| ops.len() >= 3)
    }
    pub(super) fn status(&self, sandbox: &str) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .candidates
            .get(sandbox)
            .map(|entry| format!("auto_deferred:{}", entry.reason.as_str()))
    }
    fn due(&self, row: &crate::db::SandboxRow, authority: Option<model::ArtifactDigest>) -> bool {
        let Some(execution) = row.node_execution_id else {
            return false;
        };
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.candidates.len() >= 65_536 && !state.candidates.contains_key(&row.id) {
            return false;
        }
        let entry = state
            .candidates
            .entry(row.id.clone())
            .or_insert_with(|| Consideration {
                execution,
                template: row.template_hash.clone(),
                authority,
                next: Some(now + Duration::from_secs(30 + jitter(&row.id, 91))),
                quiet_after: now + Duration::from_secs(QUIET),
                reason: Defer::Grace,
            });
        if entry.execution != execution
            || entry.template != row.template_hash
            || entry.authority != authority
        {
            *entry = Consideration {
                execution,
                template: row.template_hash.clone(),
                authority,
                next: Some(now + Duration::from_secs(30 + jitter(&row.id, 91))),
                quiet_after: now + Duration::from_secs(QUIET),
                reason: Defer::Grace,
            };
        }
        entry.next.is_some_and(|next| now >= next)
    }
    fn defer(&self, sandbox: &str, reason: Defer, seconds: Option<u64>) {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .candidates
            .get_mut(sandbox)
        {
            entry.reason = reason;
            entry.next = (!reason.until_change()).then(|| {
                Instant::now()
                    + Duration::from_secs(
                        seconds.unwrap_or(RETRY).max(PASS.as_secs()) + jitter(sandbox, 61),
                    )
            });
        }
    }
    fn quiet_remaining(&self, row: &crate::db::SandboxRow) -> u64 {
        let grace = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .candidates
            .get(&row.id)
            .map_or(0, |entry| {
                entry
                    .quiet_after
                    .saturating_duration_since(Instant::now())
                    .as_secs()
            });
        grace.max(quiet_remaining(row, now_secs()))
    }
}

// Stable per-sandbox discovery jitter does not need randomness or persistence.
fn jitter(id: &str, range: u64) -> u64 {
    id.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u64::from(byte))
    }) % range
}
pub(super) fn quiet_remaining(row: &crate::db::SandboxRow, now: u64) -> u64 {
    row.auto_lifecycle_state
        .last_access_at
        .into_iter()
        .chain(row.auto_lifecycle_state.last_command_at)
        .max()
        .map_or(0, |last| last.saturating_add(QUIET).saturating_sub(now))
}
fn customer(row: &crate::db::SandboxRow) -> bool {
    row.state.is_unowned(SandboxState::Started)
        && row.cleanup_completion.is_none()
        && row.pool_template.is_none()
        && row.node_execution_id.is_some()
        && !matches!(
            row.user.kind(),
            SandboxOwnerKind::Builder
                | SandboxOwnerKind::TemplateManager
                | SandboxOwnerKind::Orphaned
        )
}
fn automatic_target(policy: &policy::Policy, guest: &wire::Status) -> bool {
    let promotion = &policy.promotion;
    let Ok(version) = guest.committed_version.parse::<semver::Version>() else {
        return false;
    };
    let Some(committed) = guest.committed.as_ref().and_then(|s| s.digest.parse().ok()) else {
        return false;
    };
    promotion.intent == model::policy::SelectionIntent::Upgrade
        && promotion.maximum_guest == promotion.target_version
        && pplx_space_spaced_common::spaced_compatibility_family_for_version(
            &guest.committed_version,
        )
        .ok()
            == pplx_space_spaced_common::spaced_compatibility_family_for_version(
                &promotion.target_version.to_string(),
            )
            .ok()
        && promotion
            .should_install(&version, committed)
            .unwrap_or(false)
}

impl SandboxManagerService {
    pub fn start_automatic_guest_updates(
        &self,
        shutdown: CancellationToken,
        tracker: &TaskTracker,
    ) {
        // Read once: disabling optional admissions never disables receipt recovery.
        if std::env::var("SPACE_GUEST_AUTO_UPDATE").is_ok_and(|v| v == "0") {
            tracing::info!("automatic guest updates disabled");
            return;
        }
        let service = self.clone();
        tracker.spawn(async move {
            let node = service.node_manager.lazy();
            let mut tick = tokio::time::interval(PASS);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased; _ = shutdown.cancelled() => return, _ = tick.tick() => {} }
                tokio::select! { biased; _ = shutdown.cancelled() => return,
                    result = service.automatic_guest_update_pass(&node) => {
                        if let Err(error) = result { tracing::debug!(%error, "automatic guest update discovery deferred"); }
                    }
                }
            }
        });
    }

    async fn automatic_guest_update_pass(
        &self,
        node: &pplx_space_node_mgr_client::NodeManagerGrpcClient,
    ) -> Result<(), Status> {
        let catalog = match policy::catalog().await {
            Ok(catalog) => Some(catalog),
            Err(error) => {
                tracing::debug!(%error, "no usable installed guest-update catalog");
                None
            }
        };
        // Qualification lists belong to the catalog, not to every sandbox's
        // scheduling hint. Hash each entry once per pass for cache invalidation.
        let identities: HashMap<_, _> = catalog
            .as_ref()
            .into_iter()
            .flat_map(|c| c.entries())
            .map(|(_, _, policy)| {
                Ok((
                    policy.promotion.target,
                    model::ArtifactDigest::of_bytes(&serde_json::to_vec(policy).map_err(internal)?),
                ))
            })
            .collect::<Result<_, Status>>()?;
        let architecture = std::env::consts::ARCH.parse().map_err(internal)?;
        let mut cursor = crate::db::inventory::SandboxInventoryScan::default();
        let mut present = HashSet::new();
        let mut probes = 0;
        let mut disk_ready = None;
        loop {
            let rows =
                crate::db::scan::sandbox_pages(&*self.db_conn()?, &mut cursor).map_err(internal)?;
            for row in rows {
                if !customer(&row) {
                    continue;
                }
                present.insert(row.id.clone());
                let selected = qualification::platform(&row)
                    .and_then(|p| catalog.as_ref()?.select(p, architecture));
                if !self.guest_auto_updates.due(
                    &row,
                    selected.and_then(|policy| identities.get(&policy.promotion.target).copied()),
                ) {
                    continue;
                }
                let Some(policy) = selected else {
                    self.guest_auto_updates
                        .defer(&row.id, Defer::NoTarget, None);
                    continue;
                };
                if self.guest_auto_updates.cut_off(policy.promotion.target) {
                    self.guest_auto_updates.defer(&row.id, Defer::Cutoff, None);
                    continue;
                }
                let quiet = self.guest_auto_updates.quiet_remaining(&row);
                if quiet > 0 {
                    self.guest_auto_updates
                        .defer(&row.id, Defer::Quiet, Some(quiet));
                    continue;
                }
                if qualify(&row, OPERATION_BUDGET).is_err() {
                    self.guest_auto_updates
                        .defer(&row.id, Defer::Lifecycle, None);
                    continue;
                }
                if crate::db::read::active_guest_update(&*self.db_conn()?, &row.id)
                    .map_err(internal)?
                    .is_some_and(|r| {
                        Some(r.intent.execution) == row.node_execution_id
                            && r.phase != Phase::Complete
                    })
                {
                    self.guest_auto_updates
                        .defer(&row.id, Defer::Active, Some(PASS.as_secs()));
                    continue;
                }
                if probes >= PROBES_PER_PASS {
                    continue;
                }
                let Ok(permit) = self.guest_update_sem.clone().try_acquire_owned() else {
                    continue;
                };
                probes += 1;
                if disk_ready.is_none() {
                    let result = tokio::time::timeout(
                        RPC_BUDGET,
                        node.clone().get_node_resource_status(request(
                            pplx_space_node_mgr_client::proto::NodeResourceStatusRequest {},
                        )),
                    )
                    .await;
                    disk_ready = Some(
                        matches!(result, Ok(Ok(response)) if response.get_ref().cache_disk.as_ref().is_some_and(|disk|
                        disk.admission_state == pplx_space_node_mgr_client::proto::CacheDiskAdmissionState::Ready as i32)),
                    );
                }
                if disk_ready != Some(true) {
                    self.guest_auto_updates.defer(&row.id, Defer::Disk, None);
                    continue;
                }
                self.guest_auto_updates
                    .defer(&row.id, Defer::Unavailable, None);
                if let Err(error) = tokio::time::timeout(
                    RPC_BUDGET,
                    self.consider_automatic_guest_update(&row, policy, permit),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("automatic guest probe deadline"))
                .and_then(|result| result)
                {
                    tracing::debug!(sandbox_id = %row.id, %error, "automatic guest update deferred");
                }
            }
            if cursor.sweep_complete() {
                break;
            }
            tokio::task::yield_now().await;
        }
        self.guest_auto_updates
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .candidates
            .retain(|id, _| present.contains(id));
        Ok(())
    }

    async fn consider_automatic_guest_update(
        &self,
        row: &crate::db::SandboxRow,
        policy: &policy::Policy,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), Status> {
        if !self.update_template_qualified(row, policy).await? {
            self.guest_auto_updates
                .defer(&row.id, Defer::NoTarget, None);
            return Ok(());
        }
        let seen = self
            .connect_spaced_host(row)
            .await?
            .guest_update()
            .get_status(request(wire::StatusRequest {}))
            .await;
        let seen = match seen {
            Err(error) if error.code() == tonic::Code::Unimplemented => {
                self.guest_auto_updates
                    .defer(&row.id, Defer::Unsupported, None);
                return Ok(());
            }
            other => other?.into_inner(),
        };
        if model::RecoveryPlatform::from_contract(&seen.seed_contract)
            != qualification::platform(row)
        {
            self.guest_auto_updates
                .defer(&row.id, Defer::Unsupported, None);
            return Ok(());
        }
        if !automatic_target(policy, &seen) {
            self.guest_auto_updates
                .defer(&row.id, Defer::NotRequired, None);
            return Ok(());
        }
        if !seen.defer_reasons.is_empty() || seen.customer_admission_held {
            self.guest_auto_updates.defer(&row.id, Defer::Busy, None);
            return Ok(());
        }
        // Load/verify installed authority before creating a durable intent. The
        // driver checks it again before every irreversible transition.
        let bundle = policy::load(policy.promotion.target).await?;
        if &bundle.policy != policy {
            return Err(Status::aborted("installed guest target changed"));
        }
        let current = self.lookup_sandbox(&row.id)?;
        qualify(&current, OPERATION_BUDGET)?;
        if current.node_execution_id != row.node_execution_id
            || self.guest_auto_updates.quiet_remaining(&current) > 0
        {
            return Err(Status::aborted("guest execution or quiet period changed"));
        }
        let operation = Uuid::now_v7();
        self.admit_guest_update(Intent {
            operation,
            sandbox: row.id.parse().map_err(internal)?,
            execution: row
                .node_execution_id
                .ok_or_else(|| Status::aborted("guest execution disappeared"))?,
            action: Action::Apply {
                promotion: Box::new(policy.promotion.clone()),
                authorization: policy.authorization,
            },
            force: false,
            configuration_revision: 1,
        })
        .await?;
        self.guest_auto_updates
            .defer(&row.id, Defer::Active, Some(PASS.as_secs()));
        // The held permit prevents admission from spawning a competing driver.
        // A crash here is recovered from the same receipt by the existing scan.
        self.spawn_guest_update(operation, permit, true);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
