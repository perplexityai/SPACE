use std::collections::BTreeSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_svc::types::{ReconcileReason, normalize_reconcile_reason};
use pplx_space_node_api_client::proto::{
    StateReconciliationReason, TriggerStateReconciliationRequest,
};
use tokio::sync::{Mutex as TokioMutex, mpsc};
use tonic::Status;

use crate::WorkItem;

const DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE: Duration = Duration::from_millis(150);
const DEFAULT_STATE_RECONCILE_TRIGGER_JITTER_MAX: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub(crate) struct ClusterReconcileTrigger {
    tx: mpsc::Sender<WorkItem>,
    state: Arc<TokioMutex<ClusterReconcileTriggerState>>,
    debounce: Duration,
    jitter_max: Duration,
    jitter_seed: u64,
}

#[derive(Debug, Default)]
struct ClusterReconcileTriggerState {
    scheduled: bool,
    running: bool,
    epoch: u64,
    pending_reasons: BTreeSet<ReconcileReason>,
    pending_diagnostic_reasons: BTreeSet<String>,
}

#[derive(Debug)]
pub(crate) struct ClusterReconcileDrainStart {
    pub(crate) epoch: u64,
    pub(crate) reason: String,
}

impl ClusterReconcileTrigger {
    pub(crate) fn new(tx: mpsc::Sender<WorkItem>, node_id: &str) -> Self {
        Self::with_timing(
            tx,
            DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE,
            DEFAULT_STATE_RECONCILE_TRIGGER_JITTER_MAX,
            stable_jitter_seed(node_id),
        )
    }

    #[cfg(test)]
    fn with_debounce(tx: mpsc::Sender<WorkItem>, debounce: Duration) -> Self {
        Self::with_timing(tx, debounce, Duration::ZERO, 0)
    }

    fn with_timing(
        tx: mpsc::Sender<WorkItem>,
        debounce: Duration,
        jitter_max: Duration,
        jitter_seed: u64,
    ) -> Self {
        Self {
            tx,
            state: Arc::new(TokioMutex::new(ClusterReconcileTriggerState::default())),
            debounce,
            jitter_max,
            jitter_seed,
        }
    }

    pub(crate) async fn trigger(
        &self,
        reason: impl Into<TriggerRequestReason>,
    ) -> Result<(), Status> {
        let reason = reason.into();
        let schedule_epoch = {
            let mut state = self.state.lock().await;
            state.epoch = state.epoch.saturating_add(1);
            state.pending_reasons.extend(reason.reasons);
            state
                .pending_diagnostic_reasons
                .extend(reason.diagnostic_reasons);
            if state.scheduled || state.running {
                None
            } else {
                state.scheduled = true;
                Some(state.epoch)
            }
        };
        if let Some(epoch) = schedule_epoch {
            self.spawn_delayed_drain(epoch);
        }
        Ok(())
    }

    pub(crate) async fn mark_started(&self) -> ClusterReconcileDrainStart {
        let mut state = self.state.lock().await;
        state.scheduled = false;
        state.running = true;
        let reason = drain_reason_string(
            std::mem::take(&mut state.pending_reasons),
            std::mem::take(&mut state.pending_diagnostic_reasons),
        );
        ClusterReconcileDrainStart {
            epoch: state.epoch,
            reason,
        }
    }

    pub(crate) async fn mark_finished(&self, started_epoch: u64, force_follow_up: bool) {
        let schedule_epoch = {
            let mut state = self.state.lock().await;
            state.running = false;
            if force_follow_up || state.epoch != started_epoch {
                if state.scheduled {
                    None
                } else {
                    state.scheduled = true;
                    Some(state.epoch)
                }
            } else {
                None
            }
        };
        if let Some(epoch) = schedule_epoch {
            self.spawn_delayed_drain(epoch);
        }
    }

    fn spawn_delayed_drain(&self, epoch: u64) {
        let tx = self.tx.clone();
        let state = self.state.clone();
        let delay = jittered_trigger_delay(
            self.debounce,
            self.jitter_max,
            self.jitter_seed ^ epoch.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if tx.send(WorkItem::DrainClusterQueue).await.is_err() {
                let mut state = state.lock().await;
                state.scheduled = false;
            }
        });
    }
}

fn stable_jitter_seed(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn jittered_trigger_delay(base: Duration, jitter_max: Duration, seed: u64) -> Duration {
    let jitter_max_millis = jitter_max.as_millis() as u64;
    if jitter_max_millis == 0 {
        return base;
    }
    base + Duration::from_millis(seed % jitter_max_millis)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TriggerRequestReason {
    reasons: BTreeSet<ReconcileReason>,
    diagnostic_reasons: BTreeSet<String>,
}

impl TriggerRequestReason {
    fn diagnostic(reason: impl Into<String>) -> Self {
        let reason = normalize_reconcile_reason(&reason.into());
        Self {
            reasons: BTreeSet::new(),
            diagnostic_reasons: BTreeSet::from([reason]),
        }
    }

    fn from_proto(request: TriggerStateReconciliationRequest) -> Self {
        let reasons = request
            .reasons
            .into_iter()
            .filter_map(|reason| StateReconciliationReason::try_from(reason).ok())
            .filter_map(proto_reconcile_reason)
            .collect::<BTreeSet<_>>();
        if reasons.is_empty() {
            return Self::diagnostic(request.diagnostic_reason);
        }
        Self {
            reasons,
            diagnostic_reasons: BTreeSet::new(),
        }
    }
}

impl From<&str> for TriggerRequestReason {
    fn from(reason: &str) -> Self {
        Self::diagnostic(reason)
    }
}

impl From<String> for TriggerRequestReason {
    fn from(reason: String) -> Self {
        Self::diagnostic(reason)
    }
}

impl From<ReconcileReason> for TriggerRequestReason {
    fn from(reason: ReconcileReason) -> Self {
        Self {
            reasons: BTreeSet::from([reason]),
            diagnostic_reasons: BTreeSet::new(),
        }
    }
}

fn drain_reason_string(
    reasons: BTreeSet<ReconcileReason>,
    diagnostic_reasons: BTreeSet<String>,
) -> String {
    if !reasons.is_empty() {
        return normalize_reconcile_reason(
            &reasons
                .into_iter()
                .map(ReconcileReason::as_str)
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    if !diagnostic_reasons.is_empty() {
        return normalize_reconcile_reason(
            &diagnostic_reasons.into_iter().collect::<Vec<_>>().join(","),
        );
    }
    normalize_reconcile_reason("")
}

pub(crate) fn trigger_request_reason(
    request: TriggerStateReconciliationRequest,
) -> TriggerRequestReason {
    TriggerRequestReason::from_proto(request)
}

fn proto_reconcile_reason(reason: StateReconciliationReason) -> Option<ReconcileReason> {
    match reason {
        StateReconciliationReason::Unspecified => None,
        StateReconciliationReason::NodeRegistered => Some(ReconcileReason::NodeRegistered),
        StateReconciliationReason::NodeUpdated => Some(ReconcileReason::NodeUpdated),
        StateReconciliationReason::ManualNodeStatusRefresh => {
            Some(ReconcileReason::ManualNodeStatusRefresh)
        }
        StateReconciliationReason::ManualNodeReconcile => {
            Some(ReconcileReason::ManualNodeReconcile)
        }
        StateReconciliationReason::ManualNodeNetworkPolicyReconcile => {
            Some(ReconcileReason::ManualNodeNetworkPolicyReconcile)
        }
        StateReconciliationReason::NodeTemplatePulled => Some(ReconcileReason::NodeTemplatePulled),
        StateReconciliationReason::SandboxRegistered => Some(ReconcileReason::SandboxRegistered),
        StateReconciliationReason::SandboxPortExposureUpdated => {
            Some(ReconcileReason::SandboxPortExposureUpdated)
        }
        StateReconciliationReason::SandboxNetworkPolicyUpdated => {
            Some(ReconcileReason::SandboxNetworkPolicyUpdated)
        }
        StateReconciliationReason::SandboxDesiredStateUpdated => {
            Some(ReconcileReason::SandboxDesiredStateUpdated)
        }
        StateReconciliationReason::TemplateNetworkPolicyUpdated => {
            Some(ReconcileReason::TemplateNetworkPolicyUpdated)
        }
        StateReconciliationReason::TemplateDesiredStateUpdated => {
            Some(ReconcileReason::TemplateDesiredStateUpdated)
        }
        StateReconciliationReason::TemplateBaseSnapshotPolicyUpdated => {
            Some(ReconcileReason::TemplateBaseSnapshotPolicyUpdated)
        }
        StateReconciliationReason::TemplatePoolPolicyUpdated => {
            Some(ReconcileReason::TemplatePoolPolicyUpdated)
        }
        StateReconciliationReason::TemplatePoolPolicyDeleted => {
            Some(ReconcileReason::TemplatePoolPolicyDeleted)
        }
        StateReconciliationReason::NodeSandboxInventoryPublished => {
            Some(ReconcileReason::NodeSandboxInventoryPublished)
        }
        StateReconciliationReason::NodeTemplateInventoryPublished => {
            Some(ReconcileReason::NodeTemplateInventoryPublished)
        }
    }
}

#[cfg(test)]
mod tests;
