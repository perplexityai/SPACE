//! Slim, typed view of a sandbox pod: the only shape the sandbox-pod reflector
//! caches and the only shape the uncached sandbox-pod reads return.
//!
//! Every process that runs the Kubernetes substrate holds one cached entry per
//! sandbox pod, so the cache's memory floor grows with cluster size, not
//! request load. A full `Pod` carries `managedFields`, the user's process env
//! (twice: container env plus the process-env annotation), probes, volumes,
//! tolerations and dozens of annotations nothing here reads. The view keeps
//! only what a consumer reads, and the compiler enforces that read set: a new
//! consumer that needs another field has to add it here, where the conversion
//! keeps it. Facts derived from the spec or conditions (the slot request, the
//! source pin, readiness, the scheduler's verdict) are computed once at
//! conversion rather than kept as raw lists.
//!
//! Annotations and labels are retained by [`SandboxPodKey`], a closed set. A
//! lookup takes a key, so reading a key outside the set doesn't compile, and
//! every key in the set is kept by construction.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::{Arc, PoisonError, RwLock},
};

use k8s_openapi::api::core::v1::{ContainerStatus, Pod, PodCondition};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::runtime::{reflector, watcher};
use pplx_space_common_core::kubernetes::{
    NODE_NAME_MATCH_FIELD, SANDBOX_CHECKPOINT_ANNOTATION, SANDBOX_EXECUTION_GENERATION_ANNOTATION,
    SANDBOX_ID_ANNOTATION, SANDBOX_MANAGED_BY_LABEL, SANDBOX_NETWORK_SLOTS_RESOURCE,
    SANDBOX_POOL_CONFIG_ID_KEY, SANDBOX_POOL_ROLE_KEY, SANDBOX_POOL_SLOT_ID_KEY,
    SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION, SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION,
    SANDBOX_TRACEPARENT_ANNOTATION,
};

/// Declares [`SandboxPodKey`] from one list, so a variant can't exist without
/// its wire key and its place in [`SandboxPodKey::ALL`]: the conversion keeps
/// exactly the keys in `ALL`, so a variant missing from it would silently read
/// as absent.
macro_rules! sandbox_pod_keys {
    ($($variant:ident => $key:expr,)+) => {
        /// The annotation and label keys the view retains. Some keys are
        /// stamped as both an annotation and a same-keyed label, so one
        /// vocabulary covers both; [`SandboxPodView::annotation`] and
        /// [`SandboxPodView::label`] read the two sets separately.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum SandboxPodKey {
            $($variant,)+
        }

        impl SandboxPodKey {
            /// Every key, in declaration order.
            pub const ALL: &[Self] = &[$(Self::$variant,)+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $key,)+
                }
            }
        }
    };
}

sandbox_pod_keys! {
    SandboxId => SANDBOX_ID_ANNOTATION,
    MaterializationId => pplx_space_common_core::kubernetes::SANDBOX_MATERIALIZATION_ID_ANNOTATION,
    ExecutionGeneration => SANDBOX_EXECUTION_GENERATION_ANNOTATION,
    SuspendRestoreStorageUri => SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION,
    SuspendRestoreSourceNode => SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION,
    Checkpoint => SANDBOX_CHECKPOINT_ANNOTATION,
    Traceparent => SANDBOX_TRACEPARENT_ANNOTATION,
    ManagedBy => SANDBOX_MANAGED_BY_LABEL,
    PoolRole => SANDBOX_POOL_ROLE_KEY,
    PoolConfigId => SANDBOX_POOL_CONFIG_ID_KEY,
    PoolSlotId => SANDBOX_POOL_SLOT_ID_KEY,
}

impl SandboxPodKey {
    /// Restore-only annotations: `build_sandbox_pod` stamps them on restore
    /// pods and never on a fresh create. A remote restore carries the storage
    /// URI; a node-local restore carries the source node and/or checkpoint.
    /// They persist after the restore completes, so a running restored pod
    /// still carries them.
    pub const RESTORE_ANNOTATIONS: [Self; 3] = [
        Self::Checkpoint,
        Self::SuspendRestoreStorageUri,
        Self::SuspendRestoreSourceNode,
    ];

    fn parse(key: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == key)
    }
}

/// The parts of one container's `state` the consumers read. Kubernetes sets at
/// most one of waiting/running/terminated; the three are kept independently so
/// the view never invents a precedence the source object didn't have.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SandboxContainerState {
    /// `state.waiting` is set; carries `state.waiting.reason`.
    pub waiting: Option<SandboxContainerWaiting>,
    /// `state.running` is set.
    pub running: bool,
    /// `state.terminated` is set.
    pub terminated: Option<SandboxContainerTerminated>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxContainerWaiting {
    pub reason: Option<Box<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxContainerTerminated {
    pub finished_at: Option<jiff::Timestamp>,
}

/// The container-status lists from `status`. A missing list and an empty list
/// both read as empty.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SandboxContainerStatuses {
    pub containers: Box<[SandboxContainerState]>,
    pub init_containers: Box<[SandboxContainerState]>,
    pub ephemeral_containers: Box<[SandboxContainerState]>,
}

impl SandboxContainerStatuses {
    /// App and init container states, the set kubelet reports as the pod's
    /// own progress.
    pub fn app_and_init(&self) -> impl Iterator<Item = &SandboxContainerState> {
        self.containers.iter().chain(&self.init_containers)
    }

    /// Every container state across app, init and ephemeral containers.
    pub fn all(&self) -> impl Iterator<Item = &SandboxContainerState> {
        self.app_and_init().chain(&self.ephemeral_containers)
    }
}

/// Retained annotations or labels: a handful of entries at most, so a boxed
/// slice searched linearly is far smaller than a map.
type RetainedKeys = Box<[(SandboxPodKey, Box<str>)]>;

/// The fields of a sandbox `Pod` that SPACE reads. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPodView {
    name: Box<str>,
    namespace: Option<Box<str>>,
    uid: Option<Box<str>>,
    resource_version: Option<Box<str>>,
    creation_timestamp: Option<jiff::Timestamp>,
    deleting: bool,
    annotations: RetainedKeys,
    labels: RetainedKeys,
    node_name: Option<Box<str>>,
    required_source_pin: bool,
    network_slot_request: u64,
    phase: Option<Box<str>>,
    pod_ip: Option<Box<str>>,
    ready: bool,
    unschedulable_message: Option<Box<str>>,
    container_statuses: SandboxContainerStatuses,
}

impl SandboxPodView {
    /// `metadata.name`. Every pod the apiserver returns has one; a nameless
    /// object (only possible from a hand-built fixture) reads as `""`.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn materialization_id(
        &self,
    ) -> Result<
        Option<pplx_space_common_core::types::PodMaterializationId>,
        super::super::SubstrateError,
    > {
        self.annotation(SandboxPodKey::MaterializationId)
            .map(str::parse)
            .transpose()
            .map_err(|error| super::super::SubstrateError::Failed {
                message: format!("invalid pod materialization identity: {error}"),
            })
    }

    pub fn uid(&self) -> Option<&str> {
        self.uid.as_deref()
    }

    pub fn resource_version(&self) -> Option<&str> {
        self.resource_version.as_deref()
    }

    pub fn creation_timestamp(&self) -> Option<jiff::Timestamp> {
        self.creation_timestamp
    }

    /// `metadata.deletionTimestamp` is set: the pod is terminating.
    pub fn is_deleting(&self) -> bool {
        self.deleting
    }

    pub fn annotation(&self, key: SandboxPodKey) -> Option<&str> {
        find_key(&self.annotations, key)
    }

    pub fn has_annotation(&self, key: SandboxPodKey) -> bool {
        self.annotation(key).is_some()
    }

    pub fn label(&self, key: SandboxPodKey) -> Option<&str> {
        find_key(&self.labels, key)
    }

    /// The annotation, falling back to the same-keyed label. The shared pod
    /// builder stamps identity keys into both.
    pub fn annotation_or_label(&self, key: SandboxPodKey) -> Option<&str> {
        self.annotation(key).or_else(|| self.label(key))
    }

    /// Whether any restore-only annotation is present. See
    /// [`SandboxPodKey::RESTORE_ANNOTATIONS`].
    pub fn has_restore_annotation(&self) -> bool {
        SandboxPodKey::RESTORE_ANNOTATIONS
            .into_iter()
            .any(|key| self.has_annotation(key))
    }

    /// `spec.nodeName` as written, including an empty string.
    pub fn node_name(&self) -> Option<&str> {
        self.node_name.as_deref()
    }

    /// Whether `spec.affinity` carries a **required** `matchFields:
    /// metadata.name` node-selector term: the source-aware hard pin. Read from
    /// the live pod so a retry can tell a still-pinned restore pod (must
    /// re-drive) from one already re-driven cross-node (pin dropped).
    pub fn has_required_source_pin(&self) -> bool {
        self.required_source_pin
    }

    /// Sum of the containers' `space.pplx.net/network-slots` requests,
    /// regardless of phase or deletion.
    pub fn network_slot_request(&self) -> u64 {
        self.network_slot_request
    }

    pub fn phase(&self) -> Option<&str> {
        self.phase.as_deref()
    }

    /// Phase `Succeeded` or `Failed`.
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase(), Some("Succeeded" | "Failed"))
    }

    pub fn pod_ip(&self) -> Option<&str> {
        self.pod_ip.as_deref()
    }

    /// A `Ready=True` condition is present.
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// The scheduler marked the pod `PodScheduled=False, reason=Unschedulable`:
    /// the condition's message, or its reason when the message is absent.
    pub fn unschedulable_message(&self) -> Option<&str> {
        self.unschedulable_message.as_deref()
    }

    pub fn container_statuses(&self) -> &SandboxContainerStatuses {
        &self.container_statuses
    }
}

fn find_key(retained: &[(SandboxPodKey, Box<str>)], key: SandboxPodKey) -> Option<&str> {
    retained
        .iter()
        .find(|(candidate, _)| *candidate == key)
        .map(|(_, value)| &**value)
}

/// A whole-number quantity such as a network-slot request or capacity.
pub(super) fn quantity_as_u64(quantity: &Quantity) -> Option<u64> {
    quantity.0.parse::<u64>().ok()
}

impl From<Pod> for SandboxPodView {
    fn from(pod: Pod) -> Self {
        let Pod {
            metadata,
            spec,
            status,
        } = pod;
        let spec = spec.unwrap_or_default();
        let required_source_pin = spec
            .affinity
            .as_ref()
            .and_then(|affinity| affinity.node_affinity.as_ref())
            .and_then(|node_affinity| {
                node_affinity
                    .required_during_scheduling_ignored_during_execution
                    .as_ref()
            })
            .is_some_and(|selector| {
                selector.node_selector_terms.iter().any(|term| {
                    term.match_fields.as_ref().is_some_and(|fields| {
                        fields
                            .iter()
                            .any(|field| field.key == NODE_NAME_MATCH_FIELD)
                    })
                })
            });
        let network_slot_request = spec
            .containers
            .iter()
            .filter_map(|container| {
                container
                    .resources
                    .as_ref()?
                    .requests
                    .as_ref()?
                    .get(SANDBOX_NETWORK_SLOTS_RESOURCE)
                    .and_then(quantity_as_u64)
            })
            // Saturate: this runs in the reflector task, so a nonsensical
            // request must not panic it or wrap into a small total.
            .fold(0, u64::saturating_add);
        let status = status.unwrap_or_default();
        let conditions = status.conditions.unwrap_or_default();
        Self {
            name: metadata.name.unwrap_or_default().into(),
            namespace: metadata.namespace.map(Into::into),
            uid: metadata.uid.map(Into::into),
            resource_version: metadata.resource_version.map(Into::into),
            creation_timestamp: metadata.creation_timestamp.map(|time| time.0),
            deleting: metadata.deletion_timestamp.is_some(),
            annotations: retain_keys(metadata.annotations),
            labels: retain_keys(metadata.labels),
            node_name: spec.node_name.map(Into::into),
            required_source_pin,
            network_slot_request,
            phase: status.phase.map(Into::into),
            pod_ip: status.pod_ip.map(Into::into),
            ready: conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True"),
            unschedulable_message: conditions
                .into_iter()
                .find(is_unschedulable)
                .and_then(|condition| condition.message.or(condition.reason))
                .map(Into::into),
            container_statuses: SandboxContainerStatuses {
                containers: slim_container_states(status.container_statuses),
                init_containers: slim_container_states(status.init_container_statuses),
                ephemeral_containers: slim_container_states(status.ephemeral_container_statuses),
            },
        }
    }
}

fn is_unschedulable(condition: &PodCondition) -> bool {
    condition.type_ == "PodScheduled"
        && condition.status == "False"
        && condition.reason.as_deref() == Some("Unschedulable")
}

fn retain_keys(map: Option<BTreeMap<String, String>>) -> RetainedKeys {
    map.into_iter()
        .flatten()
        .filter_map(|(key, value)| SandboxPodKey::parse(&key).map(|key| (key, value.into())))
        .collect()
}

fn slim_container_states(statuses: Option<Vec<ContainerStatus>>) -> Box<[SandboxContainerState]> {
    statuses
        .into_iter()
        .flatten()
        .map(|status| {
            let Some(state) = status.state else {
                return SandboxContainerState::default();
            };
            SandboxContainerState {
                waiting: state.waiting.map(|waiting| SandboxContainerWaiting {
                    reason: waiting.reason.map(Into::into),
                }),
                running: state.running.is_some(),
                terminated: state
                    .terminated
                    .map(|terminated| SandboxContainerTerminated {
                        finished_at: terminated.finished_at.map(|time| time.0),
                    }),
            }
        })
        .collect()
}

/// Lets `kube_runtime::reflector::Store` key the view like the `Pod` it came
/// from. The view deliberately isn't a `kube::Resource`: that would require
/// carrying a full `ObjectMeta`.
impl reflector::Lookup for SandboxPodView {
    type DynamicType = ();

    fn kind(_: &()) -> Cow<'_, str> {
        Cow::Borrowed("Pod")
    }

    fn group(_: &()) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn version(_: &()) -> Cow<'_, str> {
        Cow::Borrowed("v1")
    }

    fn plural(_: &()) -> Cow<'_, str> {
        Cow::Borrowed("pods")
    }

    fn name(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.name))
    }

    fn namespace(&self) -> Option<Cow<'_, str>> {
        self.namespace().map(Cow::Borrowed)
    }

    fn resource_version(&self) -> Option<Cow<'_, str>> {
        self.resource_version().map(Cow::Borrowed)
    }

    fn uid(&self) -> Option<Cow<'_, str>> {
        self.uid().map(Cow::Borrowed)
    }
}

/// Convert a raw watcher event into the view the store holds. Runs before the
/// store sees the object, so the full `Pod` is dropped as soon as it's read.
pub(super) fn sandbox_pod_view_event(event: watcher::Event<Pod>) -> watcher::Event<SandboxPodView> {
    match event {
        watcher::Event::Apply(pod) => watcher::Event::Apply(pod.into()),
        watcher::Event::Delete(pod) => watcher::Event::Delete(pod.into()),
        watcher::Event::Init => watcher::Event::Init,
        watcher::Event::InitApply(pod) => watcher::Event::InitApply(pod.into()),
        watcher::Event::InitDone => watcher::Event::InitDone,
    }
}

/// Per-node totals of the network slots held by bound sandbox pods, maintained
/// from the same watcher events as the sandbox-pod store. Lets the create-path
/// capacity preflight and the started-recovery source-node check read a node's
/// requested slots in O(1) instead of copying the whole store and scanning
/// every pod for every node. A pod holds no slots once it's deleting or
/// terminal.
///
/// Mirrors the store's re-list semantics: `Init` starts a private buffer,
/// `InitApply` fills it, and `InitDone` swaps it in whole, so a re-list never
/// exposes a partially rebuilt index.
#[derive(Clone, Default)]
pub(super) struct NodeNetworkSlots {
    inner: Arc<RwLock<NodeNetworkSlotsState>>,
}

#[derive(Default)]
struct NodeNetworkSlotsState {
    live: NodeNetworkSlotIndex,
    rebuilding: Option<NodeNetworkSlotIndex>,
}

#[derive(Default)]
struct NodeNetworkSlotIndex {
    /// Pod name -> (node name, active slots). Holds only pods that are bound
    /// to a node and hold a nonzero request. Keyed by name alone because the
    /// sandbox-pod watch is scoped to one namespace.
    by_pod: HashMap<Box<str>, (Box<str>, u64)>,
    /// Exact per-node sums. Wider than a request so adds and removes stay
    /// symmetric even when requests near `u64::MAX`; reads saturate.
    by_node: HashMap<Box<str>, u128>,
}

impl NodeNetworkSlotIndex {
    fn upsert(&mut self, pod: &SandboxPodView) {
        let held = pod
            .node_name()
            .filter(|name| !name.is_empty())
            .filter(|_| !pod.is_deleting() && !pod.is_terminal())
            .map(|node_name| (node_name, pod.network_slot_request()))
            .filter(|(_, slots)| *slots > 0);
        let current = self
            .by_pod
            .get(pod.name())
            .map(|(node_name, slots)| (&**node_name, *slots));
        // Most applies are status updates that leave the node and request
        // alone; skip the remove/re-insert churn under the write lock.
        if current == held {
            return;
        }
        self.remove(pod.name());
        let Some((node_name, slots)) = held else {
            return;
        };
        match self.by_node.get_mut(node_name) {
            Some(total) => *total += u128::from(slots),
            None => {
                self.by_node.insert(node_name.into(), u128::from(slots));
            }
        }
        self.by_pod
            .insert(pod.name().into(), (node_name.into(), slots));
    }

    fn remove(&mut self, pod_name: &str) {
        let Some((node_name, slots)) = self.by_pod.remove(pod_name) else {
            return;
        };
        if let Some(total) = self.by_node.get_mut(&node_name) {
            *total = total.saturating_sub(u128::from(slots));
            if *total == 0 {
                self.by_node.remove(&node_name);
            }
        }
    }
}

impl NodeNetworkSlots {
    /// Active network slots requested by sandbox pods bound to `node_name`.
    pub(super) fn requested_on(&self, node_name: &str) -> u64 {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .live
            .by_node
            .get(node_name)
            .map_or(0, |&total| u64::try_from(total).unwrap_or(u64::MAX))
    }

    fn apply(&self, event: &watcher::Event<SandboxPodView>) {
        let mut state = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        match event {
            watcher::Event::Apply(pod) => state.live.upsert(pod),
            watcher::Event::Delete(pod) => state.live.remove(pod.name()),
            watcher::Event::Init => state.rebuilding = Some(NodeNetworkSlotIndex::default()),
            watcher::Event::InitApply(pod) => {
                state.rebuilding.get_or_insert_default().upsert(pod);
            }
            watcher::Event::InitDone => {
                state.live = state.rebuilding.take().unwrap_or_default();
            }
        }
    }
}

/// Writes one watcher event to both the sandbox-pod store and the per-node
/// slot index. The reflector task is the only production writer; tests use it
/// to seed caches with the same semantics.
pub(super) struct SandboxPodWriter {
    store: reflector::store::Writer<SandboxPodView>,
    network_slots: NodeNetworkSlots,
}

impl SandboxPodWriter {
    pub(super) fn apply_watcher_event(&mut self, event: &watcher::Event<SandboxPodView>) {
        // Index first. It matters at `InitDone`: the store's swap releases the
        // readiness waiters, and nothing ready should read an index that
        // hasn't caught up. No reader joins the two, so the brief lead on
        // other events is harmless.
        self.network_slots.apply(event);
        self.store.apply_watcher_event(event);
    }
}

/// Create an empty sandbox-pod store, its slot index, and their shared writer.
pub(super) fn sandbox_pod_cache() -> (
    reflector::Store<SandboxPodView>,
    NodeNetworkSlots,
    SandboxPodWriter,
) {
    let (store, writer) = reflector::store::<SandboxPodView>();
    let network_slots = NodeNetworkSlots::default();
    let writer = SandboxPodWriter {
        store: writer,
        network_slots: network_slots.clone(),
    };
    (store, network_slots, writer)
}

/// A sandbox-pod store and slot index seeded with `pods` through one complete
/// initial list (`Init`, `InitApply` per pod, `InitDone`), so the store reads
/// as ready. The writer stays usable for later events.
#[cfg(test)]
pub(super) fn seeded_sandbox_pod_cache(
    pods: impl IntoIterator<Item = Pod>,
) -> (
    reflector::Store<SandboxPodView>,
    NodeNetworkSlots,
    SandboxPodWriter,
) {
    let (store, network_slots, mut writer) = sandbox_pod_cache();
    writer.apply_watcher_event(&watcher::Event::Init);
    for pod in pods {
        writer.apply_watcher_event(&watcher::Event::InitApply(pod.into()));
    }
    writer.apply_watcher_event(&watcher::Event::InitDone);
    (store, network_slots, writer)
}

#[cfg(test)]
mod tests;
