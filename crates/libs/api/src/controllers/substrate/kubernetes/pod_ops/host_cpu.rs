//! Host-CPU-aware restore placement: the capture CPU a restore pod must
//! require, the published CPUs it stays off, and the redirect of a local-only
//! restore stranded on a host that can't load its snapshot.

use super::restore::SuspendRestorePlan;
use super::*;

impl KubernetesSubstrate {
    /// The capture host CPU a restore pod must require so it lands only where
    /// the snapshot's vCPU state can be loaded, or `None` to render no CPU
    /// constraint. Always `None` while
    /// [`SANDBOX_RESTORE_HOST_CPU_PLACEMENT_ENABLED_ENV`](pplx_space_common_core::kubernetes::SANDBOX_RESTORE_HOST_CPU_PLACEMENT_ENABLED_ENV)
    /// is off, which also leaves the source-aware shortlist unfiltered.
    ///
    /// Only a restore from a remote full snapshot gets one. A disk-only
    /// restore (a `stopped` sandbox) carries no vCPU state. A local-only
    /// restore has no storage URI and is pre-bound to the node that holds its
    /// bytes, so there's no snapshot row to read and no other node to choose.
    /// A snapshot with no recorded CPU is constrained by the live label of
    /// the node that uploaded it while that node is still in the cluster
    /// ([`Self::capture_host_cpu`]), and stays unconstrained otherwise.
    /// An evacuation's cohort pin and the node-group set narrow which nodes
    /// count, since the pod builder ANDs both with the CPU term.
    ///
    /// The capture CPU becomes a `NotIn` exclusion of every published identity
    /// that can't load it ([`Self::host_cpu_exclusions`]), which leaves
    /// unlabeled nodes eligible. It's withheld unless the node cache shows at
    /// least one node a restore pod could land on whose CPU can load it
    /// ([`Self::restore_schedulable_node_exists`]), so a cluster whose
    /// node-ctl hasn't published labels yet, or that has lost every host able
    /// to load it, keeps today's unconstrained placement rather than leaving
    /// every such restore `Unschedulable`. A cold cache withholds it for the
    /// same reason: it can't show that any node qualifies.
    ///
    /// The check is for existence, not capacity. When every node the pod may
    /// use (those whose CPU can load it, and unlabeled ones) is full, the pod stays
    /// `Unschedulable`, and the cross-node re-drive keeps the term, so the
    /// restore waits for one of them. That's deliberate. The identity can't
    /// tell which CPU pairs load each other's state (a CPU whose MSR set is a
    /// superset of another's can load that one's state, but not the reverse),
    /// so every unequal identity is excluded unless the configured
    /// [`HostCpuCompatibility`](pplx_space_common_core::types::HostCpuCompatibility)
    /// lists it as a loader. A restore that lands on a host that can't load it
    /// fails on every retry, while one that waits succeeds once capacity
    /// frees. Listing a known one-way pair removes the wait for that pair.
    ///
    /// The exclusion is fixed when the pod is built, so it only covers CPUs
    /// some node published by then. A node whose CPU model the fleet has never
    /// published, joining while a restore waits, isn't excluded, and the
    /// restore can bind there and fail. node-ctl labels a node before it
    /// advertises network-slot capacity, so a joining node of a published CPU
    /// is excluded unless its label write fails.
    pub(in crate::controllers::substrate::kubernetes) async fn restore_host_cpu_requirement(
        &self,
        sandbox: &Sandbox,
        context: &PodCreateContext<'_>,
        node_group_placement: Option<&NodeGroupPlacement>,
    ) -> Result<Option<HostCpu>, SubstrateError> {
        if !self
            .k8s
            .sandbox_pod_config
            .restore_host_cpu_placement_enabled
        {
            return Ok(None);
        }
        let Some(storage_uri) = context.suspend_restore_storage_uri() else {
            return Ok(None);
        };
        if sandbox.lifecycle.parse::<SandboxLifecycle>() == Ok(SandboxLifecycle::Stopped) {
            return Ok(None);
        }
        // Checked before the snapshot read: a cluster whose nodes publish no
        // CPU at all can't have a qualifying node, whatever the snapshot says.
        let bounds = RestorePlacementBounds::for_context(context, node_group_placement);
        if !self.restore_schedulable_node_exists(bounds, |node| node_host_cpu(node).is_some()) {
            return Ok(None);
        }
        let Some(host_cpu) = self
            .store
            .snapshot_capture_host(sandbox.id, storage_uri)
            .await?
            .and_then(|capture| self.capture_host_cpu(&capture))
        else {
            return Ok(None);
        };
        if !self.restore_schedulable_node_exists(bounds, |node| self.node_can_load(node, &host_cpu))
        {
            // Debug, not warn: this fires on every restore of such a snapshot
            // for as long as no reachable node has its CPU, which a fleet
            // change can make the norm, and which nodes carry which value is
            // directly visible on the Node objects.
            tracing::debug!(
                sandbox_id = %sandbox.id,
                %host_cpu,
                node_cache_ready = self.k8s.caches.nodes_ready(),
                "no schedulable node's CPU can load the snapshot's capture host CPU; restoring \
                 without a CPU constraint"
            );
            return Ok(None);
        }
        Ok(Some(host_cpu))
    }

    /// The CPU identities a restore pod built from `context` stays off:
    /// [`Self::host_cpu_exclusions`] of [`Self::restore_host_cpu_requirement`].
    /// Empty renders no CPU constraint.
    pub(super) async fn restore_excluded_host_cpus(
        &self,
        sandbox: &Sandbox,
        context: &PodCreateContext<'_>,
        node_group_placement: Option<&NodeGroupPlacement>,
    ) -> Result<Vec<HostCpu>, SubstrateError> {
        let capture = self
            .restore_host_cpu_requirement(sandbox, context, node_group_placement)
            .await?;
        Ok(self.host_cpu_exclusions(capture.as_ref()))
    }

    /// Whether `node` publishes a CPU that can load state captured on
    /// `capture`. An unlabeled node doesn't count: it isn't known to.
    fn node_can_load(&self, node: &Node, capture: &HostCpu) -> bool {
        node_host_cpu(node).is_some_and(|host| self.host_cpu_can_load(capture, &host))
    }

    /// Whether a host of CPU `host` can load state captured on `capture`, per
    /// the configured compatibility list.
    fn host_cpu_can_load(&self, capture: &HostCpu, host: &HostCpu) -> bool {
        self.k8s
            .sandbox_pod_config
            .host_cpu_compatibility
            .can_load(capture, host)
    }

    /// The CPU identities a restore pod for `capture` stays off: every
    /// identity a node in the cache publishes that can't load `capture`'s
    /// state (anything but `capture` itself and the loaders the configured
    /// [`HostCpuCompatibility`](pplx_space_common_core::types::HostCpuCompatibility)
    /// lists for it), sorted. Empty when `capture` is `None` or no labeled
    /// node is excluded. Rendered as `NotIn`, the exclusion leaves unlabeled
    /// nodes eligible, so a node node-ctl hasn't labeled yet, or can't, still
    /// takes restores as it did before the label.
    pub(in crate::controllers::substrate::kubernetes) fn host_cpu_exclusions(
        &self,
        capture: Option<&HostCpu>,
    ) -> Vec<HostCpu> {
        let Some(capture) = capture else {
            return Vec::new();
        };
        self.k8s
            .caches
            .nodes
            .state()
            .iter()
            .filter_map(|node| node_host_cpu(node))
            .filter(|host_cpu| !self.host_cpu_can_load(capture, host_cpu))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Whether the node cache shows a node a restore pod could land on that
    /// also satisfies `matches`: schedulable, carrying the sandbox node
    /// selector, not placement-blocked when that exclusion is rendered, inside
    /// `node_group_placement`'s candidate set, and carrying
    /// `extra_required_label` (an evacuation's cohort pin) when those are
    /// given, and without a `NoSchedule` or `NoExecute` taint the pod's
    /// tolerations don't cover. Those are the constraints the pod builder
    /// renders alongside the CPU term, so a matching node failing any of them
    /// can't host the pod. `false` while the cache is cold, since it can't
    /// show that any node qualifies.
    fn restore_schedulable_node_exists(
        &self,
        bounds: RestorePlacementBounds<'_>,
        matches: impl Fn(&Node) -> bool,
    ) -> bool {
        if !self.k8s.caches.nodes_ready() {
            return false;
        }
        let RestorePlacementBounds {
            node_group_placement,
            extra_required_label,
            extra_tolerations,
        } = bounds;
        // The tolerations the pod builder renders for a restore: the
        // evacuation's, plus the dedicated-group one when the placement
        // targets dedicated capacity. Restores never carry a node-pool pin.
        let mut tolerations = extra_tolerations.to_vec();
        tolerations.extend(node_group_placement.and_then(NodeGroupPlacement::dedicated_toleration));
        let config = &self.k8s.sandbox_pod_config;
        let selector = config.base_node_selector();
        self.k8s
            .caches
            .nodes
            .find(|node| {
                adapter::node_schedulable(node)
                    && selector.matches(node)
                    && (!config.ssd_headroom_tier_affinity
                        || adapter::node_not_placement_blocked(node))
                    && node_group_placement.is_none_or(|placement| {
                        node_group_label(node).is_some_and(|group| placement.admits_group(group))
                    })
                    && extra_required_label.is_none_or(|(key, value)| {
                        node.metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get(key))
                            .is_some_and(|label| label == value)
                    })
                    && node_taints_tolerated(node, &tolerations)
                    && matches(node)
            })
            .is_some()
    }

    /// The CPU that captured a snapshot: the recorded identity, else the live
    /// [`HOST_CPU_LABEL`] of the node that uploaded it. The label stands in
    /// for a snapshot with no recorded CPU: one that predates the field, or a
    /// full backup whose metadata was rebuilt without one. The uploader is the
    /// capturing host for a suspend snapshot, but not for a copy one node
    /// pulled and another backed up, so the stand-in can name the wrong CPU
    /// there. It's only taken from a Node object created before the
    /// snapshot's second: node names derive from the instance's IP, so a
    /// replacement instance can reuse the uploader's name, and its Node object
    /// is newer than the snapshot. `None` means unknown.
    fn capture_host_cpu(&self, capture: &SnapshotCaptureHost) -> Option<HostCpu> {
        capture.recorded.clone().or_else(|| {
            let node = self
                .k8s
                .caches
                .node_by_name(capture.source_node_name.as_deref()?)?;
            let created = node.metadata.creation_timestamp.as_ref()?.0.as_second();
            // `creationTimestamp` has whole-second precision, so a Node from
            // the snapshot's own second is ambiguous and doesn't count.
            ((created as f64) < capture.snapshot_epoch_seconds.floor())
                .then(|| node_host_cpu(&node))
                .flatten()
        })
    }

    /// Whether any pod of this sandbox could be hosting a live VM: phase
    /// `Running`, `Unknown`, or not yet reported. `Pending` doesn't count, so
    /// a pod whose sandbox kubelet can't create, which stays `Pending`, doesn't
    /// block the redirect it exists for. That's a heuristic: the runtime
    /// starts the VM while the pod is still `Pending`, so exclusion of a
    /// second runtime rests on the execution claim, as the redirect's doc
    /// says. `None` while the pod reflector is cold.
    fn sandbox_pod_may_be_live(&self, sandbox_id: SandboxId) -> Option<bool> {
        if !self.pod_reflector_ready.is_ready() {
            return None;
        }
        let sandbox_id = sandbox_id.to_string();
        Some(
            self.pod_store
                .find(|pod| {
                    pod.annotation(SandboxPodKey::SandboxId) == Some(sandbox_id.as_str())
                        && !matches!(pod.phase(), Some("Pending" | "Succeeded" | "Failed"))
                })
                .is_some(),
        )
    }

    /// A portable plan to replace a local-only restore that can't succeed:
    /// the plan pins the sandbox to `source_node`, but that node's CPU can't
    /// load the CPU that captured the sandbox's newest snapshot (unequal, and
    /// not a listed loader).
    ///
    /// This is the state a failed cross-node restore leaves behind. The target
    /// node pulled the snapshot, couldn't load its vCPU state, and became the
    /// sandbox's placement, which also moved `placement_changed_at` past the
    /// snapshot. From then on the placement-freshness gate rejects the remote
    /// snapshot, so every wake is a local-only restore pinned to the node that
    /// can't load it. Restoring from the snapshot's storage URI instead lets
    /// [`Self::restore_host_cpu_requirement`] place it on a compatible host.
    ///
    /// Returns `None`, keeping the local-only plan, unless all of these hold:
    ///
    /// * host-CPU-aware restore placement is enabled
    ///   ([`SANDBOX_RESTORE_HOST_CPU_PLACEMENT_ENABLED_ENV`](pplx_space_common_core::kubernetes::SANDBOX_RESTORE_HOST_CPU_PLACEMENT_ENABLED_ENV));
    /// * the plan didn't come from the active execution's pod (checked by the
    ///   caller, `suspend_restore_plan`);
    /// * the source node's CPU and the snapshot's capture CPU are both known
    ///   and the source can't load it, and a node the redirected pod could land
    ///   on, inside the sandbox's node-group set, can;
    /// * no pod of the sandbox could be hosting a live VM
    ///   ([`Self::sandbox_pod_may_be_live`]), read from a warm pod reflector;
    /// * the sandbox has no local artifact marker, which would name bytes
    ///   newer than any backup, its newest snapshot covers the last recorded
    ///   command and access, and, for a `restoring` row, no full snapshot
    ///   postdates the restore's start
    ///   ([`K8sStore::activity_current_markerless_snapshot`]).
    ///
    /// The marker requirement limits which stranded rows this helps. The
    /// drain and gone-node give-ups clear the marker of a row a stuck restore
    /// left pinned; a stranded row that keeps its marker keeps the local-only
    /// plan, as it would without the redirect.
    ///
    /// The restore-create path reaches this with the row already `restoring`:
    /// the restore lifecycle begins before the plan is computed, and that's
    /// also when the stranded state shows, since the retry selector rejects a
    /// snapshot older than the failed placement. An access to a `suspended`
    /// sandbox computes the plan before any restore begins, so the
    /// restoring-row bound doesn't apply there.
    ///
    /// What these establish, and what they don't. The swap loses no work the
    /// control plane recorded: the snapshot is at least as new as the
    /// command and access clocks. Those clocks only move when a newer value
    /// is more than a minute ahead (`published_state.rs`), and they see only
    /// commands and accesses, so guest work after the snapshot with neither
    /// isn't covered. The pod check is a cache read, not a lock, so it isn't
    /// the exclusion [`newest_suspend_snapshot_activity_current`]'s contract
    /// asks for. Exclusion of a second live runtime rests, as for every
    /// restore, on the execution claim and on the create path deleting
    /// older-generation pods before it materializes a successor.
    ///
    /// The checks that need only the caches run first, so a cluster whose
    /// nodes all publish one CPU, or none, never reaches the snapshot read or
    /// the node-group resolution.
    ///
    /// [`newest_suspend_snapshot_activity_current`]: pplx_space_sandbox_portability::newest_suspend_snapshot_activity_current
    pub(super) async fn cpu_incompatible_local_source_redirect(
        &self,
        sandbox: &Sandbox,
        source_node: &str,
    ) -> Result<Option<SuspendRestorePlan>, SubstrateError> {
        if !self
            .k8s
            .sandbox_pod_config
            .restore_host_cpu_placement_enabled
        {
            return Ok(None);
        }
        let Some(source_cpu) = self
            .k8s
            .caches
            .node_by_name(source_node)
            .and_then(|node| node_host_cpu(&node))
        else {
            return Ok(None);
        };
        // A necessary condition only, so deliberately looser than the final
        // check below: any node in the cache publishing a different CPU,
        // whatever its taints or group.
        if !self.k8s.caches.nodes_ready()
            || self
                .k8s
                .caches
                .nodes
                .find(|node| node_host_cpu(node).is_some_and(|cpu| cpu != source_cpu))
                .is_none()
        {
            return Ok(None);
        }
        if self.sandbox_pod_may_be_live(sandbox.id) != Some(false) {
            return Ok(None);
        }
        let Some((storage_uri, capture)) = self
            .store
            .activity_current_markerless_snapshot(sandbox.id)
            .await?
        else {
            return Ok(None);
        };
        let Some(capture_cpu) = self.capture_host_cpu(&capture) else {
            return Ok(None);
        };
        if self.host_cpu_can_load(&capture_cpu, &source_cpu) {
            return Ok(None);
        }
        // The redirected restore is a storage-only build, placed inside the
        // sandbox's node-group set. Without a host in that set whose CPU can
        // load the snapshot, the CPU term would be withheld and the restore
        // could land on another incompatible host and fail there. With one, an
        // unlabeled node still passes the `NotIn` term, so the redirect fully
        // protects only a labeled fleet: a restore that lands on an unlabeled,
        // incompatible node fails there and is pinned to it, and the redirect
        // can't fire for it again until node-ctl labels that node.
        let redirected = PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri: Some(storage_uri.as_str()),
            suspend_restore_source_node: None,
            direct_node_name: None,
            preferred_node_name: None,
        });
        let placement = self
            .resolve_node_group_placement(sandbox, &redirected)
            .await?;
        if !self.restore_schedulable_node_exists(
            RestorePlacementBounds::for_context(&redirected, placement.placement()),
            |node| self.node_can_load(node, &capture_cpu),
        ) {
            return Ok(None);
        }
        tracing::info!(
            sandbox_id = %sandbox.id,
            source_node,
            %source_cpu,
            %capture_cpu,
            "local-only restore source's CPU can't load the snapshot's capture CPU; \
             restoring from the snapshot's storage URI instead"
        );
        Ok(Some(SuspendRestorePlan {
            source_node_name: None,
            storage_uri: Some(storage_uri),
        }))
    }
}
