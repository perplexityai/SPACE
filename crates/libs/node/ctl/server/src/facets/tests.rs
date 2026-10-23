use super::*;

use pplx_space_net_common::proto::EgressDefaultAction;

const TEST_SANDBOX_ID: &str = "019e6370-cfc9-7a43-9337-399c7b51a1d9";
const TEST_ACTIVATION_ATTEMPT_ID: &str = "019e6370-cfc9-7a43-9337-399c7b51a1da";

fn sandbox_ids(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("sandbox-{index}")).collect()
}

#[tokio::test]
async fn sandbox_state_batches_preserve_the_census_or_fail_without_partial_output() {
    let ids = sandbox_ids(4097);
    for fail_second in [false, true] {
        let mut calls = 0;
        let response = fetch_sandbox_state_batches(&ids, |batch| {
            calls += 1;
            assert!(batch.len() <= pplx_space_common_core::inventory::READ_BATCH_ITEMS);
            std::future::ready(if fail_second && calls == 2 {
                Err(tonic::Status::unavailable("injected middle-batch failure"))
            } else {
                Ok(SbxStateResponse {
                    sandboxes: Vec::new(),
                    missing_ids: batch,
                })
            })
        })
        .await;
        if fail_second {
            assert!(response.is_err());
            assert_eq!(calls, 2);
        } else {
            assert_eq!(response.unwrap().missing_ids, ids);
            assert!(calls > 1);
        }
    }
}

#[test]
fn network_policy_state_requests_batches_sandbox_ids() {
    let requests = network_policy_state_requests(
        "node-1",
        Some(NodeRole::Worker),
        vec!["seed".to_string()],
        sandbox_ids(5),
        2,
    );
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.sandbox_ids.len())
            .collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    // Templates ride the first batch only; identity fields ride every batch.
    assert_eq!(requests[0].templates, vec!["seed".to_string()]);
    for request in &requests {
        assert_eq!(request.node_id, "node-1");
        assert_eq!(request.role, Some(NodeRole::Worker as i32));
    }
    for request in &requests[1..] {
        assert!(request.templates.is_empty());
    }
}

#[test]
fn network_policy_state_requests_templates_only_is_one_request() {
    let requests = network_policy_state_requests(
        "node-1",
        None,
        vec!["seed".to_string(), "base".to_string()],
        Vec::new(),
        100,
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].templates.len(), 2);
    assert!(requests[0].sandbox_ids.is_empty());
}

#[test]
fn network_policy_state_requests_clamps_zero_batch_size() {
    let requests = network_policy_state_requests("node-1", None, Vec::new(), sandbox_ids(3), 0);
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request.sandbox_ids.len() == 1)
    );
}

const PLATFORM_OWNER: &str = "user:b570a349-78c7-4714-8049-f986cc81140f";

fn local_state(sandbox_id: &str, status: &str) -> (String, SandboxInfo) {
    local_state_with_owner(sandbox_id, status, PLATFORM_OWNER)
}

fn local_state_with_owner(sandbox_id: &str, status: &str, owner: &str) -> (String, SandboxInfo) {
    (
        sandbox_id.to_string(),
        SandboxInfo {
            sandbox_id: sandbox_id.to_string(),
            lifecycle: Some(
                pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::from_label(status).unwrap()
                    as i32,
            ),
            owner: Some(owner.to_string()),
            ..SandboxInfo::default()
        },
    )
}

#[test]
fn cluster_deleted_evict_selects_suspended_and_quarantined_but_not_started_or_absent() {
    // The first leak shape: cluster row hard-deleted while the node still
    // holds a suspended wake-cache copy — must be selected. A deleted id
    // whose local row is started (restore raced the report) or already
    // absent locally must be left alone. The second selectable shape, a
    // failed cross-node restore's retained `restoring` row, is covered by
    // `cluster_deleted_evict_selects_failed_restoring_but_not_live_restoring`.
    let suspended_id = "019e6370-cfc9-7a43-9337-399c7b51a1d1";
    let started_id = "019e6370-cfc9-7a43-9337-399c7b51a1d2";
    let absent_id = "019e6370-cfc9-7a43-9337-399c7b51a1d3";
    let quarantined_id = "019e6370-cfc9-7a43-9337-399c7b51a1d4";
    let present_quarantined_id = "019e6370-cfc9-7a43-9337-399c7b51a1d5";
    let stopped_id = "019e6370-cfc9-7a43-9337-399c7b51a1d6";
    let deleted = vec![
        stopped_id.to_string(),
        suspended_id.to_string(),
        started_id.to_string(),
        absent_id.to_string(),
        quarantined_id.to_string(),
    ];
    let local_states = BTreeMap::from([
        local_state(stopped_id, "stopped"),
        local_state(suspended_id, "suspended"),
        local_state(started_id, "started"),
        local_state(quarantined_id, "quarantined"),
        local_state(present_quarantined_id, "quarantined"),
    ]);

    let candidates =
        select_cluster_deleted_evict_candidates(&deleted, &local_states, 25, |_| false);

    assert_eq!(candidates, vec![suspended_id, quarantined_id, stopped_id]);
}

#[test]
fn cluster_deleted_evict_selects_failed_restoring_but_not_live_restoring() {
    // The second leak shape: a cross-node restore failed during staging
    // on this node and sbx-manager retained the `restoring` row with its
    // failure persisted; once the cluster deletes the sandbox that row
    // must be selected. A `restoring` row without the marker is a
    // first-attempt restore and must not be — even though the cluster
    // reports the id deleted (the report and the restore can race). A
    // `failed` row carrying a failure reason is a different shape with its
    // own recovery and must not be selected either.
    let failed_restoring_id = "019e6370-cfc9-7a43-9337-399c7b51a1d1";
    let live_restoring_id = "019e6370-cfc9-7a43-9337-399c7b51a1d2";
    let blank_failure_id = "019e6370-cfc9-7a43-9337-399c7b51a1d3";
    let pool_failed_id = "019e6370-cfc9-7a43-9337-399c7b51a1d4";
    let failed_state_id = "019e6370-cfc9-7a43-9337-399c7b51a1d5";
    let deleted = [
        failed_restoring_id,
        live_restoring_id,
        blank_failure_id,
        pool_failed_id,
        failed_state_id,
    ]
    .iter()
    .map(ToString::to_string)
    .collect::<Vec<_>>();
    let mut failed = local_state(failed_restoring_id, "restoring").1;
    failed.failure_reason = Some("cross-node restore artifact staging failed: ...".to_string());
    let mut blank = local_state(blank_failure_id, "restoring").1;
    blank.failure_reason = Some("   ".to_string());
    let mut pool_failed = local_state(pool_failed_id, "restoring").1;
    pool_failed.failure_reason = Some("cross-node restore artifact staging failed".to_string());
    pool_failed.pool_template = Some("seed".to_string());
    let mut failed_state = local_state(failed_state_id, "failed").1;
    failed_state.failure_reason = Some("boot failed".to_string());
    let local_states = BTreeMap::from([
        (failed_restoring_id.to_string(), failed),
        local_state(live_restoring_id, "restoring"),
        (blank_failure_id.to_string(), blank),
        (pool_failed_id.to_string(), pool_failed),
        (failed_state_id.to_string(), failed_state),
    ]);

    let candidates =
        select_cluster_deleted_evict_candidates(&deleted, &local_states, 25, |_| false);

    assert_eq!(candidates, vec![failed_restoring_id]);
}

#[test]
fn cluster_deleted_evict_selects_terminal_teardown_suspend_but_not_a_publishable_one() {
    // The third leak shape: a sandbox deleted while one of its executions
    // was still starting leaves the node running a generation suspend whose
    // durable backup the cluster refuses forever. That `suspending` row can
    // never publish and nothing else reclaims it, so once the cluster
    // reports the sandbox deleted it must be selected. A `suspending` row
    // with no failure, or one whose failure a later attempt can still get
    // past, is a suspend that may yet publish and must not be — its staged
    // artifact can be the only copy of the sandbox's work.
    let terminal_id = "019e6370-cfc9-7a43-9337-399c7b51a1d1";
    let uploading_id = "019e6370-cfc9-7a43-9337-399c7b51a1d2";
    let other_failure_id = "019e6370-cfc9-7a43-9337-399c7b51a1d3";
    let deleted = [terminal_id, uploading_id, other_failure_id]
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let mut terminal = local_state(terminal_id, "suspending").1;
    terminal.failure_reason = Some(
        pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON.to_string(),
    );
    let mut other_failure = local_state(other_failure_id, "suspending").1;
    other_failure.failure_reason = Some("publish failed: rename_failed".to_string());
    let local_states = BTreeMap::from([
        (terminal_id.to_string(), terminal),
        local_state(uploading_id, "suspending"),
        (other_failure_id.to_string(), other_failure),
    ]);

    let candidates =
        select_cluster_deleted_evict_candidates(&deleted, &local_states, 25, |_| false);

    assert_eq!(candidates, vec![terminal_id]);
}

#[test]
fn cluster_deleted_evict_requires_platform_owner_and_no_pool() {
    // "No cluster row" is only evidence of deletion for rows the
    // cluster minted (`user:<uuid>` owners). Operator creations,
    // template-manager build sandboxes, orphan-adopted rows, rows
    // with no owner at all, and pool slots must never be selected
    // even when locally suspended.
    let platform_id = "019e6370-cfc9-7a43-9337-399c7b51a1d1";
    let operator_id = "019e6370-cfc9-7a43-9337-399c7b51a1d2";
    let tmplmgr_id = "019e6370-cfc9-7a43-9337-399c7b51a1d3";
    let orphaned_id = "019e6370-cfc9-7a43-9337-399c7b51a1d4";
    let ownerless_id = "019e6370-cfc9-7a43-9337-399c7b51a1d5";
    let pool_id = "019e6370-cfc9-7a43-9337-399c7b51a1d6";
    let deleted = [
        platform_id,
        operator_id,
        tmplmgr_id,
        orphaned_id,
        ownerless_id,
        pool_id,
    ]
    .iter()
    .map(ToString::to_string)
    .collect::<Vec<_>>();
    let mut pool_state = local_state(pool_id, "suspended").1;
    pool_state.pool_template = Some("seed".to_string());
    let mut ownerless_state = local_state(ownerless_id, "suspended").1;
    ownerless_state.owner = None;
    let local_states = BTreeMap::from([
        local_state(platform_id, "suspended"),
        local_state_with_owner(operator_id, "suspended", "root@local"),
        local_state_with_owner(tmplmgr_id, "suspended", "tmplmgr@local"),
        local_state_with_owner(orphaned_id, "suspended", "orphaned@local"),
        (ownerless_id.to_string(), ownerless_state),
        (pool_id.to_string(), pool_state),
    ]);

    let candidates =
        select_cluster_deleted_evict_candidates(&deleted, &local_states, 25, |_| false);

    assert_eq!(candidates, vec![platform_id]);
}

#[test]
fn cluster_deleted_evict_is_oldest_first_and_windowed() {
    // UUIDv7 ids sort by creation time; under the window the oldest
    // accumulated orphans must drain first.
    let older_id = "019e6370-cfc9-7a43-9337-399c7b51a1d1";
    let newer_id = "019f6370-cfc9-7a43-9337-399c7b51a1d2";
    let deleted = vec![newer_id.to_string(), older_id.to_string()];
    let local_states = BTreeMap::from([
        local_state(older_id, "suspended"),
        local_state(newer_id, "suspended"),
    ]);

    let candidates = select_cluster_deleted_evict_candidates(&deleted, &local_states, 1, |_| false);

    assert_eq!(candidates, vec![older_id]);
}

#[test]
fn cluster_deleted_evict_rotates_past_backed_off_candidates() {
    // A permanently-deferring oldest candidate must not consume a
    // window slot forever: once backed off, selection moves on to
    // the drainable ids sorted behind it.
    let stuck_oldest = "019e6370-cfc9-7a43-9337-399c7b51a1d1";
    let drainable = "019f6370-cfc9-7a43-9337-399c7b51a1d2";
    let deleted = vec![stuck_oldest.to_string(), drainable.to_string()];
    let local_states = BTreeMap::from([
        local_state(stuck_oldest, "suspended"),
        local_state(drainable, "suspended"),
    ]);

    let candidates =
        select_cluster_deleted_evict_candidates(&deleted, &local_states, 1, |sandbox_id| {
            sandbox_id == stuck_oldest
        });

    assert_eq!(candidates, vec![drainable]);
}

/// node-ctl's default poll, the sync interval the eviction tests run at
/// unless they are about another one.
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(crate::DEFAULT_POLL_INTERVAL_SECS);

#[test]
fn cluster_deleted_evict_quiescence_refusal_defers_to_the_window_end_not_the_flat_backoff() {
    // sbx-manager's quiescence refusal is a FailedPrecondition that says
    // how long the window has left. The id must be retried at that
    // horizon plus one sync of slack, not after the flat 30 min back-off.
    let refusal = pplx_space_common_grpc::with_retry_after(
        tonic::Status::failed_precondition("inside the 600s quiescence window"),
        Duration::from_secs(549),
    );
    assert_eq!(
        cluster_deleted_evict_refusal_disposition(&refusal, DEFAULT_SYNC_INTERVAL),
        ClusterDeletedEvictDisposition::Deferred(Duration::from_secs(549) + DEFAULT_SYNC_INTERVAL)
    );
}

#[test]
fn cluster_deleted_evict_quiescence_slack_is_one_configured_sync() {
    // The deferral is the window's end plus one configured poll, not a
    // default-sized minute: with a 20 s poll and a 10 s window, 30 s rather
    // than 70 s.
    let refusal = pplx_space_common_grpc::with_retry_after(
        tonic::Status::failed_precondition("inside the 10s quiescence window"),
        Duration::from_secs(10),
    );
    assert_eq!(
        cluster_deleted_evict_refusal_disposition(&refusal, Duration::from_secs(20)),
        ClusterDeletedEvictDisposition::Deferred(Duration::from_secs(30))
    );
}

#[test]
fn cluster_deleted_evict_refusal_without_a_horizon_keeps_the_flat_backoff() {
    // A genuine precondition failure (row transitioned, pool slot, not a
    // platform owner) carries no horizon and keeps the 30 min back-off,
    // as do the other non-failure codes.
    for status in [
        tonic::Status::failed_precondition("sandbox is an unclaimed pool slot"),
        tonic::Status::not_found("row gone"),
        tonic::Status::unimplemented("old sbx-manager"),
        tonic::Status::unavailable("draining"),
        tonic::Status::deadline_exceeded("teardown still running"),
    ] {
        assert_eq!(
            cluster_deleted_evict_refusal_disposition(&status, DEFAULT_SYNC_INTERVAL),
            ClusterDeletedEvictDisposition::Deferred(CLUSTER_DELETED_EVICT_BACKOFF)
        );
    }
}

#[test]
fn cluster_deleted_evict_horizon_is_capped_at_the_flat_backoff() {
    // A server that quotes a horizon longer than the flat back-off must
    // not be able to park an id for longer than the loop ever would.
    let refusal = pplx_space_common_grpc::with_retry_after(
        tonic::Status::failed_precondition("very long window"),
        Duration::from_secs(4 * 3600),
    );
    assert_eq!(
        cluster_deleted_evict_refusal_disposition(&refusal, DEFAULT_SYNC_INTERVAL),
        ClusterDeletedEvictDisposition::Deferred(CLUSTER_DELETED_EVICT_BACKOFF)
    );
}

#[test]
fn cluster_deleted_evict_aborted_retries_next_sync_and_other_codes_fail() {
    assert_eq!(
        cluster_deleted_evict_refusal_disposition(
            &tonic::Status::aborted("staging pull live"),
            DEFAULT_SYNC_INTERVAL
        ),
        ClusterDeletedEvictDisposition::RetryNextSync
    );
    assert_eq!(
        cluster_deleted_evict_refusal_disposition(
            &tonic::Status::internal("boom"),
            DEFAULT_SYNC_INTERVAL
        ),
        ClusterDeletedEvictDisposition::Failed(CLUSTER_DELETED_EVICT_BACKOFF)
    );
}

fn test_network_policy(action: EgressDefaultAction) -> ProtoNetworkPolicyV1 {
    ProtoNetworkPolicyV1 {
        egress_default_action: action as i32,
        ..ProtoNetworkPolicyV1::default()
    }
}

fn desired_sandbox_policy(
    source: NetworkPolicySource,
    policy: ProtoNetworkPolicyV1,
    generation: u64,
) -> SandboxNetworkPolicyState {
    SandboxNetworkPolicyState {
        sandbox_id: TEST_SANDBOX_ID.to_string(),
        source: source as i32,
        policy: Some(policy),
        generation,
        template: "seed".to_string(),
    }
}

fn local_sandbox_policy(
    applied_source: NetworkPolicySource,
    inherited: bool,
    policy: ProtoNetworkPolicyV1,
    generation: u64,
) -> NetworkPolicyDetail {
    NetworkPolicyDetail {
        sandbox_id: TEST_SANDBOX_ID.to_string(),
        effective_policy: Some(policy),
        inherited,
        source_template: if inherited {
            "seed".to_string()
        } else {
            String::new()
        },
        effective_generation: generation,
        applied_generation: generation,
        applied_source: applied_source as i32,
        authority: PolicyAuthority::Cluster.as_proto_i32(),
    }
}

fn test_sandbox_id() -> SandboxId {
    TEST_SANDBOX_ID.parse().expect("test sandbox id parses")
}

fn ref_names(refs: Vec<FacetRef>) -> Vec<String> {
    refs.into_iter().map(|reference| reference.facet).collect()
}

#[test]
fn inventory_facets_publish_full_snapshots_on_every_tick() {
    assert_eq!(
        ref_names(periodic_outbound_refs()),
        vec![
            FACET_SANDBOX_INVENTORY,
            FACET_TEMPLATE_INVENTORY,
            FACET_NODE_STATUS
        ]
    );
}

#[test]
fn policy_facets_pull_periodically_but_do_not_publish_unprompted() {
    assert_eq!(
        ref_names(periodic_inbound_refs()),
        vec![
            FACET_RUNTIME_PROVISIONING,
            FACET_NETWORK_POLICY,
            FACET_SANDBOX_DESIRED_STATE,
            FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
            FACET_TEMPLATE_DESIRED_STATE
        ]
    );
    assert_eq!(
        FacetKind::NetworkPolicy.plugin().outbound_tick(),
        FacetTickMode::DirtyOnly
    );
    assert_eq!(
        FacetKind::SandboxDesiredState.plugin().outbound_tick(),
        FacetTickMode::Disabled
    );
    assert_eq!(
        FacetKind::TemplateBaseSnapshotPolicy
            .plugin()
            .outbound_tick(),
        FacetTickMode::Disabled
    );
    assert_eq!(
        FacetKind::TemplateDesiredState.plugin().outbound_tick(),
        FacetTickMode::Disabled
    );
}

#[test]
fn network_policy_fetch_fallback_only_handles_not_found() {
    let not_found = SandboxInventoryEnrichmentError::GetNetworkPolicies(tonic::Status::not_found(
        "missing sandbox",
    ));
    assert!(should_fallback_to_individual_policy_fetch(&not_found));

    let unavailable = SandboxInventoryEnrichmentError::GetNetworkPolicies(
        tonic::Status::unavailable("sbx-manager unavailable"),
    );
    assert!(!should_fallback_to_individual_policy_fetch(&unavailable));
}

#[derive(Default)]
struct RecordingPortExposureApplier {
    calls: Vec<(String, u32)>,
    /// Result to return per call, in order; `Ok(())` when exhausted.
    results: std::collections::VecDeque<Result<(), SandboxPortExposureApplyError>>,
}

impl SandboxPortExposureApplier for RecordingPortExposureApplier {
    fn apply<'a>(
        &'a mut self,
        sandbox_id: &'a str,
        exposure: &'a SandboxPortExposureDesiredState,
    ) -> Pin<Box<dyn Future<Output = SandboxPortExposureApplyOutcome> + Send + 'a>> {
        self.calls
            .push((sandbox_id.to_string(), exposure.guest_port));
        let result = self.results.pop_front().unwrap_or(Ok(()));
        Box::pin(async {
            SandboxPortExposureApplyOutcome {
                result,
                bridge_status: None,
                service_generation: None,
            }
        })
    }
}

fn open_exposure(guest_port: u32) -> SandboxPortExposureDesiredState {
    SandboxPortExposureDesiredState {
        protocol: SandboxPortProtocol::Tcp as i32,
        guest_port,
        desired_state: SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen as i32,
    }
}

/// Build the stale-pod-netns terminal status the way node-manager does:
/// `FailedPrecondition` tagged with the machine-readable metadata marker.
fn stale_pod_netns_status() -> tonic::Status {
    let mut status = tonic::Status::failed_precondition(
        "pod network namespace is gone; the sandbox allocation is stale",
    );
    status.metadata_mut().insert(
        EXPOSE_TERMINAL_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(EXPOSE_TERMINAL_REASON_STALE_POD_NETNS),
    );
    status
}

fn terminal_expose_error() -> SandboxPortExposureApplyError {
    SandboxPortExposureApplyError::Rpc {
        operation: PortExposureRpc::Expose,
        status: stale_pod_netns_status(),
    }
}

fn terminal_bridge_error() -> SandboxPortExposureApplyError {
    // sbx-manager marks the guest-unreachable precondition terminal; the
    // marker (not the bare code) is what completes the ref.
    SandboxPortExposureApplyError::Bridge(OpenLoopbackBridgeError::Rpc(
        pplx_space_common_grpc::mark_desired_state_apply_terminal(
            tonic::Status::failed_precondition(
                "sandbox is failed; the guest is not reachable for loopback bridge operations",
            ),
        ),
    ))
}

#[tokio::test]
async fn sandbox_with_port_lease_but_missing_state_applies_exposures_and_retries() {
    let mut applier = RecordingPortExposureApplier::default();
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    let mut apply_states = Vec::new();
    let action = sandbox_desired_reconcile_action(false, true, true);

    reconcile_sandbox_port_exposures(
        PortExposureReconcileMode { action },
        &mut applier,
        TEST_SANDBOX_ID,
        vec![open_exposure(8_080)],
        &mut stats,
        &mut element_errors,
        &mut apply_states,
    )
    .await;

    assert_eq!(applier.calls, vec![(TEST_SANDBOX_ID.to_string(), 8_080)]);
    assert_eq!(stats.sandboxes_applied, 1);
    assert_eq!(
        element_errors.get(TEST_SANDBOX_ID).map(String::as_str),
        Some("local sandbox state unavailable")
    );
    assert_eq!(apply_states.len(), 1);
    assert_eq!(apply_states[0].sandbox_id, TEST_SANDBOX_ID);
    assert_eq!(apply_states[0].guest_port, 8_080);
    assert_eq!(
        apply_states[0].apply_state,
        SandboxPortApplyStateKind::SandboxPortApplyStateApplied as i32
    );
}

#[tokio::test]
async fn terminal_expose_failure_keeps_ref_retryable() {
    // A terminal stale-netns failure must publish the failed observation and
    // keep the ref retryable: a repair owner may not yet be available to
    // retire the stranded sandbox.
    let mut applier = RecordingPortExposureApplier {
        results: std::collections::VecDeque::from([Err(terminal_expose_error())]),
        ..RecordingPortExposureApplier::default()
    };
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    let mut apply_states = Vec::new();

    reconcile_sandbox_port_exposures(
        PortExposureReconcileMode {
            action: SandboxDesiredReconcileAction::ApplyPortExposuresAndRetry,
        },
        &mut applier,
        TEST_SANDBOX_ID,
        vec![open_exposure(8_080)],
        &mut stats,
        &mut element_errors,
        &mut apply_states,
    )
    .await;

    // Ref stays retryable: the terminal error is recorded as an element
    // error, plus the missing-local-state retry marker.
    let recorded = element_errors
        .get(TEST_SANDBOX_ID)
        .map(String::as_str)
        .unwrap_or_default();
    assert!(
        recorded.contains("pod network namespace is gone"),
        "expected terminal diagnostic recorded as element error: {recorded}"
    );
    assert!(recorded.contains("local sandbox state unavailable"));
    // The failed observation is still published for the cluster side.
    assert_eq!(apply_states.len(), 1);
    assert_eq!(
        apply_states[0].apply_state,
        SandboxPortApplyStateKind::SandboxPortApplyStateFailed as i32
    );
}

#[tokio::test]
async fn terminal_loopback_bridge_failure_with_local_state_completes_ref() {
    // The dominant unreachable-guest poison case: the guest is unreachable
    // (sandbox failed/suspending/suspended/stopped), so the loopback-bridge
    // open answers FailedPrecondition. The failed observation is published,
    // but the ref completes so it stops pinning the queue-lag gauge.
    let mut applier = RecordingPortExposureApplier {
        results: std::collections::VecDeque::from([Err(terminal_bridge_error())]),
        ..RecordingPortExposureApplier::default()
    };
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    let mut apply_states = Vec::new();
    let action = sandbox_desired_reconcile_action(true, true, true);
    assert_eq!(action, SandboxDesiredReconcileAction::ApplyAll);

    reconcile_sandbox_port_exposures(
        PortExposureReconcileMode { action },
        &mut applier,
        TEST_SANDBOX_ID,
        vec![open_exposure(8_080)],
        &mut stats,
        &mut element_errors,
        &mut apply_states,
    )
    .await;

    assert!(element_errors.is_empty());
    assert_eq!(stats.sandboxes_applied, 0);
    assert_eq!(apply_states.len(), 1);
    assert_eq!(
        apply_states[0].apply_state,
        SandboxPortApplyStateKind::SandboxPortApplyStateFailed as i32
    );
    let error = apply_states[0].error.as_deref().unwrap_or_default();
    assert!(
        error.contains("spaced_open_loopback_bridge"),
        "diagnostic missing: {error}"
    );
    assert!(
        error.contains("not reachable for loopback bridge"),
        "diagnostic missing: {error}"
    );
}

#[tokio::test]
async fn retryable_expose_failure_still_fails_the_ref() {
    let mut applier = RecordingPortExposureApplier {
        results: std::collections::VecDeque::from([Err(SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Expose,
            status: tonic::Status::internal("netlink operation timed out after 5s"),
        })]),
        ..RecordingPortExposureApplier::default()
    };
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    let mut apply_states = Vec::new();

    reconcile_sandbox_port_exposures(
        PortExposureReconcileMode {
            action: SandboxDesiredReconcileAction::ApplyPortExposuresAndRetry,
        },
        &mut applier,
        TEST_SANDBOX_ID,
        vec![open_exposure(8_080)],
        &mut stats,
        &mut element_errors,
        &mut apply_states,
    )
    .await;

    let recorded = element_errors
        .get(TEST_SANDBOX_ID)
        .map(String::as_str)
        .unwrap_or_default();
    assert!(recorded.contains("netlink operation timed out"));
    assert!(recorded.contains("local sandbox state unavailable"));
    assert_eq!(apply_states.len(), 1);
    assert_eq!(
        apply_states[0].apply_state,
        SandboxPortApplyStateKind::SandboxPortApplyStateFailed as i32
    );
}

#[tokio::test]
async fn terminal_expose_failure_with_local_state_stays_retryable() {
    // `ApplyAll` means sbx-manager returned local state, which is published
    // into `sandbox_observed_states` and excludes the row from the orphan
    // candidate query — so no live owner would repair a completed ref.
    // A terminal failure here must stay retryable (record the element error),
    // not complete the ref.
    let mut applier = RecordingPortExposureApplier {
        results: std::collections::VecDeque::from([Err(terminal_expose_error())]),
        ..RecordingPortExposureApplier::default()
    };
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    let mut apply_states = Vec::new();

    reconcile_sandbox_port_exposures(
        PortExposureReconcileMode {
            action: SandboxDesiredReconcileAction::ApplyAll,
        },
        &mut applier,
        TEST_SANDBOX_ID,
        vec![open_exposure(8_080)],
        &mut stats,
        &mut element_errors,
        &mut apply_states,
    )
    .await;

    // Ref stays retryable: no orphan owner for a surviving-local-state row.
    let recorded = element_errors
        .get(TEST_SANDBOX_ID)
        .map(String::as_str)
        .unwrap_or_default();
    assert!(
        recorded.contains("pod network namespace is gone"),
        "expected terminal diagnostic recorded as element error: {recorded}"
    );
    assert_eq!(apply_states.len(), 1);
    assert_eq!(
        apply_states[0].apply_state,
        SandboxPortApplyStateKind::SandboxPortApplyStateFailed as i32
    );
}

#[test]
fn port_exposure_errors_classify_terminal_failures_precisely() {
    // Terminal: ExposePort tagged with the stale-pod-netns marker, or
    // ExposePort allocation NotFound.
    assert!(terminal_expose_error().is_terminal());
    assert!(
        SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Expose,
            status: tonic::Status::not_found("sandbox not allocated"),
        }
        .is_terminal()
    );

    // NOT terminal: a bare ExposePort FailedPrecondition with no marker is
    // the root-namespace ingress-safety rejection — a live allocation the
    // orphan reconciler won't repair, so it must stay retryable.
    assert!(
        !SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Expose,
            status: tonic::Status::failed_precondition(
                "refusing to expose tcp/9012 in the root network namespace"
            ),
        }
        .is_terminal()
    );
    // Guest-unreachable loopback-bridge open (sandbox failed/suspending/
    // suspended/stopped), marked terminal by sbx-manager, is terminal —
    // the dominant unreachable-guest poison case.
    assert!(terminal_bridge_error().is_terminal());
    // An UNMARKED bridge FailedPrecondition — the transient
    // `guest_not_ready` relayed from a still-initializing guest — is NOT
    // terminal, so the ref keeps retrying at the queue's fast cadence.
    assert!(
        !SandboxPortExposureApplyError::Bridge(OpenLoopbackBridgeError::Rpc(
            tonic::Status::failed_precondition("guest_not_ready"),
        ))
        .is_terminal()
    );

    // Same codes on other RPCs, transport-ish codes on ExposePort, and
    // non-terminal bridge failures all stay retryable.
    assert!(
        !SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Allow,
            status: tonic::Status::not_found("sandbox not allocated"),
        }
        .is_terminal()
    );
    // Even the stale-pod-netns marker on a non-Expose RPC is not terminal —
    // only ExposePort carries this contract.
    assert!(
        !SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Allow,
            status: stale_pod_netns_status(),
        }
        .is_terminal()
    );
    assert!(
        !SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Expose,
            status: tonic::Status::internal("add ingress DNAT failed"),
        }
        .is_terminal()
    );
    assert!(
        !SandboxPortExposureApplyError::Rpc {
            operation: PortExposureRpc::Expose,
            status: tonic::Status::unavailable("node-manager unavailable"),
        }
        .is_terminal()
    );
    assert!(
        !SandboxPortExposureApplyError::Invalid("unsupported protocol".to_string()).is_terminal()
    );
    // A bridge RPC with a transient code, and the bridge not-ready /
    // connect arms, all stay retryable.
    assert!(
        !SandboxPortExposureApplyError::Bridge(OpenLoopbackBridgeError::Rpc(
            tonic::Status::unavailable("sbx-manager draining"),
        ))
        .is_terminal()
    );
    assert!(
        !SandboxPortExposureApplyError::Bridge(OpenLoopbackBridgeError::NotReady(
            "status=WaitingForListener".to_string(),
        ))
        .is_terminal()
    );
    assert!(
        !SandboxPortExposureApplyError::Bridge(OpenLoopbackBridgeError::Connect(
            "socket refused".to_string(),
        ))
        .is_terminal()
    );
}

#[test]
fn local_desired_state_apply_classifies_terminal_codes() {
    // Only a status sbx-manager marked terminal (a stuck auto-lifecycle
    // state that never settles) completes the ref.
    assert!(
        LocalDesiredStateApplyError::Rpc {
            operation: LocalDesiredStateRpc::SetAutoLifecyclePolicy,
            status: pplx_space_common_grpc::mark_desired_state_apply_terminal(
                tonic::Status::failed_precondition(
                    "cannot update auto lifecycle while sandbox is failed",
                ),
            ),
        }
        .is_terminal()
    );
    // Unmarked statuses all stay retryable: an unmarked FailedPrecondition
    // (a transient transitioning state that settles), a bare NotFound (the
    // startup race where the local row has not appeared yet), transport /
    // Internal / Unimplemented / DeadlineExceeded.
    assert!(
        !LocalDesiredStateApplyError::Rpc {
            operation: LocalDesiredStateRpc::SetAutoLifecyclePolicy,
            status: tonic::Status::failed_precondition(
                "cannot update auto lifecycle while sandbox is resuming",
            ),
        }
        .is_terminal()
    );
    for status in [
        tonic::Status::not_found("sandbox not found"),
        tonic::Status::unavailable("sbx-manager draining for restart"),
        tonic::Status::internal("db transaction commit failed"),
        tonic::Status::unimplemented("rollout skew: RPC not yet served"),
        tonic::Status::deadline_exceeded("timed out"),
    ] {
        assert!(
            !LocalDesiredStateApplyError::Rpc {
                operation: LocalDesiredStateRpc::EnableSnapshots,
                status,
            }
            .is_terminal()
        );
    }
}

#[test]
fn terminal_local_desired_state_apply_completes_ref() {
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    handle_local_desired_state_apply(
        Err(LocalDesiredStateApplyError::Rpc {
            operation: LocalDesiredStateRpc::SetAutoLifecyclePolicy,
            status: pplx_space_common_grpc::mark_desired_state_apply_terminal(
                tonic::Status::failed_precondition(
                    "cannot update auto lifecycle while sandbox is failed",
                ),
            ),
        }),
        TEST_SANDBOX_ID,
        &mut stats,
        &mut element_errors,
    );
    // Terminal: the ref completes (no element error) and is not counted as
    // an applied sandbox; periodic sync re-applies after state changes.
    assert!(element_errors.is_empty());
    assert_eq!(stats.sandboxes_applied, 0);
}

#[test]
fn retryable_local_desired_state_apply_fails_ref() {
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    handle_local_desired_state_apply(
        Err(LocalDesiredStateApplyError::Rpc {
            operation: LocalDesiredStateRpc::SetAutoLifecyclePolicy,
            status: tonic::Status::unavailable("sbx-manager draining for restart"),
        }),
        TEST_SANDBOX_ID,
        &mut stats,
        &mut element_errors,
    );
    // Transient: an element error is recorded so the queue re-drives the
    // ref, and the diagnostic names the operation.
    let error = element_errors.get(TEST_SANDBOX_ID).map(String::as_str);
    assert_eq!(stats.sandboxes_applied, 0);
    let error = error.unwrap_or_default();
    assert!(
        error.contains("set auto-lifecycle policy"),
        "diagnostic missing operation: {error}"
    );
}

#[test]
fn applied_local_desired_state_counts_as_applied() {
    let mut stats = ReconcileStats::default();
    let mut element_errors = BTreeMap::new();
    handle_local_desired_state_apply(Ok(()), TEST_SANDBOX_ID, &mut stats, &mut element_errors);
    assert!(element_errors.is_empty());
    assert_eq!(stats.sandboxes_applied, 1);
}

#[test]
fn sandbox_policy_apply_filter_skips_matching_policies() {
    let policy = test_network_policy(EgressDefaultAction::Allow);
    let desired_template = desired_sandbox_policy(NetworkPolicySource::Template, policy.clone(), 7);
    let local_template =
        local_sandbox_policy(NetworkPolicySource::Template, true, policy.clone(), 7);
    assert!(!sandbox_policy_needs_apply(
        &desired_template,
        Some(&local_template)
    ));

    let desired_override = desired_sandbox_policy(NetworkPolicySource::Override, policy.clone(), 8);
    let local_override = local_sandbox_policy(NetworkPolicySource::Override, false, policy, 8);
    assert!(!sandbox_policy_needs_apply(
        &desired_override,
        Some(&local_override)
    ));

    let sandbox_id = test_sandbox_id();
    let local_policies =
        LocalSandboxNetworkPolicies::from_policies(BTreeMap::from([(sandbox_id, local_template)]));
    assert_eq!(
        sandbox_policy_reconcile_action(&desired_template, &sandbox_id, &local_policies),
        SandboxPolicyReconcileAction::Skip
    );
}

/// An inherited policy's local generations are the template's counter, not
/// the sandbox generation node-ctl reconciles with, so they never match it
/// once the template has changed. A matching inherited policy is skipped
/// rather than re-applied, and a gateway refresh queued, on every pass.
#[test]
fn sandbox_policy_apply_filter_matches_inherited_policy_by_content() {
    let policy = test_network_policy(EgressDefaultAction::Allow);
    let desired = desired_sandbox_policy(NetworkPolicySource::Template, policy.clone(), 1);
    let local = local_sandbox_policy(NetworkPolicySource::Template, true, policy, 2);
    assert!(!sandbox_policy_needs_apply(&desired, Some(&local)));

    let stale_template = local_sandbox_policy(
        NetworkPolicySource::Template,
        true,
        test_network_policy(EgressDefaultAction::Deny),
        2,
    );
    assert!(sandbox_policy_needs_apply(&desired, Some(&stale_template)));

    // The gateway has not applied the template generation the sandbox now
    // inherits, so the policy is applied again to refresh it.
    let gateway_behind = NetworkPolicyDetail {
        applied_generation: 1,
        ..local_sandbox_policy(
            NetworkPolicySource::Template,
            true,
            test_network_policy(EgressDefaultAction::Allow),
            2,
        )
    };
    assert!(sandbox_policy_needs_apply(&desired, Some(&gateway_behind)));
}

#[test]
fn sandbox_policy_apply_filter_applies_on_missing_or_effective_drift() {
    let desired = desired_sandbox_policy(
        NetworkPolicySource::Override,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    assert!(sandbox_policy_needs_apply(&desired, None));

    let stale_generation = local_sandbox_policy(
        NetworkPolicySource::Override,
        false,
        test_network_policy(EgressDefaultAction::Allow),
        6,
    );
    assert!(sandbox_policy_needs_apply(
        &desired,
        Some(&stale_generation)
    ));

    let wrong_policy = local_sandbox_policy(
        NetworkPolicySource::Override,
        false,
        test_network_policy(EgressDefaultAction::Deny),
        7,
    );
    assert!(sandbox_policy_needs_apply(&desired, Some(&wrong_policy)));

    let wrong_source = local_sandbox_policy(
        NetworkPolicySource::Template,
        true,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    assert!(sandbox_policy_needs_apply(&desired, Some(&wrong_source)));

    let mut local_authority = local_sandbox_policy(
        NetworkPolicySource::Override,
        false,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    local_authority.authority = PolicyAuthority::Local.as_proto_i32();
    assert!(sandbox_policy_needs_apply(&desired, Some(&local_authority)));
}

#[test]
fn sandbox_policy_reconcile_action_skips_known_missing_local_sandbox() {
    let desired = desired_sandbox_policy(
        NetworkPolicySource::Override,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    let sandbox_id = test_sandbox_id();
    let mut local_policies = LocalSandboxNetworkPolicies::default();
    local_policies.insert_missing(sandbox_id);

    assert_eq!(
        sandbox_policy_reconcile_action(&desired, &sandbox_id, &local_policies),
        SandboxPolicyReconcileAction::Skip
    );
}

#[test]
fn sandbox_policy_reconcile_action_applies_when_policy_was_not_prefetched() {
    let desired = desired_sandbox_policy(
        NetworkPolicySource::Override,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    let sandbox_id = test_sandbox_id();
    let local_policies = LocalSandboxNetworkPolicies::default();

    assert_eq!(
        sandbox_policy_reconcile_action(&desired, &sandbox_id, &local_policies),
        SandboxPolicyReconcileAction::Apply
    );
}

#[test]
fn sandbox_policy_apply_filter_applies_on_stale_gateway_state() {
    let desired = desired_sandbox_policy(
        NetworkPolicySource::Template,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );

    let mut stale_applied_generation = local_sandbox_policy(
        NetworkPolicySource::Template,
        true,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    stale_applied_generation.applied_generation = 6;
    assert!(sandbox_policy_needs_apply(
        &desired,
        Some(&stale_applied_generation)
    ));

    let mut stale_applied_source = local_sandbox_policy(
        NetworkPolicySource::Template,
        true,
        test_network_policy(EgressDefaultAction::Allow),
        7,
    );
    stale_applied_source.applied_source = NetworkPolicySource::Override as i32;
    assert!(sandbox_policy_needs_apply(
        &desired,
        Some(&stale_applied_source)
    ));
}

#[test]
fn published_sandbox_state_uses_batched_enrichment() {
    let execution: pplx_space_execution_common::proto::ExecutionMetadata =
        (&pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
            pplx_space_common_core::types::Arch::Aarch64,
            vec![],
        ))
            .into();
    let sandbox = SandboxInfo {
        sandbox_id: TEST_SANDBOX_ID.to_string(),
        template: Some("seed".to_string()),
        lifecycle: Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started as i32),
        snapshots_enabled: true,
        state_degraded: true,
        activation_attempt_id: Some(TEST_ACTIVATION_ATTEMPT_ID.to_string()),
        auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_status: Some(pplx_space_sbx_mgr_client::proto::AutoLifecycleStatus {
            conditions: vec![
                pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionStatus {
                    condition_set:
                        pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionSetId::AccessIdle
                            as i32,
                    last_touched_at: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
                pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionStatus {
                    condition_set:
                        pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionSetId::CommandIdle
                            as i32,
                    last_touched_at: Some(prost_types::Timestamp {
                        seconds: 2,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ],
            last_access_at: Some(prost_types::Timestamp {
                seconds: 10,
                nanos: 0,
            }),
            last_command_at: Some(prost_types::Timestamp {
                seconds: 20,
                nanos: 0,
            }),
            ..Default::default()
        }),
        execution: Some(execution.clone()),
        state_changed_at: Some(prost_types::Timestamp {
            seconds: 30,
            nanos: 0,
        }),
        quarantine_recovery: Some(
            pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservation {
                local_quarantine_id: "019e6370-cfc9-7a43-9337-399c7b51a1db".to_string(),
                observed_owner: pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservedOwner::LegacyRecovery as i32,
                attempt_count: 3,
                next_attempt_at: Some(prost_types::Timestamp {
                    seconds: 1_786_800_000,
                    nanos: 0,
                }),
                schema_version: 1,
                ..Default::default()
            },
        ),
        ..SandboxInfo::default()
    };
    let policy = NetworkPolicyDetail {
        sandbox_id: sandbox.sandbox_id.clone(),
        applied_generation: 7,
        applied_source: NetworkPolicySource::Override as i32,
        authority: PolicyAuthority::Cluster.as_proto_i32(),
        ..NetworkPolicyDetail::default()
    };
    let snapshot = SnapshotDetail {
        id: "d-test".to_string(),
        sandbox_id: sandbox.sandbox_id.clone(),
        ..SnapshotDetail::default()
    };

    let state = published_sandbox_state(&sandbox, Some(42), Some(&policy), vec![snapshot.clone()]);

    assert_eq!(state.sandbox_id, sandbox.sandbox_id);
    assert_eq!(state.status.as_deref(), Some("started"));
    assert_eq!(state.observed_network_policy_generation, 7);
    assert_eq!(state.observed_network_policy_source, policy.applied_source);
    assert_eq!(state.observed_network_policy_authority, policy.authority);
    assert_eq!(state.disk_usage_bytes, Some(42));
    assert_eq!(state.snapshots, vec![snapshot]);
    assert_eq!(state.execution, Some(execution));
    assert_eq!(
        state.state_changed_at,
        Some(prost_types::Timestamp {
            seconds: 30,
            nanos: 0,
        })
    );
    assert_eq!(state.quarantine_recovery, sandbox.quarantine_recovery);
    assert_eq!(
        state.activation_attempt_id.as_deref(),
        Some(TEST_ACTIVATION_ATTEMPT_ID)
    );
    assert!(state.state_degraded);
    assert_eq!(
        state.last_used_at,
        Some(prost_types::Timestamp {
            seconds: 20,
            nanos: 0,
        })
    );
    assert_eq!(
        state.last_access_at,
        Some(prost_types::Timestamp {
            seconds: 10,
            nanos: 0,
        })
    );
    assert_eq!(
        state.last_command_at,
        Some(prost_types::Timestamp {
            seconds: 20,
            nanos: 0,
        })
    );
}

#[test]
fn quarantined_published_sandbox_state_wire_shape() {
    let sandbox = pplx_space_sbx_mgr_client::test_support::with_suspend_artifact(
        pplx_space_sbx_mgr_client::test_support::quarantined_sandbox_info_fixture(
            "local artifact publication failed",
            "suspend_snapshot_publication_failed",
        ),
    );

    let state = published_sandbox_state(&sandbox, None, None, Vec::new());

    insta::assert_debug_snapshot!(state);
}

#[test]
fn missing_or_unknown_lifecycle_preserves_cluster_state() {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    for (lifecycle, expected, degraded) in [
        (None, None, true),
        (
            Some(SandboxLifecycleState::Suspended as i32),
            Some("suspended"),
            false,
        ),
        (Some(0), None, true),
        (Some(999), None, true),
    ] {
        let sandbox = SandboxInfo {
            sandbox_id: TEST_SANDBOX_ID.to_string(),
            lifecycle,
            state_changed_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let publication = published_sandbox_state(&sandbox, None, None, Vec::new());
        assert_eq!(publication.status.as_deref(), expected);
        assert_eq!(
            publication.state_changed_at,
            expected.and(sandbox.state_changed_at)
        );
        assert_eq!(publication.state_degraded, degraded);
        assert!(publication.present);
    }
}

#[test]
fn recovered_suspended_published_sandbox_state_wire_shape() {
    let sandbox = pplx_space_sbx_mgr_client::test_support::with_suspend_artifact(
        pplx_space_sbx_mgr_client::test_support::recovered_suspended_sandbox_info_fixture(),
    );

    let state = published_sandbox_state(&sandbox, None, None, Vec::new());

    insta::assert_debug_snapshot!(state);
}

#[test]
fn published_sandbox_state_ignores_lifecycle_condition_clocks_for_activity() {
    let sandbox = SandboxInfo {
        sandbox_id: TEST_SANDBOX_ID.to_string(),
        template: Some("seed".to_string()),
        lifecycle: Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started as i32),
        auto_lifecycle_status: Some(pplx_space_sbx_mgr_client::proto::AutoLifecycleStatus {
            conditions: vec![
                pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionStatus {
                    condition_set:
                        pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionSetId::AccessIdle
                            as i32,
                    last_touched_at: Some(prost_types::Timestamp {
                        seconds: 10,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
                pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionStatus {
                    condition_set:
                        pplx_space_sbx_mgr_client::proto::AutoLifecycleConditionSetId::CommandIdle
                            as i32,
                    last_touched_at: Some(prost_types::Timestamp {
                        seconds: 20,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }),
        ..SandboxInfo::default()
    };
    let state = published_sandbox_state(&sandbox, None, None, Vec::new());

    assert_eq!(state.observed_network_policy, None);
    assert_eq!(state.observed_network_policy_generation, 0);
    assert_eq!(state.observed_network_policy_source, 0);
    assert_eq!(state.observed_network_policy_authority, 0);
    assert_eq!(state.last_used_at, None);
    assert_eq!(state.last_access_at, None);
    assert_eq!(state.last_command_at, None);
}

/// A fetch that outruns the budget yields no report, never an empty one.
///
/// This is the safety property the whole field rests on: an empty report
/// asserts the disk holds nothing that needs rescuing, and an enumeration
/// that never finished has established no such thing. Nothing in the type
/// system enforces it — both arms return the same `Option`, and
/// `Some(PendingLocalBackupReport::default())` compiles exactly as well as
/// `None`.
///
/// The inner future is what makes this non-vacuous: it *would* answer, and
/// with the empty report that is the dangerous value, so the assertion
/// fails if the budget stops cutting it off. Virtual time keeps it
/// hermetic and instant — the real collector would otherwise reach
/// whatever sandbox-manager happens to be listening on the host socket.
#[tokio::test(start_paused = true)]
async fn a_fetch_that_outruns_the_budget_yields_no_report() {
    let report = bounded_pending_local_backups(async {
        tokio::time::sleep(PENDING_LOCAL_BACKUP_BUDGET * 10).await;
        Some(PendingLocalBackupReport {
            pending: Vec::new(),
        })
    })
    .await;

    assert_eq!(
        report, None,
        "a fetch cut off by the budget must not claim the disk is clean"
    );
}

/// The budget must not swallow an answer that arrives in time.
///
/// What this catches is the `Ok` arm being rewritten to discard its
/// report — `Ok(_) => None`. It is not what stops a zero budget: the test
/// above already fails at zero, because the slow fetch sleeps a multiple
/// of the budget and so returns immediately too.
#[tokio::test(start_paused = true)]
async fn a_fetch_inside_the_budget_is_reported() {
    let report = bounded_pending_local_backups(async {
        tokio::time::sleep(PENDING_LOCAL_BACKUP_BUDGET / 2).await;
        Some(PendingLocalBackupReport {
            pending: Vec::new(),
        })
    })
    .await;

    assert_eq!(
        report,
        Some(PendingLocalBackupReport {
            pending: Vec::new()
        })
    );
}

/// The budget has to stay a small share of the interval it buys time
/// against.
///
/// The two tests above are relative — the slow fetch sleeps a multiple of
/// the budget, so they pass at any non-degenerate value, including one
/// large enough to starve the census they are supposed to protect. They
/// pass at every budget large enough to matter, which is precisely why
/// they can neither floor nor cap it. This pins the absolute property
/// they cannot: the report is
/// regenerated on the next poll, so spending an appreciable fraction of
/// that interval waiting for it trades a publish for a value that is
/// about to be recomputed anyway.
///
/// `DEFAULT_POLL_INTERVAL_SECS` is the last-resort fallback in
/// `resolve_poll_interval`, behind the CLI flag and the config file — but
/// it is the deployed number: `space-node-ctl`'s chart sets
/// `pollIntervalSecs: 60` and passes it as `--poll-interval-secs`. An
/// operator who shortens the interval materially would want to revisit
/// the budget, and this assertion is where that shows up.
#[test]
fn the_budget_stays_a_small_share_of_the_census_interval() {
    let poll_interval = Duration::from_secs(crate::DEFAULT_POLL_INTERVAL_SECS);

    assert!(
        PENDING_LOCAL_BACKUP_BUDGET * 10 <= poll_interval,
        "budget {PENDING_LOCAL_BACKUP_BUDGET:?} is more than a tenth of the {poll_interval:?} census interval"
    );
}

/// A `None` from the fetch reaches the caller as `None`.
///
/// The two tests above both hand the seam a `Some`, so neither can see the
/// `Ok` arm substituting a value of its own: `Ok(_) => Some(empty)` passes
/// both. That substitution is exactly the failure the field exists to
/// avoid: every failure arm inside the fetch produces `None` and all of
/// them pass through here, so an `Ok` arm that manufactured an empty
/// report would convert every one of them into "this disk is clean".
///
/// This pins the seam only. The arms themselves — connect failure, RPC
/// error, `Unimplemented` — have no test; reaching them needs a fake
/// sandbox-manager this crate has no harness for, so each is one
/// `return None` read by eye.
#[tokio::test(start_paused = true)]
async fn a_none_from_the_fetch_is_not_turned_into_an_empty_report() {
    let report = bounded_pending_local_backups(async { None }).await;

    assert_eq!(report, None);
}

/// The budget has to outlast several of `connect()`'s dial attempts.
///
/// [`PENDING_LOCAL_BACKUP_BUDGET`]'s doc says two seconds "still covers
/// several of `connect()`'s retry attempts", which is a claim about
/// another crate's constant rather than about anything in this file — so
/// pin it against that constant instead of restating it. Without a floor,
/// `Duration::from_millis(1)` satisfies every other test here while
/// guaranteeing no real dial ever completes.
///
/// What this sums is the backoff *between* attempts, not three completed
/// dials: it counts neither the policy's jitter nor the dial itself, so
/// it is a lower bound on the time three retries occupy. The headroom is
/// wide enough that the floor holds anyway.
#[test]
fn the_budget_outlasts_several_connect_retry_attempts() {
    let policy = pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY;
    let mut delay = policy.initial_delay;
    let mut backoff = Duration::ZERO;
    for _ in 0..3 {
        backoff += delay;
        delay = delay
            .mul_f64(policy.backoff_multiplier)
            .min(policy.max_delay);
    }

    assert!(
        PENDING_LOCAL_BACKUP_BUDGET > backoff,
        "budget {PENDING_LOCAL_BACKUP_BUDGET:?} does not cover the {backoff:?} of backoff \
         before a fourth dial attempt"
    );
}
