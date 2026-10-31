"""Deterministic duration-balanced partitions of an already eligible inventory."""

from typing import Any

from space_smoke.registry import Scenario

# Rounded Kind wall-clock seconds, including scenario setup and cleanup. These
# are scheduling hints, not timeouts or an allowlist. New/renamed scenarios get
# a conservative default and always participate. Refresh from successful runs'
# RUN/END timestamps; keep API surfaces separate because their costs differ.
#
# Each value is the median of the eight successful Kind Pull CI runs (otel
# mode, four shards on 16-core runners) of the pull requests merged from
# 2026-10-28T18:00Z to 2026-10-29T18:00Z, unless its comment says otherwise.
_DURATION_SECONDS = {
    "ecw": {
        "ecw.access_idle_reconnect_preserves_workspace": 22,
        "ecw.authenticated_access_with_public_traffic_disabled": 4,
        "ecw.background_process_control": 5,
        "ecw.binary_file_round_trip": 4,
        "ecw.cold_create_environment": 6,
        "ecw.command_round_trip": 4,
        "ecw.domain_access_survives_default_deny": 4,
        "ecw.filesystem_mutations": 4,
        "ecw.filesystem_ownership": 4,
        "ecw.filesystem_private_boundary": 4,
        "ecw.filesystem_read_identity": 4,
        "ecw.filesystem_round_trip": 4,
        "ecw.filesystem_watcher": 4,
        "ecw.filesystem_write_identity": 4,
        "ecw.fork_idempotency": 8,
        "ecw.fork_isolation": 11,
        "ecw.guest_vlan_frames_dropped": 9,
        "ecw.immediate_egress": 4,
        "ecw.internet_access_allowed": 4,
        "ecw.internet_access_denied": 28,
        "ecw.large_stdout_integrity": 4,
        "ecw.loopback_route_stays_open": 17,
        "ecw.pause_connect": 8,
        "ecw.pause_connect_race": 46,
        "ecw.pause_timeout_requires_connect": 14,
        "ecw.process_identity": 4,
        "ecw.process_reattach": 14,
        "ecw.public_port_to_all_interfaces": 5,
        "ecw.public_port_to_loopback": 4,
        "ecw.refresh_keepalive": 41,
        "ecw.sandbox_delete": 4,
        "ecw.sandbox_info": 4,
        "ecw.sandbox_list_pagination": 8,
        "ecw.timeout_rearm": 41,
        "ecw.timeout_update": 54,
        "ecw.warm_pool_environment": 14,
        "ecw.watcher_identity": 4,
        "local.btrfs_send_restore_fails_closed_without_parent_source": 56,
        "local.btrfs_send_restore_pulls_parent_template": 47,
        "local.btrfs_send_restore_rejects_corrupted_stream": 56,
        "local.cold_sandbox_eviction_wake_round_trip": 137,
        "local.concurrent_restore_converges_to_one_execution": 62,
        "local.deferred_btrfs_send_capture_restores_cross_node": 13,
        "local.ecw_template_build_from_oci_boots": 41,
        "local.encrypted_disk_fork": 57,
        "local.encrypted_live_disk_fork": 34,
        "local.encrypted_resuspend_preserves_cow": 49,
        "local.forced_drain_evacuates_sandbox": 22,
        "local.fork_suspended_cross_node": 13,
        "local.gentle_drain_leaves_sandbox_resident": 36,
        # At the 18 s lease TTL floor, with the budget at its 72 s floor: three
        # Kind runs took 448-469 s, most of it the outage's budget wait, the
        # contender's watch of about 120 s and two 60 s tick windows.
        "local.leader_election_brownout_keeps_one_leader": 457,
        "local.node_ctl_fence_refuses_and_reconciles": 11,
        "local.node_group_boundary_schedules": 37,
        "local.node_selector_extras_render_and_enforce": 63,
        "local.orphaned_restore_pod_gc_reaps_only_superseded": 98,
        "local.pending_backup_census_reports_blocked_upload": 17,
        "local.private_template_build_isolation": 62,
        "local.rate_limiters_enforce_and_fail_open": 48,
        "local.reconcile_queue_gc_prunes_only_gone_nodes": 132,
        "local.restarted_btrfs_send_is_rejected_and_retried": 30,
        "local.retention_claim_finish_forward": 68,
        "local.retention_restore_source_guard": 40,
        "local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown": 174,
        "local.sandbox_network_isolation_holds": 21,
        "local.snapshot_reaper_reclaims_backed_fork_snapshot": 77,
        "local.storage_exchange_scoping": 34,
        "local.suspend_restore_cross_node_from_object_storage": 11,
        "local.suspend_snapshot_rows_are_per_attempt": 16,
        "local.telemetry_reaches_local_backends": 18,
        "local.template_cache_lru_eviction_reclaims": 90,
        "local.template_startup_recovery_decision_table": 29,
        "local.terminate_gate_refuses_a_held_node": 5,
        # Not in the measured runs. A Kind bench run with the 20 s wait took
        # 116 s, 29 s of it the two node-ctl rollouts (95-96 s with a 10 s wait).
        "local.unanswered_template_deleted_after_wait": 116,
        "network_policy.v1_cidr_allow": 11,
        "network_policy.v1_cidr_deny": 11,
        "network_policy.v1_deny_precedes_allow": 11,
        "network_policy.v1_domain_binding": 5,
        "network_policy.v1_exact_ip_allow": 11,
        "network_policy.v1_exact_ip_deny": 12,
        "network_policy.v1_fastly_shared_edge_bypass": 4,
        "network_policy.v1_platform_guardrail_precedes_allow": 26,
        "network_policy.v1_rules_survive_restore": 31,
        "network_policy.v1_tcp_dns_transport_respects_ip_allow": 4,
        "network_policy.v1_udp_deny_precedes_allow": 13,
        "network_policy.v1_udp_respects_cidr_allow": 13,
        "network_policy.v1_udp_respects_cidr_deny": 13,
        "network_policy.v1_udp_respects_ip_allow": 13,
        "network_policy.v1_udp_respects_ip_deny": 13,
        "network_policy.v1_wildcard_domain_allow": 18,
    },
    "native": {
        "ecw.background_process_control": 4,
        "ecw.command_round_trip": 3,
        "ecw.pause_connect": 8,
        "ecw.sandbox_delete": 3,
        "ecw.sandbox_info": 3,
        "ecw.sandbox_list_pagination": 9,
        "native.encrypted_double_restore_retires_prior_pods": 40,
        "native.m1_generated_sdk_conformance": 18,
        # Not in the measured runs. Estimate until a bench run refreshes it:
        # three live policy updates, each waiting for gateway reconcile and an
        # HTTPS probe.
        "native.network_policy_live_update": 30,
        "native.resident_pause": 6,
        "native.stopped_restart_preserves_rootfs": 74,
        "native_concurrency.command_idempotency": 4,
        "native_concurrency.create_idempotency": 3,
        "native_concurrency.independent_commands": 4,
        "native_concurrency.lifecycle_contention": 10,
        "native_concurrency.slow_reader": 5,
        "native_contract.command_attach_after_pause": 6,
        "native_contract.command_on_sandbox_not_running": 7,
        "native_contract.command_validation": 3,
        "native_contract.command_wakes_idle_suspended_sandbox": 68,
        "native_contract.completed_delete_replay": 2,
        # Not in the measured runs; an estimate.
        "native_contract.file_idempotency_conflict": 5,
        "native_contract.idempotency_conflict": 2,
        "native_contract.permanent_command_error": 3,
        "native_contract.process_session_wakes_idle_suspended_sandbox": 69,
        # Not in the measured runs: the median of three later Kind runs, at
        # 131-136 s and 71-76 s.
        "native_contract.session_reattach_wakes_idle_suspended_sandbox": 135,
        "native_contract.session_reconnect_after_idle_suspend": 76,
        "native_contract.tenant_isolation": 3,
        "native_contract.token_scope": 3,
        "native_contract.zero_vcpu_admission": 30,
        "native_recovery.attached_stream_drains_through_data_plane_restart": 16,
        "native_recovery.command_survives_data_plane_restart": 15,
        "native_recovery.queued_create_survives_api_restart": 19,
        "native_recovery.slow_operation_does_not_block_later_work": 20,
        "native_recovery.stale_queued_work_expires_after_controller_outage": 68,
    },
}


def select_shard(
    scenarios: tuple[Scenario[Any], ...],
    *,
    index: int,
    count: int,
    api_surface: str,
) -> tuple[Scenario[Any], ...]:
    """Place expensive scenarios first on the lightest shard, then run by ID."""
    durations = _DURATION_SECONDS.get(api_surface, {})
    loads = [0] * count
    partitions: list[list[Scenario[Any]]] = [[] for _ in range(count)]
    for scenario in sorted(
        scenarios, key=lambda scenario: (-durations.get(scenario.id, 30), scenario.id)
    ):
        # Equal loads choose the lower shard index. Neither discovery order nor
        # process-local hash randomization may change the partition assignment.
        target = min(range(count), key=loads.__getitem__)
        partitions[target].append(scenario)
        loads[target] += durations.get(scenario.id, 30)
    return tuple(sorted(partitions[index - 1], key=lambda scenario: scenario.id))
