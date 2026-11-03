"""Deterministic duration-balanced partitions of an already eligible inventory."""

from collections.abc import Mapping
from typing import Any

from space_smoke.registry import Scenario

# Rounded Kind wall-clock seconds, including scenario setup and cleanup. These
# are scheduling hints, not timeouts or an allowlist. New/renamed scenarios get
# a conservative default and always participate. Refresh from successful runs'
# RUN/END timestamps; keep API surfaces separate because their costs differ.
#
# Each value is the median of the successful twelve-shard Kind runs (otel mode,
# 16-core runners, the Kind chart's 10 s reconciler ticks) of Pull CI and Main
# CI from 2026-11-02T22:15Z to 2026-11-03T10:00Z: 61 runs, 29-51 samples per
# scenario. A value with a comment of its own was not measured there; its
# comment says where it comes from.
_DURATION_SECONDS = {
    "ecw": {
        "ecw.access_idle_reconnect_preserves_workspace": 22,
        "ecw.authenticated_access_with_public_traffic_disabled": 4,
        "ecw.background_process_control": 5,
        "ecw.binary_file_round_trip": 5,
        "ecw.cold_create_environment": 6,
        "ecw.command_round_trip": 4,
        "ecw.domain_access_survives_default_deny": 4,
        "ecw.filesystem_mutations": 5,
        "ecw.filesystem_ownership": 5,
        "ecw.filesystem_private_boundary": 4,
        "ecw.filesystem_read_identity": 5,
        "ecw.filesystem_round_trip": 4,
        "ecw.filesystem_watcher": 5,
        "ecw.filesystem_write_identity": 5,
        "ecw.fork_idempotency": 9,
        "ecw.fork_isolation": 11,
        "ecw.guest_mptcp_segments_dropped": 7,
        "ecw.guest_vlan_frames_dropped": 9,
        "ecw.immediate_egress": 4,
        "ecw.internet_access_allowed": 5,
        "ecw.internet_access_denied": 28,
        "ecw.large_multipart_upload_round_trip": 5,
        "ecw.large_stdout_integrity": 4,
        "ecw.loopback_route_stays_open": 18,
        "ecw.pause_connect": 9,
        "ecw.pause_connect_race": 44,
        "ecw.pause_timeout_requires_connect": 13,
        "ecw.process_identity": 5,
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
        "local.btrfs_send_restore_pulls_parent_template": 46,
        "local.btrfs_send_restore_rejects_corrupted_stream": 56,
        "local.cold_sandbox_eviction_wake_round_trip": 138,
        "local.concurrent_restore_converges_to_one_execution": 60,
        # Three local Kind runs each, with Playwright's Chromium on the host,
        # until CI runs them: CI withholds them unless a console path changes.
        # The median of each, summing the phase durations, bootstrap included.
        # The detail scenario's restore and second suspend and the lookup
        # scenario's second sandbox are in them.
        "local.console_destructive_actions_require_confirmation": 23,
        "local.console_links_resolve": 15,
        "local.console_operator_suspends_another_orgs_sandbox": 19,
        "local.console_sandbox_detail_complete_in_every_state": 22,
        "local.console_sandbox_lookup_accepts_every_id_form": 18,
        "local.console_turns_away_non_operators": 15,
        "local.deferred_btrfs_send_capture_restores_cross_node": 14,
        "local.ecw_template_build_from_oci_boots": 39,
        "local.encrypted_disk_fork": 46,
        "local.encrypted_live_disk_fork": 33,
        "local.encrypted_resuspend_preserves_cow": 48,
        "local.forced_drain_evacuates_sandbox": 22,
        "local.fork_suspended_cross_node": 13,
        "local.gentle_drain_leaves_sandbox_resident": 35,
        "local.leader_election_brownout_keeps_one_leader": 302,
        "local.node_ctl_fence_refuses_and_reconciles": 11,
        "local.node_group_boundary_schedules": 36,
        "local.node_selector_extras_render_and_enforce": 62,
        "local.orphaned_restore_pod_gc_reaps_only_superseded": 127,
        "local.pending_backup_census_reports_blocked_upload": 18,
        "local.private_template_build_isolation": 54,
        "local.rate_limiters_enforce_and_fail_open": 49,
        "local.reconcile_queue_gc_prunes_only_gone_nodes": 16,
        "local.restarted_btrfs_send_is_rejected_and_retried": 30,
        "local.retention_claim_finish_forward": 66,
        "local.retention_restore_source_guard": 42,
        "local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown": 173,
        "local.sandbox_network_isolation_holds": 21,
        "local.snapshot_reaper_reclaims_backed_fork_snapshot": 75,
        "local.storage_exchange_scoping": 30,
        "local.suspend_restore_cross_node_from_object_storage": 11,
        "local.suspend_snapshot_rows_are_per_attempt": 16,
        "local.telemetry_reaches_local_backends": 19,
        "local.template_cache_lru_eviction_reclaims": 88,
        "local.template_startup_recovery_decision_table": 31,
        "local.terminate_gate_refuses_a_held_node": 7,
        "local.unanswered_template_deleted_after_wait": 104,
        "network_policy.v1_cidr_allow": 11,
        "network_policy.v1_cidr_deny": 11,
        "network_policy.v1_deny_precedes_allow": 11,
        "network_policy.v1_domain_binding": 4,
        "network_policy.v1_exact_ip_allow": 11,
        "network_policy.v1_exact_ip_deny": 11,
        "network_policy.v1_fastly_shared_edge_bypass": 4,
        "network_policy.v1_platform_guardrail_precedes_allow": 26,
        "network_policy.v1_rules_survive_restore": 30,
        "network_policy.v1_tcp_dns_transport_respects_ip_allow": 4,
        "network_policy.v1_udp_deny_precedes_allow": 13,
        "network_policy.v1_udp_respects_cidr_allow": 13,
        "network_policy.v1_udp_respects_cidr_deny": 13,
        "network_policy.v1_udp_respects_ip_allow": 14,
        "network_policy.v1_udp_respects_ip_deny": 14,
        "network_policy.v1_wildcard_domain_allow": 19,
    },
    "native": {
        "ecw.background_process_control": 5,
        "ecw.command_round_trip": 3,
        "ecw.pause_connect": 9,
        "ecw.sandbox_delete": 3,
        "ecw.sandbox_info": 3,
        "ecw.sandbox_list_pagination": 9,
        "native.encrypted_double_restore_retires_prior_pods": 39,
        "native.forced_drain_re_drains_re_placed_stopped_sandbox": 54,
        "native.forced_drain_re_drains_restarted_re_placed_stopped_sandbox": 63,
        "native.forced_drain_re_places_stopped_sandbox": 53,
        "native.m1_generated_sdk_conformance": 18,
        "native.network_policy_live_update": 7,
        "native.resident_pause": 5,
        "native.stopped_restart_preserves_rootfs": 75,
        "native_concurrency.command_idempotency": 3,
        "native_concurrency.create_idempotency": 3,
        "native_concurrency.independent_commands": 3,
        "native_concurrency.lifecycle_contention": 10,
        "native_concurrency.slow_reader": 5,
        "native_contract.command_attach_after_pause": 8,
        "native_contract.command_on_sandbox_not_running": 7,
        "native_contract.command_validation": 3,
        "native_contract.command_wakes_idle_suspended_sandbox": 70,
        "native_contract.completed_delete_replay": 3,
        # Not in the measured runs; an estimate.
        "native_contract.file_idempotency_across_origins": 5,
        "native_contract.file_idempotency_conflict": 3,
        # Not in the measured runs. Estimates until a bench run refreshes
        # them: an idle suspend and an inline wake; two 16 MiB writes; a data
        # plane rollout there and back.
        "native_contract.file_op_wakes_idle_suspended_sandbox": 100,
        "native_contract.file_operations_match_on_the_data_plane": 10,
        "native_contract.file_upload_admission_429": 60,
        "native_contract.file_write_size_boundary_on_the_data_plane": 15,
        "native_contract.idempotency_conflict": 3,
        "native_contract.permanent_command_error": 3,
        "native_contract.process_session_wakes_idle_suspended_sandbox": 70,
        "native_contract.session_reattach_wakes_idle_suspended_sandbox": 135,
        "native_contract.session_reconnect_after_idle_suspend": 74,
        "native_contract.tenant_isolation": 3,
        "native_contract.token_scope": 3,
        "native_contract.zero_vcpu_admission": 31,
        "native_recovery.attached_stream_drains_through_data_plane_restart": 17,
        "native_recovery.command_survives_data_plane_restart": 15,
        "native_recovery.inline_fallback_at_one_permit_loses_no_operation": 69,
        "native_recovery.inline_load_survives_leader_failover": 173,
        "native_recovery.inline_wake_survives_api_pod_kill": 101,
        "native_recovery.inline_wake_waits_behind_queued_suspend": 49,
        "native_recovery.queued_create_survives_api_restart": 22,
        "native_recovery.slow_operation_does_not_block_later_work": 21,
        "native_recovery.stale_queued_work_expires_after_controller_outage": 66,
    },
}


# The API surfaces a sharded `--all` run covers, in the order
# scripts/tests/smoke-kind-typed.sh runs them. A shard runs its ECW scenarios
# and then its native ones, so its time is the sum of both passes: they are
# balanced together, not each on its own.
JOINT_SURFACES = ("ecw", "native")


def select_shard(
    scenarios: tuple[Scenario[Any], ...],
    *,
    index: int,
    count: int,
    api_surface: str,
    inventories: Mapping[str, tuple[Scenario[Any], ...]] | None = None,
) -> tuple[Scenario[Any], ...]:
    """Place expensive scenarios first on the lightest shard, then run by ID.

    With `inventories`, the eligible scenarios of every surface in it (the
    current one's being `scenarios`), the partition is of all their (surface,
    scenario) pairs at once, and this returns the current surface's share of
    shard `index`. Every pass of a run computes the same partition, so each
    pair still runs on exactly one shard.
    """

    pools = dict(inventories or {})
    pools[api_surface] = scenarios
    items = [
        (surface, scenario, _DURATION_SECONDS.get(surface, {}).get(scenario.id, 30))
        for surface in sorted(pools)
        for scenario in pools[surface]
    ]
    loads = [0] * count
    chosen: list[Scenario[Any]] = []
    # Equal hints order by surface and ID, and equal loads choose the lower
    # shard index: neither discovery order nor process-local hash
    # randomization may change the assignment.
    for surface, scenario, seconds in sorted(items, key=lambda item: (-item[2], item[0], item[1].id)):
        target = min(range(count), key=loads.__getitem__)
        loads[target] += seconds
        if target == index - 1 and surface == api_surface:
            chosen.append(scenario)
    return tuple(sorted(chosen, key=lambda scenario: scenario.id))
