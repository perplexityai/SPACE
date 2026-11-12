"""Deterministic duration-balanced partitions of an already eligible inventory."""

from collections.abc import Mapping
from typing import Any

from space_smoke.registry import Scenario

# Rounded Kind wall-clock seconds, including scenario setup and cleanup. These
# are scheduling hints, not timeouts or an allowlist. New/renamed scenarios get
# a conservative default and always participate. Refresh from successful runs'
# RUN/END timestamps; keep API surfaces separate because their costs differ.
#
# Unchanged cases use medians from successful Pull CI runs 42857061959,
# 42864432253 and 42875733181 (2026-11-10, otel, 16-core runners). Exclude
# the last run's startup-profile shard: its unrelated scenarios did not use
# standing controller settings. Refusal uses the first two runs, with its
# ordinary setup/restoration. GC and teardown use 42872494328/42875733181,
# after their behavior changed; merged inline has one matching fixed-poll
# observation from the latter run. Do not mix old holds/separate scenarios
# into those estimates. These are scheduling hints, not latency guarantees.
# Absent diagnostic/browser cases retain their earlier documented hints.
_DURATION_SECONDS = {
    "ecw": {
        "ecw.access_idle_reconnect_preserves_workspace": 21,
        "ecw.background_process_control": 4,
        "ecw.cold_create_environment": 6,
        "ecw.command_round_trip": 4,
        "ecw.domain_access_survives_default_deny": 5,
        "ecw.filesystem_mutations": 4,
        "ecw.filesystem_ownership": 4,
        "ecw.filesystem_private_boundary": 4,
        "ecw.filesystem_read_identity": 4,
        "ecw.filesystem_watcher": 4,
        "ecw.fork_idempotency": 8,
        "ecw.fork_isolation": 12,
        "ecw.guest_mptcp_segments_dropped": 6,
        "ecw.guest_vlan_frames_dropped": 9,
        "ecw.immediate_egress": 4,
        "ecw.internet_access_allowed": 5,
        "ecw.internet_access_denied": 10,
        "ecw.large_multipart_upload_round_trip": 5,
        "ecw.large_stdout_integrity": 4,
        "ecw.loopback_route_stays_open": 13,
        "ecw.pause_connect": 7,
        "ecw.pause_connect_race": 15,
        "ecw.pause_timeout_requires_connect": 14,
        "ecw.process_identity": 3,
        "ecw.process_reattach": 4,
        "ecw.public_port_to_all_interfaces": 5,
        "ecw.refresh_keepalive": 43,
        "ecw.sandbox_list_pagination": 8,
        "ecw.timeout_update": 54,
        "ecw.warm_pool_environment": 14,
        "ecw.watcher_identity": 4,
        "local.btrfs_send_restore_fails_closed_without_parent_source": 55,
        "local.btrfs_send_restore_pulls_parent_template": 42,
        "local.btrfs_send_restore_rejects_corrupted_stream": 55,
        "local.concurrent_restore_converges_to_one_execution": 58,
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
        "local.deferred_btrfs_send_capture_restores_cross_node": 11,
        "local.ecw_template_build_from_oci_boots": 35,
        "local.encrypted_disk_fork": 39,
        "local.encrypted_live_disk_fork": 34,
        "local.encrypted_resuspend_preserves_cow": 33,
        "local.forced_drain_evacuates_sandbox": 18,
        "local.garbage_collectors_reclaim_independent_resources": 138,
        "local.fork_suspended_cross_node": 14,
        "local.gentle_drain_leaves_sandbox_resident": 29,
        "local.leader_election_brownout_keeps_one_leader": 205,
        "local.leader_election_refusal_does_not_restart": 162,
        "local.node_ctl_fence_refuses_and_reconciles": 10,
        "local.node_group_boundary_schedules": 34,
        "local.node_selector_extras_render_and_enforce": 63,
        "local.orphaned_restore_pod_gc_reaps_only_superseded": 118,
        "local.pending_backup_census_reports_blocked_upload": 22,
        "local.private_template_build_isolation": 50,
        "local.rate_limiters_enforce_and_fail_open": 49,
        "local.reconcile_queue_gc_prunes_only_gone_nodes": 12,
        "local.restarted_btrfs_send_is_rejected_and_retried": 29,
        "local.retention_claim_finish_forward": 71,
        "local.retention_restore_source_guard": 43,
        "local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown": 53,
        "local.sandbox_network_isolation_holds": 20,
        "local.storage_exchange_scoping": 26,
        "local.suspend_snapshot_rows_are_per_attempt": 15,
        "local.telemetry_reaches_local_backends": 16,
        "local.template_startup_recovery_decision_table": 18,
        "local.unanswered_template_deleted_after_wait": 104,
        "network_policy.v1_cidr_allow": 6,
        "network_policy.v1_cidr_deny": 6,
        "network_policy.v1_deny_precedes_allow": 7,
        "network_policy.v1_domain_binding": 5,
        "network_policy.v1_exact_ip_allow": 7,
        "network_policy.v1_exact_ip_deny": 6,
        "network_policy.v1_fastly_shared_edge_bypass": 4,
        "network_policy.v1_rules_survive_restore": 10,
        "network_policy.v1_tcp_dns_transport_respects_ip_allow": 6,
        "network_policy.v1_udp_deny_precedes_allow": 8,
        "network_policy.v1_udp_respects_cidr_allow": 8,
        "network_policy.v1_udp_respects_cidr_deny": 8,
        "network_policy.v1_udp_respects_ip_allow": 8,
        "network_policy.v1_udp_respects_ip_deny": 8,
        "network_policy.v1_wildcard_domain_allow": 8,
    },
    "native": {
        "ecw.background_process_control": 5,
        "ecw.command_round_trip": 3,
        "ecw.pause_connect": 8,
        "ecw.sandbox_list_pagination": 10,
        "native.encrypted_double_restore_retires_prior_pods": 35,
        "native.forced_drain_re_drains_re_placed_stopped_sandbox": 53,
        "native.forced_drain_re_drains_restarted_re_placed_stopped_sandbox": 69,
        "native.m1_generated_sdk_conformance": 19,
        "native.network_policy_live_update": 7,
        "native.resident_pause": 5,
        "native.stopped_restart_preserves_rootfs": 68,
        "native_concurrency.command_idempotency": 4,
        "native_concurrency.create_idempotency": 4,
        "native_concurrency.independent_commands": 4,
        "native_concurrency.lifecycle_contention": 7,
        "native_concurrency.slow_reader": 5,
        "native_contract.command_attach_after_pause": 5,
        "native_contract.command_on_sandbox_not_running": 6,
        "native_contract.command_validation": 3,
        "native_contract.command_wakes_idle_suspended_sandbox": 25,
        "native_contract.completed_delete_replay": 3,
        "native_contract.file_idempotency_across_origins": 3,
        "native_contract.file_idempotency_conflict": 4,
        "native_contract.file_op_wakes_idle_suspended_sandbox": 26,
        "native_contract.file_operations_match_on_the_data_plane": 4,
        "native_contract.file_upload_admission_429": 86,
        "native_contract.file_write_size_boundary_on_the_data_plane": 5,
        "native_contract.permanent_command_error": 4,
        "native_contract.process_session_wakes_idle_suspended_sandbox": 25,
        "native_contract.session_reattach_wakes_idle_suspended_sandbox": 46,
        "native_contract.session_reconnect_after_idle_suspend": 28,
        "native_contract.tenant_isolation": 4,
        "native_contract.token_scope": 3,
        "native_recovery.attached_stream_drains_through_data_plane_restart": 17,
        "native_recovery.command_survives_data_plane_restart": 16,
        "native_recovery.inline_fallback_at_one_permit_loses_no_operation": 42,
        "native_recovery.inline_load_survives_leader_failover": 62,
        "native_recovery.inline_wake_survives_api_pod_kill": 98,
        "native_recovery.queued_create_survives_api_restart": 18,
        "native_recovery.slow_operation_does_not_block_later_work": 21,
        "native_recovery.stale_queued_work_expires_after_controller_outage": 51,
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
