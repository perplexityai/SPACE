"""Deterministic duration-balanced partitions of an already eligible inventory."""

from typing import Any

from space_smoke.registry import Scenario

# Rounded Kind wall-clock seconds, including scenario setup and cleanup. These
# are scheduling hints, not timeouts or an allowlist. New/renamed scenarios get
# a conservative default and always participate. Refresh from successful runs'
# RUN/END timestamps; keep API surfaces separate because their costs differ.
_DURATION_SECONDS = {
    "ecw": {
        "ecw.access_idle_reconnect_preserves_workspace": 23,
        "ecw.authenticated_access_with_public_traffic_disabled": 6,
        "ecw.background_process_control": 4,
        "ecw.binary_file_round_trip": 5,
        "ecw.cold_create_environment": 8,
        "ecw.command_round_trip": 4,
        "ecw.domain_access_survives_default_deny": 5,
        "ecw.filesystem_mutations": 5,
        "ecw.filesystem_ownership": 5,
        "ecw.filesystem_private_boundary": 5,
        "ecw.filesystem_read_identity": 4,
        "ecw.filesystem_round_trip": 4,
        "ecw.filesystem_watcher": 4,
        "ecw.filesystem_write_identity": 5,
        "ecw.immediate_egress": 4,
        "ecw.internet_access_allowed": 5,
        "ecw.internet_access_denied": 28,
        "ecw.large_stdout_integrity": 5,
        "ecw.loopback_route_stays_open": 17,
        "ecw.pause_connect": 12,
        "ecw.pause_connect_race": 84,
        "ecw.pause_timeout_requires_connect": 16,
        "ecw.process_identity": 5,
        "ecw.process_reattach": 14,
        "ecw.public_port_to_all_interfaces": 5,
        "ecw.public_port_to_loopback": 5,
        "ecw.refresh_keepalive": 41,
        "ecw.sandbox_delete": 5,
        "ecw.sandbox_info": 4,
        "ecw.sandbox_list_pagination": 10,
        "ecw.timeout_rearm": 41,
        "ecw.timeout_update": 50,
        "ecw.warm_pool_environment": 13,
        "ecw.watcher_identity": 5,
        "local.btrfs_send_restore_fails_closed_without_parent_source": 135,
        "local.btrfs_send_restore_pulls_parent_template": 20,
        "local.btrfs_send_restore_rejects_corrupted_stream": 133,
        "local.cold_sandbox_eviction_wake_round_trip": 137,
        "local.concurrent_restore_converges_to_one_execution": 87,
        "local.encrypted_resuspend_preserves_cow": 60,
        "local.gentle_drain_leaves_sandbox_resident": 39,
        "local.node_ctl_fence_refuses_and_reconciles": 18,
        "local.node_group_boundary_schedules": 42,
        "local.node_selector_extras_render_and_enforce": 65,
        # Estimate until a bench run refreshes it: two controller rollouts, three
        # suspend/restore cycles, three threshold-bound reaps, one survival window.
        "local.orphaned_restore_pod_gc_reaps_only_superseded": 300,
        "local.pending_backup_census_reports_blocked_upload": 216,
        "local.private_template_build_isolation": 87,
        # Two episodes share passes: measured 255 s including initial
        # quarantine discovery and the real 60 s / 120 s retry gaps.
        "local.quarantine_recovery_backs_off": 260,
        "local.rate_limiters_enforce_and_fail_open": 60,
        "local.retention_claim_finish_forward": 80,
        "local.retention_restore_source_guard": 107,
        "local.snapshot_reaper_reclaims_backed_fork_snapshot": 147,
        "local.suspend_restore_cross_node_from_object_storage": 14,
        "local.suspend_snapshot_rows_are_per_attempt": 21,
        # Measured 37-38 s; Tempo's 30 s search cutoff is most of it.
        "local.telemetry_reaches_local_backends": 40,
        # Three Kind runs measured 101-120 s with the rendered 1.5 GiB budget.
        "local.template_cache_lru_eviction_reclaims": 120,
        "local.template_startup_recovery_decision_table": 45,
        "network_policy.v1_cidr_allow": 11,
        "network_policy.v1_cidr_deny": 13,
        "network_policy.v1_deny_precedes_allow": 11,
        "network_policy.v1_domain_binding": 5,
        "network_policy.v1_exact_ip_allow": 12,
        "network_policy.v1_exact_ip_deny": 12,
        "network_policy.v1_fastly_shared_edge_bypass": 4,
        "network_policy.v1_platform_guardrail_precedes_allow": 27,
        "network_policy.v1_rules_survive_restore": 33,
        "network_policy.v1_tcp_dns_transport_respects_ip_allow": 10,
        "network_policy.v1_udp_deny_precedes_allow": 18,
        "network_policy.v1_udp_respects_cidr_allow": 19,
        "network_policy.v1_udp_respects_cidr_deny": 18,
        "network_policy.v1_udp_respects_ip_allow": 19,
        "network_policy.v1_udp_respects_ip_deny": 18,
        "network_policy.v1_wildcard_domain_allow": 19,
    },
    "native": {
        "ecw.background_process_control": 5,
        "ecw.command_round_trip": 4,
        "ecw.pause_connect": 13,
        "ecw.sandbox_delete": 3,
        "ecw.sandbox_info": 4,
        "ecw.sandbox_list_pagination": 9,
        "native.resident_pause": 5,
        "native_contract.command_validation": 6,
        "native_contract.idempotency_conflict": 5,
        "native_contract.tenant_isolation": 5,
        "native_contract.token_scope": 5,
        "native_concurrency.command_idempotency": 6,
        "native_concurrency.create_idempotency": 6,
        "native_concurrency.independent_commands": 7,
        "native_concurrency.lifecycle_contention": 9,
        "native_concurrency.slow_reader": 7,
        "native_recovery.command_survives_data_plane_restart": 15,
        "native_recovery.queued_create_survives_api_restart": 25,
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
