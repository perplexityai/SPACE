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
        # Local seed run including fixture setup and cleanup: 18.0 / 18.8 s.
        # Refresh these scheduling hints from successful CI runs.
        "ecw.fork_idempotency": 20,
        "ecw.fork_isolation": 20,
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
        "ecw.timeout_update": 55,
        "ecw.warm_pool_environment": 13,
        "ecw.watcher_identity": 5,
        # The next four: otel-mode Kind runs. Both failure scenarios wait out
        # the Kind bring-up's 45 s data-plane readiness timeout.
        "local.btrfs_send_restore_fails_closed_without_parent_source": 60,
        "local.btrfs_send_restore_pulls_parent_template": 33,
        "local.btrfs_send_restore_rejects_corrupted_stream": 61,
        "local.cold_sandbox_eviction_wake_round_trip": 148,
        "local.concurrent_restore_converges_to_one_execution": 60,
        "local.ecw_template_build_from_oci_boots": 40,
        "local.encrypted_resuspend_preserves_cow": 60,
        # Local seed run including durable suspend, cross-node fork, restore
        # and cleanup: 23.5 s.
        "local.fork_suspended_cross_node": 25,
        "local.gentle_drain_leaves_sandbox_resident": 39,
        # Two Kind runs in otel mode with the budget at its 240 s floor: 765 and
        # 760 s of phases, a one-tick runner window, the outage's budget wait,
        # then the contender's watch of about 350 s.
        "local.leader_election_brownout_keeps_one_leader": 765,
        # Two otel-mode Kind runs with the reconcile requested from node-ctl:
        # 17-18 s.
        "local.node_ctl_fence_refuses_and_reconciles": 17,
        # Two Kind runs with fast-pace env rollouts: 38 and 38 s.
        "local.node_group_boundary_schedules": 40,
        # Two Kind runs with fast-pace env rollouts: 63 and 63 s.
        "local.node_selector_extras_render_and_enforce": 65,
        # Three Kind runs measured 92-101 s: two controller rollouts, three
        # suspend/restore cycles, three threshold-bound reaps, one survival window.
        "local.orphaned_restore_pod_gc_reaps_only_superseded": 95,
        # Two otel-mode Kind runs with the census requested from node-ctl:
        # 22 s each.
        "local.pending_backup_census_reports_blocked_upload": 21,
        # Kind bench runs (otel), template deletes concurrent: 60-64 s.
        "local.private_template_build_isolation": 63,
        # Two Kind runs with fast-pace env rollouts: 61 and 57 s. Kind bench
        # runs (otel) before them, no Redis settle: 98-103 s against 106 s.
        "local.rate_limiters_enforce_and_fail_open": 58,
        # Estimate until a CI run refreshes it: sandbox setup, then up to one
        # 300 s GC tick.
        "local.reconcile_queue_gc_prunes_only_gone_nodes": 330,
        # Otel-mode Kind runs: 34-37 s, 19 s of it the suspend with the
        # retried upload.
        "local.restarted_btrfs_send_is_rejected_and_retried": 36,
        "local.retention_claim_finish_forward": 65,
        "local.retention_restore_source_guard": 50,
        # One Kind bench run: 79 s, of which the sandbox-manager restart takes
        # about 10 s and the row-hold window 30 s.
        "local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown": 80,
        # Kind bench runs (otel), probes concurrent: 21-23 s.
        "local.sandbox_network_isolation_holds": 21,
        # Otel-mode Kind runs with the chart's 60 s reaper idle: 80-88 s.
        "local.snapshot_reaper_reclaims_backed_fork_snapshot": 83,
        "local.storage_exchange_scoping": 30,
        "local.suspend_restore_cross_node_from_object_storage": 14,
        "local.suspend_snapshot_rows_are_per_attempt": 21,
        # Otel-mode Kind runs with the Kind profile's 5 s Tempo search cutoff:
        # 23-27 s.
        "local.telemetry_reaches_local_backends": 25,
        # Two otel-mode Kind runs measured 92-94 s with the rendered 1.5 GiB
        # budget and 60 s grace.
        "local.template_cache_lru_eviction_reclaims": 92,
        "local.template_startup_recovery_decision_table": 45,
        # A Kind bench run with the 20 s wait took 116 s, 29 s of it the two
        # node-ctl rollouts (95-96 s with a 10 s wait).
        "local.unanswered_template_deleted_after_wait": 116,
        "network_policy.v1_cidr_allow": 11,
        "network_policy.v1_cidr_deny": 13,
        "network_policy.v1_deny_precedes_allow": 11,
        "network_policy.v1_domain_binding": 5,
        "network_policy.v1_exact_ip_allow": 12,
        "network_policy.v1_exact_ip_deny": 12,
        "network_policy.v1_fastly_shared_edge_bypass": 4,
        "network_policy.v1_platform_guardrail_precedes_allow": 27,
        "network_policy.v1_rules_survive_restore": 33,
        # The DNS transport hints: Kind bench runs (otel) without the settle
        # an accepted reply makes redundant.
        "network_policy.v1_tcp_dns_transport_respects_ip_allow": 5,
        "network_policy.v1_udp_deny_precedes_allow": 14,
        "network_policy.v1_udp_respects_cidr_allow": 13,
        "network_policy.v1_udp_respects_cidr_deny": 14,
        "network_policy.v1_udp_respects_ip_allow": 14,
        "network_policy.v1_udp_respects_ip_deny": 13,
        "network_policy.v1_wildcard_domain_allow": 19,
    },
    "native": {
        "ecw.background_process_control": 5,
        "ecw.command_round_trip": 4,
        "ecw.pause_connect": 13,
        "ecw.sandbox_delete": 3,
        "ecw.sandbox_info": 4,
        "ecw.sandbox_list_pagination": 9,
        # Estimate until a bench run refreshes it: two encrypted suspend and
        # restore cycles, each waiting for its upload and pod retirement.
        "native.encrypted_double_restore_retires_prior_pods": 90,
        # Local Kind runs, with the generator image present or pulled from
        # Docker Hub: 15-18 s, of which SDK generation is 2-5 s.
        "native.m1_generated_sdk_conformance": 20,
        # Estimate until a bench run refreshes it: three live policy updates,
        # each waiting for gateway reconcile and an HTTPS probe.
        "native.network_policy_live_update": 30,
        "native.resident_pause": 5,
        "native_contract.command_validation": 6,
        "native_contract.file_idempotency_conflict": 5,
        "native_contract.idempotency_conflict": 5,
        "native_contract.tenant_isolation": 5,
        "native_contract.token_scope": 5,
        "native_concurrency.command_idempotency": 6,
        "native_concurrency.create_idempotency": 6,
        "native_concurrency.independent_commands": 7,
        "native_concurrency.lifecycle_contention": 9,
        "native_concurrency.slow_reader": 7,
        # Estimate until a bench run refreshes it: one data-plane rollout with
        # the old pod held open by a stream, then its exit.
        "native_recovery.attached_stream_drains_through_data_plane_restart": 60,
        "native_recovery.command_survives_data_plane_restart": 15,
        "native_recovery.queued_create_survives_api_restart": 25,
        # One local Kind run: two controller rollouts around a 30 s outage.
        "native_recovery.stale_queued_work_expires_after_controller_outage": 75,
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
