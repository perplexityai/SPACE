"""Deterministic duration-balanced partitions of an already eligible inventory."""

from collections.abc import Mapping
from typing import Any

from space_smoke.registry import Scenario, ScenarioRegistry

# Rounded Kind wall-clock seconds, including scenario setup and cleanup. These
# are scheduling hints, not timeouts or an allowlist. New/renamed scenarios get
# a conservative default and always participate. Refresh from successful runs'
# RUN/END timestamps; keep API surfaces separate because their costs differ.
#
# Unchanged cases use medians from successful Pull CI runs 42857061959,
# 42864432253 and 42875733181 (2026-11-10, otel, 16-core runners). Exclude
# the last run's startup-profile shard: its unrelated scenarios did not use
# standing controller settings. GC and teardown use 42872494328/42875733181,
# after their behavior changed; merged inline has one matching fixed-poll
# observation from the latter run. Do not mix old holds/separate scenarios
# into those estimates. These are scheduling hints, not latency guarantees.
# Absent diagnostic/browser cases retain their earlier documented hints.
# The interrupted suspend's only-copy discard: estimated from three local Kind
# runs at a 45 s window (69 s), plus about 10 s for the 60 s window (15 s
# more window, less the slack the post-window wait saves by timing from the
# quarantine). Replace it with the Main CI median once one exists.
# The leader-election brownout uses the median of three local Kind runs on a
# 32-CPU host (otel, 2026-11-19), to be replaced by the CI median once CI has
# run it.
# The warm-pool policy's follow of a new release uses the median of three
# local Kind runs on a 32-CPU host with OTel telemetry (2026-11-28), to be
# replaced by the CI median once CI has run it.
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
        "ecw.warm_pool_shaped_claim": 14,
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
        "local.interrupted_suspend_keeps_only_copy_until_discard": 79,
        "local.leader_election_brownout_keeps_one_leader": 133,
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
        "local.telemetry_reaches_local_backends": 75,
        "local.template_startup_recovery_decision_table": 18,
        "local.unanswered_template_deleted_after_wait": 104,
        "local.warm_pool_policy_follows_new_release": 56,
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
        # The drain set, which runs by name and splits across benches with
        # `space_smoke list --prefix native.v2_ --shard I/N`, but for
        # native.v2_drain_evacuates_every_resident, which is routine too: the
        # median of three local Kind runs each (2026-11-25,
        # `smoke-kind-parallel.sh --repeat 3` on four benches of a 32-core
        # host, Kind's drain v2 values), setup and cleanup included. Replace
        # with CI medians once CI runs them.
        "native.v2_drain_evacuates_every_resident": 75,
        "native.v2_drain_holds_running_until_its_deadline": 122,
        "native.v2_drain_keeps_a_live_node_with_a_stale_instance": 289,
        "native.v2_drain_meets_user_calls": 145,
        "native.v2_drain_moves_ten_residents": 110,
        "native.v2_drain_outlived_by_a_dead_worker": 469,
        "native.v2_drain_paces_a_full_worker": 290,
        "native.v2_drain_preview_changes_nothing": 42,
        "native.v2_drain_refused_at_the_cap": 70,
        "native.v2_drain_refuses_stopped_sandboxes": 175,
        "native.v2_drain_replaced_while_shutting_down": 169,
        "native.v2_drain_survives_controller_restarts": 266,
        "native.v2_drain_survives_sandbox_manager_restarts": 199,
        "native.v2_drain_terminates_its_worker": 282,
        "native.v2_drain_waits_for_a_lingering_node": 267,
        "native.v2_drain_waits_for_an_unreachable_node": 205,
        "native.v2_drain_waits_out_blocked_uploads": 95,
        "native.v2_refuses_v1_drains": 6,
        "native.v2_undrain_mid_suspend": 79,
        "native.v2_undrain_unseals_the_worker": 114,
        "native.v2_undrain_with_the_controller_down": 47,
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
        # Local Kind runs (about 9 s); replace with the Main CI median once
        # one exists.
        "native_contract.file_upload_admission_429": 10,
        "native_contract.file_write_size_boundary_on_the_data_plane": 5,
        "native_contract.permanent_command_error": 4,
        "native_contract.process_session_wakes_idle_suspended_sandbox": 25,
        "native_contract.session_reattach_wakes_idle_suspended_sandbox": 46,
        "native_contract.session_reconnect_after_idle_suspend": 28,
        "native_contract.tenant_isolation": 4,
        "native_contract.token_scope": 3,
        "native_recovery.attached_stream_drains_through_data_plane_restart": 17,
        "native_recovery.command_survives_data_plane_restart": 16,
        "native_recovery.inline_load_survives_leader_failover": 62,
        "native_recovery.inline_wake_survives_api_pod_kill": 98,
        "native_recovery.slow_operation_does_not_block_later_work": 21,
    },
}


# The API surfaces a sharded `--all` run covers, in the order
# scripts/tests/smoke-kind-typed.sh runs them. A shard runs its ECW scenarios
# and then its native ones, so its time is the sum of both passes: they are
# balanced together, not each on its own.
JOINT_SURFACES = ("ecw", "native")


def joint_inventories(
    registry: ScenarioRegistry,
    *,
    template: str | None,
    tiers: tuple[str, ...],
    include_scenarios: tuple[str, ...],
    backend: str,
    browser_selection: str,
    run_scenarios: tuple[str, ...] = (),
) -> dict[str, tuple[Scenario[Any], ...]]:
    """Each joint surface's eligible inventory, as a sharded `--all` pass filters it.

    The tiers and extra scenarios, or with `run_scenarios` exactly those, in
    that order and as often as listed, whatever their tier but not whatever
    their templates; then the surface, the backend and browser withholding; mode-dependent skips come after the
    partition. Every pass of a sharded run partitions these, and so does
    anything that checks the run's coverage
    (scripts/tests/lib/kind_parallel_report.py).
    """

    selected = (
        tuple(
            scenario
            for scenario in map(registry.get, run_scenarios)
            if scenario.applies_to(template)
        )
        if run_scenarios
        else registry.select(
            category=None, template=template, tiers=tiers, include_scenarios=include_scenarios,
        )
    )
    return {
        surface: tuple(
            scenario
            for scenario in selected
            if surface in scenario.api_surfaces
            and backend in scenario.backends
            and (browser_selection != "withhold" or scenario.browser is None)
        )
        for surface in JOINT_SURFACES
    }


def named_withheld(
    registry: ScenarioRegistry,
    run_scenarios: tuple[str, ...],
    *,
    template: str | None,
    backend: str,
    browser_selection: str,
) -> list[tuple[str, str]]:
    """Each occurrence in `run_scenarios` that no joint surface's inventory
    holds (`joint_inventories`), with why: an unknown ID, a template it
    doesn't apply to, another backend, a surface no sharded run covers, or
    browser withholding. A named run that withholds any did not run what it
    named. Mode-dependent skips are a bench's, known only once it is up."""

    withheld: list[tuple[str, str]] = []
    for scenario_id in run_scenarios:
        try:
            scenario = registry.get(scenario_id)
        except KeyError:
            withheld.append((scenario_id, "no such scenario"))
            continue
        if not scenario.applies_to(template):
            reason = f"applies to template(s) {', '.join(sorted(scenario.templates or ()))}, not {template}"
        elif backend not in scenario.backends:
            reason = f"runs only on backend(s) {', '.join(sorted(scenario.backends))}"
        elif not set(scenario.api_surfaces) & set(JOINT_SURFACES):
            reason = f"runs only on API surface(s) {', '.join(sorted(scenario.api_surfaces))}"
        elif browser_selection == "withhold" and scenario.browser is not None:
            reason = "needs a browser, which this run withholds"
        else:
            continue
        withheld.append((scenario_id, reason))
    return withheld


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

    chosen = shard_positions(
        scenarios, index=index, count=count, api_surface=api_surface, inventories=inventories
    )
    return tuple(sorted((scenarios[position] for position in chosen), key=lambda scenario: scenario.id))


def shard_positions(
    scenarios: tuple[Scenario[Any], ...],
    *,
    index: int,
    count: int,
    api_surface: str,
    inventories: Mapping[str, tuple[Scenario[Any], ...]] | None = None,
) -> tuple[int, ...]:
    """`select_shard`'s share as positions in `scenarios`, in position order.

    A named run lists a scenario once per occurrence, so its share keeps each
    occurrence apart and in the order named. Occurrences of one scenario
    have equal hints, IDs and surfaces; their positions break that tie, which
    changes which occurrence lands where but not how many land on each shard.
    """

    pools = dict(inventories or {})
    pools[api_surface] = scenarios
    items = [
        (surface, position, scenario, _DURATION_SECONDS.get(surface, {}).get(scenario.id, 30))
        for surface in sorted(pools)
        for position, scenario in enumerate(pools[surface])
    ]
    loads = [0] * count
    chosen: list[int] = []
    # Equal hints order by surface and ID, and equal loads choose the lower
    # shard index: neither discovery order nor process-local hash
    # randomization may change the assignment.
    for surface, position, scenario, seconds in sorted(
        items, key=lambda item: (-item[3], item[0], item[2].id, item[1])
    ):
        target = min(range(count), key=loads.__getitem__)
        loads[target] += seconds
        if target == index - 1 and surface == api_surface:
            chosen.append(position)
    return tuple(sorted(chosen))
