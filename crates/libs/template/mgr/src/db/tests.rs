use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion,
    AutoLifecycleWakePolicy, AutoLifecycleWakeSource, Digest, ExecutionMetadata, TemplateManifest,
};

use super::*;

#[test]
fn local_policy_generations_advance_across_changes_and_template_recreation() {
    let conn = test_conn();
    let update = |policy: &NetworkPolicyV1| {
        update_template_settings_with_outbox(
            &conn,
            TemplateSettingsUpdate {
                template: "revision-test",
                default_auto_lifecycle: None,
                default_auto_lifecycle_authority: PolicyAuthority::Cluster,
                default_network_policy: Some(policy),
                default_network_policy_generation: 0,
                default_network_policy_authority: PolicyAuthority::Local,
                mark_dirty: false,
            },
        )
        .unwrap();
        let settings = get_template_settings(&conn, "revision-test")
            .unwrap()
            .unwrap();
        let notifications = list_template_network_policy_notifications(&conn, 10).unwrap();
        assert_eq!(notifications.len(), 1);
        assert_eq!(
            notifications[0].generation,
            settings.default_network_policy_generation
        );
        settings.default_network_policy_generation
    };
    let first = update(&NetworkPolicyV1::allow_all());
    assert!(first > 0);
    assert_eq!(update(&NetworkPolicyV1::allow_all()), first);
    let second = update(&NetworkPolicyV1::deny_all());
    assert!(second > first);
    delete_template_local_state(&conn, "revision-test").unwrap();
    assert!(update(&NetworkPolicyV1::allow_all()) > second);
    for i in 0..64 {
        let name = format!("temporary-{i}");
        try_upsert_template_network_policy(
            &conn,
            &name,
            &NetworkPolicyV1::deny_all(),
            0,
            PolicyAuthority::Local,
        )
        .unwrap();
        delete_template_local_state(&conn, &name).unwrap();
    }
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM template_local_policy_sequence",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM template_settings", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn delayed_legacy_policy_ack_preserves_same_generation_new_payload_or_authority() {
    let conn = test_conn();
    // Older producers can enqueue generation-zero notifications even after the
    // owner schema has normalized the committed local policy generation.
    let enqueue_legacy = |policy: &NetworkPolicyV1, authority: PolicyAuthority| {
        conn.execute(
            "INSERT INTO template_network_policy_notifications
                 (template, policy_json, generation, authority, requested_at, updated_at)
             VALUES ('legacy', ?1, 0, ?2, '0', '0')
             ON CONFLICT(template) DO UPDATE SET policy_json = excluded.policy_json,
                 authority = excluded.authority",
            params![
                serde_json::to_string_pretty(policy).unwrap(),
                authority.as_str()
            ],
        )
        .unwrap();
    };
    for (policy, authority) in [
        (NetworkPolicyV1::deny_all(), PolicyAuthority::Local),
        (NetworkPolicyV1::allow_all(), PolicyAuthority::Cluster),
    ] {
        enqueue_legacy(&NetworkPolicyV1::allow_all(), PolicyAuthority::Local);
        let sent = list_template_network_policy_notifications(&conn, 10).unwrap();
        enqueue_legacy(&policy, authority);
        assert_eq!(
            delete_template_network_policy_notifications(&conn, &sent).unwrap(),
            0
        );
        let pending = list_template_network_policy_notifications(&conn, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].policy, policy);
        assert_eq!(pending[0].authority, authority);
        assert_eq!(
            delete_template_network_policy_notifications(&conn, &pending).unwrap(),
            1
        );
    }
}

#[test]
fn local_generation_triggers_cover_old_writers_without_recursion_or_cluster_rewrites() {
    for recursive in [false, true] {
        let conn = test_conn();
        conn.pragma_update(None, "recursive_triggers", recursive)
            .unwrap();
        let generation = || {
            get_template_settings(&conn, "base")
                .unwrap()
                .unwrap()
                .default_network_policy_generation
        };
        try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::allow_all(),
            0,
            PolicyAuthority::Local,
        )
        .unwrap();
        let first = generation();
        assert!(first > 0);
        let enqueue_old_writer = || {
            conn.execute(
                "INSERT INTO template_network_policy_notifications
                     (template, policy_json, generation, authority, requested_at, updated_at)
                 SELECT template, default_network_policy_json, 0, 'local', '0', '0'
                 FROM template_settings WHERE template = 'base'
                 ON CONFLICT(template) DO UPDATE SET policy_json = excluded.policy_json,
                     generation = excluded.generation, authority = excluded.authority",
                [],
            )
            .unwrap();
            let notification = list_template_network_policy_notifications(&conn, 10)
                .unwrap()
                .remove(0);
            assert_eq!(notification.generation, generation());
            notification
        };
        let previous = enqueue_old_writer();
        conn.execute(
            "UPDATE template_settings SET default_network_policy_json = ?1,
                default_network_policy_generation = 0 WHERE template = 'base'",
            [network_policy_json(&NetworkPolicyV1::deny_all()).unwrap()],
        )
        .unwrap();
        assert_eq!(generation(), first + 1);
        let current = enqueue_old_writer();
        assert_ne!(current.policy, previous.policy);
        conn.execute(
            "UPDATE template_network_policy_notifications SET policy_json = ?1, generation = 0",
            [network_policy_json(&previous.policy).unwrap()],
        )
        .unwrap();
        let repaired = list_template_network_policy_notifications(&conn, 10).unwrap();
        assert_eq!(repaired[0].policy, current.policy);
        assert_eq!(repaired[0].generation, current.generation);
        assert_eq!(
            delete_template_network_policy_notifications(&conn, &[previous]).unwrap(),
            0
        );
        upsert_template_settings(&conn, "base", &AutoLifecyclePolicy::disabled()).unwrap();
        assert_eq!(generation(), first + 1);
        delete_template_local_state(&conn, "base").unwrap();
        for requested in [50, 51] {
            try_upsert_template_network_policy(
                &conn,
                "base",
                &NetworkPolicyV1::allow_all(),
                requested,
                PolicyAuthority::Cluster,
            )
            .unwrap();
            assert_eq!(generation(), requested);
        }
        try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::deny_all(),
            0,
            PolicyAuthority::Local,
        )
        .unwrap();
        assert_eq!(generation(), first + 2);
    }
}

fn upsert_test_build_with_execution(conn: &Connection, name: &str, content_hash: &str) {
    let execution = ExecutionMetadata::initial_firecracker(
        Arch::Aarch64,
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: None,
            version: Some(ArtifactVersion::parse("6.10.14").unwrap()),
            digest: Some(
                format!("sha256:{}", "a".repeat(64))
                    .parse::<Digest>()
                    .unwrap(),
            ),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    );
    let mut manifest = TemplateManifest {
        runtime: "fc".to_string(),
        arch: "aarch64".to_string(),
        ..Default::default()
    };
    manifest.set_execution_metadata(execution);
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    upsert_build(
        conn,
        &BuildUpsert {
            name,
            content_hash,
            build_date: "2026-06-12T00:00:00Z",
            manifest_json: &manifest_json,
            runtime: "fc",
            arch: "aarch64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
}

#[test]
fn upsert_build_projects_execution_metadata_and_unspecified_role() {
    for bound in [false, true] {
        let conn = test_conn();
        let execution = ExecutionMetadata::initial_firecracker(
            Arch::Aarch64,
            vec![ArtifactComponentMetadata {
                kind: ArtifactComponentKind::Kernel,
                role: None,
                version: Some(ArtifactVersion::parse("6.10.14").unwrap()),
                digest: Some(
                    format!("sha256:{}", "a".repeat(64))
                        .parse::<Digest>()
                        .unwrap(),
                ),
                format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
            }],
        );
        let execution = if bound {
            execution.with_environment(pplx_space_common_core::types::EnvironmentReference::new(
                Digest::sha256([1; 32]),
            ))
        } else {
            execution
        };
        let mut manifest = TemplateManifest {
            runtime: "fc".to_string(),
            arch: "aarch64".to_string(),
            ..Default::default()
        };
        manifest.set_execution_metadata(execution.clone());
        let manifest_json = serde_json::to_string(&manifest).unwrap();

        upsert_build(
            &conn,
            &BuildUpsert {
                name: "seed",
                content_hash: "sha256:build",
                build_date: "2026-06-12T00:00:00Z",
                manifest_json: &manifest_json,
                runtime: "fc",
                arch: "aarch64",
                artifact_size_bytes: 1,
                state: "ready",
            },
        )
        .unwrap();

        let projected: (String, String, String, String) = conn
            .query_row(
                "SELECT runtime_version, guest_os, state_format_name, state_format_version
             FROM template_builds WHERE name = 'seed' AND content_hash = 'sha256:build'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            projected,
            (
                "1.15.1".into(),
                "linux".into(),
                "firecracker-snapshot".into(),
                "9.0.0".into()
            )
        );
        let role_key: String = conn
            .query_row(
                "SELECT role_key FROM template_build_components
             WHERE name = 'seed' AND content_hash = 'sha256:build' AND kind = 'kernel'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(role_key, "");
        let stored_json: String = conn
            .query_row(
                "SELECT execution_metadata_json FROM template_builds
             WHERE name = 'seed' AND content_hash = 'sha256:build'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<ExecutionMetadata>(&stored_json).unwrap(),
            execution
        );
    }
}

#[test]
fn ensure_template_settings_preserves_unreadable_settings_rows() {
    let conn = test_conn();
    conn.execute(
        "INSERT INTO template_settings
         (template, default_auto_lifecycle_json, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?3)",
        params!["base", "not valid json", "now"],
    )
    .unwrap();

    let error = ensure_template_settings(&conn, "base").unwrap_err();
    assert!(matches!(
        error,
        rusqlite::Error::FromSqlConversionFailure(..)
    ));

    let stored_json: String = conn
        .query_row(
            "SELECT default_auto_lifecycle_json FROM template_settings WHERE template = ?1",
            params!["base"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_json, "not valid json");
}

#[test]
fn delete_build_removes_execution_component_rows() {
    let conn = test_conn();
    conn.pragma_update(None, "foreign_keys", false).unwrap();
    upsert_test_build_with_execution(&conn, "base", "sha256:build");

    delete_build(&conn, "base", "sha256:build").unwrap();

    let component_count: u32 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_build_components
             WHERE name = ?1 AND content_hash = ?2",
            params!["base", "sha256:build"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(component_count, 0);
}

#[test]
fn delete_template_local_state_removes_execution_component_rows() {
    let conn = test_conn();
    conn.pragma_update(None, "foreign_keys", false).unwrap();
    upsert_test_build_with_execution(&conn, "base", "sha256:build-a");
    upsert_test_build_with_execution(&conn, "base", "sha256:build-b");

    delete_template_local_state(&conn, "base").unwrap();

    let component_count: u32 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_build_components WHERE name = ?1",
            params!["base"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(component_count, 0);
}

#[test]
fn delete_template_local_state_removes_persisted_policy() {
    let conn = test_conn();
    upsert_template_settings(&conn, "base", &AutoLifecyclePolicy::disabled()).unwrap();

    assert!(get_template_settings(&conn, "base").unwrap().is_some());

    delete_template_local_state(&conn, "base").unwrap();

    assert!(get_template_settings(&conn, "base").unwrap().is_none());
}

#[test]
fn template_network_policy_defaults_to_deny_all() {
    let conn = test_conn();

    let policy = ensure_template_network_policy(&conn, "base").unwrap();

    assert_eq!(policy, NetworkPolicyV1::deny_all());
}

#[test]
fn template_network_policy_persists_by_template_name() {
    let conn = test_conn();
    try_upsert_template_network_policy(
        &conn,
        "base",
        &NetworkPolicyV1::deny_all(),
        7,
        PolicyAuthority::Cluster,
    )
    .unwrap();

    let policy = ensure_template_network_policy(&conn, "base").unwrap();

    assert_eq!(policy, NetworkPolicyV1::deny_all());
}

#[test]
fn template_network_policy_generations_are_monotonic() {
    let conn = test_conn();

    assert!(
        try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::deny_all(),
            7,
            PolicyAuthority::Cluster
        )
        .unwrap()
    );
    assert!(
        !try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::deny_all(),
            7,
            PolicyAuthority::Cluster
        )
        .unwrap()
    );
    assert!(matches!(
        try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::allow_all(),
            7,
            PolicyAuthority::Cluster
        ),
        Err(TemplateNetworkPolicyUpsertError::GenerationConflict)
    ));
    assert!(
        !try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::allow_all(),
            6,
            PolicyAuthority::Cluster
        )
        .unwrap()
    );
    assert!(
        try_upsert_template_network_policy(
            &conn,
            "base",
            &NetworkPolicyV1::allow_all(),
            8,
            PolicyAuthority::Cluster
        )
        .unwrap()
    );

    let settings = get_template_settings(&conn, "base").unwrap().unwrap();
    assert_eq!(
        settings.default_network_policy,
        NetworkPolicyV1::allow_all()
    );
    assert_eq!(settings.default_network_policy_generation, 8);
}

#[test]
fn idempotent_template_network_policy_update_does_not_enqueue_repair() {
    let conn = test_conn();

    let first = update_template_settings_with_outbox(
        &conn,
        TemplateSettingsUpdate {
            template: "base",
            default_auto_lifecycle: None,
            default_auto_lifecycle_authority: PolicyAuthority::Cluster,
            default_network_policy: Some(&NetworkPolicyV1::deny_all()),
            default_network_policy_generation: 7,
            default_network_policy_authority: PolicyAuthority::Cluster,
            mark_dirty: true,
        },
    )
    .unwrap();
    assert_eq!(first.network_policy, TemplateNetworkPolicyUpdate::Apply);
    assert!(first.queued_dirty_ref);
    assert!(first.queued_sandbox_notification);

    delete_node_dirty_refs(&conn, &list_node_dirty_refs(&conn, 10).unwrap()).unwrap();
    delete_template_network_policy_notifications(
        &conn,
        &list_template_network_policy_notifications(&conn, 10).unwrap(),
    )
    .unwrap();

    let second = update_template_settings_with_outbox(
        &conn,
        TemplateSettingsUpdate {
            template: "base",
            default_auto_lifecycle: None,
            default_auto_lifecycle_authority: PolicyAuthority::Cluster,
            default_network_policy: Some(&NetworkPolicyV1::deny_all()),
            default_network_policy_generation: 7,
            default_network_policy_authority: PolicyAuthority::Cluster,
            mark_dirty: true,
        },
    )
    .unwrap();
    assert_eq!(second.network_policy, TemplateNetworkPolicyUpdate::Skip);
    assert!(!second.queued_dirty_ref);
    assert!(!second.queued_sandbox_notification);
    assert!(list_node_dirty_refs(&conn, 10).unwrap().is_empty());
    assert!(
        list_template_network_policy_notifications(&conn, 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn template_settings_conflict_commits_auto_lifecycle_only() {
    let conn = test_conn();
    try_upsert_template_network_policy(
        &conn,
        "base",
        &NetworkPolicyV1::deny_all(),
        7,
        PolicyAuthority::Cluster,
    )
    .unwrap();
    let auto_lifecycle = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![AutoLifecycleWakeSource::Access],
        },
        ..AutoLifecyclePolicy::disabled()
    };

    let outcome = update_template_settings_with_outbox(
        &conn,
        TemplateSettingsUpdate {
            template: "base",
            default_auto_lifecycle: Some(&auto_lifecycle),
            default_auto_lifecycle_authority: PolicyAuthority::Cluster,
            default_network_policy: Some(&NetworkPolicyV1::allow_all()),
            default_network_policy_generation: 7,
            default_network_policy_authority: PolicyAuthority::Cluster,
            mark_dirty: true,
        },
    )
    .unwrap();

    assert_eq!(
        outcome.network_policy,
        TemplateNetworkPolicyUpdate::Conflict
    );
    assert!(!outcome.queued_dirty_ref);
    assert!(!outcome.queued_sandbox_notification);
    let settings = get_template_settings(&conn, "base").unwrap().unwrap();
    assert_eq!(settings.default_auto_lifecycle, auto_lifecycle);
    assert_eq!(settings.default_network_policy, NetworkPolicyV1::deny_all());
    assert_eq!(settings.default_network_policy_generation, 7);
    assert!(list_node_dirty_refs(&conn, 10).unwrap().is_empty());
    assert!(
        list_template_network_policy_notifications(&conn, 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn cluster_template_auto_lifecycle_does_not_override_local_authority() {
    let conn = test_conn();
    let local_policy = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![AutoLifecycleWakeSource::Access],
        },
        ..AutoLifecyclePolicy::disabled()
    };
    let cluster_policy = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![AutoLifecycleWakeSource::Command],
        },
        ..AutoLifecyclePolicy::disabled()
    };

    let local = update_template_settings_with_outbox(
        &conn,
        TemplateSettingsUpdate {
            template: "base",
            default_auto_lifecycle: Some(&local_policy),
            default_auto_lifecycle_authority: PolicyAuthority::Local,
            default_network_policy: None,
            default_network_policy_generation: 0,
            default_network_policy_authority: PolicyAuthority::Cluster,
            mark_dirty: true,
        },
    )
    .unwrap();
    assert!(local.auto_lifecycle_applied);

    let cluster = update_template_settings_with_outbox(
        &conn,
        TemplateSettingsUpdate {
            template: "base",
            default_auto_lifecycle: Some(&cluster_policy),
            default_auto_lifecycle_authority: PolicyAuthority::Cluster,
            default_network_policy: None,
            default_network_policy_generation: 0,
            default_network_policy_authority: PolicyAuthority::Cluster,
            mark_dirty: true,
        },
    )
    .unwrap();
    assert!(!cluster.auto_lifecycle_applied);

    let settings = get_template_settings(&conn, "base").unwrap().unwrap();
    assert_eq!(settings.default_auto_lifecycle, local_policy);
    assert_eq!(
        settings.default_auto_lifecycle_authority,
        PolicyAuthority::Local
    );
}

#[test]
fn broken_template_network_policy_fails_read() {
    let conn = test_conn();
    conn.execute(
        "INSERT INTO template_settings
             (template, default_auto_lifecycle_json, default_network_policy_json, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)",
        params![
            "base",
            serde_json::to_string(&AutoLifecyclePolicy::disabled()).unwrap(),
            "{\"version\":\"v9\",\"egress\":{\"default_action\":\"deny\"}}",
            now(),
        ],
    )
    .unwrap();

    let error = ensure_template_network_policy(&conn, "base").unwrap_err();

    assert!(matches!(
        error,
        rusqlite::Error::FromSqlConversionFailure(..)
    ));
}

#[test]
fn ensure_template_network_policy_with_seed_preserves_existing_policy() {
    let conn = test_conn();
    try_upsert_template_network_policy(
        &conn,
        "base",
        &NetworkPolicyV1::deny_all(),
        7,
        PolicyAuthority::Cluster,
    )
    .unwrap();

    let (policy, generation, created) = ensure_template_network_policy_with_seed(
        &conn,
        "base",
        Some(&NetworkPolicyV1::allow_all()),
    )
    .unwrap();

    assert_eq!(policy, NetworkPolicyV1::deny_all());
    assert_eq!(generation, 7);
    assert!(!created);
}

#[test]
fn ensure_template_network_policy_with_seed_initializes_missing_policy() {
    let conn = test_conn();

    let (policy, generation, created) = ensure_template_network_policy_with_seed(
        &conn,
        "base",
        Some(&NetworkPolicyV1::allow_all()),
    )
    .unwrap();

    assert_eq!(policy, NetworkPolicyV1::allow_all());
    assert_eq!(generation, 0);
    assert!(created);

    let settings = get_template_settings(&conn, "base").unwrap().unwrap();
    assert_eq!(
        settings.default_network_policy,
        NetworkPolicyV1::allow_all()
    );
}

fn ready_build(name: &str, content_hash: &str, size: u64) -> BuildUpsert<'static> {
    // Static-lifetime strings via `Box::leak` so we can hand a
    // `BuildUpsert<'static>` out of a helper. Acceptable in
    // `#[cfg(test)]`; do not copy into production code.
    let n: &'static str = Box::leak(name.to_string().into_boxed_str());
    let h: &'static str = Box::leak(content_hash.to_string().into_boxed_str());
    let _ = size;
    BuildUpsert {
        name: n,
        content_hash: h,
        build_date: "2026-01-01T00:00:00Z",
        manifest_json: "{}",
        runtime: "fc",
        arch: "aarch64",
        artifact_size_bytes: size,
        state: "ready",
    }
}

/// Regression test: `upsert_build`'s ON CONFLICT path must NOT clobber
/// a `state='deleting'` row back to `'ready'`. That window
/// opens when eviction Phase 1 marks deleting, a concurrent
/// on-demand pull races in (since `resolve_template` now
/// returns NotFound and the `GetTemplate` handler falls
/// through to a fresh pull), and the pull's final upsert
/// would resurrect the doomed row. Eviction's Phase 3 then
/// DELETEs it, and the on-disk state is indeterminate.
///
/// With the WHERE guard, the upsert is a no-op when state is
/// 'deleting'. The caller's `resolve_template` then still
/// sees `state='deleting'` → NotFound, no resurrection.
#[test]
fn upsert_build_refuses_to_clobber_deleting_state() {
    let conn = test_conn();
    upsert_build(&conn, &ready_build("seed", "sha256:aa", 100)).unwrap();

    // Eviction Phase 1: flip to deleting.
    conn.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE name = ?1 AND content_hash = ?2",
        params!["seed", "sha256:aa"],
    )
    .unwrap();

    // Concurrent pull's final upsert tries to flip back to ready.
    upsert_build(&conn, &ready_build("seed", "sha256:aa", 200)).unwrap();

    // State must remain 'deleting' — the pull lost the race.
    let state: String = conn
        .query_row(
            "SELECT state FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "sha256:aa"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "deleting");

    // Size from the deleting-time state is also preserved (the
    // WHERE skipped the whole UPDATE, not just `state`).
    let size: i64 = conn
        .query_row(
            "SELECT artifact_size_bytes FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "sha256:aa"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(size, 100, "ON CONFLICT WHERE skipped UPDATE entirely");
}

/// `is_build_marked_deleting` returns true exactly when there's
/// a row in `state='deleting'`. Used by the pull path's
/// early-abort guard (`do_pull` refuses to write artifacts for
/// a build that's mid-eviction).
#[test]
fn is_build_marked_deleting_matches_state() {
    let conn = test_conn();
    upsert_build(&conn, &ready_build("seed", "sha256:aa", 100)).unwrap();

    assert!(!is_build_marked_deleting(&conn, "seed", "sha256:aa").unwrap());

    conn.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE name = ?1 AND content_hash = ?2",
        params!["seed", "sha256:aa"],
    )
    .unwrap();
    assert!(is_build_marked_deleting(&conn, "seed", "sha256:aa").unwrap());

    // Missing row → false (not an error).
    assert!(!is_build_marked_deleting(&conn, "seed", "sha256:does-not-exist").unwrap());
}

/// `upsert_build` must seed `last_used_at = created_at` so a
/// freshly downloaded build sits at the correct LRU position
/// instead of pretending it was accessed at epoch zero.
#[test]
fn upsert_build_seeds_last_used_at_from_created_at() {
    let conn = test_conn();
    upsert_build(&conn, &ready_build("seed", "sha256:aa", 100)).unwrap();

    let (created, last_used): (String, String) = conn
        .query_row(
            "SELECT created_at, last_used_at FROM template_builds
             WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "sha256:aa"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        created, last_used,
        "fresh build should have last_used_at == created_at"
    );
}

/// `touch_build` advances `last_used_at` past `created_at`. The
/// test plants a synthetic future timestamp via
/// `set_last_used_at_for_test` after a real `touch_build` call so
/// it doesn't depend on wall-clock resolution.
#[test]
fn touch_build_updates_last_used_at() {
    let conn = test_conn();
    upsert_build(&conn, &ready_build("seed", "sha256:aa", 100)).unwrap();

    // Plant a known-old `last_used_at`, then touch.
    set_last_used_at_for_test(&conn, "seed", "sha256:aa", 1_000).unwrap();
    let before: String = conn
        .query_row(
            "SELECT last_used_at FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "sha256:aa"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before, "1000");

    touch_build(&conn, "seed", "sha256:aa").unwrap();

    let after: String = conn
        .query_row(
            "SELECT last_used_at FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "sha256:aa"],
            |row| row.get(0),
        )
        .unwrap();
    let after_secs: u64 = after.parse().unwrap();
    assert!(
        after_secs > 1_000,
        "touch_build must advance last_used_at past the planted value (got {after})"
    );
}

/// Touching a build that was never inserted is a no-op, not an
/// error. The GC loop reads from the same table, so a stale touch
/// on a just-evicted build has nothing to update — and that's
/// fine.
#[test]
fn touch_build_is_no_op_for_missing_row() {
    let conn = test_conn();

    touch_build(&conn, "seed", "sha256:does-not-exist").unwrap();
}

/// `list_eviction_candidates` returns timestamps as parsed u64
/// seconds (the planner doesn't know about the on-disk string
/// encoding) and the artifact_size_bytes round-trips intact.
#[test]
fn list_eviction_candidates_parses_timestamps_and_size() {
    let conn = test_conn();
    upsert_build(&conn, &ready_build("seed", "sha256:aa", 4_096)).unwrap();
    set_last_used_at_for_test(&conn, "seed", "sha256:aa", 12_345).unwrap();

    let rows = list_eviction_candidates(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.name, "seed");
    assert_eq!(row.content_hash, "sha256:aa");
    assert_eq!(row.artifact_size_bytes, 4_096);
    assert!(
        row.created_at_secs > 0,
        "created_at_secs should parse to a non-zero unix timestamp"
    );
    assert_eq!(row.last_used_at_secs, 12_345);
}

/// Only `state = 'ready'` rows are eviction candidates. A
/// partially materialized build (e.g. mid-pull, mid-build) must
/// not be evictable — that would race the pull/build path and
/// delete bytes someone is in the middle of writing.
#[test]
fn list_eviction_candidates_excludes_non_ready_states() {
    let conn = test_conn();
    upsert_build(&conn, &ready_build("seed", "sha256:ready", 100)).unwrap();
    // Direct INSERT for a non-ready state — `upsert_build` always
    // writes `ready` via the `BuildUpsert.state` field today, so
    // we exercise the SQL filter, not the upsert path.
    conn.execute(
        "INSERT INTO template_builds
             (name, content_hash, build_date, manifest_json, runtime, arch,
              artifact_size_bytes, state, created_at, last_used_at)
         VALUES (?1, ?2, '2026-01-01T00:00:00Z', '{}', 'fc', 'aarch64', 200, 'building', ?3, ?3)",
        params!["seed", "sha256:building", now()],
    )
    .unwrap();

    let rows = list_eviction_candidates(&conn).unwrap();
    assert_eq!(rows.len(), 1, "non-ready rows must be filtered out");
    assert_eq!(rows[0].content_hash, "sha256:ready");
}

/// The pinned-keys query UNIONs both pin tables and dedupes a
/// digest pinned by both a tag and a release. The eviction
/// planner takes the result as its protected set; double-counting
/// would only waste CPU but undercounting would orphan a pin.
#[test]
fn list_pinned_build_keys_unions_tag_and_release_pins() {
    let conn = test_conn();
    let ts = now();
    // Tag pin and release pin on the same digest (the common
    // "latest is also one of latest-N" case).
    conn.execute(
        "INSERT INTO template_pinned_tags (name, tag, content_hash, updated_at)
         VALUES ('seed', 'latest', 'sha256:aa', ?1)",
        params![ts],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO template_pinned_releases
             (name, content_hash, release_id, release_created_at, updated_at)
         VALUES ('seed', 'sha256:aa', 'rel-1', '2026-01-01T00:00:00Z', ?1)",
        params![ts],
    )
    .unwrap();
    // Distinct release pin for a different digest.
    conn.execute(
        "INSERT INTO template_pinned_releases
             (name, content_hash, release_id, release_created_at, updated_at)
         VALUES ('seed', 'sha256:bb', 'rel-2', '2026-02-01T00:00:00Z', ?1)",
        params![ts],
    )
    .unwrap();
    // Tag-only pin on a third digest, different template.
    conn.execute(
        "INSERT INTO template_pinned_tags (name, tag, content_hash, updated_at)
         VALUES ('code-runtime', 'latest', 'sha256:cc', ?1)",
        params![ts],
    )
    .unwrap();

    let mut keys = list_pinned_build_keys(&conn).unwrap();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            ("code-runtime".to_string(), "sha256:cc".to_string()),
            ("seed".to_string(), "sha256:aa".to_string()),
            ("seed".to_string(), "sha256:bb".to_string()),
        ],
    );
}

#[test]
fn list_template_snapshot_policies_only_returns_explicit_policy() {
    let conn = test_conn();
    upsert_template_settings(&conn, "base", &AutoLifecyclePolicy::disabled()).unwrap();

    assert!(list_template_snapshot_policies(&conn).unwrap().is_empty());

    update_template_snapshot_policy(&conn, "base", false, 2, 1024).unwrap();

    let rows = list_template_snapshot_policies(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].template, "base");
    assert!(!rows[0].ensure_base_snapshot);
    assert!(rows[0].base_snapshot_policy_set_at.is_some());
}

/// The cache-partition schema of the retired migration 20261026220935.
const PARTITIONED_SCHEMA_SQL: &str = include_str!(
    "../../../../../../db/template-manager/history/20261026220935_template_cache_partitions.up.sql"
);

/// A migrator that bundles the partition migration can apply it under this
/// release's serving pod, so each upsert inserts and updates on either schema.
#[test]
fn upserts_insert_and_update_on_the_bundled_and_partitioned_schemas() {
    for partitioned in [false, true] {
        let conn = test_conn();
        if partitioned {
            conn.execute_batch(PARTITIONED_SCHEMA_SQL).unwrap();
        }
        let build = |conn: &Connection| -> (u64, i64) {
            let size = get_build(conn, "seed", "sha256:build")
                .unwrap()
                .unwrap()
                .artifact_size_bytes;
            let components = conn
                .query_row(
                    "SELECT COUNT(*) FROM template_build_components WHERE name = 'seed'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            (size, components)
        };

        upsert_test_build_with_execution(&conn, "seed", "sha256:build");
        assert_eq!(build(&conn), (1, 1), "partitioned={partitioned}");
        upsert_build(&conn, &ready_build("seed", "sha256:build", 200)).unwrap();
        assert_eq!(build(&conn), (200, 0), "partitioned={partitioned}");
        upsert_test_build_with_execution(&conn, "seed", "sha256:build");
        assert_eq!(build(&conn), (1, 1), "partitioned={partitioned}");
        // The eviction guard still applies to the conflicting row.
        conn.execute(
            "UPDATE template_builds SET state = 'deleting' WHERE name = 'seed'",
            [],
        )
        .unwrap();
        upsert_build(&conn, &ready_build("seed", "sha256:build", 300)).unwrap();
        assert_eq!(build(&conn), (1, 1), "partitioned={partitioned}");
        conn.execute(
            "UPDATE template_builds SET state = 'ready' WHERE name = 'seed'",
            [],
        )
        .unwrap();

        upsert_snapshot(&conn, "seed", "sha256:build", 2, 2048, "sha256:first").unwrap();
        upsert_snapshot(&conn, "seed", "sha256:build", 2, 2048, "sha256:second").unwrap();
        assert_eq!(
            list_snapshots(&conn, "seed", "sha256:build")
                .unwrap()
                .into_iter()
                .map(|snapshot| snapshot.snapshot_hash)
                .collect::<Vec<_>>(),
            ["sha256:second"],
            "partitioned={partitioned}"
        );

        let store = crate::pinned_digests::PinnedDigestsStore::new(std::sync::Arc::new(
            std::sync::Mutex::new(conn),
        ));
        store.upsert_tag("seed", "latest", "sha256:first").unwrap();
        store.upsert_tag("seed", "latest", "sha256:build").unwrap();
        assert_eq!(
            store.list_pinned_hashes_for_template("seed").unwrap(),
            ["sha256:build"],
            "partitioned={partitioned}"
        );

        if partitioned {
            let conn = store.conn();
            let conn = conn.lock().unwrap();
            let partitions: i64 = conn
                .query_row(
                    "SELECT (SELECT COUNT(*) FROM template_builds WHERE cache_partition != '')
                          + (SELECT COUNT(*) FROM template_build_components WHERE cache_partition != '')
                          + (SELECT COUNT(*) FROM template_snapshots WHERE cache_partition != '')
                          + (SELECT COUNT(*) FROM template_pinned_tags WHERE cache_partition != '')",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(partitions, 0, "every write lands in the legacy partition");
        }
    }
}

/// A pool refill names the template's latest ready Firecracker build, so its
/// runtime and the rootfs sandbox-manager resolves come from one row, and
/// records the use. A template with only other runtimes' builds gets none.
#[test]
fn pool_refills_name_the_latest_firecracker_build_and_touch_it() {
    let conn = test_conn();
    let cloud_hypervisor = pplx_space_common_core::types::RuntimeKind::CloudHypervisor.alias();
    let upsert = |content_hash: &'static str, build_date, runtime| {
        upsert_build(
            &conn,
            &BuildUpsert {
                build_date,
                runtime,
                ..ready_build("seed", content_hash, 1)
            },
        )
        .unwrap();
        set_last_used_at_for_test(&conn, "seed", content_hash, 0).unwrap();
    };
    assert!(
        crate::pool_manager::pool_refill_build(&conn, "seed")
            .unwrap()
            .is_none()
    );
    upsert("sha256:ch", "2026-03-01T00:00:00Z", cloud_hypervisor);
    assert!(matches!(
        crate::pool_manager::pool_refill_build(&conn, "seed"),
        Err(crate::pool_manager::PoolError::NoPoolBuild)
    ));
    upsert("sha256:old", "2026-01-01T00:00:00Z", "fc");
    upsert("sha256:new", "2026-02-01T00:00:00Z", "fc");

    let build = crate::pool_manager::pool_refill_build(&conn, "seed")
        .unwrap()
        .unwrap();

    assert_eq!(build.content_hash, "sha256:new");
    let last_used = |content_hash: &str| -> String {
        conn.query_row(
            "SELECT last_used_at FROM template_builds WHERE content_hash = ?1",
            [content_hash],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_ne!(last_used("sha256:new"), "0");
    assert_eq!(last_used("sha256:old"), "0");
    assert_eq!(last_used("sha256:ch"), "0");
}

/// Builds with the same date resolve to the larger digest, and a row that
/// can't be decoded is an error, not a missing build.
#[test]
fn latest_build_breaks_date_ties_by_digest_and_reports_bad_rows() {
    let conn = test_conn();
    for content_hash in ["sha256:aa", "sha256:bb"] {
        upsert_build(&conn, &ready_build("seed", content_hash, 1)).unwrap();
    }
    assert_eq!(
        get_latest_build(&conn, "seed")
            .unwrap()
            .unwrap()
            .content_hash,
        "sha256:bb"
    );

    conn.execute(
        "UPDATE template_builds SET artifact_size_bytes = -1 WHERE content_hash = 'sha256:bb'",
        [],
    )
    .unwrap();
    assert!(get_latest_build(&conn, "seed").is_err());
}
