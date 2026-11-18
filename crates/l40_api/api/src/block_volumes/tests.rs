use super::*;
use pplx_space_test_env::TestEnv;

#[tokio::test]
async fn block_volume_reference_geometry_uses_only_the_owners_live_resource()
-> pplx_space_test_env::TestResult {
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db};
    use pplx_space_sandbox_store::block_volume::{VolumePhase, resource};
    let db = pg_test_db().await?;
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let volume = uuid::Uuid::now_v7();
    let mut tx = db.pool.begin().await?;
    resource::reserve(
        &mut tx,
        resource::NewResource {
            id: volume,
            org_id: org,
            managed_by: "test",
            availability_zone: "test-region-1a",
            resource_limit: 1,
        },
    )
    .await?;
    tx.commit().await?;
    // Deliberately stale translated geometry must not override the resource.
    let mount: BlockVolumeMount = serde_json::from_value(serde_json::json!({
        "type":"ebs", "size_gib":256, "permissions":"read_only", "mount":"/data"
    }))?;
    let mounts = [mount.clone()];
    let resolved = resolve_reference_mount(&db.pool, org, Some(volume), &mounts)
        .await?
        .unwrap();
    assert_eq!(u32::from(resolved.size_gib), 128);
    assert_eq!(resolved.mount, mount.mount);
    assert_eq!(resolved.permissions, mount.permissions);
    for (caller, id) in [(OrgId::new(), volume), (org, uuid::Uuid::now_v7())] {
        assert_eq!(
            resolve_reference_mount(&db.pool, caller, Some(id), &mounts)
                .await
                .unwrap_err()
                .status(),
            axum::http::StatusCode::NOT_FOUND
        );
    }
    assert!(
        resolve_reference_mount(&db.pool, org, None, &mounts)
            .await?
            .is_none()
    );
    let row = resource::get(&db.pool, org, volume).await?.unwrap();
    assert!(
        resource::observe(
            &db.pool,
            &row,
            VolumePhase::Available,
            Some("vol-00000000000000001"),
            None,
        )
        .await?
    );
    let mut tx = db.pool.begin().await?;
    assert!(resource::request_delete(&mut tx, org, volume, uuid::Uuid::now_v7()).await?);
    tx.commit().await?;
    assert_eq!(
        resolve_reference_mount(&db.pool, org, Some(volume), &mounts)
            .await
            .unwrap_err()
            .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    Ok(())
}

#[test]
fn management_requires_explicit_opt_in_and_an_allowlisted_owner() {
    let owner = OrgId::new();
    let mut env = TestEnv::default();
    assert!(management_organizations(&env).is_empty());
    for enabled in [false, true] {
        env.set(
            POLICY_ENV,
            serde_json::json!({
                "organization_ids":[owner],
                "availability_zone":"test-region-1a",
                "volume_management":enabled
            })
            .to_string(),
        );
        assert_eq!(management_organizations(&env).contains(&owner), enabled);
        assert!(!management_organizations(&env).contains(&OrgId::new()));
    }
    env.set(POLICY_ENV, "malformed");
    assert!(management_organizations(&env).is_empty());
}

#[test]
fn block_volume_admission_is_opt_in_and_owner_scoped() {
    let owner = OrgId::new();
    let other = OrgId::new();
    let mounts: Vec<BlockVolumeMount> = serde_json::from_value(serde_json::json!([{
        "type": "ebs", "size_gib": 128, "permissions": "read_write", "mount": "/data"
    }]))
    .unwrap();
    let mut env = TestEnv::default();
    admit(&env, &mounts, owner, RuntimeKind::Firecracker, false).unwrap_err();
    assert!(
        admit(&env, &[], owner, RuntimeKind::Firecracker, false)
            .unwrap()
            .is_none()
    );
    env.set(
        POLICY_ENV,
        serde_json::json!({
            "organization_ids": [owner], "availability_zone": "test-region-1a"
        })
        .to_string(),
    );
    assert!(
        admit(&env, &mounts, owner, RuntimeKind::Firecracker, false)
            .unwrap()
            .is_some()
    );
    let qualified_hash = format!("sha256:{}", "a".repeat(64));
    let other_hash = format!("sha256:{}", "b".repeat(64));
    let policy = admit(&env, &mounts, owner, RuntimeKind::Firecracker, false)
        .unwrap()
        .unwrap();
    policy.require_guest(&qualified_hash).unwrap_err();
    for version in [1, 2] {
        env.set(
            POLICY_ENV,
            serde_json::json!({
                "organization_ids": [owner],
                "availability_zone": "test-region-1a",
                "qualified_guest_images": {qualified_hash.clone(): version}
            })
            .to_string(),
        );
        let policy = admit(&env, &mounts, owner, RuntimeKind::Firecracker, false)
            .unwrap()
            .unwrap();
        assert_eq!(policy.require_guest(&qualified_hash).is_ok(), version == 1);
        policy.require_guest(&other_hash).unwrap_err();
        policy.require_guest("seed:latest").unwrap_err();
    }
    admit(&env, &mounts, other, RuntimeKind::Firecracker, false).unwrap_err();
    admit(&env, &mounts, owner, RuntimeKind::Firecracker, true).unwrap_err();
    admit(
        &env,
        &[mounts[0].clone(), mounts[0].clone()],
        owner,
        RuntimeKind::Firecracker,
        false,
    )
    .unwrap_err();
}

#[cfg(feature = "kubernetes")]
#[test]
fn block_volume_placement_intersects_every_affinity_alternative() {
    use k8s_openapi::api::core::v1::Pod;
    for hold_for_placement in [false, true] {
        let mut env = TestEnv::default();
        env.set(
            "SPACE_EBS_PLACEMENT_POLICY",
            serde_json::json!({
                "availability_zone": "test-region-1a", "node_names": ["worker-a", "worker-b"],
                "hold_for_placement": hold_for_placement,
                "tolerations": [{
                    "key": "isolated-pool", "operator": "Equal", "value": "test",
                    "effect": "NoSchedule"
                }]
            })
            .to_string(),
        );
        let mut pod: Pod = serde_json::from_value(serde_json::json!({
            "spec": {"containers": [], "affinity": {"nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "existing", "operator": "In", "values": ["first"]}]},
                    {"matchExpressions": [{"key": "existing", "operator": "In", "values": ["second"]}]}
                ]}
            }}}
        })).unwrap();
        constrain_pod(&mut pod, "test-region-1a", &env).unwrap();
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.scheduling_gates.is_some(), hold_for_placement);
        assert_eq!(
            spec.tolerations.as_ref().unwrap()[0].key.as_deref(),
            Some("isolated-pool")
        );
        assert!(
            spec.node_name.is_none(),
            "placement must still go through the scheduler"
        );
        let terms = &pod
            .spec
            .as_ref()
            .unwrap()
            .affinity
            .as_ref()
            .unwrap()
            .node_affinity
            .as_ref()
            .unwrap()
            .required_during_scheduling_ignored_during_execution
            .as_ref()
            .unwrap()
            .node_selector_terms;
        for (term, original) in terms.iter().zip(["first", "second"]) {
            let expressions = term.match_expressions.as_ref().unwrap();
            assert_eq!(expressions.len(), 3);
            assert_eq!(
                expressions[0].values.as_deref(),
                Some(&[original.to_string()][..])
            );
            assert_eq!(
                expressions[1].values.as_deref(),
                Some(&["test-region-1a".to_string()][..])
            );
            assert_eq!(
                expressions[2].values.as_deref(),
                Some(&["worker-a".to_string(), "worker-b".to_string()][..])
            );
        }
        constrain_pod(&mut pod, "test-region-1b", &env).unwrap_err();
        pod.spec.as_mut().unwrap().node_name = Some("worker-c".to_string());
        constrain_pod(&mut pod, "test-region-1a", &env).unwrap_err();
    }
}
