use super::*;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

fn request(disk_io_limits: Option<DiskIoLimits>) -> WarmPoolPolicyRequest {
    WarmPoolPolicyRequest {
        template_name: "custom-runtime".to_string(),
        vcpu: 4,
        memory_mib: 8192,
        disk_mib: None,
        reset_disk_to_template_default: false,
        min_ready_slots: 1,
        max_ready_slots: 4,
        refill_per_tick: 1,
        max_warming_slots: 2,
        retain_releases: 1,
        retain_shape_on_miss: None,
        disk_io_max_operations_per_sec: disk_io_limits
            .map(|limits| limits.max_operations_per_sec()),
        disk_io_max_bandwidth_mib_per_sec: disk_io_limits
            .map(|limits| limits.max_bandwidth_mib_per_sec()),
        reset_disk_io_to_default: false,
        enabled: true,
    }
}

fn row(disk_io_limits: Option<DiskIoLimits>) -> WarmPoolPolicyRow {
    WarmPoolPolicyRow {
        template_name: "custom-runtime".to_string(),
        runtime: "fc".to_string(),
        vcpu: 4,
        memory_mib: 8192,
        disk_mib: None,
        min_ready_slots: 1,
        max_ready_slots: 4,
        refill_per_tick: 1,
        max_warming_slots: 2,
        retain_releases: 1,
        disk_io_max_operations_per_sec: disk_io_limits
            .map(|limits| i64::try_from(limits.max_operations_per_sec()).unwrap()),
        disk_io_max_bandwidth_mib_per_sec: disk_io_limits
            .map(|limits| i64::try_from(limits.max_bandwidth_mib_per_sec()).unwrap()),
        enabled: true,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        updated_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn disk_io_policy_is_disabled_when_fields_are_omitted() {
    validate_policy_request(&request(None), DiskIoLimits::default()).unwrap();
}

#[test]
fn disk_capacity_is_bounded_and_reset_is_explicit() {
    let mut configured = request(None);
    for (disk_mib, allowed) in [
        (None, true),
        (Some(0), false),
        (Some(131072), true),
        (Some(131073), false),
    ] {
        configured.disk_mib = disk_mib;
        assert_eq!(
            validate_policy_request(&configured, DiskIoLimits::default()).is_ok(),
            allowed
        );
    }

    configured.disk_mib = Some(32768);
    configured.reset_disk_to_template_default = true;
    validate_policy_request(&configured, DiskIoLimits::default()).unwrap_err();
}

#[test]
fn disk_io_policy_must_fit_cluster_safety_ceiling() {
    let requested = DiskIoLimits::try_new(5_000, 200).unwrap();
    let error = validate_policy_request(&request(Some(requested)), DiskIoLimits::default())
        .expect_err("default ceiling must reject an unapproved increase");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    validate_policy_request(&request(Some(requested)), requested).unwrap();
}

#[test]
fn disk_io_policy_requires_a_complete_pair() {
    let mut request = request(None);
    request.disk_io_max_operations_per_sec = Some(2_000);
    assert!(validate_policy_request(&request, DiskIoLimits::default()).is_err());
}

#[test]
fn disk_io_reset_is_explicit_and_mutually_exclusive_with_limits() {
    let mut reset = request(None);
    reset.reset_disk_io_to_default = true;
    validate_policy_request(&reset, DiskIoLimits::default()).unwrap();

    reset.disk_io_max_operations_per_sec = Some(1_000);
    reset.disk_io_max_bandwidth_mib_per_sec = Some(100);
    assert!(validate_policy_request(&reset, DiskIoLimits::default()).is_err());
}

#[test]
fn legacy_admin_request_defaults_to_preserving_disk_io() {
    let request: WarmPoolPolicyRequest = serde_json::from_value(serde_json::json!({
        "template_name": "custom-runtime",
        "vcpu": 4,
        "memory_mib": 8192,
        "min_ready_slots": 1,
        "max_ready_slots": 4,
        "retain_releases": 1,
        "enabled": true
    }))
    .unwrap();

    assert!(request.disk_io_max_operations_per_sec.is_none());
    assert!(request.disk_io_max_bandwidth_mib_per_sec.is_none());
    assert!(!request.reset_disk_io_to_default);
    assert!(request.disk_mib.is_none());
    assert!(!request.reset_disk_to_template_default);
    assert!(request.retain_shape_on_miss.is_none());
}

#[tokio::test]
async fn legacy_upsert_preserves_disk_capacity_and_explicit_reset_clears_it() -> TestResult {
    let db = pg_test_db().await?;
    let mut configured = request(None);
    configured.disk_mib = Some(32768);
    assert_eq!(
        store_policy(&db.pool, &configured).await?.disk_mib,
        Some(32768)
    );

    assert_eq!(
        store_policy(&db.pool, &request(None)).await?.disk_mib,
        Some(32768)
    );

    let mut reset = request(None);
    reset.reset_disk_to_template_default = true;
    assert!(store_policy(&db.pool, &reset).await?.disk_mib.is_none());
    Ok(())
}

#[tokio::test]
async fn deprecated_retain_shape_on_miss_is_accepted_and_ignored() -> TestResult {
    let db = pg_test_db().await?;
    for legacy_value in [false, true] {
        let request: WarmPoolPolicyRequest = serde_json::from_value(serde_json::json!({
            "template_name": "custom-runtime",
            "vcpu": 4,
            "memory_mib": 8192,
            "min_ready_slots": 1,
            "max_ready_slots": 4,
            "retain_releases": 1,
            "retain_shape_on_miss": legacy_value,
            "enabled": true
        }))?;
        assert_eq!(request.retain_shape_on_miss, Some(legacy_value));

        let response = WarmPoolPolicyResponse::from(store_policy(&db.pool, &request).await?);
        assert!(
            response.retain_shape_on_miss,
            "cold misses always retain the policy shape"
        );
    }
    Ok(())
}

#[tokio::test]
async fn legacy_upsert_preserves_limits_and_explicit_reset_clears_them() -> TestResult {
    let db = pg_test_db().await?;
    let limits = DiskIoLimits::try_new(5_000, 200).unwrap();

    let inserted = store_policy(&db.pool, &request(Some(limits))).await?;
    assert_eq!(inserted.disk_io_max_operations_per_sec, Some(5_000));
    assert_eq!(inserted.disk_io_max_bandwidth_mib_per_sec, Some(200));

    let preserved = store_policy(&db.pool, &request(None)).await?;
    assert_eq!(preserved.disk_io_max_operations_per_sec, Some(5_000));
    assert_eq!(preserved.disk_io_max_bandwidth_mib_per_sec, Some(200));

    let mut reset = request(None);
    reset.reset_disk_io_to_default = true;
    let cleared = store_policy(&db.pool, &reset).await?;
    assert!(cleared.disk_io_max_operations_per_sec.is_none());
    assert!(cleared.disk_io_max_bandwidth_mib_per_sec.is_none());
    Ok(())
}

#[test]
fn omitted_disk_io_policy_is_absent_from_tooling_output() {
    let response = WarmPoolPolicyResponse::from(row(None));

    assert!(response.disk_io_max_operations_per_sec.is_none());
    assert!(response.disk_io_max_bandwidth_mib_per_sec.is_none());
    assert!(response.disk_io_source.is_none());
    assert!(response.disk_mib.is_none());
    let json = serde_json::to_value(response).unwrap();
    assert!(json.get("disk_io_max_operations_per_sec").is_none());
    assert!(json.get("disk_io_max_bandwidth_mib_per_sec").is_none());
    assert!(json.get("disk_io_source").is_none());
    assert!(json.get("disk_mib").is_none());
}

#[test]
fn explicit_disk_io_policy_reports_warm_pool_source() {
    let limits = DiskIoLimits::try_new(5_000, 200).unwrap();
    let response = WarmPoolPolicyResponse::from(row(Some(limits)));

    assert_eq!(
        response.disk_io_max_operations_per_sec,
        Some(limits.max_operations_per_sec())
    );
    assert_eq!(
        response.disk_io_max_bandwidth_mib_per_sec,
        Some(limits.max_bandwidth_mib_per_sec())
    );
    assert_eq!(response.disk_io_source, Some("warm_pool"));
}

#[test]
fn disk_io_capability_is_hidden_without_an_explicit_cluster_ceiling() {
    let capability = disk_io_policy_capability_from_values(None, None).unwrap();

    assert!(capability.disk_io.is_none());
    assert_eq!(
        serde_json::to_value(capability).unwrap(),
        serde_json::json!({ "disk_capacity": true })
    );
}

#[test]
fn disk_io_capability_reports_cluster_ceiling_and_platform_defaults() {
    let capability = disk_io_policy_capability_from_values(Some("5000"), Some("200")).unwrap();

    assert_eq!(
        serde_json::to_value(capability).unwrap(),
        serde_json::json!({
            "disk_capacity": true,
            "disk_io": {
                "max_operations_per_sec": 5_000,
                "max_bandwidth_mib_per_sec": 200,
                "default_operations_per_sec": 1_000,
                "default_bandwidth_mib_per_sec": 100
            }
        })
    );
}

#[test]
fn disk_io_capability_rejects_a_partial_cluster_ceiling() {
    let error = disk_io_policy_capability_from_values(Some("5000"), None)
        .expect_err("the ceiling must be configured as a pair");

    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
