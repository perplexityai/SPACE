use super::*;

#[tokio::test]
async fn deferred_snapshot_reconciliation_preserves_last_use() {
    let conn = crate::db::test_conn();
    let name = "idle-snapshot-template";
    let hash = format!("sha256:{}", "a".repeat(64));
    let partition = crate::db::TemplateCachePartition::Legacy;
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name,
            content_hash: &hash,
            cache_partition: partition,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "x86_64",
            artifact_size_bytes: 1,
            state: "ready",
        },
    )
    .unwrap();
    crate::db::set_last_used_at_for_test(&conn, name, &hash, 1).unwrap();
    let service = TemplateManagerService::new(conn).unwrap();
    service.defer_failed_snapshot(name, &partition.build_dir_name(&hash));

    // Policy refresh still inspects this build while a failed capture backs
    // off. Maintenance alone must not postpone its idle expiry.
    let result = service
        .reconcile_partition_base_snapshot(name, partition, 2, 1024)
        .await;
    assert_eq!(result.outcome.content_hash.as_deref(), Some(hash.as_str()));
    assert!(!result.outcome.reconciled);
    let conn = service.db.lock().unwrap();
    let builds = crate::db::list_eviction_candidates(&conn).unwrap();
    assert_eq!(builds[0].last_used_at_secs, 1);
}
