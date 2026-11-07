//! `DeleteTemplate` and background rootfs compression take the same claim
//! as every other writer of a build, so neither overlaps one.

use super::*;

/// Lays out an installed, uncompressed build of `name` under `root` and
/// returns its content hash and directory.
fn place_build(root: &Path, name: &str) -> (String, PathBuf) {
    let (fixture, content_hash, _) = expanded_test_rootfs_setup();
    let dir = build_artifacts_dir(
        root,
        name,
        &content_hash,
        crate::db::TemplateCachePartition::Legacy,
    );
    std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
    std::fs::rename(fixture.path(), &dir).unwrap();
    (content_hash, dir)
}

fn registered(service: &TemplateManagerService, name: &str, content_hash: &str) -> bool {
    crate::db::get_build(
        &service.db.lock().unwrap(),
        name,
        content_hash,
        crate::db::TemplateCachePartition::Legacy,
    )
    .unwrap()
    .is_some()
}

async fn wait_for_compression(dir: &Path) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while rootfs_zstd_generation_inflight()
            .lock()
            .unwrap()
            .contains(dir)
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("compression should finish");
}

/// While a pull, import or registration holds one of a template's builds,
/// `DeleteTemplate` refuses retryably and leaves the template on disk.
#[tokio::test]
async fn delete_template_refuses_while_one_of_its_builds_is_claimed() {
    let root = tempfile::tempdir().unwrap();
    let (hash, dir) = place_build(root.path(), "claimed-template");
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let held = service
        .volume_puller
        .try_claim_digest("claimed-template", &hash)
        .unwrap();

    let error = service
        .delete_template(internal_request(DeleteTemplateRequest {
            template: "claimed-template".to_string(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Aborted, "{error}");
    assert!(dir.join(TEMPLATE_ROOTFS_FILE).exists());
    drop(held);
    assert!(
        service
            .volume_puller
            .try_claim_template("claimed-template")
            .is_some()
    );
}

/// The compressor rewrites a build's manifest and row, so it waits for the
/// registration or import that spawned it, or a delete, to let the digest
/// go. A build deleted meanwhile gets no row back; an untouched one is
/// compressed and recorded once the claim goes.
#[tokio::test]
async fn compression_waits_for_the_digest_and_never_restores_a_deleted_build() {
    let root = tempfile::tempdir().unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();

    for deleted in [true, false] {
        let name = if deleted { "deleted" } else { "kept" };
        let (hash, dir) = place_build(root.path(), name);
        let held = service.volume_puller.try_claim_digest(name, &hash).unwrap();
        service.spawn_template_compressed_rootfs_generation(
            name.to_string(),
            hash.clone(),
            dir.display().to_string(),
            crate::db::TemplateCachePartition::Legacy,
            "test",
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!dir.join(TEMPLATE_ROOTFS_ZSTD_FILE).exists(), "{name}");
        assert!(!registered(&service, name, &hash), "{name}");

        if deleted {
            // What a delete holding the claim removes.
            std::fs::remove_dir_all(&dir).unwrap();
        }
        drop(held);
        wait_for_compression(&dir).await;

        assert_eq!(registered(&service, name, &hash), !deleted, "{name}");
        assert_eq!(dir.exists(), !deleted, "{name}");
        if !deleted {
            assert!(dir.join(TEMPLATE_ROOTFS_ZSTD_FILE).exists());
        }
    }
}

/// A base snapshot whose build `DeleteTemplateBuild` removes while the
/// capture runs isn't published. The publish waits for the delete's claim,
/// finds the build's row gone, records nothing and removes the files the
/// capture wrote. The capture itself needs sandbox-manager, so this drives
/// the publish step it ends with.
#[tokio::test]
async fn a_base_snapshot_is_not_published_for_a_build_deleted_during_capture() {
    const NAME: &str = "snapshot-during-delete";
    let legacy = crate::db::TemplateCachePartition::Legacy;
    let root = tempfile::tempdir().unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let hash = format!("sha256:{}", "a".repeat(64));
    let dir = snapshot_artifacts_dir(root.path(), NAME, &hash, legacy, 2, 1024);
    // What sandbox-manager's capture leaves behind.
    let capture = || {
        std::fs::create_dir_all(&dir).unwrap();
        for file in ["snapshot.base", "vmstate.base"] {
            std::fs::write(dir.join(file), b"captured").unwrap();
        }
    };
    let publish = |service: TemplateManagerService, hash: String, dir: PathBuf| {
        tokio::spawn(async move {
            service
                .publish_captured_base_snapshot(
                    crate::db::SnapshotKey {
                        name: NAME,
                        content_hash: &hash,
                        cache_partition: crate::db::TemplateCachePartition::Legacy,
                        vcpu: 2,
                        memory_mib: 1024,
                    },
                    "snapshot-hash",
                    &dir,
                )
                .await
        })
    };
    let snapshot_rows = |service: &TemplateManagerService| {
        crate::db::list_snapshots(&service.db.lock().unwrap(), NAME, &hash, legacy)
            .unwrap()
            .len()
    };

    insert_ready_build(&service.db.lock().unwrap(), NAME, &hash);
    capture();
    // The delete holds the digest from before the capture finishes.
    let delete = service.volume_puller.try_claim_digest(NAME, &hash).unwrap();
    let published = publish(service.clone(), hash.clone(), dir.clone());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!published.is_finished(), "the publish waits for the delete");
    crate::db::delete_build(&service.db.lock().unwrap(), NAME, &hash, legacy).unwrap();
    drop(delete);

    assert!(!published.await.unwrap().unwrap(), "nothing is published");
    assert_eq!(snapshot_rows(&service), 0);
    assert!(!dir.exists(), "the captured snapshot is discarded");

    // A build that's still there gets its snapshot.
    insert_ready_build(&service.db.lock().unwrap(), NAME, &hash);
    capture();
    assert!(
        publish(service.clone(), hash.clone(), dir.clone())
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(snapshot_rows(&service), 1);
    assert!(dir.join("snapshot.base").exists());
}

/// The publish step outlives a capture whose build timeout fires while it
/// waits for the digest's claim, as one held by a GC check of another
/// partition's dir can make it: once the claim goes, a live build still gets
/// its snapshot row and a deleted one's captured files are still removed,
/// rather than left on disk with no row.
#[tokio::test]
async fn a_base_snapshot_publish_finishes_after_its_caller_times_out() {
    const NAME: &str = "publish-after-timeout";
    let legacy = crate::db::TemplateCachePartition::Legacy;
    let root = tempfile::tempdir().unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap();
    let hash = format!("sha256:{}", "b".repeat(64));
    let dir = snapshot_artifacts_dir(root.path(), NAME, &hash, legacy, 2, 1024);
    let snapshot_rows = |service: &TemplateManagerService| {
        crate::db::list_snapshots(&service.db.lock().unwrap(), NAME, &hash, legacy)
            .unwrap()
            .len()
    };

    for deleted in [true, false] {
        insert_ready_build(&service.db.lock().unwrap(), NAME, &hash);
        std::fs::create_dir_all(&dir).unwrap();
        for file in ["snapshot.base", "vmstate.base"] {
            std::fs::write(dir.join(file), b"captured").unwrap();
        }
        let held = service.volume_puller.try_claim_digest(NAME, &hash).unwrap();
        // A zero budget: the publish is polled once, finds the claim held,
        // and the caller gives up.
        let timed_out = tokio::time::timeout(
            std::time::Duration::ZERO,
            service.publish_captured_base_snapshot(
                crate::db::SnapshotKey {
                    name: NAME,
                    content_hash: &hash,
                    cache_partition: legacy,
                    vcpu: 2,
                    memory_mib: 1024,
                },
                "snapshot-hash",
                &dir,
            ),
        )
        .await;
        assert!(
            timed_out.is_err(),
            "deleted={deleted}: the caller times out"
        );
        assert_eq!(snapshot_rows(&service), 0, "deleted={deleted}");
        if deleted {
            crate::db::delete_build(&service.db.lock().unwrap(), NAME, &hash, legacy).unwrap();
        }
        drop(held);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let published = || {
                if deleted {
                    !dir.exists()
                } else {
                    snapshot_rows(&service) == 1
                }
            };
            while !published() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("deleted={deleted}: the publish finishes"));
        assert_eq!(snapshot_rows(&service), usize::from(!deleted));
        assert_eq!(dir.join("snapshot.base").exists(), !deleted);
        // The publish has let the digest go.
        let _ = service.volume_puller.claim_digest(NAME, &hash).await;
    }
}
