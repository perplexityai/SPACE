//! `DeleteTemplate` and background rootfs compression take the same claim
//! as every other writer of a build, so neither overlaps one.

use super::*;

/// Lays out an installed, uncompressed build of `name` under `root` and
/// returns its content hash and directory.
fn place_build(root: &Path, name: &str) -> (String, PathBuf) {
    let (fixture, content_hash, _) = expanded_test_rootfs_setup();
    let dir = build_artifacts_dir(root, name, &content_hash);
    std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
    std::fs::rename(fixture.path(), &dir).unwrap();
    (content_hash, dir)
}

fn registered(service: &TemplateManagerService, name: &str, content_hash: &str) -> bool {
    crate::db::get_build(&service.db.lock().unwrap(), name, content_hash)
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
