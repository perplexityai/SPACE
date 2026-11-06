//! `RegisterTemplate` and `DeleteTemplateBuild` of one digest take its claim,
//! so a race between them is linearizable: whichever claims it first finishes
//! before the other touches the digest.

use super::*;

/// Lays out a build-manager staging dir for `name` and returns the content
/// hash registration will compute for it.
fn stage(root: &Path, name: &str) -> (tempfile::TempDir, String) {
    let (fixture, content_hash, _) = expanded_test_rootfs_setup();
    let source = root.join(name).join("build.importing.race");
    std::fs::create_dir_all(&source).unwrap();
    for file in [TEMPLATE_ROOTFS_FILE, "metadata.json"] {
        std::fs::copy(fixture.path().join(file), source.join(file)).unwrap();
    }
    (fixture, content_hash)
}

fn register(
    service: &TemplateManagerService,
    name: &'static str,
) -> tokio::task::JoinHandle<Result<Response<crate::proto::RegisterTemplateResponse>, Status>> {
    let service = service.clone();
    tokio::spawn(async move {
        service
            .register_template(internal_request(crate::proto::RegisterTemplateRequest {
                template: Some(crate::proto::TemplateRef {
                    name: name.into(),
                    ..Default::default()
                }),
                staging_dir: Some("build.importing.race".into()),
                ..Default::default()
            }))
            .await
    })
}

fn registered(service: &TemplateManagerService, name: &str, content_hash: &str) -> bool {
    crate::db::get_build(&service.db.lock().unwrap(), name, content_hash)
        .unwrap()
        .is_some()
}

async fn wait_for_compression(published: &Path) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while rootfs_zstd_generation_inflight()
            .lock()
            .unwrap()
            .contains(published)
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("registration compression should finish");
}

/// A delete that claims the digest first finishes before the registration
/// publishes anything, and the registration then lands its build. A
/// registration that claims it first holds the delete off until its row is
/// written: the delete refuses retryably and the build stays whole. Neither
/// order reports success with the build still there or loses it.
#[tokio::test]
async fn a_registration_racing_a_delete_never_succeeds_over_or_loses_a_build() {
    let root = tempfile::tempdir().unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap()
            .without_sbx_connect_retries()
            .with_production_hash_algorithm(HashAlg::Sha256);

    // The delete claims first.
    let (_fixture, hash) = stage(root.path(), "race-a");
    let published = root.path().join("race-a").join(&hash);
    let delete = service.claim_digest_for_delete("race-a", &hash).unwrap();
    let registration = register(&service, "race-a");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!registration.is_finished());
    assert!(!published.exists());
    service
        .delete_template_build_after_dependency_checks("race-a", &hash)
        .await
        .unwrap();
    drop(delete);
    tokio::time::timeout(std::time::Duration::from_secs(10), registration)
        .await
        .expect("the registration goes on once the delete lets go")
        .unwrap()
        .unwrap();
    assert!(published.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(registered(&service, "race-a", &hash));
    wait_for_compression(&published).await;

    // The registration claims first. Holding the DB parks it after it
    // publishes the dir and before it writes the row.
    let (_fixture, hash) = stage(root.path(), "race-b");
    let published = root.path().join("race-b").join(&hash);
    let db = Arc::clone(&service.db);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let parker = std::thread::spawn(move || {
        let _parked = db.lock().unwrap();
        locked_tx.send(()).unwrap();
        let _ = release_rx.recv();
    });
    locked_rx.recv().unwrap();
    let registration = register(&service, "race-b");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !published.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the registration publishes its dir");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        service.delete_template_build(internal_request(crate::proto::DeleteTemplateBuildRequest {
            template: Some(crate::proto::TemplateRef {
                name: "race-b".into(),
                content_hash: Some(hash.clone()),
                tag: None,
                environment: None,
            }),
        })),
    )
    .await
    .expect("the delete refuses without waiting for the registration")
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted, "{error}");
    drop(release_tx);
    parker.join().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), registration)
        .await
        .expect("the registration finishes")
        .unwrap()
        .unwrap();
    assert!(published.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(registered(&service, "race-b", &hash));
    wait_for_compression(&published).await;
}

/// A caller that gives up on a registration doesn't release its claim
/// early: the registration runs to completion as its own task, so a delete
/// can't slip in between its publish and its row.
#[tokio::test]
async fn a_cancelled_registration_keeps_its_claim_until_its_row_is_written() {
    let root = tempfile::tempdir().unwrap();
    let service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap()
            .without_sbx_connect_retries()
            .with_production_hash_algorithm(HashAlg::Sha256);
    let (_fixture, hash) = stage(root.path(), "race-c");
    let published = root.path().join("race-c").join(&hash);

    // Holding the DB parks the registration after it publishes the dir and
    // before it writes the row.
    let db = Arc::clone(&service.db);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let parker = std::thread::spawn(move || {
        let _parked = db.lock().unwrap();
        locked_tx.send(()).unwrap();
        let _ = release_rx.recv();
    });
    locked_rx.recv().unwrap();
    let caller = tokio::spawn({
        let service = service.clone();
        async move {
            TemplateManager::register_template(
                &service,
                internal_request(crate::proto::RegisterTemplateRequest {
                    template: Some(crate::proto::TemplateRef {
                        name: "race-c".into(),
                        ..Default::default()
                    }),
                    staging_dir: Some("build.importing.race".into()),
                    ..Default::default()
                }),
            )
            .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !published.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the registration publishes its dir");

    caller.abort();
    let _ = caller.await;
    assert!(
        service.claim_digest_for_delete("race-c", &hash).is_err(),
        "the registration still owns the digest"
    );

    drop(release_tx);
    parker.join().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while service.volume_puller.is_claimed("race-c", &hash) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the registration finishes and lets go");
    assert!(published.join(TEMPLATE_ROOTFS_FILE).exists());
    assert!(registered(&service, "race-c", &hash));
    wait_for_compression(&published).await;
}
