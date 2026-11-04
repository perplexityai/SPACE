use super::*;
use crate::proto::{ExportTemplateRequest, TemplateRef, export_template_chunk::Payload};
use crate::volume::tests::catalog::{Catalog, TEMPLATE};
use futures_util::StreamExt;
use pplx_space_node_api_client::proto::TemplateVisibility;
use pplx_space_test_env::{TestResult, enter_test_process};
use std::ffi::OsStr;

fn export_fixture() -> (tempfile::TempDir, TemplateManagerService, String) {
    let root = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let digest = format!("sha256:{}", "a".repeat(64));
    insert_ready_build(&conn, TEMPLATE, &digest);
    insert_ready_build(&conn, TEMPLATE, &format!("sha256:{}", "b".repeat(64)));
    conn.execute(
        "UPDATE template_builds SET build_date = '2026-05-14T00:00:00Z' WHERE content_hash != ?1",
        [&digest],
    )
    .unwrap();
    let dir = build_artifacts_dir(root.path(), TEMPLATE, &digest);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(TEMPLATE_ROOTFS_ZSTD_FILE),
        b"synthetic compressed artifact",
    )
    .unwrap();
    std::fs::write(dir.join("manifest.json"), serde_json::json!({
        "bootable_artifact": "rootfs.ext4",
        "files": {"rootfs.ext4.zst": {"digest": "sha256:synthetic", "size": 29, "compression": {"codec": "zstd", "level": 3}}}
    }).to_string()).unwrap();
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    (root, service, digest)
}

#[rstest::rstest]
#[case::root(0, false, true)]
#[case::operator(2001, false, true)]
#[case::forwarded(users::SPACE_NODEAPI.uid, true, true)]
#[case::missing_grant(users::SPACE_NODEAPI.uid, false, false)]
#[tokio::test]
async fn export_enforces_forwarded_authority(
    #[case] uid: u32,
    #[case] grant: bool,
    #[case] allowed: bool,
) {
    let (_root, mut service, digest) = export_fixture();
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "template-export-test",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_NODEAPI),
            UdsPeerAccount::role_with_fallback_uid(
                "template-export-operator",
                LocalRole::Operator,
                2001,
            ),
        ],
    );
    let mut request = Request::new(ExportTemplateRequest {
        template: Some(TemplateRef {
            name: TEMPLATE.into(),
            content_hash: Some(digest.clone()),
            tag: None,
        }),
        ..Default::default()
    });
    request
        .extensions_mut()
        .insert(PeerIdentity::new(uid, uid, None));
    if grant {
        request.metadata_mut().insert(
            "x-space-template-transfer-authority",
            "granted".parse().unwrap(),
        );
    }

    let result = service.export_template(request).await;

    if allowed {
        assert_export_header(result.unwrap(), &digest).await;
    } else {
        assert_eq!(result.err().unwrap().code(), tonic::Code::PermissionDenied);
    }
}

#[rstest::rstest]
#[case::catalog_latest("latest", true, true)]
#[case::unknown_tag("missing", true, false)]
#[case::uncached_release("latest", false, false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_resolves_catalog_tag(
    #[case] tag: &str,
    #[case] cached: bool,
    #[case] found: bool,
) -> TestResult {
    const CHILD: &str = "SPACE_TEST_EXPORT_CATALOG";
    if std::env::var_os(CHILD).is_none() {
        let catalog = Catalog::start(TemplateVisibility::Generic).await?;
        assert!(!enter_test_process(&[
            (CHILD, Some(OsStr::new("1"))),
            (
                "SPACE_INTERNAL_API_ENDPOINT",
                Some(OsStr::new(&catalog.endpoint))
            ),
            (
                "SPACE_CLUSTER_API_TOKEN_PATH",
                Some(catalog.token_file.path().as_os_str())
            ),
            (
                "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
                Some(OsStr::new("off"))
            ),
            ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
        ]));
        return Ok(());
    }
    assert!(enter_test_process(&[]));
    let (_root, service, digest) = export_fixture();
    if !cached {
        service.db.lock().unwrap().execute(
            "DELETE FROM template_builds WHERE content_hash = ?1",
            [&digest],
        )?;
    }

    let result = service
        .export_template(internal_request(ExportTemplateRequest {
            template: Some(TemplateRef {
                name: TEMPLATE.into(),
                content_hash: None,
                tag: Some(tag.into()),
            }),
            ..Default::default()
        }))
        .await;

    if found {
        assert_export_header(result?, &digest).await;
    } else {
        assert_eq!(result.err().unwrap().code(), tonic::Code::NotFound);
    }
    Ok(())
}

async fn assert_export_header(response: Response<TemplateExportStream>, digest: &str) {
    let mut stream = response.into_inner();
    let Some(Payload::Header(header)) = stream.next().await.unwrap().unwrap().payload else {
        panic!("export must begin with its exact release header");
    };
    assert_eq!(
        header.template.unwrap().content_hash.as_deref(),
        Some(digest)
    );
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
}

#[tokio::test]
async fn export_refuses_unsupported_or_corrupt_documents_before_streaming() {
    for (file, bytes, expected) in [
        (
            "metadata.json",
            br#"{"schema_version":99}"#.as_slice(),
            tonic::Code::FailedPrecondition,
        ),
        (
            "manifest.json",
            br#"{"schema_version":99}"#.as_slice(),
            tonic::Code::FailedPrecondition,
        ),
        (
            "metadata.json",
            b"not json".as_slice(),
            tonic::Code::DataLoss,
        ),
    ] {
        let (root, service, digest) = export_fixture();
        let dir = build_artifacts_dir(root.path(), TEMPLATE, &digest);
        std::fs::write(dir.join(file), bytes).unwrap();
        let result = service
            .export_template(internal_request(ExportTemplateRequest {
                template: Some(TemplateRef {
                    name: TEMPLATE.into(),
                    content_hash: Some(digest),
                    tag: None,
                }),
                ..Default::default()
            }))
            .await;
        assert_eq!(result.err().unwrap().code(), expected, "{file}");
        assert_eq!(
            std::fs::read(dir.join(TEMPLATE_ROOTFS_ZSTD_FILE)).unwrap(),
            b"synthetic compressed artifact"
        );
    }
}
