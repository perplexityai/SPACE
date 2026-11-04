use super::*;

#[tokio::test]
async fn register_refuses_unknown_schema_before_staging_disk() {
    for file in ["metadata.json", "manifest.json"] {
        let root = tempfile::tempdir().unwrap();
        let name = "unsupported-registration";
        let source = root.path().join(name).join("build.importing.test");
        std::fs::create_dir_all(source.join("rootfs.ext4")).unwrap();
        // A directory is an intentional disk-copy trap: schema refusal must
        // happen before registration tries to read or expand this path.
        std::fs::write(source.join(file), br#"{"schema_version":99}"#).unwrap();
        let service =
            TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
                .unwrap();
        let result = service
            .register_template(internal_request(crate::proto::RegisterTemplateRequest {
                template: Some(crate::proto::TemplateRef {
                    name: name.into(),
                    ..Default::default()
                }),
                staging_dir: Some("build.importing.test".into()),
                ..Default::default()
            }))
            .await;
        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("schema"), "{error}");
        assert!(source.join("rootfs.ext4").is_dir());
        assert_eq!(
            std::fs::read_dir(root.path().join(name)).unwrap().count(),
            1
        );
    }
}
