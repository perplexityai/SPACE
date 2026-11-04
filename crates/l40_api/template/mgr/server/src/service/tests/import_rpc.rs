use super::*;
use crate::proto::{
    ImportTemplateHeader, ImportTemplateRequest, TemplateRef, import_template_request::Payload,
    template_manager_client::TemplateManagerClient, template_manager_server::TemplateManagerServer,
};
use rstest::rstest;
use serde_json::{Value, json};
use sha2::Digest;
use tonic::Code;

#[derive(Clone, Copy, Debug)]
enum Change {
    Compatibility,
    Execution,
    Disk,
    Config,
    Digest,
    Size,
    Corruption,
    Schema,
    Registration,
    Description,
    Unchanged,
}

#[rstest]
#[case::compatibility(Change::Compatibility, Code::AlreadyExists)]
#[case::execution(Change::Execution, Code::AlreadyExists)]
#[case::disk_growth(Change::Disk, Code::AlreadyExists)]
#[case::configuration(Change::Config, Code::AlreadyExists)]
#[case::digest(Change::Digest, Code::AlreadyExists)]
#[case::size(Change::Size, Code::AlreadyExists)]
#[case::corrupted_rootfs(Change::Corruption, Code::FailedPrecondition)]
#[case::unsupported_schema(Change::Schema, Code::FailedPrecondition)]
#[case::registration_failure(Change::Registration, Code::Internal)]
#[case::description(Change::Description, Code::Ok)]
#[case::unchanged(Change::Unchanged, Code::Ok)]
#[tokio::test]
async fn existing_import_refresh_over_unix_socket(#[case] change: Change, #[case] expected: Code) {
    let mut fixture = ImportFixture::new().await;
    let header = fixture.changed_header(change);
    let before = fixture.state();

    let response = fixture
        .client
        .import_template(tokio_stream::iter([ImportTemplateRequest {
            payload: Some(Payload::Header(header)),
        }]))
        .await;

    let actual = response.as_ref().err().map_or(Code::Ok, Status::code);
    assert_eq!(actual, expected, "{response:?}");
    let after = fixture.state();
    if expected != Code::Ok {
        assert_eq!(
            after, before,
            "rejected imports must preserve files and registration"
        );
    } else {
        assert_eq!(
            response.unwrap().into_inner().template,
            Some(fixture.template.clone())
        );
        assert_eq!(
            after.rootfs, before.rootfs,
            "refresh must preserve rootfs bytes"
        );
        let metadata: Value = serde_json::from_slice(&after.metadata).unwrap();
        assert_eq!(
            metadata.get("description"),
            matches!(change, Change::Description).then_some(&json!("updated"))
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
struct BuildState {
    rootfs: (usize, String),
    manifest: Vec<u8>,
    metadata: Vec<u8>,
    registration: Value,
}

struct ImportFixture {
    _temp: tempfile::TempDir,
    directory: PathBuf,
    service: TemplateManagerService,
    template: TemplateRef,
    client: TemplateManagerClient<tonic::transport::Channel>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl ImportFixture {
    async fn new() -> Self {
        let (source, content_hash, _) = expanded_test_rootfs_setup();
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("templates");
        let template = TemplateRef {
            name: "register-refresh-test".into(),
            content_hash: Some(content_hash.clone()),
            tag: None,
        };
        let directory = build_artifacts_dir(&root, &template.name, &content_hash);
        std::fs::create_dir_all(directory.parent().unwrap()).unwrap();
        std::fs::rename(source.path(), &directory).unwrap();
        let mut service =
            TemplateManagerService::new_with_template_root(crate::db::test_conn(), root).unwrap();
        service
            .upsert_build_from_digest_dir(
                &template.name,
                &content_hash,
                directory.to_str().unwrap(),
                RootfsHashVerification::Verify,
            )
            .await
            .unwrap();
        // Map this test process to an operator; tonic still obtains its identity from SO_PEERCRED.
        service.peer_authorizer = UdsPeerAuthorizer::new(
            "import-rpc-test",
            [UdsPeerAccount::role_with_fallback_uid(
                "synthetic-import-operator",
                LocalRole::Operator,
                nix::unistd::geteuid().as_raw(),
            )],
        );
        let socket = temp.path().join("manager.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(TemplateManagerServer::new(service.clone()))
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener)),
        );
        let channel = pplx_space_common_grpc::connect_uds_channel("import-rpc-test", &socket)
            .await
            .unwrap();
        Self {
            _temp: temp,
            directory,
            service,
            template,
            client: TemplateManagerClient::new(channel),
            server,
        }
    }

    fn changed_header(&self, change: Change) -> ImportTemplateHeader {
        let mut manifest: Value =
            serde_json::from_slice(&std::fs::read(self.directory.join("manifest.json")).unwrap())
                .unwrap();
        let mut metadata: Value =
            serde_json::from_slice(&std::fs::read(self.directory.join("metadata.json")).unwrap())
                .unwrap();
        match change {
            Change::Compatibility => metadata["spaced_compatibility_family"] = json!("changed"),
            Change::Execution => {
                let execution = json!({
                    "runtime": {"kind": "firecracker", "version": "1.15.1"},
                    "guest": {"os": "linux", "architecture": "x86_64"},
                    "state_format": {"format": "firecracker-snapshot", "version": "9.0.0"},
                    "components": []
                });
                manifest["schema_version"] = json!(2);
                manifest["execution"] = execution.clone();
                metadata["schema_version"] = json!(2);
                metadata["execution"] = execution;
            }
            Change::Disk => metadata["disk_size_mb"] = json!(8),
            Change::Config => manifest["config"] = json!({"command": "different"}),
            Change::Digest => {
                manifest["content_hash"] = json!(format!("sha256:{}", "0".repeat(64)))
            }
            Change::Size => manifest["files"]["rootfs.ext4"]["size"] = json!(8),
            Change::Schema => manifest["schema_version"] = json!(99),
            Change::Corruption => {
                let path = self.directory.join("rootfs.ext4");
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
                metadata["description"] = json!("updated");
            }
            Change::Registration => {
                self.service
                    .db
                    .lock()
                    .unwrap()
                    .pragma_update(None, "query_only", true)
                    .unwrap();
                metadata["description"] = json!("updated");
            }
            Change::Description => {
                metadata["description"] = json!("updated");
                metadata["built_at"] = json!("2026-06-01T00:00:00Z");
                manifest["build_date"] = json!("2026-06-01T00:00:00Z");
            }
            Change::Unchanged => {}
        }
        ImportTemplateHeader {
            template: Some(self.template.clone()),
            manifest_json: serde_json::to_vec(&manifest).unwrap(),
            metadata_json: serde_json::to_vec(&metadata).unwrap(),
            ..Default::default()
        }
    }

    fn seed_snapshot(&self) -> PathBuf {
        let hash = self.template.content_hash.as_deref().unwrap();
        let path = snapshot_artifacts_dir(
            &self.service.artifact_store.root,
            &self.template.name,
            hash,
            1,
            512,
        );
        std::fs::create_dir_all(&path).unwrap();
        for file in [
            "snapshot.base",
            "vmstate.base",
            "metadata.json",
            "snapshot_hash",
        ] {
            std::fs::write(path.join(file), b"original").unwrap();
        }
        crate::db::upsert_snapshot(
            &self.service.db.lock().unwrap(),
            &self.template.name,
            hash,
            1,
            512,
            "original",
        )
        .unwrap();
        path
    }

    fn snapshot_state(&self, path: &Path) -> (Vec<Vec<u8>>, String, String) {
        let row = crate::db::get_snapshot(
            &self.service.db.lock().unwrap(),
            &self.template.name,
            self.template.content_hash.as_deref().unwrap(),
            1,
            512,
        )
        .unwrap()
        .unwrap();
        let files = [
            "snapshot.base",
            "vmstate.base",
            "metadata.json",
            "snapshot_hash",
        ]
        .map(|file| std::fs::read(path.join(file)).unwrap())
        .into();
        (files, row.snapshot_hash, row.built_at)
    }

    fn state(&self) -> BuildState {
        let build = crate::db::get_build(
            &self.service.db.lock().unwrap(),
            &self.template.name,
            self.template.content_hash.as_deref().unwrap(),
        )
        .unwrap()
        .unwrap();
        let rootfs = std::fs::read(self.directory.join("rootfs.ext4")).unwrap();
        BuildState {
            rootfs: (rootfs.len(), hex::encode(sha2::Sha256::digest(&rootfs))),
            manifest: std::fs::read(self.directory.join("manifest.json")).unwrap(),
            metadata: std::fs::read(self.directory.join("metadata.json")).unwrap(),
            registration: json!({
                "manifest": build.manifest_json, "execution": build.execution_metadata_json,
                "date": build.build_date, "runtime": build.runtime, "arch": build.arch,
                "size": build.artifact_size_bytes, "state": build.state,
                "owner_type": build.owner_type, "owner_id": build.owner_id,
            }),
        }
    }
}

impl Drop for ImportFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[rstest]
#[case::corrupt_rootfs(Change::Corruption, Code::FailedPrecondition)]
#[case::invalid_metadata(Change::Disk, Code::AlreadyExists)]
#[case::valid_snapshot(Change::Unchanged, Code::Ok)]
#[tokio::test]
async fn existing_import_verifies_build_before_snapshot(
    #[case] change: Change,
    #[case] expected: Code,
) {
    let mut fixture = ImportFixture::new().await;
    let mut header = fixture.changed_header(change);
    if matches!(change, Change::Corruption | Change::Unchanged) {
        header.manifest_json = std::fs::read(fixture.directory.join("manifest.json")).unwrap();
        header.metadata_json = std::fs::read(fixture.directory.join("metadata.json")).unwrap();
    }
    header.include_snapshot = true;
    header.snapshot_vcpu = Some(1);
    header.snapshot_memory_mib = Some(512);
    let snapshot = fixture.seed_snapshot();
    let before = fixture.snapshot_state(&snapshot);
    let mut requests = vec![ImportTemplateRequest {
        payload: Some(Payload::Header(header)),
    }];
    for file in ["snapshot.base", "vmstate.base", "snapshot-metadata.json"] {
        requests.push(ImportTemplateRequest {
            payload: Some(Payload::Chunk(crate::proto::FileChunk {
                file_name: file.into(),
                data: b"replacement".to_vec(),
                ..Default::default()
            })),
        });
    }

    let response = fixture
        .client
        .import_template(tokio_stream::iter(requests))
        .await;

    assert_eq!(
        response.as_ref().err().map_or(Code::Ok, Status::code),
        expected,
        "{response:?}"
    );
    if expected == Code::Ok {
        assert_eq!(
            std::fs::read(snapshot.join("snapshot.base")).unwrap(),
            b"replacement"
        );
        assert_ne!(fixture.snapshot_state(&snapshot).1, "original");
    } else {
        assert_eq!(
            fixture.snapshot_state(&snapshot),
            before,
            "refused build must not replace snapshots or their registration"
        );
    }
}

#[tokio::test]
async fn descriptive_refresh_preserves_raw_manifest_fields() {
    let mut fixture = ImportFixture::new().await;
    let path = fixture.directory.join("manifest.json");
    let mut original: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    original.as_object_mut().unwrap().remove("runtime");
    original.as_object_mut().unwrap().remove("arch");
    original["extension"] = json!({"opaque": [1, 2]});
    original["files"]["rootfs.ext4"]["extension"] = json!("retain");
    original["files"]["rootfs.ext4"]
        .as_object_mut()
        .unwrap()
        .remove("size");
    original["files"]["rootfs.ext4.zst"] = json!({"digest": "old", "size": 1, "extension": "retain", "compression": {"codec": "zstd", "level": 1, "extension": true}});
    std::fs::write(&path, serde_json::to_vec(&original).unwrap()).unwrap();
    let header = fixture.changed_header(Change::Description);
    let mut expected: Value = serde_json::from_slice(&header.manifest_json).unwrap();

    fixture
        .client
        .import_template(tokio_stream::iter([ImportTemplateRequest {
            payload: Some(Payload::Header(header)),
        }]))
        .await
        .unwrap();

    let actual: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    for key in ["digest", "size"] {
        expected["files"]["rootfs.ext4.zst"][key] = actual["files"]["rootfs.ext4.zst"][key].clone();
    }
    for key in ["codec", "level"] {
        expected["files"]["rootfs.ext4.zst"]["compression"][key] =
            actual["files"]["rootfs.ext4.zst"]["compression"][key].clone();
    }
    assert_eq!(
        actual, expected,
        "refresh must preserve unknown fields and omitted defaults"
    );
    let stored = fixture.state().registration["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(serde_json::from_str::<Value>(&stored).unwrap(), actual);
}

#[rstest]
#[case::missing_dimensions(false)]
#[case::invalid_chunk_offset(true)]
#[tokio::test]
async fn rejected_snapshot_stream_discards_prepared_refresh(#[case] valid_dimensions: bool) {
    let mut fixture = ImportFixture::new().await;
    let mut header = fixture.changed_header(Change::Description);
    header.include_snapshot = true;
    header.snapshot_vcpu = valid_dimensions.then_some(1);
    header.snapshot_memory_mib = valid_dimensions.then_some(512);
    let before = fixture.state();
    let requests = [
        ImportTemplateRequest {
            payload: Some(Payload::Header(header)),
        },
        ImportTemplateRequest {
            payload: Some(Payload::Chunk(crate::proto::FileChunk {
                file_name: "snapshot.base".into(),
                offset: 1,
                data: b"invalid offset".to_vec(),
                ..Default::default()
            })),
        },
    ];

    let response = fixture
        .client
        .import_template(tokio_stream::iter(requests))
        .await;

    assert_eq!(response.unwrap_err().code(), Code::InvalidArgument);
    assert_eq!(
        fixture.state(),
        before,
        "incomplete snapshot must not publish build refresh"
    );
    assert!(
        std::fs::read_dir(fixture.directory.parent().unwrap())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".republishing."))
    );
}

#[tokio::test]
async fn new_import_refuses_unknown_schema_without_waiting_for_disk_bytes() {
    use futures_util::StreamExt;
    for metadata in [false, true] {
        let mut fixture = ImportFixture::new().await;
        let mut header = fixture.changed_header(Change::Unchanged);
        header.template.as_mut().unwrap().name = "unsupported-import".into();
        let bytes = if metadata {
            &mut header.metadata_json
        } else {
            &mut header.manifest_json
        };
        let mut document: Value = serde_json::from_slice(bytes).unwrap();
        document["schema_version"] = json!(99);
        *bytes = serde_json::to_vec(&document).unwrap();
        let stream = tokio_stream::iter([ImportTemplateRequest {
            payload: Some(Payload::Header(header)),
        }])
        .chain(futures_util::stream::pending());
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.client.import_template(stream),
        )
        .await
        .expect("unsupported header must be refused before consuming disk data");
        assert_eq!(result.unwrap_err().code(), Code::FailedPrecondition);
        assert!(
            !fixture
                .service
                .artifact_store
                .root
                .join("unsupported-import")
                .exists()
        );
    }
}
