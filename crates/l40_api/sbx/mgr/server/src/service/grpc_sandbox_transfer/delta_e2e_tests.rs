//! Opt-in end-to-end delta transfers through a running template-manager.
//!
//! `scripts/repro-delta-transfer-e2e.sh` runs this test in a private mount
//! namespace whose `/space` is a scratch btrfs, after starting a
//! template-manager binary there and registering a template build with its
//! CLI. Sandbox-manager is this crate's service, served on a temporary socket
//! and called through the generated client, so every transfer crosses the real
//! gRPC boundary and reaches template-manager over its production socket.
//!
//! Without `SPACE_TEST_DELTA_TRANSFER_E2E=1` the test returns at once: it
//! writes under `/space`, which no other environment may.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pplx_space_host_layout::paths;
use tokio_stream::StreamExt;

use crate::proto::sandbox_manager_client::SandboxManagerClient;
use crate::proto::sandbox_manager_server::SandboxManagerServer;
use crate::proto::{
    ExportSandboxRequest, ExportSnapshotRequest, ImportSandboxFileChunk, ImportSandboxHeader,
    ImportSandboxRequest, ImportSnapshotFileChunk, ImportSnapshotHeader, ImportSnapshotRequest,
    SnapshotType, export_sandbox_chunk, export_snapshot_chunk, import_sandbox_request,
    import_snapshot_request,
};
use crate::service::SandboxManagerService;

const OPT_IN: &str = "SPACE_TEST_DELTA_TRANSFER_E2E";
const CHUNK: usize = 1 << 20;
const SNAPSHOT_NAME: &str = "d-1m-20261106T000000Z";

/// What the scenario expects: a verified delta base, or none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expect {
    Delta,
    Full,
}

struct Scenario {
    template: String,
    content_hash: String,
    expect: Expect,
    /// How a delta import is refused when there's no verified base:
    /// `FAILED_PRECONDITION` when template-manager answers that it has none,
    /// `UNAVAILABLE` when it doesn't answer.
    refusal: tonic::Code,
}

fn scenario() -> Option<Scenario> {
    if std::env::var(OPT_IN).as_deref() != Ok("1") {
        eprintln!(
            "SKIPPED: set {OPT_IN}=1 through scripts/repro-delta-transfer-e2e.sh, which runs it \
             against a template-manager in a disposable node root"
        );
        return None;
    }
    // Opting in isn't enough: this test writes under `/space` and dials
    // template-manager's production socket, so it runs only in a private
    // mount namespace, as the script provides. A namespace it can't compare
    // counts as the host's.
    let namespace = |pid: &str| std::fs::read_link(format!("/proc/{pid}/ns/mnt")).ok();
    assert!(
        matches!((namespace("self"), namespace("1")), (Some(own), Some(host)) if own != host),
        "{OPT_IN} is set outside a private mount namespace; run scripts/repro-delta-transfer-e2e.sh"
    );
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    let expect = match var("SPACE_TEST_DELTA_TRANSFER_EXPECT").as_str() {
        "delta" => Expect::Delta,
        "full" => Expect::Full,
        other => panic!("SPACE_TEST_DELTA_TRANSFER_EXPECT must be delta or full, got {other}"),
    };
    let refusal = match std::env::var("SPACE_TEST_DELTA_TRANSFER_REFUSAL").as_deref() {
        Ok("unavailable") => tonic::Code::Unavailable,
        Ok("failed_precondition") | Err(_) => tonic::Code::FailedPrecondition,
        Ok(other) => panic!(
            "SPACE_TEST_DELTA_TRANSFER_REFUSAL must be failed_precondition or unavailable, got {other}"
        ),
    };
    Some(Scenario {
        template: var("SPACE_TEST_DELTA_TRANSFER_TEMPLATE"),
        content_hash: var("SPACE_TEST_DELTA_TRANSFER_HASH"),
        expect,
        refusal,
    })
}

/// A sandbox-manager served on a temporary socket, as a node serves it.
struct Server {
    _dir: tempfile::TempDir,
    client: SandboxManagerClient<tonic::transport::Channel>,
    shutdown: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Server {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("sandbox-manager.sock");
        let listener = pplx_space_unix_socket_bind::bind_unix_socket(
            &socket,
            pplx_space_unix_socket_bind::UnixSocketOptions::new(0o600),
        )
        .unwrap();
        // The test options' resolver refuses every lookup; this one reaches
        // the template-manager serving its production socket.
        let options = crate::service::SandboxManagerServiceOptions {
            template_resolver: std::sync::Arc::new(crate::template_resolver::HostTemplateManager),
            ..crate::service::SandboxManagerServiceOptions::for_test()
        };
        let service = SandboxManagerService::new_with_options(crate::db::test_conn(), options);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let stop = shutdown.clone();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .layer(service.restart_admission_layer())
                .add_service(SandboxManagerServer::new(service))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                    async move { stop.cancelled().await },
                ),
        );
        let channel = pplx_space_common_grpc::connect_uds_channel("sandbox-manager", &socket)
            .await
            .unwrap();
        Self {
            _dir: dir,
            client: SandboxManagerClient::new(channel),
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.task.await.unwrap().unwrap();
    }
}

fn template_rootfs(scenario: &Scenario) -> PathBuf {
    Path::new(&pplx_space_common_core::types::template_build_dir(
        &scenario.template,
        &scenario.content_hash,
        pplx_space_common_core::types::TemplateCachePartition::Legacy,
    ))
    .join("rootfs.ext4")
}

/// `base` with a few pages rewritten, so a delta against it is small and
/// non-empty.
fn changed(base: &[u8], seed: u8) -> Vec<u8> {
    let mut bytes = base.to_vec();
    for (index, offset) in [8 << 20, 20 << 20, 40 << 20].into_iter().enumerate() {
        let end = (offset + 4096).min(bytes.len());
        for byte in &mut bytes[offset.min(end)..end] {
            *byte = seed.wrapping_add(index as u8);
        }
    }
    bytes
}

/// Each file's bytes as non-empty data chunks, then an empty chunk that marks
/// its end, as the exporter frames them.
fn chunks<T>(files: &[(&str, &[u8])], chunk: impl Fn(String, Vec<u8>, u64, bool) -> T) -> Vec<T> {
    let mut out = Vec::new();
    for (name, bytes) in files {
        for (index, data) in bytes.chunks(CHUNK).enumerate() {
            out.push(chunk(
                (*name).to_owned(),
                data.to_vec(),
                (index * CHUNK) as u64,
                false,
            ));
        }
        out.push(chunk(
            (*name).to_owned(),
            Vec::new(),
            bytes.len() as u64,
            true,
        ));
    }
    out
}

fn sandbox_import(
    header: ImportSandboxHeader,
    files: &[(&str, &[u8])],
) -> Vec<ImportSandboxRequest> {
    let mut requests = vec![ImportSandboxRequest {
        payload: Some(import_sandbox_request::Payload::Header(header)),
    }];
    requests.extend(chunks(files, |file_name, data, offset, eof| {
        ImportSandboxRequest {
            payload: Some(import_sandbox_request::Payload::Chunk(
                ImportSandboxFileChunk {
                    file_name,
                    data,
                    offset,
                    eof,
                },
            )),
        }
    }));
    requests
}

fn snapshot_import(
    header: ImportSnapshotHeader,
    files: &[(&str, &[u8])],
) -> Vec<ImportSnapshotRequest> {
    let mut requests = vec![ImportSnapshotRequest {
        payload: Some(import_snapshot_request::Payload::Header(header)),
    }];
    requests.extend(chunks(files, |file_name, data, offset, eof| {
        ImportSnapshotRequest {
            payload: Some(import_snapshot_request::Payload::Chunk(
                ImportSnapshotFileChunk {
                    file_name,
                    data,
                    offset,
                    eof,
                },
            )),
        }
    }));
    requests
}

/// Reassemble streamed files: chunks carry their offset, and holes read as
/// zeros.
fn assemble(files: &mut BTreeMap<String, Vec<u8>>, name: String, offset: u64, data: &[u8]) {
    let file = files.entry(name).or_default();
    let offset = usize::try_from(offset).unwrap();
    if file.len() < offset + data.len() {
        file.resize(offset + data.len(), 0);
    }
    file[offset..offset + data.len()].copy_from_slice(data);
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    hex::encode(sha2::Sha256::digest(bytes))
}

fn source_header(scenario: &Scenario, sandbox_id: &str) -> ImportSandboxHeader {
    ImportSandboxHeader {
        sandbox_id: sandbox_id.to_owned(),
        template_id: scenario.template.clone(),
        content_hash: scenario.content_hash.clone(),
        runtime: "fc".to_owned(),
        owner: "root@local".to_owned(),
        ..Default::default()
    }
}

/// What a source node sends: both export streams, reassembled, and the
/// digests of the rootfs each one must rebuild.
struct Exported {
    sandbox_header: crate::proto::ExportSandboxHeader,
    sandbox_files: BTreeMap<String, Vec<u8>>,
    sandbox_rootfs_digest: String,
    snapshot_header: crate::proto::ExportSnapshotHeader,
    snapshot_files: BTreeMap<String, Vec<u8>>,
    snapshot_rootfs_digest: String,
}

impl Exported {
    /// Hand the streams to an importer in another process: one directory per
    /// stream, its header as protobuf and its files as they arrived.
    fn write(&self, dir: &Path) {
        use prost::Message as _;
        for (stream, header, files, digest) in [
            (
                "sandbox",
                self.sandbox_header.encode_to_vec(),
                &self.sandbox_files,
                &self.sandbox_rootfs_digest,
            ),
            (
                "snapshot",
                self.snapshot_header.encode_to_vec(),
                &self.snapshot_files,
                &self.snapshot_rootfs_digest,
            ),
        ] {
            let root = dir.join(stream);
            std::fs::create_dir_all(root.join("files")).unwrap();
            std::fs::write(root.join("header.pb"), header).unwrap();
            std::fs::write(root.join("rootfs.sha256"), digest).unwrap();
            for (name, bytes) in files {
                std::fs::write(root.join("files").join(name), bytes).unwrap();
            }
        }
    }

    fn read(dir: &Path) -> Self {
        use prost::Message as _;
        let files = |stream: &str| {
            std::fs::read_dir(dir.join(stream).join("files"))
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    (
                        entry.file_name().into_string().unwrap(),
                        std::fs::read(entry.path()).unwrap(),
                    )
                })
                .collect()
        };
        let read = |stream: &str, name: &str| std::fs::read(dir.join(stream).join(name)).unwrap();
        Self {
            sandbox_header: crate::proto::ExportSandboxHeader::decode(
                read("sandbox", "header.pb").as_slice(),
            )
            .unwrap(),
            sandbox_files: files("sandbox"),
            sandbox_rootfs_digest: String::from_utf8(read("sandbox", "rootfs.sha256")).unwrap(),
            snapshot_header: crate::proto::ExportSnapshotHeader::decode(
                read("snapshot", "header.pb").as_slice(),
            )
            .unwrap(),
            snapshot_files: files("snapshot"),
            snapshot_rootfs_digest: String::from_utf8(read("snapshot", "rootfs.sha256")).unwrap(),
        }
    }
}

fn file_list(files: &BTreeMap<String, Vec<u8>>) -> Vec<(&str, &[u8])> {
    files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect()
}

/// The source side: a sandbox imported in full from the template rootfs with
/// a few pages changed, a disk snapshot on it, and both exported with
/// `use_delta`.
async fn export_phase(
    scenario: &Scenario,
    client: &mut SandboxManagerClient<tonic::transport::Channel>,
) -> Exported {
    let template_path = template_rootfs(scenario);
    let base = std::fs::read(&template_path).expect("the registered template rootfs");
    let source_id = pplx_space_common_core::types::SandboxId::new().to_string();
    let source_rootfs = changed(&base, 0x5a);
    client
        .import_sandbox(tokio_stream::iter(sandbox_import(
            source_header(scenario, &source_id),
            &[("rootfs.ext4", &source_rootfs)],
        )))
        .await
        .expect("full import of the source sandbox");
    let snapshot_rootfs = changed(&base, 0xa5);
    client
        .import_snapshot(tokio_stream::iter(snapshot_import(
            ImportSnapshotHeader {
                sandbox_id: source_id.clone(),
                template_id: scenario.template.clone(),
                content_hash: scenario.content_hash.clone(),
                snapshot_type: SnapshotType::Disk as i32,
                target_name: Some(SNAPSHOT_NAME.to_owned()),
                ..Default::default()
            },
            &[("rootfs.ext4", &snapshot_rootfs)],
        )))
        .await
        .expect("full import of the source snapshot");

    let mut stream = client
        .export_sandbox(ExportSandboxRequest {
            sandbox_id: source_id.clone(),
            use_delta: true,
        })
        .await
        .expect("export_sandbox")
        .into_inner();
    let mut sandbox_header = None;
    let mut sandbox_files = BTreeMap::new();
    while let Some(chunk) = stream.next().await {
        match chunk.expect("export stream").payload {
            Some(export_sandbox_chunk::Payload::Header(h)) => sandbox_header = Some(h),
            Some(export_sandbox_chunk::Payload::Chunk(c)) => {
                assemble(&mut sandbox_files, c.file_name, c.offset, &c.data);
            }
            None => {}
        }
    }
    let sandbox_header = sandbox_header.expect("export header");

    let mut stream = client
        .export_snapshot(ExportSnapshotRequest {
            sandbox_id: source_id.clone(),
            snapshot_name: SNAPSHOT_NAME.to_owned(),
            include_rootfs: true,
            use_delta: true,
        })
        .await
        .expect("export_snapshot")
        .into_inner();
    let mut snapshot_header = None;
    let mut snapshot_files = BTreeMap::new();
    while let Some(chunk) = stream.next().await {
        match chunk.expect("snapshot export stream").payload {
            Some(export_snapshot_chunk::Payload::Header(h)) => snapshot_header = Some(h),
            Some(export_snapshot_chunk::Payload::Chunk(c)) => {
                assemble(&mut snapshot_files, c.file_name, c.offset, &c.data);
            }
            None => {}
        }
    }
    let snapshot_header = snapshot_header.expect("snapshot export header");

    match scenario.expect {
        Expect::Delta => {
            if !sandbox_header.is_delta {
                let scratch = tempfile::tempdir().unwrap();
                let diff = pplx_space_node_runtime_core::file_delta::compute_diff(
                    &template_path,
                    Path::new(&paths::sandbox_rootfs_path(&source_id)),
                    &scratch.path().join("diff"),
                );
                panic!(
                    "export sent a full rootfs; compute_diff on its rootfs: {:?}",
                    diff.map(drop)
                );
            }
            for (stream, files) in [("sandbox", &sandbox_files), ("snapshot", &snapshot_files)] {
                assert!(
                    files.contains_key("rootfs.ext4.diff") && !files.contains_key("rootfs.ext4"),
                    "{stream} export: {:?}",
                    files.keys()
                );
                assert!(
                    files["rootfs.ext4.diff"].len() < base.len() / 4,
                    "{stream} delta of {} bytes for a {}-byte rootfs",
                    files["rootfs.ext4.diff"].len(),
                    base.len()
                );
            }
        }
        Expect::Full => {
            assert!(
                !sandbox_header.is_delta,
                "export sent a delta without a verified base"
            );
            for (stream, files) in [("sandbox", &sandbox_files), ("snapshot", &snapshot_files)] {
                assert!(
                    files.contains_key("rootfs.ext4") && !files.contains_key("rootfs.ext4.diff"),
                    "{stream} export: {:?}",
                    files.keys()
                );
            }
        }
    }
    Exported {
        sandbox_header,
        sandbox_files,
        sandbox_rootfs_digest: sha256(&source_rootfs),
        snapshot_header,
        snapshot_files,
        snapshot_rootfs_digest: sha256(&snapshot_rootfs),
    }
}

/// The target side: the exported sandbox imported as a new sandbox and the
/// exported snapshot into it, each rootfs compared with its source. With no
/// verified base, a delta import is refused and leaves no sandbox behind.
async fn import_phase(
    scenario: &Scenario,
    client: &mut SandboxManagerClient<tonic::transport::Channel>,
    exported: &Exported,
) {
    let header = &exported.sandbox_header;
    let target_id = pplx_space_common_core::types::SandboxId::new().to_string();
    let target = ImportSandboxHeader {
        sandbox_id: target_id.clone(),
        template_id: header.template_id.clone(),
        content_hash: header.content_hash.clone(),
        runtime: header.runtime.clone(),
        owner: header.owner.clone(),
        snapshots_enabled: header.snapshots_enabled,
        created_at: header.created_at.clone(),
        guest_uptime_secs: header.guest_uptime_secs,
        is_delta: header.is_delta,
        env: header.env.clone(),
        auto_lifecycle: header.auto_lifecycle.clone(),
        rootfs_size_mb: header.rootfs_size_mb,
        network_policy: header.network_policy.clone(),
        network_policy_generation: header.network_policy_generation,
        network_policy_authority: header.network_policy_authority,
        auto_lifecycle_authority: header.auto_lifecycle_authority,
        execution: header.execution.clone(),
        volume_encryption: header.volume_encryption,
        disk_io_max_operations_per_sec: header.disk_io_max_operations_per_sec,
        disk_io_max_bandwidth_mib_per_sec: header.disk_io_max_bandwidth_mib_per_sec,
    };
    client
        .import_sandbox(tokio_stream::iter(sandbox_import(
            target.clone(),
            &file_list(&exported.sandbox_files),
        )))
        .await
        .expect("import of the exported sandbox");
    let rebuilt = std::fs::read(paths::sandbox_rootfs_path(&target_id)).unwrap();
    assert_eq!(
        sha256(&rebuilt),
        exported.sandbox_rootfs_digest,
        "the imported sandbox's rootfs differs from its source"
    );

    let snapshot = &exported.snapshot_header;
    client
        .import_snapshot(tokio_stream::iter(snapshot_import(
            ImportSnapshotHeader {
                sandbox_id: target_id.clone(),
                template_id: snapshot.template_id.clone(),
                content_hash: snapshot.content_hash.clone(),
                snapshot_metadata: snapshot.snapshot_metadata.clone(),
                snapshot_type: snapshot.snapshot_type,
                target_name: Some(SNAPSHOT_NAME.to_owned()),
            },
            &file_list(&exported.snapshot_files),
        )))
        .await
        .expect("import of the exported snapshot");
    let rebuilt = std::fs::read(
        Path::new(&paths::sandbox_snapshot_dir(&target_id, SNAPSHOT_NAME)).join("rootfs.ext4"),
    )
    .unwrap();
    assert_eq!(
        sha256(&rebuilt),
        exported.snapshot_rootfs_digest,
        "the imported snapshot's rootfs differs from its source"
    );

    if scenario.expect == Expect::Full {
        let refused_id = pplx_space_common_core::types::SandboxId::new().to_string();
        let status = client
            .import_sandbox(tokio_stream::iter(sandbox_import(
                ImportSandboxHeader {
                    sandbox_id: refused_id.clone(),
                    is_delta: true,
                    ..target
                },
                &[("rootfs.ext4.diff", b"not a delta")],
            )))
            .await
            .expect_err("a delta import without a verified base");
        assert_eq!(status.code(), scenario.refusal, "{status}");
        assert!(
            !Path::new(&paths::sandbox_cache_dir(&refused_id))
                .join("instance")
                .exists(),
            "a refused import published an instance"
        );

        // A snapshot delta import is refused the same way, after its body,
        // and leaves neither a snapshot nor its staging dir behind.
        let refused_snapshot = "d-1m-20261106T000001Z";
        let status = client
            .import_snapshot(tokio_stream::iter(snapshot_import(
                ImportSnapshotHeader {
                    sandbox_id: target_id.clone(),
                    template_id: snapshot.template_id.clone(),
                    content_hash: snapshot.content_hash.clone(),
                    snapshot_type: SnapshotType::Disk as i32,
                    target_name: Some(refused_snapshot.to_owned()),
                    ..Default::default()
                },
                &[("rootfs.ext4.diff", b"not a delta")],
            )))
            .await
            .expect_err("a snapshot delta import without a verified base");
        assert_eq!(status.code(), scenario.refusal, "{status}");
        assert!(
            !Path::new(&paths::sandbox_snapshot_dir(&target_id, refused_snapshot)).exists(),
            "a refused snapshot import published a snapshot"
        );
        let staging = format!(".snapshot-import-{target_id}_");
        let leftover: Vec<_> = std::fs::read_dir(paths::SANDBOX_CACHE_DIR)
            .unwrap()
            .filter_map(|entry| entry.unwrap().file_name().into_string().ok())
            .filter(|name| name.starts_with(&staging))
            .collect();
        assert!(leftover.is_empty(), "staging left behind: {leftover:?}");
    }
}

/// Sandbox and snapshot delta transfers against the template build the
/// running template-manager verifies, or the full-rootfs fallback and delta
/// refusal when it verifies none. Each rebuilt rootfs must match its source
/// byte for byte, and the template's own rootfs must not change.
///
/// `SPACE_TEST_DELTA_TRANSFER_PHASE` runs one side only: `export` writes the
/// streams to `SPACE_TEST_DELTA_TRANSFER_HANDOFF`, and `import` reads them
/// there, so one sandbox-manager version can export and another import.
#[tokio::test(flavor = "multi_thread")]
async fn delta_transfers_round_trip_through_a_running_template_manager() {
    let Some(scenario) = scenario() else {
        return;
    };
    let template_path = template_rootfs(&scenario);
    let base_digest = sha256(&std::fs::read(&template_path).expect("the template rootfs"));
    let phase = std::env::var("SPACE_TEST_DELTA_TRANSFER_PHASE").ok();
    let handoff = || {
        PathBuf::from(
            std::env::var("SPACE_TEST_DELTA_TRANSFER_HANDOFF")
                .expect("SPACE_TEST_DELTA_TRANSFER_HANDOFF is required for one phase"),
        )
    };
    let server = Server::start().await;
    let mut client = server.client.clone();
    match phase.as_deref() {
        None => {
            let exported = export_phase(&scenario, &mut client).await;
            import_phase(&scenario, &mut client, &exported).await;
        }
        Some("export") => export_phase(&scenario, &mut client).await.write(&handoff()),
        Some("import") => import_phase(&scenario, &mut client, &Exported::read(&handoff())).await,
        Some(other) => {
            panic!("SPACE_TEST_DELTA_TRANSFER_PHASE must be export or import, got {other}")
        }
    }
    assert_eq!(
        sha256(&std::fs::read(&template_path).unwrap()),
        base_digest,
        "a transfer changed the template's rootfs"
    );
    server.stop().await;
}
