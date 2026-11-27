use std::fs;
use std::path::Path;

use pplx_space_artifact_model::SandboxArtifactMetadata;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_spec::ExecutionMetadata;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_template_model::TemplateName;

use super::*;

/// The test process stands in for root and sandbox-manager alike, and the
/// temp directory isn't btrfs.
fn staged() -> StagedDisk {
    let uid = nix::unistd::geteuid().as_raw();
    let gid = nix::unistd::getegid().as_raw();
    StagedDisk {
        owners: InstanceOwners {
            creator: uid,
            sandbox_manager: (uid, gid),
        },
        metadata_gid: gid,
        require_subvolume: false,
    }
}

const DIGEST: &str = "sha256:5e0f1d2c3b4a59687766554433221100ffeeddccbbaa99887766554433221100";

/// The checkpoint every test stages and asks for.
fn requested() -> CheckpointIdentity {
    CheckpointIdentity {
        sandbox_id: "01a1bf83-51ba-712f-9439-2d7c80fba73f".parse().unwrap(),
        local_artifact_id: "01a1bf83-51ba-712f-9439-2d7c80fba740".parse().unwrap(),
        template: Some(DIGEST.parse().unwrap()),
    }
}

fn write_sidecar(root: &Path, sidecar: serde_json::Value) {
    fs::write(
        root.join("snapshot").join("snapshot.json"),
        serde_json::to_vec(&sidecar).unwrap(),
    )
    .unwrap();
}

fn sidecar() -> serde_json::Value {
    let checkpoint = requested();
    serde_json::json!({
        "sandbox_id": checkpoint.sandbox_id.to_string(),
        "local_artifact_id": checkpoint.local_artifact_id.to_string(),
        "source": {"content_hash": DIGEST},
    })
}

/// A sandbox directory in a temporary directory reached through no symlink,
/// which the disk checks refuse to follow: on macOS `$TMPDIR` is under the
/// `/var` link to `/private/var`.
fn sandbox_dir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap()
}

/// A sandbox directory holding the requested staged checkpoint, with this
/// COW, or none.
fn staged_checkpoint_with(cow: Option<&[u8]>) -> tempfile::TempDir {
    let root = sandbox_dir();
    fs::create_dir(root.path().join("snapshot")).unwrap();
    write_sidecar(root.path(), sidecar());
    if let Some(cow) = cow {
        fs::write(root.path().join("snapshot").join(ROOTFS_COW_FILE), cow).unwrap();
    }
    root
}

fn staged_checkpoint(cow: &[u8]) -> tempfile::TempDir {
    staged_checkpoint_with(Some(cow))
}

fn instance_cow(root: &Path) -> std::path::PathBuf {
    root.join("instance").join(ROOTFS_COW_FILE)
}

#[test]
fn an_encrypted_checkpoint_gets_its_cow_and_metadata_once() {
    let root = staged_checkpoint(b"checkpoint cow");

    assert_eq!(
        materialize(
            root.path(),
            &staged(),
            &requested(),
            Some(b"{\"m\":1}"),
            true
        )
        .unwrap(),
        (StagedDiskPart::Written, StagedDiskPart::Written)
    );
    assert_eq!(
        fs::read(instance_cow(root.path())).unwrap(),
        b"checkpoint cow"
    );
    assert_eq!(
        fs::read(root.path().join("metadata.json")).unwrap(),
        b"{\"m\":1}"
    );

    // A boot wrote to the laid COW and failed, leaving the row suspended. The
    // retry lays the checkpoint's COW again, as a restore would, and keeps
    // the metadata.
    fs::write(instance_cow(root.path()), b"an earlier boot's writes").unwrap();
    assert_eq!(
        materialize(
            root.path(),
            &staged(),
            &requested(),
            Some(b"{\"m\":2}"),
            true
        )
        .unwrap(),
        (StagedDiskPart::Kept, StagedDiskPart::Written)
    );
    assert_eq!(
        fs::read(instance_cow(root.path())).unwrap(),
        b"checkpoint cow"
    );
    assert_eq!(
        fs::read(root.path().join("metadata.json")).unwrap(),
        b"{\"m\":1}"
    );
}

#[test]
fn an_unencrypted_checkpoint_gets_only_metadata() {
    let root = staged_checkpoint_with(None);

    assert_eq!(
        materialize(root.path(), &staged(), &requested(), Some(b"{}"), false).unwrap(),
        (StagedDiskPart::Written, StagedDiskPart::NotRequested)
    );
    assert!(!root.path().join("instance").exists());
}

#[test]
fn an_empty_or_linked_instance_cow_is_replaced_by_the_checkpoints() {
    for plant in ["empty", "link"] {
        let root = staged_checkpoint(b"checkpoint cow");
        fs::create_dir(root.path().join("instance")).unwrap();
        let elsewhere = root.path().join("elsewhere");
        fs::write(&elsewhere, b"not this sandbox's").unwrap();
        if plant == "empty" {
            fs::write(instance_cow(root.path()), b"").unwrap();
        } else {
            std::os::unix::fs::symlink(&elsewhere, instance_cow(root.path())).unwrap();
        }

        assert_eq!(
            materialize(root.path(), &staged(), &requested(), None, true).unwrap(),
            (StagedDiskPart::NotRequested, StagedDiskPart::Written),
            "{plant}"
        );
        let laid = fs::symlink_metadata(instance_cow(root.path())).unwrap();
        assert!(laid.file_type().is_file(), "{plant}");
        assert_eq!(
            fs::read(instance_cow(root.path())).unwrap(),
            b"checkpoint cow"
        );
        assert_eq!(
            fs::read(&elsewhere).unwrap(),
            b"not this sandbox's",
            "{plant}"
        );
    }
}

#[test]
fn a_checkpoint_without_a_usable_cow_is_refused() {
    let empty = staged_checkpoint(b"");
    let none = staged_checkpoint_with(None);
    let unstaged = sandbox_dir();

    for root in [&empty, &none, &unstaged] {
        let error =
            materialize(root.path(), &staged(), &requested(), Some(b"{}"), true).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(!instance_cow(root.path()).exists());
        assert!(!root.path().join("metadata.json").exists());
    }
}

#[test]
fn metadata_must_name_the_sandbox() {
    let id = SandboxId::new();
    let json = serde_json::to_string(&SandboxArtifactMetadata::new(
        id,
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("base").unwrap(),
        None,
    ))
    .unwrap();

    assert_eq!(
        validated_metadata(&id.to_string(), &json).unwrap(),
        Some(json.clone().into_bytes())
    );
    assert_eq!(validated_metadata(&id.to_string(), "").unwrap(), None);
    for (sandbox, json) in [
        (SandboxId::new().to_string(), json.as_str()),
        (id.to_string(), "{"),
    ] {
        assert_eq!(
            validated_metadata(&sandbox, json).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

/// The staged checkpoint must be the one asked for, by its sidecar, before
/// anything is written: another sandbox's, another artifact, another
/// template, or a sidecar that names no artifact, or no template for an
/// encrypted sandbox, is refused.
/// A named change to a staged sidecar.
type SidecarChange = (&'static str, fn(&mut serde_json::Value));

#[test]
fn only_the_requested_checkpoint_is_prepared() {
    let cases: [SidecarChange; 6] = [
        ("another sandbox", |s| {
            s["sandbox_id"] = "01a1bf83-51ba-712f-9439-2d7c80fba799".into();
        }),
        ("another artifact", |s| {
            s["local_artifact_id"] = "01a1bf83-51ba-712f-9439-2d7c80fba799".into();
        }),
        ("another template", |s| {
            s["source"]["content_hash"] =
                "sha256:0000000000000000000000000000000000000000000000000000000000000001".into();
        }),
        ("no artifact", |s| {
            s.as_object_mut().unwrap().remove("local_artifact_id");
        }),
        ("no template", |s| {
            s["source"].as_object_mut().unwrap().remove("content_hash");
        }),
        ("not json", |s| *s = serde_json::Value::Null),
    ];
    for (case, change) in cases {
        let root = staged_checkpoint(b"checkpoint cow");
        let mut changed = sidecar();
        change(&mut changed);
        write_sidecar(root.path(), changed);

        let error =
            materialize(root.path(), &staged(), &requested(), Some(b"{}"), true).unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{case}: {error}"
        );
        assert!(!root.path().join("instance").exists(), "{case}");
        assert!(!root.path().join("metadata.json").exists(), "{case}");
    }
    let unsidecarred = staged_checkpoint(b"checkpoint cow");
    fs::remove_file(unsidecarred.path().join("snapshot/snapshot.json")).unwrap();
    let error = materialize(unsidecarred.path(), &staged(), &requested(), None, true).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(!unsidecarred.path().join("instance").exists());
}

/// An unencrypted checkpoint's sidecar may name no template, and asked for
/// with none its metadata is written; an encrypted one's must name one.
#[test]
fn an_unencrypted_checkpoint_may_name_no_template() {
    let root = staged_checkpoint(b"checkpoint cow");
    let mut unnamed = sidecar();
    unnamed["source"]
        .as_object_mut()
        .unwrap()
        .remove("content_hash");
    write_sidecar(root.path(), unnamed);
    let requested = CheckpointIdentity {
        template: None,
        ..requested()
    };

    let error = materialize(root.path(), &staged(), &requested, Some(b"{}"), true).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(!root.path().join("metadata.json").exists());

    materialize(root.path(), &staged(), &requested, Some(b"{}"), false).unwrap();
    assert!(root.path().join("metadata.json").exists());
}
