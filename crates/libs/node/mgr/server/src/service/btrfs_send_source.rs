//! Artifact-owned immutable send inputs. A private parent snapshot keeps retries
//! independent of template GC; per-attempt wire clones avoid exporting an UUID
//! that a same-filesystem receive could still find in the retained source.
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use pplx_space_common_svc::{LocalRole, UdsPeerPrincipal};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::{NodeManagerService, artifact_fs, trusted_paths};
use crate::btrfs;
use crate::cache_ownership::{RETAINED_SOURCE_GID, RETAINED_SOURCE_MODE};
use crate::proto::{
    BtrfsEncodeRootfsSendResponse, BtrfsInspectSendSourceRequest, BtrfsInspectSendSourceResponse,
    BtrfsMaterializeRootfsSendRequest,
};

pub(super) use crate::cache_ownership::RETAINED_SOURCE_BUNDLE as DIRECTORY;
const MANIFEST: &str = "source.json";
const ROOTFS: &str = "rootfs.ext4";
const ALLOWED: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(
        pplx_space_common_core::host::linux::identities::user_names::SPACE_SBXMGR,
    ),
];

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    source_id: Uuid,
    source_uuid: Uuid,
    parent_uuid: Uuid,
    received_uuid: Uuid,
}

/// The caller owns the enclosing artifact until publication or cleanup. No
/// pathname is reopened after pinning it, including when a cache peer renames
/// the enclosing directory. A partial capture remains inside that same owner.
pub(super) fn capture(artifact: &Path, parent: &Path, source: &Path) -> Result<Uuid, Status> {
    let artifact = trusted_paths::open_directory(artifact)?;
    let parent = trusted_paths::open_directory(parent)?;
    let source = trusted_paths::open_directory(source)?;
    capture_open(&artifact, &parent, &source, Uuid::now_v7())
}

pub(super) fn capture_open(
    artifact: &File,
    parent: &File,
    source: &File,
    source_id: Uuid,
) -> Result<Uuid, Status> {
    let received_uuid = btrfs::info(parent)
        .map_err(artifact_fs::error)?
        .1
        .received_uuid
        .ok_or_else(|| Status::failed_precondition("send parent has no received identity"))?;
    nix::sys::stat::mkdirat(
        artifact,
        DIRECTORY,
        nix::sys::stat::Mode::from_bits_truncate(RETAINED_SOURCE_MODE as libc::mode_t),
    )
    .map_err(artifact_fs::error)?;
    let bundle =
        artifact_fs::directory(artifact, DIRECTORY.as_ref()).map_err(artifact_fs::error)?;
    trusted_paths::set_permissions(&bundle, Some(RETAINED_SOURCE_GID), RETAINED_SOURCE_MODE)?;
    let result = (|| {
        // Templates use a deterministic received UUID and stransid=0. Preserve
        // that remote identity on the private, byte-identical parent before
        // sealing it. Never change the original template's identity or flags.
        btrfs::snapshot(parent, &bundle, "parent".as_ref(), false).map_err(artifact_fs::error)?;
        let private_parent =
            artifact_fs::directory(&bundle, "parent".as_ref()).map_err(artifact_fs::error)?;
        btrfs::stamp(&private_parent, received_uuid).map_err(artifact_fs::error)?;
        btrfs::set_readonly(&private_parent, true).map_err(artifact_fs::error)?;
        btrfs::snapshot(source, &bundle, "source".as_ref(), true).map_err(artifact_fs::error)?;
        let private_source =
            artifact_fs::directory(&bundle, "source".as_ref()).map_err(artifact_fs::error)?;
        let manifest = Manifest {
            version: 1,
            source_id,
            source_uuid: subvolume_uuid(&private_source)?,
            parent_uuid: subvolume_uuid(&private_parent)?,
            received_uuid,
        };
        let bytes = serde_json::to_vec(&manifest).map_err(artifact_fs::error)?;
        let mut file = File::from(
            nix::fcntl::openat(
                &bundle,
                MANIFEST,
                nix::fcntl::OFlag::O_WRONLY
                    | nix::fcntl::OFlag::O_CREAT
                    | nix::fcntl::OFlag::O_EXCL
                    | nix::fcntl::OFlag::O_NOFOLLOW
                    | nix::fcntl::OFlag::O_CLOEXEC,
                nix::sys::stat::Mode::from_bits_truncate(0o640),
            )
            .map_err(artifact_fs::error)?,
        );
        file.write_all(&bytes).map_err(artifact_fs::error)?;
        trusted_paths::set_permissions(&file, Some(RETAINED_SOURCE_GID), 0o640)?;
        file.sync_all().map_err(artifact_fs::error)?;
        bundle.sync_all().map_err(artifact_fs::error)?;
        artifact.sync_all().map_err(artifact_fs::error)?;
        Ok(manifest.source_id)
    })();
    if result.is_err()
        && let Err(error) = artifact_fs::remove_at(artifact, DIRECTORY.as_ref())
    {
        tracing::warn!(%error, "partial send-source cleanup deferred to artifact cleanup");
    }
    result
}

fn subvolume_uuid(file: &File) -> Result<Uuid, Status> {
    btrfs::info(file)
        .map_err(artifact_fs::error)?
        .1
        .uuid
        .ok_or_else(|| Status::failed_precondition("send source has no subvolume identity"))
}

/// Why a retained source cannot be opened. Only `NotFound` and the verified
/// mismatches are evidence about the artifact; `Io` is not.
#[derive(Debug, thiserror::Error)]
pub(super) enum SourceError {
    #[error("retained send source is absent")]
    NotFound,
    #[error("retained send source bundle has no manifest")]
    ManifestMissing,
    #[error("send source manifest is not a singly linked regular file")]
    ManifestNotRegular,
    #[error("oversized send source manifest")]
    ManifestOversized,
    #[error("malformed send source manifest")]
    ManifestMalformed(#[source] serde_json::Error),
    #[error("send source identity changed")]
    IdentityChanged,
    #[error("retained send source or parent changed")]
    LineageMismatch,
    #[error("retained send source or parent is writable")]
    NotReadOnly,
    #[error("retained rootfs is missing or not a regular file")]
    RootfsNotRegular,
    #[error("artifact filesystem operation: {0}")]
    Io(#[from] io::Error),
}

impl SourceError {
    /// A definite absence becomes `absent`; every other failure stays I/O.
    fn io(error: io::Error, absent: Self) -> Self {
        if error.kind() == io::ErrorKind::NotFound {
            absent
        } else {
            Self::Io(error)
        }
    }

    /// The inspection contract: absent, unusable, or unknown.
    fn inspection_status(&self) -> Status {
        match self {
            Self::NotFound => Status::not_found(self.to_string()),
            Self::ManifestMissing
            | Self::ManifestNotRegular
            | Self::ManifestOversized
            | Self::ManifestMalformed(_)
            | Self::IdentityChanged
            | Self::LineageMismatch
            | Self::NotReadOnly
            | Self::RootfsNotRegular => Status::failed_precondition(self.to_string()),
            Self::Io(_) => Status::internal(self.to_string()),
        }
    }
}

/// Staging, reading, and materialization cannot use any failed source.
impl From<SourceError> for Status {
    fn from(error: SourceError) -> Self {
        Status::failed_precondition(error.to_string())
    }
}

pub(super) struct SendSource {
    bundle: File,
    pub(super) parent: File,
    pub(super) source: File,
}

impl SendSource {
    pub(super) fn open(artifact: &File, expected: Uuid) -> Result<Self, SourceError> {
        let bundle = artifact_fs::directory(artifact, DIRECTORY.as_ref())
            .map_err(|error| SourceError::io(error, SourceError::NotFound))?;
        let manifest = open_file(&bundle, MANIFEST)
            .map_err(|error| SourceError::io(error, SourceError::ManifestMissing))?;
        let metadata = manifest.metadata()?;
        if !metadata.is_file() || metadata.nlink() > 1 {
            return Err(SourceError::ManifestNotRegular);
        }
        let mut bytes = Vec::new();
        manifest.take(4097).read_to_end(&mut bytes)?;
        if bytes.len() > 4096 {
            return Err(SourceError::ManifestOversized);
        }
        let manifest: Manifest =
            serde_json::from_slice(&bytes).map_err(SourceError::ManifestMalformed)?;
        if manifest.version != 1 || manifest.source_id != expected {
            return Err(SourceError::IdentityChanged);
        }
        let subvolume = |name: &str| {
            let directory = artifact_fs::directory(&bundle, name.as_ref())
                .map_err(|error| SourceError::io(error, SourceError::LineageMismatch))?;
            if !btrfs::is_subvolume(&directory)? {
                return Err(SourceError::LineageMismatch);
            }
            let (_, info) = btrfs::info(&directory)?;
            Ok((directory, info))
        };
        let (parent, parent_info) = subvolume("parent")?;
        let (source, source_info) = subvolume("source")?;
        if parent_info.uuid != Some(manifest.parent_uuid)
            || source_info.uuid != Some(manifest.source_uuid)
            || parent_info.received_uuid != Some(manifest.received_uuid)
        {
            return Err(SourceError::LineageMismatch);
        }
        if !btrfs::is_readonly(&parent)? || !btrfs::is_readonly(&source)? {
            return Err(SourceError::NotReadOnly);
        }
        Ok(Self {
            bundle,
            parent,
            source,
        })
    }

    /// Length of the retained rootfs image. A send stream has its own length.
    pub(super) fn rootfs_bytes(&self) -> Result<u64, SourceError> {
        let metadata = nix::sys::stat::fstatat(
            &self.source,
            ROOTFS,
            nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(|error| SourceError::io(error.into(), SourceError::RootfsNotRegular))?;
        if metadata.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(SourceError::RootfsNotRegular);
        }
        u64::try_from(metadata.st_size).map_err(|_| SourceError::RootfsNotRegular)
    }

    pub(super) fn send(&self, output: &File) -> Result<(), Status> {
        let name = format!(".send-{}", Uuid::now_v7());
        nix::sys::stat::mkdirat(
            &self.bundle,
            name.as_str(),
            nix::sys::stat::Mode::from_bits_truncate(0o700),
        )
        .map_err(artifact_fs::error)?;
        let result = (|| {
            let attempt =
                artifact_fs::directory(&self.bundle, name.as_ref()).map_err(artifact_fs::error)?;
            btrfs::snapshot(&self.source, &attempt, "instance.ro".as_ref(), true)
                .map_err(artifact_fs::error)?;
            let wire = artifact_fs::directory(&attempt, "instance.ro".as_ref())
                .map_err(artifact_fs::error)?;
            btrfs::send(&self.parent, &wire, output).map_err(artifact_fs::error)
        })();
        // Kernel send has returned before releasing the wire clone, even if
        // the RPC waiter disappeared. Artifact cleanup owns interrupted attempts.
        let cleanup = artifact_fs::remove_at(&self.bundle, name.as_ref());
        if let Err(error) = cleanup {
            tracing::warn!(%error, "send wire-clone cleanup deferred to artifact cleanup");
            return result.and(Err(error));
        }
        result
    }
}

fn open_file(directory: &File, name: &str) -> io::Result<File> {
    nix::fcntl::openat(
        directory,
        name,
        nix::fcntl::OFlag::O_RDONLY
            | nix::fcntl::OFlag::O_NOFOLLOW
            | nix::fcntl::OFlag::O_NONBLOCK
            | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map(File::from)
    .map_err(Into::into)
}

/// Inspect a retained source in place. A missing artifact or bundle is
/// reported absent. A bundle without its manifest is unusable, not absent:
/// capture writes and syncs the manifest last, before it reports the identity
/// a caller inspects, so that manifest was lost after capture and no later
/// inspection will find it. An unreadable path proves nothing.
pub(super) fn inspect(artifact: &Path, expected: Uuid) -> Result<u64, Status> {
    let directory = trusted_paths::open_directory(artifact).map_err(|status| {
        if status.code() == tonic::Code::NotFound {
            SourceError::NotFound.inspection_status()
        } else {
            Status::internal(format!("open send source artifact: {}", status.message()))
        }
    })?;
    SendSource::open(&directory, expected)
        .and_then(|source| source.rootfs_bytes())
        .map_err(|error| error.inspection_status())
}

pub(super) fn materialize(
    artifact: &Path,
    expected: Uuid,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<u64, Status> {
    let directory = trusted_paths::open_directory(artifact)?;
    let source = SendSource::open(&directory, expected)?;
    let output =
        artifact_fs::write_atomic_at(&directory, "rootfs.btrfs-send".as_ref(), |output| {
            source.send(output)?;
            if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
                return Err(Status::cancelled("send source materialization cancelled"));
            }
            trusted_paths::set_permissions(
                output,
                Some(pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW),
                0o660,
            )
        })?;
    output
        .metadata()
        .map(|m| m.len())
        .map_err(artifact_fs::error)
}

impl NodeManagerService {
    pub(super) async fn btrfs_materialize_rootfs_send(
        &self,
        request: Request<BtrfsMaterializeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        self.authorize_peer(&request, "BtrfsMaterializeRootfsSend", ALLOWED)?;
        let req = request.into_inner();
        let artifact = crate::runtime_paths::validate_create_snapshot_dest(
            &req.sandbox_id,
            &req.artifact_dir,
        )?;
        let source_id = req
            .source_id
            .parse::<Uuid>()
            .map_err(|_| Status::invalid_argument("invalid send source identity"))?;
        self.run_artifact_mutation(&req.sandbox_id, move |service| async move {
            let owner = service.artifact_work.clone();
            let stream_bytes = service
                .off_runtime("regenerate rootfs send", move || {
                    materialize(
                        &artifact,
                        source_id,
                        owner.as_ref().map(|owner| &owner.cancel),
                    )
                })
                .await?;
            Ok(Response::new(BtrfsEncodeRootfsSendResponse {
                stream_bytes,
                source_id: Some(source_id.to_string()),
                rootfs_bytes: None,
            }))
        })
        .await
    }

    pub(super) async fn btrfs_inspect_send_source(
        &self,
        request: Request<BtrfsInspectSendSourceRequest>,
    ) -> Result<Response<BtrfsInspectSendSourceResponse>, Status> {
        self.authorize_peer(&request, "BtrfsInspectSendSource", ALLOWED)?;
        let req = request.into_inner();
        super::validation::validate_sandbox_id(&req.sandbox_id)?;
        let artifact = crate::runtime_paths::validate_create_snapshot_dest(
            &req.sandbox_id,
            &req.artifact_dir,
        )?;
        let source_id = req
            .source_id
            .parse::<Uuid>()
            .map_err(|_| Status::invalid_argument("invalid send source identity"))?;
        // Read-only opens and info ioctls: their own small lane, neither the
        // filesystem permits that suspend and restore receive share nor the
        // diagnostic lane. The closure owns admission, so an abandoned call
        // still counts until its filesystem work returns.
        let permit = self
            .send_source_inspection_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("send source inspections busy; retry"))?;
        let rootfs_bytes = self
            .off_runtime("inspect retained send source", move || {
                let _permit = permit;
                inspect(&artifact, source_id)
            })
            .await?;
        Ok(Response::new(BtrfsInspectSendSourceResponse {
            rootfs_bytes,
        }))
    }
}

#[cfg(test)]
mod tests;
