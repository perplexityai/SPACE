use super::cleanup_helpers::*;
use super::disk::read_pid_from_symlink;
use super::*;
use pplx_space_common_core::host::linux::gateway;
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::LinuxUid;

pub(super) fn sidecar_launch_uid(
    program: &str,
    uid: u32,
    gid: u32,
    sandbox_uid: u32,
    sandbox_id: &str,
) -> Result<u32, Status> {
    if program == paths::NET_GATEWAY_SVC_BIN
        && gateway::gateway_uid(LinuxUid::new(sandbox_uid)) == Some(LinuxUid::new(uid))
        && gid == uid
    {
        return Ok(uid);
    }
    validate_sandbox_uid_gid(sandbox_id, uid, gid, sandbox_uid)
}

/// Preserve an existing gateway's identity, including cgroup-only recovery.
/// Reject foreign credentials before TPROXY/socket mutation; the supervisor
/// repeats the resolved credential check under its process launch lock.
pub(super) fn resolve_sidecar_launch_uid(
    sandbox_id: &str,
    program: &str,
    uid: u32,
    gid: u32,
    sandbox_uid: u32,
) -> Result<u32, Status> {
    let binary = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Status::invalid_argument("sidecar program has no filename"))?;
    let registered = read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/{binary}.pid"));
    let mut pids = match crate::cgroup::sidecar_cgroup_process_ids(sandbox_id) {
        Ok(pids) => pids,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(Status::internal(format!("inspect sidecar cgroup: {error}"))),
    };
    if let Some(pid) = registered {
        pids.push(pid);
    }
    pids.sort_unstable();
    pids.dedup();
    let mut live_uid = None;
    for pid in pids {
        let handle = match launch::ProcessHandle::open(pid) {
            Ok(handle) => handle,
            Err(error) if error.is_not_found() => continue,
            Err(error) => return Err(Status::unavailable(format!("pin sidecar: {error}"))),
        };
        match handle
            .program_matches(program)
            .map_err(|error| Status::unavailable(format!("inspect sidecar executable: {error}")))?
        {
            None => continue,
            Some(false) if registered != Some(pid) => continue,
            Some(false) => {
                return Err(Status::failed_precondition(
                    "registered sidecar binary mismatch",
                ));
            }
            Some(true) => {}
        }
        match handle
            .sandbox_ownership(sandbox_id)
            .map_err(|error| Status::unavailable(format!("inspect sidecar ownership: {error}")))?
        {
            launch::ProcessOwnership::Gone => continue,
            launch::ProcessOwnership::Foreign => {
                return Err(Status::failed_precondition(
                    "live sidecar ownership mismatch",
                ));
            }
            launch::ProcessOwnership::Owned => {}
        }
        let Some(actual_uid) = existing_sidecar_uid(&handle, program, uid, gid, sandbox_uid)?
        else {
            continue;
        };
        if live_uid.is_some_and(|existing| existing != actual_uid) {
            return Err(Status::failed_precondition(
                "conflicting live gateway identities",
            ));
        }
        live_uid = Some(actual_uid);
    }
    Ok(live_uid.unwrap_or(uid))
}

fn existing_sidecar_uid(
    handle: &launch::ProcessHandle,
    program: &str,
    uid: u32,
    gid: u32,
    sandbox_uid: u32,
) -> Result<Option<u32>, Status> {
    let matches = |uid, gid| {
        handle.credentials_match(uid, gid).map_err(|error| {
            Status::unavailable(format!("inspect live sidecar credentials: {error}"))
        })
    };
    match matches(uid, gid)? {
        None => return Ok(None),
        Some(true) => return Ok(Some(uid)),
        Some(false) => {}
    }
    if program == paths::NET_GATEWAY_SVC_BIN {
        let alternate = if uid == sandbox_uid {
            gateway::gateway_uid(LinuxUid::new(sandbox_uid))
                .ok_or_else(|| Status::failed_precondition("invalid gateway owner identity"))?
                .get()
        } else {
            sandbox_uid
        };
        match matches(alternate, alternate)? {
            None => return Ok(None),
            Some(true) => return Ok(Some(alternate)),
            Some(false) => {}
        }
    }
    Err(Status::failed_precondition(
        "live sidecar identity mismatch",
    ))
}

pub(super) fn prepare_gateway_socket(
    sandbox_id: &str,
    uid: u32,
    isolated: bool,
) -> Result<(), Status> {
    let alias = PathBuf::from(paths::net_gateway_control_socket(sandbox_id));
    let target = PathBuf::from(paths::isolated_net_gateway_control_socket(sandbox_id));
    prepare_gateway_socket_at(&alias, &target, uid, isolated)
}

fn prepare_gateway_socket_at(
    alias: &Path,
    target: &Path,
    uid: u32,
    isolated: bool,
) -> Result<(), Status> {
    if !isolated {
        match std::fs::symlink_metadata(alias) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if std::fs::read_link(alias).map_err(|error| Status::internal(error.to_string()))?
                    != target
                {
                    return Err(Status::failed_precondition(
                        "unexpected gateway socket alias",
                    ));
                }
                require_inactive_gateway_socket(alias)?;
                std::fs::remove_file(alias).map_err(|error| Status::internal(error.to_string()))?;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Status::internal(format!("inspect gateway socket: {error}"))),
        }
        return Ok(());
    }
    // A correct alias needs no replacement, including when adopting a live
    // isolated gateway. Otherwise inventory must agree with the listener before
    // changing paths. The caller holds the sidecar launch serialization guard.
    let existing_alias = std::fs::read_link(alias).is_ok_and(|existing| existing == target);
    if !existing_alias {
        match std::fs::symlink_metadata(alias) {
            Ok(metadata) => {
                use std::os::unix::fs::FileTypeExt;
                if !metadata.file_type().is_socket() {
                    return Err(Status::failed_precondition(
                        "unexpected gateway socket path",
                    ));
                }
                require_inactive_gateway_socket(alias)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Status::unavailable(format!(
                    "inspect gateway socket: {error}"
                )));
            }
        }
    }
    let directory = target
        .parent()
        .expect("canonical gateway socket has a parent");
    let ancestor = directory.parent().expect("gateway root has a parent");
    for path in [ancestor, directory] {
        validate_sandbox_artifact_dir_shape(path)?;
        std::fs::create_dir_all(path)
            .map_err(|error| Status::internal(format!("create gateway directory: {error}")))?;
    }
    chown_path_blocking(&ancestor.display().to_string(), 0, 0)?;
    chmod_path_blocking(&ancestor.display().to_string(), 0o755)?;
    chown_path_blocking(
        &directory.display().to_string(),
        uid,
        gids::SPACE_SIDECARCLIENTS,
    )?;
    chmod_path_blocking(&directory.display().to_string(), 0o2770)?;
    if existing_alias {
        return Ok(());
    }
    let staging = alias.with_extension(format!("{}.tmp", uuid::Uuid::now_v7()));
    std::os::unix::fs::symlink(target, &staging)
        .map_err(|error| Status::internal(format!("stage gateway socket alias: {error}")))?;
    if let Err(error) = std::fs::rename(&staging, alias) {
        if let Err(cleanup) = std::fs::remove_file(&staging) {
            tracing::warn!(%cleanup, "remove failed gateway alias staging path");
        }
        return Err(Status::internal(format!(
            "publish gateway socket alias: {error}"
        )));
    }
    Ok(())
}

/// Bound listener inspection to 250 ms, including a full accept backlog.
/// Only an absent/refused socket is evidence that replacement is safe.
fn require_inactive_gateway_socket(path: &Path) -> Result<(), Status> {
    let probe = || -> std::io::Result<()> {
        let address = socket2::SockAddr::unix(path)?;
        let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        socket.connect_timeout(&address, Duration::from_millis(250))
    };
    match probe() {
        Ok(()) => Err(Status::failed_precondition(
            "gateway socket has a live listener; reconcile process ownership before replacement",
        )),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(Status::unavailable(format!(
            "cannot establish that gateway socket is inactive: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests;
