use super::*;
use pplx_space_common_core::types::{LinuxPid, LinuxUid};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ExpectedSpacedHostPeer {
    runtime_pid: LinuxPid,
    sandbox_uid: LinuxUid,
}

impl ExpectedSpacedHostPeer {
    pub(super) const fn new(runtime_pid: LinuxPid, sandbox_uid: u32) -> Self {
        Self {
            runtime_pid,
            sandbox_uid: LinuxUid::new(sandbox_uid),
        }
    }
}

pub(crate) fn validate_sandbox_id(sandbox_id: &str) -> Result<(), Status> {
    if sandbox_id.len() < 8 {
        return Err(Status::invalid_argument(
            "sandbox_id must be at least 8 characters",
        ));
    }
    if !sandbox_id
        .bytes()
        .all(|b| b.is_ascii_hexdigit() || b == b'-')
    {
        return Err(Status::invalid_argument(
            "sandbox_id must contain only hex digits and dashes",
        ));
    }
    Ok(())
}

pub(super) fn concrete_sveth_names(nft_rules: &str) -> Vec<String> {
    let mut names = std::collections::BTreeSet::new();

    for line in nft_rules.lines() {
        let mut offset = 0;
        while let Some(search) = line.get(offset..) {
            let Some(relative_start) = search.find("sveth-") else {
                break;
            };
            let start = offset + relative_start;
            let label_start = start + "sveth-".len();
            let Some(label_search) = line.get(label_start..) else {
                break;
            };
            let label = label_search
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect::<String>();

            if (2..=8).contains(&label.len()) {
                let next_is_label_char = line
                    .get(label_start + label.len()..)
                    .unwrap_or_default()
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphanumeric());
                if !next_is_label_char {
                    names.insert(format!("sveth-{label}"));
                }
            }

            offset = label_start;
        }
    }

    names.into_iter().collect()
}

pub(super) fn validate_snapshot_name(snapshot_name: &str) -> Result<(), Status> {
    use std::path::{Component, Path};

    if snapshot_name.is_empty() {
        return Err(Status::invalid_argument("snapshot_name must not be empty"));
    }
    if snapshot_name.contains('/') || snapshot_name.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "invalid snapshot_name: {snapshot_name}"
        )));
    }

    let mut components = Path::new(snapshot_name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(Status::invalid_argument(format!(
            "invalid snapshot_name: {snapshot_name}"
        ))),
    }
}

pub(super) fn validated_template_rootfs(template_dir: &str) -> Result<PathBuf, Status> {
    if template_dir.is_empty() {
        return Err(Status::invalid_argument("template_dir is required"));
    }
    validate_template_rootfs_under(Path::new(TEMPLATE_DIR), Path::new(template_dir))
}

pub(super) fn validate_template_rootfs_under(
    template_root: &Path,
    template_dir: &Path,
) -> Result<PathBuf, Status> {
    let template_root = std::fs::canonicalize(template_root).map_err(|error| {
        // The managed template root is a fixed node path. Absent, not a
        // directory, or unreadable is a node that needs repairing, not a read
        // to retry — same split as the per-template path below.
        let message = format!(
            "template root {} is not available: {error}",
            template_root.display()
        );
        if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(&error) {
            Status::internal(message)
        } else {
            Status::failed_precondition(message)
        }
    })?;
    let template_dir = std::fs::canonicalize(template_dir).map_err(|error| {
        // The path is the caller's, but a path that won't resolve is a node
        // filesystem fact, not a malformed argument — and the activation path
        // propagates `InvalidArgument` as a terminal sandbox-create failure, so
        // labelling a retryable read that way permanently fails a create that
        // would have succeeded. Split on permanence instead: absent gets its own
        // code, an unusable path shape is terminal, a failed read stays
        // retryable.
        if error.kind() == std::io::ErrorKind::NotFound {
            Status::not_found(format!(
                "template dir not found: {}",
                template_dir.display()
            ))
        } else if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(&error)
        {
            Status::internal(format!(
                "cannot resolve template dir {}: {error}",
                template_dir.display()
            ))
        } else {
            Status::failed_precondition(format!(
                "cannot resolve template dir {}: {error}",
                template_dir.display()
            ))
        }
    })?;
    if !template_dir.starts_with(&template_root) {
        return Err(Status::permission_denied(format!(
            "template dir {} is outside managed template root {}",
            template_dir.display(),
            template_root.display()
        )));
    }

    let rootfs = template_dir.join("rootfs.ext4");
    let metadata = std::fs::symlink_metadata(&rootfs).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Status::not_found(format!("template rootfs not found: {}", rootfs.display()))
        } else if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(&error)
        {
            Status::internal(format!(
                "cannot inspect template rootfs {}: {error}",
                rootfs.display()
            ))
        } else {
            Status::failed_precondition(format!(
                "cannot inspect template rootfs {}: {error}",
                rootfs.display()
            ))
        }
    })?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(Status::permission_denied(format!(
            "template rootfs must not be a symlink: {}",
            rootfs.display()
        )));
    }
    if !file_type.is_file() {
        return Err(Status::failed_precondition(format!(
            "template rootfs is not a regular file: {}",
            rootfs.display()
        )));
    }

    Ok(rootfs)
}

/// Allowed program paths for the Launch RPC.
pub(super) const ALLOWED_LAUNCH_PROGRAMS: &[&str] = &[
    pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN,
    pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN,
];

pub(super) fn require_launch_arg<'a>(
    args: &'a [String],
    program: &str,
    flag: &str,
) -> Result<&'a str, Status> {
    if args.iter().any(|argument| {
        argument
            .strip_prefix(flag)
            .is_some_and(|suffix| suffix.starts_with('='))
    }) {
        return Err(Status::invalid_argument(format!(
            "{program} launch must specify {flag} and its value as separate arguments"
        )));
    }
    let mut matches = args.windows(2).filter_map(|pair| {
        (pair.first()? == flag)
            .then(|| pair.get(1).map(String::as_str))
            .flatten()
    });
    let value = matches.next().ok_or_else(|| {
        Status::invalid_argument(format!("{program} launch requires {flag} <value>"))
    })?;
    if matches.next().is_some() {
        return Err(Status::invalid_argument(format!(
            "{program} launch may specify {flag} only once"
        )));
    }
    Ok(value)
}

pub(super) fn require_launch_arg_eq(
    args: &[String],
    program: &str,
    flag: &str,
    expected: &str,
) -> Result<(), Status> {
    let actual = require_launch_arg(args, program, flag)?;
    if actual != expected {
        return Err(Status::permission_denied(format!(
            "{program} launch {flag} must be {expected}, got {actual}"
        )));
    }
    Ok(())
}

fn reject_caller_controlled_launch_arg(
    args: &[String],
    program: &str,
    flag: &str,
) -> Result<(), Status> {
    if args.iter().any(|argument| {
        argument == flag
            || argument
                .strip_prefix(flag)
                .is_some_and(|suffix| suffix.starts_with('='))
    }) {
        return Err(Status::permission_denied(format!(
            "{program} launch {flag} is controlled by node-manager"
        )));
    }
    Ok(())
}

pub(super) fn prepare_sidecar_launch_args(
    program: &str,
    sandbox_id: &str,
    args: &[String],
    expected_spaced_host_peer: Option<ExpectedSpacedHostPeer>,
) -> Result<Vec<String>, Status> {
    if program == pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN {
        require_launch_arg_eq(
            args,
            program,
            "--socket",
            &pplx_space_common_core::host::linux::paths::net_gateway_control_socket(sandbox_id),
        )?;
        require_launch_arg_eq(args, program, "--sandbox-id", sandbox_id)?;
        return Ok(args.to_vec());
    }

    if program == pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN {
        let expected_peer = expected_spaced_host_peer.ok_or_else(|| {
            Status::failed_precondition("spaced-host launch requires a running sandbox runtime")
        })?;
        require_launch_arg_eq(
            args,
            program,
            "--socket",
            &pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
        )?;
        require_launch_arg_eq(args, program, "--sandbox-id", sandbox_id)?;
        require_launch_arg_eq(
            args,
            program,
            "--vsock-uds",
            &format!("{JAIL_DIR}/firecracker/{sandbox_id}/root/v.sock"),
        )?;
        reject_caller_controlled_launch_arg(args, program, "--firecracker-pid")?;
        reject_caller_controlled_launch_arg(args, program, "--firecracker-uid")?;
        require_launch_arg_eq(
            args,
            program,
            "--host-control-socket",
            pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET,
        )?;
        // Node-manager derives and appends the peer itself. This keeps the
        // authorization input out of the caller's control and lets old and new
        // sandbox-manager revisions coexist during a DaemonSet rollout.
        let mut prepared = args.to_vec();
        prepared.extend([
            "--firecracker-pid".to_string(),
            expected_peer.runtime_pid.get().get().to_string(),
            "--firecracker-uid".to_string(),
            expected_peer.sandbox_uid.get().to_string(),
        ]);
        return Ok(prepared);
    }

    Ok(args.to_vec())
}
