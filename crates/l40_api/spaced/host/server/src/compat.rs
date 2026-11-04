//! SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: remove legacy fallback and
//! conversions when no older guest can run, launch, or restore. Keep the
//! canonical calls. Unimplemented is the only fallback condition; a transport
//! failure never changes the operation's protocol or repeats a started stream.
#![allow(deprecated)]

use super::*;
use crate::bridge::BridgeGuestOperationBudget;
use crate::protocol::{HostFileInfoStream, bridge_file_type_to_host, relay_stream};
use pplx_space_common_core::types::{GuestPath, GuestUserName};

impl SpacedHostService {
    pub(super) async fn call_bridge_guest_stat(
        &self,
        mut request: StatRequest,
    ) -> Result<StatResponse, Status> {
        request.path = GuestPath::parse(request.path)
            .map_err(invalid_argument_status)?
            .expose()
            .to_owned();
        request
            .user
            .as_ref()
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem(
            "bridge_guest_stat",
            Requirement::Operations(&[GuestOperation::Files]),
            BridgeGuestOperationBudget::FilesystemMetadata,
            move |mut client| {
                let request = request.clone();
                async move {
                    match client.stat_info(request.clone()).await {
                        Err(status) if status.code() == tonic::Code::Unimplemented => {
                            let path = request.path.clone();
                            let legacy = client.stat(request).await?.into_inner();
                            Ok(Response::new(legacy_bridge_stat_to_host(path, legacy)?))
                        }
                        result => result,
                    }
                }
            },
        )
        .await
    }

    pub(super) async fn call_bridge_guest_list(
        &self,
        mut request: ListRequest,
    ) -> Result<HostFileInfoStream, Status> {
        request.path = GuestPath::parse(request.path)
            .map_err(invalid_argument_status)?
            .expose()
            .to_owned();
        request
            .user
            .as_ref()
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        self.call_bridge_guest_filesystem(
            "bridge_guest_list",
            Requirement::Operations(&[GuestOperation::Files]),
            BridgeGuestOperationBudget::FilesystemMetadata,
            move |mut client| {
                let request = request.clone();
                async move {
                    let stream: HostFileInfoStream = match client.list_info(request.clone()).await {
                        Err(status) if status.code() == tonic::Code::Unimplemented => relay_stream(
                            client.list(request).await?.into_inner(),
                            legacy_bridge_dir_entry_to_host,
                        ),
                        Ok(response) => Box::pin(response.into_inner()),
                        Err(status) => return Err(status),
                    };
                    Ok(Response::new(stream))
                }
            },
        )
        .await
    }
}

pub(super) fn legacy_bridge_stat_to_host(
    path: String,
    response: pplx_space_spaced_common::proto::bridge_guest::LegacyStatResponse,
) -> Result<StatResponse, Status> {
    Ok(StatResponse {
        info: Some(FileInfo {
            windows_metadata: None,
            path,
            name: None,
            file_type: bridge_file_type_to_host(response.file_type)?,
            size_bytes: response.size_bytes,
            mode: response.mode,
            modified_at: response.modified_at,
            owner_uid: response.owner_uid,
            owner_gid: response.owner_gid,
            symlink_target: response.symlink_target,
            // A legacy guest does not report it, and absent is not a count.
            link_count: None,
        }),
    })
}

pub(super) fn legacy_bridge_dir_entry_to_host(
    entry: pplx_space_spaced_common::proto::bridge_guest::LegacyDirEntry,
) -> Result<FileInfo, Status> {
    Ok(FileInfo {
        windows_metadata: None,
        name: Some(entry.name),
        path: entry.path,
        file_type: bridge_file_type_to_host(entry.file_type)?,
        size_bytes: entry.size_bytes,
        mode: entry.mode,
        modified_at: entry.modified_at,
        owner_uid: entry.owner_uid,
        owner_gid: entry.owner_gid,
        symlink_target: entry.symlink_target,
        // Unknown on the legacy path; see `legacy_bridge_stat_to_host`.
        link_count: None,
    })
}
