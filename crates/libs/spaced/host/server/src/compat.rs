//! SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: remove legacy fallback and
//! conversions when no older guest can run, launch, or restore. Keep the
//! canonical calls. Unimplemented is the only fallback condition; a transport
//! failure never changes the operation's protocol or repeats a started stream.
#![allow(deprecated)]

use super::*;
use crate::bridge::BridgeGuestOperationBudget;
use crate::protocol::{HostFileInfoStream, bridge_file_type_to_host, relay_stream};
use pplx_space_common_core::types::{GuestPath, GuestUserName};
use pplx_space_common_grpc::{GuestCapability, guest_capability_unsupported_status};

impl SpacedHostService {
    // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: remove this gate, its three
    // Process call sites, and the legacy-guest rejection test once the fleet
    // floor excludes older guests, including launchable templates and restores.
    pub(super) async fn require_interactive_process_control_support(
        &self,
        operation: &'static str,
    ) -> Result<(), Status> {
        let snapshot = match self.guest_ready_status() {
            Some(snapshot) => snapshot,
            None => self
                .refresh_guest_ready_status(Duration::from_millis(1))
                .await?
                .ok_or_else(|| {
                    guest_capability_unsupported_status(
                        GuestCapability::InteractiveProcessControl,
                        format!(
                            "guest version is not known because the guest is not ready; call WaitReady before {operation}"
                        ),
                    )
                })?,
        };
        let supported =
            pplx_space_spaced_common::spaced_guest_supports_interactive_process_control(
                &snapshot.version,
            )
            .map_err(|error| {
                guest_capability_unsupported_status(
                    GuestCapability::InteractiveProcessControl,
                    format!(
                        "guest reported invalid spaced version '{}': {error}",
                        snapshot.version
                    ),
                )
            })?;
        if supported {
            return Ok(());
        }
        Err(guest_capability_unsupported_status(
            GuestCapability::InteractiveProcessControl,
            format!(
                "guest spaced-svc version {} does not support {operation}; rebuild or restore the sandbox with spaced guest version 0.1.2 or newer",
                snapshot.version
            ),
        ))
    }

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

/// A guest that predates `resume_after_output_gap` ignores it and keeps failing
/// a lagged reader. Only a guest that honors the request acknowledges it on its
/// first connected event. Cursor-based replay requires that acknowledgement;
/// cursorless completed replay retains its existing Started/Exit shape.
/// Upstream errors remain errors. Callers that do not opt in never wait here.
pub(super) async fn require_output_gap_resume_ack<S>(
    mut stream: S,
    output_cursor: Option<u64>,
) -> Result<
    impl futures_util::Stream<
        Item = Result<pplx_space_spaced_common::proto::bridge_guest::ProcessEvent, Status>,
    > + Send
    + 'static,
    Status,
>
where
    S: futures_util::Stream<
            Item = Result<pplx_space_spaced_common::proto::bridge_guest::ProcessEvent, Status>,
        > + Send
        + Unpin
        + 'static,
{
    use futures_util::StreamExt;
    use pplx_space_spaced_common::proto::bridge_guest::process_event::Event;

    let first = stream.next().await.transpose()?;
    let acknowledged = match first.as_ref().and_then(|event| event.event.as_ref()) {
        Some(Event::Connected(connected)) => connected.resume_after_output_gap,
        Some(Event::Started(_)) => output_cursor.is_none(),
        _ => false,
    };
    if !acknowledged {
        return Err(guest_capability_unsupported_status(
            GuestCapability::ProcessOutputGapResume,
            "guest spaced-svc does not support ConnectProcess resume_after_output_gap; rebuild or restore the sandbox with a newer spaced guest",
        ));
    }
    Ok(futures_util::stream::iter(first.map(Ok)).chain(stream))
}

pub(super) fn legacy_bridge_stat_to_host(
    path: String,
    response: pplx_space_spaced_common::proto::bridge_guest::LegacyStatResponse,
) -> Result<StatResponse, Status> {
    Ok(StatResponse {
        info: Some(FileInfo {
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

#[cfg(test)]
mod tests;
