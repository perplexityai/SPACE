//! SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: also requires MIN_HOST 0.1.4.
//! Old hosts keep calling Stat/List; only these adapters emit their encodings.
#![allow(deprecated)]

use pplx_space_spaced_guest_protocol::proto::{bridge_guest, operations};
use tonic::{Response, Status};

pub(super) fn stat(
    response: Response<operations::StatResponse>,
) -> Result<Response<bridge_guest::LegacyStatResponse>, Status> {
    let info = response
        .into_inner()
        .info
        .ok_or_else(|| Status::internal("stat omitted file info"))?;
    Ok(Response::new(bridge_guest::LegacyStatResponse {
        file_type: info.file_type,
        size_bytes: info.size_bytes,
        mode: info.mode,
        modified_at: info.modified_at,
        owner_uid: info.owner_uid,
        owner_gid: info.owner_gid,
        symlink_target: info.symlink_target,
    }))
}

pub(super) fn entry(info: operations::FileInfo) -> Result<bridge_guest::LegacyDirEntry, Status> {
    Ok(bridge_guest::LegacyDirEntry {
        name: info
            .name
            .ok_or_else(|| Status::internal("list entry omitted name"))?,
        path: info.path,
        file_type: info.file_type,
        size_bytes: info.size_bytes,
        mode: info.mode,
        modified_at: info.modified_at,
        owner_uid: info.owner_uid,
        owner_gid: info.owner_gid,
        symlink_target: info.symlink_target,
    })
}
