use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{GuestPath, Pii, RelativeGuestPath};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{FileEvent, FileEventKind};

use super::unix_ms_to_timestamp;

/// Whether the wire path identifies the observed OS path without losing information.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WatchPathEncoding {
    Exact,
    Lossy,
    Unsupported,
}

#[derive(Clone)]
pub(super) struct WatchEvent {
    absolute_path: Pii<PathBuf>,
    path: Pii<PathBuf>,
    kind: FileEventKind,
    observed_unix_ms: u64,
    is_regular_file: Option<bool>,
    path_encoding: WatchPathEncoding,
}

impl WatchEvent {
    pub(super) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.absolute_path().as_os_str().as_encoded_bytes().len()
            + self.relative_path().as_os_str().as_encoded_bytes().len()
    }

    pub(super) fn new(
        absolute_path: PathBuf,
        path: PathBuf,
        kind: FileEventKind,
        observed_unix_ms: u64,
    ) -> Self {
        let path_encoding = if GuestPath::parse(absolute_path.to_string_lossy().into_owned())
            .is_err()
            || RelativeGuestPath::parse(path.to_string_lossy().into_owned()).is_err()
        {
            WatchPathEncoding::Unsupported
        } else if absolute_path.to_str().is_none() || path.to_str().is_none() {
            WatchPathEncoding::Lossy
        } else {
            WatchPathEncoding::Exact
        };
        Self {
            absolute_path: Pii::new(absolute_path),
            path: Pii::new(path),
            kind,
            observed_unix_ms,
            is_regular_file: None,
            path_encoding,
        }
    }

    pub(super) fn with_is_regular_file(mut self, is_regular_file: Option<bool>) -> Self {
        self.is_regular_file = is_regular_file;
        self
    }

    pub(super) fn relative_path(&self) -> &Path {
        self.path.expose_pii()
    }

    pub(super) fn absolute_path(&self) -> &Path {
        self.absolute_path.expose_pii()
    }

    pub(super) fn kind(&self) -> FileEventKind {
        self.kind
    }

    pub(super) fn observed_unix_ms(&self) -> u64 {
        self.observed_unix_ms
    }

    pub(super) fn is_regular_file(&self) -> Option<bool> {
        self.is_regular_file
    }

    pub(super) fn path_encoding(&self) -> WatchPathEncoding {
        self.path_encoding
    }

    pub(super) fn to_proto(&self) -> FileEvent {
        FileEvent {
            path: self.relative_path().to_string_lossy().into_owned(),
            kind: self.kind.into(),
            observed_at: Some(unix_ms_to_timestamp(self.observed_unix_ms)),
        }
    }
}

#[cfg(test)]
mod tests;
