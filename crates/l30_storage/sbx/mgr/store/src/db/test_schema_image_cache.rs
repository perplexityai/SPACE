//! A copy of the migrated test schema image shared by the processes of one
//! nextest run.
//!
//! nextest runs every test in its own process, so a process-wide image is
//! still rebuilt once per test. The first process to build it leaves a copy
//! beside the test executable, and later processes of the same run read it.
//!
//! The copy starts with a key naming the run and the build it came from: the
//! nextest run id, and the executable's length and modification time. A copy
//! from another run, or from before a rebuild, therefore carries a different
//! key and is rebuilt rather than read. Writers publish through a rename, so
//! a reader sees either the previous file or a complete new one. Outside
//! nextest there is no run to share with, and every test of a binary already
//! runs in one process, so there is no copy at all.

use std::fs;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

pub(super) struct SchemaImageCache {
    path: PathBuf,
    key: Vec<u8>,
}

impl SchemaImageCache {
    /// `None` outside nextest or when the executable cannot be identified; the
    /// caller then builds its own image exactly as it would without a cache.
    pub(super) fn for_current_exe() -> Option<Self> {
        let run_id = std::env::var("NEXTEST_RUN_ID").ok()?;
        let exe = std::env::current_exe().ok()?;
        let metadata = fs::metadata(&exe).ok()?;
        let modified = metadata
            .modified()
            .ok()?
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_nanos();
        let mut path = exe.into_os_string();
        path.push(".test-schema-image");
        Some(Self {
            path: PathBuf::from(path),
            key: format!("{run_id}:{}:{modified}\n", metadata.len()).into_bytes(),
        })
    }

    pub(super) fn read(&self) -> Option<Vec<u8>> {
        let bytes = fs::read(&self.path).ok()?;
        bytes.strip_prefix(self.key.as_slice()).map(<[u8]>::to_vec)
    }

    /// Best effort: a copy that cannot be written only means the next process
    /// builds its own image too.
    pub(super) fn write(&self, image: &[u8]) {
        let mut staging = self.path.clone().into_os_string();
        staging.push(format!(".{}", std::process::id()));
        let staging = PathBuf::from(staging);
        let mut contents = Vec::with_capacity(self.key.len() + image.len());
        contents.extend_from_slice(&self.key);
        contents.extend_from_slice(image);
        if fs::write(&staging, contents).is_err() || fs::rename(&staging, &self.path).is_err() {
            let _ = fs::remove_file(&staging);
        }
    }
}
