//! The release a Linux kernel image announces about itself.
//!
//! Every Linux image carries `linux_banner`, `Linux version <release>
//! (<builder>) ...`, the line the kernel prints first at boot. An uncompressed
//! `vmlinux` or arm64 `Image`, which is what microVM runtimes boot, holds it as
//! plain bytes, so the release can be read without running anything. A
//! compressed image hides it, and its release stays unknown.
//!
//! The scanner takes the image in chunks, so it rides along with a pass that
//! is already hashing the file.

use pplx_space_common_core::types::ArtifactVersion;

const BANNER_PREFIX: &[u8] = b"Linux version ";
/// What follows the release in the banner.
const BANNER_RELEASE_END: &[u8] = b" (";
/// The kernel's own bound on a release string (`__NEW_UTS_LEN`).
const MAX_RELEASE_BYTES: usize = 64;

/// Finds the banner release in an image fed to it in order, chunk by chunk.
#[derive(Debug, Default)]
pub struct KernelReleaseScanner {
    /// Bytes not yet ruled out: the tail of what was fed that could still
    /// begin a banner.
    window: Vec<u8>,
    release: Option<ArtifactVersion>,
}

enum Search {
    Found(ArtifactVersion),
    /// A candidate starts here but the bytes fed so far end inside it.
    Incomplete(usize),
    NotFound,
}

impl KernelReleaseScanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the next bytes of the image.
    pub fn update(&mut self, chunk: &[u8]) {
        if self.release.is_some() {
            return;
        }
        self.window.extend_from_slice(chunk);
        let keep_from = match search(&self.window) {
            Search::Found(release) => {
                self.release = Some(release);
                self.window = Vec::new();
                return;
            }
            Search::Incomplete(start) => start,
            // Only a tail shorter than the prefix can still begin one.
            Search::NotFound => self
                .window
                .len()
                .saturating_sub(BANNER_PREFIX.len().saturating_sub(1)),
        };
        self.window.drain(..keep_from);
    }

    /// The release, with the `+` a build from a modified tree appends
    /// removed: `6.1.155+` reads as `6.1.155`. `None` when the image carries
    /// no banner.
    pub fn finish(self) -> Option<ArtifactVersion> {
        self.release
    }
}

/// The banner release in a whole image held in memory.
pub fn kernel_release(image: &[u8]) -> Option<ArtifactVersion> {
    let mut scanner = KernelReleaseScanner::new();
    scanner.update(image);
    scanner.finish()
}

/// The first complete banner in `bytes`, or where an unfinished one starts.
fn search(bytes: &[u8]) -> Search {
    let mut from = 0;
    while let Some(start) = find_prefix(bytes, from) {
        let candidate = bytes.get(start + BANNER_PREFIX.len()..).unwrap_or_default();
        let release_len = candidate
            .iter()
            .take(MAX_RELEASE_BYTES + 1)
            .take_while(|byte| is_release_byte(**byte))
            .count();
        if release_len <= MAX_RELEASE_BYTES {
            let after = candidate.get(release_len..).unwrap_or_default();
            if after.len() < BANNER_RELEASE_END.len() && BANNER_RELEASE_END.starts_with(after) {
                return Search::Incomplete(start);
            }
            if after.starts_with(BANNER_RELEASE_END)
                && let Some(release) = candidate.get(..release_len).and_then(parse_release)
            {
                return Search::Found(release);
            }
        }
        // Not a banner, such as a format string; look past it.
        from = start + 1;
    }
    Search::NotFound
}

/// Where [`BANNER_PREFIX`] next occurs at or after `from`. Scanning for its
/// first byte keeps a pass over a whole image cheap.
fn find_prefix(bytes: &[u8], from: usize) -> Option<usize> {
    let mut at = from;
    loop {
        let rest = bytes.get(at..)?;
        let offset = rest
            .iter()
            .position(|byte| Some(byte) == BANNER_PREFIX.first())?;
        let start = at + offset;
        if bytes.get(start..)?.starts_with(BANNER_PREFIX) {
            return Some(start);
        }
        at = start + 1;
    }
}

fn is_release_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+')
}

fn parse_release(release: &[u8]) -> Option<ArtifactVersion> {
    let release = std::str::from_utf8(release).ok()?;
    let release = release.trim_end_matches('+');
    if !release.starts_with(|first: char| first.is_ascii_digit()) {
        return None;
    }
    ArtifactVersion::parse(release).ok()
}

#[cfg(test)]
mod tests;
