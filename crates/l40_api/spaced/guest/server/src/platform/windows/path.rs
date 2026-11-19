//! Local filesystem paths, with no device namespace, UNC, ADS or DOS aliases.
use pplx_space_guest_process::GuestPath;
use std::path::{Path, PathBuf};
use tonic::Status;

pub(super) struct LocalPath(PathBuf);
impl LocalPath {
    pub(super) fn parse(raw: &str, home: &Path) -> Result<Self, Status> {
        let raw = if raw.is_empty() { "." } else { raw };
        GuestPath::parse(raw).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let normalized = raw.replace('/', "\\");
        if normalized.starts_with('\\') {
            return Err(Status::invalid_argument(
                "UNC, device and drive-relative paths are unsupported",
            ));
        }
        let bytes = normalized.as_bytes();
        let rooted =
            matches!(bytes.get(..3), Some([drive, b':', b'\\']) if drive.is_ascii_alphabetic());
        let tail = if rooted {
            normalized.get(3..).expect("ASCII drive prefix")
        } else {
            &normalized
        };
        for part in tail.split('\\') {
            if part.is_empty() || part == "." || part == ".." {
                continue;
            }
            let base = part
                .split('.')
                .next()
                .unwrap_or("")
                .trim_end_matches(' ')
                .to_ascii_uppercase();
            if part.ends_with(['.', ' '])
                || part.contains([':', '*', '?', '"', '<', '>', '|'])
                || matches!(
                    base.as_str(),
                    "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
                )
                || matches!(
                    base.as_str(),
                    "COM¹" | "COM²" | "COM³" | "LPT¹" | "LPT²" | "LPT³"
                )
                || (base.len() == 4
                    && (base.starts_with("COM") || base.starts_with("LPT"))
                    && base.as_bytes().get(3).is_some_and(u8::is_ascii_digit))
            {
                return Err(Status::invalid_argument(
                    "unsupported Windows path component",
                ));
            }
        }
        let path = if rooted {
            PathBuf::from(normalized)
        } else {
            home.join(normalized)
        };
        Ok(Self(path))
    }
    pub(super) fn as_path(&self) -> &Path {
        &self.0
    }
}

#[cfg(test)]
mod tests;
