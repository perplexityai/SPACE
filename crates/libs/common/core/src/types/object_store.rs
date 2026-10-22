//! Object-key derivation and tunables for template and snapshot artifacts.
//!
//! Builder/template-manager (templates) and sandbox-manager (snapshots) use
//! the helpers here so they agree on object keys without coordination. Keys
//! are a pure function of the artifact identifier plus an optional key
//! prefix; nothing about the key flows on the wire.
//!
//! Also defines [`StorageUri`] — the self-describing connection-string
//! format used by `ClusterApi.RegisterTemplateRelease` /
//! `FindTemplateRelease`. The cluster service validates inserts against
//! this parser, so consumers (template-manager, etc.) can decode every
//! bit they need from the URI alone, without object-store env vars.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let template = TemplateName::parse("seed")?;
//! let hash = ContentHash::new(Digest::sha256([0xab; 32]));
//! let key = template_object_key("", &template, hash, TemplateArtifact::RootfsZstd);
//! assert!(key.starts_with("templates/seed/sha256:"));
//! assert!(key.ends_with("/rootfs.ext4.zst"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::str::FromStr;

use crate::types::template::{ContentHash, TemplateName};

/// Default chunk size for S3 multipart uploads.
///
/// Sized for the SPACE suspend workload (compressed `memory` + `rootfs.ext4`
/// in the 1–8 GiB range). 256 MiB keeps part counts low (32 parts for a
/// 8 GiB file, 4 for a 1 GiB file) so a node-wide drain stays well under
/// S3's per-prefix request budget without giving up multipart parallelism.
pub const MULTIPART_PART_SIZE: usize = 256 * 1024 * 1024;

/// Default concurrency for in-flight multipart parts.
///
/// A single TCP flow to S3 caps around 80–100 MB/s, so saturating a 25 Gbps
/// NIC requires ~32 concurrent flows. Combined with the vol-mgr default
/// `push_concurrency`, this gives `push × mp` ≈ that target.
pub const MULTIPART_CONCURRENCY: usize = 16;

/// Number of prefix shards under `suspend_snapshots/` in object storage.
///
/// S3 auto-partitions a bucket per key prefix and rate-limits each
/// partition independently (~3,500 PUT/s). A single shared prefix
/// (`suspend_snapshots/<sandbox_id>/`) throttles a node-wide drain into one
/// partition; sharding the prefix into `<shard>/<sandbox_id>/` lets S3 grant
/// each shard its own request budget, multiplying the per-second PUT
/// ceiling. 64 shards is comfortable headroom over the worst-case ~6.4K
/// parts a single 400-sandbox node-drain produces.
pub const SNAPSHOT_SHARD_COUNT: u32 = 64;
const SUSPEND_SNAPSHOT_KEY_SEGMENT: &str = "suspend_snapshots";

/// Returns the prefix shard for a sandbox's suspend-snapshot artifacts.
///
/// Deterministic and stable across Rust versions: the FNV-1a 64-bit hash
/// is a published algorithm with fixed constants, so the shard a given
/// `sandbox_id` lands in never changes. That means the storage URI written
/// into `snapshot_storage_locations` is reconstructible from the
/// `sandbox_id` alone, which simplifies operator scripts that need to find
/// artifacts without going through cluster-api.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::{snapshot_shard, SNAPSHOT_SHARD_COUNT};
/// let s = snapshot_shard("019e3e2d-7038-7c40-b8fa-b226d6a702c3");
/// assert!((s as u32) < SNAPSHOT_SHARD_COUNT);
/// // Stable: same input always lands in the same shard.
/// assert_eq!(s, snapshot_shard("019e3e2d-7038-7c40-b8fa-b226d6a702c3"));
/// ```
pub fn snapshot_shard(sandbox_id: &str) -> u8 {
    // FNV-1a 64-bit. Constants are part of the published spec, so this is
    // version- and platform-stable.
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in sandbox_id.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    (h % u64::from(SNAPSHOT_SHARD_COUNT)) as u8
}

/// Identifies one artifact of a template build.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TemplateArtifact {
    /// zstd-compressed root filesystem transfer artifact (`rootfs.ext4.zst`).
    RootfsZstd,
    /// Builder-emitted metadata document (`metadata.json`).
    Metadata,
    /// Template-manager manifest document (`manifest.json`).
    Manifest,
}

impl TemplateArtifact {
    /// Every artifact a published build consists of, in an order safe to write
    /// them in.
    ///
    /// `Manifest` goes last because it is the only one that describes the
    /// others: a puller reads `manifest.json` first and then fetches exactly the
    /// files it declares, so a manifest that lands before its siblings names
    /// objects that are not there yet. Writing it last means a partially written
    /// build has no manifest at all, which every reader treats as "not present"
    /// rather than as a build it can act on.
    ///
    /// This is the set both the builder's upload and the deploy-phase
    /// template-publish copy work from, so gaining a fourth artifact is one
    /// edit rather than three.
    pub const PUBLISHED: [Self; 3] = [Self::Metadata, Self::RootfsZstd, Self::Manifest];

    /// Returns the file name component as it appears under the build directory.
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::RootfsZstd => "rootfs.ext4.zst",
            Self::Metadata => "metadata.json",
            Self::Manifest => "manifest.json",
        }
    }
}

/// Returns the object-store key for one file of a template build.
///
/// The layout mirrors the on-node cache:
/// `<key_prefix>/templates/<template>/<content_hash>/<file>`. The key prefix is
/// joined verbatim — pass `""` to omit it. The function is total: validated
/// `TemplateName` and `ContentHash` values are guaranteed to produce a safe
/// path.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let template = TemplateName::parse("seed")?;
/// let hash = ContentHash::new(Digest::sha256([0xab; 32]));
/// let key = template_object_key("", &template, hash, TemplateArtifact::RootfsZstd);
/// assert_eq!(
///     key,
///     "templates/seed/sha256:abababababababababababababababababababababababababababababababab/rootfs.ext4.zst",
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn template_object_key(
    key_prefix: &str,
    template: &TemplateName,
    content_hash: ContentHash,
    artifact: TemplateArtifact,
) -> String {
    format!(
        "{prefix}/{file}",
        prefix = template_object_prefix(key_prefix, template, content_hash),
        file = artifact.file_name(),
    )
}

/// Returns the directory key holding every file of a template build —
/// [`template_object_key`] without the trailing file name, and never with a
/// trailing slash.
///
/// This is the value that goes into a [`StorageUri`]'s `key_prefix`: a release's
/// storage location addresses the build directory, and consumers append the
/// artifact file names themselves.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let template = TemplateName::parse("seed")?;
/// let hash = ContentHash::new(Digest::sha256([0xab; 32]));
/// let prefix = template_object_prefix("", &template, hash);
/// assert_eq!(
///     prefix,
///     "templates/seed/sha256:abababababababababababababababababababababababababababababababab",
/// );
/// assert_eq!(
///     template_object_key("", &template, hash, TemplateArtifact::Manifest),
///     format!("{prefix}/manifest.json"),
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn template_object_prefix(
    key_prefix: &str,
    template: &TemplateName,
    content_hash: ContentHash,
) -> String {
    let trimmed = key_prefix.trim_matches('/');
    let body = format!(
        "templates/{template}/{hash}",
        template = template.as_str(),
        hash = content_hash,
    );
    if trimmed.is_empty() {
        body
    } else {
        format!("{trimmed}/{body}")
    }
}

/// Identifies one artifact of a Firecracker microVM snapshot.
///
/// The rootfs slot has two distinct shapes today:
///
/// - [`Rootfs`](Self::Rootfs) — the historical full-image artifact
///   (`rootfs` on the wire, copied directly from the post-FC-freeze
///   `rootfs.ext4` file). The restore path for this format must remain
///   compatible with previously uploaded snapshots.
/// - [`RootfsBtrfsSend`](Self::RootfsBtrfsSend) — a `btrfs send -p
///   <template_subvol> <instance_ro_snap>` stream
///   (`rootfs.btrfs-send` on the wire). Restores by piping the
///   downloaded stream through `btrfs receive` into a subvolume
///   snapshot of the local template. Per-request opt-in via
///   `SuspendSnapshotFormat::BtrfsSend`.
///
/// A given snapshot upload has *exactly one* of the two rootfs
/// variants in its artifact set — never both — selected by the codec
/// the caller asked for at suspend time and recorded on the sidecar
/// as `rootfs_diff_format`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum SnapshotArtifact {
    /// Full root filesystem image (`rootfs`).
    Rootfs,
    /// Incremental `btrfs send -p` stream (`rootfs.btrfs-send`).
    RootfsBtrfsSend,
    /// Guest memory snapshot (`memfile`).
    Memory,
    /// Firecracker VM state — CPU registers and device state (`binfile`).
    VmState,
    /// BYOK encrypted snapshot volume (`snapshot.luks`): a sealed
    /// btrfs(compress-force=zstd)-over-LUKS2 image holding *both* the
    /// Firecracker vmstate and the guest memory file, compressed-then-encrypted
    /// at t0. Replaces the separate [`Memory`](Self::Memory) +
    /// [`VmState`](Self::VmState) artifacts for an encrypted suspend.
    EncryptedSnapshotVolume,
    /// BYOK encrypted rootfs delta (`cow.luks`): the dm-crypt COW backing file
    /// holding every guest write since template clone, already a LUKS2
    /// container. Replaces the plaintext rootfs artifact for an encrypted
    /// suspend; restore re-attaches it under the dm-snapshot origin.
    EncryptedRootfsCow,
    /// Optional snapshot metadata document (`metadata.json`).
    Metadata,
}

impl SnapshotArtifact {
    /// Returns the file name component as it appears under the snapshot
    /// directory. Parsing reverses it.
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::SnapshotArtifact;
    /// let artifact: SnapshotArtifact = "rootfs.btrfs-send".parse()?;
    /// assert_eq!(artifact.file_name(), "rootfs.btrfs-send");
    /// assert!("../rootfs".parse::<SnapshotArtifact>().is_err());
    /// # Ok::<(), pplx_space_common_core::types::SnapshotArtifactParseError>(())
    /// ```
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::Rootfs => "rootfs",
            Self::RootfsBtrfsSend => "rootfs.btrfs-send",
            Self::Memory => "memfile",
            Self::VmState => "binfile",
            Self::EncryptedSnapshotVolume => "snapshot.luks",
            Self::EncryptedRootfsCow => "cow.luks",
            Self::Metadata => "metadata.json",
        }
    }
}

impl std::fmt::Display for SnapshotArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.file_name())
    }
}

/// Error returned when a string is not a snapshot artifact file name.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unknown snapshot artifact file name: {0:?}")]
pub struct SnapshotArtifactParseError(String);

impl FromStr for SnapshotArtifact {
    type Err = SnapshotArtifactParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rootfs" => Ok(Self::Rootfs),
            "rootfs.btrfs-send" => Ok(Self::RootfsBtrfsSend),
            "memfile" => Ok(Self::Memory),
            "binfile" => Ok(Self::VmState),
            "snapshot.luks" => Ok(Self::EncryptedSnapshotVolume),
            "cow.luks" => Ok(Self::EncryptedRootfsCow),
            "metadata.json" => Ok(Self::Metadata),
            other => Err(SnapshotArtifactParseError(other.to_owned())),
        }
    }
}

/// Returns the object-store key for one artifact of a sandbox suspend
/// snapshot.
///
/// Layout: `<key_prefix>/suspend_snapshots/<shard>/<sandbox_id>/<snapshot_id>/<file>`,
/// where `<shard>` is the two-hex-digit value from [`snapshot_shard`]. See
/// [`SNAPSHOT_SHARD_COUNT`] for the sharding rationale.
///
/// Every suspend attempt allocates a fresh `snapshot_id` (UUIDv7) and writes
/// its artifacts under that nested directory; suspends never share or
/// overwrite each other's bytes. Restore-side readers select the latest
/// `state='ready'` snapshot row and pull from the URI stored on that row, so
/// an in-flight upload is invisible until its row flips to `ready`.
///
/// The key prefix is joined verbatim — pass `""` to omit it. Inputs are taken
/// as strings; the caller is responsible for ensuring `sandbox_id` and
/// `snapshot_id` are valid identifiers (validated UUID newtypes at the
/// boundary) before calling.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let key = snapshot_object_key("", "abc123", "snap-1", SnapshotArtifact::Memory);
/// // The shard is a deterministic function of sandbox_id (not snapshot_id),
/// // so every attempt for a given sandbox lands in the same shard.
/// let expected_shard = snapshot_shard("abc123");
/// assert_eq!(
///     key,
///     format!("suspend_snapshots/{expected_shard:02x}/abc123/snap-1/memfile"),
/// );
/// ```
pub fn snapshot_object_key(
    key_prefix: &str,
    sandbox_id: &str,
    snapshot_id: &str,
    artifact: SnapshotArtifact,
) -> String {
    let body = format!(
        "{}/{file}",
        snapshot_attempt_key_prefix("", sandbox_id, snapshot_id),
        file = artifact.file_name()
    );
    let trimmed = key_prefix.trim_matches('/');
    if trimmed.is_empty() {
        body
    } else {
        format!("{trimmed}/{body}")
    }
}

/// Returns the object-key directory shared by every artifact in one immutable
/// suspend-snapshot attempt.
pub fn snapshot_attempt_key_prefix(
    key_prefix: &str,
    sandbox_id: &str,
    snapshot_id: &str,
) -> String {
    let shard = snapshot_shard(sandbox_id);
    let body = format!("{SUSPEND_SNAPSHOT_KEY_SEGMENT}/{shard:02x}/{sandbox_id}/{snapshot_id}");
    let trimmed = key_prefix.trim_matches('/');
    if trimmed.is_empty() {
        body
    } else {
        format!("{trimmed}/{body}")
    }
}

/// Borrowed components of an artifact key produced by
/// [`snapshot_object_key`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotObjectKeyParts<'a> {
    pub sandbox_id: &'a str,
    pub snapshot_id: &'a str,
    pub file_name: &'a str,
}

/// Recognizes the canonical suspend-snapshot artifact layout inside an
/// optional caller prefix. The shard is verified against the sandbox ID so a
/// merely similar path is not treated as an immutable attempt key.
pub fn parse_snapshot_object_key(object_key: &str) -> Option<SnapshotObjectKeyParts<'_>> {
    let segments: Vec<_> = object_key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let index = segments
        .iter()
        .rposition(|segment| *segment == SUSPEND_SNAPSHOT_KEY_SEGMENT)?;
    let [shard, sandbox_id, snapshot_id, file_name] = segments.get(index + 1..)? else {
        return None;
    };
    if *shard != format!("{:02x}", snapshot_shard(sandbox_id)) {
        return None;
    }
    Some(SnapshotObjectKeyParts {
        sandbox_id,
        snapshot_id,
        file_name,
    })
}

/// Parsed shape of a self-describing object-store connection string —
/// the value stored in `template_releases.storage_connection_string`
/// and returned by `ClusterApi.FindTemplateRelease`.
///
/// Format: `s3://<bucket>/<key-prefix>?region=<region>[&endpoint=<url>][&path_style=true|false]`
///
/// `region` is required. `endpoint` defaults to none. `path_style`
/// defaults to `true` when an `endpoint` is present and `false`
/// otherwise; the query param overrides the default if supplied.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let parts = StorageUri::parse(
///     "s3://my-bucket/templates/seed?region=us-east-1",
/// )?;
/// assert_eq!(parts.bucket, "my-bucket");
/// assert_eq!(parts.region, "us-east-1");
/// # Ok::<(), StorageUriParseError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUri {
    pub bucket: String,
    /// Directory key under which the artifact files live. Empty when
    /// objects sit at the bucket root. Never has a trailing slash.
    pub key_prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
}

/// A parsed storage URI coupled to the exact validated wire value it came
/// from.
///
/// Use this type when the precise object-store source is durable identity,
/// such as a resumable restore. The parsed components are exposed only by
/// shared reference so they cannot diverge from the wire value.
#[derive(Debug, Clone)]
pub struct ExactStorageUri {
    parsed: StorageUri,
    wire: String,
}

impl PartialEq for ExactStorageUri {
    fn eq(&self, other: &Self) -> bool {
        self.parsed == other.parsed
    }
}

impl Eq for ExactStorageUri {}

/// Why a candidate storage URI didn't parse. Each variant carries the
/// offending input so callers can surface it in error messages.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum StorageUriParseError {
    #[error("not an s3:// URI: {0}")]
    NotS3(String),
    #[error("empty s3:// URI: {0}")]
    Empty(String),
    #[error("s3:// URI is missing bucket: {0}")]
    MissingBucket(String),
    #[error("s3:// URI missing required ?region=... query param: {0}")]
    MissingRegion(String),
    #[error("malformed query param '{pair}' in s3:// URI: {uri}")]
    MalformedQuery { pair: String, uri: String },
    #[error("path_style must be 'true' or 'false' in s3:// URI: {uri}")]
    InvalidPathStyle { uri: String },
    #[error("unsupported query param '{key}' in s3:// URI: {uri}")]
    UnsupportedQuery { key: String, uri: String },
}

impl StorageUri {
    /// Parses an `s3://` connection string. See the type-level docs for
    /// the format.
    pub fn parse(uri: &str) -> Result<Self, StorageUriParseError> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| StorageUriParseError::NotS3(uri.to_string()))?;
        if rest.is_empty() {
            return Err(StorageUriParseError::Empty(uri.to_string()));
        }

        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (rest, None),
        };

        let (bucket, key_prefix) = match path.split_once('/') {
            Some((b, p)) => (b.to_string(), p.trim_end_matches('/').to_string()),
            None => (path.to_string(), String::new()),
        };
        if bucket.is_empty() {
            // Catches both `s3:///path?...` (split_once arm with empty
            // bucket) and `s3://?region=...` (no slash at all, so the
            // None arm above tried to use the empty path as the
            // bucket).
            return Err(StorageUriParseError::MissingBucket(uri.to_string()));
        }

        let mut region: Option<String> = None;
        let mut endpoint: Option<String> = None;
        let mut path_style: Option<bool> = None;
        if let Some(q) = query {
            for pair in q.split('&').filter(|s| !s.is_empty()) {
                let (k, v) =
                    pair.split_once('=')
                        .ok_or_else(|| StorageUriParseError::MalformedQuery {
                            pair: pair.to_string(),
                            uri: uri.to_string(),
                        })?;
                match k {
                    "region" => region = Some(v.to_string()),
                    "endpoint" => endpoint = Some(v.to_string()),
                    "path_style" => {
                        path_style = Some(match v {
                            "true" => true,
                            "false" => false,
                            _ => {
                                return Err(StorageUriParseError::InvalidPathStyle {
                                    uri: uri.to_string(),
                                });
                            }
                        });
                    }
                    _ => {
                        return Err(StorageUriParseError::UnsupportedQuery {
                            key: k.to_string(),
                            uri: uri.to_string(),
                        });
                    }
                }
            }
        }

        let region = region.ok_or_else(|| StorageUriParseError::MissingRegion(uri.to_string()))?;
        let path_style = path_style.unwrap_or(endpoint.is_some());

        Ok(Self {
            bucket,
            key_prefix,
            region,
            endpoint,
            path_style,
        })
    }
}

impl FromStr for StorageUri {
    type Err = StorageUriParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Renders the canonical wire form, so anything that needs to *write* a
/// connection string derives it from the same type that validates reads.
///
/// Query params that carry no information are omitted: `endpoint` when there
/// is none, and `path_style` whenever it already equals the value
/// [`StorageUri::parse`] would infer from the presence of an endpoint. Both
/// omissions round-trip — `parse(&uri.to_string()) == uri` for every value
/// `parse` can produce — and the omission of `path_style` is what keeps real-S3
/// output down to `s3://<bucket>/<key-prefix>?region=<region>`.
impl std::fmt::Display for StorageUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3://{}", self.bucket)?;
        if !self.key_prefix.is_empty() {
            write!(f, "/{}", self.key_prefix)?;
        }
        write!(f, "?region={}", self.region)?;
        if let Some(endpoint) = &self.endpoint {
            write!(f, "&endpoint={endpoint}")?;
        }
        if self.path_style != self.endpoint.is_some() {
            write!(f, "&path_style={}", self.path_style)?;
        }
        Ok(())
    }
}

impl ExactStorageUri {
    /// Parse and retain an exact storage URI wire value.
    pub fn parse(uri: &str) -> Result<Self, StorageUriParseError> {
        Ok(Self {
            parsed: StorageUri::parse(uri)?,
            wire: uri.to_string(),
        })
    }

    pub fn parsed(&self) -> &StorageUri {
        &self.parsed
    }

    pub fn as_wire(&self) -> &str {
        &self.wire
    }
}

impl FromStr for ExactStorageUri {
    type Err = StorageUriParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Canonical stable snapshot metadata for an operator recovery approval.
/// Generated transport timestamps/sizes and node-local send-source handles are
/// deliberately excluded. Every other field, including future fields, is bound.
#[cfg(feature = "serde")]
pub fn snapshot_metadata_binding(bytes: &[u8]) -> Result<Vec<u8>, SnapshotMetadataBindingError> {
    let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
    let map = value
        .as_object_mut()
        .ok_or(SnapshotMetadataBindingError::Shape)?;
    if map.get("version").and_then(serde_json::Value::as_u64) != Some(2)
        || [
            "sandbox_id",
            "local_artifact_id",
            "artifact_kind",
            "artifact_origin",
            "execution",
            "source",
        ]
        .iter()
        .any(|key| map.get(*key).is_none_or(serde_json::Value::is_null))
    {
        return Err(SnapshotMetadataBindingError::Shape);
    }
    for key in ["created_at", "size_bytes", "rootfs_send_source_id"] {
        map.remove(key);
    }
    fn sort(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for child in map.values_mut() {
                    sort(child);
                }
                map.sort_keys();
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    sort(child);
                }
            }
            _ => {}
        }
    }
    sort(&mut value);
    Ok(serde_json::to_vec(&value)?)
}

#[cfg(feature = "serde")]
#[derive(Debug, thiserror::Error)]
pub enum SnapshotMetadataBindingError {
    #[error("recovery requires version 2 artifact identity and execution metadata")]
    Shape,
    #[error("invalid snapshot metadata: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests;
