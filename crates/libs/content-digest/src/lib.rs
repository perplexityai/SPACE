//! Algorithm-typed content digests: [`HashAlg`], [`Digest`], short
//! [`DigestPrefix`]es and the generic [`ContentHash`] wrapper, with their parse
//! errors and prefix-length constants.
//!
//! These are generic digest values; the template-specific wrappers live in
//! `pplx-space-common-core`, which also re-exports these types at their former
//! paths. Template, execution, object-store and archive code all name them, so
//! they live in their own crate rather than in common-core's template module.

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Digest algorithm selector.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_content_digest::HashAlg;
/// assert_eq!(HashAlg::Sha256.to_string(), "sha256");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum HashAlg {
    /// SHA-256, encoded as 32 bytes / 64 lowercase hex characters.
    Sha256,
    /// BLAKE3, encoded as 32 bytes / 64 lowercase hex characters.
    Blake3,
}

/// Default number of digest hex characters used for short display.
///
/// Twelve hex characters mirrors common container tooling ergonomics while
/// keeping the value long enough to be useful. It is still only a prefix and
/// must be resolved uniquely before use as an immutable reference.
pub const DEFAULT_DIGEST_PREFIX_HEX_LEN: usize = 12;

/// Minimum accepted digest prefix length.
pub const MIN_DIGEST_PREFIX_HEX_LEN: usize = 12;

/// Error returned when parsing a digest algorithm selector.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_content_digest::HashAlg;
/// assert_eq!(HashAlg::from_str("sha256").unwrap(), HashAlg::Sha256);
/// assert!(HashAlg::from_str("sha512").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported digest algorithm '{0}'")]
pub struct HashAlgParseError(String);

impl HashAlg {
    /// Parses a supported digest algorithm selector.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_content_digest::HashAlg;
    /// assert_eq!(HashAlg::parse("sha256").unwrap(), HashAlg::Sha256);
    /// ```
    pub fn parse(value: &str) -> Result<Self, HashAlgParseError> {
        match value {
            "sha256" => Ok(Self::Sha256),
            "blake3" => Ok(Self::Blake3),
            other => Err(HashAlgParseError(other.to_string())),
        }
    }

    /// Returns the canonical digest algorithm string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Blake3 => "blake3",
        }
    }

    /// Returns the digest byte length for this algorithm.
    pub const fn digest_len_bytes(self) -> usize {
        match self {
            Self::Sha256 | Self::Blake3 => 32,
        }
    }

    /// Returns the canonical lowercase hex length for this algorithm.
    pub const fn digest_hex_len(self) -> usize {
        self.digest_len_bytes() * 2
    }
}

impl fmt::Display for HashAlg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HashAlg {
    type Err = HashAlgParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for HashAlg {
    type Error = HashAlgParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<HashAlg> for String {
    fn from(value: HashAlg) -> Self {
        value.to_string()
    }
}

/// Error returned when parsing a typed digest string.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_content_digest::Digest;
/// assert!(Digest::from_str("sha256:not-hex").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum DigestParseError {
    #[error("digest must be formatted as <algorithm>:<lowercase-hex>")]
    MissingAlgorithmSeparator,
    #[error("unsupported digest algorithm '{0}'")]
    UnsupportedAlgorithm(String),
    #[error("digest for {algorithm} must have {expected} lowercase hex chars, got {actual}")]
    InvalidHexLength {
        algorithm: HashAlg,
        expected: usize,
        actual: usize,
    },
    #[error("digest hex must use lowercase hexadecimal characters")]
    InvalidHex,
}

/// Error returned when parsing or constructing a digest prefix.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum DigestPrefixParseError {
    #[error("digest prefix must be formatted as <algorithm>:<lowercase-hex-prefix>")]
    MissingAlgorithmSeparator,
    #[error("unsupported digest algorithm '{0}'")]
    UnsupportedAlgorithm(String),
    #[error("digest prefix must have at least {min} lowercase hex chars, got {actual}")]
    PrefixTooShort { min: usize, actual: usize },
    #[error("digest prefix for {algorithm} must be shorter than a full digest, got {actual}")]
    PrefixTooLong { algorithm: HashAlg, actual: usize },
    #[error("digest prefix hex must use lowercase hexadecimal characters")]
    InvalidHex,
}

/// Error returned when resolving a digest prefix against candidate full digests.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum DigestPrefixResolveError {
    #[error("digest prefix {0} did not match any candidate")]
    NoMatch(DigestPrefix),
    #[error("digest prefix {0} matched more than one candidate")]
    Ambiguous(DigestPrefix),
}

/// Algorithm-typed short prefix of a content digest.
///
/// Prefixes are accepted for operator input and display only. They are not
/// canonical content addresses and must be resolved against a repository/index
/// before they can stand in for a full [`Digest`].
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DigestPrefix {
    algorithm: HashAlg,
    hex_prefix: String,
}

impl DigestPrefix {
    /// Creates a digest prefix from an algorithm and lowercase hex prefix.
    pub fn new(
        algorithm: HashAlg,
        hex_prefix: impl Into<String>,
    ) -> Result<Self, DigestPrefixParseError> {
        let hex_prefix = hex_prefix.into();
        validate_digest_prefix_hex(algorithm, &hex_prefix)?;
        Ok(Self {
            algorithm,
            hex_prefix,
        })
    }

    /// Returns the digest algorithm.
    pub const fn algorithm(&self) -> HashAlg {
        self.algorithm
    }

    /// Returns the lowercase hex prefix without the algorithm.
    pub fn hex_prefix(&self) -> &str {
        &self.hex_prefix
    }

    /// Returns true when this prefix matches the given full digest.
    pub fn matches(&self, digest: Digest) -> bool {
        digest.algorithm() == self.algorithm && digest.hex_string().starts_with(&self.hex_prefix)
    }

    /// Resolves this prefix against candidate full digests.
    ///
    /// The result succeeds only when exactly one candidate matches. This is the
    /// helper repository/index code should use after accepting a short digest
    /// from an operator-facing boundary.
    pub fn resolve_unique<T, I, F>(
        &self,
        candidates: I,
        digest_of: F,
    ) -> Result<T, DigestPrefixResolveError>
    where
        I: IntoIterator<Item = T>,
        F: Fn(&T) -> Digest,
    {
        let mut matched = None;
        for candidate in candidates {
            if self.matches(digest_of(&candidate)) {
                if matched.is_some() {
                    return Err(DigestPrefixResolveError::Ambiguous(self.clone()));
                }
                matched = Some(candidate);
            }
        }
        matched.ok_or_else(|| DigestPrefixResolveError::NoMatch(self.clone()))
    }
}

impl fmt::Display for DigestPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.hex_prefix)
    }
}

impl FromStr for DigestPrefix {
    type Err = DigestPrefixParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (algorithm, hex_prefix) = value
            .split_once(':')
            .ok_or(DigestPrefixParseError::MissingAlgorithmSeparator)?;
        let algorithm = HashAlg::parse(algorithm)
            .map_err(|_| DigestPrefixParseError::UnsupportedAlgorithm(algorithm.to_string()))?;
        Self::new(algorithm, hex_prefix)
    }
}

impl TryFrom<String> for DigestPrefix {
    type Error = DigestPrefixParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<DigestPrefix> for String {
    fn from(value: DigestPrefix) -> Self {
        value.to_string()
    }
}

/// Algorithm-typed content digest.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_content_digest::{Digest, HashAlg};
/// let digest = Digest::sha256([0xab; 32]);
/// assert_eq!(digest.algorithm(), HashAlg::Sha256);
/// assert!(digest.to_string().starts_with("sha256:"));
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Digest {
    Sha256([u8; 32]),
    Blake3([u8; 32]),
}

impl Digest {
    /// Creates a SHA-256 digest from exactly 32 bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_content_digest::Digest;
    /// let digest = Digest::sha256([0xab; 32]);
    /// assert_eq!(digest.to_string().len(), "sha256:".len() + 64);
    /// ```
    pub const fn sha256(bytes: [u8; 32]) -> Self {
        Self::Sha256(bytes)
    }

    /// Creates a BLAKE3 digest from exactly 32 bytes.
    pub const fn blake3(bytes: [u8; 32]) -> Self {
        Self::Blake3(bytes)
    }

    /// Returns the digest algorithm.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_content_digest::{Digest, HashAlg};
    /// let digest = Digest::sha256([0xab; 32]);
    /// assert_eq!(digest.algorithm(), HashAlg::Sha256);
    /// ```
    pub const fn algorithm(self) -> HashAlg {
        match self {
            Self::Sha256(_) => HashAlg::Sha256,
            Self::Blake3(_) => HashAlg::Blake3,
        }
    }

    /// Returns the lowercase hex encoding without the algorithm prefix.
    pub fn hex_string(self) -> String {
        let mut encoded = String::with_capacity(self.algorithm().digest_hex_len());
        match self {
            Self::Sha256(bytes) | Self::Blake3(bytes) => {
                for byte in bytes {
                    use std::fmt::Write as _;
                    write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
                }
            }
        }
        encoded
    }

    /// Returns a short digest prefix with the default display length.
    pub fn short_prefix(self) -> DigestPrefix {
        self.prefix(DEFAULT_DIGEST_PREFIX_HEX_LEN)
            .expect("default digest prefix length is valid for all supported algorithms")
    }

    /// Returns a short digest prefix with an explicit hex length.
    pub fn prefix(self, hex_len: usize) -> Result<DigestPrefix, DigestPrefixParseError> {
        let hex = self.hex_string();
        let Some(prefix) = hex.get(..hex_len) else {
            return Err(DigestPrefixParseError::PrefixTooLong {
                algorithm: self.algorithm(),
                actual: hex_len,
            });
        };
        DigestPrefix::new(self.algorithm(), prefix)
    }

    /// Returns a short digest string such as `sha256:0123456789ab`.
    pub fn short_string(self) -> String {
        self.short_prefix().to_string()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Digest").field(&self.to_string()).finish()
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha256(bytes) | Self::Blake3(bytes) => {
                write!(f, "{}:", self.algorithm())?;
                for byte in bytes {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl FromStr for Digest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (algorithm, hex) = value
            .split_once(':')
            .ok_or(DigestParseError::MissingAlgorithmSeparator)?;
        let algorithm = HashAlg::parse(algorithm)
            .map_err(|_| DigestParseError::UnsupportedAlgorithm(algorithm.to_string()))?;

        let expected = algorithm.digest_hex_len();
        if hex.len() != expected {
            return Err(DigestParseError::InvalidHexLength {
                algorithm,
                expected,
                actual: hex.len(),
            });
        }

        if !hex
            .bytes()
            .all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch))
        {
            return Err(DigestParseError::InvalidHex);
        }

        let mut bytes = [0u8; 32];
        for (byte, chunk) in bytes.iter_mut().zip(hex.as_bytes().as_chunks::<2>().0) {
            let chunk = std::str::from_utf8(chunk).map_err(|_| DigestParseError::InvalidHex)?;
            *byte = u8::from_str_radix(chunk, 16).map_err(|_| DigestParseError::InvalidHex)?;
        }
        Ok(match algorithm {
            HashAlg::Sha256 => Self::Sha256(bytes),
            HashAlg::Blake3 => Self::Blake3(bytes),
        })
    }
}

impl TryFrom<String> for Digest {
    type Error = DigestParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<Digest> for String {
    fn from(value: Digest) -> Self {
        value.to_string()
    }
}

/// Generic content-addressed hash.
///
/// Prefer `TemplateIndexDigest` or `TemplateArtifactDigest` when the digest's
/// role is known. This wrapper remains useful for storage code and existing
/// template-manager paths that only need a generic digest.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_content_digest::{ContentHash, Digest};
/// let hash = ContentHash::new(Digest::sha256([0xab; 32]));
/// assert!(hash.to_string().starts_with("sha256:"));
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ContentHash(Digest);

impl ContentHash {
    /// Wraps a typed digest as a content hash.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_content_digest::{ContentHash, Digest};
    /// let digest = Digest::sha256([0xab; 32]);
    /// assert_eq!(ContentHash::new(digest).digest(), digest);
    /// ```
    pub const fn new(value: Digest) -> Self {
        Self(value)
    }

    /// Returns the underlying typed digest.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_content_digest::{ContentHash, Digest};
    /// let digest = Digest::sha256([0xab; 32]);
    /// let hash = ContentHash::new(digest);
    /// assert_eq!(hash.digest(), digest);
    /// ```
    pub const fn digest(self) -> Digest {
        self.0
    }

    /// Returns the default short digest prefix for operator-facing display.
    pub fn short_prefix(self) -> DigestPrefix {
        self.0.short_prefix()
    }

    /// Returns a short digest string such as `sha256:0123456789ab`.
    pub fn short_string(self) -> String {
        self.0.short_string()
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ContentHash {
    type Err = DigestParseError;

    /// Parses a content hash, tolerating a **bare** digest that omits the
    /// `sha256:` algorithm prefix by assuming sha256.
    ///
    /// A value containing a `:` is parsed as a fully-qualified [`Digest`]
    /// (so `md5:...` still fails as an unsupported algorithm). A value with
    /// no `:` is treated as a bare sha256 digest only if it is a valid
    /// 64-char lowercase hex string — the delegated `Digest::from_str`
    /// enforces the length and hex constraints, so arbitrary input is
    /// rejected rather than blindly prefixed. The tolerance lives here on
    /// `ContentHash` rather than on `Digest` so that the canonical
    /// `<algorithm>:<hex>` invariant of [`Digest`] stays strict.
    ///
    /// This mirrors the tolerant `template_releases.hash` matching the
    /// warm-pool autoscaler uses, where the same column is observed in both
    /// bare and `sha256:`-prefixed forms.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_content_digest::ContentHash;
    /// let hex = "ab".repeat(32);
    /// let bare: ContentHash = hex.parse().unwrap();
    /// let prefixed: ContentHash = format!("sha256:{hex}").parse().unwrap();
    /// assert_eq!(bare, prefixed);
    /// assert!(bare.to_string().starts_with("sha256:"));
    /// assert!("not-hex".parse::<ContentHash>().is_err());
    /// ```
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.contains(':') {
            Digest::from_str(value).map(Self)
        } else {
            Digest::from_str(&format!("{}:{value}", HashAlg::Sha256.as_str())).map(Self)
        }
    }
}

impl TryFrom<String> for ContentHash {
    type Error = DigestParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<ContentHash> for String {
    fn from(value: ContentHash) -> Self {
        value.to_string()
    }
}

fn validate_digest_prefix_hex(
    algorithm: HashAlg,
    hex_prefix: &str,
) -> Result<(), DigestPrefixParseError> {
    if hex_prefix.len() < MIN_DIGEST_PREFIX_HEX_LEN {
        return Err(DigestPrefixParseError::PrefixTooShort {
            min: MIN_DIGEST_PREFIX_HEX_LEN,
            actual: hex_prefix.len(),
        });
    }
    if hex_prefix.len() >= algorithm.digest_hex_len() {
        return Err(DigestPrefixParseError::PrefixTooLong {
            algorithm,
            actual: hex_prefix.len(),
        });
    }
    if !hex_prefix
        .bytes()
        .all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch))
    {
        return Err(DigestPrefixParseError::InvalidHex);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
