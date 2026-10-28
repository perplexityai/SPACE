//! Bounded identity of the client that activated a sandbox execution.

use std::fmt;
use std::sync::LazyLock;

use crate::types::LabelValue;

/// Bounded identity of the client that activated a sandbox execution.
///
/// A key is either an allowlisted `X-Client-ID` value or [`ClientKey::unknown`].
/// The allowlist is what keeps the value set bounded: the API resolves the
/// header once, at the request boundary, and an unrecognized, absent, or blank
/// id becomes `unknown`, so a caller rotating a fresh id per request can't mint
/// a new key (and a new pod label value, metric tag, or quota bucket) each time.
/// This type checks only the shape; the allowlist decision belongs to the
/// caller that constructs it with [`ClientKey::allowlisted`].
///
/// Every key is a non-empty Kubernetes label value, because it's rendered as
/// the value of the `space.pplx.net/client-id` pod label. An allowlisted id
/// that isn't a legal label value can't be a key and resolves to `unknown`.
///
/// ```
/// # use pplx_space_common_core::types::ClientKey;
/// assert_eq!(ClientKey::allowlisted("background-client").unwrap().as_str(), "background-client");
/// assert!(ClientKey::allowlisted("not a label").is_none());
/// assert!(ClientKey::unknown().is_unknown());
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientKey(LabelValue);

impl ClientKey {
    /// The key shared by every activation without an allowlisted client id.
    pub const UNKNOWN: &'static str = "unknown";

    /// The shared key for activations without an allowlisted client id.
    pub fn unknown() -> Self {
        Self::unknown_ref().clone()
    }

    /// [`ClientKey::unknown`] as a `'static` borrow, for callers that only
    /// need a reference.
    pub fn unknown_ref() -> &'static Self {
        static UNKNOWN: LazyLock<ClientKey> = LazyLock::new(|| {
            ClientKey(
                LabelValue::parse(ClientKey::UNKNOWN).expect("`unknown` is a valid label value"),
            )
        });
        &UNKNOWN
    }

    /// The key for an id the caller has already matched against its allowlist.
    /// Returns `None` when `id` is empty or isn't a legal Kubernetes label
    /// value; the caller then uses [`ClientKey::unknown`].
    pub fn allowlisted(id: &str) -> Option<Self> {
        if id.is_empty() {
            return None;
        }
        LabelValue::parse(id).ok().map(Self)
    }

    /// Decode a key the API stored earlier. An absent value (a record written
    /// before the key existed) or one that is no longer a valid key reads as
    /// [`ClientKey::unknown`].
    pub fn from_stored(value: Option<&str>) -> Self {
        value
            .and_then(Self::allowlisted)
            .unwrap_or_else(Self::unknown)
    }

    /// Whether this is the shared [`ClientKey::unknown`] key.
    pub fn is_unknown(&self) -> bool {
        self.as_str() == Self::UNKNOWN
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for ClientKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
