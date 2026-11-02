use std::future::Future;

/// The `client_id` tag value: an allowlisted `X-Client-ID`, or `unknown`.
///
/// Only operator-configured IDs may be allowlisted, so the tag's cardinality is
/// bounded by configuration, never by what a caller sent. A missing, blank, or
/// unlisted header is [`Self::UNKNOWN`], so a caller rotating IDs can't mint
/// new series. The variants are private: an allowlisted value exists only
/// through [`Self::allowlisted`], whose caller owns the bound.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClientIdTag(ClientIdTagValue);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ClientIdTagValue {
    Allowlisted(&'static str),
    Overflow,
    Unknown,
}

impl ClientIdTag {
    /// Every caller the allowlist doesn't name.
    pub const UNKNOWN: Self = Self(ClientIdTagValue::Unknown);
    /// An allowlisted client the resolver couldn't name without unbounded
    /// memory. It still has its own limiter bucket.
    pub const OVERFLOW: Self = Self(ClientIdTagValue::Overflow);

    /// The tag for an allowlisted client. `id` must come from operator
    /// configuration, interned by the resolver: a caller-supplied value here
    /// would put unbounded caller text on every series that carries the tag.
    pub const fn allowlisted(id: &'static str) -> Self {
        Self(ClientIdTagValue::Allowlisted(id))
    }

    pub const fn as_str(self) -> &'static str {
        match self.0 {
            ClientIdTagValue::Allowlisted(id) => id,
            ClientIdTagValue::Overflow => "overflow",
            ClientIdTagValue::Unknown => "unknown",
        }
    }

    /// Whether the tag names a client with its own limiter bucket.
    pub(super) const fn has_own_bucket(self) -> bool {
        !matches!(self.0, ClientIdTagValue::Unknown)
    }
}

tokio::task_local! {
    static REQUEST_CLIENT_ID: ClientIdTag;
}

/// Run `request` with `client_id` as the bounded caller of every API metric it
/// emits, so step, restore, wake and lifecycle metrics deep in the controllers
/// carry the same `client_id` as the route metric without threading it through
/// each metric identity.
///
/// The value is already a [`ClientIdTag`], so this adds no new cardinality
/// source. Work the request spawns onto another task leaves the scope and
/// reports [`ClientIdTag::UNKNOWN`].
pub async fn scope_request_client_id<F: Future>(client_id: ClientIdTag, request: F) -> F::Output {
    REQUEST_CLIENT_ID.scope(client_id, request).await
}

/// The bounded caller of the request this task is serving, or
/// [`ClientIdTag::UNKNOWN`] outside [`scope_request_client_id`].
pub fn request_client_id() -> ClientIdTag {
    REQUEST_CLIENT_ID
        .try_with(|client_id| *client_id)
        .unwrap_or(ClientIdTag::UNKNOWN)
}
