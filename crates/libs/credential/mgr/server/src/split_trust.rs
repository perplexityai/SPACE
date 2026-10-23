//! Default-off sandbox anchors over the existing credential-manager RPC boundary.

pub(crate) mod authority;
mod backup;
mod node;

pub(crate) use node::NodeAnchors;

pub(crate) fn enabled() -> bool {
    matches!(
        std::env::var("SPACE_SPLIT_TRUST_PROTOTYPE").as_deref(),
        Ok("1" | "true")
    )
}
