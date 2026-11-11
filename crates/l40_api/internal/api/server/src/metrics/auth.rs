//! The template node binding internal-api records for template-catalog reads.
//! The other authentication families live with their recorders: the shared
//! service-auth ones in `pplx-space-rpc-auth` (`metrics`), the template lookup
//! scope compatibility counter in `pplx-space-template-mgr-client`
//! (`lookup_scope::metrics`), UDS peer checks in `pplx-space-uds-peer-auth`,
//! node-api's wait-ready policy in node-api-server and the external console
//! families in the API crate (each in its `metrics`).

use pplx_space_metrics_emit::{counter, tag};

/// Outcome of binding a reviewed pod token to registered template-catalog nodes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemplateNodeBinding {
    Bound,
    /// Missing pod/node metadata uses registered-address matching.
    AddressFallback,
    /// Malformed or ambiguous supplied claims.
    Unbound,
    NoInventoryMatch,
}

impl TemplateNodeBinding {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bound => "bound",
            Self::AddressFallback => "address_fallback",
            Self::Unbound => "unbound",
            Self::NoInventoryMatch => "no_inventory_match",
        }
    }
}

impl std::fmt::Display for TemplateNodeBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One Kubernetes template-catalog read, including reads using cached TokenReview.
/// `address_fallback` retains the legacy address scope; `unbound` and
/// `no_inventory_match` restrict the caller to generic templates.
pub fn template_node_binding(result: TemplateNodeBinding) {
    counter(
        "auth.template_node_binding",
        1,
        &[tag("result", result.as_str())],
    );
}
