/// Node identity supplied by Kubernetes in a reviewed pod-bound token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedKubernetesNode {
    name: pplx_space_common_core::types::K8sNodeName,
    uid: pplx_space_common_core::types::K8sNodeUid,
}

/// How an authenticated caller is bound to registered node inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthenticatedNodeIdentity {
    /// Static-token callers retain the registered transport-address binding.
    RemoteAddress,
    /// An absent pod binding must not fall back to the pod's transport address.
    Kubernetes(Option<AuthenticatedKubernetesNode>),
}

impl AuthenticatedKubernetesNode {
    pub fn name(&self) -> &pplx_space_common_core::types::K8sNodeName {
        &self.name
    }

    pub fn uid(&self) -> &pplx_space_common_core::types::K8sNodeUid {
        &self.uid
    }

    #[cfg(feature = "k8s")]
    /// Extract a pod's node binding after TokenReview authentication and audience
    /// validation. This function does not authenticate the supplied UserInfo.
    ///
    /// Pod-bound token node metadata is stable in Kubernetes 1.32+. The node UID
    /// is optional upstream, so require all four unambiguous claims. Tokens
    /// without a complete pod/node binding return None. Callers must still
    /// match the node name and UID against live inventory before granting access.
    /// See <https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/#additional-metadata-in-pod-bound-tokens>.
    pub(super) fn from_reviewed_user_info(
        user: &k8s_openapi::api::authentication::v1::UserInfo,
    ) -> Option<Self> {
        use pplx_space_common_core::types::{PodName, PodUid};
        let extra = user.extra.as_ref()?;
        let claim = |key| match extra.get(key).map(Vec::as_slice) {
            Some([value]) if !value.is_empty() => Some(value.as_str()),
            _ => None,
        };
        PodName::parse(claim("authentication.kubernetes.io/pod-name")?).ok()?;
        PodUid::parse(claim("authentication.kubernetes.io/pod-uid")?).ok()?;
        Some(Self {
            name: claim("authentication.kubernetes.io/node-name")?
                .parse()
                .ok()?,
            uid: claim("authentication.kubernetes.io/node-uid")?
                .parse()
                .ok()?,
        })
    }
}
