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
    Kubernetes(AuthenticatedKubernetesNode),
    /// Incomplete metadata retains the legacy registered-address binding.
    MissingKubernetesClaims,
    /// Invalid supplied claims must not trigger an address fallback.
    MalformedKubernetesClaims,
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
        match AuthenticatedNodeIdentity::from_reviewed_user_info(user) {
            AuthenticatedNodeIdentity::Kubernetes(node) => Some(node),
            AuthenticatedNodeIdentity::RemoteAddress
            | AuthenticatedNodeIdentity::MissingKubernetesClaims
            | AuthenticatedNodeIdentity::MalformedKubernetesClaims => None,
        }
    }
}

impl AuthenticatedNodeIdentity {
    /// Classify reviewed claims without confusing absent metadata with invalid
    /// supplied values. Validate every present claim before selecting fallback.
    #[cfg(feature = "k8s")]
    pub(super) fn from_reviewed_user_info(
        user: &k8s_openapi::api::authentication::v1::UserInfo,
    ) -> Self {
        use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, PodName, PodUid};
        let claim = |key| match user.extra.as_ref().and_then(|extra| extra.get(key)) {
            None => Some(None),
            Some(values) => match values.as_slice() {
                [value] => Some(Some(value.as_str())),
                _ => None,
            },
        };
        let parse = || {
            let pod_name = claim("authentication.kubernetes.io/pod-name")?
                .map(PodName::parse)
                .transpose()
                .ok()?;
            let pod_uid = claim("authentication.kubernetes.io/pod-uid")?
                .map(PodUid::parse)
                .transpose()
                .ok()?;
            let name = claim("authentication.kubernetes.io/node-name")?
                .map(str::parse::<K8sNodeName>)
                .transpose()
                .ok()?;
            let uid = claim("authentication.kubernetes.io/node-uid")?
                .map(str::parse::<K8sNodeUid>)
                .transpose()
                .ok()?;
            Some(match (pod_name, pod_uid, name, uid) {
                (Some(_), Some(_), Some(name), Some(uid)) => {
                    Self::Kubernetes(AuthenticatedKubernetesNode { name, uid })
                }
                _ => Self::MissingKubernetesClaims,
            })
        };
        parse().unwrap_or(Self::MalformedKubernetesClaims)
    }
}
