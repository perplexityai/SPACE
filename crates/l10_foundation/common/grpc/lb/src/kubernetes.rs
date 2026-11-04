//! Resolves a Service's ready endpoints by watching its EndpointSlices.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use futures_util::{Stream, StreamExt};
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointSlice};
use kube::runtime::reflector::Store;
use kube::runtime::{WatchStreamExt, reflector, watcher};
use kube::{Api, Client};

use crate::{EndpointSet, Resolution};

/// Every EndpointSlice of a Service carries its name here; this is the watch's
/// server-side filter.
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

const FQDN_ADDRESS_TYPE: &str = "FQDN";

/// Failure to reach Kubernetes at all, kept as its own type so a caller can
/// report it without depending on `kube`.
#[derive(Debug, thiserror::Error)]
#[error("kubernetes client for endpoint discovery: {0}")]
pub struct KubernetesConnectError(#[from] kube::Error);

pub struct KubernetesEndpoints {
    service: &'static str,
    api: Api<EndpointSlice>,
    kubernetes_service: String,
    port: PortSelector,
}

/// Which EndpointSlice port carries the gRPC service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortSelector {
    Sole,
    Name(String),
    Number(u16),
}

impl fmt::Display for PortSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sole => f.write_str("<sole>"),
            Self::Name(name) => f.write_str(name),
            Self::Number(port) => write!(f, "{port}"),
        }
    }
}

impl KubernetesEndpoints {
    /// Builds one client for this resolver. The client owns a connection pool,
    /// so it is created once here rather than per watch attempt.
    pub async fn connect(
        service: &'static str,
        kubernetes_service: impl Into<String>,
        port: PortSelector,
    ) -> Result<Self, KubernetesConnectError> {
        let client = Client::try_default().await?;
        // `default_namespaced` uses the namespace from the active kubeconfig,
        // which in-cluster configuration obtains from the mounted service
        // account. Callers therefore do not need separate namespace plumbing.
        let api = Api::default_namespaced(client);
        Ok(Self {
            service,
            api,
            kubernetes_service: kubernetes_service.into(),
            port,
        })
    }

    /// Yields the ready endpoint set on every change, and `Failed` on every
    /// watch error — a healthy watch is silent between changes, so failure has
    /// to be stated for the consumer's staleness bound to mean anything.
    pub fn watch(self) -> impl Stream<Item = Resolution> + Send {
        let Self {
            service,
            api,
            kubernetes_service,
            port,
        } = self;
        // `any_semantic` serves the initial list — and every relist after a watch
        // error — from the apiserver's cache instead of a quorum read from etcd.
        // A resolver needs a starting point, not linearizability; the watch
        // corrects it immediately. The default would have every client pod issue
        // a quorum read at the moment an apiserver starts flapping.
        let config = watcher::Config::default()
            .labels(&format!("{SERVICE_NAME_LABEL}={kubernetes_service}"))
            .any_semantic();
        let (store, writer) = reflector::store::<EndpointSlice>();

        reflector::reflector(writer, watcher::watcher(api, config))
            .default_backoff()
            .filter_map(move |event| std::future::ready(resolved(service, event, &store, &port)))
    }
}

/// A Service's endpoints span several slices, so only `InitDone` — where the
/// reflector has swapped in a complete relist — and steady-state changes yield a
/// set. The `InitApply` run in between is partial.
fn resolved(
    service: &'static str,
    event: Result<watcher::Event<EndpointSlice>, watcher::Error>,
    store: &Store<EndpointSlice>,
    port: &PortSelector,
) -> Option<Resolution> {
    let event = match event {
        Ok(event) => event,
        Err(error) => {
            pplx_space_observability::with_peer_service!(
                tracing::warn!(peer.service = service, %error, "endpointslice watch failed; retrying")
            );
            return Some(Resolution::Failed);
        }
    };
    match event {
        watcher::Event::InitDone | watcher::Event::Apply(_) | watcher::Event::Delete(_) => {
            let endpoints: EndpointSet = store
                .state()
                .iter()
                .flat_map(|slice| slice_addrs(slice, port))
                .collect();
            Some(Resolution::Endpoints(endpoints))
        }
        watcher::Event::Init | watcher::Event::InitApply(_) => None,
    }
}

fn slice_addrs(slice: &EndpointSlice, selector: &PortSelector) -> Vec<SocketAddr> {
    let name = slice.metadata.name.as_deref().unwrap_or_default();
    if slice.address_type == FQDN_ADDRESS_TYPE {
        tracing::warn!(
            slice = name,
            "skipping FQDN endpointslice; only IP addresses can be balanced over"
        );
        return Vec::new();
    }
    let Some(port) = select_port(slice, selector) else {
        tracing::warn!(
            slice = name,
            %selector,
            "endpointslice has no port matching the selector"
        );
        return Vec::new();
    };
    slice
        .endpoints
        .iter()
        .filter(|endpoint| is_ready(endpoint))
        .flat_map(|endpoint| endpoint.addresses.iter())
        .filter_map(|address| address.parse::<IpAddr>().ok())
        .map(|ip| SocketAddr::new(ip, port))
        .collect()
}

/// A terminating pod normally reports `ready: false` with `serving: true`.
/// Require both readiness and non-termination so an inconsistent or lagging
/// readiness condition cannot send new traffic into a pod's grace period.
/// An absent readiness condition means ready, per the API spec.
fn is_ready(endpoint: &Endpoint) -> bool {
    let ready = endpoint
        .conditions
        .as_ref()
        .and_then(|conditions| conditions.ready)
        .unwrap_or(true);
    let terminating = endpoint
        .conditions
        .as_ref()
        .and_then(|conditions| conditions.terminating)
        .unwrap_or(false);
    ready && !terminating
}

fn select_port(slice: &EndpointSlice, selector: &PortSelector) -> Option<u16> {
    let ports = slice.ports.as_deref().unwrap_or_default();
    let port = match selector {
        PortSelector::Sole => match ports {
            [only] => only.port,
            _ => None,
        },
        PortSelector::Name(name) => ports
            .iter()
            .find(|port| port.name.as_deref() == Some(name.as_str()))
            .and_then(|port| port.port),
        PortSelector::Number(number) => ports
            .iter()
            .find(|port| port.port == Some(i32::from(*number)))
            .and_then(|port| port.port),
    };
    port.and_then(|port| u16::try_from(port).ok())
}

#[cfg(test)]
mod tests;
