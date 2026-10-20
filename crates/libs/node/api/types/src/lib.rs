/// Generated types and service traits from `proto/node/api/v1/common.proto`.
///
/// Both `pplx-space-node-api-client` (worker/builder services) and
/// `pplx-space-cluster-api-client` (cluster service) depend on this crate for
/// the shared vocabulary types (`NodeRole`, `FacetRef`, `NodeSchedulingStatus`,
/// etc.) so neither depends on the other.
pub mod proto {
    tonic::include_proto!("space.node.api.v1");
}
