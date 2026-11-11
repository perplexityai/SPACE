use crate::guest_service::connect_uds;
use pplx_space_spaced_common::proto::bridge_guest::meta_client::MetaClient as BridgeGuestClient;
use pplx_space_spaced_guest_client::proto::meta_client::MetaClient as SpacedGuestClient;
use std::time::Duration;

pub(crate) fn duration_from_millis(milliseconds: u64) -> prost_types::Duration {
    Duration::from_millis(milliseconds)
        .try_into()
        .expect("fixture duration fits the wire range")
}

pub(crate) async fn connect_bridge_guest(
    socket_path: std::path::PathBuf,
) -> BridgeGuestClient<tonic::transport::Channel> {
    let channel = connect_uds(socket_path).await;
    BridgeGuestClient::new(channel)
}

pub(crate) async fn connect_local_guest(
    socket_path: std::path::PathBuf,
) -> SpacedGuestClient<tonic::transport::Channel> {
    let channel = connect_uds(socket_path).await;
    SpacedGuestClient::new(channel)
}
