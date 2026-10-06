//! Forgetting the guest's cached link-layer address of its gateway.
//!
//! A restored guest keeps the neighbour table it was snapshotted with, and its
//! gateway entry names the network slot the snapshot was taken on; a restore
//! normally lands on another slot, whose gateway answers to a different
//! address. Deleting the entry over netlink makes the next packet resolve the
//! gateway again, without the process start that running `ip neigh flush`
//! costs while the restored guest is still faulting its memory back in.

#[cfg(target_os = "linux")]
use std::net::Ipv4Addr;

#[cfg(target_os = "linux")]
use pplx_space_common_core::guest::linux::network::{GATEWAY_IPV4_ADDR, PRIMARY_INTERFACE};
#[cfg(target_os = "linux")]
use rtnetlink::packet_route::neighbour::{
    NeighbourAddress, NeighbourAttribute, NeighbourMessage, NeighbourState,
};
use tonic::Status;

#[cfg(target_os = "linux")]
/// Whether `entry` is a cached entry for `gateway` on the interface with
/// `ifindex`: the entries `ip neigh flush <gateway> dev <interface>` deletes,
/// which leaves permanent and no-ARP entries alone.
fn is_flushable_gateway_entry(
    ifindex: u32,
    state: NeighbourState,
    attributes: &[NeighbourAttribute],
    interface_index: u32,
    gateway: Ipv4Addr,
) -> bool {
    ifindex == interface_index
        && !matches!(state, NeighbourState::Permanent | NeighbourState::Noarp)
        && attributes.iter().any(|attribute| {
            matches!(
                attribute,
                NeighbourAttribute::Destination(NeighbourAddress::Inet(address))
                    if *address == gateway
            )
        })
}

#[cfg(target_os = "linux")]
fn gateway() -> Result<Ipv4Addr, Status> {
    GATEWAY_IPV4_ADDR
        .parse()
        .map_err(|_| Status::internal("the guest gateway address is not an IPv4 address"))
}

/// Delete the guest's cached entries for its gateway on its primary
/// interface. Deleting nothing is success: the guest has not resolved the
/// gateway yet, or already forgot it.
#[cfg(target_os = "linux")]
pub(crate) async fn flush_gateway_neighbor() -> Result<(), Status> {
    use futures_util::TryStreamExt;
    use rtnetlink::packet_route::AddressFamily;

    let gateway = gateway()?;
    let netlink_error = |what: &str, error: rtnetlink::Error| {
        Status::internal(format!("{what} for the gateway neighbour flush: {error}"))
    };
    let (connection, handle, _) = rtnetlink::new_connection()
        .map_err(|error| Status::internal(format!("open a netlink socket: {error}")))?;
    let connection = tokio::spawn(connection);
    let flushed = async {
        let interface_index = handle
            .link()
            .get()
            .match_name(PRIMARY_INTERFACE.to_owned())
            .execute()
            .try_next()
            .await
            .map_err(|error| netlink_error("look up the primary interface", error))?
            .ok_or_else(|| Status::failed_precondition("the guest has no primary interface"))?
            .header
            .index;
        let mut entries = handle
            .neighbours()
            .get()
            .set_address_family(AddressFamily::Inet)
            .execute();
        let mut gateway_entries: Vec<NeighbourMessage> = Vec::new();
        while let Some(entry) = entries
            .try_next()
            .await
            .map_err(|error| netlink_error("list neighbours", error))?
        {
            if is_flushable_gateway_entry(
                entry.header.ifindex,
                entry.header.state,
                &entry.attributes,
                interface_index,
                gateway,
            ) {
                gateway_entries.push(entry);
            }
        }
        for entry in gateway_entries {
            match handle.neighbours().del(entry).execute().await {
                // The kernel dropped the entry after the dump listed it.
                Err(rtnetlink::Error::NetlinkError(error))
                    if error
                        .code
                        .is_some_and(|code| code.get().abs() == libc::ENOENT) => {}
                result => {
                    result.map_err(|error| netlink_error("delete the gateway entry", error))?
                }
            }
        }
        Ok(())
    }
    .await;
    connection.abort();
    flushed
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn flush_gateway_neighbor() -> Result<(), Status> {
    Err(Status::unimplemented(
        "the gateway neighbour flush needs Linux netlink",
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
