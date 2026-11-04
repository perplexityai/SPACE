//! Stateless DHCP for one execution-owned /30 link. It cannot choose addresses,
//! relay traffic, allocate leases or alter host configuration.

pub const MAX_PACKET: usize = 1500;
pub const GUEST: [u8; 4] = crate::guest_link::GUEST.octets();
pub const GATEWAY: [u8; 4] = crate::guest_link::GATEWAY.octets();
const COOKIE: [u8; 4] = [99, 130, 83, 99];

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PacketError {
    #[error("invalid DHCP header or packet size")]
    Header,
    #[error("DHCP client does not own this link")]
    Client,
    #[error("invalid, duplicate or unsupported DHCP option")]
    Option,
}

/// Parse a bounded request and construct a fixed-address response. None means a
/// valid message that needs no reply (release/decline/inform or another server).
/// Allocation and execution fencing belong to the socket owner.
pub fn response(packet: &[u8], mac: [u8; 6]) -> Result<Option<Vec<u8>>, PacketError> {
    if !(240..=MAX_PACKET).contains(&packet.len())
        || packet.get(..4) != Some(&[1, 1, 6, 0])
        || packet.get(236..240) != Some(COOKIE.as_slice())
        || packet.get(24..28) != Some(&[0, 0, 0, 0])
    {
        return Err(PacketError::Header);
    }
    if packet.get(28..34) != Some(mac.as_slice()) {
        return Err(PacketError::Client);
    }
    let ciaddr: [u8; 4] = packet
        .get(12..16)
        .and_then(|v| v.try_into().ok())
        .ok_or(PacketError::Header)?;
    let mut options = packet.get(240..).ok_or(PacketError::Header)?;
    let mut seen = [false; 256];
    let mut message = None;
    let mut requested = None;
    let mut server = None;
    let mut ended = false;
    let mut client_identifier = None;
    while let Some((&kind, rest)) = options.split_first() {
        options = rest;
        match kind {
            0 => continue,
            255 => {
                ended = true;
                break;
            }
            _ => {}
        }
        let duplicate = seen.get_mut(usize::from(kind)).ok_or(PacketError::Option)?;
        if *duplicate {
            return Err(PacketError::Option);
        }
        *duplicate = true;
        let (&length, rest) = options.split_first().ok_or(PacketError::Option)?;
        let (value, rest) = rest
            .split_at_checked(usize::from(length))
            .ok_or(PacketError::Option)?;
        options = rest;
        match kind {
            53 => {
                let [kind] = value else {
                    return Err(PacketError::Option);
                };
                message = Some(*kind);
            }
            50 => requested = Some(<[u8; 4]>::try_from(value).map_err(|_| PacketError::Option)?),
            54 => server = Some(<[u8; 4]>::try_from(value).map_err(|_| PacketError::Option)?),
            61 => {
                // This responder supports Ethernet/MAC identifiers, not DUIDs.
                if value.first() != Some(&1) || value.get(1..) != Some(mac.as_slice()) {
                    return Err(PacketError::Client);
                }
                client_identifier = Some(value);
            }
            // No option-overload parsing and no relay-agent trust boundary.
            52 | 82 => return Err(PacketError::Option),
            _ => {}
        }
    }
    if !ended || options.iter().any(|byte| *byte != 0) {
        return Err(PacketError::Option);
    }
    if server.is_some_and(|address| address != GATEWAY) {
        return Ok(None);
    }
    let response_kind = match message {
        // A DISCOVER's requested address is a hint, not an allocation constraint.
        Some(1) if ciaddr == [0; 4] => 2,
        Some(3) => {
            let address = match (ciaddr, requested, server) {
                ([0, 0, 0, 0], Some(address), _) => address,
                (address, None, None) if address != [0; 4] => address,
                _ => return Err(PacketError::Option),
            };
            if address == GUEST { 5 } else { 6 } // ACK or NAK, including INIT-REBOOT.
        }
        Some(4 | 7 | 8) => return Ok(None),
        _ => return Err(PacketError::Option),
    };
    let mut reply = vec![0_u8; 240];
    put(&mut reply, 0, &[2, 1, 6, 0])?;
    put(&mut reply, 4, packet.get(4..8).ok_or(PacketError::Header)?)?;
    put(&mut reply, 10, &[0x80, 0])?; // Broadcast, including before the guest has an IP.
    if response_kind != 6 {
        put(&mut reply, 16, &GUEST)?;
        put(&mut reply, 20, &GATEWAY)?;
    }
    put(&mut reply, 28, &mac)?;
    put(&mut reply, 236, &COOKIE)?;
    reply.extend_from_slice(&[53, 1, response_kind, 54, 4]);
    reply.extend_from_slice(&GATEWAY);
    if let Some(identifier) = client_identifier {
        // RFC 6842: echo the validated client identifier in every response.
        reply.extend_from_slice(&[61, 7]);
        reply.extend_from_slice(identifier);
    }
    if response_kind != 6 {
        reply.extend_from_slice(&[51, 4, 0, 0, 14, 16]); // One-hour lease.
        reply.extend_from_slice(&[1, 4]);
        reply.extend_from_slice(&crate::guest_link::NETMASK.octets());
        reply.extend_from_slice(&[3, 4]);
        reply.extend_from_slice(&GATEWAY);
        reply.extend_from_slice(&[6, 8, 1, 1, 1, 1, 8, 8, 8, 8]);
    }
    reply.push(255);
    reply.resize(reply.len().max(300), 0);
    Ok(Some(reply))
}

fn put(bytes: &mut [u8], offset: usize, value: &[u8]) -> Result<(), PacketError> {
    bytes
        .get_mut(offset..offset + value.len())
        .ok_or(PacketError::Header)?
        .copy_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests;
