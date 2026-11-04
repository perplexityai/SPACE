use super::*;
const MAC: [u8; 6] = [0xaa, 0xfc, 0, 0, 0, 1];
fn request(options: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0; 240];
    bytes[..4].copy_from_slice(&[1, 1, 6, 0]);
    bytes[4..8].copy_from_slice(&[1, 2, 3, 4]);
    bytes[28..34].copy_from_slice(&MAC);
    bytes[236..240].copy_from_slice(&COOKIE);
    bytes.extend_from_slice(options);
    bytes
}
#[test]
fn fixed_link_dhcp_negotiation_and_refusals() {
    for (options, result) in [
        (vec![53, 1, 1, 255], Ok(Some(2))),
        (vec![53, 1, 3, 50, 4, 169, 254, 0, 21, 255], Ok(Some(5))),
        (vec![53, 1, 3, 255], Err(PacketError::Option)),
        (vec![53, 1, 1, 50, 4, 10, 0, 0, 9, 255], Ok(Some(2))),
        (vec![53, 1, 3, 50, 4, 169, 254, 0, 23, 255], Ok(Some(6))),
        (vec![53, 1, 1, 53, 1, 1, 255], Err(PacketError::Option)),
        (vec![53, 1, 1, 52, 1, 1, 255], Err(PacketError::Option)),
        (vec![53, 1, 1, 50, 4, 169], Err(PacketError::Option)),
        (vec![53, 1, 1], Err(PacketError::Option)),
        (vec![53, 1, 1, 54, 4, 10, 0, 0, 1, 255], Ok(None)),
        (
            vec![53, 1, 3, 50, 4, 10, 0, 0, 9, 54, 4, 10, 0, 0, 1, 255],
            Ok(None),
        ),
        (vec![53, 1, 7, 255], Ok(None)),
    ] {
        let observed = response(&request(&options), MAC).map(|reply| {
            reply.map(|reply| {
                assert_eq!(&reply[4..8], &[1, 2, 3, 4]);
                assert_eq!(
                    &reply[16..20],
                    if reply[242] == 6 { &[0; 4] } else { &GUEST }
                );
                assert_eq!(&reply[28..34], &MAC);
                assert!(reply.len() <= MAX_PACKET);
                reply[242]
            })
        });
        assert_eq!(observed, result, "options: {options:?}");
    }
    let bytes = request(&[53, 1, 1, 255]);
    assert_eq!(response(&bytes, [0; 6]), Err(PacketError::Client));
    for end in 0..bytes.len() {
        response(&bytes[..end], MAC).unwrap_err();
    }
}

#[test]
fn renewal_and_client_identity() {
    for (address, expected) in [(GUEST, 5), ([10, 0, 0, 9], 6)] {
        let mut packet = request(&[53, 1, 3, 61, 7, 1, 0xaa, 0xfc, 0, 0, 0, 1, 255]);
        packet[12..16].copy_from_slice(&address);
        let reply = response(&packet, MAC).unwrap().unwrap();
        assert_eq!(reply[242], expected);
        assert!(
            reply[240..]
                .windows(9)
                .any(|v| v == [61, 7, 1, 0xaa, 0xfc, 0, 0, 0, 1])
        );
        if expected == 6 {
            assert_eq!(&reply[12..28], &[0; 16]);
            assert!(!reply[240..].windows(2).any(|v| v == [51, 4]));
        }
    }
    for options in [
        vec![53, 1, 3, 50, 4, 169, 254, 0, 21, 255],
        vec![53, 1, 3, 54, 4, 169, 254, 0, 22, 255],
    ] {
        let mut packet = request(&options);
        packet[12..16].copy_from_slice(&GUEST);
        assert_eq!(response(&packet, MAC), Err(PacketError::Option));
    }
    for identifier in [
        vec![1, 0xaa, 0xfc, 0, 0, 0, 2],
        vec![255, 0, 0, 0, 1],
        vec![],
    ] {
        let mut options = vec![53, 1, 1, 61, u8::try_from(identifier.len()).unwrap()];
        options.extend(identifier);
        options.push(255);
        assert_eq!(response(&request(&options), MAC), Err(PacketError::Client));
    }
}
