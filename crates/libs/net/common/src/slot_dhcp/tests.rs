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
        (
            vec![53, 1, 3, 50, 4, 169, 254, 0, 23, 255],
            Err(PacketError::Address),
        ),
        (vec![53, 1, 1, 53, 1, 1, 255], Err(PacketError::Option)),
        (vec![53, 1, 1, 52, 1, 1, 255], Err(PacketError::Option)),
        (vec![53, 1, 1, 50, 4, 169], Err(PacketError::Option)),
        (vec![53, 1, 1], Err(PacketError::Option)),
        (vec![53, 1, 1, 54, 4, 10, 0, 0, 1, 255], Ok(None)),
        (vec![53, 1, 7, 255], Ok(None)),
    ] {
        let observed = response(&request(&options), MAC).map(|reply| {
            reply.map(|reply| {
                assert_eq!(&reply[4..8], &[1, 2, 3, 4]);
                assert_eq!(&reply[16..20], &GUEST);
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
