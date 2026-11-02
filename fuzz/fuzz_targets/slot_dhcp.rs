#![no_main]
use libfuzzer_sys::fuzz_target;
use pplx_space_net_common::slot_dhcp::{GUEST, MAX_PACKET, response};

fuzz_target!(|bytes: &[u8]| {
    // Use the presented address to reach option parsing, including arbitrary
    // client identifiers. The production caller supplies the assigned MAC.
    let mac = bytes
        .get(28..34)
        .and_then(|value| value.try_into().ok())
        .unwrap_or([0; 6]);
    if let Ok(Some(reply)) = response(bytes, mac) {
        assert!(reply.len() <= MAX_PACKET);
        let address = if reply.get(242) == Some(&6) {
            [0; 4]
        } else {
            GUEST
        };
        assert_eq!(reply.get(16..20), Some(address.as_slice()));
        assert_eq!(reply.get(28..34), Some(mac.as_slice()));
    }
});
