use super::{LinuxInterfaceName, LinuxNetnsName};

#[test]
fn linux_netns_name_allows_current_64_byte_policy() {
    LinuxNetnsName::parse("n".repeat(64)).unwrap();
    LinuxNetnsName::parse("n".repeat(65)).unwrap_err();
}

#[test]
fn linux_interface_name_enforces_kernel_ifnamsiz_limit() {
    LinuxInterfaceName::parse("123456789012345").unwrap();
    LinuxInterfaceName::parse("1234567890123456").unwrap_err();
    LinuxInterfaceName::parse("my-long-tap-iface-name").unwrap_err();
}

#[test]
fn linux_network_slot_checks_configured_range() {
    let slot = super::LinuxNetworkSlot::try_from_u32(999, 1000).unwrap();
    assert_eq!(slot.get(), 999);
    assert_eq!(slot.label(), "000999");
    super::LinuxNetworkSlot::try_from_u32(1000, 1000).unwrap_err();
    super::LinuxNetworkSlot::try_from_u32(0, 0).unwrap_err();
    super::LinuxNetworkSlot::try_from_u32(
        super::MAX_LINUX_NETWORK_SLOT,
        super::MAX_LINUX_NETWORK_SLOT + 1,
    )
    .unwrap();
    super::LinuxNetworkSlot::try_from_u32(
        super::MAX_LINUX_NETWORK_SLOT + 1,
        super::MAX_LINUX_NETWORK_SLOT + 2,
    )
    .unwrap_err();
}
