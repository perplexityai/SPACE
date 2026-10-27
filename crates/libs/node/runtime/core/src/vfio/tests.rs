use super::*;

#[test]
fn pci_locators_are_canonical_and_cannot_name_paths_or_non_pci_functions() {
    for address in ["0000:00:00.0", "ffff:ff:1f.7", "0001:ab:10.3"] {
        let parsed: PciAddress = address.parse().unwrap();
        assert_eq!(parsed.to_string(), address);
        assert_eq!(
            PciAddress::from_locator(&format!("pci:{address}")).unwrap(),
            parsed
        );
        assert_eq!(
            parsed.sysfs_path(),
            PathBuf::from("/sys/bus/pci/devices").join(address)
        );
    }
    for value in [
        "",
        "../devices",
        "0000:00:20.0",
        "0000:00:00.8",
        "0000:AB:00.0",
        "0000:00:00.0/driver",
        "0000:00:00.0\0",
        "0:00:00.0",
        "0000:00:00.00",
        "0000:00:00.é",
    ] {
        value.parse::<PciAddress>().unwrap_err();
    }
    for locator in [
        "0000:00:00.0",
        "pci:/sys/devices/0000:00:00.0",
        "mdev:0000:00:00.0",
        "pci:../0000:00:00.0",
    ] {
        PciAddress::from_locator(locator).unwrap_err();
    }
}

#[test]
fn iommu_membership_requires_the_whole_group_and_the_same_device_inodes() {
    use std::collections::BTreeMap;
    let first: PciAddress = "0000:02:00.0".parse().unwrap();
    let second: PciAddress = "0000:02:00.1".parse().unwrap();
    let assigned = BTreeMap::from([(first, (1, 2)), (second, (1, 3))]);
    check_group_members(&assigned, &assigned).unwrap();
    for observed in [
        BTreeMap::new(),
        BTreeMap::from([(first, (1, 2))]),
        BTreeMap::from([(first, (1, 2)), (second, (1, 4))]),
    ] {
        check_group_members(&assigned, &observed).unwrap_err();
    }
    let partial = BTreeMap::from([(first, (1, 2))]);
    check_group_members(&partial, &assigned).unwrap_err();
    check_group_members(&BTreeMap::new(), &BTreeMap::new()).unwrap_err();
}
