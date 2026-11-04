use super::*;

fn intel_cpuinfo(model_name: &str, stepping: u32) -> String {
    let block = |processor: u32| {
        format!(
            "processor\t: {processor}\n\
             vendor_id\t: GenuineIntel\n\
             cpu family\t: 6\n\
             model\t\t: 85\n\
             model name\t: {model_name}\n\
             stepping\t: {stepping}\n\
             microcode\t: 0x2007006\n\
             flags\t\t: fpu vme de pse tsc msr\n"
        )
    };
    format!("{}\n{}\n", block(0), block(1))
}

#[test]
fn skylake_and_cascade_lake_m5d_hosts_get_distinct_identities() {
    // The two CPUs behind m5d.metal share family and model and differ only in
    // stepping, so the stepping is what keeps their identities apart.
    let skylake = HostCpu::from_cpuinfo(&intel_cpuinfo(
        "Intel(R) Xeon(R) Platinum 8175M CPU @ 2.50GHz",
        4,
    ))
    .unwrap();
    let cascade_lake = HostCpu::from_cpuinfo(&intel_cpuinfo(
        "Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz",
        7,
    ))
    .unwrap();

    assert_eq!(skylake.as_str(), "genuineintel-6-85-4");
    assert_eq!(cascade_lake.as_str(), "genuineintel-6-85-7");
    assert_ne!(skylake, cascade_lake);
}

#[test]
fn microcode_revision_does_not_change_the_identity() {
    let before = intel_cpuinfo("Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz", 7);
    let after = before.replace("microcode\t: 0x2007006", "microcode\t: 0x5003707");
    assert_ne!(before, after);

    assert_eq!(
        HostCpu::from_cpuinfo(&before),
        HostCpu::from_cpuinfo(&after)
    );
}

#[test]
fn identity_comes_from_the_first_processor_block() {
    let cpuinfo = "\n\nprocessor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 106\nstepping\t: 6\n\n\
                   processor\t: 1\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 143\nstepping\t: 8\n";
    assert_eq!(
        HostCpu::from_cpuinfo(cpuinfo).unwrap().as_str(),
        "genuineintel-6-106-6"
    );

    // A field missing from the first block isn't borrowed from the next one.
    let first_incomplete = "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 106\n\n\
                            processor\t: 1\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 106\nstepping\t: 6\n";
    assert_eq!(HostCpu::from_cpuinfo(first_incomplete), None);
}

#[test]
fn amd_and_graviton_hosts_get_identities() {
    let amd = "processor\t: 0\nvendor_id\t: AuthenticAMD\ncpu family\t: 25\nmodel\t\t: 1\n\
               model name\t: AMD EPYC 7R13 Processor\nstepping\t: 1\nmicrocode\t: 0xa0011d1\n";
    assert_eq!(
        HostCpu::from_cpuinfo(amd).unwrap().as_str(),
        "authenticamd-25-1-1"
    );

    // Graviton lists BogoMIPS and Features before the CPU fields.
    let graviton = "processor\t: 0\nBogoMIPS\t: 243.75\nFeatures\t: fp asimd evtstrm aes pmull\n\
                    CPU implementer\t: 0x41\nCPU architecture: 8\nCPU variant\t: 0x1\n\
                    CPU part\t: 0xd40\nCPU revision\t: 1\n\n\
                    processor\t: 1\nBogoMIPS\t: 243.75\nCPU implementer\t: 0x41\n";
    assert_eq!(
        HostCpu::from_cpuinfo(graviton).unwrap().as_str(),
        "arm-41-d40-1-1"
    );
}

#[test]
fn incomplete_or_malformed_cpuinfo_is_unknown() {
    for cpuinfo in [
        "",
        "processor\t: 0\n",
        // x86 vendor without the stepping.
        "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 85\n",
        // Non-numeric model.
        "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: x\nstepping\t: 4\n",
        // Arm without the part number.
        "processor\t: 0\nCPU implementer\t: 0x41\nCPU variant\t: 0x1\nCPU revision\t: 1\n",
        // A vendor string that can't be a label value.
        "processor\t: 0\nvendor_id\t: Genuine Intel\ncpu family\t: 6\nmodel\t\t: 85\nstepping\t: 4\n",
    ] {
        assert_eq!(HostCpu::from_cpuinfo(cpuinfo), None, "{cpuinfo:?}");
    }
}

#[test]
fn parse_accepts_label_values_and_rejects_everything_else() {
    assert_eq!(
        HostCpu::parse("genuineintel-6-85-7").unwrap().as_str(),
        "genuineintel-6-85-7"
    );
    assert_eq!(
        HostCpu::parse("GenuineIntel-6-85-7").unwrap().as_str(),
        "genuineintel-6-85-7"
    );
    assert_eq!(HostCpu::parse("a".repeat(63)).unwrap().as_str().len(), 63);
    assert_eq!(HostCpu::parse(""), Err(HostCpuParseError::Empty));
    for invalid in [
        "-leading",
        "trailing-",
        "has space",
        "Intel(R)",
        &"a".repeat(64),
    ] {
        HostCpu::parse(invalid).unwrap_err();
    }
}

#[cfg(feature = "serde")]
#[test]
fn serde_round_trips_and_rejects_invalid_values() {
    let cpu = HostCpu::parse("genuineintel-6-85-4").unwrap();
    let json = serde_json::to_string(&cpu).unwrap();
    assert_eq!(json, "\"genuineintel-6-85-4\"");
    assert_eq!(serde_json::from_str::<HostCpu>(&json).unwrap(), cpu);
    serde_json::from_str::<HostCpu>("\"Intel(R) Xeon(R)\"").unwrap_err();
}

#[test]
fn compatibility_pairs_are_one_way_and_reflexive() {
    let compat = HostCpuCompatibility::parse(
        " genuineintel-6-85-4 => genuineintel-6-85-7 , ,genuineintel-6-85-4=>genuineintel-6-106-6",
    )
    .unwrap();
    let skylake = HostCpu::parse("genuineintel-6-85-4").unwrap();
    let cascade_lake = HostCpu::parse("genuineintel-6-85-7").unwrap();
    let ice_lake = HostCpu::parse("genuineintel-6-106-6").unwrap();

    assert!(compat.can_load(&skylake, &cascade_lake));
    assert!(compat.can_load(&skylake, &ice_lake));
    assert!(!compat.can_load(&cascade_lake, &skylake));
    // Not transitive: Cascade Lake state on Ice Lake isn't listed.
    assert!(!compat.can_load(&cascade_lake, &ice_lake));
    assert!(compat.can_load(&cascade_lake, &cascade_lake));
    assert!(
        HostCpuCompatibility::parse("GenuineIntel-6-85-4=>GENUINEINTEL-6-85-7")
            .unwrap()
            .can_load(&skylake, &cascade_lake)
    );
    assert_eq!(
        HostCpuCompatibility::parse("").unwrap(),
        HostCpuCompatibility::default()
    );
}

#[test]
fn compatibility_rejects_malformed_entries() {
    assert!(matches!(
        HostCpuCompatibility::parse("genuineintel-6-85-4"),
        Err(HostCpuCompatibilityParseError::MissingArrow { .. })
    ));
    assert!(matches!(
        HostCpuCompatibility::parse("genuineintel-6-85-4=>Intel(R)"),
        Err(HostCpuCompatibilityParseError::InvalidHostCpu { .. })
    ));
}
