use super::*;

#[test]
fn persisted_launch_authority_rejects_recycled_pid_and_other_subjects() {
    let id = SandboxId::new();
    let witness = Witness {
        sandbox_id: id,
        pid: 123,
        uid: 2_000_042,
        capability: "a".repeat(64),
        incarnation: Incarnation {
            boot_id: "one boot".into(),
            start_ticks: 21,
            executable_device: 2,
            executable_inode: 3,
            network_namespace: 4,
            cgroup_namespace: 5,
        },
    };
    let encoded = serde_json::to_vec(&witness).unwrap();
    let recovered: Witness = serde_json::from_slice(&encoded).unwrap();
    for (subject, pid, uid, capability, allowed) in [
        (id, 123, 2_000_042, "a".repeat(64), true),
        // An old connection can keep the same SO_PEERCRED PID after replacement.
        (id, 123, 2_000_042, "b".repeat(64), false),
        (id, 123, 2_000_042, String::new(), false),
        (SandboxId::new(), 123, 2_000_042, "a".repeat(64), false),
        (id, 124, 2_000_042, "a".repeat(64), false),
        (id, 123, 10_042, "a".repeat(64), false),
    ] {
        assert_eq!(
            recovered
                .authorize(subject, pid, uid, capability.as_bytes())
                .is_ok(),
            allowed
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn birth_evidence_is_sampled_while_the_original_child_is_pinned()
-> Result<(), Box<dyn std::error::Error>> {
    let mut child = std::process::Command::new("sleep").arg("30").spawn()?;
    let result = (|| {
        let handle = ProcessHandle::open(child.id())?;
        let observed = incarnation(&handle)?;
        assert!(observed.start_ticks > 0);
        assert!(observed.executable_inode > 0);
        assert_eq!(incarnation(&handle)?, observed);
        child.kill()?;
        child.wait()?;
        // Even after reaping makes this numeric PID reusable, the original
        // pidfd must not accept evidence belonging to a successor process.
        incarnation(&handle).unwrap_err();
        Ok::<_, Box<dyn std::error::Error>>(())
    })();
    if child.try_wait()?.is_none() {
        child.kill()?;
        child.wait()?;
    }
    result
}
