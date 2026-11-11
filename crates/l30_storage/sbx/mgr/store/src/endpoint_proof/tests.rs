use super::*;

fn proof_and_runtime() -> (SpacedEndpointProof, SpacedEndpointRuntimeIdentity) {
    let proof = SpacedEndpointProof {
        sandbox_id: SandboxId::new(),
        execution_generation: ExecutionGeneration::new(1).unwrap(),
        activation_attempt_id: SandboxActivationAttemptId::new(),
        spaced_host: ProcessActivationIdentity {
            pid: LinuxPid::try_from_u32(1).unwrap(),
            start_time_ticks: 1,
            executable_device: 1,
            executable_inode: 1,
        },
        runtime_process: ProcessActivationIdentity {
            pid: LinuxPid::try_from_u32(2).unwrap(),
            start_time_ticks: 2,
            executable_device: 2,
            executable_inode: 2,
        },
        vsock: VsockActivationIdentity {
            path: "/run/v.sock".into(),
            device: 3,
            inode: 3,
            owner_uid: LinuxUid::new(1000),
            owner_gid: 1000,
            guest_cid: VsockCid::try_from_u32(3).unwrap(),
            runtime_fd: 4,
            runtime_socket_inode: 5,
        },
    };
    let runtime = SpacedEndpointRuntimeIdentity {
        sandbox_id: proof.sandbox_id,
        execution_generation: proof.execution_generation,
        activation_attempt_id: proof.activation_attempt_id,
        spaced_host_pid: proof.spaced_host.pid,
        runtime_pid: proof.runtime_process.pid,
        sandbox_uid: proof.vsock.owner_uid,
        guest_cid: proof.vsock.guest_cid,
        vsock_path: proof.vsock.path.clone(),
    };
    (proof, runtime)
}

#[test]
fn execution_dimensions_are_part_of_the_stable_proof() {
    let (proof, runtime) = proof_and_runtime();
    for mutate in [
        |runtime: &mut SpacedEndpointRuntimeIdentity| runtime.sandbox_id = SandboxId::new(),
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.execution_generation = ExecutionGeneration::new(2).unwrap();
        },
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.activation_attempt_id = SandboxActivationAttemptId::new();
        },
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.runtime_pid = LinuxPid::try_from_u32(3).unwrap();
        },
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.spaced_host_pid = LinuxPid::try_from_u32(3).unwrap();
        },
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.sandbox_uid = LinuxUid::new(1001);
        },
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.guest_cid = VsockCid::try_from_u32(4).unwrap();
        },
        |runtime: &mut SpacedEndpointRuntimeIdentity| {
            runtime.vsock_path = "/run/replaced.sock".into();
        },
    ] {
        let mut mismatched = runtime.clone();
        mutate(&mut mismatched);
        assert!(matches!(
            proof.validate_runtime(&mismatched),
            Err(SpacedEndpointProofError::Mismatch(_))
        ));
    }
}

#[test]
fn node_proof_round_trips_only_for_the_expected_runtime() {
    let (proof, runtime) = proof_and_runtime();
    assert_eq!(
        SpacedEndpointProof::from_node_proof(&runtime, proof.node_proof()).unwrap(),
        proof
    );

    let mut wrong_runtime = runtime;
    wrong_runtime.spaced_host_pid = LinuxPid::try_from_u32(9).unwrap();
    assert!(matches!(
        SpacedEndpointProof::from_node_proof(&wrong_runtime, proof.node_proof()),
        Err(SpacedEndpointProofError::Mismatch(_))
    ));
}

#[test]
fn node_proof_rejects_transport_identity_outside_the_active_runtime() {
    let (proof, runtime) = proof_and_runtime();
    let mut wrong_path = proof.node_proof();
    wrong_path.vsock.as_mut().unwrap().path = "/run/replaced.sock".to_string();
    assert!(matches!(
        SpacedEndpointProof::from_node_proof(&runtime, wrong_path),
        Err(SpacedEndpointProofError::Mismatch(_))
    ));

    let mut wrong_owner = proof.node_proof();
    wrong_owner.vsock.as_mut().unwrap().owner_uid += 1;
    assert!(matches!(
        SpacedEndpointProof::from_node_proof(&runtime, wrong_owner),
        Err(SpacedEndpointProofError::Mismatch(_))
    ));
}
