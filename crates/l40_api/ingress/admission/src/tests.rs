use super::*;

#[test]
fn saturated_sandbox_does_not_block_others_and_drop_restores_capacity() {
    let sandbox = SandboxId::new();
    let held: Vec<_> = (0..SANDBOX_LIMIT)
        .map(|_| IngressPreparationPermit::acquire(Some(sandbox)).unwrap())
        .collect();
    assert!(matches!(
        IngressPreparationPermit::acquire(Some(sandbox)),
        Err(status) if status.code() == tonic::Code::ResourceExhausted
    ));
    let other = IngressPreparationPermit::acquire(Some(SandboxId::new())).unwrap();
    let mut unresolved = IngressPreparationPermit::acquire(None).unwrap();
    assert_eq!(
        unresolved.bind_sandbox(sandbox).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    drop(held);
    unresolved.bind_sandbox(sandbox).unwrap();
    // Both forms of cancelled preparation release exactly their own capacity.
    drop(unresolved);
    drop(other);
    let replacement: Vec<_> = (0..SANDBOX_LIMIT)
        .map(|_| IngressPreparationPermit::acquire(Some(sandbox)).unwrap())
        .collect();
    drop(replacement);
}
