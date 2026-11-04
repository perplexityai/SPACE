use super::*;
use std::os::unix::process::ExitStatusExt as _;

#[test]
fn cache_requires_pinned_identity_and_bounded_fresh_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("namespace");
    std::fs::write(&path, []).unwrap();
    let evidence = || Evidence {
        namespace: Some("pod".into()),
        interface: "eth0".into(),
        path: path.display().to_string(),
        file: File::open(&path).unwrap(),
    };
    let mut entries = Vec::new();
    remember(
        &mut entries,
        Entry {
            namespace: Some("pod".into()),
            interface: "eth0".into(),
            file: File::open(&path).unwrap(),
            rules: b"required rules".to_vec(),
            observed: Instant::now(),
        },
    );
    assert_eq!(
        cached_rules(&mut entries, &evidence()),
        Some(b"required rules".to_vec())
    );
    let original = evidence();
    for (rules, status, expected) in [
        (b"required rules".as_slice(), 0, true),
        (b"missing a rule".as_slice(), 0, false),
        (b"".as_slice(), 0, false),
        (b"required rules".as_slice(), 256, false),
    ] {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(status),
            stdout: rules.to_vec(),
            stderr: Vec::new(),
        };
        assert_eq!(
            original.matches_output(b"required rules", &output).unwrap(),
            expected
        );
    }
    // Namespace-name reuse cannot hit the old, still-pinned inode.
    std::fs::rename(&path, dir.path().join("old-namespace")).unwrap();
    std::fs::write(&path, []).unwrap();
    original.unchanged().unwrap_err();
    assert_eq!(cached_rules(&mut entries, &evidence()), None);
    for n in 0..100 {
        remember(
            &mut entries,
            Entry {
                namespace: Some(format!("pod-{n}")),
                interface: "eth0".into(),
                file: File::open(&path).unwrap(),
                rules: vec![1],
                observed: Instant::now(),
            },
        );
        assert!(entries.len() <= CAPACITY);
    }
    for entry in &mut entries {
        entry.observed = Instant::now() - MAX_AGE;
    }
    assert_eq!(cached_rules(&mut entries, &evidence()), None);
    assert!(entries.is_empty());
    remember(
        &mut entries,
        Entry {
            namespace: None,
            interface: "eth0".into(),
            file: File::open(&path).unwrap(),
            rules: vec![0; MAX_RULE_BYTES + 1],
            observed: Instant::now(),
        },
    );
    assert!(entries.is_empty());
}
