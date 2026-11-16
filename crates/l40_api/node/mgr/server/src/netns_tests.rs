//! Disposable network namespaces for privileged tests. Needs root and
//! iproute2; tests using it skip unless root and belong in the nextest
//! privileged group.

/// A network namespace with a dummy `tap0`, deleted on drop.
#[derive(Debug)]
pub(crate) struct TestNamespace(String);

impl TestNamespace {
    pub(crate) fn create() -> Self {
        let name = format!("space-test-{}", uuid::Uuid::now_v7().simple());
        ip(&["netns", "add", &name]);
        let namespace = Self(name);
        // nft binds the guest L2 chain to tap0 by name; a dummy link stands in.
        ip(&[
            "-n",
            namespace.name(),
            "link",
            "add",
            "tap0",
            "type",
            "dummy",
        ]);
        namespace
    }

    pub(crate) fn name(&self) -> &str {
        &self.0
    }
}

impl Drop for TestNamespace {
    fn drop(&mut self) {
        let output = std::process::Command::new("ip")
            .args(["netns", "del", &self.0])
            .output();
        if !output.as_ref().is_ok_and(|output| output.status.success()) {
            eprintln!("failed to remove test namespace {}: {output:?}", self.0);
        }
    }
}

pub(crate) fn ip(args: &[&str]) {
    let output = std::process::Command::new("ip")
        .args(args)
        .output()
        .expect("ip runs");
    assert!(output.status.success(), "ip {args:?}: {output:?}");
}
