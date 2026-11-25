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

/// Runs `f` on a thread moved into a fresh network namespace, which threads
/// and processes `f` starts inherit: a missed namespace entry must land in a
/// throwaway namespace, not the runner's.
pub(crate) fn contained<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET).expect("unshare runs");
                f()
            })
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// Runs `f` on a thread inside the named namespace; sockets it opens stay there.
pub(crate) fn inside<T: Send>(name: &str, f: impl FnOnce() -> T + Send) -> T {
    let netns = std::fs::File::open(format!("/run/netns/{name}")).expect("namespace opens");
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                nix::sched::setns(&netns, nix::sched::CloneFlags::CLONE_NEWNET)
                    .expect("setns runs");
                f()
            })
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// Replaces `target`'s dummy tap0 with a veth whose peer, `guest`'s eth0, can
/// send: 10.0.0.1 on tap0 and 10.0.0.2 on eth0.
pub(crate) fn link_guest(target: &TestNamespace, guest: &TestNamespace) {
    let (t, g) = (target.name(), guest.name());
    ip(&["-n", t, "link", "del", "tap0"]);
    ip(&[
        "-n", t, "link", "add", "tap0", "type", "veth", "peer", "name", "eth0", "netns", g,
    ]);
    ip(&["-n", t, "addr", "add", "10.0.0.1/24", "dev", "tap0"]);
    ip(&["-n", g, "addr", "add", "10.0.0.2/24", "dev", "eth0"]);
    ip(&["-n", t, "link", "set", "tap0", "up"]);
    ip(&["-n", g, "link", "set", "eth0", "up"]);
}

pub(crate) fn ip(args: &[&str]) {
    let output = std::process::Command::new("ip")
        .args(args)
        .output()
        .expect("ip runs");
    assert!(output.status.success(), "ip {args:?}: {output:?}");
}
