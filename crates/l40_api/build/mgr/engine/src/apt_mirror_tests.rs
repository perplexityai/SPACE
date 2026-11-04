use super::*;

#[test]
fn package_sources_change_only_for_an_explicit_mirror() {
    let sources = [
        (
            "sources.list",
            "deb http://archive.ubuntu.com/ubuntu noble main\n",
        ),
        (
            "ubuntu.sources",
            "Types: deb\nURIs: https://security.ubuntu.com/ubuntu\nSuites: noble-security\n",
        ),
        (
            "custom.sources",
            "Types: deb\nURIs: http://packages.example/debian\nSuites: stable\n",
        ),
    ];
    for mirror in [
        None,
        Some("http://mirror.example/ubuntu"),
        Some("https://mirror.example/ubuntu"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let apt = root.path().join("apt");
        fs::create_dir(&apt).unwrap();
        for (name, content) in sources {
            fs::write(apt.join(name), content).unwrap();
        }
        if let Some(command) = guest_apt_mirror_command(mirror) {
            // Run the guest command against disposable sources, with no installed helper.
            let command = command.replace("/etc/apt", apt.to_str().unwrap()).replace(
                "/usr/local/sbin/space-configure-apt-mirror",
                root.path().join("absent-helper").to_str().unwrap(),
            );
            let output = Command::new("bash")
                .args(["-c", &command])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        for (name, content) in sources {
            let expected = match mirror {
                None => content.to_owned(),
                Some(mirror) => content
                    .replace("http://archive.ubuntu.com/ubuntu", mirror)
                    .replace("https://security.ubuntu.com/ubuntu", mirror),
            };
            assert_eq!(
                fs::read_to_string(apt.join(name)).unwrap(),
                expected,
                "{mirror:?}: {name}"
            );
        }
        assert_eq!(apt.join("apt.conf.d").exists(), mirror.is_some());
    }
}
