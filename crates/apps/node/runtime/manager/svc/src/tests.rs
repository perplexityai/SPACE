use super::*;

#[test]
fn release_source_options_reject_credentials_without_echoing_them() {
    for flag in ["--release-source", "--cloud-hypervisor-release-source"] {
        for value in [
            "https://user:synthetic-private-value@example.test/releases",
            "https://example.test/releases?token=synthetic-private-value",
            "https://example.test/releases#synthetic-private-value",
        ] {
            let error = Cli::try_parse_from(["space-rtmgr-svc", flag, value])
                .err()
                .expect("credential-bearing release sources must be refused");
            assert!(!error.to_string().contains("synthetic-private-value"));
        }
        Cli::try_parse_from([
            "space-rtmgr-svc",
            flag,
            "https://example.test/releases/@stable",
        ])
        .unwrap();
    }
}
