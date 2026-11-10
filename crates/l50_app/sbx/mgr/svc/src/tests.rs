use super::*;

#[test]
fn automatic_update_setting_is_typed_and_reaches_service_configuration() {
    for (value, expected) in [
        ("0", false),
        ("false", false),
        ("off", false),
        ("1", true),
        ("true", true),
        ("on", true),
    ] {
        let cli =
            Cli::try_parse_from(["space-sbxmgr-svc", "--automatic-guest-updates", value]).unwrap();
        assert_eq!(SbxMgrRunConfig::from(cli).automatic_guest_updates, expected);
    }
    assert!(
        Cli::try_parse_from(["space-sbxmgr-svc", "--automatic-guest-updates", "typo"]).is_err()
    );
}
