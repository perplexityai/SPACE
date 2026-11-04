use super::*;

fn parse(raw: &str) -> Result<BalloonConfig, String> {
    serde_json::from_str::<BalloonConfig>(raw).map_err(|error| error.to_string())
}

#[test]
fn metadata_balloon_settings_parse_strictly() {
    let inflate = BalloonConfig::new(
        BalloonDevice::Inflate {
            amount_mib: NonZeroU32::new(1024).unwrap(),
            deflate_on_oom: true,
        },
        0,
    );
    let cases: &[(&str, Result<BalloonConfig, &str>)] = &[
        (
            r#"{"mode":"inflate","amount_mib":1024,"deflate_on_oom":true}"#,
            Ok(inflate),
        ),
        (
            r#"{"mode":"report","stats_polling_interval_s":5}"#,
            Ok(BalloonConfig::new(BalloonDevice::FreePageReporting, 5)),
        ),
        (
            r#"{"mode":"hint"}"#,
            Ok(BalloonConfig::new(BalloonDevice::FreePageHinting, 0)),
        ),
        (r#"{"mode":"balloon"}"#, Err("unknown variant `balloon`")),
        (r#"{"amount_mib":1024}"#, Err("missing field `mode`")),
        (
            r#"{"mode":"hint","free_page_hinting":true}"#,
            Err("unknown field `free_page_hinting`"),
        ),
        (
            r#"{"mode":"inflate","deflate_on_oom":true}"#,
            Err("balloon mode inflate requires amount_mib"),
        ),
        (
            r#"{"mode":"inflate","amount_mib":1024}"#,
            Err("balloon mode inflate requires deflate_on_oom"),
        ),
        (
            r#"{"mode":"inflate","amount_mib":0,"deflate_on_oom":true}"#,
            Err("balloon amount_mib must be greater than 0"),
        ),
        (
            r#"{"mode":"report","amount_mib":1024}"#,
            Err("balloon amount_mib is only valid for mode inflate, not report"),
        ),
        (
            r#"{"mode":"hint","deflate_on_oom":false}"#,
            Err("balloon deflate_on_oom is only valid for mode inflate, not hint"),
        ),
        (
            r#"{"mode":"report","stats_polling_interval_s":65536}"#,
            Err("invalid value"),
        ),
    ];
    for (raw, expected) in cases {
        match (parse(raw), expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(actual, *expected, "{raw}");
                let reserialized = serde_json::to_string(&actual).unwrap();
                assert_eq!(parse(&reserialized), Ok(actual), "{raw} round trip");
            }
            (Err(actual), Err(expected)) => {
                assert!(actual.contains(expected), "{raw}: {actual}");
            }
            (actual, expected) => panic!("{raw}: expected {expected:?}, got {actual:?}"),
        }
    }
}

#[test]
fn inflation_must_leave_guest_memory() {
    let inflate = |amount_mib| {
        BalloonConfig::new(
            BalloonDevice::Inflate {
                amount_mib: NonZeroU32::new(amount_mib).unwrap(),
                deflate_on_oom: true,
            },
            0,
        )
    };
    assert_eq!(inflate(1024).validate_for_memory_mib(8192), Ok(()));
    assert_eq!(
        inflate(8192).validate_for_memory_mib(8192),
        Err(BalloonConfigError::AmountNotBelowMemory {
            amount_mib: 8192,
            memory_mib: 8192
        })
    );
    for mode in [
        BalloonDevice::FreePageReporting,
        BalloonDevice::FreePageHinting,
    ] {
        assert_eq!(
            BalloonConfig::new(mode, 0).validate_for_memory_mib(1),
            Ok(())
        );
    }
}

#[test]
fn mode_names_round_trip() {
    for mode in BalloonMode::ALL {
        assert_eq!(mode.as_str().parse::<BalloonMode>(), Ok(mode));
    }
    "Inflate".parse::<BalloonMode>().unwrap_err();
}
