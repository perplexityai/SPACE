use super::*;

#[test]
fn parses_valid_labels() {
    for value in ["prod-1", "prod-2", "testing", "p2", "a", "0", "a-b-c-9"] {
        let id = ClusterId::parse(value).expect(value);
        assert_eq!(id.as_str(), value);
        assert_eq!(id.to_string(), value);
    }
}

#[test]
fn rejects_empty() {
    assert_eq!(ClusterId::parse(""), Err(ClusterIdParseError::Empty));
}

#[test]
fn rejects_too_long() {
    let value = "a".repeat(MAX_CLUSTER_ID_BYTES + 1);
    assert_eq!(
        ClusterId::parse(value.clone()),
        Err(ClusterIdParseError::TooLong {
            max: MAX_CLUSTER_ID_BYTES,
            actual: value.len(),
        })
    );
    // Exactly at the limit is accepted.
    ClusterId::parse("a".repeat(MAX_CLUSTER_ID_BYTES)).unwrap();
}

#[test]
fn rejects_invalid_charset_and_boundaries() {
    for value in [
        "Prod-2", // uppercase
        "prod_2", // underscore
        "prod.2", // dot
        "prod 2", // space
        "prod,2", // comma — would inject a spurious DogStatsD tag if emitted
        "-prod",  // leading hyphen
        "prod-",  // trailing hyphen
        "prod/2", // slash
    ] {
        assert_eq!(
            ClusterId::parse(value),
            Err(ClusterIdParseError::InvalidLabel),
            "{value} should be rejected"
        );
    }
}

#[test]
fn from_env_value_handles_unset_empty_valid_and_invalid() {
    // Unset (or non-UTF-8) → None; this is the documented "no cluster id"
    // behavior local/dev processes rely on.
    assert_eq!(ClusterId::from_env_value(None), Ok(None));
    // Empty string is treated the same as unset.
    assert_eq!(ClusterId::from_env_value(Some(String::new())), Ok(None));
    // A valid label parses.
    assert_eq!(
        ClusterId::from_env_value(Some("prod-1".to_string())),
        Ok(Some(ClusterId::parse("prod-1").unwrap()))
    );
    // A set-but-invalid value is a hard error (callers drop + log it).
    assert_eq!(
        ClusterId::from_env_value(Some("Prod-1".to_string())),
        Err(ClusterIdParseError::InvalidLabel)
    );
}

#[test]
fn from_str_and_try_from_agree() {
    let expected = ClusterId::parse("prod-2").unwrap();
    assert_eq!("prod-2".parse::<ClusterId>().unwrap(), expected);
    assert_eq!(ClusterId::try_from("prod-2").unwrap(), expected);
    assert_eq!(ClusterId::try_from("prod-2".to_string()).unwrap(), expected);
    "BAD".parse::<ClusterId>().unwrap_err();
}

#[test]
fn round_trips_through_string() {
    let id = ClusterId::parse("prod-2").unwrap();
    assert_eq!(String::from(id.clone()), "prod-2");
    assert_eq!(id.as_str(), "prod-2");
}

#[cfg(feature = "serde")]
#[test]
fn serde_round_trip() {
    let id = ClusterId::parse("prod-2").unwrap();
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, "\"prod-2\"");
    assert_eq!(serde_json::from_str::<ClusterId>(&json).unwrap(), id);
    serde_json::from_str::<ClusterId>("\"BAD\"").unwrap_err();
}
