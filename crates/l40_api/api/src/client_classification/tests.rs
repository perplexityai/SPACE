use super::*;

#[test]
fn configured_client_policy_classifies_exact_ids_and_defaults_to_other() {
    let policy = ClientClassification::parse(
        r#"{" foreground-client ":" Interactive ","scheduled-client":"CRON","background-client":"other"}"#,
    )
    .unwrap();

    for (client, expected) in [
        (Some("foreground-client"), ClientClass::Interactive),
        (Some("scheduled-client"), ClientClass::Cron),
        (Some("background-client"), ClientClass::Other),
        (Some("unknown-client"), ClientClass::Other),
        (Some("foreground-client-suffix"), ClientClass::Other),
        (None, ClientClass::Other),
    ] {
        assert_eq!(policy.classify(client), expected);
        assert_eq!(
            ClientClassification::default().classify(client),
            ClientClass::Other
        );
    }
}

#[test]
fn malformed_client_policy_is_rejected() {
    for (raw, context) in [
        ("null", "expected a JSON object"),
        ("[]", "expected a JSON object"),
        (r#""client""#, "expected a JSON object"),
        (r#"{"client":3}"#, "expected a string"),
        (
            r#"{"client":"unknown"}"#,
            r#"client "client", class "unknown""#,
        ),
        (r#"{"":"interactive"}"#, "empty client ID"),
        (r#"{"   ":"interactive"}"#, "empty client ID"),
        (
            r#"{"client":"interactive","client":"cron"}"#,
            r#"duplicate client ID "client""#,
        ),
        (
            r#"{"client":"interactive"," client ":"cron"}"#,
            r#"duplicate client ID "client""#,
        ),
    ] {
        let error = ClientClassification::parse(raw).unwrap_err();

        assert!(matches!(&error, ClientClassificationError::Json(source) if source.is_data()));
        assert!(error.to_string().contains(context), "{error}");
    }
}

#[test]
fn blank_client_policy_uses_the_empty_default() {
    for raw in ["", "   ", "\n\t", "{}"] {
        let policy = ClientClassification::parse(raw).unwrap();

        assert_eq!(policy.classify(Some("client")), ClientClass::Other);
    }
}

#[test]
fn malformed_json_reports_its_location_in_the_startup_error() {
    let error = ClientClassification::parse(r#"{"client":"interactive",}"#).unwrap_err();

    assert!(matches!(&error, ClientClassificationError::Json(source) if source.is_syntax()));
    assert!(error.to_string().contains("line 1 column"), "{error}");
}
