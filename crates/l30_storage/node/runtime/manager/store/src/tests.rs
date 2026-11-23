use super::*;

#[test]
fn closed_vocabularies_round_trip_through_text() {
    for phase in [JobPhase::Publishing, JobPhase::Removing] {
        assert_eq!(phase.to_string().parse::<JobPhase>().unwrap(), phase);
    }
    for outcome in [
        JobOutcome::Installed,
        JobOutcome::AlreadyInstalled,
        JobOutcome::Uninstalled,
    ] {
        assert_eq!(outcome.to_string().parse::<JobOutcome>().unwrap(), outcome);
    }
    "rolling-back".parse::<JobPhase>().unwrap_err();
    "kept".parse::<JobOutcome>().unwrap_err();
}
