use super::*;

#[test]
fn only_template_managers_own_answer_reports_that_it_reads_twins() {
    let answer = |reads_twin_metadata| {
        Ok(GetCapabilitiesResponse {
            reads_twin_metadata,
        })
    };
    assert_eq!(from_answer(answer(true)), Some(true));
    assert_eq!(from_answer(answer(false)), Some(false));
    assert_eq!(
        from_answer(Err(Status::unimplemented("predates GetCapabilities"))),
        Some(false),
        "an older template-manager doesn't read twins"
    );
    for status in [
        Status::unavailable("socket gone"),
        Status::permission_denied("not an observer"),
        Status::deadline_exceeded("slow"),
    ] {
        assert_eq!(from_answer(Err(status)), None, "unknown, not true");
    }
}
