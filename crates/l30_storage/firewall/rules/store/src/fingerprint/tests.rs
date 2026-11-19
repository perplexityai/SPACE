use super::*;

const ROLLOUT: &str = "/admin/firewall-rules/rollouts/01970000-0000-7000-8000-000000000001";
const OTHER_ROLLOUT: &str = "/admin/firewall-rules/rollouts/01970000-0000-7000-8000-000000000002";
const RULES: &[u8] = br#"{"rules":[{"allow":"10.0.0.0/8"},{"deny":"0.0.0.0/0"}]}"#;
const REORDERED_RULES: &[u8] = br#"{"rules":[{"deny":"0.0.0.0/0"},{"allow":"10.0.0.0/8"}]}"#;

fn request<'a>(path: &'a str, body: &'a [u8], if_match: Option<&'a str>) -> MutationRequest<'a> {
    MutationRequest {
        method: "POST",
        path,
        body,
        if_match,
    }
}

/// Catches a change to the v1 encoding, which would make every stored
/// receipt conflict with its own retry.
#[test]
fn the_v1_encoding_keeps_its_digest() {
    let path = format!("{ROLLOUT}/pause");
    let fingerprint = request(&path, RULES, Some("7")).fingerprint();
    // Equal inputs from separate buffers agree.
    let (body, if_match) = (RULES.to_vec(), String::from("7"));
    assert_eq!(
        request(&path.clone(), &body, Some(if_match.as_str())).fingerprint(),
        fingerprint
    );
    // Checked against an independent encoder of the v1 framing.
    assert_eq!(
        fingerprint.as_str(),
        "9285f9105047a7595c9202d560d3e638d8e8fe663502f5aa5b3b9a3ed7d60ede"
    );
}

/// Catches a field left out of the digest, or an encoding in which two
/// requests' fields run together, so a changed request replays another's
/// result instead of conflicting.
#[test]
fn each_distinct_request_has_its_own_digest() {
    let pause = format!("{ROLLOUT}/pause");
    let other_pause = format!("{OTHER_ROLLOUT}/pause");
    let requests = [
        request(&pause, RULES, Some("7")),
        MutationRequest {
            method: "PUT",
            ..request(&pause, RULES, Some("7"))
        },
        request(&other_pause, RULES, Some("7")),
        request(&pause, br#"{"reason":"hold"}"#, Some("7")),
        request(&pause, REORDERED_RULES, Some("7")),
        request(&pause, RULES, Some("9")),
        // An absent precondition is not an empty one.
        request(&pause, RULES, None),
        request(&pause, RULES, Some("")),
        // Bytes moved across a field boundary.
        request("/a", b"b", None),
        request("/ab", b"", None),
        request("/a", b"", Some("b")),
    ];
    for (i, left) in requests.iter().enumerate() {
        for right in requests.iter().skip(i + 1) {
            assert_ne!(
                left.fingerprint(),
                right.fingerprint(),
                "{left:?} and {right:?}"
            );
        }
    }
}
