use super::*;

fn s(value: &str) -> Option<String> {
    Some(value.to_string())
}

#[test]
fn the_current_boot_is_reported_with_its_key() {
    let desired = desired_annotations(s("boot-2"), s("boot-2"), s("arn:key"));
    assert_eq!(desired.get(ENCRYPTED_BOOT_ANNOTATION).map(String::as_str), Some("boot-2"));
    assert_eq!(desired.get(ENCRYPTED_KEY_ANNOTATION).map(String::as_str), Some("arn:key"));
}

#[test]
fn a_state_dir_from_an_earlier_boot_or_a_partial_one_reports_nothing() {
    assert!(desired_annotations(s("boot-2"), s("boot-1"), s("arn:key")).is_empty());
    assert!(desired_annotations(s("boot-2"), None, s("arn:key")).is_empty());
    assert!(desired_annotations(s("boot-2"), s("boot-2"), None).is_empty());
    assert!(desired_annotations(None, s("boot-2"), s("arn:key")).is_empty());
}

#[test]
fn current_means_both_keys_match_including_absent() {
    let desired = desired_annotations(s("b"), s("b"), s("k"));
    let mut current = BTreeMap::new();
    assert!(!annotations_current(&current, &desired));
    current.insert(ENCRYPTED_BOOT_ANNOTATION.to_string(), "b".to_string());
    current.insert(ENCRYPTED_KEY_ANNOTATION.to_string(), "k".to_string());
    assert!(annotations_current(&current, &desired));
    assert!(!annotations_current(&current, &BTreeMap::new()));
    current.clear();
    assert!(annotations_current(&current, &BTreeMap::new()));
}
