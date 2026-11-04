use super::LabelValue;

#[test]
fn label_value_allows_empty_or_alphanumeric_edges() {
    LabelValue::parse("").unwrap();
    LabelValue::parse("foo").unwrap();
    LabelValue::parse("foo.bar_baz-1").unwrap();
}

#[test]
fn label_value_rejects_non_alphanumeric_edges() {
    LabelValue::parse("-foo").unwrap_err();
    LabelValue::parse("foo-").unwrap_err();
    LabelValue::parse(".foo").unwrap_err();
    LabelValue::parse("foo_").unwrap_err();
}

#[test]
fn label_value_rejects_invalid_chars_and_lengths() {
    LabelValue::parse("foo/bar").unwrap_err();
    LabelValue::parse("a".repeat(64)).unwrap_err();
}
