use super::{dogstatsd_kind, dogstatsd_value};
use crate::sink::{MetricKind, MetricValue};

#[test]
fn dogstatsd_values_preserve_existing_formatting() {
    assert_eq!(dogstatsd_value(MetricValue::Unsigned(7)), "7");
    assert_eq!(dogstatsd_value(MetricValue::Float(12.3456)), "12.346");
}

#[test]
fn distributions_use_dogstatsd_distribution_type() {
    assert_eq!(dogstatsd_kind(MetricKind::Distribution), "d");
}
