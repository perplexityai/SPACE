use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    task::{Context, Poll, Waker},
};

use super::*;

fn captured_counter(name: &str) -> CapturedCounter {
    CapturedCounter {
        name: name.to_string(),
        value: 1,
        tags: Vec::new(),
    }
}

fn captured_value(name: &str) -> CapturedValue {
    CapturedValue {
        name: name.to_string(),
        value: 1.0,
        tags: Vec::new(),
    }
}

#[test]
fn counter_capture_cleans_up_after_panic() {
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _capture = capture_counters_guard();
        record("before-panic", 1, &[]);
        panic!("test panic");
    }));
    assert!(panic.is_err());

    let capture = capture_counters_guard();
    record("after-panic", 1, &[]);
    assert_eq!(capture.finish(), vec![captured_counter("after-panic")]);
    BUFFER.with(|buffer| assert!(buffer.borrow().is_empty()));
}

#[test]
fn value_capture_cleans_up_after_early_return() {
    fn return_without_finishing() {
        let _capture = capture_values_guard();
        record_value("before-return", 1.0, &[]);
    }

    return_without_finishing();
    let capture = capture_values_guard();
    record_value("after-return", 1.0, &[]);
    assert_eq!(capture.finish(), vec![captured_value("after-return")]);
    VALUE_BUFFER.with(|buffer| assert!(buffer.borrow().is_empty()));
}

#[test]
fn aborted_capture_future_cleans_up() {
    let mut future = Box::pin(async {
        let _capture = capture_counters_guard();
        record("before-abort", 1, &[]);
        std::future::pending::<()>().await;
    });
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);

    drop(future);
    let capture = capture_counters_guard();
    record("after-abort", 1, &[]);
    assert_eq!(capture.finish(), vec![captured_counter("after-abort")]);
    BUFFER.with(|buffer| assert!(buffer.borrow().is_empty()));
}

#[test]
fn dropping_outer_capture_cannot_clear_nested_capture() {
    let outer = capture_counters_guard();
    record("outer", 1, &[]);
    let current = capture_counters_guard();
    record("current-before-drop", 1, &[]);

    drop(outer);
    record("current-after-drop", 1, &[]);

    assert_eq!(
        current.finish(),
        vec![
            captured_counter("current-before-drop"),
            captured_counter("current-after-drop"),
        ]
    );
    BUFFER.with(|buffer| assert!(buffer.borrow().is_empty()));
}

#[test]
fn finishing_outer_capture_cannot_clear_nested_capture() {
    let outer = capture_values_guard();
    record_value("outer", 1.0, &[]);
    let current = capture_values_guard();
    record_value("current-before-finish", 1.0, &[]);

    assert_eq!(outer.finish(), vec![captured_value("outer")]);
    record_value("current-after-finish", 1.0, &[]);

    assert_eq!(
        current.finish(),
        vec![
            captured_value("current-before-finish"),
            captured_value("current-after-finish"),
        ]
    );
    VALUE_BUFFER.with(|buffer| assert!(buffer.borrow().is_empty()));
}

#[test]
fn finishing_nested_capture_resumes_outer_capture() {
    let outer = capture_counters_guard();
    record("outer-before-nesting", 1, &[]);
    let nested = capture_counters_guard();
    record("nested", 1, &[]);

    assert_eq!(nested.finish(), vec![captured_counter("nested")]);
    record("outer-after-nesting", 1, &[]);
    assert_eq!(
        outer.finish(),
        vec![
            captured_counter("outer-before-nesting"),
            captured_counter("outer-after-nesting"),
        ]
    );
}

#[test]
fn counter_and_value_captures_can_be_nested_independently() {
    let counters = capture_counters_guard();
    record("counter-before-value", 1, &[]);
    let values = capture_values_guard();
    record_value("value", 1.0, &[]);
    record("counter-after-value", 1, &[]);

    assert_eq!(values.finish(), vec![captured_value("value")]);
    assert_eq!(
        counters.finish(),
        vec![
            captured_counter("counter-before-value"),
            captured_counter("counter-after-value"),
        ]
    );
}
