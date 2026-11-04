use futures_util::{Stream, StreamExt};
use pplx_space_spaced_common::proto::bridge_guest::{
    ProcessEvent, process_event::Event, process_exit::Outcome,
};

pub(crate) async fn first_process_event(
    mut stream: impl Stream<Item = Result<ProcessEvent, tonic::Status>> + Unpin,
) -> ProcessEvent {
    stream
        .next()
        .await
        .expect("first process event")
        .expect("process stream message")
}

pub(crate) async fn collect_process_events(
    mut stream: impl Stream<Item = Result<ProcessEvent, tonic::Status>> + Unpin,
) -> Vec<ProcessEvent> {
    let mut events = Vec::new();
    while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("process event timed out")
    {
        events.push(event.expect("process stream message"));
    }
    events
}

pub(crate) fn successful_stdout(events: &[ProcessEvent]) -> Vec<u8> {
    let outcome = process_exit(events);
    assert!(
        matches!(outcome, Outcome::Exited(exit) if exit.exit_code == 0),
        "expected successful process exit, got {outcome:?}"
    );
    events
        .iter()
        .filter_map(|event| match &event.event {
            Some(Event::Stdout(output)) => Some(output.data.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect()
}

pub(crate) fn process_exit(events: &[ProcessEvent]) -> &Outcome {
    match events
        .last()
        .and_then(|event| event.event.as_ref())
        .expect("last process event")
    {
        Event::Exit(exit) => exit.outcome.as_ref().expect("exit outcome"),
        other => panic!("expected final process exit, got {other:?}"),
    }
}
