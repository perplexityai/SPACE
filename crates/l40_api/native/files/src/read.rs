//! When a file read's response starts.

use std::time::Duration;

use futures_util::{Stream, StreamExt as _, stream};

/// Wait at most `budget` for a read's first message before its response
/// starts. A read the sandbox refuses at once (a directory, a FIFO, a socket)
/// then answers with that refusal's status instead of a 200 and an aborted
/// body, and a read whose first chunk never comes ends instead of holding the
/// response open. The returned stream yields that first message, then the
/// rest; a failure after it can still only abort the body.
pub async fn start_read<T, S>(
    mut stream: S,
    budget: Duration,
) -> Result<impl Stream<Item = Result<T, tonic::Status>>, tonic::Status>
where
    S: Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    let first = match tokio::time::timeout(budget, stream.next()).await {
        Ok(Some(Err(status))) => return Err(status),
        Ok(first) => first,
        Err(_) => {
            return Err(tonic::Status::deadline_exceeded(
                "the file read sent nothing in time",
            ));
        }
    };
    Ok(stream::iter(first).chain(stream))
}

#[cfg(test)]
mod tests;
