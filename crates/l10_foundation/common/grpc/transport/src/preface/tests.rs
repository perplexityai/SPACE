use super::PrefaceDeadline;
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[tokio::test(start_paused = true)]
async fn silent_and_partial_prefaces_expire() {
    for prefix in [0, 12, 23] {
        let (mut peer, stream) = duplex(128);
        let mut stream = PrefaceDeadline::new(stream, Duration::from_secs(5));
        peer.write_all(&PREFACE[..prefix]).await.unwrap();
        let mut buf = [0; 24];
        if prefix > 0 {
            stream.read_exact(&mut buf[..prefix]).await.unwrap();
        }
        let error = stream.read_exact(&mut buf[prefix..]).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TimedOut);
    }
}

#[tokio::test(start_paused = true)]
async fn fragmented_preface_does_not_reset_deadline() {
    let (mut peer, stream) = duplex(128);
    let mut stream = PrefaceDeadline::new(stream, Duration::from_secs(5));
    let mut buf = [0; 12];
    tokio::time::advance(Duration::from_secs(4)).await;
    peer.write_all(&PREFACE[..12]).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    peer.write_all(&PREFACE[12..]).await.unwrap();
    assert_eq!(
        stream.read_exact(&mut buf).await.unwrap_err().kind(),
        ErrorKind::TimedOut
    );
}

#[tokio::test(start_paused = true)]
async fn established_stream_has_no_read_deadline() {
    let (mut peer, stream) = duplex(128);
    let mut stream = PrefaceDeadline::new(stream, Duration::from_secs(5));
    let mut preface = [0; 24];
    for fragment in PREFACE.chunks(6) {
        peer.write_all(fragment).await.unwrap();
        stream
            .read_exact(&mut preface[..fragment.len()])
            .await
            .unwrap();
    }
    assert!(stream.deadline.is_none());
    tokio::time::advance(Duration::from_secs(3600)).await;
    peer.write_all(b"later").await.unwrap();
    let mut buf = [0; 5];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"later");
    stream.write_all(b"reply").await.unwrap();
    peer.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"reply");
    drop(peer);
    assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
}
