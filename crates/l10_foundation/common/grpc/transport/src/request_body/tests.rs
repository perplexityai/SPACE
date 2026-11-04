use super::*;
use tonic::codec::{DecodeBuf, Decoder};

struct EmptyDecoder;
impl Decoder for EmptyDecoder {
    type Item = ();
    type Error = Status;
    fn decode(&mut self, _: &mut DecodeBuf<'_>) -> Result<Option<()>, Status> {
        Ok(Some(()))
    }
}

struct Cancelled;
impl HttpBody for Cancelled {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Status>>> {
        Poll::Ready(Some(Err(Status::cancelled("peer reset"))))
    }
}

#[tokio::test]
async fn interrupted_request_body_is_not_successful_eof() {
    // Pin the dependency behavior motivating the boundary adapter.
    let mut unprotected =
        tonic::Streaming::new_request(EmptyDecoder, Body::new(Cancelled), None, None);
    assert!(
        unprotected
            .message()
            .await
            .expect("tonic maps cancellation to EOF")
            .is_none()
    );
    let mut protected =
        tonic::Streaming::new_request(EmptyDecoder, InputBody(Body::new(Cancelled)), None, None);
    assert_eq!(
        protected
            .message()
            .await
            .expect_err("cancellation must remain an error")
            .code(),
        tonic::Code::Aborted
    );
}

/// Reports the code its request body fails with.
struct FirstFrameCode;
impl Service<http::Request<Body>> for FirstFrameCode {
    type Response = tonic::Code;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<tonic::Code, Self::Error>>>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let mut body = request.into_body();
        Box::pin(async move {
            let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
            Ok(frame.expect("a frame").expect_err("the body fails").code())
        })
    }
}

#[tokio::test]
async fn only_selected_methods_keep_cancellation_an_error() {
    for (methods, path, expected) in [
        (None, "/pkg.Service/Ingress", tonic::Code::Aborted),
        (
            Some(&["Write"][..]),
            "/pkg.Service/Write",
            tonic::Code::Aborted,
        ),
        (
            Some(&["Write"][..]),
            "/pkg.Service/Ingress",
            tonic::Code::Cancelled,
        ),
    ] {
        let mut service = match methods {
            None => PreserveInputErrors::new(FirstFrameCode),
            Some(methods) => PreserveInputErrors::for_methods(FirstFrameCode, methods),
        };
        let request = http::Request::builder()
            .uri(path)
            .body(Body::new(Cancelled))
            .expect("request");
        assert_eq!(service.call(request).await.unwrap(), expected, "{path}");
    }
}
