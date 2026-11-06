//! The request-body extractor every Native route reads JSON with, and the
//! `invalid_request` a body it cannot read answers with.

use axum::{
    Json,
    extract::{FromRequest, OptionalFromRequest, Request, rejection::JsonRejection},
    http::StatusCode,
};
use serde::de::DeserializeOwned;

use crate::PublicApiError;

/// [`axum::Json`], refused in the Native envelope with what is wrong with the
/// body rather than a bare `invalid request`. A body that does not match the
/// schema names the offending field in `details`, as the routes' own field
/// checks do; the status stays the framework's (413, 415, 400 or 422).
#[derive(Debug, Clone, Copy, Default)]
pub struct PublicJson<T>(pub T);

impl<S, T> FromRequest<S> for PublicJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = PublicApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        <Json<T> as FromRequest<S>>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(PublicApiError::from)
    }
}

impl<S, T> OptionalFromRequest<S> for PublicJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = PublicApiError;

    async fn from_request(request: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        <Json<T> as OptionalFromRequest<S>>::from_request(request, state)
            .await
            .map(|body| body.map(|Json(value)| Self(value)))
            .map_err(PublicApiError::from)
    }
}

impl From<JsonRejection> for PublicApiError {
    fn from(rejection: JsonRejection) -> Self {
        let status = rejection.status();
        let error = match &rejection {
            JsonRejection::JsonDataError(_) => match body_error(&rejection) {
                Some(body) => {
                    let error = Self::bad_request(format!("invalid request body: {body}"));
                    match field_path(body) {
                        Some(field) => error.with_field(field),
                        None => error,
                    }
                }
                None => Self::bad_request("request body does not match the schema"),
            },
            JsonRejection::JsonSyntaxError(_) => match body_error(&rejection) {
                Some(body) => Self::bad_request(format!("request body is not valid JSON: {body}")),
                None => Self::bad_request("request body is not valid JSON"),
            },
            JsonRejection::MissingJsonContentType(_) => {
                Self::bad_request("Content-Type must be application/json")
            }
            JsonRejection::BytesRejection(_) if status == StatusCode::PAYLOAD_TOO_LARGE => {
                Self::bad_request("request body is too large")
            }
            JsonRejection::BytesRejection(_) => Self::bad_request("request body could not be read"),
            _ => Self::bad_request("invalid request body"),
        };
        error.with_status(status)
    }
}

/// The deserializer's error behind a body rejection. axum keeps it, with the
/// path to the field it failed at, as the rejection's `axum::Error` source.
fn body_error(rejection: &JsonRejection) -> Option<&serde_path_to_error::Error<serde_json::Error>> {
    let wrapped = match rejection {
        JsonRejection::JsonDataError(error) => std::error::Error::source(error),
        JsonRejection::JsonSyntaxError(error) => std::error::Error::source(error),
        _ => None,
    }?;
    wrapped
        .source()?
        .downcast_ref::<serde_path_to_error::Error<serde_json::Error>>()
}

/// The field a body error is about, as the dotted path the routes' field
/// checks name (`resources.vcpu`); none for the body as a whole.
fn field_path(error: &serde_path_to_error::Error<serde_json::Error>) -> Option<String> {
    let path = error.path();
    path.iter()
        .any(|segment| !matches!(segment, serde_path_to_error::Segment::Unknown))
        .then(|| path.to_string())
}

#[cfg(test)]
mod tests;
