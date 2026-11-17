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
                    let field = field_path(body);
                    let reason = without_echoed_value(&body.inner().to_string());
                    let message = match &field {
                        Some(field) => format!("invalid request body: {field}: {reason}"),
                        None => format!("invalid request body: {reason}"),
                    };
                    let error = Self::bad_request(bounded(message));
                    match field {
                        Some(field) => error.with_field(bounded(field)),
                        None => error,
                    }
                }
                None => Self::bad_request("request body does not match the schema"),
            },
            JsonRejection::JsonSyntaxError(_) => match body_error(&rejection) {
                Some(body) => {
                    Self::bad_request(bounded(format!("request body is not valid JSON: {body}")))
                }
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

/// The longest message, or field detail, a refused body answers with. A
/// field path holds the map keys the client sent, and a deserializer's own
/// message can quote what it was given, so neither is answered back at the
/// size of the body.
const MAX_ANSWERED_LEN: usize = 512;

fn bounded(mut text: String) -> String {
    if text.len() > MAX_ANSWERED_LEN {
        let mut end = MAX_ANSWERED_LEN - '…'.len_utf8();
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
    text
}

/// The deserializer's message without the value the client sent where the
/// schema wanted another: a string of the wrong type or value keeps only
/// that it is a string, and an unknown enum variant only that it is
/// unknown. Such a value can be a secret sent to the wrong field, and the
/// field detail already says where it is.
fn without_echoed_value(message: &str) -> String {
    for prefix in ["invalid type: string ", "invalid value: string "] {
        if let Some(after) = message.strip_prefix(prefix).and_then(after_debug_quoted) {
            return format!("{}{after}", prefix.trim_end());
        }
    }
    // serde writes the variant as it was sent, unescaped, then the variants
    // it expected, so the last of these separators ends the variant.
    if let Some(rest) = message.strip_prefix("unknown variant `")
        && let Some(end) = ["`, expected ", "`, there are no variants"]
            .into_iter()
            .filter_map(|separator| rest.rfind(separator))
            .max()
        && let Some(after) = rest.get(end + 1..)
    {
        return format!("unknown variant{after}");
    }
    message.to_owned()
}

/// What follows the `Debug`-quoted string `text` starts with; `Debug`
/// escapes every quote and backslash inside it.
fn after_debug_quoted(text: &str) -> Option<&str> {
    let mut chars = text.char_indices();
    if chars.next()? != (0, '"') {
        return None;
    }
    while let Some((index, char)) = chars.next() {
        match char {
            '\\' => {
                chars.next()?;
            }
            '"' => return text.get(index + 1..),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests;
