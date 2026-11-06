//! The sandbox-specific parts of SPACE's gRPC plumbing, which name
//! common-core's sandbox types and so sit above `pplx-space-common-grpc`:
//!
//! - [`SandboxTraceContext`], the sandbox identity a call records on its
//!   spans, and [`record_sandbox_id`];
//! - the session-route-at-rest status marker
//!   ([`mark_session_route_at_rest`], [`session_route_at_rest`]).

mod session_route;
mod trace;

pub use session_route::{SessionRouteAtRest, mark_session_route_at_rest, session_route_at_rest};
pub use trace::{SandboxTraceContext, record_sandbox_id};
