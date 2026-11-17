pub mod autoscaling;
pub mod ec2;
pub mod sts;

mod query;

pub use query::{AwsClientError, QueryResponse, QueryTransport, ReqwestTransport};
