//! Dependency-light protobuf bindings for the provider-neutral intermediate-key
//! broker contract. Broker implementations use the generated server trait;
//! SPACE uses the generated client through its authenticated transport wrapper.

pub mod v2 {
    tonic::include_proto!("space.credential.provider.v2");
}

#[cfg(feature = "conformance")]
pub mod conformance;

#[cfg(test)]
mod wire_shape_tests;
