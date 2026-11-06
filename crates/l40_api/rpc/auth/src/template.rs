use tonic::{Request, Status};

use super::{NetworkPolicyMutationAuthority, NodeApiAuthenticatedCaller};

pub fn require_template_mutation_authority<T>(request: &Request<T>) -> Result<(), Status> {
    if request
        .extensions()
        .get::<NodeApiAuthenticatedCaller>()
        .is_some_and(|caller| {
            caller.network_policy_authority() == NetworkPolicyMutationAuthority::Granted
        })
    {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "template mutation requires control-plane authority",
        ))
    }
}

#[cfg(test)]
mod tests;
