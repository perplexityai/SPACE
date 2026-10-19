use pplx_space_common_svc::auth::{NodeApiAuthenticatedCaller, TemplateReleaseReadAuthority};
use pplx_space_common_svc::template_lookup::TemplateLookupScope;
use tonic::{Request, Status};

pub(crate) fn bind_caller_scope<T>(
    request: &mut Request<T>,
) -> Result<TemplateLookupScope, Status> {
    let caller = request
        .extensions()
        .get::<NodeApiAuthenticatedCaller>()
        .ok_or_else(|| Status::unauthenticated("template lookup requires authenticated caller"))?;
    let scope = match caller.template_release_read_authority() {
        TemplateReleaseReadAuthority::Unscoped => TemplateLookupScope::Cluster,
        TemplateReleaseReadAuthority::NodeScoped => TemplateLookupScope::Node(
            request
                .remote_addr()
                .ok_or_else(|| Status::permission_denied("template caller address is missing"))?
                .ip(),
        ),
    };
    scope.attach(request);
    Ok(scope)
}

/// Control-plane creation delegates the stored sandbox placement, not the API
/// pod's address. Peer forwarding retains both nodes' placement requirements.
pub(crate) fn bind_create_caller_scope<T>(request: &mut Request<T>) -> Result<(), Status> {
    use pplx_space_common_svc::auth::NetworkPolicyMutationAuthority;
    if request
        .extensions()
        .get::<NodeApiAuthenticatedCaller>()
        .is_some_and(|caller| {
            caller.network_policy_authority() == NetworkPolicyMutationAuthority::Granted
        })
    {
        TemplateLookupScope::Cluster.attach(request);
    } else {
        bind_caller_scope(request)?;
    }
    Ok(())
}
