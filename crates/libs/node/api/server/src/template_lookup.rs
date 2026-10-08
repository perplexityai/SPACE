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
