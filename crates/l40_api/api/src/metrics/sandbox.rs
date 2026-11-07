use pplx_space_metrics_emit::tag;

pub mod api;
pub mod restore;

fn sandbox_tags(backend: &str, template: Option<&str>, result: &str) -> Vec<String> {
    let mut tags = vec![tag("backend", backend), tag("result", result)];
    if let Some(template) = template {
        tags.push(tag("template", template));
    }
    tags
}
