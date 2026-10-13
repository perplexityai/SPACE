use crate::emit::tag;

pub mod admission;
pub mod api;
pub mod publication;
pub mod restore;
pub mod vm;
pub mod worker;

fn sandbox_tags(backend: &str, template: Option<&str>, result: &str) -> Vec<String> {
    let mut tags = vec![tag("backend", backend), tag("result", result)];
    if let Some(template) = template {
        tags.push(tag("template", template));
    }
    tags
}
