use super::{NodePathError, NodeSpacePath};

#[test]
fn node_space_path_rejects_relative_and_parent_traversal() {
    assert_eq!(
        NodeSpacePath::parse("space").unwrap_err(),
        NodePathError::NotAbsolute
    );
    assert_eq!(
        NodeSpacePath::parse("/space/../etc").unwrap_err(),
        NodePathError::ParentTraversal
    );
    NodeSpacePath::parse("/space").unwrap();
}
