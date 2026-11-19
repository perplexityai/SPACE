use super::*;

#[test]
fn visibility_round_trips() {
    for visibility in [TemplateVisibility::Generic, TemplateVisibility::OrgSpecific] {
        assert_eq!(
            TemplateVisibility::parse(visibility.as_str()).unwrap(),
            visibility
        );
    }
    TemplateVisibility::parse("private").unwrap_err();
    TemplateVisibility::parse("org-specific").unwrap_err();
}
