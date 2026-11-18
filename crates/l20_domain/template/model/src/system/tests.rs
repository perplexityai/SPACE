use super::*;

#[test]
fn system_templates_convert_to_valid_names() {
    for template in [SystemTemplate::Seed, SystemTemplate::CodeRuntime] {
        let expected = match template {
            SystemTemplate::Seed => "seed",
            SystemTemplate::CodeRuntime => "code-runtime",
        };

        let name = TemplateName::from(template);

        assert_eq!(name, TemplateName::parse(expected).unwrap());
        assert_eq!(name.as_str().parse::<SystemTemplate>().unwrap(), template);
    }
}
