use std::str::FromStr;

#[derive(Debug)]
pub(crate) struct InvalidStoredEnum {
    pub(crate) field: &'static str,
    pub(crate) value: String,
}

pub(crate) fn parse_stored_enum<T>(field: &'static str, value: &str) -> Result<T, InvalidStoredEnum>
where
    T: FromStr,
{
    value.parse::<T>().map_err(|_| InvalidStoredEnum {
        field,
        value: value.to_string(),
    })
}
