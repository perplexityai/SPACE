pub fn dedupe_non_empty(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        let value = value.to_string();
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}
