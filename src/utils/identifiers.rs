pub fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub fn quote_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema_name) if !schema_name.is_empty() => {
            format!(
                "{}.{}",
                quote_identifier(schema_name),
                quote_identifier(name)
            )
        }
        _ => quote_identifier(name),
    }
}

#[cfg(test)]
mod tests {
    use super::{quote_identifier, quote_qualified_name};

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_identifier("MY_TABLE"), "\"MY_TABLE\"");
        assert_eq!(quote_identifier("A\"B"), "\"A\"\"B\"");
    }

    #[test]
    fn quotes_qualified_names() {
        assert_eq!(
            quote_qualified_name(Some("APP"), "USERS"),
            "\"APP\".\"USERS\""
        );
        assert_eq!(quote_qualified_name(None, "USERS"), "\"USERS\"");
    }
}
