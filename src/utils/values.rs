use serde_json::Value;

pub fn json_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(flag) => {
            if *flag {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Number(number) => number.to_string(),
        Value::String(string) => format!("'{}'", string.replace('\'', "''")),
        Value::Array(_) | Value::Object(_) => {
            format!("'{}'", value.to_string().replace('\'', "''"))
        }
    }
}

pub fn text_row_to_json(value: Option<&str>) -> Value {
    match value {
        None => Value::Null,
        Some(raw) => Value::String(raw.to_string()),
    }
}

pub fn bool_from_catalog_flag(value: Option<&str>) -> bool {
    matches!(value, Some("Y" | "y" | "1" | "true" | "TRUE"))
}

#[cfg(test)]
mod tests {
    use super::{bool_from_catalog_flag, json_to_sql_literal, text_row_to_json};
    use serde_json::json;

    #[test]
    fn serializes_sql_literals() {
        assert_eq!(json_to_sql_literal(&json!(null)), "NULL");
        assert_eq!(json_to_sql_literal(&json!(true)), "1");
        assert_eq!(json_to_sql_literal(&json!("O'Reilly")), "'O''Reilly'");
    }

    #[test]
    fn converts_catalog_booleans() {
        assert!(bool_from_catalog_flag(Some("Y")));
        assert!(!bool_from_catalog_flag(Some("N")));
        assert!(!bool_from_catalog_flag(None));
    }

    #[test]
    fn converts_text_rows_to_json() {
        assert_eq!(text_row_to_json(None), serde_json::Value::Null);
        assert_eq!(text_row_to_json(Some("abc")), json!("abc"));
    }
}
