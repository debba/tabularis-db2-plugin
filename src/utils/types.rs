use crate::models::ColumnDefinition;

pub fn column_type_sql(column: &ColumnDefinition) -> String {
    let mut sql = format!("{} {}", column.name, column.data_type);
    if !column.is_nullable {
        sql.push_str(" NOT NULL");
    }
    if let Some(default_value) = &column.default_value {
        sql.push_str(&format!(" DEFAULT {}", default_value));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::column_type_sql;
    use crate::models::ColumnDefinition;

    #[test]
    fn builds_column_definition_sql() {
        let column = ColumnDefinition {
            name: "ID".to_string(),
            data_type: "INTEGER".to_string(),
            is_nullable: false,
            is_pk: true,
            is_auto_increment: false,
            default_value: None,
        };
        assert_eq!(column_type_sql(&column), "ID INTEGER NOT NULL");
    }
}
