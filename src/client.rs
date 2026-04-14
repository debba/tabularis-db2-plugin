use crate::error::{err_to_string, PluginResult};
use crate::models::{ConnectionParams, PluginSettings};
use odbc_api::buffers::TextRowSet;
use odbc_api::{ConnectionOptions, Cursor, Environment};
use std::sync::OnceLock;

static ENVIRONMENT: OnceLock<Environment> = OnceLock::new();

#[derive(Debug)]
pub struct TextQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

fn environment() -> PluginResult<&'static Environment> {
    if let Some(environment) = ENVIRONMENT.get() {
        return Ok(environment);
    }

    let environment = Environment::new().map_err(err_to_string)?;
    let _ = ENVIRONMENT.set(environment);
    ENVIRONMENT
        .get()
        .ok_or_else(|| "Failed to initialize the ODBC environment".to_string())
}

pub fn build_connection_string(
    params: &ConnectionParams,
    settings: &PluginSettings,
    schema_override: Option<&str>,
) -> String {
    let mut parts = vec![format!(
        "Driver={{{}}}",
        settings
            .driver_name
            .clone()
            .unwrap_or_else(|| "IBM DB2 ODBC DRIVER".to_string())
    )];

    if let Some(host) = &params.host {
        parts.push(format!("Hostname={host}"));
    }
    parts.push(format!("Port={}", params.port.unwrap_or(50000)));
    parts.push("Protocol=TCPIP".to_string());
    parts.push(format!("Database={}", params.database.primary()));

    if let Some(username) = &params.username {
        parts.push(format!("Uid={username}"));
    }
    if let Some(password) = &params.password {
        parts.push(format!("Pwd={password}"));
    }

    let security = settings
        .security
        .as_deref()
        .or(params.ssl_mode.as_deref())
        .unwrap_or("none");
    if matches!(security, "ssl" | "SSL" | "require") {
        parts.push("Security=SSL".to_string());
    }

    if let Some(schema) = schema_override
        .filter(|value| !value.is_empty())
        .or(settings
            .current_schema
            .as_deref()
            .filter(|value| !value.is_empty()))
    {
        parts.push(format!("CurrentSchema={schema}"));
    }

    if let Some(extra) = settings
        .extra_properties
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(extra.to_string());
    }

    parts.join(";")
}

fn fetch_text_rows(mut cursor: impl Cursor) -> PluginResult<TextQueryResult> {
    let columns = cursor
        .column_names()
        .map_err(err_to_string)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err_to_string)?;

    let buffer = TextRowSet::for_cursor(256, &mut cursor, Some(4096)).map_err(err_to_string)?;
    let mut row_set_cursor = cursor.bind_buffer(buffer).map_err(err_to_string)?;
    let mut rows = Vec::new();

    while let Some(batch) = row_set_cursor.fetch().map_err(err_to_string)? {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_cols());
            for column_index in 0..batch.num_cols() {
                let value = batch
                    .at_as_str(column_index, row_index)
                    .map_err(err_to_string)?;
                row.push(value.map(ToString::to_string));
            }
            rows.push(row);
        }
    }

    Ok(TextQueryResult { columns, rows })
}

pub fn query_text_rows(
    params: &ConnectionParams,
    settings: &PluginSettings,
    schema_override: Option<&str>,
    sql: &str,
) -> PluginResult<TextQueryResult> {
    let env = environment()?;
    let connection_string = build_connection_string(params, settings, schema_override);
    let connection = env
        .connect_with_connection_string(&connection_string, ConnectionOptions::default())
        .map_err(err_to_string)?;
    let maybe_cursor = connection.execute(sql, ()).map_err(err_to_string)?;
    let cursor = maybe_cursor.ok_or_else(|| "The query did not return a result set".to_string())?;
    fetch_text_rows(cursor)
}

pub fn execute_statement(
    params: &ConnectionParams,
    settings: &PluginSettings,
    schema_override: Option<&str>,
    sql: &str,
) -> PluginResult<u64> {
    let env = environment()?;
    let connection_string = build_connection_string(params, settings, schema_override);
    let connection = env
        .connect_with_connection_string(&connection_string, ConnectionOptions::default())
        .map_err(err_to_string)?;
    let _ = connection.execute(sql, ()).map_err(err_to_string)?;
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::build_connection_string;
    use crate::models::{ConnectionParams, DatabaseSelection, PluginSettings};

    #[test]
    fn builds_db2_connection_string() {
        let params = ConnectionParams {
            driver: "db2".to_string(),
            host: Some("localhost".to_string()),
            port: Some(50000),
            username: Some("db2inst1".to_string()),
            password: Some("secret".to_string()),
            database: DatabaseSelection::Single("sample".to_string()),
            ssl_mode: None,
        };
        let settings = PluginSettings::default();
        let connection_string = build_connection_string(&params, &settings, Some("APP"));
        assert!(connection_string.contains("Database=sample"));
        assert!(connection_string.contains("CurrentSchema=APP"));
    }
}
