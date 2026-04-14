mod client;
mod error;
mod handlers;
mod models;
mod rpc;
mod utils;

use handlers::{crud, ddl, metadata, query};
use models::{ColumnDefinition, ConnectionParams, PluginSettings};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead};

fn schema_param(
    params: &Value,
    request_params: &ConnectionParams,
    settings: &PluginSettings,
) -> String {
    params
        .get("schema")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            settings
                .current_schema
                .clone()
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| {
            request_params
                .username
                .clone()
                .unwrap_or_else(|| "DB2INST1".to_string())
        })
}

fn parse_connection_params(params: &Value) -> Result<ConnectionParams, String> {
    let parsed: ConnectionParams =
        serde_json::from_value(params.clone()).map_err(|error| error.to_string())?;
    if parsed.driver != "db2" {
        return Err(format!("Unsupported driver '{}'", parsed.driver));
    }
    Ok(parsed)
}

fn parse_column_definition(value: &Value) -> Result<ColumnDefinition, String> {
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut settings = PluginSettings::default();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) if !line.trim().is_empty() => line,
            Ok(_) => continue,
            Err(error) => {
                eprintln!("Failed to read stdin: {error}");
                break;
            }
        };

        let request: Value = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("Invalid JSON-RPC payload: {error}");
                continue;
            }
        };

        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = match request.get("method").and_then(Value::as_str) {
            Some(method) => method,
            None => {
                let _ = rpc::send_error(&mut stdout, id, -32600, "Method not specified");
                continue;
            }
        };
        let params = request.get("params").cloned().unwrap_or_else(|| json!({}));

        let response = match method {
            "initialize" => {
                settings = serde_json::from_value(
                    params.get("settings").cloned().unwrap_or_else(|| json!({})),
                )
                .unwrap_or_default();
                rpc::send_success(&mut stdout, id, json!(null))
            }
            "test_connection" | "ping" => {
                let request_params =
                    parse_connection_params(params.get("params").unwrap_or(&Value::Null));
                match request_params
                    .and_then(|request_params| query::test_connection(&request_params, &settings))
                {
                    Ok(()) => rpc::send_success(&mut stdout, id, json!(null)),
                    Err(error) => rpc::send_error(&mut stdout, id, -32000, &error),
                }
            }
            "get_databases" => {
                let request_params =
                    parse_connection_params(params.get("params").unwrap_or(&Value::Null));
                match request_params
                    .and_then(|request_params| metadata::get_databases(&request_params, &settings))
                {
                    Ok(result) => rpc::send_success(&mut stdout, id, result),
                    Err(error) => rpc::send_error(&mut stdout, id, -32001, &error),
                }
            }
            "get_schemas" => {
                let request_params =
                    parse_connection_params(params.get("params").unwrap_or(&Value::Null));
                match request_params
                    .and_then(|request_params| metadata::get_schemas(&request_params, &settings))
                {
                    Ok(result) => rpc::send_success(&mut stdout, id, result),
                    Err(error) => rpc::send_error(&mut stdout, id, -32002, &error),
                }
            }
            "get_tables"
            | "get_views"
            | "get_view_definition"
            | "get_view_columns"
            | "get_columns"
            | "get_foreign_keys"
            | "get_indexes"
            | "get_routines"
            | "get_routine_parameters"
            | "get_routine_definition"
            | "get_all_columns_batch"
            | "get_all_foreign_keys_batch"
            | "get_schema_snapshot"
            | "execute_query"
            | "explain_query"
            | "insert_record"
            | "update_record"
            | "delete_record"
            | "drop_index"
            | "drop_foreign_key" => {
                let request_params =
                    match parse_connection_params(params.get("params").unwrap_or(&Value::Null)) {
                        Ok(request_params) => request_params,
                        Err(error) => {
                            let _ = rpc::send_error(&mut stdout, id, -32602, &error);
                            continue;
                        }
                    };
                let schema = schema_param(&params, &request_params, &settings);

                match method {
                    "get_tables" => match metadata::get_tables(&request_params, &settings, &schema)
                    {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32003, &error),
                    },
                    "get_views" => match metadata::get_views(&request_params, &settings, &schema) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32004, &error),
                    },
                    "get_view_definition" => match metadata::get_view_definition(
                        &request_params,
                        &settings,
                        &schema,
                        params
                            .get("view_name")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32005, &error),
                    },
                    "get_view_columns" => match metadata::get_columns(
                        &request_params,
                        &settings,
                        &schema,
                        params
                            .get("view_name")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32006, &error),
                    },
                    "get_columns" => match metadata::get_columns(
                        &request_params,
                        &settings,
                        &schema,
                        params.get("table").and_then(Value::as_str).unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32007, &error),
                    },
                    "get_foreign_keys" => match metadata::get_foreign_keys(
                        &request_params,
                        &settings,
                        &schema,
                        params.get("table").and_then(Value::as_str).unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32008, &error),
                    },
                    "get_indexes" => match metadata::get_indexes(
                        &request_params,
                        &settings,
                        &schema,
                        params.get("table").and_then(Value::as_str).unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32009, &error),
                    },
                    "get_routines" => {
                        match metadata::get_routines(&request_params, &settings, &schema) {
                            Ok(result) => rpc::send_success(&mut stdout, id, result),
                            Err(error) => rpc::send_error(&mut stdout, id, -32010, &error),
                        }
                    }
                    "get_routine_parameters" => match metadata::get_routine_parameters(
                        &request_params,
                        &settings,
                        &schema,
                        params
                            .get("routine_name")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32011, &error),
                    },
                    "get_routine_definition" => match metadata::get_routine_definition(
                        &request_params,
                        &settings,
                        &schema,
                        params
                            .get("routine_name")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32012, &error),
                    },
                    "get_all_columns_batch" => {
                        match metadata::get_all_columns_batch(&request_params, &settings, &schema) {
                            Ok(result) => rpc::send_success(&mut stdout, id, result),
                            Err(error) => rpc::send_error(&mut stdout, id, -32013, &error),
                        }
                    }
                    "get_all_foreign_keys_batch" => match metadata::get_all_foreign_keys_batch(
                        &request_params,
                        &settings,
                        &schema,
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32014, &error),
                    },
                    "get_schema_snapshot" => {
                        match metadata::get_schema_snapshot(&request_params, &settings, &schema) {
                            Ok(result) => rpc::send_success(&mut stdout, id, result),
                            Err(error) => rpc::send_error(&mut stdout, id, -32015, &error),
                        }
                    }
                    "execute_query" => match query::execute_query(
                        &request_params,
                        &settings,
                        params.get("query").and_then(Value::as_str).unwrap_or(""),
                        params
                            .get("limit")
                            .and_then(Value::as_u64)
                            .map(|value| value as u32),
                        params
                            .get("page")
                            .and_then(Value::as_u64)
                            .map(|value| value as u32)
                            .unwrap_or(1),
                        Some(&schema),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32016, &error),
                    },
                    "explain_query" => match query::explain_query(
                        &request_params,
                        &settings,
                        params.get("query").and_then(Value::as_str).unwrap_or(""),
                        params
                            .get("analyze")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                        Some(&schema),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32017, &error),
                    },
                    "insert_record" => {
                        let data: HashMap<String, Value> = serde_json::from_value(
                            params.get("data").cloned().unwrap_or_else(|| json!({})),
                        )
                        .unwrap_or_default();
                        match crud::insert_record(
                            &request_params,
                            &settings,
                            params.get("table").and_then(Value::as_str).unwrap_or(""),
                            &data,
                            Some(&schema),
                        ) {
                            Ok(result) => rpc::send_success(&mut stdout, id, result),
                            Err(error) => rpc::send_error(&mut stdout, id, -32018, &error),
                        }
                    }
                    "update_record" => match crud::update_record(
                        &request_params,
                        &settings,
                        params.get("table").and_then(Value::as_str).unwrap_or(""),
                        params.get("pk_col").and_then(Value::as_str).unwrap_or(""),
                        params.get("pk_val").unwrap_or(&Value::Null),
                        params.get("col_name").and_then(Value::as_str).unwrap_or(""),
                        params.get("new_val").unwrap_or(&Value::Null),
                        Some(&schema),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32019, &error),
                    },
                    "delete_record" => match crud::delete_record(
                        &request_params,
                        &settings,
                        params.get("table").and_then(Value::as_str).unwrap_or(""),
                        params.get("pk_col").and_then(Value::as_str).unwrap_or(""),
                        params.get("pk_val").unwrap_or(&Value::Null),
                        Some(&schema),
                    ) {
                        Ok(result) => rpc::send_success(&mut stdout, id, result),
                        Err(error) => rpc::send_error(&mut stdout, id, -32020, &error),
                    },
                    "drop_index" => match client::execute_statement(
                        &request_params,
                        &settings,
                        Some(&schema),
                        &ddl::drop_index_sql(
                            params.get("table").and_then(Value::as_str).unwrap_or(""),
                            params
                                .get("index_name")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            Some(&schema),
                        ),
                    ) {
                        Ok(_) => rpc::send_success(&mut stdout, id, json!(null)),
                        Err(error) => rpc::send_error(&mut stdout, id, -32021, &error),
                    },
                    "drop_foreign_key" => match client::execute_statement(
                        &request_params,
                        &settings,
                        Some(&schema),
                        &ddl::drop_foreign_key_sql(
                            params.get("table").and_then(Value::as_str).unwrap_or(""),
                            params.get("fk_name").and_then(Value::as_str).unwrap_or(""),
                            Some(&schema),
                        ),
                    ) {
                        Ok(_) => rpc::send_success(&mut stdout, id, json!(null)),
                        Err(error) => rpc::send_error(&mut stdout, id, -32022, &error),
                    },
                    _ => rpc::send_error(&mut stdout, id, -32601, "Method not implemented"),
                }
            }
            "get_create_table_sql"
            | "get_add_column_sql"
            | "get_alter_column_sql"
            | "get_create_index_sql"
            | "get_create_foreign_key_sql" => match method {
                "get_create_table_sql" => {
                    let columns = params
                        .get("columns")
                        .and_then(Value::as_array)
                        .map(|columns| {
                            columns
                                .iter()
                                .map(parse_column_definition)
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .unwrap_or_else(|| Ok(Vec::new()));
                    match columns {
                        Ok(columns) => rpc::send_success(
                            &mut stdout,
                            id,
                            ddl::get_create_table_sql(
                                params
                                    .get("table_name")
                                    .and_then(Value::as_str)
                                    .unwrap_or(""),
                                &columns,
                                params.get("schema").and_then(Value::as_str),
                            ),
                        ),
                        Err(error) => rpc::send_error(&mut stdout, id, -32602, &error),
                    }
                }
                "get_add_column_sql" => {
                    match parse_column_definition(params.get("column").unwrap_or(&Value::Null)) {
                        Ok(column) => rpc::send_success(
                            &mut stdout,
                            id,
                            ddl::get_add_column_sql(
                                params.get("table").and_then(Value::as_str).unwrap_or(""),
                                &column,
                                params.get("schema").and_then(Value::as_str),
                            ),
                        ),
                        Err(error) => rpc::send_error(&mut stdout, id, -32602, &error),
                    }
                }
                "get_alter_column_sql" => {
                    let old_column =
                        parse_column_definition(params.get("old_column").unwrap_or(&Value::Null));
                    let new_column =
                        parse_column_definition(params.get("new_column").unwrap_or(&Value::Null));
                    match (old_column, new_column) {
                        (Ok(old_column), Ok(new_column)) => rpc::send_success(
                            &mut stdout,
                            id,
                            ddl::get_alter_column_sql(
                                params.get("table").and_then(Value::as_str).unwrap_or(""),
                                &old_column,
                                &new_column,
                                params.get("schema").and_then(Value::as_str),
                            ),
                        ),
                        (Err(error), _) | (_, Err(error)) => {
                            rpc::send_error(&mut stdout, id, -32602, &error)
                        }
                    }
                }
                "get_create_index_sql" => {
                    let columns = params
                        .get("columns")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(Value::as_str)
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    rpc::send_success(
                        &mut stdout,
                        id,
                        ddl::get_create_index_sql(
                            params.get("table").and_then(Value::as_str).unwrap_or(""),
                            params
                                .get("index_name")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            &columns,
                            params
                                .get("is_unique")
                                .and_then(Value::as_bool)
                                .unwrap_or(false),
                            params.get("schema").and_then(Value::as_str),
                        ),
                    )
                }
                "get_create_foreign_key_sql" => rpc::send_success(
                    &mut stdout,
                    id,
                    ddl::get_create_foreign_key_sql(
                        params.get("table").and_then(Value::as_str).unwrap_or(""),
                        params.get("fk_name").and_then(Value::as_str).unwrap_or(""),
                        params.get("column").and_then(Value::as_str).unwrap_or(""),
                        params
                            .get("ref_table")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                        params
                            .get("ref_column")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                        params.get("on_delete").and_then(Value::as_str),
                        params.get("on_update").and_then(Value::as_str),
                        params.get("schema").and_then(Value::as_str),
                    ),
                ),
                _ => rpc::send_error(&mut stdout, id, -32601, "Method not implemented"),
            },
            _ => rpc::send_error(&mut stdout, id, -32601, "Method not implemented"),
        };

        if let Err(error) = response {
            eprintln!("Failed to write JSON-RPC response: {error}");
        }
    }
}
