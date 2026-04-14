use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum DatabaseSelection {
    Single(String),
    Multiple(Vec<String>),
}

impl DatabaseSelection {
    pub fn primary(&self) -> &str {
        match self {
            Self::Single(value) => value.as_str(),
            Self::Multiple(values) => values.first().map(String::as_str).unwrap_or(""),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionParams {
    pub driver: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: DatabaseSelection,
    pub ssl_mode: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableColumn {
    pub name: String,
    pub data_type: String,
    pub is_pk: bool,
    pub is_nullable: bool,
    pub is_auto_increment: bool,
    pub default_value: Option<String>,
    pub character_maximum_length: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub column_name: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Index {
    pub name: String,
    pub column_name: String,
    pub is_unique: bool,
    pub is_primary: bool,
    pub seq_in_index: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Pagination {
    pub page: u32,
    pub page_size: u32,
    pub total_rows: Option<u64>,
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub affected_rows: u64,
    #[serde(default)]
    pub truncated: bool,
    pub pagination: Option<Pagination>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExplainNode {
    pub id: String,
    pub node_type: String,
    pub relation: Option<String>,
    pub startup_cost: Option<f64>,
    pub total_cost: Option<f64>,
    pub plan_rows: Option<f64>,
    pub actual_rows: Option<f64>,
    pub actual_time_ms: Option<f64>,
    pub actual_loops: Option<u64>,
    pub buffers_hit: Option<u64>,
    pub buffers_read: Option<u64>,
    pub filter: Option<String>,
    pub index_condition: Option<String>,
    pub join_type: Option<String>,
    pub hash_condition: Option<String>,
    #[serde(default)]
    pub extra: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub children: Vec<ExplainNode>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExplainPlan {
    pub root: ExplainNode,
    pub planning_time_ms: Option<f64>,
    pub execution_time_ms: Option<f64>,
    pub original_query: String,
    pub driver: String,
    pub has_analyze_data: bool,
    pub raw_output: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<TableColumn>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoutineInfo {
    pub name: String,
    pub routine_type: String,
    pub definition: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoutineParameter {
    pub name: String,
    pub data_type: String,
    pub mode: String,
    pub ordinal_position: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,
    pub definition: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_pk: bool,
    pub is_auto_increment: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct PluginSettings {
    pub driver_name: Option<String>,
    pub security: Option<String>,
    pub current_schema: Option<String>,
    pub extra_properties: Option<String>,
}
