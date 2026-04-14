use crate::client::{execute_statement, query_text_rows};
use crate::error::PluginResult;
use crate::models::{
    ConnectionParams, ExplainNode, ExplainPlan, Pagination, PluginSettings, QueryResult,
};
use crate::utils::pagination::page_offset;
use crate::utils::values::text_row_to_json;

pub fn test_connection(params: &ConnectionParams, settings: &PluginSettings) -> PluginResult<()> {
    let _ = query_text_rows(
        params,
        settings,
        None,
        "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1 WITH UR",
    )?;
    Ok(())
}

pub fn execute_query(
    params: &ConnectionParams,
    settings: &PluginSettings,
    query: &str,
    limit: Option<u32>,
    page: u32,
    schema: Option<&str>,
) -> PluginResult<QueryResult> {
    let trimmed = query.trim();
    let is_select_like = trimmed
        .chars()
        .take(10)
        .collect::<String>()
        .to_uppercase()
        .starts_with("SELECT")
        || trimmed.to_uppercase().starts_with("WITH");

    if !is_select_like {
        let affected_rows = execute_statement(params, settings, schema, query)?;
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            affected_rows,
            truncated: false,
            pagination: None,
        });
    }

    let page_size = limit.unwrap_or(200);
    let paged_sql = format!(
        "SELECT * FROM ({}) AS TABULARIS_PAGE OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
        trimmed,
        page_offset(page, page_size),
        page_size
    );
    let result = query_text_rows(params, settings, schema, &paged_sql)?;
    let rows = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| text_row_to_json(value.as_deref()))
                .collect()
        })
        .collect::<Vec<Vec<serde_json::Value>>>();
    let has_more = rows.len() as u32 == page_size;

    Ok(QueryResult {
        columns: result.columns,
        rows,
        affected_rows: 0,
        truncated: false,
        pagination: Some(Pagination {
            page,
            page_size,
            total_rows: None,
            has_more,
        }),
    })
}

pub fn explain_query(
    params: &ConnectionParams,
    settings: &PluginSettings,
    query: &str,
    analyze: bool,
    schema: Option<&str>,
) -> PluginResult<ExplainPlan> {
    let prefix = if analyze {
        "EXPLAIN PLAN FOR"
    } else {
        "EXPLAIN PLAN FOR"
    };
    let sql = format!("{prefix} {query}");
    let _ = execute_statement(params, settings, schema, &sql)?;

    Ok(ExplainPlan {
        root: ExplainNode {
            id: "db2-explain".to_string(),
            node_type: "DB2 Explain".to_string(),
            relation: None,
            startup_cost: None,
            total_cost: None,
            plan_rows: None,
            actual_rows: None,
            actual_time_ms: None,
            actual_loops: None,
            buffers_hit: None,
            buffers_read: None,
            filter: None,
            index_condition: None,
            join_type: None,
            hash_condition: None,
            extra: Default::default(),
            children: vec![],
        },
        planning_time_ms: None,
        execution_time_ms: None,
        original_query: query.to_string(),
        driver: "db2".to_string(),
        has_analyze_data: false,
        raw_output: Some(
            "DB2 EXPLAIN PLAN executed. Detailed visual plan extraction is not implemented yet."
                .to_string(),
        ),
    })
}
