use serde::Serialize;
use serde_json::{json, Value};
use std::io::Write;

pub fn send_success(
    writer: &mut impl Write,
    id: Value,
    result: impl Serialize,
) -> Result<(), String> {
    let payload = json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id,
    });
    writeln!(writer, "{payload}").map_err(|error| error.to_string())
}

pub fn send_error(
    writer: &mut impl Write,
    id: Value,
    code: i32,
    message: &str,
) -> Result<(), String> {
    let payload = json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message
        },
        "id": id,
    });
    writeln!(writer, "{payload}").map_err(|error| error.to_string())
}
