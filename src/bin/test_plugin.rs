use serde_json::json;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn main() {
    let mut child = Command::new("cargo")
        .args(["run", "--bin", "tabularis-db2-plugin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to spawn plugin process");

    let mut stdin = child.stdin.take().expect("Failed to open stdin");
    let stdout = child.stdout.take().expect("Failed to open stdout");

    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            println!("PLUGIN: {}", line.unwrap());
        }
    });

    let conn_params = json!({
        "driver": "db2",
        "host": "localhost",
        "port": 50000,
        "database": "sample",
        "username": "db2inst1",
        "password": "secret"
    });

    let requests = vec![
        json!({
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "settings": {
                    "driver_name": "IBM DB2 ODBC DRIVER",
                    "current_schema": "DB2INST1"
                }
            },
            "id": 1
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "test_connection",
            "params": { "params": conn_params.clone() },
            "id": 2
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "get_schemas",
            "params": { "params": conn_params.clone() },
            "id": 3
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "execute_query",
            "params": {
                "params": conn_params,
                "query": "SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1",
                "limit": 10,
                "page": 1,
                "schema": "DB2INST1"
            },
            "id": 4
        }),
    ];

    for request in requests {
        let mut payload = serde_json::to_string(&request).unwrap();
        payload.push('\n');
        println!("SENDING: {}", payload.trim());
        stdin.write_all(payload.as_bytes()).unwrap();
        stdin.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    drop(stdin);
    child.wait().unwrap();
}
