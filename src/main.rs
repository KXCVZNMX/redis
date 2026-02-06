mod db;
mod resp;

use crate::db::{DBData, DBVal, Db};
use crate::resp::Value;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

/// Redis Clone
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {}

#[tokio::main]
#[allow(unused)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let listener = TcpListener::bind("localhost:6379").await?;

    let db: Db = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");

                let db_thread = db.clone();

                tokio::spawn(async move { handle_connection(stream, db_thread).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(stream: TcpStream, db: Db) {
    let mut handler = resp::RespHandler::new(stream);

    println!("Starting Loop");

    let mut i: usize = 0;

    const CLEAR_TOKEN_ITERATIONS: usize = 1000;

    loop {
        i += 1;

        if i >= CLEAR_TOKEN_ITERATIONS {
            let is_expired = |val: &DBData| {
                val.exp()
                    .map(|ms| val.created_at().elapsed() >= Duration::from_millis(ms))
                    .unwrap_or(false)
            };

            let mut db_temp = db.write().await;
            db_temp.retain(|_, val| !is_expired(val));

            i = 0;
        }

        let value = handler.read().await.unwrap_or_else(|e| {
            eprintln!("Failed to read token: {e}");
            Some(Value::Array(vec![
                Value::BulkString("ECHO".to_string()),
                Value::BulkString(format!("(error) Failed to read token: {e}")),
            ]))
        });

        println!("Got Value: {value:?}");

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap_or_else(|e| {
                eprintln!("Error extracting commands: {e}");
                (
                    "ECHO".to_string(),
                    vec![Value::BulkString(format!(
                        "(error) Error extracting commands: {e}"
                    ))],
                )
            });
            match command.to_lowercase().as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args
                    .first()
                    .unwrap_or(&Value::BulkString(
                        "You did not provide an argument to ECHO back".to_string(),
                    ))
                    .clone(),
                "set" => {
                    let ret = if args.len() == 2 {
                        if let (Value::BulkString(key), value) = (&args[0], &args[1]) {
                            let mut db_temp = db.write().await;
                            db_temp.insert(
                                key.to_string(),
                                DBData::new(determine_type(value).unwrap(), Instant::now(), None),
                            );
                        }
                        Value::SimpleString("OK".to_string())
                    } else if args.len() == 4 {
                        if let (
                            Value::BulkString(key),
                            value,
                            Value::BulkString(exp_type),
                            Value::BulkString(exp_time),
                        ) = (&args[0], &args[1], &args[2], &args[3])
                        {
                            let exp_time = exp_time.parse::<u64>().unwrap_or_default();
                            let expire_time = match exp_type.to_lowercase().as_str() {
                                "ex" => exp_time * 1000,
                                "px" => exp_time,
                                _ => 0,
                            };

                            let mut db_temp = db.write().await;
                            db_temp.insert(
                                key.to_string(),
                                DBData::new(
                                    determine_type(value).unwrap(),
                                    Instant::now(),
                                    Some(expire_time),
                                ),
                            );
                        }
                        Value::SimpleString("OK".to_string())
                    } else {
                        Value::BulkString("(error) Invalid arguments for: SET".to_string())
                    };

                    ret
                }
                "get" => {
                    if args.len() != 1 {
                        Value::BulkString("(error) Invalid arguments for GET".to_string())
                    } else {
                        let ret: Value = if let Some(Value::BulkString(key)) = args.get(0) {
                            let mut db = db.write().await;

                            match db.get(key) {
                                None => Value::BulkString("-1".to_string()),
                                Some(val) => {
                                    let expired = val
                                        .exp()
                                        .map(|ms| {
                                            val.created_at().elapsed() >= Duration::from_millis(ms)
                                        })
                                        .unwrap_or(false);

                                    if expired {
                                        db.remove(key);
                                        Value::BulkString("-1".to_string())
                                    } else {
                                        match val.data() {
                                            DBVal::Int(n) => Value::BulkString(n.to_string()),
                                            DBVal::String(s) => Value::BulkString(s.clone()),
                                        }
                                    }
                                }
                            }
                        } else {
                            Value::BulkString("-1".to_string())
                        };

                        ret
                    }
                }
                c => Value::BulkString(format!("(error) Invalid command: {}", c)),
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write(response).await.expect("Failed to write")
    }
}

fn determine_type(value: &Value) -> anyhow::Result<DBVal> {
    match value {
        Value::BulkString(s) => {
            if let Ok(num) = s.parse::<i64>() {
                Ok(DBVal::Int(num))
            } else {
                Ok(DBVal::String(s.clone()))
            }
        }
        _ => Err(anyhow::anyhow!("Expected input to be a bulk string")),
    }
}

fn extract_command(value: Value) -> anyhow::Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(
                a.first()
                    .unwrap_or(&Value::BulkString(
                        "Received non-bulk-string input or empty input".to_string(),
                    ))
                    .clone(),
            )?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> anyhow::Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
