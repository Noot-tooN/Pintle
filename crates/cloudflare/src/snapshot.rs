//! Atomic SQLite snapshots for authorities that retain one bounded record per key.
use crate::{Fault, SqlTransaction, atomic, connect::*, schedule::Schedule};
use pintle_core::Transaction;
use serde::{Serialize, de::DeserializeOwned};
use worker::*;

fn open(
    sql: SqlStorage,
    fault: Option<Fault>,
) -> std::result::Result<(SqlTransaction, bool), String> {
    // The table itself marks initialization. An existing table with a missing
    // snapshot must fail closed to preserve admission history.
    #[derive(serde::Deserialize)]
    struct Table {
        name: String,
    }
    let exists = sql
        .exec(
            "SELECT name FROM sqlite_schema WHERE name = 'records'",
            None,
        )
        .and_then(|c| c.to_array::<Table>())
        .map_err(|e| e.to_string())?
        .iter()
        .any(|t| t.name == "records");
    if !exists {
        sql.exec(
            "CREATE TABLE records (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT",
            None,
        )
        .map_err(|e| e.to_string())?;
    }
    let tx = SqlTransaction {
        sql,
        fault,
        writes: 0,
    };
    Ok((tx, exists))
}

pub(super) async fn commit<R: Serialize + DeserializeOwned>(
    state: &State,
    mut schedule: Schedule,
    deadline: u64,
    key: &'static str,
    transition: impl FnOnce(Option<&str>, u64) -> std::result::Result<(String, R), String> + 'static,
) -> RpcResult<R> {
    let fault = schedule
        .at("before_commit")
        .await
        .map_err(|_| unavailable())?;
    check_deadline(schedule.now(), deadline)?;
    let storage = state.storage();
    let sql = storage.sql();
    let clock = schedule.clone();
    let raw = atomic(&storage, move || {
        let (mut tx, exists) = open(sql, fault)?;
        let raw = if exists {
            Some(tx.get(key)?.ok_or("missing authority record")?)
        } else {
            None
        };
        let (snapshot, reply) = transition(raw.as_deref(), clock.now())?;
        tx.put(key, &snapshot)?;
        serde_json::to_string(&reply).map_err(|e| e.to_string())
    })
    .map_err(|_| unavailable())?;
    schedule
        .at("after_commit")
        .await
        .map_err(|_| unavailable())?;
    schedule.sync(&storage).await.map_err(|_| unavailable())?;
    schedule
        .at("before_response")
        .await
        .map_err(|_| unavailable())?;
    check_deadline(schedule.now(), deadline)?;
    serde_json::from_str(&raw).map_err(|_| unavailable())
}

pub(super) fn response(bytes: Vec<u8>) -> RpcResult<Response> {
    let r = Response::from_bytes(bytes).map_err(|_| unavailable())?;
    r.headers()
        .set("Content-Type", "application/proto")
        .map_err(|_| unavailable())?;
    r.headers()
        .set("Cache-Control", "no-store")
        .map_err(|_| unavailable())?;
    Ok(r)
}
