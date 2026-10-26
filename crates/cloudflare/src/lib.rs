mod connect;
mod lease_rpc;
mod metrics;
mod notify;
mod schedule;

use pintle_core::{Request as GrantRequest, Transaction};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};
use worker::{SqlStorage, *};

// workers-rs 0.8.7 exposes Storage::as_raw(), but not transactionSync.
// This is the entire additional JS API surface; no domain logic lives in JS.
#[wasm_bindgen]
extern "C" {
    type SyncStorage;
    #[wasm_bindgen(method, structural, catch, js_name = transactionSync)]
    fn transaction_sync(
        this: &SyncStorage,
        callback: &js_sys::Function,
    ) -> std::result::Result<JsValue, JsValue>;
}

fn atomic(
    storage: &Storage,
    f: impl FnOnce() -> std::result::Result<String, String> + 'static,
) -> Result<String> {
    // wasm-bindgen converts Err into a JS exception, causing transactionSync
    // to roll back. Returning an error VALUE instead would commit partial writes.
    let callback =
        Closure::once_assert_unwind_safe(move || -> std::result::Result<JsValue, JsValue> {
            f().map(|v| JsValue::from_str(&v))
                .map_err(|e| js_sys::Error::new(&e).into())
        });
    let raw: &JsValue = storage.as_raw().as_ref();
    let result = raw
        .unchecked_ref::<SyncStorage>()
        .transaction_sync(callback.as_ref().unchecked_ref());
    // The platform invokes synchronously. The closure is dropped on both paths.
    result
        .map_err(Error::from)?
        .as_string()
        .ok_or_else(|| Error::RustError("invalid transaction result".into()))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Fault {
    #[cfg(feature = "test-hooks")]
    Read {
        key: String,
    },
    Sql {
        after_write: u8,
    },
    Error {
        after_write: u8,
    },
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Grant {
        request: GrantRequest,
        fault: Option<Fault>,
    },
    Snapshot,
    Seed {
        fence: String,
        revision: String,
    },
    Vacate,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    namespace: String,
    key: String,
    operation: Operation,
}

#[derive(Serialize, Deserialize)]
struct Row {
    key: String,
    value: String,
}

struct SqlTransaction {
    sql: SqlStorage,
    fault: Option<Fault>,
    writes: u8,
}
impl Transaction for SqlTransaction {
    fn get(&self, key: &str) -> std::result::Result<Option<String>, String> {
        #[cfg(feature = "test-hooks")]
        if let Some(Fault::Read { key: failed }) = &self.fault
            && key == failed
        {
            return Err("injected storage read failure".into());
        }
        self.sql
            .exec(
                "SELECT key, value FROM records WHERE key = ?",
                vec![key.into()],
            )
            .and_then(|c| c.to_array::<Row>())
            .map(|rows| rows.into_iter().next().map(|r| r.value))
            .map_err(|e| e.to_string())
    }
    fn put(&mut self, key: &str, value: &str) -> std::result::Result<(), String> {
        self.sql.exec("INSERT INTO records (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", vec![key.into(), value.into()]).map_err(|e| e.to_string())?;
        self.writes += 1;
        match self.fault {
            Some(Fault::Sql { after_write }) if after_write == self.writes => {
                // Duplicate the row just written: a real UNIQUE violation at any position.
                self.sql
                    .exec(
                        "INSERT INTO records (key, value) VALUES (?, 'invalid')",
                        vec![key.into()],
                    )
                    .map_err(|e| e.to_string())?;
            }
            Some(Fault::Error { after_write }) if after_write == self.writes => {
                return Err("injected Rust callback error".into());
            }
            _ => {}
        }
        Ok(())
    }
}

fn enabled(env: &Env) -> bool {
    env.var("LOCAL_SPIKE")
        .is_ok_and(|v| v.to_string() == "true")
}

fn authority_name(tenant: &str, namespace: &str, key: &str) -> Result<String> {
    Ok(serde_json::to_string(&(
        tenant, namespace, "p0-grant", key,
    ))?)
}

#[event(fetch)]
pub async fn main(mut req: worker::Request, env: Env, _ctx: Context) -> Result<Response> {
    if req.path().starts_with("/pintle.v1.") {
        if lease_rpc::enabled(&env) {
            return lease_rpc::gateway(req, &env).await;
        }
        return connect::gateway(req, &env).await;
    }
    if req.path() == "/health" && lease_rpc::enabled(&env) {
        return Response::ok("pintle-p1");
    }
    if req.path() == "/health"
        && req.method() == Method::Get
        && env
            .var("P0B_ENABLED")
            .is_ok_and(|v| v.to_string() == "true")
    {
        return Response::ok("pintle-p0b");
    }
    if !enabled(&env) {
        return Response::error("P0 fixture disabled", 403);
    }
    if req.path() == "/health" && req.method() == Method::Get {
        return Response::ok("pintle-p0");
    }
    if req.path() != "/spike" || req.method() != Method::Post {
        return Response::error("not found", 404);
    }
    // Explicitly insecure LOCAL developer identities. Never public auth policy.
    let tenant = match req.headers().get("Authorization")?.as_deref() {
        Some("Bearer local-a") => "tenant-a",
        Some("Bearer local-b") => "tenant-b",
        _ => return Response::error("local identity required", 401),
    };
    let bytes = req.bytes().await?;
    if bytes.len() > 8192 {
        return Response::error("fixture payload too large", 413);
    }
    let envelope: Envelope = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return Response::error("invalid fixture request", 400),
    };
    if [&envelope.namespace, &envelope.key]
        .iter()
        .any(|s| s.is_empty() || s.len() > 128)
    {
        return Response::error("namespace and key must be 1..128 bytes", 400);
    }
    // JSON tuple encoding is unambiguous even for delimiter-containing keys.
    let name = authority_name(tenant, &envelope.namespace, &envelope.key)?;
    let namespace = env.durable_object("AUTHORITIES")?;
    let stub = namespace.id_from_name(&name)?.get_stub()?;
    // Forward the validated bytes; no tenant or raw object ID is accepted in body.
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_body(Some(JsValue::from_str(
            std::str::from_utf8(&bytes).map_err(|e| Error::RustError(e.to_string()))?,
        )));
    stub.fetch_with_request(worker::Request::new_with_init(
        "https://authority.internal/spike",
        &init,
    )?)
    .await
}

#[durable_object]
pub struct PintleAuthority {
    notifications: notify::Notifications,
    state: State,
    env: Env,
}

impl DurableObject for PintleAuthority {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            notifications: notify::Notifications::default(),
        }
    }
    async fn alarm(&self) -> Result<Response> {
        lease_rpc::alarm(&self.env, &self.state, &self.notifications).await
    }
    async fn fetch(&self, mut req: worker::Request) -> Result<Response> {
        if lease_rpc::is_internal(&req) {
            return lease_rpc::authority(req, &self.env, &self.state, &self.notifications).await;
        }
        if connect::is_internal(&req) {
            return connect::authority(req, &self.env, &self.state).await;
        }
        if !enabled(&self.env) {
            return Response::error("P0 fixture disabled", 403);
        }
        let Envelope { operation, .. } = req.json::<Envelope>().await?;
        let storage = self.state.storage();
        let sql = storage.sql();
        sql.exec(
            "CREATE TABLE IF NOT EXISTS records (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT",
            None,
        )?;
        let result = atomic(&storage, move || {
            let mut tx = SqlTransaction {
                sql,
                fault: None,
                writes: 0,
            };
            match operation {
                Operation::Grant { request, fault } => {
                    if fault.as_ref().is_some_and(|f| match f {
                        #[cfg(feature = "test-hooks")]
                        Fault::Read { .. } => false,
                        Fault::Sql { after_write } | Fault::Error { after_write } => {
                            !(1..=4).contains(after_write)
                        }
                    }) {
                        return Err("fault position must be 1..4".into());
                    }
                    tx.fault = fault;
                    serde_json::to_string(&pintle_core::try_grant(&mut tx, &request)?)
                        .map_err(|e| e.to_string())
                }
                Operation::Snapshot => {
                    let rows = tx
                        .sql
                        .exec("SELECT key, value FROM records ORDER BY key", None)
                        .and_then(|c| c.to_array::<Row>())
                        .map_err(|e| e.to_string())?;
                    serde_json::to_string(&rows).map_err(|e| e.to_string())
                }
                Operation::Seed { fence, revision } => {
                    pintle_core::counter(&fence)?;
                    pintle_core::counter(&revision)?;
                    let rows = tx
                        .sql
                        .exec("SELECT key, value FROM records", None)
                        .and_then(|c| c.to_array::<Row>())
                        .map_err(|e| e.to_string())?;
                    if !rows.is_empty() {
                        return Err("seed only allowed on a pristine fixture".into());
                    }
                    tx.put("fence", &fence)?;
                    tx.put("revision", &revision)?;
                    Ok("{}".into())
                }
                Operation::Vacate => {
                    // Test-only availability change, NOT an authorized Release API.
                    // Preserve fence and historical dedupe records.
                    let revision =
                        pintle_core::counter(&tx.get("revision")?.unwrap_or_else(|| "0".into()))?
                            .checked_add(1)
                            .ok_or("revision exhausted")?;
                    tx.sql
                        .exec("DELETE FROM records WHERE key='owner'", None)
                        .map_err(|e| e.to_string())?;
                    tx.put("revision", &revision.to_string())?;
                    Ok("{}".into())
                }
            }
        });
        match result {
            Ok(json) => {
                // Explicit durability boundary before acknowledging any result.
                // Failure here is ambiguous, not evidence that the grant aborted.
                if storage.sync().await.is_err() {
                    return Response::error("commit unknown; retry the SAME request ID", 503);
                }
                let headers = Headers::new();
                headers.set("Content-Type", "application/json")?;
                Ok(Response::ok(json)?.with_headers(headers))
            }
            Err(e) => Response::error(format!("transaction aborted: {e}"), 500),
        }
    }
}
