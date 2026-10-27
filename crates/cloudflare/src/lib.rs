mod connect;
mod lease_rpc;
mod metrics;
mod notify;
mod schedule;

use pintle_core::Transaction;
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

#[event(fetch)]
pub async fn main(req: worker::Request, env: Env, _ctx: Context) -> Result<Response> {
    if req.path().starts_with("/pintle.v1.") {
        return lease_rpc::gateway(req, &env).await;
    }
    if req.path() == "/health" && req.method() == Method::Get && lease_rpc::enabled(&env) {
        return Response::ok("pintle-mutex");
    }
    Response::error("not found", 404)
}

#[durable_object]
pub struct PintleAuthority {
    instance: std::cell::OnceCell<Option<String>>,
    notifications: notify::Notifications,
    state: State,
    env: Env,
}

impl DurableObject for PintleAuthority {
    fn new(state: State, env: Env) -> Self {
        Self {
            // Initialize diagnostic entropy inside an event, not construction.
            instance: std::cell::OnceCell::new(),
            state,
            env,
            notifications: notify::Notifications::default(),
        }
    }
    async fn alarm(&self) -> Result<Response> {
        let result = lease_rpc::alarm(&self.env, &self.state, &self.notifications).await;
        let instance = self.instance.get_or_init(|| metrics::instance(&self.env));
        metrics::authority(instance.as_deref(), "alarm", &result);
        result
    }
    async fn fetch(&self, req: worker::Request) -> Result<Response> {
        if lease_rpc::is_internal(&req) {
            let result =
                lease_rpc::authority(req, &self.env, &self.state, &self.notifications).await;
            let instance = self.instance.get_or_init(|| metrics::instance(&self.env));
            metrics::authority(instance.as_deref(), "fetch", &result);
            return result;
        }
        Response::error("not found", 404)
    }
}
