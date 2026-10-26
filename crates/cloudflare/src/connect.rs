//! Deliberately narrow Connect unary/binary Fetch adapter for the P0b experiment.
//! No coordination algorithm is implemented here. Unsupported modes fail explicitly.
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use pintle_core::{Outcome, Request as DomainRequest};
use pintle_protocol::v1::{self, TryAcquireRequest, TryAcquireResponse, try_acquire_response};
use prost::Message;
use wasm_bindgen::JsValue;
use worker::*;

use crate::{SqlTransaction, atomic, schedule::Schedule};

pub const PROCEDURE: &str = "/pintle.v1.LockService/TryAcquire";
const MAX_BODY: usize = 8192;
const MAX_TIMEOUT_MS: u64 = 10_000;
const INTERNAL_PATH: &str = "/internal/p0b/try-acquire";
const SUBJECT: &str = "x-pintle-subject";
const DEADLINE: &str = "x-pintle-deadline-ms";

pub(super) type RpcResult<T> = std::result::Result<T, RpcError>;
pub(super) struct RpcError {
    code: &'static str,
    message: &'static str,
    status: u16,
}
impl RpcError {
    pub(super) fn new(code: &'static str, message: &'static str, status: u16) -> Self {
        Self {
            code,
            message,
            status,
        }
    }
    pub(super) fn response(self) -> Result<Response> {
        let response =
            Response::from_json(&serde_json::json!({"code":self.code,"message":self.message}))?
                .with_status(self.status);
        response.headers().set("Cache-Control", "no-store")?;
        if self.status == 405 {
            response.headers().set("Allow", "POST")?;
        }
        Ok(response)
    }
}
pub(super) fn invalid(message: &'static str) -> RpcError {
    RpcError::new("invalid_argument", message, 400)
}
pub(super) fn unavailable() -> RpcError {
    RpcError::new(
        "unavailable",
        "outcome may be unknown; retry the same request_id and payload",
        503,
    )
}
pub(super) fn expired() -> RpcError {
    RpcError::new(
        "deadline_exceeded",
        "deadline exceeded; reconcile with the same request_id and payload",
        504,
    )
}
pub(super) fn check_deadline(now: u64, deadline: u64) -> RpcResult<()> {
    if now >= deadline {
        Err(expired())
    } else {
        Ok(())
    }
}
pub(super) fn header(req: &Request, name: &str) -> RpcResult<Option<String>> {
    req.headers()
        .get(name)
        .map_err(|_| invalid("invalid headers"))
}
fn enabled(env: &Env) -> bool {
    env.var("P0B_ENABLED")
        .is_ok_and(|v| v.to_string() == "true")
}

pub(super) use pintle_auth::identifier;
pub(super) fn authenticate(req: &Request, env: &Env, now: u64) -> RpcResult<pintle_auth::Identity> {
    let mode = env
        .var("PINTLE_AUTH_MODE")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "test".into());
    let static_mode = match mode.as_str() {
        "static" => true,
        "test" => false,
        _ => return Err(unavailable()),
    };
    // Never fall back to fixture credentials when static configuration is broken.
    let secret = if static_mode {
        "PINTLE_CREDENTIALS"
    } else {
        "PINTLE_TEST_CREDENTIALS"
    };
    let json = env.secret(secret).map_err(|_| unavailable())?.to_string();
    let authorization = header(req, "Authorization")?.unwrap_or_default();
    let token = authorization.strip_prefix("Bearer ").unwrap_or_default();
    if token.len() > 256 {
        return Err(RpcError::new(
            "unauthenticated",
            "valid bearer token required",
            401,
        ));
    }
    pintle_auth::authenticate(&json, token, now, static_mode).map_err(|e| match e {
        pintle_auth::Error::Configuration => unavailable(),
        pintle_auth::Error::Unauthorized => {
            RpcError::new("unauthenticated", "valid bearer token required", 401)
        }
    })
}
fn validate(request: &TryAcquireRequest) -> RpcResult<&v1::ResourceId> {
    let resource = request
        .resource
        .as_ref()
        .ok_or_else(|| invalid("resource is required"))?;
    if !identifier(&resource.namespace, 64)
        || resource.key.is_empty()
        || resource.key.len() > 128
        || resource.key.chars().any(char::is_control)
        || !identifier(&request.request_id, 128)
        || !identifier(&request.holder_ref, 64)
    {
        return Err(invalid("invalid resource, request_id, or holder_ref"));
    }
    Ok(resource)
}
pub(super) fn timeout(req: &Request) -> RpcResult<u64> {
    match header(req, "Connect-Timeout-Ms")? {
        None => Ok(MAX_TIMEOUT_MS),
        Some(v) if !v.is_empty() && v.len() <= 10 && v.bytes().all(|b| b.is_ascii_digit()) => {
            // Zero is treated as already expired (clients may round down).
            Ok(v.parse::<u64>()
                .map_err(|_| invalid("invalid timeout"))?
                .min(MAX_TIMEOUT_MS))
        }
        _ => Err(invalid(
            "Connect-Timeout-Ms must contain at most 10 ASCII digits",
        )),
    }
}
pub(super) async fn body(req: &mut Request) -> RpcResult<Vec<u8>> {
    if let Some(length) = header(req, "Content-Length")? {
        let length = length
            .parse::<u64>()
            .map_err(|_| invalid("invalid content length"))?;
        if length > MAX_BODY as u64 {
            return Err(RpcError::new(
                "resource_exhausted",
                "request exceeds 8192 bytes",
                429,
            ));
        }
    }
    let mut stream = req
        .stream()
        .map_err(|_| invalid("unreadable request body"))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| invalid("incomplete request body"))?;
        if chunk.len() > MAX_BODY - bytes.len() {
            return Err(RpcError::new(
                "resource_exhausted",
                "request exceeds 8192 bytes",
                429,
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(super) fn transport_headers(req: &Request) -> RpcResult<()> {
    if req.method() != Method::Post {
        return Err(RpcError::new("unimplemented", "RPC requires POST", 405));
    }
    if header(req, "Connect-Protocol-Version")?.as_deref() != Some("1") {
        return Err(invalid("Connect-Protocol-Version: 1 required"));
    }
    let content_type = header(req, "Content-Type")?.unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("application/proto")
    {
        return Err(RpcError::new(
            "unimplemented",
            "supported message codec: application/proto",
            415,
        ));
    }
    if header(req, "Content-Encoding")?.is_some_and(|v| v != "identity") {
        return Err(RpcError::new(
            "unimplemented",
            "supported content encoding: identity",
            501,
        ));
    }
    Ok(())
}

pub async fn gateway(mut req: Request, env: &Env) -> Result<Response> {
    let result = async {
        if !enabled(env) {
            return Err(RpcError::new(
                "unavailable",
                "P0b protocol experiment disabled",
                503,
            ));
        }
        if req.path() != PROCEDURE {
            return Err(RpcError::new("unimplemented", "unknown RPC", 501));
        }
        transport_headers(&req)?;
        let mut schedule = Schedule::new(env, &req).map_err(|_| unavailable())?;
        schedule
            .at("gateway_start")
            .await
            .map_err(|_| unavailable())?;
        let duration = timeout(&req)?;
        let deadline = schedule.now().saturating_add(duration);
        let timer = schedule.clone().timeout(duration);
        let work = async {
            check_deadline(schedule.now(), deadline)?;
            let credential = authenticate(&req, env, schedule.now())?;
            let bytes = body(&mut req).await?;
            let request = TryAcquireRequest::decode(bytes.as_slice())
                .map_err(|_| invalid("malformed protobuf"))?;
            let resource = validate(&request)?;
            if !credential.namespaces.contains(&resource.namespace) {
                return Err(RpcError::new(
                    "permission_denied",
                    "namespace not authorized",
                    403,
                ));
            }
            check_deadline(schedule.now(), deadline)?;
            schedule
                .at("before_dispatch")
                .await
                .map_err(|_| unavailable())?;
            check_deadline(schedule.now(), deadline)?;
            let name =
                crate::authority_name(&credential.tenant, &resource.namespace, &resource.key)
                    .map_err(|_| unavailable())?;
            let namespace = env
                .durable_object("AUTHORITIES")
                .map_err(|_| unavailable())?;
            let stub = namespace
                .id_from_name(&name)
                .and_then(|id| id.get_stub())
                .map_err(|_| unavailable())?;
            let mut init = RequestInit::new();
            let headers = Headers::new();
            schedule.forward(&headers).map_err(|_| unavailable())?;
            // Construct fresh internal metadata. Client-supplied routing/identity/
            // deadline headers are never forwarded to the authority.
            headers
                .set(SUBJECT, &credential.subject)
                .map_err(|_| unavailable())?;
            headers
                .set(DEADLINE, &deadline.to_string())
                .map_err(|_| unavailable())?;
            init.with_method(Method::Post)
                .with_headers(headers)
                .with_body(Some(JsValue::from(js_sys::Uint8Array::from(
                    request.encode_to_vec().as_slice(),
                ))));
            let internal = Request::new_with_init(
                &format!("https://authority.internal{INTERNAL_PATH}"),
                &init,
            )
            .map_err(|_| unavailable())?;
            let response = stub
                .fetch_with_request(internal)
                .await
                .map_err(|_| unavailable())?;
            schedule
                .at("gateway_response")
                .await
                .map_err(|_| unavailable())?;
            check_deadline(schedule.now(), deadline)?;
            Ok(response)
        };
        match select(Box::pin(work), Box::pin(timer)).await {
            Either::Left((result, _)) => result,
            Either::Right((result, _)) => {
                result.map_err(|_| unavailable())?;
                Err(expired())
            }
        }
    }
    .await;
    match result {
        Ok(response) => Ok(response),
        Err(error) => error.response(),
    }
}

pub fn is_internal(req: &Request) -> bool {
    req.path() == INTERNAL_PATH
}

pub async fn authority(mut req: Request, env: &Env, state: &State) -> Result<Response> {
    let result = async {
        if !enabled(env) {
            return Err(unavailable());
        }
        let mut schedule = Schedule::new(env, &req).map_err(|_| unavailable())?;
        schedule
            .at("authority_start")
            .await
            .map_err(|_| unavailable())?;
        let subject = header(&req, SUBJECT)?
            .filter(|v| identifier(v, 32))
            .ok_or_else(unavailable)?;
        let deadline = header(&req, DEADLINE)?
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(unavailable)?;
        check_deadline(schedule.now(), deadline)?;
        let request = TryAcquireRequest::decode(body(&mut req).await?.as_slice())
            .map_err(|_| invalid("malformed protobuf"))?;
        validate(&request)?;
        let fault = schedule
            .at("before_commit")
            .await
            .map_err(|_| unavailable())?;
        check_deadline(schedule.now(), deadline)?;
        let domain_request = DomainRequest {
            request_id: request.request_id.clone(),
            owner: serde_json::to_string(&(subject, &request.holder_ref))
                .map_err(|_| unavailable())?,
        };
        let storage = state.storage();
        let sql = storage.sql();
        sql.exec(
            "CREATE TABLE IF NOT EXISTS records (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT",
            None,
        )
        .map_err(|_| unavailable())?;
        let outcome = atomic(&storage, move || {
            // Keep all conditional reads and mutations synchronous, as in P0.
            let mut tx = SqlTransaction {
                sql,
                fault,
                writes: 0,
            };
            serde_json::to_string(&pintle_core::try_grant(&mut tx, &domain_request)?)
                .map_err(|e| e.to_string())
        })
        .map_err(|_| unavailable())?;
        schedule
            .at("after_commit")
            .await
            .map_err(|_| unavailable())?;
        schedule.sync(&storage).await.map_err(|_| unavailable())?;
        let outcome: Outcome = serde_json::from_str(&outcome).map_err(|_| unavailable())?;
        let response = match outcome {
            Outcome::Granted { grant, replayed } => TryAcquireResponse {
                outcome: Some(try_acquire_response::Outcome::Acquired(v1::GrantReceipt {
                    resource: request.resource,
                    request_id: request.request_id,
                    holder_ref: request.holder_ref,
                    fencing_token: grant.fence,
                    revision: grant.revision,
                    ..Default::default()
                })),
                replayed,
                observation: None,
            },
            Outcome::Contended => TryAcquireResponse {
                outcome: Some(try_acquire_response::Outcome::Contended(v1::Contended {})),
                replayed: false,
                observation: None,
            },
            Outcome::RequestIdConflict => {
                return Err(RpcError::new(
                    "already_exists",
                    "request_id already used with different parameters or principal",
                    409,
                ));
            }
            Outcome::Exhausted => {
                return Err(RpcError::new(
                    "resource_exhausted",
                    "counter exhausted",
                    429,
                ));
            }
        };
        schedule
            .at("before_response")
            .await
            .map_err(|_| unavailable())?;
        check_deadline(schedule.now(), deadline)?;
        let response = Response::from_bytes(response.encode_to_vec()).map_err(|_| unavailable())?;
        response
            .headers()
            .set("Content-Type", "application/proto")
            .map_err(|_| unavailable())?;
        response
            .headers()
            .set("Cache-Control", "no-store")
            .map_err(|_| unavailable())?;
        Ok(response)
    }
    .await;
    match result {
        Ok(response) => Ok(response),
        Err(error) => error.response(),
    }
}
