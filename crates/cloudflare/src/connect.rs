//! Shared, bounded Connect unary/binary transport and authentication validation.
use futures_util::StreamExt;
use worker::*;

const MAX_BODY: usize = 8192;
const MAX_TIMEOUT_MS: u64 = 10_000;

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
pub(super) use pintle_auth::identifier;
pub(super) fn authenticate(req: &Request, env: &Env, now: u64) -> RpcResult<pintle_auth::Identity> {
    let mode = env
        .var("PINTLE_AUTH_MODE")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "static".into());
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
