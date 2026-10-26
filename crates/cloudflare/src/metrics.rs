//! Opt-in bounded structured events. Never log identities, resources, request
//! bodies/IDs, capabilities, credentials, or raw errors. The exporter is external.
use worker::*;
pub fn method(path: &str) -> &'static str {
    match path.strip_prefix("/pintle.v1.LockService/") {
        Some("Acquire") => "Acquire",
        Some("CancelPending") => "CancelPending",
        Some("TryAcquire") => "TryAcquire",
        Some("Renew") => "Renew",
        Some("Release") => "Release",
        Some("GetRequestStatus") => "GetRequestStatus",
        Some("OpenSession") => "OpenSession",
        Some("AcknowledgeSession") => "AcknowledgeSession",
        _ => "unknown",
    }
}
pub fn emit(env: &Env, operation: &str, start: u64, result: &Result<Response>) {
    if !env
        .var("PINTLE_METRICS")
        .is_ok_and(|v| v.to_string() == "true")
    {
        return;
    }
    let status = result.as_ref().map(|r| r.status_code()).unwrap_or(500);
    console_log!(
        "{}",
        serde_json::json!({"event":"pintle.rpc","operation":operation,"status":status,"duration_ms":Date::now().as_millis().saturating_sub(start)})
    );
}
