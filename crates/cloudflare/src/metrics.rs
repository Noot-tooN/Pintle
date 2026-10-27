//! Opt-in bounded structured events. Never log identities, resources, request
//! bodies/IDs, capabilities, credentials, or raw errors. The exporter is external.
use worker::*;
pub fn instance(env: &Env) -> Option<String> {
    enabled(env)
        .then(|| crate::lease_rpc::new_token().ok())
        .flatten()
}
fn enabled(env: &Env) -> bool {
    env.var("PINTLE_METRICS")
        .is_ok_and(|v| v.to_string() == "true")
}

// A fresh random diagnostic ID per construction, unrelated to resource identity
// or ownership capabilities. Kept only in memory; never used by coordination.
pub fn authority(instance: Option<&str>, operation: &str, result: &Result<Response>) {
    if let Some(instance) = instance {
        let status = result.as_ref().map(|r| r.status_code()).unwrap_or(500);
        console_log!(
            "{}",
            serde_json::json!({"event":"pintle.authority","instance":instance,"operation":operation,"status":status})
        );
    }
}
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
    if !enabled(env) {
        return;
    }
    let status = result.as_ref().map(|r| r.status_code()).unwrap_or(500);
    console_log!(
        "{}",
        serde_json::json!({"event":"pintle.rpc","operation":operation,"status":status,"duration_ms":Date::now().as_millis().saturating_sub(start)})
    );
}
