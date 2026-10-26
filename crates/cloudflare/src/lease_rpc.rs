//! Lease/wait wire and platform adaptation. All coordination decisions live in pintle-core.
use crate::{SqlTransaction, atomic, connect::*, schedule::Schedule};
use futures_util::future::{Either, select};
use pintle_core::Transaction;
use pintle_core::lease::{self, Command, Mutation, Rejection, Reply};
use pintle_core::session::{self, waiting};
use pintle_protocol::v1::*;
use prost::Message;
use wasm_bindgen::prelude::*;
use worker::*;

const PREFIX: &str = "/pintle.v1.LockService/";
const INTERNAL: &str = "/internal/p1/";
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = crypto, js_name = getRandomValues, catch)]
    fn random_values(array: &js_sys::Uint8Array) -> std::result::Result<JsValue, JsValue>;
}
fn new_token() -> Result<String> {
    let bytes = js_sys::Uint8Array::new_with_length(32);
    random_values(&bytes)?;
    Ok(bytes.to_vec().iter().map(|b| format!("{b:02x}")).collect())
}
pub fn enabled(env: &Env) -> bool {
    sessions_enabled(env) || env.var("P1_ENABLED").is_ok_and(|v| v.to_string() == "true")
}
pub fn waiting_enabled(env: &Env) -> bool {
    env.var("P2_ENABLED").is_ok_and(|v| v.to_string() == "true")
}
pub fn sessions_enabled(env: &Env) -> bool {
    waiting_enabled(env)
        || env
            .var("P11_ENABLED")
            .is_ok_and(|v| v.to_string() == "true")
}
#[derive(Clone)]
enum Management {
    Open,
    Ack { id: String, through: u64 },
}
pub fn is_internal(req: &Request) -> bool {
    req.path().starts_with(INTERNAL)
}
#[derive(Clone)]
struct Input {
    resource: ResourceId,
    command: Command,
    attempt: Option<session::Frame>,
    management: Option<Management>,
    waiting: Option<(u64, bool)>,
}
fn decode(method: &str, bytes: &[u8]) -> RpcResult<Input> {
    let mut attempt = None;
    let mut management = None;
    let mut waiting = None;
    let (resource, command) = match method {
        "Acquire" | "CancelPending" => {
            let r = if method == "CancelPending" {
                CancelPendingRequest::decode(bytes)
                    .map_err(|_| invalid("malformed protobuf"))?
                    .acquire
                    .ok_or_else(|| invalid("acquire required"))?
            } else {
                AcquireRequest::decode(bytes).map_err(|_| invalid("malformed protobuf"))?
            };
            let a = r
                .acquisition
                .ok_or_else(|| invalid("acquisition required"))?;
            attempt = frame(a.attempt)?;
            waiting = Some((u64::from(r.wait_timeout_ms), method == "CancelPending"));
            (
                a.resource,
                Command::Acquire {
                    id: a.request_id,
                    holder: a.holder_ref,
                    ttl_ms: a.lease_ttl_ms.into(),
                },
            )
        }
        "TryAcquire" => {
            let r = TryAcquireRequest::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
            attempt = frame(r.attempt)?;
            (
                r.resource,
                Command::Acquire {
                    id: r.request_id,
                    holder: r.holder_ref,
                    ttl_ms: r.lease_ttl_ms.into(),
                },
            )
        }
        "Renew" => {
            let r = RenewRequest::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
            attempt = frame(r.attempt)?;
            (
                r.resource,
                Command::Renew {
                    id: r.request_id,
                    token: r.lease_id,
                    ttl_ms: r.lease_ttl_ms.into(),
                },
            )
        }
        "Release" => {
            let r = ReleaseRequest::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
            attempt = frame(r.attempt)?;
            (
                r.resource,
                Command::Release {
                    id: r.request_id,
                    token: r.lease_id,
                },
            )
        }
        "GetRequestStatus" => {
            let r = GetRequestStatusRequest::decode(bytes)
                .map_err(|_| invalid("malformed protobuf"))?;
            {
                attempt = frame(r.attempt)?;
                (r.resource, Command::Status { id: r.request_id })
            }
        }
        "OpenSession" => {
            let r = OpenSessionRequest::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
            management = Some(Management::Open);
            (
                r.resource,
                Command::Status {
                    id: "management".into(),
                },
            )
        }
        "AcknowledgeSession" => {
            let r = AcknowledgeSessionRequest::decode(bytes)
                .map_err(|_| invalid("malformed protobuf"))?;
            management = Some(Management::Ack {
                id: r.session_id,
                through: decimal(&r.acknowledged_through)?,
            });
            (
                r.resource,
                Command::Status {
                    id: "management".into(),
                },
            )
        }
        _ => return Err(RpcError::new("unimplemented", "unknown RPC", 501)),
    };
    let resource = resource.ok_or_else(|| invalid("resource required"))?;
    if !identifier(&resource.namespace, 64)
        || resource.key.is_empty()
        || resource.key.len() > 128
        || resource.key.chars().any(char::is_control)
    {
        return Err(invalid("invalid resource"));
    }
    Ok(Input {
        resource,
        command,
        attempt,
        management,
        waiting,
    })
}
fn decimal(s: &str) -> RpcResult<u64> {
    pintle_core::counter(s).map_err(|_| invalid("invalid decimal counter"))
}
fn frame(a: Option<RequestAttempt>) -> RpcResult<Option<session::Frame>> {
    a.map(|a| {
        Ok(session::Frame {
            session_id: a.session_id,
            sequence: decimal(&a.sequence)?,
            acknowledged: decimal(&a.acknowledged_through)?,
        })
    })
    .transpose()
}
pub async fn gateway(req: Request, env: &Env) -> Result<Response> {
    let start = Date::now().as_millis();
    let operation = crate::metrics::method(&req.path());
    let result = gateway_inner(req, env).await;
    crate::metrics::emit(env, operation, start, &result);
    result
}
async fn gateway_inner(mut req: Request, env: &Env) -> Result<Response> {
    let result = async {
        if (waiting_enabled(env)
            && env
                .var("P11_ENABLED")
                .is_ok_and(|v| v.to_string() == "true"))
            || !enabled(env)
            || env
                .var("P0B_ENABLED")
                .is_ok_and(|v| v.to_string() == "true")
            || crate::enabled(env)
            || (sessions_enabled(env)
                && env.var("P1_ENABLED").is_ok_and(|v| v.to_string() == "true"))
        {
            return Err(unavailable());
        }
        let path = req.path();
        let method = path
            .strip_prefix(PREFIX)
            .ok_or_else(|| RpcError::new("unimplemented", "unknown RPC", 501))?;
        if !matches!(
            method,
            "Acquire"
                | "CancelPending"
                | "TryAcquire"
                | "Renew"
                | "Release"
                | "GetRequestStatus"
                | "OpenSession"
                | "AcknowledgeSession"
        ) {
            return Err(RpcError::new("unimplemented", "unknown RPC", 501));
        }
        if matches!(method, "Acquire" | "CancelPending") && !waiting_enabled(env) {
            return Err(RpcError::new("unimplemented", "P2 disabled", 501));
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
            let input = decode(method, &bytes)?;
            if !credential.namespaces.contains(&input.resource.namespace) {
                return Err(RpcError::new(
                    "permission_denied",
                    "namespace not authorized",
                    403,
                ));
            }
            schedule
                .at("before_dispatch")
                .await
                .map_err(|_| unavailable())?;
            check_deadline(schedule.now(), deadline)?;
            let name = serde_json::to_string(&(
                &credential.tenant,
                &input.resource.namespace,
                "mutex-v1",
                &input.resource.key,
            ))
            .map_err(|_| unavailable())?;
            let ns = env
                .durable_object("AUTHORITIES")
                .map_err(|_| unavailable())?;
            let stub = ns
                .id_from_name(&name)
                .and_then(|id| id.get_stub())
                .map_err(|_| unavailable())?;
            let headers = Headers::new();
            headers
                .set("x-pintle-subject", &credential.subject)
                .map_err(|_| unavailable())?;
            headers
                .set("x-pintle-deadline-ms", &deadline.to_string())
                .map_err(|_| unavailable())?;
            schedule.forward(&headers).map_err(|_| unavailable())?;
            let mut init = RequestInit::new();
            init.with_method(Method::Post)
                .with_headers(headers)
                .with_body(Some(JsValue::from(js_sys::Uint8Array::from(
                    bytes.as_slice(),
                ))));
            let internal = Request::new_with_init(
                &format!("https://authority.internal{INTERNAL}{method}"),
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
        Ok(r) => Ok(r),
        Err(e) => e.response(),
    }
}
fn rejection(r: Rejection) -> RpcError {
    match r {
        Rejection::SessionExpired => RpcError::new(
            "failed_precondition",
            "session_expired: old attempts cannot execute; reconcile before new work",
            400,
        ),
        Rejection::Acknowledged => RpcError::new(
            "failed_precondition",
            "result_acknowledged: receipt reclaimed; attempt cannot execute again",
            400,
        ),
        Rejection::InvalidArgument => invalid("invalid request, credential, or TTL (100..60000ms)"),
        Rejection::Conflict => RpcError::new(
            "already_exists",
            "request_id reused with different operation or parameters",
            409,
        ),
        Rejection::NotOwner => {
            RpcError::new("permission_denied", "not the current lease owner", 403)
        }
        Rejection::LeaseLost => {
            RpcError::new("failed_precondition", "lease expired or released", 400)
        }
        Rejection::Exhausted => RpcError::new(
            "resource_exhausted",
            "counter or receipt capacity exhausted",
            429,
        ),
        Rejection::ClockRegression => RpcError::new(
            "unavailable",
            "authority clock regressed; retry SAME request after clock recovery",
            503,
        ),
        Rejection::NotFound => RpcError::new(
            "not_found",
            "no committed receipt observed; in-flight requests may still commit",
            404,
        ),
        Rejection::PermissionDenied => RpcError::new(
            "permission_denied",
            "receipt belongs to another principal",
            403,
        ),
    }
}
fn wire_lease(l: lease::Lease, resource: &ResourceId) -> GrantReceipt {
    GrantReceipt {
        resource: Some(resource.clone()),
        request_id: l.acquisition_id,
        holder_ref: l.holder,
        fencing_token: l.fence,
        revision: l.revision,
        lease_id: l.token,
        expires_at_unix_ms: l.expires_at_ms,
    }
}
fn observation(o: Option<lease::Observation>, resource: &ResourceId) -> Option<LeaseObservation> {
    o.map(|o| LeaseObservation {
        state: match o.state {
            lease::LeaseState::Active => LeaseState::Active,
            lease::LeaseState::Expired => LeaseState::Expired,
            lease::LeaseState::Released => LeaseState::Released,
            lease::LeaseState::Superseded => LeaseState::Superseded,
        }
        .into(),
        current: o.current.map(|l| wire_lease(l, resource)),
    })
}
fn encode(reply: Reply, resource: &ResourceId, status: bool) -> RpcResult<Vec<u8>> {
    let (receipt, replayed, obs) = match reply {
        Reply::Rejected(r) => return Err(rejection(r)),
        Reply::Contended => {
            let r = TryAcquireResponse {
                outcome: Some(try_acquire_response::Outcome::Contended(Contended {})),
                replayed: false,
                observation: None,
            };
            return Ok(if status {
                GetRequestStatusResponse {
                    outcome: Some(get_request_status_response::Outcome::Acquired(r)),
                }
                .encode_to_vec()
            } else {
                r.encode_to_vec()
            });
        }
        Reply::Applied {
            receipt,
            replayed,
            observation: obs,
        } => (receipt, replayed, observation(obs, resource)),
    };
    let (bytes, outcome) = match receipt.mutation {
        Mutation::Acquired(l) => {
            let r = TryAcquireResponse {
                outcome: Some(try_acquire_response::Outcome::Acquired(wire_lease(
                    l, resource,
                ))),
                replayed,
                observation: obs,
            };
            (
                r.encode_to_vec(),
                get_request_status_response::Outcome::Acquired(r),
            )
        }
        Mutation::Renewed(l) => {
            let r = RenewResponse {
                lease: Some(wire_lease(l, resource)),
                replayed,
                observation: obs,
            };
            (
                r.encode_to_vec(),
                get_request_status_response::Outcome::Renewed(r),
            )
        }
        Mutation::Released { revision, .. } => {
            let r = ReleaseResponse { revision, replayed };
            (
                r.encode_to_vec(),
                get_request_status_response::Outcome::Released(r),
            )
        }
    };
    Ok(if status {
        GetRequestStatusResponse {
            outcome: Some(outcome),
        }
        .encode_to_vec()
    } else {
        bytes
    })
}
pub async fn authority(
    mut req: Request,
    env: &Env,
    state: &State,
    notifications: &crate::notify::Notifications,
) -> Result<Response> {
    let result = async {
        if !enabled(env) {
            return Err(unavailable());
        }
        let mut schedule = Schedule::new(env, &req).map_err(|_| unavailable())?;
        schedule
            .at("authority_start")
            .await
            .map_err(|_| unavailable())?;
        let subject = header(&req, "x-pintle-subject")?
            .filter(|s| identifier(s, 32))
            .ok_or_else(unavailable)?;
        let deadline = header(&req, "x-pintle-deadline-ms")?
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(unavailable)?;
        check_deadline(schedule.now(), deadline)?;
        let path = req.path();
        let method = path.strip_prefix(INTERNAL).ok_or_else(unavailable)?;
        let input = decode(method, &body(&mut req).await?)?;
        let use_sessions = sessions_enabled(env);
        if (use_sessions && input.management.is_none() && input.attempt.is_none())
            || (!use_sessions && (input.management.is_some() || input.attempt.is_some()))
        {
            return Err(invalid(
                "P1.1 requires a session attempt; legacy P1 is a separate fixture",
            ));
        }
        let subscription = if method == "Acquire" {
            Some(
                notifications
                    .subscribe()
                    .ok_or_else(|| rejection(Rejection::Exhausted))?,
            )
        } else {
            None
        };
        let nonce = new_token().map_err(|_| unavailable())?;
        let fault = schedule
            .at("before_commit")
            .await
            .map_err(|_| unavailable())?;
        check_deadline(schedule.now(), deadline)?;
        let storage = state.storage();
        let sql = storage.sql();
        sql.exec(
            "CREATE TABLE IF NOT EXISTS records (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT",
            None,
        )
        .map_err(|_| unavailable())?;
        // Arm a durable fallback BEFORE admitting any session mutation. A crash
        // after commit cannot strand cleanup by losing the subsequent scheduling.
        if use_sessions {
            arm_fallback(&storage, env)
                .await
                .map_err(|_| unavailable())?;
        }
        // Alarm persistence may yield; enforce the deadline again immediately
        // before admitting the synchronous transaction.
        check_deadline(schedule.now(), deadline)?;
        let use_waiting = waiting_enabled(env);
        let wait_command = input.waiting;
        let request_id = input.command.id().to_owned();
        let query_subject = subject.clone();
        let query_attempt = input.attempt.clone();
        let command = input.command;
        let management = input.management.clone();
        let attempt = input.attempt;
        let status = matches!(command, Command::Status { .. });
        // Sample time at entry to the synchronous transaction. No awaits inside.
        let clock = schedule.clone();
        let json = atomic(&storage, move || {
            let mut tx = SqlTransaction {
                sql,
                fault,
                writes: 0,
            };
            let result = if use_waiting {
                let action = if let Some((duration, cancel)) = wait_command {
                    waiting::Action::Acquire {
                        frame: attempt.as_ref().ok_or("missing attempt")?,
                        command: &command,
                        duration,
                        cancel,
                    }
                } else {
                    match &management {
                        Some(Management::Open) => waiting::Action::Open,
                        Some(Management::Ack { id, through }) => waiting::Action::Acknowledge {
                            id,
                            through: *through,
                        },
                        None => waiting::Action::Execute {
                            frame: attempt.as_ref().ok_or("missing attempt")?,
                            command: &command,
                        },
                    }
                };
                waiting::apply(&mut tx, &subject, action, clock.now(), &nonce)?
            } else if use_sessions {
                match management {
                    Some(Management::Open) => session::open(&mut tx, &subject, clock.now())?,
                    Some(Management::Ack { id, through }) => {
                        session::ack(&mut tx, &subject, &id, through, clock.now())?
                    }
                    None => session::execute(
                        &mut tx,
                        &subject,
                        &attempt.ok_or("missing attempt")?,
                        &command,
                        clock.now(),
                        &nonce,
                    )?,
                }
            } else {
                session::Output::Operation(lease::execute(
                    &mut tx,
                    &subject,
                    &command,
                    clock.now(),
                    &nonce,
                )?)
            };
            serde_json::to_string(&result).map_err(|e| e.to_string())
        })
        .map_err(|_| unavailable())?;
        if use_waiting {
            notifications.notify();
        }
        schedule
            .at("after_commit")
            .await
            .map_err(|_| unavailable())?;
        schedule.sync(&storage).await.map_err(|_| unavailable())?;
        if use_sessions {
            schedule_cleanup(&storage, env, schedule.now())
                .await
                .map_err(|_| unavailable())?;
        }
        let mut reply: session::Output = serde_json::from_str(&json).map_err(|_| unavailable())?;
        if let Some(subscription) = subscription {
            schedule
                .at("wait_registered")
                .await
                .map_err(|_| unavailable())?;
            let hold = deadline
                .saturating_sub(schedule.now())
                .saturating_sub(100)
                .min(8_000);
            let timer = schedule.clone().timeout(hold);
            let mut timer = Box::pin(timer);
            while matches!(reply, session::Output::Waiting(waiting::State::Pending)) && hold > 0 {
                match select(Box::pin(subscription.changed()), timer).await {
                    Either::Right((r, _)) => {
                        r.map_err(|_| unavailable())?;
                        break;
                    }
                    Either::Left((_, remaining)) => {
                        timer = remaining;
                    }
                }
                schedule.at("wait_wake").await.map_err(|_| unavailable())?;
                check_deadline(schedule.now(), deadline)?;
                let sql = storage.sql();
                let subject = query_subject.clone();
                let frame = query_attempt.clone().ok_or_else(unavailable)?;
                let id = request_id.clone();
                let clock = schedule.clone();
                let json = atomic(&storage, move || {
                    let mut tx = SqlTransaction {
                        sql,
                        fault: None,
                        writes: 0,
                    };
                    let result = session::execute(
                        &mut tx,
                        &subject,
                        &frame,
                        &Command::Status { id },
                        clock.now(),
                        "",
                    )?;
                    serde_json::to_string(&result).map_err(|e| e.to_string())
                })
                .map_err(|_| unavailable())?;
                storage.sync().await.map_err(|_| unavailable())?;
                reply = serde_json::from_str(&json).map_err(|_| unavailable())?;
            }
        }
        let bytes = match reply {
            session::Output::Rejected(r) => return Err(rejection(r)),
            session::Output::Operation(r) => {
                let bytes = encode(r, &input.resource, status)?;
                if wait_command.is_some() {
                    AcquireResponse {
                        outcome: Some(acquire_response::Outcome::Acquired(Box::new(
                            TryAcquireResponse::decode(bytes.as_slice())
                                .map_err(|_| unavailable())?,
                        ))),
                    }
                    .encode_to_vec()
                } else {
                    bytes
                }
            }
            session::Output::Waiting(s) => {
                let state = match s {
                    waiting::State::Pending => WaitState::Pending,
                    waiting::State::Cancelled => WaitState::Cancelled,
                    waiting::State::Expired => WaitState::Expired,
                    waiting::State::Complete => return Err(unavailable()),
                } as i32;
                if status {
                    GetRequestStatusResponse {
                        outcome: Some(get_request_status_response::Outcome::Waiting(state)),
                    }
                    .encode_to_vec()
                } else {
                    AcquireResponse {
                        outcome: Some(acquire_response::Outcome::Waiting(state)),
                    }
                    .encode_to_vec()
                }
            }
            session::Output::Session(s) => {
                let session = Some(RecoverySession {
                    session_id: s.id,
                    acknowledged_through: s.acknowledged.to_string(),
                    expires_at_unix_ms: s.expires_at_ms.to_string(),
                    request_window: session::WINDOW as u32,
                });
                if method == "OpenSession" {
                    OpenSessionResponse { session }.encode_to_vec()
                } else {
                    AcknowledgeSessionResponse { session }.encode_to_vec()
                }
            }
        };
        let bytes = if method == "CancelPending" {
            CancelPendingResponse {
                result: Some(AcquireResponse::decode(bytes.as_slice()).map_err(|_| unavailable())?),
            }
            .encode_to_vec()
        } else {
            bytes
        };
        schedule
            .at("before_response")
            .await
            .map_err(|_| unavailable())?;
        check_deadline(schedule.now(), deadline)?;
        let response = Response::from_bytes(bytes).map_err(|_| unavailable())?;
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
        Ok(r) => Ok(r),
        Err(e) => e.response(),
    }
}

async fn arm_fallback(storage: &Storage, _env: &Env) -> Result<()> {
    let delay = 60_000;
    #[cfg(feature = "test-hooks")]
    let delay = if _env
        .var("PINTLE_TEST_REAL_ALARM")
        .is_ok_and(|v| v.to_string() == "true")
    {
        20
    } else {
        delay
    };
    storage
        .set_alarm(std::time::Duration::from_millis(delay))
        .await
}
async fn schedule_cleanup(storage: &Storage, _env: &Env, now: u64) -> Result<()> {
    // No await between reading the committed schedule and issuing the storage
    // alarm operation; never use an earlier request's cached expiry snapshot.
    let tx = SqlTransaction {
        sql: storage.sql(),
        fault: None,
        writes: 0,
    };
    let next = if waiting_enabled(_env) {
        waiting::next_wakeup(&tx)
    } else {
        session::next_expiry(&tx)
    }
    .map_err(Error::RustError)?;
    if let Some(next) = next {
        let floor = pintle_core::counter(
            &tx.get("clock")
                .map_err(Error::RustError)?
                .unwrap_or_else(|| "0".into()),
        )
        .map_err(Error::RustError)?;
        let delay = next.saturating_sub(now.max(floor)).max(1);
        #[cfg(feature = "test-hooks")]
        let delay = if _env
            .var("PINTLE_TEST_REAL_ALARM")
            .is_ok_and(|v| v.to_string() == "true")
        {
            20
        } else {
            delay
        };
        storage
            .set_alarm(std::time::Duration::from_millis(delay))
            .await
    } else {
        storage.delete_alarm().await
    }
}
pub async fn alarm(
    env: &Env,
    state: &State,
    notifications: &crate::notify::Notifications,
) -> Result<Response> {
    let start = Date::now().as_millis();
    let result = alarm_inner(env, state, notifications).await;
    crate::metrics::emit(env, "alarm", start, &result);
    result
}
async fn alarm_inner(
    env: &Env,
    state: &State,
    notifications: &crate::notify::Notifications,
) -> Result<Response> {
    if !sessions_enabled(env) {
        return Response::ok("no session cleanup");
    }
    let storage = state.storage();
    // Re-arm before cleanup so errors also leave an independent durable retry.
    arm_fallback(&storage, env).await?;
    let req = Request::new("https://authority.internal/alarm", Method::Post)?;
    #[cfg(feature = "test-hooks")]
    req.headers().set("x-pintle-test-call", "alarm")?;
    let mut schedule = Schedule::new(env, &req)?;
    let fault = schedule.at("alarm_start").await?;
    let sql = storage.sql();
    sql.exec(
        "CREATE TABLE IF NOT EXISTS records (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT",
        None,
    )?;
    let clock = schedule.clone();
    let use_waiting = waiting_enabled(env);
    let nonce = new_token()?;
    let result = atomic(&storage, move || {
        let mut tx = SqlTransaction {
            sql,
            fault,
            writes: 0,
        };
        let result = if use_waiting {
            waiting::drive(&mut tx, clock.now(), &nonce)?
        } else {
            session::cleanup(&mut tx, clock.now())?
        };
        serde_json::to_string(&result).map_err(|e| e.to_string())
    })?;
    if use_waiting {
        notifications.notify();
    }
    storage.sync().await?;
    let rejection: Option<Rejection> = serde_json::from_str(&result)?;
    if rejection.is_some() {
        return Err(Error::RustError("cleanup clock unavailable".into()));
    }
    schedule_cleanup(&storage, env, schedule.now()).await?;
    schedule.at("alarm_done").await?;
    Response::ok("session cleanup complete")
}
