//! Connect and SQLite adapter for the opt-in local singleflight service.
use crate::{Fault, SqlTransaction, atomic, connect::*, lease_rpc, schedule::Schedule};
use pintle_core::{
    Transaction,
    singleflight::{
        self as sf,
        durable::{Action, Authority, Rejection, Reply},
    },
};
use pintle_protocol::v1::*;
use prost::Message;
use worker::*;

pub(super) const PREFIX: &str = "/pintle.v1.SingleflightService/";
pub(super) const INTERNAL: &str = "/internal/singleflight/";
pub(super) fn enabled(env: &Env) -> bool {
    lease_rpc::waiting_enabled(env)
        && env
            .var("PINTLE_SINGLEFLIGHT_ENABLED")
            .is_ok_and(|v| v.to_string() == "true")
}
pub(super) fn known(method: &str) -> bool {
    matches!(
        method,
        "GetFlightGeneration" | "JoinFlight" | "CompleteFlight" | "CancelFlight" | "ObserveFlight"
    )
}
fn message<M: Message + Default>(bytes: &[u8]) -> RpcResult<M> {
    M::decode(bytes).map_err(|_| invalid("malformed protobuf"))
}
pub(super) fn decode(method: &str, bytes: &[u8]) -> RpcResult<(ResourceId, Action)> {
    let (resource, action) = match method {
        "GetFlightGeneration" => (
            message::<GetFlightGenerationRequest>(bytes)?.resource,
            Action::Generation,
        ),
        "JoinFlight" => {
            let r: JoinFlightRequest = message(bytes)?;
            let generation =
                pintle_core::counter(&r.generation).map_err(|_| invalid("invalid generation"))?;
            (
                r.resource,
                Action::Join {
                    generation,
                    request_id: r.request_id,
                },
            )
        }
        "ObserveFlight" => {
            let r: ObserveFlightRequest = message(bytes)?;
            (r.resource, Action::Observe(r.ticket))
        }
        "CancelFlight" => {
            let r: CancelFlightRequest = message(bytes)?;
            (r.resource, Action::Cancel(r.ticket))
        }
        "CompleteFlight" => {
            use complete_flight_request::Completion;
            let r: CompleteFlightRequest = message(bytes)?;
            let outcome = match r.completion {
                Some(Completion::Value(v)) if v.len() <= sf::MAX_RESULT_BYTES => {
                    sf::Outcome::Value(v)
                }
                Some(Completion::Value(_)) => sf::Outcome::ResultTooLarge,
                Some(Completion::Failure(f)) => match FlightFailure::try_from(f) {
                    Ok(FlightFailure::FillFailed) => sf::Outcome::FillFailed,
                    Ok(FlightFailure::ResultTooLarge) => sf::Outcome::ResultTooLarge,
                    _ => return Err(invalid("unknown failure")),
                },
                None => return Err(invalid("completion required")),
            };
            (
                r.resource,
                Action::Complete {
                    ticket: r.ticket,
                    outcome,
                },
            )
        }
        _ => return Err(RpcError::new("unimplemented", "unknown RPC", 501)),
    };
    Ok((lease_rpc::checked_resource(resource)?, action))
}

fn rejection(e: Rejection) -> RpcError {
    match e {
        Rejection::Invalid => invalid("invalid flight request"),
        Rejection::ClosedGeneration => RpcError::new(
            "failed_precondition",
            "generation closed; reconcile the cache before a fresh admission",
            400,
        ),
        Rejection::Conflict => RpcError::new(
            "already_exists",
            "completion conflicts with recorded outcome",
            409,
        ),
        Rejection::Core(sf::Error::UnknownTicket) => {
            RpcError::new("not_found", "unknown flight ticket", 404)
        }
        Rejection::Core(sf::Error::WrongRole) => RpcError::new(
            "permission_denied",
            "ticket role cannot perform this operation",
            403,
        ),
        Rejection::Core(sf::Error::Capacity | sf::Error::GenerationExhausted) => {
            RpcError::new("resource_exhausted", "flight capacity exhausted", 429)
        }
        Rejection::Core(sf::Error::Clock) => unavailable(),
    }
}
fn encode(method: &str, reply: Reply) -> RpcResult<Vec<u8>> {
    Ok(match reply {
        Reply::Generation(generation) => GetFlightGenerationResponse {
            generation: generation.to_string(),
        }
        .encode_to_vec(),
        Reply::Joined { ticket, producer } => {
            JoinFlightResponse { ticket, producer }.encode_to_vec()
        }
        Reply::Observation(o) => {
            use flight_observation::Outcome;
            let outcome = match o {
                sf::Observation::Pending => Outcome::Status(FlightStatus::Pending as i32),
                sf::Observation::Cancelled => Outcome::Status(FlightStatus::Cancelled as i32),
                sf::Observation::Finished(result) => match result {
                    sf::Outcome::Value(v) => Outcome::Value(v),
                    sf::Outcome::FillFailed => Outcome::Status(FlightStatus::FillFailed as i32),
                    sf::Outcome::LeaderExpired => {
                        Outcome::Status(FlightStatus::LeaderExpired as i32)
                    }
                    sf::Outcome::ResultTooLarge => {
                        Outcome::Status(FlightStatus::ResultTooLarge as i32)
                    }
                },
            };
            let observation = Some(FlightObservation {
                outcome: Some(outcome),
            });
            match method {
                "CompleteFlight" => CompleteFlightResponse { observation }.encode_to_vec(),
                "ObserveFlight" => ObserveFlightResponse { observation }.encode_to_vec(),
                "CancelFlight" => CancelFlightResponse { observation }.encode_to_vec(),
                _ => return Err(unavailable()),
            }
        }
    })
}

fn transact(
    sql: SqlStorage,
    fault: Option<Fault>,
    now: u64,
    subject: &str,
    action: Action,
    nonce: &str,
) -> std::result::Result<String, String> {
    // The table itself marks initialization. An existing table with a missing
    // snapshot must never silently reissue generation one.
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
    let mut tx = SqlTransaction {
        sql,
        fault,
        writes: 0,
    };
    let mut group = if exists {
        Authority::restore(&tx.get("singleflight/v1")?.ok_or("missing flight record")?)?
    } else {
        Authority::default()
    };
    let reply = group.apply(now, subject, action, nonce);
    tx.put(
        "singleflight/v1",
        &serde_json::to_string(&group).map_err(|e| e.to_string())?,
    )?;
    serde_json::to_string(&reply).map_err(|e| e.to_string())
}

pub(super) async fn execute(
    state: &State,
    mut schedule: Schedule,
    subject: String,
    deadline: u64,
    method: &str,
    action: Action,
) -> RpcResult<Response> {
    let nonce = if matches!(action, Action::Join { .. }) {
        lease_rpc::new_token().map_err(|_| unavailable())?
    } else {
        String::new()
    };
    let fault = schedule
        .at("before_commit")
        .await
        .map_err(|_| unavailable())?;
    check_deadline(schedule.now(), deadline)?;
    let storage = state.storage();
    let sql = storage.sql();
    let clock = schedule.clone();
    let raw = atomic(&storage, move || {
        transact(sql, fault, clock.now(), &subject, action, &nonce)
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
    let reply: std::result::Result<Reply, Rejection> =
        serde_json::from_str(&raw).map_err(|_| unavailable())?;
    let r = Response::from_bytes(encode(method, reply.map_err(rejection)?)?)
        .map_err(|_| unavailable())?;
    r.headers()
        .set("Content-Type", "application/proto")
        .map_err(|_| unavailable())?;
    r.headers()
        .set("Cache-Control", "no-store")
        .map_err(|_| unavailable())?;
    Ok(r)
}
