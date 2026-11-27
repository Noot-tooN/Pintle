//! Worker transport for resource-local cooldown decisions.
use crate::{connect::*, lease_rpc, schedule::Schedule, snapshot};
use pintle_core::cooldown::{Attempt, Cooldown, Decision, Error as Rejection};
use pintle_protocol::v1::*;
use prost::Message;
use serde::{Deserialize, Serialize};
use worker::*;

pub(super) const PREFIX: &str = "/pintle.v1.CooldownService/";
pub(super) const INTERNAL: &str = "/internal/cooldown/";
pub(super) fn enabled(env: &Env) -> bool {
    lease_rpc::waiting_enabled(env)
        && env
            .var("PINTLE_COOLDOWN_ENABLED")
            .is_ok_and(|v| v.to_string() == "true")
}
pub(super) fn known(method: &str) -> bool {
    matches!(method, "GetCooldownWindow" | "TryAdmit")
}
pub(super) enum Action {
    Prepare,
    Admit {
        window_id: u64,
        request_id: String,
        interval_ms: u64,
    },
}
pub(super) fn decode(method: &str, bytes: &[u8]) -> RpcResult<(ResourceId, Action)> {
    let (resource, action) = match method {
        "GetCooldownWindow" => (
            GetCooldownWindowRequest::decode(bytes)
                .map_err(|_| invalid("malformed protobuf"))?
                .resource,
            Action::Prepare,
        ),
        "TryAdmit" => {
            let r = TryAdmitRequest::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
            (
                r.resource,
                Action::Admit {
                    window_id: pintle_core::counter(&r.window_id)
                        .map_err(|_| invalid("invalid window ID"))?,
                    request_id: r.request_id,
                    interval_ms: r.interval_ms.into(),
                },
            )
        }
        _ => return Err(RpcError::new("unimplemented", "unknown RPC", 501)),
    };
    Ok((lease_rpc::checked_resource(resource)?, action))
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Window(u64),
    Decision(Decision),
}

pub(super) async fn execute(
    state: &State,
    schedule: Schedule,
    subject: String,
    deadline: u64,
    action: Action,
) -> RpcResult<Response> {
    let reply = snapshot::commit(state, schedule, deadline, "cooldown/v1", move |raw, now| {
        let mut cooldown = raw.map(Cooldown::restore).transpose()?.unwrap_or_default();
        let result = match action {
            Action::Prepare => cooldown.prepare(now).map(Reply::Window),
            Action::Admit {
                window_id,
                request_id,
                interval_ms,
            } => cooldown
                .try_admit(
                    now,
                    &subject,
                    Attempt {
                        window_id,
                        request_id: &request_id,
                        interval_ms,
                    },
                )
                .map(Reply::Decision),
        };
        Ok((
            serde_json::to_string(&cooldown).map_err(|e| e.to_string())?,
            result,
        ))
    })
    .await?;
    let bytes = match reply.map_err(|e| match e {
        Rejection::Clock => unavailable(),
        Rejection::InvalidWindow => invalid("invalid cooldown window"),
        Rejection::InvalidArgument => {
            invalid("invalid attempt or cooldown interval (1..86400000 ms)")
        }
        Rejection::RequestConflict => {
            RpcError::new("already_exists", "winning attempt changed interval", 409)
        }
    })? {
        Reply::Window(id) => GetCooldownWindowResponse {
            window_id: id.to_string(),
        }
        .encode_to_vec(),
        Reply::Decision(d) => {
            let (status, retry_at_unix_ms, admitted_at_unix_ms) = match d {
                Decision::Admitted {
                    admitted_at_ms,
                    retry_at_ms,
                } => (CooldownStatus::Admitted, retry_at_ms, Some(admitted_at_ms)),
                Decision::CoolingDown { retry_at_ms } => {
                    (CooldownStatus::CoolingDown, retry_at_ms, None)
                }
                Decision::Superseded { retry_at_ms } => {
                    (CooldownStatus::Superseded, retry_at_ms, None)
                }
            };
            TryAdmitResponse {
                status: status.into(),
                retry_at_unix_ms,
                admitted_at_unix_ms,
            }
            .encode_to_vec()
        }
    };
    snapshot::response(bytes)
}
